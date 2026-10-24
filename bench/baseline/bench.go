package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	// connectTimeout bounds the initial connection and TLS handshake.
	connectTimeout = 10 * time.Second
	// drainGrace is how long after the measured window a call may still run
	// before the run gives up on it.
	drainGrace = 10 * time.Second
	// errorBackoff keeps a worker from spinning when every call fails.
	errorBackoff = time.Millisecond
	// padKey is the request metadata key of --metadata-size.
	padKey = "bench-pad"
	// maxMetadataSize is the largest --metadata-size, the largest value
	// TrevRPC metadata allows, so both runtimes accept the same sizes.
	maxMetadataSize = 8 * 1024
)

type benchConfig struct {
	addr, ca, serverName, transport, scenario string
	size, concurrency, messages               int
	metadataSize                              int
	duration, warmup                          time.Duration
	payload, compression                      string
}

func parseBenchFlags(args []string) (benchConfig, error) {
	var (
		cfg              benchConfig
		duration, warmup float64
		tcpAddr          string
	)
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.StringVar(&cfg.addr, "addr", "", "server address (host:port)")
	fs.StringVar(&tcpAddr, "tcp-addr", "", "accepted and ignored; present for flag compatibility")
	fs.StringVar(&cfg.ca, "ca", "", "CA certificate PEM file")
	fs.StringVar(&cfg.serverName, "server-name", "localhost", "TLS server name")
	fs.StringVar(&cfg.transport, "transport", "grpc", "grpc|connect")
	fs.StringVar(&cfg.scenario, "scenario", "", "unary|server-stream|client-stream|bidi")
	fs.IntVar(&cfg.size, "size", 64, "EchoRequest.message length in bytes")
	fs.IntVar(&cfg.concurrency, "concurrency", 1, "number of workers")
	fs.Float64Var(&duration, "duration", 5, "measured seconds")
	fs.Float64Var(&warmup, "warmup", 1, "warmup seconds")
	fs.IntVar(&cfg.messages, "messages", 100, "messages per streaming call")
	fs.IntVar(&cfg.metadataSize, "metadata-size", 0, "bytes of the bench-pad request metadata entry sent with every call; 0 sends none")
	fs.StringVar(&cfg.payload, "payload", "repeat", "EchoRequest.message: repeat (x repeated) or records (a new log-records message per request)")
	fs.StringVar(&cfg.compression, "compression", "none", "none, gzip or zstd")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	cfg.duration = time.Duration(duration * float64(time.Second))
	cfg.warmup = time.Duration(warmup * float64(time.Second))
	switch {
	case cfg.addr == "" || cfg.ca == "":
		return cfg, errors.New("bench needs --addr and --ca")
	case !slices.Contains(transports, cfg.transport):
		return cfg, fmt.Errorf("unsupported --transport %q (this binary speaks %s)", cfg.transport, strings.Join(transports, ", "))
	case !slices.Contains(scenarios, cfg.scenario):
		return cfg, fmt.Errorf("--scenario must be one of %s", strings.Join(scenarios, ", "))
	case cfg.size < 0, cfg.concurrency < 1, cfg.messages < 1:
		return cfg, errors.New("--size must be >= 0, --concurrency and --messages >= 1")
	case cfg.metadataSize < 0, cfg.metadataSize > maxMetadataSize:
		return cfg, fmt.Errorf("--metadata-size must be between 0 and %d", maxMetadataSize)
	case cfg.duration <= 0, cfg.warmup < 0:
		return cfg, errors.New("--duration must be > 0 and --warmup >= 0")
	case cfg.payload != "repeat" && cfg.payload != "records":
		return cfg, fmt.Errorf("--payload must be repeat or records, not %q", cfg.payload)
	case !slices.Contains(compressions, cfg.compression):
		return cfg, fmt.Errorf("--compression must be one of %s", strings.Join(compressions, ", "))
	case cfg.compression != "none" && !slices.Contains(compressedScenarios, cfg.scenario):
		return cfg, fmt.Errorf("--compression %s does not run the %s scenario", cfg.compression, cfg.scenario)
	}
	return cfg, nil
}

var (
	transports = []string{"grpc", "connect"}
	scenarios  = []string{"unary", "server-stream", "client-stream", "bidi"}
)

// latencySummary is in microseconds.
type latencySummary struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
}

// benchResult is the JSON line described in bench/README.md.
type benchResult struct {
	Scenario        string         `json:"scenario"`
	Transport       string         `json:"transport"`
	Size            int            `json:"size"`
	MetadataSize    int            `json:"metadata_size"`
	Concurrency     int            `json:"concurrency"`
	DurationS       float64        `json:"duration_s"`
	Ops             uint64         `json:"ops"`
	Msgs            uint64         `json:"msgs"`
	Errors          uint64         `json:"errors"`
	OpsPerSec       float64        `json:"ops_per_sec"`
	MsgsPerSec      float64        `json:"msgs_per_sec"`
	BytesPerSec     float64        `json:"bytes_per_sec"`
	LatencyUS       latencySummary `json:"latency_us"`
	WireBytesPerMsg *float64       `json:"wire_bytes_per_msg,omitempty"`
	Payload         string         `json:"payload"`
	Compression     string         `json:"compression"`
}

func (r benchResult) write(w io.Writer) error {
	return json.NewEncoder(w).Encode(r)
}

// workerResult is what one worker measured. Each worker owns its slice, so no
// locking is needed on the hot path.
type workerResult struct {
	latencies []time.Duration
	msgs      uint64
	errors    uint64
}

// runBench connects, runs the warmup and the measured window, and summarizes.
// All workers share one connection; see dial.
func runBench(ctx context.Context, cfg benchConfig) (benchResult, error) {
	var zero benchResult
	conn, err := dial(ctx, cfg)
	if err != nil {
		return zero, err
	}
	defer conn.close()

	// runCtx bounds every call; the deadline is armed once the window is known.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := make([]worker, cfg.concurrency)
	for i := range workers {
		workers[i] = newWorker(runCtx, conn.client, cfg, newBenchPayload(cfg.payload, cfg.size, i))
	}
	defer func() {
		for _, w := range workers {
			w.close()
		}
	}()

	start := time.Now()
	measureStart := start.Add(cfg.warmup)
	measureEnd := measureStart.Add(cfg.duration)
	defer time.AfterFunc(time.Until(measureEnd.Add(drainGrace)), cancel).Stop()

	results := make([]workerResult, cfg.concurrency)
	var wg sync.WaitGroup
	for i, w := range workers {
		wg.Go(func() { results[i] = runWorker(runCtx, w, measureStart, measureEnd) })
	}

	wireStart, wireEnd, ok := snapshotWire(ctx, conn.wire, measureStart, measureEnd)
	wg.Wait()
	if !ok {
		return zero, ctx.Err()
	}
	return summarize(cfg, results, wireEnd-wireStart)
}

// snapshotWire returns the connection's byte total at the start and end of the
// measured window. ok is false if ctx ended first.
func snapshotWire(ctx context.Context, counter *wireCounter, measureStart, measureEnd time.Time) (start, end uint64, ok bool) {
	if !sleepUntil(ctx, measureStart) {
		return 0, 0, false
	}
	start = counter.total()
	if !sleepUntil(ctx, measureEnd) {
		return 0, 0, false
	}
	return start, counter.total(), true
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// runWorker runs operations back to back until the measured window ends. An
// operation counts only if it starts at or after measureStart and finishes by
// measureEnd. A failed operation counts as an error if it started inside the
// window; failures during warmup are ignored.
func runWorker(ctx context.Context, w worker, measureStart, measureEnd time.Time) workerResult {
	var result workerResult
	for {
		opStart := time.Now()
		if !opStart.Before(measureEnd) || ctx.Err() != nil {
			return result
		}
		msgs, err := w.do(ctx)
		opEnd := time.Now()
		inWindow := !opStart.Before(measureStart)
		if err != nil {
			if inWindow {
				result.errors++
			}
			if !sleepUntil(ctx, opEnd.Add(errorBackoff)) {
				return result
			}
			continue
		}
		if inWindow && !opEnd.After(measureEnd) {
			result.latencies = append(result.latencies, opEnd.Sub(opStart))
			result.msgs += uint64(msgs)
		}
	}
}

func summarize(cfg benchConfig, results []workerResult, wireBytes uint64) (benchResult, error) {
	var latencies []time.Duration
	var msgs, errs uint64
	for _, r := range results {
		latencies = append(latencies, r.latencies...)
		msgs += r.msgs
		errs += r.errors
	}
	if len(latencies) == 0 && errs == 0 {
		return benchResult{}, errors.New("no operation completed inside the measured window")
	}
	slices.Sort(latencies)

	seconds := cfg.duration.Seconds()
	res := benchResult{
		Scenario:     cfg.scenario,
		Transport:    cfg.transport,
		Size:         cfg.size,
		MetadataSize: cfg.metadataSize,
		Concurrency:  cfg.concurrency,
		Payload:      cfg.payload,
		Compression:  cfg.compression,
		DurationS:    seconds,
		Ops:          uint64(len(latencies)),
		Msgs:         msgs,
		Errors:       errs,
		OpsPerSec:    float64(len(latencies)) / seconds,
		MsgsPerSec:   float64(msgs) / seconds,
		LatencyUS: latencySummary{
			P50:  percentile(latencies, 0.50),
			P90:  percentile(latencies, 0.90),
			P99:  percentile(latencies, 0.99),
			P999: percentile(latencies, 0.999),
		},
	}
	res.BytesPerSec = res.MsgsPerSec * float64(cfg.size)
	if msgs > 0 {
		wire := float64(wireBytes) / float64(msgs)
		res.WireBytesPerMsg = &wire
	}
	return res, nil
}

// percentile returns the nearest-rank percentile of sorted, in microseconds.
func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := max(int(math.Ceil(p*float64(len(sorted))))-1, 0)
	return float64(sorted[min(rank, len(sorted)-1)]) / float64(time.Microsecond)
}
