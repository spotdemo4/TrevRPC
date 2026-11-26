package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	// slowUS is --slow-us: the latency in microseconds above which an
	// operation counts as slow, or 0 to not count.
	slowUS int
	// timelineMS is --timeline-ms: the width in milliseconds of the intervals
	// the delivered messages are counted in, or 0 to not count.
	timelineMS int
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
	fs.IntVar(&cfg.slowUS, "slow-us", 0, "count the operations slower than this many microseconds as slow; 0 counts none")
	fs.IntVar(&cfg.timelineMS, "timeline-ms", 0, "count the messages delivered in each interval of this many milliseconds of the window; 0 counts none")
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
	case cfg.slowUS < 0, cfg.timelineMS < 0:
		return cfg, errors.New("--slow-us and --timeline-ms must be >= 0")
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
	// Slow counts the measured operations slower than --slow-us, and is nil
	// without it.
	Slow *uint64 `json:"slow,omitempty"`
	// Timeline counts the messages delivered in each interval of the window,
	// and is nil without --timeline-ms.
	Timeline *timelineResult `json:"timeline,omitempty"`
}

// timelineResult is the timeline field of the result line.
type timelineResult struct {
	StartUnixUS int64    `json:"start_unix_us"`
	BucketMS    int      `json:"bucket_ms"`
	Msgs        []uint64 `json:"msgs"`
}

// timeline counts messages in the consecutive intervals, buckets, of the
// measured window [start, end). Only one goroutine writes it, so each worker
// has its own, which summarize adds up. A nil timeline counts nothing.
type timeline struct {
	start, end time.Time
	bucket     time.Duration
	msgs       []uint64
}

// newTimeline returns a timeline with ceil(window / bucket) buckets. The window
// is rounded to a microsecond first, so that --duration 0.3, which converts to
// a few nanoseconds more or less than 300 ms, does not gain or lose a bucket
// against --timeline-ms 100.
func newTimeline(start, end time.Time, bucket time.Duration) *timeline {
	buckets := int((end.Sub(start).Round(time.Microsecond) + bucket - 1) / bucket)
	return &timeline{start: start, end: end, bucket: bucket, msgs: make([]uint64, buckets)}
}

// add counts msgs in the bucket of the instant at, unless the window does not
// contain it.
func (t *timeline) add(at time.Time, msgs int) {
	if t == nil || at.Before(t.start) || !at.Before(t.end) {
		return
	}
	t.msgs[min(int(at.Sub(t.start)/t.bucket), len(t.msgs)-1)] += uint64(msgs)
}

// workerOptions are the optional counts of a worker, both off in its zero value.
type workerOptions struct {
	// slowAfter is the latency above which a measured operation counts in
	// workerResult.slow, or 0 to count none.
	slowAfter time.Duration
	// timeline counts the messages of every operation that succeeds, by the
	// instant it ends, or is nil.
	timeline *timeline
}

// newWorkerOptions returns the options --slow-us and --timeline-ms ask for,
// for the window [start, end).
func newWorkerOptions(cfg benchConfig, start, end time.Time) workerOptions {
	options := workerOptions{slowAfter: time.Duration(cfg.slowUS) * time.Microsecond}
	if cfg.timelineMS > 0 {
		options.timeline = newTimeline(start, end, time.Duration(cfg.timelineMS)*time.Millisecond)
	}
	return options
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
	// slow counts the latencies above workerOptions.slowAfter.
	slow     uint64
	timeline *timeline
	// firstError is the first of errors, which reportErrors prints.
	firstError error
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
	sampler := newWireSampler(conn.wire, cfg.concurrency, measureStart, measureEnd)
	var wg sync.WaitGroup
	for i, w := range workers {
		wg.Go(func() {
			results[i] = runWorker(runCtx, w, sampler, i, measureStart, measureEnd, newWorkerOptions(cfg, measureStart, measureEnd))
		})
	}

	ok := sleepUntil(ctx, measureStart)
	if ok {
		sampler.observe(time.Now())
		ok = sleepUntil(ctx, measureEnd)
	}
	if ok {
		sampler.observe(time.Now())
	}
	wg.Wait()
	reportErrors(os.Stderr, results)
	if !ok {
		return zero, ctx.Err()
	}
	return summarize(cfg, results, sampler.sample())
}

// wireSample is the bytes the connection carried between two instants, and
// the messages the workers delivered between the same two instants.
type wireSample struct {
	bytes, msgs uint64
}

// wireSampler samples the connection's byte total and the workers' delivered
// messages at the start and at the end of the measured window, so the bytes
// are divided by the messages of the same interval. Whoever first sees a
// boundary pass takes its sample: the main goroutine when its timer fires, or
// a worker about to start an operation. A busy machine delays the timer, but
// every operation the window measures starts after the start sample and ends
// before the end sample.
type wireSampler struct {
	counter                  *wireCounter
	delivered                []atomic.Uint64
	measureStart, measureEnd time.Time
	begin, end               sync.Once
	// at holds the byte total and the delivered messages at the start and at
	// the end.
	at [2]wireSample
}

func newWireSampler(counter *wireCounter, workers int, measureStart, measureEnd time.Time) *wireSampler {
	return &wireSampler{counter: counter, delivered: make([]atomic.Uint64, workers), measureStart: measureStart, measureEnd: measureEnd}
}

// deliver counts msgs delivered by worker.
func (s *wireSampler) deliver(worker int, msgs uint64) {
	s.delivered[worker].Add(msgs)
}

// observe takes the sample of each boundary that now has passed, unless it is
// already taken. The start sample is always taken before the end sample.
func (s *wireSampler) observe(now time.Time) {
	if now.Before(s.measureStart) {
		return
	}
	s.begin.Do(func() { s.at[0] = s.snapshot() })
	if !now.Before(s.measureEnd) {
		s.end.Do(func() { s.at[1] = s.snapshot() })
	}
}

func (s *wireSampler) snapshot() wireSample {
	var msgs uint64
	for i := range s.delivered {
		msgs += s.delivered[i].Load()
	}
	return wireSample{bytes: s.counter.total(), msgs: msgs}
}

// sample returns what changed between the two samples, once both are taken.
func (s *wireSampler) sample() wireSample {
	return wireSample{bytes: s.at[1].bytes - s.at[0].bytes, msgs: s.at[1].msgs - s.at[0].msgs}
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
// window; failures during warmup are ignored. The first such error is kept.
// The worker tells sampler, as worker index, when it starts an operation and
// the messages of every operation that succeeds, in the window or not, and
// counts them in options.timeline by the instant the operation ends.
func runWorker(ctx context.Context, w worker, sampler *wireSampler, index int, measureStart, measureEnd time.Time, options workerOptions) workerResult {
	result := workerResult{timeline: options.timeline}
	for {
		opStart := time.Now()
		sampler.observe(opStart)
		if !opStart.Before(measureEnd) || ctx.Err() != nil {
			return result
		}
		msgs, err := w.do(ctx)
		if err == nil {
			// Before opEnd is read, so that an operation that ends in the
			// window is delivered before the end sample.
			sampler.deliver(index, uint64(msgs))
		}
		opEnd := time.Now()
		inWindow := !opStart.Before(measureStart)
		if err == nil {
			result.timeline.add(opEnd, msgs)
		}
		if err != nil {
			if inWindow {
				if result.errors == 0 {
					result.firstError = err
				}
				result.errors++
			}
			if !sleepUntil(ctx, opEnd.Add(errorBackoff)) {
				return result
			}
			continue
		}
		if inWindow && !opEnd.After(measureEnd) {
			latency := opEnd.Sub(opStart)
			result.latencies = append(result.latencies, latency)
			if options.slowAfter > 0 && latency > options.slowAfter {
				result.slow++
			}
			result.msgs += uint64(msgs)
		}
	}
}

// reportErrors prints the first failed call of every worker that had one, so
// the log of a run that reports errors says why.
func reportErrors(w io.Writer, results []workerResult) {
	for i, r := range results {
		if r.firstError != nil {
			_, _ = fmt.Fprintf(w, "worker %d: %d call(s) failed, first: %v\n", i, r.errors, r.firstError)
		}
	}
}

// errStarved is the error of a run in which no operation completed inside the
// measured window and none failed: every call ran past it.
var errStarved = errors.New("no operation completed inside the measured window")

func summarize(cfg benchConfig, results []workerResult, wire wireSample) (benchResult, error) {
	var latencies []time.Duration
	var msgs, errs, slow uint64
	var timeline *timelineResult
	for _, r := range results {
		latencies = append(latencies, r.latencies...)
		msgs += r.msgs
		errs += r.errors
		slow += r.slow
		if t := r.timeline; t != nil {
			if timeline == nil {
				timeline = &timelineResult{StartUnixUS: t.start.UnixMicro(), BucketMS: cfg.timelineMS, Msgs: make([]uint64, len(t.msgs))}
			}
			for i, count := range t.msgs {
				timeline.Msgs[i] += count
			}
		}
	}
	if len(latencies) == 0 && errs == 0 {
		return benchResult{}, errStarved
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
	res.Timeline = timeline
	if cfg.slowUS > 0 {
		res.Slow = &slow
	}
	// A sampler starved until the window had passed sees no message; the
	// window's are then the only count there is.
	wireMsgs := wire.msgs
	if wireMsgs == 0 {
		wireMsgs = msgs
	}
	if wireMsgs > 0 {
		perMsg := float64(wire.bytes) / float64(wireMsgs)
		res.WireBytesPerMsg = &perMsg
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
