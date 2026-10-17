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
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
)

// benchGrace is how long operations that are still running when the measured
// window ends get to finish before they are cancelled.
const benchGrace = scenarioTimeout

// benchErrorBackoff is how long a worker pauses after a failed operation, so a
// dead connection does not turn into a busy loop.
const benchErrorBackoff = time.Millisecond

type benchConfig struct {
	connectionConfig
	scenario    string
	size        int
	concurrency int
	messages    int
	duration    float64
	warmup      float64
}

func (c *benchConfig) register(flags *flag.FlagSet) {
	c.connectionConfig.register(flags, "transport: quic or tcp (QMux over TLS)")
	flags.StringVar(&c.scenario, "scenario", "", "unary, server-stream, client-stream or bidi")
	flags.IntVar(&c.size, "size", 64, "length of EchoRequest.message in bytes")
	flags.IntVar(&c.concurrency, "concurrency", 1, "workers, each with one call or stream in flight")
	flags.IntVar(&c.messages, "messages", 100, "messages per streaming call")
	flags.Float64Var(&c.duration, "duration", 5, "measured seconds")
	flags.Float64Var(&c.warmup, "warmup", 1, "seconds run before measuring")
}

func (c *benchConfig) validate() error {
	if err := c.check("bench", "quic", "tcp"); err != nil {
		return err
	}
	if _, ok := benchScenarios[c.scenario]; !ok {
		return fmt.Errorf("bench: unknown --scenario %q", c.scenario)
	}
	switch {
	case c.size < 0:
		return errors.New("bench: --size must not be negative")
	case c.concurrency < 1:
		return errors.New("bench: --concurrency must be at least 1")
	case c.messages < 1 || c.messages > math.MaxUint32:
		return errors.New("bench: --messages must be between 1 and 2^32-1")
	case !(c.duration > 0):
		return errors.New("bench: --duration must be positive")
	case !(c.warmup >= 0):
		return errors.New("bench: --warmup must not be negative")
	}
	return nil
}

// benchOperation is one worker's unit of work. A worker runs its operations
// one after another and never concurrently.
type benchOperation interface {
	// run performs one operation and returns the application messages it
	// carried. The context bounds the operation; a stream it opens lives as
	// long as the operation's owner.
	run(ctx context.Context) (messages int, err error)
	// close releases whatever the operation holds across runs.
	close()
}

// benchScenarios builds the operation of one worker for each scenario of
// bench/README.md.
var benchScenarios = map[string]func(client *interoppb.EchoClient, config benchConfig) benchOperation{
	"unary": func(client *interoppb.EchoClient, config benchConfig) benchOperation {
		return &unaryOperation{client: client, message: strings.Repeat("x", config.size)}
	},
	"server-stream": func(client *interoppb.EchoClient, config benchConfig) benchOperation {
		return &serverStreamOperation{client: client, message: strings.Repeat("x", config.size), count: config.messages}
	},
	"client-stream": func(client *interoppb.EchoClient, config benchConfig) benchOperation {
		return &clientStreamOperation{client: client, message: strings.Repeat("x", config.size), count: config.messages}
	},
	"bidi": func(client *interoppb.EchoClient, config benchConfig) benchOperation {
		return &bidiOperation{client: client, message: strings.Repeat("x", config.size)}
	},
}

type unaryOperation struct {
	client  *interoppb.EchoClient
	message string
}

func (o *unaryOperation) run(ctx context.Context) (int, error) {
	response, err := o.client.Unary(ctx, &interoppb.EchoRequest{Message: o.message})
	if err != nil {
		return 0, err
	}
	if len(response.Message) != len(o.message) {
		return 0, fmt.Errorf("response has %d bytes, want %d", len(response.Message), len(o.message))
	}
	return 1, nil
}

func (*unaryOperation) close() {}

type serverStreamOperation struct {
	client  *interoppb.EchoClient
	message string
	count   int
}

func (o *serverStreamOperation) run(ctx context.Context) (int, error) {
	stream, err := o.client.ServerStream(ctx, &interoppb.EchoRequest{Message: o.message, Count: uint32(o.count)})
	if err != nil {
		return 0, err
	}
	defer closeQuietly(stream)
	received := 0
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if len(response.Message) != len(o.message) {
			return 0, fmt.Errorf("response has %d bytes, want %d", len(response.Message), len(o.message))
		}
		received++
	}
	if received != o.count {
		return 0, fmt.Errorf("received %d responses, want %d", received, o.count)
	}
	return received, nil
}

func (*serverStreamOperation) close() {}

type clientStreamOperation struct {
	client  *interoppb.EchoClient
	message string
	count   int
}

func (o *clientStreamOperation) run(ctx context.Context) (int, error) {
	call, err := o.client.ClientStream(ctx)
	if err != nil {
		return 0, err
	}
	defer closeQuietly(call)
	request := &interoppb.EchoRequest{Message: o.message}
	for range o.count {
		if err := call.Send(request); err != nil {
			return 0, err
		}
	}
	response, err := call.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	if int(response.Index) != o.count || len(response.Message) != o.count*len(o.message) {
		return 0, fmt.Errorf("response covers %d messages and %d bytes, want %d and %d",
			response.Index, len(response.Message), o.count, o.count*len(o.message))
	}
	return o.count, nil
}

func (*clientStreamOperation) close() {}

// bidiOperation keeps one Bidi stream open across operations, opening it again
// after a failure.
type bidiOperation struct {
	client  *interoppb.EchoClient
	message string
	call    trevrpc.BidirectionalStreamingCall[*interoppb.EchoRequest, *interoppb.EchoResponse]
}

func (o *bidiOperation) run(ctx context.Context) (int, error) {
	if o.call == nil {
		call, err := o.client.Bidi(ctx)
		if err != nil {
			return 0, err
		}
		o.call = call
	}
	if err := o.roundTrip(); err != nil {
		o.close()
		return 0, err
	}
	return 1, nil
}

func (o *bidiOperation) roundTrip() error {
	if err := o.call.Send(&interoppb.EchoRequest{Message: o.message}); err != nil {
		return err
	}
	response, err := o.call.Recv()
	if err != nil {
		return err
	}
	if len(response.Message) != len(o.message) {
		return fmt.Errorf("response has %d bytes, want %d", len(response.Message), len(o.message))
	}
	return nil
}

func (o *bidiOperation) close() {
	if o.call == nil {
		return
	}
	_ = o.call.CloseSend()
	closeQuietly(o.call)
	o.call = nil
}

// benchWorker holds what one worker measured. Only its goroutine writes it.
type benchWorker struct {
	latencies []time.Duration
	messages  uint64
	errors    uint64
}

// loop runs operations back to back until end. It counts only those that start
// and finish within [start, end].
func (w *benchWorker) loop(ctx context.Context, operation benchOperation, start, end time.Time) {
	defer operation.close()
	for {
		began := time.Now()
		if !began.Before(end) {
			return
		}
		messages, err := operation.run(ctx)
		finished := time.Now()
		if err != nil {
			if !began.Before(start) && !finished.After(end) {
				w.errors++
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(benchErrorBackoff):
			}
			continue
		}
		if !began.Before(start) && !finished.After(end) {
			w.latencies = append(w.latencies, finished.Sub(began))
			w.messages += uint64(messages)
		}
	}
}

// wireCounter is a qlog trace that sums the sizes of the QUIC packets a client
// connection sends and receives. It is the only per-connection byte counter
// quic-go exposes: Conn has no statistics API.
type wireCounter struct {
	sent     atomic.Uint64
	received atomic.Uint64
}

func (c *wireCounter) AddProducer() qlogwriter.Recorder { return c }

func (c *wireCounter) SupportsSchemas(schema string) bool { return schema == qlog.EventSchema }

func (c *wireCounter) RecordEvent(event qlogwriter.Event) {
	switch event := event.(type) {
	case qlog.PacketSent:
		c.sent.Add(uint64(event.Raw.Length))
	case qlog.PacketReceived:
		c.received.Add(uint64(event.Raw.Length))
	}
}

func (*wireCounter) Close() error { return nil }

func (c *wireCounter) total() uint64 { return c.sent.Load() + c.received.Load() }

// quicConfig makes every connection report its packets to c.
func (c *wireCounter) quicConfig() *quic.Config {
	return &quic.Config{Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return c }}
}

type benchLatency struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
}

// benchResult is the JSON line of bench/README.md.
type benchResult struct {
	Scenario        string       `json:"scenario"`
	Transport       string       `json:"transport"`
	Size            int          `json:"size"`
	Concurrency     int          `json:"concurrency"`
	DurationS       float64      `json:"duration_s"`
	Ops             uint64       `json:"ops"`
	Msgs            uint64       `json:"msgs"`
	Errors          uint64       `json:"errors"`
	OpsPerSec       float64      `json:"ops_per_sec"`
	MsgsPerSec      float64      `json:"msgs_per_sec"`
	BytesPerSec     float64      `json:"bytes_per_sec"`
	LatencyUS       benchLatency `json:"latency_us"`
	WireBytesPerMsg *float64     `json:"wire_bytes_per_msg,omitempty"`
}

// runBench connects, runs the scenario for the warmup and the measured window
// and prints the result as one JSON line to out.
func runBench(ctx context.Context, config benchConfig, out io.Writer) error {
	// Only the QUIC client exposes byte counters; QMux dials its own TLS
	// connection, which the client cannot observe.
	var wire *wireCounter
	var dialOptions trevrpc.DialOptions
	if config.transport == "quic" {
		wire = new(wireCounter)
		dialOptions.QUICConfig = wire.quicConfig()
	}
	channel, err := config.dial(ctx, dialOptions)
	if err != nil {
		return err
	}
	defer closeQuietly(channel)
	client := interoppb.NewEchoClient(channel)

	warmup := time.Duration(config.warmup * float64(time.Second))
	window := time.Duration(config.duration * float64(time.Second))
	start := time.Now().Add(warmup)
	end := start.Add(window)
	opCtx, cancel := context.WithDeadline(ctx, end.Add(benchGrace))
	defer cancel()

	workers := make([]benchWorker, config.concurrency)
	var group sync.WaitGroup
	for i := range workers {
		operation := benchScenarios[config.scenario](client, config)
		group.Go(func() { workers[i].loop(opCtx, operation, start, end) })
	}
	var wireBefore, wireAfter uint64
	waitErr := sleepUntil(ctx, start)
	if wire != nil {
		wireBefore = wire.total()
	}
	if waitErr == nil {
		waitErr = sleepUntil(ctx, end)
	}
	if wire != nil {
		wireAfter = wire.total()
	}
	if waitErr != nil {
		cancel()
	}
	group.Wait()
	if waitErr != nil {
		return waitErr
	}

	result, err := summarize(config, channel.Transport(), window, workers)
	if err != nil {
		return err
	}
	if wire != nil {
		perMsg := float64(wireAfter-wireBefore) / float64(result.Msgs)
		result.WireBytesPerMsg = &perMsg
	}
	return json.NewEncoder(out).Encode(result)
}

// sleepUntil waits for t, or fails when ctx ends first.
func sleepUntil(ctx context.Context, t time.Time) error {
	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// summarize turns what the workers measured into the result line.
func summarize(config benchConfig, transport trevrpc.TransportKind, window time.Duration, workers []benchWorker) (benchResult, error) {
	var latencies []time.Duration
	var messages, failures uint64
	for _, w := range workers {
		latencies = append(latencies, w.latencies...)
		messages += w.messages
		failures += w.errors
	}
	if len(latencies) == 0 {
		return benchResult{}, fmt.Errorf("no operation completed in the %s measured window (%d failed)", window, failures)
	}
	slices.Sort(latencies)
	seconds := window.Seconds()
	ops := uint64(len(latencies))
	return benchResult{
		Scenario:    config.scenario,
		Transport:   string(transport),
		Size:        config.size,
		Concurrency: config.concurrency,
		DurationS:   seconds,
		Ops:         ops,
		Msgs:        messages,
		Errors:      failures,
		OpsPerSec:   float64(ops) / seconds,
		MsgsPerSec:  float64(messages) / seconds,
		BytesPerSec: float64(messages) / seconds * float64(config.size),
		LatencyUS: benchLatency{
			P50:  percentileUS(latencies, 0.50),
			P90:  percentileUS(latencies, 0.90),
			P99:  percentileUS(latencies, 0.99),
			P999: percentileUS(latencies, 0.999),
		},
	}, nil
}

// percentileUS returns the nearest-rank percentile p of sorted latencies in
// microseconds.
func percentileUS(sorted []time.Duration, p float64) float64 {
	rank := max(int(math.Ceil(p*float64(len(sorted)))), 1)
	return float64(sorted[rank-1]) / float64(time.Microsecond)
}
