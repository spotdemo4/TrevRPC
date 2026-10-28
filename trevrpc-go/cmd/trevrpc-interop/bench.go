package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
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

// pacedDeadline is how long after its send a paced message's echo may arrive
// and still count as delivered. A later echo is too late for real-time
// traffic, so it counts as lost.
const pacedDeadline = 500 * time.Millisecond

const (
	// benchPadKey is the request metadata key of --metadata-size.
	benchPadKey = "bench-pad"
	// benchMaxMetadataSize is the largest value TrevRPC metadata allows.
	benchMaxMetadataSize = 8 * 1024
)

type benchConfig struct {
	connectionConfig
	scenario string
	size     int
	metadata int
	// metadataScope is where the bench-pad entry goes: "call" sends it with
	// every call, "connection" as connection metadata.
	metadataScope string
	concurrency   int
	// connections is how many channels the workers spread over.
	connections int
	messages    int
	rate        float64
	duration    float64
	warmup      float64
	payload     string
	compression string
	dictionary  string
}

func (c *benchConfig) register(flags *flag.FlagSet) {
	c.connectionConfig.register(flags, "transport: quic or tcp (QMux over TLS)")
	flags.StringVar(&c.scenario, "scenario", "", "unary, server-stream, client-stream, bidi, unreliable-server-stream, unreliable-client-stream, paced or unreliable-paced")
	flags.IntVar(&c.size, "size", 64, "length of EchoRequest.message in bytes")
	flags.IntVar(&c.metadata, "metadata-size", 0, "bytes of the bench-pad request metadata entry sent with every call; 0 sends none")
	flags.StringVar(&c.metadataScope, "metadata-scope", "call", "call (the bench-pad entry goes with every call) or connection (as connection metadata)")
	flags.IntVar(&c.connections, "connections", 1, "channels, each with its own connection, that the workers spread over in turn")
	flags.IntVar(&c.concurrency, "concurrency", 1, "workers, each with one call or stream in flight")
	flags.IntVar(&c.messages, "messages", 100, "messages per streaming call")
	flags.Float64Var(&c.rate, "rate", 100, "messages per second each paced or unreliable-paced worker sends")
	flags.Float64Var(&c.duration, "duration", 5, "measured seconds")
	flags.Float64Var(&c.warmup, "warmup", 1, "seconds run before measuring")
	flags.StringVar(&c.payload, "payload", "repeat", "EchoRequest.message: repeat (x repeated) or records (a new log-records message per request)")
	flags.StringVar(&c.compression, "compression", "none", "none, dictionary, stream or stream-dictionary")
	flags.StringVar(&c.dictionary, "dictionary", "", "zstd dictionary file for the dictionary and stream-dictionary compression modes")
}

// benchCompressions are the --compression modes and the scenarios each runs.
// The stream modes call CompressedEcho, whose streaming methods allow stream
// compression and whose Unary allows compression without a dictionary.
var benchCompressions = map[string][]string{
	"none":              nil, // every scenario
	"dictionary":        {"unary", "client-stream", "bidi"},
	"stream":            {"unary", "client-stream", "bidi"},
	"stream-dictionary": {"unary", "client-stream", "bidi"},
}

// usesDictionary reports whether the compression mode needs --dictionary.
func (c *benchConfig) usesDictionary() bool {
	return c.compression == "dictionary" || c.compression == "stream-dictionary"
}

// streamCompressed reports whether the compression mode calls CompressedEcho.
func (c *benchConfig) streamCompressed() bool {
	return c.compression == "stream" || c.compression == "stream-dictionary"
}

func (c *benchConfig) validate() error {
	if err := c.check("bench", "quic", "tcp"); err != nil {
		return err
	}
	if _, ok := benchScenarios[c.scenario]; !ok {
		return fmt.Errorf("bench: unknown --scenario %q", c.scenario)
	}
	scenarios, ok := benchCompressions[c.compression]
	switch {
	case !ok:
		return fmt.Errorf("bench: unknown --compression %q", c.compression)
	case scenarios != nil && !slices.Contains(scenarios, c.scenario):
		return fmt.Errorf("bench: --compression %s does not run the %s scenario", c.compression, c.scenario)
	case c.usesDictionary() != (c.dictionary != ""):
		return fmt.Errorf("bench: --compression %s and --dictionary go together", c.compression)
	case c.payload != "repeat" && c.payload != "records":
		return fmt.Errorf("bench: unknown --payload %q", c.payload)
	case c.metadataScope != "call" && c.metadataScope != "connection":
		return fmt.Errorf("bench: unknown --metadata-scope %q", c.metadataScope)
	}
	switch {
	case c.size < 0:
		return errors.New("bench: --size must not be negative")
	case c.metadata < 0 || c.metadata > benchMaxMetadataSize:
		return fmt.Errorf("bench: --metadata-size must be between 0 and %d", benchMaxMetadataSize)
	case c.concurrency < 1:
		return errors.New("bench: --concurrency must be at least 1")
	case c.connections < 1 || c.connections > c.concurrency:
		return errors.New("bench: --connections must be between 1 and --concurrency")
	case c.messages < 1 || c.messages > math.MaxUint32:
		return errors.New("bench: --messages must be between 1 and 2^32-1")
	case !(c.rate > 0) || time.Duration(float64(time.Second)/c.rate) < 1:
		return errors.New("bench: --rate must be positive and at most 10^9")
	case !(c.duration > 0):
		return errors.New("bench: --duration must be positive")
	case !(c.warmup >= 0):
		return errors.New("bench: --warmup must not be negative")
	}
	return nil
}

// paced reports whether the scenario sends at --rate.
func (c *benchConfig) paced() bool {
	return c.scenario == "paced" || c.scenario == "unreliable-paced"
}

// interval is the time between the messages of a paced worker.
func (c *benchConfig) interval() time.Duration {
	return time.Duration(float64(time.Second) / c.rate)
}

// padMetadata returns the bench-pad request metadata entry, if --metadata-size
// asks for one.
func (c *benchConfig) padMetadata() trevrpc.Metadata {
	if c.metadata == 0 {
		return nil
	}
	return trevrpc.Metadata{benchPadKey: bytes.Repeat([]byte{'x'}, c.metadata)}
}

// callOptions returns the options every call carries: the bench-pad entry,
// unless it is connection metadata.
func (c *benchConfig) callOptions() []trevrpc.CallOption {
	if c.metadata == 0 || c.metadataScope == "connection" {
		return nil
	}
	return []trevrpc.CallOption{trevrpc.WithMetadata(benchPadKey, bytes.Repeat([]byte{'x'}, c.metadata))}
}

// benchWorkload is what one worker runs from the start of the warmup to the
// end of the measured window, recording what it measured in w.
type benchWorkload interface {
	work(ctx context.Context, w *benchWorker, start, end time.Time)
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

// lossyOperation is implemented by the operations whose messages may be lost.
type lossyOperation interface {
	// lost returns how many messages the last successful run sent but did not
	// deliver. Loss is not an error.
	lost() int
}

// benchClients are the clients of the services the scenarios call.
type benchClients struct {
	echo       *interoppb.EchoClient
	unreliable *interoppb.UnreliableEchoClient
	compressed *interoppb.CompressedEchoClient
}

// The calls of Echo and CompressedEcho, which have the same messages.
type (
	unaryCall        func(context.Context, *interoppb.EchoRequest, ...trevrpc.CallOption) (*interoppb.EchoResponse, error)
	openClientStream func(context.Context, ...trevrpc.CallOption) (trevrpc.ClientStreamingCall[*interoppb.EchoRequest, *interoppb.EchoResponse], error)
	openBidi         func(context.Context, ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*interoppb.EchoRequest, *interoppb.EchoResponse], error)
)

// unary returns the Unary call the compression mode uses.
func (c benchClients) unary(config benchConfig) unaryCall {
	if config.streamCompressed() {
		return c.compressed.Unary
	}
	return c.echo.Unary
}

// clientStream returns the ClientStream call the compression mode uses.
func (c benchClients) clientStream(config benchConfig) openClientStream {
	if config.streamCompressed() {
		return c.compressed.ClientStream
	}
	return c.echo.ClientStream
}

// bidi returns the Bidi call the compression mode uses.
func (c benchClients) bidi(config benchConfig) openBidi {
	if config.streamCompressed() {
		return c.compressed.Bidi
	}
	return c.echo.Bidi
}

// benchScenarios builds the operation of one worker for each scenario of
// bench/README.md. payload supplies the worker's request messages, all of
// --size bytes.
var benchScenarios = map[string]func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload{
	"unary": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return operationLoop{&unaryOperation{call: clients.unary(config), options: config.callOptions(), payload: payload}}
	},
	"server-stream": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return operationLoop{&serverStreamOperation{client: clients.echo, options: config.callOptions(), payload: payload, count: config.messages}}
	},
	"client-stream": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return operationLoop{&clientStreamOperation{open: clients.clientStream(config), options: config.callOptions(), payload: payload, count: config.messages}}
	},
	"bidi": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return operationLoop{&bidiOperation{open: clients.bidi(config), options: config.callOptions(), payload: payload}}
	},
	"unreliable-server-stream": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return operationLoop{&unreliableServerStreamOperation{client: clients.unreliable, options: config.callOptions(), message: payload.next(), count: config.messages}}
	},
	"unreliable-client-stream": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return operationLoop{&unreliableClientStreamOperation{client: clients.unreliable, options: config.callOptions(), message: payload.next(), count: config.messages}}
	},
	"paced": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return &pacedWorkload{open: clients.echo.Bidi, options: config.callOptions(), payload: payload, interval: config.interval()}
	},
	"unreliable-paced": func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		return &pacedWorkload{open: clients.unreliable.Bidi, options: config.callOptions(), payload: payload, interval: config.interval()}
	},
}

type unaryOperation struct {
	call    unaryCall
	options []trevrpc.CallOption
	payload benchPayload
}

func (o *unaryOperation) run(ctx context.Context) (int, error) {
	message := o.payload.next()
	response, err := o.call(ctx, &interoppb.EchoRequest{Message: message}, o.options...)
	if err != nil {
		return 0, err
	}
	if len(response.Message) != len(message) {
		return 0, fmt.Errorf("response has %d bytes, want %d", len(response.Message), len(message))
	}
	return 1, nil
}

func (*unaryOperation) close() {}

type serverStreamOperation struct {
	client  *interoppb.EchoClient
	options []trevrpc.CallOption
	payload benchPayload
	count   int
}

func (o *serverStreamOperation) run(ctx context.Context) (int, error) {
	message := o.payload.next()
	stream, err := o.client.ServerStream(ctx, &interoppb.EchoRequest{Message: message, Count: uint32(o.count)}, o.options...)
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
		if len(response.Message) != len(message) {
			return 0, fmt.Errorf("response has %d bytes, want %d", len(response.Message), len(message))
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
	open    openClientStream
	options []trevrpc.CallOption
	payload benchPayload
	count   int
}

func (o *clientStreamOperation) run(ctx context.Context) (int, error) {
	call, err := o.open(ctx, o.options...)
	if err != nil {
		return 0, err
	}
	defer closeQuietly(call)
	sent := 0
	for range o.count {
		message := o.payload.next()
		sent += len(message)
		if err := call.Send(&interoppb.EchoRequest{Message: message}); err != nil {
			return 0, err
		}
	}
	response, err := call.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	if int(response.Index) != o.count || len(response.Message) != sent {
		return 0, fmt.Errorf("response covers %d messages and %d bytes, want %d and %d",
			response.Index, len(response.Message), o.count, sent)
	}
	return o.count, nil
}

func (*clientStreamOperation) close() {}

// unreliableServerStreamOperation sends one request that asks for count
// responses on an UnreliableEcho.Bidi stream and reads the responses that
// arrive until the status. The responses may travel as datagrams, and the
// ones that do not arrive are lost.
type unreliableServerStreamOperation struct {
	client  *interoppb.UnreliableEchoClient
	options []trevrpc.CallOption
	message string
	count   int
	missing int
}

func (o *unreliableServerStreamOperation) run(ctx context.Context) (int, error) {
	call, err := o.client.Bidi(ctx, o.options...)
	if err != nil {
		return 0, err
	}
	defer closeQuietly(call)
	if err := call.Send(&interoppb.EchoRequest{Message: o.message, Count: uint32(o.count)}); err != nil {
		return 0, err
	}
	if err := call.CloseSend(); err != nil {
		return 0, err
	}
	received := 0
	for {
		response, err := call.Recv()
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
	if received > o.count {
		return 0, fmt.Errorf("received %d responses, want at most %d", received, o.count)
	}
	o.missing = o.count - received
	return received, nil
}

func (o *unreliableServerStreamOperation) lost() int { return o.missing }

func (*unreliableServerStreamOperation) close() {}

// unreliableClientStreamOperation sends count requests that ask for no
// response on an UnreliableEcho.Bidi stream, and reads from the status how
// many of them the server received. The requests may travel as datagrams, and
// the ones that do not arrive are lost.
type unreliableClientStreamOperation struct {
	client  *interoppb.UnreliableEchoClient
	options []trevrpc.CallOption
	message string
	count   int
	missing int
}

func (o *unreliableClientStreamOperation) run(ctx context.Context) (int, error) {
	call, err := o.client.Bidi(ctx, o.options...)
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
	if err := call.CloseSend(); err != nil {
		return 0, err
	}
	if err := expectEnd(call); err != nil {
		return 0, err
	}
	status, ok := call.TerminalStatus()
	if !ok {
		return 0, errors.New("no terminal status after the end of the stream")
	}
	received, err := metadataCount(status.Metadata, receivedKey)
	if err != nil {
		return 0, err
	}
	if received > o.count {
		return 0, fmt.Errorf("server received %d requests, want at most %d", received, o.count)
	}
	o.missing = o.count - received
	return received, nil
}

func (o *unreliableClientStreamOperation) lost() int { return o.missing }

func (*unreliableClientStreamOperation) close() {}

// pacedWorkload keeps one Bidi stream open and sends a request that asks for
// one echo every interval, as real-time traffic does, while it reads the
// echoes as they arrive: on Echo.Bidi for paced, and on UnreliableEcho.Bidi,
// whose requests and echoes may travel as datagrams, for unreliable-paced.
// Each message scheduled inside the measured window is one operation:
// delivered, with the time from its send to its echo, if the echo arrives
// within pacedDeadline of the send, and lost otherwise.
type pacedWorkload struct {
	open     openBidi
	options  []trevrpc.CallOption
	payload  benchPayload
	interval time.Duration
}

func (p *pacedWorkload) work(ctx context.Context, w *benchWorker, start, end time.Time) {
	base := time.Now()
	// Sending goes on past the window, so the echoes of its last messages
	// arrive under the same load.
	total := int(end.Add(pacedDeadline).Sub(base)/p.interval) + 1
	schedule := pacedSchedule{base: base, sent: make([]atomic.Int64, total), rtts: make([]time.Duration, total)}
	for i := range schedule.rtts {
		schedule.rtts[i] = -1
	}
	for next := 0; next < total; {
		var err error
		next, err = p.stream(ctx, &schedule, next)
		if err == nil {
			break
		}
		if now := time.Now(); !now.Before(start) && !now.After(end) {
			w.errors++
		}
		if sleepUntil(ctx, time.Now().Add(benchErrorBackoff)) != nil {
			break
		}
		// The messages whose time passed while the stream was down are lost,
		// not sent late.
		next = max(next, int(time.Since(base)/p.interval)+1)
	}
	for i, rtt := range schedule.rtts {
		scheduled := base.Add(time.Duration(i) * p.interval)
		if scheduled.Before(start) || !scheduled.Before(end) {
			continue
		}
		if rtt >= 0 && rtt <= pacedDeadline {
			w.latencies = append(w.latencies, rtt)
			w.messages++
		} else {
			w.lost++
		}
	}
}

// pacedSchedule is what a paced worker knows of its messages,
// one scheduled every interval from base.
type pacedSchedule struct {
	base time.Time
	// sent[i] is when message i was sent, as nanoseconds after base plus one,
	// or 0 if it has not been. The sender writes it and the receiver reads it.
	sent []atomic.Int64
	// rtts[i] is how long after its send message i was echoed, or -1 if it
	// has not been. Only the receiver writes it.
	rtts []time.Duration
}

// stream opens a stream and sends messages next to the end of the schedule,
// each at its time, recording their echoes. It returns the first message it
// did not send.
func (p *pacedWorkload) stream(ctx context.Context, schedule *pacedSchedule, next int) (int, error) {
	call, err := p.open(ctx, p.options...)
	if err != nil {
		return next, err
	}
	received := make(chan error, 1)
	go func() { received <- receivePaced(call, schedule) }()
	for ; next < len(schedule.sent) && err == nil; next++ {
		if err = sleepUntil(ctx, schedule.base.Add(time.Duration(next)*p.interval)); err == nil {
			request := &interoppb.EchoRequest{Message: pacedMessage(next, p.payload.next()), Count: 1}
			schedule.sent[next].Store(int64(time.Since(schedule.base)) + 1)
			err = call.Send(request)
		}
	}
	if err == nil {
		err = call.CloseSend()
	}
	if err != nil {
		// Closing the call ends the receiver's Recv.
		closeQuietly(call)
		_ = <-received
		return next, err
	}
	err = <-received
	closeQuietly(call)
	return next, err
}

// receivePaced reads echoes until the status, recording how long after its
// send each arrived.
func receivePaced(call trevrpc.BidirectionalStreamingCall[*interoppb.EchoRequest, *interoppb.EchoResponse], schedule *pacedSchedule) error {
	for {
		response, err := call.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		arrived := int64(time.Since(schedule.base)) + 1
		sequence, ok := pacedSequence(response.Message)
		if !ok || sequence >= len(schedule.sent) {
			return fmt.Errorf("echo %.20q carries no message number", response.Message)
		}
		sent := schedule.sent[sequence].Load()
		if sent == 0 {
			return fmt.Errorf("echo of message %d, which was not sent", sequence)
		}
		if schedule.rtts[sequence] < 0 {
			schedule.rtts[sequence] = time.Duration(arrived - sent)
		}
	}
}

// pacedMessage is the request message of paced message number
// sequence: the number in decimal and a space in place of the start of
// payload.
func pacedMessage(sequence int, payload string) string {
	prefix := strconv.Itoa(sequence) + " "
	if len(payload) <= len(prefix) {
		return prefix
	}
	return prefix + payload[len(prefix):]
}

// pacedSequence returns the message number a paced echo carries.
func pacedSequence(message string) (int, bool) {
	digits, _, ok := strings.Cut(message, " ")
	if !ok {
		return 0, false
	}
	sequence, err := strconv.Atoi(digits)
	return sequence, err == nil && sequence >= 0
}

// bidiOperation keeps one Bidi stream open across operations, opening it again
// after a failure.
type bidiOperation struct {
	open    openBidi
	options []trevrpc.CallOption
	payload benchPayload
	call    trevrpc.BidirectionalStreamingCall[*interoppb.EchoRequest, *interoppb.EchoResponse]
}

func (o *bidiOperation) run(ctx context.Context) (int, error) {
	if o.call == nil {
		call, err := o.open(ctx, o.options...)
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
	message := o.payload.next()
	if err := o.call.Send(&interoppb.EchoRequest{Message: message}); err != nil {
		return err
	}
	response, err := o.call.Recv()
	if err != nil {
		return err
	}
	if len(response.Message) != len(message) {
		return fmt.Errorf("response has %d bytes, want %d", len(response.Message), len(message))
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
	lost      uint64
	errors    uint64
}

// operationLoop runs its operation back to back until end. It counts only
// the operations that start and finish within [start, end].
type operationLoop struct{ operation benchOperation }

func (l operationLoop) work(ctx context.Context, w *benchWorker, start, end time.Time) {
	operation := l.operation
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
			if lossy, ok := operation.(lossyOperation); ok {
				w.lost += uint64(lossy.lost())
			}
		}
	}
}

// wireCounter is a qlog trace that sums the sizes of the QUIC packets a client
// connection sends and receives, and counts the DATAGRAM frames it receives.
// It is the only per-connection counter quic-go exposes: Conn has no
// statistics API.
type wireCounter struct {
	sent     atomic.Uint64
	received atomic.Uint64
	// datagramFrames counts the DATAGRAM frames received.
	datagramFrames atomic.Uint64
}

func (c *wireCounter) AddProducer() qlogwriter.Recorder { return c }

func (c *wireCounter) SupportsSchemas(schema string) bool { return schema == qlog.EventSchema }

func (c *wireCounter) RecordEvent(event qlogwriter.Event) {
	switch event := event.(type) {
	case qlog.PacketSent:
		c.sent.Add(uint64(event.Raw.Length))
	case qlog.PacketReceived:
		c.received.Add(uint64(event.Raw.Length))
		for _, frame := range event.Frames {
			if _, ok := frame.Frame.(*qlog.DatagramFrame); ok {
				c.datagramFrames.Add(1)
			}
		}
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
	MetadataSize    int          `json:"metadata_size"`
	MetadataScope   string       `json:"metadata_scope"`
	Connections     int          `json:"connections"`
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
	// Lost counts the messages sent but not delivered, which only the
	// unreliable scenarios can have.
	Lost uint64 `json:"lost"`
	// Rate is the messages per second each worker sends, which only the paced
	// scenarios have.
	Rate        float64 `json:"rate,omitempty"`
	Payload     string  `json:"payload"`
	Compression string  `json:"compression"`
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
	if config.usesDictionary() {
		compression, err := interopCompression(config.dictionary)
		if err != nil {
			return err
		}
		dialOptions.Compression = compression
	}
	if config.metadataScope == "connection" {
		dialOptions.ConnectionMetadata = config.padMetadata()
	}
	channels := make([]*trevrpc.Channel, config.connections)
	clients := make([]benchClients, config.connections)
	for i := range channels {
		channel, err := config.dial(ctx, dialOptions)
		if err != nil {
			return err
		}
		defer closeQuietly(channel)
		channels[i] = channel
		clients[i] = benchClients{
			echo:       interoppb.NewEchoClient(channel),
			unreliable: interoppb.NewUnreliableEchoClient(channel),
			compressed: interoppb.NewCompressedEchoClient(channel),
		}
	}

	warmup := time.Duration(config.warmup * float64(time.Second))
	window := time.Duration(config.duration * float64(time.Second))
	start := time.Now().Add(warmup)
	end := start.Add(window)
	opCtx, cancel := context.WithDeadline(ctx, end.Add(benchGrace))
	defer cancel()

	workers := make([]benchWorker, config.concurrency)
	var group sync.WaitGroup
	for i := range workers {
		workload := benchScenarios[config.scenario](clients[i%len(clients)], config, newBenchPayload(config.payload, config.size, i))
		group.Go(func() { workload.work(opCtx, &workers[i], start, end) })
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

	result, err := summarize(config, channels[0].Transport(), window, workers)
	if err != nil {
		return err
	}
	if wire != nil && result.Msgs > 0 {
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
	var messages, lost, failures uint64
	for _, w := range workers {
		latencies = append(latencies, w.latencies...)
		messages += w.messages
		lost += w.lost
		failures += w.errors
	}
	if len(latencies) == 0 {
		return benchResult{}, fmt.Errorf("no operation completed in the %s measured window (%d failed)", window, failures)
	}
	slices.Sort(latencies)
	seconds := window.Seconds()
	ops := uint64(len(latencies))
	var rate float64
	if config.paced() {
		rate = config.rate
	}
	return benchResult{
		Scenario:      config.scenario,
		Transport:     string(transport),
		Size:          config.size,
		MetadataSize:  config.metadata,
		MetadataScope: config.metadataScope,
		Connections:   config.connections,
		Concurrency:   config.concurrency,
		DurationS:     seconds,
		Ops:           ops,
		Msgs:          messages,
		Lost:          lost,
		Rate:          rate,
		Payload:       config.payload,
		Compression:   config.compression,
		Errors:        failures,
		OpsPerSec:     float64(ops) / seconds,
		MsgsPerSec:    float64(messages) / seconds,
		BytesPerSec:   float64(messages) / seconds * float64(config.size),
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
