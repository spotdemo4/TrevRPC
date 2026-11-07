package trevrpc

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

const (
	datagramChatPath  = "/example.Greeter/DatagramChat"
	datagramWatchPath = "/example.Greeter/DatagramWatch"
	datagramBurstPath = "/example.Greeter/DatagramBurst"

	// datagramBurstSize is how many messages DatagramBurst sends.
	datagramBurstSize = 20

	// Trailing metadata of DatagramChat.
	datagramsTrailer = "datagrams"
	receivedTrailer  = "received"
)

// registerDatagramService registers DatagramChat, which echoes every request
// upper-cased and reports in its trailing metadata how many requests it
// received and how many of them arrived as datagrams, and DatagramWatch, a
// server-streaming method that sends three messages and then waits for its
// gate. Both send their responses as datagrams.
func registerDatagramService(server *Server) {
	RegisterBidirectionalStreaming(server, datagramChatPath, newTestMessage,
		func(ctx context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
			return &datagramChatStream{ctx: ctx, requests: requests}, nil
		}, RouteUnreliable(Unreliable{Responses: true}))
	// Burst sends all its messages at once from a stream that never blocks, and
	// ends the call right behind them.
	RegisterServerStreaming(server, datagramBurstPath, newTestMessage,
		func(context.Context, *testMessage) (MessageStream[*testMessage], error) {
			messages := make([]*testMessage, datagramBurstSize)
			for i := range messages {
				messages[i] = &testMessage{Value: "burst " + strconv.Itoa(i)}
			}
			return StreamOf(messages...), nil
		}, RouteUnreliable(Unreliable{Responses: true}))
	RegisterServerStreaming(server, datagramWatchPath, newTestMessage,
		func(ctx context.Context, request *testMessage) (MessageStream[*testMessage], error) {
			gate, _ := testGates.Load(request.Value)
			return &datagramWatchStream{ctx: ctx, gate: gate.(chan struct{})}, nil
		}, RouteUnreliable(Unreliable{Responses: true}))
}

type datagramChatStream struct {
	ctx      context.Context
	requests MessageStream[*testMessage]
	received int
}

func (s *datagramChatStream) Recv() (*testMessage, error) {
	request, err := s.requests.Recv()
	if errors.Is(err, io.EOF) {
		var datagrams int64
		if info, ok := HandlerContextFromContext(s.ctx); ok {
			datagrams = info.RequestDatagrams()
		}
		if err := SetResponseMetadata(s.ctx, datagramsTrailer, []byte(strconv.FormatInt(datagrams, 10))); err != nil {
			return nil, err
		}
		if err := SetResponseMetadata(s.ctx, receivedTrailer, []byte(strconv.Itoa(s.received))); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	s.received++
	return &testMessage{Value: strings.ToUpper(request.Value)}, nil
}

func (s *datagramChatStream) Close() error { return s.requests.Close() }

type datagramWatchStream struct {
	ctx  context.Context
	gate chan struct{}
	sent int
}

func (s *datagramWatchStream) Recv() (*testMessage, error) {
	if s.sent < 3 {
		s.sent++
		return &testMessage{Value: "m" + strconv.Itoa(s.sent)}, nil
	}
	select {
	case <-s.gate:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*datagramWatchStream) Close() error { return nil }

// datagramFrameCounter counts the DATAGRAM frames of the connections it
// traces: those they received and those they sent.
type datagramFrameCounter struct{ received, sent atomic.Int64 }

func (c *datagramFrameCounter) AddProducer() qlogwriter.Recorder { return c }

func (c *datagramFrameCounter) SupportsSchemas(schema string) bool { return schema == qlog.EventSchema }

func (c *datagramFrameCounter) RecordEvent(event qlogwriter.Event) {
	var frames []qlog.Frame
	count := &c.received
	switch packet := event.(type) {
	case qlog.PacketReceived:
		frames = packet.Frames
	case qlog.PacketSent:
		frames, count = packet.Frames, &c.sent
	}
	for _, frame := range frames {
		if _, ok := frame.Frame.(*qlog.DatagramFrame); ok {
			count.Add(1)
		}
	}
}

func (*datagramFrameCounter) Close() error { return nil }

func (c *datagramFrameCounter) quicConfig() *quic.Config {
	return &quic.Config{Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return c }}
}

// countingDatagramConn counts the datagrams that pass through a connection.
type countingDatagramConn struct {
	datagramConn
	sent, received atomic.Int64
}

func (c *countingDatagramConn) SendDatagram(payload []byte) error {
	c.sent.Add(1)
	return c.datagramConn.SendDatagram(payload)
}

func (c *countingDatagramConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	datagram, err := c.datagramConn.ReceiveDatagram(ctx)
	if err == nil {
		c.received.Add(1)
	}
	return datagram, err
}

var bothUnreliable = WithUnreliable(Unreliable{Requests: true, Responses: true})

// dialDatagramChannel dials the listener over native QUIC and returns the
// channel with a counter of the DATAGRAM frames it receives.
func dialDatagramChannel(t *testing.T, pki testPKI, listener *Listener) (*Channel, *datagramFrameCounter) {
	t.Helper()
	return dialDatagramChannelWith(t, pki, listener, nil)
}

// dialDatagramChannelWith is dialDatagramChannel with a channel event hook.
func dialDatagramChannelWith(t *testing.T, pki testPKI, listener *Listener, onEvent func(ChannelEvent)) (*Channel, *datagramFrameCounter) {
	t.Helper()
	counter := new(datagramFrameCounter)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client, QUICConfig: counter.quicConfig(), Transport: TransportQUICOnly, OnEvent: onEvent})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel, counter
}

// chat runs messages through DatagramChat one at a time, checks every echo, and
// returns the trailing metadata.
func chat(t *testing.T, transport Transport, messages []string, options ...CallOption) Metadata {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, datagramChatPath, newTestMessage, options...)
	if err != nil {
		t.Fatalf("DatagramChat: %v", err)
	}
	defer func() { _ = call.Close() }()
	for _, message := range messages {
		if err := call.Send(&testMessage{Value: message}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		reply, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if want := strings.ToUpper(message); reply.Value != want {
			t.Fatalf("reply = %.20q, want %.20q", reply.Value, want)
		}
	}
	if err := call.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv at the end = %v, want io.EOF", err)
	}
	status, ok := call.TerminalStatus()
	if !ok || !status.IsOK() {
		t.Fatalf("terminal status = %v, %v", status, ok)
	}
	return status.Metadata
}

func trailerCount(t *testing.T, trailers Metadata, key string) int {
	t.Helper()
	count, err := strconv.Atoi(string(trailers[key]))
	if err != nil {
		t.Fatalf("trailing metadata %s = %q", key, trailers[key])
	}
	return count
}

func numbered(count int) []string {
	messages := make([]string, count)
	for i := range messages {
		messages[i] = "message " + strconv.Itoa(i)
	}
	return messages
}

func TestNativeQUICCarriesMessagesAsDatagramsInBothDirections(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	channel, counter := dialDatagramChannel(t, pki, listener)

	trailers := chat(t, channel, numbered(20), bothUnreliable)
	if received := trailerCount(t, trailers, receivedTrailer); received < 1 || received > 20 {
		t.Fatalf("server received %d requests", received)
	}
	// Both legs are checked at the wire: the server counts the requests that
	// arrived as datagrams, and the client's connection saw DATAGRAM frames
	// carrying the responses.
	if got := trailerCount(t, trailers, datagramsTrailer); got < 1 {
		t.Fatal("the server received no request as a datagram")
	}
	if counter.received.Load() < 1 {
		t.Fatal("the client received no DATAGRAM frame, so the server sent no response as a datagram")
	}

	// The call's flow is gone from the connection with the call.
	generation, err := channel.currentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the call's flow to be unregistered", func() bool {
		return generation.(*streamGeneration).client.datagrams.liveFlows() == 0
	})
}

func TestNativeQUICLeavesMessagesThatDoNotFitOnTheStream(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	channel, _ := dialDatagramChannel(t, pki, listener)

	// No datagram holds 8 KiB, so the message and its echo use the stream.
	trailers := chat(t, channel, []string{strings.Repeat("x", 8<<10)}, bothUnreliable)
	if got := trailerCount(t, trailers, receivedTrailer); got != 1 {
		t.Fatalf("server received %d requests, want the one on the stream", got)
	}
	if got := trailerCount(t, trailers, datagramsTrailer); got != 0 {
		t.Fatalf("server counted %d datagram requests, want none", got)
	}
}

func TestCallsWithoutUnreliableOptionsUseTheStreamOnly(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	channel, _ := dialDatagramChannel(t, pki, listener)

	trailers := chat(t, channel, numbered(5))
	if got := trailerCount(t, trailers, receivedTrailer); got != 5 {
		t.Fatalf("server received %d requests, want 5", got)
	}
	if got := trailerCount(t, trailers, datagramsTrailer); got != 0 {
		t.Fatalf("server counted %d datagram requests, want none", got)
	}
}

func TestWebTransportCarriesMessagesAsDatagramsInBothDirections(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	response, session, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: testOrigin})
	if err != nil {
		t.Fatalf("WebTransport dial: %v", err)
	}
	t.Cleanup(func() {
		_ = session.CloseWithError(0, "test done")
		_ = response.Body.Close()
	})
	counting := &countingDatagramConn{datagramConn: session}
	// The first request stream is the session, so its quarter ID is one byte.
	client := newStreamClient(webTransportTestOpener{session}, DefaultMaxFrameSize)
	client.datagrams = newDatagramMux(counting, 1, false, 0)
	defer client.datagrams.close()

	trailers := chat(t, client, numbered(20), bothUnreliable)
	if got := trailerCount(t, trailers, datagramsTrailer); got < 1 {
		t.Fatal("the server received no request as a datagram")
	}
	if counting.sent.Load() < 1 || counting.received.Load() < 1 {
		t.Fatalf("the session sent %d and received %d datagrams, want both directions used", counting.sent.Load(), counting.received.Load())
	}
}

func TestQMuxNeverSetsAFlowAndKeepsMessagesOnTheStream(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	channel := newQMuxTestClient(t, pki, listener)

	trailers := chat(t, channel, numbered(5), bothUnreliable)
	if got := trailerCount(t, trailers, receivedTrailer); got != 5 {
		t.Fatalf("server received %d requests, want 5", got)
	}
	if got := trailerCount(t, trailers, datagramsTrailer); got != 0 {
		t.Fatalf("server counted %d datagram requests over QMux", got)
	}
	generation, err := channel.currentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if generation.(*streamGeneration).client.datagrams != nil {
		t.Fatal("a QMux connection has a datagram demultiplexer")
	}
	request := &wireRequest{Method: datagramChatPath, Kind: RPCKindBidirectionalStreaming, unreliable: Unreliable{Requests: true, Responses: true}}
	if flow := generation.(*streamGeneration).client.openFlow(request, nil); flow != nil || request.DatagramFlow != 0 {
		t.Fatalf("QMux opened flow %v for the request", flow)
	}
}

func TestServerStreamingCallWithOnlyDatagramResponsesGetsItsHeaders(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	channel, counter := dialDatagramChannel(t, pki, listener)

	gate := make(chan struct{})
	testGates.Store("watch", gate)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	stream, err := ServerStreaming[*testMessage, *testMessage](ctx, channel, datagramWatchPath, &testMessage{Value: "watch"}, newTestMessage, WithUnreliable(Unreliable{Responses: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()

	// The handler sends no headers and then only datagram messages, and ends
	// when the test opens the gate. The server flushed HEADERS anyway, so the
	// client does not wait for a message or the status to see them.
	headers, err := stream.Headers()
	if err != nil || len(headers) != 0 {
		t.Fatalf("Headers() = %v, %v; want empty headers before any message", headers, err)
	}
	got := map[string]bool{}
	for range 3 {
		reply, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v (got %v)", err, got)
		}
		got[reply.Value] = true
	}
	if len(got) != 3 || !got["m1"] || !got["m2"] || !got["m3"] {
		t.Fatalf("messages = %v, want m1, m2 and m3 once each", got)
	}
	if counter.received.Load() < 1 {
		t.Fatal("the client received no DATAGRAM frame")
	}
	close(gate)
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv at the end = %v, want io.EOF", err)
	}
}

func TestAServersBurstSharesADatagramAndIsNotOvertakenByTheStatus(t *testing.T) {
	pki := newTestPKI(t)
	sent := new(datagramFrameCounter)
	server := NewServer()
	registerDatagramService(server)
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server, QUICConfig: sent.quicConfig()})
	channel, received := dialDatagramChannel(t, pki, listener)

	// The server sends all its messages at once and ends the call right behind
	// them. The messages are ready together, so they share one datagram, which
	// the status cannot overtake: every message is delivered, once.
	//
	// Loopback can still lose a packet when the receiver falls behind, as it
	// does on a loaded machine, and QUIC then sends the status again but never
	// the datagram. A round whose datagram the client's connection never
	// received proves nothing and is run again; one whose datagram arrived,
	// even after the status, must deliver every message.
	const rounds, attempts = 5, 20
	complete := 0
	for attempt := 0; complete < rounds; attempt++ {
		if attempt == attempts {
			t.Fatalf("only %d of %d rounds kept their datagram in %d attempts", complete, rounds, attempts)
		}
		sentBefore, receivedBefore := sent.sent.Load(), received.received.Load()
		ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
		stream, err := ServerStreaming[*testMessage, *testMessage](ctx, channel, datagramBurstPath, &testMessage{}, newTestMessage, WithUnreliable(Unreliable{Responses: true}))
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for {
			reply, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("attempt %d: Recv: %v", attempt, err)
			}
			if !strings.HasPrefix(reply.Value, "burst ") || got[reply.Value] {
				t.Fatalf("attempt %d: unexpected or repeated message %q", attempt, reply.Value)
			}
			got[reply.Value] = true
		}
		_ = stream.Close()
		cancel()
		if frames := sent.sent.Load() - sentBefore; frames != 1 {
			t.Fatalf("attempt %d: the server sent %d DATAGRAM frames for a burst of %d messages, want one", attempt, frames, datagramBurstSize)
		}
		if len(got) == datagramBurstSize {
			complete++
			continue
		}
		// Give a datagram that overtook nothing, but arrived late, the time to
		// show up: it fails the round all the same.
		deadline := time.Now().Add(200 * time.Millisecond)
		for received.received.Load() == receivedBefore && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if received.received.Load() != receivedBefore {
			t.Fatalf("attempt %d: %d of %d messages delivered, although the client received the datagram", attempt, len(got), datagramBurstSize)
		}
	}
}

// pollOnlyConn hides the datagrams of a connection from the reader goroutine,
// which waits as if it had not got round to them, and passes polls, calls
// whose context is done already, through. A call's messages then arrive only
// through the flush at the end of its stream.
type pollOnlyConn struct{ datagramConn }

func (c pollOnlyConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return c.datagramConn.ReceiveDatagram(ctx)
	default:
	}
	<-ctx.Done()
	return nil, context.Cause(ctx)
}

// TestTheEndOfAStreamPollsAWebTransportSessionsDatagrams polls a real
// WebTransport session, whose datagrams webtransport-go receives through
// quic-go's HTTP/3 streams, which return the error of a context that is done
// before they look at their queue.
func TestTheEndOfAStreamPollsAWebTransportSessionsDatagrams(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	response, session, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: testOrigin})
	if err != nil {
		t.Fatalf("WebTransport dial: %v", err)
	}
	t.Cleanup(func() {
		_ = session.CloseWithError(0, "test done")
		_ = response.Body.Close()
	})
	client := newStreamClient(webTransportTestOpener{session}, DefaultMaxFrameSize)
	client.datagrams = newDatagramMux(pollOnlyConn{session}, 1, false, 0)
	defer client.datagrams.close()

	// A burst arrives whole or, if loopback lost its datagram, not at all: a
	// poll that missed what the session queued would deliver nothing every
	// time.
	for attempt := range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
		stream, err := ServerStreaming[*testMessage, *testMessage](ctx, client, datagramBurstPath, &testMessage{}, newTestMessage, WithUnreliable(Unreliable{Responses: true}))
		if err != nil {
			t.Fatal(err)
		}
		got := 0
		for {
			if _, err := stream.Recv(); err != nil {
				if !errors.Is(err, io.EOF) {
					t.Fatalf("attempt %d: Recv: %v", attempt, err)
				}
				break
			}
			got++
		}
		_ = stream.Close()
		cancel()
		switch got {
		case datagramBurstSize:
			return
		case 0:
			continue
		default:
			t.Fatalf("attempt %d: %d of %d messages delivered", attempt, got, datagramBurstSize)
		}
	}
	t.Fatal("no burst was delivered: the end of the stream did not poll the session's datagrams")
}

func TestAClientsReadyRequestsShareDatagrams(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	response, session, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: testOrigin})
	if err != nil {
		t.Fatalf("WebTransport dial: %v", err)
	}
	t.Cleanup(func() {
		_ = session.CloseWithError(0, "test done")
		_ = response.Body.Close()
	})
	counting := &countingDatagramConn{datagramConn: session}
	client := newStreamClient(webTransportTestOpener{session}, DefaultMaxFrameSize)
	client.datagrams = newDatagramMux(counting, 1, false, 0)
	defer client.datagrams.close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, client, datagramChatPath, newTestMessage, bothUnreliable)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = call.Close() }()
	// Send without waiting for replies: the writer takes the requests that are
	// ready together and packs them.
	const messages = 400
	replied := make(chan struct{})
	go func() {
		for i := range messages {
			if call.Send(&testMessage{Value: "request " + strconv.Itoa(i)}) != nil {
				return
			}
		}
		// quic-go sends a stream's end ahead of the datagrams still in its
		// queue when a DATAGRAM frame does not fit beside an ACK, and the
		// server drops the datagrams of a call whose requests ended. Every
		// request may then be lost: end the requests once one came back.
		select {
		case <-replied:
		case <-ctx.Done():
		}
		_ = call.CloseSend()
	}()
	for {
		if _, err := call.Recv(); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Recv: %v", err)
			}
			break
		}
		select {
		case <-replied:
		default:
			close(replied)
		}
	}
	status, ok := call.TerminalStatus()
	if !ok {
		t.Fatal("no terminal status")
	}
	if got := trailerCount(t, status.Metadata, datagramsTrailer); got < 1 {
		t.Fatal("the server received no request as a datagram")
	}
	if sent := counting.sent.Load(); sent < 1 || sent >= messages {
		t.Fatalf("the client sent %d datagrams for %d requests, want fewer than one per request", sent, messages)
	}
}

func TestDatagramMessagesCountTowardTheServersStreamLimits(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, func(options *ServerOptions) { options.MaxStreamMessages = 3 }, registerDatagramService)
	channel, _ := dialDatagramChannel(t, pki, listener)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, channel, datagramChatPath, newTestMessage, bothUnreliable)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = call.Close() }()
	go func() {
		for i := range 10 {
			if call.Send(&testMessage{Value: strconv.Itoa(i)}) != nil {
				return
			}
		}
	}()
	var lastErr error
	for lastErr == nil {
		_, lastErr = call.Recv()
	}
	if status := StatusFromError(lastErr); status.Code != CodeResourceExhausted {
		t.Fatalf("call ended with %v, want ResourceExhausted", lastErr)
	}
}

func TestEachReconnectGetsFreshFlowIDs(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	// The reconnect is end to end (backoff, then a new handshake), so wait for
	// its Ready event with the RPC timeout rather than polling briefly, and
	// keep failed attempts to explain a timeout.
	reconnected := make(chan struct{})
	var failures []error
	var failuresMu sync.Mutex
	channel, _ := dialDatagramChannelWith(t, pki, listener, func(event ChannelEvent) {
		switch {
		case event.Type == ChannelEventReady && event.Generation == 2:
			close(reconnected)
		case event.Type == ChannelEventReconnectFailed:
			failuresMu.Lock()
			failures = append(failures, event.Err)
			failuresMu.Unlock()
		}
	})

	firstMux := func() *datagramMux {
		generation, err := channel.currentGeneration()
		if err != nil {
			t.Fatal(err)
		}
		return generation.(*streamGeneration).client.datagrams
	}
	chat(t, channel, numbered(1), bothUnreliable)
	chat(t, channel, numbered(1), bothUnreliable)
	first := firstMux()
	if got := first.nextFlow.Load(); got != 2 {
		t.Fatalf("two calls allocated up to flow %d, want 2", got)
	}

	generation, _ := channel.currentGeneration()
	_ = generation.Close()
	select {
	case <-reconnected:
	case <-time.After(rpcTestTimeout):
		failuresMu.Lock()
		defer failuresMu.Unlock()
		t.Fatalf("timed out waiting for the channel to reconnect (state %v, failed attempts: %v)", channel.Snapshot(), failures)
	}
	chat(t, channel, numbered(1), bothUnreliable)
	second := firstMux()
	if second == first {
		t.Fatal("the new connection reuses the old demultiplexer")
	}
	if got := second.nextFlow.Load(); got != 1 {
		t.Fatalf("the first call on the new connection used flow %d, want 1", got)
	}
}

func TestClientFlowIsUnregisteredWhenTheCallIsCancelled(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	channel, _ := dialDatagramChannel(t, pki, listener)
	generation, err := channel.currentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	mux := generation.(*streamGeneration).client.datagrams

	ctx, cancel := context.WithCancel(context.Background())
	call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, channel, datagramChatPath, newTestMessage, bothUnreliable)
	if err != nil {
		t.Fatal(err)
	}
	if err := call.Send(&testMessage{Value: "a"}); err != nil {
		t.Fatal(err)
	}
	if got := mux.liveFlows(); got != 1 {
		t.Fatalf("live flows = %d during the call, want 1", got)
	}
	cancel()
	_ = call.Close()
	eventually(t, "the cancelled call's flow to be unregistered", func() bool { return mux.liveFlows() == 0 })
}

// rawNativeConn dials the native protocol without a client, to speak the wire
// by hand.
func rawNativeConn(t *testing.T, pki testPKI, listener *Listener) *quic.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	tlsConfig := pki.client.Clone()
	tlsConfig.NextProtos = []string{ALPN}
	conn, err := quic.DialAddr(ctx, listener.Addr().String(), tlsConfig, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
	return conn
}

func TestServerSpeaksTheDatagramWireFormatAndRejectsADuplicateLiveFlow(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	conn := rawNativeConn(t, pki, listener)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()

	const flow = 7
	request := &wireRequest{Method: datagramChatPath, Kind: RPCKindBidirectionalStreaming, DatagramFlow: flow}
	first, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeTestRequestFrame(t, first, request)
	reader := newFrameReader(first, DefaultMaxFrameSize, -1)

	// A server that sends datagrams flushes HEADERS first, empty if it has no
	// metadata: 02 00.
	next, err := reader.next()
	if err != nil || next.typ != frameTypeHeaders || len(next.payload) != 0 {
		t.Fatalf("first frame = %v %x, %v; want empty HEADERS", next.typ, next.payload, err)
	}

	// The flow is live, so a second call with it fails.
	second, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeTestRequestFrame(t, second, request)
	rejected := readTestFrame(t, second)
	if rejected.Kind != frameTypeStatus || rejected.statusValue().Code != CodeInvalidArgument {
		t.Fatalf("second call answered %v %v, want STATUS InvalidArgument", rejected.Kind, rejected.statusValue())
	}

	// A request datagram reaches the call, and the response comes back as a
	// datagram of the same flow.
	body := mustMarshal(t, &testMessage{Value: "by datagram"})
	if err := conn.SendDatagram(appendDatagram(nil, flow, [][]byte{body})); err != nil {
		t.Fatal(err)
	}
	datagram, err := conn.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("no response datagram: %v", err)
	}
	gotFlow, frames, ok := parseDatagram(datagram)
	messages, valid := datagramMessages(frames)
	if !ok || !valid || gotFlow != flow || len(messages) != 1 {
		t.Fatalf("response datagram %x is not one message on flow %d", datagram, flow)
	}
	message := messages[0]
	var reply testMessage
	if err := unmarshalMessage(message, &reply); err != nil || reply.Value != "BY DATAGRAM" {
		t.Fatalf("response datagram message = %q, %v", reply.Value, err)
	}

	// A request message on the stream is answered on the stream or as a
	// datagram, and finishing the request ends the call with its status.
	if _, err := first.Write(rawMessage(mustMarshal(t, &testMessage{Value: "by stream"})).frame); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		next, err = reader.next()
		if err != nil {
			t.Fatalf("response ended without a status: %v", err)
		}
		if next.typ == frameTypeStatus {
			status, err := decodeStatus(next.payload)
			if err != nil || !status.IsOK() || string(status.Metadata[datagramsTrailer]) != "1" {
				t.Fatalf("status = %v, %v; want OK with one datagram request counted", status, err)
			}
			break
		}
	}
}

func TestServerHoldsRequestDatagramsThatOvertakeTheirRequest(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerDatagramService)
	conn := rawNativeConn(t, pki, listener)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()

	// The datagram is sent first, as QUIC packs DATAGRAM frames ahead of stream
	// data, and the REQUEST follows.
	const flow = 3
	body := mustMarshal(t, &testMessage{Value: "early"})
	if err := conn.SendDatagram(appendDatagram(nil, flow, [][]byte{body})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeTestRequestFrame(t, stream, &wireRequest{Method: datagramChatPath, Kind: RPCKindBidirectionalStreaming, DatagramFlow: flow})
	datagram, err := conn.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("the held datagram was not delivered to its call: %v", err)
	}
	_, frames, ok := parseDatagram(datagram)
	messages, valid := datagramMessages(frames)
	if !ok || !valid || len(messages) != 1 {
		t.Fatalf("response datagram %x is malformed", datagram)
	}
	message := messages[0]
	var reply testMessage
	if err := unmarshalMessage(message, &reply); err != nil || reply.Value != "EARLY" {
		t.Fatalf("response = %q, %v; want EARLY", reply.Value, err)
	}
}

func TestServerIgnoresTheFlowOfTransportsWithoutDatagrams(t *testing.T) {
	// A stream that cannot carry datagrams, such as a QMux stream: the flow of
	// the REQUEST is ignored and the request is served as usual.
	server := NewServer()
	registerDatagramService(server)
	request := &wireRequest{Method: datagramChatPath, Kind: RPCKindBidirectionalStreaming, DatagramFlow: 9}
	_, frames := serveTestStream(t, server, finiteRequestBytes(t, request))
	if last := frames[len(frames)-1]; last.Kind != frameTypeStatus || last.statusValue().Code != CodeOK {
		t.Fatalf("frames = %v, want the call to end OK", frames)
	}
}
