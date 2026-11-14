package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// eofOnCancelStream is a blocking response stream that ends cleanly the moment
// its call is cancelled, as a handler that watches ctx does.
type eofOnCancelStream struct {
	ctx     context.Context
	release chan struct{} // the first Recv returns a message when released
	n       int
}

func (s *eofOnCancelStream) Recv() (encodedMessage, error) {
	s.n++
	if s.n == 1 {
		<-s.release
		return rawMessage([]byte("m")), nil
	}
	<-s.ctx.Done()
	return encodedMessage{}, io.EOF
}

func (*eofOnCancelStream) Close() error { return nil }

// A response stream that ends cleanly because its call was cancelled is not a
// response that finished. The read-ahead writer chose at random between the
// queued io.EOF and the cancelled call, and sent the call as OK about half the
// time.
func TestCancelledCallWhoseResponseStreamEndsCleanlyIsNotFinishedAsOK(t *testing.T) {
	const rounds = 200
	for round := range rounds {
		var metrics recordingMetrics
		server := NewServer()
		options := DefaultServerOptions()
		options.StreamIdleTimeout = 0
		server.SetOptions(options)
		server.SetMetrics(&metrics)
		stream := &eofOnCancelStream{release: make(chan struct{})}
		server.routeStreaming("/example.Greeter/Download", RPCKindServerStreaming, func(ctx context.Context, _ []byte, _ byteStream) (encodedStream, error) {
			stream.ctx = ctx
			return stream, nil
		})
		request := newWireRequest("/example.Greeter/Download")
		request.Kind = RPCKindServerStreaming
		ctx, cancel := context.WithCancel(context.Background())
		response := server.handleStreamingWriter(ctx, request, nil, newEmptyStream[[]byte]())

		writer := newGatedWriter()
		first := make(chan struct{})
		go func() {
			defer close(first)
			_, _ = response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
		}()
		close(stream.release) // message 0 reaches the writer, which blocks writing it
		<-writer.entered
		cancel()                           // the call ends ...
		time.Sleep(200 * time.Microsecond) // ... and the handler's stream ends with EOF
		close(writer.gate)
		<-first
		// The next write waits for the read-ahead (blocked == true), which
		// has both the queued EOF and the cancelled context ready.
		final := &recordingWriter{}
		_, _ = response.writeNext(context.Background(), final, DefaultMaxFrameSize, nil)
		discardMessageStream(response)
		_, finished := metrics.snapshot()
		if len(finished) != 1 {
			t.Fatalf("round %d: finished = %d", round, len(finished))
		}
		if finished[0].Code != CodeCancelled {
			t.Fatalf("round %d: a cancelled call was reported as %v (%v)", round, finished[0].Code, finished[0].Err)
		}
		frames := readTestFrames(t, bytes.Join(final.writes, nil), DefaultMaxFrameSize)
		requireStatusFrame(t, frames, CodeCancelled)
	}
}

// Without a deadline a unary handler is not raced against its context, so a
// call that the client cancelled used to be reported as whatever the handler
// returned.
func TestUnaryCallCancelledWithoutDeadlineIsReportedCancelled(t *testing.T) {
	for name, handler := range map[string]func(context.Context) ([]byte, error){
		"handler returns ctx.Err()": func(ctx context.Context) ([]byte, error) { return nil, ctx.Err() },
		"handler ignores ctx":       func(context.Context) ([]byte, error) { return []byte("late"), nil },
	} {
		t.Run(name, func(t *testing.T) {
			server := NewServer()
			capture := newFinishedCapture(server)
			diagnostics := newDiagnosticCapture(server)
			started := make(chan struct{})
			routeBytes(server, "/example.Greeter/Call", func(ctx context.Context, _ []byte) ([]byte, error) {
				close(started)
				<-ctx.Done()
				return handler(ctx)
			})
			stream := &countingRPCStream{reader: bytes.NewReader(finiteRequestBytes(t, newWireRequest("/example.Greeter/Call")))}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); handleRPCStream(ctx, nil, server, stream, nil, nil) }()
			<-started
			cancel()
			<-done
			if event := capture.next(t); event.Code != CodeCancelled {
				t.Fatalf("finished %v (%v), want Cancelled", event.Code, event.Err)
			}
			// A cancelled call is not an internal error.
			diagnostics.mu.Lock()
			defer diagnostics.mu.Unlock()
			if len(diagnostics.events) != 0 {
				t.Fatalf("diagnostics = %v", diagnostics.events)
			}
		})
	}
}

// A handler that returns a panic after its call ended is still a panic.
func TestHandlerPanicIsNotHiddenByAnEndedCall(t *testing.T) {
	server := NewServer()
	diagnostics := newDiagnosticCapture(server)
	started := make(chan struct{})
	routeBytes(server, "/example.Greeter/Call", func(ctx context.Context, _ []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		panic("after the call ended")
	})
	stream := &countingRPCStream{reader: bytes.NewReader(finiteRequestBytes(t, newWireRequest("/example.Greeter/Call")))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); handleRPCStream(ctx, nil, server, stream, nil, nil) }()
	<-started
	cancel()
	<-done
	diagnostics.waitFor(t, ServerDiagnosticHandlerPanic)
}

// A call that the server's shutdown cut off is Unavailable, as one it cut off
// before it dispatched the call is, not Cancelled.
func TestCallCutOffByShutdownIsReportedUnavailable(t *testing.T) {
	for name, deadline := range map[string]time.Duration{"without a deadline": 0, "with a deadline": time.Hour} {
		for _, kind := range []RPCKind{RPCKindUnary, RPCKindServerStreaming} {
			t.Run(name+"/"+kind.String(), func(t *testing.T) {
				server := NewServer()
				capture := newFinishedCapture(server)
				started := make(chan struct{})
				if kind == RPCKindUnary {
					routeBytes(server, "/example.Greeter/Call", func(ctx context.Context, _ []byte) ([]byte, error) {
						close(started)
						<-ctx.Done()
						return nil, ctx.Err()
					})
				} else {
					server.routeStreaming("/example.Greeter/Call", RPCKindServerStreaming, func(ctx context.Context, _ []byte, _ byteStream) (encodedStream, error) {
						close(started)
						return respond(&cancelledStream{ctx: ctx})
					})
				}
				request := newWireRequest("/example.Greeter/Call")
				request.Kind = kind
				request.TimeoutNanos = uint64(deadline)
				stream := &countingRPCStream{reader: bytes.NewReader(finiteRequestBytes(t, request))}
				ctx, cancel := context.WithCancelCause(context.Background())
				done := make(chan struct{})
				go func() { defer close(done); handleRPCStream(ctx, nil, server, stream, nil, nil) }()
				<-started
				cancel(errServerShutdown)
				<-done
				event := capture.next(t)
				if event.Code != CodeUnavailable {
					t.Fatalf("finished %v (%v), want Unavailable", event.Code, event.Err)
				}
				if !strings.Contains(event.Err.Error(), "shutting down") {
					t.Fatalf("err = %v, want it to say the server is shutting down", event.Err)
				}
			})
		}
	}
}

// cancelledStream is a response stream that waits for its call to end and
// fails with the context's error.
type cancelledStream struct{ ctx context.Context }

func (s *cancelledStream) Recv() (encodedMessage, error) {
	<-s.ctx.Done()
	return encodedMessage{}, s.ctx.Err()
}

func (*cancelledStream) Close() error { return nil }

// A response that the client never got is not reported as one it did: not OK
// for a unary call, whose end was recorded before the write, and not whatever
// the handler decided.
func TestCallWhoseResponseCouldNotBeWrittenIsReportedAsAFailure(t *testing.T) {
	for name, test := range map[string]struct {
		writeErr error
		want     Code
	}{
		"the client is gone":        {writeErr: errors.New("stream reset"), want: CodeCancelled},
		"the write ran out of time": {writeErr: os.ErrDeadlineExceeded, want: CodeDeadlineExceeded},
	} {
		for _, kind := range []RPCKind{RPCKindUnary, RPCKindServerStreaming} {
			t.Run(name+"/"+kind.String(), func(t *testing.T) {
				server := NewServer()
				capture := newFinishedCapture(server)
				routeBytes(server, "/example.Greeter/Unary", func(context.Context, []byte) ([]byte, error) { return []byte("answer"), nil })
				server.routeStreaming("/example.Greeter/Stream", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (encodedStream, error) {
					return rawResponse("one", "two")
				})
				request := newWireRequest("/example.Greeter/Unary")
				if kind != RPCKindUnary {
					request = newWireRequest("/example.Greeter/Stream")
					request.Kind = kind
				}
				stream := &countingRPCStream{reader: bytes.NewReader(finiteRequestBytes(t, request)), writeErr: test.writeErr}
				handleRPCStream(context.Background(), nil, server, stream, nil, nil)
				event := capture.next(t)
				if event.Code != test.want || !errors.Is(event.Err, test.writeErr) {
					t.Fatalf("finished %v (%v), want %v (%v)", event.Code, event.Err, test.want, test.writeErr)
				}
			})
		}
	}
}

// A response that does not fit the frame limit is a bug of the server, not a
// failure of the transport: the client is told with a lone STATUS instead of a
// reset it cannot tell from a network failure, the call is reported as
// ResourceExhausted, and the operator gets a diagnostic.
func TestResponseOverTheFrameLimitEndsTheCallWithResourceExhausted(t *testing.T) {
	const limit = 128
	bigHeader := bytes.Repeat([]byte("h"), 2*limit)
	oversized := func(context.Context) ([]byte, error) { return make([]byte, 8*limit), nil }
	tests := map[string]struct {
		kind     RPCKind
		register func(*Server)
		// messages are the messages the client still gets before the STATUS.
		messages []string
	}{
		"unary message": {
			kind: RPCKindUnary,
			register: func(server *Server) {
				routeBytes(server, "/example.Greeter/Call", func(ctx context.Context, _ []byte) ([]byte, error) { return oversized(ctx) })
			},
		},
		"unary HEADERS": {
			kind: RPCKindUnary,
			register: func(server *Server) {
				routeBytes(server, "/example.Greeter/Call", func(ctx context.Context, _ []byte) ([]byte, error) {
					return []byte("small"), SetResponseHeader(ctx, "x-big", bigHeader)
				})
			},
		},
		"streaming message": {
			kind: RPCKindServerStreaming,
			register: func(server *Server) {
				server.routeStreaming("/example.Greeter/Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (encodedStream, error) {
					return respond(StreamOf(rawMessage([]byte("fits")), rawMessage(make([]byte, 8*limit))))
				})
			},
			messages: []string{"fits"},
		},
		"streaming HEADERS": {
			kind: RPCKindServerStreaming,
			register: func(server *Server) {
				server.routeStreaming("/example.Greeter/Call", RPCKindServerStreaming, func(ctx context.Context, _ []byte, _ byteStream) (encodedStream, error) {
					if err := SetResponseHeader(ctx, "x-big", bigHeader); err != nil {
						return nil, err
					}
					return rawResponse("fits")
				})
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.MaxFrameSize = limit
			server.SetOptions(options)
			capture := newFinishedCapture(server)
			diagnostics := newDiagnosticCapture(server)
			test.register(server)
			request := newWireRequest("/example.Greeter/Call")
			request.Kind = test.kind
			stream := &countingRPCStream{reader: bytes.NewReader(finiteRequestBytes(t, request))}

			handleRPCStream(context.Background(), nil, server, stream, nil, nil)

			frames := readTestFrames(t, stream.written.Bytes(), limit)
			status := frames[len(frames)-1]
			if status.Kind != frameTypeStatus || CodeFromUint32(status.Status) != CodeResourceExhausted || !strings.HasPrefix(status.Message, "response ") {
				t.Fatalf("frames = %#v, want the STATUS ResourceExhausted last", frames)
			}
			var messages []string
			for _, frame := range frames {
				if frame.Kind == frameTypeMessage {
					messages = append(messages, string(frame.Body))
				}
			}
			if strings.Join(messages, ",") != strings.Join(test.messages, ",") {
				t.Fatalf("messages = %q, want %q", messages, test.messages)
			}
			if stream.closeCount != 1 || stream.cancelReadCount != 0 {
				t.Fatalf("closes = %d, read cancellations = %d: the stream must end cleanly after its STATUS", stream.closeCount, stream.cancelReadCount)
			}
			event := capture.next(t)
			if tooLarge, ok := errors.AsType[*FrameTooLargeError](event.Err); event.Code != CodeResourceExhausted || !ok || tooLarge.Max != limit {
				t.Fatalf("finished %v (%v), want ResourceExhausted with a FrameTooLargeError", event.Code, event.Err)
			}
			diagnostic := diagnostics.waitFor(t, ServerDiagnosticInvalidResponse)
			if _, ok := errors.AsType[*FrameTooLargeError](diagnostic.Err); !ok || diagnostic.Method != "Call" {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
		})
	}
}

// When even a STATUS does not fit, there is nothing to tell the client with,
// and the write fails. The call is still a failure, and not an OK.
func TestResponseThatLeavesNoRoomForAStatusIsReportedAsAFailure(t *testing.T) {
	server := NewServer()
	var metrics recordingMetrics
	server.SetMetrics(&metrics)
	request := newWireRequest("/example.Greeter/Call")
	ctx, cancel, err := requestContext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	call := callRecord{runtime: server.freeze(), request: request, startedAt: time.Now()}
	response := call.successfulUnary(ctx, rawMessage([]byte("answer")))
	writer := &recordingWriter{}

	if _, err := response.writeNext(context.Background(), writer, 1, nil); !isFrameTooLarge(err) {
		t.Fatalf("writeNext = %v, want a FrameTooLargeError", err)
	}

	if len(writer.writes) != 0 {
		t.Fatalf("writes = %d", len(writer.writes))
	}
	if _, finished := metrics.snapshot(); len(finished) != 1 || finished[0].Code != CodeResourceExhausted {
		t.Fatalf("finished = %v, want one ResourceExhausted", finished)
	}
}

// After a Recv failed, later Recvs fail the same way: a failure must not read
// as a clean end of the request stream.
func TestRequestStreamsReplayTheirFailureInsteadOfEOF(t *testing.T) {
	t.Run("limitedByteStream", func(t *testing.T) {
		limits := streamLimits{maxMessages: 1, maxBodySize: -1}
		stream := limitByteStream(context.Background(), StreamOf([]byte("a"), []byte("b"), []byte("c")), limits, nil, nil)
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
		_, first := stream.Recv()
		_, second := stream.Recv()
		if StatusFromError(first).Code != CodeResourceExhausted || !errors.Is(second, first) {
			t.Fatalf("first failure %v, then %v", first, second)
		}
	})
	t.Run("limitedByteStream closed", func(t *testing.T) {
		stream := limitByteStream(context.Background(), StreamOf([]byte("a")), streamLimits{maxMessages: -1, maxBodySize: -1}, nil, nil)
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv after Close = %v, want io.EOF", err)
		}
	})
	t.Run("rpcRequestStream", func(t *testing.T) {
		frames, err := appendAnswerFrame(nil, &wireAnswer{}, DefaultMaxFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		stream := &rpcRequestStream{ctx: context.Background(), reader: newFrameReader(bytes.NewReader(frames), DefaultMaxFrameSize, -1)}
		_, first := stream.Recv()
		_, second := stream.Recv()
		if StatusFromError(first).Code != CodeInvalidArgument || !errors.Is(second, first) {
			t.Fatalf("first failure %v, then %v", first, second)
		}
	})
	t.Run("rpcRequestStream ended", func(t *testing.T) {
		stream := &rpcRequestStream{ctx: context.Background(), reader: newFrameReader(bytes.NewReader(nil), DefaultMaxFrameSize, -1)}
		for range 2 {
			if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("Recv = %v, want io.EOF", err)
			}
		}
		if !stream.finished() {
			t.Fatal("a request that reached its end is finished")
		}
	})
}

// endlessStream is a response stream of messages that never ends.
type endlessStream struct{ message encodedMessage }

func (s endlessStream) Recv() (encodedMessage, error) { return s.message, nil }
func (endlessStream) Close() error                    { return nil }
func (endlessStream) trevrpcNonBlockingStream() bool  { return true }

// stalledClient is a client that sends one request and then never reads the
// response, so that the server's writes fill the flow-control window and wait.
type stalledClient struct {
	conn   *quic.Conn
	stream *quic.Stream
}

func dialStalledClient(t *testing.T, listener *Listener, pki testPKI, request *wireRequest) *stalledClient {
	t.Helper()
	// A window of a few KiB, far below what the server writes.
	conn, err := quic.DialAddr(t.Context(), listener.Addr().String(), clientTLSConfig(pki.client, "localhost"), withTestHandshakeTimeout(clientQUICConfig(nil, 4<<10)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
	stream, err := conn.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(finiteRequestBytes(t, request)); err != nil {
		t.Fatal(err)
	}
	// The server dispatches a call whose request is one message once that
	// request ended.
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	return &stalledClient{conn: conn, stream: stream}
}

// stalledWriteServer serves a method that writes more than a client's
// flow-control window holds, with one request slot, and reports how its calls
// end.
func stalledWriteServer(parent context.Context, t *testing.T, kind RPCKind) (*Listener, testPKI, *recordingMetrics) {
	t.Helper()
	const frameLimit = 64 << 10
	pki := newTestPKI(t)
	metrics := &recordingMetrics{}
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxFrameSize = frameLimit
	options.MaxStreamMessages = -1
	options.MaxStreamBodySize = -1
	options.StreamIdleTimeout = 0
	options.MaxConcurrentRequests = 1
	server.SetOptions(options)
	server.SetMetrics(metrics)
	if kind == RPCKindUnary {
		routeBytes(server, "/example.Greeter/Call", func(context.Context, []byte) ([]byte, error) { return make([]byte, frameLimit-64), nil })
	} else {
		message := rawMessage(make([]byte, 16<<10))
		server.routeStreaming("/example.Greeter/Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (encodedStream, error) {
			return respond(endlessStream{message: message})
		})
	}
	listener := serveTestListenerContext(parent, t, server, ListenOptions{TLSConfig: pki.server})
	return listener, pki, metrics
}

func waitForFinished(t *testing.T, metrics *recordingMetrics, within time.Duration) RPCFinished {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, finished := metrics.snapshot(); len(finished) > 0 {
			return finished[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the call did not finish within %v", within)
	return RPCFinished{}
}

// A client that keeps its connection but stops reading holds the writes of
// the server's response on flow control. The call's deadline bounds them, so
// the call ends, and frees its request slot, instead of living until the
// connection closes.
func TestDeadlineEndsACallWhoseClientStoppedReading(t *testing.T) {
	previous := responseWriteGrace
	responseWriteGrace = 100 * time.Millisecond
	// A cleanup, unlike a defer, runs after the parallel subtests finish.
	t.Cleanup(func() { responseWriteGrace = previous })
	for _, kind := range []RPCKind{RPCKindServerStreaming, RPCKindUnary} {
		t.Run(kind.String(), func(t *testing.T) {
			// Each kind has a server of its own.
			t.Parallel()
			listener, pki, metrics := stalledWriteServer(context.Background(), t, kind)
			request := newWireRequest("/example.Greeter/Call")
			request.Kind = kind
			request.TimeoutNanos = uint64(300 * time.Millisecond)
			startedAt := time.Now()
			dialStalledClient(t, listener, pki, request)

			event := waitForFinished(t, metrics, 5*time.Second)
			if elapsed := time.Since(startedAt); elapsed > 3*time.Second {
				t.Fatalf("the call outlived its 300ms deadline by %v", elapsed)
			}
			if kind == RPCKindServerStreaming && event.Code != CodeDeadlineExceeded {
				t.Fatalf("finished %v (%v), want DeadlineExceeded", event.Code, event.Err)
			}
			if kind == RPCKindUnary && event.Code == CodeOK {
				t.Fatalf("a response the client never got finished OK")
			}
			waitFor(t, "the request slot to be released", func() bool { return len(listener.runtime.requestLimit) == 0 })
		})
	}
}

// The same client holds the call for as long as nothing ends it. The shutdown
// of the server does.
func TestShutdownEndsACallWhoseClientStoppedReading(t *testing.T) {
	parent, shutdown := context.WithCancel(context.Background())
	t.Cleanup(shutdown)
	listener, pki, metrics := stalledWriteServer(parent, t, RPCKindServerStreaming)
	request := newWireRequest("/example.Greeter/Call")
	request.Kind = RPCKindServerStreaming
	dialStalledClient(t, listener, pki, request)
	// Wait for the call to be stalled in a write: its request slot is held and
	// nothing ends it.
	waitFor(t, "the call to hold its request slot", func() bool { return len(listener.runtime.requestLimit) == 1 })
	time.Sleep(200 * time.Millisecond)
	if _, finished := metrics.snapshot(); len(finished) != 0 {
		t.Fatalf("the call finished before the shutdown: %v", finished)
	}

	startedAt := time.Now()
	shutdown()
	event := waitForFinished(t, metrics, 5*time.Second)
	if elapsed := time.Since(startedAt); elapsed > 3*time.Second {
		t.Fatalf("shutdown took %v to end the stalled call, GracefulShutdownTimeout is %v", elapsed, DefaultServerOptions().GracefulShutdownTimeout)
	}
	if event.Code != CodeUnavailable {
		t.Fatalf("finished %v (%v), want Unavailable", event.Code, event.Err)
	}
	waitFor(t, "the request slot to be released", func() bool { return len(listener.runtime.requestLimit) == 0 })
}
