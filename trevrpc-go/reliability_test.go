package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reliabilityDialers returns a function per transport the test server speaks.
// Each call dials a new connection.
func reliabilityDialers(t *testing.T, pki testPKI, listener *Listener) map[string]func() Transport {
	t.Helper()
	return map[string]func() Transport{
		"quic":         func() Transport { return dialNative(t, pki, listener) },
		"qmux":         func() Transport { return newQMuxTestClient(t, pki, listener) },
		"webtransport": func() Transport { return newWebTransportTestClient(t, pki, listener, testOrigin, nil) },
		"websocket":    func() Transport { return newWebSocketTestClient(t, pki, listener, testOrigin) },
		"http3":        func() Transport { return newHTTP3TestClient(t, pki, listener) },
		"http2":        func() Transport { return newHTTP2TestClient(t, pki, listener) },
	}
}

// reliabilityTransports dials one connection per transport.
func reliabilityTransports(t *testing.T, pki testPKI, listener *Listener) map[string]Transport {
	t.Helper()
	transports := map[string]Transport{}
	for name, dial := range reliabilityDialers(t, pki, listener) {
		transports[name] = dial()
	}
	return transports
}

// TestConnectionStreamSlotIsFreeWhenClientSeesTheStatus covers a client that
// never has more calls in flight on a connection than the server allows. The
// server must release a call's stream slot before the client can see the
// call's terminal status; otherwise the client's next call races the release
// and is rejected as over the limit.
func TestConnectionStreamSlotIsFreeWhenClientSeesTheStatus(t *testing.T) {
	const limit = 4
	const callsPerWorker = 300
	const connections = 5

	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.MaxConcurrentStreamsPerConnection = limit
	})
	calls := map[string]func(context.Context, Transport) error{
		"unary": func(ctx context.Context, transport Transport) error {
			_, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage)
			return err
		},
		"server streaming": func(ctx context.Context, transport Transport) error {
			stream, err := ServerStreaming(ctx, transport, "/example.Greeter/Count", &testMessage{Value: "x"}, newTestMessage)
			if err != nil {
				return err
			}
			for _, err := range Messages(stream) {
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	dialers := reliabilityDialers(t, pki, listener)
	for _, name := range []string{"quic", "qmux"} {
		dial := dialers[name]
		for shape, call := range calls {
			t.Run(name+"/"+shape, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
				defer cancel()
				// Several connections keep the server busy, which widens the
				// window between the last write of a call and its cleanup.
				var transports []Transport
				for range connections {
					transports = append(transports, dial())
				}
				var rejected atomic.Int64
				var firstErr atomic.Value
				var workers sync.WaitGroup
				for _, transport := range transports {
					for range limit {
						workers.Go(func() {
							for range callsPerWorker {
								if err := call(ctx, transport); err != nil {
									if StatusFromError(err).Code == CodeUnavailable {
										rejected.Add(1)
									}
									firstErr.CompareAndSwap(nil, err)
								}
							}
						})
					}
				}
				workers.Wait()
				if rejected.Load() != 0 {
					t.Fatalf("%d calls were rejected as over the limit, first error: %v", rejected.Load(), firstErr.Load())
				}
				if err, _ := firstErr.Load().(error); err != nil {
					t.Fatalf("call failed: %v", err)
				}
			})
		}
	}
}

// TestOverLimitStreamStillGetsUnavailable checks that the release ordering
// keeps rejecting a stream that really is over the limit with a lone status.
func TestOverLimitStreamStillGetsUnavailable(t *testing.T) {
	const limit = 2
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.MaxConcurrentStreamsPerConnection = limit
	})
	for name, transport := range map[string]Transport{
		"quic": dialNative(t, pki, listener),
		"qmux": newQMuxTestClient(t, pki, listener),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
			defer cancel()
			var held []ClientStreamingCall[*testMessage, *testMessage]
			for range limit {
				call, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, "/example.Greeter/Concat", newTestMessage)
				if err != nil {
					t.Fatal(err)
				}
				if err := call.Send(&testMessage{Value: "x"}); err != nil {
					t.Fatal(err)
				}
				held = append(held, call)
			}
			t.Cleanup(func() {
				for _, call := range held {
					_ = call.Close()
				}
			})
			// The server has accepted both streams once their first message
			// is processed; give it a moment to start the handlers.
			deadline := time.Now().Add(time.Second)
			for {
				_, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage)
				if status := StatusFromError(err); status.Code == CodeUnavailable && strings.Contains(status.Message, "too many concurrent streams") {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("over-limit call error = %v, want Unavailable", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// uploadRejectionTimeout bounds how long a Send may take to notice that the
// server ended the call. The call deadline is far longer, so a Send that only
// returns at the deadline fails the test.
const uploadRejectionTimeout = 2 * time.Second

// registerEarlyFinishService adds handlers that end calls without consuming
// every request.
func registerEarlyFinishService(server *Server) {
	registerTestService(server)
	for _, kind := range []RPCKind{RPCKindClientStreaming, RPCKindBidirectionalStreaming} {
		server.routeStreaming(testPath(fmt.Sprintf("FailEarly%d", kind)), kind, func(context.Context, []byte, byteStream) (streamResponse, error) {
			return streamResponse{}, NewStatus(CodeFailedPrecondition, "no uploads")
		})
	}
}

// sendUntilError sends large messages until Send fails and returns that error.
// It fails the test when Send is still blocked after uploadRejectionTimeout.
func sendUntilError(t *testing.T, send func(*testMessage) error) error {
	t.Helper()
	payload := &testMessage{Value: strings.Repeat("u", 256<<10)}
	result := make(chan error, 1)
	go func() {
		for range 1000 {
			if err := send(payload); err != nil {
				result <- err
				return
			}
		}
		result <- errors.New("Send never failed")
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(uploadRejectionTimeout):
		t.Fatalf("Send still blocked %v after the server ended the call", uploadRejectionTimeout)
		return nil
	}
}

func TestSendFailsPromptlyAfterServerRejectsUpload(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, func(options *ServerOptions) {
		options.MaxStreamMessages = 2
	}, registerEarlyFinishService)

	for name, transport := range reliabilityTransports(t, pki, listener) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			t.Run("client streaming", func(t *testing.T) {
				call, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, "/example.Greeter/Concat", newTestMessage)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = call.Close() }()
				sendErr := sendUntilError(t, call.Send)
				requireStatus(t, "Send", sendErr, CodeResourceExhausted)
				// Later sends fail at once as well.
				requireStatus(t, "later Send", call.Send(&testMessage{Value: "again"}), CodeResourceExhausted)
				_, err = call.CloseAndRecv()
				requireStatus(t, "CloseAndRecv", err, CodeResourceExhausted)
			})
			t.Run("bidirectional streaming", func(t *testing.T) {
				call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/example.Greeter/Chat", newTestMessage)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = call.Close() }()
				sendErr := sendUntilError(t, call.Send)
				requireStatus(t, "Send", sendErr, CodeResourceExhausted)
				requireStatus(t, "later Send", call.Send(&testMessage{Value: "again"}), CodeResourceExhausted)
				var recvErr error
				for recvErr == nil {
					_, recvErr = call.Recv()
				}
				requireStatus(t, "Recv", recvErr, CodeResourceExhausted)
				if status, ok := call.TerminalStatus(); !ok || status.Code != CodeResourceExhausted {
					t.Fatalf("TerminalStatus() = %v, %v, want ResourceExhausted", status, ok)
				}
			})
		})
	}
}

func TestSendFailsPromptlyAfterHandlerReturnsEarly(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerEarlyFinishService)

	for name, transport := range reliabilityTransports(t, pki, listener) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			t.Run("client streaming", func(t *testing.T) {
				call, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, testPath(fmt.Sprintf("FailEarly%d", RPCKindClientStreaming)), newTestMessage)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = call.Close() }()
				requireStatus(t, "Send", sendUntilError(t, call.Send), CodeFailedPrecondition)
				_, err = call.CloseAndRecv()
				requireStatus(t, "CloseAndRecv", err, CodeFailedPrecondition)
			})
			t.Run("bidirectional streaming", func(t *testing.T) {
				call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, testPath(fmt.Sprintf("FailEarly%d", RPCKindBidirectionalStreaming)), newTestMessage)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = call.Close() }()
				requireStatus(t, "Send", sendUntilError(t, call.Send), CodeFailedPrecondition)
				_, err = call.Recv()
				requireStatus(t, "Recv", err, CodeFailedPrecondition)
			})
		})
	}
}

// TestSendReportsEOFAfterServerFinishesCallSuccessfully checks the Send
// contract for an OK terminal status that arrives mid-upload.
func TestSendReportsEOFAfterServerFinishesCallSuccessfully(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		registerTestService(server)
		server.routeStreaming("/example.Greeter/Done", RPCKindBidirectionalStreaming, func(context.Context, []byte, byteStream) (streamResponse, error) {
			return rawResponse()
		})
	})
	for name, transport := range reliabilityTransports(t, pki, listener) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/example.Greeter/Done", newTestMessage)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = call.Close() }()
			if err := sendUntilError(t, call.Send); !errors.Is(err, io.EOF) {
				t.Fatalf("Send error = %v, want io.EOF", err)
			}
			if _, err := call.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("Recv error = %v, want io.EOF", err)
			}
		})
	}
}

func requireStatus(t *testing.T, what string, err error, code Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s error = nil, want status %v", what, code)
	}
	if status := StatusFromError(err); status.Code != code {
		t.Fatalf("%s error = %v, want status %v", what, err, code)
	}
}

// slotProbeStream is a server stream that records, at every write, whether the
// resources a client may reuse once it sees the call complete are free.
type slotProbeStream struct {
	reader      *bytes.Reader
	written     bytes.Buffer
	free        func() bool
	freeAtWrite []bool
	cancelRead  atomic.Int32
}

func (s *slotProbeStream) Read(data []byte) (int, error) { return s.reader.Read(data) }
func (s *slotProbeStream) Close() error                  { return nil }
func (s *slotProbeStream) trevrpcCancelRead()            { s.cancelRead.Add(1) }

func (s *slotProbeStream) Write(data []byte) (int, error) {
	s.freeAtWrite = append(s.freeAtWrite, s.free())
	return s.written.Write(data)
}

// TestStreamSlotAndRequestLeaseAreFreeBeforeTerminalStatus checks, for every
// way a response can end, that the connection's stream slot and the server's
// request lease are released before the write that carries the terminal
// STATUS.
func TestStreamSlotAndRequestLeaseAreFreeBeforeTerminalStatus(t *testing.T) {
	slow := func(request *wireRequest) *wireRequest {
		request.TimeoutNanos = uint64(30 * time.Millisecond)
		return request
	}
	unary := func(method string) []byte {
		return testRequestBytes(t, requestFor(method, RPCKindUnary), encodedTestMessage(t, "x"))
	}
	cases := []struct {
		name    string
		options func(*ServerOptions)
		input   []byte
		code    Code
		// handlerMayOutliveStatus is set for calls whose handler can still be
		// running when the deadline status goes out. The request lease then
		// stays held until the handler returns, by design, so it bounds
		// running handlers.
		handlerMayOutliveStatus bool
	}{
		{name: "unary ok", input: unary("Echo"), code: CodeOK},
		{name: "unary handler error", input: testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "missing")), code: CodeNotFound},
		{name: "unknown method", input: unary("Nope"), code: CodeUnimplemented},
		{name: "unary deadline", input: testRequestBytes(t, slow(requestFor("Echo", RPCKindUnary)), encodedTestMessage(t, "slow")), code: CodeDeadlineExceeded, handlerMayOutliveStatus: true},
		{name: "server streaming", input: testRequestBytes(t, requestFor("Count", RPCKindServerStreaming), encodedTestMessage(t, "n")), code: CodeOK},
		{name: "client streaming ok", input: testRequestBytes(t, requestFor("Concat", RPCKindClientStreaming), encodedTestMessage(t, "a")), code: CodeOK},
		{
			name:    "client streaming over message limit",
			options: func(options *ServerOptions) { options.MaxStreamMessages = 1 },
			input: testRequestBytes(t, requestFor("Concat", RPCKindClientStreaming),
				encodedTestMessage(t, "a"), encodedTestMessage(t, "b")),
			code: CodeResourceExhausted,
		},
		{name: "streaming handler error", input: testRequestBytes(t, requestFor("FailFast", RPCKindBidirectionalStreaming)), code: CodeAborted},
		{name: "invalid first frame", input: messageBytes("not a request"), code: CodeInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newProtocolTestServer(func(options *ServerOptions) {
				options.MaxConcurrentRequests = 1
				if tc.options != nil {
					tc.options(options)
				}
			})
			runtime := server.freeze()
			slot, ok := acquireStreamSlot(newSemaphore(1))
			if !ok {
				t.Fatal("no stream slot")
			}
			stream := &slotProbeStream{
				reader: bytes.NewReader(tc.input),
				free: func() bool {
					return len(slot.limit) == 0 && (tc.handlerMayOutliveStatus || len(runtime.requestLimit) == 0)
				},
			}
			handleRPCStream(context.Background(), server, stream, slot)

			frames := readTestFrames(t, stream.written.Bytes(), DefaultMaxFrameSize)
			if last := frames[len(frames)-1]; last.Kind != frameTypeStatus || CodeFromUint32(last.Status) != tc.code {
				t.Fatalf("last frame = %+v, want STATUS %v", last, tc.code)
			}
			if len(stream.freeAtWrite) == 0 || !stream.freeAtWrite[len(stream.freeAtWrite)-1] {
				t.Fatalf("slot or request lease still held at the terminal STATUS write: %v", stream.freeAtWrite)
			}
			if len(slot.limit) != 0 {
				t.Fatal("stream slot leaked after the call")
			}
		})
	}

	t.Run("over request limit", func(t *testing.T) {
		server := newProtocolTestServer(func(options *ServerOptions) { options.MaxConcurrentRequests = 1 })
		runtime := server.freeze()
		held, ok := runtime.tryRequestLease(nil)
		if !ok {
			t.Fatal("no request lease")
		}
		defer held.release()
		slot, _ := acquireStreamSlot(newSemaphore(1))
		stream := &slotProbeStream{reader: bytes.NewReader(unary("Echo")), free: func() bool { return len(slot.limit) == 0 }}
		handleRPCStream(context.Background(), server, stream, slot)
		requireStatusFrame(t, readTestFrames(t, stream.written.Bytes(), DefaultMaxFrameSize), CodeUnavailable)
		if len(stream.freeAtWrite) != 1 || !stream.freeAtWrite[0] {
			t.Fatalf("slot still held at the rejection write: %v", stream.freeAtWrite)
		}
	})

	t.Run("cancelled call", func(t *testing.T) {
		server := newProtocolTestServer(func(options *ServerOptions) { options.MaxConcurrentRequests = 1 })
		slot, _ := acquireStreamSlot(newSemaphore(1))
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		stream := &slotProbeStream{
			reader: bytes.NewReader(testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "slow"))),
			free:   func() bool { return true },
		}
		handleRPCStream(ctx, server, stream, slot)
		if len(slot.limit) != 0 {
			t.Fatal("slot leaked after a cancelled call")
		}
	})
}

// TestStreamSlotReleaseIsIdempotent checks that releasing a slot twice frees
// the limit only once.
func TestStreamSlotReleaseIsIdempotent(t *testing.T) {
	limit := newSemaphore(2)
	first, _ := acquireStreamSlot(limit)
	second, _ := acquireStreamSlot(limit)
	first.release()
	first.release()
	if len(limit) != 1 {
		t.Fatalf("limit holds %d slots, want 1", len(limit))
	}
	second.release()
	var none *streamSlot
	none.release()
	if _, ok := acquireStreamSlot(nil); !ok {
		t.Fatal("an unlimited connection must always yield a slot")
	}
}
