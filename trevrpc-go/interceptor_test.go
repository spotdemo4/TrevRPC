package trevrpc

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type serveValueKey struct{}

type interceptorValueKey struct{}

type authorizerValueKey struct{}

// finishRecorder records the finish events of a server's interceptors.
type finishRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *finishRecorder) record(name string) func(RPCFinished) {
	return func(event RPCFinished) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, name+" "+event.Method+" "+event.Code.String())
	}
}

func (r *finishRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := r.events
	r.events = nil
	return events
}

// waitForFinishes waits until recorder holds count events and returns them.
func (r *finishRecorder) waitForFinishes(t *testing.T, count int) []string {
	t.Helper()
	deadline := time.Now().Add(rpcTestTimeout)
	for {
		r.mu.Lock()
		got := len(r.events)
		r.mu.Unlock()
		if got >= count || time.Now().After(deadline) {
			return r.take()
		}
		time.Sleep(time.Millisecond)
	}
}

// valuesInterceptor sets interceptorValueKey to name, after the values earlier
// interceptors set, and records its finish.
func valuesInterceptor(name string, recorder *finishRecorder) Interceptor {
	return InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
		value := name
		if outer, ok := ctx.Value(interceptorValueKey{}).(string); ok {
			value = outer + ">" + name
		}
		return context.WithValue(ctx, interceptorValueKey{}, value), recorder.record(name), nil
	})
}

// contextValues is what a handler of the inspect service answers: the values
// the context passed to Serve, the authorizer and the interceptors gave its
// context.
func contextValues(ctx context.Context) string {
	serve, _ := ctx.Value(serveValueKey{}).(string)
	authorized, _ := ctx.Value(authorizerValueKey{}).(string)
	intercepted, _ := ctx.Value(interceptorValueKey{}).(string)
	return serve + "|" + authorized + "|" + intercepted
}

func registerInspectService(server *Server) {
	RegisterUnary(server, "/example.Inspect/Unary", newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
		return &testMessage{Value: contextValues(ctx)}, nil
	})
	RegisterServerStreaming(server, "/example.Inspect/ServerStreaming", newTestMessage, func(ctx context.Context, _ *testMessage) (MessageStream[*testMessage], error) {
		return StreamOf(&testMessage{Value: contextValues(ctx)}), nil
	})
	RegisterClientStreaming(server, "/example.Inspect/ClientStreaming", newTestMessage, func(ctx context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
		for _, err := range Messages(requests) {
			if err != nil {
				return nil, err
			}
		}
		return &testMessage{Value: contextValues(ctx)}, nil
	})
	RegisterBidirectionalStreaming(server, "/example.Inspect/Bidirectional", newTestMessage, func(ctx context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
		if err := requests.Close(); err != nil {
			return nil, err
		}
		return StreamOf(&testMessage{Value: contextValues(ctx)}), nil
	})
}

// inspectAll calls each method of the inspect service on transport and returns
// the answers by method.
func inspectAll(ctx context.Context, t *testing.T, transport Transport) map[string]string {
	t.Helper()
	answers := map[string]string{}
	reply, err := Unary(ctx, transport, "/example.Inspect/Unary", &testMessage{}, newTestMessage)
	if err != nil {
		t.Fatalf("unary: %v", err)
	}
	answers["Unary"] = reply.Value

	stream, err := ServerStreaming(ctx, transport, "/example.Inspect/ServerStreaming", &testMessage{}, newTestMessage)
	if err != nil {
		t.Fatalf("server streaming: %v", err)
	}
	for reply, err := range Messages(stream) {
		if err != nil {
			t.Fatalf("server streaming: %v", err)
		}
		answers["ServerStreaming"] = reply.Value
	}

	upload, err := ClientStreaming[*testMessage](ctx, transport, "/example.Inspect/ClientStreaming", newTestMessage)
	if err != nil {
		t.Fatalf("client streaming: %v", err)
	}
	if err := upload.Send(&testMessage{Value: "x"}); err != nil {
		t.Fatalf("client streaming send: %v", err)
	}
	if reply, err = upload.CloseAndRecv(); err != nil {
		t.Fatalf("client streaming: %v", err)
	}
	answers["ClientStreaming"] = reply.Value

	chat, err := BidirectionalStreaming[*testMessage](ctx, transport, "/example.Inspect/Bidirectional", newTestMessage)
	if err != nil {
		t.Fatalf("bidirectional: %v", err)
	}
	defer func() { _ = chat.Close() }()
	if err := chat.CloseSend(); err != nil {
		t.Fatalf("bidirectional close send: %v", err)
	}
	if reply, err = chat.Recv(); err != nil {
		t.Fatalf("bidirectional: %v", err)
	}
	answers["Bidirectional"] = reply.Value
	if _, err := chat.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("bidirectional end: %v", err)
	}
	return answers
}

func TestInterceptorsAndServeValuesReachEveryHandlerOverEveryTransport(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &finishRecorder{}
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request AdmissionRequest) bool { return request.Origin == testOrigin }
	server.SetOptions(options)
	server.SetAuthorizer(AuthorizerFunc(func(ctx context.Context, info HandlerContext) (context.Context, error) {
		return context.WithValue(ctx, authorizerValueKey{}, "authorized "+info.Method), nil
	}))
	server.SetInterceptors(valuesInterceptor("outer", recorder), valuesInterceptor("inner", recorder))
	registerInspectService(server)
	serveCtx := context.WithValue(context.Background(), serveValueKey{}, "served")
	listener := serveTestListenerContext(serveCtx, t, server, ListenOptions{TLSConfig: pki.server})

	transports := map[string]Transport{
		"native":         dialNative(t, pki, listener),
		"http3":          newHTTP3TestClient(t, pki, listener),
		"webtransport":   newWebTransportTestClient(t, pki, listener, testOrigin, nil),
		"http2":          newHTTP2TestClient(t, pki, listener),
		"qmux":           newQMuxTestClient(t, pki, listener),
		"qmux websocket": newWebSocketTestClient(t, pki, listener, testOrigin),
	}
	methods := []string{"Unary", "ServerStreaming", "ClientStreaming", "Bidirectional"}
	for name, transport := range transports {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), rpcTestTimeout)
			defer cancel()
			answers := inspectAll(ctx, t, transport)
			for _, method := range methods {
				if got, want := answers[method], "served|authorized "+method+"|outer>inner"; got != want {
					t.Errorf("%s handler saw %q, want %q", method, got, want)
				}
			}
			var want []string
			for _, method := range methods {
				want = append(want, "inner "+method+" "+CodeOK.String(), "outer "+method+" "+CodeOK.String())
			}
			got := recorder.waitForFinishes(t, len(want))
			// Calls finish in order, each inner first; streaming calls may
			// finish after the next call started, so compare per method.
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("finishes %v, want %v", got, want)
			}
		})
	}
}

func TestInterceptorErrorEndsTheCallBeforeTheHandler(t *testing.T) {
	recorder := &finishRecorder{}
	server := NewServer()
	var handled, later atomic.Int64
	server.SetInterceptors(
		valuesInterceptor("outer", recorder),
		InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
			return ctx, recorder.record("limiter"), ResourceExhausted("rate limited")
		}),
		InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
			later.Add(1)
			return ctx, nil, nil
		}),
	)
	routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) {
		handled.Add(1)
		return nil, nil
	})
	response := server.handleRequest(context.Background(), newWireRequest("/service/method"))
	if CodeFromUint32(response.Status) != CodeResourceExhausted || response.Message != "rate limited" {
		t.Fatalf("response = %#v, want the interceptor's status", response)
	}
	if handled.Load() != 0 || later.Load() != 0 {
		t.Fatalf("handler ran %d times and the later interceptor %d times, want neither", handled.Load(), later.Load())
	}
	exhausted := CodeResourceExhausted.String()
	want := []string{"limiter method " + exhausted, "outer method " + exhausted}
	if got := recorder.take(); !slices.Equal(got, want) {
		t.Fatalf("finishes %v, want %v", got, want)
	}
}

func TestInterceptorMustDeriveTheHandlerContext(t *testing.T) {
	server := NewServer()
	server.SetInterceptors(InterceptorFunc(func(context.Context, HandlerContext) (context.Context, func(RPCFinished), error) {
		return context.Background(), nil, nil
	}))
	var handled atomic.Int64
	routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) {
		handled.Add(1)
		return nil, nil
	})
	response := server.handleRequest(context.Background(), newWireRequest("/service/method"))
	if CodeFromUint32(response.Status) != CodeInternal || handled.Load() != 0 {
		t.Fatalf("response = %#v, handled %d times; want Internal before the handler", response, handled.Load())
	}
}

func TestInterceptorPanicIsInternalWithDiagnostic(t *testing.T) {
	server := NewServer()
	diagnostics := make(chan ServerDiagnostic, 4)
	server.SetDiagnostics(func(event ServerDiagnostic) { diagnostics <- event })
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
		return ctx, func(RPCFinished) { panic("finish bug") }, nil
	}), InterceptorFunc(func(context.Context, HandlerContext) (context.Context, func(RPCFinished), error) {
		panic("intercept bug")
	}))
	routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	response := server.handleRequest(context.Background(), newWireRequest("/service/method"))
	if CodeFromUint32(response.Status) != CodeInternal || strings.Contains(response.Message, "bug") {
		t.Fatalf("response = %#v, want an Internal status that hides the panic", response)
	}
	var panics []any
	for range 2 {
		select {
		case event := <-diagnostics:
			if event.Phase != ServerDiagnosticInterceptorPanic {
				t.Fatalf("diagnostic phase %q, want %q", event.Phase, ServerDiagnosticInterceptorPanic)
			}
			panics = append(panics, event.Panic)
		case <-time.After(rpcTestTimeout):
			t.Fatal("missing interceptor panic diagnostic")
		}
	}
	if !slices.Contains(panics, any("intercept bug")) || !slices.Contains(panics, any("finish bug")) {
		t.Fatalf("panics %v, want both", panics)
	}
}

func TestInterceptorThatOutlivesTheDeadlineStillFinishesOnce(t *testing.T) {
	server := NewServer()
	finished := make(chan RPCFinished, 2)
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
		<-ctx.Done()
		return ctx, func(event RPCFinished) { finished <- event }, nil
	}))
	routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	request := newWireRequest("/service/method")
	request.TimeoutNanos = uint64(10 * time.Millisecond)
	response := server.handleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("response = %#v, want deadline exceeded", response)
	}
	select {
	case event := <-finished:
		if event.Code != CodeDeadlineExceeded {
			t.Fatalf("finished with %v, want DeadlineExceeded", event.Code)
		}
	case <-time.After(rpcTestTimeout):
		t.Fatal("the late interceptor's finish never ran")
	}
	select {
	case event := <-finished:
		t.Fatalf("finish ran twice, again with %v", event.Code)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSetInterceptorsPanicsAfterFirstUse(t *testing.T) {
	server := NewServer()
	server.freeze()
	defer func() {
		if err, _ := recover().(error); !errors.Is(err, ErrServerFrozen) {
			t.Fatalf("recovered %v, want ErrServerFrozen", err)
		}
	}()
	server.SetInterceptors()
}
