package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

var errLoadUser = errors.New("load user: connection refused")

// failingStream yields one message, then err.
type failingStream struct {
	sent bool
	err  error
}

func (s *failingStream) Recv() (*testMessage, error) {
	if !s.sent {
		s.sent = true
		return &testMessage{Value: "first"}, nil
	}
	return nil, s.err
}

func (*failingStream) Close() error { return nil }

// finishedCapture records the events the server's metrics and the finish
// function of an interceptor got for each call.
type finishedCapture struct {
	metrics     recordingMetrics
	interceptor chan RPCFinished
	// consumed counts the metrics events next has matched, in order.
	consumed int
}

func newFinishedCapture(server *Server) *finishedCapture {
	capture := &finishedCapture{interceptor: make(chan RPCFinished, 8)}
	server.SetMetrics(&capture.metrics)
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
		return ctx, func(event RPCFinished) { capture.interceptor <- event }, nil
	}))
	return capture
}

// next returns the event of the call that just ended, as the interceptor and
// the metrics saw it.
func (c *finishedCapture) next(t *testing.T) RPCFinished {
	t.Helper()
	select {
	case event := <-c.interceptor:
		// The interceptors' finish functions run before the metrics hook, so
		// the metrics event of this call may not have arrived yet.
		var finished []RPCFinished
		deadline := time.Now().Add(rpcTestTimeout)
		for {
			_, finished = c.metrics.snapshot()
			if len(finished) > c.consumed || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if len(finished) <= c.consumed {
			t.Fatal("metrics did not see the call finish")
		}
		got := finished[c.consumed]
		c.consumed++
		if !errors.Is(got.Err, event.Err) || got.Code != event.Code {
			t.Fatalf("metrics saw %v with %v, the interceptor %v with %v", got.Code, got.Err, event.Code, event.Err)
		}
		return event
	case <-time.After(rpcTestTimeout):
		t.Fatal("the call never finished")
		return RPCFinished{}
	}
}

func TestRPCFinishedErrIsTheErrorTheCallEndedWithBeforeRedaction(t *testing.T) {
	server := NewServer()
	capture := newFinishedCapture(server)
	server.SetAuthorizer(AuthorizerFunc(func(ctx context.Context, info HandlerContext) (context.Context, error) {
		if info.Method == "Denied" {
			return nil, PermissionDenied("not for you")
		}
		return ctx, nil
	}))
	handlers := map[string]func() (*testMessage, error){
		"Succeeds": func() (*testMessage, error) { return &testMessage{}, nil },
		"Wrapped":  func() (*testMessage, error) { return nil, errors.Join(errors.New("context"), errLoadUser) },
		"Internal": func() (*testMessage, error) { return nil, Internal("db down") },
		"NotFound": func() (*testMessage, error) { return nil, NotFound("no such user") },
		"Panics":   func() (*testMessage, error) { panic("handler bug") },
	}
	for name, handler := range handlers {
		RegisterUnary(server, "/example.Errors/"+name, newTestMessage, func(context.Context, *testMessage) (*testMessage, error) { return handler() })
	}
	RegisterUnary(server, "/example.Errors/Denied", newTestMessage, func(context.Context, *testMessage) (*testMessage, error) {
		t.Error("the handler of a denied call ran")
		return &testMessage{}, nil
	})
	transport := localTransport{server: server}

	for _, tt := range []struct {
		method string
		code   Code
		// message is what the caller is told.
		message string
		// rejected is set for a call the server ends before its interceptors
		// run, so only the metrics see it finish.
		rejected bool
		// check inspects the Err of the event.
		check func(t *testing.T, err error)
	}{
		{method: "Succeeds", code: CodeOK, check: func(t *testing.T, err error) {
			if err != nil {
				t.Errorf("Err = %v, want nil", err)
			}
		}},
		{method: "Wrapped", code: CodeInternal, message: "internal server error", check: func(t *testing.T, err error) {
			if !errors.Is(err, errLoadUser) {
				t.Errorf("Err = %v, want the handler's error", err)
			}
		}},
		{method: "Internal", code: CodeInternal, message: "internal server error", check: func(t *testing.T, err error) {
			if status, ok := errors.AsType[*Status](err); !ok || status.Message != "db down" {
				t.Errorf("Err = %v, want the unredacted Internal status", err)
			}
		}},
		{method: "NotFound", code: CodeNotFound, message: "no such user", check: func(t *testing.T, err error) {
			if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodeNotFound {
				t.Errorf("Err = %v, want the NotFound status", err)
			}
		}},
		{method: "Panics", code: CodeInternal, message: "internal server error", check: func(t *testing.T, err error) {
			if panicErr, ok := errors.AsType[*serverPanicError](err); !ok || panicErr.recovered != "handler bug" {
				t.Errorf("Err = %v, want the handler panic", err)
			}
		}},
		{method: "Denied", code: CodePermissionDenied, message: "not for you", rejected: true, check: func(t *testing.T, err error) {
			if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodePermissionDenied {
				t.Errorf("Err = %v, want the authorizer's status", err)
			}
		}},
		{method: "Unknown", code: CodeUnimplemented, message: "unknown RPC method /example.Errors/Unknown", rejected: true, check: func(t *testing.T, err error) {
			if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodeUnimplemented {
				t.Errorf("Err = %v, want an Unimplemented status", err)
			}
		}},
	} {
		t.Run(tt.method, func(t *testing.T) {
			_, err := Unary(context.Background(), transport, "/example.Errors/"+tt.method, &testMessage{}, newTestMessage)
			if got := StatusFromError(err); got.Code != tt.code || got.Message != tt.message {
				t.Fatalf("caller got %v %q, want %v %q", got.Code, got.Message, tt.code, tt.message)
			}
			var event RPCFinished
			if tt.rejected {
				_, finished := capture.metrics.snapshot()
				event = finished[len(finished)-1]
			} else {
				event = capture.next(t)
			}
			if event.Code != tt.code {
				t.Fatalf("code = %v, want %v", event.Code, tt.code)
			}
			tt.check(t, event.Err)
		})
	}
}

func TestRPCFinishedErrOfAnInterceptorErrorAndAResponseStreamError(t *testing.T) {
	server := NewServer()
	metrics := &recordingMetrics{}
	server.SetMetrics(metrics)
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, info HandlerContext) (context.Context, func(RPCFinished), error) {
		if info.Method == "Limited" {
			return ctx, nil, ResourceExhausted("rate limited")
		}
		return ctx, nil, nil
	}))
	RegisterUnary(server, "/example.Errors/Limited", newTestMessage, func(context.Context, *testMessage) (*testMessage, error) {
		t.Error("the handler of a limited call ran")
		return &testMessage{}, nil
	})
	RegisterServerStreaming(server, "/example.Errors/Stream", newTestMessage, func(context.Context, *testMessage) (MessageStream[*testMessage], error) {
		return &failingStream{err: errLoadUser}, nil
	})
	transport := localTransport{server: server}

	if _, err := Unary(context.Background(), transport, "/example.Errors/Limited", &testMessage{}, newTestMessage); StatusFromError(err).Code != CodeResourceExhausted {
		t.Fatalf("limited call: %v", err)
	}
	stream, err := ServerStreaming(context.Background(), transport, "/example.Errors/Stream", &testMessage{}, newTestMessage)
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	for _, err := range Messages(stream) {
		streamErr = err
	}
	if got := StatusFromError(streamErr); got.Code != CodeInternal || got.Message != "internal server error" {
		t.Fatalf("stream ended with %v, want a redacted Internal", streamErr)
	}

	deadline := time.Now().Add(rpcTestTimeout)
	var finished []RPCFinished
	for len(finished) < 2 && time.Now().Before(deadline) {
		_, finished = metrics.snapshot()
		time.Sleep(time.Millisecond)
	}
	if len(finished) != 2 {
		t.Fatalf("finished %d calls, want 2", len(finished))
	}
	if status, ok := errors.AsType[*Status](finished[0].Err); !ok || status.Message != "rate limited" {
		t.Errorf("interceptor error call: Err = %v, want the interceptor's status", finished[0].Err)
	}
	if finished[1].Code != CodeInternal || !errors.Is(finished[1].Err, errLoadUser) {
		t.Errorf("response stream call: %v with Err = %v, want Internal with the stream's error", finished[1].Code, finished[1].Err)
	}
}

type diagnosticContextKey struct{}

// diagnosticCapture collects the diagnostics of a server.
type diagnosticCapture struct {
	mu     sync.Mutex
	events []ServerDiagnostic
	wake   chan struct{}
}

func newDiagnosticCapture(server *Server) *diagnosticCapture {
	capture := &diagnosticCapture{wake: make(chan struct{}, 16)}
	server.SetDiagnostics(func(event ServerDiagnostic) {
		capture.mu.Lock()
		capture.events = append(capture.events, event)
		capture.mu.Unlock()
		capture.wake <- struct{}{}
	})
	return capture
}

// waitFor returns the first diagnostic of phase.
func (c *diagnosticCapture) waitFor(t *testing.T, phase ServerDiagnosticPhase) ServerDiagnostic {
	t.Helper()
	deadline := time.After(rpcTestTimeout)
	for {
		c.mu.Lock()
		for _, event := range c.events {
			if event.Phase == phase {
				c.mu.Unlock()
				return event
			}
		}
		c.mu.Unlock()
		select {
		case <-c.wake:
		case <-deadline:
			t.Fatalf("no %s diagnostic", phase)
		}
	}
}

func TestDiagnosticsOfACallCarryTheContextOfItsInterceptors(t *testing.T) {
	server := NewServer()
	diagnostics := newDiagnosticCapture(server)
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, info HandlerContext) (context.Context, func(RPCFinished), error) {
		ctx = context.WithValue(ctx, diagnosticContextKey{}, "trace-"+info.Method)
		return ctx, func(RPCFinished) {
			if info.Method == "FinishPanics" {
				panic("finish bug")
			}
		}, nil
	}))
	for method, handler := range map[string]func() (*testMessage, error){
		"Fails":        func() (*testMessage, error) { return nil, errLoadUser },
		"Panics":       func() (*testMessage, error) { panic("handler bug") },
		"FinishPanics": func() (*testMessage, error) { return &testMessage{}, nil },
	} {
		RegisterUnary(server, "/example.Errors/"+method, newTestMessage, func(context.Context, *testMessage) (*testMessage, error) { return handler() })
	}
	RegisterServerStreaming(server, "/example.Errors/Stream", newTestMessage, func(context.Context, *testMessage) (MessageStream[*testMessage], error) {
		return &failingStream{err: errLoadUser}, nil
	})
	// The typed registration reports a nil stream as an error of the handler,
	// so only a raw route reaches the server's check.
	server.routeStreaming("/example.Errors/NilStream", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (encodedStream, error) {
		return nil, nil
	})
	transport := localTransport{server: server}
	for _, method := range []string{"Fails", "Panics", "FinishPanics"} {
		_, _ = Unary(context.Background(), transport, "/example.Errors/"+method, &testMessage{}, newTestMessage)
	}
	for _, method := range []string{"Stream", "NilStream"} {
		stream, err := ServerStreaming(context.Background(), transport, "/example.Errors/"+method, &testMessage{}, newTestMessage)
		if err != nil {
			t.Fatal(err)
		}
		for _, err := range Messages(stream) {
			_ = err
		}
	}

	seen := 0
	check := func(event ServerDiagnostic, method string) {
		t.Helper()
		seen++
		if event.Context == nil {
			t.Fatalf("%s %s diagnostic has no context", event.Phase, method)
		}
		if got, want := event.Context.Value(diagnosticContextKey{}), "trace-"+method; got != want {
			t.Fatalf("%s diagnostic context value = %v, want %v", event.Phase, got, want)
		}
	}
	waitForMethod := func(phase ServerDiagnosticPhase, method string) ServerDiagnostic {
		t.Helper()
		deadline := time.After(rpcTestTimeout)
		for {
			diagnostics.mu.Lock()
			for _, event := range diagnostics.events {
				if event.Phase == phase && event.Method == method {
					diagnostics.mu.Unlock()
					return event
				}
			}
			diagnostics.mu.Unlock()
			select {
			case <-diagnostics.wake:
			case <-deadline:
				t.Fatalf("no %s diagnostic of %s", phase, method)
			}
		}
	}
	check(waitForMethod(ServerDiagnosticInternalError, "Fails"), "Fails")
	check(waitForMethod(ServerDiagnosticHandlerPanic, "Panics"), "Panics")
	check(waitForMethod(ServerDiagnosticInterceptorPanic, "FinishPanics"), "FinishPanics")
	check(waitForMethod(ServerDiagnosticInternalError, "Stream"), "Stream")
	check(waitForMethod(ServerDiagnosticInvalidResponse, "NilStream"), "NilStream")
	if seen != 5 {
		t.Fatalf("checked %d diagnostics, want 5", seen)
	}
}

func TestDiagnosticsOfACallWithoutInterceptorsCarryTheHandlerContext(t *testing.T) {
	server := NewServer()
	diagnostics := newDiagnosticCapture(server)
	RegisterUnary(server, "/example.Errors/Fails", newTestMessage, func(context.Context, *testMessage) (*testMessage, error) { return nil, errLoadUser })
	ctx := context.WithValue(context.Background(), diagnosticContextKey{}, "served")
	_, _ = Unary(ctx, localTransport{server: server}, "/example.Errors/Fails", &testMessage{}, newTestMessage)

	event := diagnostics.waitFor(t, ServerDiagnosticInternalError)
	if event.Context == nil {
		t.Fatal("the diagnostic has no context")
	}
	if info, ok := HandlerContextFromContext(event.Context); !ok || info.Method != "Fails" {
		t.Fatalf("the diagnostic context carries %+v, want the call's handler context", info)
	}
	if !errors.Is(event.Err, errLoadUser) {
		t.Fatalf("Err = %v, want the handler's error", event.Err)
	}
}

// contextAttrHandler is a slog handler that adds the value of
// diagnosticContextKey in the context of a record to it, as a handler that
// reads trace IDs does.
type contextAttrHandler struct {
	slog.Handler
}

func (h contextAttrHandler) Handle(ctx context.Context, record slog.Record) error {
	if value, ok := ctx.Value(diagnosticContextKey{}).(string); ok {
		record.AddAttrs(slog.String("trace_id", value))
	}
	return h.Handler.Handle(ctx, record)
}

func TestLogDiagnosticsLogsWithTheContextOfTheDiagnostic(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(contextAttrHandler{slog.NewTextHandler(&out, nil)})
	handler := LogDiagnostics(logger)

	ctx := context.WithValue(context.Background(), diagnosticContextKey{}, "abc123")
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	handler(ServerDiagnostic{Phase: ServerDiagnosticInternalError, Context: ctx, Service: "example.Errors", Method: "Fails", Err: errLoadUser})
	handler(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("logged %q, want two records", out.String())
	}
	if !strings.Contains(lines[0], "trace_id=abc123") || !strings.Contains(lines[0], "trevrpc: internal_error") {
		t.Errorf("call record %q, want the trace ID of the call's context", lines[0])
	}
	if strings.Contains(lines[1], "trace_id") {
		t.Errorf("record without a call %q, want no trace ID", lines[1])
	}
}
