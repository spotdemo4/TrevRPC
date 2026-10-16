package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const http3TestTimeout = 2 * time.Second

func TestHTTP3Defaults(t *testing.T) {
	options := DefaultServerOptions()
	if options.EnableHTTP3 {
		t.Fatal("HTTP/3 RPC should be opt-in")
	}
	if options.EnableWebTransport {
		t.Fatal("WebTransport should be opt-in")
	}
	if options.HTTP3Path != DefaultHTTP3Path {
		t.Fatalf("default HTTP/3 path = %q", options.HTTP3Path)
	}

	for _, value := range []string{HTTP3ContentType, "Application/TrevRPC"} {
		if !isTrevRPCMediaType([]string{value}) {
			t.Errorf("expected media type %q to be accepted", value)
		}
	}
	for _, values := range [][]string{
		nil,
		{""},
		{"application/octet-stream"},
		{"application/trevrpc; charset=utf-8"},
		{"application/trevrpc; version=1"},
		{HTTP3ContentType, HTTP3ContentType},
	} {
		if isTrevRPCMediaType(values) {
			t.Errorf("expected media type values %#v to be rejected", values)
		}
	}
}

// newHandlerTestListener returns a Listener that has no socket. It is enough to
// drive the HTTP/3 request handlers directly with httptest.
func newHandlerTestListener(configure func(*ServerOptions)) (*Listener, *Server) {
	server := NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
	if configure != nil {
		configure(&options)
	}
	server.SetOptions(options)
	server.Route("example.Greeter", "SayHello", func(_ context.Context, body []byte) ([]byte, error) {
		return append([]byte("reply:"), body...), nil
	})
	return &Listener{server: server, runtime: server.freeze()}, server
}

func newHTTP3TestRequest(t *testing.T, method, path string, body []byte, contentTypes ...string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, "https://example.test"+path, bytes.NewReader(body))
	for _, contentType := range contentTypes {
		request.Header.Add("Content-Type", contentType)
	}
	return request
}

func encodeHTTP3TestRequest(t *testing.T, request *RpcRequest) []byte {
	t.Helper()
	body, err := EncodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return body
}

func TestHTTP3HandlerRejectsInvalidRequestsBeforeRPCHandling(t *testing.T) {
	listener, _ := newHandlerTestListener(nil)
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", nil))

	tests := []struct {
		name         string
		method       string
		path         string
		contentTypes []string
		wantStatus   int
		wantAllow    string
	}{
		{name: "path", method: "POST", path: "/wrong", contentTypes: []string{HTTP3ContentType}, wantStatus: http.StatusNotFound},
		{name: "method", method: "GET", path: DefaultHTTP3Path, contentTypes: []string{HTTP3ContentType}, wantStatus: http.StatusMethodNotAllowed, wantAllow: "POST"},
		{name: "missing media type", method: "POST", path: DefaultHTTP3Path, wantStatus: http.StatusUnsupportedMediaType},
		{name: "wrong media type", method: "POST", path: DefaultHTTP3Path, contentTypes: []string{"application/octet-stream"}, wantStatus: http.StatusUnsupportedMediaType},
		{name: "media type parameter", method: "POST", path: DefaultHTTP3Path, contentTypes: []string{HTTP3ContentType + "; charset=utf-8"}, wantStatus: http.StatusUnsupportedMediaType},
		{name: "multiple media type values", method: "POST", path: DefaultHTTP3Path, contentTypes: []string{HTTP3ContentType, HTTP3ContentType}, wantStatus: http.StatusUnsupportedMediaType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			listener.handleHTTP3RPC(recorder, newHTTP3TestRequest(t, test.method, test.path, body, test.contentTypes...))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if test.wantAllow != "" && recorder.Header().Get("Allow") != test.wantAllow {
				t.Fatalf("Allow = %q, want %q", recorder.Header().Get("Allow"), test.wantAllow)
			}
			if _, err := ReadFrameOrEOF(recorder.Body, &RpcResponse{}, DefaultMaxFrameSize); err == nil {
				t.Fatal("rejected request wrote an RPC response frame")
			}
		})
	}
}

func TestHTTP3DisabledRejectsRequests(t *testing.T) {
	listener, _ := newHandlerTestListener(func(options *ServerOptions) { options.EnableHTTP3 = false })
	recorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(recorder, newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, nil, HTTP3ContentType))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestHTTP3AdmissionReceivesSnapshotAndCanDeny(t *testing.T) {
	var seen AdmissionRequest
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.HTTP3Admission = func(request AdmissionRequest) bool {
			seen = request
			request.Header.Set("Content-Type", "mutated")
			return request.Header.Get("X-Allow") == "yes"
		}
	})
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", nil))

	denied := newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, body, HTTP3ContentType)
	denied.Header.Set("X-Allow", "no")
	denied.Header.Set("Origin", "https://app.example.test")
	deniedRecorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(deniedRecorder, denied)
	if deniedRecorder.Code != http.StatusForbidden {
		t.Fatalf("denied status = %d, want %d", deniedRecorder.Code, http.StatusForbidden)
	}
	if denied.Header.Get("Content-Type") != HTTP3ContentType {
		t.Fatalf("admission callback mutated the original headers: %v", denied.Header)
	}
	if seen.Path != DefaultHTTP3Path || seen.Method != "POST" || seen.Authority != "example.test" || seen.Origin != "https://app.example.test" || !seen.Secure {
		t.Fatalf("incomplete admission snapshot: %#v", seen)
	}

	allowed := newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, body, HTTP3ContentType)
	allowed.Header.Set("X-Allow", "yes")
	allowedRecorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(allowedRecorder, allowed)
	if allowedRecorder.Code != http.StatusOK {
		t.Fatalf("allowed status = %d", allowedRecorder.Code)
	}
	if got := allowedRecorder.Header().Get("Content-Type"); got != HTTP3ContentType {
		t.Fatalf("response content type = %q", got)
	}
}

func TestHTTP3AdmissionSaturationIsBounded(t *testing.T) {
	entered := make(chan struct{})
	releaseCallback := make(chan struct{})
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.MaxConcurrentAdmissionCallbacks = 1
		options.HTTP3Admission = func(AdmissionRequest) bool {
			close(entered)
			<-releaseCallback
			return true
		}
	})
	runtime := listener.runtime
	request := newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, nil, HTTP3ContentType)
	first := make(chan error, 1)
	go func() {
		_, err := runtime.invokeAdmission(runtime.options.HTTP3Admission, true, request)
		first <- err
	}()

	select {
	case <-entered:
	case <-time.After(http3TestTimeout):
		t.Fatal("admission callback did not start")
	}
	if admitted, err := runtime.invokeAdmission(runtime.options.HTTP3Admission, true, request); admitted || !errors.Is(err, errAdmissionSaturated) {
		t.Fatalf("saturated admission = %t, %v", admitted, err)
	}
	recorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated admission status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	close(releaseCallback)
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first admission: %v", err)
		}
	case <-time.After(http3TestTimeout):
		t.Fatal("first admission did not finish")
	}
}

func TestHTTP3AdmissionPanicIsContainedAndFutureRequestsContinue(t *testing.T) {
	var calls atomic.Int64
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.HTTP3Admission = func(AdmissionRequest) bool {
			if calls.Add(1) == 1 {
				panic("secret admission panic")
			}
			return true
		}
	})
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", nil))

	panicRecorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(panicRecorder, newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, body, HTTP3ContentType))
	if panicRecorder.Code != http.StatusInternalServerError || bytes.Contains(panicRecorder.Body.Bytes(), []byte("secret")) {
		t.Fatalf("admission panic leaked: status=%d body=%q", panicRecorder.Code, panicRecorder.Body.String())
	}

	futureRecorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(futureRecorder, newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, body, HTTP3ContentType))
	if futureRecorder.Code != http.StatusOK {
		t.Fatalf("request after admission panic status=%d", futureRecorder.Code)
	}
}

func TestHTTP3UnaryRoundTripAndStreamSaturation(t *testing.T) {
	listener, _ := newHandlerTestListener(nil)
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", []byte("request")))

	// A connection whose stream limit is exhausted rejects before any RPC work.
	exhausted := &http3ConnState{streamLimit: newSemaphore(1)}
	exhausted.streamLimit <- struct{}{}
	rejected := newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, body, HTTP3ContentType)
	rejected = rejected.WithContext(context.WithValue(rejected.Context(), http3ConnStateKey{}, exhausted))
	rejectedRecorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(rejectedRecorder, rejected)
	if rejectedRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("stream saturation status = %d, want %d", rejectedRecorder.Code, http.StatusServiceUnavailable)
	}

	recorder := httptest.NewRecorder()
	listener.handleHTTP3RPC(recorder, newHTTP3TestRequest(t, "POST", DefaultHTTP3Path, body, HTTP3ContentType))
	if recorder.Code != http.StatusOK {
		t.Fatalf("unary status = %d", recorder.Code)
	}
	response := &RpcResponse{}
	if err := ReadFrame(recorder.Body, response, DefaultMaxFrameSize); err != nil {
		t.Fatalf("decode unary response: %v", err)
	}
	if Code(response.Status) != CodeOK || string(response.Body) != "reply:request" {
		t.Fatalf("unary response = %#v, want OK reply", response)
	}
}
