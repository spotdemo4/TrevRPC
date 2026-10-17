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
	if options.EnableHTTP {
		t.Fatal("HTTP/3 RPC should be opt-in")
	}
	if options.EnableWebTransport {
		t.Fatal("WebTransport should be opt-in")
	}
	if options.HTTPPath != DefaultHTTPPath {
		t.Fatalf("default HTTP/3 path = %q", options.HTTPPath)
	}

	for _, value := range []string{httpContentType, "Application/TrevRPC"} {
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
		{httpContentType, httpContentType},
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
	options.EnableHTTP = true
	if configure != nil {
		configure(&options)
	}
	server.SetOptions(options)
	routeBytes(server, "example.Greeter", "SayHello", func(_ context.Context, body []byte) ([]byte, error) {
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

func encodeHTTP3TestRequest(t *testing.T, request *wireRequest) []byte {
	t.Helper()
	body, err := encodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return body
}

func TestHTTP3HandlerRejectsInvalidRequestsBeforeRPCHandling(t *testing.T) {
	listener, _ := newHandlerTestListener(nil)
	body := encodeHTTP3TestRequest(t, newWireRequest("example.Greeter", "SayHello", nil))

	tests := []struct {
		name         string
		method       string
		path         string
		contentTypes []string
		wantStatus   int
		wantAllow    string
	}{
		{name: "path", method: "POST", path: "/wrong", contentTypes: []string{httpContentType}, wantStatus: http.StatusNotFound},
		{name: "method", method: "GET", path: DefaultHTTPPath, contentTypes: []string{httpContentType}, wantStatus: http.StatusMethodNotAllowed, wantAllow: "POST"},
		{name: "missing media type", method: "POST", path: DefaultHTTPPath, wantStatus: http.StatusUnsupportedMediaType},
		{name: "wrong media type", method: "POST", path: DefaultHTTPPath, contentTypes: []string{"application/octet-stream"}, wantStatus: http.StatusUnsupportedMediaType},
		{name: "media type parameter", method: "POST", path: DefaultHTTPPath, contentTypes: []string{httpContentType + "; charset=utf-8"}, wantStatus: http.StatusUnsupportedMediaType},
		{name: "multiple media type values", method: "POST", path: DefaultHTTPPath, contentTypes: []string{httpContentType, httpContentType}, wantStatus: http.StatusUnsupportedMediaType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			listener.handleHTTPRPC(recorder, newHTTP3TestRequest(t, test.method, test.path, body, test.contentTypes...))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if test.wantAllow != "" && recorder.Header().Get("Allow") != test.wantAllow {
				t.Fatalf("Allow = %q, want %q", recorder.Header().Get("Allow"), test.wantAllow)
			}
			if _, err := readFrameOrEOF(recorder.Body, &wireResponse{}, DefaultMaxFrameSize); err == nil {
				t.Fatal("rejected request wrote an RPC response frame")
			}
		})
	}
}

func TestHTTP3DisabledRejectsRequests(t *testing.T) {
	listener, _ := newHandlerTestListener(func(options *ServerOptions) { options.EnableHTTP = false })
	recorder := httptest.NewRecorder()
	listener.handleHTTPRPC(recorder, newHTTP3TestRequest(t, "POST", DefaultHTTPPath, nil, httpContentType))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestHTTP3AdmissionReceivesSnapshotAndCanDeny(t *testing.T) {
	var seen AdmissionRequest
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.HTTPAdmission = func(request AdmissionRequest) bool {
			seen = request
			request.Header.Set("Content-Type", "mutated")
			return request.Header.Get("X-Allow") == "yes"
		}
	})
	body := encodeHTTP3TestRequest(t, newWireRequest("example.Greeter", "SayHello", nil))

	denied := newHTTP3TestRequest(t, "POST", DefaultHTTPPath, body, httpContentType)
	denied.Header.Set("X-Allow", "no")
	denied.Header.Set("Origin", "https://app.example.test")
	deniedRecorder := httptest.NewRecorder()
	listener.handleHTTPRPC(deniedRecorder, denied)
	if deniedRecorder.Code != http.StatusForbidden {
		t.Fatalf("denied status = %d, want %d", deniedRecorder.Code, http.StatusForbidden)
	}
	if denied.Header.Get("Content-Type") != httpContentType {
		t.Fatalf("admission callback mutated the original headers: %v", denied.Header)
	}
	if seen.Path != DefaultHTTPPath || seen.Method != "POST" || seen.Authority != "example.test" || seen.Origin != "https://app.example.test" || !seen.Secure {
		t.Fatalf("incomplete admission snapshot: %#v", seen)
	}

	allowed := newHTTP3TestRequest(t, "POST", DefaultHTTPPath, body, httpContentType)
	allowed.Header.Set("X-Allow", "yes")
	allowedRecorder := httptest.NewRecorder()
	listener.handleHTTPRPC(allowedRecorder, allowed)
	if allowedRecorder.Code != http.StatusOK {
		t.Fatalf("allowed status = %d", allowedRecorder.Code)
	}
	if got := allowedRecorder.Header().Get("Content-Type"); got != httpContentType {
		t.Fatalf("response content type = %q", got)
	}
}

func TestHTTP3AdmissionSaturationIsBounded(t *testing.T) {
	entered := make(chan struct{})
	releaseCallback := make(chan struct{})
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.MaxConcurrentAdmissionCallbacks = 1
		options.HTTPAdmission = func(AdmissionRequest) bool {
			close(entered)
			<-releaseCallback
			return true
		}
	})
	runtime := listener.runtime
	request := newHTTP3TestRequest(t, "POST", DefaultHTTPPath, nil, httpContentType)
	first := make(chan error, 1)
	go func() {
		_, err := runtime.invokeAdmission(runtime.options.HTTPAdmission, true, request)
		first <- err
	}()

	select {
	case <-entered:
	case <-time.After(http3TestTimeout):
		t.Fatal("admission callback did not start")
	}
	if admitted, err := runtime.invokeAdmission(runtime.options.HTTPAdmission, true, request); admitted || !errors.Is(err, errAdmissionSaturated) {
		t.Fatalf("saturated admission = %t, %v", admitted, err)
	}
	recorder := httptest.NewRecorder()
	listener.handleHTTPRPC(recorder, request)
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
		options.HTTPAdmission = func(AdmissionRequest) bool {
			if calls.Add(1) == 1 {
				panic("secret admission panic")
			}
			return true
		}
	})
	body := encodeHTTP3TestRequest(t, newWireRequest("example.Greeter", "SayHello", nil))

	panicRecorder := httptest.NewRecorder()
	listener.handleHTTPRPC(panicRecorder, newHTTP3TestRequest(t, "POST", DefaultHTTPPath, body, httpContentType))
	if panicRecorder.Code != http.StatusInternalServerError || bytes.Contains(panicRecorder.Body.Bytes(), []byte("secret")) {
		t.Fatalf("admission panic leaked: status=%d body=%q", panicRecorder.Code, panicRecorder.Body.String())
	}

	futureRecorder := httptest.NewRecorder()
	listener.handleHTTPRPC(futureRecorder, newHTTP3TestRequest(t, "POST", DefaultHTTPPath, body, httpContentType))
	if futureRecorder.Code != http.StatusOK {
		t.Fatalf("request after admission panic status=%d", futureRecorder.Code)
	}
}

func TestHTTP3UnaryRoundTripAndStreamSaturation(t *testing.T) {
	listener, _ := newHandlerTestListener(nil)
	body := encodeHTTP3TestRequest(t, newWireRequest("example.Greeter", "SayHello", []byte("request")))

	// A connection whose stream limit is exhausted rejects before any RPC work.
	exhausted := &httpConnState{streamLimit: newSemaphore(1)}
	exhausted.streamLimit <- struct{}{}
	rejected := newHTTP3TestRequest(t, "POST", DefaultHTTPPath, body, httpContentType)
	rejected = rejected.WithContext(context.WithValue(rejected.Context(), httpConnStateKey{}, exhausted))
	rejectedRecorder := httptest.NewRecorder()
	listener.handleHTTPRPC(rejectedRecorder, rejected)
	if rejectedRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("stream saturation status = %d, want %d", rejectedRecorder.Code, http.StatusServiceUnavailable)
	}

	recorder := httptest.NewRecorder()
	listener.handleHTTPRPC(recorder, newHTTP3TestRequest(t, "POST", DefaultHTTPPath, body, httpContentType))
	if recorder.Code != http.StatusOK {
		t.Fatalf("unary status = %d", recorder.Code)
	}
	response := &wireResponse{}
	if err := readFrame(recorder.Body, response, DefaultMaxFrameSize); err != nil {
		t.Fatalf("decode unary response: %v", err)
	}
	if Code(response.Status) != CodeOK || string(response.Body) != "reply:request" {
		t.Fatalf("unary response = %#v, want OK reply", response)
	}
}
