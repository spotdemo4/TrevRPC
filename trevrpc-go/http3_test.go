package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

const (
	httpStatusOK                  = 200
	httpStatusForbidden           = 403
	httpStatusNotFound            = 404
	httpStatusMethodNotAllowed    = 405
	httpStatusInternalServerError = 500
	httpStatusServiceUnavailable  = 503
	httpStatusUnsupportedMedia    = 415
	http3TestTimeout              = 2 * time.Second
)

func TestHTTP3DefaultsAndMediaTypeValidation(t *testing.T) {
	options := DefaultServerOptions()
	if options.EnableHTTP3 {
		t.Fatal("HTTP/3 RPC should be opt-in")
	}
	if options.EnableWebTransport {
		t.Fatal("WebTransport should be opt-in")
	}
	if options.HTTP3Path != DefaultHTTP3Path || http3Path(ServerOptions{}) != DefaultHTTP3Path {
		t.Fatalf("unexpected default HTTP/3 path: options=%q zero-value=%q", options.HTTP3Path, http3Path(ServerOptions{}))
	}
	if path := http3Path(ServerOptions{HTTP3Path: "/rpc"}); path != "/rpc" {
		t.Fatalf("configured HTTP/3 path = %q", path)
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

func TestHTTP3HandlerRejectsInvalidRequestsBeforeRPCHandling(t *testing.T) {
	server := newHTTP3TestServer(t, nil)
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", nil))

	tests := []struct {
		name        string
		method      string
		path        string
		headers     transportapi.HeaderFields
		wantStatus  int
		wantAllow   string
		wantMessage string
	}{
		{name: "path", method: "POST", path: "/wrong", headers: validHTTP3Headers(), wantStatus: httpStatusNotFound},
		{name: "method", method: "GET", path: DefaultHTTP3Path, headers: validHTTP3Headers(), wantStatus: httpStatusMethodNotAllowed, wantAllow: "POST"},
		{name: "missing media type", method: "POST", path: DefaultHTTP3Path, wantStatus: httpStatusUnsupportedMedia},
		{name: "wrong media type", method: "POST", path: DefaultHTTP3Path, headers: transportapi.HeaderFields{{Name: "Content-Type", Value: "application/octet-stream"}}, wantStatus: httpStatusUnsupportedMedia},
		{name: "media type parameter", method: "POST", path: DefaultHTTP3Path, headers: transportapi.HeaderFields{{Name: "Content-Type", Value: HTTP3ContentType + "; charset=utf-8"}}, wantStatus: httpStatusUnsupportedMedia},
		{name: "multiple media type values", method: "POST", path: DefaultHTTP3Path, headers: transportapi.HeaderFields{{Name: "Content-Type", Value: HTTP3ContentType}, {Name: "Content-Type", Value: HTTP3ContentType}}, wantStatus: httpStatusUnsupportedMedia},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newFakeHTTP3Request(body)
			request.method = test.method
			request.path = test.path
			request.headers = test.headers

			handleTransportHTTP3RPC(request, server, nil, newSemaphore(1))
			if request.errorStatus != test.wantStatus {
				t.Fatalf("error status = %d, want %d", request.errorStatus, test.wantStatus)
			}
			if request.responseStatus != 0 {
				t.Fatalf("response status = %d, want no response header", request.responseStatus)
			}
			if test.wantAllow != "" && responseHeader(request, "Allow") != test.wantAllow {
				t.Fatalf("Allow = %q, want %q", responseHeader(request, "Allow"), test.wantAllow)
			}
			if request.stream.writes.Len() != 0 {
				t.Fatal("rejected request wrote an RPC response frame")
			}
		})
	}
}

func TestHTTP3AdmissionReceivesSnapshotAndCanDeny(t *testing.T) {
	var seen HTTP3AdmissionRequest
	server := newHTTP3TestServer(t, func(server *Server) {
		options := server.Options()
		options.HTTP3Admission = func(request HTTP3AdmissionRequest) bool {
			seen = request
			request.Headers[0].Value = "mutated"
			return headerValue(request.Headers, "X-Allow") == "yes"
		}
		server.SetOptions(options)
	})
	server.Route("example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
		return []byte("hello"), nil
	})
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", nil))

	denied := newFakeHTTP3Request(body)
	denied.headers = transportapi.HeaderFields{
		{Name: "Content-Type", Value: HTTP3ContentType},
		{Name: "X-Allow", Value: "no"},
	}
	handleTransportHTTP3RPC(denied, server, nil, newSemaphore(1))
	if denied.errorStatus != httpStatusForbidden {
		t.Fatalf("denied status = %d, want %d", denied.errorStatus, httpStatusForbidden)
	}
	if denied.headers[0].Value != HTTP3ContentType {
		t.Fatalf("admission callback mutated original headers: %#v", denied.headers)
	}
	if seen.Path != DefaultHTTP3Path || seen.Method != "POST" || seen.Authority != "example.test" || !seen.Secure {
		t.Fatalf("incomplete admission snapshot: %#v", seen)
	}

	allowed := newFakeHTTP3Request(body)
	allowed.headers = transportapi.HeaderFields{
		{Name: "Content-Type", Value: HTTP3ContentType},
		{Name: "X-Allow", Value: "yes"},
	}
	handleTransportHTTP3RPC(allowed, server, nil, newSemaphore(1))
	if allowed.responseStatus != httpStatusOK || allowed.errorStatus != 0 {
		t.Fatalf("allowed request status=%d error=%d", allowed.responseStatus, allowed.errorStatus)
	}
	if responseHeader(allowed, "Content-Type") != HTTP3ContentType {
		t.Fatalf("response content type = %q", responseHeader(allowed, "Content-Type"))
	}
	if allowed.flushes != 1 {
		t.Fatalf("flush count = %d, want 1", allowed.flushes)
	}
}

func TestHTTP3AdmissionSaturationIsBounded(t *testing.T) {
	entered := make(chan struct{})
	releaseCallback := make(chan struct{})
	server := newHTTP3TestServer(t, func(server *Server) {
		options := server.Options()
		options.MaxConcurrentAdmissionCallbacks = 1
		options.HTTP3Admission = func(HTTP3AdmissionRequest) bool {
			close(entered)
			<-releaseCallback
			return true
		}
		server.SetOptions(options)
	})
	runtime := server.freeze()
	request := newFakeHTTP3Request(nil)
	first := make(chan error, 1)
	go func() {
		_, err := runtime.transportHTTP3Admitted(request)
		first <- err
	}()

	select {
	case <-entered:
	case <-time.After(http3TestTimeout):
		t.Fatal("admission callback did not start")
	}
	if admitted, err := runtime.transportHTTP3Admitted(request); admitted || !errors.Is(err, errAdmissionSaturated) {
		t.Fatalf("saturated admission = %t, %v", admitted, err)
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
	server := newHTTP3TestServer(t, func(server *Server) {
		options := server.Options()
		options.HTTP3Admission = func(HTTP3AdmissionRequest) bool {
			if calls.Add(1) == 1 {
				panic("secret admission panic")
			}
			return true
		}
		server.SetOptions(options)
	})
	server.Route("example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
		return []byte("hello"), nil
	})
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", nil))

	panicRequest := newFakeHTTP3Request(body)
	handleTransportHTTP3RPC(panicRequest, server, nil, newSemaphore(1))
	if panicRequest.errorStatus != httpStatusInternalServerError || panicRequest.errorMessage != "internal server error" {
		t.Fatalf("admission panic leaked: status=%d message=%q", panicRequest.errorStatus, panicRequest.errorMessage)
	}

	futureRequest := newFakeHTTP3Request(body)
	handleTransportHTTP3RPC(futureRequest, server, nil, newSemaphore(1))
	if futureRequest.responseStatus != httpStatusOK || futureRequest.errorStatus != 0 {
		t.Fatalf("request after admission panic status=%d error=%d", futureRequest.responseStatus, futureRequest.errorStatus)
	}
}

func TestHTTP3RequestSaturationAndUnaryRoundTrip(t *testing.T) {
	server := newHTTP3TestServer(t, nil)
	server.Route("example.Greeter", "SayHello", func(_ context.Context, body []byte) ([]byte, error) {
		return append([]byte("reply:"), body...), nil
	})
	body := encodeHTTP3TestRequest(t, NewRpcRequest("example.Greeter", "SayHello", []byte("request")))

	streamLimit := newSemaphore(1)
	streamLimit <- struct{}{}
	rejected := newFakeHTTP3Request(body)
	handleTransportHTTP3RPC(rejected, server, nil, streamLimit)
	if rejected.errorStatus != httpStatusServiceUnavailable {
		t.Fatalf("stream saturation status = %d, want %d", rejected.errorStatus, httpStatusServiceUnavailable)
	}
	if rejected.responseStatus != 0 || rejected.stream.writes.Len() != 0 {
		t.Fatal("stream-saturated request wrote an RPC response")
	}

	request := newFakeHTTP3Request(body)
	handleTransportHTTP3RPC(request, server, nil, newSemaphore(1))
	if request.responseStatus != httpStatusOK || request.errorStatus != 0 {
		t.Fatalf("unary status=%d error=%d", request.responseStatus, request.errorStatus)
	}
	if request.flushes != 1 {
		t.Fatalf("flush count = %d, want 1", request.flushes)
	}
	response := &RpcResponse{}
	if err := ReadFrame(bytes.NewReader(request.stream.writes.Bytes()), response, DefaultMaxFrameSize); err != nil {
		t.Fatalf("decode unary response: %v", err)
	}
	if Code(response.Status) != CodeOK || string(response.Body) != "reply:request" {
		t.Fatalf("unary response = %#v, want OK reply", response)
	}
}

func newHTTP3TestServer(t *testing.T, configure func(*Server)) *Server {
	t.Helper()
	server := NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
	server.SetOptions(options)
	if configure != nil {
		configure(server)
	}
	return server
}

func validHTTP3Headers() transportapi.HeaderFields {
	return transportapi.HeaderFields{{Name: "Content-Type", Value: HTTP3ContentType}}
}

func encodeHTTP3TestRequest(t *testing.T, request *RpcRequest) []byte {
	t.Helper()
	body, err := EncodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return body
}

type fakeHTTP3Request struct {
	ctx       context.Context
	method    string
	path      string
	authority string
	secure    bool
	headers   transportapi.HeaderFields
	stream    *fakeBidirectionalStream

	responseHeaders transportapi.HeaderFields
	responseStatus  int
	errorMessage    string
	errorStatus     int
	flushes         int
	flushErr        error
}

func newFakeHTTP3Request(body []byte) *fakeHTTP3Request {
	return &fakeHTTP3Request{
		ctx:       context.Background(),
		method:    "POST",
		path:      DefaultHTTP3Path,
		authority: "example.test",
		secure:    true,
		headers:   validHTTP3Headers(),
		stream:    &fakeBidirectionalStream{reader: bytes.NewReader(body)},
	}
}

func (r *fakeHTTP3Request) Context() context.Context                 { return r.ctx }
func (r *fakeHTTP3Request) Method() string                           { return r.method }
func (r *fakeHTTP3Request) Path() string                             { return r.path }
func (r *fakeHTTP3Request) Authority() string                        { return r.authority }
func (r *fakeHTTP3Request) Secure() bool                             { return r.secure }
func (r *fakeHTTP3Request) Headers() transportapi.HeaderFields       { return r.headers }
func (r *fakeHTTP3Request) Stream() transportapi.BidirectionalStream { return r.stream }

func (r *fakeHTTP3Request) SetResponseHeader(name, value string) {
	for index := range r.responseHeaders {
		if equalHeaderName(r.responseHeaders[index].Name, name) {
			r.responseHeaders[index].Value = value
			return
		}
	}
	r.responseHeaders = append(r.responseHeaders, transportapi.HeaderField{Name: name, Value: value})
}

func (r *fakeHTTP3Request) WriteResponseHeader(status int) { r.responseStatus = status }

func (r *fakeHTTP3Request) WriteError(message string, status int) {
	r.errorMessage = message
	r.errorStatus = status
}

func (r *fakeHTTP3Request) Flush() error {
	r.flushes++
	return r.flushErr
}

func responseHeader(request *fakeHTTP3Request, name string) string {
	for _, header := range request.responseHeaders {
		if equalHeaderName(header.Name, name) {
			return header.Value
		}
	}
	return ""
}

func headerValue(headers transportapi.HeaderFields, name string) string {
	for _, header := range headers {
		if equalHeaderName(header.Name, name) {
			return header.Value
		}
	}
	return ""
}

func equalHeaderName(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftByte, rightByte := left[index], right[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		if leftByte != rightByte {
			return false
		}
	}
	return true
}

type fakeBidirectionalStream struct {
	reader *bytes.Reader
	mu     sync.Mutex
	writes bytes.Buffer
	closed bool
}

func (s *fakeBidirectionalStream) Read(data []byte) (int, error) { return s.reader.Read(data) }

func (s *fakeBidirectionalStream) Write(data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	return s.writes.Write(data)
}

func (s *fakeBidirectionalStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

var _ transportapi.HTTP3Request = (*fakeHTTP3Request)(nil)
var _ transportapi.BidirectionalStream = (*fakeBidirectionalStream)(nil)
