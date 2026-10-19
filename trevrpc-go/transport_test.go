package trevrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

const (
	testServiceName = "example.Greeter"
	rpcTestTimeout  = 10 * time.Second
	testOrigin      = "https://app.example.test"
)

// testPath returns the method path of method in testServiceName.
func testPath(method string) string { return "/" + testServiceName + "/" + method }

// testPKI is a self-signed localhost certificate with matching client trust.
type testPKI struct {
	server *tls.Config
	client *tls.Config
}

func newTestPKI(t testing.TB) testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return testPKI{
		server: &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		client: &tls.Config{RootCAs: roots, ServerName: "localhost"},
	}
}

func newTestMessage() *testMessage { return &testMessage{} }

// registerTestService registers one handler per RPC shape.
func registerTestService(server *Server) {
	RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(ctx context.Context, request *testMessage) (*testMessage, error) {
		switch request.Value {
		case "missing":
			return nil, NotFound("nothing here")
		case "slow":
			<-ctx.Done()
			return nil, ctx.Err()
		}
		answerTestHeaders(ctx)
		return &testMessage{Value: request.Value + string(RequestMetadataFromContext(ctx)["suffix"])}, nil
	})
	RegisterServerStreaming(server, "/example.Greeter/Count", newTestMessage, func(ctx context.Context, request *testMessage) (MessageStream[*testMessage], error) {
		replies := make([]*testMessage, 0, 3)
		for _, suffix := range []string{"1", "2", "3"} {
			replies = append(replies, &testMessage{Value: request.Value + suffix})
		}
		answerTestHeaders(ctx)
		return newSliceStream(replies...), nil
	})
	// Gated sends its headers at once and its only message once the test
	// opens the gate registered under the request value.
	RegisterServerStreaming(server, "/example.Greeter/Gated", newTestMessage, func(ctx context.Context, request *testMessage) (MessageStream[*testMessage], error) {
		gate, _ := testGates.Load(request.Value)
		answerTestHeaders(ctx)
		return &gatedStream{ctx: ctx, gate: gate.(chan struct{})}, nil
	})
	RegisterClientStreaming(server, "/example.Greeter/Concat", newTestMessage, func(ctx context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
		var joined strings.Builder
		for request, err := range Messages(requests) {
			if err != nil {
				return nil, err
			}
			joined.WriteString(request.Value)
		}
		answerTestHeaders(ctx)
		return &testMessage{Value: joined.String()}, nil
	})
	// FailFast fails a bidirectional call without reading any request message.
	server.routeStreaming("/example.Greeter/FailFast", RPCKindBidirectionalStreaming, func(context.Context, []byte, byteStream) (encodedStream, error) {
		return nil, NewStatus(CodeAborted, "abort")
	})
	RegisterBidirectionalStreaming(server, "/example.Greeter/Chat", newTestMessage, func(ctx context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
		answerTestHeaders(ctx)
		return &upperStream{requests: requests}, nil
	})
}

// testGates holds the gates of Gated calls by request value.
var testGates sync.Map

// gatedStream yields one message once its gate is open.
type gatedStream struct {
	ctx  context.Context
	gate chan struct{}
	sent bool
}

func (s *gatedStream) Recv() (*testMessage, error) {
	if s.sent {
		return nil, io.EOF
	}
	select {
	case <-s.gate:
		s.sent = true
		return &testMessage{Value: "released"}, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*gatedStream) Close() error { return nil }

const (
	testWantHeadersKey = "want-headers"
	testHeaderKey      = "x-header"
	testTrailerKey     = "x-trailer"
)

// answerTestHeaders answers the want-headers request metadata with a response
// header and trailing metadata carrying its value.
func answerTestHeaders(ctx context.Context) {
	value, ok := RequestMetadataFromContext(ctx)[testWantHeadersKey]
	if !ok {
		return
	}
	if err := SetResponseHeader(ctx, testHeaderKey, value); err != nil {
		panic(err)
	}
	if err := SetResponseMetadata(ctx, testTrailerKey, value); err != nil {
		panic(err)
	}
}

// upperStream answers every request with its upper-cased value.
type upperStream struct {
	requests MessageStream[*testMessage]
}

func (s *upperStream) Recv() (*testMessage, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}
	return &testMessage{Value: strings.ToUpper(request.Value)}, nil
}

func (s *upperStream) Close() error { return s.requests.Close() }

// startTestListener serves the test service on a loopback UDP port until the
// test ends and checks that Serve shuts down cleanly.
func startTestListener(t testing.TB, pki testPKI, configure func(*ServerOptions)) *Listener {
	t.Helper()
	return startTestListenerWith(t, pki, configure, registerTestService)
}

// startTestListenerWith is startTestListener with register adding the services
// the listener serves.
func startTestListenerWith(t testing.TB, pki testPKI, configure func(*ServerOptions), register func(*Server)) *Listener {
	t.Helper()
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request AdmissionRequest) bool { return request.Origin == testOrigin }
	if configure != nil {
		configure(&options)
	}
	server.SetOptions(options)
	register(server)

	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: pki.server})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(ctx) }()
	for !listener.serving.Load() {
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(rpcTestTimeout):
			t.Error("Serve did not stop after cancellation")
		}
	})
	return listener
}

func dialNative(t testing.TB, pki testPKI, listener *Listener) *Channel {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel
}

func TestListenerServesEveryRPCShapeOverEveryTransport(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)

	transports := map[string]Transport{
		"native":                    dialNative(t, pki, listener),
		"http3":                     newHTTP3TestClient(t, pki, listener),
		"webtransport":              newWebTransportTestClient(t, pki, listener, testOrigin, nil),
		"webtransport flow control": newWebTransportTestClient(t, pki, listener, testOrigin, flowControlledWebTransportConfig),
		"http2":                     newHTTP2TestClient(t, pki, listener),
		"qmux":                      newQMuxTestClient(t, pki, listener),
		"qmux websocket":            newWebSocketTestClient(t, pki, listener, testOrigin),
	}
	for name, transport := range transports {
		t.Run(name, func(t *testing.T) { exerciseRPCShapes(t, transport) })
	}
}

func exerciseRPCShapes(t *testing.T, transport Transport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()

	t.Run("unary", func(t *testing.T) {
		reply, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "hello"}, newTestMessage, WithMetadata("suffix", []byte("!")))
		if err != nil || reply.Value != "hello!" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
	})
	t.Run("unary error", func(t *testing.T) {
		_, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "missing"}, newTestMessage)
		if status := StatusFromError(err); status.Code != CodeNotFound || status.Message != "nothing here" {
			t.Fatalf("status = %v", status)
		}
	})
	t.Run("unary large", func(t *testing.T) {
		large := strings.Repeat("x", 1<<20)
		reply, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: large}, newTestMessage)
		if err != nil || reply.Value != large {
			t.Fatalf("large echo failed: err=%v", err)
		}
	})
	t.Run("unknown method", func(t *testing.T) {
		_, err := Unary(ctx, transport, "/example.Greeter/Nope", &testMessage{}, newTestMessage)
		if status := StatusFromError(err); status.Code != CodeUnimplemented {
			t.Fatalf("status = %v", status)
		}
	})
	t.Run("response headers", func(t *testing.T) {
		exerciseResponseHeaders(ctx, t, transport)
	})
	t.Run("unary deadline", func(t *testing.T) {
		_, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "slow"}, newTestMessage, WithTimeout(100*time.Millisecond))
		if status := StatusFromError(err); status.Code != CodeDeadlineExceeded {
			t.Fatalf("status = %v", status)
		}
	})
	t.Run("server streaming", func(t *testing.T) {
		stream, err := ServerStreaming(ctx, transport, "/example.Greeter/Count", &testMessage{Value: "n"}, newTestMessage)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for reply, err := range Messages(stream) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, reply.Value)
		}
		if strings.Join(got, ",") != "n1,n2,n3" {
			t.Fatalf("replies = %v", got)
		}
	})
	t.Run("client streaming", func(t *testing.T) {
		reply, err := runTestClientStreaming(ctx, transport, "/example.Greeter/Concat", []string{"a", "b", "c"})
		if err != nil || reply.Value != "abc" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
	})
	t.Run("bidirectional streaming", func(t *testing.T) {
		call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/example.Greeter/Chat", newTestMessage)
		if err != nil {
			t.Fatal(err)
		}
		closeOnCleanup(t, call)
		for _, value := range []string{"one", "two", "three"} {
			if err := call.Send(&testMessage{Value: value}); err != nil {
				t.Fatal(err)
			}
			reply, err := call.Recv()
			if err != nil || reply.Value != strings.ToUpper(value) {
				t.Fatalf("reply = %v, err = %v", reply, err)
			}
		}
		if err := call.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err := call.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after CloseSend, got %v", err)
		}
	})
}

// requireHeaders fails the test unless metadata holds exactly {key: value}.
func requireHeaders(t *testing.T, what string, metadata Metadata, key, value string) {
	t.Helper()
	if len(metadata) != 1 || string(metadata[key]) != value {
		t.Fatalf("%s = %v, want {%s: %q}", what, metadata, key, value)
	}
}

// exerciseResponseHeaders covers response headers and trailing metadata of
// every RPC shape. A call that asks for none sees none.
func exerciseResponseHeaders(ctx context.Context, t *testing.T, transport Transport) {
	t.Helper()
	want := WithMetadata(testWantHeadersKey, []byte("v"))

	t.Run("unary", func(t *testing.T) {
		var headers, trailers Metadata
		reply, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "hello"}, newTestMessage, want, CaptureResponseHeaders(&headers), CaptureResponseMetadata(&trailers))
		if err != nil || reply.Value != "hello" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
		requireHeaders(t, "headers", headers, testHeaderKey, "v")
		requireHeaders(t, "trailing metadata", trailers, testTrailerKey, "v")

		headers, trailers = Metadata{"stale": nil}, Metadata{"stale": nil}
		if _, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "hello"}, newTestMessage, CaptureResponseHeaders(&headers), CaptureResponseMetadata(&trailers)); err != nil {
			t.Fatal(err)
		}
		if len(headers) != 0 || len(trailers) != 0 {
			t.Fatalf("headers = %v, trailers = %v, want none", headers, trailers)
		}
	})
	t.Run("client streaming", func(t *testing.T) {
		var headers, trailers Metadata
		reply, err := runTestClientStreaming(ctx, transport, "/example.Greeter/Concat", []string{"a", "b"}, want, CaptureResponseHeaders(&headers), CaptureResponseMetadata(&trailers))
		if err != nil || reply.Value != "ab" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
		requireHeaders(t, "headers", headers, testHeaderKey, "v")
		requireHeaders(t, "trailing metadata", trailers, testTrailerKey, "v")
	})
	t.Run("server streaming", func(t *testing.T) {
		stream, err := ServerStreaming(ctx, transport, "/example.Greeter/Count", &testMessage{Value: "n"}, newTestMessage, want)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		headers, err := stream.Headers()
		if err != nil {
			t.Fatalf("Headers() error = %v", err)
		}
		requireHeaders(t, "headers", headers, testHeaderKey, "v")
		// Reading the headers consumed no message.
		for _, value := range []string{"n1", "n2", "n3"} {
			if reply, err := stream.Recv(); err != nil || reply.Value != value {
				t.Fatalf("reply = %v, err = %v, want %s", reply, err, value)
			}
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
		status, ok := stream.TerminalStatus()
		if !ok {
			t.Fatal("no terminal status")
		}
		requireHeaders(t, "trailing metadata", status.Metadata, testTrailerKey, "v")
		if headers, err := stream.Headers(); err != nil || string(headers[testHeaderKey]) != "v" {
			t.Fatalf("Headers() after the end = %v, %v", headers, err)
		}
	})
	t.Run("server streaming without headers", func(t *testing.T) {
		stream, err := ServerStreaming(ctx, transport, "/example.Greeter/Count", &testMessage{Value: "n"}, newTestMessage)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		if headers, err := stream.Headers(); err != nil || len(headers) != 0 {
			t.Fatalf("Headers() = %v, %v, want none", headers, err)
		}
		if reply, err := stream.Recv(); err != nil || reply.Value != "n1" {
			t.Fatalf("first reply = %v, err = %v: Headers() consumed the message", reply, err)
		}
	})
	t.Run("server streaming headers precede the first message", func(t *testing.T) {
		gate := make(chan struct{})
		testGates.Store("gated", gate)
		defer testGates.Delete("gated")
		stream, err := ServerStreaming(ctx, transport, "/example.Greeter/Gated", &testMessage{Value: "gated"}, newTestMessage, want)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		// The server cannot send a message before the gate opens, so the
		// headers must arrive without one.
		headers, err := stream.Headers()
		if err != nil {
			t.Fatalf("Headers() error = %v", err)
		}
		requireHeaders(t, "headers", headers, testHeaderKey, "v")
		close(gate)
		if reply, err := stream.Recv(); err != nil || reply.Value != "released" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
	})
	t.Run("bidirectional streaming", func(t *testing.T) {
		call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/example.Greeter/Chat", newTestMessage, want)
		if err != nil {
			t.Fatal(err)
		}
		closeOnCleanup(t, call)
		// Nothing was sent yet: the server sends the headers on its own.
		headers, err := call.Headers()
		if err != nil {
			t.Fatalf("Headers() error = %v", err)
		}
		requireHeaders(t, "headers", headers, testHeaderKey, "v")
		if err := call.Send(&testMessage{Value: "hi"}); err != nil {
			t.Fatal(err)
		}
		if reply, err := call.Recv(); err != nil || reply.Value != "HI" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
		if err := call.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err := call.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
		status, ok := call.TerminalStatus()
		if !ok {
			t.Fatal("no terminal status")
		}
		requireHeaders(t, "trailing metadata", status.Metadata, testTrailerKey, "v")
	})
}

func TestDialRejectsNonNativeTargets(t *testing.T) {
	for _, target := range []string{"https://localhost:4433/trevrpc", "localhost", "", ":4433"} {
		if _, err := Dial(context.Background(), target, DialOptions{}); StatusFromError(err).Code != CodeInvalidArgument {
			t.Errorf("Dial(%q) error = %v, want InvalidArgument", target, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Dial(ctx, "127.0.0.1:1", DialOptions{}); StatusFromError(err).Code != CodeCancelled {
		t.Errorf("Dial with a cancelled context = %v, want Cancelled", err)
	}
}

func TestListenValidatesArguments(t *testing.T) {
	if _, err := Listen("127.0.0.1:0", nil, ListenOptions{}); StatusFromError(err).Code != CodeInvalidArgument {
		t.Errorf("nil server error = %v", err)
	}
	if _, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{}); StatusFromError(err).Code != CodeInvalidArgument {
		t.Errorf("missing certificate error = %v", err)
	}
	if _, err := Listen("not an address", NewServer(), ListenOptions{TLSConfig: newTestPKI(t).server}); err == nil {
		t.Error("invalid address was accepted")
	}
}

func TestListenerCloseWithoutServeReleasesSocket(t *testing.T) {
	pki := newTestPKI(t)
	listener, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{TLSConfig: pki.server})
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	var listenConfig net.ListenConfig
	rebound, err := listenConfig.ListenPacket(t.Context(), "udp", addr)
	if err != nil {
		t.Fatalf("socket was not released: %v", err)
	}
	_ = rebound.Close()
	if err := listener.Serve(context.Background()); err != nil {
		t.Fatalf("Serve after Close = %v, want nil", err)
	}
}

func TestListenerServeTwiceFails(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	if err := listener.Serve(context.Background()); StatusFromError(err).Code != CodeFailedPrecondition {
		t.Fatalf("second Serve = %v, want FailedPrecondition", err)
	}
}

func TestListenerCloseStopsServeAndDrainsConnections(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	registerTestService(server)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: pki.server})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(context.Background()) }()
	channel := dialNative(t, pki, listener)
	if _, err := Unary(context.Background(), channel, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatal(err)
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve = %v, want nil after Close", err)
		}
	case <-time.After(rpcTestTimeout):
		t.Fatal("Serve did not return after Close")
	}
	deadline := time.Now().Add(rpcTestTimeout)
	for channel.State() == ChannelStateReady {
		if time.Now().After(deadline) {
			t.Fatal("client connection was not closed by the server")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestListenerShutdownClosesEveryTransportPromptly(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(AdmissionRequest) bool { return true }
	server.SetOptions(options)
	registerTestService(server)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: pki.server})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(ctx) }()

	for name, transport := range map[string]Transport{
		"native":         dialNative(t, pki, listener),
		"http3":          newHTTP3TestClient(t, pki, listener),
		"webtransport":   newWebTransportTestClient(t, pki, listener, "", nil),
		"http2":          newHTTP2TestClient(t, pki, listener),
		"qmux":           newQMuxTestClient(t, pki, listener),
		"qmux websocket": newWebSocketTestClient(t, pki, listener, ""),
	} {
		if _, err := Unary(context.Background(), transport, "/example.Greeter/Echo", &testMessage{Value: name}, newTestMessage); err != nil {
			t.Fatalf("%s call: %v", name, err)
		}
	}

	started := time.Now()
	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(rpcTestTimeout):
		t.Fatal("Serve did not stop after cancellation")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("shutdown took %v", elapsed)
	}
}

func TestShutdownCancelsInFlightHandlers(t *testing.T) {
	pki := newTestPKI(t)
	handlerCancelled := make(chan struct{})
	handlerStarted := make(chan struct{})
	server := NewServer()
	routeBytes(server, "/example.Greeter/Block", func(ctx context.Context, _ []byte) ([]byte, error) {
		close(handlerStarted)
		<-ctx.Done()
		close(handlerCancelled)
		return nil, ctx.Err()
	})
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: pki.server})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(ctx) }()
	channel := dialNative(t, pki, listener)

	callDone := make(chan error, 1)
	go func() {
		_, err := Unary(context.Background(), channel, "/example.Greeter/Block", &testMessage{}, newTestMessage)
		callDone <- err
	}()
	<-handlerStarted
	cancel()
	select {
	case <-handlerCancelled:
	case <-time.After(rpcTestTimeout):
		t.Fatal("shutdown did not cancel the in-flight handler")
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if err := <-callDone; err == nil {
		t.Fatal("in-flight call succeeded across shutdown")
	}
}

func TestConnectionLimitClosesExcessConnections(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) { options.MaxConcurrentConnections = 1 })
	first := dialNative(t, pki, listener)
	if _, err := Unary(context.Background(), first, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatal(err)
	}

	second := dialNative(t, pki, listener)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	if _, err := Unary(ctx, second, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err == nil {
		t.Fatal("call on a connection over the limit succeeded")
	}
}

func TestChannelReconnectsToRestartedServer(t *testing.T) {
	pki := newTestPKI(t)
	first := startTestListener(t, pki, nil)
	addr := first.Addr().String()

	events := make(chan ChannelEvent, 16)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, addr, DialOptions{TLSConfig: pki.client, OnEvent: func(event ChannelEvent) { events <- event }})
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, channel)
	if _, err := Unary(ctx, channel, "/example.Greeter/Echo", &testMessage{Value: "before"}, newTestMessage); err != nil {
		t.Fatal(err)
	}

	// Stop the first server; the channel notices the lost connection.
	_ = first.Close()
	waitForChannelEvent(t, events, ChannelEventDisconnected)

	server := NewServer()
	registerTestService(server)
	var restarted *Listener
	deadline := time.Now().Add(rpcTestTimeout)
	for {
		restarted, err = Listen(addr, server, ListenOptions{TLSConfig: pki.server})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebind %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	go func() { _ = restarted.Serve(t.Context()) }()

	if err := channel.WaitUntilReady(ctx); err != nil {
		t.Fatalf("channel did not reconnect: %v", err)
	}
	if generation := channel.Generation(); generation != 2 {
		t.Fatalf("generation = %d, want 2", generation)
	}
	reply, err := Unary(ctx, channel, "/example.Greeter/Echo", &testMessage{Value: "after"}, newTestMessage)
	if err != nil || reply.Value != "after" {
		t.Fatalf("call after reconnect: %v, %v", reply, err)
	}
}

func waitForChannelEvent(t *testing.T, events <-chan ChannelEvent, want ChannelEventType) {
	t.Helper()
	timeout := time.After(rpcTestTimeout)
	for {
		select {
		case event := <-events:
			if event.Type == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for channel event %v", want)
		}
	}
}

func TestHTTP3AndWebTransportAreRefusedWhenDisabled(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.EnableHTTP = false
		options.EnableWebTransport = false
	})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	// The h3 ALPN is not offered, so the handshake itself fails.
	_, err := quic.DialAddr(ctx, listener.Addr().String(), &tls.Config{RootCAs: pki.client.RootCAs, ServerName: "localhost", NextProtos: []string{http3.NextProtoH3}}, &quic.Config{})
	if err == nil {
		t.Fatal("h3 connection to a native-only listener succeeded")
	}
}

func TestHTTP3RequestsOverTheWire(t *testing.T) {
	pki := newTestPKI(t)
	var admissions atomic.Int64
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.HTTPAdmission = func(request AdmissionRequest) bool {
			admissions.Add(1)
			return request.Header.Get("X-Allow") != "no"
		}
	})
	url := "https://" + listener.Addr().String() + DefaultHTTPPath
	roundTripper := &http3.Transport{TLSClientConfig: pki.client}
	t.Cleanup(func() { _ = roundTripper.Close() })

	type result struct {
		StatusCode int
		Header     http.Header
	}
	do := func(method, target, contentType, allow string) result {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader(""))
		if err != nil {
			t.Fatal(err)
		}
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		if allow != "" {
			request.Header.Set("X-Allow", allow)
		}
		response, err := roundTripper.RoundTrip(request)
		if err != nil {
			t.Fatalf("%s %s: %v", method, target, err)
		}
		_ = response.Body.Close()
		return result{StatusCode: response.StatusCode, Header: response.Header}
	}

	if got := do(http.MethodPost, url+"x", httpContentType, "").StatusCode; got != http.StatusNotFound {
		t.Errorf("unknown path status = %d", got)
	}
	if response := do(http.MethodGet, url, httpContentType, ""); response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") != "POST" {
		t.Errorf("GET status = %d, Allow = %q", response.StatusCode, response.Header.Get("Allow"))
	}
	if got := do(http.MethodPost, url, "text/plain", "").StatusCode; got != http.StatusUnsupportedMediaType {
		t.Errorf("wrong media type status = %d", got)
	}
	if got := do(http.MethodPost, url, httpContentType, "no").StatusCode; got != http.StatusForbidden {
		t.Errorf("denied admission status = %d", got)
	}
	if response := do(http.MethodPost, url, httpContentType, "yes"); response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != httpContentType {
		t.Errorf("admitted status = %d, content type = %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	if admissions.Load() != 2 {
		t.Errorf("admission callbacks = %d, want 2 (only valid requests reach admission)", admissions.Load())
	}
}

func TestWebTransportAdmissionAndPath(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)

	dial := func(path, origin string) (int, error) {
		wt := &webtransport.Transport{
			TLSClientConfig: pki.client.Clone(),
			QUICConfig:      &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true},
		}
		header := http.Header{}
		if origin != "" {
			header.Set("Origin", origin)
		}
		ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
		defer cancel()
		response, session, err := wt.Dial(ctx, "https://"+listener.Addr().String()+path, header)
		if session != nil {
			_ = session.CloseWithError(0, "")
		}
		if response == nil {
			return 0, err
		}
		_ = response.Body.Close()
		return response.StatusCode, err
	}

	if status, err := dial(DefaultHTTPPath, "https://evil.example.test"); err == nil || status != http.StatusForbidden {
		t.Errorf("denied origin: status=%d err=%v", status, err)
	}
	if status, err := dial(DefaultHTTPPath, ""); err == nil || status != http.StatusForbidden {
		t.Errorf("missing origin: status=%d err=%v", status, err)
	}
	if status, err := dial("/elsewhere", testOrigin); err == nil || status != http.StatusNotFound {
		t.Errorf("wrong path: status=%d err=%v", status, err)
	}
	if _, err := dial(DefaultHTTPPath, testOrigin); err != nil {
		t.Errorf("admitted session: %v", err)
	}
}

func TestWebTransportNegotiatesTheWireVersion(t *testing.T) {
	pki := newTestPKI(t)
	var diagnostics sync.Map
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		registerTestService(server)
		server.SetDiagnostics(func(event ServerDiagnostic) {
			if event.Phase == ServerDiagnosticSessionAdmission {
				diagnostics.Store(event.Message, event)
			}
		})
	})

	tests := []struct {
		name         string
		dial         webTransportTestDial
		wantStatus   int // 0 means the session is established
		wantSelected string
	}{
		{
			name:         "offered",
			dial:         webTransportTestDial{protocols: []string{"trevrpc-1"}},
			wantSelected: "trevrpc-1",
		},
		{
			name:         "offered after an unknown version",
			dial:         webTransportTestDial{protocols: []string{"trevrpc-2", "trevrpc-1"}},
			wantSelected: "trevrpc-1",
		},
		{
			name:         "offered with parameters",
			dial:         webTransportTestDial{protocols: []string{"trevrpc-1"}, available: `"trevrpc-2";q=1, "trevrpc-1";q=0.5`},
			wantSelected: "trevrpc-1",
		},
		{
			name: "no field means version 1",
			dial: webTransportTestDial{},
		},
		{
			name: "empty field is absent",
			dial: webTransportTestDial{available: " "},
		},
		{
			name: "malformed field is ignored",
			dial: webTransportTestDial{available: `trevrpc-2`},
		},
		{
			name: "field of the wrong type is ignored",
			dial: webTransportTestDial{available: `("trevrpc-2")`},
		},
		{
			name:       "only another version",
			dial:       webTransportTestDial{protocols: []string{"trevrpc-2"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "only other protocols",
			dial:       webTransportTestDial{protocols: []string{"chat", "trevrpc-10"}},
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.dial.origin = testOrigin
			response, session, err := dialWebTransportTest(pki, listener, tt.dial)
			if session != nil {
				defer func() { _ = session.CloseWithError(0, "test done") }()
			}
			if response != nil {
				defer func() { _ = response.Body.Close() }()
			}
			if tt.wantStatus != 0 {
				if err == nil || response == nil || response.StatusCode != tt.wantStatus {
					t.Fatalf("dial: status = %v, err = %v, want status %d", response, err, tt.wantStatus)
				}
				return
			}
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			if got := session.SessionState().ApplicationProtocol; got != tt.wantSelected {
				t.Fatalf("selected protocol = %q, want %q", got, tt.wantSelected)
			}
			// The session carries RPCs.
			transport := newStreamClient(webTransportTestOpener{session}, DefaultMaxFrameSize)
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
			defer cancel()
			reply, err := Unary(ctx, transport, testPath("Echo"), &testMessage{Value: "hi"}, newTestMessage)
			if err != nil || reply.Value != "hi" {
				t.Fatalf("Unary() = %v, %v", reply, err)
			}
		})
	}

	const wantMessage = `unsupported WebTransport protocol; offered "chat", "trevrpc-10"`
	// Diagnostics are delivered asynchronously.
	deadline := time.Now().Add(rpcTestTimeout)
	for {
		if _, ok := diagnostics.Load(wantMessage); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session admission diagnostic %q", wantMessage)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebTransportProtocolMismatchSkipsAdmission(t *testing.T) {
	pki := newTestPKI(t)
	var admissions atomic.Int64
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.WebTransportAdmission = func(AdmissionRequest) bool {
			admissions.Add(1)
			return true
		}
	})
	response, session, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: testOrigin, protocols: []string{"trevrpc-2"}})
	if session != nil {
		_ = session.CloseWithError(0, "")
	}
	if response == nil || response.StatusCode != http.StatusBadRequest || err == nil {
		t.Fatalf("dial: response = %v, err = %v, want 400", response, err)
	}
	_ = response.Body.Close()
	if admissions.Load() != 0 {
		t.Errorf("admission callbacks = %d, want 0 for a request the server cannot serve", admissions.Load())
	}
}

// WebTransport SETTINGS codepoints from draft-ietf-webtrans-http3-14.
const (
	settingWTMaxSessions           = 0x14e9cd29
	settingWTInitialMaxData        = 0x2b61
	settingWTInitialMaxStreamsUni  = 0x2b64
	settingWTInitialMaxStreamsBidi = 0x2b65
)

// flowControlledWebTransportConfig makes a test client advertise WT_INITIAL_*
// SETTINGS like Safari, which enables WebTransport session flow control.
var flowControlledWebTransportConfig = &webtransport.Config{
	MaxIncomingStreams:    100,
	MaxIncomingUniStreams: 100,
	MaxIncomingData:       64 * 1024,
}

// Safari enables session flow control and then cannot open a stream until the
// server grants initial stream and data credit, so the server must advertise it.
func TestWebTransportAdvertisesSessionFlowControl(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.MaxFrameSize = 1024
		options.MaxStreamBodySize = 4096
		options.MaxConcurrentStreamsPerConnection = 10
	})

	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	tlsConfig := pki.client.Clone()
	tlsConfig.NextProtos = []string{http3.NextProtoH3}
	conn, err := quic.DialAddr(ctx, listener.Addr().String(), tlsConfig, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseWithError(0, "") }()
	client := (&http3.Transport{EnableDatagrams: true}).NewClientConn(conn)
	select {
	case <-client.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal("server SETTINGS not received")
	}

	settings := client.Settings().Other
	if settings[settingWTMaxSessions] <= 1 {
		t.Errorf("WT_MAX_SESSIONS = %d; Safari needs it, and above 1 it enables flow control", settings[settingWTMaxSessions])
	}
	if got := settings[settingWTInitialMaxStreamsBidi]; got != 20 {
		t.Errorf("WT_INITIAL_MAX_STREAMS_BIDI = %d, want twice the per-connection limit", got)
	}
	if got, ok := settings[settingWTInitialMaxStreamsUni]; !ok || got != 0 {
		t.Errorf("WT_INITIAL_MAX_STREAMS_UNI = %d (sent %t), want an explicit 0", got, ok)
	}
	if got := settings[settingWTInitialMaxData]; got != 4096 {
		t.Errorf("WT_INITIAL_MAX_DATA = %d, want the connection receive window", got)
	}
}

// With session flow control enabled, the server must keep granting stream and
// data credit as calls complete, and honor the client's credit when replying.
func TestWebTransportSessionFlowControlReplenishesCredit(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.MaxFrameSize = 64 * 1024
		options.MaxConcurrentStreamsPerConnection = 2
	})
	transport := newWebTransportTestClient(t, pki, listener, testOrigin, flowControlledWebTransportConfig)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	// 16 calls exceed the 3-stream grant, and 512 KiB each way exceeds both the
	// server's ~128 KiB and the client's 64 KiB session data windows.
	value := strings.Repeat("x", 32*1024)
	for i := range 16 {
		reply, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: value}, newTestMessage)
		if err != nil || reply.Value != value {
			t.Fatalf("call %d: err = %v", i, err)
		}
	}
}

func TestWebTransportIsRejectedWithoutAdmissionCallback(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) { options.WebTransportAdmission = nil })
	wt := &webtransport.Transport{
		TLSClientConfig: pki.client.Clone(),
		QUICConfig:      &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true},
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	response, _, err := wt.Dial(ctx, "https://"+listener.Addr().String()+DefaultHTTPPath, nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("response=%v err=%v, want 403 without an admission callback", response, err)
	}
}

// TestTerminalStatusReachesClientWhoseRequestSideIsStillOpen covers a handler
// that fails a bidirectional call while the client has not finished sending.
// The server must deliver the status and then drain, not stop, the request
// side; only a client that keeps sending past the bound is stopped.
func TestTerminalStatusReachesClientWhoseRequestSideIsStillOpen(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)

	conn, err := quic.DialAddr(t.Context(), listener.Addr().String(), clientTLSConfig(pki.client, "localhost"), clientQUICConfig(nil, DefaultMaxFrameSize))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
	openers := map[string]streamOpener{
		"native":       connOpener{conn},
		"webtransport": newWebTransportTestOpener(t, pki, listener, testOrigin, nil),
	}
	for name, opener := range openers {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
			defer cancel()
			stream, err := opener.openStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request := newWireRequest(testPath("FailFast"))
			request.Kind = RPCKindBidirectionalStreaming
			writeTestRequestFrame(t, stream, request)

			// The status arrives although the request side is still open.
			frame := readTestFrame(t, stream)
			if status := frame.statusValue(); frame.Kind != frameTypeStatus || status.Code != CodeAborted || status.Message != "abort" {
				t.Fatalf("frame = %+v, want Aborted \"abort\" status", frame)
			}
			if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("read after status = %v, want EOF", err)
			}

			// The server keeps accepting request bytes, so the client is not
			// reset right after the status...
			chunk := make([]byte, 16*1024)
			var sent int64
			for range 4 {
				n, err := stream.Write(chunk)
				sent += int64(n)
				if err != nil {
					t.Fatalf("write %d bytes after the terminal status: %v", sent, err)
				}
				time.Sleep(25 * time.Millisecond)
			}
			// ...but a client that never finishes is stopped once the bound is hit.
			for sent <= requestDrainLimit+1<<20 {
				n, err := stream.Write(chunk)
				sent += int64(n)
				if err != nil {
					if sent < requestDrainLimit {
						t.Fatalf("stopped after %d bytes, below the %d byte bound", sent, requestDrainLimit)
					}
					return
				}
			}
			t.Fatalf("a client that kept sending %d bytes was never stopped", sent)
		})
	}
}

func TestDrainedRequestSideIsStoppedAfterTimeBound(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	channel := dialNative(t, pki, listener)
	stream, err := channel.current.(*streamGeneration).client.opener.openStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request := newWireRequest(testPath("FailFast"))
	request.Kind = RPCKindBidirectionalStreaming
	writeTestRequestFrame(t, stream, request)
	readTestFrame(t, stream)

	// A silent client is stopped after about requestDrainTimeout.
	started := time.Now()
	for {
		if _, err := stream.Write([]byte{0}); err != nil {
			break
		}
		if time.Since(started) > 5*time.Second {
			t.Fatal("silent client was never stopped")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if elapsed := time.Since(started); elapsed < requestDrainTimeout-200*time.Millisecond {
		t.Fatalf("stopped after %v, before the %v time bound", elapsed, requestDrainTimeout)
	}
}
