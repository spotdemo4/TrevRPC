package trevrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
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
	RegisterUnary(server, testServiceName, "Echo", newTestMessage, func(ctx context.Context, request *testMessage) (*Response[*testMessage], error) {
		switch request.Value {
		case "missing":
			return nil, NotFound("nothing here")
		case "slow":
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return testResponse(&testMessage{Value: request.Value + string(RequestMetadataFromContext(ctx)["suffix"])}), nil
	})
	RegisterServerStreaming(server, testServiceName, "Count", newTestMessage, func(_ context.Context, request *testMessage) (MessageStream[*testMessage], error) {
		replies := make([]*testMessage, 0, 3)
		for _, suffix := range []string{"1", "2", "3"} {
			replies = append(replies, &testMessage{Value: request.Value + suffix})
		}
		return newSliceStream(replies...), nil
	})
	RegisterClientStreaming(server, testServiceName, "Concat", newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (*Response[*testMessage], error) {
		var joined strings.Builder
		for request, err := range Messages(requests) {
			if err != nil {
				return nil, err
			}
			joined.WriteString(request.Value)
		}
		return testResponse(&testMessage{Value: joined.String()}), nil
	})
	// FailFast fails a bidirectional call without reading any request message.
	server.routeStreaming(testServiceName, "FailFast", RpcKindBidirectionalStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return nil, NewStatus(CodeAborted, "abort")
	})
	RegisterBidirectionalStreaming(server, testServiceName, "Chat", newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
		return &upperStream{requests: requests}, nil
	})
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
func startTestListener(t *testing.T, pki testPKI, configure func(*ServerOptions)) *Listener {
	t.Helper()
	server := NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request AdmissionRequest) bool { return request.Origin == testOrigin }
	if configure != nil {
		configure(&options)
	}
	server.SetOptions(options)
	registerTestService(server)

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

func dialNative(t *testing.T, pki testPKI, listener *Listener) *Channel {
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
		"native":       dialNative(t, pki, listener),
		"http3":        newHTTP3TestClient(t, pki, listener),
		"webtransport": newWebTransportTestClient(t, pki, listener, testOrigin),
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
		reply, err := Unary(ctx, transport, testServiceName, "Echo", &testMessage{Value: "hello"}, newTestMessage, WithMetadata("suffix", []byte("!")))
		if err != nil || reply.Value != "hello!" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
	})
	t.Run("unary error", func(t *testing.T) {
		_, err := Unary(ctx, transport, testServiceName, "Echo", &testMessage{Value: "missing"}, newTestMessage)
		if status := StatusFromError(err); status.Code != CodeNotFound || status.Message != "nothing here" {
			t.Fatalf("status = %v", status)
		}
	})
	t.Run("unary large", func(t *testing.T) {
		large := strings.Repeat("x", 1<<20)
		reply, err := Unary(ctx, transport, testServiceName, "Echo", &testMessage{Value: large}, newTestMessage)
		if err != nil || reply.Value != large {
			t.Fatalf("large echo failed: err=%v", err)
		}
	})
	t.Run("unknown method", func(t *testing.T) {
		_, err := Unary(ctx, transport, testServiceName, "Nope", &testMessage{}, newTestMessage)
		if status := StatusFromError(err); status.Code != CodeUnimplemented {
			t.Fatalf("status = %v", status)
		}
	})
	t.Run("unary deadline", func(t *testing.T) {
		_, err := Unary(ctx, transport, testServiceName, "Echo", &testMessage{Value: "slow"}, newTestMessage, WithTimeout(100*time.Millisecond))
		if status := StatusFromError(err); status.Code != CodeDeadlineExceeded {
			t.Fatalf("status = %v", status)
		}
	})
	t.Run("server streaming", func(t *testing.T) {
		stream, err := ServerStreaming(ctx, transport, testServiceName, "Count", &testMessage{Value: "n"}, newTestMessage)
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
		reply, err := runTestClientStreaming(ctx, transport, testServiceName, "Concat", []string{"a", "b", "c"})
		if err != nil || reply.Value != "abc" {
			t.Fatalf("reply = %v, err = %v", reply, err)
		}
	})
	t.Run("bidirectional streaming", func(t *testing.T) {
		call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, testServiceName, "Chat", newTestMessage)
		if err != nil {
			t.Fatal(err)
		}
		defer call.Close()
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
		if _, err := call.Recv(); err != io.EOF {
			t.Fatalf("expected EOF after CloseSend, got %v", err)
		}
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
	rebound, err := net.ListenPacket("udp", addr)
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
	if _, err := Unary(context.Background(), channel, testServiceName, "Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
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

func TestListenerShutdownClosesNativeHTTP3AndWebTransportPromptly(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
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
		"native":       dialNative(t, pki, listener),
		"http3":        newHTTP3TestClient(t, pki, listener),
		"webtransport": newWebTransportTestClient(t, pki, listener, ""),
	} {
		if _, err := Unary(context.Background(), transport, testServiceName, "Echo", &testMessage{Value: name}, newTestMessage); err != nil {
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
	routeBytes(server, testServiceName, "Block", func(ctx context.Context, _ []byte) ([]byte, error) {
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
		_, err := channel.call(context.Background(), newWireRequest(testServiceName, "Block", nil))
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
	if _, err := Unary(context.Background(), first, testServiceName, "Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatal(err)
	}

	second := dialNative(t, pki, listener)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	if _, err := Unary(ctx, second, testServiceName, "Echo", &testMessage{Value: "x"}, newTestMessage); err == nil {
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
	defer channel.Close()
	if _, err := Unary(ctx, channel, testServiceName, "Echo", &testMessage{Value: "before"}, newTestMessage); err != nil {
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
	reply, err := Unary(ctx, channel, testServiceName, "Echo", &testMessage{Value: "after"}, newTestMessage)
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
		options.EnableHTTP3 = false
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
		options.HTTP3Admission = func(request AdmissionRequest) bool {
			admissions.Add(1)
			return request.Header.Get("X-Allow") != "no"
		}
	})
	url := "https://" + listener.Addr().String() + DefaultHTTP3Path
	roundTripper := &http3.Transport{TLSClientConfig: pki.client}
	t.Cleanup(func() { _ = roundTripper.Close() })

	do := func(method, target, contentType, allow string) *http.Response {
		t.Helper()
		request, err := http.NewRequest(method, target, strings.NewReader(""))
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
		return response
	}

	if got := do(http.MethodPost, url+"x", http3ContentType, "").StatusCode; got != http.StatusNotFound {
		t.Errorf("unknown path status = %d", got)
	}
	if response := do(http.MethodGet, url, http3ContentType, ""); response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") != "POST" {
		t.Errorf("GET status = %d, Allow = %q", response.StatusCode, response.Header.Get("Allow"))
	}
	if got := do(http.MethodPost, url, "text/plain", "").StatusCode; got != http.StatusUnsupportedMediaType {
		t.Errorf("wrong media type status = %d", got)
	}
	if got := do(http.MethodPost, url, http3ContentType, "no").StatusCode; got != http.StatusForbidden {
		t.Errorf("denied admission status = %d", got)
	}
	if response := do(http.MethodPost, url, http3ContentType, "yes"); response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != http3ContentType {
		t.Errorf("admitted status = %d, content type = %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	if admissions.Load() != 2 {
		t.Errorf("admission callbacks = %d, want 2 (only valid requests reach admission)", admissions.Load())
	}
}

func TestWebTransportAdmissionAndPath(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)

	dial := func(path, origin string) (*http.Response, error) {
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
		return response, err
	}

	if response, err := dial(DefaultHTTP3Path, "https://evil.example.test"); err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Errorf("denied origin: response=%v err=%v", response, err)
	}
	if response, err := dial(DefaultHTTP3Path, ""); err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Errorf("missing origin: response=%v err=%v", response, err)
	}
	if response, err := dial("/elsewhere", testOrigin); err == nil || response == nil || response.StatusCode != http.StatusNotFound {
		t.Errorf("wrong path: response=%v err=%v", response, err)
	}
	if _, err := dial(DefaultHTTP3Path, testOrigin); err != nil {
		t.Errorf("admitted session: %v", err)
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
	response, _, err := wt.Dial(ctx, "https://"+listener.Addr().String()+DefaultHTTP3Path, nil)
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
		"webtransport": newWebTransportTestOpener(t, pki, listener, testOrigin),
	}
	for name, opener := range openers {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
			defer cancel()
			stream, err := opener.openStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request := newWireRequest(testServiceName, "FailFast", nil)
			request.Kind = RpcKindBidirectionalStreaming
			if err := writeFrame(stream, request, DefaultMaxFrameSize); err != nil {
				t.Fatal(err)
			}

			// The status arrives although the request side is still open.
			frame := &wireStreamFrame{}
			if err := readFrame(stream, frame, DefaultMaxFrameSize); err != nil {
				t.Fatalf("read terminal status: %v", err)
			}
			if status := StatusFromError(&Status{Code: CodeFromUint32(frame.Status), Message: frame.Message}); frame.Kind != wireFrameStatus || status.Code != CodeAborted || status.Message != "abort" {
				t.Fatalf("frame = %+v, want Aborted \"abort\" status", frame)
			}
			if _, err := stream.Read(make([]byte, 1)); err != io.EOF {
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
	stream, err := channel.current.(*connGeneration).client.opener.openStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request := newWireRequest(testServiceName, "FailFast", nil)
	request.Kind = RpcKindBidirectionalStreaming
	if err := writeFrame(stream, request, DefaultMaxFrameSize); err != nil {
		t.Fatal(err)
	}
	if err := readFrame(stream, &wireStreamFrame{}, DefaultMaxFrameSize); err != nil {
		t.Fatal(err)
	}

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
