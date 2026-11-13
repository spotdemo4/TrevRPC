package trevrpc

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
	"golang.org/x/net/http2"
)

func TestTCPListenerDefaultsToTheUDPPortNumber(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	udpPort := listener.Addr().(*net.UDPAddr).Port
	if tcpPort := listener.TCPAddr().(*net.TCPAddr).Port; tcpPort != udpPort {
		t.Fatalf("TCP port %d, want the UDP port %d", tcpPort, udpPort)
	}
}

func TestTCPCanBeDisabled(t *testing.T) {
	pki := newTestPKI(t)
	listener, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{TLSConfig: pki.server, DisableTCP: true})
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, listener)
	if listener.TCPAddr() != nil {
		t.Fatalf("TCPAddr = %v with TCP disabled", listener.TCPAddr())
	}
}

func TestTCPOffersOnlyEnabledProtocols(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.EnableHTTP = false
		options.EnableWebTransport = false
	})
	for _, protocol := range []string{http2ALPN, http1ALPN} {
		config := pki.client.Clone()
		config.NextProtos = []string{protocol}
		conn, err := (&tls.Dialer{Config: config}).DialContext(t.Context(), "tcp", listener.TCPAddr().String())
		if err == nil {
			_ = conn.Close()
			t.Errorf("%s handshake with HTTP disabled succeeded", protocol)
		}
	}
	qmux := newQMuxTestClient(t, pki, listener)
	if _, err := Unary(context.Background(), qmux, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatalf("QMux call: %v", err)
	}
}

// Browsers must open WebSocket sessions over HTTP/1.1, the only binding
// served, so HTTP/2 must not offer extended CONNECT (RFC 8441).
func TestHTTP2DoesNotAdvertiseExtendedConnect(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	config := pki.client.Clone()
	config.NextProtos = []string{http2ALPN}
	conn, err := (&tls.Dialer{Config: config}).DialContext(t.Context(), "tcp", listener.TCPAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, conn)
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	if err := framer.WriteSettings(); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(rpcTestTimeout))
	frame, err := framer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	settings, ok := frame.(*http2.SettingsFrame)
	if !ok {
		t.Fatalf("first frame is %T, want SETTINGS", frame)
	}
	if value, ok := settings.Value(http2.SettingEnableConnectProtocol); ok && value != 0 {
		t.Fatal("server advertises SETTINGS_ENABLE_CONNECT_PROTOCOL")
	}
	if value, ok := settings.Value(http2.SettingMaxConcurrentStreams); !ok || value != uint32(DefaultServerOptions().MaxConcurrentStreamsPerConnection+1) {
		t.Fatalf("SETTINGS_MAX_CONCURRENT_STREAMS = %d, %v", value, ok)
	}
}

func TestHTTP2ResponsesAdvertiseHTTP3(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	want := `h3=":` + strings.TrimPrefix(listener.Addr().String(), "127.0.0.1:") + `"; ma=86400`
	if got := http2TestAltSvc(t, pki, listener); got != want {
		t.Fatalf("Alt-Svc = %q, want %q", got, want)
	}
}

// Behind a proxy that exposes the UDP socket on another port, Alt-Svc
// advertises that port.
func TestHTTP2ResponsesAdvertiseAltSvcPort(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	server.SetOptions(options)
	registerTestService(server)
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server, AltSvcPort: 443})
	if got, want := http2TestAltSvc(t, pki, listener), `h3=":443"; ma=86400`; got != want {
		t.Fatalf("Alt-Svc = %q, want %q", got, want)
	}
}

func TestListenRejectsInvalidAltSvcPort(t *testing.T) {
	pki := newTestPKI(t)
	for _, port := range []int{-1, 65536} {
		_, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{TLSConfig: pki.server, AltSvcPort: port})
		if StatusFromError(err).Code != CodeInvalidArgument {
			t.Fatalf("Listen with AltSvcPort %d: error %v, want InvalidArgument", port, err)
		}
	}
}

// http2TestAltSvc sends a unary RPC to listener over HTTP/2 and returns the
// Alt-Svc header of its response.
func http2TestAltSvc(t *testing.T, pki testPKI, listener *Listener) string {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: pki.client.Clone(), ForceAttemptHTTP2: true}}
	defer client.CloseIdleConnections()
	body := encodeHTTP3TestRequest(t, newWireRequest(testPath("Echo")), "")
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://"+listener.TCPAddr().String()+DefaultHTTPPath, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", httpContentType)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.ProtoMajor != 2 || response.StatusCode != http.StatusOK {
		t.Fatalf("response %s %d, want HTTP/2 200", response.Proto, response.StatusCode)
	}
	return response.Header.Get("Alt-Svc")
}

func TestHTTP1RequestsAreRefused(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: pki.client.Clone(), Protocols: protocols}}
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://"+listener.TCPAddr().String()+DefaultHTTPPath, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", httpContentType)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusHTTPVersionNotSupported {
		t.Fatalf("HTTP/1.1 status = %d, want 505", response.StatusCode)
	}
}

func TestHTTP2EnforcesTheStreamLimitPerConnection(t *testing.T) {
	pki := newTestPKI(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHold := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHold)
	listener := startTestListenerWith(t, pki, func(options *ServerOptions) {
		options.MaxConcurrentStreamsPerConnection = 1
	}, func(server *Server) {
		registerTestService(server)
		// Hold occupies the connection's only RPC slot until released.
		RegisterUnary(server, "/example.Greeter/Hold", newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
			entered <- struct{}{}
			select {
			case <-release:
				return &testMessage{Value: "held"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	})
	// By default net/http opens another connection when one is at the
	// server's stream limit, which would bypass the per-connection limit.
	transport := newHTTP2TestClientWith(t, pki, listener, func(roundTripper *http.Transport) {
		roundTripper.HTTP2 = &http.HTTP2Config{StrictMaxConcurrentRequests: true}
	})

	held := make(chan error, 1)
	go func() {
		_, err := Unary(t.Context(), transport, "/example.Greeter/Hold", &testMessage{}, newTestMessage)
		held <- err
	}()
	select {
	case <-entered:
	case <-time.After(rpcTestTimeout):
		t.Fatal("held RPC did not reach its handler")
	}

	_, err := Unary(t.Context(), transport, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage)
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("RPC on a saturated connection = %v, want HTTP 503", err)
	}

	releaseHold()
	select {
	case err := <-held:
		if err != nil {
			t.Fatalf("held RPC = %v", err)
		}
	case <-time.After(rpcTestTimeout):
		t.Fatal("held RPC did not finish after release")
	}
	// The server frees the slot before it sends the held RPC's status, so the
	// next RPC is admitted.
	if _, err := Unary(t.Context(), transport, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatalf("RPC after the slot was freed = %v", err)
	}
}

func TestWebSocketAdmissionPathAndSubprotocol(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	dial := func(path, origin string, subprotocols ...string) (int, error) {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
		defer cancel()
		header := http.Header{}
		if origin != "" {
			header.Set("Origin", origin)
		}
		conn, response, err := websocket.Dial(ctx, "wss://"+listener.TCPAddr().String()+path, &websocket.DialOptions{
			HTTPClient:   &http.Client{Transport: &http.Transport{TLSClientConfig: pki.client.Clone()}},
			HTTPHeader:   header,
			Subprotocols: subprotocols,
		})
		if conn != nil {
			_ = conn.CloseNow()
		}
		if response == nil {
			return 0, err
		}
		if response.Body != nil {
			_ = response.Body.Close()
		}
		return response.StatusCode, err
	}
	for name, test := range map[string]struct {
		path, origin string
		subprotocols []string
		want         int
	}{
		"denied origin":       {DefaultHTTPPath, "https://evil.example.test", []string{QMuxALPN}, http.StatusForbidden},
		"wrong path":          {"/elsewhere", testOrigin, []string{QMuxALPN}, http.StatusNotFound},
		"missing subprotocol": {DefaultHTTPPath, testOrigin, nil, http.StatusBadRequest},
		"other subprotocol":   {DefaultHTTPPath, testOrigin, []string{"chat"}, http.StatusBadRequest},
		"admitted":            {DefaultHTTPPath, testOrigin, []string{"chat", QMuxALPN}, http.StatusSwitchingProtocols},
	} {
		if got, err := dial(test.path, test.origin, test.subprotocols...); got != test.want {
			t.Errorf("%s: status %d (%v), want %d", name, got, err, test.want)
		}
	}
}

// serveForShutdownTest serves the test service with an unbounded graceful
// shutdown, so that only the waits before an RPC, never the drain timeout, can
// end a shutdown promptly. configure, if not nil, adjusts the server. stop shuts
// the listener down and fails the test unless Serve returns within 2 s, well
// within the 10 s timeouts those waits used to run to.
func serveForShutdownTest(t *testing.T, pki testPKI, configure func(*Server)) (listener *Listener, stop func()) {
	t.Helper()
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request AdmissionRequest) bool { return request.Origin == testOrigin }
	options.GracefulShutdownTimeout = time.Hour
	server.SetOptions(options)
	registerTestService(server)
	if configure != nil {
		configure(server)
	}
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: pki.server})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(ctx) }()
	for !listener.serving.Load() {
		time.Sleep(time.Millisecond)
	}
	return listener, func() {
		t.Helper()
		cancel()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Fatalf("Serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			logGoroutines(t)
			t.Fatal("Serve did not stop promptly")
		}
	}
}

// Connections that have not started an RPC end as soon as the listener shuts
// down. The HTTP server would otherwise wait up to 10 s for an HTTP/2 preface,
// and keep a connection without an HTTP/1.1 request for up to 5 s.
func TestShutdownEndsConnectionsThatHaveNotStarted(t *testing.T) {
	pki := newTestPKI(t)
	listener, stop := serveForShutdownTest(t, pki, nil)

	addr := listener.TCPAddr().String()
	noClientHello, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conns := []net.Conn{noClientHello}
	for _, protocol := range []string{http2ALPN, http1ALPN, QMuxALPN} {
		config := pki.client.Clone()
		config.NextProtos = []string{protocol}
		conn, err := (&tls.Dialer{Config: config}).DialContext(t.Context(), "tcp", addr)
		if err != nil {
			t.Fatalf("%s handshake: %v", protocol, err)
		}
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	// The HTTP/2 and HTTP/1.1 connections reach the HTTP server.
	waitFor(t, "the HTTP server to take both HTTP connections", func() bool {
		listener.httpTCP.mu.Lock()
		defer listener.httpTCP.mu.Unlock()
		return len(listener.httpTCP.unstarted) == 2
	})

	stop()
	for _, conn := range conns {
		// Closed, with or without close_notify, or reset; not timed out.
		_ = conn.SetReadDeadline(time.Now().Add(rpcTestTimeout))
		if _, err := io.Copy(io.Discard, conn); isTimeoutError(err) {
			t.Errorf("connection from %s was not closed", conn.LocalAddr())
		}
	}
}

// A stream whose REQUEST frame has not arrived when the listener begins
// shutting down fails with Unavailable at once: no handler has run, so the
// client may retry the call elsewhere. The connection's close usually overtakes
// the answer, so the metrics show the status.
func TestShutdownRefusesStreamsStillAwaitingTheirRequest(t *testing.T) {
	for _, transport := range []string{"native", "webtransport"} {
		t.Run(transport, func(t *testing.T) {
			pki := newTestPKI(t)
			metrics := &recordingMetrics{}
			listener, stop := serveForShutdownTest(t, pki, func(server *Server) { server.SetMetrics(metrics) })
			var opener streamOpener
			if transport == "native" {
				conn, err := quic.DialAddr(t.Context(), listener.Addr().String(), clientTLSConfig(pki.client, "localhost"), withTestHandshakeTimeout(clientQUICConfig(nil, DefaultMaxFrameSize)))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
				opener = connOpener{conn}
			} else {
				opener = newWebTransportTestOpener(t, pki, listener, testOrigin, nil)
			}
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
			defer cancel()

			waiting, err := opener.openStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// A REQUEST frame whose last byte never follows.
			request := testRequestBytes(t, requestFor("Echo", RPCKindUnary))
			if _, err := waiting.Write(request[:len(request)-1]); err != nil {
				t.Fatal(err)
			}
			// Wait until the server is reading it before shutting down: a
			// later call that completes first proves nothing, since
			// WebTransport may accept streams out of order. The listener's
			// workers run nothing but its RPC streams.
			waitFor(t, "the server to read the waiting stream", func() bool { return listener.workers.busy() == 1 })
			call, err := opener.openStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := call.Write(testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "x"))); err != nil {
				t.Fatal(err)
			}
			_ = call.Close()
			_ = call.SetReadDeadline(time.Now().Add(rpcTestTimeout))
			if _, err := io.ReadAll(call); err != nil {
				t.Fatalf("read the later call: %v", err)
			}

			stop()
			_, finished := metrics.snapshot()
			if len(finished) != 2 || finished[0].Code != CodeOK || finished[1].Code != CodeUnavailable {
				t.Fatalf("finished RPCs = %+v, want the later call OK, then the waiting one Unavailable", finished)
			}
			// The connection's close may cut the answer short.
			_ = waiting.SetReadDeadline(time.Now().Add(rpcTestTimeout))
			next, err := newFrameReader(waiting, DefaultMaxFrameSize, -1).next()
			if isTimeoutError(err) {
				t.Fatal("stream was neither answered nor closed")
			}
			if err != nil {
				return
			}
			frame, err := decodeTestFrame(next)
			if err != nil {
				t.Fatal(err)
			}
			if status := frame.statusValue(); frame.Kind != frameTypeStatus || status.Code != CodeUnavailable || status.Message != errServerShutdown.Error() {
				t.Fatalf("frame = %+v, want an Unavailable %q status", frame, errServerShutdown)
			}
		})
	}
}
