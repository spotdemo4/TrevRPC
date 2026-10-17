package trevrpc

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
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
	if _, err := Unary(context.Background(), qmux, testServiceName, "Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
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
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: pki.client.Clone(), ForceAttemptHTTP2: true}}
	defer client.CloseIdleConnections()
	body := encodeHTTP3TestRequest(t, newWireRequest(testServiceName, "Echo", nil))
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
	want := `h3=":` + strings.TrimPrefix(listener.Addr().String(), "127.0.0.1:") + `"; ma=86400`
	if got := response.Header.Get("Alt-Svc"); got != want {
		t.Fatalf("Alt-Svc = %q, want %q", got, want)
	}
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
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.MaxConcurrentStreamsPerConnection = 1
	})
	transport := newHTTP2TestClient(t, pki, listener)
	holdCtx, release := context.WithCancel(t.Context())
	defer release()
	// "slow" holds the connection's only RPC slot until cancelled. It is
	// restarted whenever it loses the race for the slot.
	held := make(chan struct{}, 1)
	hold := func() {
		go func() {
			_, _ = Unary(holdCtx, transport, testServiceName, "Echo", &testMessage{Value: "slow"}, newTestMessage)
			held <- struct{}{}
		}()
	}
	hold()
	deadline := time.Now().Add(rpcTestTimeout)
	for {
		select {
		case <-held:
			hold()
		default:
		}
		_, err := Unary(context.Background(), transport, testServiceName, "Echo", &testMessage{Value: "x"}, newTestMessage)
		if err != nil && strings.Contains(err.Error(), "503") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("concurrent RPC on a saturated connection = %v, want HTTP 503", err)
		}
		time.Sleep(10 * time.Millisecond)
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
