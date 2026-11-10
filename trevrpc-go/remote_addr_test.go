package trevrpc

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"testing"
)

const remoteAddrMethod = "/example.Remote/Addr"

// remoteAddrRecorder records the client addresses the authorizer and the
// admission callbacks see, and serves remoteAddrMethod, which answers the one
// its handler sees.
type remoteAddrRecorder struct {
	mu         sync.Mutex
	authorized []netip.AddrPort
	admitted   []netip.AddrPort
}

func (r *remoteAddrRecorder) Authorize(ctx context.Context, info HandlerContext) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.authorized = append(r.authorized, info.RemoteAddr)
	return ctx, nil
}

func (r *remoteAddrRecorder) admit(request AdmissionRequest) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admitted = append(r.admitted, request.RemoteAddr)
	return true
}

// lastAuthorized and lastAdmitted return the address the authorizer or an
// admission callback saw last.
func (r *remoteAddrRecorder) lastAuthorized() netip.AddrPort {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.authorized[len(r.authorized)-1]
}

func (r *remoteAddrRecorder) lastAdmitted() netip.AddrPort {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.admitted) == 0 {
		return netip.AddrPort{}
	}
	return r.admitted[len(r.admitted)-1]
}

// startRemoteAddrListener serves remoteAddrMethod on every transport, with
// recorder as the authorizer and admission, and listenOptions.
func startRemoteAddrListener(t *testing.T, pki testPKI, recorder *remoteAddrRecorder, listenOptions ListenOptions) *Listener {
	t.Helper()
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.HTTPAdmission = recorder.admit
	options.WebTransportAdmission = recorder.admit
	server.SetOptions(options)
	server.SetAuthorizer(recorder)
	RegisterUnary(server, remoteAddrMethod, newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
		info, _ := HandlerContextFromContext(ctx)
		return &testMessage{Value: info.RemoteAddr.String()}, nil
	})
	listenOptions.TLSConfig = pki.server
	return serveTestListener(t, server, listenOptions)
}

// callRemoteAddr returns the client address the handler of remoteAddrMethod
// sees for a call on transport.
func callRemoteAddr(t *testing.T, transport Transport) netip.AddrPort {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	response, err := Unary(ctx, transport, remoteAddrMethod, &testMessage{}, newTestMessage)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	addr, err := netip.ParseAddrPort(response.Value)
	if err != nil {
		t.Fatalf("handler saw remote address %q: %v", response.Value, err)
	}
	return addr
}

func TestHandlersAuthorizersAndAdmissionSeeTheClientAddress(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &remoteAddrRecorder{}
	listener := startRemoteAddrListener(t, pki, recorder, ListenOptions{})

	for _, transport := range []struct {
		name      string
		transport func() Transport
		admitted  bool
	}{
		{"native", func() Transport { return dialNative(t, pki, listener) }, false},
		{"http3", func() Transport { return newHTTP3TestClient(t, pki, listener) }, true},
		{"webtransport", func() Transport { return newWebTransportTestClient(t, pki, listener, testOrigin, nil) }, true},
		{"http2", func() Transport { return newHTTP2TestClient(t, pki, listener) }, true},
		{"qmux", func() Transport { return newQMuxTestClient(t, pki, listener) }, false},
		{"qmux websocket", func() Transport { return newWebSocketTestClient(t, pki, listener, testOrigin) }, true},
	} {
		t.Run(transport.name, func(t *testing.T) {
			got := callRemoteAddr(t, transport.transport())
			if got.Addr() != netip.MustParseAddr("127.0.0.1") || got.Port() == 0 {
				t.Errorf("handler saw %v, want 127.0.0.1 and the client's port", got)
			}
			if authorized := recorder.lastAuthorized(); authorized != got {
				t.Errorf("authorizer saw %v, handler %v", authorized, got)
			}
			if admitted := recorder.lastAdmitted(); transport.admitted && admitted != got {
				t.Errorf("admission saw %v, handler %v", admitted, got)
			}
		})
	}
}

// startProxyForwarder forwards the TCP connections it accepts to target, each
// preceded by header, as a load balancer speaking PROXY protocol does. It
// returns its address.
func startProxyForwarder(t *testing.T, target string, header []byte) string {
	t.Helper()
	var listenConfig net.ListenConfig
	ln, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, ln)
	var conns sync.WaitGroup
	t.Cleanup(conns.Wait)
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Go(func() {
				defer func() { _ = client.Close() }()
				var dialer net.Dialer
				upstream, err := dialer.DialContext(context.Background(), "tcp", target)
				if err != nil {
					return
				}
				defer func() { _ = upstream.Close() }()
				if _, err := upstream.Write(header); err != nil {
					return
				}
				go func() {
					_, _ = io.Copy(upstream, client)
					_ = upstream.Close()
				}()
				_, _ = io.Copy(client, upstream)
			})
		}
	}()
	return ln.Addr().String()
}

// newQMuxTestClientAt returns a Channel over QMux over TLS to addr.
func newQMuxTestClientAt(t *testing.T, pki testPKI, addr string) (*Channel, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	connector := &qmuxConnector{
		addr:         addr,
		tls:          clientQMuxTLSConfig(pki.client, "localhost"),
		maxFrameSize: DefaultMaxFrameSize,
	}
	channel, err := dialChannel(ctx, connector, nil)
	if err == nil {
		t.Cleanup(func() { _ = channel.Close() })
	}
	return channel, err
}

// newHTTP2TestClientAt returns a Transport that sends every RPC as one
// HTTP/2 request to addr.
func newHTTP2TestClientAt(t *testing.T, pki testPKI, addr string) Transport {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	roundTripper := &http.Transport{TLSClientConfig: pki.client.Clone(), Protocols: protocols}
	t.Cleanup(roundTripper.CloseIdleConnections)
	return newStreamClient(httpTestOpener{roundTripper: roundTripper, url: "https://" + addr + DefaultHTTPPath}, DefaultMaxFrameSize)
}

func TestTrustedProxyHeaderNamesTheClient(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &remoteAddrRecorder{}
	listener := startRemoteAddrListener(t, pki, recorder, ListenOptions{
		TrustedTCPProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
	})
	target := listener.TCPAddr().String()
	v1 := startProxyForwarder(t, target, []byte("PROXY TCP4 203.0.113.7 198.51.100.1 4242 443\r\n"))
	v2 := startProxyForwarder(t, target, proxyV2Header(0x21, 0x21, proxyV2Addresses(
		netip.MustParseAddrPort("[2001:db8::7]:4343"), netip.MustParseAddrPort("[2001:db8::1]:443"))))
	local := startProxyForwarder(t, target, proxyV2Header(0x20, 0x00, nil))

	qmuxV1, err := newQMuxTestClientAt(t, pki, v1)
	if err != nil {
		t.Fatalf("QMux dial through the proxy: %v", err)
	}
	for _, test := range []struct {
		name      string
		transport Transport
		want      netip.AddrPort
	}{
		{"qmux v1", qmuxV1, netip.MustParseAddrPort("203.0.113.7:4242")},
		{"http2 v2", newHTTP2TestClientAt(t, pki, v2), netip.MustParseAddrPort("[2001:db8::7]:4343")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := callRemoteAddr(t, test.transport); got != test.want {
				t.Errorf("handler saw %v, want %v", got, test.want)
			}
			if authorized := recorder.lastAuthorized(); authorized != test.want {
				t.Errorf("authorizer saw %v, want %v", authorized, test.want)
			}
		})
	}
	if admitted := recorder.lastAdmitted(); admitted != netip.MustParseAddrPort("[2001:db8::7]:4343") {
		t.Errorf("HTTP admission saw %v, want the client the header names", admitted)
	}

	// A LOCAL header, as health checks send, leaves the proxy's own address.
	got := callRemoteAddr(t, newHTTP2TestClientAt(t, pki, local))
	if got.Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("handler saw %v after a LOCAL header, want the proxy's address", got)
	}
	// Native QUIC is not proxied.
	if got := callRemoteAddr(t, dialNative(t, pki, listener)); got.Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("native call saw %v, want the UDP source address", got)
	}
}

func TestTrustedProxyMustSendAHeader(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	diagnostics := make(chan ServerDiagnostic, 16)
	server.SetDiagnostics(func(diagnostic ServerDiagnostic) {
		if diagnostic.Phase == ServerDiagnosticProxyHeaderFailed {
			diagnostics <- diagnostic
		}
	})
	registerTestService(server)
	listener := serveTestListener(t, server, ListenOptions{
		TLSConfig:         pki.server,
		TrustedTCPProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
	})
	if _, err := newQMuxTestClientAt(t, pki, listener.TCPAddr().String()); err == nil {
		t.Fatal("a trusted peer connected without a PROXY header")
	}
	diagnostic := <-diagnostics
	if diagnostic.Err == nil || diagnostic.RemoteAddr == "" {
		t.Errorf("diagnostic %+v lacks the error or the peer", diagnostic)
	}
}

func TestUntrustedPeersAreNotReadForAHeader(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &remoteAddrRecorder{}
	listener := startRemoteAddrListener(t, pki, recorder, ListenOptions{
		TrustedTCPProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	channel, err := newQMuxTestClientAt(t, pki, listener.TCPAddr().String())
	if err != nil {
		t.Fatalf("QMux dial: %v", err)
	}
	if got := callRemoteAddr(t, channel); got.Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("handler saw %v, want the socket's peer", got)
	}
	// A header from an untrusted peer is not believed: it reaches TLS, which
	// fails.
	forwarder := startProxyForwarder(t, listener.TCPAddr().String(), []byte("PROXY TCP4 203.0.113.7 198.51.100.1 4242 443\r\n"))
	if _, err := newQMuxTestClientAt(t, pki, forwarder); err == nil {
		t.Error("a PROXY header from an untrusted peer was accepted")
	}
}

func TestListenRejectsInvalidTrustedTCPProxies(t *testing.T) {
	pki := newTestPKI(t)
	_, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{TLSConfig: pki.server, TrustedTCPProxies: []netip.Prefix{{}}})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("Listen error %v, want InvalidArgument", err)
	}
}
