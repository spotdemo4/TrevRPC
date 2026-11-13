package trevrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
)

// proxyV2UDPHeader returns the version 2 header of a datagram over UDP from
// source to destination.
func proxyV2UDPHeader(source, destination netip.AddrPort) []byte {
	family := byte(0x12)
	if source.Addr().Is6() {
		family = 0x22
	}
	return proxyV2Header(0x21, family, proxyV2Addresses(source, destination))
}

func TestParseProxyV2Datagram(t *testing.T) {
	client := netip.MustParseAddrPort("203.0.113.7:4242")
	server := netip.MustParseAddrPort("198.51.100.1:443")
	client6 := netip.MustParseAddrPort("[2001:db8::7]:4343")
	server6 := netip.MustParseAddrPort("[2001:db8::1]:443")
	tlv := []byte{0x04, 0x00, 0x03, 'a', 'b', 'c'} // PP2_TYPE_NOOP
	for _, test := range []struct {
		name   string
		header []byte
		want   netip.AddrPort
		err    bool
	}{
		{"UDP over IPv4", proxyV2UDPHeader(client, server), client, false},
		{"UDP over IPv6", proxyV2UDPHeader(client6, server6), client6, false},
		{"IPv4-mapped", proxyV2UDPHeader(netip.MustParseAddrPort("[::ffff:203.0.113.7]:4242"), server6), client, false},
		{"extensions", proxyV2Header(0x21, 0x12, append(proxyV2Addresses(client, server), tlv...)), client, false},
		{"LOCAL", proxyV2Header(0x20, 0x00, nil), netip.AddrPort{}, false},
		{"TCP family", proxyV2Header(0x21, 0x11, proxyV2Addresses(client, server)), netip.AddrPort{}, false},
		{"no header", []byte{0xc0, 0, 0, 0, 1}, netip.AddrPort{}, true},
		{"version 1", []byte("PROXY TCP4 203.0.113.7 198.51.100.1 4242 443\r\n"), netip.AddrPort{}, true},
		{"short address block", proxyV2Header(0x21, 0x12, make([]byte, 11)), netip.AddrPort{}, true},
		{"unknown command", proxyV2Header(0x22, 0x12, proxyV2Addresses(client, server)), netip.AddrPort{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			datagram := append(append([]byte(nil), test.header...), "quic"...)
			got, length, err := parseProxyV2Datagram(datagram)
			if test.err {
				if err == nil {
					t.Fatalf("parsed %v, want an error", got)
				}
				return
			}
			if err != nil || got != test.want || length != len(test.header) {
				t.Fatalf("got %v, %d, %v; want %v, %d", got, length, err, test.want, len(test.header))
			}
		})
	}
	// The header's length must fit in the datagram.
	truncated := proxyV2UDPHeader(client, server)
	if _, _, err := parseProxyV2Datagram(truncated[:len(truncated)-1]); err == nil {
		t.Error("parsed a truncated header")
	}
}

// newTestProxiedConn returns a proxiedUDPConn without a socket, trusting
// 127.0.0.0/8.
func newTestProxiedConn() *proxiedUDPConn {
	return &proxiedUDPConn{
		trusted: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
		clients: &proxiedClients{clients: make(map[netip.AddrPort]netip.AddrPort)},
		flows:   make(map[netip.AddrPort]*proxiedFlow),
	}
}

func TestProxiedUDPConnStripsHeadersAndNamesClients(t *testing.T) {
	conn := newTestProxiedConn()
	proxy := netip.MustParseAddrPort("127.0.0.1:5000")
	client := netip.MustParseAddrPort("203.0.113.7:4242")
	server := netip.MustParseAddrPort("127.0.0.1:443")
	now := time.Unix(1000, 0)

	datagram := append(proxyV2UDPHeader(client, server), "quic"...)
	if n := conn.strip(datagram, net.UDPAddrFromAddrPort(proxy), &now); string(datagram[:n]) != "quic" {
		t.Fatalf("stripped %q, want the payload", datagram[:n])
	}
	conn.finish(now)
	if got := conn.clients.clientOf(proxy); got != client {
		t.Fatalf("client of %v is %v, want %v", proxy, got, client)
	}
	// Clients behind other proxies, and other addresses, are not affected.
	if other := netip.MustParseAddrPort("127.0.0.1:5001"); conn.clients.clientOf(other) != other {
		t.Error("another address of the proxy has a client")
	}

	// An untrusted peer's datagram is left as it is, header and all.
	untrusted := append(proxyV2UDPHeader(client, server), "quic"...)
	if n := conn.strip(untrusted, net.UDPAddrFromAddrPort(netip.MustParseAddrPort("192.0.2.1:5000")), &now); n != len(untrusted) {
		t.Errorf("untrusted datagram became %d bytes, want %d", n, len(untrusted))
	}

	// A LOCAL header leaves the proxy's own address.
	local := append(proxyV2Header(0x20, 0x00, nil), "quic"...)
	if n := conn.strip(local, net.UDPAddrFromAddrPort(proxy), &now); string(local[:n]) != "quic" {
		t.Fatalf("stripped %q from a LOCAL datagram", local[:n])
	}
	conn.finish(now)
	if got := conn.clients.clientOf(proxy); got != proxy {
		t.Errorf("client of %v after a LOCAL header is %v, want the proxy", proxy, got)
	}

	// A datagram from a trusted proxy without a header is dropped, and
	// reported.
	if n := conn.strip([]byte{0xc0, 0, 0, 0, 1}, net.UDPAddrFromAddrPort(proxy), &now); n != 0 {
		t.Errorf("headerless datagram kept %d bytes", n)
	}
	report := conn.finish(now)
	if report == nil || report.Phase != ServerDiagnosticProxyHeaderFailed || !errors.Is(report.Err, errProxyHeader) || report.RemoteAddr != proxy.String() {
		t.Fatalf("report %+v, want a proxy header failure from %v", report, proxy)
	}
	// Further drops are reported at most once per interval.
	conn.strip([]byte{0xc0}, net.UDPAddrFromAddrPort(proxy), &now)
	if report := conn.finish(now.Add(time.Second)); report != nil {
		t.Errorf("reported %+v again within the interval", report)
	}
	if report := conn.finish(now.Add(proxyDropReportInterval)); report == nil || report.Message != "dropped 1 datagrams from trusted UDP proxies" {
		t.Errorf("report %+v after the interval, want the drop since the last one", report)
	}
}

func TestProxiedUDPConnForgetsQuietAddresses(t *testing.T) {
	conn := newTestProxiedConn()
	server := netip.MustParseAddrPort("127.0.0.1:443")
	quiet := netip.MustParseAddrPort("127.0.0.1:5000")
	busy := netip.MustParseAddrPort("127.0.0.1:5001")
	start := time.Unix(1000, 0)
	send := func(peer netip.AddrPort, client string, at time.Time) {
		t.Helper()
		datagram := append(proxyV2UDPHeader(netip.MustParseAddrPort(client), server), 'q')
		if n := conn.strip(datagram, net.UDPAddrFromAddrPort(peer), &at); n != 1 {
			t.Fatalf("stripped datagram has %d bytes", n)
		}
		conn.finish(at)
	}
	send(quiet, "203.0.113.1:1", start)
	send(busy, "203.0.113.2:2", start)
	for elapsed := proxiedFlowSweepInterval; elapsed <= 3*proxiedFlowSweepInterval; elapsed += proxiedFlowSweepInterval {
		send(busy, "203.0.113.2:2", start.Add(elapsed))
	}
	if got := conn.clients.clientOf(quiet); got != quiet {
		t.Errorf("quiet address still names %v", got)
	}
	if got := conn.clients.clientOf(busy); got != netip.MustParseAddrPort("203.0.113.2:2") {
		t.Errorf("busy address names %v", got)
	}
	if len(conn.flows) != 1 {
		t.Errorf("%d addresses remembered, want 1", len(conn.flows))
	}
	// A client that moves behind the same address is followed.
	send(busy, "203.0.113.3:3", start.Add(4*proxiedFlowSweepInterval))
	if got := conn.clients.clientOf(busy); got != netip.MustParseAddrPort("203.0.113.3:3") {
		t.Errorf("busy address names %v after the client changed", got)
	}
}

func TestProxiedUDPConnBoundsAddresses(t *testing.T) {
	conn := newTestProxiedConn()
	now := time.Unix(1000, 0)
	for i := range maxProxiedFlows {
		conn.flows[netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 1, byte(i >> 8), byte(i)}), uint16(i>>16))] = &proxiedFlow{seen: now}
	}
	datagram := append(proxyV2UDPHeader(netip.MustParseAddrPort("203.0.113.7:1"), netip.MustParseAddrPort("127.0.0.1:443")), 'q')
	if n := conn.strip(datagram, net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:5000")), &now); n != 0 {
		t.Fatal("a datagram from a new address was accepted past the limit")
	}
	if report := conn.finish(now); report == nil || !errors.Is(report.Err, errTooManyProxiedFlows) {
		t.Fatalf("report %+v, want too many addresses", report)
	}
}

// TestProxiedUDPConnReads reads through a real socket with each method
// quic-go may use.
func TestProxiedUDPConnReads(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, udp)
	conn := newProxiedUDPConn(udp, []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, nil)
	sender, err := net.DialUDP("udp", nil, udp.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, sender)
	header := proxyV2UDPHeader(netip.MustParseAddrPort("203.0.113.7:4242"), netip.MustParseAddrPort("127.0.0.1:443"))
	proxied := func(payload string) []byte { return append(append([]byte(nil), header...), payload...) }
	send := func(datagrams ...[]byte) {
		t.Helper()
		for _, datagram := range datagrams {
			if _, err := sender.Write(datagram); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := udp.SetReadDeadline(time.Now().Add(rpcTestTimeout)); err != nil {
		t.Fatal(err)
	}

	// The datagram without a header is dropped from the batch, with no bytes
	// in its place.
	send(proxied("one"), []byte("bad"), proxied("two"))
	messages := make([]ipv4.Message, 3)
	for i := range messages {
		messages[i].Buffers = [][]byte{make([]byte, 1500)}
	}
	var got [][]byte
	for len(got) < 3 {
		n, err := conn.ReadBatch(messages[:3-len(got)], 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range messages[:n] {
			got = append(got, append([]byte(nil), message.Buffers[0][:message.N]...))
		}
	}
	if want := [][]byte{[]byte("one"), {}, []byte("two")}; !equalDatagrams(got, want) {
		t.Errorf("ReadBatch read %q, want %q", got, want)
	}
	peer := addrPortOf(sender.LocalAddr())
	if client := conn.clients.clientOf(peer); client != netip.MustParseAddrPort("203.0.113.7:4242") {
		t.Errorf("client of %v is %v", peer, client)
	}

	// ReadFrom and ReadMsgUDP skip dropped datagrams.
	send([]byte("bad"), proxied("three"), []byte("bad"), proxied("four"))
	buffer := make([]byte, 1500)
	n, addr, err := conn.ReadFrom(buffer)
	if err != nil || string(buffer[:n]) != "three" || addrPortOf(addr) != peer {
		t.Errorf("ReadFrom read %q from %v, %v", buffer[:n], addr, err)
	}
	n, _, _, udpAddr, err := conn.ReadMsgUDP(buffer, make([]byte, 128))
	if err != nil || string(buffer[:n]) != "four" || addrPortOf(udpAddr) != peer {
		t.Errorf("ReadMsgUDP read %q from %v, %v", buffer[:n], udpAddr, err)
	}
}

func equalDatagrams(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// startUDPProxyForwarder forwards the datagrams of each client to target from
// a socket of its own, each preceded by the header header returns for the
// client, as a UDP load balancer speaking PROXY protocol does, and forwards the
// replies back, after showing them to onReply if set. A nil header forwards
// datagrams as they are. It returns its address.
func startUDPProxyForwarder(t *testing.T, target string, header func(client netip.AddrPort) []byte, onReply func([]byte)) string {
	t.Helper()
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	targetAddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu        sync.Mutex
		upstreams = make(map[netip.AddrPort]*net.UDPConn)
		relays    sync.WaitGroup
	)
	t.Cleanup(func() {
		_ = front.Close()
		mu.Lock()
		for _, upstream := range upstreams {
			_ = upstream.Close()
		}
		mu.Unlock()
		relays.Wait()
	})
	relays.Go(func() {
		buffer := make([]byte, 65536)
		for {
			n, client, err := front.ReadFromUDPAddrPort(buffer)
			if err != nil {
				return
			}
			mu.Lock()
			upstream, ok := upstreams[client]
			if !ok {
				upstream, err = net.DialUDP("udp", nil, targetAddr)
				if err != nil {
					mu.Unlock()
					continue
				}
				upstreams[client] = upstream
				relays.Go(func() {
					reply := make([]byte, 65536)
					for {
						n, err := upstream.Read(reply)
						if err != nil {
							return
						}
						if onReply != nil {
							onReply(reply[:n])
						}
						_, _ = front.WriteToUDPAddrPort(reply[:n], client)
					}
				})
			}
			mu.Unlock()
			var datagram []byte
			if header != nil {
				datagram = header(client)
			}
			_, _ = upstream.Write(append(datagram, buffer[:n]...))
		}
	})
	return front.LocalAddr().String()
}

// proxiedTestClient names the client of the datagrams from client as
// 203.0.113.7 with client's port.
func proxiedTestClient(client netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("203.0.113.7"), client.Port())
}

func TestTrustedUDPProxyHeadersNameTheClient(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &remoteAddrRecorder{}
	listener := startRemoteAddrListener(t, pki, recorder, ListenOptions{
		TrustedUDPProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
	})
	server := addrPortOf(listener.Addr())
	proxy := startUDPProxyForwarder(t, listener.Addr().String(), func(client netip.AddrPort) []byte {
		return proxyV2UDPHeader(proxiedTestClient(client), server)
	}, nil)

	native := func() Transport {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
		defer cancel()
		channel, err := Dial(ctx, proxy, DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly})
		if err != nil {
			t.Fatalf("Dial through the proxy: %v", err)
		}
		t.Cleanup(func() { _ = channel.Close() })
		return channel
	}
	webTransport := func() Transport {
		return newStreamClient(dialWebTransportTestOpener(t, pki, listener, webTransportTestDial{origin: testOrigin, addr: proxy}), DefaultMaxFrameSize)
	}
	for _, transport := range []struct {
		name      string
		transport func() Transport
		admitted  bool
	}{
		{"native", native, false},
		{"http3", func() Transport { return newHTTP3TestClientAt(t, pki, proxy) }, true},
		{"webtransport", webTransport, true},
	} {
		t.Run(transport.name, func(t *testing.T) {
			got := callRemoteAddr(t, transport.transport())
			if got.Addr() != netip.MustParseAddr("203.0.113.7") || got.Port() == 0 {
				t.Errorf("handler saw %v, want the client the header names", got)
			}
			if authorized := recorder.lastAuthorized(); authorized != got {
				t.Errorf("authorizer saw %v, handler %v", authorized, got)
			}
			if admitted := recorder.lastAdmitted(); transport.admitted && admitted != got {
				t.Errorf("admission saw %v, handler %v", admitted, got)
			}
		})
	}

	// Without a proxy in between, a trusted address's datagrams lack a header
	// and are dropped. The dial is expected to fail, so a short timeout can
	// only make a slow handshake look dropped, never fail the test wrongly.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if channel, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly}); err == nil {
		_ = channel.Close()
		t.Error("a trusted address connected without PROXY headers")
	}
}

func TestTrustedUDPProxyLocalHeaderLeavesTheProxyAddress(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &remoteAddrRecorder{}
	listener := startRemoteAddrListener(t, pki, recorder, ListenOptions{
		TrustedUDPProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
	})
	proxy := startUDPProxyForwarder(t, listener.Addr().String(), func(netip.AddrPort) []byte {
		return proxyV2Header(0x20, 0x00, nil)
	}, nil)
	if got := callRemoteAddr(t, newHTTP3TestClientAt(t, pki, proxy)); got.Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("handler saw %v after LOCAL headers, want the proxy's address", got)
	}
}

func TestUntrustedUDPPeersAreNotReadForAHeader(t *testing.T) {
	pki := newTestPKI(t)
	recorder := &remoteAddrRecorder{}
	listener := startRemoteAddrListener(t, pki, recorder, ListenOptions{
		TrustedUDPProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if got := callRemoteAddr(t, dialNative(t, pki, listener)); got.Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("handler saw %v, want the UDP source address", got)
	}
	if got := callRemoteAddr(t, newHTTP2TestClient(t, pki, listener)); got.Addr() != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("HTTP/2 handler saw %v, want the TCP peer", got)
	}
	// An untrusted peer's header is not stripped, so QUIC cannot read its
	// datagrams. As in TestTrustedUDPProxyHeadersNameTheClient, the dial is
	// expected to fail, so its timeout is short.
	server := addrPortOf(listener.Addr())
	proxy := startUDPProxyForwarder(t, listener.Addr().String(), func(client netip.AddrPort) []byte {
		return proxyV2UDPHeader(proxiedTestClient(client), server)
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if channel, err := Dial(ctx, proxy, DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly}); err == nil {
		_ = channel.Close()
		t.Error("a PROXY header from an untrusted peer was accepted")
	}
}

func TestListenRejectsInvalidTrustedUDPProxies(t *testing.T) {
	pki := newTestPKI(t)
	_, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{TLSConfig: pki.server, TrustedUDPProxies: []netip.Prefix{{}}})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("Listen error %v, want InvalidArgument", err)
	}
}

// fakeRemote is a connection whose remote address a test sets.
type fakeRemote struct{ addr atomic.Pointer[net.UDPAddr] }

func (f *fakeRemote) RemoteAddr() net.Addr { return f.addr.Load() }

func (f *fakeRemote) set(addr string) {
	f.addr.Store(net.UDPAddrFromAddrPort(netip.MustParseAddrPort(addr)))
}

func TestQUICPeerLooksUpTheClientOnlyWhenItsAddressChanges(t *testing.T) {
	clients := &proxiedClients{clients: map[netip.AddrPort]netip.AddrPort{
		netip.MustParseAddrPort("127.0.0.1:5000"): netip.MustParseAddrPort("203.0.113.1:1"),
		netip.MustParseAddrPort("127.0.0.1:5001"): netip.MustParseAddrPort("203.0.113.2:2"),
	}}
	conn := &fakeRemote{}
	conn.set("127.0.0.1:5000")
	peer := (&Listener{udpClients: clients}).newQUICPeer(conn)
	if got := peer.remoteAddr(); got != netip.MustParseAddrPort("203.0.113.1:1") {
		t.Fatalf("client %v, want the one at the handshake", got)
	}
	// A header forged for the connection's address later does not move it.
	clients.apply([]proxiedFlowChange{{peer: netip.MustParseAddrPort("127.0.0.1:5000"), client: netip.MustParseAddrPort("198.51.100.66:6")}})
	if got := peer.remoteAddr(); got != netip.MustParseAddrPort("203.0.113.1:1") {
		t.Errorf("client %v after a later header, want the one at the handshake", got)
	}
	// A connection that migrates to another address is looked up again.
	conn.set("127.0.0.1:5001")
	if got := peer.remoteAddr(); got != netip.MustParseAddrPort("203.0.113.2:2") {
		t.Errorf("client %v after migrating, want the new address's", got)
	}
	// Without trusted proxies, the remote address is the client's.
	direct := (&Listener{}).newQUICPeer(conn)
	if got := direct.remoteAddr(); got != netip.MustParseAddrPort("127.0.0.1:5001") || direct.proxied() {
		t.Errorf("client %v without proxies, want the remote address", got)
	}
}

func TestRetryLimiter(t *testing.T) {
	now := time.Unix(1000, 0)
	limiter := newRetryLimiter(2, func() time.Time { return now })
	got := []bool{limiter.validate(nil), limiter.validate(nil), limiter.validate(nil)}
	if want := []bool{false, false, true}; !slices.Equal(got, want) {
		t.Fatalf("a burst validated %v, want %v", got, want)
	}
	now = now.Add(500 * time.Millisecond)
	if limiter.validate(nil) || !limiter.validate(nil) {
		t.Error("half a second did not let exactly one attempt through")
	}
	// Quiet time refills up to a second's worth, not more.
	now = now.Add(time.Hour)
	got = []bool{limiter.validate(nil), limiter.validate(nil), limiter.validate(nil)}
	if want := []bool{false, false, true}; !slices.Equal(got, want) {
		t.Errorf("after a quiet hour validated %v, want %v", got, want)
	}

	if addressValidator(DefaultServerOptions()) != nil {
		t.Error("addresses are validated by default")
	}
	if validate := addressValidator(ServerOptions{ValidateAllAddresses: true, AddressValidationRate: 5}); validate == nil || !validate(nil) {
		t.Error("ValidateAllAddresses does not validate every attempt")
	}
}

// isRetry reports whether datagram is a QUIC version 1 Retry packet.
func isRetry(datagram []byte) bool {
	return len(datagram) > 5 && datagram[0]&0xb0 == 0xb0 && binary.BigEndian.Uint32(datagram[1:5]) == 1
}

// callEcho makes one unary call on transport.
func callEcho(t *testing.T, transport Transport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	response, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "hi"}, newTestMessage)
	if err != nil || response.Value != "hi" {
		t.Fatalf("Echo: %v, %v", response, err)
	}
}

func TestAddressValidationSendsRetry(t *testing.T) {
	pki := newTestPKI(t)
	for _, test := range []struct {
		name    string
		options func(*ServerOptions)
		retries bool
	}{
		{"default", func(*ServerOptions) {}, false},
		{"all", func(options *ServerOptions) { options.ValidateAllAddresses = true }, true},
		// The first connection fits the burst; the others do not.
		{"above rate", func(options *ServerOptions) { options.AddressValidationRate = 1 }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			options := server.Options()
			options.EnableHTTP = true
			test.options(&options)
			server.SetOptions(options)
			registerTestService(server)
			listener := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server})
			var retries atomic.Int64
			relay := startUDPProxyForwarder(t, listener.Addr().String(), nil, func(reply []byte) {
				if isRetry(reply) {
					retries.Add(1)
				}
			})
			for range 2 {
				ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
				channel, err := Dial(ctx, relay, DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly})
				cancel()
				if err != nil {
					t.Fatalf("Dial: %v", err)
				}
				callEcho(t, channel)
				_ = channel.Close()
			}
			callEcho(t, newHTTP3TestClientAt(t, pki, relay))
			if got := retries.Load() > 0; got != test.retries {
				t.Errorf("%d Retry packets, want some: %v", retries.Load(), test.retries)
			}
		})
	}
}
