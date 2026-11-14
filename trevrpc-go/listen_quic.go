package trevrpc

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// listenQUIC binds the UDP socket that serves native QUIC, HTTP/3 and
// WebTransport, and returns it as a connection source.
func (l *Listener) listenQUIC(addr string, options ListenOptions) (connSource, error) {
	tlsConfig, err := serverTLSConfig(options.TLSConfig, l.runtime.options)
	if err != nil {
		return connSource{}, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return connSource{}, InvalidArgument("invalid listen address: " + err.Error())
	}
	udp, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return connSource{}, Unavailable("listen: " + err.Error())
	}
	var conn net.PacketConn = udp
	l.udpClients = nil
	if len(l.trustedUDPProxies) > 0 {
		proxied := newProxiedUDPConn(udp, l.trustedUDPProxies, l.runtime.emitDiagnostic)
		conn, l.udpClients = proxied, proxied.clients
	}
	tr := &quic.Transport{Conn: conn, VerifySourceAddress: addressValidator(l.runtime.options)}
	ql, err := tr.Listen(tlsConfig, serverQUICConfig(options.QUICConfig, l.runtime.options))
	if err != nil {
		_ = udp.Close()
		return connSource{}, Unavailable("listen: " + err.Error())
	}
	l.udp, l.tr, l.quic = udp, tr, ql
	if l.runtime.options.servesHTTP() {
		l.setupHTTP3()
	}

	return connSource{
		accept: func(ctx context.Context) (acceptedConn, error) {
			conn, err := ql.Accept(ctx)
			if err != nil {
				return acceptedConn{}, err
			}
			return acceptedConn{
				serve: func() { l.serveQUICConn(conn) },
				close: func(message string) { _ = conn.CloseWithError(0, message) },
			}, nil
		},
		stop: ql.Close,
		release: func() {
			l.closeHTTP3()
			_ = tr.Close()
			_ = udp.Close()
		},
	}, nil
}

// serveQUICConn dispatches one established QUIC connection on its negotiated
// ALPN.
func (l *Listener) serveQUICConn(conn *quic.Conn) {
	switch conn.ConnectionState().TLS.NegotiatedProtocol {
	case ALPN:
		l.serveNative(conn)
	case http3.NextProtoH3:
		l.serveHTTP3Conn(conn)
	default:
		_ = conn.CloseWithError(0, "unsupported ALPN")
	}
}

func (l *Listener) serveNative(conn *quic.Conn) {
	peer := l.newQUICPeer(conn)
	datagrams := l.newDatagramMux(conn, 0)
	defer datagrams.close()
	connectionMetadata := new(connectionMetadataStore)
	l.serveStreams(l.ctx, streamAcceptor{
		accept: func(ctx context.Context) (rpcStream, context.Context, error) {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return nil, nil, err
			}
			draining := newDrainingStream(ctx, stream, stream.SetReadDeadline, func() { stream.CancelRead(streamErrorCancelled) })
			draining.datagrams = datagrams
			draining.connectionMetadata = connectionMetadata
			// Read per stream: the client may have migrated to a new address.
			draining.remoteAddr = peer.remoteAddr()
			return draining, stream.Context(), nil
		},
		close:               func(message string) { _ = conn.CloseWithError(0, message) },
		overloadMessage:     "too many concurrent streams on connection",
		drainTimeoutMessage: "server stream drain timed out",
		shutdownMessage:     "server drained connection",
	})
}

// newDatagramMux returns the demultiplexer of the datagram messages of one
// connection or session, whose transport adds overhead bytes to every
// datagram. It remembers twice as many ended flows as the connection has
// streams.
func (l *Listener) newDatagramMux(conn datagramConn, overhead int) *datagramMux {
	mux := newDatagramMux(conn, overhead, true, 2*l.runtime.options.MaxConcurrentStreamsPerConnection)
	if wait := l.datagramCountWait.Load(); wait > 0 {
		mux.countWait = time.Duration(wait)
	}
	return mux
}

// quicPeer resolves the client address of one QUIC connection.
type quicPeer struct {
	conn interface{ RemoteAddr() net.Addr }
	// clients names the clients of trusted UDP proxies, or is nil when there
	// are none.
	clients *proxiedClients
	last    atomic.Pointer[quicPeerAddrs]
}

// quicPeerAddrs is a connection's remote address and the client it stands
// for.
type quicPeerAddrs struct {
	peer, client netip.AddrPort
}

// newQUICPeer resolves the client address of conn, which has completed its
// handshake.
func (l *Listener) newQUICPeer(conn interface{ RemoteAddr() net.Addr }) *quicPeer {
	peer := &quicPeer{conn: conn, clients: l.udpClients}
	peer.remoteAddr()
	return peer
}

// proxied reports whether p is behind trusted UDP proxies.
func (p *quicPeer) proxied() bool {
	return p != nil && p.clients != nil
}

// remoteAddr returns the client address of the connection: the client the
// PROXY headers of a trusted UDP proxy at its remote address name, or that
// address itself. The client is looked up when the connection has completed
// its handshake, and again only when its remote address changes, so a header
// forged later in the name of the proxy does not move a connection to another
// client.
func (p *quicPeer) remoteAddr() netip.AddrPort {
	peer := addrPortOf(p.conn.RemoteAddr())
	if p.clients == nil {
		return peer
	}
	if last := p.last.Load(); last != nil && last.peer == peer {
		return last.client
	}
	client := p.clients.clientOf(peer)
	p.last.Store(&quicPeerAddrs{peer: peer, client: client})
	return client
}
