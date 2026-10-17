package trevrpc

import (
	"context"
	"net"

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
	tr := &quic.Transport{Conn: udp}
	ql, err := tr.Listen(tlsConfig, serverQUICConfig(options.QUICConfig, l.runtime.options))
	if err != nil {
		_ = udp.Close()
		return connSource{}, Unavailable("listen: " + err.Error())
	}
	l.udp, l.tr, l.quic = udp, tr, ql
	if l.runtime.options.EnableHTTP || l.runtime.options.EnableWebTransport {
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
	l.serveStreams(l.ctx, streamAcceptor{
		accept: func(ctx context.Context) (rpcStream, context.Context, error) {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return nil, nil, err
			}
			return newDrainingStream(stream, stream.SetReadDeadline, func() { stream.CancelRead(streamErrorCancelled) }, ctx), stream.Context(), nil
		},
		close:               func(message string) { _ = conn.CloseWithError(0, message) },
		overloadMessage:     "too many concurrent streams on connection",
		drainTimeoutMessage: "server stream drain timed out",
		shutdownMessage:     "server drained connection",
	})
}
