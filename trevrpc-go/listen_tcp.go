package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"time"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

const (
	defaultTLSHandshakeTimeout = 10 * time.Second
	// http2ALPN and http1ALPN are the TLS application protocols of HTTP/2 and
	// HTTP/1.1, which carries WebSocket upgrades.
	http2ALPN = "h2"
	http1ALPN = "http/1.1"
	// Accept failures that are not caused by closing the listener, such as
	// running out of file descriptors, are retried with backoff.
	minAcceptBackoff = 5 * time.Millisecond
	maxAcceptBackoff = time.Second
)

// listenTCP binds the TCP socket that serves clients that cannot use UDP. It
// must be called after listenQUIC.
func (l *Listener) listenTCP(addr string, options ListenOptions) (connSource, error) {
	tlsConfig, err := serverTCPTLSConfig(options.TLSConfig, l.runtime.options)
	if err != nil {
		return connSource{}, err
	}
	tcpAddr := options.TCPAddr
	if tcpAddr == "" {
		host, _, _ := net.SplitHostPort(addr)
		tcpAddr = net.JoinHostPort(host, strconv.Itoa(l.udp.LocalAddr().(*net.UDPAddr).Port))
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(l.ctx, "tcp", tcpAddr)
	if err != nil {
		return connSource{}, err
	}
	l.tcp, l.tcpTLS = ln, tlsConfig
	l.handshakeTimeout = options.TLSHandshakeTimeout
	if l.handshakeTimeout <= 0 {
		l.handshakeTimeout = defaultTLSHandshakeTimeout
	}
	if l.runtime.options.servesHTTP() {
		l.httpTCP = newHTTPTCPServer(l)
	}

	return connSource{
		accept: func(ctx context.Context) (acceptedConn, error) {
			conn, err := acceptWithBackoff(ctx, ln)
			if err != nil {
				return acceptedConn{}, err
			}
			return acceptedConn{
				serve: func() { l.serveTCPConn(conn) },
				close: func(string) { _ = conn.Close() },
			}, nil
		},
		stop: func() error {
			err := ln.Close()
			if l.httpTCP != nil {
				l.httpTCP.shutdown(l.runtime.options.GracefulShutdownTimeout)
			}
			return err
		},
		release: func() {
			_ = ln.Close()
			if l.httpTCP != nil {
				l.httpTCP.close()
			}
		},
	}, nil
}

// acceptWithBackoff accepts the next connection, retrying failures that do
// not come from closing the listener.
func acceptWithBackoff(ctx context.Context, ln net.Listener) (net.Conn, error) {
	backoff := minAcceptBackoff
	for {
		conn, err := ln.Accept()
		if err == nil {
			return conn, nil
		}
		if errors.Is(err, net.ErrClosed) {
			return nil, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, maxAcceptBackoff)
	}
}

// serveTCPConn completes the TLS handshake of one TCP connection and serves it
// on its negotiated ALPN until it ends.
func (l *Listener) serveTCPConn(raw net.Conn) {
	conn := tls.Server(raw, l.tcpTLS)
	ctx, cancel := context.WithTimeout(l.ctx, l.handshakeTimeout)
	defer cancel()
	if err := conn.HandshakeContext(ctx); err != nil {
		l.runtime.emitDiagnostic(ServerDiagnostic{
			Phase:      ServerDiagnosticTLSHandshakeFailed,
			Err:        err,
			RemoteAddr: raw.RemoteAddr().String(),
		})
		_ = raw.Close()
		return
	}
	switch proto := conn.ConnectionState().NegotiatedProtocol; {
	case proto == QMuxALPN:
		session, err := l.openServerQMux(ctx, qmux.NewStreamConn(conn, qmuxMaxRecordSize))
		if err != nil {
			_ = conn.Close()
			return
		}
		cancel()
		l.serveQMuxSession(session, nil)
	case l.httpTCP != nil && (proto == http2ALPN || proto == http1ALPN || proto == ""):
		cancel()
		l.httpTCP.serveConn(conn)
	default:
		_ = conn.Close()
	}
}

// serverTCPTLSConfig clones base and restricts it to the ALPN protocols the TCP
// listener serves.
func serverTCPTLSConfig(base *tls.Config, options ServerOptions) (*tls.Config, error) {
	config, err := serverTLSConfig(base, options)
	if err != nil {
		return nil, err
	}
	config.NextProtos = []string{QMuxALPN}
	if options.servesHTTP2() {
		config.NextProtos = append(config.NextProtos, http2ALPN)
	}
	if options.servesHTTP1() {
		config.NextProtos = append(config.NextProtos, http1ALPN)
	}
	return config, nil
}
