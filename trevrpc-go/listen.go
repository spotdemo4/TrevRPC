package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// ListenOptions configures Listen.
type ListenOptions struct {
	// TLSConfig supplies the server certificate. It is cloned; NextProtos is
	// always set to the protocols the Server enables.
	TLSConfig *tls.Config
	// QUICConfig optionally tunes the QUIC connections. It is cloned. Flow
	// control windows and stream limits are capped by the Server's limits, and
	// 0-RTT is always disabled.
	QUICConfig *quic.Config
}

// Listener serves a Server on one UDP socket. Native QUIC (ALPN trevrpc/1) is
// always served. When enabled in ServerOptions, HTTP/3 (ALPN h3) requests and
// WebTransport sessions are served on the same socket, dispatched by the
// negotiated ALPN protocol.
type Listener struct {
	server  *Server
	runtime *serverRuntime

	udp  *net.UDPConn
	tr   *quic.Transport
	quic *quic.Listener
	h3   *http3.Server        // nil unless HTTP/3 or WebTransport is enabled
	wt   *webtransport.Server // nil unless WebTransport is enabled

	// ctx is cancelled when the listener begins shutting down. In-flight RPCs
	// observe it through their contexts.
	ctx    context.Context
	cancel context.CancelFunc

	serving      atomic.Bool
	closeOnce    sync.Once
	closeErr     error
	teardownOnce sync.Once

	connMu sync.Mutex
	conns  map[*quic.Conn]struct{}
}

// Listen binds a UDP socket on addr and prepares to serve server. The server's
// configuration is frozen. Call Serve to start accepting connections.
func Listen(addr string, server *Server, options ListenOptions) (*Listener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}
	runtime := server.freeze()
	tlsConfig, err := serverTLSConfig(options.TLSConfig, runtime.options)
	if err != nil {
		return nil, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, InvalidArgument("invalid listen address: " + err.Error())
	}
	udp, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, Unavailable("listen: " + err.Error())
	}
	tr := &quic.Transport{Conn: udp}
	ql, err := tr.Listen(tlsConfig, serverQUICConfig(options.QUICConfig, runtime.options))
	if err != nil {
		_ = udp.Close()
		return nil, Unavailable("listen: " + err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		server:  server,
		runtime: runtime,
		udp:     udp,
		tr:      tr,
		quic:    ql,
		ctx:     ctx,
		cancel:  cancel,
		conns:   map[*quic.Conn]struct{}{},
	}
	if runtime.options.EnableHTTP3 || runtime.options.EnableWebTransport {
		l.setupHTTP3()
	}
	return l, nil
}

// Addr returns the local UDP address.
func (l *Listener) Addr() net.Addr { return l.udp.LocalAddr() }

// Close stops accepting connections. A running Serve then drains in-flight work
// and returns. Closing a listener that is not serving releases its socket.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() { l.closeErr = l.quic.Close() })
	if !l.serving.Load() {
		l.teardown()
	}
	return l.closeErr
}

// Serve accepts connections until ctx is cancelled or the listener is closed,
// then cancels in-flight RPCs and waits up to GracefulShutdownTimeout for
// handlers to finish. It returns nil after a requested shutdown.
func (l *Listener) Serve(ctx context.Context) error {
	if !l.serving.CompareAndSwap(false, true) {
		return FailedPrecondition("listener is already serving")
	}
	defer l.teardown()
	stopOnCancel := context.AfterFunc(ctx, func() { _ = l.Close() })
	defer stopOnCancel()

	connectionLimit := l.runtime.connectionLimit
	var connectionTasks sync.WaitGroup
	for {
		conn, err := l.quic.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			l.shutdown(&connectionTasks, "server accept failed")
			return transportStatus(err)
		}
		if !tryAcquire(connectionLimit) {
			_ = conn.CloseWithError(0, "too many concurrent connections")
			continue
		}
		l.track(conn)
		connectionTasks.Go(func() {
			defer release(connectionLimit)
			defer l.untrack(conn)
			l.serveConn(conn)
		})
	}
	l.shutdown(&connectionTasks, "server graceful shutdown timed out")
	return nil
}

func (l *Listener) shutdown(connectionTasks *sync.WaitGroup, timeoutMessage string) {
	_ = l.quic.Close()
	l.cancel()
	waitForWaitGroup(connectionTasks, l.runtime.options.GracefulShutdownTimeout, func() {
		l.runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})
		l.closeConns(timeoutMessage)
	})
	l.teardown()
	if !l.runtime.waitForExecutions(l.runtime.options.GracefulShutdownTimeout) {
		l.runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})
	}
}

// teardown releases everything the listener owns. It is safe to call repeatedly.
func (l *Listener) teardown() {
	l.teardownOnce.Do(func() {
		l.cancel()
		_ = l.quic.Close()
		l.closeConns("server closed")
		l.closeHTTP3()
		_ = l.tr.Close()
		_ = l.udp.Close()
	})
}

func (l *Listener) track(conn *quic.Conn) {
	l.connMu.Lock()
	l.conns[conn] = struct{}{}
	l.connMu.Unlock()
}

func (l *Listener) untrack(conn *quic.Conn) {
	l.connMu.Lock()
	delete(l.conns, conn)
	l.connMu.Unlock()
}

func (l *Listener) closeConns(message string) {
	l.connMu.Lock()
	conns := make([]*quic.Conn, 0, len(l.conns))
	for conn := range l.conns {
		conns = append(conns, conn)
	}
	l.connMu.Unlock()
	for _, conn := range conns {
		_ = conn.CloseWithError(0, message)
	}
}

// serveConn dispatches one established connection on its negotiated ALPN.
func (l *Listener) serveConn(conn *quic.Conn) {
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

// streamAcceptor abstracts a connection or WebTransport session that yields
// one bidirectional stream per RPC.
type streamAcceptor struct {
	// accept returns the next RPC stream and the context bound to its lifetime.
	accept func(context.Context) (rpcStream, context.Context, error)
	// close ends the connection or session with a diagnostic message.
	close               func(message string)
	overloadMessage     string
	drainTimeoutMessage string
	shutdownMessage     string
	onAcceptError       func(error)
}

// serveStreams runs RPCs from acceptor until accepting fails or ctx is
// cancelled, then waits for in-flight RPCs to unwind.
func (l *Listener) serveStreams(ctx context.Context, acceptor streamAcceptor) {
	runtime := l.runtime
	streamLimit := newSemaphore(runtime.options.MaxConcurrentStreamsPerConnection)
	var streamTasks sync.WaitGroup

	for {
		stream, streamCtx, err := acceptor.accept(ctx)
		if err != nil {
			if acceptor.onAcceptError != nil {
				acceptor.onAcceptError(err)
			}
			break
		}
		if !tryAcquire(streamLimit) {
			writeStatusResponse(stream, Unavailable(acceptor.overloadMessage), runtime.options.MaxFrameSize)
			continue
		}

		streamTasks.Go(func() {
			defer release(streamLimit)
			streamCtx, cancel := contextWithAdditionalCancel(streamCtx, ctx)
			defer cancel()
			handleRPCStream(streamCtx, l.server, stream)
		})
	}

	waitForWaitGroup(&streamTasks, runtime.options.GracefulShutdownTimeout, func() {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})
		acceptor.close(acceptor.drainTimeoutMessage)
	})
	if ctx.Err() != nil {
		acceptor.close(acceptor.shutdownMessage)
	}
}

func contextWithAdditionalCancel(ctx context.Context, cancelOn context.Context) (context.Context, context.CancelFunc) {
	if cancelOn.Done() == nil {
		return ctx, func() {}
	}

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(cancelOn, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
