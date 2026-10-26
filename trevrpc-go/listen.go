package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
	// TCPAddr is the address of the TCP listener that serves clients that
	// cannot use UDP. It defaults to the host of the listen address and the
	// port number of the UDP socket.
	TCPAddr string
	// DisableTCP serves QUIC only.
	DisableTCP bool
	// TLSHandshakeTimeout bounds the TLS handshake of TCP connections and the
	// QMux handshake or HTTP request headers that follow it. It defaults to
	// 10 seconds.
	TLSHandshakeTimeout time.Duration
}

// Listener serves a Server on one UDP socket and, unless disabled, one TCP
// socket. The UDP socket always serves native QUIC (ALPN trevrpc/1) and, when
// enabled in ServerOptions, HTTP/3 (ALPN h3) requests and WebTransport
// sessions. The TCP socket always serves native QMux over TLS (ALPN
// qmux-02.trevrpc-1) and, when enabled, HTTP/2 requests and QMux over
// WebSocket sessions. Connections are dispatched on the negotiated ALPN.
type Listener struct {
	server  *Server
	runtime *serverRuntime

	// sources are the transports connections are accepted from. They share
	// the connection limit, the connection registry and shutdown.
	sources []connSource
	conns   connRegistry

	udp  *net.UDPConn
	tr   *quic.Transport
	quic *quic.Listener
	h3   *http3.Server        // nil unless HTTP/3 or WebTransport is enabled
	wt   *webtransport.Server // nil unless WebTransport is enabled

	tcp              net.Listener // nil when TCP is disabled
	tcpTLS           *tls.Config
	handshakeTimeout time.Duration
	httpTCP          *httpTCPServer // nil unless HTTP or WebTransport is enabled

	// ctx is cancelled when the listener begins shutting down. In-flight RPCs
	// observe it through their contexts.
	ctx    context.Context
	cancel context.CancelFunc

	// workers run the RPC streams of every connection.
	workers *streamWorkers

	serving      atomic.Bool
	stopped      atomic.Bool
	stopOnce     sync.Once
	stopErr      error
	teardownOnce sync.Once
}

// connSource is one transport the listener accepts connections from.
type connSource struct {
	// accept blocks until the next connection arrives. It fails once stop has
	// been called.
	accept func(context.Context) (acceptedConn, error)
	// stop stops accepting new connections.
	stop func() error
	// release frees the source's sockets once its connections are closed.
	release func()
}

// acceptedConn is a connection accepted by a connSource.
type acceptedConn struct {
	// serve runs the connection until it ends.
	serve func()
	// close forces the connection closed with a diagnostic message.
	close func(message string)
}

// Listen binds a UDP socket on addr, and a TCP socket unless disabled, and
// prepares to serve server. The server's configuration is frozen. Call Serve to
// start accepting connections.
func Listen(addr string, server *Server, options ListenOptions) (*Listener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}
	if err := server.compressionError(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		server:  server,
		runtime: server.freeze(),
		ctx:     ctx,
		cancel:  cancel,
		workers: newStreamWorkers(ctx.Done(), streamWorkerIdle),
	}
	if err := l.bind(addr, options); err != nil {
		cancel()
		return nil, err
	}
	return l, nil
}

// bindAttempts bounds how often Listen looks for a port number that is free
// for both UDP and TCP when the caller lets the system choose.
const bindAttempts = 8

// bind binds the UDP socket and, unless disabled, the TCP socket. When the
// TCP port defaults to the UDP port and the system chose that port, another
// pair is tried if the TCP port is taken.
func (l *Listener) bind(addr string, options ListenOptions) error {
	_, port, _ := net.SplitHostPort(addr)
	retry := !options.DisableTCP && options.TCPAddr == "" && (port == "0" || port == "")
	for attempt := 1; ; attempt++ {
		quicSource, err := l.listenQUIC(addr, options)
		if err != nil {
			return err
		}
		if options.DisableTCP {
			l.sources = []connSource{quicSource}
			return nil
		}
		tcpSource, err := l.listenTCP(addr, options)
		if err == nil {
			l.sources = []connSource{quicSource, tcpSource}
			return nil
		}
		_ = quicSource.stop()
		quicSource.release()
		if !retry || attempt == bindAttempts || !errors.Is(err, syscall.EADDRINUSE) {
			return Unavailable("listen tcp: " + err.Error())
		}
	}
}

// Addr returns the local UDP address.
func (l *Listener) Addr() net.Addr { return l.udp.LocalAddr() }

// TCPAddr returns the local TCP address, or nil when TCP is disabled.
func (l *Listener) TCPAddr() net.Addr {
	if l.tcp == nil {
		return nil
	}
	return l.tcp.Addr()
}

// Close stops accepting connections. A running Serve then drains in-flight work
// and returns. Closing a listener that is not serving releases its sockets.
func (l *Listener) Close() error {
	err := l.stopAccepting()
	if !l.serving.Load() {
		l.teardown()
	}
	return err
}

// stopAccepting stops every source from accepting new connections and reports
// the first error.
func (l *Listener) stopAccepting() error {
	l.stopOnce.Do(func() {
		l.stopped.Store(true)
		for _, source := range l.sources {
			if err := source.stop(); err != nil && l.stopErr == nil {
				l.stopErr = err
			}
		}
	})
	return l.stopErr
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

	var connectionTasks, acceptLoops sync.WaitGroup
	var acceptErr error
	var acceptErrOnce sync.Once
	for _, source := range l.sources {
		acceptLoops.Go(func() {
			if err := l.acceptConns(ctx, source, &connectionTasks); err != nil {
				acceptErrOnce.Do(func() { acceptErr = err })
				_ = l.stopAccepting()
			}
		})
	}
	acceptLoops.Wait()
	if acceptErr != nil {
		l.shutdown(&connectionTasks, "server accept failed")
		return acceptErr
	}
	l.shutdown(&connectionTasks, "server graceful shutdown timed out")
	return nil
}

// acceptStopping reports whether an accept error comes from the listener
// shutting down rather than from a failure.
func (l *Listener) acceptStopping(ctx context.Context, err error) bool {
	return ctx.Err() != nil || l.stopped.Load() || errors.Is(err, net.ErrClosed)
}

// acceptFailure maps an accept error to the result of acceptConns: nil when the
// listener is shutting down, and the error as a status otherwise.
func (l *Listener) acceptFailure(ctx context.Context, err error) error {
	if l.acceptStopping(ctx, err) {
		return nil
	}
	return transportStatus(err)
}

// acceptConns serves connections from source until it stops accepting. It
// returns an error only when accepting fails for another reason.
func (l *Listener) acceptConns(ctx context.Context, source connSource, connectionTasks *sync.WaitGroup) error {
	connectionLimit := l.runtime.connectionLimit
	for {
		conn, err := source.accept(ctx)
		if err != nil {
			return l.acceptFailure(ctx, err)
		}
		if !tryAcquire(connectionLimit) {
			conn.close("too many concurrent connections")
			continue
		}
		untrack := l.conns.add(conn.close)
		connectionTasks.Go(func() {
			defer release(connectionLimit)
			defer untrack()
			conn.serve()
		})
	}
}

func (l *Listener) shutdown(connectionTasks *sync.WaitGroup, timeoutMessage string) {
	_ = l.stopAccepting()
	l.cancel()
	waitForWaitGroup(connectionTasks, l.runtime.options.GracefulShutdownTimeout, func() {
		l.runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})
		l.conns.closeAll(timeoutMessage)
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
		_ = l.stopAccepting()
		l.conns.closeAll("server closed")
		for _, source := range l.sources {
			source.release()
		}
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
		slot, ok := acquireStreamSlot(streamLimit)
		if !ok {
			writeStatusAndAbort(stream, Unavailable(acceptor.overloadMessage), runtime.options.MaxFrameSize, nil)
			continue
		}

		streamTasks.Add(1)
		l.workers.run(func() {
			defer streamTasks.Done()
			streamCtx, cancel := contextWithAdditionalCancel(streamCtx, ctx)
			defer cancel()
			handleRPCStream(streamCtx, l.server, stream, slot, l.workers)
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
