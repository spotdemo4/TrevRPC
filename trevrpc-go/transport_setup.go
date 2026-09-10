package trevrpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

// ClientTransport is a TrevRPC client transport that can release its underlying connection.
type ClientTransport interface {
	Transport
	Close() error
}

// ServerListener serves a TrevRPC server over a bound transport listener.
type ServerListener interface {
	Addr() net.Addr
	Serve(context.Context) error
	Close() error
}

// ListenOptions configures the built-in native listener.
type ListenOptions struct {
	Transport   TransportConfig
	Limits      TransportLimits
	Credentials *TransportCredentials
}

// DialOptions configures the built-in native dialer.
type DialOptions struct {
	Transport    TransportConfig
	Limits       TransportLimits
	Credentials  *TransportCredentials
	MaxFrameSize int
	OnEvent      func(ChannelEvent)
	WebTransport WebTransportOptions
}

// WebTransportOptions configures native WebTransport targets passed to Dial.
type WebTransportOptions struct {
	RequestHeaders          HeaderFields
	StreamReorderingTimeout time.Duration
}

func cloneWebTransportOptions(options WebTransportOptions) WebTransportOptions {
	options.RequestHeaders = options.RequestHeaders.Clone()
	return options
}

// Listen creates a built-in native transport listener for server and binds it to addr.
func Listen(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}
	return ListenWithBackend(
		addr,
		server,
		options,
		defaultNativeBackend(),
	)
}

// ListenWithBackend creates a listener with an explicit optional backend.
// Backend packages normally wrap this function with their own typed options.
func ListenWithBackend(
	addr string,
	server *Server,
	options ListenOptions,
	backend ListenBackend,
) (ServerListener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}
	if backend == nil {
		return nil, InvalidArgument("listen backend is nil")
	}
	if _, err := resolveExplicitBackend(backend.Backend()); err != nil {
		return nil, err
	}
	runtime := server.freeze()
	serverOptions := backendServerOptions(runtime.options)
	listener, err := backend.Listen(addr, BackendListenOptions{
		Transport:         backendListenTransport(runtime.options, options.Transport),
		Limits:            backendListenLimits(runtime.options, options.Limits),
		Credentials:       cloneTransportCredentials(options.Credentials),
		Server:            serverOptions,
		AdmitHTTP3:        runtime.invokeHTTP3Admission,
		AdmitWebTransport: runtime.invokeWebTransportAdmission,
	})
	if err != nil {
		return nil, err
	}
	if listener == nil {
		return nil, InvalidArgument("listen backend returned a nil listener")
	}
	return &backendServerListener{
		backend:  backend,
		listener: listener,
		server:   server,
		options:  serverOptions,
	}, nil
}

func backendServerOptions(options ServerOptions) BackendServerOptions {
	return BackendServerOptions{
		MaxFrameSize:                      options.MaxFrameSize,
		MaxConcurrentConnections:          options.MaxConcurrentConnections,
		MaxConcurrentStreamsPerConnection: options.MaxConcurrentStreamsPerConnection,
		EnableHTTP3:                       options.EnableHTTP3,
		EnableWebTransport:                options.EnableWebTransport,
		HTTP3Path:                         http3Path(options),
		GracefulShutdownTimeout:           options.GracefulShutdownTimeout,
	}
}

func backendListenTransport(options ServerOptions, override TransportConfig) TransportConfig {
	return mergeTransportConfig(transportConfigFromServerOptions(options), override)
}

func backendListenLimits(options ServerOptions, override TransportLimits) TransportLimits {
	base := transportLimitsFromServerOptions(options)
	limits := TransportLimits{
		StreamReceiveWindow:          base.StreamReceiveWindow,
		ConnectionReceiveWindow:      base.ConnectionReceiveWindow,
		IncomingBidirectionalStreams: base.IncomingBidiStreams,
	}
	if override.StreamReceiveWindow > 0 && override.StreamReceiveWindow < limits.StreamReceiveWindow {
		limits.StreamReceiveWindow = override.StreamReceiveWindow
	}
	if override.ConnectionReceiveWindow > 0 && override.ConnectionReceiveWindow < limits.ConnectionReceiveWindow {
		limits.ConnectionReceiveWindow = override.ConnectionReceiveWindow
	}
	if override.IncomingBidirectionalStreams > 0 && override.IncomingBidirectionalStreams < limits.IncomingBidirectionalStreams {
		limits.IncomingBidirectionalStreams = override.IncomingBidirectionalStreams
	}
	return limits
}

type backendServerListener struct {
	backend  ListenBackend
	listener transportapi.Listener
	server   *Server
	options  BackendServerOptions
}

func (l *backendServerListener) Addr() net.Addr {
	address := l.listener.Address()
	return transportStringAddress{network: address.Network, value: address.String()}
}

func (l *backendServerListener) Serve(ctx context.Context) error {
	lifecycle, _ := l.listener.(interface {
		beginServe() error
		endServe() error
	})
	if lifecycle != nil {
		if err := lifecycle.beginServe(); err != nil {
			return err
		}
	}
	serveErr := serveTransportListener(
		ctx,
		l.listener,
		l.server,
		func(
			connectionCtx context.Context,
			connection transportapi.Connection,
			server *Server,
			requestLimit semaphore,
		) {
			l.backend.ServeConnection(
				connectionCtx,
				connection,
				&backendServerConnection{
					server:       server,
					requestLimit: requestLimit,
					streamLimit: newSemaphore(
						server.freeze().options.MaxConcurrentStreamsPerConnection,
					),
					options: l.options,
				},
			)
		},
		l.backend.ListenerClosed,
		l.backend.MapStatus,
	)
	if lifecycle == nil {
		return serveErr
	}
	endErr := lifecycle.endServe()
	if endErr != nil {
		endErr = l.backend.MapStatus(endErr)
	}
	return errors.Join(serveErr, endErr)
}

func (l *backendServerListener) Close() error {
	return l.listener.Close()
}

type backendServerConnection struct {
	server       *Server
	requestLimit semaphore
	streamLimit  semaphore
	options      BackendServerOptions
}

func (c *backendServerConnection) Options() BackendServerOptions {
	return c.options
}

func (c *backendServerConnection) ServeNativeQUIC(
	ctx context.Context,
	connection transportapi.Connection,
) {
	handleTransportConnection(ctx, connection, c.server, c.requestLimit, true)
}

func (c *backendServerConnection) ServeHTTP3(
	ctx context.Context,
	endpoint transportapi.StreamEndpoint,
) {
	handleTransportStreamEndpoint(
		ctx,
		endpoint,
		c.server,
		c.requestLimit,
		transportStreamEndpointOptions{
			overloadMessage:     "too many concurrent HTTP/3 requests",
			drainTimeoutMessage: "server HTTP/3 request drain timed out",
			closeOnShutdown:     true,
			shutdownMessage:     "server drained HTTP/3 request",
		},
	)
}

func (c *backendServerConnection) HandleHTTP3(request transportapi.HTTP3Request) {
	handleTransportHTTP3RPC(
		request,
		c.server,
		c.requestLimit,
		c.streamLimit,
	)
}

func (c *backendServerConnection) UpgradeWebTransport(
	request transportapi.WebTransportRequest,
) (transportapi.WebTransportSession, bool) {
	if !c.options.EnableWebTransport {
		request.WriteError("WebTransport is disabled", http.StatusNotFound)
		return nil, false
	}
	return handleWebTransportRequest(request, c.server.freeze())
}

func (c *backendServerConnection) ServeWebTransport(
	ctx context.Context,
	session transportapi.WebTransportSession,
) {
	handleWebTransportSession(ctx, session, c.server, c.requestLimit)
}

func (c *backendServerConnection) EmitDiagnostic(diagnostic ServerDiagnostic) {
	c.server.freeze().emitDiagnostic(diagnostic)
}

type transportStringAddress struct {
	network string
	value   string
}

func (a transportStringAddress) Network() string { return a.network }
func (a transportStringAddress) String() string  { return a.value }
