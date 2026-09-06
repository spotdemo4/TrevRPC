package trevrpc

import (
	"context"
	"errors"
	"math/bits"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
)

const (
	nativeStatusInvalidArgument   = -22
	nativeStatusNotSupportedBSD   = -45
	nativeStatusNotSupportedLinux = -95
)

func newNativeChannelConnector(
	target string,
	options DialOptions,
) (channelConnector, error) {
	if strings.HasPrefix(target, "https://") {
		return newNativeWebTransportConnector(target, options)
	}
	return newNativeEngineQUICConnector(target, options)
}

type nativeEngineQUICConnector struct {
	endpoint         native.EndpointConfig
	engineConfig     native.EngineConfig
	credentials      *TransportCredentials
	maxFrameSize     int
	requestedBackend TransportBackend
}

func newNativeEngineQUICConnector(
	target string,
	options DialOptions,
) (*nativeEngineQUICConnector, error) {
	if options.TLSConfig != nil || options.QUICConfig != nil || len(options.WebTransport.RequestHeader) != 0 {
		return nil, InvalidArgument("native dial does not accept legacy TLSConfig, QUICConfig, or RequestHeader")
	}
	if !native.Available() {
		return nil, nativeBackendUnavailable()
	}
	if strings.HasPrefix(target, "https://") {
		return nil, Unimplemented("native WebTransport dialing is not implemented yet")
	}
	if scheme, _, ok := strings.Cut(target, "://"); ok {
		return nil, InvalidArgument("unsupported dial target scheme " + scheme)
	}
	if len(options.WebTransport.RequestHeaders) != 0 ||
		len(options.WebTransport.ApplicationProtocols) != 0 ||
		options.WebTransport.StreamReorderingTimeout != 0 {
		return nil, InvalidArgument("native QUIC dial does not accept WebTransport options")
	}

	host, port, err := splitNativeAddress(target, false)
	if err != nil {
		return nil, err
	}
	credentials := cloneTransportCredentials(options.Credentials)
	if err := validateNativeClientCredentials(credentials, host); err != nil {
		return nil, err
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	if uint64(maxFrameSize) > uint64(^uint32(0))-transportFrameHeaderSize {
		return nil, InvalidArgument("maximum frame size exceeds native Engine capacity")
	}
	endpoint, err := nativeClientEndpointConfig(
		host,
		port,
		maxFrameSize,
		options.Transport,
		options.Limits,
	)
	if err != nil {
		return nil, err
	}
	engineConfig := native.DefaultEngineConfig()
	engineConfig.ListenerCapacity = 1
	engineConfig.ConnectionCapacity = 1
	if engineConfig.MaxReceiveOwnedBytes < endpoint.MaxFrameSize {
		engineConfig.MaxReceiveOwnedBytes = endpoint.MaxFrameSize
	}
	return &nativeEngineQUICConnector{
		endpoint:         endpoint,
		engineConfig:     engineConfig,
		credentials:      credentials,
		maxFrameSize:     maxFrameSize,
		requestedBackend: options.Backend,
	}, nil
}

func (c *nativeEngineQUICConnector) Connect(
	ctx context.Context,
) (channelGeneration, error) {
	engine, err := native.NewEngine(c.engineConfig)
	if err != nil {
		return nil, nativeTransportOrContextStatus(ctx, err)
	}
	return connectNativeGeneration(
		ctx,
		engine,
		c.endpoint,
		c.credentials,
		c.maxFrameSize,
		c.requestedBackend,
	)
}

type nativeWebTransportConnector struct {
	endpoint         native.EndpointConfig
	engineConfig     native.EngineConfig
	credentials      *TransportCredentials
	maxFrameSize     int
	requestedBackend TransportBackend
}

func newNativeWebTransportConnector(
	target string,
	options DialOptions,
) (*nativeWebTransportConnector, error) {
	if options.TLSConfig != nil || options.QUICConfig != nil || len(options.WebTransport.RequestHeader) != 0 {
		return nil, InvalidArgument("native WebTransport dial does not accept legacy TLSConfig, QUICConfig, or RequestHeader")
	}
	if !native.Available() {
		return nil, nativeBackendUnavailable()
	}
	if len(options.WebTransport.ApplicationProtocols) != 0 {
		return nil, InvalidArgument("native WebTransport dial does not support application protocols")
	}
	origin, err := nativeWebTransportOrigin(options.WebTransport.RequestHeaders)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, InvalidArgument("native WebTransport target must be an https URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, InvalidArgument("native WebTransport target must not contain user information, a query, or a fragment")
	}
	host := parsed.Hostname()
	port := uint16(443)
	if parsed.Port() != "" {
		value, parseErr := strconv.ParseUint(parsed.Port(), 10, 16)
		if parseErr != nil || value == 0 {
			return nil, InvalidArgument("native WebTransport target port is invalid")
		}
		port = uint16(value)
	}
	path := parsed.EscapedPath()
	if path == "" {
		path = DefaultHTTP3Path
	}
	credentials := cloneTransportCredentials(options.Credentials)
	if err := validateNativeTransportClientCredentials(credentials); err != nil {
		return nil, err
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	if uint64(maxFrameSize) > uint64(^uint32(0))-transportFrameHeaderSize {
		return nil, InvalidArgument("maximum frame size exceeds native Transport capacity")
	}
	endpoint, err := nativeClientEndpointConfig(
		host,
		port,
		maxFrameSize,
		options.Transport,
		options.Limits,
	)
	if err != nil {
		return nil, err
	}
	endpoint.Protocol = native.ProtocolWebTransport
	endpoint.ALPN = nil
	endpoint.Path = path
	endpoint.Origin = origin
	if credentials != nil && credentials.ServerName != "" {
		endpoint.ServerName = credentials.ServerName
	}
	if options.WebTransport.StreamReorderingTimeout != 0 {
		endpoint.UnresolvedStreamTimeoutMS, err = nativeMilliseconds64(
			"WebTransport stream reordering timeout",
			options.WebTransport.StreamReorderingTimeout,
		)
		if err != nil {
			return nil, err
		}
	}
	engineConfig := native.DefaultEngineConfig()
	engineConfig.ListenerCapacity = 1
	engineConfig.ConnectionCapacity = 1
	if engineConfig.MaxReceiveOwnedBytes < endpoint.MaxFrameSize {
		engineConfig.MaxReceiveOwnedBytes = endpoint.MaxFrameSize
	}
	return &nativeWebTransportConnector{
		endpoint:         endpoint,
		engineConfig:     engineConfig,
		credentials:      credentials,
		maxFrameSize:     maxFrameSize,
		requestedBackend: options.Backend,
	}, nil
}

func (c *nativeWebTransportConnector) Connect(
	ctx context.Context,
) (channelGeneration, error) {
	engine, err := native.NewTransport(c.engineConfig)
	if err != nil {
		return nil, nativeTransportOrContextStatus(ctx, err)
	}
	return connectNativeGeneration(
		ctx,
		engine,
		c.endpoint,
		c.credentials,
		c.maxFrameSize,
		c.requestedBackend,
	)
}

func connectNativeGeneration(
	ctx context.Context,
	engine *native.Engine,
	endpoint native.EndpointConfig,
	credentials *TransportCredentials,
	maxFrameSize int,
	requestedBackend TransportBackend,
) (channelGeneration, error) {
	endpoint, cleanup, err := materializeNativeCredentials(
		endpoint,
		credentials,
		false,
	)
	if err != nil {
		_ = engine.Close()
		return nil, err
	}
	connection, dialErr := engine.Dial(ctx, endpoint)
	cleanupErr := cleanup()
	if dialErr != nil || cleanupErr != nil {
		_ = engine.Close()
		if dialErr != nil {
			return nil, nativeTransportOrContextStatus(ctx, dialErr)
		}
		return nil, Internal("remove native credential files: " + cleanupErr.Error())
	}

	return &nativeEngineQUICGeneration{
		engine:           engine,
		connection:       connection,
		requestedBackend: requestedBackend,
		client: newTransportStreamClient(
			connection,
			maxFrameSize,
			nativeTransportOrContextStatus,
		),
	}, nil
}

func nativeWebTransportOrigin(headers HeaderFields) (string, error) {
	if err := validateHeaderFields(headers); err != nil {
		return "", err
	}
	var origin string
	for _, header := range headers {
		if !strings.EqualFold(header.Name, "Origin") {
			return "", InvalidArgument("native WebTransport dial supports only the Origin request header")
		}
		if origin != "" {
			return "", InvalidArgument("native WebTransport dial accepts at most one Origin request header")
		}
		origin = header.Value
	}
	return origin, nil
}

type nativeEngineQUICGeneration struct {
	engine           *native.Engine
	connection       *native.Connection
	client           *transportStreamClient
	requestedBackend TransportBackend
	closeOnce        sync.Once
	closeErr         error
}

func (g *nativeEngineQUICGeneration) Call(
	ctx context.Context,
	request *RpcRequest,
) (*RpcResponse, error) {
	return g.client.Call(ctx, request)
}

func (g *nativeEngineQUICGeneration) StreamingCall(
	ctx context.Context,
	request *RpcRequest,
	requestBody ByteStream,
) (FrameStream, error) {
	return g.client.StreamingCall(ctx, request, requestBody)
}

func (g *nativeEngineQUICGeneration) Close() error {
	if g == nil {
		return nil
	}
	g.closeOnce.Do(func() {
		var connectionErr error
		select {
		case <-g.connection.Done():
		default:
			connectionErr = g.connection.Close(TransportCloseReason{
				Local:   true,
				Clean:   true,
				Message: "client closed",
			})
		}
		g.closeErr = errors.Join(connectionErr, g.engine.Close())
	})
	return g.closeErr
}

func (g *nativeEngineQUICGeneration) Done() <-chan struct{} {
	return g.connection.Done()
}

func (g *nativeEngineQUICGeneration) Err() error {
	return g.connection.Err()
}

func (g *nativeEngineQUICGeneration) Info() ConnectionInfo {
	info := g.connection.Info()
	info.RequestedBackend = g.requestedBackend
	return info
}

func (g *nativeEngineQUICGeneration) CloseReason(err error) TransportCloseReason {
	return nativeCloseReason(err)
}

func (g *nativeEngineQUICGeneration) releaseDisconnected() {
	_ = g.Close()
}

func nativeServerAdmissionHandler(server *Server) native.AdmissionHandler {
	runtime := server.freeze()
	return func(request native.AdmissionRequest) uint16 {
		headers := make(HeaderFields, len(request.Headers))
		for index, header := range request.Headers {
			headers[index] = HeaderField{Name: header.Name, Value: header.Value}
		}
		switch request.Kind {
		case native.AdmissionHTTP3:
			if !runtime.options.EnableHTTP3 || request.Path != http3Path(runtime.options) {
				return http.StatusNotFound
			}
			if request.Method != http.MethodPost {
				return http.StatusMethodNotAllowed
			}
			if !isTrevRPCMediaType(headerFieldValues(headers, "Content-Type")) {
				return http.StatusUnsupportedMediaType
			}
			admitted, err := runtime.invokeHTTP3Admission(HTTP3AdmissionRequest{
				Headers:   headers,
				Path:      request.Path,
				Method:    request.Method,
				Authority: request.Authority,
				Secure:    request.Secure,
			})
			if err != nil {
				if errors.Is(err, errAdmissionSaturated) {
					return http.StatusServiceUnavailable
				}
				return http.StatusInternalServerError
			}
			if !admitted {
				return http.StatusForbidden
			}
			return http.StatusOK
		case native.AdmissionWebTransport:
			if !runtime.options.EnableWebTransport || request.Path != http3Path(runtime.options) {
				return http.StatusNotFound
			}
			admitted, err := runtime.invokeWebTransportAdmission(WebTransportAdmissionRequest{
				Headers:   headers,
				Path:      request.Path,
				Authority: request.Authority,
				Origin:    request.Origin,
				Secure:    request.Secure,
			})
			if err != nil {
				if errors.Is(err, errAdmissionSaturated) {
					return http.StatusServiceUnavailable
				}
				return http.StatusInternalServerError
			}
			if !admitted {
				return http.StatusForbidden
			}
			return http.StatusOK
		default:
			return http.StatusInternalServerError
		}
	}
}

type nativeQUICServerListener struct {
	engine   *native.Engine
	listener *native.Listener
	server   *Server

	mu        sync.Mutex
	serving   bool
	closeOnce sync.Once
	closeErr  error
}

func newNativeQUICServerListener(
	addr string,
	server *Server,
	options ListenOptions,
) (ServerListener, error) {
	if options.TLSConfig != nil || options.QUICConfig != nil {
		return nil, InvalidArgument("native listener does not accept legacy TLSConfig or QUICConfig")
	}
	if !native.Available() {
		return nil, nativeBackendUnavailable()
	}
	credentials := cloneTransportCredentials(options.Credentials)
	if err := validateNativeServerCredentials(credentials); err != nil {
		return nil, err
	}

	runtime := server.freeze()
	serverOptions := runtime.options
	host, port, err := splitNativeAddress(addr, true)
	if err != nil {
		return nil, err
	}
	endpoint, err := nativeServerEndpointConfig(
		host,
		port,
		serverOptions,
		options.Transport,
		options.Limits,
	)
	if err != nil {
		return nil, err
	}
	engineConfig, err := nativeServerEngineConfig(serverOptions, endpoint.MaxFrameSize)
	if err != nil {
		return nil, err
	}
	openRuntime := native.NewEngine
	if endpoint.Protocol == native.ProtocolMultiplexed {
		endpoint.Admission = nativeServerAdmissionHandler(server)
		openRuntime = native.NewTransport
	}
	engine, err := openRuntime(engineConfig)
	if err != nil {
		return nil, nativeTransportStatus(err)
	}
	endpoint, cleanup, err := materializeNativeCredentials(endpoint, credentials, true)
	if err != nil {
		_ = engine.Close()
		return nil, err
	}
	listener, listenErr := engine.Listen(context.Background(), endpoint)
	cleanupErr := cleanup()
	if listenErr != nil || cleanupErr != nil {
		_ = engine.Close()
		if listenErr != nil {
			return nil, nativeTransportStatus(listenErr)
		}
		return nil, Internal("remove native credential files: " + cleanupErr.Error())
	}
	return &nativeQUICServerListener{
		engine:   engine,
		listener: listener,
		server:   server,
	}, nil
}

func (l *nativeQUICServerListener) Addr() net.Addr {
	address := l.listener.Address()
	return transportStringAddress{network: address.Network, value: address.String()}
}

func (l *nativeQUICServerListener) Serve(ctx context.Context) error {
	l.mu.Lock()
	if l.serving {
		l.mu.Unlock()
		return InvalidArgument("native listener is already serving")
	}
	l.serving = true
	l.mu.Unlock()

	defer func() { _ = l.engine.Close() }()
	return serveTransportListener(
		ctx,
		l.listener,
		l.server,
		func(
			connectionsCtx context.Context,
			connection transportinternal.Connection,
			server *Server,
			requestLimit semaphore,
		) {
			handleNativeServerConnection(
				connectionsCtx,
				connection,
				server,
				requestLimit,
			)
		},
		nativeListenerClosed,
		nativeTransportStatus,
	)
}

func handleNativeServerConnection(
	ctx context.Context,
	connection transportinternal.Connection,
	server *Server,
	requestLimit semaphore,
) {
	switch connection.Info().Protocol {
	case transportinternal.ProtocolNativeQUIC:
		handleTransportConnection(ctx, connection, server, requestLimit, true)
	case transportinternal.ProtocolHTTP3:
		handleTransportStreamEndpoint(
			ctx,
			connection,
			server,
			requestLimit,
			transportStreamEndpointOptions{
				overloadMessage:     "too many concurrent HTTP/3 requests",
				drainTimeoutMessage: "server HTTP/3 request drain timed out",
				closeOnShutdown:     true,
				shutdownMessage:     "server drained HTTP/3 request",
			},
		)
	case transportinternal.ProtocolWebTransport:
		handleWebTransportSession(ctx, connection, server, requestLimit)
	default:
		_ = connection.Close(TransportCloseReason{
			Local:   true,
			Message: "unsupported native transport protocol",
		})
	}
}

func (l *nativeQUICServerListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		select {
		case <-l.engine.Done():
			l.closeErr = l.engine.Err()
			return
		default:
		}
		listenerErr := l.listener.Close()
		l.mu.Lock()
		serving := l.serving
		l.mu.Unlock()
		if serving {
			l.closeErr = listenerErr
			return
		}
		l.closeErr = errors.Join(listenerErr, l.engine.Close())
	})
	return l.closeErr
}

func nativeListenerClosed(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return true
	}
	var eventErr *native.EventError
	return errors.As(err, &eventErr) && eventErr.Local
}

func nativeClientEndpointConfig(
	host string,
	port uint16,
	maxFrameSize int,
	transport TransportConfig,
	limits TransportLimits,
) (native.EndpointConfig, error) {
	endpoint := native.DefaultEndpointConfig()
	endpoint.Host = host
	endpoint.Port = port
	endpoint.ALPN = []byte(ALPN)
	endpoint.MaxFrameSize = uint64(maxFrameSize)
	endpoint.PeerBidirectionalStreams = 1
	streamWindow := transportFrameReceiveWindow(maxFrameSize)
	connectionWindow := streamWindow
	if limits.StreamReceiveWindow > 0 && limits.StreamReceiveWindow < streamWindow {
		streamWindow = limits.StreamReceiveWindow
	}
	if limits.ConnectionReceiveWindow > 0 && limits.ConnectionReceiveWindow < connectionWindow {
		connectionWindow = limits.ConnectionReceiveWindow
	}
	if limits.IncomingBidirectionalStreams > 0 {
		peerStreams, err := nativePeerStreamCount(limits.IncomingBidirectionalStreams)
		if err != nil {
			return native.EndpointConfig{}, err
		}
		endpoint.PeerBidirectionalStreams = peerStreams
	}
	if err := applyNativeEndpointSettings(&endpoint, transport, streamWindow, connectionWindow); err != nil {
		return native.EndpointConfig{}, err
	}
	return endpoint, nil
}

func nativeServerEndpointConfig(
	host string,
	port uint16,
	options ServerOptions,
	transport TransportConfig,
	limits TransportLimits,
) (native.EndpointConfig, error) {
	if options.MaxFrameSize <= 0 || uint64(options.MaxFrameSize) > uint64(^uint32(0))-transportFrameHeaderSize {
		return native.EndpointConfig{}, InvalidArgument("maximum frame size exceeds native Engine capacity")
	}
	endpoint := native.DefaultEndpointConfig()
	endpoint.Host = host
	endpoint.Port = port
	endpoint.ALPN = []byte(ALPN)
	endpoint.MaxFrameSize = uint64(options.MaxFrameSize)
	if options.EnableHTTP3 || options.EnableWebTransport {
		endpoint.Protocol = native.ProtocolMultiplexed
		endpoint.ALPN = nil
		endpoint.Path = http3Path(options)
		endpoint.DeferAdmission = true
		if !options.EnableWebTransport {
			endpoint.WebTransportProfiles = 0
			endpoint.MaxSessions = 0
		} else if options.WebTransportDraft07Only {
			endpoint.WebTransportProfiles = native.WebTransportProfileDraft07
		}
	}

	serverLimits := transportLimitsFromServerOptions(options)
	streamWindow := serverLimits.StreamReceiveWindow
	connectionWindow := serverLimits.ConnectionReceiveWindow
	incomingStreams := serverLimits.IncomingBidiStreams
	if limits.StreamReceiveWindow > 0 && limits.StreamReceiveWindow < streamWindow {
		streamWindow = limits.StreamReceiveWindow
	}
	if limits.ConnectionReceiveWindow > 0 && limits.ConnectionReceiveWindow < connectionWindow {
		connectionWindow = limits.ConnectionReceiveWindow
	}
	if limits.IncomingBidirectionalStreams > 0 && limits.IncomingBidirectionalStreams < incomingStreams {
		incomingStreams = limits.IncomingBidirectionalStreams
	}
	peerStreams, err := nativePeerStreamCount(incomingStreams)
	if err != nil {
		return native.EndpointConfig{}, err
	}
	endpoint.PeerBidirectionalStreams = peerStreams
	transport = mergeTransportConfig(transportConfigFromServerOptions(options), transport)
	if err := applyNativeEndpointSettings(&endpoint, transport, streamWindow, connectionWindow); err != nil {
		return native.EndpointConfig{}, err
	}
	return endpoint, nil
}

func applyNativeEndpointSettings(
	endpoint *native.EndpointConfig,
	transport TransportConfig,
	streamWindow uint64,
	connectionWindow uint64,
) error {
	var err error
	endpoint.MaxIdleTimeoutMilliseconds, err = nativeMilliseconds64(
		"maximum idle timeout",
		transport.MaxIdleTimeout,
	)
	if err != nil {
		return err
	}
	endpoint.KeepAliveMilliseconds, err = nativeMilliseconds32(
		"keep-alive interval",
		transport.KeepAlive,
	)
	if err != nil {
		return err
	}
	endpoint.StreamReceiveWindow, err = nativeStreamReceiveWindow(streamWindow)
	if err != nil {
		return err
	}
	endpoint.ConnectionFlowControlWindow, err = nativeUint32(
		"connection receive window",
		connectionWindow,
	)
	return err
}

func nativeServerEngineConfig(
	options ServerOptions,
	maxFrameSize uint64,
) (native.EngineConfig, error) {
	config := native.DefaultEngineConfig()
	config.ListenerCapacity = 1
	connections := max(options.MaxConcurrentConnections, 1)
	streamsPerConnection := max(options.MaxConcurrentStreamsPerConnection+1, 1)
	connectionCapacity, err := nativeUint32("maximum concurrent connections", uint64(connections))
	if err != nil {
		return native.EngineConfig{}, err
	}
	streamCapacity64 := saturatingMulUint64(uint64(connections), uint64(streamsPerConnection))
	streamCapacity, err := nativeUint32("maximum concurrent streams", streamCapacity64)
	if err != nil {
		return native.EngineConfig{}, err
	}
	config.ConnectionCapacity = connectionCapacity
	config.StreamCapacity = streamCapacity
	if config.MaxReceiveOwnedBytes < maxFrameSize {
		config.MaxReceiveOwnedBytes = maxFrameSize
	}
	return config, nil
}

func validateNativeClientCredentials(
	credentials *TransportCredentials,
	host string,
) error {
	if err := validateTransportCredentials(credentials); err != nil {
		return err
	}
	if credentials == nil {
		return nil
	}
	if len(credentials.CertificateChainPEM) != 0 {
		return InvalidArgument("native Engine dial does not support client certificates")
	}
	if credentials.RequireClientCertificate {
		return InvalidArgument("RequireClientCertificate is valid only for listeners")
	}
	if credentials.ServerName != "" && credentials.ServerName != host {
		return InvalidArgument("native Engine dial cannot override the target server name")
	}
	if _, err := transportRootPool(credentials); err != nil {
		return err
	}
	return nil
}

func validateNativeTransportClientCredentials(credentials *TransportCredentials) error {
	if err := validateTransportCredentials(credentials); err != nil {
		return err
	}
	if credentials == nil {
		return nil
	}
	if len(credentials.CertificateChainPEM) != 0 {
		return InvalidArgument("native Transport dial does not support client certificates")
	}
	if credentials.RequireClientCertificate {
		return InvalidArgument("RequireClientCertificate is valid only for listeners")
	}
	_, err := transportRootPool(credentials)
	return err
}

func validateNativeServerCredentials(credentials *TransportCredentials) error {
	if err := validateTransportCredentials(credentials); err != nil {
		return err
	}
	if credentials == nil || len(credentials.CertificateChainPEM) == 0 {
		return InvalidArgument("native listener requires certificate credentials")
	}
	if credentials.InsecureSkipVerify {
		return InvalidArgument("InsecureSkipVerify is valid only for dials")
	}
	if credentials.ServerName != "" {
		return InvalidArgument("ServerName is valid only for dials")
	}
	if credentials.RequireClientCertificate || len(credentials.RootCAPEM) != 0 {
		return InvalidArgument("native listener does not support client certificate verification")
	}
	_, err := transportCertificate(credentials)
	return err
}

func materializeNativeCredentials(
	endpoint native.EndpointConfig,
	credentials *TransportCredentials,
	server bool,
) (native.EndpointConfig, func() error, error) {
	if credentials == nil {
		return endpoint, func() error { return nil }, nil
	}
	endpoint.SkipCertificateValidation = credentials.InsecureSkipVerify
	needsCertificate := server && len(credentials.CertificateChainPEM) != 0
	needsRoot := !server && len(credentials.RootCAPEM) != 0
	if !needsCertificate && !needsRoot {
		return endpoint, func() error { return nil }, nil
	}

	directory, err := os.MkdirTemp("", "trevrpc-native-credentials-")
	if err != nil {
		return native.EndpointConfig{}, nil, Internal("create native credential directory: " + err.Error())
	}
	cleanup := func() error { return os.RemoveAll(directory) }
	fail := func(err error) (native.EndpointConfig, func() error, error) {
		_ = cleanup()
		return native.EndpointConfig{}, nil, err
	}
	if needsCertificate {
		endpoint.CertificateFile = filepath.Join(directory, "certificate.pem")
		endpoint.PrivateKeyFile = filepath.Join(directory, "private-key.pem")
		if err := os.WriteFile(endpoint.CertificateFile, credentials.CertificateChainPEM, 0o600); err != nil {
			return fail(Internal("write native certificate file: " + err.Error()))
		}
		if err := os.WriteFile(endpoint.PrivateKeyFile, credentials.PrivateKeyPEM, 0o600); err != nil {
			return fail(Internal("write native private-key file: " + err.Error()))
		}
	}
	if needsRoot {
		endpoint.CACertificateFile = filepath.Join(directory, "ca-certificate.pem")
		if err := os.WriteFile(endpoint.CACertificateFile, credentials.RootCAPEM, 0o600); err != nil {
			return fail(Internal("write native CA certificate file: " + err.Error()))
		}
	}
	return endpoint, cleanup, nil
}

func splitNativeAddress(address string, allowZeroPort bool) (string, uint16, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, InvalidArgument("native transport address must contain a host and port: " + err.Error())
	}
	if host == "" {
		host = "0.0.0.0"
	}
	portValue, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return "", 0, InvalidArgument("native transport port is invalid: " + err.Error())
	}
	if portValue == 0 && !allowZeroPort {
		return "", 0, InvalidArgument("native dial port must be nonzero")
	}
	return host, uint16(portValue), nil
}

func nativePeerStreamCount(value int64) (uint16, error) {
	if value <= 0 {
		return 0, nil
	}
	if value > int64(^uint16(0)) {
		return 0, InvalidArgument("incoming bidirectional stream limit exceeds native Engine capacity")
	}
	return uint16(value), nil
}

func nativeUint32(name string, value uint64) (uint32, error) {
	if value > uint64(^uint32(0)) {
		return 0, InvalidArgument(name + " exceeds native Engine capacity")
	}
	return uint32(value), nil
}

func nativeStreamReceiveWindow(value uint64) (uint32, error) {
	if value == 0 {
		return 0, nil
	}
	exponent := bits.Len64(value - 1)
	if exponent >= 32 {
		return 0, InvalidArgument("stream receive window exceeds native Engine capacity")
	}
	return uint32(1) << exponent, nil
}

func nativeMilliseconds64(name string, value time.Duration) (uint64, error) {
	if value <= 0 {
		return 0, nil
	}
	milliseconds := value.Milliseconds()
	if milliseconds == 0 {
		milliseconds = 1
	}
	if milliseconds < 0 {
		return 0, InvalidArgument(name + " is invalid")
	}
	return uint64(milliseconds), nil
}

func nativeMilliseconds32(name string, value time.Duration) (uint32, error) {
	milliseconds, err := nativeMilliseconds64(name, value)
	if err != nil {
		return 0, err
	}
	return nativeUint32(name, milliseconds)
}

func nativeTransportOrContextStatus(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	return nativeTransportStatus(err)
}

func nativeTransportStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, native.ErrUnavailable) {
		return nativeBackendUnavailable()
	}
	var statusErr *native.StatusError
	if errors.As(err, &statusErr) {
		switch statusErr.Status {
		case nativeStatusInvalidArgument:
			return InvalidArgument(statusErr.Error())
		case nativeStatusNotSupportedBSD, nativeStatusNotSupportedLinux:
			return Unimplemented(statusErr.Error())
		}
	}
	var eventErr *native.EventError
	if errors.As(err, &eventErr) && eventErr.Local && !eventErr.Peer &&
		!eventErr.TransportError && eventErr.ProviderErrorCode == 0 &&
		(eventErr.Clean || eventErr.ApplicationErrorCode != 0) {
		return Cancelled("transport closed locally")
	}
	return transportStatus(err)
}

func nativeCloseReason(err error) TransportCloseReason {
	reason := TransportCloseReason{Err: err}
	var eventErr *native.EventError
	if errors.As(err, &eventErr) {
		reason.Local = eventErr.Local
		reason.Peer = eventErr.Peer
		reason.Clean = eventErr.Clean
		reason.ApplicationCode = eventErr.ApplicationErrorCode
		if eventErr.TransportError {
			reason.TransportCode = eventErr.ProviderErrorCode
		}
		reason.Message = eventErr.Message
		return reason
	}
	if err != nil {
		reason.Message = err.Error()
	}
	return reason
}

var (
	_ channelConnector  = (*nativeEngineQUICConnector)(nil)
	_ channelGeneration = (*nativeEngineQUICGeneration)(nil)
	_ ServerListener    = (*nativeQUICServerListener)(nil)
)
