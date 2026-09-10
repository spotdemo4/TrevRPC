package trevrpc

import (
	"context"
	"errors"
	"math/bits"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

const (
	nativeStatusInvalidArgument          = -22
	nativeStatusNotSupportedBSD          = -45
	nativeStatusNotSupportedLinux        = -95
	nativeTransportReceiveBudgetOverhead = 4096
)

func newNativeChannelConnectorWithProvider(
	provider trevrpcc.Provider,
	target string,
	options DialOptions,
	requestedBackend TransportBackend,
) (BackendConnector, error) {
	if strings.HasPrefix(target, "https://") {
		return newNativeWebTransportConnectorWithProvider(provider, target, options, requestedBackend)
	}
	return newNativeEngineQUICConnectorWithProvider(provider, target, options, requestedBackend)
}

type nativeEngineQUICConnector struct {
	provider         trevrpcc.Provider
	endpoint         native.EndpointConfig
	engineConfig     native.EngineConfig
	credentials      *TransportCredentials
	requestedBackend TransportBackend
}

func newNativeEngineQUICConnectorWithProvider(
	provider trevrpcc.Provider,
	target string,
	options DialOptions,
	requestedBackend TransportBackend,
) (*nativeEngineQUICConnector, error) {
	if !native.Available() || provider == nil {
		return nil, nativeBackendUnavailable()
	}
	if strings.HasPrefix(target, "https://") {
		return nil, Unimplemented("native WebTransport dialing is not implemented yet")
	}
	if scheme, _, ok := strings.Cut(target, "://"); ok {
		return nil, InvalidArgument("unsupported dial target scheme " + scheme)
	}
	if len(options.WebTransport.RequestHeaders) != 0 ||
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
	minimumReceiveOwnedBytes := nativeEndpointReceiveOwnedBytes(endpoint)
	if engineConfig.MaxReceiveOwnedBytes < minimumReceiveOwnedBytes {
		engineConfig.MaxReceiveOwnedBytes = minimumReceiveOwnedBytes
	}
	return &nativeEngineQUICConnector{
		provider:         provider,
		endpoint:         endpoint,
		engineConfig:     engineConfig,
		credentials:      credentials,
		requestedBackend: requestedBackend,
	}, nil
}

func (c *nativeEngineQUICConnector) Connect(
	ctx context.Context,
) (BackendConnection, error) {
	engine, err := native.NewEngine(c.provider, c.engineConfig)
	if err != nil {
		return BackendConnection{}, nativeTransportOrContextStatus(ctx, err)
	}
	return connectNativeBackend(
		ctx,
		engine,
		c.endpoint,
		c.credentials,
		c.requestedBackend,
	)
}

type nativeWebTransportConnector struct {
	provider         trevrpcc.Provider
	endpoint         native.EndpointConfig
	engineConfig     native.EngineConfig
	credentials      *TransportCredentials
	requestedBackend TransportBackend
}

func newNativeWebTransportConnectorWithProvider(
	provider trevrpcc.Provider,
	target string,
	options DialOptions,
	requestedBackend TransportBackend,
) (*nativeWebTransportConnector, error) {
	if !native.Available() || provider == nil {
		return nil, nativeBackendUnavailable()
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
	ensureNativeTransportReceiveBudget(&endpoint)
	engineConfig := native.DefaultEngineConfig()
	engineConfig.ListenerCapacity = 1
	engineConfig.ConnectionCapacity = 1
	minimumReceiveOwnedBytes := nativeEndpointReceiveOwnedBytes(endpoint)
	if engineConfig.MaxReceiveOwnedBytes < minimumReceiveOwnedBytes {
		engineConfig.MaxReceiveOwnedBytes = minimumReceiveOwnedBytes
	}
	return &nativeWebTransportConnector{
		provider:         provider,
		endpoint:         endpoint,
		engineConfig:     engineConfig,
		credentials:      credentials,
		requestedBackend: requestedBackend,
	}, nil
}

func (c *nativeWebTransportConnector) Connect(
	ctx context.Context,
) (BackendConnection, error) {
	engine, err := native.NewTransport(c.provider, c.engineConfig)
	if err != nil {
		return BackendConnection{}, nativeTransportOrContextStatus(ctx, err)
	}
	return connectNativeBackend(
		ctx,
		engine,
		c.endpoint,
		c.credentials,
		c.requestedBackend,
	)
}

func connectNativeBackend(
	ctx context.Context,
	engine *native.Engine,
	endpoint native.EndpointConfig,
	credentials *TransportCredentials,
	requestedBackend TransportBackend,
) (BackendConnection, error) {
	endpoint, cleanup, err := materializeNativeCredentials(
		endpoint,
		credentials,
		false,
	)
	if err != nil {
		_ = engine.Close()
		return BackendConnection{}, err
	}
	connection, dialErr := engine.Dial(ctx, endpoint)
	cleanupErr := cleanup()
	if dialErr != nil || cleanupErr != nil {
		_ = engine.Close()
		if dialErr != nil {
			return BackendConnection{}, nativeTransportOrContextStatus(ctx, dialErr)
		}
		return BackendConnection{}, Internal("remove native credential files: " + cleanupErr.Error())
	}

	wrapped := &nativeBackendEndpoint{
		Connection:       connection,
		requestedBackend: requestedBackend,
	}
	var closeOnce sync.Once
	var closeErr error
	return BackendConnection{
		Endpoint: wrapped,
		Close: func() error {
			closeOnce.Do(func() {
				var connectionErr error
				select {
				case <-connection.Done():
				default:
					connectionErr = connection.Close(TransportCloseReason{
						Local:   true,
						Clean:   true,
						Message: "client closed",
					})
				}
				closeErr = errors.Join(connectionErr, engine.Close())
			})
			return closeErr
		},
		MapStatus:   nativeTransportOrContextStatus,
		CloseReason: nativeCloseReason,
	}, nil
}

type nativeBackendEndpoint struct {
	transportinternal.Connection
	requestedBackend TransportBackend
}

func (e *nativeBackendEndpoint) Info() ConnectionInfo {
	info := e.Connection.Info()
	info.RequestedBackend = e.requestedBackend
	return info
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

func nativeServerAdmissionHandler(server *Server) native.AdmissionHandler {
	runtime := server.freeze()
	return nativeBackendAdmissionHandler(BackendListenOptions{
		Server:            backendServerOptions(runtime.options),
		AdmitHTTP3:        runtime.invokeHTTP3Admission,
		AdmitWebTransport: runtime.invokeWebTransportAdmission,
	})
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
		}
		ensureNativeTransportReceiveBudget(&endpoint)
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

func ensureNativeTransportReceiveBudget(endpoint *native.EndpointConfig) {
	minimum := saturatingAddUint64(
		saturatingMulUint64(endpoint.MaxFrameSize, 2),
		nativeTransportReceiveBudgetOverhead,
	)
	if endpoint.MaxPendingReceiveBytes < minimum {
		endpoint.MaxPendingReceiveBytes = minimum
	}
}

func nativeEndpointReceiveOwnedBytes(endpoint native.EndpointConfig) uint64 {
	return max(
		endpoint.MaxFrameSize,
		endpoint.MaxPendingReceiveBytes,
		endpoint.UnresolvedStreamBytes,
		uint64(endpoint.StreamReceiveWindow),
		uint64(endpoint.ConnectionFlowControlWindow),
	)
}

func nativeServerEngineConfig(
	options ServerOptions,
	minimumReceiveOwnedBytes uint64,
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
	if config.MaxReceiveOwnedBytes < minimumReceiveOwnedBytes {
		config.MaxReceiveOwnedBytes = minimumReceiveOwnedBytes
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
	_ BackendConnector = (*nativeEngineQUICConnector)(nil)
	_ BackendConnector = (*nativeWebTransportConnector)(nil)
)
