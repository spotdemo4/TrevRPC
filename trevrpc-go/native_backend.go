package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

// NativeBackend adapts one explicit trevrpc-c provider to the high-level Go
// client and server APIs. Provider selection is endpoint-scoped and immutable.
// The zero value is unavailable; use NewNativeBackend to select a provider.
type NativeBackend struct {
	provider  trevrpcc.Provider
	automatic bool
}

// NewNativeBackend returns an endpoint-scoped backend bound to provider.
func NewNativeBackend(provider trevrpcc.Provider) NativeBackend {
	return NativeBackend{provider: provider}
}

func defaultNativeBackend() NativeBackend {
	backend := NewNativeBackend(defaultNativeProvider())
	backend.automatic = true
	return backend
}

func (b NativeBackend) providerWithCapabilities(required uint64) (trevrpcc.Provider, error) {
	if b.provider == nil {
		return nil, Unimplemented("native transport backend has no provider; use NewNativeBackend to select one")
	}
	metadata := b.provider.Metadata()
	if metadata.Name == "" {
		return nil, InvalidArgument("native transport provider metadata name is empty")
	}
	if metadata.Version == "" {
		return nil, InvalidArgument(fmt.Sprintf("native transport provider %q metadata version is empty", metadata.Name))
	}
	missing := required &^ metadata.Capabilities
	if missing != 0 {
		return nil, Unimplemented(fmt.Sprintf(
			"native transport provider %q version %q lacks required capabilities 0x%x",
			metadata.Name,
			metadata.Version,
			missing,
		))
	}
	return b.provider, nil
}

var (
	_ DialBackend   = NativeBackend{}
	_ ListenBackend = NativeBackend{}
)

func (NativeBackend) Backend() TransportBackend {
	return TransportBackendNative
}

func (b NativeBackend) NewConnector(
	target string,
	options BackendDialOptions,
) (BackendConnector, error) {
	if !native.Available() {
		return nil, nativeBackendUnavailable()
	}
	requiredCapabilities := trevrpcc.ProviderCapabilityEngine |
		trevrpcc.ProviderCapabilityNativeQUIC
	if strings.HasPrefix(target, "https://") {
		requiredCapabilities = trevrpcc.ProviderCapabilityTransport |
			trevrpcc.ProviderCapabilityWebTransport |
			trevrpcc.ProviderCapabilityMultiplexed
	}
	provider, err := b.providerWithCapabilities(requiredCapabilities)
	if err != nil {
		return nil, err
	}
	requestedBackend := TransportBackendNative
	if b.automatic {
		requestedBackend = TransportBackendAuto
	}
	return newNativeChannelConnectorWithProvider(
		provider,
		target,
		DialOptions{
			Transport:    options.Transport,
			Limits:       options.Limits,
			Credentials:  options.Credentials,
			MaxFrameSize: options.MaxFrameSize,
			WebTransport: options.WebTransport,
		},
		requestedBackend,
	)
}

func (b NativeBackend) Listen(
	addr string,
	options BackendListenOptions,
) (transportapi.Listener, error) {
	if !native.Available() {
		return nil, nativeBackendUnavailable()
	}
	requiredCapabilities := trevrpcc.ProviderCapabilityEngine |
		trevrpcc.ProviderCapabilityNativeQUIC
	if options.Server.EnableHTTP3 || options.Server.EnableWebTransport {
		requiredCapabilities = trevrpcc.ProviderCapabilityTransport |
			trevrpcc.ProviderCapabilityMultiplexed
		if options.Server.EnableHTTP3 {
			requiredCapabilities |= trevrpcc.ProviderCapabilityHTTP3
		}
		if options.Server.EnableWebTransport {
			requiredCapabilities |= trevrpcc.ProviderCapabilityWebTransport
		}
	}
	provider, err := b.providerWithCapabilities(requiredCapabilities)
	if err != nil {
		return nil, err
	}
	credentials := cloneTransportCredentials(options.Credentials)
	if err := validateNativeServerCredentials(credentials); err != nil {
		return nil, err
	}
	host, port, err := splitNativeAddress(addr, true)
	if err != nil {
		return nil, err
	}
	serverOptions := ServerOptions{
		MaxFrameSize:                      options.Server.MaxFrameSize,
		MaxConcurrentConnections:          options.Server.MaxConcurrentConnections,
		MaxConcurrentStreamsPerConnection: options.Server.MaxConcurrentStreamsPerConnection,
		EnableHTTP3:                       options.Server.EnableHTTP3,
		EnableWebTransport:                options.Server.EnableWebTransport,
		HTTP3Path:                         options.Server.HTTP3Path,
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
	engineConfig, err := nativeServerEngineConfig(
		serverOptions,
		nativeEndpointReceiveOwnedBytes(endpoint),
	)
	if err != nil {
		return nil, err
	}
	openRuntime := native.NewEngine
	if endpoint.Protocol == native.ProtocolMultiplexed {
		endpoint.Admission = nativeBackendAdmissionHandler(options)
		openRuntime = native.NewTransport
	}
	engine, err := openRuntime(provider, engineConfig)
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
	return &nativeBackendListener{engine: engine, listener: listener}, nil
}

func (NativeBackend) ServeConnection(
	ctx context.Context,
	connection transportapi.Connection,
	server BackendServerConnection,
) {
	switch connection.Info().Protocol {
	case transportapi.ProtocolNativeQUIC:
		server.ServeNativeQUIC(ctx, connection)
	case transportapi.ProtocolHTTP3:
		server.ServeHTTP3(ctx, connection)
	case transportapi.ProtocolWebTransport:
		server.ServeWebTransport(ctx, connection)
	default:
		_ = connection.Close(TransportCloseReason{
			Local:   true,
			Message: "unsupported native transport protocol",
		})
	}
}

func (NativeBackend) ListenerClosed(err error) bool {
	return nativeListenerClosed(err)
}

func (NativeBackend) MapStatus(err error) error {
	return nativeTransportStatus(err)
}

type nativeBackendListener struct {
	engine   *native.Engine
	listener *native.Listener

	mu        sync.Mutex
	serving   bool
	closeOnce sync.Once
	closeErr  error
}

var _ transportapi.Listener = (*nativeBackendListener)(nil)

func (l *nativeBackendListener) Accept(ctx context.Context) (transportapi.Connection, error) {
	return l.listener.Accept(ctx)
}

func (l *nativeBackendListener) Address() transportapi.Address {
	return l.listener.Address()
}

func (l *nativeBackendListener) beginServe() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.serving {
		return InvalidArgument("native listener is already serving")
	}
	l.serving = true
	return nil
}

func (l *nativeBackendListener) endServe() error {
	l.mu.Lock()
	l.serving = false
	l.mu.Unlock()
	return l.engine.Close()
}

func (l *nativeBackendListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
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

func nativeBackendAdmissionHandler(options BackendListenOptions) native.AdmissionHandler {
	return func(request native.AdmissionRequest) uint16 {
		headers := make(HeaderFields, len(request.Headers))
		for index, header := range request.Headers {
			headers[index] = HeaderField{Name: header.Name, Value: header.Value}
		}
		switch request.Kind {
		case native.AdmissionHTTP3:
			if !options.Server.EnableHTTP3 || request.Path != options.Server.HTTP3Path {
				return http.StatusNotFound
			}
			if request.Method != http.MethodPost {
				return http.StatusMethodNotAllowed
			}
			if !isTrevRPCMediaType(headerFieldValues(headers, "Content-Type")) {
				return http.StatusUnsupportedMediaType
			}
			if options.AdmitHTTP3 == nil {
				return http.StatusOK
			}
			admitted, err := options.AdmitHTTP3(HTTP3AdmissionRequest{
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
			if !options.Server.EnableWebTransport || request.Path != options.Server.HTTP3Path {
				return http.StatusNotFound
			}
			if options.AdmitWebTransport == nil {
				return http.StatusForbidden
			}
			admitted, err := options.AdmitWebTransport(WebTransportAdmissionRequest{
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
