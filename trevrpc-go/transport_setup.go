package trevrpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
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

// ListenOptions configures Listen.
type ListenOptions struct {
	Backend     TransportBackend
	Transport   TransportConfig
	Limits      TransportLimits
	Credentials *TransportCredentials
	// TLSConfig is supported only by the Legacy backend.
	// Deprecated: use Credentials for backend-neutral endpoint configuration.
	TLSConfig *tls.Config
	// QUICConfig is supported only by the Legacy backend.
	// Deprecated: use Transport and Limits for backend-neutral endpoint configuration.
	QUICConfig *quic.Config
}

// DialOptions configures Dial.
type DialOptions struct {
	Backend     TransportBackend
	Transport   TransportConfig
	Limits      TransportLimits
	Credentials *TransportCredentials
	// TLSConfig is supported only by the Legacy backend.
	// Deprecated: use Credentials for backend-neutral endpoint configuration.
	TLSConfig *tls.Config
	// QUICConfig is supported only by the Legacy backend.
	// Deprecated: use Transport and Limits for backend-neutral endpoint configuration.
	QUICConfig   *quic.Config
	MaxFrameSize int
	OnEvent      func(ChannelEvent)
	WebTransport WebTransportOptions
}

// WebTransportOptions configures WebTransport targets passed to Dial.
type WebTransportOptions struct {
	// RequestHeaders supplies backend-neutral ordered request fields. The
	// Legacy backend preserves duplicate values, but its upstream http.Header
	// API cannot preserve ordering between different field names on the wire.
	RequestHeaders HeaderFields
	// RequestHeader is supported only by the Legacy backend.
	// Deprecated: use RequestHeaders for backend-neutral request fields.
	RequestHeader           http.Header
	ApplicationProtocols    []string
	StreamReorderingTimeout time.Duration
}

// Listen creates a transport listener for server and binds it to addr.
func Listen(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}
	backend, err := resolveTransportBackend(options.Backend)
	if err != nil {
		return nil, err
	}
	if backend == TransportBackendNative {
		if options.TLSConfig != nil || options.QUICConfig != nil {
			return nil, InvalidArgument("native listener does not accept legacy TLSConfig or QUICConfig")
		}
		return nil, nativeBackendUnavailable()
	}
	tlsConfig, err := legacyServerTLSConfig(options.TLSConfig, cloneTransportCredentials(options.Credentials))
	if err != nil {
		return nil, err
	}
	runtime := server.freeze()
	serverOptions := runtime.options
	if options.Credentials != nil {
		tlsConfig.NextProtos = []string{ALPN}
		if serverOptions.EnableHTTP3 || serverOptions.EnableWebTransport {
			tlsConfig.NextProtos = append(
				tlsConfig.NextProtos,
				http3.NextProtoH3,
			)
		}
	}
	config := QUICServerConfig(serverOptions, options.QUICConfig)
	applyDefaultQUICTransportConfig(config, mergeTransportConfig(transportConfigFromServerOptions(serverOptions), options.Transport))
	applyQUICTransportLimits(config, options.Limits)
	listener, err := quic.ListenAddr(addr, tlsConfig, config)
	if err != nil {
		return nil, transportStatus(err)
	}
	return &quicServerListener{
		listener:         listener,
		server:           server,
		requestedBackend: options.Backend,
	}, nil
}

type quicServerListener struct {
	listener         *quic.Listener
	server           *Server
	requestedBackend TransportBackend
}

func (l *quicServerListener) Addr() net.Addr {
	return l.listener.Addr()
}

func (l *quicServerListener) Serve(ctx context.Context) error {
	return serveLegacyQUIC(
		ctx,
		l.listener,
		l.server,
		l.requestedBackend,
	)
}

func (l *quicServerListener) Close() error {
	return l.listener.Close()
}
