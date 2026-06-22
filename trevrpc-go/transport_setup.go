package trevrpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/quic-go/quic-go"
)

// TransportKind selects the QUIC transport backend for Listen and Dial.
type TransportKind uint8

const (
	// TransportQUICGo uses the pure Go quic-go backend.
	TransportQUICGo TransportKind = iota
	// TransportMsQuic uses the native trevrpc-c MsQuic backend.
	TransportMsQuic
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
	Kind       TransportKind
	Transport  TransportConfig
	TLSConfig  *tls.Config
	QUICConfig *quic.Config
	MsQuic     MsQuicConfig
}

// DialOptions configures Dial.
type DialOptions struct {
	Kind         TransportKind
	Transport    TransportConfig
	TLSConfig    *tls.Config
	QUICConfig   *quic.Config
	MsQuic       MsQuicConfig
	MaxFrameSize int
}

// Listen creates a transport listener for server and binds it to addr.
func Listen(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}

	switch options.Kind {
	case TransportQUICGo:
		if options.TLSConfig == nil {
			return nil, InvalidArgument("quic-go listener requires TLSConfig")
		}
		serverOptions := server.Options()
		config := QUICServerConfig(serverOptions, options.QUICConfig)
		applyDefaultQUICTransportConfig(config, mergeTransportConfig(transportConfigFromServerOptions(serverOptions), options.Transport))
		listener, err := quic.ListenAddr(addr, options.TLSConfig, config)
		if err != nil {
			return nil, transportStatus(err)
		}
		return &quicServerListener{listener: listener, server: server}, nil
	case TransportMsQuic:
		return listenMsQuic(addr, server, options)
	default:
		return nil, InvalidArgument(fmt.Sprintf("unsupported transport kind %d", options.Kind))
	}
}

// Dial connects to addr and returns a TrevRPC client transport.
func Dial(ctx context.Context, addr string, options DialOptions) (ClientTransport, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}

	switch options.Kind {
	case TransportQUICGo:
		if options.TLSConfig == nil {
			return nil, InvalidArgument("quic-go dial requires TLSConfig")
		}
		config := QUICClientConfig(maxFrameSize, options.QUICConfig)
		applyDefaultQUICTransportConfig(config, options.Transport)
		conn, err := quic.DialAddr(ctx, addr, options.TLSConfig, config)
		if err != nil {
			return nil, transportOrContextStatus(ctx, err)
		}
		return NewQuicClient(conn).WithMaxFrameSize(maxFrameSize), nil
	case TransportMsQuic:
		return dialMsQuic(ctx, addr, options, maxFrameSize)
	default:
		return nil, InvalidArgument(fmt.Sprintf("unsupported transport kind %d", options.Kind))
	}
}

type quicServerListener struct {
	listener *quic.Listener
	server   *Server
}

func (l *quicServerListener) Addr() net.Addr {
	return l.listener.Addr()
}

func (l *quicServerListener) Serve(ctx context.Context) error {
	return ServeQUIC(ctx, l.listener, l.server)
}

func (l *quicServerListener) Close() error {
	return l.listener.Close()
}
