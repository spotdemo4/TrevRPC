package trevrpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
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
	Transport  TransportConfig
	TLSConfig  *tls.Config
	QUICConfig *quic.Config
}

// DialOptions configures Dial.
type DialOptions struct {
	Transport    TransportConfig
	TLSConfig    *tls.Config
	QUICConfig   *quic.Config
	MaxFrameSize int
	OnEvent      func(ChannelEvent)
	WebTransport WebTransportOptions
}

// WebTransportOptions configures WebTransport targets passed to Dial.
type WebTransportOptions struct {
	RequestHeader           http.Header
	ApplicationProtocols    []string
	StreamReorderingTimeout time.Duration
}

// Listen creates a transport listener for server and binds it to addr.
func Listen(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	if server == nil {
		return nil, InvalidArgument("server is nil")
	}
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
