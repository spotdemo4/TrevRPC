package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
)

// DialOptions configures Dial.
type DialOptions struct {
	// TLSConfig verifies the server and may supply a client certificate. It is
	// cloned; NextProtos is always set to TrevRPC's ALPN. ServerName defaults to
	// the host of the dial address.
	TLSConfig *tls.Config
	// QUICConfig optionally tunes the QUIC connection, for example MaxIdleTimeout
	// and KeepAlivePeriod. It is cloned. Flow-control windows are capped to the
	// frame size and 0-RTT is always disabled.
	QUICConfig *quic.Config
	// MaxFrameSize bounds a single frame in bytes. It defaults to DefaultMaxFrameSize.
	MaxFrameSize int
	// Transport selects the transports Dial and reconnects use. The default,
	// TransportAuto, prefers QUIC and falls back to QMux over TLS when QUIC
	// does not connect within FallbackDelay.
	Transport TransportPolicy
	// TCPAddr is the host:port of the server's TCP listener. It defaults to the
	// dial address.
	TCPAddr string
	// FallbackDelay is how long QUIC may take to connect before QMux over TLS
	// is tried in parallel. It defaults to 300 milliseconds.
	FallbackDelay time.Duration
	// ReprobeInterval is how long reconnects keep using QMux over TLS after it
	// won a race, before QUIC is tried again. It defaults to 5 minutes.
	ReprobeInterval time.Duration
	// OnEvent receives channel lifecycle events asynchronously from a bounded queue.
	OnEvent func(ChannelEvent)
}

// TransportPolicy selects the transports a Channel connects with.
type TransportPolicy uint8

const (
	// TransportAuto prefers QUIC and falls back to QMux over TLS when UDP is
	// blocked.
	TransportAuto TransportPolicy = iota
	// TransportQUICOnly connects over QUIC only.
	TransportQUICOnly
	// TransportTCPOnly connects over QMux over TLS only.
	TransportTCPOnly
)

// TransportKind identifies the transport of a connection.
type TransportKind string

const (
	// TransportQUIC is native TrevRPC over QUIC (ALPN trevrpc/1).
	TransportQUIC TransportKind = "quic"
	// TransportQMux is native TrevRPC over QMux over TLS (ALPN qmux-02.trevrpc-1).
	TransportQMux TransportKind = "qmux"
)

// Dial connects to a TrevRPC server and returns a Channel that reconnects after
// connection loss. addr is a host:port. By default QUIC is preferred and QMux
// over TLS is the fallback; see DialOptions.Transport. The context applies
// only to the initial dial; Close controls the Channel afterwards.
//
// Each RPC uses exactly one connection generation and is never retried or
// replayed. Calls fail fast with Unavailable while the channel is reconnecting.
func Dial(ctx context.Context, addr string, options DialOptions) (*Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" || strings.Contains(addr, "://") {
		return nil, InvalidArgument("dial address must be host:port")
	}
	tcpAddr := options.TCPAddr
	if tcpAddr == "" {
		tcpAddr = addr
	} else if _, _, err := net.SplitHostPort(tcpAddr); err != nil {
		return nil, InvalidArgument("TCP address must be host:port")
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	quicConnector := &quicConnector{
		addr:         addr,
		tls:          clientTLSConfig(options.TLSConfig, host),
		quic:         clientQUICConfig(options.QUICConfig, maxFrameSize),
		maxFrameSize: maxFrameSize,
	}
	qmuxConnector := &qmuxConnector{
		addr:         tcpAddr,
		tls:          clientQMuxTLSConfig(options.TLSConfig, host),
		maxFrameSize: maxFrameSize,
	}
	var connector channelConnector
	switch options.Transport {
	case TransportQUICOnly:
		connector = quicConnector
	case TransportTCPOnly:
		connector = qmuxConnector
	default:
		connector = newRacingConnector(quicConnector, qmuxConnector, options.FallbackDelay, options.ReprobeInterval)
	}
	return dialChannel(ctx, connector, options.OnEvent)
}

type quicConnector struct {
	addr         string
	tls          *tls.Config
	quic         *quic.Config
	maxFrameSize int
}

func (c *quicConnector) Connect(ctx context.Context) (channelGeneration, error) {
	conn, err := quic.DialAddr(ctx, c.addr, c.tls.Clone(), c.quic.Clone())
	if err != nil {
		if ctx.Err() != nil {
			return nil, statusFromContextError(ctx.Err())
		}
		return nil, Unavailable("transport unavailable: " + err.Error())
	}
	return &streamGeneration{
		kind:   TransportQUIC,
		client: newStreamClient(connOpener{conn}, c.maxFrameSize),
		close: func() error {
			err := conn.CloseWithError(0, "client closed")
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		},
		done: conn.Context().Done(),
		err:  func() error { return context.Cause(conn.Context()) },
	}, nil
}

// streamGeneration is one connection owned by a Channel. Every transport
// carries RPCs on one bidirectional stream each, so only opening streams and
// the connection lifecycle differ between them.
type streamGeneration struct {
	kind   TransportKind
	client *streamClient
	close  func() error
	done   <-chan struct{}
	err    func() error
}

func (g *streamGeneration) call(ctx context.Context, request *wireRequest, message ProtoMessage) (frameStream, error) {
	return g.client.call(ctx, request, message)
}

func (g *streamGeneration) streamingCall(ctx context.Context, request *wireRequest, requests encodedStream) (frameStream, error) {
	return g.client.streamingCall(ctx, request, requests)
}

func (g *streamGeneration) Close() error { return g.close() }

func (g *streamGeneration) Done() <-chan struct{} { return g.done }

func (g *streamGeneration) Err() error { return g.err() }

func (g *streamGeneration) transportKind() TransportKind { return g.kind }

type connOpener struct{ conn *quic.Conn }

func (o connOpener) openStream(ctx context.Context) (clientStream, error) {
	stream, err := o.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return quicClientStream{stream}, nil
}

type quicClientStream struct{ *quic.Stream }

func (s quicClientStream) cancelRead()  { s.CancelRead(streamErrorCancelled) }
func (s quicClientStream) cancelWrite() { s.CancelWrite(streamErrorCancelled) }
