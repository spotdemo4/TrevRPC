package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"

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
	// OnEvent receives channel lifecycle events asynchronously from a bounded queue.
	OnEvent func(ChannelEvent)
}

// Dial connects to a TrevRPC server over native QUIC and returns a Channel that
// reconnects after connection loss. addr is a host:port. The context applies
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
		return nil, InvalidArgument("dial address must be host:port; clients speak native QUIC only")
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	connector := &quicConnector{
		addr:         addr,
		tls:          clientTLSConfig(options.TLSConfig, host),
		quic:         clientQUICConfig(options.QUICConfig, maxFrameSize),
		maxFrameSize: maxFrameSize,
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
	return &connGeneration{
		conn:   conn,
		client: newStreamClient(connOpener{conn}, c.maxFrameSize),
	}, nil
}

// connGeneration is one native QUIC connection owned by a Channel.
type connGeneration struct {
	conn   *quic.Conn
	client *streamClient
}

func (g *connGeneration) call(ctx context.Context, request *wireRequest) (*wireResponse, error) {
	return g.client.call(ctx, request)
}

func (g *connGeneration) streamingCall(ctx context.Context, request *wireRequest, requestBody byteStream) (frameStream, error) {
	return g.client.streamingCall(ctx, request, requestBody)
}

func (g *connGeneration) Close() error {
	err := g.conn.CloseWithError(0, "client closed")
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (g *connGeneration) Done() <-chan struct{} { return g.conn.Context().Done() }

func (g *connGeneration) Err() error { return context.Cause(g.conn.Context()) }

type connOpener struct{ conn *quic.Conn }

func (o connOpener) openStream(ctx context.Context) (clientStream, error) {
	stream, err := o.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return quicClientStream{stream}, nil
}

type quicClientStream struct{ *quic.Stream }

func (s quicClientStream) cancelRead()  { s.Stream.CancelRead(streamErrorCancelled) }
func (s quicClientStream) cancelWrite() { s.Stream.CancelWrite(streamErrorCancelled) }
