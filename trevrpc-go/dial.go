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
	// Compression lets messages travel compressed with the dictionaries it
	// holds; see Compression. It needs cgo, and Dial fails when it is invalid.
	Compression *Compression
	// StreamCompressionMemory bounds the memory, in bytes, of the zstd
	// contexts that stream compression holds for the channel's calls (see
	// StreamCompression): about 350 KiB for a direction the client decodes and
	// 585 KiB for one it compresses. It also holds the requests an optimistic
	// call keeps until the server accepts it, up to 1 MiB per call; a send
	// without room waits for the server's answer. Calls beyond it are
	// compressed per message. Zero means 64 MiB, and a negative value turns
	// stream compression off.
	StreamCompressionMemory int64
	// ConnectionMetadata is request metadata for every call of the channel,
	// such as a bearer token, which each connection carries once instead of
	// in every call: calls send it until the server confirms it stored it for
	// the connection, and refer to it after that. A call's own metadata wins
	// for a key both have. Channel.SetConnectionMetadata replaces it. It is
	// held to the limits of request metadata, and so is each call's merged
	// metadata.
	ConnectionMetadata Metadata
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
	compression, err := newCompressionConfig(options.Compression)
	if err != nil {
		return nil, err
	}
	// The budget is the channel's, shared by its connections.
	streams := newStreamBudget(options.StreamCompressionMemory, defaultClientStreamCompressionMemory, compression)
	connectionMetadata, err := newConnectionMetadataSource(options.ConnectionMetadata)
	if err != nil {
		return nil, err
	}
	quicConnector := &quicConnector{
		addr:         addr,
		tls:          clientTLSConfig(options.TLSConfig, host),
		quic:         clientQUICConfig(options.QUICConfig, maxFrameSize),
		maxFrameSize: maxFrameSize,
		compression:  compression,
		streams:      streams,
		metadata:     connectionMetadata,
	}
	qmuxConnector := &qmuxConnector{
		addr:         tcpAddr,
		tls:          clientQMuxTLSConfig(options.TLSConfig, host),
		maxFrameSize: maxFrameSize,
		compression:  compression,
		streams:      streams,
		metadata:     connectionMetadata,
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
	channel, err := dialChannel(ctx, connector, options.OnEvent)
	if err != nil {
		return nil, err
	}
	channel.connectionMetadata = connectionMetadata
	return channel, nil
}

type quicConnector struct {
	addr         string
	tls          *tls.Config
	quic         *quic.Config
	maxFrameSize int
	compression  *compressionConfig
	streams      *streamBudget
	// metadata is the channel's connection metadata, which every connection
	// offers afresh.
	metadata *connectionMetadataSource
}

func (c *quicConnector) Connect(ctx context.Context) (channelGeneration, error) {
	conn, err := quic.DialAddr(ctx, c.addr, c.tls.Clone(), c.quic.Clone())
	if err != nil {
		if ctx.Err() != nil {
			return nil, statusFromContextError(ctx.Err())
		}
		return nil, Unavailable("transport unavailable: " + err.Error())
	}
	client := newStreamClient(connOpener{conn}, c.maxFrameSize)
	// Each connection negotiates its dictionaries afresh.
	client.compression = newClientCompression(c.compression)
	client.streams = c.streams
	client.connectionMetadata = newClientConnectionMetadata(c.metadata)
	if conn.ConnectionState().SupportsDatagrams.Remote {
		// Each connection, and so each reconnect, has its own flow IDs.
		client.datagrams = newDatagramMux(conn, 0, false, 0)
	}
	return &streamGeneration{
		kind:   TransportQUIC,
		client: client,
		close: func() error {
			client.datagrams.close()
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
