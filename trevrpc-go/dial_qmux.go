package trevrpc

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

// qmuxDialTimeout bounds connecting, the TLS handshake and the QMux handshake,
// so a server that accepts TCP but never answers cannot stall a reconnect.
const qmuxDialTimeout = 10 * time.Second

// qmuxConnector dials TrevRPC over QMux over TLS, the fallback for networks
// that block UDP.
type qmuxConnector struct {
	addr         string
	tls          *tls.Config
	maxFrameSize int
	dialer       net.Dialer
}

func (c *qmuxConnector) Connect(parent context.Context) (channelGeneration, error) {
	ctx, cancel := context.WithTimeout(parent, qmuxDialTimeout)
	defer cancel()
	raw, err := c.dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, dialError(parent, err)
	}
	conn := tls.Client(raw, c.tls.Clone())
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, dialError(parent, err)
	}
	if protocol := conn.ConnectionState().NegotiatedProtocol; protocol != QMuxALPN {
		_ = conn.Close()
		return nil, Unavailable("transport unavailable: server negotiated ALPN " + `"` + protocol + `"` + " instead of " + QMuxALPN)
	}
	session, err := qmux.Open(ctx, qmux.NewStreamConn(conn, qmuxMaxRecordSize), qmux.RoleClient, clientQMuxConfig(c.maxFrameSize))
	if err != nil {
		_ = conn.Close()
		return nil, dialError(parent, err)
	}
	return newQMuxGeneration(session, c.maxFrameSize), nil
}

// dialError reports a failed dial. Only the caller's context turns it into a
// cancellation or deadline status; the dial's own timeout is Unavailable.
func dialError(parent context.Context, err error) error {
	if parent.Err() != nil {
		return statusFromContextError(parent.Err())
	}
	return Unavailable("transport unavailable: " + err.Error())
}

// clientQMuxConfig mirrors clientQUICConfig: receive windows hold one frame
// and the server may not open streams.
func clientQMuxConfig(maxFrameSize int) qmux.Config {
	window := frameReceiveWindow(maxFrameSize)
	return qmux.Config{Parameters: qmux.Parameters{
		InitialMaxData:                window,
		InitialMaxStreamDataBidiLocal: window,
		MaxRecordSize:                 qmuxMaxRecordSize,
	}}
}

// clientQMuxTLSConfig clones base for a QMux connection to host.
func clientQMuxTLSConfig(base *tls.Config, host string) *tls.Config {
	config := clientTLSConfig(base, host)
	config.NextProtos = []string{QMuxALPN}
	// Tickets issued over QUIC are not valid for TLS over TCP, so the two
	// transports keep separate session caches.
	config.ClientSessionCache = tls.NewLRUClientSessionCache(defaultSessionCacheSize)
	return config
}

// newQMuxGeneration is one QMux session owned by a Channel.
func newQMuxGeneration(session *qmux.Session, maxFrameSize int) *streamGeneration {
	return &streamGeneration{
		kind:   TransportQMux,
		client: newStreamClient(qmuxOpener{session}, maxFrameSize),
		close:  func() error { return session.CloseWithError(0, "client closed") },
		done:   session.Context().Done(),
		err:    func() error { return context.Cause(session.Context()) },
	}
}

type qmuxOpener struct{ session *qmux.Session }

func (o qmuxOpener) openStream(ctx context.Context) (clientStream, error) {
	stream, err := o.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return qmuxClientStream{stream}, nil
}

type qmuxClientStream struct{ *qmux.Stream }

func (s qmuxClientStream) cancelRead()  { s.CancelRead(streamErrorCancelled) }
func (s qmuxClientStream) cancelWrite() { s.CancelWrite(streamErrorCancelled) }
