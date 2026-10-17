package trevrpc

import (
	"context"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

// qmuxMaxRecordSize is the largest QMux record a TrevRPC endpoint accepts.
const qmuxMaxRecordSize = 64 << 10

// serverQMuxConfig derives the QMux transport parameters from the server
// limits, mirroring the QUIC configuration of serverQUICConfig.
func serverQMuxConfig(options ServerOptions) qmux.Config {
	streamWindow := frameReceiveWindow(options.MaxFrameSize)
	return qmux.Config{
		Parameters: qmux.Parameters{
			MaxIdleTimeout:                 options.StreamIdleTimeout,
			InitialMaxData:                 connectionReceiveWindow(options, streamWindow),
			InitialMaxStreamDataBidiRemote: streamWindow,
			InitialMaxStreamsBidi:          uint64(serverStreamLimit(options)),
			MaxRecordSize:                  qmuxMaxRecordSize,
		},
		KeepAlivePeriod: options.StreamIdleTimeout / 2,
	}
}

// openServerQMux runs the QMux handshake on conn, bounded by timeout.
func (l *Listener) openServerQMux(conn qmux.RecordConn, timeout context.Context) (*qmux.Session, error) {
	return qmux.Open(timeout, conn, qmux.RoleServer, serverQMuxConfig(l.runtime.options))
}

// serveQMuxSession serves RPCs on the streams of a QMux session until it ends.
func (l *Listener) serveQMuxSession(session *qmux.Session, onClosed func(error)) {
	l.serveStreams(l.ctx, streamAcceptor{
		accept: func(ctx context.Context) (rpcStream, context.Context, error) {
			stream, err := session.AcceptStream(ctx)
			if err != nil {
				return nil, nil, err
			}
			return newDrainingStream(stream, stream.SetReadDeadline, func() { stream.CancelRead(streamErrorCancelled) }, ctx), stream.Context(), nil
		},
		close:               func(message string) { _ = session.CloseWithError(0, message) },
		overloadMessage:     "too many concurrent streams on connection",
		drainTimeoutMessage: "server stream drain timed out",
		shutdownMessage:     "server drained connection",
		onAcceptError:       onClosed,
	})
	// The connection stays tracked until the session has released it.
	<-session.Done()
}
