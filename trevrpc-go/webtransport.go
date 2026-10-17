package trevrpc

import (
	"context"
	"net/http"
)

// handleWebTransport admits and upgrades a WebTransport CONNECT request, then
// serves RPCs on the session's streams until the session ends. It runs on the
// HTTP/3 request handler, so the HTTP/3 server keeps the request alive for the
// lifetime of the session.
func (l *Listener) handleWebTransport(w http.ResponseWriter, r *http.Request) {
	runtime := l.runtime
	if !runtime.admitBrowserSession(w, r) {
		return
	}

	session, err := l.wt.Upgrade(w, r)
	if err != nil {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionUpgrade, Err: err})
		http.Error(w, "WebTransport upgrade failed", http.StatusBadRequest)
		return
	}
	runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionUpgradeSuccess, Message: "accepted"})

	l.serveStreams(l.ctx, streamAcceptor{
		accept: func(ctx context.Context) (rpcStream, context.Context, error) {
			stream, err := session.AcceptStream(ctx)
			if err != nil {
				return nil, nil, err
			}
			return newDrainingStream(stream, stream.SetReadDeadline, func() { stream.CancelRead(streamErrorCancelled) }, ctx), stream.Context(), nil
		},
		close:               func(message string) { _ = session.CloseWithError(0, message) },
		overloadMessage:     "too many concurrent streams on WebTransport session",
		drainTimeoutMessage: "server WebTransport stream drain timed out",
		shutdownMessage:     "server shutdown",
		onAcceptError: func(err error) {
			runtime.emitDiagnostic(ServerDiagnostic{
				Phase:      ServerDiagnosticSessionClosed,
				Message:    err.Error(),
				Err:        err,
				RemoteAddr: r.RemoteAddr,
			})
		},
	})
}
