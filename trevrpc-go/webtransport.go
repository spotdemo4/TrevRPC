package trevrpc

import (
	"context"
	"errors"
	"net/http"
)

// handleWebTransport admits and upgrades a WebTransport CONNECT request, then
// serves RPCs on the session's streams until the session ends. It runs on the
// HTTP/3 request handler, so the HTTP/3 server keeps the request alive for the
// lifetime of the session.
func (l *Listener) handleWebTransport(w http.ResponseWriter, r *http.Request) {
	runtime := l.runtime
	options := runtime.options
	if r.URL.Path != options.HTTP3Path {
		http.Error(w, "404 page not found", http.StatusNotFound)
		return
	}
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:      ServerDiagnosticWebTransportConnect,
		Message:    "path=" + r.URL.Path + " authority=" + r.Host + " origin=" + r.Header.Get("Origin"),
		RemoteAddr: r.RemoteAddr,
	})

	admitted, err := runtime.invokeAdmission(options.WebTransportAdmission, false, r)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportAdmission, Message: "error", Err: err})
		http.Error(w, "internal server error", status)
		return
	}
	if !admitted {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportAdmission, Message: "denied"})
		http.Error(w, "WebTransport admission denied", http.StatusForbidden)
		return
	}
	runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportAdmission, Message: "accepted"})

	session, err := l.wt.Upgrade(w, r)
	if err != nil {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportUpgrade, Err: err})
		http.Error(w, "WebTransport upgrade failed", http.StatusBadRequest)
		return
	}
	runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportUpgradeSuccess, Message: "accepted"})

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
				Phase:      ServerDiagnosticWebTransportSessionClosed,
				Message:    err.Error(),
				Err:        err,
				RemoteAddr: r.RemoteAddr,
			})
		},
	})
}
