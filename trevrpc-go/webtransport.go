package trevrpc

import (
	"context"
	"net/http"
	"slices"
	"strings"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

const (
	// webTransportProtocol is the WebTransport application protocol that names
	// TrevRPC wire version 1 (draft-ietf-webtrans-http3, WT-Protocol).
	webTransportProtocol = "trevrpc-1"

	// wtAvailableProtocolsHeader is the CONNECT request header in which a client
	// offers WebTransport application protocols.
	wtAvailableProtocolsHeader = "WT-Available-Protocols"
)

// offersOnlyOtherWebTransportProtocols reports whether the request's
// WT-Available-Protocols field is a valid, non-empty sf-list of strings that
// does not offer webTransportProtocol. A missing or empty field means version
// 1 and a field that does not parse is ignored, as draft-ietf-webtrans-http3
// says, so none of those is rejected.
func offersOnlyOtherWebTransportProtocols(header http.Header) bool {
	values := header.Values(wtAvailableProtocolsHeader)
	if len(values) == 0 {
		return false
	}
	// Several field lines are one list joined with commas (RFC 9110, 5.3);
	// empty lines are empty lists and contribute no members.
	values = slices.DeleteFunc(slices.Clone(values), func(v string) bool { return strings.TrimSpace(v) == "" })
	offered, ok := parseStringList(strings.Join(values, ", "))
	// An empty list is sent by omitting the field (RFC 9651, 3.1), so a present
	// but empty one means the same as a missing one.
	return ok && len(offered) > 0 && !slices.Contains(offered, webTransportProtocol)
}

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

	// The session ID is the ID of the CONNECT stream. Every datagram of the
	// session starts with its quarter, which counts against the datagram size.
	// Asking for the stream hijacks the response, so this follows the upgrade.
	var datagramOverhead int
	if streamer, ok := w.(http3.HTTPStreamer); ok {
		datagramOverhead = quicvarint.Len(uint64(streamer.HTTPStream().StreamID()) / 4)
	}

	datagrams := l.newDatagramMux(session, datagramOverhead)
	defer datagrams.close()
	l.serveStreams(l.ctx, streamAcceptor{
		accept: func(ctx context.Context) (rpcStream, context.Context, error) {
			stream, err := session.AcceptStream(ctx)
			if err != nil {
				return nil, nil, err
			}
			draining := newDrainingStream(ctx, stream, stream.SetReadDeadline, func() { stream.CancelRead(streamErrorCancelled) })
			draining.datagrams = datagrams
			return draining, stream.Context(), nil
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

// truncateDiagnostic bounds peer-controlled text placed in a diagnostic.
func truncateDiagnostic(s string) string {
	const limit = 128
	if len(s) > limit {
		return s[:limit] + "..."
	}
	return s
}
