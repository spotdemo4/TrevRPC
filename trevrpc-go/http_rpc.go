package trevrpc

import (
	"context"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultHTTPPath is the default endpoint for TrevRPC over HTTP and browser sessions.
	DefaultHTTPPath = "/trevrpc"
	// httpContentType is the media type for TrevRPC request and response bodies.
	httpContentType = "application/trevrpc"
)

// httpConnState is per-connection state shared by every HTTP request on it.
type httpConnState struct {
	streamLimit semaphore
	// conn is the TCP listener's connection; it is nil over HTTP/3.
	conn net.Conn
}

type httpConnStateKey struct{}

func (l *Listener) handleHTTPRPC(w http.ResponseWriter, r *http.Request) {
	runtime := l.runtime
	options := runtime.options
	if !options.EnableHTTP || r.URL.Path != options.HTTPPath {
		http.Error(w, "404 page not found", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method must be POST", http.StatusMethodNotAllowed)
		return
	}
	if !isTrevRPCMediaType(r.Header.Values("Content-Type")) {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	if !runtime.admit(w, r, options.HTTPAdmission, true, "HTTP admission denied") {
		return
	}
	slot, ok := acquireHTTPStreamSlot(r)
	if !ok {
		http.Error(w, "too many concurrent RPCs on HTTP connection", http.StatusServiceUnavailable)
		return
	}
	defer slot.release()

	w.Header().Set("Content-Type", httpContentType)
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		return
	}
	handleRPCStream(r.Context(), l.server, &httpBody{body: r.Body, writer: w, controller: controller}, slot, l.workers)
}

// acquireHTTPStreamSlot takes a stream slot of the connection r arrived on.
// It returns false when the connection is at its stream limit.
func acquireHTTPStreamSlot(r *http.Request) (*streamSlot, bool) {
	var streamLimit semaphore
	if state, ok := r.Context().Value(httpConnStateKey{}).(*httpConnState); ok {
		streamLimit = state.streamLimit
	}
	return acquireStreamSlot(streamLimit)
}

func isTrevRPCMediaType(values []string) bool {
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	return err == nil && strings.EqualFold(mediaType, httpContentType) && len(parameters) == 0
}

// httpBody exposes the request body and response writer of one HTTP request
// as an RPC stream. Every write is flushed so frames reach the peer promptly.
// Close releases the request body, which aborts the receive direction if the
// peer is still sending. The HTTP server finishes the response only when the
// handler returns, so unlike multiplexed-transport streams the request side
// cannot be drained after the response ends.
type httpBody struct {
	body       io.ReadCloser
	writer     http.ResponseWriter
	controller *http.ResponseController
	closeOnce  sync.Once
}

func (b *httpBody) Read(p []byte) (int, error) { return b.body.Read(p) }

func (b *httpBody) Write(p []byte) (int, error) {
	n, err := b.writer.Write(p)
	if err == nil {
		err = b.controller.Flush()
	}
	return n, err
}

func (b *httpBody) Close() error {
	var err error
	b.closeOnce.Do(func() { err = b.body.Close() })
	return err
}

func (b *httpBody) SetReadDeadline(deadline time.Time) error {
	return b.controller.SetReadDeadline(deadline)
}

func (b *httpBody) trevrpcCancelRead() { _ = b.Close() }

func (b *httpBody) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return watchContextCancellation(ctx, b.trevrpcCancelRead)
}

// admitBrowserSession validates the path and offered protocols of a browser
// session request and runs WebTransportAdmission, writing the HTTP rejection when the session is not
// admitted. A nil WebTransportAdmission rejects every session.
func (r *serverRuntime) admitBrowserSession(w http.ResponseWriter, request *http.Request) bool {
	options := r.options
	if request.URL.Path != options.HTTPPath {
		http.Error(w, "404 page not found", http.StatusNotFound)
		return false
	}
	r.emitDiagnostic(ServerDiagnostic{
		Phase:      ServerDiagnosticSessionConnect,
		Message:    "proto=" + request.Proto + " path=" + request.URL.Path + " authority=" + request.Host + " origin=" + request.Header.Get("Origin"),
		RemoteAddr: request.RemoteAddr,
	})

	if offersOnlyOtherWebTransportProtocols(request.Header) {
		r.emitDiagnostic(ServerDiagnostic{
			Phase:      ServerDiagnosticSessionAdmission,
			Message:    "unsupported WebTransport protocol; offered " + truncateDiagnostic(request.Header.Get(wtAvailableProtocolsHeader)),
			RemoteAddr: request.RemoteAddr,
		})
		http.Error(w, "unsupported WebTransport protocol; this server speaks "+webTransportProtocol, http.StatusBadRequest)
		return false
	}

	admitted, err := r.invokeAdmission(options.WebTransportAdmission, false, request)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionAdmission, Message: "error", Err: err})
		http.Error(w, "internal server error", status)
		return false
	}
	if !admitted {
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionAdmission, Message: "denied"})
		http.Error(w, "browser session admission denied", http.StatusForbidden)
		return false
	}
	r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionAdmission, Message: "accepted"})
	return true
}

var errAdmissionSaturated = errors.New("server admission callbacks saturated")

// admit runs an admission callback for r and writes the HTTP rejection when
// the request is not admitted. A nil callback admits iff defaultAdmit.
func (r *serverRuntime) admit(w http.ResponseWriter, request *http.Request, callback Admission, defaultAdmit bool, deniedMessage string) bool {
	admitted, err := r.invokeAdmission(callback, defaultAdmit, request)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, "internal server error", status)
		return false
	}
	if !admitted {
		http.Error(w, deniedMessage, http.StatusForbidden)
	}
	return admitted
}

func (r *serverRuntime) invokeAdmission(callback Admission, defaultAdmit bool, request *http.Request) (admitted bool, err error) {
	if callback == nil {
		return defaultAdmit, nil
	}
	if !tryAcquire(r.admissionLimit) {
		return false, errAdmissionSaturated
	}
	defer release(r.admissionLimit)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &serverPanicError{
				phase:     ServerDiagnosticAdmissionPanic,
				recovered: recovered,
				stack:     debug.Stack(),
			}
			r.emitDiagnostic(ServerDiagnostic{
				Phase: ServerDiagnosticAdmissionPanic,
				Panic: recovered,
				Stack: debug.Stack(),
				Err:   err,
			})
		}
	}()
	return callback(AdmissionRequest{
		Header:    request.Header.Clone(),
		Path:      request.URL.Path,
		Method:    request.Method,
		Proto:     request.Proto,
		Authority: request.Host,
		Origin:    request.Header.Get("Origin"),
		Secure:    request.TLS != nil,
	}), nil
}
