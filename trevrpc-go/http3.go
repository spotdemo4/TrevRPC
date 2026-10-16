package trevrpc

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

const (
	// DefaultHTTP3Path is the default endpoint for TrevRPC over HTTP/3 and WebTransport.
	DefaultHTTP3Path = "/trevrpc"
	// http3ContentType is the media type for TrevRPC request and response bodies.
	http3ContentType = "application/trevrpc"
)

// http3ConnState is per-connection state shared by every HTTP/3 request on it.
type http3ConnState struct {
	streamLimit semaphore
}

type http3ConnStateKey struct{}

// setupHTTP3 prepares the HTTP/3 server shared by every h3 connection. Its
// connections are accepted by the Listener and handed over with ServeQUICConn.
func (l *Listener) setupHTTP3() {
	options := l.runtime.options
	l.h3 = &http3.Server{
		Handler: http.HandlerFunc(l.handleHTTP3),
		ConnContext: func(ctx context.Context, _ *quic.Conn) context.Context {
			state := &http3ConnState{streamLimit: newSemaphore(options.MaxConcurrentStreamsPerConnection)}
			return context.WithValue(ctx, http3ConnStateKey{}, state)
		},
	}
	if options.EnableWebTransport {
		// Admission decides which sessions are allowed; the same-origin default
		// would reject browsers served from another origin.
		l.wt = &webtransport.Server{
			H3:          l.h3,
			CheckOrigin: func(*http.Request) bool { return true },
		}
	}
}

// serveHTTP3Conn serves HTTP/3 requests, and WebTransport sessions when
// enabled, on one connection until it closes.
func (l *Listener) serveHTTP3Conn(conn *quic.Conn) {
	stop := context.AfterFunc(l.ctx, func() { _ = conn.CloseWithError(0, "server shutdown") })
	defer stop()

	var err error
	if l.wt != nil {
		err = l.wt.ServeQUICConn(conn)
	} else {
		err = l.h3.ServeQUICConn(conn)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		l.runtime.emitDiagnostic(ServerDiagnostic{
			Phase:      ServerDiagnosticHTTP3ConnectionClosed,
			Err:        err,
			RemoteAddr: conn.RemoteAddr().String(),
		})
	}
	// Make sure CONNECTION_CLOSE reaches the peer before the socket is released.
	_ = conn.CloseWithError(0, "server closed connection")
}

// closeHTTP3 releases the HTTP/3 servers. Connections are already closed by
// the caller; the servers are closed asynchronously so that a handler that
// ignores cancellation cannot block teardown.
func (l *Listener) closeHTTP3() {
	switch {
	case l.wt != nil:
		go func() { _ = l.wt.Close() }()
	case l.h3 != nil:
		go func() { _ = l.h3.Close() }()
	}
}

func (l *Listener) handleHTTP3(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect && l.wt != nil {
		l.handleWebTransport(w, r)
		return
	}
	l.handleHTTP3RPC(w, r)
}

func (l *Listener) handleHTTP3RPC(w http.ResponseWriter, r *http.Request) {
	runtime := l.runtime
	options := runtime.options
	if !options.EnableHTTP3 || r.URL.Path != options.HTTP3Path {
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
	if !runtime.admit(w, r, options.HTTP3Admission, true, "HTTP/3 admission denied") {
		return
	}
	var streamLimit semaphore
	if state, ok := r.Context().Value(http3ConnStateKey{}).(*http3ConnState); ok {
		streamLimit = state.streamLimit
	}
	if !tryAcquire(streamLimit) {
		http.Error(w, "too many concurrent RPCs on HTTP/3 connection", http.StatusServiceUnavailable)
		return
	}
	defer release(streamLimit)

	w.Header().Set("Content-Type", http3ContentType)
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		return
	}
	handleRPCStream(r.Context(), l.server, &http3Body{body: r.Body, writer: w, controller: controller})
}

func isTrevRPCMediaType(values []string) bool {
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	return err == nil && strings.EqualFold(mediaType, http3ContentType) && len(parameters) == 0
}

// http3Body exposes the request body and response writer of one HTTP/3 request
// as an RPC stream. Every write is flushed so frames reach the peer promptly.
// Close releases the request body, which aborts the receive direction if the
// peer is still sending. The HTTP/3 server finishes the response only when the
// handler returns, so unlike native QUIC and WebTransport streams the request
// side cannot be drained after the response ends.
type http3Body struct {
	body       io.ReadCloser
	writer     http.ResponseWriter
	controller *http.ResponseController
	closeOnce  sync.Once
}

func (b *http3Body) Read(p []byte) (int, error) { return b.body.Read(p) }

func (b *http3Body) Write(p []byte) (int, error) {
	n, err := b.writer.Write(p)
	if err == nil {
		err = b.controller.Flush()
	}
	return n, err
}

func (b *http3Body) Close() error {
	var err error
	b.closeOnce.Do(func() { err = b.body.Close() })
	return err
}

func (b *http3Body) SetReadDeadline(deadline time.Time) error {
	return b.controller.SetReadDeadline(deadline)
}

func (b *http3Body) trevrpcCancelRead() { _ = b.Close() }

func (b *http3Body) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return watchContextCancellation(ctx, b.trevrpcCancelRead)
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
		Authority: request.Host,
		Origin:    request.Header.Get("Origin"),
		Secure:    request.TLS != nil,
	}), nil
}
