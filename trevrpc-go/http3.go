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
	webtransport "github.com/quic-go/webtransport-go"
)

const (
	// DefaultHTTP3Path is the default endpoint for TrevRPC over HTTP/3.
	DefaultHTTP3Path = "/trevrpc"
	// HTTP3ContentType is the media type for TrevRPC request and response bodies.
	HTTP3ContentType = "application/trevrpc"
)

func isHTTP3QUICConnection(conn *quic.Conn, options ServerOptions) bool {
	return (options.EnableHTTP3 || options.EnableWebTransport) && conn.ConnectionState().TLS.NegotiatedProtocol == http3.NextProtoH3
}

func handleHTTP3Connection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore, closeOnShutdown bool) {
	runtime := server.freeze()
	sessionsCtx, stopSessions := context.WithCancel(ctx)
	defer stopSessions()

	var sessionTasks sync.WaitGroup
	rpcStreamLimit := newSemaphore(runtime.options.MaxConcurrentStreamsPerConnection)
	releaseConnContext := func() {}
	h3Server := &http3.Server{
		ConnContext: func(connCtx context.Context, _ *quic.Conn) context.Context {
			combined, cancel := context.WithCancel(connCtx)
			stopServerCancel := context.AfterFunc(ctx, cancel)
			releaseConnContext = func() {
				stopServerCancel()
				cancel()
			}
			return combined
		},
	}
	defer func() { releaseConnContext() }()

	var wtServer *webtransport.Server
	h3Server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if runtime.options.EnableWebTransport && r.Method == http.MethodConnect {
			runtime.emitDiagnostic(ServerDiagnostic{
				Phase: ServerDiagnosticWebTransportConnect,
				Message: "path=" + r.URL.Path + " authority=" + r.Host + " origin=" + r.Header.Get("Origin") +
					" remote=" + r.RemoteAddr,
			})
			admitted, err := runtime.webTransportAdmitted(r)
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

			session, err := wtServer.Upgrade(w, r)
			if err != nil {
				runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportUpgrade, Err: err})
				http.Error(w, "WebTransport upgrade failed", http.StatusBadRequest)
				return
			}
			runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticWebTransportUpgradeSuccess, Message: "accepted"})

			sessionTasks.Go(func() {
				handleWebTransportSession(sessionsCtx, session, server, requestLimit)
			})
			return
		}

		handleHTTP3RPC(w, r, server, requestLimit, rpcStreamLimit)
	})

	if runtime.options.EnableWebTransport {
		wtServer = &webtransport.Server{
			CheckOrigin: func(*http.Request) bool { return true },
			Draft07Only: runtime.options.WebTransportDraft07Only,
			H3:          h3Server,
		}
		webtransport.ConfigureHTTP3Server(h3Server)
	}

	serveDone := make(chan struct{})
	shutdownByServer := make(chan bool, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-serveDone:
			shutdownByServer <- false
			return
		}
		stopSessions()
		if wtServer != nil {
			_ = wtServer.Close()
			shutdownByServer <- true
			return
		}

		shutdownCtx := context.Background()
		cancel := func() {}
		if runtime.options.GracefulShutdownTimeout > 0 {
			shutdownCtx, cancel = context.WithTimeout(shutdownCtx, runtime.options.GracefulShutdownTimeout)
		}
		defer cancel()
		_ = h3Server.Shutdown(shutdownCtx)
		shutdownByServer <- true
	}()

	var serveErr error
	if wtServer != nil {
		serveErr = wtServer.ServeQUICConn(conn)
	} else {
		serveErr = h3Server.ServeQUICConn(conn)
	}
	connErr := context.Cause(conn.Context())
	if serveErr != nil || connErr != nil {
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:   ServerDiagnosticHTTP3ConnectionClosed,
			Message: "serve_error=" + errorString(serveErr) + " connection_error=" + errorString(connErr),
			Err:     firstError(serveErr, connErr),
		})
	}
	close(serveDone)
	if shutdownPerformed := <-shutdownByServer; !shutdownPerformed {
		if wtServer != nil {
			_ = wtServer.Close()
		} else {
			_ = h3Server.Close()
		}
	}
	stopSessions()
	waitForWaitGroup(&sessionTasks, runtime.options.GracefulShutdownTimeout, func() {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})
		if conn.Context().Err() == nil {
			conn.CloseWithError(0, "server WebTransport session drain timed out")
		}
	})

	if closeOnShutdown && ctx.Err() != nil && conn.Context().Err() == nil {
		conn.CloseWithError(0, "server drained HTTP/3 connection")
	}
}

func errorString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func firstError(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func handleHTTP3RPC(w http.ResponseWriter, r *http.Request, server *Server, requestLimit, streamLimit semaphore) {
	runtime := server.freeze()
	if !runtime.options.EnableHTTP3 || r.URL.Path != http3Path(runtime.options) {
		http.NotFound(w, r)
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
	admitted, err := runtime.http3Admitted(r)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, "internal server error", status)
		return
	}
	if !admitted {
		http.Error(w, "HTTP/3 admission denied", http.StatusForbidden)
		return
	}
	if !tryAcquire(streamLimit) {
		http.Error(w, "too many concurrent RPCs on HTTP/3 connection", http.StatusServiceUnavailable)
		return
	}
	defer release(streamLimit)

	w.Header().Set("Content-Type", HTTP3ContentType)
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		return
	}

	handleRPCStream(r.Context(), server, requestLimit, &http3RPCStream{
		body:       r.Body,
		writer:     w,
		controller: controller,
	})
}

func http3Path(options ServerOptions) string {
	if options.HTTP3Path == "" {
		return DefaultHTTP3Path
	}
	return options.HTTP3Path
}

func isTrevRPCMediaType(values []string) bool {
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	return err == nil && strings.EqualFold(mediaType, HTTP3ContentType) && len(parameters) == 0
}

var errAdmissionSaturated = errors.New("server admission callbacks saturated")

func cloneAdmissionRequest(request *http.Request) *http.Request {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Body = http.NoBody
	return clone
}

func (r *serverRuntime) http3Admitted(request *http.Request) (admitted bool, err error) {
	callback := r.options.HTTP3Admission
	if callback == nil {
		return true, nil
	}
	if !tryAcquire(r.admissionLimit) {
		return false, errAdmissionSaturated
	}
	defer release(r.admissionLimit)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &serverPanicError{phase: ServerDiagnosticAdmissionPanic, recovered: recovered, stack: debug.Stack()}
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticAdmissionPanic, Panic: recovered, Stack: debug.Stack(), Err: err})
		}
	}()
	snapshot := cloneAdmissionRequest(request)
	return callback(HTTP3AdmissionRequest{Request: snapshot, Path: snapshot.URL.Path, Method: snapshot.Method, Authority: snapshot.Host, Secure: snapshot.TLS != nil}), nil
}

type http3RPCStream struct {
	body       io.ReadCloser
	writer     http.ResponseWriter
	controller *http.ResponseController
	closeOnce  sync.Once
}

func (s *http3RPCStream) Read(data []byte) (int, error) {
	return s.body.Read(data)
}

func (s *http3RPCStream) Write(data []byte) (int, error) {
	written, err := s.writer.Write(data)
	if err != nil {
		return written, err
	}
	if err := s.controller.Flush(); err != nil {
		return written, err
	}
	return written, nil
}

func (s *http3RPCStream) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.body.Close() })
	return err
}

func (s *http3RPCStream) SetReadDeadline(deadline time.Time) error {
	return s.controller.SetReadDeadline(deadline)
}

func (s *http3RPCStream) trevrpcCancelRead() {
	_ = s.Close()
}

func (s *http3RPCStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-done:
		}
	}()

	return func() { stopOnce.Do(func() { close(done) }) }
}
