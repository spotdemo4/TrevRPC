package trevrpc

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
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

func handleHTTP3Connection(
	ctx context.Context,
	connection *legacyQUICConnection,
	server *Server,
	requestLimit semaphore,
	closeOnShutdown bool,
) {
	conn := connection.conn
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
			request := newLegacyWebTransportRequest(
				w,
				r,
				wtServer,
				connection.requestedBackend,
			)
			session, upgraded := handleWebTransportRequest(request, runtime)
			if !upgraded {
				return
			}
			sessionTasks.Go(func() {
				handleWebTransportSession(
					sessionsCtx,
					session,
					server,
					requestLimit,
				)
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
		closeErr := firstError(serveErr, connErr)
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:       ServerDiagnosticHTTP3ConnectionClosed,
			Message:     "serve_error=" + errorString(serveErr) + " connection_error=" + errorString(connErr),
			Err:         closeErr,
			Connection:  connection.Info(),
			CloseReason: legacyQUICCloseReason(closeErr),
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

func handleHTTP3RPC(
	w http.ResponseWriter,
	r *http.Request,
	server *Server,
	requestLimit,
	streamLimit semaphore,
) {
	handleTransportHTTP3RPC(
		newLegacyHTTP3Request(w, r),
		server,
		requestLimit,
		streamLimit,
	)
}

func handleTransportHTTP3RPC(
	request transportinternal.HTTP3Request,
	server *Server,
	requestLimit,
	streamLimit semaphore,
) {
	runtime := server.freeze()
	if !runtime.options.EnableHTTP3 ||
		request.Path() != http3Path(runtime.options) {
		request.WriteError("404 page not found", http.StatusNotFound)
		return
	}
	if request.Method() != http.MethodPost {
		request.SetResponseHeader("Allow", http.MethodPost)
		request.WriteError("method must be POST", http.StatusMethodNotAllowed)
		return
	}
	if !isTrevRPCMediaType(
		headerFieldValues(request.Headers(), "Content-Type"),
	) {
		request.WriteError(
			"unsupported media type",
			http.StatusUnsupportedMediaType,
		)
		return
	}
	admitted, err := runtime.transportHTTP3Admitted(request)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		request.WriteError("internal server error", status)
		return
	}
	if !admitted {
		request.WriteError("HTTP/3 admission denied", http.StatusForbidden)
		return
	}
	if !tryAcquire(streamLimit) {
		request.WriteError(
			"too many concurrent RPCs on HTTP/3 connection",
			http.StatusServiceUnavailable,
		)
		return
	}
	defer release(streamLimit)

	request.SetResponseHeader("Content-Type", HTTP3ContentType)
	request.WriteResponseHeader(http.StatusOK)
	if err := request.Flush(); err != nil {
		return
	}
	handleRPCStream(
		request.Context(),
		server,
		requestLimit,
		newTransportRPCStream(request.Stream()),
	)
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

func (r *serverRuntime) http3Admitted(
	request *http.Request,
) (bool, error) {
	snapshot := cloneAdmissionRequest(request)
	return r.invokeHTTP3Admission(HTTP3AdmissionRequest{
		Request:   snapshot,
		Headers:   headerFieldsFromHTTP(snapshot.Header),
		Path:      snapshot.URL.Path,
		Method:    snapshot.Method,
		Authority: snapshot.Host,
		Secure:    snapshot.TLS != nil,
	})
}

func (r *serverRuntime) transportHTTP3Admitted(
	request transportinternal.HTTP3Request,
) (bool, error) {
	admission := HTTP3AdmissionRequest{
		Headers:   request.Headers().Clone(),
		Path:      request.Path(),
		Method:    request.Method(),
		Authority: request.Authority(),
		Secure:    request.Secure(),
	}
	if legacy, ok := request.(interface{ legacyRequest() *http.Request }); ok {
		admission.Request = cloneAdmissionRequest(legacy.legacyRequest())
	}
	return r.invokeHTTP3Admission(admission)
}

func (r *serverRuntime) invokeHTTP3Admission(
	request HTTP3AdmissionRequest,
) (admitted bool, err error) {
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
	return callback(request), nil
}
