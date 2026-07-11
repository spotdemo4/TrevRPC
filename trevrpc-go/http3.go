package trevrpc

import (
	"context"
	"io"
	"mime"
	"net/http"
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
	sessionsCtx, stopSessions := context.WithCancel(ctx)
	defer stopSessions()

	var sessionTasks sync.WaitGroup
	rpcStreamLimit := newSemaphore(server.options.MaxConcurrentStreamsPerConnection)
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
		if server.options.EnableWebTransport && r.Method == http.MethodConnect {
			if !webTransportAdmitted(server.options, r) {
				http.Error(w, "WebTransport admission denied", http.StatusForbidden)
				return
			}

			session, err := wtServer.Upgrade(w, r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			sessionTasks.Go(func() {
				handleWebTransportSession(sessionsCtx, session, server, requestLimit)
			})
			return
		}

		handleHTTP3RPC(w, r, server, requestLimit, rpcStreamLimit)
	})

	if server.options.EnableWebTransport {
		wtServer = &webtransport.Server{
			CheckOrigin: func(*http.Request) bool { return true },
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
		if server.options.GracefulShutdownTimeout > 0 {
			shutdownCtx, cancel = context.WithTimeout(shutdownCtx, server.options.GracefulShutdownTimeout)
		}
		defer cancel()
		_ = h3Server.Shutdown(shutdownCtx)
		shutdownByServer <- true
	}()

	if wtServer != nil {
		_ = wtServer.ServeQUICConn(conn)
	} else {
		_ = h3Server.ServeQUICConn(conn)
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
	waitForWaitGroup(&sessionTasks, server.options.GracefulShutdownTimeout, func() {
		if conn.Context().Err() == nil {
			conn.CloseWithError(0, "server WebTransport session drain timed out")
		}
	})

	if closeOnShutdown && ctx.Err() != nil && conn.Context().Err() == nil {
		conn.CloseWithError(0, "server drained HTTP/3 connection")
	}
}

func handleHTTP3RPC(w http.ResponseWriter, r *http.Request, server *Server, requestLimit, streamLimit semaphore) {
	if !server.options.EnableHTTP3 || r.URL.Path != http3Path(server.options) {
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
	if !http3Admitted(server.options, r) {
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

func http3Admitted(options ServerOptions, r *http.Request) bool {
	if options.HTTP3Admission == nil {
		return true
	}

	return options.HTTP3Admission(HTTP3AdmissionRequest{
		Request:   r,
		Path:      r.URL.Path,
		Method:    r.Method,
		Authority: r.Host,
		Secure:    r.TLS != nil,
	})
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
