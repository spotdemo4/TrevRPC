package trevrpc

import (
	"context"
	"errors"
	"net/http"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// setupHTTP3 prepares the HTTP/3 server shared by every h3 connection. Its
// connections are accepted by the Listener and handed over with ServeQUICConn.
func (l *Listener) setupHTTP3() {
	options := l.runtime.options
	l.h3 = &http3.Server{
		Handler: http.HandlerFunc(l.handleHTTP3),
		ConnContext: func(ctx context.Context, _ *quic.Conn) context.Context {
			state := &httpConnState{streamLimit: newSemaphore(options.MaxConcurrentStreamsPerConnection)}
			return context.WithValue(ctx, httpConnStateKey{}, state)
		},
	}
	if options.EnableWebTransport {
		// Admission decides which sessions are allowed; the same-origin default
		// would reject browsers served from another origin.
		l.wt = &webtransport.Server{
			H3:          l.h3,
			Config:      serverWebTransportConfig(options),
			CheckOrigin: func(*http.Request) bool { return true },
			// Selects webTransportProtocol for clients that offer it, through
			// WT-Protocol on the CONNECT response.
			ApplicationProtocols: []string{webTransportProtocol},
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
// The servers are captured first: a retried bind replaces them while these
// goroutines may still run.
func (l *Listener) closeHTTP3() {
	switch wt, h3 := l.wt, l.h3; {
	case wt != nil:
		go func() { _ = wt.Close() }()
	case h3 != nil:
		go func() { _ = h3.Close() }()
	}
}

func (l *Listener) handleHTTP3(w http.ResponseWriter, r *http.Request) {
	if !l.runtime.options.claimsPath(r.URL.Path) {
		l.serveFallback(w, r)
		return
	}
	if r.Method == http.MethodConnect && l.wt != nil {
		l.handleWebTransport(w, r)
		return
	}
	l.handleHTTPRPC(w, r)
}
