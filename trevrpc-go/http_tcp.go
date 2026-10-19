package trevrpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"sync"
	"time"
)

// httpTCPServer serves HTTP/2 RPCs, WebSocket sessions and fallback requests
// on the TLS connections of the TCP listener. Each connection is handed over
// after its TLS handshake and stays tracked by the listener until it ends.
type httpTCPServer struct {
	listener *Listener
	server   *http.Server
	queue    *connQueue
	// altSvc advertises HTTP/3 on the UDP socket to HTTP/2 and fallback
	// clients.
	altSvc       string
	conns        sync.Map // net.Conn -> chan struct{} closed when the connection ends
	shutdownOnce sync.Once
}

func newHTTPTCPServer(l *Listener) *httpTCPServer {
	options := l.runtime.options
	h := &httpTCPServer{listener: l, queue: newConnQueue(l.tcp.Addr())}
	if options.servesHTTP2() {
		h.altSvc = fmt.Sprintf(`h3=":%d"; ma=86400`, l.udp.LocalAddr().(*net.UDPAddr).Port)
	}
	streamWindow := frameReceiveWindow(options.MaxFrameSize)
	h.server = &http.Server{
		Handler:           http.HandlerFunc(h.handle),
		ReadHeaderTimeout: l.handshakeTimeout,
		IdleTimeout:       options.ConnectionIdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return l.ctx },
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			state := &httpConnState{streamLimit: newSemaphore(options.MaxConcurrentStreamsPerConnection), conn: conn}
			return context.WithValue(ctx, httpConnStateKey{}, state)
		},
		ConnState: func(conn net.Conn, state http.ConnState) {
			// A hijacked WebSocket connection ends when its session does.
			if state == http.StateClosed {
				h.finished(conn)
			}
		},
		// Extended CONNECT (RFC 8441) stays off, so browsers open WebSocket
		// sessions over HTTP/1.1, the only binding served.
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          http2StreamLimit(options),
			MaxReceiveBufferPerStream:     clampInt32(streamWindow),
			MaxReceiveBufferPerConnection: clampInt32(connectionReceiveWindow(options, streamWindow)),
			SendPingTimeout:               options.ConnectionIdleTimeout,
		},
		ErrorLog: log.New(io.Discard, "", 0),
	}
	go func() { _ = h.server.Serve(h.queue) }()
	return h
}

func clampInt32(value uint64) int { return int(min(value, math.MaxInt32)) }

// serveConn serves one TLS connection until it ends.
func (h *httpTCPServer) serveConn(conn net.Conn) {
	done := make(chan struct{})
	h.conns.Store(conn, done)
	if !h.queue.push(conn) {
		h.conns.Delete(conn)
		_ = conn.Close()
		return
	}
	<-done
}

// finished reports that conn has ended.
func (h *httpTCPServer) finished(conn net.Conn) {
	if done, ok := h.conns.LoadAndDelete(conn); ok {
		close(done.(chan struct{}))
	}
}

func (h *httpTCPServer) handle(w http.ResponseWriter, r *http.Request) {
	l := h.listener
	if !l.runtime.options.claimsPath(r.URL.Path) {
		h.setAltSvc(w)
		l.serveFallback(w, r)
		return
	}
	if r.ProtoMajor == 1 {
		if l.runtime.options.EnableWebTransport && isWebSocketUpgrade(r) {
			l.handleWebSocket(w, r, h)
			return
		}
		http.Error(w, "TrevRPC over HTTP requires HTTP/2 or HTTP/3", http.StatusHTTPVersionNotSupported)
		return
	}
	h.setAltSvc(w)
	l.handleHTTPRPC(w, r)
}

// setAltSvc advertises HTTP/3 on a response, when it is served. A handler can
// still replace the header.
func (h *httpTCPServer) setAltSvc(w http.ResponseWriter) {
	if h.altSvc != "" {
		w.Header().Set("Alt-Svc", h.altSvc)
	}
}

// shutdown stops serving: HTTP/2 clients receive GOAWAY, and idle
// connections are closed.
func (h *httpTCPServer) shutdown(timeout time.Duration) {
	h.shutdownOnce.Do(func() {
		go func() {
			ctx := context.Background()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			_ = h.server.Shutdown(ctx)
		}()
	})
}

// close closes every connection the HTTP server still owns.
func (h *httpTCPServer) close() {
	_ = h.server.Close()
	_ = h.queue.Close()
}

// connQueue is the net.Listener of the HTTP server: the TCP listener hands it
// connections whose TLS handshake has completed.
type connQueue struct {
	addr      net.Addr
	conns     chan net.Conn
	closed    chan struct{}
	closeOnce sync.Once
}

func newConnQueue(addr net.Addr) *connQueue {
	return &connQueue{addr: addr, conns: make(chan net.Conn), closed: make(chan struct{})}
}

// push hands conn to the HTTP server. It fails once the queue is closed.
func (q *connQueue) push(conn net.Conn) bool {
	select {
	case q.conns <- conn:
		return true
	case <-q.closed:
		return false
	}
}

func (q *connQueue) Accept() (net.Conn, error) {
	select {
	case conn := <-q.conns:
		return conn, nil
	case <-q.closed:
		return nil, net.ErrClosed
	}
}

func (q *connQueue) Close() error {
	q.closeOnce.Do(func() { close(q.closed) })
	return nil
}

func (q *connQueue) Addr() net.Addr { return q.addr }
