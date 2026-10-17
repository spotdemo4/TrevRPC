package trevrpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/coder/websocket"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

// isWebSocketUpgrade reports whether r asks to open a WebSocket.
func isWebSocketUpgrade(r *http.Request) bool {
	return r.Method == http.MethodGet && headerHasToken(r.Header, "Connection", "upgrade") &&
		headerHasToken(r.Header, "Upgrade", "websocket")
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for item := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(item), token) {
				return true
			}
		}
	}
	return false
}

// offersSubprotocol reports whether a WebSocket request offers protocol.
func offersSubprotocol(r *http.Request, protocol string) bool {
	for _, value := range r.Header.Values("Sec-WebSocket-Protocol") {
		for item := range strings.SplitSeq(value, ",") {
			if strings.TrimSpace(item) == protocol {
				return true
			}
		}
	}
	return false
}

// handleWebSocket admits and upgrades a WebSocket request, then serves RPCs on
// the streams of its QMux session until the session ends.
func (l *Listener) handleWebSocket(w http.ResponseWriter, r *http.Request, h *httpTCPServer) {
	runtime := l.runtime
	if !offersSubprotocol(r, QMuxALPN) {
		http.Error(w, "WebSocket subprotocol "+QMuxALPN+" is required", http.StatusBadRequest)
		return
	}
	if !runtime.admitBrowserSession(w, r) {
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{QMuxALPN},
		// Admission has already decided which origins are allowed.
		InsecureSkipVerify: true,
	})
	if err != nil {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionUpgrade, Err: err, RemoteAddr: r.RemoteAddr})
		return
	}
	// The connection was hijacked: it is done once the session is.
	if state, ok := r.Context().Value(httpConnStateKey{}).(*httpConnState); ok {
		defer h.finished(state.conn)
	}
	conn.SetReadLimit(qmuxMaxRecordSize)
	runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticSessionUpgradeSuccess, Message: "accepted", RemoteAddr: r.RemoteAddr})

	ctx, cancel := context.WithTimeout(l.ctx, l.handshakeTimeout)
	session, err := l.openServerQMux(qmux.NewMessageConn(newWebSocketMessages(conn), qmuxMaxRecordSize), ctx)
	cancel()
	if err != nil {
		_ = conn.CloseNow()
		return
	}
	l.serveQMuxSession(session, func(err error) {
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:      ServerDiagnosticSessionClosed,
			Message:    err.Error(),
			Err:        err,
			RemoteAddr: r.RemoteAddr,
		})
	})
}

// webSocketMessages carries QMux records as binary WebSocket messages.
type webSocketMessages struct {
	conn   *websocket.Conn
	buffer []byte
}

func newWebSocketMessages(conn *websocket.Conn) *webSocketMessages {
	return &webSocketMessages{conn: conn}
}

var errTextMessage = errors.New("QMux over WebSocket forbids text messages")

func (m *webSocketMessages) ReadMessage() ([]byte, error) {
	kind, reader, err := m.conn.Reader(context.Background())
	if err != nil {
		return nil, err
	}
	if kind != websocket.MessageBinary {
		_ = m.conn.Close(websocket.StatusUnsupportedData, errTextMessage.Error())
		return nil, errTextMessage
	}
	m.buffer = m.buffer[:0]
	for {
		if len(m.buffer) == cap(m.buffer) {
			m.buffer = slices.Grow(m.buffer, max(512, cap(m.buffer)))
		}
		n, err := reader.Read(m.buffer[len(m.buffer):cap(m.buffer)])
		m.buffer = m.buffer[:len(m.buffer)+n]
		if errors.Is(err, io.EOF) {
			return m.buffer, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (m *webSocketMessages) WriteMessage(message []byte) error {
	return m.conn.Write(context.Background(), websocket.MessageBinary, message)
}

func (m *webSocketMessages) Close() error { return m.conn.Close(websocket.StatusNormalClosure, "") }

func (m *webSocketMessages) CloseNow() error { return m.conn.CloseNow() }
