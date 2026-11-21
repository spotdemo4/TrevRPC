package trevrpc

import (
	"net"
	"strconv"
	"time"
)

// handshakeFailureMessage is the Message of a ServerDiagnosticTLSHandshakeFailed
// diagnostic: how long after the listener accepted conn its handshake ended,
// and, where the platform reports it, how far the TCP connection got (see
// tcpProgress). Together they tell a connection that waited to be accepted
// from a client or server that stalled during the handshake. conn is the
// accepted socket, not a wrapper around it.
func handshakeFailureMessage(conn net.Conn, accepted time.Time) string {
	message := "ended " + strconv.FormatInt(time.Since(accepted).Milliseconds(), 10) + " ms after accept"
	if progress := tcpProgress(conn); progress != "" {
		message += "; " + progress
	}
	return message
}
