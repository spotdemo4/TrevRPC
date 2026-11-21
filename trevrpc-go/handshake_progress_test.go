package trevrpc

import (
	"crypto/tls"
	"net"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// A failed TLS handshake on the TCP listener reports how long after accept it
// ended and, on Linux, how far the connection got, so that a connection that
// waited to be accepted can be told from a client that stalled mid-handshake.
func TestTLSHandshakeFailureReportsProgress(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	diagnostics := make(chan ServerDiagnostic, 16)
	server.SetDiagnostics(func(diagnostic ServerDiagnostic) {
		if diagnostic.Phase == ServerDiagnosticTLSHandshakeFailed {
			diagnostics <- diagnostic
		}
	})
	registerTestService(server)
	addr := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server}).TCPAddr().String()
	pattern := regexp.MustCompile(`^ended (\d+) ms after accept(?:; received (\d+) bytes, the last (\d+) ms ago; sent (\d+) bytes, (\d+) acknowledged)?$`)
	next := func() []uint64 {
		t.Helper()
		select {
		case diagnostic := <-diagnostics:
			match := pattern.FindStringSubmatch(diagnostic.Message)
			if match == nil {
				t.Fatalf("message %q does not match %s", diagnostic.Message, pattern)
			}
			if runtime.GOOS == "linux" && match[2] == "" {
				t.Fatalf("message %q has no TCP progress on Linux", diagnostic.Message)
			}
			values := make([]uint64, 0, len(match)-1)
			for _, field := range match[1:] {
				value, _ := strconv.ParseUint(field, 10, 64)
				values = append(values, value)
			}
			return values
		case <-time.After(rpcTestTimeout):
			t.Fatal("no tls_handshake_failed diagnostic")
			return nil
		}
	}

	// A client that never sends a ClientHello: the server received nothing but,
	// sometimes, the FIN's sequence number, and sent nothing, and the
	// connection's age shows as the time since data arrived.
	silent, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// The server accepts the connection, and the kernel stamps it established,
	// up to a little after the sleep starts, and the kernel's clock ticks in
	// jiffies, so the bounds allow some slack below the sleep. A connection
	// that did not wait would report about 0 ms.
	const idle = 200 * time.Millisecond
	const least = uint64((idle - 50*time.Millisecond) / time.Millisecond)
	time.Sleep(idle)
	_ = silent.Close()
	got := next()
	if got[0] < least {
		t.Errorf("silent client: ended %d ms after accept, want at least %d", got[0], least)
	}
	if runtime.GOOS == "linux" {
		if received, last, sent := got[1], got[2], got[3]; received > 1 || sent != 0 || last < least {
			t.Errorf("silent client: received %d bytes, the last %d ms ago, sent %d; want at most 1, at least %d, 0", received, last, sent, least)
		}
	}

	// A client that rejects the server's certificate: the server received the
	// ClientHello and the client acknowledged the server's flight.
	untrusting := &tls.Config{NextProtos: []string{QMuxALPN}, ServerName: "localhost"}
	if conn, err := (&tls.Dialer{Config: untrusting}).DialContext(t.Context(), "tcp", addr); err == nil {
		_ = conn.Close()
		t.Fatal("handshake succeeded without trusting the server certificate")
	}
	got = next()
	if runtime.GOOS == "linux" {
		if received, sent, acknowledged := got[1], got[3], got[4]; received == 0 || sent == 0 || acknowledged == 0 {
			t.Errorf("rejecting client: received %d bytes, sent %d, %d acknowledged; want all above 0", received, sent, acknowledged)
		}
	}
}
