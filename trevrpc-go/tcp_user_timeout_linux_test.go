//go:build linux

package trevrpc

import (
	"net"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// socketUserTimeout reads TCP_USER_TIMEOUT back from conn's socket.
func socketUserTimeout(t *testing.T, conn net.Conn) time.Duration {
	t.Helper()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	return rawUserTimeout(t, raw)
}

// rawUserTimeout reads TCP_USER_TIMEOUT back from a socket.
func rawUserTimeout(t *testing.T, raw syscall.RawConn) time.Duration {
	t.Helper()
	var ms int
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		ms, sockErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}); err != nil {
		t.Fatal(err)
	}
	if sockErr != nil {
		t.Fatal(sockErr)
	}
	return time.Duration(ms) * time.Millisecond
}

// TestTCPUserTimeoutReachesAcceptedAndDialedSockets checks that the Control function sets
// TCP_USER_TIMEOUT on a dialed socket, and on the sockets a listener accepts, which inherit it.
func TestTCPUserTimeoutReachesAcceptedAndDialedSockets(t *testing.T) {
	lc := tcpUserTimeoutListenConfig(7 * time.Second)
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	dialer := tcpUserTimeoutDialer(9 * time.Second)
	client, err := dialer.DialContext(t.Context(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	if got := socketUserTimeout(t, server); got != 7*time.Second {
		t.Errorf("accepted socket: TCP_USER_TIMEOUT %v, want 7s", got)
	}
	if got := socketUserTimeout(t, client); got != 9*time.Second {
		t.Errorf("dialed socket: TCP_USER_TIMEOUT %v, want 9s", got)
	}
	if tcpUserTimeoutControl(0) != nil {
		t.Error("a zero timeout sets the option")
	}
}

// TestListenSetsTCPUserTimeout checks that Listen sets TCP_USER_TIMEOUT on its TCP listener, whose
// accepted sockets inherit it, with the default of the connection idle timeout.
func TestListenSetsTCPUserTimeout(t *testing.T) {
	pki := newTestPKI(t)
	for _, tc := range []struct {
		name      string
		configure func(*ServerOptions)
		listen    ListenOptions
		want      time.Duration
	}{
		{"the connection idle timeout by default", nil, ListenOptions{}, 30 * time.Second},
		{"an explicit value", nil, ListenOptions{TCPUserTimeout: 7 * time.Second}, 7 * time.Second},
		{"disabled", nil, ListenOptions{DisableTCPUserTimeout: true}, 0},
		{"none without an idle timeout", func(o *ServerOptions) { o.DisableConnectionIdleTimeout = true }, ListenOptions{}, 0},
	} {
		server := NewServer()
		if tc.configure != nil {
			options := server.Options()
			tc.configure(&options)
			server.SetOptions(options)
		}
		listenOptions := tc.listen
		listenOptions.TLSConfig = pki.server
		listener := serveTestListener(t, server, listenOptions)
		raw, err := listener.tcp.(*net.TCPListener).SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		if tc.want == 0 {
			// Without the option the listener keeps Go's default, MPTCP where the kernel has
			// it, whose sockets don't report TCP_USER_TIMEOUT at all.
			if protocol := rawProtocol(t, raw); protocol == unix.IPPROTO_MPTCP {
				continue
			}
		} else if protocol := rawProtocol(t, raw); protocol != unix.IPPROTO_TCP {
			t.Errorf("%s: protocol %d, want TCP", tc.name, protocol)
		}
		if got := rawUserTimeout(t, raw); got != tc.want {
			t.Errorf("%s: TCP_USER_TIMEOUT %v, want %v", tc.name, got, tc.want)
		}
	}
}

// rawProtocol reads a socket's protocol: IPPROTO_TCP or IPPROTO_MPTCP.
func rawProtocol(t *testing.T, raw syscall.RawConn) int {
	t.Helper()
	var protocol int
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		protocol, sockErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PROTOCOL)
	}); err != nil {
		t.Fatal(err)
	}
	if sockErr != nil {
		t.Fatal(sockErr)
	}
	return protocol
}

// TestDialSetsTCPUserTimeout checks that Dial gives its QMux over TLS dialer TCP_USER_TIMEOUT,
// 30 seconds by default.
func TestDialSetsTCPUserTimeout(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	for _, tc := range []struct {
		name    string
		options DialOptions
		want    time.Duration
	}{
		{"30 s by default", DialOptions{}, 30 * time.Second},
		{"an explicit value", DialOptions{TCPUserTimeout: 9 * time.Second}, 9 * time.Second},
		{"disabled", DialOptions{DisableTCPUserTimeout: true}, 0},
	} {
		options := tc.options
		options.TLSConfig = pki.client
		options.Transport = TransportTCPOnly
		channel, err := Dial(t.Context(), listener.Addr().String(), options)
		if err != nil {
			t.Fatal(err)
		}
		closeOnCleanup(t, channel)
		conn, err := channel.connector.(*qmuxConnector).dialer.DialContext(t.Context(), "tcp", listener.TCPAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		if got := socketUserTimeout(t, conn); got != tc.want {
			t.Errorf("%s: TCP_USER_TIMEOUT %v, want %v", tc.name, got, tc.want)
		}
		_ = conn.Close()
	}
}
