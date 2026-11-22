package trevrpc

import (
	"net"
	"syscall"
	"time"
)

// defaultTCPUserTimeout bounds unacknowledged data on a client's TCP connection by default: the
// server's default connection idle timeout.
const defaultTCPUserTimeout = 30 * time.Second

// tcpUserTimeoutControl returns a net.ListenConfig or net.Dialer Control function that sets
// TCP_USER_TIMEOUT on the socket, or nil when timeout is zero.
//
// QMux restarts its idle timeout on every record sent (draft-ietf-quic-qmux-02 section 7.1), so
// keep-alives keep a session to a vanished peer open for as long as its socket takes writes. With
// TCP_USER_TIMEOUT the kernel aborts a connection whose data stays unacknowledged that long, which
// is what a vanished peer leaves behind. Sockets a listener accepts inherit it.
func tcpUserTimeoutControl(timeout time.Duration) func(network, address string, c syscall.RawConn) error {
	if timeout == 0 {
		return nil
	}
	return func(_, _ string, c syscall.RawConn) error {
		return setTCPUserTimeout(c, timeout)
	}
}

// tcpUserTimeoutListenConfig is the net.ListenConfig of a listener whose sockets get
// TCP_USER_TIMEOUT. Go listens with MPTCP by default on Linux, but MPTCP sockets refuse the
// option, and Go falls back to TCP quietly when the Control function fails; so a listener with the
// timeout asks for TCP outright, and one without keeps Go's default.
func tcpUserTimeoutListenConfig(timeout time.Duration) net.ListenConfig {
	lc := net.ListenConfig{Control: tcpUserTimeoutControl(timeout)}
	if timeout > 0 {
		lc.SetMultipathTCP(false)
	}
	return lc
}

// tcpUserTimeoutDialer is the net.Dialer of connections that get TCP_USER_TIMEOUT, over TCP as
// tcpUserTimeoutListenConfig explains.
func tcpUserTimeoutDialer(timeout time.Duration) net.Dialer {
	dialer := net.Dialer{Control: tcpUserTimeoutControl(timeout)}
	if timeout > 0 {
		dialer.SetMultipathTCP(false)
	}
	return dialer
}

// resolveTCPUserTimeout applies the defaults of the TCPUserTimeout options: zero means fallback,
// and disabled means none.
func resolveTCPUserTimeout(name string, timeout, fallback time.Duration, disabled bool) (time.Duration, error) {
	switch {
	case timeout < 0:
		return 0, InvalidArgument(name + " must not be negative")
	case disabled:
		return 0, nil
	case timeout == 0:
		return fallback, nil
	default:
		return timeout, nil
	}
}
