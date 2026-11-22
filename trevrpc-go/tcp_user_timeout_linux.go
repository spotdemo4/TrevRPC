//go:build linux

package trevrpc

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// setTCPUserTimeout sets TCP_USER_TIMEOUT, in milliseconds, on the socket.
func setTCPUserTimeout(c syscall.RawConn, timeout time.Duration) error {
	var err error
	if controlErr := c.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(timeout.Milliseconds()))
	}); controlErr != nil {
		return controlErr
	}
	return err
}
