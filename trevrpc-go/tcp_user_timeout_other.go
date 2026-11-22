//go:build !linux

package trevrpc

import (
	"syscall"
	"time"
)

// setTCPUserTimeout does nothing: TCP_USER_TIMEOUT is a Linux socket option.
func setTCPUserTimeout(syscall.RawConn, time.Duration) error { return nil }
