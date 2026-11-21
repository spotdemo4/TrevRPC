//go:build !linux

package trevrpc

import "net"

// tcpProgress reports nothing: only Linux exposes the TCP_INFO it reads.
func tcpProgress(net.Conn) string { return "" }
