package trevrpc

import (
	"net"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// tcpProgress reports, from the kernel's TCP_INFO for conn, the bytes it
// received and how long ago the last of them arrived, and the bytes it sent
// and the peer acknowledged. The received count may include 1 for the peer's
// FIN, which takes a sequence number. Before any data arrives, the kernel
// counts that age from when the connection was established, so it also shows
// how long a connection waited to be accepted. It is empty when conn is not a
// socket or is already closed, as it is after the handshake's context ended
// it.
func tcpProgress(conn net.Conn) string {
	socket, ok := conn.(syscall.Conn)
	if !ok {
		return ""
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return ""
	}
	var info *unix.TCPInfo
	var infoErr error
	if err := raw.Control(func(fd uintptr) {
		info, infoErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	}); err != nil || infoErr != nil {
		return ""
	}
	return "received " + strconv.FormatUint(info.Bytes_received, 10) +
		" bytes, the last " + strconv.FormatUint(uint64(info.Last_data_recv), 10) +
		" ms ago; sent " + strconv.FormatUint(info.Bytes_sent, 10) +
		" bytes, " + strconv.FormatUint(info.Bytes_acked, 10) + " acknowledged"
}
