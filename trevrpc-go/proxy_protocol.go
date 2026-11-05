package trevrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

// PROXY protocol (https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt)
// lets a load balancer that does not terminate TLS tell the server the address
// of the client it forwards. Only connections from trusted TCP proxies are read
// for a header; see ListenOptions.TrustedTCPProxies. The UDP socket's version
// of it is in proxy_protocol_udp.go.

const (
	// proxyV1MaxLength is the longest version 1 header, its CRLF included.
	proxyV1MaxLength = 107
	// proxyV2HeaderLength is the fixed part of a version 2 header: the
	// signature, the version and command, the family and transport, and the
	// length of the rest.
	proxyV2HeaderLength = 16
	// proxyReadBuffer holds a version 1 header or the fixed part and the
	// addresses of a version 2 header.
	proxyReadBuffer = 256
)

var (
	proxyV1Prefix    = []byte("PROXY ")
	proxyV2Signature = []byte("\r\n\r\n\x00\r\nQUIT\n")
)

var errProxyHeader = errors.New("invalid PROXY protocol header")

// readProxyHeaderBefore reads the PROXY header of conn, as readProxyHeader
// does, failing once ctx is done.
func readProxyHeaderBefore(ctx context.Context, conn net.Conn) (net.Conn, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Unix(1, 0)) })
	proxied, err := readProxyHeader(conn)
	if !stop() {
		return nil, context.Cause(ctx)
	}
	if err != nil {
		return nil, err
	}
	return proxied, conn.SetReadDeadline(time.Time{})
}

// readProxyHeader reads the PROXY protocol header, version 1 or 2, that must
// begin conn, and returns conn with the client address the header carries as
// its remote address. A header for the proxy's own connections, such as its
// health checks, or for an address family TCP does not use leaves the remote
// address as it is. Bytes read past the header are read again from the
// returned connection.
func readProxyHeader(conn net.Conn) (net.Conn, error) {
	reader := bufio.NewReaderSize(conn, proxyReadBuffer)
	start, err := reader.Peek(len(proxyV1Prefix))
	if err != nil {
		return nil, err
	}
	var source netip.AddrPort
	switch {
	case bytes.Equal(start, proxyV1Prefix):
		source, err = readProxyV1(reader)
	case bytes.Equal(start, proxyV2Signature[:len(proxyV1Prefix)]):
		source, err = readProxyV2(reader)
	default:
		err = errProxyHeader
	}
	if err != nil {
		return nil, err
	}
	proxied := &proxiedConn{Conn: conn, remote: conn.RemoteAddr()}
	if source.IsValid() {
		proxied.remote = net.TCPAddrFromAddrPort(source)
	}
	if buffered := reader.Buffered(); buffered > 0 {
		rest, _ := reader.Peek(buffered)
		proxied.rest = append([]byte(nil), rest...)
	}
	return proxied, nil
}

// readProxyV1 reads a version 1 header, the text line
// "PROXY TCP4|TCP6 <source> <destination> <source port> <destination port>"
// or "PROXY UNKNOWN ...", and returns its source address, or the zero
// AddrPort for UNKNOWN.
func readProxyV1(reader *bufio.Reader) (netip.AddrPort, error) {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > proxyV1MaxLength {
		return netip.AddrPort{}, errProxyHeader
	}
	if err != nil {
		return netip.AddrPort{}, err
	}
	text, ok := strings.CutSuffix(string(line), "\r\n")
	if !ok {
		return netip.AddrPort{}, errProxyHeader
	}
	fields := strings.Split(text, " ")
	if len(fields) >= 2 && fields[1] == "UNKNOWN" {
		return netip.AddrPort{}, nil
	}
	if len(fields) != 6 || (fields[1] != "TCP4" && fields[1] != "TCP6") {
		return netip.AddrPort{}, errProxyHeader
	}
	source, sourceErr := netip.ParseAddr(fields[2])
	destination, destinationErr := netip.ParseAddr(fields[3])
	sourcePort, sourcePortOK := parseProxyPort(fields[4])
	_, destinationPortOK := parseProxyPort(fields[5])
	if sourceErr != nil || destinationErr != nil || !sourcePortOK || !destinationPortOK ||
		source.Zone() != "" || destination.Zone() != "" ||
		source.Is4() != (fields[1] == "TCP4") || destination.Is4() != source.Is4() {
		return netip.AddrPort{}, errProxyHeader
	}
	return unmapAddrPort(netip.AddrPortFrom(source, sourcePort)), nil
}

// parseProxyPort parses a version 1 port number: decimal, without a sign or
// leading zeros.
func parseProxyPort(text string) (uint16, bool) {
	if text == "" || text[0] < '0' || text[0] > '9' || (len(text) > 1 && text[0] == '0') {
		return 0, false
	}
	port, err := strconv.ParseUint(text, 10, 16)
	return uint16(port), err == nil
}

// readProxyV2 reads a binary version 2 header and returns its source address,
// or the zero AddrPort for a LOCAL command or an address family other than
// TCP over IPv4 or IPv6. Type-length-value extensions are skipped.
func readProxyV2(reader *bufio.Reader) (netip.AddrPort, error) {
	header, err := reader.Peek(proxyV2HeaderLength)
	if err != nil {
		return netip.AddrPort{}, err
	}
	addressLength, length, err := parseProxyV2Fixed(header, proxyV2Stream)
	if err != nil {
		return netip.AddrPort{}, err
	}
	var source netip.AddrPort
	if addressLength > 0 {
		block, err := reader.Peek(proxyV2HeaderLength + addressLength)
		if err != nil {
			return netip.AddrPort{}, err
		}
		source = proxyV2Source(block[proxyV2HeaderLength:])
	}
	if _, err := reader.Discard(length); err != nil {
		return netip.AddrPort{}, err
	}
	return source, nil
}

// The transports of a version 2 header's address family byte.
const (
	proxyV2Stream   = 1
	proxyV2Datagram = 2
)

// parseProxyV2Fixed checks header, the fixed part of a version 2 header, and
// returns the length of its address block for a PROXY command over transport
// on IPv4 or IPv6, or 0 when the header carries no client address for it (a
// LOCAL command or another family), and the length of the whole header.
func parseProxyV2Fixed(header []byte, transport byte) (addressLength, length int, err error) {
	if len(header) < proxyV2HeaderLength || !bytes.Equal(header[:len(proxyV2Signature)], proxyV2Signature) || header[12]>>4 != 2 {
		return 0, 0, errProxyHeader
	}
	command, family := header[12]&0x0f, header[13]
	length = proxyV2HeaderLength + int(binary.BigEndian.Uint16(header[14:16]))
	switch {
	case command > 1:
		return 0, 0, errProxyHeader
	case command == 1 && family == 0x10|transport: // PROXY over IPv4
		addressLength = 12
	case command == 1 && family == 0x20|transport: // PROXY over IPv6
		addressLength = 36
	}
	if length < proxyV2HeaderLength+addressLength {
		return 0, 0, errProxyHeader
	}
	return addressLength, length, nil
}

// proxyV2Source returns the source address of a version 2 address block of
// 12 (IPv4) or 36 (IPv6) bytes, IPv4-mapped IPv6 addresses as IPv4.
func proxyV2Source(addresses []byte) netip.AddrPort {
	ipLength := 4
	if len(addresses) >= 36 {
		ipLength = 16
	}
	ip, _ := netip.AddrFromSlice(addresses[:ipLength])
	port := binary.BigEndian.Uint16(addresses[2*ipLength:])
	return unmapAddrPort(netip.AddrPortFrom(ip, port))
}

// proxiedConn is a TCP connection from a trusted proxy, whose remote address
// is the client's from the PROXY header.
type proxiedConn struct {
	net.Conn
	remote net.Addr
	// rest holds the bytes read past the header, which are read first.
	rest []byte
}

func (c *proxiedConn) Read(p []byte) (int, error) {
	if len(c.rest) > 0 {
		n := copy(p, c.rest)
		c.rest = c.rest[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

func (c *proxiedConn) RemoteAddr() net.Addr { return c.remote }

// CloseWrite half-closes the TCP connection, as QMux does when its session
// ends.
func (c *proxiedConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return nil
}

// trustedProxy reports whether addr, the remote address of a TCP connection,
// belongs to one of the trusted proxies.
func trustedProxy(trusted []netip.Prefix, addr net.Addr) bool {
	return len(trusted) > 0 && trustedProxyIP(trusted, addrPortOf(addr).Addr())
}

// trustedProxyIP reports whether ip, an IPv4 address unmapped from IPv6,
// belongs to one of the trusted proxies.
func trustedProxyIP(trusted []netip.Prefix, ip netip.Addr) bool {
	for _, prefix := range trusted {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

// trustedProxyPrefixes validates the trusted proxy networks and returns them
// masked, IPv4 networks written as IPv4-mapped IPv6 ones as IPv4, so they
// match the IPv4 addresses remote addresses are reported as.
func trustedProxyPrefixes(prefixes []netip.Prefix) ([]netip.Prefix, error) {
	masked := make([]netip.Prefix, 0, len(prefixes))
	for _, prefix := range prefixes {
		if !prefix.IsValid() {
			return nil, InvalidArgument("invalid trusted proxy network " + prefix.String())
		}
		if addr := prefix.Addr(); addr.Is4In6() && prefix.Bits() >= 96 {
			prefix = netip.PrefixFrom(addr.Unmap(), prefix.Bits()-96)
		}
		masked = append(masked, prefix.Masked())
	}
	return masked, nil
}
