package trevrpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
)

// proxyTestPeer is the address a trusted proxy connects from in the parser
// tests.
var proxyTestPeer = &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1000}

// readerConn is a connection whose reads come from a reader.
type readerConn struct {
	net.Conn
	reader io.Reader
}

func (c readerConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (readerConn) RemoteAddr() net.Addr         { return proxyTestPeer }

// proxyV2Header builds a version 2 header with the given version and command
// byte, family and transport byte, and the rest of the header.
func proxyV2Header(versionCommand, family byte, rest []byte) []byte {
	header := append([]byte(nil), proxyV2Signature...)
	header = append(header, versionCommand, family)
	header = binary.BigEndian.AppendUint16(header, uint16(len(rest)))
	return append(header, rest...)
}

// proxyV2Addresses returns the address block of a header for TCP from source
// to destination.
func proxyV2Addresses(source, destination netip.AddrPort) []byte {
	block := append(source.Addr().AsSlice(), destination.Addr().AsSlice()...)
	block = binary.BigEndian.AppendUint16(block, source.Port())
	return binary.BigEndian.AppendUint16(block, destination.Port())
}

func TestReadProxyHeader(t *testing.T) {
	ipv4 := proxyV2Addresses(netip.MustParseAddrPort("192.0.2.1:56324"), netip.MustParseAddrPort("198.51.100.1:443"))
	ipv6 := proxyV2Addresses(netip.MustParseAddrPort("[2001:db8::1]:8080"), netip.MustParseAddrPort("[2001:db8::2]:443"))
	tlv := []byte{0x04, 0x00, 0x03, 'a', 'b', 'c'} // PP2_TYPE_NOOP
	peer := proxyTestPeer.String()
	for _, test := range []struct {
		name   string
		header []byte
		want   string // the remote address, or "" for an error
	}{
		{"v1 TCP4", []byte("PROXY TCP4 192.0.2.1 198.51.100.1 56324 443\r\n"), "192.0.2.1:56324"},
		{"v1 TCP6", []byte("PROXY TCP6 2001:db8::1 2001:db8::2 8080 443\r\n"), "[2001:db8::1]:8080"},
		{"v1 TCP6 mapped IPv4", []byte("PROXY TCP6 ::ffff:192.0.2.1 ::ffff:198.51.100.1 1 2\r\n"), "192.0.2.1:1"},
		{"v1 UNKNOWN", []byte("PROXY UNKNOWN\r\n"), peer},
		{"v1 UNKNOWN with addresses", []byte("PROXY UNKNOWN 192.0.2.1 198.51.100.1 1 2\r\n"), peer},
		{"v1 longest", []byte("PROXY TCP6 ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff 65535 65535\r\n"), "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"},
		{"v1 too long", []byte("PROXY UNKNOWN " + strings.Repeat("x", 100) + "\r\n"), ""},
		{"v1 without CRLF", []byte("PROXY " + strings.Repeat("x", 300)), ""},
		{"v1 LF only", []byte("PROXY TCP4 192.0.2.1 198.51.100.1 56324 443\n"), ""},
		{"v1 family mismatch", []byte("PROXY TCP4 2001:db8::1 2001:db8::2 1 2\r\n"), ""},
		{"v1 mixed families", []byte("PROXY TCP4 192.0.2.1 2001:db8::2 1 2\r\n"), ""},
		{"v1 port out of range", []byte("PROXY TCP4 192.0.2.1 198.51.100.1 65536 443\r\n"), ""},
		{"v1 port leading zero", []byte("PROXY TCP4 192.0.2.1 198.51.100.1 01 443\r\n"), ""},
		{"v1 port sign", []byte("PROXY TCP4 192.0.2.1 198.51.100.1 +1 443\r\n"), ""},
		{"v1 missing field", []byte("PROXY TCP4 192.0.2.1 198.51.100.1 56324\r\n"), ""},
		{"v1 unknown protocol", []byte("PROXY UDP4 192.0.2.1 198.51.100.1 1 2\r\n"), ""},
		{"v1 zone", []byte("PROXY TCP6 fe80::1%eth0 fe80::2 1 2\r\n"), ""},
		{"v2 PROXY IPv4", proxyV2Header(0x21, 0x11, ipv4), "192.0.2.1:56324"},
		{"v2 PROXY IPv6", proxyV2Header(0x21, 0x21, ipv6), "[2001:db8::1]:8080"},
		{"v2 extensions", proxyV2Header(0x21, 0x11, append(ipv4, tlv...)), "192.0.2.1:56324"},
		{"v2 LOCAL", proxyV2Header(0x20, 0x00, nil), peer},
		{"v2 LOCAL with addresses", proxyV2Header(0x20, 0x11, ipv4), peer},
		{"v2 UNSPEC", proxyV2Header(0x21, 0x00, tlv), peer},
		{"v2 UDP", proxyV2Header(0x21, 0x12, ipv4), peer},
		{"v2 UNIX", proxyV2Header(0x21, 0x31, make([]byte, 216)), peer},
		{"v2 longest", proxyV2Header(0x21, 0x11, append(ipv4, make([]byte, 65535-len(ipv4))...)), "192.0.2.1:56324"},
		{"v2 version 1", proxyV2Header(0x11, 0x11, ipv4), ""},
		{"v2 unknown command", proxyV2Header(0x22, 0x11, ipv4), ""},
		{"v2 short IPv4 block", proxyV2Header(0x21, 0x11, ipv4[:8]), ""},
		{"v2 short IPv6 block", proxyV2Header(0x21, 0x21, ipv4), ""},
		{"v2 bad signature", append([]byte("\r\n\r\n\x00\r\nQUIX\n"), 0x21, 0x11, 0, 0), ""},
		{"TLS", []byte{0x16, 0x03, 0x01, 0x00, 0x05, 0x01, 0x00, 0x00, 0x01, 0x00}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			const tls = "\x16\x03\x01 the TLS handshake"
			conn, err := readProxyHeader(readerConn{reader: bytes.NewReader(append(test.header, tls...))})
			if test.want == "" {
				if err == nil {
					t.Fatalf("accepted %q as the client %v", test.header, conn.RemoteAddr())
				}
				return
			}
			if err != nil {
				t.Fatalf("readProxyHeader: %v", err)
			}
			if got := conn.RemoteAddr().String(); got != test.want {
				t.Errorf("remote address %s, want %s", got, test.want)
			}
			// What follows the header goes to TLS.
			if rest, _ := io.ReadAll(conn); string(rest) != tls {
				t.Errorf("read %q after the header, want %q", rest, tls)
			}
		})
	}
}

func TestReadProxyHeaderFailsOnTruncatedHeaders(t *testing.T) {
	ipv4 := proxyV2Addresses(netip.MustParseAddrPort("192.0.2.1:1"), netip.MustParseAddrPort("198.51.100.1:2"))
	for _, header := range [][]byte{
		[]byte("PRO"),
		[]byte("PROXY TCP4 192.0.2.1"),
		proxyV2Header(0x21, 0x11, ipv4)[:20],
		proxyV2Header(0x21, 0x11, append(ipv4, make([]byte, 20)...))[:40],
	} {
		_, err := readProxyHeader(readerConn{reader: bytes.NewReader(header)})
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("header %q: error %v, want an end of file", header, err)
		}
	}
}

func TestTrustedProxyPrefixes(t *testing.T) {
	got, err := trustedProxyPrefixes([]netip.Prefix{
		netip.MustParsePrefix("10.1.2.3/8"),
		netip.MustParsePrefix("::ffff:192.168.0.0/112"),
		netip.MustParsePrefix("2001:db8::1/32"),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("192.168.0.0/16"),
		netip.MustParsePrefix("2001:db8::/32"),
	}
	if len(got) != len(want) {
		t.Fatalf("prefixes %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("prefix %d is %v, want %v", i, got[i], want[i])
		}
	}
	if _, err := trustedProxyPrefixes([]netip.Prefix{{}}); err == nil {
		t.Error("accepted the zero prefix")
	}

	mapped := &net.TCPAddr{IP: net.ParseIP("::ffff:192.168.1.1"), Port: 1}
	if !trustedProxy(got, mapped) {
		t.Error("an IPv4-mapped peer does not match its IPv4 network")
	}
	if trustedProxy(got, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}) {
		t.Error("an untrusted peer is trusted")
	}
}
