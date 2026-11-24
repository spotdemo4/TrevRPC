package trevrpc

import (
	"net/http"
	"net/netip"
	"strings"
)

// rejectMisdirected answers 421 Misdirected Request (RFC 9110 §15.5.20) to an
// HTTP request for another name than its connection's TLS server name, and
// reports whether it did. Browsers send such requests over a connection whose
// certificate also covers the name they want (connection coalescing, RFC 9113
// §9.1.1 and RFC 9114 §3.3); a 421 makes them retry on a connection of its
// own, which a proxy that routes by server name sends to the right server.
func (l *Listener) rejectMisdirected(w http.ResponseWriter, r *http.Request) bool {
	if l.runtime.options.DisableMisdirectedRequestCheck || r.TLS == nil || !misdirected(r.Host, r.TLS.ServerName) {
		return false
	}
	l.runtime.emitDiagnostic(ServerDiagnostic{
		Phase:      ServerDiagnosticMisdirectedRequest,
		Message:    "host " + truncateDiagnostic(r.Host) + " on a connection for " + truncateDiagnostic(r.TLS.ServerName),
		RemoteAddr: r.RemoteAddr,
	})
	http.Error(w, "misdirected request: this connection serves "+r.TLS.ServerName, http.StatusMisdirectedRequest)
	return true
}

// misdirected reports whether a request for host, the authority of an HTTP
// request, came over a connection whose client asked for serverName, its TLS
// server name. The port, a trailing dot and case do not count. Browsers
// coalesce connections between names only, so neither a connection without a
// server name, as clients open to an IP address, nor a request without a host
// or for an IP address, which a client sends when it reaches the server by
// address and names it only in TLS, is ever misdirected. A server name that is
// an IP address, which RFC 6066 forbids but some clients send, counts as none.
func misdirected(host, serverName string) bool {
	host = authorityHost(host)
	if serverName == "" || host == "" || isIPAddress(serverName) || isIPAddress(host) {
		return false
	}
	return !strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(serverName, "."))
}

// authorityHost returns the host of an HTTP authority without its port and
// the brackets of an IPv6 address.
func authorityHost(authority string) string {
	if rest, ok := strings.CutPrefix(authority, "["); ok {
		host, _, _ := strings.Cut(rest, "]")
		return host
	}
	if host, _, ok := strings.Cut(authority, ":"); ok {
		return host
	}
	return authority
}

func isIPAddress(host string) bool {
	_, err := netip.ParseAddr(host)
	return err == nil
}
