package trevrpc

import (
	"net"
	"net/netip"
)

// remoteAddrStream is implemented by the server streams that know the
// address of the client of their connection or session.
type remoteAddrStream interface {
	trevrpcRemoteAddr() netip.AddrPort
}

// streamRemoteAddr returns the client address of the connection stream
// belongs to, or the zero AddrPort when it is not known.
func streamRemoteAddr(stream rpcStream) netip.AddrPort {
	if remote, ok := stream.(remoteAddrStream); ok {
		return remote.trevrpcRemoteAddr()
	}
	return netip.AddrPort{}
}

// addrPortOf returns addr as an AddrPort, IPv4-mapped IPv6 addresses as IPv4,
// or the zero AddrPort when addr is not an IP address.
func addrPortOf(addr net.Addr) netip.AddrPort {
	var addrPort netip.AddrPort
	switch addr := addr.(type) {
	case *net.TCPAddr:
		addrPort = addr.AddrPort()
	case *net.UDPAddr:
		addrPort = addr.AddrPort()
	case nil:
	default:
		addrPort, _ = netip.ParseAddrPort(addr.String())
	}
	return unmapAddrPort(addrPort)
}

// remoteAddrOf returns the remote address of an HTTP request, as addrPortOf
// does.
func remoteAddrOf(remoteAddr string) netip.AddrPort {
	addrPort, _ := netip.ParseAddrPort(remoteAddr)
	return unmapAddrPort(addrPort)
}

func unmapAddrPort(addrPort netip.AddrPort) netip.AddrPort {
	if addrPort.Addr().Is4In6() {
		return netip.AddrPortFrom(addrPort.Addr().Unmap(), addrPort.Port())
	}
	return addrPort
}
