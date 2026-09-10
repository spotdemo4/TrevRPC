package transport

import (
	"net"
	"strconv"
)

// Backend identifies the implementation used for a transport endpoint.
type Backend uint8

const (
	BackendAuto Backend = iota
	BackendNative
	BackendQUICGo
)

// Protocol identifies the wire protocol carried by a transport endpoint.
type Protocol uint8

const (
	ProtocolNativeQUIC Protocol = iota
	ProtocolHTTP3
	ProtocolWebTransport
)

// Credentials contains copied TLS identity and verification material.
type Credentials struct {
	CertificateChainPEM      []byte
	PrivateKeyPEM            []byte
	RootCAPEM                []byte
	ServerName               string
	InsecureSkipVerify       bool
	RequireClientCertificate bool
}

// Clone returns an independent credentials value.
func (c Credentials) Clone() Credentials {
	c.CertificateChainPEM = append([]byte(nil), c.CertificateChainPEM...)
	c.PrivateKeyPEM = append([]byte(nil), c.PrivateKeyPEM...)
	c.RootCAPEM = append([]byte(nil), c.RootCAPEM...)
	return c
}

// Limits contains backend-neutral flow-control and stream limits.
type Limits struct {
	StreamReceiveWindow          uint64
	ConnectionReceiveWindow      uint64
	IncomingBidirectionalStreams int64
}

// HeaderField is one ordered HTTP field.
type HeaderField struct {
	Name  string
	Value string
}

// HeaderFields preserves field order and duplicate names.
type HeaderFields []HeaderField

// Clone returns an independent header list.
func (fields HeaderFields) Clone() HeaderFields {
	return append(HeaderFields(nil), fields...)
}

// Address is a copied transport address.
type Address struct {
	Network string
	Host    string
	Port    uint16
	Zone    string
}

// AddressFromNet copies an address without retaining backend-owned state.
func AddressFromNet(addr net.Addr) Address {
	if addr == nil {
		return Address{}
	}
	result := Address{Network: addr.Network()}
	address := addr.String()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		if zoneHost, zone, ok := splitZone(address); ok {
			result.Host = zoneHost
			result.Zone = zone
		} else {
			result.Host = address
		}
		return result
	}
	if zoneHost, zone, ok := splitZone(host); ok {
		result.Host = zoneHost
		result.Zone = zone
	} else {
		result.Host = host
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err == nil {
		result.Port = uint16(parsedPort)
	}
	return result
}

func splitZone(host string) (string, string, bool) {
	for i := len(host) - 1; i >= 0; i-- {
		if host[i] == '%' {
			return host[:i], host[i+1:], true
		}
	}
	return "", "", false
}

// String returns a stable host and port representation.
func (a Address) String() string {
	host := a.Host
	if a.Zone != "" {
		host += "%" + a.Zone
	}
	if a.Port == 0 {
		return host
	}
	return net.JoinHostPort(host, strconv.FormatUint(uint64(a.Port), 10))
}

// CloseReason describes a normalized transport closure.
type CloseReason struct {
	Local           bool
	Peer            bool
	Clean           bool
	ApplicationCode uint64
	TransportCode   uint64
	Message         string
	Err             error
}

// ConnectionInfo is an immutable connection or session snapshot.
type ConnectionInfo struct {
	RequestedBackend    Backend
	ResolvedBackend     Backend
	Protocol            Protocol
	LocalAddress        Address
	RemoteAddress       Address
	NegotiatedProtocol  string
	WebTransportProfile string
	Provider            string
	ProviderVersion     string
	TransportABIVersion uint32
}
