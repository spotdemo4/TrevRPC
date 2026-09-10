package trevrpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

// TransportBackend identifies the implementation used for an endpoint.
type TransportBackend = transportinternal.Backend

const (
	// TransportBackendAuto selects the built-in native C transport backend.
	TransportBackendAuto = transportinternal.BackendAuto
	// TransportBackendNative selects the built-in native C transport backend.
	TransportBackendNative = transportinternal.BackendNative
	// TransportBackendQUICGo selects the optional quic-go and webtransport-go backend.
	TransportBackendQUICGo = transportinternal.BackendQUICGo
)

// TransportProtocol identifies the wire protocol carried by an endpoint.
type TransportProtocol = transportinternal.Protocol

const (
	TransportProtocolNativeQUIC   = transportinternal.ProtocolNativeQUIC
	TransportProtocolHTTP3        = transportinternal.ProtocolHTTP3
	TransportProtocolWebTransport = transportinternal.ProtocolWebTransport
)

// TransportCredentials contains copied TLS identity and verification material.
type TransportCredentials = transportinternal.Credentials

// TransportLimits contains backend-neutral flow-control and stream limits.
type TransportLimits = transportinternal.Limits

// HeaderField is one ordered HTTP field.
type HeaderField = transportinternal.HeaderField

// HeaderFields preserves HTTP field order and duplicate names.
type HeaderFields = transportinternal.HeaderFields

// TransportAddress is a copied transport address.
type TransportAddress = transportinternal.Address

// TransportCloseReason describes a normalized transport closure.
type TransportCloseReason = transportinternal.CloseReason

// ConnectionInfo is an immutable connection or WebTransport session snapshot.
type ConnectionInfo = transportinternal.ConnectionInfo

func resolveTransportBackend(backend TransportBackend) (TransportBackend, error) {
	switch backend {
	case TransportBackendAuto, TransportBackendNative:
		return TransportBackendNative, nil
	case TransportBackendQUICGo:
		return TransportBackendQUICGo, nil
	default:
		return TransportBackendAuto, InvalidArgument(fmt.Sprintf("unsupported transport backend %d", backend))
	}
}

func resolveExplicitBackend(backend TransportBackend) (TransportBackend, error) {
	switch backend {
	case TransportBackendNative, TransportBackendQUICGo:
		return backend, nil
	case TransportBackendAuto:
		return TransportBackendAuto, InvalidArgument("explicit backend must not identify as Auto")
	default:
		return TransportBackendAuto, InvalidArgument(fmt.Sprintf("unsupported explicit transport backend %d", backend))
	}
}

func nativeBackendUnavailable() error {
	return Unimplemented("bundled native transport requires cgo on glibc-based linux/amd64 or linux/arm64; no host TrevRPC or MsQuic installation is required")
}

func cloneTransportCredentials(credentials *TransportCredentials) *TransportCredentials {
	if credentials == nil {
		return nil
	}
	clone := credentials.Clone()
	return &clone
}

func validateTransportCredentials(credentials *TransportCredentials) error {
	if credentials == nil {
		return nil
	}
	hasCertificate := len(credentials.CertificateChainPEM) != 0
	hasKey := len(credentials.PrivateKeyPEM) != 0
	if hasCertificate != hasKey {
		return InvalidArgument("transport credentials require both certificate chain and private key")
	}
	if credentials.RequireClientCertificate && len(credentials.RootCAPEM) == 0 {
		return InvalidArgument("client certificate verification requires a root CA")
	}
	return nil
}

func transportCertificate(
	credentials *TransportCredentials,
) (*tls.Certificate, error) {
	if credentials == nil || len(credentials.CertificateChainPEM) == 0 {
		return nil, nil
	}
	certificate, err := tls.X509KeyPair(
		credentials.CertificateChainPEM,
		credentials.PrivateKeyPEM,
	)
	if err != nil {
		return nil, InvalidArgument(
			"invalid transport certificate credentials: " + err.Error(),
		)
	}
	return &certificate, nil
}

func transportRootPool(
	credentials *TransportCredentials,
) (*x509.CertPool, error) {
	if credentials == nil || len(credentials.RootCAPEM) == 0 {
		return nil, nil
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(credentials.RootCAPEM) {
		return nil, InvalidArgument(
			"transport root CA contains no certificates",
		)
	}
	return roots, nil
}

func validateHeaderFields(fields HeaderFields) error {
	for _, field := range fields {
		if strings.TrimSpace(field.Name) == "" {
			return InvalidArgument("HTTP header name is empty")
		}
		if strings.HasPrefix(field.Name, ":") {
			return InvalidArgument("HTTP pseudo-headers cannot be supplied as request headers")
		}
		if strings.ContainsAny(field.Name, "\r\n") || strings.ContainsAny(field.Value, "\r\n") {
			return InvalidArgument("HTTP headers cannot contain newlines")
		}
	}
	return nil
}

func headerFieldValues(fields HeaderFields, name string) []string {
	var values []string
	for _, field := range fields {
		if strings.EqualFold(field.Name, name) {
			values = append(values, field.Value)
		}
	}
	return values
}
