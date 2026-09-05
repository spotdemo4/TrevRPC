package trevrpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"sort"
	"strings"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

// TransportBackend identifies the implementation used for an endpoint.
type TransportBackend = transportinternal.Backend

const (
	// TransportBackendAuto applies the current release policy. It resolves to Legacy in this release.
	TransportBackendAuto = transportinternal.BackendAuto
	// TransportBackendNative selects the canonical C transport backend.
	TransportBackendNative = transportinternal.BackendNative
	// TransportBackendLegacy selects the quic-go and webtransport-go backend.
	TransportBackendLegacy = transportinternal.BackendLegacy
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
	case TransportBackendAuto, TransportBackendLegacy:
		return TransportBackendLegacy, nil
	case TransportBackendNative:
		return TransportBackendNative, nil
	default:
		return TransportBackendAuto, InvalidArgument(fmt.Sprintf("unsupported transport backend %d", backend))
	}
}

func nativeBackendUnavailable() error {
	return Unimplemented("native transport backend is not available in this build")
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

func legacyServerTLSConfig(config *tls.Config, credentials *TransportCredentials) (*tls.Config, error) {
	if config != nil && credentials != nil {
		return nil, InvalidArgument("TLSConfig and Credentials cannot both be set")
	}
	if config != nil {
		return config.Clone(), nil
	}
	if err := validateTransportCredentials(credentials); err != nil {
		return nil, err
	}
	if credentials == nil || len(credentials.CertificateChainPEM) == 0 {
		return nil, InvalidArgument("legacy listener requires TLSConfig or certificate credentials")
	}
	certificate, err := transportCertificate(credentials)
	if err != nil {
		return nil, err
	}
	roots, err := transportRootPool(credentials)
	if err != nil {
		return nil, err
	}
	result := &tls.Config{
		Certificates:       []tls.Certificate{*certificate},
		InsecureSkipVerify: credentials.InsecureSkipVerify,
		ServerName:         credentials.ServerName,
		ClientCAs:          roots,
	}
	if roots != nil {
		if credentials.RequireClientCertificate {
			result.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			result.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return result, nil
}

func legacyClientTLSConfig(config *tls.Config, credentials *TransportCredentials) (*tls.Config, error) {
	if config != nil && credentials != nil {
		return nil, InvalidArgument("TLSConfig and Credentials cannot both be set")
	}
	if config != nil {
		return config.Clone(), nil
	}
	if err := validateTransportCredentials(credentials); err != nil {
		return nil, err
	}
	if credentials == nil {
		return nil, InvalidArgument("legacy dial requires TLSConfig or transport credentials")
	}
	certificate, err := transportCertificate(credentials)
	if err != nil {
		return nil, err
	}
	roots, err := transportRootPool(credentials)
	if err != nil {
		return nil, err
	}
	result := &tls.Config{
		ServerName:         credentials.ServerName,
		InsecureSkipVerify: credentials.InsecureSkipVerify,
		RootCAs:            roots,
	}
	if certificate != nil {
		result.Certificates = []tls.Certificate{*certificate}
	}
	return result, nil
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

func legacyRequestHeaders(fields HeaderFields, legacy http.Header) (http.Header, error) {
	if len(fields) != 0 && len(legacy) != 0 {
		return nil, InvalidArgument("RequestHeaders and RequestHeader cannot both be set")
	}
	if err := validateHeaderFields(fields); err != nil {
		return nil, err
	}
	if len(fields) != 0 {
		return headerFieldsToHTTP(fields), nil
	}
	return legacy.Clone(), nil
}

func headerFieldsToHTTP(fields HeaderFields) http.Header {
	headers := make(http.Header)
	for _, field := range fields {
		headers.Add(field.Name, field.Value)
	}
	return headers
}

func headerFieldsFromHTTP(headers http.Header) HeaderFields {
	if len(headers) == 0 {
		return nil
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	var fields HeaderFields
	for _, name := range names {
		for _, value := range headers[name] {
			fields = append(fields, HeaderField{Name: name, Value: value})
		}
	}
	return fields
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
