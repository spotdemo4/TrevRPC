package benchutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	idleTimeout = 10 * time.Minute
	keepAlive   = 5 * time.Second
)

// QUICConfig returns the common long-lived benchmark connection settings.
func QUICConfig() *quic.Config {
	return &quic.Config{MaxIdleTimeout: idleTimeout, KeepAlivePeriod: keepAlive}
}

// ListenHTTP3 starts an HTTP/3-only TrevRPC listener.
func ListenHTTP3(addr, certFile, keyFile string, server *trevrpc.Server) (trevrpc.ServerListener, error) {
	return listenQUIC(addr, certFile, keyFile, http3.NextProtoH3, "HTTP/3", server)
}

// ListenNativeQUIC starts a native TrevRPC listener with the supplied identity.
func ListenNativeQUIC(addr, certFile, keyFile string, server *trevrpc.Server) (trevrpc.ServerListener, error) {
	return listenQUIC(addr, certFile, keyFile, trevrpc.ALPN, "quic", server)
}

func listenQUIC(
	addr, certFile, keyFile, protocol, serverKind string,
	server *trevrpc.Server,
) (trevrpc.ServerListener, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("%s server requires -cert and -key", serverKind)
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return trevrpc.Listen(addr, server, trevrpc.ListenOptions{
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{certificate},
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{protocol},
		},
		QUICConfig: QUICConfig(),
	})
}

// VerifiedClientTLSConfig trusts certFile and verifies the host in address.
func VerifiedClientTLSConfig(certFile, address string) (*tls.Config, error) {
	return VerifiedClientTLSConfigForProtocol(certFile, address, trevrpc.ALPN)
}

// VerifiedClientTLSConfigForProtocol trusts certFile, verifies the address host,
// and negotiates protocol over TLS.
func VerifiedClientTLSConfigForProtocol(certFile, address, protocol string) (*tls.Config, error) {
	if certFile == "" {
		return nil, errors.New("client requires certificate")
	}
	certificatePEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		return nil, errors.New("CA certificate contains no PEM certificates")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("parse server address: %w", err)
	}
	if zone := strings.LastIndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	if host == "" {
		return nil, errors.New("server address has an empty host")
	}
	return &tls.Config{
		RootCAs:    roots,
		ServerName: host,
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{protocol},
	}, nil
}

// DialNativeQUIC establishes one caller-owned native TrevRPC connection.
func DialNativeQUIC(ctx context.Context, address string, tlsConfig *tls.Config) (*trevrpc.RawQUICClient, error) {
	return DialNativeQUICWithMaxFrameSize(ctx, address, tlsConfig, trevrpc.DefaultMaxFrameSize)
}

// DialNativeQUICWithMaxFrameSize establishes one connection with an explicit frame limit.
func DialNativeQUICWithMaxFrameSize(ctx context.Context, address string, tlsConfig *tls.Config, maxFrameSize int) (*trevrpc.RawQUICClient, error) {
	connection, err := quic.DialAddr(ctx, address, tlsConfig, trevrpc.QUICClientConfig(maxFrameSize, QUICConfig()))
	if err != nil {
		return nil, err
	}
	return trevrpc.Advanced.NewRawQUICClient(connection).WithMaxFrameSize(maxFrameSize), nil
}
