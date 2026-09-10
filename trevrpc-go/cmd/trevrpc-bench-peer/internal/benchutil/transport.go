package benchutil

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	idleTimeout = 10 * time.Minute
	keepAlive   = 5 * time.Second
)

// ListenHTTP3 starts an HTTP/3-only TrevRPC listener.
func ListenHTTP3(addr, certFile, keyFile string, server *trevrpc.Server) (trevrpc.ServerListener, error) {
	return listenQUIC(addr, certFile, keyFile, "HTTP/3", server)
}

// ListenNativeQUIC starts a native TrevRPC listener with the supplied identity.
func ListenNativeQUIC(addr, certFile, keyFile string, server *trevrpc.Server) (trevrpc.ServerListener, error) {
	return listenQUIC(addr, certFile, keyFile, "quic", server)
}

func listenQUIC(
	addr, certFile, keyFile, serverKind string,
	server *trevrpc.Server,
) (trevrpc.ServerListener, error) {
	credentials, err := loadServerCredentials(certFile, keyFile, serverKind)
	if err != nil {
		return nil, err
	}
	return trevrpc.Listen(addr, server, trevrpc.ListenOptions{
		Transport: benchmarkTransportConfig(),
		Limits: trevrpc.TransportLimits{
			ConnectionReceiveWindow: 256 * 1024 * 1024,
		},
		Credentials: credentials,
	})
}

func benchmarkTransportConfig() trevrpc.TransportConfig {
	return trevrpc.TransportConfig{MaxIdleTimeout: idleTimeout, KeepAlive: keepAlive}
}

func loadServerCredentials(certFile, keyFile, serverKind string) (*trevrpc.TransportCredentials, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("%s server requires -cert and -key", serverKind)
	}
	certificate, err := os.ReadFile(certFile)
	if err != nil {
		return nil, err
	}
	privateKey, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	return &trevrpc.TransportCredentials{
		CertificateChainPEM: certificate,
		PrivateKeyPEM:       privateKey,
	}, nil
}

// VerifiedClientCredentials trusts certFile and validates the native dial address.
func VerifiedClientCredentials(certFile, address string) (*trevrpc.TransportCredentials, error) {
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
	return &trevrpc.TransportCredentials{RootCAPEM: certificatePEM}, nil
}

// DialNativeQUIC establishes one caller-owned native TrevRPC channel.
func DialNativeQUIC(ctx context.Context, address string, credentials *trevrpc.TransportCredentials) (*trevrpc.Channel, error) {
	return DialNativeQUICWithMaxFrameSize(ctx, address, credentials, trevrpc.DefaultMaxFrameSize)
}

// DialNativeQUICWithMaxFrameSize establishes one channel with an explicit frame limit.
func DialNativeQUICWithMaxFrameSize(ctx context.Context, address string, credentials *trevrpc.TransportCredentials, maxFrameSize int) (*trevrpc.Channel, error) {
	return trevrpc.Dial(ctx, address, trevrpc.DialOptions{
		Transport:    benchmarkTransportConfig(),
		Credentials:  credentials,
		MaxFrameSize: maxFrameSize,
	})
}
