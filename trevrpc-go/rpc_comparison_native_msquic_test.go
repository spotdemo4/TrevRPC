//go:build trevrpc_msquic_native && cgo

package trevrpc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

func BenchmarkRPCComparisonNativeMsQuic(b *testing.B) {
	client := startTrevRPCNativeMsQuicComparisonClient(b)
	warmTrevRPCNativeMsQuicComparisonClient(b, client)

	b.Run("unary_round_trip/trevrpc_msquic", func(b *testing.B) {
		benchmarkTrevRPCUnary(b, client)
	})
	b.Run("server_stream_16_messages/trevrpc_msquic", func(b *testing.B) {
		benchmarkTrevRPCServerStreaming(b, client)
	})
	b.Run("client_stream_16_messages/trevrpc_msquic", func(b *testing.B) {
		benchmarkTrevRPCClientStreaming(b, client)
	})
	b.Run("bidi_stream_16_messages/trevrpc_msquic", func(b *testing.B) {
		benchmarkTrevRPCBidiStreaming(b, client)
	})
}

func startTrevRPCNativeMsQuicComparisonClient(b *testing.B) *greeter.GreeterClient {
	b.Helper()

	server := newComparisonTrevRPCServer()
	certFile, keyFile := comparisonNativeMsQuicCertificateFiles(b)
	addr := freeNativeMsQuicUDPAddr(b)
	serverConfig := comparisonNativeMsQuicConfig(server.Options())
	serverConfig.CertFile = certFile
	serverConfig.KeyFile = keyFile

	listener, err := trevrpc.ListenNativeMsQuic(addr, serverConfig)
	if err != nil {
		b.Fatalf("listen TrevRPC native MsQuic: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- trevrpc.ServeNativeMsQuic(ctx, listener, server)
	}()

	conn, err := trevrpc.DialNativeMsQuic(ctx, addr, comparisonNativeMsQuicConfig(server.Options()))
	if err != nil {
		cancel()
		_ = listener.Close()
		b.Fatalf("dial TrevRPC native MsQuic: %v", err)
	}

	b.Cleanup(func() {
		_ = conn.Close()
		cancel()
		_ = listener.Close()
		select {
		case err := <-serveDone:
			if err != nil {
				b.Logf("TrevRPC native MsQuic server stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			b.Log("timed out waiting for TrevRPC native MsQuic server shutdown")
		}
	})

	return greeter.NewGreeterClient(trevrpc.NewNativeMsQuicClient(conn), trevrpc.WithoutStreamIdleTimeout())
}

func warmTrevRPCNativeMsQuicComparisonClient(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.SayHello(ctx, comparisonRequests[0]); err != nil {
		b.Fatalf("warm TrevRPC native MsQuic unary: %v", err)
	}
	if _, err := trevrpcServerStreamingCall(ctx, client); err != nil {
		b.Fatalf("warm TrevRPC native MsQuic server streaming: %v", err)
	}
	if _, err := trevrpcClientStreamingCall(ctx, client); err != nil {
		b.Fatalf("warm TrevRPC native MsQuic client streaming: %v", err)
	}
	if _, err := trevrpcBidiStreamingCall(ctx, client); err != nil {
		b.Fatalf("warm TrevRPC native MsQuic bidi streaming: %v", err)
	}
}

func comparisonNativeMsQuicConfig(options trevrpc.ServerOptions) trevrpc.NativeMsQuicConfig {
	return trevrpc.NativeMsQuicConfig{
		MaxIdleTimeout:                comparisonQUICIdleTimeout,
		KeepAlive:                     comparisonQUICKeepAlive,
		PeerBidiStreamCount:           options.MaxConcurrentStreamsPerConnection,
		MaxStatelessOperations:        options.MaxConcurrentRequests,
		MaxBindingStatelessOperations: options.MaxConcurrentConnections,
	}
}

func comparisonNativeMsQuicCertificateFiles(tb testing.TB) (string, string) {
	tb.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatalf("generate native MsQuic TLS key: %v", err)
	}
	notBefore := time.Now().Add(-time.Hour)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		tb.Fatalf("create native MsQuic TLS certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		tb.Fatalf("marshal native MsQuic TLS key: %v", err)
	}

	dir := tb.TempDir()
	certFile := filepath.Join(dir, "server.cert")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0o600); err != nil {
		tb.Fatalf("write native MsQuic TLS certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		tb.Fatalf("write native MsQuic TLS key: %v", err)
	}

	return certFile, keyFile
}

func freeNativeMsQuicUDPAddr(tb testing.TB) string {
	tb.Helper()

	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve UDP benchmark address: %v", err)
	}
	addr := packetConn.LocalAddr().String()
	if err := packetConn.Close(); err != nil {
		tb.Fatalf("release UDP benchmark address: %v", err)
	}

	return addr
}
