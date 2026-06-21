//go:build trevrpc_webtransport_native && cgo

package trevrpc

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
)

func TestNativeWebTransportRoundTripsUnaryAndAllStreamingModes(t *testing.T) {
	running := startTestNativeWebTransportServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	session := connectTestNativeWebTransportSession(t, running)
	defer session.Close()
	transport := NewNativeWebTransportClient(session)

	for index := range 4 {
		if err := runMixedQUICCall(transport, index); err != nil {
			t.Fatal(err)
		}
	}
}

type runningTestNativeWebTransportServer struct {
	addr     string
	listener *NativeWebTransportListener
	cancel   context.CancelFunc
	done     chan error
}

func startTestNativeWebTransportServer(t *testing.T, configure func(*Server)) *runningTestNativeWebTransportServer {
	t.Helper()

	server := NewServer()
	registerTestGreeter(server)
	configure(server)

	certFile, keyFile := nativeWebTransportCertificateFiles(t)
	addr := nativeWebTransportUDPAddr(t)
	listener, err := ListenNativeWebTransport(addr, nativeWebTransportTestConfig(server.Options(), certFile, keyFile))
	if err != nil {
		t.Fatalf("listen native WebTransport: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeNativeWebTransport(ctx, listener, server)
	}()

	running := &runningTestNativeWebTransportServer{
		addr:     addr,
		listener: listener,
		cancel:   cancel,
		done:     done,
	}
	t.Cleanup(func() {
		running.stop(t)
	})

	return running
}

func connectTestNativeWebTransportSession(t *testing.T, running *runningTestNativeWebTransportServer) *NativeWebTransportSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	session, err := DialNativeWebTransport(ctx, "https://"+running.addr+"/trevrpc", NativeWebTransportConfig{
		Path:                      "/trevrpc",
		SkipCertificateValidation: true,
		MaxStreamsPerSession:      DefaultServerOptions().MaxConcurrentStreamsPerConnection,
		HandshakeTimeout:          testTimeout,
		IdleTimeout:               testTimeout,
	})
	if err != nil {
		t.Fatalf("dial native WebTransport: %v", err)
	}

	return session
}

func (s *runningTestNativeWebTransportServer) stop(t *testing.T) {
	t.Helper()
	if s.cancel == nil {
		return
	}

	s.cancel()
	s.cancel = nil
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("serve native WebTransport: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("native WebTransport server did not shut down")
	}
	if err := s.listener.Close(); err != nil {
		t.Fatalf("close native WebTransport listener: %v", err)
	}
}

func nativeWebTransportTestConfig(options ServerOptions, certFile, keyFile string) NativeWebTransportConfig {
	return NativeWebTransportConfig{
		Path:                 options.WebTransportPath,
		CertFile:             certFile,
		KeyFile:              keyFile,
		MaxStreamsPerSession: options.MaxConcurrentStreamsPerConnection,
		HandshakeTimeout:     testTimeout,
		IdleTimeout:          options.StreamIdleTimeout,
	}
}

func nativeWebTransportCertificateFiles(t *testing.T) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate native WebTransport TLS key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create native WebTransport TLS certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal native WebTransport TLS key: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.cert")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0o600); err != nil {
		t.Fatalf("write native WebTransport TLS certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write native WebTransport TLS key: %v", err)
	}

	return certFile, keyFile
}

func nativeWebTransportUDPAddr(t *testing.T) string {
	t.Helper()

	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve native WebTransport UDP address: %v", err)
	}
	addr := packetConn.LocalAddr().String()
	if err := packetConn.Close(); err != nil {
		t.Fatalf("release native WebTransport UDP address: %v", err)
	}

	return addr
}
