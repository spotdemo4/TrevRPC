//go:build trevrpc_msquic && cgo

package cruntimetest

import (
	"bytes"
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
	"strconv"
	"testing"
	"time"
)

func TestCRuntimeMsQuicUnaryAndStreamingRoundTrip(t *testing.T) {
	certFile, keyFile := certificateFiles(t)
	host, portText, err := net.SplitHostPort(udpAddr(t))
	if err != nil {
		t.Fatalf("split C runtime MsQuic address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse C runtime MsQuic port: %v", err)
	}

	server, err := startEchoServer(host, certFile, keyFile, uint16(port))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.close(); err != nil {
			t.Error(err)
		}
	}()

	body := []byte("hello from trevrpc-c")
	got, status, err := callUnary(host, uint16(port), "test.EchoService", "Echo", body)
	if err != nil {
		t.Fatal(err)
	}
	if status != statusOK {
		t.Fatalf("C runtime response status = %d, want OK", status)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("C runtime response body = %q, want %q", got, body)
	}

	streamMessages, streamStatus, err := callServerStream(host, uint16(port), body)
	if err != nil {
		t.Fatal(err)
	}
	if streamStatus != statusOK {
		t.Fatalf("C runtime stream status = %d, want OK", streamStatus)
	}
	if len(streamMessages) != 2 {
		t.Fatalf("C runtime stream message count = %d, want 2", len(streamMessages))
	}
	for i, message := range streamMessages {
		if !bytes.Equal(message, body) {
			t.Fatalf("C runtime stream message %d = %q, want %q", i, message, body)
		}
	}

	deadlineBody, deadlineStatus, err := callUnaryWithTimeout(
		host, uint16(port), "test.EchoService", "Deadline", body, uint64(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if deadlineStatus != statusDeadlineExceeded {
		t.Fatalf("C runtime deadline status = %d, want DEADLINE_EXCEEDED", deadlineStatus)
	}
	if len(deadlineBody) != 0 {
		t.Fatalf("C runtime deadline body = %q, want empty body", deadlineBody)
	}

	_, invalidStatus, err := callUnaryWithTimeout(
		host, uint16(port), "test.EchoService", "Echo", body, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if invalidStatus != statusInvalidArgument {
		t.Fatalf("C runtime oversized timeout status = %d, want INVALID_ARGUMENT", invalidStatus)
	}
}

func certificateFiles(t *testing.T) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate C runtime MsQuic TLS key: %v", err)
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
		t.Fatalf("create C runtime MsQuic TLS certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal C runtime MsQuic TLS key: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.cert")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0o600); err != nil {
		t.Fatalf("write C runtime MsQuic TLS certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write C runtime MsQuic TLS key: %v", err)
	}

	return certFile, keyFile
}

func udpAddr(t *testing.T) string {
	t.Helper()

	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve C runtime MsQuic UDP address: %v", err)
	}
	addr := packetConn.LocalAddr().String()
	if err := packetConn.Close(); err != nil {
		t.Fatalf("release C runtime MsQuic UDP address: %v", err)
	}

	return addr
}
