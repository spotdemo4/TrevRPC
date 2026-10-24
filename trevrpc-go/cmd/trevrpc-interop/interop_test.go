package main

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

var wantScenarios = []string{
	"unary", "unary-metadata", "unary-error", "unary-large", "client-stream",
	"server-stream", "server-stream-error", "bidi", "bidi-error",
	"unary-headers", "server-stream-headers", "unreliable", "compressed", "compressed-stream",
}

// writeTestDictionary writes the compression dictionary of the shared golden
// vectors, as interop/run.py does, and returns its path.
func writeTestDictionary(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("../../../testdata/zstd-golden-vectors.txt")
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		value, ok := strings.CutPrefix(line, "dict.d1.dictionary = ")
		if !ok {
			continue
		}
		dictionary, err := hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "interop.dict")
		if err := os.WriteFile(path, dictionary, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Fatal("no dictionary in the golden vectors")
	return ""
}

// writeTestCertificate writes a localhost ECDSA P-256 certificate and key like
// the one interop/run.py generates.
func writeTestCertificate(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(10 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// TestServerAndClientCommandsFollowTheContract drives both subcommands through
// run, exactly as interop/run.py does with the built binary: without
// compression, and with a dictionary on both sides.
func TestServerAndClientCommandsFollowTheContract(t *testing.T) {
	t.Run("plain", func(t *testing.T) { exerciseContract(t, nil) })
	t.Run("compressed", func(t *testing.T) {
		if !zstd.Available {
			t.Skip("compression needs cgo")
		}
		exerciseContract(t, []string{"--dictionary", writeTestDictionary(t)})
	})
}

func exerciseContract(t *testing.T, extra []string) {
	certFile, keyFile := writeTestCertificate(t)

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	var stderr bytes.Buffer
	exit := make(chan int, 1)
	go func() {
		exit <- run(append([]string{"server", "--cert", certFile, "--key", keyFile, "--port", "0"}, extra...), stdinReader, stdoutWriter, &stderr)
		_ = stdoutWriter.Close()
	}()

	line, err := bufio.NewReader(stdoutReader).ReadString('\n')
	if err != nil {
		t.Fatalf("server did not print its listening line: %v (stderr: %s)", err, stderr.String())
	}
	var ports listeningLine
	if err := json.Unmarshal([]byte(line), &ports); err != nil {
		t.Fatalf("listening line %q: %v", line, err)
	}
	if ports.Native == 0 || ports.HTTP3 == 0 || ports.WebTransport == 0 {
		t.Fatalf("listening line %q has an unset port", line)
	}

	var stdout bytes.Buffer
	args := append([]string{"client", "--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)), "--ca", certFile}, extra...)
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("client exit code = %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	var got []string
	for line := range strings.SplitSeq(strings.TrimSpace(stdout.String()), "\n") {
		got = append(got, strings.TrimPrefix(line, "ok "))
		if !strings.HasPrefix(line, "ok ") {
			t.Errorf("scenario did not pass: %s", line)
		}
	}
	if strings.Join(got, ",") != strings.Join(wantScenarios, ",") {
		t.Fatalf("scenarios = %v, want %v", got, wantScenarios)
	}

	// Closing stdin shuts the server down gracefully with exit code 0.
	_ = stdinWriter.Close()
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("server exit code = %d (stderr: %s)", code, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop after stdin reached EOF")
	}
}

func TestClientFailsWhenServerNameIsNotCovered(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	stdinReader, stdinWriter := io.Pipe()
	t.Cleanup(func() {
		if err := stdinWriter.Close(); err != nil {
			t.Errorf("close server stdin: %v", err)
		}
	})
	stdoutReader, stdoutWriter := io.Pipe()
	var stderr bytes.Buffer
	go run([]string{"server", "--cert", certFile, "--key", keyFile}, stdinReader, stdoutWriter, &stderr)
	line, err := bufio.NewReader(stdoutReader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var ports listeningLine
	if err := json.Unmarshal([]byte(line), &ports); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	// The TLS handshake fails before any scenario runs.
	args := []string{"client", "--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)), "--ca", certFile, "--server-name", "wrong.example.test"}
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code == 0 {
		t.Fatal("client succeeded with a server name the certificate does not cover")
	}
}

func TestCommandLineErrors(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"bogus"},
		{"server"},
		{"client"},
		{"client", "--addr", "127.0.0.1:1"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code == 0 {
			t.Errorf("run(%v) = 0, want a usage error", args)
		}
	}
}
