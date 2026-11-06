package trevrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// acmeTestDomain is the name Pebble issues certificates for. Its mock DNS
// server resolves every name to 127.0.0.1.
const acmeTestDomain = "trevrpc.test"

func TestTCPListenerKeepsACMETLSALPN(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	registerTestService(server)
	tlsConfig := pki.server.Clone()
	tlsConfig.NextProtos = []string{acmeTLSALPN}
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: tlsConfig})
	client := pki.client.Clone()
	client.NextProtos = []string{acmeTLSALPN}

	conn, err := (&tls.Dialer{Config: client}).DialContext(t.Context(), "tcp", listener.TCPAddr().String())
	if err != nil {
		t.Fatalf("acme-tls/1 handshake: %v", err)
	}
	closeOnCleanup(t, conn)
	if got := conn.(*tls.Conn).ConnectionState().NegotiatedProtocol; got != acmeTLSALPN {
		t.Fatalf("negotiated %q, want %q", got, acmeTLSALPN)
	}
	// The certificate authority only needs the handshake.
	_ = conn.SetReadDeadline(time.Now().Add(rpcTestTimeout))
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read after the acme-tls/1 handshake: %v, want EOF", err)
	}

	// Certificate authorities validate TLS-ALPN-01 over TCP only.
	ctx, cancel := context.WithTimeout(t.Context(), rpcTestTimeout)
	defer cancel()
	if quicConn, err := quic.DialAddr(ctx, listener.Addr().String(), client, nil); err == nil {
		_ = quicConn.CloseWithError(0, "")
		t.Fatal("QUIC offered acme-tls/1")
	}

	qmux := newQMuxTestClient(t, pki, listener)
	if _, err := Unary(context.Background(), qmux, testPath("Echo"), &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatalf("QMux call: %v", err)
	}
}

func TestTCPListenerOffersACMETLSALPNOnlyWhenConfigured(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	client := pki.client.Clone()
	client.NextProtos = []string{acmeTLSALPN}
	conn, err := (&tls.Dialer{Config: client}).DialContext(t.Context(), "tcp", listener.TCPAddr().String())
	if err == nil {
		_ = conn.Close()
		t.Fatal("acme-tls/1 handshake succeeded without acme-tls/1 in the TLS config")
	}
}

// An autocert.Manager behind the listener gets a certificate from Pebble, an
// ACME test server, which validates TLS-ALPN-01 on the TCP listener, and
// serves it over QUIC and QMux over TLS.
func TestAutocertTLSALPN01(t *testing.T) {
	server := NewServer()
	registerTestService(server)
	// Pebble validates on the TCP listener's port, so the manager, which
	// needs Pebble's address, is set once the listener is bound.
	var manager atomic.Pointer[autocert.Manager]
	tlsConfig := &tls.Config{
		NextProtos: []string{acme.ALPNProto},
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return manager.Load().GetCertificate(hello)
		},
	}
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: tlsConfig, TLSHandshakeTimeout: time.Minute})
	pebble := startPebble(t, listener.TCPAddr().(*net.TCPAddr).Port)
	manager.Store(&autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(acmeTestDomain),
		Client:     &acme.Client{DirectoryURL: pebble.directory, HTTPClient: pebble.client},
	})
	client := &tls.Config{RootCAs: pebble.roots, ServerName: acmeTestDomain}

	// The first handshake for the name waits for the certificate.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	first := client.Clone()
	first.NextProtos = []string{QMuxALPN}
	conn, err := (&tls.Dialer{Config: first}).DialContext(ctx, "tcp", listener.TCPAddr().String())
	if err != nil {
		t.Fatalf("handshake that requests the certificate: %v", err)
	}
	_ = conn.Close()

	for _, transport := range []TransportPolicy{TransportQUICOnly, TransportTCPOnly} {
		ctx, cancel := context.WithTimeout(t.Context(), rpcTestTimeout)
		channel, err := Dial(ctx, listener.Addr().String(), DialOptions{
			TLSConfig: client,
			Transport: transport,
			TCPAddr:   listener.TCPAddr().String(),
		})
		if err != nil {
			cancel()
			t.Fatalf("Dial with transport %d: %v", transport, err)
		}
		_, err = Unary(ctx, channel, testPath("Echo"), &testMessage{Value: "x"}, newTestMessage)
		cancel()
		_ = channel.Close()
		if err != nil {
			t.Fatalf("call with transport %d: %v", transport, err)
		}
	}
}

// testPebble is a running Pebble ACME server.
type testPebble struct {
	// directory is the URL of its ACME directory.
	directory string
	// client trusts the certificate of its ACME and management interfaces.
	client *http.Client
	// roots trusts the certificates it issues.
	roots *x509.CertPool
}

// startPebble runs Pebble and its mock DNS server until the test ends. Pebble
// validates TLS-ALPN-01 challenges on validationPort of 127.0.0.1. It binds
// ports another process may take first, so it retries with fresh ones.
func startPebble(t *testing.T, validationPort int) testPebble {
	t.Helper()
	for _, name := range []string{"pebble", "pebble-challtestsrv"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Fatalf("%s is not on PATH (it is in the dev shell): %v", name, err)
		}
	}
	pki := newTestPKI(t)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTestKeyPair(t, pki.server.Certificates[0], certFile, keyFile)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: pki.client}, Timeout: rpcTestTimeout}
	t.Cleanup(client.CloseIdleConnections)

	var err error
	for range 3 {
		var pebble testPebble
		pebble, err = tryStartPebble(t, dir, certFile, keyFile, validationPort, client)
		if err == nil {
			return pebble
		}
		t.Logf("starting Pebble: %v", err)
	}
	t.Fatalf("starting Pebble: %v", err)
	return testPebble{}
}

func tryStartPebble(t *testing.T, dir, certFile, keyFile string, validationPort int, client *http.Client) (testPebble, error) {
	t.Helper()
	dnsAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(freeTestPort(t, "udp")))
	acmeAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(freeTestPort(t, "tcp")))
	managementAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(freeTestPort(t, "tcp")))

	dns := startTestProcess(t, nil, "pebble-challtestsrv",
		"-dns01", dnsAddr,
		"-defaultIPv4", "127.0.0.1",
		// Pebble validates the first address, which would be an IPv6 one.
		"-defaultIPv6", "",
		"-http01", "", "-https01", "", "-tlsalpn01", "", "-doh", "",
		"-management", "127.0.0.1:0",
	)
	if err := waitForTestDNS(t, dns, dnsAddr); err != nil {
		dns.stop()
		return testPebble{}, err
	}

	config, err := json.Marshal(map[string]any{"pebble": map[string]any{
		"listenAddress":                  acmeAddr,
		"managementListenAddress":        managementAddr,
		"certificate":                    certFile,
		"privateKey":                     keyFile,
		"httpPort":                       5002,
		"tlsPort":                        validationPort,
		"ocspResponderURL":               "",
		"externalAccountBindingRequired": false,
		"retryAfter":                     map[string]int{"authz": 1, "order": 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(dir, "pebble-config.json")
	if err := os.WriteFile(configFile, config, 0o600); err != nil {
		t.Fatal(err)
	}
	pebble := startTestProcess(t, []string{
		// Validate at once, and never reject a nonce to test client retries.
		"PEBBLE_VA_NOSLEEP=1",
		"PEBBLE_WFE_NONCEREJECT=0",
	}, "pebble", "-config", configFile, "-dnsserver", dnsAddr)
	directory := "https://" + acmeAddr + "/dir"
	if err := waitForTestHTTPS(pebble, client, directory); err != nil {
		pebble.stop()
		dns.stop()
		return testPebble{}, err
	}
	root, err := getTestHTTPS(t.Context(), client, "https://"+managementAddr+"/roots/0")
	if err != nil {
		pebble.stop()
		dns.stop()
		return testPebble{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(root) {
		t.Fatalf("Pebble's root is not PEM:\n%s", root)
	}
	return testPebble{directory: directory, client: client, roots: roots}, nil
}

// writeTestKeyPair writes the certificate and key of cert as PEM files.
func writeTestKeyPair(t *testing.T, cert tls.Certificate, certFile, keyFile string) {
	t.Helper()
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}

// freeTestPort returns a loopback port of network that was free a moment ago.
func freeTestPort(t *testing.T, network string) int {
	t.Helper()
	var lc net.ListenConfig
	if network == "udp" {
		conn, err := lc.ListenPacket(t.Context(), "udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		return conn.LocalAddr().(*net.UDPAddr).Port
	}
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// waitForTestDNS waits until the mock DNS server at addr resolves
// acmeTestDomain to 127.0.0.1.
func waitForTestDNS(t *testing.T, process *testProcess, addr string) error {
	t.Helper()
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	return waitForTestProcess(process, func(ctx context.Context) error {
		addrs, err := resolver.LookupHost(ctx, acmeTestDomain)
		if err != nil {
			return err
		}
		if len(addrs) != 1 || addrs[0] != "127.0.0.1" {
			t.Fatalf("mock DNS resolves %s to %v, want only 127.0.0.1", acmeTestDomain, addrs)
		}
		return nil
	})
}

// waitForTestHTTPS waits until url answers.
func waitForTestHTTPS(process *testProcess, client *http.Client, url string) error {
	return waitForTestProcess(process, func(ctx context.Context) error {
		_, err := getTestHTTPS(ctx, client, url)
		return err
	})
}

// waitForTestProcess polls ready until it succeeds, failing once process
// exits or after 10 seconds.
func waitForTestProcess(process *testProcess, ready func(context.Context) error) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := ready(ctx)
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-process.done:
			return fmt.Errorf("%s exited: %w\n%s", process.name, process.err, process.output.String())
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not ready: %w\n%s", process.name, err, process.output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// getTestHTTPS returns the body of a successful GET of url.
func getTestHTTPS(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s: %s", url, response.Status, body)
	}
	return body, nil
}

// testProcess is a child process that runs until the test ends.
type testProcess struct {
	name   string
	cmd    *exec.Cmd
	output *syncBuffer
	done   chan struct{}
	err    error // set before done is closed
	once   sync.Once
}

// startTestProcess runs name with args and extra environment variables until
// stop is called or the test ends, and logs its output if the test fails.
func startTestProcess(t *testing.T, env []string, name string, args ...string) *testProcess {
	t.Helper()
	process := &testProcess{name: name, output: new(syncBuffer), done: make(chan struct{})}
	process.cmd = exec.CommandContext(t.Context(), name, args...)
	process.cmd.Env = append(os.Environ(), env...)
	process.cmd.Stdout = process.output
	process.cmd.Stderr = process.output
	if err := process.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		process.err = process.cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		process.stop()
		if t.Failed() {
			t.Logf("%s output:\n%s", name, process.output.String())
		}
	})
	return process
}

// stop kills the process and waits for it to exit.
func (p *testProcess) stop() {
	p.once.Do(func() {
		_ = p.cmd.Process.Kill()
		<-p.done
	})
}

// syncBuffer is a bytes.Buffer that is safe for concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
