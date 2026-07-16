package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
)

func TestStackOperationsAndCertificateVerification(t *testing.T) {
	for _, stack := range []stackKind{stackNativeQUIC, stackGRPCHTTP2} {
		t.Run(string(stack), func(t *testing.T) {
			certFile, keyFile := writeTestCertificate(t)
			address, stopServer := startTestBenchmarkServer(t, stack, certFile, keyFile)
			defer stopServer()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			baseConfig := testClientConfig(stack, address, certFile, rpcUnary)
			client, closeClient, err := dialBenchmarkClient(ctx, baseConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer closeClient()

			tests := []struct {
				kind             rpcKind
				requestMessages  uint64
				responseMessages uint64
			}{
				{kind: rpcUnary, requestMessages: 1, responseMessages: 1},
				{kind: rpcClientStream, requestMessages: 4, responseMessages: 1},
				{kind: rpcServerStream, requestMessages: 1, responseMessages: 4},
				{kind: rpcBidi, requestMessages: 4, responseMessages: 4},
			}
			for _, test := range tests {
				t.Run(string(test.kind), func(t *testing.T) {
					config := testClientConfig(stack, address, certFile, test.kind)
					counts, err := newBenchmarkOperation(client, config)(ctx, 17)
					if err != nil {
						t.Fatal(err)
					}
					if counts.requestMessages != test.requestMessages || counts.responseMessages != test.responseMessages {
						t.Fatalf("message counts = %+v, want request=%d response=%d", counts, test.requestMessages, test.responseMessages)
					}
				})
			}

			untrustedCert, _ := writeTestCertificate(t)
			untrustedConfig := testClientConfig(stack, address, untrustedCert, rpcUnary)
			untrustedCtx, untrustedCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer untrustedCancel()
			untrustedClient, closeUntrusted, err := dialBenchmarkClient(untrustedCtx, untrustedConfig)
			if err == nil {
				defer closeUntrusted()
				_, err = newBenchmarkOperation(untrustedClient, untrustedConfig)(untrustedCtx, 0)
			}
			if err == nil {
				t.Fatal("RPC with an untrusted certificate succeeded")
			}
		})
	}
}

func TestGRPCMaximumPayloadAndTerminalStatus(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	address, stopServer := startTestBenchmarkServer(t, stackGRPCHTTP2, certFile, keyFile)
	defer stopServer()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	config := testClientConfig(stackGRPCHTTP2, address, certFile, rpcUnary)
	client, closeClient, err := dialBenchmarkClient(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()

	config.requestBytes = maxBenchmarkPayloadBytes
	if _, err := newBenchmarkOperation(client, config)(ctx, 0); err != nil {
		t.Fatalf("maximum request payload: %v", err)
	}
	config.requestBytes = 0
	config.responseBytes = maxBenchmarkPayloadBytes
	if _, err := newBenchmarkOperation(client, config)(ctx, 0); err != nil {
		t.Fatalf("maximum response payload: %v", err)
	}

	responses, err := client.ServerStream(ctx, &benchmarkpb.StreamRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := responses.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid server stream status = %v, want InvalidArgument", status.Code(err))
	}
}

func TestPreparedPhaseFixedAdmissionAndDrain(t *testing.T) {
	const concurrency = 3
	release := make(chan struct{})
	var started atomic.Uint64
	operation := func(context.Context, uint64) (operationCounts, error) {
		started.Add(1)
		<-release
		return operationCounts{requestMessages: 2, responseMessages: 3}, nil
	}
	phase := preparePhase(context.Background(), concurrency, operation, true)
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()
	result := phase.run(5 * time.Millisecond)
	if got := started.Load(); got != concurrency {
		t.Fatalf("admitted operations = %d, want exactly one per lane (%d)", got, concurrency)
	}
	if result.completed != concurrency || result.failed != 0 {
		t.Fatalf("phase counts = completed %d failed %d", result.completed, result.failed)
	}
	if result.requestMessages != concurrency*2 || result.responseMessages != concurrency*3 {
		t.Fatalf("phase message counts = request %d response %d", result.requestMessages, result.responseMessages)
	}
	if result.elapsed < 20*time.Millisecond {
		t.Fatalf("elapsed = %v, want drain beyond 20ms", result.elapsed)
	}
	if got := histogramCount(result.histogram); got != result.completed {
		t.Fatalf("histogram count = %d, want %d", got, result.completed)
	}
}

func TestLogLinearV1Buckets(t *testing.T) {
	tests := []struct {
		value uint64
		want  uint64
	}{
		{value: 1, want: 1},
		{value: 1_023, want: 1_023},
		{value: 1_024, want: 1_025},
		{value: 1_025, want: 1_025},
		{value: 2_047, want: 2_047},
		{value: 2_048, want: 2_051},
	}
	for _, test := range tests {
		if got := logLinearUpperBound(test.value); got != test.want {
			t.Errorf("upper bound for %d = %d, want %d", test.value, got, test.want)
		}
	}
	buckets := histogramEventBuckets(map[uint64]uint64{2_051: 2, 1: 1, 99: 0})
	if len(buckets) != 2 || buckets[0].UpperBoundNS != "1" || buckets[1].UpperBoundNS != "2051" {
		t.Fatalf("sorted buckets = %+v", buckets)
	}
}

func TestClientSTARTEmitsArmedAndSample(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	address, stopServer := startTestBenchmarkServer(t, stackNativeQUIC, certFile, keyFile)
	defer stopServer()
	config := testClientConfig(stackNativeQUIC, address, certFile, rpcUnary)
	config.concurrency = 2
	config.warmup = 2 * time.Millisecond
	config.measurement = 5 * time.Millisecond

	var output bytes.Buffer
	if err := runClient(config, strings.NewReader("START\n"), newEventEmitter(&output)); err != nil {
		t.Fatal(err)
	}
	events := decodeEvents(t, output.Bytes())
	if len(events) != 2 || events[0]["event"] != "armed" || events[1]["event"] != "sample" {
		t.Fatalf("events = %v", events)
	}
	assertDecimalString(t, events[1], "admission_ns")
	assertDecimalString(t, events[1], "completed")
	assertDecimalString(t, events[1], "failed")
	if events[1]["failed"] != "0" {
		t.Fatalf("sample failure count = %v", events[1]["failed"])
	}
	histogram, ok := events[1]["histogram"].([]any)
	if !ok || len(histogram) == 0 {
		t.Fatalf("sample histogram = %#v", events[1]["histogram"])
	}
	var histogramTotal uint64
	for _, rawBucket := range histogram {
		bucket := rawBucket.(map[string]any)
		count, err := strconv.ParseUint(bucket["count"].(string), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		histogramTotal += count
	}
	completed, err := strconv.ParseUint(events[1]["completed"].(string), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if histogramTotal != completed {
		t.Fatalf("histogram count = %d, completed = %d", histogramTotal, completed)
	}
}

func TestServerSHUTDOWNEmitsReadyAndStopped(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	var output bytes.Buffer
	for _, stack := range []stackKind{stackNativeQUIC, stackGRPCHTTP2} {
		t.Run(string(stack), func(t *testing.T) {
			output.Reset()
			config := serverConfig{stack: stack, listen: "127.0.0.1:0", certFile: certFile, keyFile: keyFile}
			if err := runServer(config, strings.NewReader("SHUTDOWN\n"), newEventEmitter(&output)); err != nil {
				t.Fatal(err)
			}
			events := decodeEvents(t, output.Bytes())
			if len(events) != 2 || events[0]["event"] != "ready" || events[1]["event"] != "stopped" {
				t.Fatalf("events = %v", events)
			}
			if _, _, err := net.SplitHostPort(events[0]["address"].(string)); err != nil {
				t.Fatalf("ready address: %v", err)
			}
		})
	}
}

func TestClientSHUTDOWNWhileArmed(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	address, stopServer := startTestBenchmarkServer(t, stackNativeQUIC, certFile, keyFile)
	defer stopServer()
	var output bytes.Buffer
	config := testClientConfig(stackNativeQUIC, address, certFile, rpcUnary)
	if err := runClient(config, strings.NewReader("SHUTDOWN\n"), newEventEmitter(&output)); err != nil {
		t.Fatal(err)
	}
	events := decodeEvents(t, output.Bytes())
	if len(events) != 1 || events[0]["event"] != "armed" {
		t.Fatalf("events = %v", events)
	}
}

func TestClientConfigRejectsDuplicateAndOversizedOptions(t *testing.T) {
	base := []string{
		"--stack", "trevrpc_native_quic",
		"--address", "127.0.0.1:1",
		"--cert", "cert.pem",
		"--rpc", "unary",
		"--concurrency", "1",
		"--warmup-ms", "0",
		"--measurement-ms", "1",
		"--request-bytes", "0",
		"--response-bytes", "0",
		"--messages-per-stream", "1",
	}
	if _, err := parseClientConfig(base); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if _, err := parseClientConfig(base[2:]); err == nil {
		t.Fatal("missing --stack was accepted")
	}
	unsupported := append([]string{}, base...)
	unsupported[1] = "unknown"
	if _, err := parseClientConfig(unsupported); err == nil {
		t.Fatal("unsupported --stack was accepted")
	}
	duplicate := append(append([]string{}, base...), "--rpc", "bidi")
	if _, err := parseClientConfig(duplicate); err == nil {
		t.Fatal("duplicate option was accepted")
	}
	oversized := append([]string{}, base...)
	for index, value := range oversized {
		if value == "--request-bytes" {
			oversized[index+1] = strconv.Itoa(maxBenchmarkPayloadBytes + 1)
			break
		}
	}
	if _, err := parseClientConfig(oversized); err == nil {
		t.Fatal("oversized request payload was accepted")
	}
}

func TestServerConfigRequiresSupportedStack(t *testing.T) {
	base := []string{
		"--stack", "grpc_http2",
		"--listen", "127.0.0.1:0",
		"--cert", "cert.pem",
		"--key", "key.pem",
	}
	config, err := parseServerConfig(base)
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if config.stack != stackGRPCHTTP2 {
		t.Fatalf("stack = %q, want %q", config.stack, stackGRPCHTTP2)
	}
	if _, err := parseServerConfig(base[2:]); err == nil {
		t.Fatal("missing --stack was accepted")
	}
	unsupported := append([]string{}, base...)
	unsupported[1] = "unknown"
	if _, err := parseServerConfig(unsupported); err == nil {
		t.Fatal("unsupported --stack was accepted")
	}
}

func testClientConfig(stack stackKind, address, certFile string, kind rpcKind) clientConfig {
	return clientConfig{
		stack:             stack,
		address:           address,
		certFile:          certFile,
		rpc:               kind,
		concurrency:       1,
		measurement:       time.Millisecond,
		requestBytes:      13,
		responseBytes:     17,
		messagesPerStream: 4,
	}
}

func startTestBenchmarkServer(t *testing.T, stack stackKind, certFile, keyFile string) (string, func()) {
	t.Helper()
	listener, err := listenBenchmarkServer(serverConfig{
		stack:    stack,
		listen:   "127.0.0.1:0",
		certFile: certFile,
		keyFile:  keyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Serve(ctx) }()
	return listener.Addr().String(), func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("server shutdown: %v", err)
		}
	}
}

func writeTestCertificate(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile := filepath.Join(directory, "cert.pem")
	keyFile := filepath.Join(directory, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateKeyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func decodeEvents(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	var events []map[string]any
	for decoder.More() {
		var event map[string]any
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

func assertDecimalString(t *testing.T, event map[string]any, name string) {
	t.Helper()
	value, ok := event[name].(string)
	if !ok {
		t.Fatalf("%s = %#v, want decimal string", name, event[name])
	}
	if _, err := strconv.ParseUint(value, 10, 64); err != nil {
		t.Fatalf("%s = %q: %v", name, value, err)
	}
}

func Example_runCapabilities() {
	var output bytes.Buffer
	err := run([]string{"capabilities"}, strings.NewReader(""), newEventEmitter(&output))
	fmt.Print(output.String(), err)
	// Output:
	// {"schema_version":3,"event":"capabilities","peer":"go","roles":["client","server"],"rpc_kinds":["unary","client_stream","server_stream","bidi"],"stacks":["trevrpc_native_quic","grpc_http2"],"histogram":"log_linear_v1"}
	// <nil>
}
