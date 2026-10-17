package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"trev.zip/llc/trevrpc/bench/grpc-go/echopb"
)

// testServer is a running `server` subcommand backed by an in-process listener.
type testServer struct {
	addr, ca string
	stdin    *io.PipeWriter
	done     chan error
}

// writeCert writes a self-signed ECDSA localhost certificate and key.
func writeCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
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
	certPath, keyPath = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// startServer runs the real `server` subcommand and reads its port line.
func startServer(t *testing.T) *testServer {
	t.Helper()
	cert, key := writeCert(t)
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	srv := &testServer{ca: cert, stdin: stdinW, done: make(chan error, 1)}
	go func() {
		err := run(t.Context(), []string{"server", "--cert", cert, "--key", key}, stdinR, stdoutW)
		_ = stdoutW.Close()
		srv.done <- err
	}()
	line, err := bufio.NewReader(stdoutR).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read port line: %v", err)
	}
	var ports map[string]int
	if err := json.Unmarshal(line, &ports); err != nil || len(ports) != 1 || ports["grpc"] == 0 {
		t.Fatalf("port line %q is not {\"grpc\":<port>}: %v", line, err)
	}
	srv.addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(ports["grpc"]))
	t.Cleanup(func() { srv.stop(t) })
	return srv
}

// stop closes the server's stdin and checks that it exits.
func (s *testServer) stop(t *testing.T) {
	t.Helper()
	_ = s.stdin.Close()
	select {
	case err := <-s.done:
		if err != nil {
			t.Errorf("server exited with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("server did not exit after stdin EOF")
	}
	s.done <- nil // keep later stops (Cleanup after an explicit stop) from blocking
}

func TestBenchScenarios(t *testing.T) {
	srv := startServer(t)
	for _, scenario := range scenarios {
		for _, concurrency := range []int{1, 4} {
			t.Run(scenario+"/c"+strconv.Itoa(concurrency), func(t *testing.T) {
				var out bytes.Buffer
				args := []string{
					"bench", "--addr", srv.addr, "--tcp-addr", "ignored:1", "--ca", srv.ca,
					"--server-name", "localhost", "--transport", "grpc", "--scenario", scenario,
					"--size", "100", "--concurrency", strconv.Itoa(concurrency),
					"--duration", "0.2", "--warmup", "0.05", "--messages", "10",
				}
				if err := run(t.Context(), args, nil, &out); err != nil {
					t.Fatal(err)
				}
				checkResult(t, out.Bytes(), scenario, concurrency)
			})
		}
	}
}

func checkResult(t *testing.T, line []byte, scenario string, concurrency int) {
	t.Helper()
	if bytes.Count(line, []byte("\n")) != 1 || !bytes.HasSuffix(line, []byte("\n")) {
		t.Fatalf("want exactly one JSON line, got %q", line)
	}
	var r map[string]any
	if err := json.Unmarshal(line, &r); err != nil {
		t.Fatal(err)
	}
	num := func(key string) float64 {
		t.Helper()
		v, ok := r[key].(float64)
		if !ok {
			t.Fatalf("%s missing or not a number in %s", key, line)
		}
		return v
	}
	if r["scenario"] != scenario || r["transport"] != "grpc" {
		t.Errorf("scenario/transport = %v/%v", r["scenario"], r["transport"])
	}
	if num("size") != 100 || int(num("concurrency")) != concurrency || num("duration_s") != 0.2 {
		t.Errorf("echoed parameters wrong: %s", line)
	}
	if num("errors") != 0 {
		t.Errorf("errors = %v", r["errors"])
	}
	ops, msgs := num("ops"), num("msgs")
	perOp := 1.0
	if scenario == "server-stream" || scenario == "client-stream" {
		perOp = 10
	}
	if ops == 0 || msgs != ops*perOp {
		t.Errorf("ops = %v, msgs = %v, want msgs = ops x %v", ops, msgs, perOp)
	}
	if got, want := num("ops_per_sec"), ops/0.2; math.Abs(got-want) > 1e-6*want {
		t.Errorf("ops_per_sec = %v, want %v", got, want)
	}
	if got, want := num("msgs_per_sec"), msgs/0.2; math.Abs(got-want) > 1e-6*want {
		t.Errorf("msgs_per_sec = %v, want %v", got, want)
	}
	if got, want := num("bytes_per_sec"), num("msgs_per_sec")*100; math.Abs(got-want) > 1e-6*want {
		t.Errorf("bytes_per_sec = %v, want %v", got, want)
	}
	latency, ok := r["latency_us"].(map[string]any)
	if !ok {
		t.Fatalf("latency_us missing in %s", line)
	}
	var prev float64
	for _, key := range []string{"p50", "p90", "p99", "p999"} {
		v, ok := latency[key].(float64)
		if !ok || v <= 0 || v < prev {
			t.Errorf("latency_us.%s = %v, want positive and nondecreasing (prev %v)", key, latency[key], prev)
		}
		prev = v
	}
	if wire := num("wire_bytes_per_msg"); wire < 100 {
		t.Errorf("wire_bytes_per_msg = %v, want at least the 100 payload bytes", wire)
	}
}

func TestBenchRejectsOtherTransports(t *testing.T) {
	err := run(t.Context(), []string{"bench", "--addr", "x:1", "--ca", "ca.pem", "--scenario", "unary", "--transport", "quic"}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "transport") {
		t.Fatalf("err = %v, want unsupported transport", err)
	}
}

func TestServerExitsOnStdinEOF(t *testing.T) {
	startServer(t).stop(t)
}

// TestEchoSemantics checks the interop Echo behavior from interop/README.md.
func TestEchoSemantics(t *testing.T) {
	srv := startServer(t)
	pem, err := os.ReadFile(srv.ca)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	conn, err := grpc.NewClient(srv.addr, grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost"})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := echopb.NewEchoClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("unary suffix", func(t *testing.T) {
		callCtx := metadata.AppendToOutgoingContext(ctx, "interop-suffix", "!")
		var header, trailer metadata.MD
		resp, err := client.Unary(callCtx, &echopb.EchoRequest{Message: "hello"}, grpc.Header(&header), grpc.Trailer(&trailer))
		if err != nil || resp.GetMessage() != "hello!" || resp.GetIndex() != 0 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
		if len(header.Get("interop-header")) != 0 || len(trailer.Get("interop-trailer")) != 0 {
			t.Errorf("unexpected header %v / trailer %v without interop-header", header, trailer)
		}
	})
	t.Run("unary headers", func(t *testing.T) {
		callCtx := metadata.AppendToOutgoingContext(ctx, "interop-header", "h")
		var header, trailer metadata.MD
		if _, err := client.Unary(callCtx, &echopb.EchoRequest{Message: "hello"}, grpc.Header(&header), grpc.Trailer(&trailer)); err != nil {
			t.Fatal(err)
		}
		if header.Get("interop-header")[0] != "h" || trailer.Get("interop-trailer")[0] != "h" {
			t.Errorf("header %v, trailer %v", header, trailer)
		}
	})
	t.Run("unary error", func(t *testing.T) {
		_, err := client.Unary(ctx, &echopb.EchoRequest{FailCode: 5, FailMessage: "missing"})
		if s := status.Convert(err); s.Code() != codes.NotFound || s.Message() != "missing" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("client stream", func(t *testing.T) {
		stream, err := client.ClientStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range []string{"a", "b", "c"} {
			if err := stream.Send(&echopb.EchoRequest{Message: m}); err != nil {
				t.Fatal(err)
			}
		}
		resp, err := stream.CloseAndRecv()
		if err != nil || resp.GetMessage() != "abc" || resp.GetIndex() != 3 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
	})
	t.Run("server stream error", func(t *testing.T) {
		stream, err := client.ServerStream(ctx, &echopb.EchoRequest{Message: "s", Count: 2, FailCode: 9, FailMessage: "stop"})
		if err != nil {
			t.Fatal(err)
		}
		for i := range uint32(2) {
			resp, err := stream.Recv()
			if err != nil || resp.GetMessage() != "s" || resp.GetIndex() != i {
				t.Fatalf("resp %d = %v, err = %v", i, resp, err)
			}
		}
		if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "stop" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bidi error", func(t *testing.T) {
		stream, err := client.Bidi(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&echopb.EchoRequest{Message: "1"}); err != nil {
			t.Fatal(err)
		}
		if resp, err := stream.Recv(); err != nil || resp.GetMessage() != "1" || resp.GetIndex() != 0 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
		if err := stream.Send(&echopb.EchoRequest{FailCode: 10, FailMessage: "abort"}); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.Aborted || status.Convert(err).Message() != "abort" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bidi ok", func(t *testing.T) {
		stream, err := client.Bidi(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for i, m := range []string{"1", "2", "3"} {
			if err := stream.Send(&echopb.EchoRequest{Message: m}); err != nil {
				t.Fatal(err)
			}
			if resp, err := stream.Recv(); err != nil || resp.GetMessage() != m || int(resp.GetIndex()) != i {
				t.Fatalf("resp = %v, err = %v", resp, err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want EOF", err)
		}
	})
}
