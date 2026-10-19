package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
)

// startBenchServer runs the server subcommand in-process and returns the
// certificate file and the server's listening line.
func startBenchServer(t *testing.T) (certFile string, ports listeningLine) {
	t.Helper()
	certFile, keyFile := writeTestCertificate(t)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	var stderr bytes.Buffer
	exit := make(chan int, 1)
	go func() {
		exit <- run([]string{"server", "--cert", certFile, "--key", keyFile}, stdinReader, stdoutWriter, &stderr)
		_ = stdoutWriter.Close()
	}()
	t.Cleanup(func() {
		_ = stdinWriter.Close()
		select {
		case code := <-exit:
			if code != 0 {
				t.Errorf("server exit code = %d (stderr: %s)", code, stderr.String())
			}
		case <-time.After(10 * time.Second):
			t.Error("server did not stop after stdin reached EOF")
		}
	})
	line, err := bufio.NewReader(stdoutReader).ReadString('\n')
	if err != nil {
		t.Fatalf("server did not print its listening line: %v (stderr: %s)", err, stderr.String())
	}
	if err := json.Unmarshal([]byte(line), &ports); err != nil {
		t.Fatalf("listening line %q: %v", line, err)
	}
	return certFile, ports
}

func TestBenchScenariosReportTheContractFields(t *testing.T) {
	certFile, ports := startBenchServer(t)
	const size, messages = 100, 5

	for _, tc := range []struct {
		transport     string
		wantTransport string
		wantWire      bool
	}{
		{"quic", "quic", true},
		{"tcp", "qmux", false},
	} {
		for _, scenario := range []string{"unary", "server-stream", "client-stream", "bidi", "unreliable-server-stream", "unreliable-client-stream"} {
			// A metadata size of 0 sends no metadata; the others send one entry
			// of that many bytes, the largest being the limit.
			for _, metadataSize := range []int{0, 300, benchMaxMetadataSize} {
				if metadataSize == benchMaxMetadataSize && scenario != "unary" {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/md%d", tc.transport, scenario, metadataSize), func(t *testing.T) {
					var stdout, stderr bytes.Buffer
					args := []string{
						"bench",
						"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
						"--tcp-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.QMux)),
						"--ca", certFile,
						"--transport", tc.transport,
						"--scenario", scenario,
						"--size", strconv.Itoa(size),
						"--metadata-size", strconv.Itoa(metadataSize),
						"--messages", strconv.Itoa(messages),
						"--concurrency", "2",
						"--duration", "0.2",
						"--warmup", "0.05",
					}
					if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
						t.Fatalf("exit code = %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
					}
					checkBenchLine(t, stdout.String(), scenario, tc.wantTransport, size, metadataSize, messages, tc.wantWire)
				})
			}
		}
	}
}

func checkBenchLine(t *testing.T, output, scenario, transport string, size, metadataSize, messages int, wantWire bool) {
	t.Helper()
	if strings.Count(output, "\n") != 1 {
		t.Fatalf("stdout = %q, want exactly one line", output)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("stdout %q is not JSON: %v", output, err)
	}
	number := func(key string) float64 {
		t.Helper()
		value, ok := got[key].(float64)
		if !ok {
			t.Fatalf("%s = %v, want a number in %s", key, got[key], output)
		}
		return value
	}
	if got["scenario"] != scenario || got["transport"] != transport {
		t.Errorf("scenario, transport = %v, %v, want %s, %s", got["scenario"], got["transport"], scenario, transport)
	}
	if number("size") != float64(size) || number("concurrency") != 2 {
		t.Errorf("size, concurrency = %v, %v", got["size"], got["concurrency"])
	}
	if number("metadata_size") != float64(metadataSize) {
		t.Errorf("metadata_size = %v, want %d", got["metadata_size"], metadataSize)
	}
	if number("duration_s") != 0.2 {
		t.Errorf("duration_s = %v, want 0.2", got["duration_s"])
	}
	ops, msgs, lost := number("ops"), number("msgs"), number("lost")
	perOp := 1.0
	if strings.HasSuffix(scenario, "stream") {
		perOp = float64(messages)
	}
	if strings.HasPrefix(scenario, "unreliable-") {
		// Every message is either delivered or lost, and TCP loses none.
		if ops < 1 || msgs+lost != ops*perOp || (transport == "qmux" && lost != 0) {
			t.Errorf("ops, msgs, lost = %v, %v, %v, want msgs + lost = %v per op and no loss over QMux", ops, msgs, lost, perOp)
		}
	} else if ops < 1 || msgs != ops*perOp || lost != 0 {
		t.Errorf("ops, msgs, lost = %v, %v, %v, want msgs = %v per op and no loss", ops, msgs, lost, perOp)
	}
	if number("errors") != 0 {
		t.Errorf("errors = %v, want 0", got["errors"])
	}
	if want := ops / 0.2; number("ops_per_sec") != want {
		t.Errorf("ops_per_sec = %v, want %v", got["ops_per_sec"], want)
	}
	if want := msgs / 0.2; number("msgs_per_sec") != want {
		t.Errorf("msgs_per_sec = %v, want %v", got["msgs_per_sec"], want)
	}
	if want := msgs / 0.2 * float64(size); number("bytes_per_sec") != want {
		t.Errorf("bytes_per_sec = %v, want %v", got["bytes_per_sec"], want)
	}
	latency, ok := got["latency_us"].(map[string]any)
	if !ok {
		t.Fatalf("latency_us = %v, want an object", got["latency_us"])
	}
	previous := 0.0
	for _, key := range []string{"p50", "p90", "p99", "p999"} {
		value, ok := latency[key].(float64)
		if !ok || value <= 0 || value < previous {
			t.Errorf("latency_us.%s = %v, want a positive percentile no smaller than the one before", key, latency[key])
		}
		previous = value
	}
	wire, hasWire := got["wire_bytes_per_msg"]
	if hasWire != wantWire {
		t.Fatalf("wire_bytes_per_msg present = %v, want %v", hasWire, wantWire)
	}
	if hasWire {
		if value, ok := wire.(float64); !ok || value < float64(size) {
			t.Errorf("wire_bytes_per_msg = %v, want at least the %d payload bytes", wire, size)
		}
	}
}

func TestBenchCommandLineErrors(t *testing.T) {
	for _, args := range [][]string{
		{"bench"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "bogus"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--transport", "auto"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--concurrency", "0"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--duration", "0"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--metadata-size", "-1"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--metadata-size", strconv.Itoa(benchMaxMetadataSize + 1)},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 2 {
			t.Errorf("run(%v) = %d, want usage error 2", args, code)
		}
	}
}

// padRecorder is an Echo server that records the bench-pad metadata of every
// call it handles. Only Unary and Bidi are implemented.
type padRecorder struct {
	interoppb.EchoServer
	pads chan []byte
}

func (r padRecorder) record(ctx context.Context) {
	r.pads <- trevrpc.RequestMetadataFromContext(ctx)[benchPadKey]
}

func (r padRecorder) Unary(ctx context.Context, request *interoppb.EchoRequest) (*trevrpc.Response[*interoppb.EchoResponse], error) {
	r.record(ctx)
	return &trevrpc.Response[*interoppb.EchoResponse]{Message: &interoppb.EchoResponse{Message: request.Message}}, nil
}

func (r padRecorder) Bidi(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (trevrpc.MessageStream[*interoppb.EchoResponse], error) {
	r.record(ctx)
	return &echoOnce{requests: requests}, nil
}

// echoOnce echoes the first request and then ends the stream.
type echoOnce struct {
	requests trevrpc.MessageStream[*interoppb.EchoRequest]
	done     bool
}

func (s *echoOnce) Recv() (*interoppb.EchoResponse, error) {
	if s.done {
		return nil, io.EOF
	}
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}
	s.done = true
	return &interoppb.EchoResponse{Message: request.Message}, nil
}

func (s *echoOnce) Close() error { return s.requests.Close() }

// TestBenchMetadataSizeAddsOnePadEntryToEveryCall checks what the smoke test
// cannot: that the server receives exactly one bench-pad entry of the asked
// size, and none when the size is 0.
func TestBenchMetadataSizeAddsOnePadEntryToEveryCall(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	recorder := padRecorder{pads: make(chan []byte, 8)}
	server := trevrpc.NewServer()
	interoppb.RegisterEchoServer(server, recorder)
	listener, err := trevrpc.Listen("127.0.0.1:0", server, trevrpc.ListenOptions{TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { defer close(served); _ = listener.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-served
		_ = listener.Close()
	})

	for _, size := range []int{0, 1, 4096} {
		config := benchConfig{
			connectionConfig: connectionConfig{addr: listener.Addr().String(), caFile: certFile, serverName: "localhost", transport: "quic"},
			metadata:         size,
		}
		channel, err := config.dial(ctx, trevrpc.DialOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer closeQuietly(channel)
		clients := benchClients{echo: interoppb.NewEchoClient(channel), unreliable: interoppb.NewUnreliableEchoClient(channel)}
		for _, scenario := range []string{"unary", "bidi"} {
			operation := benchScenarios[scenario](clients, config)
			if _, err := operation.run(ctx); err != nil {
				t.Fatalf("%s with metadata size %d: %v", scenario, size, err)
			}
			operation.close()
			select {
			case pad := <-recorder.pads:
				if size == 0 && pad != nil {
					t.Errorf("%s sent %d bytes of %s, want no metadata", scenario, len(pad), benchPadKey)
				}
				if size > 0 && !bytes.Equal(pad, bytes.Repeat([]byte{'x'}, size)) {
					t.Errorf("%s sent %d bytes of %s, want %d 'x' bytes", scenario, len(pad), benchPadKey, size)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s with metadata size %d did not reach the server", scenario, size)
			}
		}
	}
}
