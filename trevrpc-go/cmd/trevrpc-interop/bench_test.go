package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
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
		for _, scenario := range []string{"unary", "server-stream", "client-stream", "bidi"} {
			t.Run(tc.transport+"/"+scenario, func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				args := []string{
					"bench",
					"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
					"--tcp-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.QMux)),
					"--ca", certFile,
					"--transport", tc.transport,
					"--scenario", scenario,
					"--size", strconv.Itoa(size),
					"--messages", strconv.Itoa(messages),
					"--concurrency", "2",
					"--duration", "0.2",
					"--warmup", "0.05",
				}
				if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
					t.Fatalf("exit code = %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
				}
				checkBenchLine(t, stdout.String(), scenario, tc.wantTransport, size, messages, tc.wantWire)
			})
		}
	}
}

func checkBenchLine(t *testing.T, output, scenario, transport string, size, messages int, wantWire bool) {
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
	if number("duration_s") != 0.2 {
		t.Errorf("duration_s = %v, want 0.2", got["duration_s"])
	}
	ops, msgs := number("ops"), number("msgs")
	perOp := 1.0
	if scenario == "server-stream" || scenario == "client-stream" {
		perOp = float64(messages)
	}
	if ops < 1 || msgs != ops*perOp {
		t.Errorf("ops, msgs = %v, %v, want msgs = %v per op", ops, msgs, perOp)
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
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 2 {
			t.Errorf("run(%v) = %d, want usage error 2", args, code)
		}
	}
}
