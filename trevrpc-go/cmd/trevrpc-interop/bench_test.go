package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
)

// startBenchServer runs the server subcommand in-process, with extra flags,
// and returns the certificate file and the server's listening line.
func startBenchServer(t *testing.T, extra ...string) (certFile string, ports listeningLine) {
	t.Helper()
	certFile, keyFile := writeTestCertificate(t)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	var stderr bytes.Buffer
	exit := make(chan int, 1)
	go func() {
		exit <- run(append([]string{"server", "--cert", certFile, "--key", keyFile}, extra...), stdinReader, stdoutWriter, &stderr)
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

// benchStarvedRetries is how many more times benchRetryingStarved runs a bench
// that completed no operation in its measured window. Keep it in step with
// RETRIES in bench/run.py, the baseline smoke tests and the Rust self-interop
// tests.
const benchStarvedRetries = 2

// benchSlots bounds how many benches run at once, whatever -parallel allows.
// The bench tests run their subtests in parallel, and each bench keeps its
// workers busy back to back, so a few at once already fill the machine. With
// dozens at once on a many-core machine under load, and more so under the
// race detector, the goroutines that time each window wake late: windows
// complete no operation, paced echoes miss their deadline and the wire bytes
// are counted over a shorter span than the operations. Four held up in race
// runs on a machine at a load average of 60 to 100, where eight still ran a
// window again. The baseline smoke tests (bench/baseline/smoke_test.go) have
// the same bound for the same reason, duplicated because that module does not
// depend on trevrpc-go; change both together.
var benchSlots = make(chan struct{}, 4)

// benchLine runs the bench subcommand through benchRetryingStarved and returns
// its result line, failing the test if it does not succeed.
func benchLine(t *testing.T, args ...string) string {
	t.Helper()
	stdout, err := benchRetryingStarved(t, args)
	if err != nil {
		t.Fatal(err)
	}
	return stdout
}

// benchRetryingStarved runs the bench subcommand and returns its stdout. A run
// that completed no operation in its measured window while no call failed runs
// again up to benchStarvedRetries times. The tests keep the windows to a few
// tenths of a second, so on a machine saturated by other builds the workers
// may not fit a single call inside one, which says nothing about the code
// under test. Every other failure is returned at once, and a bench that never
// completes an operation still fails.
func benchRetryingStarved(t *testing.T, args []string) (string, error) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		var stdout, stderr bytes.Buffer
		benchSlots <- struct{}{}
		code := run(args, strings.NewReader(""), &stdout, &stderr)
		<-benchSlots
		if code == 0 {
			return stdout.String(), nil
		}
		if attempt < benchStarvedRetries && benchStarved(stderr.String()) {
			t.Logf("%s; running the bench again", lastLine(stderr.String()))
			continue
		}
		return "", fmt.Errorf("bench %v: exit code = %d\nstdout:\n%s\nstderr:\n%s", args[1:], code, stdout.String(), stderr.String())
	}
}

// benchStarved reports whether a bench's stderr ends by saying that no
// operation completed in its measured window and none failed.
func benchStarved(stderr string) bool {
	line := lastLine(stderr)
	return strings.Contains(line, "no operation completed in the") && strings.HasSuffix(line, "measured window (0 failed)")
}

// scriptedOperation stands in for the operation it wraps, which it only
// closes: every run fails with err, or carries one message after delay.
type scriptedOperation struct {
	benchOperation
	delay time.Duration
	err   error
}

func (o scriptedOperation) run(ctx context.Context) (int, error) {
	if o.err != nil {
		return 0, o.err
	}
	if err := sleepUntil(ctx, time.Now().Add(o.delay)); err != nil {
		return 0, err
	}
	return 1, nil
}

// TestBenchRetryingStarvedRunsOnlyAStarvedBenchAgain scripts every run of a
// one-worker unary bench, so that none depends on how busy the machine is: a
// starved run's every call outlasts the whole run, so none fits in its window,
// a failing run's every call fails, and a fast run's calls take a millisecond
// without a round trip.
func TestBenchRetryingStarvedRunsOnlyAStarvedBenchAgain(t *testing.T) {
	certFile, ports := startBenchServer(t)
	const warmup, duration = 50 * time.Millisecond, 200 * time.Millisecond
	starved := scriptedOperation{delay: 2 * (warmup + duration)}
	failing := scriptedOperation{err: errors.New("scripted failure")}
	fast := scriptedOperation{delay: time.Millisecond}
	unary := benchScenarios["unary"]
	t.Cleanup(func() { benchScenarios["unary"] = unary })
	for _, test := range []struct {
		name    string
		script  []scriptedOperation
		wantErr *regexp.Regexp
	}{
		{name: "fast", script: []scriptedOperation{fast}},
		{name: "starved once", script: []scriptedOperation{starved, fast}},
		{name: "starved twice", script: []scriptedOperation{starved, starved, fast}},
		{name: "always starved", script: []scriptedOperation{starved, starved, starved}, wantErr: regexp.MustCompile(`no operation completed in the 200ms measured window \(0 failed\)`)},
		{name: "failing", script: []scriptedOperation{failing}, wantErr: regexp.MustCompile(`no operation completed in the 200ms measured window \([1-9][0-9]* failed\)`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			runs := 0
			benchScenarios["unary"] = func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
				workload := unary(clients, config, payload).(operationLoop)
				operation := fast
				if runs++; runs <= len(test.script) {
					operation = test.script[runs-1]
				}
				operation.benchOperation = workload.operation
				workload.operation = operation
				return workload
			}
			_, err := benchRetryingStarved(t, []string{
				"bench",
				"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
				"--ca", certFile,
				"--scenario", "unary",
				"--concurrency", "1",
				"--duration", strconv.FormatFloat(duration.Seconds(), 'f', -1, 64),
				"--warmup", strconv.FormatFloat(warmup.Seconds(), 'f', -1, 64),
			})
			if runs != len(test.script) {
				t.Errorf("ran the bench %d times, want %d", runs, len(test.script))
			}
			switch {
			case test.wantErr == nil && err != nil:
				t.Errorf("err = %v, want success", err)
			case test.wantErr != nil && (err == nil || !test.wantErr.MatchString(err.Error())):
				t.Errorf("err = %v, want one matching %q", err, test.wantErr)
			}
		})
	}
}

// TestBenchScenariosReportTheContractFields runs every scenario over each
// transport and checks its result line against the contract.
//
// The bench tests run their subtests in parallel, each bench on channels of
// its own to the test's server, since a bench spends most of its time waiting
// out a fixed window. benchSlots bounds how many benches run at once, and a
// window that still completes no operation runs again in benchLine. The
// subtests only read benchScenarios, and the tests that swap its entries never
// run alongside them: those are top-level tests without t.Parallel, which run
// one after another and restore the map in a cleanup before the next starts.
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
		for _, scenario := range []string{"unary", "server-stream", "client-stream", "bidi", "unreliable-server-stream", "unreliable-client-stream", "paced", "unreliable-paced"} {
			// A metadata size of 0 sends no metadata; the others send one entry
			// of that many bytes, the largest being the limit.
			for _, metadataSize := range []int{0, 300, benchMaxMetadataSize} {
				if metadataSize == benchMaxMetadataSize && scenario != "unary" {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/md%d", tc.transport, scenario, metadataSize), func(t *testing.T) {
					t.Parallel()
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
						"--rate", "1000",
						"--concurrency", "2",
						"--duration", "0.2",
						"--warmup", "0.05",
					}
					stdout := benchLine(t, args...)
					checkBenchLine(t, stdout, scenario, tc.wantTransport, size, metadataSize, messages, tc.wantWire, false)
					checkBenchModes(t, stdout, "repeat", "none")
				})
			}
		}
	}
}

// TestBenchPacedWireBytesCountLateMessagesWhenSent holds back three of four
// paced workers until the window has ended, so they send its messages in a
// burst after it, and checks that the wire bytes are still divided by the
// messages sent while they were counted: the burst counts as delivered, but
// its bytes are not in the window. It runs paced, whose stream delivers the
// whole burst; unreliable-paced counts the same way but may drop its datagrams.
func TestBenchPacedWireBytesCountLateMessagesWhenSent(t *testing.T) {
	certFile, ports := startBenchServer(t)
	const size, warmup, duration = 100, 50 * time.Millisecond, 200 * time.Millisecond
	pace := benchScenarios["paced"]
	t.Cleanup(func() { benchScenarios["paced"] = pace })
	workers := 0
	benchScenarios["paced"] = func(clients benchClients, config benchConfig, payload benchPayload) benchWorkload {
		workload := pace(clients, config, payload).(*pacedWorkload)
		if workers++; workers > 1 {
			open, opened := workload.open, false
			workload.open = func(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*interoppb.EchoRequest, *interoppb.EchoResponse], error) {
				if !opened {
					opened = true
					if err := sleepUntil(ctx, time.Now().Add(warmup+duration+50*time.Millisecond)); err != nil {
						return nil, err
					}
				}
				return open(ctx, options...)
			}
		}
		return workload
	}
	var stdout, stderr bytes.Buffer
	args := []string{
		"bench",
		"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
		"--ca", certFile,
		"--scenario", "paced",
		"--size", strconv.Itoa(size),
		"--rate", "1000",
		"--concurrency", "4",
		"--duration", strconv.FormatFloat(duration.Seconds(), 'f', -1, 64),
		"--warmup", strconv.FormatFloat(warmup.Seconds(), 'f', -1, 64),
	}
	if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	var got struct {
		Msgs            uint64
		WireBytesPerMsg float64 `json:"wire_bytes_per_msg"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout %q is not JSON: %v", stdout.String(), err)
	}
	// Each worker schedules about 200 messages in the window, so more than
	// that means the late workers' burst was delivered and counted.
	if got.Msgs <= 250 {
		t.Errorf("msgs = %d, want the late workers' messages delivered too", got.Msgs)
	}
	if got.WireBytesPerMsg < size {
		t.Errorf("wire_bytes_per_msg = %v, want at least the %d payload bytes", got.WireBytesPerMsg, size)
	}
}

// TestBenchValidationScenariosCallEveryRequestType runs the validation
// scenarios with each --validation value and checks that the result names it.
func TestBenchValidationScenariosCallEveryRequestType(t *testing.T) {
	certFile, ports := startBenchServer(t)
	const size, messages = 100, 5
	for _, scenario := range []string{"validation-unary", "validation-client-stream"} {
		for _, validation := range []string{"none", "standard", "cel"} {
			t.Run(scenario+"/"+validation, func(t *testing.T) {
				t.Parallel()
				args := []string{
					"bench",
					"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
					"--ca", certFile,
					"--scenario", scenario,
					"--validation", validation,
					"--size", strconv.Itoa(size),
					"--messages", strconv.Itoa(messages),
					"--concurrency", "2",
					"--duration", "0.2",
					"--warmup", "0.05",
				}
				stdout := benchLine(t, args...)
				checkBenchLine(t, stdout, scenario, "quic", size, 0, messages, true, false)
				var got struct{ Validation string }
				if err := json.Unmarshal([]byte(stdout), &got); err != nil {
					t.Fatal(err)
				}
				if got.Validation != validation {
					t.Errorf("validation = %q, want %q", got.Validation, validation)
				}
			})
		}
	}
}

// TestBenchSendsConnectionMetadata checks --metadata-scope connection, which
// sends the bench-pad entry as connection metadata.
func TestBenchSendsConnectionMetadata(t *testing.T) {
	certFile, ports := startBenchServer(t)
	for _, transport := range []string{"quic", "tcp"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			args := []string{
				"bench",
				"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
				"--tcp-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.QMux)),
				"--ca", certFile,
				"--transport", transport,
				"--scenario", "unary",
				"--metadata-size", "300",
				"--metadata-scope", "connection",
				"--concurrency", "2",
				"--duration", "0.2",
				"--warmup", "0.05",
			}
			stdout := benchLine(t, args...)
			var got map[string]any
			if err := json.Unmarshal([]byte(stdout), &got); err != nil {
				t.Fatalf("result %q: %v", stdout, err)
			}
			if got["metadata_scope"] != "connection" || got["metadata_size"] != float64(300) || got["errors"] != float64(0) {
				t.Fatalf("result = %s", stdout)
			}
		})
	}
}

// TestBenchSpreadsWorkersOverConnections checks --connections, which gives
// the workers that many channels.
func TestBenchSpreadsWorkersOverConnections(t *testing.T) {
	certFile, ports := startBenchServer(t)
	for _, transport := range []string{"quic", "tcp"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			args := []string{
				"bench",
				"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
				"--tcp-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.QMux)),
				"--ca", certFile,
				"--transport", transport,
				"--scenario", "unary",
				"--concurrency", "6",
				"--connections", "3",
				"--duration", "0.2",
				"--warmup", "0.05",
			}
			stdout := benchLine(t, args...)
			var got map[string]any
			if err := json.Unmarshal([]byte(stdout), &got); err != nil {
				t.Fatalf("result %q: %v", stdout, err)
			}
			if got["connections"] != float64(3) || got["errors"] != float64(0) || got["ops"] == float64(0) {
				t.Fatalf("result = %s", stdout)
			}
		})
	}
}

// checkBenchLine checks a result line against the contract. A compressed run
// may carry fewer wire bytes than its payload.
func checkBenchLine(t *testing.T, output, scenario, transport string, size, metadataSize, messages int, wantWire, compressed bool) {
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
	switch {
	case strings.HasSuffix(scenario, "paced"):
		// Each operation is one delivered message. Two workers at 1000
		// messages per second schedule about 400 in the window, each either
		// delivered or lost, and a stream loses none.
		if ops < 1 || msgs != ops || msgs+lost < 300 || msgs+lost > 500 || ((transport == "qmux" || scenario == "paced") && lost != 0) {
			t.Errorf("ops, msgs, lost = %v, %v, %v, want msgs = ops, about 400 messages and no loss on a stream", ops, msgs, lost)
		}
		if number("rate") != 1000 {
			t.Errorf("rate = %v, want 1000", got["rate"])
		}
	case got["rate"] != nil:
		t.Errorf("rate = %v, want none outside the paced scenarios", got["rate"])
	case strings.HasPrefix(scenario, "unreliable-"):
		// Every message is either delivered or lost, and TCP loses none.
		if ops < 1 || msgs+lost != ops*perOp || (transport == "qmux" && lost != 0) {
			t.Errorf("ops, msgs, lost = %v, %v, %v, want msgs + lost = %v per op and no loss over QMux", ops, msgs, lost, perOp)
		}
	case ops < 1 || msgs != ops*perOp || lost != 0:
		t.Errorf("ops, msgs, lost = %v, %v, %v, want msgs = %v per op and no loss", ops, msgs, lost, perOp)
	}
	if number("errors") != 0 {
		t.Errorf("errors = %v, want 0", got["errors"])
	}
	if _, has := got["validation"]; has != strings.HasPrefix(scenario, "validation-") {
		t.Errorf("validation = %v, want it in the validation scenarios only", got["validation"])
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
		if value, ok := wire.(float64); !ok || value <= 0 || (!compressed && value < float64(size)) {
			t.Errorf("wire_bytes_per_msg = %v, want at least the %d payload bytes", wire, size)
		}
	}
}

// checkBenchModes checks the payload and compression fields of a result line.
func checkBenchModes(t *testing.T, output, payload, compression string) {
	t.Helper()
	var got struct{ Payload, Compression string }
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("stdout %q is not JSON: %v", output, err)
	}
	if got.Payload != payload || got.Compression != compression {
		t.Errorf("payload, compression = %q, %q, want %q, %q", got.Payload, got.Compression, payload, compression)
	}
}

// TestBenchCompressionModesShrinkTheRecordsPayload runs bidi round trips of
// records messages over QUIC in every compression mode, against a server that
// holds the dictionary, and checks that each mode carries fewer bytes than
// none.
func TestBenchCompressionModesShrinkTheRecordsPayload(t *testing.T) {
	dictionary := writeTestDictionary(t)
	certFile, ports := startBenchServer(t, "--dictionary", dictionary)
	const size = 1024
	wire := make(map[string]float64)
	for _, compression := range []string{"none", "dictionary", "stream", "stream-dictionary"} {
		for _, scenario := range []string{"client-stream", "bidi"} {
			args := []string{
				"bench",
				"--addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(ports.Native)),
				"--ca", certFile,
				"--scenario", scenario,
				"--size", strconv.Itoa(size),
				"--messages", "5",
				"--concurrency", "2",
				"--duration", "0.2",
				"--warmup", "0.1",
				"--payload", "records",
				"--compression", compression,
			}
			if compression == "dictionary" || compression == "stream-dictionary" {
				args = append(args, "--dictionary", dictionary)
			}
			stdout := benchLine(t, args...)
			checkBenchLine(t, stdout, scenario, "quic", size, 0, 5, true, compression != "none")
			checkBenchModes(t, stdout, "records", compression)
			if scenario == "bidi" {
				var result struct {
					WireBytesPerMsg float64 `json:"wire_bytes_per_msg"`
				}
				if err := json.Unmarshal([]byte(stdout), &result); err != nil {
					t.Fatal(err)
				}
				wire[compression] = result.WireBytesPerMsg
			}
		}
	}
	for _, compression := range []string{"dictionary", "stream", "stream-dictionary"} {
		if wire[compression] >= wire["none"] {
			t.Errorf("%s carried %.0f bytes per round trip, no fewer than none's %.0f", compression, wire[compression], wire["none"])
		}
	}
}

// closedAddr returns a loopback address that nothing listens on over TCP.
func closedAddr(t *testing.T) string {
	t.Helper()
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// lastLine returns the last non-empty line of text.
func lastLine(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return lines[len(lines)-1]
}

// TestBenchSaysWhenItCouldNotConnect checks the line bench/run.py runs a
// cell again on, and that a configuration error does not print it.
func TestBenchSaysWhenItCouldNotConnect(t *testing.T) {
	certFile, _ := writeTestCertificate(t)
	addr := closedAddr(t)
	for _, test := range []struct {
		addr, ca string
		want     bool
	}{
		{addr: addr, ca: certFile, want: true},
		{addr: addr, ca: certFile + ".missing", want: false},
		// Dial rejects an address without a port before it connects.
		{addr: "127.0.0.1", ca: certFile, want: false},
	} {
		args := []string{"bench", "--addr", test.addr, "--ca", test.ca, "--transport", "tcp", "--scenario", "unary", "--duration", "1", "--warmup", "0"}
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code != 1 {
			t.Fatalf("run(%v) = %d, want 1 (stderr: %s)", args, code, stderr.String())
		}
		if got := strings.Contains(lastLine(stderr.String()), "could not connect"); got != test.want {
			t.Errorf("--addr %s --ca %s: last line %q says could not connect = %v, want %v", test.addr, test.ca, lastLine(stderr.String()), got, test.want)
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
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--metadata-scope", "stream"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--connections", "0"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--concurrency", "2", "--connections", "3"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--metadata-size", strconv.Itoa(benchMaxMetadataSize + 1)},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--payload", "bogus"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--compression", "gzip"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "server-stream", "--compression", "stream"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "server-stream", "--compression", "dictionary", "--dictionary", "d"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unreliable-client-stream", "--compression", "stream"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unreliable-paced", "--compression", "dictionary", "--dictionary", "d"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unreliable-paced", "--rate", "0"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "bidi", "--compression", "dictionary"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "bidi", "--dictionary", "d"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "validation-unary", "--validation", "bogus"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", "--validation", "cel"},
		{"bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "validation-client-stream", "--compression", "stream"},
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

func (r padRecorder) Unary(ctx context.Context, request *interoppb.EchoRequest) (*interoppb.EchoResponse, error) {
	r.record(ctx)
	return &interoppb.EchoResponse{Message: request.Message}, nil
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
			operation := benchScenarios[scenario](clients, config, newBenchPayload("repeat", config.size, 0)).(operationLoop).operation
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

// TestWireBytesAreDividedByTheMessagesOfTheSampledInterval divides a sample's
// bytes by the messages delivered between its two instants, not by the
// window's msgs: the sampler wakes late on a busy machine, so the two cover
// different intervals.
func TestWireBytesAreDividedByTheMessagesOfTheSampledInterval(t *testing.T) {
	config := &benchConfig{scenario: "unary"}
	got := wireSample{bytes: 1000, delivered: 4}.perMessage(config, nil, 10)
	if got == nil || *got != 250 {
		t.Fatalf("wire bytes per message = %v, want 250", got)
	}
	// A sampler starved past the window saw no message: the window's count is
	// the only one.
	if got := (wireSample{bytes: 1000}).perMessage(config, nil, 10); got == nil || *got != 100 {
		t.Fatalf("wire bytes per message with no message sampled = %v, want 100", got)
	}
	if got := (wireSample{bytes: 1000}).perMessage(config, nil, 0); got != nil {
		t.Fatalf("wire bytes per message with no message at all = %v, want none", *got)
	}
}

// stoppingOperation delivers one message on its first run and cancels the
// loop's context, so its next run fails and the loop returns.
type stoppingOperation struct {
	stop context.CancelFunc
	runs *int
}

func (o stoppingOperation) run(ctx context.Context) (int, error) {
	if *o.runs++; *o.runs == 1 {
		o.stop()
		return 1, nil
	}
	return 0, ctx.Err()
}
func (stoppingOperation) close() {}

// TestOperationsCountDeliveredMessagesOutsideTheWindow runs an operation
// before the window starts: it is not measured, but counts as delivered,
// which is what the wire sampling reads at its instants.
func TestOperationsCountDeliveredMessagesOutsideTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var w benchWorker
	var runs int
	start := time.Now().Add(time.Hour)
	operationLoop{operation: stoppingOperation{stop: cancel, runs: &runs}}.work(ctx, &w, start, start.Add(time.Hour))
	if w.messages != 0 {
		t.Fatalf("measured messages = %d, want 0 before the window", w.messages)
	}
	if got := w.delivered.Load(); got != 1 {
		t.Fatalf("delivered messages = %d, want the first operation's 1", got)
	}
}

// TestWireSamplerTakesWhatChangedBetweenTheBoundaries samples a connection and
// workers that already carried bytes and messages before the window, and has
// a worker see the start pass before the late main goroutine does: the sample
// holds only what changed between the first observation of each boundary.
func TestWireSamplerTakesWhatChangedBetweenTheBoundaries(t *testing.T) {
	start := time.Now()
	end := start.Add(time.Second)
	wire := new(wireCounter)
	workers := make([]benchWorker, 2)
	sampler := &wireSampler{wire: wire, workers: workers, start: start, end: end}
	wire.sent.Store(500)
	workers[0].delivered.Add(7)

	sampler.observe(start.Add(-time.Millisecond))
	wire.sent.Store(600)
	sampler.observe(start)
	wire.sent.Store(900)
	workers[1].delivered.Add(3)
	sampler.observe(start.Add(time.Millisecond))
	sampler.observe(end)
	wire.sent.Store(1000)
	workers[1].delivered.Add(1)
	sampler.observe(end.Add(time.Millisecond))

	got := sampler.sample()
	if got.bytes != 300 || got.delivered != 3 {
		t.Fatalf("sample = %d bytes and %d messages, want 300 and 3", got.bytes, got.delivered)
	}
	if got.to.Before(got.from) {
		t.Fatalf("sample from %v to %v, want its end after its start", got.from, got.to)
	}
}
