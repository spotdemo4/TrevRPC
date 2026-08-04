package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStrictCommandParsing(t *testing.T) {
	valid := "RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001"
	command, stop, err := parseCommand(valid)
	if err != nil || stop || command.Operation != "codec.decode" {
		t.Fatalf("valid command: %#v %v %v", command, stop, err)
	}
	for _, invalid := range []string{
		"RUN\t01\tcase.id\tcodec.decode\trpc_request\t3001",
		"RUN\t1\tCase\tcodec.decode\trpc_request\t3001",
		"RUN\t1\tcase.id\tcodec.decode\trpc_request\tABC0",
		"RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001\textra",
	} {
		if _, _, err := parseCommand(invalid); err == nil {
			t.Fatalf("accepted invalid command %q", invalid)
		}
	}
}

func TestEventSizeBoundary(t *testing.T) {
	rawString := func(payloadBytes int) json.RawMessage {
		return json.RawMessage(`"` + strings.Repeat("a", payloadBytes) + `"`)
	}

	var exact bytes.Buffer
	if err := emitEvent(bufio.NewWriter(&exact), rawString(maxEventBytes-3)); err != nil {
		t.Fatalf("emit exact-limit event: %v", err)
	}
	if exact.Len() != maxEventBytes {
		t.Fatalf("exact event length = %d, want %d", exact.Len(), maxEventBytes)
	}

	var oversized bytes.Buffer
	err := emitEvent(bufio.NewWriter(&oversized), rawString(maxEventBytes-2))
	if !errors.Is(err, errEventTooLarge) {
		t.Fatalf("oversized event error = %v, want %v", err, errEventTooLarge)
	}
	if oversized.Len() != 0 {
		t.Fatalf("oversized event wrote %d bytes", oversized.Len())
	}
}

func buildAdapter(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "adapter")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build adapter: %v\n%s", err, output)
	}
	return binary
}

func runAdapter(t *testing.T, binary, input string) (string, string, error) {
	t.Helper()
	command := exec.Command(binary, "--protocol", "1")
	command.Stdin = bytes.NewBufferString(input)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.String(), stderr.String(), err
}

func TestCommandLineBoundariesAndHugeCounts(t *testing.T) {
	binary := buildAdapter(t)
	prefix := "RUN\t1\tboundary\tcodec.decode\trpc_request\t"
	if (maxCommandBytes-len(prefix))%2 != 0 {
		prefix = "RUN\t1\tboundaryx\tcodec.decode\trpc_request\t"
	}
	exact := prefix + strings.Repeat("00", (maxCommandBytes-len(prefix))/2)
	if len(exact) != maxCommandBytes {
		t.Fatalf("exact command length = %d", len(exact))
	}
	stdout, stderr, err := runAdapter(t, binary, exact+"\nSTOP\n")
	if err != nil || !strings.Contains(stdout, `"event":"result"`) {
		t.Fatalf("exact-limit command failed: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}

	stdout, _, err = runAdapter(t, binary, exact+"0\n")
	if err == nil || !strings.Contains(stdout, `"event":"fatal"`) {
		t.Fatalf("over-limit command did not fail strictly: %v\n%s", err, stdout)
	}

	stdout, _, err = runAdapter(t, binary, "RUN\t1\thuge\tstate.server_stream\t9223372036854775807\n")
	if err == nil || !strings.Contains(stdout, `"event":"fatal"`) {
		t.Fatalf("huge count did not produce fatal event: %v\n%s", err, stdout)
	}
}

func TestAdapterStdoutIsProtocolOnly(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "adapter")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build adapter: %v\n%s", err, output)
	}
	command := exec.Command(binary, "--protocol", "1")
	command.Stdin = bytes.NewBufferString("STOP\n")
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run adapter: %v: %s", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"event":"ready"`)) {
		t.Fatalf("missing ready event: %s", stdout.String())
	}
}
