package trevrpc

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func TestWireGoldenVectors(t *testing.T) {
	vectors := loadWireGoldenVectors(t)

	timeoutRequest := NewRpcRequest("svc", "m", []byte("hi"))
	timeoutRequest.TimeoutNanos = 123456

	metadataRequest := NewRpcRequest("svc", "m", []byte("hi"))
	metadataRequest.Metadata["authorization"] = []byte("ok")

	tests := []struct {
		name    string
		message ProtoMessage
	}{
		{name: "rpc_request.unary", message: NewRpcRequest("svc", "m", []byte("hi"))},
		{name: "rpc_request.timeout", message: timeoutRequest},
		{name: "rpc_request.metadata", message: metadataRequest},
		{name: "rpc_stream_frame.message", message: MessageFrame([]byte("hi"))},
		{name: "rpc_stream_frame.status", message: StatusFrame(Unavailable("down"))},
		{name: "rpc_response.ok_body", message: OKResponse([]byte("hi"))},
		{name: "rpc_response.unavailable", message: Unavailable("down").IntoResponse(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWireGoldenVector(t, vectors, tt.name, tt.message)
		})
	}
}

func assertWireGoldenVector(t *testing.T, vectors map[string][]byte, name string, message ProtoMessage) {
	t.Helper()

	body, err := MarshalMessage(message)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	assertGoldenBytes(t, name+".body", body, vectors)

	frame, err := EncodeFrame(message, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode frame %s: %v", name, err)
	}
	assertGoldenBytes(t, name+".frame", frame, vectors)
}

func assertGoldenBytes(t *testing.T, name string, actual []byte, vectors map[string][]byte) {
	t.Helper()

	expected, ok := vectors[name]
	if !ok {
		t.Fatalf("missing wire golden vector %q", name)
	}

	if !bytes.Equal(actual, expected) {
		t.Fatalf("unexpected %s encoding:\nactual:   %x\nexpected: %x", name, actual, expected)
	}
}

func loadWireGoldenVectors(t *testing.T) map[string][]byte {
	t.Helper()

	data, err := os.ReadFile("../testdata/wire-golden-vectors.txt")
	if err != nil {
		t.Fatalf("read wire golden vectors: %v", err)
	}

	vectors := map[string][]byte{}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("invalid wire golden vector line %d", lineNumber)
		}

		decoded, err := hex.DecodeString(strings.TrimSpace(value))
		if err != nil {
			t.Fatalf("decode wire golden vector %q on line %d: %v", strings.TrimSpace(name), lineNumber, err)
		}

		vectors[strings.TrimSpace(name)] = decoded
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan wire golden vectors: %v", err)
	}

	return vectors
}
