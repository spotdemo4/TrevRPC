package trevrpc

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"google.golang.org/protobuf/proto"
)

// goldenVector is one payload/frame pair of the shared wire vectors: the
// documented logical value, encoded the way a conforming sender does.
type goldenVector struct {
	name string
	// payload and frame encode the logical value.
	payload func(t *testing.T) []byte
	frame   func(t *testing.T) []byte
	// check decodes the frame and compares it with the logical value.
	check func(t *testing.T, got testFrame)
}

func mustMarshal(t *testing.T, message ProtoMessage) []byte {
	t.Helper()
	payload, err := proto.Marshal(protoMessageV2(message))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func mustFrame(t *testing.T, frame []byte, err error) []byte {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func requestVector(name string, request *wireRequest) goldenVector {
	return goldenVector{
		name:    name,
		payload: func(t *testing.T) []byte { return mustMarshal(t, request) },
		frame: func(t *testing.T) []byte {
			frame, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
			return mustFrame(t, frame, err)
		},
		check: func(t *testing.T, got testFrame) {
			t.Helper()
			if got.Kind != frameTypeRequest {
				t.Fatalf("frame type = %v, want REQUEST", got.Kind)
			}
			decoded := got.Request
			if decoded.Service != request.Service || decoded.Method != request.Method || decoded.Kind != request.Kind ||
				decoded.Version != request.Version || decoded.TimeoutNanos != request.TimeoutNanos ||
				!slices.Equal(decoded.Extensions, request.Extensions) || !metadataEqual(decoded.Metadata, request.Metadata) {
				t.Fatalf("decoded request = %#v, want %#v", decoded, request)
			}
		},
	}
}

func TestWireGoldenVectors(t *testing.T) {
	vectors := loadWireGoldenVectors(t)

	withMetadata := newWireRequest("svc", "m")
	withMetadata.Metadata["authorization"] = []byte("ok")
	bidi := newWireRequest("svc", "m")
	bidi.Kind = RPCKindBidirectionalStreaming
	bidi.TimeoutNanos = 123456
	extensions := newWireRequest("svc", "m")
	extensions.Extensions = []uint64{1, 300}

	statusVector := func(name string, status *Status) goldenVector {
		return goldenVector{
			name:    name,
			payload: func(t *testing.T) []byte { return mustMarshal(t, status.wire()) },
			frame: func(t *testing.T) []byte {
				frame, err := appendStatusFrame(nil, status, DefaultMaxFrameSize)
				return mustFrame(t, frame, err)
			},
			check: func(t *testing.T, got testFrame) {
				t.Helper()
				if got.Kind != frameTypeStatus {
					t.Fatalf("frame type = %v, want STATUS", got.Kind)
				}
				decoded := got.statusValue()
				if decoded.Code != status.Code || decoded.Message != status.Message || !metadataEqual(decoded.Metadata, status.Metadata) {
					t.Fatalf("decoded status = %#v, want %#v", decoded, status)
				}
			},
		}
	}
	headersVector := func(name string, headers *wireHeaders) goldenVector {
		return goldenVector{
			name:    name,
			payload: func(t *testing.T) []byte { return mustMarshal(t, headers) },
			frame: func(t *testing.T) []byte {
				message := protoMessageV2(headers)
				frame, err := appendProtoFrame(nil, frameTypeHeaders, message, proto.Size(message))
				return mustFrame(t, frame, err)
			},
			check: func(t *testing.T, got testFrame) {
				t.Helper()
				if got.Kind != frameTypeHeaders || !metadataEqual(got.Headers, headers.Metadata) {
					t.Fatalf("decoded headers = %#v, want %#v", got, headers)
				}
			},
		}
	}
	messageVector := func(name, body string) goldenVector {
		return goldenVector{
			name:    name,
			payload: func(*testing.T) []byte { return []byte(body) },
			frame:   func(*testing.T) []byte { return rawMessage([]byte(body)).frame },
			check: func(t *testing.T, got testFrame) {
				t.Helper()
				if got.Kind != frameTypeMessage || string(got.Body) != body {
					t.Fatalf("decoded message = %#v, want %q", got, body)
				}
			},
		}
	}

	tests := []goldenVector{
		requestVector("request.unary", newWireRequest("svc", "m")),
		requestVector("request.bidi_timeout", bidi),
		requestVector("request.metadata", withMetadata),
		requestVector("request.extensions", extensions),
		messageVector("message.hi", "hi"),
		messageVector("message.empty", ""),
		headersVector("headers.metadata", &wireHeaders{Metadata: Metadata{"x-a": []byte("1")}}),
		headersVector("headers.empty", &wireHeaders{}),
		headersVector("headers.extensions", &wireHeaders{Extensions: []uint64{1}}),
		statusVector("status.ok", OK()),
		statusVector("status.unavailable", Unavailable("down")),
		statusVector("status.metadata", NewStatus(CodeFailedPrecondition, "stop").WithMetadata(Metadata{"x-t": []byte("2")})),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertGoldenBytes(t, tt.name+".payload", tt.payload(t), vectors)
			frame := tt.frame(t)
			assertGoldenBytes(t, tt.name+".frame", frame, vectors)

			reader := newFrameReader(bytes.NewReader(frame), DefaultMaxFrameSize, -1)
			next, err := reader.next()
			if err != nil {
				t.Fatalf("read %s.frame: %v", tt.name, err)
			}
			if !bytes.Equal(next.payload, vectors[tt.name+".payload"]) {
				t.Fatalf("%s.frame payload = %x, want %x", tt.name, next.payload, vectors[tt.name+".payload"])
			}
			decoded, err := decodeTestFrame(next)
			if err != nil {
				t.Fatalf("decode %s.frame: %v", tt.name, err)
			}
			tt.check(t, decoded)
		})
	}

	t.Run("headers.extensions.decode", func(t *testing.T) {
		var decoded wireHeaders
		if err := decodePayload(vectors["headers.extensions.payload"], &decoded); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(decoded.Extensions, []uint64{1}) {
			t.Fatalf("decoded headers = %#v", decoded)
		}
	})

	t.Run("grease.two_byte_type", func(t *testing.T) {
		frame := appendFrameHeader(nil, 0x40, 3)
		frame = append(frame, 0, 1, 2)
		assertGoldenBytes(t, "grease.two_byte_type.frame", frame, vectors)
		assertGoldenBytes(t, "grease.two_byte_type.payload", frame[len(frame)-3:], vectors)
		if next, err := newFrameReader(bytes.NewReader(frame), DefaultMaxFrameSize, -1).next(); !errors.Is(err, io.EOF) {
			t.Fatalf("grease frame yielded %v, %v; want it skipped", next, err)
		}
	})
}

// TestWireGoldenStreams parses every shared byte stream with the real frame
// reader, whole and one byte at a time, and compares the known frames with the
// vector.
func TestWireGoldenStreams(t *testing.T) {
	entries := loadGoldenEntries(t)
	streams := 0
	for name, value := range entries {
		stream, ok := strings.CutSuffix(name, ".stream")
		if !ok {
			continue
		}
		streams++
		wantFrames, ok := entries[stream+".frames"]
		if !ok {
			t.Fatalf("wire golden vector %q has no .frames", name)
		}
		data := decodeGoldenHex(t, name, value)

		for readerName, wrap := range map[string]func(io.Reader) io.Reader{
			"whole":     func(r io.Reader) io.Reader { return r },
			"one byte":  iotest.OneByteReader,
			"half read": iotest.HalfReader,
		} {
			t.Run(stream+"/"+readerName, func(t *testing.T) {
				reader := newFrameReader(wrap(bytes.NewReader(data)), DefaultMaxFrameSize, -1)
				var got []string
				for {
					next, err := reader.next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("read %s: %v", name, err)
					}
					got = append(got, fmt.Sprintf("%d:%x", uint64(next.typ), next.payload))
				}
				if joined := strings.Join(got, ","); joined != wantFrames {
					t.Fatalf("%s frames = %q, want %q", name, joined, wantFrames)
				}
			})
		}
	}
	if streams == 0 {
		t.Fatal("no wire golden streams were found")
	}
}

func metadataEqual(left, right Metadata) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if !bytes.Equal(value, right[key]) {
			return false
		}
	}
	return true
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

func decodeGoldenHex(t *testing.T, name, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode wire golden vector %q: %v", name, err)
	}
	return decoded
}

// loadGoldenEntries reads the raw entries of the shared wire vectors. An empty
// value has no trailing space, so entries split on the first "=".
func loadGoldenEntries(t *testing.T) map[string]string {
	t.Helper()

	data, err := os.ReadFile("../testdata/wire-golden-vectors.txt")
	if err != nil {
		t.Fatalf("read wire golden vectors: %v", err)
	}

	entries := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("invalid wire golden vector line %d", lineNumber)
		}
		entries[strings.TrimSpace(name)] = strings.TrimSpace(value)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan wire golden vectors: %v", err)
	}

	return entries
}

// loadWireGoldenVectors returns the hex entries (payloads and frames) decoded.
func loadWireGoldenVectors(t *testing.T) map[string][]byte {
	t.Helper()

	vectors := map[string][]byte{}
	for name, value := range loadGoldenEntries(t) {
		if strings.HasSuffix(name, ".frames") {
			continue
		}
		vectors[name] = decodeGoldenHex(t, name, value)
	}
	return vectors
}
