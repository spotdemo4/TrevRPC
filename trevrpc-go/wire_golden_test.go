package trevrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/quic-go/quic-go/quicvarint"
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
			if decoded.Method != request.Method || decoded.Kind != request.Kind || decoded.TimeoutNanos != request.TimeoutNanos || decoded.DatagramFlow != request.DatagramFlow ||
				decoded.StreamCompression != request.StreamCompression || !metadataEqual(decoded.Metadata, request.Metadata) ||
				!connectionMetadataEqual(decoded.ConnectionMetadata, request.ConnectionMetadata) {
				t.Fatalf("decoded request = %#v, want %#v", decoded, request)
			}
		},
	}
}

// answerEqual reports whether two ANSWER payloads hold the same answers.
func answerEqual(a, b wireAnswer) bool {
	return a.StreamCompression == b.StreamCompression && a.ConnectionMetadata == b.ConnectionMetadata &&
		slices.EqualFunc(a.Dictionaries, b.Dictionaries, func(x, y *dictionaryInfo) bool {
			return x.ID == y.ID && bytes.Equal(x.Fingerprint, y.Fingerprint)
		})
}

func connectionMetadataEqual(a, b *connectionMetadataInfo) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ID == b.ID && metadataEqual(a.Metadata, b.Metadata)
}

func TestWireGoldenVectors(t *testing.T) {
	vectors := loadWireGoldenVectors(t)

	withMetadata := newWireRequest("/svc/m")
	withMetadata.Metadata["authorization"] = []byte("ok")
	bidi := newWireRequest("/svc/m")
	bidi.Kind = RPCKindBidirectionalStreaming
	bidi.TimeoutNanos = 123456
	datagramFlow := newWireRequest("/svc/m")
	datagramFlow.Kind = RPCKindBidirectionalStreaming
	datagramFlow.DatagramFlow = 1
	connectionOffer := newWireRequest("/svc/m")
	connectionOffer.ConnectionMetadata = &connectionMetadataInfo{ID: 1, Metadata: Metadata{"authorization": []byte("ok")}}
	connectionReference := newWireRequest("/svc/m")
	connectionReference.Metadata["x-a"] = []byte("1")
	connectionReference.ConnectionMetadata = &connectionMetadataInfo{ID: 1}

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
				if decoded.Code != status.Code || decoded.Message != status.Message || !metadataEqual(decoded.Metadata, status.Metadata) || !reflect.DeepEqual(decoded.Details, status.Details) {
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
	answerVector := func(name string, answer *wireAnswer) goldenVector {
		return goldenVector{
			name:    name,
			payload: func(t *testing.T) []byte { return mustMarshal(t, answer) },
			frame: func(t *testing.T) []byte {
				frame, err := appendAnswerFrame(nil, answer, DefaultMaxFrameSize)
				return mustFrame(t, frame, err)
			},
			check: func(t *testing.T, got testFrame) {
				t.Helper()
				if got.Kind != frameTypeAnswer || !answerEqual(got.Answer, *answer) {
					t.Fatalf("decoded answer = %#v, want %#v", got, answer)
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
		requestVector("request.unary", newWireRequest("/svc/m")),
		requestVector("request.bidi_timeout", bidi),
		requestVector("request.metadata", withMetadata),
		requestVector("request.datagram_flow", datagramFlow),
		requestVector("request.connection_metadata_offer", connectionOffer),
		requestVector("request.connection_metadata_reference", connectionReference),
		messageVector("message.hi", "hi"),
		messageVector("message.empty", ""),
		headersVector("headers.metadata", &wireHeaders{Metadata: Metadata{"x-a": []byte("1")}}),
		headersVector("headers.empty", &wireHeaders{}),
		answerVector("answer.connection_metadata", &wireAnswer{ConnectionMetadata: 1}),
		answerVector("answer.empty", &wireAnswer{}),
		statusVector("status.ok", OK()),
		statusVector("status.unavailable", Unavailable("down")),
		statusVector("status.metadata", NewStatus(CodeFailedPrecondition, "stop").WithMetadata(Metadata{"x-t": []byte("2")})),
		statusVector("status.details", InvalidArgument("bad").WithDetails(Detail{Type: "x.Y", Value: []byte{0x08, 0x01}})),
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

	// The reader takes a DATAGRAM_COUNT in itself instead of returning it.
	for name, count := range map[string]uint64{"datagram_count.one": 1, "datagram_count.large": 300} {
		t.Run(name, func(t *testing.T) {
			frame := appendDatagramCountFrame(nil, count)
			assertGoldenBytes(t, name+".frame", frame, vectors)
			assertGoldenBytes(t, name+".payload", quicvarint.Append(nil, count), vectors)
			reader := newFrameReader(bytes.NewReader(frame), DefaultMaxFrameSize, -1)
			if next, err := reader.next(); !errors.Is(err, io.EOF) {
				t.Fatalf("read %s.frame = %v, %v; want it taken in", name, next, err)
			}
			if !reader.counted || reader.datagramCount() != count {
				t.Fatalf("datagram count = %d (counted %v), want %d", reader.datagramCount(), reader.counted, count)
			}
		})
	}

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

// TestWireGoldenDatagrams checks every datagram.* vector: the flow ID is a hex
// integer and the messages are comma separated hex. An entry with a .datagram
// is sent from its messages by the real sender, byte for byte, and received;
// one with a .received is only received, to its messages (none for a datagram
// that is dropped whole). Receiving goes through the connection's dispatch
// into the call's queue, as a datagram from the network does.
func TestWireGoldenDatagrams(t *testing.T) {
	entries := loadGoldenEntries(t)
	vectors := map[string]bool{}
	for name := range entries {
		if vector, ok := strings.CutSuffix(name, ".flow"); ok && strings.HasPrefix(vector, "datagram.") {
			vectors[vector] = true
		}
	}
	if len(vectors) == 0 {
		t.Fatal("no datagram golden vectors were found")
	}
	for vector := range vectors {
		t.Run(vector, func(t *testing.T) {
			flow, err := strconv.ParseUint(entries[vector+".flow"], 16, 64)
			if err != nil {
				t.Fatalf("%s.flow: %v", vector, err)
			}
			var want [][]byte
			if value := entries[vector+".messages"]; value != "" {
				for i, hexMessage := range strings.Split(value, ",") {
					want = append(want, decodeGoldenHex(t, fmt.Sprintf("%s.messages[%d]", vector, i), hexMessage))
				}
			}

			wire, encoded := entries[vector+".datagram"]
			received, decodeOnly := entries[vector+".received"]
			if encoded == decodeOnly {
				t.Fatalf("%s must have exactly one of .datagram and .received", vector)
			}
			if decodeOnly {
				wire = received
			}
			datagram := decodeGoldenHex(t, vector, wire)

			// The vector's flow is a call of a server connection, which both
			// sends and receives its datagrams.
			conn := newFakeDatagramConn()
			defer conn.close()
			mux := newServerMux(conn)
			defer mux.close()
			call, err := mux.register(flow, true, DefaultMaxFrameSize, nil)
			if err != nil {
				t.Fatal(err)
			}

			if encoded {
				batch := make([]encodedMessage, len(want))
				for i, message := range want {
					batch[i] = rawMessage(message)
				}
				kept, err := call.sendBatch(context.Background(), batch)
				if err != nil || len(kept) != 0 {
					t.Fatalf("sendBatch(%x) kept %d messages for the stream, err = %v; want one datagram", want, len(kept), err)
				}
				if sent := conn.sentDatagrams(); len(sent) != 1 || !bytes.Equal(sent[0], datagram) {
					t.Errorf("sent datagrams = %x, want %x", sent, datagram)
				}
			}

			gotFlow, _, ok := parseDatagram(datagram)
			if !ok || gotFlow != flow {
				t.Fatalf("parseDatagram(%x) = %d, %v; want flow %d", datagram, gotFlow, ok, flow)
			}
			mux.dispatch(datagram)
			var got [][]byte
			for {
				message, ok := call.sink.pop()
				if !ok {
					break
				}
				got = append(got, message)
			}
			if len(got) != len(want) {
				t.Fatalf("decoded %x, want %x", got, want)
			}
			for i := range want {
				if !bytes.Equal(got[i], want[i]) {
					t.Fatalf("message %d = %x, want %x", i, got[i], want[i])
				}
			}
		})
	}

	// A payload that is empty, cut inside the varint, or addressed to flow 0
	// belongs to no call.
	for _, malformed := range [][]byte{nil, {0x40}, {0x00, 0x00, 0x00}} {
		if flow, frames, ok := parseDatagram(malformed); ok {
			t.Errorf("parseDatagram(%x) = %d, %x, true; want it rejected", malformed, flow, frames)
		}
	}
	// Frames that do not exactly fill the datagram drop it whole, whatever
	// precedes the fault.
	for _, frames := range [][]byte{
		{0x00},            // a type with no length
		{0x00, 0x40},      // a length cut inside its varint
		{0x00, 0x02, 'a'}, // a length past the end
		{0x00, 0x01, 'a', 0x00, 0x05, 'b'},
		{0x1f, 0x01}, // a skipped type with a length past the end
	} {
		if walkDatagramFrames(frames, nil) {
			t.Errorf("walkDatagramFrames(%x) = true; want the datagram dropped", frames)
		}
	}
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
		wantFrames = datagramCountValues(t, wantFrames)
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
					counted := reader.counted
					next, err := reader.next()
					if !counted && reader.counted {
						// Taken in on the way to the frame or the end returned.
						got = append(got, fmt.Sprintf("7:#%d", reader.datagramCount()))
					}
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

// datagramCountValues replaces the payload of every DATAGRAM_COUNT in a
// .frames value with the count it encodes, as #count, since the reader keeps
// the count rather than the payload.
func datagramCountValues(t *testing.T, frames string) string {
	t.Helper()
	entries := strings.Split(frames, ",")
	for i, entry := range entries {
		payload, ok := strings.CutPrefix(entry, "7:")
		if !ok {
			continue
		}
		count, n, err := quicvarint.Parse(decodeGoldenHex(t, entry, payload))
		if err != nil || n != len(payload)/2 {
			t.Fatalf("DATAGRAM_COUNT payload %q is not one varint", payload)
		}
		entries[i] = fmt.Sprintf("7:#%d", count)
	}
	return strings.Join(entries, ",")
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
		// Frame lists and the datagram entries have formats of their own.
		if strings.HasSuffix(name, ".frames") || strings.HasPrefix(name, "datagram.") {
			continue
		}
		vectors[name] = decodeGoldenHex(t, name, value)
	}
	return vectors
}
