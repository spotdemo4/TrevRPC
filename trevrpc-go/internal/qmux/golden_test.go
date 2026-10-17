package qmux

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

func loadGoldenVectors(t *testing.T) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile("../../../testdata/qmux-golden-vectors.txt")
	if err != nil {
		t.Fatalf("read QMux golden vectors: %v", err)
	}
	vectors := map[string][]byte{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		name, value, ok := strings.Cut(text, "=")
		if !ok {
			t.Fatalf("invalid golden vector line %d", line)
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(value))
		if err != nil {
			t.Fatalf("decode golden vector on line %d: %v", line, err)
		}
		vectors[strings.TrimSpace(name)] = decoded
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestGoldenVarints(t *testing.T) {
	vectors := loadGoldenVectors(t)
	for name, value := range map[string]uint64{
		"varint.1byte": 37,
		"varint.2byte": 15293,
		"varint.4byte": 494878333,
		"varint.8byte": 151288809941952652,
	} {
		if got := quicvarint.Append(nil, value); !bytes.Equal(got, vectors[name]) {
			t.Errorf("%s encodes as %x, want %x", name, got, vectors[name])
		}
	}
}

func goldenParameters() map[string]Parameters {
	return map[string]Parameters{
		"params.server": {
			MaxIdleTimeout:                 30 * time.Second,
			InitialMaxData:                 16777216,
			InitialMaxStreamDataBidiRemote: 4194308,
			InitialMaxStreamsBidi:          65,
			MaxRecordSize:                  65536,
		},
		"params.client": {
			InitialMaxData:                4194308,
			InitialMaxStreamDataBidiLocal: 4194308,
			MaxRecordSize:                 65536,
		},
	}
}

func TestGoldenFrames(t *testing.T) {
	vectors := loadGoldenVectors(t)
	encoded := map[string][]byte{
		"frame.stream.fin":            appendStreamFrame(nil, 0, 0, []byte("hi"), true),
		"frame.stream.offset":         appendStreamFrame(nil, 4, 16, []byte("hi"), false),
		"frame.reset_stream":          appendResetStream(nil, 0, 1, 5),
		"frame.stop_sending":          appendStopSending(nil, 0, 1),
		"frame.max_data":              appendMaxData(nil, 65),
		"frame.max_stream_data":       appendMaxStreamData(nil, 4, 1028),
		"frame.max_streams_bidi":      appendMaxStreamsBidi(nil, 65),
		"frame.application_close":     appendConnectionClose(nil, nil, 0, "bye"),
		"frame.transport_close":       appendConnectionClose(nil, &TransportError{ErrorCode: ProtocolViolation, FrameType: 0x08}, 0, ""),
		"frame.ping":                  appendPing(nil, Draft02.PingType, 1),
		"frame.ping_response":         appendPing(nil, Draft02.PingResponseType, 1),
		"message.stream_and_max_data": append(appendStreamFrame(nil, 0, 0, []byte("hi"), true), appendMaxData(nil, 65)...),
	}
	for name, params := range goldenParameters() {
		encoded[name] = appendTransportParameters(nil, &Draft02, params)
	}
	for name, got := range encoded {
		want, ok := vectors[name]
		if !ok {
			t.Errorf("missing golden vector %s", name)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s encodes as %x, want %x", name, got, want)
		}
		parser := frameParser{buf: want, version: &Draft02}
		for parser.more() {
			if _, err := parser.next(); err != nil {
				t.Errorf("%s does not decode: %v", name, err)
				break
			}
		}
	}
}

func TestGoldenParametersDecode(t *testing.T) {
	vectors := loadGoldenVectors(t)
	for name, want := range goldenParameters() {
		parser := frameParser{buf: vectors[name], version: &Draft02}
		f, err := parser.next()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := decodeParameters(f.params, &Draft02)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != want {
			t.Errorf("%s decodes as %+v, want %+v", name, got, want)
		}
	}
}

func TestGoldenRecords(t *testing.T) {
	vectors := loadGoldenVectors(t)
	frames := vectors["message.stream_and_max_data"]
	var buf bytes.Buffer
	conn := &streamConn{writer: bufio.NewWriter(&buf)}
	if err := conn.WriteRecord(frames); err != nil {
		t.Fatal(err)
	}
	_ = conn.Flush()
	if !bytes.Equal(buf.Bytes(), vectors["record.stream_and_max_data"]) {
		t.Fatalf("record encodes as %x, want %x", buf.Bytes(), vectors["record.stream_and_max_data"])
	}
	reader := &streamConn{reader: bufio.NewReader(bytes.NewReader(buf.Bytes())), maxRecordSize: DefaultMaxRecordSize}
	got, err := reader.ReadRecord()
	if err != nil || !bytes.Equal(got, frames) {
		t.Fatalf("record decodes as %x, %v", got, err)
	}
}

func TestGoldenInvalid(t *testing.T) {
	vectors := loadGoldenVectors(t)
	codes := map[string]TransportErrorCode{
		"frame_encoding":      FrameEncodingError,
		"transport_parameter": TransportParameterError,
	}
	tested := 0
	for name, input := range vectors {
		rest, ok := strings.CutPrefix(name, "invalid.")
		if !ok {
			continue
		}
		class, _, _ := strings.Cut(rest, ".")
		want, ok := codes[class]
		if !ok {
			t.Fatalf("%s has unknown error class %q", name, class)
		}
		tested++
		parser := frameParser{buf: input, version: &Draft02}
		var err error
		for parser.more() && err == nil {
			var f frame
			f, err = parser.next()
			if err == nil && f.typ == Draft02.TransportParametersType {
				_, err = decodeParameters(f.params, &Draft02)
			}
		}
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.ErrorCode != want {
			t.Errorf("%s decodes with %v, want %s", name, err, want)
		}
	}
	if tested == 0 {
		t.Fatal("no invalid vectors")
	}
}
