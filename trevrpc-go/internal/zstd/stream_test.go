//go:build cgo

package zstd

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// splitStreamPayload splits a ZSTD_STREAM_MESSAGE payload into its message
// size, a QUIC varint, and its chunk.
func splitStreamPayload(t testing.TB, payload []byte) (int, []byte) {
	t.Helper()
	if len(payload) == 0 {
		t.Fatal("empty stream payload")
	}
	n := 1 << (payload[0] >> 6)
	size := int(payload[0] & 0x3f)
	for _, b := range payload[1:n] {
		size = size<<8 | int(b)
	}
	return size, payload[n:]
}

// appendStreamPayload appends a ZSTD_STREAM_MESSAGE payload: the size of
// message as a minimal QUIC varint, then the chunk it compresses to.
func appendStreamPayload(t testing.TB, encoder *StreamEncoder, message []byte) []byte {
	t.Helper()
	var payload []byte
	switch size := len(message); {
	case size < 1<<6:
		payload = []byte{byte(size)}
	case size < 1<<14:
		payload = []byte{0x40 | byte(size>>8), byte(size)}
	default:
		payload = []byte{0x80 | byte(size>>24), byte(size >> 16), byte(size >> 8), byte(size)}
	}
	payload, err := encoder.Encode(payload, message)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// streamSequence returns the payloads and messages of the stream.<name>
// vectors, in order.
func streamSequence(vectors map[string][]byte, name string) (payloads, messages [][]byte) {
	for i := 0; ; i++ {
		payload, ok := vectors[fmt.Sprintf("stream.%s.%d.payload", name, i)]
		if !ok {
			return payloads, messages
		}
		payloads = append(payloads, payload)
		messages = append(messages, vectors[fmt.Sprintf("stream.%s.%d.message", name, i)])
	}
}

// decodeSequence decodes payloads in order with a fresh decoder that holds d1,
// the way a receiver does, and returns the messages or the first failure.
func decodeSequence(t testing.TB, d1 *Dict, payloads [][]byte) ([][]byte, error) {
	t.Helper()
	decoder, err := NewStreamDecoder(StreamWindowLog)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	inFrame := false
	var messages [][]byte
	for _, payload := range payloads {
		size, chunk := splitStreamPayload(t, payload)
		if size > maxMessage {
			return messages, errTooLarge
		}
		info, err := ParseStreamChunk(chunk, !inFrame, StreamWindowLog)
		if err != nil {
			return messages, err
		}
		var dict *Dict
		if info.DictID != 0 {
			if info.DictID != d1.ID {
				return messages, ErrInvalid
			}
			dict = d1
		}
		message, err := decoder.Decode(nil, chunk, info, dict, size)
		if err != nil {
			return messages, err
		}
		if len(chunk) > 0 {
			inFrame = !info.EndsFrame
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func TestStreamVectorsDecode(t *testing.T) {
	v := loadVectors(t)
	d1 := newD1(t, v)
	for _, name := range []string{"plain", "seeded", "reset", "empty"} {
		payloads, want := streamSequence(v, name)
		if len(payloads) < 2 {
			t.Fatalf("stream.%s has %d payloads", name, len(payloads))
		}
		got, err := decodeSequence(t, d1, payloads)
		if err != nil {
			t.Fatalf("stream.%s: %v", name, err)
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("stream.%s.%d = %x, want %x", name, i, got[i], want[i])
			}
		}
	}
}

func TestStreamBadVectorsAreRejected(t *testing.T) {
	v := loadVectors(t)
	d1 := newD1(t, v)
	bad := names(v, "stream_bad.", ".payload")
	if len(bad) < 9 {
		t.Fatalf("only %d stream_bad vectors", len(bad))
	}
	for _, name := range bad {
		if _, err := decodeSequence(t, d1, [][]byte{v[name+".payload"]}); err == nil || errors.Is(err, errTooLarge) {
			t.Errorf("%s: err = %v, want invalid", name, err)
		}
	}
	for _, name := range names(v, "stream_too_large.", ".payload") {
		if _, err := decodeSequence(t, d1, [][]byte{v[name+".payload"]}); !errors.Is(err, errTooLarge) {
			t.Errorf("%s: err = %v, want too large", name, err)
		}
	}
}

// TestStreamEncoderWritesTheVectors checks the encoder's parameters: the
// vectors came from libzstd 1.5.7 at level 1 with a 64 KiB window, which this
// encoder runs too, so it reproduces them byte for byte.
func TestStreamEncoderWritesTheVectors(t *testing.T) {
	v := loadVectors(t)
	d1 := newD1(t, v)
	for _, test := range []struct {
		name string
		dict *Dict
	}{{"plain", nil}, {"seeded", d1}, {"empty", nil}} {
		encoder, err := NewStreamEncoder(test.dict, 1, StreamWindowLog)
		if err != nil {
			t.Fatal(err)
		}
		payloads, messages := streamSequence(v, test.name)
		for i, message := range messages {
			if got := appendStreamPayload(t, encoder, message); !bytes.Equal(got, payloads[i]) {
				t.Fatalf("stream.%s.%d payload = %x, want %x", test.name, i, got, payloads[i])
			}
		}
		encoder.Close()
	}
}

func TestStreamRoundTripAcrossBlocks(t *testing.T) {
	encoder, err := NewStreamEncoder(nil, 3, StreamWindowLog)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	var payloads, messages [][]byte
	for i := range 40 {
		// Messages from empty to a few blocks long, with repeats to match.
		message := bytes.Repeat(fmt.Appendf(nil, "message %d of the stream;", i%7), i*i*20)
		messages = append(messages, message)
		payloads = append(payloads, appendStreamPayload(t, encoder, message))
	}
	got, err := decodeSequence(t, nil, payloads)
	if err != nil {
		t.Fatal(err)
	}
	for i := range messages {
		if !bytes.Equal(got[i], messages[i]) {
			t.Fatalf("message %d does not round-trip", i)
		}
	}
}

func TestStreamEncoderEndsFramesBeforeTheWindowWraps(t *testing.T) {
	v := loadVectors(t)
	d1 := newD1(t, v)
	for _, dict := range []*Dict{nil, d1} {
		encoder, err := NewStreamEncoder(dict, 1, StreamWindowLog)
		if err != nil {
			t.Fatal(err)
		}
		var payloads, messages [][]byte
		inFrame, frames := false, 0
		total := 0
		for i := range 200 {
			message := bytes.Repeat(fmt.Appendf(nil, "record %d with some text;", i), 50)[:1000]
			payload := appendStreamPayload(t, encoder, message)
			messages, payloads = append(messages, message), append(payloads, payload)
			_, chunk := splitStreamPayload(t, payload)
			info, err := ParseStreamChunk(chunk, !inFrame, StreamWindowLog)
			if err != nil {
				t.Fatal(err)
			}
			if info.StartsFrame {
				frames++
				if dict != nil && info.DictID != dict.ID {
					t.Fatalf("frame %d starts from dictionary %d, want %d", frames, info.DictID, dict.ID)
				}
			}
			total += len(message)
			if wantEnd := total >= frameLimit; info.EndsFrame != wantEnd {
				t.Fatalf("message %d ends frame = %v, want %v", i, info.EndsFrame, wantEnd)
			}
			if info.EndsFrame {
				total = 0
			}
			inFrame = !info.EndsFrame
		}
		encoder.Close()
		if want := 200*1000/frameLimit + 1; frames != want {
			t.Fatalf("%d frames, want %d", frames, want)
		}
		got, err := decodeSequence(t, d1, payloads)
		if err != nil {
			t.Fatal(err)
		}
		for i := range messages {
			if !bytes.Equal(got[i], messages[i]) {
				t.Fatalf("message %d does not round-trip", i)
			}
		}
	}
}

func TestStreamContextsFailAfterClose(t *testing.T) {
	encoder, err := NewStreamEncoder(nil, 1, StreamWindowLog)
	if err != nil {
		t.Fatal(err)
	}
	encoder.Close()
	encoder.Close()
	if _, err := encoder.Encode(nil, []byte("x")); err == nil {
		t.Fatal("Encode after Close succeeded")
	}
	decoder, err := NewStreamDecoder(StreamWindowLog)
	if err != nil {
		t.Fatal(err)
	}
	decoder.Close()
	if _, err := decoder.Decode(nil, []byte{0}, StreamChunk{}, nil, 1); err == nil {
		t.Fatal("Decode after Close succeeded")
	}
}

func TestStreamCostsAreLibzstdEstimates(t *testing.T) {
	// About 585 KiB and 350 KiB with a 64 KiB window at level 1.
	if cost := StreamEncoderCost(1, StreamWindowLog); cost < 400<<10 || cost > 800<<10 {
		t.Fatalf("encoder cost %d", cost)
	}
	if cost := StreamDecoderCost(StreamWindowLog); cost < 250<<10 || cost > 500<<10 {
		t.Fatalf("decoder cost %d", cost)
	}
}

func newD1(t testing.TB, v map[string][]byte) *Dict {
	t.Helper()
	dict, err := NewDict(v["dict.d1.dictionary"], 1)
	if err != nil {
		t.Fatal(err)
	}
	return dict
}
