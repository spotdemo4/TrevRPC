package trevrpc

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"
)

// randomWireBytes returns random bytes, biased towards protobuf-looking data.
func randomWireBytes(rng *rand.Rand) []byte {
	out := make([]byte, rng.IntN(24))
	for i := range out {
		out[i] = byte(rng.IntN(256))
		if rng.IntN(3) == 0 {
			out[i] = byte(rng.IntN(16)<<3 | rng.IntN(6))
		}
	}
	return out
}

func randomMetadata(rng *rand.Rand) Metadata {
	if rng.IntN(3) == 0 {
		return nil
	}
	metadata := Metadata{}
	for range rng.IntN(4) {
		key := string(prefix(randomWireBytes(rng), rng.IntN(5)))
		if rng.IntN(2) == 0 {
			key = []string{"a", "x-header", "authorization", ""}[rng.IntN(4)]
		}
		metadata[key] = prefix(randomWireBytes(rng), rng.IntN(6))
	}
	return metadata
}

// TestWireCodecMatchesProtobuf checks that the direct codec decodes and
// encodes exactly as the protobuf runtime does, including on garbage.
func TestWireCodecMatchesProtobuf(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		payload := randomWireBytes(rng)
		if rng.IntN(2) == 0 {
			// A well-formed payload from the reflective encoder.
			request := &wireRequest{Method: string(prefix(randomWireBytes(rng), rng.IntN(6))), Metadata: randomMetadata(rng), Kind: RPCKind(rng.Int32N(6) - 1), TimeoutNanos: rng.Uint64() >> rng.UintN(64)}
			addRandomDictionaries(rng, request)
			payload, _ = proto.Marshal(protoMessageV2(request))
			if rng.IntN(3) == 0 && len(payload) > 0 {
				payload = payload[:rng.IntN(len(payload))]
			}
		}

		var want wireRequest
		wantErr := unmarshalMessage(payload, &want)
		var got wireRequest
		if err := decodeRequest(payload, &got); (err == nil) != (wantErr == nil) {
			t.Fatalf("request %x: err = %v, protobuf err = %v", payload, err, wantErr)
		} else if err == nil && !wireRequestEqual(&got, &want) {
			t.Fatalf("request %x: got %+v, want %+v", payload, got, want)
		}

		var wantStatus wireStatus
		wantStatusErr := unmarshalMessage(payload, &wantStatus)
		status, err := decodeStatus(payload)
		if len(payload) == 0 {
			continue
		}
		if (err == nil) != (wantStatusErr == nil) {
			t.Fatalf("status %x: err = %v, protobuf err = %v", payload, err, wantStatusErr)
		}
		if err == nil {
			wantDecoded := statusFromWire(&wantStatus)
			if status.Code != wantDecoded.Code || status.Message != wantDecoded.Message || !metadataEqual(status.Metadata, wantDecoded.Metadata) {
				t.Fatalf("status %x: got %+v, want %+v", payload, status, wantDecoded)
			}
		}

		var wantHeaders wireHeaders
		wantHeadersErr := unmarshalMessage(payload, &wantHeaders)
		headers, err := decodeHeaders(payload)
		if (err == nil) != (wantHeadersErr == nil) {
			t.Fatalf("headers %x: err = %v, protobuf err = %v", payload, err, wantHeadersErr)
		}
		if err == nil && !metadataEqual(headers, cloneMetadata(wantHeaders.Metadata)) {
			t.Fatalf("headers %x: got %v, want %v", payload, headers, wantHeaders.Metadata)
		}

		var wantAnswer wireAnswer
		wantAnswerErr := unmarshalMessage(payload, &wantAnswer)
		answer, err := decodeAnswer(payload)
		if (err == nil) != (wantAnswerErr == nil) {
			t.Fatalf("answer %x: err = %v, protobuf err = %v", payload, err, wantAnswerErr)
		}
		if err == nil && !answerEqual(answer, wantAnswer) {
			t.Fatalf("answer %x: got %+v, want %+v", payload, answer, wantAnswer)
		}
	}

	for range 20000 {
		request := &wireRequest{Method: string(prefix(randomWireBytes(rng), rng.IntN(6))), Metadata: randomMetadata(rng), Kind: RPCKind(rng.Int32N(6) - 1), TimeoutNanos: rng.Uint64() >> rng.UintN(64)}
		addRandomDictionaries(rng, request)
		checkEncoding(t, frameTypeRequest, func(dst []byte) ([]byte, error) { return appendRequestFrame(dst, request, DefaultMaxFrameSize) }, request, len(request.Metadata) > 1)

		headers := &wireHeaders{Metadata: randomMetadata(rng)}
		checkEncoding(t, frameTypeHeaders, func(dst []byte) ([]byte, error) {
			return appendHeadersFrame(dst, headers.Metadata, DefaultMaxFrameSize)
		}, headers, len(headers.Metadata) > 1)

		answer := &wireAnswer{StreamCompression: rng.IntN(2) == 0, ConnectionMetadata: rng.Uint32() >> rng.UintN(33)}
		if rng.IntN(3) == 0 {
			answer.Dictionaries = randomDictionaryInfos(rng)
		}
		checkEncoding(t, frameTypeAnswer, func(dst []byte) ([]byte, error) { return appendAnswerFrame(dst, answer, DefaultMaxFrameSize) }, answer, false)

		status := &Status{Code: Code(rng.IntN(20)), Message: string(prefix(randomWireBytes(rng), rng.IntN(5))), Metadata: randomMetadata(rng)}
		if status.Code == CodeOK && status.Message == "" && len(status.Metadata) == 0 {
			continue
		}
		checkEncoding(t, frameTypeStatus, func(dst []byte) ([]byte, error) { return appendStatusFrame(dst, status, DefaultMaxFrameSize) }, status.wire(), len(status.Metadata) > 1)
	}
}

// checkEncoding compares a frame built by the codec with the one the
// protobuf runtime builds. Maps with several entries encode in random order,
// so those are compared by decoding.
func checkEncoding(t *testing.T, typ frameType, appendFrame func([]byte) ([]byte, error), message ProtoMessage, unordered bool) {
	t.Helper()
	got, err := appendFrame([]byte{0xaa})
	protoMessage := protoMessageV2(message)
	want, wantErr := appendLimitedProtoFrame([]byte{0xaa}, typ, message, DefaultMaxFrameSize)
	if (err == nil) != (wantErr == nil) {
		t.Fatalf("%v %+v: err = %v, protobuf err = %v", typ, message, err, wantErr)
	}
	if err != nil {
		return
	}
	if len(got) != len(want) {
		t.Fatalf("%v %+v: frame length %d, want %d", typ, message, len(got), len(want))
	}
	if !unordered {
		if !bytes.Equal(got, want) {
			t.Fatalf("%v %+v: frame %x, want %x", typ, message, got, want)
		}
		return
	}
	decoded := protoMessage.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(got[len(got)-proto.Size(protoMessage):], decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded, protoMessage) {
		t.Fatalf("%v: decoded %v, want %v", typ, decoded, protoMessage)
	}
}

func wireRequestEqual(a, b *wireRequest) bool {
	return a.Method == b.Method && a.Kind == b.Kind && a.TimeoutNanos == b.TimeoutNanos &&
		a.DatagramFlow == b.DatagramFlow && metadataEqual(a.Metadata, b.Metadata) &&
		slices.Equal(a.AcceptDictionaries, b.AcceptDictionaries) && dictionaryInfosEqual(a.DictionaryQuery, b.DictionaryQuery) &&
		a.StreamCompression == b.StreamCompression && connectionMetadataEqual(a.ConnectionMetadata, b.ConnectionMetadata)
}

func dictionaryInfosEqual(a, b []*dictionaryInfo) bool {
	return slices.EqualFunc(a, b, func(x, y *dictionaryInfo) bool {
		return x.GetID() == y.GetID() && bytes.Equal(x.GetFingerprint(), y.GetFingerprint())
	})
}

// addRandomDictionaries sometimes gives request negotiation fields.
func addRandomDictionaries(rng *rand.Rand, request *wireRequest) {
	if rng.IntN(3) == 0 {
		request.AcceptDictionaries = []uint32{rng.Uint32() >> rng.UintN(32), 1}
	}
	if rng.IntN(4) == 0 {
		request.DictionaryQuery = randomDictionaryInfos(rng)
	}
	// Sometimes a value beyond the defined ones, which decodes all the same.
	request.StreamCompression = streamCompressionMode(rng.Int32N(4))
}

func randomDictionaryInfos(rng *rand.Rand) []*dictionaryInfo {
	infos := make([]*dictionaryInfo, 1+rng.IntN(3))
	for i := range infos {
		infos[i] = &dictionaryInfo{ID: rng.Uint32() >> rng.UintN(32), Fingerprint: prefix(randomWireBytes(rng), 8)}
	}
	return infos
}

func prefix(data []byte, n int) []byte { return data[:min(n, len(data))] }

// TestAppendFramesRejectInvalidUTF8 checks that a string field protobuf
// refuses is an error and not a panic, and leaves the buffer as it was.
func TestAppendFramesRejectInvalidUTF8(t *testing.T) {
	prefix := []byte{0xaa}
	if frame, err := appendStatusFrame(prefix, Internal("\xff"), DefaultMaxFrameSize); err == nil || !bytes.Equal(frame, prefix) {
		t.Fatalf("status frame = %x, err = %v", frame, err)
	}
	if frame, err := appendHeadersFrame(prefix, Metadata{"\xff": nil}, DefaultMaxFrameSize); err == nil || !bytes.Equal(frame, prefix) {
		t.Fatalf("headers frame = %x, err = %v", frame, err)
	}
	if frame, err := appendRequestFrame(prefix, newWireRequest("/svc/\xff"), DefaultMaxFrameSize); err == nil || !bytes.Equal(frame, prefix) {
		t.Fatalf("request frame = %x, err = %v", frame, err)
	}
}
