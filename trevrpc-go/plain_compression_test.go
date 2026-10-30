package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

// plainOnlyConfig is the compression of a peer without dictionaries, which
// still compresses without one.
func plainOnlyConfig(t testing.TB) *compressionConfig {
	t.Helper()
	config, err := newCompressionConfig(nil)
	if err != nil || config == nil || config.plain == nil {
		t.Fatalf("plain-only config = %+v, %v", config, err)
	}
	return config
}

// plainSample is a message that compresses on its own: the plain.records
// golden vector's JSON record, twice over.
func plainSample(t testing.TB) *wrapperspb.StringValue {
	t.Helper()
	return wrapperspb.String(strings.Repeat(string(loadZstdVectors(t)["plain.records.message"]), 2))
}

func TestPlainCompressionNegotiationGoldenVectors(t *testing.T) {
	vectors := loadZstdVectors(t)
	client := newClientCompression(plainOnlyConfig(t))
	request := &wireRequest{Method: "/svc/m", plainCompression: PlainCompression{Requests: true, Responses: true}}
	accept, queried := client.prepare(request)
	if !queried || accept.lookup(plainDictID) == nil {
		t.Fatalf("prepare = %+v, %v; want a query and plain responses accepted", accept, queried)
	}
	frame, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["request.plain.frame"]) {
		t.Fatalf("REQUEST frame = %x, %v; want %x", frame, err, vectors["request.plain.frame"])
	}
	var fast, reflective wireRequest
	if !fast.decodeFast(vectors["request.plain.payload"]) {
		if err := decodeRequest(vectors["request.plain.payload"], &fast); err != nil {
			t.Fatal(err)
		}
	}
	if err := decodePayload(vectors["request.plain.payload"], &reflective); err != nil {
		t.Fatal(err)
	}
	for _, decoded := range []*wireRequest{&fast, &reflective} {
		if fmt.Sprint(decoded.AcceptDictionaries) != "[0]" || len(decoded.DictionaryQuery) != 1 || decoded.DictionaryQuery[0].GetID() != 0 || len(decoded.DictionaryQuery[0].GetFingerprint()) != 0 {
			t.Fatalf("decoded request %+v", decoded)
		}
	}

	// The server confirms ID 0 and may compress the responses so.
	server := plainOnlyConfig(t)
	result := server.negotiate(&fast, true)
	if result == nil || !result.answered || len(result.answer) != 1 || result.answer[0].ID != plainDictID || result.responses.compressorFor(plainSample(t)) != server.plain {
		t.Fatalf("negotiate = %+v", result)
	}
	frame, err = appendAnswerFrame(nil, &wireAnswer{Dictionaries: result.answer}, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["answer.plain.frame"]) {
		t.Fatalf("ANSWER frame = %x, %v; want %x", frame, err, vectors["answer.plain.frame"])
	}
	decoded, err := decodeAnswer(vectors["answer.plain.payload"])
	if err != nil {
		t.Fatal(err)
	}
	client.learn(decoded.Dictionaries)
	if !client.shared.Load().has(client.config.plain) {
		t.Fatalf("shared set %+v lacks compression without a dictionary", client.shared.Load())
	}
	// Once answered, the connection's calls query no more.
	if _, queried := client.prepare(&wireRequest{Method: "/svc/m", plainCompression: PlainCompression{Requests: true}}); queried {
		t.Fatal("an answered connection queried again")
	}
}

func TestPlainCompressionNegotiatesOnlyWhereAllowed(t *testing.T) {
	client := newClientCompression(plainOnlyConfig(t))
	// A call that compresses neither direction carries no negotiation, so
	// peers without dictionaries talk as before.
	request := &wireRequest{Method: "/svc/m"}
	if accept, queried := client.prepare(request); accept != nil || queried || request.AcceptDictionaries != nil || request.DictionaryQuery != nil {
		t.Fatalf("prepare = %v, %v, request %+v", accept, queried, request)
	}
	// Responses alone accept ID 0 without querying.
	request = &wireRequest{Method: "/svc/m", plainCompression: PlainCompression{Responses: true}}
	if accept, queried := client.prepare(request); queried || fmt.Sprint(request.AcceptDictionaries) != "[0]" || accept.lookup(plainDictID) == nil {
		t.Fatalf("prepare = %v, %v, request %+v", accept, queried, request)
	}
	// A route that does not allow it keeps its responses uncompressed.
	server := plainOnlyConfig(t)
	if result := server.negotiate(&wireRequest{AcceptDictionaries: []uint32{plainDictID}}, false); result.responses.compressorFor(plainSample(t)) != nil {
		t.Fatal("a route without compression without a dictionary compressed its responses")
	}

	// With a dictionary, both are offered, and its type keeps it.
	config := testCompressionConfig(t)
	withDict := newClientCompression(config)
	request = &wireRequest{Method: "/svc/m", responseType: compressedType, plainCompression: PlainCompression{Responses: true}}
	accept, queried := withDict.prepare(request)
	if !queried || fmt.Sprint(request.AcceptDictionaries) != "[1 0]" || accept.lookup(1) == nil || accept.lookup(plainDictID) == nil {
		t.Fatalf("prepare = %v, %v, request %+v", accept, queried, request)
	}
	if len(request.DictionaryQuery) != 2 || request.DictionaryQuery[0].ID != plainDictID {
		t.Fatalf("query = %+v, want ID 0 and dictionary 1", request.DictionaryQuery)
	}
}

func TestPlainCompressionEncodes(t *testing.T) {
	config := testCompressionConfig(t)
	all := func(*compressionDict) bool { return true }
	plainOnly := func(d *compressionDict) bool { return d == config.plain }
	sample := plainSample(t)
	// A string field costs a tag and a 2-byte length on top of its text.
	small := wrapperspb.String(strings.Repeat("a", plainCompressionMinSize-4))
	big := wrapperspb.String(strings.Repeat("a", plainCompressionMinSize-3))
	incompressible := wrapperspb.Bytes(loadZstdVectors(t)["ok.raw_block.message"])
	random := make([]byte, 600)
	var x uint32 = 1
	for i := range random {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		random[i] = byte(x)
	}
	noise := wrapperspb.Bytes(random)

	cases := []struct {
		name       string
		compressor *messageCompressor
		message    ProtoMessage
		// dictID is the Dictionary_ID of the ZSTD_MESSAGE, or -1 for a
		// MESSAGE.
		dictID int
	}{
		{"the type's dictionary wins", &messageCompressor{config: config, allowed: all, plain: true}, compressionSample(1), 1},
		{"without a dictionary for the type", &messageCompressor{config: config, allowed: all, plain: true}, wrapperspb.Bytes([]byte(sample.GetValue())), 0},
		{"the type's dictionary not shared", &messageCompressor{config: config, allowed: plainOnly, plain: true}, sample, 0},
		{"not allowed on the method", &messageCompressor{config: config, allowed: all}, wrapperspb.Bytes([]byte(sample.GetValue())), -1},
		{"ID 0 not negotiated", &messageCompressor{config: config, allowed: func(d *compressionDict) bool { return d != config.plain }, plain: true}, wrapperspb.Bytes([]byte(sample.GetValue())), -1},
		{"under the size floor", &messageCompressor{config: config, allowed: plainOnly, plain: true}, small, -1},
		{"at the size floor", &messageCompressor{config: config, allowed: plainOnly, plain: true}, big, 0},
		{"incompressible", &messageCompressor{config: config, allowed: plainOnly, plain: true}, noise, -1},
		{"short and incompressible", &messageCompressor{config: config, allowed: plainOnly, plain: true}, incompressible, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			encoded, err := c.compressor.encode(c.message)
			if err != nil {
				t.Fatal(err)
			}
			frames := parseRawFrames(t, encoded.frame)
			if len(frames) != 1 {
				t.Fatalf("frames = %v", frames)
			}
			got := -1
			if frames[0].typ == frameTypeZstdMessage {
				header, err := zstd.ParseHeader(frames[0].payload)
				if err != nil {
					t.Fatal(err)
				}
				got = int(header.DictID)
				message, err := inflateMessage(config, frames[0].payload, DefaultMaxFrameSize)
				if err != nil || len(message) != encoded.size {
					t.Fatalf("inflate = %d bytes, %v; want %d", len(message), err, encoded.size)
				}
			}
			if got != c.dictID {
				t.Fatalf("Dictionary_ID = %d, want %d (frames %v)", got, c.dictID, frames)
			}
		})
	}
}

func TestPlainCompressionGivesWayToStreamCompression(t *testing.T) {
	config := plainOnlyConfig(t)
	per := &messageCompressor{config: config, allowed: config.plain.only.has, plain: true}
	compressor := newStreamCompressor(newStreamBudget(0, 1<<20, config), per)
	compressor.enable()
	defer compressor.close()
	var types []frameType
	for range 4 {
		encoded, err := compressor.encode(plainSample(t))
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, parseRawFrames(t, encoded.frame)[0].typ)
	}
	// The first two messages go per message, without a dictionary; the
	// stream context, which does not start from one, takes over from the
	// third.
	if fmt.Sprint(types) != "[ZSTD_MESSAGE ZSTD_MESSAGE ZSTD_STREAM_MESSAGE ZSTD_STREAM_MESSAGE]" {
		t.Fatalf("frames = %v", types)
	}
}

func TestFrameReaderDecompressesPlainGoldenVectors(t *testing.T) {
	vectors := loadZstdVectors(t)
	config := testCompressionConfig(t)
	read := func(payload []byte, inflate dictionaryLookup) ([]byte, error) {
		reader := newFrameReader(bytes.NewReader(compressedFrame(payload)), DefaultMaxFrameSize, -1)
		reader.inflate = inflate
		f, err := reader.next()
		return f.payload, err
	}
	for _, accept := range []dictionaryLookup{config, config.plain.only} {
		if got, err := read(vectors["plain.records.payload"], accept); err != nil || !bytes.Equal(got, vectors["plain.records.message"]) {
			t.Fatalf("plain.records accepting ID 0 = %x, %v", got, err)
		}
		if _, err := read(vectors["bad.no_dictionary_id.payload"], accept); !errors.As(err, new(*frameDecodeError)) {
			t.Fatalf("bad.no_dictionary_id accepting ID 0: %v, want a decode error", err)
		}
	}
	if _, err := read(vectors["plain.records.payload"], config.byID[1].only); !errors.As(err, new(*frameDecodeError)) {
		t.Fatalf("plain.records accepting only dict.d1: %v, want a decode error", err)
	}
}

// TestPlainCompressionEndToEnd makes two calls to a route that allows
// compression without a dictionary: the first's response travels compressed
// and its answer lets the second compress its request too.
func TestPlainCompressionEndToEnd(t *testing.T) {
	pki := newTestPKI(t)
	var plainRequests atomic.Int64
	register := func(server *Server) {
		RegisterUnary(server, testPath("PlainEcho"), newStringValue, func(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if info, ok := HandlerContextFromContext(ctx); ok {
				plainRequests.Store(info.PlainCompressedRequests())
			}
			return wrapperspb.String(request.GetValue()), nil
		}, RoutePlainCompression(PlainCompression{Requests: true, Responses: true}))
		RegisterUnary(server, testPath("Echo"), newStringValue, func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return wrapperspb.String(request.GetValue()), nil
		})
	}
	listener := startTestListenerWith(t, pki, nil, register)
	client := newHTTP3TestClient(t, pki, listener).(*streamClient)
	var mu sync.Mutex
	var log []*recordedStream
	client.opener = recordingOpener{inner: client.opener, mu: &mu, log: &log}
	client.compression = newClientCompression(plainOnlyConfig(t))
	sample := plainSample(t)
	call := func(method string, options ...CallOption) {
		t.Helper()
		response, err := Unary(t.Context(), client, testPath(method), sample, newStringValue, options...)
		if err != nil || response.GetValue() != sample.GetValue() {
			t.Fatalf("%s = %.32q, %v", method, response.GetValue(), err)
		}
	}
	allowed := WithPlainCompression(PlainCompression{Requests: true, Responses: true})
	call("PlainEcho", allowed)
	if plainRequests.Load() != 0 {
		t.Fatal("the first call's request arrived compressed before the answer")
	}
	call("PlainEcho", allowed)
	if plainRequests.Load() != 1 {
		t.Fatalf("PlainCompressedRequests = %d, want 1", plainRequests.Load())
	}
	// A method without the option compresses nothing.
	call("Echo")

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"[REQUEST MESSAGE] [ANSWER ZSTD_MESSAGE STATUS]",
		"[REQUEST ZSTD_MESSAGE] [ZSTD_MESSAGE STATUS]",
		"[REQUEST MESSAGE] [MESSAGE STATUS]",
	}
	if len(log) != len(want) {
		t.Fatalf("%d streams, want %d", len(log), len(want))
	}
	for i, stream := range log {
		sent, received := stream.frames(t)
		if got := fmt.Sprint(sent) + " " + fmt.Sprint(received); got != want[i] {
			t.Fatalf("call %d: %s, want %s", i, got, want[i])
		}
	}
}
