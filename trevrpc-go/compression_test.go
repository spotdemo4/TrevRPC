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
	"strings"
	"sync"
	"testing"

	"github.com/quic-go/quic-go/quicvarint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

// compressedType is the message type the test dictionary compresses: its
// samples are the JSON documents compressionSample writes.
const compressedType = "google.protobuf.StringValue"

func loadZstdVectors(t testing.TB) map[string][]byte {
	t.Helper()
	f, err := os.Open("../testdata/zstd-golden-vectors.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	vectors := map[string][]byte{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, _ := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if strings.HasSuffix(key, ".id") {
			continue
		}
		decoded, err := hex.DecodeString(value)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		vectors[key] = decoded
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func requireCompression(t testing.TB) {
	t.Helper()
	if !zstd.Available {
		t.Skip("compression needs cgo")
	}
}

func testCompression(t testing.TB) *Compression {
	t.Helper()
	return &Compression{Dictionaries: []Dictionary{{
		Data:     loadZstdVectors(t)["dict.d1.dictionary"],
		Messages: []string{compressedType},
	}}}
}

func testCompressionConfig(t testing.TB) *compressionConfig {
	t.Helper()
	config, err := newCompressionConfig(testCompression(t))
	if err != nil {
		t.Fatal(err)
	}
	return config
}

// compressionSample is a message like the ones the test dictionary was
// trained on.
func compressionSample(i int) *wrapperspb.StringValue {
	return wrapperspb.String(fmt.Sprintf(`{"service":"search","region":"us-east-1","status":404,"latency_ms":%d}`, 100+i))
}

func newStringValue() *wrapperspb.StringValue { return new(wrapperspb.StringValue) }

func registerCompressionService(server *Server) {
	RegisterUnary(server, testPath("CompressedEcho"), newStringValue, func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String(request.GetValue()), nil
	})
	RegisterBidirectionalStreaming(server, testPath("CompressedChat"), newStringValue, func(_ context.Context, requests MessageStream[*wrapperspb.StringValue]) (MessageStream[*wrapperspb.StringValue], error) {
		return requests, nil
	})
}

func TestCompressionNegotiationGoldenVectors(t *testing.T) {
	vectors := loadZstdVectors(t)
	fingerprint := vectors["dict.d1.fingerprint"]
	request := &wireRequest{
		Method:             "/svc/m",
		AcceptDictionaries: []uint32{1, 300},
		DictionaryQuery:    []*dictionaryInfo{{ID: 1, Fingerprint: fingerprint}},
	}
	frame, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["request.dictionaries.frame"]) {
		t.Fatalf("REQUEST frame = %x, %v; want %x", frame, err, vectors["request.dictionaries.frame"])
	}
	var fast, reflective wireRequest
	if !fast.decodeFast(vectors["request.dictionaries.payload"]) {
		// The query is left to the reflective decoder.
		if err := decodeRequest(vectors["request.dictionaries.payload"], &fast); err != nil {
			t.Fatal(err)
		}
	}
	if err := decodePayload(vectors["request.dictionaries.payload"], &reflective); err != nil {
		t.Fatal(err)
	}
	for _, decoded := range []*wireRequest{&fast, &reflective} {
		if decoded.Method != "/svc/m" || fmt.Sprint(decoded.AcceptDictionaries) != "[1 300]" ||
			len(decoded.DictionaryQuery) != 1 || decoded.DictionaryQuery[0].ID != 1 || !bytes.Equal(decoded.DictionaryQuery[0].Fingerprint, fingerprint) {
			t.Fatalf("decoded request %+v", decoded)
		}
	}

	frame, err = appendHeadersFrame(nil, nil, headersAnswer{dictionaries: []*dictionaryInfo{{ID: 1, Fingerprint: fingerprint}}}, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["headers.dictionaries.frame"]) {
		t.Fatalf("HEADERS frame = %x, %v; want %x", frame, err, vectors["headers.dictionaries.frame"])
	}
	_, decoded, err := decodeHeaders(vectors["headers.dictionaries.payload"])
	answer := decoded.dictionaries
	if err != nil || len(answer) != 1 || answer[0].ID != 1 || !bytes.Equal(answer[0].Fingerprint, fingerprint) {
		t.Fatalf("decoded answer %+v, %v", answer, err)
	}
}

func TestFastCodecDecodesAcceptedDictionaries(t *testing.T) {
	request := &wireRequest{Method: "/svc/m", AcceptDictionaries: []uint32{1, 70000}}
	frame, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	reader := newFrameReader(bytes.NewReader(frame), DefaultMaxFrameSize, -1)
	next, err := reader.next()
	if err != nil {
		t.Fatal(err)
	}
	var decoded wireRequest
	if !decoded.decodeFast(next.payload) || fmt.Sprint(decoded.AcceptDictionaries) != "[1 70000]" {
		t.Fatalf("fast decode = %+v", decoded)
	}
}

func compressedFrame(payload []byte) []byte {
	return append(appendFrameHeader(nil, frameTypeZstdMessage, len(payload)), payload...)
}

func TestFrameReaderDecompressesGoldenVectors(t *testing.T) {
	requireCompression(t)
	vectors := loadZstdVectors(t)
	config := testCompressionConfig(t)
	read := func(frame []byte, inflate dictionaryLookup) (frame, error) {
		reader := newFrameReader(bytes.NewReader(frame), DefaultMaxFrameSize, -1)
		reader.inflate = inflate
		return reader.next()
	}

	got, err := read(vectors["zstd_message.small.frame"], config)
	if err != nil || got.typ != frameTypeMessage || !bytes.Equal(got.payload, vectors["ok.small.message"]) {
		t.Fatalf("ZSTD_MESSAGE = %v %x, %v", got.typ, got.payload, err)
	}
	for key, payload := range vectors {
		name, isPayload := strings.CutSuffix(key, ".payload")
		if !isPayload {
			continue
		}
		switch {
		case strings.HasPrefix(name, "ok."):
			got, err := read(compressedFrame(payload), config)
			if err != nil || !bytes.Equal(got.payload, vectors[name+".message"]) {
				t.Errorf("%s: %x, %v", name, got.payload, err)
			}
		case strings.HasPrefix(name, "bad."):
			if _, err := read(compressedFrame(payload), config); !isFrameDecodeError(err) {
				t.Errorf("%s: got %v, want a decode error", name, err)
			}
		case strings.HasPrefix(name, "too_large."):
			if _, err := read(compressedFrame(payload), config); !isFrameTooLarge(err) {
				t.Errorf("%s: got %v, want FrameTooLargeError", name, err)
			}
		}
	}
	// A reader that accepts no dictionary rejects every compressed message.
	if _, err := read(vectors["zstd_message.small.frame"], nil); !isFrameDecodeError(err) {
		t.Fatalf("reader without dictionaries: %v", err)
	}
}

func isFrameDecodeError(err error) bool {
	_, ok := errors.AsType[*frameDecodeError](err)
	return ok
}

func isFrameTooLarge(err error) bool {
	_, ok := errors.AsType[*FrameTooLargeError](err)
	return ok
}

func TestCompressionConfigValidation(t *testing.T) {
	requireCompression(t)
	dict := loadZstdVectors(t)["dict.d1.dictionary"]
	// Without dictionaries a peer still compresses without one.
	for _, compression := range []*Compression{nil, {}} {
		config, err := newCompressionConfig(compression)
		if err != nil || config == nil || config.dictionaries || config.plain == nil || config.dict(plainDictID) != config.plain {
			t.Fatalf("Compression %v = %+v, %v", compression, config, err)
		}
	}
	for name, compression := range map[string]*Compression{
		"raw content":       {Dictionaries: []Dictionary{{Data: []byte("not a zstd dictionary")}}},
		"duplicate ID":      {Dictionaries: []Dictionary{{Data: dict}, {Data: dict}}},
		"invalid type name": {Dictionaries: []Dictionary{{Data: dict, Messages: []string{"not a name!"}}}},
		"type in two": {Dictionaries: []Dictionary{
			{Data: dict, Messages: []string{compressedType}},
			{Data: withDictID(dict, 2), Messages: []string{compressedType}},
		}},
	} {
		if _, err := newCompressionConfig(compression); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	server := NewServer()
	options := server.Options()
	options.Compression = &Compression{Dictionaries: []Dictionary{{Data: []byte("bad")}}}
	server.SetOptions(options)
	if _, err := Listen("127.0.0.1:0", server, ListenOptions{}); err == nil {
		t.Fatal("Listen accepted invalid compression")
	}
}

// withDictID returns a copy of a zstd-format dictionary with another ID.
func withDictID(dict []byte, id uint32) []byte {
	out := bytes.Clone(dict)
	out[4], out[5], out[6], out[7] = byte(id), byte(id>>8), byte(id>>16), byte(id>>24)
	return out
}

// rawFrame is one frame as it crossed the wire, before any decompression.
type rawFrame struct {
	typ     frameType
	payload []byte
}

func parseRawFrames(t testing.TB, data []byte) []rawFrame {
	t.Helper()
	var frames []rawFrame
	for len(data) > 0 {
		typ, n, err := quicvarint.Parse(data)
		if err != nil {
			t.Fatalf("frame type: %v", err)
		}
		data = data[n:]
		length, n, err := quicvarint.Parse(data)
		if err != nil || uint64(len(data)-n) < length {
			t.Fatalf("frame length: %v", err)
		}
		frames = append(frames, rawFrame{typ: frameType(typ), payload: data[n : n+int(length)]})
		data = data[n+int(length):]
	}
	return frames
}

func newCompressionTestServer(t testing.TB, compression *Compression) *Server {
	t.Helper()
	server := NewServer()
	options := server.Options()
	options.Compression = compression
	server.SetOptions(options)
	registerCompressionService(server)
	return server
}

func compressionTestRequest(t testing.TB, request *wireRequest, messages ...[]byte) []byte {
	t.Helper()
	input, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		input = append(input, message...)
	}
	return input
}

func protoMessageFrame(t testing.TB, message proto.Message) []byte {
	t.Helper()
	encoded, err := encodeMessage(message.(ProtoMessage))
	if err != nil {
		t.Fatal(err)
	}
	return encoded.frame
}

func TestServerAnswersQueryAndCompressesResponses(t *testing.T) {
	requireCompression(t)
	config := testCompressionConfig(t)
	d1 := config.byID[1]
	server := newCompressionTestServer(t, testCompression(t))
	sample := compressionSample(1)

	request := requestFor("CompressedEcho", RPCKindUnary)
	request.DictionaryQuery = []*dictionaryInfo{d1.info}
	request.AcceptDictionaries = []uint32{1}
	stream, _ := serveTestStreamRaw(t, server, compressionTestRequest(t, request, protoMessageFrame(t, sample)))
	frames := parseRawFrames(t, stream)
	if len(frames) != 3 || frames[0].typ != frameTypeHeaders || frames[1].typ != frameTypeZstdMessage || frames[2].typ != frameTypeStatus {
		t.Fatalf("frames = %v, want HEADERS ZSTD_MESSAGE STATUS", frames)
	}
	_, decoded, err := decodeHeaders(frames[0].payload)
	answer := decoded.dictionaries
	if err != nil || len(answer) != 1 || !d1.matches(answer[0]) {
		t.Fatalf("answer = %+v, %v", answer, err)
	}
	message, err := inflateMessage(config, frames[1].payload, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	var echoed wrapperspb.StringValue
	if err := proto.Unmarshal(message, &echoed); err != nil || echoed.GetValue() != sample.GetValue() {
		t.Fatalf("echoed %q, %v", echoed.GetValue(), err)
	}
}

func TestServerRefusesUnconfirmedDictionary(t *testing.T) {
	requireCompression(t)
	server := newCompressionTestServer(t, testCompression(t))
	request := requestFor("CompressedEcho", RPCKindUnary)
	// The fingerprint does not match the server's dictionary 1.
	request.DictionaryQuery = []*dictionaryInfo{{ID: 1, Fingerprint: []byte("12345678")}}
	request.AcceptDictionaries = []uint32{1}
	stream, _ := serveTestStreamRaw(t, server, compressionTestRequest(t, request, protoMessageFrame(t, compressionSample(1))))
	frames := parseRawFrames(t, stream)
	if len(frames) != 3 || frames[0].typ != frameTypeHeaders || frames[1].typ != frameTypeMessage {
		t.Fatalf("frames = %v, want HEADERS MESSAGE STATUS", frames)
	}
	if _, answer, err := decodeHeaders(frames[0].payload); err != nil || len(answer.dictionaries) != 0 {
		t.Fatalf("answer = %+v, %v; want none", answer, err)
	}
}

func TestServerWithoutCompressionAnswersQuery(t *testing.T) {
	requireCompression(t)
	server := newCompressionTestServer(t, nil)
	request := requestFor("CompressedEcho", RPCKindUnary)
	request.DictionaryQuery = testCompressionConfig(t).query
	request.AcceptDictionaries = []uint32{1}
	stream, _ := serveTestStreamRaw(t, server, compressionTestRequest(t, request, protoMessageFrame(t, compressionSample(1))))
	frames := parseRawFrames(t, stream)
	if len(frames) != 3 || frames[0].typ != frameTypeHeaders || frames[1].typ != frameTypeMessage {
		t.Fatalf("frames = %v, want an empty answer, then MESSAGE STATUS", frames)
	}
}

func TestServerDecompressesRequests(t *testing.T) {
	requireCompression(t)
	vectors := loadZstdVectors(t)
	config := testCompressionConfig(t)
	server := newCompressionTestServer(t, testCompression(t))
	sample := compressionSample(2)
	body, err := proto.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	compressed := compressPayload(body, config.byID[1].Dict)
	if compressed.frame[0] != byte(frameTypeZstdMessage) {
		t.Fatal("sample did not compress")
	}

	cases := []struct {
		name    string
		message []byte
		code    Code
	}{
		{"compressed", compressed.frame, CodeOK},
		{"unknown dictionary", compressedFrame(vectors["bad.unknown_dictionary.payload"]), CodeInvalidArgument},
		{"corrupt", compressedFrame(vectors["bad.content_size_mismatch.payload"]), CodeInvalidArgument},
		{"too large", compressedFrame(vectors["too_large.zeros.payload"]), CodeResourceExhausted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stream, _ := serveTestStreamRaw(t, server, compressionTestRequest(t, requestFor("CompressedEcho", RPCKindUnary), c.message))
			frames := parseRawFrames(t, stream)
			status, err := decodeStatus(frames[len(frames)-1].payload)
			if err != nil || status.Code != c.code {
				t.Fatalf("status = %v, %v; want %v", status, err, c.code)
			}
		})
	}

	// A server without compression rejects compressed requests.
	plain := newCompressionTestServer(t, nil)
	stream, _ := serveTestStreamRaw(t, plain, compressionTestRequest(t, requestFor("CompressedEcho", RPCKindUnary), compressed.frame))
	frames := parseRawFrames(t, stream)
	if status, err := decodeStatus(frames[len(frames)-1].payload); err != nil || status.Code != CodeInvalidArgument {
		t.Fatalf("status = %v, %v; want InvalidArgument", status, err)
	}
}

// serveTestStreamRaw is serveTestStream returning the raw response bytes.
func serveTestStreamRaw(t *testing.T, server *Server, input []byte) ([]byte, *countingRPCStream) {
	t.Helper()
	stream := &countingRPCStream{reader: bytes.NewReader(input)}
	handleRPCStream(context.Background(), server, stream, nil)
	return stream.written.Bytes(), stream
}

func TestCompressPayloadKeepsIncompressibleMessages(t *testing.T) {
	requireCompression(t)
	dict := testCompressionConfig(t).byID[1].Dict
	vectors := loadZstdVectors(t)
	for _, message := range [][]byte{nil, vectors["ok.raw_block.message"]} {
		encoded := compressPayload(message, dict)
		frames := parseRawFrames(t, encoded.frame)
		if len(frames) != 1 || frames[0].typ != frameTypeMessage || !bytes.Equal(frames[0].payload, message) || encoded.size != len(message) {
			t.Fatalf("incompressible message framed as %v", frames)
		}
		if cap(encoded.frame)-len(encoded.frame) < frameTailSlack {
			t.Fatal("frame has no tail slack")
		}
	}
	encoded := compressPayload(vectors["ok.small.message"], dict)
	if encoded.frame[0] != byte(frameTypeZstdMessage) || encoded.size != len(vectors["ok.small.message"]) || cap(encoded.frame)-len(encoded.frame) < frameTailSlack {
		t.Fatalf("compressible message framed as %x (size %d)", encoded.frame, encoded.size)
	}
}

// recordingOpener records the bytes of every stream it opens, in both
// directions.
type recordingOpener struct {
	inner streamOpener
	mu    *sync.Mutex
	log   *[]*recordedStream
}

type recordedStream struct {
	clientStream
	mu       sync.Mutex
	sent     bytes.Buffer
	received bytes.Buffer
}

func (o recordingOpener) openStream(ctx context.Context) (clientStream, error) {
	stream, err := o.inner.openStream(ctx)
	if err != nil {
		return nil, err
	}
	recorded := &recordedStream{clientStream: stream}
	o.mu.Lock()
	*o.log = append(*o.log, recorded)
	o.mu.Unlock()
	return recorded, nil
}

func (s *recordedStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.sent.Write(p)
	s.mu.Unlock()
	return s.clientStream.Write(p)
}

func (s *recordedStream) Read(p []byte) (int, error) {
	n, err := s.clientStream.Read(p)
	s.mu.Lock()
	s.received.Write(p[:n])
	s.mu.Unlock()
	return n, err
}

func (s *recordedStream) frames(t testing.TB) (sent, received []frameType) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range parseRawFrames(t, s.sent.Bytes()) {
		sent = append(sent, f.typ)
	}
	for _, f := range parseRawFrames(t, s.received.Bytes()) {
		received = append(received, f.typ)
	}
	return sent, received
}

// TestCompressionOverEveryTransport runs compressed calls over each transport
// the server serves, checking which frames crossed the wire: the first call
// queries, and once the answer is in both directions compress.
func TestCompressionOverEveryTransport(t *testing.T) {
	requireCompression(t)
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, func(options *ServerOptions) { options.Compression = testCompression(t) }, registerCompressionService)
	config := testCompressionConfig(t)

	clients := map[string]*streamClient{
		"http3":          newHTTP3TestClient(t, pki, listener).(*streamClient),
		"webtransport":   newWebTransportTestClient(t, pki, listener, testOrigin, nil).(*streamClient),
		"http2":          newHTTP2TestClient(t, pki, listener).(*streamClient),
		"qmux websocket": newWebSocketTestClient(t, pki, listener, testOrigin).(*streamClient),
	}
	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var log []*recordedStream
			client.opener = recordingOpener{inner: client.opener, mu: &mu, log: &log}
			client.compression = newClientCompression(config)
			exerciseCompressedCalls(t, client)

			mu.Lock()
			defer mu.Unlock()
			if len(log) != 2 {
				t.Fatalf("%d streams, want 2", len(log))
			}
			// The first call queries, accepts dictionary 1 unconfirmed, and
			// gets its answer and a compressed response.
			sent, received := log[0].frames(t)
			if fmt.Sprint(sent) != "[REQUEST MESSAGE]" || fmt.Sprint(received) != "[HEADERS ZSTD_MESSAGE STATUS]" {
				t.Fatalf("first call sent %v, received %v", sent, received)
			}
			// The second compresses its requests too, and queries no more.
			sent, received = log[1].frames(t)
			if fmt.Sprint(sent) != "[REQUEST ZSTD_MESSAGE ZSTD_MESSAGE ZSTD_MESSAGE]" || !strings.Contains(fmt.Sprint(received), "ZSTD_MESSAGE ZSTD_MESSAGE ZSTD_MESSAGE STATUS") || strings.Contains(fmt.Sprint(received), "HEADERS") {
				t.Fatalf("second call sent %v, received %v", sent, received)
			}
		})
	}

	// The native transports go through Dial and the QMux connector.
	native, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, Compression: testCompression(t), Transport: TransportQUICOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = native.Close() })
	qmuxChannel, err := dialChannel(t.Context(), &qmuxConnector{
		addr:         listener.TCPAddr().String(),
		tls:          clientQMuxTLSConfig(pki.client, "localhost"),
		maxFrameSize: DefaultMaxFrameSize,
		compression:  config,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qmuxChannel.Close() })
	for name, channel := range map[string]*Channel{"native": native, "qmux": qmuxChannel} {
		t.Run(name, func(t *testing.T) {
			exerciseCompressedCalls(t, channel)
			generation, err := channel.callGeneration(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if shared := generation.(*streamGeneration).client.compression.shared.Load(); !shared.has(generation.(*streamGeneration).client.compression.config.byID[1]) {
				t.Fatalf("shared set %+v lacks dictionary 1", shared)
			}
		})
	}
}

func exerciseCompressedCalls(t *testing.T, transport Transport) {
	t.Helper()
	ctx := t.Context()
	sample := compressionSample(0)
	response, err := Unary(ctx, transport, testPath("CompressedEcho"), sample, newStringValue)
	if err != nil || response.GetValue() != sample.GetValue() {
		t.Fatalf("unary = %q, %v", response.GetValue(), err)
	}
	chat, err := BidirectionalStreaming[*wrapperspb.StringValue](ctx, transport, testPath("CompressedChat"), newStringValue)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = chat.Close() }()
	for i := 1; i <= 3; i++ {
		if err := chat.Send(compressionSample(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		got, err := chat.Recv()
		if err != nil || got.GetValue() != compressionSample(i).GetValue() {
			t.Fatalf("message %d = %q, %v", i, got.GetValue(), err)
		}
	}
	if _, err := chat.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("end of stream: %v", err)
	}
}

func TestDatagramSinkDecompresses(t *testing.T) {
	requireCompression(t)
	vectors := loadZstdVectors(t)
	plain := appendFrameHeader(nil, frameTypeMessage, 2)
	plain = append(plain, "hi"...)

	sink := newDatagramSink(DefaultMaxFrameSize)
	sink.inflate = testCompressionConfig(t)
	sink.pushFrames(append(bytes.Clone(plain), vectors["zstd_message.small.frame"]...))
	for _, want := range [][]byte{[]byte("hi"), vectors["ok.small.message"]} {
		if got, ok := sink.pop(); !ok || !bytes.Equal(got, want) {
			t.Fatalf("popped %x, %v; want %x", got, ok, want)
		}
	}

	// A rejected compressed message drops the whole datagram, plain
	// messages included.
	for name, frames := range map[string][]byte{
		"truncated":  append(bytes.Clone(plain), compressedFrame(vectors["bad.truncated.payload"])...),
		"too large":  append(bytes.Clone(plain), compressedFrame(vectors["too_large.zeros.payload"])...),
		"unaccepted": append(bytes.Clone(plain), compressedFrame(vectors["bad.unknown_dictionary.payload"])...),
	} {
		sink.pushFrames(frames)
		if got, ok := sink.pop(); ok {
			t.Fatalf("%s: popped %x", name, got)
		}
	}
	none := newDatagramSink(DefaultMaxFrameSize)
	none.pushFrames(vectors["zstd_message.small.frame"])
	if got, ok := none.pop(); ok {
		t.Fatalf("sink without dictionaries popped %x", got)
	}
}
