package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

// streamTestMessages is how many messages the stream compression tests send:
// the first two go per message, the rest stream-compressed.
const streamTestMessages = 6

// streamCompressedRequests records what the last call of the StreamChat route
// saw: how many of its requests arrived stream-compressed.
var streamCompressedRequests atomic.Int64

func registerStreamCompressionService(server *Server) {
	both := RouteStreamCompression(StreamCompression{Requests: true, Responses: true})
	RegisterBidirectionalStreaming(server, testPath("StreamChat"), newStringValue, func(ctx context.Context, requests MessageStream[*wrapperspb.StringValue]) (MessageStream[*wrapperspb.StringValue], error) {
		return &countingRequests{ctx: ctx, inner: requests}, nil
	}, both)
	RegisterServerStreaming(server, testPath("StreamFeed"), newStringValue, func(context.Context, *wrapperspb.StringValue) (MessageStream[*wrapperspb.StringValue], error) {
		responses := make([]*wrapperspb.StringValue, streamTestMessages)
		for i := range responses {
			responses[i] = compressionSample(i)
		}
		return newSliceStream(responses...), nil
	}, both)
}

// countingRequests echoes the requests of a call and records, at their end,
// how many arrived stream-compressed.
type countingRequests struct {
	ctx   context.Context
	inner MessageStream[*wrapperspb.StringValue]
}

func (s *countingRequests) Recv() (*wrapperspb.StringValue, error) {
	message, err := s.inner.Recv()
	if errors.Is(err, io.EOF) {
		info, _ := HandlerContextFromContext(s.ctx)
		streamCompressedRequests.Store(info.StreamCompressedRequests())
	}
	return message, err
}

func (s *countingRequests) Close() error { return s.inner.Close() }

func TestStreamCompressionNegotiationGoldenVectors(t *testing.T) {
	vectors := loadZstdVectors(t)
	request := &wireRequest{Method: "/svc/m", StreamCompression: streamOffer()}
	frame, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["request.stream_compression.frame"]) {
		t.Fatalf("REQUEST frame = %x, %v; want %x", frame, err, vectors["request.stream_compression.frame"])
	}
	var decoded wireRequest
	if err := decodeRequest(vectors["request.stream_compression.payload"], &decoded); err != nil || decoded.StreamCompression.GetMaxWindowLog() != zstd.StreamWindowLog {
		t.Fatalf("decoded request %+v, %v", decoded, err)
	}
	frame, err = appendHeadersFrame(nil, nil, headersAnswer{stream: streamOffer()}, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["headers.stream_compression.frame"]) {
		t.Fatalf("HEADERS frame = %x, %v; want %x", frame, err, vectors["headers.stream_compression.frame"])
	}
	if _, answer, err := decodeHeaders(vectors["headers.stream_compression.payload"]); err != nil || answer.stream.GetMaxWindowLog() != zstd.StreamWindowLog {
		t.Fatalf("decoded answer %+v, %v", answer, err)
	}
}

// GetMaxWindowLog returns the window, or zero for a nil info.
func (m *streamCompressionInfo) GetMaxWindowLog() uint32 {
	if m == nil {
		return 0
	}
	return m.MaxWindowLog
}

func TestSenderWindowLog(t *testing.T) {
	for _, test := range []struct {
		info *streamCompressionInfo
		want int
	}{
		{nil, 0},
		{&streamCompressionInfo{}, 0},
		{&streamCompressionInfo{MaxWindowLog: 9}, 0},
		{&streamCompressionInfo{MaxWindowLog: 10}, 10},
		{&streamCompressionInfo{MaxWindowLog: 16}, 16},
		{&streamCompressionInfo{MaxWindowLog: 31}, 16},
	} {
		if got := senderWindowLog(test.info); got != test.want {
			t.Errorf("senderWindowLog(%+v) = %d, want %d", test.info, got, test.want)
		}
	}
}

// streamFrame frames a ZSTD_STREAM_MESSAGE payload.
func streamFrame(payload []byte) []byte {
	return append(appendFrameHeader(nil, frameTypeZstdStreamMessage, len(payload)), payload...)
}

func TestFrameReaderDecompressesStreamVectors(t *testing.T) {
	requireCompression(t)
	vectors := loadZstdVectors(t)
	config := testCompressionConfig(t)
	read := func(payloads ...[]byte) ([][]byte, error) {
		var wire []byte
		for _, payload := range payloads {
			wire = append(wire, streamFrame(payload)...)
		}
		reader := newFrameReader(bytes.NewReader(wire), DefaultMaxFrameSize, -1)
		reader.stream = newStreamInflater(config, nil)
		defer reader.stream.close()
		var messages [][]byte
		for {
			next, err := reader.next()
			if errors.Is(err, io.EOF) {
				return messages, nil
			}
			if err != nil {
				return messages, err
			}
			if next.typ != frameTypeMessage {
				t.Fatalf("reader returned %s", next.typ)
			}
			messages = append(messages, next.payload)
		}
	}
	for _, name := range []string{"plain", "seeded", "reset", "empty"} {
		var payloads, want [][]byte
		for i := 0; ; i++ {
			payload, ok := vectors[fmt.Sprintf("stream.%s.%d.payload", name, i)]
			if !ok {
				break
			}
			payloads = append(payloads, payload)
			want = append(want, vectors[fmt.Sprintf("stream.%s.%d.message", name, i)])
		}
		got, err := read(payloads...)
		if err != nil || len(got) != len(want) {
			t.Fatalf("stream.%s: %d messages, %v", name, len(got), err)
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("stream.%s.%d = %x, want %x", name, i, got[i], want[i])
			}
		}
	}
	for name, payload := range vectors {
		if !strings.HasPrefix(name, "stream_bad.") {
			continue
		}
		if _, err := read(payload); !isFrameDecodeError(err) {
			t.Errorf("%s: err = %v, want a decode error", name, err)
		}
	}
	if _, err := read(vectors["stream_too_large.zeros.payload"]); !isFrameTooLarge(err) {
		t.Fatalf("too large: err = %v", err)
	}
	// A failed chunk ends the direction, valid ones after it included.
	if got, err := read(vectors["stream_bad.truncated.payload"], vectors["stream.plain.0.payload"]); len(got) != 0 || !isFrameDecodeError(err) {
		t.Fatalf("after a failure: %d messages, %v", len(got), err)
	}
}

func TestFrameReaderRejectsStreamMessagesUnlessEnabled(t *testing.T) {
	vectors := loadZstdVectors(t)
	reader := newFrameReader(bytes.NewReader(streamFrame(vectors["stream.plain.0.payload"])), DefaultMaxFrameSize, -1)
	if _, err := reader.next(); !isFrameDecodeError(err) {
		t.Fatalf("err = %v, want a decode error", err)
	}
}

func TestStreamInflaterChargesMessageSizes(t *testing.T) {
	requireCompression(t)
	vectors := loadZstdVectors(t)
	payload := vectors["stream.plain.0.payload"]
	size := len(vectors["stream.plain.0.message"])
	wire := streamFrame(payload)
	reader := newFrameReader(bytes.NewReader(wire), DefaultMaxFrameSize, size-1)
	reader.chargeMessages = true
	reader.stream = newStreamInflater(nil, nil)
	defer reader.stream.close()
	if _, err := reader.next(); err == nil || StatusFromError(err).Code != CodeResourceExhausted {
		t.Fatalf("err = %v, want the body limit", err)
	}
	// A message above the max frame size is refused before decompressing.
	reader = newFrameReader(bytes.NewReader(wire), size-1, -1)
	reader.stream = newStreamInflater(nil, nil)
	defer reader.stream.close()
	if _, err := reader.next(); !isFrameTooLarge(err) {
		t.Fatalf("err = %v, want too large", err)
	}
}

func TestDatagramsDropStreamMessages(t *testing.T) {
	vectors := loadZstdVectors(t)
	plain := append(appendFrameHeader(nil, frameTypeMessage, 2), "hi"...)
	if !walkDatagramFrames(plain, nil) {
		t.Fatal("plain datagram dropped")
	}
	if walkDatagramFrames(append(plain, vectors["zstd_stream_message.first.frame"]...), nil) {
		t.Fatal("datagram with a ZSTD_STREAM_MESSAGE accepted")
	}
}

// streamClientFor gives client a stream compression budget, records its
// streams, and returns the log.
func streamClientFor(client *streamClient, budget *streamBudget) (*sync.Mutex, *[]*recordedStream) {
	var mu sync.Mutex
	var log []*recordedStream
	client.opener = recordingOpener{inner: client.opener, mu: &mu, log: &log}
	client.streams = budget
	return &mu, &log
}

// exerciseStreamCalls runs a bidirectional call that waits for each echo, and
// a server-streaming call, over transport.
func exerciseStreamCalls(t *testing.T, transport Transport) {
	t.Helper()
	ctx := t.Context()
	chat, err := BidirectionalStreaming[*wrapperspb.StringValue](ctx, transport, testPath("StreamChat"), newStringValue, WithStreamCompression(StreamCompression{Requests: true, Responses: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = chat.Close() }()
	for i := range streamTestMessages {
		if err := chat.Send(compressionSample(i)); err != nil {
			t.Fatal(err)
		}
		// Waiting for the echo makes the answer in HEADERS arrive before the
		// third request.
		got, err := chat.Recv()
		if err != nil || got.GetValue() != compressionSample(i).GetValue() {
			t.Fatalf("message %d = %q, %v", i, got.GetValue(), err)
		}
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := chat.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("end of chat: %v", err)
	}
	if got := streamCompressedRequests.Load(); got != streamTestMessages-streamCompressionStart+1 {
		t.Fatalf("server saw %d stream-compressed requests, want %d", got, streamTestMessages-streamCompressionStart+1)
	}

	feed, err := ServerStreaming(ctx, transport, testPath("StreamFeed"), compressionSample(0), newStringValue, WithStreamCompression(StreamCompression{Responses: true}))
	if err != nil {
		t.Fatal(err)
	}
	for i := range streamTestMessages {
		got, err := feed.Recv()
		if err != nil || got.GetValue() != compressionSample(i).GetValue() {
			t.Fatalf("feed message %d = %q, %v", i, got.GetValue(), err)
		}
	}
	if _, err := feed.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("end of feed: %v", err)
	}
}

// streamedFrames is what a stream of streamTestMessages messages in one
// direction looks like: two per message, then the rest stream-compressed.
func streamedFrames(first string) string {
	frames := []string{first, "MESSAGE", "MESSAGE"}
	for range streamTestMessages - 2 {
		frames = append(frames, "ZSTD_STREAM_MESSAGE")
	}
	return strings.Join(frames, " ")
}

// checkStreamFrames checks the frames of the two calls exerciseStreamCalls
// made.
func checkStreamFrames(t *testing.T, mu *sync.Mutex, log *[]*recordedStream) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if len(*log) != 2 {
		t.Fatalf("%d streams, want 2", len(*log))
	}
	sent, received := (*log)[0].frames(t)
	if want := "[" + streamedFrames("REQUEST") + "]"; fmt.Sprint(sent) != want {
		t.Fatalf("chat sent %v, want %s", sent, want)
	}
	if want := "[" + streamedFrames("HEADERS") + " STATUS]"; fmt.Sprint(received) != want {
		t.Fatalf("chat received %v, want %s", received, want)
	}
	sent, received = (*log)[1].frames(t)
	if fmt.Sprint(sent) != "[REQUEST MESSAGE]" {
		t.Fatalf("feed sent %v", sent)
	}
	// A server-streaming call has no request stream to answer for, so it
	// sends no HEADERS.
	if want := "[" + strings.TrimPrefix(streamedFrames(""), " ") + " STATUS]"; fmt.Sprint(received) != want {
		t.Fatalf("feed received %v, want %s", received, want)
	}
}

// waitForEmptyBudget waits until every reservation of budget was returned,
// which happens as the calls finish on both sides.
func waitForEmptyBudget(t *testing.T, name string, budget *streamBudget) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for budget.used.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s budget still holds %d bytes", name, budget.used.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStreamCompressionOverEveryTransport runs stream-compressed calls over
// each transport the server serves, checking which frames crossed the wire
// and that every context's memory is returned afterwards.
func TestStreamCompressionOverEveryTransport(t *testing.T) {
	requireCompression(t)
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerStreamCompressionService)
	clients := map[string]*streamClient{
		"http3":          newHTTP3TestClient(t, pki, listener).(*streamClient),
		"webtransport":   newWebTransportTestClient(t, pki, listener, testOrigin, nil).(*streamClient),
		"http2":          newHTTP2TestClient(t, pki, listener).(*streamClient),
		"qmux websocket": newWebSocketTestClient(t, pki, listener, testOrigin).(*streamClient),
	}
	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			budget := newStreamBudget(0, defaultClientStreamCompressionMemory, nil)
			mu, log := streamClientFor(client, budget)
			exerciseStreamCalls(t, client)
			checkStreamFrames(t, mu, log)
			waitForEmptyBudget(t, "client", budget)
			waitForEmptyBudget(t, "server", listener.server.freeze().streams)
		})
	}

	// The native transports go through Dial and the QMux connector.
	native, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = native.Close() })
	qmuxChannel, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, TCPAddr: listener.TCPAddr().String(), Transport: TransportTCPOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qmuxChannel.Close() })
	for name, channel := range map[string]*Channel{"native": native, "qmux": qmuxChannel} {
		t.Run(name, func(t *testing.T) {
			generation, err := channel.callGeneration(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			client := generation.(*streamGeneration).client
			mu, log := streamClientFor(client, client.streams)
			exerciseStreamCalls(t, channel)
			checkStreamFrames(t, mu, log)
			waitForEmptyBudget(t, "client", client.streams)
			waitForEmptyBudget(t, "server", listener.server.freeze().streams)
		})
	}
}

// TestStreamCompressionStartsFromDictionaries checks that a stream starts
// from the dictionary negotiated for its message type, on both sides.
func TestStreamCompressionStartsFromDictionaries(t *testing.T) {
	requireCompression(t)
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, func(options *ServerOptions) { options.Compression = testCompression(t) }, registerStreamCompressionService)
	channel, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, Compression: testCompression(t), Transport: TransportQUICOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	generation, err := channel.callGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	client := generation.(*streamGeneration).client
	mu, log := streamClientFor(client, client.streams)
	// A first call learns the shared set from its HEADERS, so the chat's
	// requests use it.
	feed, err := ServerStreaming(t.Context(), channel, testPath("StreamFeed"), compressionSample(0), newStringValue)
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range Messages(feed) {
		if err != nil {
			t.Fatal(err)
		}
	}
	exerciseStreamCalls(t, channel)

	mu.Lock()
	defer mu.Unlock()
	if len(*log) != 3 {
		t.Fatalf("%d streams, want 3", len(*log))
	}
	sent := parseRawFrames(t, (*log)[1].sent.Bytes())
	received := parseRawFrames(t, (*log)[1].received.Bytes())
	for direction, frames := range map[string][]rawFrame{"requests": sent, "responses": received} {
		var types []frameType
		var first []byte
		for _, f := range frames {
			types = append(types, f.typ)
			if f.typ == frameTypeZstdStreamMessage && first == nil {
				first = f.payload
			}
		}
		if !strings.Contains(fmt.Sprint(types), "ZSTD_MESSAGE ZSTD_MESSAGE ZSTD_STREAM_MESSAGE") {
			t.Fatalf("%s: frames %v", direction, types)
		}
		// The first chunk starts a frame that names dictionary 1.
		_, n, err := quicvarint.Parse(first)
		if err != nil {
			t.Fatal(err)
		}
		info, err := zstd.ParseStreamChunk(first[n:], true, zstd.StreamWindowLog)
		if err != nil || info.DictID != 1 {
			t.Fatalf("%s: first chunk %+v, %v", direction, info, err)
		}
	}
}

// TestStreamCompressionWithinBudget checks that calls beyond the budget, and
// on a peer that turned stream compression off, compress per message only.
func TestStreamCompressionWithinBudget(t *testing.T) {
	requireCompression(t)
	pki := newTestPKI(t)
	for _, test := range []struct {
		name   string
		server int64
		client int64
	}{
		{"server budget too small", 1, 0},
		{"server off", -1, 0},
		{"client budget too small", 0, 1},
		{"client off", 0, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener := startTestListenerWith(t, pki, func(options *ServerOptions) { options.StreamCompressionMemory = test.server }, registerStreamCompressionService)
			channel, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly, StreamCompressionMemory: test.client})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = channel.Close() })
			generation, err := channel.callGeneration(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			client := generation.(*streamGeneration).client
			var mu sync.Mutex
			var log []*recordedStream
			client.opener = recordingOpener{inner: client.opener, mu: &mu, log: &log}
			chat, err := BidirectionalStreaming[*wrapperspb.StringValue](t.Context(), channel, testPath("StreamChat"), newStringValue, WithStreamCompression(StreamCompression{Requests: true, Responses: true}))
			if err != nil {
				t.Fatal(err)
			}
			for i := range streamTestMessages {
				if err := chat.Send(compressionSample(i)); err != nil {
					t.Fatal(err)
				}
				if _, err := chat.Recv(); err != nil {
					t.Fatal(err)
				}
			}
			_ = chat.CloseSend()
			if _, err := chat.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("end of chat: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			sent, received := log[0].frames(t)
			if strings.Contains(fmt.Sprint(sent, received), "ZSTD_STREAM_MESSAGE") {
				t.Fatalf("sent %v, received %v", sent, received)
			}
			if budget := client.streams; budget != nil {
				waitForEmptyBudget(t, "client", budget)
			}
			if budget := listener.server.freeze().streams; budget != nil {
				waitForEmptyBudget(t, "server", budget)
			}
		})
	}
}

func TestNegotiateStreamsFollowsTheRoute(t *testing.T) {
	requireCompression(t)
	server := NewServer()
	echo := func(_ context.Context, requests MessageStream[*wrapperspb.StringValue]) (MessageStream[*wrapperspb.StringValue], error) {
		return requests, nil
	}
	RegisterBidirectionalStreaming(server, testPath("Both"), newStringValue, echo, RouteStreamCompression(StreamCompression{Requests: true, Responses: true}))
	RegisterBidirectionalStreaming(server, testPath("Requests"), newStringValue, echo, RouteStreamCompression(StreamCompression{Requests: true}))
	RegisterBidirectionalStreaming(server, testPath("Unreliable"), newStringValue, echo,
		RouteStreamCompression(StreamCompression{Requests: true, Responses: true}), RouteUnreliable(Unreliable{Requests: true, Responses: true}))
	RegisterBidirectionalStreaming(server, testPath("Plain"), newStringValue, echo)
	runtime := server.freeze()
	for _, test := range []struct {
		method              string
		offer               *streamCompressionInfo
		answers, compresses bool
	}{
		{"Both", streamOffer(), true, true},
		{"Both", nil, false, false},
		{"Both", &streamCompressionInfo{MaxWindowLog: 9}, true, false},
		{"Requests", streamOffer(), true, false},
		{"Unreliable", streamOffer(), false, false},
		{"Plain", streamOffer(), false, false},
		{"Missing", streamOffer(), false, false},
	} {
		request := requestFor(test.method, RPCKindBidirectionalStreaming)
		request.StreamCompression = test.offer
		streams := runtime.negotiateStreams(request)
		if (streams.requestInflater() != nil) != test.answers || (streams != nil && (streams.answer != nil) != test.answers) || (streams.responseCompressor() != nil) != test.compresses {
			t.Errorf("%s with %+v: %+v", test.method, test.offer, streams)
		}
		streams.close()
	}
	if used := runtime.streams.used.Load(); used != 0 {
		t.Fatalf("budget holds %d bytes after the calls", used)
	}
}

func TestStreamCompressorStartsAtTheThirdMessage(t *testing.T) {
	requireCompression(t)
	budget := newStreamBudget(0, defaultClientStreamCompressionMemory, nil)
	compressor := newStreamCompressor(budget, nil)
	defer compressor.close()
	var types []frameType
	for i := range 5 {
		if i == 3 {
			// Enabled late, it starts with the next message.
			compressor.enable(zstd.StreamWindowLog)
		}
		encoded, err := compressor.encode(compressionSample(i))
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, parseRawFrames(t, encoded.frame)[0].typ)
		if encoded.size != len(compressionSample(i).GetValue())+2 {
			t.Fatalf("message %d size %d", i, encoded.size)
		}
	}
	if fmt.Sprint(types) != "[MESSAGE MESSAGE MESSAGE ZSTD_STREAM_MESSAGE ZSTD_STREAM_MESSAGE]" {
		t.Fatalf("frames %v", types)
	}
	if budget.used.Load() != budget.encoderCost {
		t.Fatalf("budget holds %d, want one encoder", budget.used.Load())
	}
	compressor.close()
	if budget.used.Load() != 0 {
		t.Fatalf("budget holds %d after close", budget.used.Load())
	}
}

func TestStreamCompressionIsOffWithoutCgo(t *testing.T) {
	if zstd.Available {
		t.Skip("built with cgo")
	}
	if newStreamBudget(0, defaultServerStreamCompressionMemory, nil) != nil {
		t.Fatal("a budget without cgo")
	}
}
