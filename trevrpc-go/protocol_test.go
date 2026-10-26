package trevrpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// greaseFrame is a frame of a reserved grease type with size payload bytes.
func greaseFrame(typ frameType, size int) []byte {
	return append(appendFrameHeader(nil, typ, size), bytes.Repeat([]byte{0xee}, size)...)
}

// statusBytes is a STATUS frame.
func statusBytes(t testing.TB, status *Status) []byte {
	t.Helper()
	frame, err := appendStatusFrame(nil, status, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// headersBytes is a HEADERS frame.
func headersBytes(t testing.TB, metadata Metadata) []byte {
	t.Helper()
	frame, err := appendHeadersFrame(nil, metadata, headersAnswer{}, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func messageBytes(body string) []byte { return rawMessage([]byte(body)).frame }

// concat joins byte slices.
func concat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// serveTestStream runs handleRPCStream over input and returns what the server
// wrote and the stream's bookkeeping.
func serveTestStream(t *testing.T, server *Server, input []byte) (*countingRPCStream, []testFrame) {
	t.Helper()
	stream := &countingRPCStream{reader: bytes.NewReader(input)}
	handleRPCStream(context.Background(), server, stream, nil, nil)
	return stream, readTestFrames(t, stream.written.Bytes(), DefaultMaxFrameSize)
}

func requestFor(method string, kind RPCKind) *wireRequest {
	request := newWireRequest(testPath(method))
	request.Kind = kind
	return request
}

func encodedTestMessage(t testing.TB, value string) string {
	t.Helper()
	body, err := marshalMessage(&testMessage{Value: value})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func newProtocolTestServer(options ...func(*ServerOptions)) *Server {
	server := NewServer()
	serverOptions := server.Options()
	for _, configure := range options {
		configure(&serverOptions)
	}
	server.SetOptions(serverOptions)
	registerTestService(server)
	return server
}

// requireReply fails the test unless frames are one MESSAGE carrying a
// testMessage value and an OK STATUS.
func requireReply(t *testing.T, frames []testFrame, value string) {
	t.Helper()
	if len(frames) != 2 || frames[0].Kind != frameTypeMessage || frames[1].Kind != frameTypeStatus || CodeFromUint32(frames[1].Status) != CodeOK {
		t.Fatalf("frames = %#v, want MESSAGE and OK STATUS", frames)
	}
	reply := &testMessage{}
	if err := unmarshalMessage(frames[0].Body, reply); err != nil || reply.Value != value {
		t.Fatalf("reply = %#v, %v, want %q", reply, err, value)
	}
}

func TestServerSkipsUnknownFramesEverywhere(t *testing.T) {
	server := newProtocolTestServer()
	grease := greaseFrame(0x21, 5)
	greaseTwoByte := greaseFrame(0x40, 300)
	greaseEmpty := greaseFrame(0x1f*9+0x21, 0)
	hello := encodedTestMessage(t, "hello")

	t.Run("unary", func(t *testing.T) {
		request := testRequestBytes(t, requestFor("Echo", RPCKindUnary))
		_, frames := serveTestStream(t, server, concat(grease, request, greaseTwoByte, messageBytes(hello), greaseEmpty, grease))
		requireReply(t, frames, "hello")
	})
	t.Run("server streaming", func(t *testing.T) {
		request := testRequestBytes(t, requestFor("Count", RPCKindServerStreaming))
		_, frames := serveTestStream(t, server, concat(greaseTwoByte, request, grease, messageBytes(encodedTestMessage(t, "n")), greaseEmpty))
		if len(frames) != 4 || frames[3].Kind != frameTypeStatus || CodeFromUint32(frames[3].Status) != CodeOK {
			t.Fatalf("frames = %#v, want three MESSAGEs and an OK STATUS", frames)
		}
	})
	for _, kind := range []RPCKind{RPCKindClientStreaming, RPCKindBidirectionalStreaming} {
		t.Run(fmt.Sprintf("kind %d", kind), func(t *testing.T) {
			method := map[RPCKind]string{RPCKindClientStreaming: "Concat", RPCKindBidirectionalStreaming: "Chat"}[kind]
			request := testRequestBytes(t, requestFor(method, kind))
			input := concat(grease, request, greaseTwoByte, messageBytes(encodedTestMessage(t, "a")), grease,
				messageBytes(encodedTestMessage(t, "b")), greaseEmpty, statusBytes(t, OK()), greaseTwoByte)
			_, frames := serveTestStream(t, server, input)
			if kind == RPCKindClientStreaming {
				requireReply(t, frames, "ab")
				return
			}
			if len(frames) != 3 || frames[2].Kind != frameTypeStatus || CodeFromUint32(frames[2].Status) != CodeOK {
				t.Fatalf("frames = %#v, want two MESSAGEs and an OK STATUS", frames)
			}
		})
	}
}

func TestServerRejectsKnownFramesOutOfPlace(t *testing.T) {
	var called atomic.Int64
	server := newProtocolTestServer()
	server.routeUnary("/example.Greeter/Probe", func(context.Context, []byte) (encodedMessage, error) {
		called.Add(1)
		return rawMessage(nil), nil
	})
	server.routeStreaming("/example.Greeter/Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (encodedStream, error) {
		called.Add(1)
		return rawResponse()
	})
	unary := testRequestBytes(t, requestFor("Probe", RPCKindUnary))
	download := testRequestBytes(t, requestFor("Download", RPCKindServerStreaming))
	concatRequest := testRequestBytes(t, requestFor("Concat", RPCKindClientStreaming))
	chatRequest := testRequestBytes(t, requestFor("Chat", RPCKindBidirectionalStreaming))
	one, two := messageBytes(""), messageBytes("x")
	headers := headersBytes(t, Metadata{"x-a": []byte("1")})
	status := statusBytes(t, OK())

	tests := []struct {
		name  string
		input []byte
		// handler reports whether the handler may run.
		handler bool
	}{
		{name: "message before REQUEST", input: concat(one, unary)},
		{name: "status before REQUEST", input: concat(status, unary)},
		{name: "headers before REQUEST", input: concat(headers, unary)},
		{name: "unary without message", input: unary},
		{name: "unary with two messages", input: concat(unary, one, two)},
		{name: "unary with a second REQUEST", input: concat(unary, unary, one)},
		{name: "unary with headers", input: concat(unary, headers, one)},
		{name: "unary with status in place of the message", input: concat(unary, status)},
		{name: "unary with status after the message", input: concat(unary, one, status)},
		{name: "server streaming without message", input: download},
		{name: "server streaming with two messages", input: concat(download, one, two)},
		{name: "server streaming with status after the message", input: concat(download, one, status)},
		{name: "client streaming with headers", input: concat(concatRequest, headers), handler: true},
		{name: "client streaming with a second REQUEST", input: concat(concatRequest, concatRequest), handler: true},
		{name: "bidirectional with headers", input: concat(chatRequest, headers), handler: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called.Store(0)
			stream, frames := serveTestStream(t, server, test.input)
			requireStatusFrame(t, frames, CodeInvalidArgument)
			if !test.handler && called.Load() != 0 {
				t.Fatal("handler ran for a malformed request")
			}
			if stream.closeCount != 1 {
				t.Fatalf("stream closed %d times, want 1", stream.closeCount)
			}
		})
	}
}

func TestServerRejectsOversizedAndTruncatedRequestFrames(t *testing.T) {
	server := newProtocolTestServer(func(options *ServerOptions) { options.MaxFrameSize = 64 })
	request := testRequestBytes(t, requestFor("Echo", RPCKindUnary))

	tests := []struct {
		name  string
		input []byte
		code  Code
	}{
		{name: "oversized REQUEST", input: appendFrameHeader(nil, frameTypeRequest, 1<<40), code: CodeResourceExhausted},
		{name: "oversized MESSAGE", input: concat(request, appendFrameHeader(nil, frameTypeMessage, 65)), code: CodeResourceExhausted},
		{name: "oversized grease", input: concat(appendFrameHeader(nil, 0x21, 65), request), code: CodeResourceExhausted},
		{name: "MESSAGE at the limit", input: concat(request, rawMessage(bytes.Repeat([]byte{0x0a, 0x00}, 32)).frame), code: CodeOK},
		{name: "mid-header EOF in the type", input: []byte{0x40}, code: CodeUnavailable},
		{name: "mid-header EOF in the length", input: concat(request, []byte{0x00, 0x40}), code: CodeUnavailable},
		{name: "mid-payload EOF", input: concat(request, []byte{0x00, 0x04, 'a'}), code: CodeUnavailable},
		{name: "empty stream", input: nil, code: CodeUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, frames := serveTestStream(t, server, test.input)
			if test.code == CodeOK {
				if len(frames) == 0 || frames[len(frames)-1].Kind != frameTypeStatus {
					t.Fatalf("frames = %#v, want a STATUS", frames)
				}
				return
			}
			requireStatusFrame(t, frames, test.code)
		})
	}
}

func TestServerChargesUnknownFramesToTheStreamBodyLimit(t *testing.T) {
	server := newProtocolTestServer(func(options *ServerOptions) {
		options.MaxStreamBodySize = 64
		options.MaxStreamMessages = -1
	})
	hello := messageBytes(encodedTestMessage(t, "hello"))

	t.Run("unary", func(t *testing.T) {
		request := testRequestBytes(t, requestFor("Echo", RPCKindUnary))
		// Each grease frame is 22 bytes and the message payload is 7: two
		// grease frames fit in the 64 byte limit, three do not.
		grease := greaseFrame(0x21, 20)
		_, frames := serveTestStream(t, server, concat(request, grease, grease, hello))
		requireReply(t, frames, "hello")

		_, frames = serveTestStream(t, server, concat(request, grease, grease, grease, hello))
		requireStatusFrame(t, frames, CodeResourceExhausted)
	})
	t.Run("grease alone", func(t *testing.T) {
		request := testRequestBytes(t, requestFor("Echo", RPCKindUnary))
		var flood []byte
		for range 100 {
			flood = append(flood, greaseFrame(0x21, 0)...)
		}
		_, frames := serveTestStream(t, server, concat(request, flood, hello))
		requireStatusFrame(t, frames, CodeResourceExhausted)
	})
	t.Run("client streaming shares the limit with its messages", func(t *testing.T) {
		request := testRequestBytes(t, requestFor("Concat", RPCKindClientStreaming))
		messages := concat(hello, hello, hello) // three 9 byte message frames: 7 payload bytes each
		_, frames := serveTestStream(t, server, concat(request, messages))
		requireReply(t, frames, strings.Repeat("hello", 3))

		_, frames = serveTestStream(t, server, concat(request, messages, greaseFrame(0x21, 50)))
		requireStatusFrame(t, frames, CodeResourceExhausted)
	})
}

func TestServerDoesNotSendHeadersWithoutAny(t *testing.T) {
	server := newProtocolTestServer()
	request := testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "hi"))
	_, frames := serveTestStream(t, server, request)
	requireReply(t, frames, "hi")

	withHeaders := newWireRequest(testPath("Echo"))
	withHeaders.Metadata[testWantHeadersKey] = []byte("v")
	_, frames = serveTestStream(t, server, testRequestBytes(t, withHeaders, encodedTestMessage(t, "hi")))
	if len(frames) != 3 || frames[0].Kind != frameTypeHeaders || frames[1].Kind != frameTypeMessage || frames[2].Kind != frameTypeStatus {
		t.Fatalf("frames = %#v, want HEADERS, MESSAGE, STATUS", frames)
	}
	requireHeaders(t, "headers", frames[0].Headers, testHeaderKey, "v")
	requireHeaders(t, "trailing metadata", frames[2].Metadata, testTrailerKey, "v")
}

func TestServerStreamingHeadersAreWrittenBeforeTheFirstMessageIsPulled(t *testing.T) {
	server := NewServer()
	release := make(chan struct{})
	pulled := make(chan struct{})
	RegisterServerStreaming(server, "/example.Greeter/Slow", newTestMessage, func(ctx context.Context, _ *testMessage) (MessageStream[*testMessage], error) {
		if err := SetResponseHeader(ctx, "x-h", []byte("1")); err != nil {
			return nil, err
		}
		return &notifyingStream{pulled: pulled, release: release}, nil
	})
	request := testRequestBytes(t, requestFor("Slow", RPCKindServerStreaming), "")
	stream := &lockedRPCStream{reader: bytes.NewReader(request)}
	done := make(chan struct{})
	go func() {
		handleRPCStream(context.Background(), server, stream, nil, nil)
		close(done)
	}()

	select {
	case <-pulled:
	case <-time.After(testTimeout):
		t.Fatal("the handler's stream was never pulled")
	}
	// The stream is blocked pulling its first message: the headers are out.
	var written []testFrame
	deadline := time.Now().Add(testTimeout)
	for len(written) == 0 && time.Now().Before(deadline) {
		written = readTestFrames(t, stream.snapshot(), DefaultMaxFrameSize)
		time.Sleep(time.Millisecond)
	}
	if len(written) != 1 || written[0].Kind != frameTypeHeaders {
		t.Fatalf("written before the first message = %#v, want only HEADERS", written)
	}
	close(release)
	<-done
}

// lockedRPCStream is an RPC stream whose output can be read while the server
// is still writing it.
type lockedRPCStream struct {
	reader  *bytes.Reader
	mu      sync.Mutex
	written bytes.Buffer
}

func (s *lockedRPCStream) Read(data []byte) (int, error) { return s.reader.Read(data) }

func (s *lockedRPCStream) Write(data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written.Write(data)
}

func (*lockedRPCStream) Close() error { return nil }

func (s *lockedRPCStream) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.written.Bytes())
}

// notifyingStream signals when it is first pulled and then waits for release.
type notifyingStream struct {
	pulled  chan struct{}
	release chan struct{}
	sent    bool
}

func (s *notifyingStream) Recv() (*testMessage, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	close(s.pulled)
	<-s.release
	return &testMessage{Value: "late"}, nil
}

func (*notifyingStream) Close() error { return nil }

func TestServerIgnoresRequestExtensions(t *testing.T) {
	server := newProtocolTestServer()
	request := requestFor("Echo", RPCKindUnary)
	request.Extensions = []uint64{1, 300}
	_, frames := serveTestStream(t, server, testRequestBytes(t, request, encodedTestMessage(t, "hi")))
	requireReply(t, frames, "hi")
}

func TestServerMetricsReportTheRequestMessageLength(t *testing.T) {
	metrics := &recordingMetrics{}
	server := newProtocolTestServer()
	server.SetMetrics(metrics)
	body := encodedTestMessage(t, "hello")

	for _, test := range []struct {
		method string
		kind   RPCKind
	}{{"Echo", RPCKindUnary}, {"Count", RPCKindServerStreaming}} {
		serveTestStream(t, server, testRequestBytes(t, requestFor(test.method, test.kind), body))
	}
	started, finished := metrics.snapshot()
	if len(started) != 2 || len(finished) != 2 {
		t.Fatalf("started = %v, finished = %v, want two of each", started, finished)
	}
	for index, event := range started {
		if event.RequestBodyLen != len(body) || finished[index].RequestBodyLen != len(body) {
			t.Fatalf("call %d reported request lengths %d and %d, want %d", index, event.RequestBodyLen, finished[index].RequestBodyLen, len(body))
		}
	}
	if finished[0].ResponseBodyLen != len(body) {
		t.Fatalf("unary response length = %d, want %d", finished[0].ResponseBodyLen, len(body))
	}
}
