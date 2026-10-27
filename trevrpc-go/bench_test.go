package trevrpc

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// benchSize is one payload size a benchmark runs at.
type benchSize struct {
	name  string
	bytes int
}

var benchSizes = []benchSize{{"16B", 16}, {"1KiB", 1 << 10}, {"64KiB", 64 << 10}}

// runSizes runs fn as one sub-benchmark per payload size. Each reports
// allocations and the payload bytes one operation moves.
func runSizes(b *testing.B, fn func(b *testing.B, size int)) {
	b.Helper()
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size.bytes))
			fn(b, size.bytes)
		})
	}
}

// --- Frame layer ---

// BenchmarkFrameWriteMessage measures marshalling a message into a freshly
// allocated MESSAGE frame, as the server does for every response.
func BenchmarkFrameWriteMessage(b *testing.B) {
	runSizes(b, func(b *testing.B, size int) {
		message := &testMessage{Value: strings.Repeat("x", size)}
		for b.Loop() {
			if _, err := encodeMessage(message); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFrameAppendMessage measures marshalling a message into a reused
// buffer, as the client does for every request.
func BenchmarkFrameAppendMessage(b *testing.B) {
	runSizes(b, func(b *testing.B, size int) {
		message := &testMessage{Value: strings.Repeat("x", size)}
		buf := make([]byte, 0, size+frameHeaderLen(frameTypeMessage, size+8))
		for b.Loop() {
			var err error
			if buf, err = appendLimitedProtoFrame(buf[:0], frameTypeMessage, message, DefaultMaxFrameSize); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFrameReadMessage measures the frame reader on a stream of MESSAGE
// frames, without decoding their payloads.
func BenchmarkFrameReadMessage(b *testing.B) {
	runSizes(b, func(b *testing.B, size int) {
		encoded, err := encodeMessage(&testMessage{Value: strings.Repeat("x", size)})
		if err != nil {
			b.Fatal(err)
		}
		reader := newFrameReader(&repeatReader{data: encoded.frame}, DefaultMaxFrameSize, -1)
		for b.Loop() {
			next, err := reader.next()
			if err != nil {
				b.Fatal(err)
			}
			if next.typ != frameTypeMessage || len(next.payload) != encoded.size {
				b.Fatalf("frame = %v with %d payload bytes", next.typ, len(next.payload))
			}
		}
	})
}

// BenchmarkFrameDecodeMessage measures decoding a MESSAGE payload.
func BenchmarkFrameDecodeMessage(b *testing.B) {
	runSizes(b, func(b *testing.B, size int) {
		encoded, err := encodeMessage(&testMessage{Value: strings.Repeat("x", size)})
		if err != nil {
			b.Fatal(err)
		}
		payload := benchFramePayload(b, encoded.frame)
		for b.Loop() {
			if err := unmarshalMessage(payload, new(testMessage)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFrameDecodeStatus measures decoding an error STATUS payload whose
// message is size bytes long.
func BenchmarkFrameDecodeStatus(b *testing.B) {
	runSizes(b, func(b *testing.B, size int) {
		status := NewStatus(CodeNotFound, strings.Repeat("x", size))
		frame, err := appendStatusFrame(nil, status, DefaultMaxFrameSize)
		if err != nil {
			b.Fatal(err)
		}
		payload := benchFramePayload(b, frame)
		for b.Loop() {
			decoded, err := decodeStatus(payload)
			if err != nil || decoded.Code != CodeNotFound {
				b.Fatalf("status = %v, err = %v", decoded, err)
			}
		}
	})
}

// BenchmarkFrameDecodeHeaders measures decoding a HEADERS payload whose single
// metadata value is size bytes long.
func BenchmarkFrameDecodeHeaders(b *testing.B) {
	runSizes(b, func(b *testing.B, size int) {
		frame, err := appendHeadersFrame(nil, Metadata{"x-header": []byte(strings.Repeat("x", size))}, DefaultMaxFrameSize)
		if err != nil {
			b.Fatal(err)
		}
		payload := benchFramePayload(b, frame)
		for b.Loop() {
			metadata, err := decodeHeaders(payload)
			if err != nil || len(metadata["x-header"]) != size {
				b.Fatalf("metadata length = %d, err = %v", len(metadata["x-header"]), err)
			}
		}
	})
}

// benchFramePayload returns the payload of the single frame in data.
func benchFramePayload(b *testing.B, data []byte) []byte {
	b.Helper()
	next, err := newFrameReader(strings.NewReader(string(data)), DefaultMaxFrameSize, -1).next()
	if err != nil {
		b.Fatal(err)
	}
	return next.payload
}

// repeatReader reads data over and over.
type repeatReader struct {
	data   []byte
	offset int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.offset:])
	r.offset = (r.offset + n) % len(r.data)
	return n, nil
}

// --- End to end over loopback ---

const (
	// benchCountKey is the request metadata key naming how many messages
	// Repeat streams.
	benchCountKey = "count"
	// benchChunk bounds the messages of one streaming call, so a call stays
	// short next to the connection setup it amortises.
	benchChunk = 4096
	// benchTimeout bounds one whole benchmark run; a hung transport fails
	// instead of running into the test binary's timeout.
	benchTimeout = 5 * time.Minute
)

// registerBenchService registers handlers that carry any payload:
//   - Unary returns the request.
//   - Repeat streams the request as many times as the count metadata says.
//   - Sink counts the messages it receives.
//   - Chat returns every message it receives.
func registerBenchService(server *Server) {
	RegisterUnary(server, "/bench.Echo/Unary", newTestMessage, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return request, nil
	})
	RegisterServerStreaming(server, "/bench.Echo/Repeat", newTestMessage, func(ctx context.Context, request *testMessage) (MessageStream[*testMessage], error) {
		count, err := strconv.Atoi(string(RequestMetadataFromContext(ctx)[benchCountKey]))
		if err != nil {
			return nil, InvalidArgument("count: " + err.Error())
		}
		return &repeatStream{message: request, left: count}, nil
	})
	RegisterClientStreaming(server, "/bench.Echo/Sink", newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
		var count int
		for _, err := range Messages(requests) {
			if err != nil {
				return nil, err
			}
			count++
		}
		return &testMessage{Value: strconv.Itoa(count)}, nil
	})
	RegisterBidirectionalStreaming(server, "/bench.Echo/Chat", newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
		return echoStream{requests}, nil
	})
}

// repeatStream yields message left times.
type repeatStream struct {
	message *testMessage
	left    int
}

func (s *repeatStream) Recv() (*testMessage, error) {
	if s.left == 0 {
		return nil, io.EOF
	}
	s.left--
	return s.message, nil
}

func (*repeatStream) Close() error { return nil }

// echoStream answers every request with itself.
type echoStream struct{ requests MessageStream[*testMessage] }

func (s echoStream) Recv() (*testMessage, error) { return s.requests.Recv() }
func (s echoStream) Close() error                { return s.requests.Close() }

// benchTransport dials the test client of one transport.
type benchTransport struct {
	name string
	dial func(testing.TB, testPKI, *Listener) Transport
}

var benchTransports = []benchTransport{
	{"quic", func(tb testing.TB, pki testPKI, l *Listener) Transport { return dialNative(tb, pki, l) }},
	{"qmux", func(tb testing.TB, pki testPKI, l *Listener) Transport { return newQMuxTestClient(tb, pki, l) }},
	{"h3", newHTTP3TestClient},
	{"h2", newHTTP2TestClient},
	{"webtransport", func(tb testing.TB, pki testPKI, l *Listener) Transport {
		return newWebTransportTestClient(tb, pki, l, testOrigin, nil)
	}},
	{"websocket", func(tb testing.TB, pki testPKI, l *Listener) Transport {
		return newWebSocketTestClient(tb, pki, l, testOrigin)
	}},
}

// benchEndToEnd serves the bench service on one listener for the whole
// benchmark and runs fn once per transport and payload size, each on a fresh
// client connection. Dialling and closing the client stay outside the timed
// region, which fn starts with b.Loop or b.ResetTimer.
func benchEndToEnd(b *testing.B, fn func(ctx context.Context, b *testing.B, transport Transport, payload string)) {
	b.Helper()
	pki := newTestPKI(b)
	listener := startTestListenerWith(b, pki, nil, registerBenchService)
	for _, tr := range benchTransports {
		b.Run(tr.name, func(b *testing.B) {
			runSizes(b, func(b *testing.B, size int) {
				transport := tr.dial(b, pki, listener)
				ctx, cancel := context.WithTimeout(context.Background(), benchTimeout)
				b.Cleanup(cancel)
				fn(ctx, b, transport, strings.Repeat("x", size))
			})
		})
	}
}

// reportMessages reports the message throughput of the b.N messages of the
// timed region so far.
func reportMessages(b *testing.B) {
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

// eachChunk calls fn with the sizes of the calls that together carry b.N
// messages. It stops at the first error.
func eachChunk(b *testing.B, fn func(count int) error) {
	b.Helper()
	for left := b.N; left > 0; {
		count := min(left, benchChunk)
		if err := fn(count); err != nil {
			b.Fatal(err)
		}
		left -= count
	}
}

func BenchmarkUnary(b *testing.B) {
	benchEndToEnd(b, func(ctx context.Context, b *testing.B, transport Transport, payload string) {
		request := &testMessage{Value: payload}
		for b.Loop() {
			reply, err := Unary(ctx, transport, "/bench.Echo/Unary", request, newTestMessage)
			if err != nil {
				b.Fatal(err)
			}
			if len(reply.Value) != len(payload) {
				b.Fatalf("reply length = %d, want %d", len(reply.Value), len(payload))
			}
		}
	})
}

// benchWorkers is how many calls BenchmarkUnaryParallel keeps in flight on
// its one connection, which is the server's default stream limit.
const benchWorkers = 64

// BenchmarkUnaryParallel measures unary calls on one connection with
// benchWorkers calls in flight, so it reports throughput under concurrency
// rather than the latency of one call.
func BenchmarkUnaryParallel(b *testing.B) {
	benchEndToEnd(b, func(ctx context.Context, b *testing.B, transport Transport, payload string) {
		request := &testMessage{Value: payload}
		b.SetParallelism(benchWorkers / runtime.GOMAXPROCS(0))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				reply, err := Unary(ctx, transport, "/bench.Echo/Unary", request, newTestMessage)
				if err != nil {
					b.Error(err)
					return
				}
				if len(reply.Value) != len(payload) {
					b.Errorf("reply length = %d, want %d", len(reply.Value), len(payload))
					return
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
	})
}

// BenchmarkServerStream reports the time per streamed message.
func BenchmarkServerStream(b *testing.B) {
	benchEndToEnd(b, func(ctx context.Context, b *testing.B, transport Transport, payload string) {
		request := &testMessage{Value: payload}
		b.ResetTimer()
		eachChunk(b, func(count int) error {
			stream, err := ServerStreaming(ctx, transport, "/bench.Echo/Repeat", request, newTestMessage,
				WithMetadata(benchCountKey, []byte(strconv.Itoa(count))))
			if err != nil {
				return err
			}
			received := 0
			for reply, err := range Messages(stream) {
				if err != nil {
					_ = stream.Close()
					return err
				}
				if len(reply.Value) != len(payload) {
					_ = stream.Close()
					return errors.New("reply has the wrong length")
				}
				received++
			}
			if received != count {
				return errors.New("stream ended early")
			}
			return stream.Close()
		})
		reportMessages(b)
	})
}

// BenchmarkClientStream reports the time per uploaded message.
func BenchmarkClientStream(b *testing.B) {
	benchEndToEnd(b, func(ctx context.Context, b *testing.B, transport Transport, payload string) {
		request := &testMessage{Value: payload}
		b.ResetTimer()
		eachChunk(b, func(count int) error {
			call, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, "/bench.Echo/Sink", newTestMessage)
			if err != nil {
				return err
			}
			for range count {
				if err := call.Send(request); err != nil {
					_ = call.Close()
					return err
				}
			}
			reply, err := call.CloseAndRecv()
			if err != nil {
				_ = call.Close()
				return err
			}
			if reply.Value != strconv.Itoa(count) {
				return errors.New("server counted " + reply.Value + " messages, want " + strconv.Itoa(count))
			}
			return call.Close()
		})
		reportMessages(b)
	})
}

// BenchmarkBidiPingPong measures one echo round trip on one open stream.
func BenchmarkBidiPingPong(b *testing.B) {
	benchEndToEnd(b, func(ctx context.Context, b *testing.B, transport Transport, payload string) {
		call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/bench.Echo/Chat", newTestMessage)
		if err != nil {
			b.Fatal(err)
		}
		request := &testMessage{Value: payload}
		for b.Loop() {
			if err := call.Send(request); err != nil {
				b.Fatal(err)
			}
			reply, err := call.Recv()
			if err != nil {
				b.Fatal(err)
			}
			if len(reply.Value) != len(payload) {
				b.Fatalf("reply length = %d, want %d", len(reply.Value), len(payload))
			}
		}
		// b.Loop has stopped the timer: end the call before the connection closes.
		if err := call.CloseSend(); err != nil {
			b.Fatal(err)
		}
		if _, err := call.Recv(); !errors.Is(err, io.EOF) {
			b.Fatalf("Recv after CloseSend error = %v, want EOF", err)
		}
		if err := call.Close(); err != nil {
			b.Fatal(err)
		}
	})
}

// BenchmarkBidiPingPongParallel measures ping-pong on one connection with
// benchWorkers bidirectional calls in flight, each exchanging one message at
// a time, so it reports the per-message cost of the server under concurrency.
func BenchmarkBidiPingPongParallel(b *testing.B) {
	benchEndToEnd(b, func(ctx context.Context, b *testing.B, transport Transport, payload string) {
		request := &testMessage{Value: payload}
		b.SetParallelism(benchWorkers / runtime.GOMAXPROCS(0))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/bench.Echo/Chat", newTestMessage)
			if err != nil {
				b.Error(err)
				return
			}
			defer func() { _ = call.Close() }()
			for pb.Next() {
				if err := call.Send(request); err != nil {
					b.Error(err)
					return
				}
				reply, err := call.Recv()
				if err != nil {
					b.Error(err)
					return
				}
				if len(reply.Value) != len(payload) {
					b.Errorf("reply length = %d, want %d", len(reply.Value), len(payload))
					return
				}
			}
			if err := call.CloseSend(); err != nil {
				b.Error(err)
				return
			}
			if _, err := call.Recv(); !errors.Is(err, io.EOF) {
				b.Errorf("Recv after CloseSend error = %v, want EOF", err)
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
}
