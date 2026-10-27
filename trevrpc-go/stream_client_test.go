package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type unaryTestOpener struct {
	stream *unaryTransportTestStream
}

func (o unaryTestOpener) openStream(context.Context) (clientStream, error) {
	return o.stream, nil
}

type unaryTransportTestStream struct {
	response      bytes.Reader
	written       bytes.Buffer
	eof           chan struct{}
	cancelled     chan struct{}
	waitingForEOF chan struct{}
	waitOnce      sync.Once
	cancelOnce    sync.Once
	cancelReads   atomic.Int32
	cancelWrites  atomic.Int32
	writes        atomic.Int32
	sendClosed    atomic.Bool
}

func newUnaryTransportTestStream(response []byte) *unaryTransportTestStream {
	stream := &unaryTransportTestStream{
		eof:           make(chan struct{}),
		cancelled:     make(chan struct{}),
		waitingForEOF: make(chan struct{}),
	}
	stream.response.Reset(response)
	return stream
}

func (s *unaryTransportTestStream) Read(data []byte) (int, error) {
	if s.response.Len() != 0 {
		return s.response.Read(data)
	}
	s.waitOnce.Do(func() { close(s.waitingForEOF) })
	select {
	case <-s.eof:
		return 0, io.EOF
	case <-s.cancelled:
		return 0, context.Canceled
	}
}

func (s *unaryTransportTestStream) Write(data []byte) (int, error) {
	s.writes.Add(1)
	return s.written.Write(data)
}

func (s *unaryTransportTestStream) Close() error {
	s.sendClosed.Store(true)
	return nil
}

func (s *unaryTransportTestStream) SetReadDeadline(time.Time) error { return nil }

func (s *unaryTransportTestStream) cancelRead() {
	s.cancelReads.Add(1)
	s.cancelOnce.Do(func() { close(s.cancelled) })
}

func (s *unaryTransportTestStream) cancelWrite() {
	s.cancelWrites.Add(1)
}

// encodeUnaryTestResponse is the response of a unary call: a MESSAGE and an OK STATUS.
func encodeUnaryTestResponse(t *testing.T) []byte {
	t.Helper()
	message, err := encodeMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	data := append([]byte(nil), message.frame...)
	data, err = appendStatusFrame(data, OK(), DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("appendStatusFrame() error = %v", err)
	}
	return data
}

func callUnaryTestClient(
	ctx context.Context,
	stream *unaryTransportTestStream,
) (*testMessage, error) {
	client := newStreamClient(unaryTestOpener{stream: stream}, DefaultMaxFrameSize)
	return Unary(ctx, client, "/service/method", &testMessage{Value: "request"}, func() *testMessage { return &testMessage{} })
}

func TestTransportUnaryWaitsForCleanPeerEOF(t *testing.T) {
	stream := newUnaryTransportTestStream(encodeUnaryTestResponse(t))
	result := make(chan struct {
		response *testMessage
		err      error
	}, 1)
	go func() {
		response, err := callUnaryTestClient(context.Background(), stream)
		result <- struct {
			response *testMessage
			err      error
		}{response: response, err: err}
	}()

	select {
	case <-stream.waitingForEOF:
	case <-time.After(time.Second):
		t.Fatal("Unary() did not reach peer EOF wait")
	}
	select {
	case completed := <-result:
		t.Fatalf("Unary() returned before peer EOF: %#v", completed)
	default:
	}

	close(stream.eof)
	select {
	case completed := <-result:
		if completed.err != nil {
			t.Fatalf("Unary() error = %v", completed.err)
		}
		if completed.response.Value != "response" {
			t.Fatalf("Unary() response = %q", completed.response.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("Unary() did not return after peer EOF")
	}
	if got := stream.cancelReads.Load(); got != 0 {
		t.Fatalf("CancelRead() calls = %d, want 0", got)
	}
	if !stream.sendClosed.Load() {
		t.Fatal("Unary() did not gracefully close the send direction")
	}
}

func TestTransportUnarySendsRequestAndMessageInOneWrite(t *testing.T) {
	stream := newUnaryTransportTestStream(encodeUnaryTestResponse(t))
	close(stream.eof)

	if _, err := callUnaryTestClient(context.Background(), stream); err != nil {
		t.Fatalf("Unary() error = %v", err)
	}
	if got := stream.writes.Load(); got != 1 {
		t.Fatalf("writes = %d, want one for REQUEST and MESSAGE together", got)
	}
	frames := readTestFrames(t, stream.written.Bytes(), DefaultMaxFrameSize)
	if len(frames) != 2 || frames[0].Kind != frameTypeRequest || frames[1].Kind != frameTypeMessage {
		t.Fatalf("request frames = %#v, want REQUEST then MESSAGE", frames)
	}
}

func TestTransportUnaryRejectsKnownFrameAfterStatus(t *testing.T) {
	response := append(encodeUnaryTestResponse(t), rawMessage([]byte("extra")).frame...)
	stream := newUnaryTransportTestStream(response)
	close(stream.eof)

	actual, err := callUnaryTestClient(context.Background(), stream)
	if actual != nil {
		t.Fatalf("Unary() response = %#v, want nil", actual)
	}
	if reason, _ := responseStreamFailureReason(err); reason != "trailing_frame" || StatusFromError(err).Code != CodeInternal {
		t.Fatalf("Unary() error = %v (reason %q), want Internal trailing_frame", err, reason)
	}
	if got := stream.cancelReads.Load(); got != 1 {
		t.Fatalf("CancelRead() calls = %d, want 1", got)
	}
}

func TestTransportUnarySkipsUnknownFramesAnywhere(t *testing.T) {
	grease := []byte{0x21, 0x03, 0xaa, 0xbb, 0xcc}
	message, err := encodeMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatal(err)
	}
	var response []byte
	response = append(response, grease...)
	response = append(response, message.frame...)
	response = append(response, grease...)
	response, err = appendStatusFrame(response, OK(), DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	response = append(response, 0x40, 0x40, 0x00)
	stream := newUnaryTransportTestStream(response)
	close(stream.eof)

	actual, err := callUnaryTestClient(context.Background(), stream)
	if err != nil || actual.Value != "response" {
		t.Fatalf("Unary() = %#v, %v, want the response", actual, err)
	}
}

func TestTransportUnaryCancellationWhileAwaitingPeerEOF(t *testing.T) {
	stream := newUnaryTransportTestStream(encodeUnaryTestResponse(t))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := callUnaryTestClient(ctx, stream)
		result <- err
	}()

	select {
	case <-stream.waitingForEOF:
	case <-time.After(time.Second):
		t.Fatal("Unary() did not reach peer EOF wait")
	}
	cancel()
	select {
	case err := <-result:
		// The server's STATUS arrived before the cancellation, so it decides
		// the outcome; cancelling only cuts the wait for the end of the stream.
		if err != nil {
			t.Fatalf("Unary() = %v, want the completed response", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Unary() did not return after context cancellation")
	}
	if got := stream.cancelReads.Load(); got == 0 {
		t.Fatal("Unary() did not cancel the receive direction")
	}
}

// streamingTestResponse starts a server-streaming call whose response is data.
func streamingTestResponse(t *testing.T, data []byte) (ServerStreamingCall[*testMessage], *unaryTransportTestStream) {
	t.Helper()
	stream := newUnaryTransportTestStream(data)
	close(stream.eof)
	client := newStreamClient(unaryTestOpener{stream: stream}, DefaultMaxFrameSize)
	call, err := ServerStreaming(context.Background(), client, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatal(err)
	}
	return call, stream
}

// TestTransportTakesTheAnswerBeforeHeaders checks that an ANSWER is not
// reported as the call's headers: Headers waits for the HEADERS behind it.
func TestTransportTakesTheAnswerBeforeHeaders(t *testing.T) {
	first, err := encodeMessage(&testMessage{Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := appendAnswerFrame(nil, &wireAnswer{ConnectionMetadata: 1}, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	response := concat(answer, headersBytes(t, Metadata{"x-h": []byte("1")}), first.frame, statusBytes(t, OK()))
	call, _ := streamingTestResponse(t, response)
	defer func() { _ = call.Close() }()
	if headers, err := call.Headers(); err != nil || string(headers["x-h"]) != "1" {
		t.Fatalf("Headers() = %v, %v", headers, err)
	}
	if message, err := call.Recv(); err != nil || message.Value != "one" {
		t.Fatalf("Recv() = %v, %v", message, err)
	}
	if _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() = %v, want EOF", err)
	}
}

func TestTransportServerStreamingSkipsUnknownFramesAndReadsHeaders(t *testing.T) {
	grease := greaseFrame(0x21, 4)
	first, err := encodeMessage(&testMessage{Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeMessage(&testMessage{Value: "two"})
	if err != nil {
		t.Fatal(err)
	}
	status := NewStatus(CodeOK, "").WithMetadata(Metadata{"x-t": []byte("2")})
	response := concat(grease, headersBytes(t, Metadata{"x-h": []byte("1")}), greaseFrame(0x40, 2), first.frame, grease,
		second.frame, grease, statusBytes(t, status), greaseFrame(0x1f*3+0x21, 0), greaseFrame(0x40, 9))
	call, _ := streamingTestResponse(t, response)
	defer func() { _ = call.Close() }()

	headers, err := call.Headers()
	if err != nil || string(headers["x-h"]) != "1" {
		t.Fatalf("Headers() = %v, %v", headers, err)
	}
	for _, want := range []string{"one", "two"} {
		if message, err := call.Recv(); err != nil || message.Value != want {
			t.Fatalf("Recv() = %v, %v, want %q", message, err, want)
		}
	}
	if _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() = %v, want EOF", err)
	}
	terminal, ok := call.TerminalStatus()
	if !ok || string(terminal.Metadata["x-t"]) != "2" {
		t.Fatalf("TerminalStatus() = %v, %t", terminal, ok)
	}
}

func TestTransportServerStreamingEndsMidFrame(t *testing.T) {
	first, err := encodeMessage(&testMessage{Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	for name, tail := range map[string][]byte{
		"mid-header":  {byte(frameTypeStatus)},
		"mid-length":  {0x00, 0x40},
		"mid-payload": {0x00, 0x04, 'a'},
		"mid-grease":  {0x21, 0x04, 0xee},
	} {
		t.Run(name, func(t *testing.T) {
			call, _ := streamingTestResponse(t, concat(first.frame, tail))
			defer func() { _ = call.Close() }()
			if message, err := call.Recv(); err != nil || message.Value != "one" {
				t.Fatalf("Recv() = %v, %v", message, err)
			}
			_, err := call.Recv()
			if err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("Recv() = %v, want a failure for a stream that ends inside a frame", err)
			}
		})
	}
}

func TestTransportServerStreamingEndsBeforeStatus(t *testing.T) {
	first, err := encodeMessage(&testMessage{Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	call, _ := streamingTestResponse(t, first.frame)
	defer func() { _ = call.Close() }()
	if _, err := call.Recv(); err != nil {
		t.Fatal(err)
	}
	_, err = call.Recv()
	if reason, _ := responseStreamFailureReason(err); reason != "missing_terminal_status" || StatusFromError(err).Code != CodeInternal {
		t.Fatalf("Recv() = %v (reason %q), want Internal missing_terminal_status", err, reason)
	}
}

func TestTransportRejectsMisplacedHeaders(t *testing.T) {
	first, err := encodeMessage(&testMessage{Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	headers := headersBytes(t, Metadata{"x-h": []byte("1")})
	answer, err := appendAnswerFrame(nil, &wireAnswer{}, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	for name, response := range map[string][]byte{
		"second HEADERS":        concat(headers, headers, first.frame, statusBytes(t, OK())),
		"HEADERS after MESSAGE": concat(first.frame, headers, statusBytes(t, OK())),
		"second ANSWER":         concat(answer, answer, first.frame, statusBytes(t, OK())),
		"ANSWER after HEADERS":  concat(headers, answer, first.frame, statusBytes(t, OK())),
		"ANSWER after MESSAGE":  concat(first.frame, answer, statusBytes(t, OK())),
		"REQUEST":               concat(testRequestBytes(t, requestFor("Echo", RPCKindUnary)), first.frame, statusBytes(t, OK())),
	} {
		t.Run(name, func(t *testing.T) {
			call, _ := streamingTestResponse(t, response)
			defer func() { _ = call.Close() }()
			var err error
			for err == nil {
				_, err = call.Recv()
			}
			if StatusFromError(err).Code != CodeInternal || errors.Is(err, io.EOF) {
				t.Fatalf("Recv() = %v, want Internal", err)
			}
		})
	}
}

func TestTransportRejectsOversizedResponseFrameBeforeReadingIt(t *testing.T) {
	stream := newUnaryTransportTestStream(appendFrameHeader(nil, frameTypeMessage, 1<<30))
	close(stream.eof)
	client := newStreamClient(unaryTestOpener{stream: stream}, 1024)
	allocated := measureAllocation(func() {
		_, err := Unary(context.Background(), client, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} })
		if StatusFromError(err).Code != CodeResourceExhausted {
			t.Fatalf("Unary() = %v, want ResourceExhausted", err)
		}
	})
	if allocated > 1<<20 {
		t.Fatalf("an oversized frame header made the client allocate %d bytes", allocated)
	}
}
