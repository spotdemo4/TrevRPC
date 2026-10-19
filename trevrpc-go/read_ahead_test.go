package trevrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// aheadStream is a response stream the server must read through its read-ahead
// goroutine: it is not marked non-blocking. It yields total messages of size
// bytes (forever if total is negative), each carrying its index in its first
// eight bytes, and blocks from message blockAt on until it is released or
// closed.
type aheadStream struct {
	total, size, blockAt int
	release              chan struct{}

	recvs      atomic.Int64
	closes     atomic.Int64
	inFlight   atomic.Int32
	overlapped atomic.Bool
	closed     chan struct{}
	closeOnce  sync.Once
}

func newAheadStream(total, size int) *aheadStream {
	return &aheadStream{total: total, size: size, blockAt: -1, release: make(chan struct{}), closed: make(chan struct{})}
}

// Recv counts the calls, which include the one that returns io.EOF.
func (s *aheadStream) Recv() (encodedMessage, error) {
	if s.inFlight.Add(1) != 1 {
		s.overlapped.Store(true)
	}
	defer s.inFlight.Add(-1)
	index := int(s.recvs.Add(1)) - 1
	if s.total >= 0 && index >= s.total {
		return encodedMessage{}, io.EOF
	}
	if s.blockAt >= 0 && index >= s.blockAt {
		select {
		case <-s.release:
		case <-s.closed:
			return encodedMessage{}, io.EOF
		}
	}
	body := make([]byte, max(s.size, 8))
	binary.BigEndian.PutUint64(body, uint64(index))
	return rawMessage(body), nil
}

func (s *aheadStream) Close() error {
	s.closes.Add(1)
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

// recvCount is the number of messages the handler produced (not counting the
// call that ended the stream).
func (s *aheadStream) recvCount() int { return int(s.recvs.Load()) }

// waitForRecvs waits for the handler to have been read want times, and then,
// to see that it does not go on, for a moment longer. It returns how many
// times the handler was read.
func (s *aheadStream) waitForRecvs(t *testing.T, want int) int {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for s.recvCount() < want {
		if time.Now().After(deadline) {
			t.Fatalf("the handler was read %d times, want %d", s.recvCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	return s.recvCount()
}

// gatedWriter records the writes of a response and blocks the first one until
// it is released, so the response is stuck writing while the handler goes on.
type gatedWriter struct {
	mu      sync.Mutex
	writes  [][]byte
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newGatedWriter() *gatedWriter {
	return &gatedWriter{gate: make(chan struct{}), entered: make(chan struct{})}
}

func (w *gatedWriter) Write(data []byte) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.gate
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, bytes.Clone(data))
	return len(data), nil
}

func (w *gatedWriter) written() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.writes...)
}

func newAheadResponse(ctx context.Context, t *testing.T, stream encodedStream, headers Metadata, configure func(*ServerOptions)) (*Server, *serverResponseStream) {
	t.Helper()
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.MaxStreamBodySize = -1
	if configure != nil {
		configure(&options)
	}
	server.SetOptions(options)
	server.routeStreaming("/example.Greeter/Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (streamResponse, error) {
		return streamResponse{stream: stream, headers: headers}, nil
	})
	request := newWireRequest("/example.Greeter/Download")
	request.Kind = RPCKindServerStreaming
	response, ok := server.handleStreamingWriter(ctx, request, nil, newEmptyStream[[]byte]()).(*serverResponseStream)
	if !ok {
		t.Fatal("the call did not produce a streaming response")
	}
	t.Cleanup(func() { discardMessageStream(response) })
	// Whatever the machine does to the speed of the test handler, it is read
	// ahead. The test of the adaptive rule sets its own.
	response.readAhead.fastRecv = time.Hour
	return server, response
}

// writeAll drives the response until it is done and returns every write.
func writeAll(t *testing.T, response *serverResponseStream) [][]byte {
	t.Helper()
	writer := &recordingWriter{}
	for {
		done, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
		if err != nil {
			t.Fatalf("writeNext: %v", err)
		}
		if done {
			return writer.writes
		}
	}
}

type recordingWriter struct{ writes [][]byte }

func (w *recordingWriter) Write(data []byte) (int, error) {
	w.writes = append(w.writes, bytes.Clone(data))
	return len(data), nil
}

// messageIndexes decodes the frames of writes into the indexes of the messages
// and the final STATUS code.
func messageIndexes(t *testing.T, writes [][]byte) ([]int, Code) {
	t.Helper()
	var indexes []int
	code := CodeUnknown
	for _, data := range writes {
		for _, frame := range readTestFrames(t, data, DefaultMaxFrameSize) {
			switch frame.Kind {
			case frameTypeMessage:
				indexes = append(indexes, int(binary.BigEndian.Uint64(frame.Body)))
			case frameTypeStatus:
				code = CodeFromUint32(frame.Status)
			}
		}
	}
	return indexes, code
}

func TestReadAheadSendsQueuedMessagesInOneWrite(t *testing.T) {
	stream := newAheadStream(10, 16)
	_, response := newAheadResponse(context.Background(), t, stream, nil, nil)
	response.readAhead.start()
	// The stream, EOF included, is read before the response is written.
	stream.waitForRecvs(t, 11)

	writes := writeAll(t, response)
	if len(writes) != 1 {
		t.Fatalf("the response took %d writes, want the queued messages and the status in one", len(writes))
	}
	indexes, code := messageIndexes(t, writes)
	if len(indexes) != 10 || code != CodeOK {
		t.Fatalf("messages = %v, code = %v, want 10 messages and OK", indexes, code)
	}
	for want, got := range indexes {
		if got != want {
			t.Fatalf("messages = %v, want them in order", indexes)
		}
	}
}

func TestReadAheadWritesAMessageAsSoonAsItIsReady(t *testing.T) {
	stream := newAheadStream(-1, 16)
	stream.blockAt = 1
	_, response := newAheadResponse(context.Background(), t, stream, Metadata{"h": []byte("v")}, nil)
	writer := &countingWriter{}

	// HEADERS go out first, in a write of their own.
	type result struct {
		done bool
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		done, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
		finished <- result{done, err}
	}()
	select {
	case got := <-finished:
		if got.err != nil || got.done {
			t.Fatalf("writeNext = %t, %v", got.done, got.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("the lone message was not written while the handler is blocked on the next one")
	}
	frames := readTestFrames(t, writer.Bytes(), DefaultMaxFrameSize)
	if writer.writeCount != 2 || len(frames) != 2 || frames[0].Kind != frameTypeHeaders || frames[1].Kind != frameTypeMessage {
		t.Fatalf("got %d writes of %#v, want HEADERS and then the message, each in its own write", writer.writeCount, frames)
	}
	close(stream.release)
}

func TestReadAheadIsBoundedByCount(t *testing.T) {
	stream := newAheadStream(100, 16)
	_, response := newAheadResponse(context.Background(), t, stream, Metadata{"h": []byte("v")}, nil)
	writer := newGatedWriter()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		// The HEADERS write blocks while the handler is read.
		_, _ = response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
	}()
	<-writer.entered

	if got := stream.waitForRecvs(t, handlerReadAheadMessages); got != handlerReadAheadMessages {
		t.Fatalf("the handler was read %d times with the writer blocked, want %d", got, handlerReadAheadMessages)
	}
	close(writer.gate)
	<-finished

	writes := append(writer.written(), writeAll(t, response)...)
	indexes, code := messageIndexes(t, writes)
	if len(indexes) != 100 || code != CodeOK {
		t.Fatalf("got %d messages and %v, want 100 and OK", len(indexes), code)
	}
	for want, got := range indexes {
		if got != want {
			t.Fatalf("message %d has index %d", want, got)
		}
	}
	for _, write := range writes {
		count := 0
		for _, frame := range readTestFrames(t, write, DefaultMaxFrameSize) {
			if frame.Kind == frameTypeMessage {
				count++
			}
		}
		if count > handlerReadAheadMessages {
			t.Fatalf("a write carried %d messages, want at most %d", count, handlerReadAheadMessages)
		}
	}
	if stream.overlapped.Load() {
		t.Fatal("the handler was read concurrently")
	}
}

func TestReadAheadIsBoundedByBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		want int
	}{
		// The queue is full once it holds 64 KiB.
		{name: "exact", size: handlerReadAheadBytes / 4, want: 4},
		// A message that crosses the limit is queued, and no more.
		{name: "crossing", size: 30000, want: 3},
		{name: "one large message", size: 3 * handlerReadAheadBytes, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := newAheadStream(-1, tc.size)
			_, response := newAheadResponse(context.Background(), t, stream, Metadata{"h": []byte("v")}, nil)
			writer := newGatedWriter()
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				_, _ = response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
			}()
			<-writer.entered
			got := stream.waitForRecvs(t, tc.want)
			close(writer.gate)
			<-finished
			if got != tc.want {
				t.Fatalf("the handler was read %d times with the writer blocked, want %d", got, tc.want)
			}
		})
	}
}

func TestReadAheadCancelledWithAFullQueueLeaksNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		// blocked makes the handler wait in Recv, as one does that has
		// nothing more to say, instead of producing without end.
		blocked bool
	}{{name: "full queue"}, {name: "handler blocked in Recv", blocked: true}} {
		t.Run(tc.name, func(t *testing.T) {
			before := runtime.NumGoroutine()
			stream := newAheadStream(-1, 16)
			if tc.blocked {
				stream.blockAt = 4
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server, response := newAheadResponse(ctx, t, stream, Metadata{"h": []byte("v")}, nil)
			writer := newGatedWriter()
			finished := make(chan [2]any, 1)
			go func() {
				done, err := response.writeNext(ctx, writer, DefaultMaxFrameSize, nil)
				finished <- [2]any{done, err}
			}()
			<-writer.entered
			want := handlerReadAheadMessages
			if tc.blocked {
				want = 5
			}
			stream.waitForRecvs(t, want)

			cancel()
			close(writer.gate)
			<-finished
			// handleRPCStream closes the response once the write failed.
			discardMessageStream(response)

			if stream.closes.Load() == 0 {
				t.Fatal("the handler's stream was not closed")
			}
			waitForServerExecutionCount(t, server, 0)
			waitForAtomicCount(t, &server.freeze().detached, 0, "detached executions")
			if got := len(server.freeze().requestLimit); got != 0 {
				t.Fatalf("%d request slots are still held", got)
			}
			deadline := time.Now().Add(testTimeout)
			for runtime.NumGoroutine() > before {
				if time.Now().After(deadline) {
					t.Fatalf("goroutines = %d, want at most %d", runtime.NumGoroutine(), before)
				}
				time.Sleep(time.Millisecond)
			}
			if stream.overlapped.Load() {
				t.Fatal("the handler was read concurrently")
			}
		})
	}
}

func TestReadAheadCancelledCallDropsQueuedMessages(t *testing.T) {
	stream := newAheadStream(-1, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, response := newAheadResponse(ctx, t, stream, Metadata{"h": []byte("v")}, nil)
	writer := newGatedWriter()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = response.writeNext(ctx, writer, DefaultMaxFrameSize, nil)
	}()
	<-writer.entered
	stream.waitForRecvs(t, handlerReadAheadMessages)

	cancel()
	close(writer.gate)
	<-finished

	// The messages that were queued are not sent: the call ended.
	writes := writer.written()
	indexes, _ := messageIndexes(t, writes)
	if len(indexes) != 0 {
		t.Fatalf("messages %v were written after the call was cancelled", indexes)
	}
	if _, err := response.writeNext(context.Background(), &recordingWriter{}, DefaultMaxFrameSize, nil); err != nil {
		t.Fatalf("writeNext after cancellation: %v", err)
	}
}

func TestReadAheadStillEnforcesTheStreamLimits(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*ServerOptions)
		size      int
		messages  int
	}{
		{name: "message count", configure: func(options *ServerOptions) { options.MaxStreamMessages = 5 }, size: 16, messages: 5},
		{name: "body size", configure: func(options *ServerOptions) { options.MaxStreamBodySize = 3*16 + 8 }, size: 16, messages: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := newAheadStream(-1, tc.size)
			server, response := newAheadResponse(context.Background(), t, stream, nil, tc.configure)
			response.readAhead.start()
			// The handler is far ahead of the limit by the time it is checked.
			stream.waitForRecvs(t, handlerReadAheadMessages)

			writes := writeAll(t, response)
			indexes, code := messageIndexes(t, writes)
			if len(indexes) != tc.messages || code != CodeResourceExhausted {
				t.Fatalf("got %d messages and %v, want %d and ResourceExhausted", len(indexes), code, tc.messages)
			}
			if stream.closes.Load() == 0 {
				t.Fatal("the handler's stream was not closed after the limit")
			}
			waitForServerExecutionCount(t, server, 0)
			if got := len(server.freeze().requestLimit); got != 0 {
				t.Fatalf("%d request slots are still held", got)
			}
		})
	}
}

func TestReadAheadStillEnforcesTheIdleTimeout(t *testing.T) {
	stream := newAheadStream(-1, 16)
	stream.blockAt = 2
	server, response := newAheadResponse(context.Background(), t, stream, nil, func(options *ServerOptions) {
		options.StreamIdleTimeout = 50 * time.Millisecond
	})

	writes := writeAll(t, response)
	indexes, code := messageIndexes(t, writes)
	if len(indexes) != 2 || code != CodeUnavailable {
		t.Fatalf("got %v and %v, want the two messages and Unavailable", indexes, code)
	}
	// The Recv that gave up is detached, and ends once the stream is closed.
	waitForServerExecutionCount(t, server, 0)
	waitForAtomicCount(t, &server.freeze().detached, 0, "detached executions")
}

func TestReadAheadIdleTimeoutMeasuresTheHandlerNotTheWriter(t *testing.T) {
	stream := newAheadStream(20, 16)
	_, response := newAheadResponse(context.Background(), t, stream, Metadata{"h": []byte("v")}, func(options *ServerOptions) {
		options.StreamIdleTimeout = 100 * time.Millisecond
	})
	writer := newGatedWriter()
	finished := make(chan error, 1)
	go func() {
		_, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
		finished <- err
	}()
	<-writer.entered
	// The writer is stuck for longer than the idle timeout, while the handler
	// has been quick all along.
	time.Sleep(250 * time.Millisecond)
	close(writer.gate)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}

	writes := append(writer.written(), writeAll(t, response)...)
	indexes, code := messageIndexes(t, writes)
	if len(indexes) != 20 || code != CodeOK {
		t.Fatalf("got %d messages and %v, want 20 and OK", len(indexes), code)
	}
}

func TestReadAheadReportsTheHandlersFailureAfterItsMessages(t *testing.T) {
	failing := &failAfterStream{aheadStream: newAheadStream(-1, 16), after: 6, err: NewStatus(CodeAborted, "stop")}
	_, call := newAheadResponse(context.Background(), t, failing, nil, nil)
	call.readAhead.start()
	failing.waitForRecvs(t, 7)

	writes := writeAll(t, call)
	indexes, code := messageIndexes(t, writes)
	if len(writes) != 1 || len(indexes) != 6 || code != CodeAborted {
		t.Fatalf("got %d writes with %v and %v, want one with 6 messages and Aborted", len(writes), indexes, code)
	}
}

// failAfterStream fails after a number of messages.
type failAfterStream struct {
	*aheadStream
	after int
	err   error
}

func (s *failAfterStream) Recv() (encodedMessage, error) {
	if int(s.recvs.Load()) >= s.after {
		s.recvs.Add(1)
		return encodedMessage{}, s.err
	}
	return s.aheadStream.Recv()
}

func TestReadAheadNeverReadsTheHandlerConcurrently(t *testing.T) {
	stream := newAheadStream(2000, 16)
	_, response := newAheadResponse(context.Background(), t, stream, nil, nil)
	writer := &slowWriter{}
	for {
		done, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if stream.overlapped.Load() {
		t.Fatal("the handler was read concurrently")
	}
	indexes, code := messageIndexes(t, writer.writes)
	if len(indexes) != 2000 || code != CodeOK {
		t.Fatalf("got %d messages and %v, want 2000 and OK", len(indexes), code)
	}
	for want, got := range indexes {
		if got != want {
			t.Fatalf("message %d has index %d", want, got)
		}
	}
	if writer.most < 2 {
		t.Fatalf("no write carried more than %d messages, want batching", writer.most)
	}
}

// slowWriter keeps the writer behind the handler on every other write.
type slowWriter struct {
	recordingWriter
	most int
	n    int
}

func (w *slowWriter) Write(data []byte) (int, error) {
	w.n++
	if w.n%2 == 0 {
		time.Sleep(100 * time.Microsecond)
	}
	count := 0
	reader := newFrameReader(bytes.NewReader(data), DefaultMaxFrameSize, -1)
	for {
		frame, err := reader.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err == nil && frame.typ == frameTypeMessage {
			count++
		}
	}
	w.most = max(w.most, count)
	return w.recordingWriter.Write(data)
}

func TestReadAheadReusesTheWriteBuffer(t *testing.T) {
	stream := newAheadStream(-1, 64)
	_, response := newAheadResponse(context.Background(), t, stream, nil, nil)
	response.readAhead.start()
	stream.waitForRecvs(t, handlerReadAheadMessages)
	writer := &recordingWriter{}
	if _, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil); err != nil {
		t.Fatal(err)
	}
	first := cap(response.buffer)
	if first == 0 {
		t.Fatal("the joined write left no buffer to reuse")
	}
	stream.waitForRecvs(t, 2*handlerReadAheadMessages)
	if _, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil); err != nil {
		t.Fatal(err)
	}
	if got := cap(response.buffer); got != first {
		t.Fatalf("the write buffer was reallocated from %d to %d bytes", first, got)
	}
}

// TestReadAheadReadsASlowHandlerOnlyWhenAsked checks that a handler that
// waits for something on every message, as one does that answers its peer, is
// read when the writer wants a message and not before: reading it ahead gains
// nothing.
func TestReadAheadReadsASlowHandlerOnlyWhenAsked(t *testing.T) {
	stream := newAheadStream(-1, 16)
	stream.blockAt = 0
	_, response := newAheadResponse(context.Background(), t, stream, nil, nil)
	response.readAhead.fastRecv = 5 * time.Millisecond
	writer := &recordingWriter{}

	for round := range 5 {
		written := make(chan error, 1)
		go func() {
			_, err := response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
			written <- err
		}()
		// The handler takes longer than the speed that counts as fast.
		time.Sleep(10 * time.Millisecond)
		stream.release <- struct{}{}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		// The writer does not ask for another message for a while.
		time.Sleep(20 * time.Millisecond)
		// The first message is read before it is asked for, and the others are
		// read for the writer that asks.
		if got := stream.recvCount(); got != round+1 {
			t.Fatalf("after message %d the handler was read %d times, want %d", round, got, round+1)
		}
	}
	indexes, _ := messageIndexes(t, writer.writes)
	if len(indexes) != 5 {
		t.Fatalf("wrote %d messages, want 5", len(indexes))
	}
}

// TestReadAheadReadsAFastHandlerAhead checks that the default speed rule reads
// a handler that produces its messages at once ahead of the writer.
func TestReadAheadReadsAFastHandlerAhead(t *testing.T) {
	stream := newAheadStream(-1, 16)
	_, response := newAheadResponse(context.Background(), t, stream, Metadata{"h": []byte("v")}, nil)
	// Slow machines and the race detector stretch the handler, so the rule is
	// checked with room to spare.
	response.readAhead.fastRecv = 50 * time.Millisecond
	writer := newGatedWriter()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = response.writeNext(context.Background(), writer, DefaultMaxFrameSize, nil)
	}()
	<-writer.entered
	got := stream.waitForRecvs(t, handlerReadAheadMessages)
	close(writer.gate)
	<-finished
	if got != handlerReadAheadMessages {
		t.Fatalf("the handler was read %d times with the writer blocked, want %d", got, handlerReadAheadMessages)
	}
}
