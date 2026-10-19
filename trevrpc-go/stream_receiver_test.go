package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

// goroutineID returns the id of the calling goroutine.
func goroutineID() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	fields := bytes.Fields(buf)
	return string(fields[1])
}

// blockingTestStream yields n values, each from the goroutine that calls
// Recv, and records which goroutines those were. It reports blocking, so it
// is read through the receiver's goroutine.
type blockingTestStream struct {
	mu         sync.Mutex
	left       int
	goroutines map[string]struct{}
}

func (s *blockingTestStream) Recv() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.goroutines == nil {
		s.goroutines = map[string]struct{}{}
	}
	s.goroutines[goroutineID()] = struct{}{}
	if s.left == 0 {
		return 0, io.EOF
	}
	s.left--
	return s.left, nil
}

func (*blockingTestStream) Close() error { return nil }

func TestStreamReceiverReadsEveryMessageOnOneGoroutine(t *testing.T) {
	stream := &blockingTestStream{left: 100}
	receiver := newStreamReceiver(context.Background(), MessageStream[int](stream), time.Minute, "response", nil)
	defer receiver.close()
	for want := 99; want >= 0; want-- {
		got, err := receiver.recv()
		if err != nil || got != want {
			t.Fatalf("recv = %d, %v, want %d", got, err, want)
		}
	}
	if _, err := receiver.recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("recv at the end = %v, want EOF", err)
	}
	if got := len(stream.goroutines); got != 1 {
		t.Fatalf("messages were received on %d goroutines, want 1", got)
	}
	if _, here := stream.goroutines[goroutineID()]; here {
		t.Fatal("a stream that can block was received on the calling goroutine")
	}
}

func TestStreamReceiverGivesUpOnIdleStreamAndKeepsAdmission(t *testing.T) {
	server := NewServer()
	runtime := server.freeze()
	lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
	if !ok {
		t.Fatal("failed to acquire request lease")
	}
	defer lease.release()
	release := make(chan struct{})
	receiver := newStreamReceiver(context.Background(), MessageStream[struct{}](&controlledStream[struct{}]{release: release}), time.Millisecond, "request", lease)
	defer receiver.close()

	_, err := receiver.recv()
	if status := StatusFromError(err); status.Code != CodeUnavailable {
		t.Fatalf("recv error = %v, want unavailable", err)
	}
	if again := func() error { _, err := receiver.recv(); return err }(); StatusFromError(again).Code != CodeUnavailable {
		t.Fatalf("recv after giving up = %v, want the same status", again)
	}
	if got := runtime.active.Load(); got != 1 {
		t.Fatalf("active executions = %d, want the abandoned receive", got)
	}
	close(release)
	waitForServerExecutionCount(t, server, 0)
}

func TestStreamReceiverCloseEndsPendingRecv(t *testing.T) {
	receiver := newStreamReceiver(context.Background(), MessageStream[struct{}](pendingStream[struct{}]{}), time.Minute, "response", nil)
	result := make(chan error, 1)
	go func() {
		_, err := receiver.recv()
		result <- err
	}()
	time.Sleep(5 * time.Millisecond)
	receiver.close()
	receiver.close()
	select {
	case err := <-result:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("recv after close = %v, want EOF", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("close did not end the pending recv")
	}
}

type recvPanicStream struct{}

func (recvPanicStream) Recv() (struct{}, error) { panic("stream failed") }
func (recvPanicStream) Close() error            { return nil }

func TestStreamReceiverReportsStreamPanic(t *testing.T) {
	receiver := newStreamReceiver(context.Background(), MessageStream[struct{}](recvPanicStream{}), time.Minute, "request", nil)
	defer receiver.close()
	for range 2 {
		_, err := receiver.recv()
		panicErr, ok := errors.AsType[*serverPanicError](err)
		if !ok || panicErr.phase != ServerDiagnosticRequestStreamPanic {
			t.Fatalf("recv error = %v, want a request stream panic", err)
		}
	}
}

// TestStreamReceiverNeverLeaksAnExecution races the end of the call against a
// receive: whether the goroutine took the request or the call ended first,
// the execution that stands for the receive must end.
func TestStreamReceiverNeverLeaksAnExecution(t *testing.T) {
	server := NewServer()
	runtime := server.freeze()
	for range 3000 {
		lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		ctx, cancel := context.WithCancel(context.Background())
		receiver := newStreamReceiver(ctx, MessageStream[int](&blockingTestStream{left: 1}), 0, "request", lease)
		go cancel()
		_, _ = receiver.recv()
		receiver.close()
		lease.release()
		cancel()
	}
	waitForServerExecutionCount(t, server, 0)
	if got := runtime.detached.Load(); got != 0 {
		t.Fatalf("detached executions = %d, want 0", got)
	}
	// Every lease was released, so the whole request limit is free again.
	for range cap(runtime.requestLimit) {
		if !tryAcquire(runtime.requestLimit) {
			t.Fatal("a request slot was left held by a receive")
		}
	}
}
