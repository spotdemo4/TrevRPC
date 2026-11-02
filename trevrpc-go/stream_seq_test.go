package trevrpc

import (
	"context"
	"errors"
	"io"
	"iter"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// countingSeq yields 0 to n-1, then err if it is set, and counts how often its
// deferred cleanup runs.
func countingSeq(n int, err error, cleanups *atomic.Int64) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		defer cleanups.Add(1)
		for i := range n {
			if !yield(i, nil) {
				return
			}
		}
		if err != nil {
			yield(0, err)
		}
	}
}

func TestStreamFromSeqYieldsUntilTheSeqReturns(t *testing.T) {
	var cleanups atomic.Int64
	stream := StreamFromSeq(countingSeq(3, nil, &cleanups))
	for want := range 3 {
		if got, err := stream.Recv(); err != nil || got != want {
			t.Fatalf("Recv = %d, %v; want %d", got, err, want)
		}
	}
	for range 2 {
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv after the end = %v, want io.EOF", err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want once", got)
	}
}

func TestStreamFromSeqReturnsTheYieldedError(t *testing.T) {
	var cleanups atomic.Int64
	failure := Aborted("rows failed")
	stream := StreamFromSeq(countingSeq(1, failure, &cleanups))
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, failure) {
		t.Fatalf("Recv = %v, want the yielded error", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want once", got)
	}
}

func TestStreamFromSeqCloseStopsTheSeq(t *testing.T) {
	var cleanups atomic.Int64
	unstarted := StreamFromSeq(countingSeq(3, nil, &cleanups))
	if err := unstarted.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := unstarted.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Close = %v, want io.EOF", err)
	}
	// An unstarted seq never runs, so it has nothing to clean up.
	if got := cleanups.Load(); got != 0 {
		t.Fatalf("cleanup of an unstarted seq ran %d times", got)
	}

	started := StreamFromSeq(countingSeq(3, nil, &cleanups))
	if _, err := started.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	for range 2 {
		if err := started.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want once", got)
	}
	if _, err := started.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Close = %v, want io.EOF", err)
	}
}

func TestStreamFromSeqCloseDuringRecvStopsTheSeqWhenItYields(t *testing.T) {
	var cleanups atomic.Int64
	release := make(chan struct{})
	waiting := make(chan struct{})
	stream := StreamFromSeq(func(yield func(int, error) bool) {
		defer cleanups.Add(1)
		close(waiting)
		<-release
		for i := 0; yield(i, nil); i++ {
		}
	})

	received := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		received <- err
	}()
	<-waiting
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := cleanups.Load(); got != 0 {
		t.Fatalf("cleanup ran %d times while the seq was still running", got)
	}
	close(release)
	if err := <-received; err != nil {
		t.Fatalf("pending Recv = %v", err)
	}
	if got := cleanups.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want once", got)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Close = %v, want io.EOF", err)
	}
}

func TestStreamFromSeqPropagatesAPanic(t *testing.T) {
	stream := StreamFromSeq(func(func(int, error) bool) { panic("seq bug") })
	func() {
		defer func() {
			if recovered := recover(); recovered != "seq bug" {
				t.Fatalf("recovered %v, want the seq's panic", recovered)
			}
		}()
		_, _ = stream.Recv()
	}()
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after a panic = %v, want io.EOF", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStreamFromSeqHandlerCleansUpWhenTheClientStopsReading(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	cleaned := make(chan struct{})
	RegisterServerStreaming(server, "/example.Rows/List", newTestMessage, func(ctx context.Context, _ *testMessage) (MessageStream[*testMessage], error) {
		return StreamFromSeq(func(yield func(*testMessage, error) bool) {
			defer close(cleaned)
			for i := 0; ctx.Err() == nil; i++ {
				if !yield(&testMessage{Value: strconv.Itoa(i)}, nil) {
					return
				}
			}
		}), nil
	})
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server})
	transport := dialNative(t, pki, listener)

	ctx, cancel := context.WithTimeout(t.Context(), rpcTestTimeout)
	defer cancel()
	stream, err := ServerStreaming(ctx, transport, "/example.Rows/List", &testMessage{}, newTestMessage)
	if err != nil {
		t.Fatalf("server streaming: %v", err)
	}
	for want := range 3 {
		reply, err := stream.Recv()
		if err != nil || reply.Value != strconv.Itoa(want) {
			t.Fatalf("Recv = %v, %v; want %d", reply, err, want)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-cleaned:
	case <-time.After(rpcTestTimeout):
		t.Fatal("the seq's cleanup did not run after the client closed the stream")
	}
}
