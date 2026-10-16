package trevrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// unclosableConn lets a net.Pipe stand in for one QUIC stream: Close finishes
// only the send direction, which the tests do not observe.
type unclosableConn struct{ net.Conn }

func (unclosableConn) Close() error { return nil }

type drainFixture struct {
	stream  *drainingStream
	client  net.Conn // the peer's request side
	stopped chan struct{}
}

func newDrainFixture(t *testing.T, shutdown context.Context) *drainFixture {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() { _ = serverEnd.Close(); _ = clientEnd.Close() })
	fixture := &drainFixture{client: clientEnd, stopped: make(chan struct{})}
	var once atomic.Bool
	fixture.stream = newDrainingStream(unclosableConn{serverEnd}, serverEnd.SetReadDeadline, func() {
		if once.CompareAndSwap(false, true) {
			close(fixture.stopped)
		}
	}, shutdown)
	return fixture
}

func (f *drainFixture) waitStopped(t *testing.T, within time.Duration) time.Duration {
	t.Helper()
	started := time.Now()
	select {
	case <-f.stopped:
		return time.Since(started)
	case <-time.After(within):
		t.Fatal("request side was not stopped")
		return 0
	}
}

func (f *drainFixture) expectNotStopped(t *testing.T, for_ time.Duration) {
	t.Helper()
	select {
	case <-f.stopped:
		t.Fatal("request side was stopped")
	case <-time.After(for_):
	}
}

func setDrainBounds(t *testing.T, timeout time.Duration, limit int64) {
	t.Helper()
	oldTimeout, oldLimit := requestDrainTimeout, requestDrainLimit
	requestDrainTimeout, requestDrainLimit = timeout, limit
	t.Cleanup(func() { requestDrainTimeout, requestDrainLimit = oldTimeout, oldLimit })
}

func TestDrainingStreamDoesNotStopReadingBeforeResponseIsFinished(t *testing.T) {
	setDrainBounds(t, time.Second, 1<<20)
	fixture := newDrainFixture(t, context.Background())

	readFailed := make(chan error, 1)
	go func() {
		_, err := fixture.stream.Read(make([]byte, 8))
		readFailed <- err
	}()
	time.Sleep(20 * time.Millisecond)

	// Aborting unblocks the handler's read but must not send STOP_SENDING:
	// the terminal status may not have been written yet.
	fixture.stream.trevrpcCancelRead()
	select {
	case err := <-readFailed:
		if !errors.Is(err, errRequestAborted) {
			t.Fatalf("pending read error = %v, want errRequestAborted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("abort did not unblock the pending read")
	}
	if _, err := fixture.stream.Read(make([]byte, 1)); !errors.Is(err, errRequestAborted) {
		t.Fatalf("read after abort = %v, want errRequestAborted", err)
	}
	fixture.expectNotStopped(t, 100*time.Millisecond)
}

func TestDrainingStreamDiscardsRequestUntilClientFinishes(t *testing.T) {
	setDrainBounds(t, time.Second, 1<<20)
	fixture := newDrainFixture(t, context.Background())
	fixture.stream.trevrpcCancelRead()
	if err := fixture.stream.Close(); err != nil {
		t.Fatal(err)
	}

	// The client may keep sending for a while and then finishes normally.
	chunk := make([]byte, 4096)
	for range 8 {
		if _, err := fixture.client.Write(chunk); err != nil {
			t.Fatalf("client write during drain: %v", err)
		}
	}
	_ = fixture.client.Close()
	fixture.expectNotStopped(t, 100*time.Millisecond)
}

func TestDrainingStreamStopsAfterByteBound(t *testing.T) {
	setDrainBounds(t, 10*time.Second, 64*1024)
	fixture := newDrainFixture(t, context.Background())
	fixture.stream.trevrpcCancelRead()
	_ = fixture.stream.Close()

	var sent atomic.Int64
	go func() {
		chunk := make([]byte, 4096)
		for {
			n, err := fixture.client.Write(chunk)
			sent.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	fixture.waitStopped(t, 5*time.Second)
	if got := sent.Load(); got < 64*1024 || got > 64*1024+64*1024 {
		t.Fatalf("client sent %d bytes before being stopped, want about the 64 KiB bound", got)
	}
}

func TestDrainingStreamStopsAfterTimeBound(t *testing.T) {
	setDrainBounds(t, 100*time.Millisecond, 1<<20)
	fixture := newDrainFixture(t, context.Background())
	fixture.stream.trevrpcCancelRead()
	_ = fixture.stream.Close()
	// The client stays silent and never finishes.
	elapsed := fixture.waitStopped(t, 5*time.Second)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("stopped after %v, before the time bound", elapsed)
	}
}

func TestDrainingStreamEndsAtServerShutdown(t *testing.T) {
	setDrainBounds(t, time.Hour, 1<<20)
	shutdown, stopServer := context.WithCancel(context.Background())
	fixture := newDrainFixture(t, shutdown)
	fixture.stream.trevrpcCancelRead()
	_ = fixture.stream.Close()
	fixture.expectNotStopped(t, 50*time.Millisecond)
	stopServer()
	fixture.waitStopped(t, 5*time.Second)
}

func TestDrainingStreamStartedAfterShutdownEndsImmediately(t *testing.T) {
	setDrainBounds(t, time.Hour, 1<<20)
	shutdown, stopServer := context.WithCancel(context.Background())
	stopServer()
	fixture := newDrainFixture(t, shutdown)
	_ = fixture.stream.Close()
	fixture.stream.trevrpcCancelRead()
	fixture.waitStopped(t, 5*time.Second)
}

func TestDrainingStreamSkipsDrainWhenRequestAlreadyEnded(t *testing.T) {
	setDrainBounds(t, 50*time.Millisecond, 1<<20)
	fixture := newDrainFixture(t, context.Background())
	_ = fixture.client.Close()
	if _, err := fixture.stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read = %v, want EOF", err)
	}
	fixture.stream.trevrpcCancelRead()
	_ = fixture.stream.Close()
	fixture.expectNotStopped(t, 150*time.Millisecond)
}

func TestDrainingStreamWithoutAbortNeverDrains(t *testing.T) {
	setDrainBounds(t, 50*time.Millisecond, 1<<20)
	fixture := newDrainFixture(t, context.Background())
	_ = fixture.stream.Close()
	fixture.expectNotStopped(t, 150*time.Millisecond)
}
