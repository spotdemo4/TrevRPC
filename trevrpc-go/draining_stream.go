package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// requestDrainTimeout and requestDrainLimit bound how long and how much of a
// client's unread request side is discarded after the server finished its
// response. They are variables only so tests can shorten them.
var (
	requestDrainTimeout = time.Second
	requestDrainLimit   = int64(1 << 20)
)

var errRequestAborted = errors.New("request side aborted")

// drainingStream is the server side of a native QUIC or WebTransport stream.
//
// Aborting the receive direction (a cancelled context, an idle timeout, or an
// error status) sends STOP_SENDING. Some peers, notably Firefox, then tear down
// the whole bidirectional stream, including a terminal status the server has
// already written but the client has not yet read. So once the response is
// finished the request side is not stopped at once: it is read and discarded
// until the client finishes or resets it, for at most requestDrainTimeout and
// requestDrainLimit bytes. Only when a bound is hit is STOP_SENDING sent.
//
// An abort request first only fails and unblocks the handler's reads; the stop
// is deferred to Close, because the abort usually precedes the final write.
type drainingStream struct {
	rw              io.ReadWriteCloser
	setReadDeadline func(time.Time) error
	stopReading     func()
	// shutdown ends a running drain early.
	shutdown     context.Context
	drainTimeout time.Duration
	drainLimit   int64

	mu         sync.Mutex
	idle       *sync.Cond // signalled when readers drops to zero
	readers    int
	aborted    bool // handlers may no longer read
	unblocked  bool // pending reads were interrupted; a drain may take over
	sendClosed bool
	readEnded  bool // the request side reached EOF or failed: nothing to drain
	draining   bool
}

func newDrainingStream(shutdown context.Context, rw io.ReadWriteCloser, setReadDeadline func(time.Time) error, stopReading func()) *drainingStream {
	s := &drainingStream{
		rw: rw, setReadDeadline: setReadDeadline, stopReading: stopReading, shutdown: shutdown,
		drainTimeout: requestDrainTimeout, drainLimit: requestDrainLimit,
	}
	s.idle = sync.NewCond(&s.mu)
	return s
}

func (s *drainingStream) Read(data []byte) (int, error) {
	s.mu.Lock()
	if s.aborted {
		s.mu.Unlock()
		return 0, errRequestAborted
	}
	s.readers++
	s.mu.Unlock()

	read, err := s.rw.Read(data)

	s.mu.Lock()
	s.readers--
	if err != nil {
		s.readEnded = true
		if s.aborted && !errors.Is(err, io.EOF) {
			err = errRequestAborted
		}
	}
	if s.readers == 0 {
		s.idle.Broadcast()
	}
	s.mu.Unlock()
	return read, err
}

func (s *drainingStream) Write(data []byte) (int, error) { return s.rw.Write(data) }

// Close finishes the send direction and, if the request side was aborted and
// is still open, starts draining it.
func (s *drainingStream) Close() error {
	err := s.rw.Close()
	s.mu.Lock()
	s.sendClosed = true
	s.maybeDrainLocked()
	s.mu.Unlock()
	return err
}

func (s *drainingStream) SetReadDeadline(deadline time.Time) error {
	s.mu.Lock()
	aborted := s.aborted
	s.mu.Unlock()
	if aborted {
		return nil
	}
	return s.setReadDeadline(deadline)
}

// trevrpcCancelRead aborts reading: pending and future handler reads fail, but
// the peer is not told to stop sending until the drain bound is hit.
func (s *drainingStream) trevrpcCancelRead() {
	s.mu.Lock()
	first := !s.aborted
	s.aborted = true
	s.mu.Unlock()
	if first {
		// Unblock a pending Read without sending STOP_SENDING. This must
		// precede the drain, which sets its own deadline.
		_ = s.setReadDeadline(time.Now())
		s.mu.Lock()
		s.unblocked = true
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.maybeDrainLocked()
	s.mu.Unlock()
}

func (s *drainingStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return watchContextCancellation(ctx, s.trevrpcCancelRead)
}

// maybeDrainLocked starts the drain once the response is finished and the
// request side was aborted while still open.
func (s *drainingStream) maybeDrainLocked() {
	if s.draining || !s.unblocked || !s.sendClosed || s.readEnded {
		return
	}
	s.draining = true
	go s.drain()
}

func (s *drainingStream) drain() {
	// Wait until the handler's reads, unblocked by the abort, have returned so
	// the drain is the only reader.
	s.mu.Lock()
	for s.readers > 0 {
		s.idle.Wait()
	}
	s.mu.Unlock()

	_ = s.setReadDeadline(time.Now().Add(s.drainTimeout))
	// Registered after the deadline so an already-closed shutdown wins.
	stop := context.AfterFunc(s.shutdown, func() { _ = s.setReadDeadline(time.Now()) })
	defer stop()

	// Reading one byte past the limit tells a client that sent exactly the
	// limit and finished apart from one that keeps sending.
	discarded, err := io.Copy(io.Discard, io.LimitReader(s.rw, s.drainLimit+1))
	if err == nil && discarded <= s.drainLimit {
		return // the client finished its request side
	}
	s.stopReading()
}
