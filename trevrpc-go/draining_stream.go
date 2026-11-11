package trevrpc

import (
	"context"
	"errors"
	"io"
	"net/netip"
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
//
// The request side moves through requestState. Only a true end of it, EOF or a
// reset by the peer, leaves nothing to drain. A read that fails on a deadline,
// including the one the abort itself sets to interrupt it, does not end it.
type drainingStream struct {
	rw              io.ReadWriteCloser
	setReadDeadline func(time.Time) error
	stopReading     func()
	// datagrams is the demultiplexer of the connection's datagram messages, nil
	// for a connection without them.
	datagrams *datagramMux
	// connectionMetadata is the connection metadata the server keeps for the
	// stream's connection or session.
	connectionMetadata *connectionMetadataStore
	// remoteAddr is the client address of the stream's connection or session.
	remoteAddr netip.AddrPort
	// shutdown ends a running drain early.
	shutdown     context.Context
	drainTimeout time.Duration
	drainLimit   int64

	mu         sync.Mutex
	idle       *sync.Cond // signalled when readers drops to zero; set once a drain waits
	readers    int
	request    requestState
	sendClosed bool // the response is finished
}

// requestState is the lifecycle of a stream's request side:
// open -> aborted -> draining, or open | aborted -> ended.
type requestState uint8

const (
	// requestOpen: handlers read the stream.
	requestOpen requestState = iota
	// requestEnded: the stream reached EOF or the peer reset it, so there is
	// nothing to drain. Reads keep returning that outcome, even after an abort.
	requestEnded
	// requestAborted: handlers may no longer read and pending reads were
	// interrupted. The drain starts once the response is finished.
	requestAborted
	// requestDraining: the drain is the only reader. It ends when the peer
	// finishes the request side or with STOP_SENDING.
	requestDraining
)

// handlersMayRead reports whether handler reads still reach the stream.
func (s requestState) handlersMayRead() bool { return s < requestAborted }

// endsRequestSide reports whether a failed read means the request side is over
// for good. A read deadline only interrupts one Read and says nothing about
// what the peer will send.
func endsRequestSide(err error) bool { return !isTimeoutError(err) }

func newDrainingStream(shutdown context.Context, rw io.ReadWriteCloser, setReadDeadline func(time.Time) error, stopReading func()) *drainingStream {
	return &drainingStream{
		rw: rw, setReadDeadline: setReadDeadline, stopReading: stopReading, shutdown: shutdown,
		drainTimeout: requestDrainTimeout, drainLimit: requestDrainLimit,
	}
}

func (s *drainingStream) trevrpcDatagrams() *datagramMux { return s.datagrams }

func (s *drainingStream) trevrpcConnectionMetadata() *connectionMetadataStore {
	return s.connectionMetadata
}

func (s *drainingStream) trevrpcRemoteAddr() netip.AddrPort { return s.remoteAddr }

func (s *drainingStream) Read(data []byte) (int, error) {
	s.mu.Lock()
	if !s.request.handlersMayRead() {
		s.mu.Unlock()
		return 0, errRequestAborted
	}
	s.readers++
	s.mu.Unlock()

	read, err := s.rw.Read(data)

	s.mu.Lock()
	s.readers--
	if err != nil {
		ended := endsRequestSide(err)
		if s.request == requestAborted && !errors.Is(err, io.EOF) {
			err = errRequestAborted
		}
		if ended && (s.request == requestOpen || s.request == requestAborted) {
			s.request = requestEnded
		}
	}
	if s.readers == 0 && s.idle != nil {
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

// SetReadDeadline is ignored once the request side is aborted: the abort's
// deadline and then the drain's own must stand. It is set under the lock so a
// handler's deadline cannot overwrite the abort's.
func (s *drainingStream) SetReadDeadline(deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.request.handlersMayRead() {
		return nil
	}
	return s.setReadDeadline(deadline)
}

// trevrpcCancelRead aborts reading: pending and future handler reads fail, but
// the peer is not told to stop sending until the drain bound is hit.
func (s *drainingStream) trevrpcCancelRead() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.request == requestOpen {
		s.request = requestAborted
		// Unblock a pending Read without sending STOP_SENDING. Held under the
		// lock, this precedes the drain, which sets its own deadline.
		_ = s.setReadDeadline(time.Now())
	}
	s.maybeDrainLocked()
}

func (s *drainingStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return watchContextCancellation(ctx, s.trevrpcCancelRead)
}

// maybeDrainLocked starts the drain once the response is finished and the
// request side was aborted before it ended.
func (s *drainingStream) maybeDrainLocked() {
	if s.request != requestAborted || !s.sendClosed {
		return
	}
	s.request = requestDraining
	go s.drain()
}

func (s *drainingStream) drain() {
	// Wait until the handler's reads, unblocked by the abort, have returned so
	// the drain is the only reader.
	s.mu.Lock()
	if s.idle == nil {
		s.idle = sync.NewCond(&s.mu)
	}
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
