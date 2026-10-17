package qmux

import (
	"context"
	"io"
	"os"
	"time"
)

// Stream is a bidirectional QMux stream. Read and Write may be used
// concurrently with each other and with the cancel and deadline methods.
type Stream struct {
	id      uint64
	session *Session
	ctx     context.Context
	cancel  context.CancelFunc

	// All fields below are guarded by session.mu.

	recvBuf       []byte
	recvHead      int
	received      uint64 // bytes received
	read          uint64 // bytes read or discarded
	recvMax       uint64 // flow control limit advertised to the peer
	recvWindow    uint64
	finalKnown    bool
	finalSize     uint64
	finReceived   bool
	resetReceived *StreamError
	readCancelled bool
	readCode      uint64
	readDeadline  time.Time
	readNotify    notifier

	sendBuf       []byte
	sendHead      int
	sent          uint64 // bytes framed
	sendMax       uint64 // flow control limit granted by the peer
	finRequested  bool
	finSent       bool
	resetSent     bool
	writeErr      error
	writeDeadline time.Time
	writeNotify   notifier
	inReady       bool
	removed       bool
}

func newStream(session *Session, id, sendMax, recvWindow uint64) *Stream {
	ctx, cancel := context.WithCancel(session.ctx)
	return &Stream{
		id:         id,
		session:    session,
		ctx:        ctx,
		cancel:     cancel,
		sendMax:    sendMax,
		recvMax:    recvWindow,
		recvWindow: recvWindow,
	}
}

// StreamID returns the QUIC stream ID.
func (st *Stream) StreamID() uint64 { return st.id }

// Context is cancelled when the sending direction ends: after Close or
// CancelWrite, when the peer sends STOP_SENDING, or when the session ends.
func (st *Stream) Context() context.Context { return st.ctx }

// Read reads stream data. It returns io.EOF after the peer's FIN, and a
// *StreamError after a reset or CancelRead.
func (st *Stream) Read(p []byte) (int, error) {
	s := st.session
	s.mu.Lock()
	for {
		if st.readCancelled {
			s.mu.Unlock()
			return 0, &StreamError{StreamID: st.id, ErrorCode: st.readCode}
		}
		if st.resetReceived != nil {
			err := st.resetReceived
			s.mu.Unlock()
			return 0, err
		}
		if pending := len(st.recvBuf) - st.recvHead; pending > 0 {
			n := copy(p, st.recvBuf[st.recvHead:])
			st.recvHead += n
			if st.recvHead == len(st.recvBuf) {
				st.recvBuf, st.recvHead = st.recvBuf[:0], 0
			}
			st.consumedLocked(uint64(n))
			s.mu.Unlock()
			return n, nil
		}
		if st.finalKnown && st.read == st.finalSize {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.closeErr != nil {
			err := s.closeErr
			s.mu.Unlock()
			return 0, err
		}
		if len(p) == 0 {
			s.mu.Unlock()
			return 0, nil
		}
		changed, deadline := st.readNotify.wait(), st.readDeadline
		s.mu.Unlock()
		if err := waitUntil(changed, deadline); err != nil {
			return 0, err
		}
		s.mu.Lock()
	}
}

// consumedLocked returns read credit to the peer.
func (st *Stream) consumedLocked(n uint64) {
	s := st.session
	st.read += n
	if !st.finalKnown && st.recvMax-st.read <= st.recvWindow/2 {
		st.recvMax = st.read + st.recvWindow
		s.queueControlLocked(appendMaxStreamData(nil, st.id, st.recvMax))
	}
	s.consumedLocked(n)
	st.maybeRemoveLocked()
}

// Write buffers p for sending, blocking while the stream's send buffer is
// full.
func (st *Stream) Write(p []byte) (int, error) {
	s := st.session
	written := 0
	s.mu.Lock()
	for {
		if st.writeErr != nil {
			err := st.writeErr
			s.mu.Unlock()
			return written, err
		}
		if s.closeErr != nil {
			err := s.closeErr
			s.mu.Unlock()
			return written, err
		}
		if written == len(p) {
			s.mu.Unlock()
			return written, nil
		}
		if space := s.sendBuffer - (len(st.sendBuf) - st.sendHead); space > 0 {
			if st.sendHead > 0 && cap(st.sendBuf)-len(st.sendBuf) < space {
				st.sendBuf = st.sendBuf[:copy(st.sendBuf, st.sendBuf[st.sendHead:])]
				st.sendHead = 0
			}
			n := min(space, len(p)-written)
			st.sendBuf = append(st.sendBuf, p[written:written+n]...)
			written += n
			st.markReadyLocked()
			continue
		}
		changed, deadline := st.writeNotify.wait(), st.writeDeadline
		s.mu.Unlock()
		if err := waitUntil(changed, deadline); err != nil {
			return written, err
		}
		s.mu.Lock()
	}
}

// Close ends the sending direction with a FIN once buffered data is sent.
func (st *Stream) Close() error {
	s := st.session
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.finRequested || st.resetSent {
		return nil
	}
	st.finRequested = true
	st.writeErr = errWriteClosed
	st.cancel()
	st.writeNotify.notify()
	st.markReadyLocked()
	return nil
}

var errWriteClosed = &closedError{"write on closed stream"}

type closedError struct{ message string }

func (e *closedError) Error() string { return e.message }

// CancelWrite abandons the sending direction with RESET_STREAM.
func (st *Stream) CancelWrite(code uint64) {
	s := st.session
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.sendDone() {
		return
	}
	st.writeErr = &StreamError{StreamID: st.id, ErrorCode: code}
	st.resetSendLocked(code)
}

// resetSendLocked discards unsent data and queues RESET_STREAM.
func (st *Stream) resetSendLocked(code uint64) {
	s := st.session
	st.sendBuf, st.sendHead = nil, 0
	st.resetSent = true
	s.queueControlLocked(appendResetStream(nil, st.id, code, st.sent))
	st.cancel()
	st.writeNotify.notify()
	st.maybeRemoveLocked()
}

// CancelRead abandons the receiving direction with STOP_SENDING. Data that
// still arrives is discarded.
func (st *Stream) CancelRead(code uint64) {
	s := st.session
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.readCancelled || st.resetReceived != nil || (st.finalKnown && st.read == st.finalSize) {
		return
	}
	st.readCancelled = true
	st.readCode = code
	if unread := st.received - st.read; unread > 0 {
		st.read = st.received
		st.recvBuf, st.recvHead = nil, 0
		s.consumedLocked(unread)
	}
	if !st.finalKnown {
		s.queueControlLocked(appendStopSending(nil, st.id, code))
	}
	st.readNotify.notify()
	st.maybeRemoveLocked()
}

// SetReadDeadline bounds Read. A zero time disables the deadline.
func (st *Stream) SetReadDeadline(t time.Time) error {
	s := st.session
	s.mu.Lock()
	st.readDeadline = t
	st.readNotify.notify()
	s.mu.Unlock()
	return nil
}

// SetWriteDeadline bounds Write. A zero time disables the deadline.
func (st *Stream) SetWriteDeadline(t time.Time) error {
	s := st.session
	s.mu.Lock()
	st.writeDeadline = t
	st.writeNotify.notify()
	s.mu.Unlock()
	return nil
}

// SetDeadline sets both deadlines.
func (st *Stream) SetDeadline(t time.Time) error {
	_ = st.SetReadDeadline(t)
	return st.SetWriteDeadline(t)
}

func (st *Stream) sendDone() bool { return st.finSent || st.resetSent }

func (st *Stream) recvDone() bool { return st.finalKnown && st.read == st.finalSize }

// markReadyLocked queues the stream for the writer, which may have data, a FIN
// or new credit to act on.
func (st *Stream) markReadyLocked() {
	if st.resetSent {
		return
	}
	if !st.inReady {
		st.inReady = true
		st.session.ready = append(st.session.ready, st)
	}
	st.session.wakeWriter()
}

// maybeRemoveLocked forgets the stream once both directions have ended.
func (st *Stream) maybeRemoveLocked() {
	if !st.removed && st.sendDone() && st.recvDone() {
		st.removed = true
		st.session.streamCompletedLocked(st)
	}
}

// waitUntil waits for changed, failing with os.ErrDeadlineExceeded once a
// non-zero deadline passes.
func waitUntil(changed <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		<-changed
		return nil
	}
	wait := time.Until(deadline)
	if wait <= 0 {
		return os.ErrDeadlineExceeded
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-changed:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}
