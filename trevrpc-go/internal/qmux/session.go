package qmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Role is the side of a session. Clients open streams with even IDs, servers
// with odd IDs.
type Role int

// The two session roles.
const (
	RoleClient Role = iota
	RoleServer
)

const (
	defaultStreamSendBuffer = 64 << 10
	defaultCloseTimeout     = time.Second
	// maxSendRecordSize bounds the records this endpoint builds, whatever the
	// peer accepts.
	maxSendRecordSize = 64 << 10
	// streamQuantum is how much one stream may send before the others get a
	// turn.
	streamQuantum = 16 << 10
)

// Config configures a session.
type Config struct {
	// Version selects the wire codepoints. It defaults to Draft02.
	Version *Version
	// Parameters are the transport parameters advertised to the peer.
	Parameters Parameters
	// KeepAlivePeriod sends a QX_PING after this long without sending a
	// record. Zero disables keep-alives.
	KeepAlivePeriod time.Duration
	// StreamSendBuffer bounds the bytes Write buffers per stream before it
	// blocks. It defaults to 64 KiB.
	StreamSendBuffer int
	// CloseTimeout bounds how long a closing session waits for the peer to end
	// the connection. It defaults to one second.
	CloseTimeout time.Duration
}

type closeMode int

const (
	modeOpen closeMode = iota
	// modeClose sends CONNECTION_CLOSE, ends the sending direction and waits
	// for the peer to finish.
	modeClose
	// modeQuiet ends the sending direction without frames: the peer closed or
	// the idle timeout expired.
	modeQuiet
	// modeAbort releases the connection at once: it failed.
	modeAbort
)

// Session is a QMux connection carrying bidirectional streams.
type Session struct {
	conn      RecordConn
	role      Role
	version   *Version
	local     Parameters
	keepAlive time.Duration

	ctx    context.Context
	cancel context.CancelCauseFunc

	handshakeDone chan struct{}
	wake          chan struct{}
	readerDone    chan struct{}
	writerDone    chan struct{}

	lastReceived atomic.Int64
	lastSent     atomic.Int64

	mu sync.Mutex
	// peer is valid once handshakeDone is closed.
	peer        Parameters
	idleTimeout time.Duration
	sendBuffer  int
	closeWait   time.Duration

	closeErr   error
	mode       closeMode
	closeFrame []byte

	streams map[uint64]*Stream
	// Streams this endpoint opened and may open.
	openedLocal uint64
	maxLocal    uint64
	openNotify  notifier
	// Streams the peer opened and may open.
	openedRemote   uint64
	maxRemote      uint64
	sendMaxStreams bool
	acceptQueue    []*Stream
	acceptNotify   notifier
	connSendMax    uint64
	connSent       uint64
	connRecvMax    uint64
	connRecvWindow uint64
	connReceived   uint64
	connConsumed   uint64
	sendMaxData    bool
	control        [][]byte
	ready          []*Stream
	pingSent       uint64
	pingReceived   uint64 // the highest QX_PING request sequence, once receivedPing
	receivedPing   bool
	sendPing       bool
	sendPingReply  bool
	paramsReceived bool
}

// Open starts a session over conn. It sends the local transport parameters at
// once and returns when the peer's have arrived. If ctx ends first the
// connection is closed.
func Open(ctx context.Context, conn RecordConn, role Role, config Config) (*Session, error) {
	s := newSession(conn, role, config)
	select {
	case <-s.handshakeDone:
		return s, nil
	case <-s.ctx.Done():
		return nil, context.Cause(s.ctx)
	case <-ctx.Done():
		err := context.Cause(ctx)
		s.closeWith(fmt.Errorf("QMux handshake: %w", err), nil, modeAbort)
		return nil, err
	}
}

func newSession(conn RecordConn, role Role, config Config) *Session {
	version := config.Version
	if version == nil {
		version = &Draft02
	}
	sendBuffer := config.StreamSendBuffer
	if sendBuffer <= 0 {
		sendBuffer = defaultStreamSendBuffer
	}
	closeWait := config.CloseTimeout
	if closeWait <= 0 {
		closeWait = defaultCloseTimeout
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	s := &Session{
		conn:           conn,
		role:           role,
		version:        version,
		local:          config.Parameters,
		keepAlive:      config.KeepAlivePeriod,
		ctx:            ctx,
		cancel:         cancel,
		handshakeDone:  make(chan struct{}),
		wake:           make(chan struct{}, 1),
		readerDone:     make(chan struct{}),
		writerDone:     make(chan struct{}),
		sendBuffer:     sendBuffer,
		closeWait:      closeWait,
		streams:        map[uint64]*Stream{},
		maxRemote:      config.Parameters.InitialMaxStreamsBidi,
		connRecvMax:    config.Parameters.InitialMaxData,
		connRecvWindow: config.Parameters.InitialMaxData,
		peer:           Parameters{MaxRecordSize: DefaultMaxRecordSize},
	}
	now := time.Now().UnixNano()
	s.lastReceived.Store(now)
	s.lastSent.Store(now)
	// The transport parameters must be the first frame on the wire.
	s.control = append(s.control, appendTransportParameters(nil, version, s.local))
	s.wakeWriter()
	go s.readLoop()
	go s.writeLoop()
	return s
}

// Context is cancelled when the session ends. Its cause is the reason.
func (s *Session) Context() context.Context { return s.ctx }

// Done is closed once the session has released its connection.
func (s *Session) Done() <-chan struct{} { return s.writerDone }

// CloseWithError closes the session with an application error code and
// message. Data already written to streams is sent first when flow control
// allows.
func (s *Session) CloseWithError(code uint64, message string) error {
	err := &ApplicationError{ErrorCode: code, ErrorMessage: message}
	s.closeWith(err, func(b []byte) []byte { return appendConnectionClose(b, nil, code, message) }, modeClose)
	return nil
}

// AcceptStream returns the next stream opened by the peer.
func (s *Session) AcceptStream(ctx context.Context) (*Stream, error) {
	s.mu.Lock()
	for {
		if len(s.acceptQueue) > 0 {
			stream := s.acceptQueue[0]
			s.acceptQueue[0] = nil
			s.acceptQueue = s.acceptQueue[1:]
			s.mu.Unlock()
			return stream, nil
		}
		if s.closeErr != nil {
			err := s.closeErr
			s.mu.Unlock()
			return nil, err
		}
		changed := s.acceptNotify.wait()
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		s.mu.Lock()
	}
}

// OpenStreamSync opens a stream, waiting until the peer's stream limit allows
// it.
func (s *Session) OpenStreamSync(ctx context.Context) (*Stream, error) {
	s.mu.Lock()
	for {
		if s.closeErr != nil {
			err := s.closeErr
			s.mu.Unlock()
			return nil, err
		}
		if s.openedLocal < s.maxLocal {
			id := s.openedLocal<<2 | uint64(s.role)
			s.openedLocal++
			stream := newStream(s, id, s.peer.InitialMaxStreamDataBidiRemote, s.local.InitialMaxStreamDataBidiLocal)
			s.streams[id] = stream
			s.mu.Unlock()
			return stream, nil
		}
		changed := s.openNotify.wait()
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		s.mu.Lock()
	}
}

// closeWith ends the session with err unless it has already ended. frame
// encodes the CONNECTION_CLOSE to send in modeClose.
func (s *Session) closeWith(err error, frame func([]byte) []byte, mode closeMode) {
	s.mu.Lock()
	if s.closeErr != nil {
		s.mu.Unlock()
		return
	}
	s.closeErr = err
	s.mode = mode
	if frame != nil {
		s.closeFrame = frame(nil)
	}
	s.cancel(err)
	s.acceptNotify.notify()
	s.openNotify.notify()
	for _, stream := range s.streams {
		stream.readNotify.notify()
		stream.writeNotify.notify()
	}
	s.mu.Unlock()
	if mode == modeAbort {
		_ = s.conn.Close()
	}
	s.wakeWriter()
}

// fail closes the session after a local protocol error.
func (s *Session) fail(err *TransportError) {
	s.closeWith(err, func(b []byte) []byte {
		reason := err.ErrorMessage
		if len(reason) > 256 {
			reason = reason[:256]
		}
		return appendConnectionClose(b, err, 0, reason)
	}, modeClose)
}

func (s *Session) wakeWriter() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Session) readLoop() {
	defer close(s.readerDone)
	for {
		record, err := s.conn.ReadRecord()
		if err != nil {
			s.readFailed(err)
			return
		}
		s.lastReceived.Store(time.Now().UnixNano())
		if err := s.handleRecord(record); err != nil {
			s.fail(err)
		}
	}
}

func (s *Session) readFailed(err error) {
	var transportErr *TransportError
	switch {
	case errors.As(err, &transportErr):
		s.fail(transportErr)
		// Keep reading until the writer gives up on the peer.
		for {
			if _, err := s.conn.ReadRecord(); err != nil {
				if _, ok := errors.AsType[*TransportError](err); !ok {
					return
				}
			}
		}
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		s.closeWith(ErrConnectionLost, nil, modeAbort)
	default:
		s.closeWith(fmt.Errorf("%w: %w", ErrConnectionLost, err), nil, modeAbort)
	}
}

// handleRecord applies the frames of one record. Once the session is closing,
// records are discarded.
func (s *Session) handleRecord(record []byte) *TransportError {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeErr != nil {
		return nil
	}
	parser := frameParser{buf: record, version: s.version}
	for parser.more() {
		f, err := parser.next()
		if err != nil {
			if transportErr, ok := errors.AsType[*TransportError](err); ok {
				return transportErr
			}
			return protocolError(FrameEncodingError, 0, "%v", err)
		}
		if !s.paramsReceived && f.typ != s.version.TransportParametersType {
			return protocolError(ProtocolViolation, f.typ, "first frame is not QX_TRANSPORT_PARAMETERS")
		}
		if err := s.handleFrameLocked(&f); err != nil {
			return err
		}
		if s.closeErr != nil {
			return nil
		}
	}
	return nil
}

func (s *Session) handleFrameLocked(f *frame) *TransportError {
	switch {
	case f.typ == s.version.TransportParametersType:
		return s.handleParametersLocked(f)
	case f.typ == frameTypePadding:
		return nil
	case f.typ >= frameTypeStream && f.typ <= frameTypeStream|0x07:
		return s.handleStreamLocked(f)
	case f.typ == frameTypeResetStream:
		return s.handleResetStreamLocked(f)
	case f.typ == frameTypeStopSending:
		return s.handleStopSendingLocked(f)
	case f.typ == frameTypeMaxData:
		if f.maximum > s.connSendMax {
			s.connSendMax = f.maximum
			s.wakeWriter()
		}
		return nil
	case f.typ == frameTypeMaxStreamData:
		return s.handleMaxStreamDataLocked(f)
	case f.typ == frameTypeMaxStreamsBidi, f.typ == frameTypeMaxStreamsUni:
		if f.maximum > maxStreamsLimit {
			return protocolError(FrameEncodingError, f.typ, "MAX_STREAMS %d exceeds 2^60", f.maximum)
		}
		if f.typ == frameTypeMaxStreamsBidi && f.maximum > s.maxLocal {
			s.maxLocal = f.maximum
			s.openNotify.notify()
		}
		return nil
	case f.typ == frameTypeStreamsBlockedBidi, f.typ == frameTypeStreamsBlockedUni:
		if f.maximum > maxStreamsLimit {
			return protocolError(FrameEncodingError, f.typ, "STREAMS_BLOCKED %d exceeds 2^60", f.maximum)
		}
		return nil
	case f.typ == frameTypeDataBlocked:
		return nil
	case f.typ == frameTypeStreamDataBlocked:
		_, err := s.streamLocked(f.streamID, f.typ)
		return err
	case f.typ == frameTypeConnectionClose, f.typ == frameTypeApplicationClose:
		s.handleConnectionCloseLocked(f)
		return nil
	case f.typ == s.version.PingType:
		// Sequence numbers must strictly increase but may start anywhere,
		// including 0, which is where the qmux crate starts.
		if s.receivedPing && f.sequence <= s.pingReceived {
			return protocolError(ProtocolViolation, f.typ, "QX_PING sequence number %d is not increasing", f.sequence)
		}
		s.pingReceived, s.receivedPing = f.sequence, true
		s.sendPingReply = true
		s.wakeWriter()
		return nil
	case f.typ == s.version.PingResponseType:
		if f.sequence > s.pingSent {
			return protocolError(ProtocolViolation, f.typ, "QX_PING response %d was never requested", f.sequence)
		}
		return nil
	default:
		return protocolError(FrameEncodingError, f.typ, "unexpected frame type 0x%x", f.typ)
	}
}

func (s *Session) handleParametersLocked(f *frame) *TransportError {
	if s.paramsReceived {
		return protocolError(ProtocolViolation, f.typ, "repeated QX_TRANSPORT_PARAMETERS")
	}
	peer, err := decodeParameters(f.params, s.version)
	if err != nil {
		if transportErr, ok := errors.AsType[*TransportError](err); ok {
			return transportErr
		}
		return protocolError(TransportParameterError, f.typ, "%v", err)
	}
	s.paramsReceived = true
	s.peer = peer
	s.maxLocal = peer.InitialMaxStreamsBidi
	s.connSendMax = peer.InitialMaxData
	s.idleTimeout = s.local.MaxIdleTimeout
	if peer.MaxIdleTimeout > 0 && (s.idleTimeout == 0 || peer.MaxIdleTimeout < s.idleTimeout) {
		s.idleTimeout = peer.MaxIdleTimeout
	}
	close(s.handshakeDone)
	go s.timerLoop()
	return nil
}

func (s *Session) handleConnectionCloseLocked(f *frame) {
	var err error
	if f.typ == frameTypeApplicationClose {
		err = &ApplicationError{Remote: true, ErrorCode: f.errorCode, ErrorMessage: string(f.reason)}
	} else {
		err = &TransportError{Remote: true, ErrorCode: TransportErrorCode(f.errorCode), FrameType: f.frameType, ErrorMessage: string(f.reason)}
	}
	// closeWith takes the lock itself.
	s.mu.Unlock()
	s.closeWith(err, nil, modeQuiet)
	s.mu.Lock()
}

// isLocal reports whether this endpoint opened the stream.
func (s *Session) isLocal(id uint64) bool { return id&1 == uint64(s.role) }

// streamLocked returns the stream a frame refers to, opening peer streams up
// to it. It returns nil for a stream that has already completed.
func (s *Session) streamLocked(id uint64, frameType uint64) (*Stream, *TransportError) {
	num := id >> 2
	if id&2 != 0 {
		if s.isLocal(id) {
			return nil, protocolError(StreamStateError, frameType, "unidirectional stream %d was never opened", id)
		}
		return nil, protocolError(StreamLimitError, frameType, "unidirectional streams are not allowed")
	}
	if s.isLocal(id) {
		if num >= s.openedLocal {
			return nil, protocolError(StreamStateError, frameType, "stream %d was never opened", id)
		}
		return s.streams[id], nil
	}
	if num < s.openedRemote {
		return s.streams[id], nil
	}
	if num >= s.maxRemote {
		return nil, protocolError(StreamLimitError, frameType, "stream %d exceeds the stream limit %d", id, s.maxRemote)
	}
	for ; s.openedRemote <= num; s.openedRemote++ {
		streamID := s.openedRemote<<2 | uint64(1-s.role)
		stream := newStream(s, streamID, s.peer.InitialMaxStreamDataBidiLocal, s.local.InitialMaxStreamDataBidiRemote)
		s.streams[streamID] = stream
		s.acceptQueue = append(s.acceptQueue, stream)
	}
	s.acceptNotify.notify()
	return s.streams[id], nil
}

func (s *Session) handleStreamLocked(f *frame) *TransportError {
	stream, err := s.streamLocked(f.streamID, f.typ)
	if err != nil || stream == nil {
		return err
	}
	end := f.offset + uint64(len(f.data))
	if stream.finalKnown && (end > stream.finalSize || (f.fin && end != stream.finalSize)) {
		return protocolError(FinalSizeError, f.typ, "stream %d data beyond its final size", f.streamID)
	}
	if f.offset != stream.received {
		return protocolError(ProtocolViolation, f.typ, "stream %d data at offset %d, expected %d", f.streamID, f.offset, stream.received)
	}
	if end > stream.recvMax {
		return protocolError(FlowControlError, f.typ, "stream %d exceeds its flow control limit", f.streamID)
	}
	s.connReceived += uint64(len(f.data))
	if s.connReceived > s.connRecvMax {
		return protocolError(FlowControlError, f.typ, "connection flow control limit exceeded")
	}
	stream.received = end
	if f.fin {
		stream.finalKnown = true
		stream.finalSize = end
		stream.finReceived = true
	}
	if stream.readCancelled || stream.resetReceived != nil {
		stream.read = end
		s.consumedLocked(uint64(len(f.data)))
	} else {
		stream.appendReceivedLocked(f.data)
	}
	stream.readNotify.notify()
	stream.maybeRemoveLocked()
	return nil
}

func (s *Session) handleResetStreamLocked(f *frame) *TransportError {
	stream, err := s.streamLocked(f.streamID, f.typ)
	if err != nil || stream == nil {
		return err
	}
	if stream.finReceived && f.finalSize == stream.finalSize {
		// Every byte has already arrived, so the reset changes nothing.
		return nil
	}
	if (stream.finalKnown && f.finalSize != stream.finalSize) || f.finalSize < stream.received {
		return protocolError(FinalSizeError, f.typ, "stream %d reset with an inconsistent final size", f.streamID)
	}
	if f.finalSize > stream.recvMax {
		return protocolError(FlowControlError, f.typ, "stream %d final size exceeds its flow control limit", f.streamID)
	}
	s.connReceived += f.finalSize - stream.received
	if s.connReceived > s.connRecvMax {
		return protocolError(FlowControlError, f.typ, "connection flow control limit exceeded")
	}
	stream.received = f.finalSize
	stream.finalKnown = true
	stream.finalSize = f.finalSize
	if !stream.readCancelled && stream.resetReceived == nil {
		stream.resetReceived = &StreamError{StreamID: f.streamID, ErrorCode: f.errorCode, Remote: true}
	}
	if unread := stream.finalSize - stream.read; unread > 0 {
		s.consumedLocked(unread)
		stream.read = stream.finalSize
		stream.recvBuf, stream.recvHead = stream.recvBuf[:0], 0
	}
	stream.readNotify.notify()
	stream.maybeRemoveLocked()
	return nil
}

func (s *Session) handleStopSendingLocked(f *frame) *TransportError {
	stream, err := s.streamLocked(f.streamID, f.typ)
	if err != nil || stream == nil {
		return err
	}
	if !stream.sendDone() {
		stream.writeErr = &StreamError{StreamID: f.streamID, ErrorCode: f.errorCode, Remote: true}
		stream.resetSendLocked(f.errorCode)
	}
	return nil
}

func (s *Session) handleMaxStreamDataLocked(f *frame) *TransportError {
	stream, err := s.streamLocked(f.streamID, f.typ)
	if err != nil || stream == nil {
		return err
	}
	if f.maximum > stream.sendMax {
		stream.sendMax = f.maximum
		stream.markReadyLocked()
	}
	return nil
}

// consumedLocked returns n received bytes of connection credit to the peer.
func (s *Session) consumedLocked(n uint64) {
	s.connConsumed += n
	if s.connRecvMax-s.connConsumed <= s.connRecvWindow/2 {
		s.connRecvMax = s.connConsumed + s.connRecvWindow
		s.sendMaxData = true
		s.wakeWriter()
	}
}

// streamCompletedLocked forgets a stream whose both directions have ended,
// granting the peer a new stream when it opened it.
func (s *Session) streamCompletedLocked(stream *Stream) {
	delete(s.streams, stream.id)
	if !s.isLocal(stream.id) {
		s.maxRemote++
		s.sendMaxStreams = true
		s.wakeWriter()
	}
}

func (s *Session) queueControlLocked(frame []byte) {
	s.control = append(s.control, frame)
	s.wakeWriter()
}

func (s *Session) writeLoop() {
	defer close(s.writerDone)
	record := make([]byte, 0, maxSendRecordSize)
	for {
		select {
		case <-s.wake:
		case <-s.ctx.Done():
		}
		for {
			s.mu.Lock()
			frames, full, mode := s.buildRecordLocked(record[:0])
			s.mu.Unlock()
			if len(frames) > 0 {
				if err := s.conn.WriteRecord(frames); err != nil {
					s.closeWith(fmt.Errorf("%w: %w", ErrConnectionLost, err), nil, modeAbort)
					return
				}
				s.lastSent.Store(time.Now().UnixNano())
			}
			if mode != modeOpen {
				s.finish(mode)
				return
			}
			if !full {
				break
			}
		}
		if err := s.conn.Flush(); err != nil {
			s.closeWith(fmt.Errorf("%w: %w", ErrConnectionLost, err), nil, modeAbort)
			return
		}
	}
}

// finish ends the connection once the session has stopped sending.
func (s *Session) finish(mode closeMode) {
	switch mode {
	case modeClose:
		_ = s.conn.CloseWrite()
		timer := time.NewTimer(s.closeWait)
		select {
		case <-s.readerDone:
		case <-timer.C:
		}
		timer.Stop()
	case modeQuiet:
		_ = s.conn.CloseWrite()
	}
	_ = s.conn.Close()
}

// buildRecordLocked appends the next record's frames to b. full reports that
// the record ran out of space with more to send. A mode other than modeOpen
// means the session is done sending once these frames are written.
func (s *Session) buildRecordLocked(b []byte) (frames []byte, full bool, mode closeMode) {
	limit := min(int(min(s.peer.maxRecordSize(), uint64(maxSendRecordSize))), cap(b))
	if s.mode == modeAbort {
		return nil, false, modeAbort
	}
	if s.mode == modeQuiet {
		return nil, false, modeQuiet
	}

	sent := 0
	for _, frame := range s.control {
		if len(b)+len(frame) > limit {
			break
		}
		b = append(b, frame...)
		sent++
	}
	clear(s.control[:sent])
	s.control = s.control[sent:]
	if len(s.control) > 0 {
		return b, true, modeOpen
	}
	b = s.appendFlowControlLocked(b, limit)
	b, full = s.appendStreamDataLocked(b, limit)
	if full || s.mode == modeOpen {
		return b, full, modeOpen
	}
	// Closing: everything sendable has been sent, so CONNECTION_CLOSE goes last.
	if len(b)+len(s.closeFrame) > limit {
		return b, true, modeOpen
	}
	return append(b, s.closeFrame...), false, modeClose
}

func (s *Session) appendFlowControlLocked(b []byte, limit int) []byte {
	const maxFrame = 1 + 8 + 8
	if s.sendMaxData && len(b)+maxFrame <= limit {
		b = appendMaxData(b, s.connRecvMax)
		s.sendMaxData = false
	}
	if s.sendMaxStreams && len(b)+maxFrame <= limit {
		b = appendMaxStreamsBidi(b, s.maxRemote)
		s.sendMaxStreams = false
	}
	if s.sendPingReply && len(b)+maxFrame+8 <= limit {
		b = appendPing(b, s.version.PingResponseType, s.pingReceived)
		s.sendPingReply = false
	}
	if s.sendPing && len(b)+maxFrame+8 <= limit {
		s.pingSent++
		b = appendPing(b, s.version.PingType, s.pingSent)
		s.sendPing = false
	}
	return b
}

// appendStreamDataLocked appends STREAM frames for ready streams in turn until
// the record is full or nothing more may be sent. It reports whether another
// record has data to send.
func (s *Session) appendStreamDataLocked(b []byte, limit int) ([]byte, bool) {
	const minFrame = 32
	var again []*Stream
	retained := s.ready[:0]
	full := false
	for i, stream := range s.ready {
		if limit-len(b) < minFrame {
			retained = append(retained, s.ready[i:]...)
			full = true
			break
		}
		if stream.resetSent {
			stream.inReady = false
			continue
		}
		pending := len(stream.sendBuf) - stream.sendHead
		credit := min(stream.sendMax-stream.sent, s.connSendMax-s.connSent)
		n := min(uint64(pending), credit, streamQuantum)
		room := max(limit-len(b)-streamFrameOverhead(stream.id, stream.sent, int(n)), 0)
		if uint64(room) < n {
			n = uint64(room)
		}
		fin := stream.finRequested && n == uint64(pending)
		if n == 0 && !fin {
			if pending == 0 || stream.sent == stream.sendMax {
				// Nothing to send, or blocked on the stream's own limit: Write,
				// Close or MAX_STREAM_DATA re-queues it.
				stream.inReady = false
			} else {
				// Blocked on the connection limit: MAX_DATA wakes the writer.
				retained = append(retained, stream)
			}
			continue
		}
		data := stream.sendBuf[stream.sendHead : stream.sendHead+int(n)]
		b = appendStreamFrame(b, stream.id, stream.sent, data, fin)
		stream.sendHead += int(n)
		if stream.sendHead == len(stream.sendBuf) {
			stream.sendBuf, stream.sendHead = stream.sendBuf[:0], 0
		}
		stream.sent += n
		s.connSent += n
		stream.writeNotify.notify()
		switch {
		case fin:
			stream.finSent = true
			stream.inReady = false
			stream.maybeRemoveLocked()
		case len(stream.sendBuf) > stream.sendHead || stream.finRequested:
			again = append(again, stream)
		default:
			stream.inReady = false
		}
	}
	s.ready = append(retained, again...)
	// Streams that used up their turn send again in the next record.
	return b, full || len(again) > 0
}

// timerLoop enforces the idle timeout and sends keep-alives.
func (s *Session) timerLoop() {
	s.mu.Lock()
	idle := s.idleTimeout
	s.mu.Unlock()
	keepAlive := s.keepAlive
	if idle > 0 && keepAlive > idle/2 {
		// Keep-alives must reach the peer well within the negotiated timeout.
		keepAlive = idle / 2
	}
	if idle == 0 && keepAlive == 0 {
		return
	}
	tick := idle / 4
	if keepAlive > 0 && (tick == 0 || keepAlive/2 < tick) {
		tick = keepAlive / 2
	}
	tick = max(tick, 10*time.Millisecond)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			lastSent := time.Unix(0, s.lastSent.Load())
			lastActivity := time.Unix(0, max(s.lastReceived.Load(), s.lastSent.Load()))
			if idle > 0 && now.Sub(lastActivity) >= idle {
				s.closeWith(ErrIdleTimeout, nil, modeQuiet)
				return
			}
			if keepAlive > 0 && now.Sub(lastSent) >= keepAlive {
				s.mu.Lock()
				s.sendPing = true
				s.mu.Unlock()
				s.wakeWriter()
			}
		}
	}
}

// notifier wakes every goroutine waiting for a change. It is used with the
// session lock held.
type notifier struct{ ch chan struct{} }

func (n *notifier) wait() <-chan struct{} {
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	return n.ch
}

func (n *notifier) notify() {
	if n.ch != nil {
		close(n.ch)
		n.ch = nil
	}
}
