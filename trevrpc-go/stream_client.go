package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// clientStream is the stream surface the RPC client needs from a transport.
// Close closes only the send direction.
type clientStream interface {
	io.ReadWriteCloser
	SetReadDeadline(time.Time) error
	// cancelRead aborts the receive direction.
	cancelRead()
	// cancelWrite aborts the send direction.
	cancelWrite()
}

// streamOpener opens one bidirectional stream per RPC.
type streamOpener interface {
	openStream(context.Context) (clientStream, error)
}

// streamClient runs RPCs over bidirectional streams, one stream per call.
type streamClient struct {
	opener       streamOpener
	maxFrameSize int
}

func newStreamClient(opener streamOpener, maxFrameSize int) *streamClient {
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	return &streamClient{opener: opener, maxFrameSize: maxFrameSize}
}

// call sends the request and its single message in one write, finishes the
// request side, and returns the response frames.
func (c *streamClient) call(
	ctx context.Context,
	request *wireRequest,
	message ProtoMessage,
) (frameStream, error) {
	frames, err := appendRequestFrame(nil, request, c.maxFrameSize)
	if err != nil {
		return nil, err
	}
	frames, err = appendLimitedProtoFrame(frames, frameTypeMessage, message, c.maxFrameSize)
	if err != nil {
		return nil, err
	}

	stream, err := c.opener.openStream(ctx)
	if err != nil {
		return nil, callStatus(ctx, err)
	}
	response := &transportResponseStream{
		ctx:          ctx,
		stream:       stream,
		reader:       newFrameReader(stream, c.maxFrameSize, -1),
		stopCancel:   cancelStreamOnContext(ctx, stream),
		maxFrameSize: c.maxFrameSize,
	}

	if _, err := stream.Write(frames); err != nil {
		stream.cancelWrite()
		response.finish(true)
		return nil, callStatus(ctx, err)
	}
	if err := stream.Close(); err != nil {
		response.finish(true)
		return nil, callStatus(ctx, err)
	}
	return response, nil
}

func (c *streamClient) streamingCall(
	ctx context.Context,
	request *wireRequest,
	requests encodedStream,
) (frameStream, error) {
	header, err := appendRequestFrame(nil, request, c.maxFrameSize)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.opener.openStream(streamCtx)
	if err != nil {
		statusErr := callStatus(streamCtx, err)
		cancel()
		return nil, statusErr
	}

	// The upload has a context of its own, so it can be stopped when the
	// response ends while the response stream stays readable.
	uploadCtx, cancelUpload := context.WithCancel(streamCtx)
	writerDone := make(chan error, 1)
	response := &transportResponseStream{
		ctx:          streamCtx,
		stream:       stream,
		reader:       newFrameReader(stream, c.maxFrameSize, -1),
		writerDone:   writerDone,
		cancelUpload: cancelUpload,
		cancel:       cancel,
		stopCancel:   cancelStreamOnContext(streamCtx, stream),
		maxFrameSize: c.maxFrameSize,
		frames:       make(chan pumpedFrame, responseReadAheadFrames),
		drained:      make(chan struct{}, 1),
		pumpStop:     make(chan struct{}),
		outcome:      make(chan struct{}),
	}
	if aborter, ok := requests.(uploadAborter); ok {
		response.abortUpload = aborter.abortUpload
	}
	go func() {
		writerDone <- c.writeStreamingRequest(uploadCtx, stream, header, requests, response.outcome)
	}()
	// The response is read while the upload runs, so that a terminal status
	// reaches a caller that is only sending.
	go response.pump()
	return response, nil
}

// writeStreamingRequest uploads the request and then the messages of requests.
// It closes requests when it returns, which fails the Send calls blocked on
// them. When the stream itself refuses the upload, because the server stopped
// reading, it first waits for outcome: the server's terminal status explains
// the failure, and Send reports it instead of a closed pipe.
func (c *streamClient) writeStreamingRequest(
	ctx context.Context,
	stream clientStream,
	header []byte,
	requests encodedStream,
	outcome <-chan struct{},
) error {
	requests = closeStreamOnContext(ctx, requests)
	defer discardMessageStream(requests)

	writer := &trackedWriter{writer: stream}
	if err := writeRequestFrames(ctx, writer, header, requests, c.maxFrameSize); err != nil {
		stream.cancelWrite()
		if writer.failed {
			select {
			case <-outcome:
			case <-ctx.Done():
			}
		}
		return callStatus(ctx, err)
	}
	if err := stream.Close(); err != nil {
		return callStatus(ctx, err)
	}
	return nil
}

// trackedWriter records whether a write to the stream failed, as opposed to
// the source of the data failing.
type trackedWriter struct {
	writer io.Writer
	failed bool
}

func (w *trackedWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err != nil {
		w.failed = true
	}
	return n, err
}

// callStatus maps a transport failure observed during a call to a status. A
// finished call context always wins so cancellations and deadlines are never
// reported as transport failures.
func callStatus(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	// A read deadline set from the call deadline can fire before the context
	// reports that the deadline passed.
	if deadline, ok := ctx.Deadline(); ok && isTimeoutError(err) && !time.Now().Before(deadline) {
		return statusFromContextError(context.DeadlineExceeded)
	}
	return transportStatus(err)
}

// uploadAborter is implemented by request streams that can tell a blocked or
// later Send why the upload ended: the server's terminal status.
type uploadAborter interface {
	// abortUpload makes Send fail with err. io.EOF reports that the server
	// finished the call successfully.
	abortUpload(err error)
}

// pumpedFrame is what the pump read from the response: a frame, a decoded
// STATUS, or the error that ended the stream.
type pumpedFrame struct {
	frame     frame
	status    *Status
	statusErr error
	err       error
}

// transportResponseStream reads the response frames of one call from its
// stream. A call that streams its request has a writer goroutine, whose
// result is reported together with the response, and a pump that reads the
// response ahead of Recv.
type transportResponseStream struct {
	ctx          context.Context
	stream       clientStream
	reader       *frameReader
	writerDone   <-chan error
	cancelUpload context.CancelFunc // set while the request is streamed
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool

	// The fields below are set for calls that stream their request.
	//
	// frames carries the pump's frames to Recv. The pump closes it after
	// delivering the error that ended the stream. queued counts the payload
	// bytes in it, and drained is signalled whenever Recv takes a frame.
	frames  chan pumpedFrame
	queued  atomic.Int64
	drained chan struct{}
	// pumpStop ends the pump when the call is finished.
	pumpStop chan struct{}
	stopPump sync.Once
	// outcome is closed once the terminal status or the failure that ended
	// the response is known.
	outcome        chan struct{}
	outcomeReached sync.Once
	// abortUpload reports why the upload ended to Send.
	abortUpload func(error)
}

func (s *transportResponseStream) trevrpcContextCancelsRecv() bool { return true }

// SetReadDeadline sets the stream's read deadline. A pumped stream is read by
// the pump alone, and the call context bounds it through cancellation.
func (s *transportResponseStream) SetReadDeadline(deadline time.Time) error {
	if s.frames != nil {
		return nil
	}
	return s.stream.SetReadDeadline(deadline)
}

const (
	// responseReadAheadBytes and responseReadAheadFrames bound how much of a
	// response the pump reads ahead of Recv. Beyond that it stops reading, so
	// the transport's flow control applies to a caller that does not receive.
	responseReadAheadBytes  = 1 << 20
	responseReadAheadFrames = 32
)

// pump reads the response while the caller is busy uploading. As soon as the
// terminal STATUS arrives it stops the upload and tells Send why, even though
// Recv has not asked for the frame yet. Frames wait in a bounded queue.
func (s *transportResponseStream) pump() {
	defer close(s.frames)
	for {
		for s.queued.Load() >= responseReadAheadBytes {
			select {
			case <-s.drained:
			case <-s.pumpStop:
				return
			case <-s.ctx.Done():
				return
			}
		}
		next, err := s.reader.next()
		item := pumpedFrame{frame: next, err: err}
		switch {
		case err != nil:
			if !errors.Is(err, io.EOF) {
				s.endUpload(callStatus(s.ctx, err))
			} else {
				s.endUpload(nil)
			}
		case next.typ == frameTypeStatus:
			item.status, item.statusErr = decodeStatus(next.payload)
			if item.statusErr == nil {
				s.endUpload(uploadEndError(item.status))
			}
		}
		s.queued.Add(int64(len(next.payload)))
		select {
		case s.frames <- item:
		case <-s.pumpStop:
			return
		case <-s.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// uploadEndError is the error Send reports once the server ended the call
// with status.
func uploadEndError(status *Status) error {
	if status.IsOK() {
		return io.EOF
	}
	return status
}

// endUpload stops uploading because the response has ended. err is why Send
// should fail, or nil if the stream gave no reason.
func (s *transportResponseStream) endUpload(err error) {
	if err != nil && s.abortUpload != nil {
		s.abortUpload(err)
	}
	s.stopUpload()
	s.outcomeReached.Do(func() { close(s.outcome) })
}

// nextFrame returns the next known frame of the response.
func (s *transportResponseStream) nextFrame() (pumpedFrame, error) {
	if s.frames == nil {
		next, err := s.reader.next()
		item := pumpedFrame{frame: next, err: err}
		if err == nil && next.typ == frameTypeStatus {
			item.status, item.statusErr = decodeStatus(next.payload)
		}
		return item, nil
	}
	select {
	case item, ok := <-s.frames:
		if !ok {
			// The pump stopped without a final frame because the call ended.
			if err := s.ctx.Err(); err != nil {
				return pumpedFrame{}, statusFromContextError(err)
			}
			return pumpedFrame{err: io.EOF}, nil
		}
		s.queued.Add(-int64(len(item.frame.payload)))
		select {
		case s.drained <- struct{}{}:
		default:
		}
		return item, nil
	case <-s.ctx.Done():
		return pumpedFrame{}, statusFromContextError(s.ctx.Err())
	}
}

// Recv returns the next known response frame. After the STATUS frame it keeps
// reading, skipping unknown frames, so the end of the stream can be told from
// more data: the next call returns io.EOF when the stream ended cleanly.
func (s *transportResponseStream) Recv() (responseFrame, error) {
	if s.done {
		return responseFrame{}, io.EOF
	}

	item, err := s.nextFrame()
	if err != nil {
		return responseFrame{}, err
	}
	next := item.frame
	if item.err != nil {
		statusErr := io.EOF
		if !errors.Is(item.err, io.EOF) {
			statusErr = callStatus(s.ctx, item.err)
		}
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return responseFrame{}, writerErr
		}
		return responseFrame{}, statusErr
	}

	switch next.typ {
	case frameTypeMessage:
		return responseFrame{kind: frameTypeMessage, body: next.payload}, nil
	case frameTypeHeaders:
		headers, err := decodeHeaders(next.payload)
		if err != nil {
			return responseFrame{}, err
		}
		return responseFrame{kind: frameTypeHeaders, headers: headers}, nil
	case frameTypeStatus:
		if item.statusErr != nil {
			return responseFrame{}, item.statusErr
		}
		status := item.status
		// The call is over: stop uploading and settle the writer. The context
		// watcher keeps guarding the wait for the end of the stream.
		s.stopUpload()
		if status.IsOK() {
			if err := s.writerError(true); err != nil {
				return responseFrame{}, err
			}
		} else {
			s.ignoreWriterError()
		}
		return responseFrame{kind: frameTypeStatus, status: status}, nil
	default:
		return responseFrame{kind: next.typ}, nil
	}
}

// stopUpload cancels the request side of a streamed request. The response
// side stays readable.
func (s *transportResponseStream) stopUpload() {
	if s.cancelUpload != nil {
		s.cancelUpload()
		s.stream.cancelWrite()
	}
}

func (s *transportResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *transportResponseStream) finish(cancelRead bool) {
	if s.done {
		return
	}
	s.done = true
	if s.pumpStop != nil {
		s.stopPump.Do(func() { close(s.pumpStop) })
	}
	if s.stopCancel != nil {
		s.stopCancel()
	}
	s.stopUpload()
	if s.cancel != nil {
		s.cancel()
	}
	if cancelRead {
		s.stream.cancelRead()
	}
}

func (s *transportResponseStream) writerError(ignoreCancelled bool) error {
	if s.writerDone == nil {
		return nil
	}
	err := <-s.writerDone
	s.writerDone = nil
	if err == nil {
		return nil
	}
	if ignoreCancelled && StatusFromError(err).Code == CodeCancelled {
		return nil
	}
	return err
}

func (s *transportResponseStream) ignoreWriterError() {
	s.writerDone = nil
}

func cancelStreamOnContext(ctx context.Context, stream clientStream) func() {
	return watchContextCancellation(ctx, func() {
		stream.cancelRead()
		stream.cancelWrite()
	})
}

func watchContextCancellation(ctx context.Context, cancel func()) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
	}()
	return func() { closeOnce.Do(func() { close(done) }) }
}
