package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
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
	stopCancel := cancelStreamOnContext(streamCtx, stream)
	go func() {
		writerDone <- c.writeStreamingRequest(uploadCtx, stream, header, requests)
	}()

	return &transportResponseStream{
		ctx:          streamCtx,
		stream:       stream,
		reader:       newFrameReader(stream, c.maxFrameSize, -1),
		writerDone:   writerDone,
		cancelUpload: cancelUpload,
		cancel:       cancel,
		stopCancel:   stopCancel,
		maxFrameSize: c.maxFrameSize,
	}, nil
}

func (c *streamClient) writeStreamingRequest(
	ctx context.Context,
	stream clientStream,
	header []byte,
	requests encodedStream,
) error {
	requests = closeStreamOnContext(ctx, requests)
	defer discardMessageStream(requests)

	if err := writeRequestFrames(ctx, stream, header, requests, c.maxFrameSize); err != nil {
		stream.cancelWrite()
		return callStatus(ctx, err)
	}
	if err := stream.Close(); err != nil {
		return callStatus(ctx, err)
	}
	return nil
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

// transportResponseStream reads the response frames of one call from its
// stream. A call that streams its request has a writer goroutine, whose
// result is reported together with the response.
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
}

func (s *transportResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *transportResponseStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

// Recv returns the next known response frame. After the STATUS frame it keeps
// reading, skipping unknown frames, so the end of the stream can be told from
// more data: the next call returns io.EOF when the stream ended cleanly.
func (s *transportResponseStream) Recv() (responseFrame, error) {
	if s.done {
		return responseFrame{}, io.EOF
	}

	next, err := s.reader.next()
	if err != nil {
		statusErr := io.EOF
		if !errors.Is(err, io.EOF) {
			statusErr = callStatus(s.ctx, err)
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
		status, err := decodeStatus(next.payload)
		if err != nil {
			return responseFrame{}, err
		}
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
