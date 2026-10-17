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

func (c *streamClient) call(
	ctx context.Context,
	request *wireRequest,
) (*wireResponse, error) {
	stream, err := c.opener.openStream(ctx)
	if err != nil {
		return nil, callStatus(ctx, err)
	}
	cancelRead := true
	defer func() {
		if cancelRead {
			stream.cancelRead()
		}
	}()
	stopCancel := cancelStreamOnContext(ctx, stream)
	defer stopCancel()

	if err := writeFrame(stream, request, c.maxFrameSize); err != nil {
		stream.cancelWrite()
		return nil, callStatus(ctx, err)
	}
	if err := stream.Close(); err != nil {
		return nil, callStatus(ctx, err)
	}

	response := &wireResponse{}
	if err := readFrame(stream, response, c.maxFrameSize); err != nil {
		return nil, callStatus(ctx, err)
	}
	if err := waitForCleanStreamEOF(stream); err != nil {
		return nil, callStatus(ctx, err)
	}
	cancelRead = false
	return response, nil
}

func waitForCleanStreamEOF(reader io.Reader) error {
	trailing := [1]byte{}
	for {
		read, err := reader.Read(trailing[:])
		if read != 0 {
			return &frameDecodeError{
				Err: errors.New("unexpected trailing data after unary response"),
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (c *streamClient) streamingCall(
	ctx context.Context,
	request *wireRequest,
	requestBody byteStream,
) (frameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.opener.openStream(streamCtx)
	if err != nil {
		statusErr := callStatus(streamCtx, err)
		cancel()
		return nil, statusErr
	}

	writerDone := make(chan error, 1)
	stopCancel := cancelStreamOnContext(streamCtx, stream)
	go func() {
		writerDone <- c.writeStreamingRequest(streamCtx, stream, request, requestBody)
	}()

	return &transportResponseStream{
		ctx:          streamCtx,
		stream:       stream,
		writerDone:   writerDone,
		cancel:       cancel,
		stopCancel:   stopCancel,
		maxFrameSize: c.maxFrameSize,
	}, nil
}

func (c *streamClient) writeStreamingRequest(
	ctx context.Context,
	stream clientStream,
	request *wireRequest,
	requestBody byteStream,
) error {
	requestBody = closeStreamOnContext(ctx, requestBody)
	defer discardMessageStream(requestBody)

	if err := writeFrame(stream, request, c.maxFrameSize); err != nil {
		stream.cancelWrite()
		return callStatus(ctx, err)
	}
	if err := writeRequestBodyFrames(ctx, stream, requestBody, c.maxFrameSize); err != nil {
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
	return transportStatus(err)
}

type transportResponseStream struct {
	ctx          context.Context
	stream       clientStream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
}

func (s *transportResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *transportResponseStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

func (s *transportResponseStream) Recv() (*wireStreamFrame, error) {
	frame, err := s.trevrpcRecvStreamFrameFields()
	if err != nil {
		return nil, err
	}
	return frame.rpcStreamFrame(), nil
}

func (s *transportResponseStream) trevrpcRecvStreamFrameFields() (streamFrameFields, error) {
	if s.done {
		return streamFrameFields{}, io.EOF
	}

	frame, read, err := readStreamFrameFieldsOrEOF(s.stream, s.maxFrameSize)
	if err != nil {
		statusErr := callStatus(s.ctx, err)
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, writerErr
		}
		return streamFrameFields{}, statusErr
	}
	if !read {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, writerErr
		}
		return streamFrameFields{}, io.EOF
	}
	if frame.kind == wireFrameStatus {
		s.finish(false)
		if frame.statusValue().IsOK() {
			if err := s.writerError(true); err != nil {
				return streamFrameFields{}, err
			}
		} else {
			s.ignoreWriterError()
		}
	}
	return frame, nil
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
		s.stopCancel = nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	if cancelRead {
		s.stream.cancelRead()
	}
	s.stream.cancelWrite()
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
