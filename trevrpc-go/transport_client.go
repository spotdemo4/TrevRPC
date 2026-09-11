package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

var cancelledTransportReason = TransportCloseReason{
	Local:           true,
	ApplicationCode: 1,
	Message:         "RPC cancelled",
}

type transportStatusMapper func(context.Context, error) error

type transportStreamClient struct {
	endpoint     transportinternal.StreamEndpoint
	maxFrameSize int
	mapStatus    transportStatusMapper
}

func newTransportStreamClient(
	endpoint transportinternal.StreamEndpoint,
	maxFrameSize int,
	mapStatus transportStatusMapper,
) *transportStreamClient {
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	return &transportStreamClient{
		endpoint:     endpoint,
		maxFrameSize: maxFrameSize,
		mapStatus:    mapStatus,
	}
}

func (c *transportStreamClient) Call(
	ctx context.Context,
	request *RpcRequest,
) (*RpcResponse, error) {
	stream, err := c.endpoint.OpenStream(ctx)
	if err != nil {
		return nil, c.status(ctx, err)
	}
	cancelRead := true
	defer func() {
		if cancelRead {
			cancelTransportStreamRead(stream)
		}
	}()
	stopCancel := cancelTransportStreamOnContext(ctx, stream)
	defer stopCancel()

	if err := WriteFrame(stream, request, c.maxFrameSize); err != nil {
		cancelTransportStreamWrite(stream)
		return nil, c.status(ctx, err)
	}
	if err := stream.Close(); err != nil {
		return nil, c.status(ctx, err)
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, c.maxFrameSize); err != nil {
		return nil, c.status(ctx, err)
	}
	if err := waitForCleanTransportStreamEOF(stream); err != nil {
		return nil, c.status(ctx, err)
	}
	cancelRead = false
	return response, nil
}

func waitForCleanTransportStreamEOF(reader io.Reader) error {
	trailing := [1]byte{}
	for {
		read, err := reader.Read(trailing[:])
		if read != 0 {
			return &FrameDecodeError{
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

func (c *transportStreamClient) StreamingCall(
	ctx context.Context,
	request *RpcRequest,
	requestBody ByteStream,
) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.endpoint.OpenStream(streamCtx)
	if err != nil {
		statusErr := c.status(streamCtx, err)
		cancel()
		return nil, statusErr
	}

	writerDone := make(chan error, 1)
	stopCancel := cancelTransportStreamOnContext(streamCtx, stream)
	go func() {
		writerDone <- c.writeStreamingRequest(
			streamCtx,
			stream,
			request,
			requestBody,
		)
	}()

	return &transportResponseStream{
		ctx:          streamCtx,
		stream:       stream,
		writerDone:   writerDone,
		cancel:       cancel,
		stopCancel:   stopCancel,
		maxFrameSize: c.maxFrameSize,
		mapStatus:    c.mapStatus,
	}, nil
}

func (c *transportStreamClient) writeStreamingRequest(
	ctx context.Context,
	stream transportinternal.BidirectionalStream,
	request *RpcRequest,
	requestBody ByteStream,
) error {
	requestBody = closeStreamOnContext(ctx, requestBody)
	defer closeMessageStream(requestBody)

	if err := WriteFrame(stream, request, c.maxFrameSize); err != nil {
		cancelTransportStreamWrite(stream)
		return c.status(ctx, err)
	}
	if err := writeRequestBodyFrames(
		ctx,
		stream,
		requestBody,
		c.maxFrameSize,
	); err != nil {
		cancelTransportStreamWrite(stream)
		return c.status(ctx, err)
	}
	if err := stream.Close(); err != nil {
		return c.status(ctx, err)
	}
	return nil
}

func (c *transportStreamClient) status(ctx context.Context, err error) error {
	if c.mapStatus != nil {
		return c.mapStatus(ctx, err)
	}
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	return transportStatus(err)
}

type transportResponseStream struct {
	ctx          context.Context
	stream       transportinternal.BidirectionalStream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	mapStatus    transportStatusMapper
	done         bool
}

func (s *transportResponseStream) trevrpcContextCancelsRecv() bool {
	_, ok := s.stream.(transportinternal.ReadCanceler)
	return ok
}

func (s *transportResponseStream) SetReadDeadline(deadline time.Time) error {
	setter, ok := s.stream.(transportinternal.ReadDeadlineSetter)
	if !ok {
		return Unimplemented("transport stream does not support read deadlines")
	}
	return setter.SetReadDeadline(deadline)
}

func (s *transportResponseStream) Recv() (*RpcStreamFrame, error) {
	frame, _, err := s.trevrpcRecvStreamFrameFields()
	if err != nil {
		return nil, err
	}
	return frame.rpcStreamFrame(), nil
}

func (s *transportResponseStream) trevrpcRecvStreamFrameFields() (
	streamFrameFields,
	func(),
	error,
) {
	if s.done {
		return streamFrameFields{}, nil, io.EOF
	}

	frame, read, err := readStreamFrameFieldsOrEOF(
		s.stream,
		s.maxFrameSize,
	)
	if err != nil {
		statusErr := s.status(err)
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, statusErr
	}
	if !read {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, io.EOF
	}
	if frame.kind == RpcStreamFrameKindStatus {
		s.finish(false)
		if frame.statusValue().IsOK() {
			if err := s.writerError(true); err != nil {
				return streamFrameFields{}, nil, err
			}
		} else {
			s.ignoreWriterError()
		}
	}
	return frame, nil, nil
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
		cancelTransportStreamRead(s.stream)
	}
	cancelTransportStreamWrite(s.stream)
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

func (s *transportResponseStream) status(err error) error {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if s.mapStatus != nil {
		return s.mapStatus(ctx, err)
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		return statusFromContextError(s.ctx.Err())
	}
	return transportStatus(err)
}

func cancelTransportStreamRead(stream transportinternal.BidirectionalStream) {
	if canceler, ok := stream.(transportinternal.ReadCanceler); ok {
		canceler.CancelRead(cancelledTransportReason)
	}
}

func cancelTransportStreamWrite(stream transportinternal.BidirectionalStream) {
	if canceler, ok := stream.(transportinternal.WriteCanceler); ok {
		canceler.CancelWrite(cancelledTransportReason)
	}
}

func cancelTransportStreamOnContext(
	ctx context.Context,
	stream transportinternal.BidirectionalStream,
) func() {
	return watchContextCancellation(ctx, func() {
		cancelTransportStreamRead(stream)
		cancelTransportStreamWrite(stream)
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
