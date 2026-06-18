package trevrpc

import (
	"context"
	"errors"
	"io"

	"github.com/quic-go/quic-go"
)

const cancelledStreamCode quic.StreamErrorCode = 1

type QuinnTransport struct {
	conn         *quic.Conn
	maxFrameSize int
}

func NewQuinnTransport(conn *quic.Conn) *QuinnTransport {
	return &QuinnTransport{conn: conn, maxFrameSize: DefaultMaxFrameSize}
}

func (t *QuinnTransport) WithMaxFrameSize(maxFrameSize int) *QuinnTransport {
	t.maxFrameSize = maxFrameSize
	return t
}

func (t *QuinnTransport) Conn() *quic.Conn {
	return t.conn
}

func (t *QuinnTransport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	stream, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, transportStatus(err)
	}
	defer stream.CancelRead(cancelledStreamCode)

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		stream.CancelWrite(cancelledStreamCode)
		return nil, transportStatus(err)
	}

	if err := stream.Close(); err != nil {
		return nil, transportStatus(err)
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, transportStatus(err)
	}

	return response, nil
}

func (t *QuinnTransport) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	stream, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, transportStatus(err)
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writeStreamingRequest(stream, request, requestBody, t.maxFrameSize)
	}()

	return &quinnResponseStream{stream: stream, writerDone: writerDone, maxFrameSize: t.maxFrameSize}, nil
}

type quinnResponseStream struct {
	stream       *quic.Stream
	writerDone   <-chan error
	maxFrameSize int
	done         bool
}

func (s *quinnResponseStream) Recv() (*RpcStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	frame := &RpcStreamFrame{}
	read, err := ReadFrameOrEOF(s.stream, frame, s.maxFrameSize)
	if err != nil {
		s.done = true
		return nil, transportStatus(err)
	}

	if !read {
		s.done = true
		return nil, io.EOF
	}

	if frame.Kind == RpcStreamFrameKindStatus {
		s.done = true
	}

	return frame, nil
}

func writeStreamingRequest(stream *quic.Stream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledStreamCode)
		return transportStatus(err)
	}

	for {
		body, err := requestBody.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			stream.CancelWrite(cancelledStreamCode)
			return transportStatus(err)
		}

		if err := WriteFrame(stream, MessageFrame(body), maxFrameSize); err != nil {
			stream.CancelWrite(cancelledStreamCode)
			return transportStatus(err)
		}
	}

	return transportStatus(stream.Close())
}

func ServeQUIC(ctx context.Context, listener *quic.Listener, server *Server) error {
	connectionLimit := newSemaphore(server.options.MaxConcurrentConnections)
	requestLimit := newSemaphore(server.options.MaxConcurrentRequests)

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) {
				return nil
			}

			return transportStatus(err)
		}

		if !tryAcquire(connectionLimit) {
			conn.CloseWithError(0, "too many concurrent connections")
			continue
		}

		go func() {
			defer release(connectionLimit)
			HandleQUICConnection(ctx, conn, server, requestLimit)
		}()
	}
}

func HandleQUICConnection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore) {
	streamLimit := newSemaphore(server.options.MaxConcurrentStreamsPerConnection)

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}

		if !tryAcquire(streamLimit) {
			writeStatusResponse(stream, Unavailable("too many concurrent streams on connection"), server.options.MaxFrameSize)
			continue
		}

		if !tryAcquire(requestLimit) {
			release(streamLimit)
			writeStatusResponse(stream, Unavailable("too many concurrent RPCs"), server.options.MaxFrameSize)
			continue
		}

		go func() {
			defer release(streamLimit)
			defer release(requestLimit)
			handleQUICStream(ctx, server, stream)
		}()
	}
}

func handleQUICStream(ctx context.Context, server *Server, stream *quic.Stream) {
	request := &RpcRequest{}
	if err := ReadFrame(stream, request, server.options.MaxFrameSize); err != nil {
		_ = WriteFrame(stream, Internal(err.Error()).IntoResponse(nil), server.options.MaxFrameSize)
		_ = stream.Close()
		return
	}

	if request.RPCKind() == RpcKindUnary {
		response := server.HandleRequest(ctx, request)
		if err := WriteFrame(stream, response, server.options.MaxFrameSize); err == nil {
			_ = stream.Close()
		}
		return
	}

	requestBody := &quinnRequestStream{stream: stream, maxFrameSize: server.options.MaxFrameSize}
	response := server.HandleStreamingRequest(ctx, request, requestBody)
	for {
		frame, err := response.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			frame = StatusFrame(StatusFromError(err))
		}

		isStatus := frame.Kind == RpcStreamFrameKindStatus
		if err := WriteFrame(stream, frame, server.options.MaxFrameSize); err != nil {
			return
		}

		if isStatus {
			break
		}
	}

	_ = stream.Close()
}

func writeStatusResponse(stream *quic.Stream, status *Status, maxFrameSize int) {
	_ = WriteFrame(stream, status.IntoResponse(nil), maxFrameSize)
	_ = stream.Close()
}

type quinnRequestStream struct {
	stream       *quic.Stream
	maxFrameSize int
	done         bool
}

func (s *quinnRequestStream) Recv() ([]byte, error) {
	if s.done {
		return nil, io.EOF
	}

	frame := &RpcStreamFrame{}
	read, err := ReadFrameOrEOF(s.stream, frame, s.maxFrameSize)
	if err != nil {
		s.done = true
		return nil, transportStatus(err)
	}

	if !read {
		s.done = true
		return nil, io.EOF
	}

	frameKind, ok := frame.FrameKind()
	if !ok {
		s.done = true
		return nil, Internal("request stream contained an unknown frame kind")
	}

	if frameKind == RpcStreamFrameKindStatus {
		s.done = true
		status := frame.StatusValue()
		if status.IsOK() {
			return nil, io.EOF
		}

		return nil, status
	}

	return frame.Body, nil
}

func transportStatus(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return Cancelled("transport closed locally")
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return DeadlineExceeded("transport deadline exceeded")
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Unavailable("transport unavailable: " + err.Error())
	}

	return Unavailable("transport unavailable: " + err.Error())
}

type semaphore chan struct{}

func newSemaphore(limit int) semaphore {
	if limit <= 0 {
		return nil
	}

	return make(semaphore, limit)
}

func tryAcquire(limit semaphore) bool {
	if limit == nil {
		return true
	}

	select {
	case limit <- struct{}{}:
		return true
	default:
		return false
	}
}

func release(limit semaphore) {
	if limit == nil {
		return
	}

	<-limit
}
