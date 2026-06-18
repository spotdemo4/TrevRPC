package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

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
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := t.conn.OpenStreamSync(streamCtx)
	if err != nil {
		cancel()
		return nil, transportStatus(err)
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writeStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()

	return &quinnResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, maxFrameSize: t.maxFrameSize}, nil
}

type quinnResponseStream struct {
	stream       *quic.Stream
	writerDone   <-chan error
	cancel       context.CancelFunc
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
		s.finish(false)
		return nil, transportStatus(err)
	}

	if !read {
		s.finish(false)
		return nil, io.EOF
	}

	if frame.Kind == RpcStreamFrameKindStatus {
		s.finish(false)
	}

	return frame, nil
}

func (s *quinnResponseStream) Close() error {
	s.finish(true)
	return nil
}

func (s *quinnResponseStream) finish(cancelRead bool) {
	if s.cancel != nil {
		s.cancel()
	}

	if s.done {
		return
	}

	s.done = true
	if cancelRead {
		s.stream.CancelRead(cancelledStreamCode)
	}
	s.stream.CancelWrite(cancelledStreamCode)
}

func writeStreamingRequest(ctx context.Context, stream *quic.Stream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledStreamCode)
		return transportStatus(err)
	}

	for {
		body, err := recvRequestBody(ctx, requestBody)
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

func recvRequestBody(ctx context.Context, requestBody ByteStream) ([]byte, error) {
	type recvResult struct {
		body []byte
		err  error
	}

	results := make(chan recvResult, 1)
	go func() {
		body, err := requestBody.Recv()
		results <- recvResult{body: body, err: err}
	}()

	select {
	case result := <-results:
		return result.body, result.err
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	}
}

func ServeQUIC(ctx context.Context, listener *quic.Listener, server *Server) error {
	connectionLimit := newSemaphore(server.options.MaxConcurrentConnections)
	requestLimit := newSemaphore(server.options.MaxConcurrentRequests)
	connectionsCtx, stopConnections := context.WithCancel(context.Background())
	defer stopConnections()

	var connectionTasks sync.WaitGroup
	var activeMu sync.Mutex
	active := map[*quic.Conn]struct{}{}

	closeActiveConnections := func(message string) {
		activeMu.Lock()
		defer activeMu.Unlock()
		for conn := range active {
			conn.CloseWithError(0, message)
		}
	}

	go func() {
		<-ctx.Done()
		stopConnections()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) {
				break
			}

			stopConnections()
			closeActiveConnections("server accept failed")
			waitForWaitGroup(&connectionTasks, server.options.GracefulShutdownTimeout, func() {
				closeActiveConnections("server connection drain timed out")
			})
			return transportStatus(err)
		}

		if !tryAcquire(connectionLimit) {
			conn.CloseWithError(0, "too many concurrent connections")
			continue
		}

		activeMu.Lock()
		active[conn] = struct{}{}
		activeMu.Unlock()
		connectionTasks.Go(func() {
			defer release(connectionLimit)
			defer func() {
				activeMu.Lock()
				delete(active, conn)
				activeMu.Unlock()
			}()
			handleQUICConnection(connectionsCtx, conn, server, requestLimit, true)
		})
	}

	stopConnections()
	waitForWaitGroup(&connectionTasks, server.options.GracefulShutdownTimeout, func() {
		closeActiveConnections("server graceful shutdown timed out")
	})

	return nil
}

func HandleQUICConnection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore) {
	handleQUICConnection(ctx, conn, server, requestLimit, true)
}

func handleQUICConnection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore, closeOnShutdown bool) {
	streamLimit := newSemaphore(server.options.MaxConcurrentStreamsPerConnection)
	var streamTasks sync.WaitGroup

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			break
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

		streamTasks.Go(func() {
			defer release(streamLimit)
			defer release(requestLimit)
			handleQUICStream(stream.Context(), server, stream)
		})
	}

	waitForWaitGroup(&streamTasks, server.options.GracefulShutdownTimeout, func() {
		conn.CloseWithError(0, "server stream drain timed out")
	})

	if closeOnShutdown && ctx.Err() != nil {
		conn.CloseWithError(0, "server drained connection")
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
		response, ok := handleUnaryQUICRequest(ctx, server, request)
		if !ok {
			return
		}
		if err := WriteFrame(stream, response, server.options.MaxFrameSize); err == nil {
			_ = stream.Close()
		}
		return
	}

	requestBody := &quinnRequestStream{stream: stream, maxFrameSize: server.options.MaxFrameSize}
	response := server.HandleStreamingRequest(ctx, request, requestBody)
	for {
		frame, err := recvResponseFrame(ctx, response)
		if err == io.EOF {
			break
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			closeMessageStream(response)
			return
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

func handleUnaryQUICRequest(ctx context.Context, server *Server, request *RpcRequest) (*RpcResponse, bool) {
	result := make(chan *RpcResponse, 1)
	go func() {
		result <- server.HandleRequest(ctx, request)
	}()

	select {
	case response := <-result:
		return response, true
	case <-ctx.Done():
		return nil, false
	}
}

func recvResponseFrame(ctx context.Context, response FrameStream) (*RpcStreamFrame, error) {
	type recvResult struct {
		frame *RpcStreamFrame
		err   error
	}

	results := make(chan recvResult, 1)
	go func() {
		frame, err := response.Recv()
		results <- recvResult{frame: frame, err: err}
	}()

	select {
	case result := <-results:
		return result.frame, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func waitForWaitGroup(group *sync.WaitGroup, timeout time.Duration, onTimeout func()) {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()

	if timeout <= 0 {
		<-done
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		onTimeout()
		<-done
	}
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

	var status *Status
	if errors.As(err, &status) {
		return status
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

	var streamError *quic.StreamError
	if errors.As(err, &streamError) {
		return Cancelled(err.Error())
	}

	var applicationError *quic.ApplicationError
	if errors.As(err, &applicationError) {
		if applicationError.Remote {
			return Unavailable("transport unavailable: " + err.Error())
		}

		return Cancelled("transport closed locally")
	}

	var idleTimeout *quic.IdleTimeoutError
	if errors.As(err, &idleTimeout) {
		return Unavailable("transport unavailable: " + err.Error())
	}

	var handshakeTimeout *quic.HandshakeTimeoutError
	if errors.As(err, &handshakeTimeout) {
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
