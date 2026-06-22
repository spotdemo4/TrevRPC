package trevrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const cancelledStreamCode quic.StreamErrorCode = 1

const maxMessageFrameBatch = 16

// QuicClient sends TrevRPC calls over an established QUIC connection.
type QuicClient struct {
	conn         *quic.Conn
	maxFrameSize int
}

// NewQuicClient creates a TrevRPC client over an established QUIC connection.
func NewQuicClient(conn *quic.Conn) *QuicClient {
	return &QuicClient{conn: conn, maxFrameSize: DefaultMaxFrameSize}
}

// WithMaxFrameSize sets the maximum TrevRPC frame size for the client.
func (t *QuicClient) WithMaxFrameSize(maxFrameSize int) *QuicClient {
	t.maxFrameSize = maxFrameSize
	return t
}

// Conn returns the underlying QUIC connection.
func (t *QuicClient) Conn() *quic.Conn {
	return t.conn
}

// Close closes the underlying QUIC connection.
func (t *QuicClient) Close() error {
	if t == nil || t.conn == nil {
		return nil
	}
	return t.conn.CloseWithError(0, "client closed")
}

// Call sends a unary RPC request over QUIC and returns its response.
func (t *QuicClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	stream, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, transportOrContextStatus(ctx, err)
	}
	defer stream.CancelRead(cancelledStreamCode)
	stopCancel := cancelQUICStreamOnContext(ctx, stream)
	defer stopCancel()

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		stream.CancelWrite(cancelledStreamCode)
		return nil, transportOrContextStatus(ctx, err)
	}

	if err := stream.Close(); err != nil {
		return nil, transportOrContextStatus(ctx, err)
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, transportOrContextStatus(ctx, err)
	}

	return response, nil
}

// StreamingCall sends a streaming RPC request over QUIC and returns response frames.
func (t *QuicClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := t.conn.OpenStreamSync(streamCtx)
	if err != nil {
		cancel()
		return nil, transportOrContextStatus(streamCtx, err)
	}

	writerDone := make(chan error, 1)
	stopCancel := cancelQUICStreamOnContext(streamCtx, stream)
	go func() {
		writerDone <- writeStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()

	return &quicResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, stopCancel: stopCancel, maxFrameSize: t.maxFrameSize}, nil
}

type quicResponseStream struct {
	stream       *quic.Stream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
}

func (s *quicResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *quicResponseStream) Recv() (*RpcStreamFrame, error) {
	frame, _, err := s.trevrpcRecvStreamFrameFields()
	if err != nil {
		return nil, err
	}

	return frame.rpcStreamFrame(), nil
}

func (s *quicResponseStream) trevrpcRecvStreamFrameFields() (streamFrameFields, func(), error) {
	if s.done {
		return streamFrameFields{}, nil, io.EOF
	}

	fields, read, err := readStreamFrameFieldsOrEOF(s.stream, s.maxFrameSize)
	if err != nil {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, transportStatus(err)
	}

	if !read {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, io.EOF
	}

	if fields.kind == RpcStreamFrameKindStatus {
		s.finish(false)
		if fields.statusValue().IsOK() {
			if err := s.writerError(true); err != nil {
				return streamFrameFields{}, nil, err
			}
		} else {
			s.ignoreWriterError()
		}
	}

	return fields, nil, nil
}

func (s *quicResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *quicResponseStream) finish(cancelRead bool) {
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
		s.stream.CancelRead(cancelledStreamCode)
	}
	s.stream.CancelWrite(cancelledStreamCode)
}

func (s *quicResponseStream) writerError(ignoreCancelled bool) error {
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

func (s *quicResponseStream) ignoreWriterError() {
	s.writerDone = nil
}

func cancelQUICStreamOnContext(ctx context.Context, stream *quic.Stream) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			stream.CancelRead(cancelledStreamCode)
			stream.CancelWrite(cancelledStreamCode)
		case <-done:
		}
	}()

	return func() { closeOnce.Do(func() { close(done) }) }
}

func writeStreamingRequest(ctx context.Context, stream *quic.Stream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	defer closeMessageStream(requestBody)

	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledStreamCode)
		return transportOrContextStatus(ctx, err)
	}

	if err := writeRequestBodyFrames(ctx, stream, requestBody, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledStreamCode)
		return transportOrContextStatus(ctx, err)
	}

	if err := stream.Close(); err != nil {
		return transportOrContextStatus(ctx, err)
	}

	return nil
}

func writeRequestBodyFrames(ctx context.Context, writer io.Writer, requestBody ByteStream, maxFrameSize int) error {
	nonBlocking := isNonBlockingStream(requestBody)
	var batch [maxMessageFrameBatch][]byte
	for {
		count := 0
		done := false
		for count < len(batch) {
			body, err := recvRequestBody(ctx, requestBody)
			if err == io.EOF {
				done = true
				break
			}
			if err != nil {
				return err
			}

			batch[count] = body
			count++
			if !nonBlocking {
				break
			}
		}

		if count > 0 {
			if err := writeMessageStreamFrames(writer, batch[:count], maxFrameSize); err != nil {
				return err
			}
			clear(batch[:count])
		}
		if done {
			return nil
		}
	}
}

func recvRequestBody(ctx context.Context, requestBody ByteStream) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	if isNonBlockingStream(requestBody) {
		body, err := requestBody.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, statusFromContextError(ctxErr)
			}
		}

		return body, err
	}

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
		if result.err != nil {
			if err := ctx.Err(); err != nil {
				return nil, statusFromContextError(err)
			}
		}

		return result.body, result.err
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	}
}

// ServeQUIC accepts QUIC connections and serves TrevRPC until ctx is cancelled.
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
			if isWebTransportQUICConnection(conn, server.options) {
				handleWebTransportConnection(connectionsCtx, conn, server, requestLimit, true)
				return
			}

			handleQUICConnection(connectionsCtx, conn, server, requestLimit, true)
		})
	}

	stopConnections()
	waitForWaitGroup(&connectionTasks, server.options.GracefulShutdownTimeout, func() {
		closeActiveConnections("server graceful shutdown timed out")
	})

	return nil
}

// HandleQUICConnection serves TrevRPC streams on an accepted QUIC connection.
func HandleQUICConnection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore) {
	handleQUICConnection(ctx, conn, server, requestLimit, true)
}

func handleQUICConnection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore, closeOnShutdown bool) {
	if conn.ConnectionState().TLS.NegotiatedProtocol != ALPN {
		conn.CloseWithError(0, "unsupported ALPN")
		return
	}

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

		streamTasks.Go(func() {
			defer release(streamLimit)
			streamCtx, cancel := contextWithAdditionalCancel(stream.Context(), ctx)
			defer cancel()
			handleQUICStream(streamCtx, server, requestLimit, stream)
		})
	}

	waitForWaitGroup(&streamTasks, server.options.GracefulShutdownTimeout, func() {
		conn.CloseWithError(0, "server stream drain timed out")
	})

	if closeOnShutdown && ctx.Err() != nil {
		conn.CloseWithError(0, "server drained connection")
	}
}

func handleQUICStream(ctx context.Context, server *Server, requestLimit semaphore, stream *quic.Stream) {
	handleRPCStream(ctx, server, requestLimit, quicRPCStream{stream: stream})
}

type quicRPCStream struct {
	stream *quic.Stream
}

func (s quicRPCStream) Read(data []byte) (int, error) {
	return s.stream.Read(data)
}

func (s quicRPCStream) Write(data []byte) (int, error) {
	return s.stream.Write(data)
}

func (s quicRPCStream) Close() error {
	return s.stream.Close()
}

func (s quicRPCStream) SetReadDeadline(ttl time.Time) error {
	return s.stream.SetReadDeadline(ttl)
}

func (s quicRPCStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			s.stream.CancelRead(cancelledStreamCode)
		case <-done:
		}
	}()

	return func() { closeOnce.Do(func() { close(done) }) }
}

type rpcStream interface {
	io.Reader
	io.Writer
	Close() error
}

type transportResponseFrameWriter interface {
	trevrpcWriteNextFrame(context.Context, io.Writer, int) (bool, error)
}

func handleRPCStream(ctx context.Context, server *Server, requestLimit semaphore, stream rpcStream) {
	request := &RpcRequest{}
	if err := readInitialRequestFrame(ctx, server, stream, request); err != nil {
		status := requestFrameStatus(err)
		server.recordPreHandlerFailure(status)
		_ = WriteFrame(stream, status.IntoResponse(nil), server.options.MaxFrameSize)
		_ = stream.Close()
		return
	}
	if !tryAcquire(requestLimit) {
		status := Unavailable("too many concurrent RPCs")
		server.recordRejectedRequest(request, status)
		writeRPCStatus(stream, request, status, server.options.MaxFrameSize)
		return
	}
	defer release(requestLimit)

	if request.RPCKind() == RpcKindUnary {
		response := server.HandleRequest(ctx, request)
		if ctx.Err() != nil {
			return
		}
		if err := WriteFrame(stream, response, server.options.MaxFrameSize); err == nil {
			_ = stream.Close()
		}
		return
	}

	requestBody := &rpcRequestStream{stream: stream, maxFrameSize: server.options.MaxFrameSize}
	if request.RPCKind() == RpcKindClientStreaming || request.RPCKind() == RpcKindBidirectionalStreaming {
		if cancellable, ok := stream.(contextCancelReadStream); ok {
			requestBody.cancelReadOnContext = cancellable.trevrpcCancelReadOnContext
		}
	}
	response := server.HandleStreamingRequest(ctx, request, requestBody)
	defer closeMessageStream(response)
	if frameWriter, ok := response.(transportResponseFrameWriter); ok {
		for {
			done, err := frameWriter.trevrpcWriteNextFrame(ctx, stream, server.options.MaxFrameSize)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeMessageStream(response)
				return
			}
			if err != nil {
				return
			}
			if done {
				break
			}
		}

		_ = stream.Close()
		return
	}
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

func requestFrameStatus(err error) *Status {
	if err == nil {
		return OK()
	}

	var frameTooLarge *FrameTooLargeError
	if errors.As(err, &frameTooLarge) {
		return ResourceExhausted(frameTooLarge.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return statusFromContextError(err)
	}
	if isTimeoutError(err) {
		return DeadlineExceeded("initial request frame timeout")
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Unavailable("transport unavailable: " + err.Error())
	}

	return InvalidArgument("invalid RPC request frame: " + err.Error())
}

func isTimeoutError(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	var netError net.Error
	return errors.As(err, &netError) && netError.Timeout()
}

func recvResponseFrame(ctx context.Context, response FrameStream) (*RpcStreamFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNonBlockingStream(response) || streamContextCancelsRecv(response) {
		frame, err := response.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
		}

		return frame, err
	}

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
	}
}

type readDeadlineStream interface {
	SetReadDeadline(time.Time) error
}

func readInitialRequestFrame(ctx context.Context, server *Server, stream rpcStream, request *RpcRequest) error {
	if deadline, ok := readDeadline(ctx, server.options.InitialRequestTimeout); ok {
		if deadlineStream, ok := stream.(readDeadlineStream); ok {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer deadlineStream.SetReadDeadline(time.Time{})
		}
	}

	return ReadFrame(stream, request, server.options.MaxFrameSize)
}

func readDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	deadline, ok := ctx.Deadline()
	if timeout > 0 {
		requestDeadline := time.Now().Add(timeout)
		if !ok || requestDeadline.Before(deadline) {
			deadline = requestDeadline
			ok = true
		}
	}

	return deadline, ok
}

func writeStatusResponse(stream rpcStream, status *Status, maxFrameSize int) {
	_ = WriteFrame(stream, status.IntoResponse(nil), maxFrameSize)
	_ = stream.Close()
}

func writeRPCStatus(stream rpcStream, request *RpcRequest, status *Status, maxFrameSize int) {
	if request.RPCKind() == RpcKindUnary {
		writeStatusResponse(stream, status, maxFrameSize)
		return
	}

	_ = WriteFrame(stream, StatusFrame(status), maxFrameSize)
	_ = stream.Close()
}

type rpcRequestStream struct {
	stream              io.Reader
	maxFrameSize        int
	cancelReadOnContext func(context.Context) func()
	done                bool
}

func (s *rpcRequestStream) trevrpcContextCancelsRecv() bool {
	return s.cancelReadOnContext != nil
}

func (s *rpcRequestStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	if s.cancelReadOnContext == nil {
		return func() {}
	}

	return s.cancelReadOnContext(ctx)
}

func (s *rpcRequestStream) Recv() ([]byte, error) {
	body, _, err := s.recv(false)
	return body, err
}

func (s *rpcRequestStream) trevrpcRecvBytes() ([]byte, func(), error) {
	return s.recv(true)
}

func (s *rpcRequestStream) recv(releasable bool) ([]byte, func(), error) {
	if s.done {
		return nil, nil, io.EOF
	}

	frame, release, read, err := s.readFrame(releasable)
	if err != nil {
		s.done = true
		if release != nil {
			release()
		}
		return nil, nil, transportStatus(err)
	}

	if !read {
		s.done = true
		return nil, nil, io.EOF
	}

	if frame.kind == RpcStreamFrameKindStatus {
		s.done = true
		if release != nil {
			release()
		}
		status := frame.statusValue()
		if status.IsOK() {
			return nil, nil, io.EOF
		}

		return nil, nil, status
	}

	return frame.body, release, nil
}

func (s *rpcRequestStream) readFrame(releasable bool) (streamFrameFields, func(), bool, error) {
	if releasable {
		if reader, ok := s.stream.(optimizedReleasableStreamFrameReader); ok {
			return reader.trevrpcReadStreamFrameReleasable(s.maxFrameSize)
		}
	}

	frame, read, err := readStreamFrameFieldsOrEOF(s.stream, s.maxFrameSize)
	return frame, nil, read, err
}

func (s *rpcRequestStream) Close() error {
	s.done = true
	return nil
}

func transportOrContextStatus(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}

	return transportStatus(err)
}

func transportStatus(err error) error {
	if err == nil {
		return nil
	}

	var status *Status
	if errors.As(err, &status) {
		return status
	}

	var frameDecode *FrameDecodeError
	if errors.As(err, &frameDecode) {
		return InvalidArgument(frameDecode.Error())
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
