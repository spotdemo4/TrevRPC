package trevrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const maxMessageFrameBatch = 16

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
	if isNonBlockingStream(requestBody) || streamContextCancelsRecv(requestBody) {
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

type rpcStream interface {
	io.Reader
	io.Writer
	Close() error
}

type transportResponseFrameWriter interface {
	trevrpcWriteNextFrame(context.Context, io.Writer, int) (bool, error)
}

type transportResponseFramesWriter interface {
	trevrpcWriteNextFrames(context.Context, io.Writer, int) (bool, error)
}

func handleRPCStream(ctx context.Context, server *Server, _ semaphore, stream rpcStream) {
	runtime := server.freeze()
	startedAt := time.Now()
	request := &RpcRequest{}
	if err := readInitialRequestFrame(ctx, runtime.options, startedAt, stream, request); err != nil {
		status := requestFrameStatus(err)
		runtime.recordPreHandlerFailure(startedAt, status)
		_ = WriteFrame(stream, status.IntoResponse(nil), runtime.options.MaxFrameSize)
		abortRPCStream(stream)
		return
	}
	if err := validateRequest(request); err != nil {
		status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
		runtime.recordRequestFailure(startedAt, request, status)
		writeRPCStatus(runtime.options, stream, request, status)
		return
	}
	requestCtx, cancelRequest := requestLifetimeContext(ctx, request)
	defer cancelRequest()

	if requestEndsAfterInitialFrame(request.RPCKind()) {
		if err := drainRequestEnd(requestCtx, runtime.options, startedAt, stream); err != nil {
			status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
			runtime.recordRequestFailure(startedAt, request, status)
			writeRPCStatus(runtime.options, stream, request, status)
			return
		}
	}

	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		status := Unavailable("too many concurrent RPCs")
		runtime.recordRejectedRequest(startedAt, request, status)
		writeRPCStatus(runtime.options, stream, request, status)
		return
	}

	if request.RPCKind() == RpcKindUnary {
		defer lease.release()
		response := runtime.handleRequest(requestCtx, request, lease)
		if ctx.Err() != nil {
			abortRPCStream(stream)
			return
		}
		if err := WriteFrame(stream, response, runtime.options.MaxFrameSize); err != nil {
			abortRPCStream(stream)
			return
		}
		_ = stream.Close()
		return
	}

	requestBody := &rpcRequestStream{stream: stream, maxFrameSize: runtime.options.MaxFrameSize}
	if request.RPCKind() == RpcKindClientStreaming || request.RPCKind() == RpcKindBidirectionalStreaming {
		if cancellable, ok := stream.(contextCancelReadStream); ok {
			requestBody.cancelReadOnContext = cancellable.trevrpcCancelReadOnContext
		}
		if cancellable, ok := stream.(immediateCancelReadStream); ok {
			requestBody.cancelRead = cancellable.trevrpcCancelRead
		}
	}
	response := runtime.handleStreamingRequest(requestCtx, request, requestBody, lease)
	defer closeMessageStream(response)
	if frameWriter, ok := response.(transportResponseFramesWriter); ok {
		for {
			done, err := frameWriter.trevrpcWriteNextFrames(ctx, stream, runtime.options.MaxFrameSize)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeMessageStream(response)
				abortRPCStream(stream)
				return
			}
			if err != nil {
				abortRPCStream(stream)
				return
			}
			if done {
				break
			}
		}

		_ = stream.Close()
		return
	}
	if frameWriter, ok := response.(transportResponseFrameWriter); ok {
		for {
			done, err := frameWriter.trevrpcWriteNextFrame(ctx, stream, runtime.options.MaxFrameSize)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeMessageStream(response)
				abortRPCStream(stream)
				return
			}
			if err != nil {
				abortRPCStream(stream)
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
			abortRPCStream(stream)
			return
		}

		if err != nil {
			frame = StatusFrame(runtime.wireStatus(err, ServerDiagnosticInternalError, request))
		}

		isStatus := frame.Kind == RpcStreamFrameKindStatus
		if err := WriteFrame(stream, frame, runtime.options.MaxFrameSize); err != nil {
			abortRPCStream(stream)
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

func readInitialRequestFrame(ctx context.Context, options ServerOptions, startedAt time.Time, stream rpcStream, request *RpcRequest) error {
	stopCancelRead := cancelReadOnContext(ctx, stream)
	defer stopCancelRead()

	if deadline, ok := readDeadline(ctx, startedAt, options.InitialRequestTimeout); ok {
		if deadlineStream, ok := stream.(readDeadlineStream); ok {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer deadlineStream.SetReadDeadline(time.Time{})
		}
	}

	err := ReadFrame(stream, request, options.MaxFrameSize)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func readDeadline(ctx context.Context, startedAt time.Time, timeout time.Duration) (time.Time, bool) {
	deadline, ok := ctx.Deadline()
	if timeout > 0 {
		requestDeadline := startedAt.Add(timeout)
		if !ok || requestDeadline.Before(deadline) {
			deadline = requestDeadline
			ok = true
		}
	}

	return deadline, ok
}

func drainRequestEnd(ctx context.Context, options ServerOptions, startedAt time.Time, stream rpcStream) error {
	stopCancelRead := cancelReadOnContext(ctx, stream)
	defer stopCancelRead()
	return readRequestEnd(ctx, options, startedAt, stream)
}

func readRequestEnd(ctx context.Context, options ServerOptions, startedAt time.Time, stream rpcStream) error {
	if deadline, ok := readDeadline(ctx, startedAt, options.InitialRequestTimeout); ok {
		if deadlineStream, ok := stream.(readDeadlineStream); ok {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer deadlineStream.SetReadDeadline(time.Time{})
		}
	}

	var buf [1024]byte
	for {
		read, err := stream.Read(buf[:])
		if read > 0 {
			return InvalidArgument("request stream contained data after the initial request frame")
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return statusFromContextError(ctxErr)
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return DeadlineExceeded("initial request completion timeout")
			}
			return transportStatus(err)
		}
	}
}

func requestEndsAfterInitialFrame(kind RpcKind) bool {
	return kind == RpcKindUnary || kind == RpcKindServerStreaming
}

func abortRPCStream(stream rpcStream) {
	_ = stream.Close()
	cancelStreamRead(stream)
}

func cancelReadOnContext(ctx context.Context, stream rpcStream) func() {
	if cancellable, ok := stream.(contextCancelReadStream); ok {
		return cancellable.trevrpcCancelReadOnContext(ctx)
	}
	return func() {}
}

func writeStatusResponse(stream rpcStream, status *Status, maxFrameSize int) {
	_ = WriteFrame(stream, status.IntoResponse(nil), maxFrameSize)
	abortRPCStream(stream)
}

func writeRPCStatus(options ServerOptions, stream rpcStream, request *RpcRequest, status *Status) {
	if request.RPCKind() == RpcKindUnary {
		_ = WriteFrame(stream, status.IntoResponse(nil), options.MaxFrameSize)
	} else {
		_ = WriteFrame(stream, StatusFrame(status), options.MaxFrameSize)
	}
	abortRPCStream(stream)
}

type rpcRequestStream struct {
	stream              io.Reader
	maxFrameSize        int
	cancelReadOnContext func(context.Context) func()
	cancelRead          func()
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

func (s *rpcRequestStream) trevrpcCancelRead() {
	if s.cancelRead != nil {
		s.cancelRead()
	}
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
