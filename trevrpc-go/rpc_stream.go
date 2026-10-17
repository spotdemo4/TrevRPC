package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const maxMessageFrameBatch = 16

// writeRequestFrames writes header, the already encoded REQUEST frame, and
// then the messages of requests as MESSAGE frames until requests ends. When
// requests never blocks, header and the messages share batched writes;
// otherwise header goes out first, because the server must see the request
// before the caller has anything to send.
func writeRequestFrames(ctx context.Context, writer io.Writer, header []byte, requests encodedStream, maxFrameSize int) error {
	nonBlocking := isNonBlockingStream(requests)
	if !nonBlocking && len(header) > 0 {
		if _, err := writer.Write(header); err != nil {
			return err
		}
		header = nil
	}

	var batch [maxMessageFrameBatch]encodedMessage
	for {
		count := 0
		done := false
		for count < len(batch) {
			message, err := recvRequestBody(ctx, requests)
			if errors.Is(err, io.EOF) {
				done = true
				break
			}
			if err != nil {
				return err
			}
			if message.size > maxFrameSize {
				return &FrameTooLargeError{Len: message.size, Max: maxFrameSize}
			}

			batch[count] = message
			count++
			if !nonBlocking {
				break
			}
		}

		if count > 0 || len(header) > 0 {
			if err := writeMessageFrames(writer, header, batch[:count]); err != nil {
				return err
			}
			header = nil
			clear(batch[:count])
		}
		if done {
			return nil
		}
	}
}

// writeMessageFrames writes header and messages in one Write, without copying
// when that is only one message.
func writeMessageFrames(writer io.Writer, header []byte, messages []encodedMessage) error {
	if len(header) == 0 && len(messages) == 1 {
		_, err := writer.Write(messages[0].frame)
		return err
	}
	total := len(header)
	for _, message := range messages {
		total += len(message.frame)
	}
	frames := make([]byte, 0, total)
	frames = append(frames, header...)
	for _, message := range messages {
		frames = append(frames, message.frame...)
	}
	_, err := writer.Write(frames)
	return err
}

func recvRequestBody(ctx context.Context, requests encodedStream) (encodedMessage, error) {
	if err := ctx.Err(); err != nil {
		return encodedMessage{}, statusFromContextError(err)
	}
	if isNonBlockingStream(requests) || streamContextCancelsRecv(requests) {
		message, err := requests.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return encodedMessage{}, statusFromContextError(ctxErr)
			}
		}

		return message, err
	}

	type recvResult struct {
		message encodedMessage
		err     error
	}

	results := make(chan recvResult, 1)
	go func() {
		message, err := requests.Recv()
		results <- recvResult{message: message, err: err}
	}()

	select {
	case result := <-results:
		if result.err != nil {
			if err := ctx.Err(); err != nil {
				return encodedMessage{}, statusFromContextError(err)
			}
		}

		return result.message, result.err
	case <-ctx.Done():
		return encodedMessage{}, statusFromContextError(ctx.Err())
	}
}

type rpcStream interface {
	io.Reader
	io.Writer
	Close() error
}

func handleRPCStream(ctx context.Context, server *Server, stream rpcStream) {
	runtime := server.freeze()
	options := runtime.options
	startedAt := time.Now()
	reader := newFrameReader(stream, options.MaxFrameSize, options.MaxStreamBodySize)
	request, err := readRequestFrame(ctx, options, startedAt, stream, reader)
	if err != nil {
		status := requestFrameStatus(err)
		runtime.recordPreHandlerFailure(startedAt, status)
		writeStatusAndAbort(stream, status, options.MaxFrameSize)
		return
	}
	if err := validateRequest(request); err != nil {
		status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
		runtime.recordRequestFailure(startedAt, request, 0, status)
		writeStatusAndAbort(stream, status, options.MaxFrameSize)
		return
	}
	requestCtx, cancelRequest := requestLifetimeContext(ctx, request)
	defer cancelRequest()

	var body []byte
	if requestEndsAfterMessage(request.rpcKind()) {
		body, err = readRequestMessage(requestCtx, options, startedAt, stream, reader)
		if err != nil {
			status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
			runtime.recordRequestFailure(startedAt, request, 0, status)
			writeStatusAndAbort(stream, status, options.MaxFrameSize)
			return
		}
	}

	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		status := Unavailable("too many concurrent RPCs")
		runtime.recordRejectedRequest(startedAt, request, len(body), status)
		writeStatusAndAbort(stream, status, options.MaxFrameSize)
		return
	}

	var response responseWriter
	if request.rpcKind() == RPCKindUnary {
		defer lease.release()
		response = runtime.handleRequest(requestCtx, request, body, lease)
	} else {
		reader.chargeMessages = true
		requests := &rpcRequestStream{reader: reader}
		if request.rpcKind() == RPCKindClientStreaming || request.rpcKind() == RPCKindBidirectionalStreaming {
			if cancellable, ok := stream.(contextCancelReadStream); ok {
				requests.cancelReadOnContext = cancellable.trevrpcCancelReadOnContext
			}
			if cancellable, ok := stream.(immediateCancelReadStream); ok {
				requests.cancelRead = cancellable.trevrpcCancelRead
			}
		}
		response = runtime.handleStreamingRequest(requestCtx, request, body, requests, lease)
	}
	defer discardMessageStream(response)

	for {
		done, err := response.writeNext(ctx, stream, options.MaxFrameSize)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			discardMessageStream(response)
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
}

func requestFrameStatus(err error) *Status {
	if err == nil {
		return OK()
	}

	if status, ok := errors.AsType[*Status](err); ok {
		return status
	}
	if frameTooLarge, ok := errors.AsType[*FrameTooLargeError](err); ok {
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

// readRequestFrame reads the REQUEST frame that opens a request stream.
func readRequestFrame(ctx context.Context, options ServerOptions, startedAt time.Time, stream rpcStream, reader *frameReader) (*wireRequest, error) {
	stopCancelRead := cancelReadOnContext(ctx, stream)
	defer stopCancelRead()

	if deadline, ok := readDeadline(ctx, startedAt, options.InitialRequestTimeout); ok {
		if deadlineStream, ok := stream.(readDeadlineStream); ok {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer func() { _ = deadlineStream.SetReadDeadline(time.Time{}) }()
		}
	}

	request := &wireRequest{}
	first, err := reader.next()
	if err == nil && first.typ != frameTypeRequest {
		err = InvalidArgument(fmt.Sprintf("request stream must start with REQUEST, not %s", first.typ))
	}
	if err == nil {
		err = decodePayload(first.payload, request)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	return request, nil
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

// readRequestMessage reads the single MESSAGE of a unary or server-streaming
// request and then waits for the end of the request stream. Frames of unknown
// type are skipped; a missing message, a second one, or any other known frame
// is InvalidArgument.
func readRequestMessage(ctx context.Context, options ServerOptions, startedAt time.Time, stream rpcStream, reader *frameReader) ([]byte, error) {
	stopCancelRead := cancelReadOnContext(ctx, stream)
	defer stopCancelRead()

	if deadline, ok := readDeadline(ctx, startedAt, options.InitialRequestTimeout); ok {
		if deadlineStream, ok := stream.(readDeadlineStream); ok {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer func() { _ = deadlineStream.SetReadDeadline(time.Time{}) }()
		}
	}

	body, err := readSingleMessage(reader)
	if err != nil {
		if _, ok := errors.AsType[*Status](err); ok {
			return nil, err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, statusFromContextError(ctxErr)
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return nil, DeadlineExceeded("initial request completion timeout")
		}
		return nil, transportStatus(err)
	}
	return body, nil
}

func readSingleMessage(reader *frameReader) ([]byte, error) {
	message, err := reader.next()
	if errors.Is(err, io.EOF) {
		return nil, InvalidArgument("request stream ended before the request message")
	}
	if err != nil {
		return nil, err
	}
	if message.typ != frameTypeMessage {
		return nil, InvalidArgument(fmt.Sprintf("request stream carried %s where the request message belongs", message.typ))
	}

	extra, err := reader.next()
	if errors.Is(err, io.EOF) {
		return message.payload, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, InvalidArgument(fmt.Sprintf("request stream carried %s after the request message", extra.typ))
}

// requestEndsAfterMessage reports whether the request of kind is exactly one
// message, which the server reads before dispatching.
func requestEndsAfterMessage(kind RPCKind) bool {
	return kind == RPCKindUnary || kind == RPCKindServerStreaming
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

// writeStatusAndAbort answers a request that failed before any response with a
// lone STATUS and aborts the stream.
func writeStatusAndAbort(stream rpcStream, status *Status, maxFrameSize int) {
	if frames, err := appendStatusFrame(nil, status, maxFrameSize); err == nil {
		_, _ = stream.Write(frames)
	}
	abortRPCStream(stream)
}

// rpcRequestStream yields the MESSAGE payloads of a client-streaming or
// bidirectional request. A request STATUS ends the stream: OK as io.EOF and any
// other status as an error.
type rpcRequestStream struct {
	reader              *frameReader
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
	if s.done {
		return nil, io.EOF
	}

	next, err := s.reader.next()
	if err != nil {
		s.done = true
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, transportStatus(err)
	}

	switch next.typ {
	case frameTypeMessage:
		return next.payload, nil
	case frameTypeStatus:
		s.done = true
		status, err := decodeStatus(next.payload)
		if err != nil {
			return nil, transportStatus(err)
		}
		if status.IsOK() {
			return nil, io.EOF
		}

		return nil, status
	default:
		s.done = true
		return nil, InvalidArgument(fmt.Sprintf("request stream carried %s after REQUEST", next.typ))
	}
}

func (s *rpcRequestStream) Close() error {
	s.done = true
	return nil
}

func transportStatus(err error) error {
	if err == nil {
		return nil
	}

	if status, ok := errors.AsType[*Status](err); ok {
		return status
	}

	if frameDecode, ok := errors.AsType[*frameDecodeError](err); ok {
		return InvalidArgument(frameDecode.Error())
	}

	if frameTooLarge, ok := errors.AsType[*FrameTooLargeError](err); ok {
		return ResourceExhausted(frameTooLarge.Error())
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
