package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const maxMessageFrameBatch = 16

// writeRequestFrames writes header, the already encoded REQUEST frame, and
// then the messages of requests as MESSAGE frames until requests ends. When
// requests never blocks, header and the messages share batched writes;
// otherwise header goes out first, because the server must see the request
// before the caller has anything to send.
//
// A non-nil datagrams sends the messages that fit as datagrams instead, and
// only the others as frames. Its header always goes out first, on its own,
// because a datagram must not overtake the REQUEST by more than the network
// reorders it.
func writeRequestFrames(ctx context.Context, writer io.Writer, header []byte, requests encodedStream, datagrams *datagramSender, maxFrameSize int) error {
	nonBlocking := isNonBlockingStream(requests)
	if (!nonBlocking || datagrams != nil) && len(header) > 0 {
		if _, err := writer.Write(header); err != nil {
			return err
		}
		header = nil
	}

	// A sender that sends datagrams takes the messages that are ready along
	// with the one it waited for, so that they share datagrams.
	var ready readyReceiver[encodedMessage]
	if datagrams != nil && !nonBlocking {
		ready, _ = requests.(readyReceiver[encodedMessage])
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
				for ready != nil && count < len(batch) {
					next, ok, err := ready.recvReady()
					if err != nil {
						return err
					}
					if !ok {
						break
					}
					if next.size > maxFrameSize {
						return &FrameTooLargeError{Len: next.size, Max: maxFrameSize}
					}
					batch[count] = next
					count++
				}
				break
			}
		}

		if datagrams != nil && count > 0 {
			// The REQUEST was written above. What does not travel as a
			// datagram stays in the batch.
			kept, err := datagrams.send(batch[:count])
			if err != nil {
				return err
			}
			count = len(kept)
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

// messageFramesWriter is a writer that decides itself how header and messages
// are written; see optimisticUpload.
type messageFramesWriter interface {
	writeMessageFrames(header []byte, messages []encodedMessage) error
}

// writeMessageFrames writes header and messages, through writer's own
// writeMessageFrames if it has one.
func writeMessageFrames(writer io.Writer, header []byte, messages []encodedMessage) error {
	if framesWriter, ok := writer.(messageFramesWriter); ok {
		return framesWriter.writeMessageFrames(header, messages)
	}
	return writeFrames(writer, header, messages)
}

// writeFrames writes header and messages in one Write, without copying when
// that is only one message.
func writeFrames(writer io.Writer, header []byte, messages []encodedMessage) error {
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

// handleRPCStream runs one RPC on stream. slot is the connection's stream slot
// the RPC occupies, or nil. It is released before the terminal STATUS is
// written, so a client that sees the call complete finds the slot free.
func handleRPCStream(ctx context.Context, server *Server, stream rpcStream, slot *streamSlot) {
	defer slot.release()
	runtime := server.freeze()
	options := runtime.options
	startedAt := time.Now()
	reader := newFrameReader(stream, options.MaxFrameSize, options.MaxStreamBodySize)
	request, err := readRequestFrame(ctx, options, startedAt, stream, reader)
	if err != nil {
		status := requestFrameStatus(err)
		runtime.recordPreHandlerFailure(startedAt, status)
		writeStatusAndAbort(stream, status, options.MaxFrameSize, slot)
		return
	}
	// The connection's metadata joins the call's before anything reads it.
	err = streamConnectionMetadata(stream).resolve(request)
	if err == nil {
		err = validateRequest(request)
	}
	if err != nil {
		status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
		runtime.recordRequestFailure(startedAt, request, 0, status)
		writeStatusAndAbort(stream, status, options.MaxFrameSize, slot)
		return
	}
	// The server accepts every dictionary it holds on request messages, and
	// messages without a dictionary. Routes are fixed once the server runs,
	// so the call's own decides whether its responses may go without one.
	request.compression = runtime.compression.negotiate(request, runtime.routes[request.Method].plainCompression.Responses)
	inflate := runtime.compression.requestLookup(request)
	reader.inflate = inflate
	// Stream compression, negotiated once the call is authorized and routed,
	// ends with the call, whichever way it ends.
	defer func() { request.streams.close() }()
	if request.DatagramFlow != 0 {
		// A connection without datagrams ignores the flow.
		if datagrams, ok := stream.(datagramStream); ok {
			if mux := datagrams.trevrpcDatagrams(); mux != nil {
				flow, err := mux.register(request.DatagramFlow, request.rpcKind().streamsRequests(), options.MaxFrameSize, inflate)
				if err != nil {
					status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
					runtime.recordRequestFailure(startedAt, request, 0, status)
					writeStatusAndAbort(stream, status, options.MaxFrameSize, slot)
					return
				}
				// The flow ends with the call, whichever way it ends.
				defer flow.release()
				request.flow = flow
			}
		}
	}
	requestCtx, cancelRequest := requestLifetimeContext(ctx, request)
	defer cancelRequest()
	// The rest of a call that ends after its message is read in any case.
	reader.readAhead = requestEndsAfterMessage(request.rpcKind())

	var body []byte
	if requestEndsAfterMessage(request.rpcKind()) {
		body, err = readRequestMessage(requestCtx, options, startedAt, stream, reader)
		if err != nil {
			status := runtime.wireStatus(err, ServerDiagnosticInternalError, request)
			runtime.recordRequestFailure(startedAt, request, 0, status)
			writeStatusAndAbort(stream, status, options.MaxFrameSize, slot)
			return
		}
	}

	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		status := Unavailable("too many concurrent RPCs")
		runtime.recordRejectedRequest(startedAt, request, len(body), status)
		writeStatusAndAbort(stream, status, options.MaxFrameSize, slot)
		return
	}

	var response responseWriter
	// upload is the request stream of a call that the client keeps uploading
	// to after the request; nil for calls whose request is a single message.
	var upload *rpcRequestStream
	if request.rpcKind() == RPCKindUnary {
		response = runtime.handleRequest(requestCtx, request, body, lease)
		// The handler is done. Free the lease before the response goes out,
		// as the other kinds do when they finish, so a client that sees the
		// status finds the request limit free.
		lease.release()
	} else {
		reader.chargeMessages = true
		reader.readAhead = true
		requests := &rpcRequestStream{reader: reader}
		if request.flow != nil && request.flow.sink != nil {
			requests.merge = newRequestMerge(request.flow)
		}
		if request.rpcKind() == RPCKindClientStreaming || request.rpcKind() == RPCKindBidirectionalStreaming {
			upload = requests
			if requests.merge != nil {
				// Ends the goroutine that reads the request ahead of a call
				// with a flow, however the call ends.
				defer func() { _ = requests.Close() }()
			}
			if cancellable, ok := stream.(contextCancelReadStream); ok {
				requests.cancelReadOnContext = cancellable.trevrpcCancelReadOnContext
			}
			if cancellable, ok := stream.(immediateCancelReadStream); ok {
				requests.cancelRead = cancellable.trevrpcCancelRead
			}
		}
		response = runtime.handleStreamingRequest(requestCtx, request, body, requests, lease, stream)
	}
	defer discardMessageStream(response)

	for {
		done, err := response.writeNext(ctx, stream, options.MaxFrameSize, slot.release)
		if err != nil {
			discardMessageStream(response)
			slot.release()
			abortRPCStream(stream)
			return
		}
		if done {
			break
		}
	}

	slot.release()
	// A handler that finished without consuming every request leaves the
	// client's upload unread. Stop it, or the client's writes block on flow
	// control until its deadline.
	if upload != nil && !upload.finished() {
		cancelStreamRead(stream)
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

	netError, ok := errors.AsType[net.Error](err)
	return ok && netError.Timeout()
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
		err = decodeRequest(first.payload, request)
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
// lone STATUS and aborts the stream. It releases slot, if any, before writing.
func writeStatusAndAbort(stream rpcStream, status *Status, maxFrameSize int, slot *streamSlot) {
	frames, err := appendStatusFrame(nil, status, maxFrameSize)
	slot.release()
	if err == nil {
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
	// merge is set for a call that receives its request messages as datagrams
	// as well; see requestMerge.
	merge *requestMerge
	// done and ended are atomic because Close may run on another goroutine
	// than Recv, and the connection handler reads ended after the response.
	done atomic.Bool
	// ended is set once the client finished its request side: the stream
	// reached its end or carried an OK STATUS.
	ended atomic.Bool
}

// finished reports whether the client finished its upload, so that nothing is
// left to read or stop.
func (s *rpcRequestStream) finished() bool { return s.ended.Load() }

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
	if s.done.Load() {
		return nil, io.EOF
	}
	if s.merge != nil {
		return s.recvMerged()
	}

	next, err := s.reader.next()
	return s.receive(next, err)
}

// receive turns the frame the reader returned into the result of Recv.
func (s *rpcRequestStream) receive(next frame, err error) ([]byte, error) {
	if err != nil {
		s.done.Store(true)
		if errors.Is(err, io.EOF) {
			s.ended.Store(true)
			return nil, io.EOF
		}
		return nil, transportStatus(err)
	}

	switch next.typ {
	case frameTypeMessage:
		return next.payload, nil
	case frameTypeStatus:
		s.done.Store(true)
		status, err := decodeStatus(next.payload)
		if err != nil {
			return nil, transportStatus(err)
		}
		if status.IsOK() {
			s.ended.Store(true)
			return nil, io.EOF
		}

		return nil, status
	default:
		s.done.Store(true)
		return nil, InvalidArgument(fmt.Sprintf("request stream carried %s after REQUEST", next.typ))
	}
}

func (s *rpcRequestStream) Close() error {
	s.done.Store(true)
	if s.merge != nil {
		s.merge.stopOnce.Do(func() { close(s.merge.stop) })
	}
	return nil
}

// requestMerge merges the request messages of a call that arrive as datagrams
// with its stream frames. The stream is read by a goroutine of its own, so
// that a pending Recv wakes on a datagram without waiting for the next frame.
type requestMerge struct {
	flow *datagramFlow
	// items carries what the goroutine read; it is unbuffered, so the goroutine
	// is at most one frame ahead of Recv. It is created, with the goroutine,
	// by the first Recv.
	items chan requestItem
	// stop ends the goroutine; Close closes it.
	stop     chan struct{}
	stopOnce sync.Once
	// terminal is the frame that ended the request, held back until the
	// datagram messages queued before it were delivered.
	terminal *requestItem
}

type requestItem struct {
	frame frame
	err   error
}

func newRequestMerge(flow *datagramFlow) *requestMerge {
	return &requestMerge{flow: flow, stop: make(chan struct{})}
}

// readLoop reads the request frames. Once the request ended, the datagram
// direction ends with it: later datagrams are dropped.
func (m *requestMerge) readLoop(reader *frameReader) {
	for {
		next, err := reader.next()
		terminal := err != nil || next.typ != frameTypeMessage
		if terminal {
			m.flow.endReceive()
		}
		select {
		case m.items <- requestItem{frame: next, err: err}:
		case <-m.stop:
			return
		}
		if terminal {
			return
		}
	}
}

// recvMerged returns the next request message, whether it came as a frame or
// as a datagram, in the order they arrive. The messages queued when the
// request ends are delivered before it ends. The call's message and byte
// limits and idle timer apply to the result, as to any other.
func (s *rpcRequestStream) recvMerged() ([]byte, error) {
	m := s.merge
	sink := m.flow.sink
	if m.items == nil {
		m.items = make(chan requestItem)
		go m.readLoop(s.reader)
	}
	for {
		if m.terminal != nil {
			if message, ok := sink.pop(); ok {
				m.flow.received.Add(1)
				return message, nil
			}
			item := *m.terminal
			m.terminal = nil
			return s.receive(item.frame, item.err)
		}
		select {
		case item := <-m.items:
			switch {
			case item.err == nil && item.frame.typ == frameTypeMessage:
				return item.frame.payload, nil
			case item.err != nil && !errors.Is(item.err, io.EOF):
				// The stream failed: what is queued is not delivered.
				return s.receive(item.frame, item.err)
			}
			m.terminal = &item
		case <-sink.wake:
			if message, ok := sink.pop(); ok {
				m.flow.received.Add(1)
				return message, nil
			}
		case <-m.stop:
			s.done.Store(true)
			return nil, io.EOF
		}
	}
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

// streamSlot is a slot taken from a connection's stream limit. It is released
// at most once, however many paths try.
type streamSlot struct {
	limit    semaphore
	released atomic.Bool
}

// acquireStreamSlot takes a slot from limit. It returns false when the limit
// is reached. An unlimited connection yields a nil slot, which is valid.
func acquireStreamSlot(limit semaphore) (*streamSlot, bool) {
	if limit == nil {
		return nil, true
	}
	if !tryAcquire(limit) {
		return nil, false
	}
	return &streamSlot{limit: limit}, true
}

// release frees the slot. It is safe on a nil slot and to call repeatedly.
func (s *streamSlot) release() {
	if s != nil && s.released.CompareAndSwap(false, true) {
		release(s.limit)
	}
}
