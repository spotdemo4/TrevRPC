package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Transport carries RPCs for generated clients. *Channel implements it; the
// interface is sealed because requests and responses use the internal wire
// format.
type Transport interface {
	// call sends a request and its single message, as unary and
	// server-streaming RPCs do, and returns the response frames.
	call(context.Context, *wireRequest, ProtoMessage) (frameStream, error)
	// streamingCall sends a request followed by the messages of the stream, as
	// client-streaming and bidirectional RPCs do, and returns the response frames.
	streamingCall(context.Context, *wireRequest, encodedStream) (frameStream, error)
}

// ServerStreamingCall is the response stream of a server-streaming RPC.
type ServerStreamingCall[Res ProtoMessage] interface {
	ResponseStream[Res]
	// Headers returns the response headers. It blocks until the first response
	// frame (headers, a message, or the final status) arrives and returns empty
	// metadata if the server sent no headers. It does not consume a message:
	// Recv still returns it.
	Headers() (Metadata, error)
}

// ClientStreamingCall sends request messages and receives the final response.
type ClientStreamingCall[Req ProtoMessage, Res ProtoMessage] interface {
	// Send sends one request message. Once the server has ended the call, Send
	// stops uploading and fails promptly instead of blocking until the call
	// deadline. It returns the terminal status as the error when that status is
	// not OK, so Send(...) and the final error agree, and io.EOF when the
	// server finished the call successfully without reading every message.
	// The same applies to blocked Send calls.
	Send(Req) error
	// CloseAndRecv closes the request stream and returns the unary response.
	// Response headers and trailing metadata are reported through
	// CaptureResponseHeaders and CaptureResponseMetadata.
	CloseAndRecv() (Res, error)
	// Close releases call resources without waiting for a response.
	Close() error
}

// BidirectionalStreamingCall sends request messages and receives response messages.
type BidirectionalStreamingCall[Req ProtoMessage, Res ProtoMessage] interface {
	// Send sends one request message. Once the server has ended the call, Send
	// stops uploading and fails promptly instead of blocking until the call
	// deadline. It returns the terminal status as the error when that status is
	// not OK, so Send(...) and the final error agree, and io.EOF when the
	// server finished the call successfully without reading every message.
	// The same applies to blocked Send calls.
	Send(Req) error
	// Recv receives one response message or io.EOF after the response stream completes.
	Recv() (Res, error)
	// CloseSend closes the request stream while keeping the response stream readable.
	CloseSend() error
	// Close releases call resources without waiting for more responses.
	Close() error
	// TerminalStatus returns the server's final status, including its
	// metadata, once the status frame has been consumed.
	TerminalStatus() (*Status, bool)
	// Headers returns the response headers. It blocks until the first response
	// frame (headers, a message, or the final status) arrives and returns empty
	// metadata if the server sent no headers. It does not consume a message:
	// Recv still returns it.
	Headers() (Metadata, error)
}

// callConfig holds the resolved options of one RPC call.
type callConfig struct {
	timeout          time.Duration
	hasTimeout       bool
	maxResponseSize  int
	metadata         Metadata
	responseHeaders  *Metadata
	responseMetadata *Metadata
}

// CallOption customizes a single RPC call.
type CallOption func(*callConfig)

func defaultCallConfig() callConfig {
	return callConfig{maxResponseSize: DefaultMaxFrameSize, metadata: Metadata{}}
}

// WithTimeout bounds the RPC by timeout. The earlier of this and any deadline
// on the call context applies; there is no timeout by default.
func WithTimeout(timeout time.Duration) CallOption {
	return func(config *callConfig) {
		config.timeout = timeout
		config.hasTimeout = true
	}
}

// WithMetadata adds one request metadata entry. Keys are lowercased; repeating
// a key replaces its value.
func WithMetadata(key string, value []byte) CallOption {
	key = NormalizeMetadataKey(key)
	value = append([]byte(nil), value...)
	return func(config *callConfig) {
		metadata := cloneMetadata(config.metadata)
		metadata[key] = append([]byte(nil), value...)
		config.metadata = metadata
	}
}

// WithMaxFrameSize bounds the encoded size of each response message in bytes.
// It defaults to DefaultMaxFrameSize; the channel's own frame size limit still applies.
func WithMaxFrameSize(size int) CallOption {
	return func(config *callConfig) {
		config.maxResponseSize = size
	}
}

// CaptureResponseHeaders stores the response headers of a successful unary or
// client-streaming call in *dst, replacing its previous value. It is written
// before the call returns (for client streaming, before CloseAndRecv returns)
// and is not touched when the call fails. Streaming responses report headers
// through Headers instead, so server- and bidirectional-streaming calls ignore
// this option.
func CaptureResponseHeaders(dst *Metadata) CallOption {
	return func(config *callConfig) {
		config.responseHeaders = dst
	}
}

// CaptureResponseMetadata stores the trailing metadata of a successful unary or
// client-streaming call in *dst, replacing its previous value. It is written
// before the call returns (for client streaming, before CloseAndRecv returns)
// and is not touched when the call fails; the metadata of a failed call is on
// the returned *Status. Streaming responses report trailing metadata through
// TerminalStatus instead, so server- and bidirectional-streaming calls ignore
// this option.
func CaptureResponseMetadata(dst *Metadata) CallOption {
	return func(config *callConfig) {
		config.responseMetadata = dst
	}
}

// Unary calls a unary RPC and decodes the protobuf response.
func Unary[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (Res, error) {
	var zero Res
	config := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, config, time.Now)
	if err != nil {
		return zero, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RPCKindUnary, config, deadline, time.Now)
	if err != nil {
		return zero, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, deadline)
	defer cancel()

	frames, err := transport.call(ctx, rpcRequest, request)
	if err != nil {
		return zero, err
	}

	message, headers, trailers, err := readSingleResponse(newResponseMessageStream(ctx, frames, newResponse, config, cancel))
	if err != nil {
		return zero, err
	}

	config.captureResponseHeaders(headers)
	config.captureResponseMetadata(trailers)
	return message, nil
}

// ServerStreaming calls a server-streaming RPC and returns its response stream.
// The terminal status and trailing metadata are available from TerminalStatus
// after Recv reports io.EOF or the stream's error, and the response headers
// from Headers.
func ServerStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (ServerStreamingCall[Res], error) {
	config := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, config, time.Now)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RPCKindServerStreaming, config, deadline, time.Now)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, deadline)
	frames, err := transport.call(ctx, rpcRequest, request)
	if err != nil {
		cancel()
		return nil, err
	}

	return newResponseMessageStream(ctx, frames, newResponse, config, cancel), nil
}

// ClientStreaming starts a client-streaming RPC.
func ClientStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (ClientStreamingCall[Req, Res], error) {
	config := applyCallOptions(options)
	requests, responses, cancel, err := startStreamingCall[Req](ctx, transport, service, method, RPCKindClientStreaming, newResponse, config)
	if err != nil {
		return nil, err
	}

	return &clientStreamingCall[Req, Res]{requests: requests, responses: responses, cancel: cancel, config: config}, nil
}

// BidirectionalStreaming starts a bidirectional-streaming RPC.
func BidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (BidirectionalStreamingCall[Req, Res], error) {
	requests, responses, _, err := startStreamingCall[Req](ctx, transport, service, method, RPCKindBidirectionalStreaming, newResponse, applyCallOptions(options))
	if err != nil {
		return nil, err
	}

	return &bidirectionalStreamingCall[Req, Res]{requests: requests, responses: responses}, nil
}

// startStreamingCall opens a streaming RPC whose requests are sent through the
// returned pipe.
func startStreamingCall[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, kind RPCKind, newResponse func() Res, config callConfig) (*messagePipe[Req], *responseMessageStream[Res], context.CancelFunc, error) {
	deadline, err := resolveClientDeadline(ctx, config, time.Now)
	if err != nil {
		return nil, nil, nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, kind, config, deadline, time.Now)
	if err != nil {
		return nil, nil, nil, err
	}

	ctx, cancel := callContext(ctx, deadline)
	requests := newMessagePipe[Req](ctx)
	response, err := transport.streamingCall(ctx, rpcRequest, newEncodeStream(requests))
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	return requests, newResponseMessageStream(ctx, response, newResponse, config, cancel), cancel, nil
}

type clientStreamingCall[Req ProtoMessage, Res ProtoMessage] struct {
	requests  *messagePipe[Req]
	responses *responseMessageStream[Res]
	cancel    context.CancelFunc
	config    callConfig
}

func (c *clientStreamingCall[Req, Res]) Send(request Req) error {
	return c.requests.Send(request)
}

func (c *clientStreamingCall[Req, Res]) CloseAndRecv() (Res, error) {
	var zero Res
	if err := c.requests.Close(); err != nil {
		return zero, err
	}

	message, headers, trailers, err := readSingleResponse(c.responses)
	if err != nil {
		return zero, err
	}
	c.config.captureResponseHeaders(headers)
	c.config.captureResponseMetadata(trailers)
	return message, nil
}

func (c *clientStreamingCall[Req, Res]) Close() error {
	c.cancel()
	return errors.Join(c.requests.Close(), closeMessageStream(c.responses))
}

type bidirectionalStreamingCall[Req ProtoMessage, Res ProtoMessage] struct {
	requests  *messagePipe[Req]
	responses *responseMessageStream[Res]
}

func (c *bidirectionalStreamingCall[Req, Res]) Send(request Req) error {
	return c.requests.Send(request)
}

func (c *bidirectionalStreamingCall[Req, Res]) Recv() (Res, error) {
	return c.responses.Recv()
}

func (c *bidirectionalStreamingCall[Req, Res]) CloseSend() error {
	return c.requests.Close()
}

func (c *bidirectionalStreamingCall[Req, Res]) Close() error {
	return errors.Join(c.requests.Close(), closeMessageStream(c.responses))
}

func (c *bidirectionalStreamingCall[Req, Res]) TerminalStatus() (*Status, bool) {
	return c.responses.TerminalStatus()
}

func (c *bidirectionalStreamingCall[Req, Res]) Headers() (Metadata, error) {
	return c.responses.Headers()
}

func applyCallOptions(options []CallOption) callConfig {
	config := defaultCallConfig()
	for _, option := range options {
		option(&config)
	}

	return config
}

func (c callConfig) captureResponseHeaders(headers Metadata) {
	if c.responseHeaders != nil {
		*c.responseHeaders = cloneMetadata(headers)
	}
}

func (c callConfig) captureResponseMetadata(metadata Metadata) {
	if c.responseMetadata != nil {
		*c.responseMetadata = cloneMetadata(metadata)
	}
}

type clientDeadline struct {
	at  time.Time
	set bool
}

func resolveClientDeadline(ctx context.Context, config callConfig, now func() time.Time) (clientDeadline, error) {
	if config.hasTimeout && config.timeout < 0 {
		return clientDeadline{}, InvalidArgument("RPC timeout is negative")
	}

	deadline, hasDeadline := ctx.Deadline()
	if config.hasTimeout {
		optionDeadline := now().Add(config.timeout)
		if !hasDeadline || optionDeadline.Before(deadline) {
			deadline = optionDeadline
			hasDeadline = true
		}
	}

	return clientDeadline{at: deadline, set: hasDeadline}, nil
}

func prepareClientRequest(service, method string, kind RPCKind, config callConfig, deadline clientDeadline, now func() time.Time) (*wireRequest, error) {
	metadata := cloneMetadata(config.metadata)
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}

	request := newWireRequest(service, method)
	request.Kind = kind
	request.Metadata = metadata
	request.TimeoutNanos = deadlineTimeoutNanos(deadline, now)

	return request, nil
}

func deadlineTimeoutNanos(deadline clientDeadline, now func() time.Time) uint64 {
	if !deadline.set {
		return 0
	}

	remaining := deadline.at.Sub(now())
	if remaining <= 0 {
		return 1
	}

	return uint64(remaining)
}

func callContext(ctx context.Context, deadline clientDeadline) (context.Context, context.CancelFunc) {
	if deadline.set {
		return context.WithDeadline(ctx, deadline.at)
	}

	return context.WithCancel(ctx)
}

func callContextWithoutLocalCancel(ctx context.Context, deadline clientDeadline) (context.Context, context.CancelFunc) {
	if deadline.set || ctx.Done() != nil {
		return callContext(ctx, deadline)
	}

	return ctx, func() {}
}

func invokeResponseFactory[T ProtoMessage](factory func() T) (message T, err error) {
	defer func() {
		if recover() != nil {
			var zero T
			message = zero
			err = Internal("response factory panicked")
		}
	}()

	return factory(), nil
}

type responseStreamFailure struct {
	reason string
	status *Status
	cause  error
}

func (e *responseStreamFailure) Error() string { return e.status.Error() }
func (e *responseStreamFailure) Unwrap() error { return e.status }

func newResponseStreamFailure(reason string, status *Status, cause error) error {
	return &responseStreamFailure{reason: reason, status: status, cause: cause}
}

// responseMessageStream decodes the response frames of one call and enforces
// the response grammar: optional HEADERS, then messages, then one STATUS. It
// implements ServerStreamingCall.
type responseMessageStream[T ProtoMessage] struct {
	inner      frameStream
	newMessage func() T
	config     callConfig
	ctx        context.Context
	cancel     context.CancelFunc
	finishOnce sync.Once

	// readMu serializes frame reads by Recv and Headers.
	readMu sync.Mutex
	// ready is closed, once Headers has had to wait on it, when the headers
	// are known: the first response frame has arrived or the stream finished.
	// It is guarded by mu.
	ready chan struct{}
	// pending is a frame, or the error, that Headers read ahead for Recv.
	pending    responseFrame
	hasPending bool
	pendingErr error

	mu             sync.Mutex
	headers        Metadata
	headersClosed  bool // a frame arrived, so HEADERS may no longer
	done           bool
	closedLocally  bool
	closeErr       error
	closeErrRead   bool
	terminalStatus *Status
}

func newResponseMessageStream[T ProtoMessage](ctx context.Context, inner frameStream, newMessage func() T, config callConfig, cancel context.CancelFunc) *responseMessageStream[T] {
	return &responseMessageStream[T]{inner: inner, newMessage: newMessage, config: config, ctx: ctx, cancel: cancel}
}

func (s *responseMessageStream[T]) Recv() (T, error) {
	var zero T
	if err := s.takePendingError(); err != nil {
		return zero, err
	}
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done {
		return zero, io.EOF
	}

	for {
		frame, err := s.nextFrame()
		if err != nil {
			return zero, err
		}

		switch frame.kind {
		case frameTypeHeaders:
			continue
		case frameTypeMessage:
			if len(frame.body) > s.config.maxResponseSize {
				s.abort()
				return zero, &FrameTooLargeError{Len: len(frame.body), Max: s.config.maxResponseSize}
			}

			message, err := invokeResponseFactory(s.newMessage)
			if err != nil {
				s.abort()
				return zero, err
			}
			if err := unmarshalMessage(frame.body, message); err != nil {
				s.abort()
				return zero, newResponseStreamFailure(
					"malformed_protobuf",
					Internal("failed to decode response: "+err.Error()),
					err,
				)
			}

			return message, nil
		case frameTypeStatus:
			return zero, s.finishWithStatus(frame.status)
		default:
			s.abort()
			return zero, newResponseStreamFailure(
				"unsupported_frame_kind",
				Internal(fmt.Sprintf("response stream contained %s", frame.kind)),
				nil,
			)
		}
	}
}

// finishWithStatus completes the stream with its terminal status. The
// transport must end right after it; frames of unknown type are skipped
// before that.
func (s *responseMessageStream[T]) finishWithStatus(status *Status) error {
	if err := validateMetadata(status.Metadata); err != nil {
		s.abort()
		return newResponseStreamFailure(
			"invalid_metadata",
			Internal("invalid response metadata: "+err.Error()),
			err,
		)
	}
	_, trailingErr := recvClientFrame(s.ctx, s.inner)
	if trailingErr == nil {
		s.abort()
		return newResponseStreamFailure(
			"trailing_frame",
			Internal("response stream contained data after final status"),
			nil,
		)
	}
	var cleanupErr error
	switch {
	case errors.Is(trailingErr, io.EOF):
		s.mu.Lock()
		s.terminalStatus = status
		s.mu.Unlock()
		cleanupErr = s.finish()
	case s.ctx.Err() != nil:
		// The call's own deadline or cancellation cut the wait for the end of
		// the stream short. The server's status already decided the outcome,
		// so report it rather than a protocol error.
		s.mu.Lock()
		s.terminalStatus = status
		s.mu.Unlock()
		s.abort()
	default:
		s.abort()
		return newResponseStreamFailure(
			"trailing_frame",
			Internal("response stream did not end cleanly after final status"),
			trailingErr,
		)
	}

	if status.IsOK() {
		if cleanupErr != nil {
			return cleanupErr
		}
		return io.EOF
	}

	return newResponseStreamFailure("remote_status", status, nil)
}

// Headers returns the response headers, reading ahead to the first response
// frame if it has not arrived yet. A message or status read this way is kept
// for Recv. A failure is returned here and again by Recv.
func (s *responseMessageStream[T]) Headers() (Metadata, error) {
	for {
		if headers, resolved, err := s.resolvedHeaders(); resolved {
			return headers, err
		}
		if !s.readMu.TryLock() {
			// Recv is waiting for a frame. It records the headers when they
			// arrive, so Headers does not have to wait for a message.
			<-s.readySignal()
			continue
		}
		if headers, err := s.readAhead(); headers != nil || err != nil {
			return headers, err
		}
	}
}

// readAhead reads the first response frame for Headers. The caller holds
// readMu; readAhead releases it.
func (s *responseMessageStream[T]) readAhead() (Metadata, error) {
	defer s.readMu.Unlock()
	if headers, resolved, err := s.resolvedHeaders(); resolved {
		return headers, err
	}

	frame, err := s.readFrame()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, Cancelled("response stream closed")
		}
		s.pendingErr = err
		return nil, err
	}
	if frame.kind != frameTypeHeaders {
		s.pending, s.hasPending = frame, true
	}
	headers, _, _ := s.resolvedHeaders()
	return headers, nil
}

// readySignal returns a channel that is closed once the headers are known.
func (s *responseMessageStream[T]) readySignal() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready == nil {
		s.ready = make(chan struct{})
		if s.headersClosed || s.done {
			close(s.ready)
		}
	}
	return s.ready
}

// signalReadyLocked wakes the Headers calls that wait for the headers. The
// caller holds mu.
func (s *responseMessageStream[T]) signalReadyLocked() {
	if s.ready == nil {
		return
	}
	select {
	case <-s.ready:
	default:
		close(s.ready)
	}
}

// takePendingError returns the failure Headers read ahead for Recv, once.
func (s *responseMessageStream[T]) takePendingError() error {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	err := s.pendingErr
	s.pendingErr = nil
	return err
}

// resolvedHeaders reports whether the headers are known, either because the
// first frame arrived or because the stream ended without one.
func (s *responseMessageStream[T]) resolvedHeaders() (Metadata, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headersClosed {
		return cloneMetadata(s.headers), true, nil
	}
	if s.done {
		if s.closedLocally {
			return nil, true, Cancelled("response stream closed")
		}
		return Metadata{}, true, nil
	}
	return nil, false, nil
}

// nextFrame returns the frame to process next: one Headers read ahead, or the
// next frame of the transport.
func (s *responseMessageStream[T]) nextFrame() (responseFrame, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.hasPending {
		frame := s.pending
		s.pending, s.hasPending = responseFrame{}, false
		return frame, nil
	}
	return s.readFrame()
}

// readFrame receives the next frame from the transport. It validates and
// records HEADERS, and fails the stream when HEADERS comes second or after
// another frame. On failure the stream has been aborted and the returned error
// is the one to report; it is io.EOF when the stream was closed locally.
func (s *responseMessageStream[T]) readFrame() (responseFrame, error) {
	frame, err := recvClientFrame(s.ctx, s.inner)
	if err != nil {
		if s.wasClosedLocally() {
			return responseFrame{}, io.EOF
		}

		s.abort()
		if errors.Is(err, io.EOF) {
			return responseFrame{}, newResponseStreamFailure(
				"missing_terminal_status",
				Internal("response stream ended before final status"),
				err,
			)
		}
		if _, ok := errors.AsType[*frameDecodeError](err); ok {
			return responseFrame{}, newResponseStreamFailure(
				"malformed_protobuf",
				Internal("failed to decode response frame: "+err.Error()),
				err,
			)
		}

		return responseFrame{}, err
	}

	s.mu.Lock()
	headersClosed := s.headersClosed
	s.mu.Unlock()
	if frame.kind == frameTypeHeaders {
		if headersClosed {
			s.abort()
			return responseFrame{}, newResponseStreamFailure(
				"unexpected_headers",
				Internal("response stream contained HEADERS after another frame"),
				nil,
			)
		}
		if err := validateMetadata(frame.headers); err != nil {
			s.abort()
			return responseFrame{}, newResponseStreamFailure(
				"invalid_metadata",
				Internal("invalid response headers: "+err.Error()),
				err,
			)
		}
	}

	s.mu.Lock()
	if frame.kind == frameTypeHeaders {
		s.headers = frame.headers
	}
	s.headersClosed = true
	s.signalReadyLocked()
	s.mu.Unlock()
	return frame, nil
}

func (s *responseMessageStream[T]) TerminalStatus() (*Status, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalStatus == nil {
		return nil, false
	}

	return cloneStatus(s.terminalStatus), true
}

func (s *responseMessageStream[T]) finish() error {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.done = true
		s.signalReadyLocked()
		s.mu.Unlock()

		s.cancel()
		err := closeMessageStream(s.inner)

		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
	})

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeErrRead {
		return nil
	}
	s.closeErrRead = true
	return s.closeErr
}

// abort finishes the stream on a failure path. The caller already returns a
// more specific error, so the cleanup error is discarded.
func (s *responseMessageStream[T]) abort() {
	_ = s.finish()
}

func (s *responseMessageStream[T]) wasClosedLocally() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closedLocally
}

func (s *responseMessageStream[T]) Close() error {
	s.mu.Lock()
	s.closedLocally = true
	s.mu.Unlock()
	return s.finish()
}

// recvClientFrame receives the next response frame, ending early with a
// status when ctx is cancelled or its deadline passes.
func recvClientFrame(ctx context.Context, stream frameStream) (responseFrame, error) {
	return recvWithContext(ctx, stream, stream.Recv)
}

// recvWithContext runs recv on stream so that ctx cancellation and its deadline
// interrupt it. Streams that support read deadlines or cancel reads on context
// avoid the helper goroutine.
func recvWithContext[T any](ctx context.Context, stream any, recv func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, statusFromContextError(err)
	}
	deadline, hasDeadline := ctx.Deadline()
	if deadlineStream, ok := stream.(readDeadlineStream); ok && (hasDeadline || streamContextCancelsRecv(stream)) {
		if hasDeadline {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer func() { _ = deadlineStream.SetReadDeadline(time.Time{}) }()
		}

		value, err := recv()
		if err != nil {
			if deadlineErr := readDeadlineError(ctx, hasDeadline, err); deadlineErr != nil {
				return zero, deadlineErr
			}
		}

		return value, err
	}
	if isNonBlockingStream(stream) || streamContextCancelsRecv(stream) {
		value, err := recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return zero, statusFromContextError(ctxErr)
			}
		}

		return value, err
	}

	type recvResult struct {
		value T
		err   error
	}

	results := make(chan recvResult, 1)
	go func() {
		value, err := recv()
		results <- recvResult{value: value, err: err}
	}()

	select {
	case result := <-results:
		if result.err != nil {
			if err := ctx.Err(); err != nil {
				return zero, statusFromContextError(err)
			}
		}

		return result.value, result.err
	case <-ctx.Done():
		return zero, statusFromContextError(ctx.Err())
	}
}

func readDeadlineError(ctx context.Context, hasDeadline bool, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return statusFromContextError(ctxErr)
	}
	if hasDeadline && isTimeoutError(err) {
		return statusFromContextError(context.DeadlineExceeded)
	}
	return nil
}

// readSingleResponse drains the response of a unary or client-streaming call
// and returns its only message with the response headers and trailing
// metadata. The whole stream is read first so a server-reported failure takes
// precedence over a cardinality error.
func readSingleResponse[Res ProtoMessage](responses *responseMessageStream[Res]) (Res, Metadata, Metadata, error) {
	var first, zero Res
	responseCount := 0
	for {
		response, err := responses.Recv()
		if err == nil {
			responseCount++
			if responseCount == 1 {
				first = response
			}
			continue
		}
		if !errors.Is(err, io.EOF) {
			return zero, nil, nil, err
		}
		break
	}

	status, ok := responses.TerminalStatus()
	if !ok || status == nil {
		return zero, nil, nil, Internal("response stream ended before final status")
	}
	if responseCount != 1 {
		return zero, nil, nil, newResponseStreamFailure(
			"response_cardinality",
			Internal(fmt.Sprintf("RPC returned %d response messages; expected exactly one", responseCount)),
			nil,
		)
	}
	headers, err := responses.Headers()
	if err != nil {
		return zero, nil, nil, err
	}
	return first, headers, status.Metadata, nil
}
