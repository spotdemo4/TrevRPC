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
	// call sends a unary RPC request and returns its response.
	call(context.Context, *wireRequest) (*wireResponse, error)
	// streamingCall sends a streaming RPC request and returns response frames.
	streamingCall(context.Context, *wireRequest, byteStream) (frameStream, error)
}

// ClientStreamingCall sends request messages and receives the final response.
type ClientStreamingCall[Req ProtoMessage, Res ProtoMessage] interface {
	// Send sends one request message.
	Send(Req) error
	// CloseAndRecv closes the request stream and returns the unary response.
	// Response metadata is reported through CaptureResponseMetadata.
	CloseAndRecv() (Res, error)
	// Close releases call resources without waiting for a response.
	Close() error
}

// BidirectionalStreamingCall sends request messages and receives response messages.
type BidirectionalStreamingCall[Req ProtoMessage, Res ProtoMessage] interface {
	// Send sends one request message.
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
}

type responseStreamFrameFieldsReceiver interface {
	trevrpcRecvStreamFrameFields() (streamFrameFields, error)
}

// callConfig holds the resolved options of one RPC call.
type callConfig struct {
	timeout          time.Duration
	hasTimeout       bool
	maxResponseSize  int
	metadata         Metadata
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

// CaptureResponseMetadata stores the response metadata of a successful unary or
// client-streaming call in *dst, replacing its previous value. It is written
// before the call returns (for client streaming, before CloseAndRecv returns)
// and is not touched when the call fails; the metadata of a failed call is on
// the returned *Status. Streaming responses report metadata through
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

	requestBody, err := marshalMessage(request)
	if err != nil {
		return zero, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RPCKindUnary, requestBody, config, deadline, time.Now)
	if err != nil {
		return zero, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, deadline)
	defer cancel()

	response, err := transport.call(ctx, rpcRequest)
	if err != nil {
		return zero, err
	}

	if err := validateResponse(response, config.maxResponseSize); err != nil {
		return zero, err
	}

	message, err := invokeResponseFactory(newResponse)
	if err != nil {
		return zero, err
	}
	if err := unmarshalMessage(response.Body, message); err != nil {
		return zero, err
	}

	config.captureResponseMetadata(response.Metadata)
	return message, nil
}

// ServerStreaming calls a server-streaming RPC and returns its response stream.
// The terminal status and metadata are available from TerminalStatus after Recv
// reports io.EOF or the stream's error.
func ServerStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (ResponseStream[Res], error) {
	config := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, config, time.Now)
	if err != nil {
		return nil, err
	}

	requestBody, err := marshalMessage(request)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RPCKindServerStreaming, requestBody, config, deadline, time.Now)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, deadline)
	response, err := transport.streamingCall(ctx, rpcRequest, newEmptyStream[[]byte]())
	if err != nil {
		cancel()
		return nil, err
	}

	return newResponseMessageStream(ctx, response, newResponse, config, cancel), nil
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

	rpcRequest, err := prepareClientRequest(service, method, kind, nil, config, deadline, time.Now)
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

	message, metadata, err := readSingleResponse(c.responses)
	if err != nil {
		return zero, err
	}
	c.config.captureResponseMetadata(metadata)
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

func applyCallOptions(options []CallOption) callConfig {
	config := defaultCallConfig()
	for _, option := range options {
		option(&config)
	}

	return config
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

func prepareClientRequest(service, method string, kind RPCKind, body []byte, config callConfig, deadline clientDeadline, now func() time.Time) (*wireRequest, error) {
	metadata := cloneMetadata(config.metadata)
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}

	request := newWireRequest(service, method, body)
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

func validateResponse(response *wireResponse, maxBodySize int) error {
	if response == nil {
		return Internal("missing RPC response")
	}

	if err := validateMetadata(response.Metadata); err != nil {
		return Internal("invalid response metadata: " + err.Error())
	}

	if len(response.Body) > maxBodySize {
		return &FrameTooLargeError{Len: len(response.Body), Max: maxBodySize}
	}

	if CodeFromUint32(response.Status) != CodeOK {
		return statusFromWireResponse(response)
	}

	return nil
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

// responseMessageStream decodes the response frames of one streaming call. It
// implements ResponseStream.
type responseMessageStream[T ProtoMessage] struct {
	inner          frameStream
	newMessage     func() T
	config         callConfig
	ctx            context.Context
	cancel         context.CancelFunc
	finishOnce     sync.Once
	mu             sync.Mutex
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
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done {
		return zero, io.EOF
	}

	frame, err := recvClientFrame(s.ctx, s.inner)
	if err != nil {
		if s.wasClosedLocally() {
			return zero, io.EOF
		}

		s.abort()
		if errors.Is(err, io.EOF) {
			return zero, newResponseStreamFailure(
				"missing_terminal_status",
				Internal("response stream ended before final status"),
				err,
			)
		}
		if _, ok := errors.AsType[*frameDecodeError](err); ok {
			return zero, newResponseStreamFailure(
				"malformed_protobuf",
				Internal("failed to decode response frame: "+err.Error()),
				err,
			)
		}

		return zero, err
	}

	switch frame.kind {
	case wireFrameMessage:
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
	case wireFrameStatus:
		if err := validateMetadata(frame.metadata); err != nil {
			s.abort()
			return zero, newResponseStreamFailure(
				"invalid_metadata",
				Internal("invalid response metadata: "+err.Error()),
				err,
			)
		}
		status := frame.statusValue()
		_, trailingErr := recvClientFrame(s.ctx, s.inner)
		if trailingErr == nil {
			s.abort()
			return zero, newResponseStreamFailure(
				"trailing_frame",
				Internal("response stream contained data after final status"),
				nil,
			)
		}
		if !errors.Is(trailingErr, io.EOF) {
			s.abort()
			return zero, newResponseStreamFailure(
				"trailing_frame",
				Internal("response stream did not end cleanly after final status"),
				trailingErr,
			)
		}
		s.mu.Lock()
		s.terminalStatus = status
		s.mu.Unlock()
		cleanupErr := s.finish()

		if status.IsOK() {
			if cleanupErr != nil {
				return zero, cleanupErr
			}
			return zero, io.EOF
		}

		return zero, newResponseStreamFailure("remote_status", status, nil)
	default:
		s.abort()
		return zero, newResponseStreamFailure(
			"unsupported_frame_kind",
			InvalidArgument("response stream contained an unknown frame kind"),
			nil,
		)
	}
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

// recvClientFrame receives the next response frame fields, ending early with a
// status when ctx is cancelled or its deadline passes.
func recvClientFrame(ctx context.Context, stream frameStream) (streamFrameFields, error) {
	if fieldsReceiver, ok := stream.(responseStreamFrameFieldsReceiver); ok {
		return recvWithContext(ctx, stream, fieldsReceiver.trevrpcRecvStreamFrameFields)
	}

	return recvWithContext(ctx, stream, func() (streamFrameFields, error) {
		frame, err := stream.Recv()
		if err != nil {
			return streamFrameFields{}, err
		}

		return streamFrameFields{
			kind:     frame.Kind,
			status:   frame.Status,
			message:  frame.Message,
			body:     frame.Body,
			metadata: frame.Metadata,
		}, nil
	})
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

// readSingleResponse drains a client-streaming response stream and returns its
// only message with the terminal metadata. The whole stream is read first so a
// server-reported failure takes precedence over a cardinality error.
func readSingleResponse[Res ProtoMessage](responses ResponseStream[Res]) (Res, Metadata, error) {
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
			return zero, nil, err
		}
		break
	}

	status, ok := responses.TerminalStatus()
	if !ok || status == nil {
		return zero, nil, Internal("response stream ended before final status")
	}
	if responseCount != 1 {
		return zero, nil, newResponseStreamFailure(
			"response_cardinality",
			Internal(fmt.Sprintf("client-streaming RPC returned %d response messages; expected exactly one", responseCount)),
			nil,
		)
	}
	return first, status.Metadata, nil
}
