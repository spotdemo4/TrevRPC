package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

// Transport sends unary and streaming TrevRPC requests.
type Transport interface {
	// Call sends a unary RPC request and returns its response.
	Call(context.Context, *RpcRequest) (*RpcResponse, error)
	// StreamingCall sends a streaming RPC request and returns response frames.
	StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error)
}

// ResponseStream yields decoded messages and exposes the terminal stream status.
type ResponseStream[T ProtoMessage] interface {
	MessageStream[T]
	TerminalStatus() (*Status, bool)
}

// ClientStreamingCall sends request messages and receives the final response.
type ClientStreamingCall[Req ProtoMessage, Res ProtoMessage] interface {
	// Send sends one request message.
	Send(Req) error
	// CloseAndRecv closes the request stream and returns the unary response.
	CloseAndRecv() (Res, error)
	// Close releases call resources without waiting for a response.
	Close() error
}

// ClientStreamingResponseCall is a client-streaming call that exposes response metadata.
type ClientStreamingResponseCall[Req ProtoMessage, Res ProtoMessage] interface {
	ClientStreamingCall[Req, Res]
	// CloseAndRecvResponse closes the request stream and returns the response envelope.
	CloseAndRecvResponse() (*Response[Res], error)
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
}

// BidirectionalStreamingResponseCall is a bidirectional call that exposes terminal status metadata.
type BidirectionalStreamingResponseCall[Req ProtoMessage, Res ProtoMessage] interface {
	BidirectionalStreamingCall[Req, Res]
	// TerminalStatus returns the terminal status after the status frame has been consumed.
	TerminalStatus() (*Status, bool)
}

type responseStreamFrameFieldsReceiver interface {
	trevrpcRecvStreamFrameFields() (streamFrameFields, func(), error)
}

// CallOptions controls client-side timeouts, response limits, and request metadata.
type CallOptions struct {
	Timeout                   time.Duration
	HasTimeout                bool
	MaxResponseBodySize       int
	MaxResponseMessages       int
	MaxResponseStreamBodySize int
	StreamIdleTimeout         time.Duration
	Metadata                  Metadata
}

// CallOption mutates CallOptions for a single RPC call.
type CallOption func(*CallOptions)

// DefaultCallOptions returns the default client call options.
func DefaultCallOptions() CallOptions {
	return CallOptions{
		MaxResponseBodySize:       DefaultMaxFrameSize,
		MaxResponseMessages:       4096,
		MaxResponseStreamBodySize: 16 * 1024 * 1024,
		StreamIdleTimeout:         30 * time.Second,
		Metadata:                  Metadata{},
	}
}

// WithTimeout sets a deadline for the RPC call.
func WithTimeout(timeout time.Duration) CallOption {
	return func(options *CallOptions) {
		options.Timeout = timeout
		options.HasTimeout = true
	}
}

// WithoutTimeout clears the RPC call deadline.
func WithoutTimeout() CallOption {
	return func(options *CallOptions) {
		options.Timeout = 0
		options.HasTimeout = false
	}
}

// WithMaxResponseBodySize sets the maximum unary response body size in bytes.
func WithMaxResponseBodySize(max int) CallOption {
	return func(options *CallOptions) {
		options.MaxResponseBodySize = max
	}
}

// WithMaxResponseMessages sets the maximum number of response messages allowed on a stream.
func WithMaxResponseMessages(max int) CallOption {
	return func(options *CallOptions) {
		options.MaxResponseMessages = max
	}
}

// WithoutMaxResponseMessages disables the response message count limit.
func WithoutMaxResponseMessages() CallOption {
	return func(options *CallOptions) {
		options.MaxResponseMessages = -1
	}
}

// WithMaxResponseStreamBodySize sets the maximum total response stream body size in bytes.
func WithMaxResponseStreamBodySize(max int) CallOption {
	return func(options *CallOptions) {
		options.MaxResponseStreamBodySize = max
	}
}

// WithoutMaxResponseStreamBodySize disables the response stream body size limit.
func WithoutMaxResponseStreamBodySize() CallOption {
	return func(options *CallOptions) {
		options.MaxResponseStreamBodySize = -1
	}
}

// WithStreamIdleTimeout sets the maximum idle time between response stream messages.
func WithStreamIdleTimeout(timeout time.Duration) CallOption {
	return func(options *CallOptions) {
		options.StreamIdleTimeout = timeout
	}
}

// WithoutStreamIdleTimeout disables the response stream idle timeout.
func WithoutStreamIdleTimeout() CallOption {
	return func(options *CallOptions) {
		options.StreamIdleTimeout = 0
	}
}

// WithMetadata adds request metadata after normalizing the metadata key.
func WithMetadata(key string, value []byte) CallOption {
	key = NormalizeMetadataKey(key)
	value = append([]byte(nil), value...)
	return func(options *CallOptions) {
		metadata := cloneMetadata(options.Metadata)
		if metadata == nil {
			metadata = Metadata{}
		}
		metadata[key] = append([]byte(nil), value...)
		options.Metadata = metadata
	}
}

// WithMetadataMap replaces the metadata map sent with the request.
func WithMetadataMap(metadata Metadata) CallOption {
	metadata = cloneMetadata(metadata)
	return func(options *CallOptions) {
		options.Metadata = cloneMetadata(metadata)
	}
}

// Unary calls a unary RPC and decodes the protobuf response.
func Unary[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (Res, error) {
	response, err := UnaryResponse(ctx, transport, service, method, request, newResponse, options...)
	if err != nil {
		var zero Res
		return zero, err
	}

	return response.Message, nil
}

// UnaryResponse calls a unary RPC and returns the decoded response envelope.
func UnaryResponse[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (*Response[Res], error) {
	callOptions := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, callOptions, time.Now)
	if err != nil {
		return nil, err
	}

	requestBody, err := MarshalMessage(request)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindUnary, requestBody, callOptions, deadline, time.Now)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, deadline)
	defer cancel()

	response, err := transport.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}

	if err := validateResponse(response, callOptions.MaxResponseBodySize); err != nil {
		return nil, err
	}

	message, err := invokeResponseFactory(newResponse)
	if err != nil {
		return nil, err
	}
	if err := UnmarshalMessage(response.Body, message); err != nil {
		return nil, err
	}

	return &Response[Res]{Message: message, Metadata: cloneMetadata(response.Metadata)}, nil
}

// ServerStreaming calls a server-streaming RPC and returns a decoded response stream.
func ServerStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (MessageStream[Res], error) {
	return ServerStreamingResponse(ctx, transport, service, method, request, newResponse, options...)
}

// ServerStreamingResponse calls a server-streaming RPC and returns an envelope-aware response stream.
func ServerStreamingResponse[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (ResponseStream[Res], error) {
	callOptions := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, callOptions, time.Now)
	if err != nil {
		return nil, err
	}

	requestBody, err := MarshalMessage(request)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindServerStreaming, requestBody, callOptions, deadline, time.Now)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, deadline)
	response, err := transport.StreamingCall(ctx, rpcRequest, EmptyStream[[]byte]())
	if err != nil {
		cancel()
		return nil, err
	}

	return newResponseMessageStream(response, newResponse, callOptions, ctx, cancel), nil
}

// ClientStreaming starts a client-streaming RPC.
func ClientStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (ClientStreamingCall[Req, Res], error) {
	return ClientStreamingResponse[Req, Res](ctx, transport, service, method, newResponse, options...)
}

// ClientStreamingResponse starts a client-streaming RPC that exposes response metadata.
func ClientStreamingResponse[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (ClientStreamingResponseCall[Req, Res], error) {
	callOptions := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, callOptions, time.Now)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindClientStreaming, nil, callOptions, deadline, time.Now)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContext(ctx, deadline)
	requests := NewMessagePipe[Req](ctx)
	response, err := transport.StreamingCall(ctx, rpcRequest, EncodeStream(requests))
	if err != nil {
		cancel()
		return nil, err
	}

	return &clientStreamingCall[Req, Res]{
		requests:  requests,
		responses: newResponseMessageStream(response, newResponse, callOptions, ctx, cancel),
		cancel:    cancel,
	}, nil
}

// ClientStreamingFromStream calls a client-streaming RPC from an existing request stream.
func ClientStreamingFromStream[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, requests MessageStream[Req], newResponse func() Res, options ...CallOption) (Res, error) {
	response, err := ClientStreamingFromStreamResponse(ctx, transport, service, method, requests, newResponse, options...)
	if err != nil {
		var zero Res
		return zero, err
	}

	return response.Message, nil
}

// ClientStreamingFromStreamResponse calls a client-streaming RPC and returns its response envelope.
func ClientStreamingFromStreamResponse[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, requests MessageStream[Req], newResponse func() Res, options ...CallOption) (*Response[Res], error) {
	responses, _, err := startRequestStreaming(ctx, transport, service, method, RpcKindClientStreaming, requests, newResponse, options)
	if err != nil {
		return nil, err
	}

	return readUnaryResponseFromResponseStream(responses)
}

// BidirectionalStreaming starts a bidirectional-streaming RPC.
func BidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (BidirectionalStreamingCall[Req, Res], error) {
	return BidirectionalStreamingResponse[Req, Res](ctx, transport, service, method, newResponse, options...)
}

// BidirectionalStreamingResponse starts a bidirectional RPC that exposes terminal status metadata.
func BidirectionalStreamingResponse[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (BidirectionalStreamingResponseCall[Req, Res], error) {
	callOptions := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, callOptions, time.Now)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindBidirectionalStreaming, nil, callOptions, deadline, time.Now)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContext(ctx, deadline)
	requests := NewMessagePipe[Req](ctx)
	response, err := transport.StreamingCall(ctx, rpcRequest, EncodeStream(requests))
	if err != nil {
		cancel()
		return nil, err
	}

	return &bidirectionalStreamingCall[Req, Res]{
		requests:  requests,
		responses: newResponseMessageStream(response, newResponse, callOptions, ctx, cancel),
	}, nil
}

// BidirectionalStreamingFromStream calls a bidirectional-streaming RPC from an existing request stream.
func BidirectionalStreamingFromStream[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, requests MessageStream[Req], newResponse func() Res, options ...CallOption) (MessageStream[Res], error) {
	return BidirectionalStreamingFromStreamResponse(ctx, transport, service, method, requests, newResponse, options...)
}

// BidirectionalStreamingFromStreamResponse calls a bidirectional RPC and exposes terminal status metadata.
func BidirectionalStreamingFromStreamResponse[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, requests MessageStream[Req], newResponse func() Res, options ...CallOption) (ResponseStream[Res], error) {
	responses, _, err := startRequestStreaming(ctx, transport, service, method, RpcKindBidirectionalStreaming, requests, newResponse, options)
	if err != nil {
		return nil, err
	}

	return responses, nil
}

func startRequestStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, kind RpcKind, requests MessageStream[Req], newResponse func() Res, options []CallOption) (ResponseStream[Res], context.CancelFunc, error) {
	if requests == nil {
		return nil, func() {}, InvalidArgument("request stream is nil")
	}

	callOptions := applyCallOptions(options)
	deadline, err := resolveClientDeadline(ctx, callOptions, time.Now)
	if err != nil {
		return nil, func() {}, err
	}

	rpcRequest, err := prepareClientRequest(service, method, kind, nil, callOptions, deadline, time.Now)
	if err != nil {
		return nil, func() {}, err
	}

	ctx, cancel := callContextForRequestStream(ctx, deadline, requests)
	response, err := transport.StreamingCall(ctx, rpcRequest, EncodeStream(requests))
	if err != nil {
		cancel()
		return nil, func() {}, err
	}

	return newResponseMessageStream(response, newResponse, callOptions, ctx, cancel), cancel, nil
}

type clientStreamingCall[Req ProtoMessage, Res ProtoMessage] struct {
	requests  *MessagePipe[Req]
	responses ResponseStream[Res]
	cancel    context.CancelFunc
}

func (c *clientStreamingCall[Req, Res]) Send(request Req) error {
	return c.requests.Send(request)
}

func (c *clientStreamingCall[Req, Res]) CloseAndRecv() (Res, error) {
	response, err := c.CloseAndRecvResponse()
	if err != nil {
		var zero Res
		return zero, err
	}

	return response.Message, nil
}

func (c *clientStreamingCall[Req, Res]) CloseAndRecvResponse() (*Response[Res], error) {
	if err := c.requests.Close(); err != nil {
		return nil, err
	}

	return readUnaryResponseFromResponseStream(c.responses)
}

func (c *clientStreamingCall[Req, Res]) Close() error {
	c.cancel()
	return errors.Join(c.requests.Close(), closeMessageStream(c.responses))
}

type bidirectionalStreamingCall[Req ProtoMessage, Res ProtoMessage] struct {
	requests  *MessagePipe[Req]
	responses ResponseStream[Res]
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

func applyCallOptions(options []CallOption) CallOptions {
	callOptions := DefaultCallOptions()
	for _, option := range options {
		option(&callOptions)
	}

	return callOptions
}

type clientDeadline struct {
	at  time.Time
	set bool
}

func resolveClientDeadline(ctx context.Context, options CallOptions, now func() time.Time) (clientDeadline, error) {
	if options.HasTimeout && options.Timeout < 0 {
		return clientDeadline{}, InvalidArgument("RPC timeout is negative")
	}

	deadline, hasDeadline := ctx.Deadline()
	if options.HasTimeout {
		optionDeadline := now().Add(options.Timeout)
		if !hasDeadline || optionDeadline.Before(deadline) {
			deadline = optionDeadline
			hasDeadline = true
		}
	}

	return clientDeadline{at: deadline, set: hasDeadline}, nil
}

func prepareClientRequest(service, method string, kind RpcKind, body []byte, options CallOptions, deadline clientDeadline, now func() time.Time) (*RpcRequest, error) {
	metadata := cloneMetadata(options.Metadata)
	if err := ValidateMetadata(metadata); err != nil {
		return nil, err
	}

	request := NewRpcRequest(service, method, body)
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

func callContextForRequestStream[Req ProtoMessage](ctx context.Context, deadline clientDeadline, requests MessageStream[Req]) (context.Context, context.CancelFunc) {
	if isNonBlockingStream(requests) {
		return callContextWithoutLocalCancel(ctx, deadline)
	}

	return callContext(ctx, deadline)
}

func validateResponse(response *RpcResponse, maxBodySize int) error {
	if response == nil {
		return Internal("missing RPC response")
	}

	if err := ValidateMetadata(response.Metadata); err != nil {
		return Internal("invalid response metadata: " + err.Error())
	}

	if len(response.Body) > maxBodySize {
		return &FrameTooLargeError{Len: len(response.Body), Max: maxBodySize}
	}

	if CodeFromUint32(response.Status) != CodeOK {
		return StatusFromResponse(response)
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

// ResponseStreamFailureReason returns the stable production reason for a response-stream failure.
func ResponseStreamFailureReason(err error) (string, bool) {
	var failure *responseStreamFailure
	if !errors.As(err, &failure) {
		return "", false
	}
	return failure.reason, true
}

func newResponseStreamFailure(reason string, status *Status, cause error) error {
	return &responseStreamFailure{reason: reason, status: status, cause: cause}
}

type responseMessageStream[T ProtoMessage] struct {
	inner          FrameStream
	newMessage     func() T
	options        CallOptions
	ctx            context.Context
	cancel         context.CancelFunc
	messages       int
	streamBodySize int
	finishOnce     sync.Once
	mu             sync.Mutex
	done           bool
	closedLocally  bool
	closeErr       error
	closeErrRead   bool
	terminalStatus *Status
}

func newResponseMessageStream[T ProtoMessage](inner FrameStream, newMessage func() T, options CallOptions, ctx context.Context, cancel context.CancelFunc) *responseMessageStream[T] {
	return &responseMessageStream[T]{inner: inner, newMessage: newMessage, options: options, ctx: ctx, cancel: cancel}
}

func (s *responseMessageStream[T]) Recv() (T, error) {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done {
		var zero T
		return zero, io.EOF
	}

	frame, release, err := recvFrameFieldsWithTimeout(s.ctx, s.inner, s.options.StreamIdleTimeout)
	if release != nil {
		defer release()
	}
	if err != nil {
		if s.wasClosedLocally() {
			var zero T
			return zero, io.EOF
		}

		s.finish()
		var zero T
		if err == io.EOF {
			return zero, newResponseStreamFailure(
				"missing_terminal_status",
				Internal("response stream ended before final status"),
				err,
			)
		}
		var decodeError *FrameDecodeError
		if errors.As(err, &decodeError) {
			return zero, newResponseStreamFailure(
				"malformed_protobuf",
				Internal("failed to decode response frame: "+err.Error()),
				err,
			)
		}

		return zero, err
	}

	switch frame.kind {
	case RpcStreamFrameKindMessage:
		if s.options.MaxResponseMessages >= 0 && s.messages >= s.options.MaxResponseMessages {
			s.finish()
			var zero T
			return zero, ResourceExhausted(fmt.Sprintf("response stream exceeded maximum of %d messages", s.options.MaxResponseMessages))
		}

		if len(frame.body) > s.options.MaxResponseBodySize {
			s.finish()
			var zero T
			return zero, &FrameTooLargeError{Len: len(frame.body), Max: s.options.MaxResponseBodySize}
		}

		s.messages++
		s.streamBodySize = saturatingAdd(s.streamBodySize, len(frame.body))
		if s.options.MaxResponseStreamBodySize >= 0 && s.streamBodySize > s.options.MaxResponseStreamBodySize {
			s.finish()
			var zero T
			return zero, ResourceExhausted(fmt.Sprintf("response stream exceeded maximum body size of %d bytes", s.options.MaxResponseStreamBodySize))
		}

		message, err := invokeResponseFactory(s.newMessage)
		if err != nil {
			s.finish()
			var zero T
			return zero, err
		}
		if err := UnmarshalMessage(frame.body, message); err != nil {
			s.finish()
			var zero T
			return zero, newResponseStreamFailure(
				"malformed_protobuf",
				Internal("failed to decode response: "+err.Error()),
				err,
			)
		}

		return message, nil
	case RpcStreamFrameKindStatus:
		if err := ValidateMetadata(frame.metadata); err != nil {
			s.finish()
			var zero T
			return zero, newResponseStreamFailure(
				"invalid_metadata",
				Internal("invalid response metadata: "+err.Error()),
				err,
			)
		}
		status := frame.statusValue()
		trailing, trailingRelease, trailingErr := recvFrameFieldsWithTimeout(s.ctx, s.inner, s.options.StreamIdleTimeout)
		if trailingRelease != nil {
			trailingRelease()
		}
		if trailingErr == nil {
			_ = trailing
			s.finish()
			var zero T
			return zero, newResponseStreamFailure(
				"trailing_frame",
				Internal("response stream contained data after final status"),
				nil,
			)
		}
		if trailingErr != io.EOF {
			s.finish()
			var zero T
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
				var zero T
				return zero, cleanupErr
			}
			var zero T
			return zero, io.EOF
		}

		var zero T
		return zero, newResponseStreamFailure("remote_status", status, nil)
	default:
		s.finish()
		var zero T
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

	return cloneResponseStatus(s.terminalStatus), true
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

func recvFrameWithTimeout(ctx context.Context, stream FrameStream, idleTimeout time.Duration) (*RpcStreamFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	deadline, source, hasDeadline := responseReadDeadline(ctx, idleTimeout)
	if deadlineStream, ok := stream.(readDeadlineStream); ok && (hasDeadline || streamContextCancelsRecv(stream)) {
		if hasDeadline {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer deadlineStream.SetReadDeadline(time.Time{})
		}

		frame, err := stream.Recv()
		if err != nil {
			if deadlineErr := responseReadDeadlineError(ctx, source, err); deadlineErr != nil {
				return nil, deadlineErr
			}
		}

		return frame, err
	}
	if idleTimeout <= 0 && (isNonBlockingStream(stream) || streamContextCancelsRecv(stream)) {
		frame, err := stream.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, statusFromContextError(ctxErr)
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
		frame, err := stream.Recv()
		results <- recvResult{frame: frame, err: err}
	}()

	var idle <-chan time.Time
	var timer *time.Timer
	if idleTimeout > 0 {
		timer = time.NewTimer(idleTimeout)
		idle = timer.C
		defer timer.Stop()
	}

	select {
	case result := <-results:
		if result.err != nil {
			if err := ctx.Err(); err != nil {
				return nil, statusFromContextError(err)
			}
		}

		return result.frame, result.err
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	case <-idle:
		if err := ctx.Err(); err != nil {
			return nil, statusFromContextError(err)
		}

		return nil, Unavailable("response stream idle timeout")
	}
}

func recvFrameFieldsWithTimeout(ctx context.Context, stream FrameStream, idleTimeout time.Duration) (streamFrameFields, func(), error) {
	if fieldsReceiver, ok := stream.(responseStreamFrameFieldsReceiver); ok {
		return recvOptimizedFrameFieldsWithTimeout(ctx, fieldsReceiver, idleTimeout, streamContextCancelsRecv(stream))
	}

	frame, err := recvFrameWithTimeout(ctx, stream, idleTimeout)
	if err != nil {
		return streamFrameFields{}, nil, err
	}

	return streamFrameFields{
		kind:     frame.Kind,
		status:   frame.Status,
		message:  frame.Message,
		body:     frame.Body,
		metadata: frame.Metadata,
	}, nil, nil
}

func recvOptimizedFrameFieldsWithTimeout(ctx context.Context, stream responseStreamFrameFieldsReceiver, idleTimeout time.Duration, contextCancelsRecv bool) (streamFrameFields, func(), error) {
	if err := ctx.Err(); err != nil {
		return streamFrameFields{}, nil, statusFromContextError(err)
	}
	deadline, source, hasDeadline := responseReadDeadline(ctx, idleTimeout)
	if deadlineStream, ok := stream.(readDeadlineStream); ok && (hasDeadline || contextCancelsRecv) {
		if hasDeadline {
			_ = deadlineStream.SetReadDeadline(deadline)
			defer deadlineStream.SetReadDeadline(time.Time{})
		}

		frame, release, err := stream.trevrpcRecvStreamFrameFields()
		if err != nil {
			if deadlineErr := responseReadDeadlineError(ctx, source, err); deadlineErr != nil {
				if release != nil {
					release()
				}
				return streamFrameFields{}, nil, deadlineErr
			}
		}

		return frame, release, err
	}
	if idleTimeout <= 0 && contextCancelsRecv {
		frame, release, err := stream.trevrpcRecvStreamFrameFields()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				if release != nil {
					release()
				}
				return streamFrameFields{}, nil, statusFromContextError(ctxErr)
			}
		}

		return frame, release, err
	}

	type recvResult struct {
		frame   streamFrameFields
		release func()
		err     error
	}

	results := make(chan recvResult, 1)
	go func() {
		frame, release, err := stream.trevrpcRecvStreamFrameFields()
		results <- recvResult{frame: frame, release: release, err: err}
	}()

	var idle <-chan time.Time
	var timer *time.Timer
	if idleTimeout > 0 {
		timer = time.NewTimer(idleTimeout)
		idle = timer.C
		defer timer.Stop()
	}

	select {
	case result := <-results:
		if result.err != nil {
			if err := ctx.Err(); err != nil {
				if result.release != nil {
					result.release()
				}
				return streamFrameFields{}, nil, statusFromContextError(err)
			}
		}

		return result.frame, result.release, result.err
	case <-ctx.Done():
		return streamFrameFields{}, nil, statusFromContextError(ctx.Err())
	case <-idle:
		if err := ctx.Err(); err != nil {
			return streamFrameFields{}, nil, statusFromContextError(err)
		}

		return streamFrameFields{}, nil, Unavailable("response stream idle timeout")
	}
}

type responseReadDeadlineSource uint8

const (
	responseReadDeadlineNone responseReadDeadlineSource = iota
	responseReadDeadlineContext
	responseReadDeadlineIdle
)

func responseReadDeadline(ctx context.Context, idleTimeout time.Duration) (time.Time, responseReadDeadlineSource, bool) {
	var deadline time.Time
	source := responseReadDeadlineNone
	if contextDeadline, ok := ctx.Deadline(); ok {
		deadline = contextDeadline
		source = responseReadDeadlineContext
	}
	if idleTimeout > 0 {
		idleDeadline := time.Now().Add(idleTimeout)
		if source == responseReadDeadlineNone || idleDeadline.Before(deadline) {
			deadline = idleDeadline
			source = responseReadDeadlineIdle
		}
	}

	return deadline, source, source != responseReadDeadlineNone
}

func responseReadDeadlineError(ctx context.Context, source responseReadDeadlineSource, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return statusFromContextError(ctxErr)
	}
	if !isTimeoutError(err) {
		return nil
	}
	switch source {
	case responseReadDeadlineContext:
		return statusFromContextError(context.DeadlineExceeded)
	case responseReadDeadlineIdle:
		return Unavailable("response stream idle timeout")
	default:
		return nil
	}
}

func readUnaryResponseFromResponseStream[Res ProtoMessage](responses ResponseStream[Res]) (*Response[Res], error) {
	var first Res
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
		if err != io.EOF {
			return nil, err
		}
		break
	}

	status, ok := responses.TerminalStatus()
	if !ok || status == nil {
		return nil, Internal("response stream ended before final status")
	}
	if responseCount != 1 {
		return nil, newResponseStreamFailure(
			"response_cardinality",
			Internal(fmt.Sprintf("client-streaming RPC returned %d response messages; expected exactly one", responseCount)),
			nil,
		)
	}
	return &Response[Res]{Message: first, Metadata: cloneMetadata(status.Metadata)}, nil
}

func saturatingAdd(left, right int) int {
	if right > 0 && left > math.MaxInt-right {
		return math.MaxInt
	}

	return left + right
}
