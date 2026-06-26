package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
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
	return func(options *CallOptions) {
		if options.Metadata == nil {
			options.Metadata = Metadata{}
		}

		options.Metadata[NormalizeMetadataKey(key)] = value
	}
}

// WithMetadataMap replaces the metadata map sent with the request.
func WithMetadataMap(metadata Metadata) CallOption {
	return func(options *CallOptions) {
		options.Metadata = metadata
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
	if err := ValidateMetadata(callOptions.Metadata); err != nil {
		return nil, err
	}

	requestBody, err := MarshalMessage(request)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindUnary, requestBody, callOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, callOptions)
	defer cancel()

	response, err := transport.Call(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}

	if err := validateResponse(response, callOptions.MaxResponseBodySize); err != nil {
		return nil, err
	}

	message := newResponse()
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
	requestBody, err := MarshalMessage(request)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindServerStreaming, requestBody, callOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContextWithoutLocalCancel(ctx, callOptions)
	response, err := transport.StreamingCall(ctx, rpcRequest, EmptyStream[[]byte]())
	if err != nil {
		cancel()
		return nil, err
	}

	return newResponseMessageStream(response, newResponse, callOptions, ctx, cancel), nil
}

// ClientStreaming starts a client-streaming RPC.
func ClientStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (ClientStreamingCall[Req, Res], error) {
	callOptions := applyCallOptions(options)
	rpcRequest, err := prepareClientRequest(service, method, RpcKindClientStreaming, nil, callOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContext(ctx, callOptions)
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
	responses, _, err := startRequestStreaming(ctx, transport, service, method, RpcKindClientStreaming, requests, newResponse, options)
	if err != nil {
		var zero Res
		return zero, err
	}

	return readUnaryResponseFromMessageStream(responses)
}

// BidirectionalStreaming starts a bidirectional-streaming RPC.
func BidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, newResponse func() Res, options ...CallOption) (BidirectionalStreamingCall[Req, Res], error) {
	callOptions := applyCallOptions(options)
	rpcRequest, err := prepareClientRequest(service, method, RpcKindBidirectionalStreaming, nil, callOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContext(ctx, callOptions)
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
	responses, _, err := startRequestStreaming(ctx, transport, service, method, RpcKindBidirectionalStreaming, requests, newResponse, options)
	if err != nil {
		return nil, err
	}

	return responses, nil
}

func startRequestStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, kind RpcKind, requests MessageStream[Req], newResponse func() Res, options []CallOption) (MessageStream[Res], context.CancelFunc, error) {
	if requests == nil {
		return nil, func() {}, InvalidArgument("request stream is nil")
	}

	callOptions := applyCallOptions(options)
	rpcRequest, err := prepareClientRequest(service, method, kind, nil, callOptions)
	if err != nil {
		return nil, func() {}, err
	}

	ctx, cancel := callContextForRequestStream(ctx, callOptions, requests)
	response, err := transport.StreamingCall(ctx, rpcRequest, EncodeStream(requests))
	if err != nil {
		cancel()
		return nil, func() {}, err
	}

	return newResponseMessageStream(response, newResponse, callOptions, ctx, cancel), cancel, nil
}

type clientStreamingCall[Req ProtoMessage, Res ProtoMessage] struct {
	requests  *MessagePipe[Req]
	responses MessageStream[Res]
	cancel    context.CancelFunc
}

func (c *clientStreamingCall[Req, Res]) Send(request Req) error {
	return c.requests.Send(request)
}

func (c *clientStreamingCall[Req, Res]) CloseAndRecv() (Res, error) {
	if err := c.requests.Close(); err != nil {
		var zero Res
		return zero, err
	}

	return readUnaryResponseFromMessageStream(c.responses)
}

func (c *clientStreamingCall[Req, Res]) Close() error {
	c.cancel()
	return errors.Join(c.requests.Close(), closeMessageStream(c.responses))
}

type bidirectionalStreamingCall[Req ProtoMessage, Res ProtoMessage] struct {
	requests  *MessagePipe[Req]
	responses MessageStream[Res]
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

func applyCallOptions(options []CallOption) CallOptions {
	callOptions := DefaultCallOptions()
	for _, option := range options {
		option(&callOptions)
	}

	return callOptions
}

func prepareClientRequest(service, method string, kind RpcKind, body []byte, options CallOptions) (*RpcRequest, error) {
	if err := ValidateMetadata(options.Metadata); err != nil {
		return nil, err
	}

	timeoutNanos, err := timeoutNanos(options)
	if err != nil {
		return nil, err
	}

	request := NewRpcRequest(service, method, body)
	request.Kind = kind
	request.Metadata = options.Metadata
	request.TimeoutNanos = timeoutNanos

	return request, nil
}

func timeoutNanos(options CallOptions) (uint64, error) {
	if !options.HasTimeout {
		return 0, nil
	}

	if options.Timeout < 0 {
		return 0, InvalidArgument("RPC timeout is negative")
	}

	if options.Timeout == 0 {
		return 1, nil
	}

	return uint64(options.Timeout), nil
}

func callContext(ctx context.Context, options CallOptions) (context.Context, context.CancelFunc) {
	if options.HasTimeout {
		return context.WithTimeout(ctx, options.Timeout)
	}

	return context.WithCancel(ctx)
}

func callContextWithoutLocalCancel(ctx context.Context, options CallOptions) (context.Context, context.CancelFunc) {
	if options.HasTimeout || ctx.Done() != nil {
		return callContext(ctx, options)
	}

	return ctx, func() {}
}

func callContextForRequestStream[Req ProtoMessage](ctx context.Context, options CallOptions, requests MessageStream[Req]) (context.Context, context.CancelFunc) {
	if isNonBlockingStream(requests) {
		return callContextWithoutLocalCancel(ctx, options)
	}

	return callContext(ctx, options)
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

type responseMessageStream[T ProtoMessage] struct {
	inner          FrameStream
	newMessage     func() T
	options        CallOptions
	ctx            context.Context
	cancel         context.CancelFunc
	messages       int
	streamBodySize int
	done           bool
	terminalStatus *Status
}

func newResponseMessageStream[T ProtoMessage](inner FrameStream, newMessage func() T, options CallOptions, ctx context.Context, cancel context.CancelFunc) *responseMessageStream[T] {
	return &responseMessageStream[T]{inner: inner, newMessage: newMessage, options: options, ctx: ctx, cancel: cancel}
}

func (s *responseMessageStream[T]) Recv() (T, error) {
	if s.done {
		var zero T
		return zero, io.EOF
	}

	frame, release, err := recvFrameFieldsWithTimeout(s.ctx, s.inner, s.options.StreamIdleTimeout)
	if release != nil {
		defer release()
	}
	if err != nil {
		s.finish()
		var zero T
		if err == io.EOF {
			return zero, Internal("response stream ended before final status")
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

		message := s.newMessage()
		if err := UnmarshalMessage(frame.body, message); err != nil {
			s.finish()
			var zero T
			return zero, InvalidArgument("failed to decode response: " + err.Error())
		}

		return message, nil
	case RpcStreamFrameKindStatus:
		if err := ValidateMetadata(frame.metadata); err != nil {
			s.finish()
			var zero T
			return zero, Internal("invalid response metadata: " + err.Error())
		}
		cleanupErr := s.finish()

		status := frame.statusValue()
		s.terminalStatus = status
		if status.IsOK() {
			if cleanupErr != nil {
				var zero T
				return zero, cleanupErr
			}
			var zero T
			return zero, io.EOF
		}

		var zero T
		return zero, status
	default:
		s.finish()
		var zero T
		return zero, InvalidArgument("response stream contained an unknown frame kind")
	}
}

func (s *responseMessageStream[T]) TerminalStatus() (*Status, bool) {
	if s.terminalStatus == nil {
		return nil, false
	}

	return s.terminalStatus, true
}

func (s *responseMessageStream[T]) finish() error {
	if !s.done {
		s.done = true
		s.cancel()
		return closeMessageStream(s.inner)
	}

	return nil
}

func (s *responseMessageStream[T]) Close() error {
	return s.finish()
}

func recvFrameWithTimeout(ctx context.Context, stream FrameStream, idleTimeout time.Duration) (*RpcStreamFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
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

func readUnaryResponseFromMessageStream[Res ProtoMessage](responses MessageStream[Res]) (Res, error) {
	first, err := responses.Recv()
	if err != nil {
		var zero Res
		if err == io.EOF {
			return zero, Internal("response stream ended without a response message")
		}

		return zero, err
	}

	second, err := responses.Recv()
	if err == io.EOF {
		return first, nil
	}

	if err != nil {
		var zero Res
		return zero, err
	}

	_ = second
	var zero Res
	return zero, Internal("client-streaming RPC returned more than one response message")
}

func saturatingAdd(left, right int) int {
	if right > 0 && left > math.MaxInt-right {
		return math.MaxInt
	}

	return left + right
}
