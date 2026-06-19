package trevrpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"
)

type Transport interface {
	Call(context.Context, *RpcRequest) (*RpcResponse, error)
	StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error)
}

type CallOptions struct {
	Timeout                   time.Duration
	HasTimeout                bool
	MaxResponseBodySize       int
	MaxResponseMessages       int
	MaxResponseStreamBodySize int
	StreamIdleTimeout         time.Duration
	Metadata                  Metadata
}

type CallOption func(*CallOptions)

func DefaultCallOptions() CallOptions {
	return CallOptions{
		MaxResponseBodySize:       DefaultMaxFrameSize,
		MaxResponseMessages:       4096,
		MaxResponseStreamBodySize: 16 * 1024 * 1024,
		StreamIdleTimeout:         30 * time.Second,
		Metadata:                  Metadata{},
	}
}

func WithTimeout(timeout time.Duration) CallOption {
	return func(options *CallOptions) {
		options.Timeout = timeout
		options.HasTimeout = true
	}
}

func WithoutTimeout() CallOption {
	return func(options *CallOptions) {
		options.Timeout = 0
		options.HasTimeout = false
	}
}

func WithMaxResponseBodySize(max int) CallOption {
	return func(options *CallOptions) {
		options.MaxResponseBodySize = max
	}
}

func WithMaxResponseMessages(max int) CallOption {
	return func(options *CallOptions) {
		options.MaxResponseMessages = max
	}
}

func WithoutMaxResponseMessages() CallOption {
	return func(options *CallOptions) {
		options.MaxResponseMessages = -1
	}
}

func WithMaxResponseStreamBodySize(max int) CallOption {
	return func(options *CallOptions) {
		options.MaxResponseStreamBodySize = max
	}
}

func WithoutMaxResponseStreamBodySize() CallOption {
	return func(options *CallOptions) {
		options.MaxResponseStreamBodySize = -1
	}
}

func WithStreamIdleTimeout(timeout time.Duration) CallOption {
	return func(options *CallOptions) {
		options.StreamIdleTimeout = timeout
	}
}

func WithoutStreamIdleTimeout() CallOption {
	return func(options *CallOptions) {
		options.StreamIdleTimeout = 0
	}
}

func WithMetadata(key string, value []byte) CallOption {
	return func(options *CallOptions) {
		if options.Metadata == nil {
			options.Metadata = Metadata{}
		}

		options.Metadata[NormalizeMetadataKey(key)] = value
	}
}

func WithMetadataMap(metadata Metadata) CallOption {
	return func(options *CallOptions) {
		options.Metadata = metadata
	}
}

func Unary[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (Res, error) {
	callOptions := applyCallOptions(options)
	if err := ValidateMetadata(callOptions.Metadata); err != nil {
		var zero Res
		return zero, err
	}

	requestBody, err := MarshalMessage(request)
	if err != nil {
		var zero Res
		return zero, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindUnary, requestBody, callOptions)
	if err != nil {
		var zero Res
		return zero, err
	}

	ctx, cancel := callContext(ctx, callOptions)
	defer cancel()

	response, err := transport.Call(ctx, rpcRequest)
	if err != nil {
		var zero Res
		return zero, err
	}

	if err := validateResponse(response, callOptions.MaxResponseBodySize); err != nil {
		var zero Res
		return zero, err
	}

	message := newResponse()
	if err := UnmarshalMessage(response.Body, message); err != nil {
		var zero Res
		return zero, err
	}

	return message, nil
}

func ServerStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, request Req, newResponse func() Res, options ...CallOption) (MessageStream[Res], error) {
	callOptions := applyCallOptions(options)
	requestBody, err := MarshalMessage(request)
	if err != nil {
		return nil, err
	}

	rpcRequest, err := prepareClientRequest(service, method, RpcKindServerStreaming, requestBody, callOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContext(ctx, callOptions)
	response, err := transport.StreamingCall(ctx, rpcRequest, EmptyStream[[]byte]())
	if err != nil {
		cancel()
		return nil, err
	}

	return newResponseMessageStream(response, newResponse, callOptions, ctx, cancel), nil
}

func ClientStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, requests MessageStream[Req], newResponse func() Res, options ...CallOption) (Res, error) {
	callOptions := applyCallOptions(options)
	rpcRequest, err := prepareClientRequest(service, method, RpcKindClientStreaming, nil, callOptions)
	if err != nil {
		var zero Res
		return zero, err
	}

	ctx, cancel := callContext(ctx, callOptions)
	defer cancel()

	response, err := transport.StreamingCall(ctx, rpcRequest, EncodeStream(requests))
	if err != nil {
		var zero Res
		return zero, err
	}

	return readUnaryResponseFromStream(response, newResponse, callOptions, ctx)
}

func BidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](ctx context.Context, transport Transport, service, method string, requests MessageStream[Req], newResponse func() Res, options ...CallOption) (MessageStream[Res], error) {
	callOptions := applyCallOptions(options)
	rpcRequest, err := prepareClientRequest(service, method, RpcKindBidirectionalStreaming, nil, callOptions)
	if err != nil {
		return nil, err
	}

	ctx, cancel := callContext(ctx, callOptions)
	response, err := transport.StreamingCall(ctx, rpcRequest, EncodeStream(requests))
	if err != nil {
		cancel()
		return nil, err
	}

	return newResponseMessageStream(response, newResponse, callOptions, ctx, cancel), nil
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

	status := StatusFromResponse(response)
	if !status.IsOK() {
		return status
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
}

func newResponseMessageStream[T ProtoMessage](inner FrameStream, newMessage func() T, options CallOptions, ctx context.Context, cancel context.CancelFunc) MessageStream[T] {
	return &responseMessageStream[T]{inner: inner, newMessage: newMessage, options: options, ctx: ctx, cancel: cancel}
}

func (s *responseMessageStream[T]) Recv() (T, error) {
	if s.done {
		var zero T
		return zero, io.EOF
	}

	frame, err := recvFrameWithTimeout(s.ctx, s.inner, s.options.StreamIdleTimeout)
	if err != nil {
		s.finish()
		var zero T
		if err == io.EOF {
			return zero, Internal("response stream ended before final status")
		}

		return zero, err
	}

	frameKind, ok := frame.FrameKind()
	if !ok {
		s.finish()
		var zero T
		return zero, InvalidArgument("response stream contained an unknown frame kind")
	}

	switch frameKind {
	case RpcStreamFrameKindMessage:
		if s.options.MaxResponseMessages >= 0 && s.messages >= s.options.MaxResponseMessages {
			s.finish()
			var zero T
			return zero, ResourceExhausted(fmt.Sprintf("response stream exceeded maximum of %d messages", s.options.MaxResponseMessages))
		}

		if len(frame.Body) > s.options.MaxResponseBodySize {
			s.finish()
			var zero T
			return zero, &FrameTooLargeError{Len: len(frame.Body), Max: s.options.MaxResponseBodySize}
		}

		s.messages++
		s.streamBodySize = saturatingAdd(s.streamBodySize, len(frame.Body))
		if s.options.MaxResponseStreamBodySize >= 0 && s.streamBodySize > s.options.MaxResponseStreamBodySize {
			s.finish()
			var zero T
			return zero, ResourceExhausted(fmt.Sprintf("response stream exceeded maximum body size of %d bytes", s.options.MaxResponseStreamBodySize))
		}

		message := s.newMessage()
		if err := UnmarshalMessage(frame.Body, message); err != nil {
			s.finish()
			var zero T
			return zero, InvalidArgument("failed to decode response: " + err.Error())
		}

		return message, nil
	case RpcStreamFrameKindStatus:
		if err := ValidateMetadata(frame.Metadata); err != nil {
			s.finish()
			var zero T
			return zero, Internal("invalid response metadata: " + err.Error())
		}
		cleanupErr := s.finish()

		status := frame.StatusValue()
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

func readUnaryResponseFromStream[Res ProtoMessage](stream FrameStream, newResponse func() Res, options CallOptions, ctx context.Context) (Res, error) {
	responses := newResponseMessageStream(stream, newResponse, options, ctx, func() {})
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
