package trevrpc

import (
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"
)

// UnaryHandler handles an encoded unary request body and returns an encoded response body.
type UnaryHandler func(context.Context, []byte) ([]byte, error)

// StreamingHandler handles encoded streaming request bodies and returns encoded response bodies.
type StreamingHandler func(context.Context, []byte, ByteStream) (ByteStream, error)

type route struct {
	kind             RpcKind
	unaryHandler     UnaryHandler
	streamingHandler StreamingHandler
}

type methodKey struct {
	service string
	method  string
}

// Server routes and handles TrevRPC requests.
type Server struct {
	routes     map[methodKey]route
	options    ServerOptions
	authorizer Authorizer
	metrics    Metrics
}

// ServerOptions configures server limits, timeouts, and WebTransport behavior.
type ServerOptions struct {
	MaxFrameSize                      int
	MaxConcurrentConnections          int
	MaxConcurrentStreamsPerConnection int
	MaxConcurrentRequests             int
	GracefulShutdownTimeout           time.Duration
	InitialRequestTimeout             time.Duration
	MaxStreamMessages                 int
	MaxStreamBodySize                 int
	StreamIdleTimeout                 time.Duration
	EnableWebTransport                bool
	WebTransportPath                  string
	WebTransportCheckOrigin           func(*http.Request) bool
}

// DefaultServerOptions returns the default server limits and timeouts.
func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		MaxFrameSize:                      DefaultMaxFrameSize,
		MaxConcurrentConnections:          256,
		MaxConcurrentStreamsPerConnection: 64,
		MaxConcurrentRequests:             1024,
		GracefulShutdownTimeout:           30 * time.Second,
		InitialRequestTimeout:             10 * time.Second,
		MaxStreamMessages:                 4096,
		MaxStreamBodySize:                 16 * 1024 * 1024,
		StreamIdleTimeout:                 30 * time.Second,
		WebTransportPath:                  "/trevrpc",
	}
}

// Authorizer authorizes a request after metadata and protocol validation.
type Authorizer interface {
	// Authorize returns nil when the request is authorized.
	Authorize(context.Context, *RpcRequest) error
}

// MetadataValueAuthorizer authorizes requests with an exact metadata value.
type MetadataValueAuthorizer struct {
	key   string
	value []byte
}

// NewMetadataValueAuthorizer creates an authorizer for an exact metadata key/value pair.
func NewMetadataValueAuthorizer(key string, value []byte) MetadataValueAuthorizer {
	return MetadataValueAuthorizer{key: NormalizeMetadataKey(key), value: value}
}

// BearerAuthorizer creates an authorizer for the authorization bearer token metadata.
func BearerAuthorizer(token string) MetadataValueAuthorizer {
	return NewMetadataValueAuthorizer("authorization", []byte("Bearer "+token))
}

// Authorize checks whether request metadata contains the expected value.
func (a MetadataValueAuthorizer) Authorize(_ context.Context, request *RpcRequest) error {
	if subtle.ConstantTimeCompare(request.Metadata[a.key], a.value) == 1 {
		return nil
	}

	return Unauthenticated("request is not authenticated")
}

// Metrics receives RPC lifecycle events.
type Metrics interface {
	// RPCStarted is called when an RPC starts.
	RPCStarted(RPCStarted)
	// RPCFinished is called when an RPC finishes.
	RPCFinished(RPCFinished)
}

// NoopMetrics ignores all RPC lifecycle events.
type NoopMetrics struct{}

// RPCStarted ignores an RPC start event.
func (NoopMetrics) RPCStarted(RPCStarted) {}

// RPCFinished ignores an RPC finish event.
func (NoopMetrics) RPCFinished(RPCFinished) {}

// RPCStarted describes an RPC start event.
type RPCStarted struct {
	Service        string
	Method         string
	RequestBodyLen int
}

// RPCFinished describes an RPC completion event.
type RPCFinished struct {
	Service         string
	Method          string
	RequestBodyLen  int
	ResponseBodyLen int
	Code            Code
	Elapsed         time.Duration
}

// NewServer creates an empty server with default options.
func NewServer() *Server {
	return &Server{
		routes:  map[methodKey]route{},
		options: DefaultServerOptions(),
		metrics: NoopMetrics{},
	}
}

// SetOptions replaces all server options.
func (s *Server) SetOptions(options ServerOptions) {
	s.options = options
}

// Options returns a copy of the server options.
func (s *Server) Options() ServerOptions {
	return s.options
}

// SetAuthorizer installs an authorizer that runs before route lookup.
func (s *Server) SetAuthorizer(authorizer Authorizer) {
	s.authorizer = authorizer
}

// ClearAuthorizer removes the configured authorizer.
func (s *Server) ClearAuthorizer() {
	s.authorizer = nil
}

// SetMetrics installs metrics callbacks for RPC lifecycle events.
func (s *Server) SetMetrics(metrics Metrics) {
	if metrics == nil {
		metrics = NoopMetrics{}
	}

	s.metrics = metrics
}

// Route registers a unary route handler for a service and method.
func (s *Server) Route(service, method string, handler UnaryHandler) {
	s.routes[methodKey{service: service, method: method}] = route{kind: RpcKindUnary, unaryHandler: handler}
}

// RouteStreaming registers a streaming route handler for a service, method, and RPC kind.
func (s *Server) RouteStreaming(service, method string, kind RpcKind, handler StreamingHandler) {
	s.routes[methodKey{service: service, method: method}] = route{kind: kind, streamingHandler: handler}
}

// RegisterUnary registers a typed unary protobuf handler on the server.
func RegisterUnary[Req ProtoMessage, Res ProtoMessage](s *Server, service, method string, newRequest func() Req, handler func(context.Context, Req) (Res, error)) {
	s.Route(service, method, func(ctx context.Context, body []byte) ([]byte, error) {
		request := newRequest()
		if err := UnmarshalMessage(body, request); err != nil {
			return nil, InvalidArgument("failed to decode request: " + err.Error())
		}

		response, err := handler(ctx, request)
		if err != nil {
			return nil, err
		}

		return MarshalMessage(response)
	})
}

// HandleRequest handles a unary RPC request and returns a response.
func (s *Server) HandleRequest(ctx context.Context, request *RpcRequest) *RpcResponse {
	startedAt := time.Now()
	service := request.Service
	method := request.Method
	requestBodyLen := len(request.Body)
	recordRPCStarted(s.metrics, RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen})

	ctx, cancel, err := s.prepareRequest(ctx, request)
	if err != nil {
		return s.finishResponse(service, method, requestBodyLen, startedAt, StatusFromError(err).IntoResponse(nil))
	}
	defer cancel()

	route, ok := s.routes[methodKey{service: request.Service, method: request.Method}]
	if !ok || route.unaryHandler == nil {
		return s.finishResponse(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown RPC method %s/%s", request.Service, request.Method)).IntoResponse(nil))
	}

	responseBody, err := invokeUnaryHandler(ctx, route.unaryHandler, request.Body)
	if err != nil {
		return s.finishResponse(service, method, requestBodyLen, startedAt, StatusFromError(err).IntoResponse(nil))
	}

	return s.finishResponse(service, method, requestBodyLen, startedAt, OKResponse(responseBody))
}

// HandleStreamingRequest handles a streaming RPC request and returns response frames.
func (s *Server) HandleStreamingRequest(ctx context.Context, request *RpcRequest, requestBody ByteStream) FrameStream {
	startedAt := time.Now()
	service := request.Service
	method := request.Method
	requestBodyLen := len(request.Body)
	recordRPCStarted(s.metrics, RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen})

	ctx, cancel, err := s.prepareRequest(ctx, request)
	if err != nil {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, StatusFromError(err))
	}

	limits := streamLimitsFromOptions(s.options)
	requestBody = limitByteStream(ctx, requestBody, limits, "request")

	route, ok := s.routes[methodKey{service: request.Service, method: request.Method}]
	if !ok || route.streamingHandler == nil {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown streaming RPC method %s/%s", request.Service, request.Method)))
	}

	if route.kind != request.RPCKind() {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, InvalidArgument(fmt.Sprintf("streaming RPC kind mismatch for %s/%s: expected %d, got %d", request.Service, request.Method, route.kind, request.RPCKind())))
	}

	responseBody, err := invokeStreamingHandler(ctx, route.streamingHandler, request.Body, requestBody)
	if err != nil {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, StatusFromError(err))
	}

	return &serverResponseStream{
		inner:          responseBody,
		metrics:        s.metrics,
		service:        service,
		method:         method,
		requestBodyLen: requestBodyLen,
		startedAt:      startedAt,
		ctx:            ctx,
		limits:         limits,
		cancel:         cancel,
	}
}

type unaryHandlerResult struct {
	body []byte
	err  error
}

func invokeUnaryHandler(ctx context.Context, handler UnaryHandler, body []byte) ([]byte, error) {
	result := make(chan unaryHandlerResult, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- unaryHandlerResult{err: Internal(fmt.Sprintf("RPC handler panicked: %v", recovered))}
			}
		}()

		responseBody, err := handler(ctx, body)
		result <- unaryHandlerResult{body: responseBody, err: err}
	}()

	select {
	case result := <-result:
		return result.body, result.err
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	}
}

type streamingHandlerResult struct {
	stream ByteStream
	err    error
}

func invokeStreamingHandler(ctx context.Context, handler StreamingHandler, body []byte, requestBody ByteStream) (ByteStream, error) {
	result := make(chan streamingHandlerResult, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- streamingHandlerResult{err: Internal(fmt.Sprintf("RPC handler panicked: %v", recovered))}
			}
		}()

		responseBody, err := handler(ctx, body, requestBody)
		result <- streamingHandlerResult{stream: responseBody, err: err}
	}()

	select {
	case result := <-result:
		return result.stream, result.err
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	}
}

func (s *Server) prepareRequest(ctx context.Context, request *RpcRequest) (context.Context, context.CancelFunc, error) {
	if err := ValidateMetadata(request.Metadata); err != nil {
		return ctx, func() {}, err
	}

	if err := request.ValidateProtocol(); err != nil {
		return ctx, func() {}, err
	}

	ctx, cancel, err := requestContext(ctx, request)
	if err != nil {
		return ctx, cancel, err
	}

	if s.authorizer != nil {
		if err := s.authorizer.Authorize(ctx, request); err != nil {
			cancel()
			return ctx, func() {}, err
		}
	}

	return ctx, cancel, nil
}

func requestContext(ctx context.Context, request *RpcRequest) (context.Context, context.CancelFunc, error) {
	if request.TimeoutNanos == 0 {
		ctx, cancel := context.WithCancel(ctx)
		return ctx, cancel, nil
	}

	if request.TimeoutNanos > math.MaxInt64 {
		return ctx, func() {}, InvalidArgument("RPC timeout is too large")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.TimeoutNanos))
	return ctx, cancel, nil
}

func (s *Server) finishResponse(service, method string, requestBodyLen int, startedAt time.Time, response *RpcResponse) *RpcResponse {
	recordRPCFinished(s.metrics, RPCFinished{
		Service:         service,
		Method:          method,
		RequestBodyLen:  requestBodyLen,
		ResponseBodyLen: len(response.Body),
		Code:            CodeFromUint32(response.Status),
		Elapsed:         time.Since(startedAt),
	})

	return response
}

func (s *Server) finishStreamingStatus(service, method string, requestBodyLen int, startedAt time.Time, status *Status) FrameStream {
	s.finishStreamingResponse(service, method, requestBodyLen, 0, startedAt, status.Code)
	return StatusStream(status)
}

func (s *Server) recordRejectedRequest(request *RpcRequest, status *Status) {
	startedAt := time.Now()
	service := request.Service
	method := request.Method
	requestBodyLen := len(request.Body)
	recordRPCStarted(s.metrics, RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen})
	s.finishStreamingResponse(service, method, requestBodyLen, 0, startedAt, status.Code)
}

func (s *Server) recordPreHandlerFailure(status *Status) {
	startedAt := time.Now()
	recordRPCStarted(s.metrics, RPCStarted{})
	s.finishStreamingResponse("", "", 0, 0, startedAt, status.Code)
}

func (s *Server) finishStreamingResponse(service, method string, requestBodyLen, responseBodyLen int, startedAt time.Time, code Code) {
	recordRPCFinished(s.metrics, RPCFinished{
		Service:         service,
		Method:          method,
		RequestBodyLen:  requestBodyLen,
		ResponseBodyLen: responseBodyLen,
		Code:            code,
		Elapsed:         time.Since(startedAt),
	})
}

type streamLimits struct {
	maxMessages int
	maxBodySize int
	idleTimeout time.Duration
}

func streamLimitsFromOptions(options ServerOptions) streamLimits {
	return streamLimits{maxMessages: options.MaxStreamMessages, maxBodySize: options.MaxStreamBodySize, idleTimeout: options.StreamIdleTimeout}
}

type limitedByteStream struct {
	inner     ByteStream
	ctx       context.Context
	limits    streamLimits
	direction string
	messages  int
	bodySize  int
	done      bool
}

func limitByteStream(ctx context.Context, inner ByteStream, limits streamLimits, direction string) ByteStream {
	return &limitedByteStream{inner: inner, ctx: ctx, limits: limits, direction: direction}
}

func (s *limitedByteStream) Recv() ([]byte, error) {
	if s.done {
		return nil, io.EOF
	}

	body, err := recvByteWithTimeout(s.ctx, s.inner, s.limits.idleTimeout, s.direction)
	if err != nil {
		s.done = true
		return nil, err
	}

	if err := checkStreamLimits(s.direction, s.limits, &s.messages, &s.bodySize, len(body)); err != nil {
		s.done = true
		return nil, err
	}

	return body, nil
}

func (s *limitedByteStream) Close() error {
	if !s.done {
		s.done = true
		closeMessageStream(s.inner)
	}

	return nil
}

func recvByteWithTimeout(ctx context.Context, stream ByteStream, idleTimeout time.Duration, direction string) ([]byte, error) {
	type recvResult struct {
		body []byte
		err  error
	}

	results := make(chan recvResult, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				results <- recvResult{err: Internal(fmt.Sprintf("%s stream panicked: %v", direction, recovered))}
			}
		}()

		body, err := stream.Recv()
		results <- recvResult{body: body, err: err}
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
		return result.body, result.err
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	case <-idle:
		return nil, Unavailable(fmt.Sprintf("%s stream idle timeout", direction))
	}
}

func checkStreamLimits(direction string, limits streamLimits, messages, bodySize *int, itemLen int) error {
	if limits.maxMessages >= 0 && *messages >= limits.maxMessages {
		return ResourceExhausted(fmt.Sprintf("%s stream exceeded maximum of %d messages", direction, limits.maxMessages))
	}

	*messages = *messages + 1
	*bodySize = saturatingAdd(*bodySize, itemLen)

	if limits.maxBodySize >= 0 && *bodySize > limits.maxBodySize {
		return ResourceExhausted(fmt.Sprintf("%s stream exceeded maximum body size of %d bytes", direction, limits.maxBodySize))
	}

	return nil
}

type serverResponseStream struct {
	inner           ByteStream
	metrics         Metrics
	service         string
	method          string
	requestBodyLen  int
	responseBodyLen int
	startedAt       time.Time
	ctx             context.Context
	limits          streamLimits
	messages        int
	done            bool
	cancel          context.CancelFunc
	finishOnce      sync.Once
}

func (s *serverResponseStream) Recv() (*RpcStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	body, err := recvByteWithTimeout(s.ctx, s.inner, s.limits.idleTimeout, "response")
	if err == io.EOF {
		s.finish(CodeOK)
		return StatusFrame(OK()), nil
	}

	if err != nil {
		status := StatusFromError(err)
		s.finish(status.Code)
		return StatusFrame(status), nil
	}

	if err := checkStreamLimits("response", s.limits, &s.messages, &s.responseBodyLen, len(body)); err != nil {
		status := StatusFromError(err)
		s.finish(status.Code)
		return StatusFrame(status), nil
	}

	return MessageFrame(body), nil
}

func (s *serverResponseStream) Close() error {
	if !s.done {
		s.finish(CodeCancelled)
	}

	return nil
}

func (s *serverResponseStream) finish(code Code) {
	s.finishOnce.Do(func() {
		s.done = true
		s.cancel()
		closeMessageStream(s.inner)
		recordRPCFinished(s.metrics, RPCFinished{
			Service:         s.service,
			Method:          s.method,
			RequestBodyLen:  s.requestBodyLen,
			ResponseBodyLen: s.responseBodyLen,
			Code:            code,
			Elapsed:         time.Since(s.startedAt),
		})
	})
}

func recordRPCStarted(metrics Metrics, event RPCStarted) {
	defer func() { _ = recover() }()
	metrics.RPCStarted(event)
}

func recordRPCFinished(metrics Metrics, event RPCFinished) {
	defer func() { _ = recover() }()
	metrics.RPCFinished(event)
}
