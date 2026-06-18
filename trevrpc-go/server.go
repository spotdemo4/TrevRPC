package trevrpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"
)

type UnaryHandler func(context.Context, []byte) ([]byte, error)
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

type Server struct {
	routes     map[methodKey]route
	options    ServerOptions
	authorizer Authorizer
	metrics    Metrics
}

type ServerOptions struct {
	MaxFrameSize                      int
	MaxConcurrentConnections          int
	MaxConcurrentStreamsPerConnection int
	MaxConcurrentRequests             int
	GracefulShutdownTimeout           time.Duration
	MaxStreamMessages                 int
	MaxStreamBodySize                 int
	StreamIdleTimeout                 time.Duration
}

func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		MaxFrameSize:                      DefaultMaxFrameSize,
		MaxConcurrentConnections:          1024,
		MaxConcurrentStreamsPerConnection: 128,
		MaxConcurrentRequests:             4096,
		GracefulShutdownTimeout:           30 * time.Second,
		MaxStreamMessages:                 4096,
		MaxStreamBodySize:                 64 * 1024 * 1024,
		StreamIdleTimeout:                 30 * time.Second,
	}
}

type Authorizer interface {
	Authorize(context.Context, *RpcRequest) error
}

type MetadataValueAuthorizer struct {
	key   string
	value []byte
}

func NewMetadataValueAuthorizer(key string, value []byte) MetadataValueAuthorizer {
	return MetadataValueAuthorizer{key: NormalizeMetadataKey(key), value: value}
}

func BearerAuthorizer(token string) MetadataValueAuthorizer {
	return NewMetadataValueAuthorizer("authorization", []byte("Bearer "+token))
}

func (a MetadataValueAuthorizer) Authorize(_ context.Context, request *RpcRequest) error {
	if string(request.Metadata[a.key]) == string(a.value) {
		return nil
	}

	return Unauthenticated("request is not authenticated")
}

type Metrics interface {
	RPCStarted(RPCStarted)
	RPCFinished(RPCFinished)
}

type NoopMetrics struct{}

func (NoopMetrics) RPCStarted(RPCStarted)   {}
func (NoopMetrics) RPCFinished(RPCFinished) {}

type RPCStarted struct {
	Service        string
	Method         string
	RequestBodyLen int
}

type RPCFinished struct {
	Service         string
	Method          string
	RequestBodyLen  int
	ResponseBodyLen int
	Code            Code
	Elapsed         time.Duration
}

func NewServer() *Server {
	return &Server{
		routes:  map[methodKey]route{},
		options: DefaultServerOptions(),
		metrics: NoopMetrics{},
	}
}

func (s *Server) SetOptions(options ServerOptions) {
	s.options = options
}

func (s *Server) Options() ServerOptions {
	return s.options
}

func (s *Server) SetAuthorizer(authorizer Authorizer) {
	s.authorizer = authorizer
}

func (s *Server) ClearAuthorizer() {
	s.authorizer = nil
}

func (s *Server) SetMetrics(metrics Metrics) {
	if metrics == nil {
		metrics = NoopMetrics{}
	}

	s.metrics = metrics
}

func (s *Server) Route(service, method string, handler UnaryHandler) {
	s.routes[methodKey{service: service, method: method}] = route{kind: RpcKindUnary, unaryHandler: handler}
}

func (s *Server) RouteStreaming(service, method string, kind RpcKind, handler StreamingHandler) {
	s.routes[methodKey{service: service, method: method}] = route{kind: kind, streamingHandler: handler}
}

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

func (s *Server) HandleRequest(ctx context.Context, request *RpcRequest) *RpcResponse {
	startedAt := time.Now()
	service := request.Service
	method := request.Method
	requestBodyLen := len(request.Body)
	s.metrics.RPCStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen})

	ctx, cancel, err := s.prepareRequest(ctx, request)
	if err != nil {
		return s.finishResponse(service, method, requestBodyLen, startedAt, StatusFromError(err).IntoResponse(nil))
	}
	defer cancel()

	route, ok := s.routes[methodKey{service: request.Service, method: request.Method}]
	if !ok || route.unaryHandler == nil {
		return s.finishResponse(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown RPC method %s/%s", request.Service, request.Method)).IntoResponse(nil))
	}

	responseBody, err := route.unaryHandler(ctx, request.Body)
	if err != nil {
		return s.finishResponse(service, method, requestBodyLen, startedAt, StatusFromError(err).IntoResponse(nil))
	}

	return s.finishResponse(service, method, requestBodyLen, startedAt, OKResponse(responseBody))
}

func (s *Server) HandleStreamingRequest(ctx context.Context, request *RpcRequest, requestBody ByteStream) FrameStream {
	startedAt := time.Now()
	service := request.Service
	method := request.Method
	requestBodyLen := len(request.Body)
	s.metrics.RPCStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen})

	ctx, cancel, err := s.prepareRequest(ctx, request)
	if err != nil {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, StatusFromError(err))
	}

	limits := streamLimitsFromOptions(s.options)
	requestBody = limitByteStream(requestBody, limits, "request")

	route, ok := s.routes[methodKey{service: request.Service, method: request.Method}]
	if !ok || route.streamingHandler == nil {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown streaming RPC method %s/%s", request.Service, request.Method)))
	}

	if route.kind != request.RPCKind() {
		cancel()
		return s.finishStreamingStatus(service, method, requestBodyLen, startedAt, InvalidArgument(fmt.Sprintf("streaming RPC kind mismatch for %s/%s: expected %d, got %d", request.Service, request.Method, route.kind, request.RPCKind())))
	}

	responseBody, err := route.streamingHandler(ctx, request.Body, requestBody)
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
		limits:         limits,
		cancel:         cancel,
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
	if request.DeadlineUnixNanos == 0 {
		ctx, cancel := context.WithCancel(ctx)
		return ctx, cancel, nil
	}

	if request.DeadlineUnixNanos > math.MaxInt64 {
		return ctx, func() {}, InvalidArgument("RPC deadline is too large")
	}

	deadline := time.Unix(0, int64(request.DeadlineUnixNanos))
	if time.Until(deadline) <= 0 {
		return ctx, func() {}, DeadlineExceeded("RPC deadline exceeded")
	}

	ctx, cancel := context.WithDeadline(ctx, deadline)
	return ctx, cancel, nil
}

func (s *Server) finishResponse(service, method string, requestBodyLen int, startedAt time.Time, response *RpcResponse) *RpcResponse {
	s.metrics.RPCFinished(RPCFinished{
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

func (s *Server) finishStreamingResponse(service, method string, requestBodyLen, responseBodyLen int, startedAt time.Time, code Code) {
	s.metrics.RPCFinished(RPCFinished{
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
	limits    streamLimits
	direction string
	messages  int
	bodySize  int
	done      bool
}

func limitByteStream(inner ByteStream, limits streamLimits, direction string) ByteStream {
	return &limitedByteStream{inner: inner, limits: limits, direction: direction}
}

func (s *limitedByteStream) Recv() ([]byte, error) {
	if s.done {
		return nil, io.EOF
	}

	body, err := s.inner.Recv()
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
	limits          streamLimits
	messages        int
	done            bool
	cancel          context.CancelFunc
}

func (s *serverResponseStream) Recv() (*RpcStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	body, err := s.inner.Recv()
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

func (s *serverResponseStream) finish(code Code) {
	if s.done {
		return
	}

	s.done = true
	s.cancel()
	s.metrics.RPCFinished(RPCFinished{
		Service:         s.service,
		Method:          s.method,
		RequestBodyLen:  s.requestBodyLen,
		ResponseBodyLen: s.responseBodyLen,
		Code:            code,
		Elapsed:         time.Since(s.startedAt),
	})
}
