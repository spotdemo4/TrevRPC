package trevrpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// unaryHandler handles an encoded unary request and returns a full response envelope.
type unaryHandler func(context.Context, []byte) (*wireResponse, error)

// streamingHandler handles encoded streaming request bodies and returns encoded response bodies.
type streamingHandler func(context.Context, []byte, byteStream) (byteStream, error)

type route struct {
	kind      RpcKind
	unary     unaryHandler
	streaming streamingHandler
}

type methodKey struct {
	service string
	method  string
}

// ErrServerFrozen is the panic value used when a server is mutated after first use.
var ErrServerFrozen = errors.New("trevrpc: server configuration is frozen")

// Server routes and handles TrevRPC requests.
type Server struct {
	mu          sync.Mutex
	routes      map[methodKey]route
	options     ServerOptions
	authorizer  Authorizer
	metrics     Metrics
	diagnostics ServerDiagnosticHandler
	runtime     *serverRuntime
}

// ServerOptions configures server limits, timeouts, and the HTTP and browser
// session transports.
//
// EnableHTTP serves the HTTP mapping (POST to HTTPPath) over HTTP/3 and, on
// the TCP listener, HTTP/2. EnableWebTransport serves browser sessions at
// HTTPPath: WebTransport over HTTP/3 and, on the TCP listener, QMux over
// WebSocket. Native QUIC and QMux over TLS are always served.
type ServerOptions struct {
	MaxFrameSize                      int
	MaxConcurrentConnections          int
	MaxConcurrentStreamsPerConnection int
	MaxConcurrentRequests             int
	MaxConcurrentAdmissionCallbacks   int
	GracefulShutdownTimeout           time.Duration
	DisableGracefulShutdownTimeout    bool
	InitialRequestTimeout             time.Duration
	DisableInitialRequestTimeout      bool
	MaxStreamMessages                 int
	MaxStreamBodySize                 int
	StreamIdleTimeout                 time.Duration
	DisableStreamIdleTimeout          bool
	EnableHTTP                        bool
	HTTPPath                          string
	HTTPAdmission                     Admission
	EnableWebTransport                bool
	WebTransportAdmission             Admission
}

// AdmissionRequest describes an HTTP RPC request or a browser session request
// before the server accepts it.
type AdmissionRequest struct {
	// Header is a private copy of the request headers.
	Header http.Header
	Path   string
	// Method is the HTTP method: POST for HTTP RPCs, CONNECT for WebTransport
	// sessions and GET for WebSocket sessions.
	Method string
	// Proto is the HTTP version of the request, such as "HTTP/3.0".
	Proto     string
	Authority string
	// Origin is the value of the Origin header, if any.
	Origin string
	Secure bool
}

// Admission decides whether to accept an otherwise valid HTTP RPC request or
// browser session. A nil HTTPAdmission accepts valid requests; a nil
// WebTransportAdmission rejects every browser session, over WebTransport and
// WebSocket alike.
type Admission func(AdmissionRequest) bool

// ServerDiagnosticPhase identifies a local server-runtime failure boundary.
type ServerDiagnosticPhase string

const (
	ServerDiagnosticHandlerPanic           ServerDiagnosticPhase = "handler_panic"
	ServerDiagnosticAuthorizerPanic        ServerDiagnosticPhase = "authorizer_panic"
	ServerDiagnosticRequestStreamPanic     ServerDiagnosticPhase = "request_stream_panic"
	ServerDiagnosticResponseStreamPanic    ServerDiagnosticPhase = "response_stream_panic"
	ServerDiagnosticMetricsPanic           ServerDiagnosticPhase = "metrics_panic"
	ServerDiagnosticAdmissionPanic         ServerDiagnosticPhase = "admission_panic"
	ServerDiagnosticSessionConnect         ServerDiagnosticPhase = "session_connect"
	ServerDiagnosticSessionAdmission       ServerDiagnosticPhase = "session_admission"
	ServerDiagnosticSessionUpgrade         ServerDiagnosticPhase = "session_upgrade"
	ServerDiagnosticSessionUpgradeSuccess  ServerDiagnosticPhase = "session_upgrade_success"
	ServerDiagnosticSessionClosed          ServerDiagnosticPhase = "session_closed"
	ServerDiagnosticHTTP3ConnectionClosed  ServerDiagnosticPhase = "http3_connection_closed"
	ServerDiagnosticTLSHandshakeFailed     ServerDiagnosticPhase = "tls_handshake_failed"
	ServerDiagnosticInvalidResponse        ServerDiagnosticPhase = "invalid_response"
	ServerDiagnosticInternalError          ServerDiagnosticPhase = "internal_error"
	ServerDiagnosticExecutionDetached      ServerDiagnosticPhase = "execution_detached"
	ServerDiagnosticExecutionFinallyExited ServerDiagnosticPhase = "execution_finally_exited"
	ServerDiagnosticShutdownIncomplete     ServerDiagnosticPhase = "shutdown_incomplete"
)

// ServerDiagnostic describes a local server failure. Request bodies and metadata are never included.
type ServerDiagnostic struct {
	Phase                  ServerDiagnosticPhase
	Message                string
	RemoteAddr             string
	Service                string
	Method                 string
	Kind                   RpcKind
	Err                    error
	Panic                  any
	Stack                  []byte
	ActiveExecutionCount   int64
	DetachedExecutionCount int64
	DroppedEventCount      uint64
}

// ServerDiagnosticHandler receives bounded asynchronous local server diagnostics.
type ServerDiagnosticHandler func(ServerDiagnostic)

const serverDiagnosticQueueCapacity = 64

// DefaultServerOptions returns the default server limits and timeouts.
func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		MaxFrameSize:                      DefaultMaxFrameSize,
		MaxConcurrentConnections:          256,
		MaxConcurrentStreamsPerConnection: 64,
		MaxConcurrentRequests:             1024,
		MaxConcurrentAdmissionCallbacks:   64,
		GracefulShutdownTimeout:           30 * time.Second,
		InitialRequestTimeout:             10 * time.Second,
		MaxStreamMessages:                 4096,
		MaxStreamBodySize:                 16 * 1024 * 1024,
		StreamIdleTimeout:                 30 * time.Second,
		HTTPPath:                          DefaultHTTPPath,
	}
}

// Authorizer authorizes a request after metadata and protocol validation. The
// request body is not available to it.
type Authorizer interface {
	// Authorize returns nil when the request is authorized. A returned *Status
	// is sent to the caller; any other error becomes Internal.
	Authorize(context.Context, HandlerContext) error
}

// metadataValueAuthorizer authorizes requests with an exact metadata value.
type metadataValueAuthorizer struct {
	key   string
	value []byte
}

// NewMetadataValueAuthorizer creates an authorizer for an exact metadata key/value pair.
func NewMetadataValueAuthorizer(key string, value []byte) Authorizer {
	return metadataValueAuthorizer{key: NormalizeMetadataKey(key), value: append([]byte(nil), value...)}
}

// BearerAuthorizer creates an authorizer for the authorization bearer token metadata.
func BearerAuthorizer(token string) Authorizer {
	return NewMetadataValueAuthorizer("authorization", []byte("Bearer "+token))
}

// Authorize checks whether request metadata contains the expected value.
func (a metadataValueAuthorizer) Authorize(_ context.Context, info HandlerContext) error {
	if subtle.ConstantTimeCompare(info.Metadata[a.key], a.value) == 1 {
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

// SetOptions canonicalizes and replaces the server options. It panics after first use.
func (s *Server) SetOptions(options ServerOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.options = canonicalServerOptions(options)
}

// Options returns the canonical server options snapshot.
func (s *Server) Options() ServerOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runtime != nil {
		return s.runtime.options
	}
	return s.options
}

// SetAuthorizer installs an authorizer that runs before route lookup. A nil
// authorizer removes it.
func (s *Server) SetAuthorizer(authorizer Authorizer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.authorizer = authorizer
}

// SetMetrics installs metrics callbacks for RPC lifecycle events.
func (s *Server) SetMetrics(metrics Metrics) {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.metrics = metrics
}

// SetDiagnostics installs a bounded asynchronous local diagnostic callback.
func (s *Server) SetDiagnostics(handler ServerDiagnosticHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.diagnostics = handler
}

// routeUnary registers a unary route handler for a service and method.
func (s *Server) routeUnary(service, method string, handler unaryHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	validateRoute(service, method, RpcKindUnary, handler != nil)
	s.routes[methodKey{service: service, method: method}] = route{kind: RpcKindUnary, unary: handler}
}

// routeStreaming registers a streaming route handler for a service, method, and RPC kind.
func (s *Server) routeStreaming(service, method string, kind RpcKind, handler streamingHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	if kind == RpcKindUnary || !kind.isValid() {
		panic("trevrpc: streaming route requires a streaming RPC kind")
	}
	validateRoute(service, method, kind, handler != nil)
	s.routes[methodKey{service: service, method: method}] = route{kind: kind, streaming: handler}
}

// RegisterUnary registers a unary handler. The handler may attach response
// metadata through the returned Response.
func RegisterUnary[Req ProtoMessage, Res ProtoMessage](s *Server, service, method string, newRequest func() Req, handler func(context.Context, Req) (*Response[Res], error)) {
	s.routeUnary(service, method, func(ctx context.Context, body []byte) (*wireResponse, error) {
		request, err := decodeRegisteredRequest(body, newRequest)
		if err != nil {
			return nil, err
		}

		response, err := handler(ctx, request)
		if err != nil {
			return nil, err
		}
		return encodeRegisteredResponse(response)
	})
}

// RegisterClientStreaming registers a client-streaming handler. The handler
// may attach response metadata through the returned Response.
func RegisterClientStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, service, method string, newRequest func() Req, handler func(context.Context, MessageStream[Req]) (*Response[Res], error)) {
	s.routeStreaming(service, method, RpcKindClientStreaming, func(ctx context.Context, _ []byte, requests byteStream) (byteStream, error) {
		response, err := handler(ctx, newDecodeStream(requests, newRequest))
		if err != nil {
			return nil, err
		}
		if err := validateRegisteredResponse(response); err != nil {
			return nil, err
		}
		return newEncodeStream(NewResponseStream(newSliceStream(response.Message), response.Metadata)), nil
	})
}

// RegisterServerStreaming registers a server-streaming handler. If the
// returned stream implements ResponseStream (see NewResponseStream), its
// terminal metadata is sent with the final status.
func RegisterServerStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, service, method string, newRequest func() Req, handler func(context.Context, Req) (MessageStream[Res], error)) {
	s.routeStreaming(service, method, RpcKindServerStreaming, func(ctx context.Context, body []byte, _ byteStream) (byteStream, error) {
		request, err := decodeRegisteredRequest(body, newRequest)
		if err != nil {
			return nil, err
		}

		responses, err := handler(ctx, request)
		if err != nil {
			return nil, err
		}
		if isNilRegisteredValue(responses) {
			return nil, Internal("handler returned nil response stream")
		}
		return newEncodeStream(responses), nil
	})
}

// RegisterBidirectionalStreaming registers a bidirectional-streaming handler.
// If the returned stream implements ResponseStream (see NewResponseStream), its
// terminal metadata is sent with the final status.
func RegisterBidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, service, method string, newRequest func() Req, handler func(context.Context, MessageStream[Req]) (MessageStream[Res], error)) {
	s.routeStreaming(service, method, RpcKindBidirectionalStreaming, func(ctx context.Context, _ []byte, requests byteStream) (byteStream, error) {
		responses, err := handler(ctx, newDecodeStream(requests, newRequest))
		if err != nil {
			return nil, err
		}
		if isNilRegisteredValue(responses) {
			return nil, Internal("handler returned nil response stream")
		}
		return newEncodeStream(responses), nil
	})
}

func decodeRegisteredRequest[Req ProtoMessage](body []byte, newRequest func() Req) (Req, error) {
	request := newRequest()
	if isNilRegisteredValue(request) {
		var zero Req
		return zero, Internal("request factory returned nil")
	}
	if err := unmarshalMessage(body, request); err != nil {
		var zero Req
		return zero, InvalidArgument("failed to decode request: " + err.Error())
	}
	return request, nil
}

func encodeRegisteredResponse[Res ProtoMessage](response *Response[Res]) (*wireResponse, error) {
	if err := validateRegisteredResponse(response); err != nil {
		return nil, err
	}
	body, err := marshalMessage(response.Message)
	if err != nil {
		return nil, err
	}
	return OK().intoResponseWithMetadata(body, cloneMetadata(response.Metadata)), nil
}

func validateRegisteredResponse[Res ProtoMessage](response *Response[Res]) error {
	if response == nil {
		return Internal("handler returned nil response")
	}
	if isNilRegisteredValue(response.Message) {
		return Internal("handler returned nil response message")
	}
	if err := validateMetadata(response.Metadata); err != nil {
		return Internal("handler returned invalid response metadata: " + err.Error())
	}
	return nil
}

func isNilRegisteredValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type serverRuntime struct {
	options          ServerOptions
	routes           map[methodKey]route
	authorizer       Authorizer
	metrics          Metrics
	requestLimit     semaphore
	connectionLimit  semaphore
	admissionLimit   semaphore
	diagnostics      *serverDiagnosticDispatcher
	active           atomic.Int64
	detached         atomic.Int64
	executionMu      sync.Mutex
	executionChanged chan struct{}
}

func canonicalServerOptions(options ServerOptions) ServerOptions {
	defaults := DefaultServerOptions()
	canonical := options
	canonical.MaxFrameSize = canonicalPositiveLimit("MaxFrameSize", options.MaxFrameSize, defaults.MaxFrameSize)
	if uint64(canonical.MaxFrameSize) > math.MaxUint32 {
		panic("trevrpc: MaxFrameSize exceeds uint32 framing limit")
	}
	canonical.MaxConcurrentConnections = canonicalPositiveLimit("MaxConcurrentConnections", options.MaxConcurrentConnections, defaults.MaxConcurrentConnections)
	canonical.MaxConcurrentStreamsPerConnection = canonicalPositiveLimit("MaxConcurrentStreamsPerConnection", options.MaxConcurrentStreamsPerConnection, defaults.MaxConcurrentStreamsPerConnection)
	canonical.MaxConcurrentRequests = canonicalPositiveLimit("MaxConcurrentRequests", options.MaxConcurrentRequests, defaults.MaxConcurrentRequests)
	canonical.MaxConcurrentAdmissionCallbacks = canonicalPositiveLimit("MaxConcurrentAdmissionCallbacks", options.MaxConcurrentAdmissionCallbacks, defaults.MaxConcurrentAdmissionCallbacks)
	canonical.MaxStreamMessages = canonicalStreamLimit("MaxStreamMessages", options.MaxStreamMessages, defaults.MaxStreamMessages)
	canonical.MaxStreamBodySize = canonicalStreamLimit("MaxStreamBodySize", options.MaxStreamBodySize, defaults.MaxStreamBodySize)
	canonical.GracefulShutdownTimeout = canonicalDuration("GracefulShutdownTimeout", options.GracefulShutdownTimeout, defaults.GracefulShutdownTimeout, options.DisableGracefulShutdownTimeout)
	canonical.InitialRequestTimeout = canonicalDuration("InitialRequestTimeout", options.InitialRequestTimeout, defaults.InitialRequestTimeout, options.DisableInitialRequestTimeout)
	canonical.StreamIdleTimeout = canonicalDuration("StreamIdleTimeout", options.StreamIdleTimeout, defaults.StreamIdleTimeout, options.DisableStreamIdleTimeout)
	if canonical.HTTPPath == "" {
		canonical.HTTPPath = defaults.HTTPPath
	}
	parsed, err := url.ParseRequestURI(canonical.HTTPPath)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" || canonical.HTTPPath[0] != '/' {
		panic("trevrpc: HTTPPath must be an absolute path without query or fragment")
	}
	return canonical
}

func canonicalPositiveLimit(name string, value, fallback int) int {
	if value < 0 {
		panic("trevrpc: " + name + " must be positive")
	}
	if value == 0 {
		return fallback
	}
	return value
}

func canonicalStreamLimit(name string, value, fallback int) int {
	if value < -1 {
		panic("trevrpc: " + name + " must be -1 or positive")
	}
	if value == 0 {
		return fallback
	}
	return value
}

func canonicalDuration(name string, value, fallback time.Duration, disabled bool) time.Duration {
	if value < 0 {
		panic("trevrpc: " + name + " must not be negative")
	}
	if disabled {
		return 0
	}
	if value == 0 {
		return fallback
	}
	return value
}

func validateRoute(service, method string, _ RpcKind, hasHandler bool) {
	if service == "" || method == "" {
		panic("trevrpc: route service and method must be non-empty")
	}
	if !hasHandler {
		panic("trevrpc: route handler must not be nil")
	}
}

func (s *Server) requireMutableLocked() {
	if s.runtime != nil {
		panic(ErrServerFrozen)
	}
}

func (s *Server) freeze() *serverRuntime {
	if s == nil {
		panic("trevrpc: server is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runtime != nil {
		return s.runtime
	}
	options := canonicalServerOptions(s.options)
	routes := maps.Clone(s.routes)
	metrics := s.metrics
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	runtime := &serverRuntime{
		options:          options,
		routes:           routes,
		authorizer:       s.authorizer,
		metrics:          metrics,
		requestLimit:     newSemaphore(options.MaxConcurrentRequests),
		connectionLimit:  newSemaphore(options.MaxConcurrentConnections),
		admissionLimit:   newSemaphore(options.MaxConcurrentAdmissionCallbacks),
		executionChanged: make(chan struct{}),
	}
	runtime.diagnostics = newServerDiagnosticDispatcher(s.diagnostics, runtime)
	s.options = options
	s.runtime = runtime
	return runtime
}

type serverDiagnosticDispatcher struct {
	hook    ServerDiagnosticHandler
	runtime *serverRuntime
	mu      sync.Mutex
	queue   []ServerDiagnostic
	running bool
	dropped atomic.Uint64
}

func newServerDiagnosticDispatcher(hook ServerDiagnosticHandler, runtime *serverRuntime) *serverDiagnosticDispatcher {
	return &serverDiagnosticDispatcher{hook: hook, runtime: runtime}
}

func (d *serverDiagnosticDispatcher) emit(event ServerDiagnostic) {
	if d == nil || d.hook == nil {
		return
	}
	event.ActiveExecutionCount = d.runtime.active.Load()
	event.DetachedExecutionCount = d.runtime.detached.Load()
	d.mu.Lock()
	if len(d.queue) == serverDiagnosticQueueCapacity {
		copy(d.queue, d.queue[1:])
		d.queue = d.queue[:serverDiagnosticQueueCapacity-1]
		d.dropped.Add(1)
	}
	event.DroppedEventCount = d.dropped.Load()
	d.queue = append(d.queue, event)
	if !d.running {
		d.running = true
		go d.run()
	}
	d.mu.Unlock()
}

func (d *serverDiagnosticDispatcher) run() {
	for {
		d.mu.Lock()
		if len(d.queue) == 0 {
			d.running = false
			d.mu.Unlock()
			return
		}
		event := d.queue[0]
		d.queue = d.queue[1:]
		d.mu.Unlock()
		func() {
			defer func() { _ = recover() }()
			d.hook(event)
		}()
	}
}

func (r *serverRuntime) emitDiagnostic(event ServerDiagnostic) {
	if r != nil && r.diagnostics != nil {
		r.diagnostics.emit(event)
	}
}

func (r *serverRuntime) executionStarted() {
	r.executionMu.Lock()
	r.active.Add(1)
	close(r.executionChanged)
	r.executionChanged = make(chan struct{})
	r.executionMu.Unlock()
}

func (r *serverRuntime) executionFinished() {
	r.executionMu.Lock()
	r.active.Add(-1)
	close(r.executionChanged)
	r.executionChanged = make(chan struct{})
	r.executionMu.Unlock()
}

func (r *serverRuntime) waitForExecutions(timeout time.Duration) bool {
	var timeoutChannel <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutChannel = timer.C
		defer timer.Stop()
	}

	for {
		r.executionMu.Lock()
		if r.active.Load() == 0 {
			r.executionMu.Unlock()
			return true
		}
		changed := r.executionChanged
		r.executionMu.Unlock()

		select {
		case <-changed:
		case <-timeoutChannel:
			return false
		}
	}
}

type requestExecutionLease struct {
	runtime *serverRuntime
	limit   semaphore
	service string
	method  string
	kind    RpcKind
	refs    atomic.Int64
}

func (r *serverRuntime) tryRequestLease(request *wireRequest) (*requestExecutionLease, bool) {
	if !tryAcquire(r.requestLimit) {
		return nil, false
	}
	lease := &requestExecutionLease{runtime: r, limit: r.requestLimit}
	lease.refs.Store(1)
	if request != nil {
		lease.service, lease.method, lease.kind = request.Service, request.Method, request.rpcKind()
	}
	return lease, true
}

func (l *requestExecutionLease) release() {
	if l != nil && l.refs.Add(-1) == 0 {
		release(l.limit)
	}
}

func (l *requestExecutionLease) retain(phase ServerDiagnosticPhase) *executionReference {
	if l == nil {
		return nil
	}
	l.refs.Add(1)
	l.runtime.executionStarted()
	return &executionReference{lease: l, phase: phase}
}

type executionReference struct {
	lease    *requestExecutionLease
	phase    ServerDiagnosticPhase
	mu       sync.Mutex
	detached bool
	finished bool
}

func (r *executionReference) detach() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.detached || r.finished {
		r.mu.Unlock()
		return
	}
	r.detached = true
	r.lease.runtime.detached.Add(1)
	r.mu.Unlock()
	r.lease.runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticExecutionDetached, Service: r.lease.service, Method: r.lease.method, Kind: r.lease.kind})
}

func (r *executionReference) finish() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	r.finished = true
	detached := r.detached
	r.mu.Unlock()
	r.lease.release()
	r.lease.runtime.executionFinished()
	if detached {
		r.lease.runtime.detached.Add(-1)
		r.lease.runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticExecutionFinallyExited, Service: r.lease.service, Method: r.lease.method, Kind: r.lease.kind})
	}
}

type serverPanicError struct {
	phase     ServerDiagnosticPhase
	recovered any
	stack     []byte
}

func (e *serverPanicError) Error() string {
	return fmt.Sprintf("server callback panicked: %v", e.recovered)
}

func internalServerStatus() *Status { return Internal("internal server error") }

func (r *serverRuntime) wireStatus(err error, phase ServerDiagnosticPhase, request *wireRequest) *Status {
	if err == nil {
		return OK()
	}
	var panicErr *serverPanicError
	if errors.As(err, &panicErr) {
		r.emitDiagnostic(ServerDiagnostic{Phase: panicErr.phase, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err, Panic: panicErr.recovered, Stack: panicErr.stack})
		return internalServerStatus()
	}
	var status *Status
	if errors.As(err, &status) && status.Code != CodeInternal {
		if validateMetadata(status.Metadata) == nil {
			return NewStatus(status.Code, status.Message).WithMetadata(status.Metadata)
		}
	}
	r.emitDiagnostic(ServerDiagnostic{Phase: phase, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err})
	return internalServerStatus()
}

func requestService(request *wireRequest) string {
	if request == nil {
		return ""
	}
	return request.Service
}
func requestMethod(request *wireRequest) string {
	if request == nil {
		return ""
	}
	return request.Method
}
func requestKind(request *wireRequest) RpcKind {
	if request == nil {
		return RpcKindUnary
	}
	return request.rpcKind()
}

func (r *serverRuntime) handleRequest(ctx context.Context, request *wireRequest, lease *requestExecutionLease) *wireResponse {
	startedAt := time.Now()
	if request == nil {
		r.recordStarted(RPCStarted{}, nil)
		return r.finishResponse("", "", 0, startedAt, InvalidArgument("RPC request is nil").intoResponse(nil), nil)
	}
	service, method, requestBodyLen := request.Service, request.Method, len(request.Body)
	r.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen}, request)

	ctx, cancel, err := r.prepareRequest(ctx, request, lease)
	if err != nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request).intoResponse(nil), request)
	}
	defer cancel()

	route, ok := r.routes[methodKey{service: request.Service, method: request.Method}]
	if !ok || route.unary == nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown RPC method %s/%s", request.Service, request.Method)).intoResponse(nil), request)
	}
	response, err := invokeUnaryHandler(ctx, route.unary, request.Body, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request).intoResponse(nil), request)
	}
	response = r.sanitizeResponse(response, request)
	return r.finishResponse(service, method, requestBodyLen, startedAt, response, request)
}

func (r *serverRuntime) sanitizeResponse(response *wireResponse, request *wireRequest) *wireResponse {
	if response == nil {
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: errors.New("handler returned nil response")})
		return internalServerStatus().intoResponse(nil)
	}
	status := statusFromWireResponse(response)
	if status.Code == CodeInternal {
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInternalError, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: status})
		return internalServerStatus().intoResponse(nil)
	}
	if err := validateMetadata(response.Metadata); err != nil {
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err})
		return internalServerStatus().intoResponse(nil)
	}
	clone := *response
	clone.Body = append([]byte(nil), response.Body...)
	clone.Metadata = cloneMetadata(response.Metadata)
	return &clone
}

func invokeUnaryHandler(
	ctx context.Context,
	handler unaryHandler,
	body []byte,
	raceContext bool,
	lease *requestExecutionLease,
) (response *wireResponse, err error) {
	if !raceContext {
		defer func() {
			if recovered := recover(); recovered != nil {
				response = nil
				err = &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}
			}
		}()
		return handler(ctx, body)
	}
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	result := make(chan struct {
		response *wireResponse
		err      error
	}, 1)
	execution := lease.retain(ServerDiagnosticHandlerPanic)
	go func() {
		defer execution.finish()
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- struct {
					response *wireResponse
					err      error
				}{err: &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}}
			}
		}()
		response, err := handler(ctx, body)
		result <- struct {
			response *wireResponse
			err      error
		}{response: response, err: err}
	}()

	select {
	case result := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, statusFromContextError(ctxErr)
		}
		return result.response, result.err
	case <-ctx.Done():
		execution.detach()
		return nil, statusFromContextError(ctx.Err())
	}
}

func (r *serverRuntime) handleStreamingRequest(ctx context.Context, request *wireRequest, requestBody byteStream, lease *requestExecutionLease) frameStream {
	startedAt := time.Now()
	if request == nil {
		r.recordStarted(RPCStarted{}, nil)
		lease.release()
		return r.finishStreamingStatus("", "", 0, startedAt, InvalidArgument("RPC request is nil"), nil)
	}
	service, method, requestBodyLen := request.Service, request.Method, len(request.Body)
	r.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen}, request)

	ctx, cancel, err := r.prepareRequest(ctx, request, lease)
	if err != nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request), request)
	}

	limits := streamLimitsFromOptions(r.options)
	requestBody = limitByteStream(ctx, requestBody, limits, "request", lease, request)
	route, ok := r.routes[methodKey{service: request.Service, method: request.Method}]
	if !ok || route.streaming == nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown streaming RPC method %s/%s", request.Service, request.Method)), request)
	}
	if route.kind != request.rpcKind() {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, InvalidArgument(fmt.Sprintf("streaming RPC kind mismatch for %s/%s: expected %d, got %d", request.Service, request.Method, route.kind, request.rpcKind())), request)
	}

	responseBody, err := invokeStreamingHandler(ctx, route.streaming, request.Body, requestBody, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request), request)
	}
	if responseBody == nil {
		cancel()
		lease.release()
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: service, Method: method, Kind: request.rpcKind(), Err: errors.New("handler returned nil response stream")})
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, internalServerStatus(), request)
	}
	responseBody = closeStreamOnContext(ctx, responseBody)
	return &serverResponseStream{inner: responseBody, runtime: r, request: request, lease: lease, service: service, method: method, requestBodyLen: requestBodyLen, startedAt: startedAt, ctx: ctx, limits: limits, cancel: cancel}
}

type streamingHandlerResult struct {
	stream byteStream
	err    error
}

func invokeStreamingHandler(
	ctx context.Context,
	handler streamingHandler,
	body []byte,
	requestBody byteStream,
	raceContext bool,
	lease *requestExecutionLease,
) (stream byteStream, err error) {
	if !raceContext {
		defer func() {
			if recovered := recover(); recovered != nil {
				stream = nil
				err = &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}
			}
		}()
		return handler(ctx, body, requestBody)
	}
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	result := make(chan streamingHandlerResult)
	abandoned := make(chan struct{})
	execution := lease.retain(ServerDiagnosticHandlerPanic)
	go func() {
		defer execution.finish()
		resultValue := func() (result streamingHandlerResult) {
			defer func() {
				if recovered := recover(); recovered != nil {
					result = streamingHandlerResult{err: &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}}
				}
			}()
			result.stream, result.err = handler(ctx, body, requestBody)
			return result
		}()
		select {
		case result <- resultValue:
		case <-abandoned:
			closeMessageStream(resultValue.stream)
		}
	}()

	select {
	case result := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			closeMessageStream(result.stream)
			return nil, statusFromContextError(ctxErr)
		}
		return result.stream, result.err
	case <-ctx.Done():
		execution.detach()
		close(abandoned)
		return nil, statusFromContextError(ctx.Err())
	}
}

func handlerNeedsDeadlineRace(ctx context.Context) bool {
	// Deadlines must become terminal statuses even if a handler blocks. Cancellation
	// without an RPC deadline is still delivered through ctx, but stays cooperative
	// to avoid a goroutine per no-timeout handler invocation.
	_, ok := ctx.Deadline()
	return ok
}

func validateRequest(request *wireRequest) error {
	if err := validateMetadata(request.Metadata); err != nil {
		return err
	}
	if err := request.validateProtocol(); err != nil {
		return err
	}
	_, _, err := rpcRequestTimeout(request)
	return err
}

func (r *serverRuntime) prepareRequest(ctx context.Context, request *wireRequest, lease *requestExecutionLease) (context.Context, context.CancelFunc, error) {
	if err := validateRequest(request); err != nil {
		return ctx, func() {}, err
	}
	ctx, cancel, err := requestContext(ctx, request)
	if err != nil {
		return ctx, cancel, err
	}
	if r.authorizer != nil {
		if err := invokeAuthorizer(ctx, r.authorizer, newHandlerContext(request), handlerNeedsDeadlineRace(ctx), lease); err != nil {
			cancel()
			return ctx, func() {}, err
		}
	}
	return ctx, cancel, nil
}

func invokeAuthorizer(ctx context.Context, authorizer Authorizer, info HandlerContext, raceContext bool, lease *requestExecutionLease) (err error) {
	invoke := func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = &serverPanicError{phase: ServerDiagnosticAuthorizerPanic, recovered: recovered, stack: debug.Stack()}
			}
		}()
		return authorizer.Authorize(ctx, info)
	}
	if !raceContext {
		return invoke()
	}
	if err := ctx.Err(); err != nil {
		return statusFromContextError(err)
	}
	result := make(chan error, 1)
	execution := lease.retain(ServerDiagnosticAuthorizerPanic)
	go func() { defer execution.finish(); result <- invoke() }()
	select {
	case err := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return statusFromContextError(ctxErr)
		}
		return err
	case <-ctx.Done():
		execution.detach()
		return statusFromContextError(ctx.Err())
	}
}

func rpcRequestTimeout(request *wireRequest) (time.Duration, bool, error) {
	if request.TimeoutNanos == 0 {
		return 0, false, nil
	}
	if request.TimeoutNanos > math.MaxInt64 {
		return 0, false, InvalidArgument("RPC timeout is too large")
	}
	return time.Duration(request.TimeoutNanos), true, nil
}

func requestLifetimeContext(ctx context.Context, request *wireRequest) (context.Context, context.CancelFunc) {
	timeout, ok, err := rpcRequestTimeout(request)
	if err != nil || !ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func requestContext(ctx context.Context, request *wireRequest) (context.Context, context.CancelFunc, error) {
	timeout, ok, err := rpcRequestTimeout(request)
	if err != nil {
		return ctx, func() {}, err
	}
	if !ok {
		ctx, cancel := context.WithCancel(ctx)
		return contextWithHandlerContext(ctx, request), cancel, nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	return contextWithHandlerContext(ctx, request), cancel, nil
}

func (r *serverRuntime) finishResponse(service, method string, requestBodyLen int, startedAt time.Time, response *wireResponse, request *wireRequest) *wireResponse {
	if response == nil {
		response = internalServerStatus().intoResponse(nil)
	}
	r.recordFinished(RPCFinished{Service: service, Method: method, RequestBodyLen: requestBodyLen, ResponseBodyLen: len(response.Body), Code: CodeFromUint32(response.Status), Elapsed: time.Since(startedAt)}, request)
	return response
}

func (r *serverRuntime) finishStreamingStatus(service, method string, requestBodyLen int, startedAt time.Time, status *Status, request *wireRequest) frameStream {
	r.finishStreamingResponse(service, method, requestBodyLen, 0, startedAt, status.Code, request)
	return statusStream(status)
}

func (r *serverRuntime) recordRejectedRequest(startedAt time.Time, request *wireRequest, status *Status) {
	r.recordRequestFailure(startedAt, request, status)
}

func (r *serverRuntime) recordRequestFailure(startedAt time.Time, request *wireRequest, status *Status) {
	service, method, requestBodyLen := requestService(request), requestMethod(request), 0
	if request != nil {
		requestBodyLen = len(request.Body)
	}
	r.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen}, request)
	r.finishStreamingResponse(service, method, requestBodyLen, 0, startedAt, status.Code, request)
}

func (r *serverRuntime) recordPreHandlerFailure(startedAt time.Time, status *Status) {
	r.recordStarted(RPCStarted{}, nil)
	r.finishStreamingResponse("", "", 0, 0, startedAt, status.Code, nil)
}

func (r *serverRuntime) finishStreamingResponse(service, method string, requestBodyLen, responseBodyLen int, startedAt time.Time, code Code, request *wireRequest) {
	r.recordFinished(RPCFinished{Service: service, Method: method, RequestBodyLen: requestBodyLen, ResponseBodyLen: responseBodyLen, Code: code, Elapsed: time.Since(startedAt)}, request)
}

func (r *serverRuntime) recordStarted(event RPCStarted, request *wireRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticMetricsPanic, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Panic: recovered, Stack: debug.Stack()})
		}
	}()
	r.metrics.RPCStarted(event)
}

func (r *serverRuntime) recordFinished(event RPCFinished, request *wireRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticMetricsPanic, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Panic: recovered, Stack: debug.Stack()})
		}
	}()
	r.metrics.RPCFinished(event)
}

type streamLimits struct {
	maxMessages int
	maxBodySize int
	idleTimeout time.Duration
}

func streamLimitsFromOptions(options ServerOptions) streamLimits {
	return streamLimits{maxMessages: options.MaxStreamMessages, maxBodySize: options.MaxStreamBodySize, idleTimeout: options.StreamIdleTimeout}
}

type immediateCancelReadStream interface {
	trevrpcCancelRead()
}

func cancelStreamRead(stream any) {
	if cancellable, ok := stream.(immediateCancelReadStream); ok {
		cancellable.trevrpcCancelRead()
	}
}

type limitedByteStream struct {
	inner          byteStream
	ctx            context.Context
	limits         streamLimits
	direction      string
	lease          *requestExecutionLease
	request        *wireRequest
	messages       int
	bodySize       int
	stopCancelRead func()
	done           bool
}

func limitByteStream(ctx context.Context, inner byteStream, limits streamLimits, direction string, lease *requestExecutionLease, request *wireRequest) byteStream {
	stream := &limitedByteStream{inner: inner, ctx: ctx, limits: limits, direction: direction, lease: lease, request: request}
	if cancellable, ok := inner.(contextCancelReadStream); ok {
		stream.stopCancelRead = cancellable.trevrpcCancelReadOnContext(ctx)
	}

	return stream
}

func (s *limitedByteStream) Recv() ([]byte, error) {
	if s.done {
		return nil, io.EOF
	}

	body, err := recvByteWithTimeout(s.ctx, s.inner, s.limits.idleTimeout, s.direction, s.lease, s.request)
	if err != nil {
		s.finish()
		return nil, err
	}

	if err := checkStreamLimits(s.direction, s.limits, &s.messages, &s.bodySize, len(body)); err != nil {
		s.finish()
		return nil, err
	}

	return body, nil
}

func (s *limitedByteStream) Close() error {
	if !s.done {
		s.finish()
		closeMessageStream(s.inner)
	}

	return nil
}

func (s *limitedByteStream) trevrpcContextCancelsRecv() bool {
	return s.limits.idleTimeout <= 0 && streamContextCancelsRecv(s.inner)
}

func (s *limitedByteStream) finish() {
	s.done = true
	if s.stopCancelRead != nil {
		s.stopCancelRead()
		s.stopCancelRead = nil
	}
}

func recvByteWithTimeout(ctx context.Context, stream byteStream, idleTimeout time.Duration, direction string, lease *requestExecutionLease, request *wireRequest) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	if idleTimeout <= 0 && (isNonBlockingStream(stream) || streamContextCancelsRecv(stream)) {
		body, err := recvByte(stream, direction)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, statusFromContextError(ctxErr)
		}
		return body, err
	}
	type recvResult struct {
		body []byte
		err  error
	}
	phase := ServerDiagnosticRequestStreamPanic
	if direction == "response" {
		phase = ServerDiagnosticResponseStreamPanic
	}
	results := make(chan recvResult, 1)
	execution := lease.retain(phase)
	go func() {
		defer execution.finish()
		body, err := recvByte(stream, direction)
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
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, statusFromContextError(ctxErr)
		}
		return result.body, result.err
	case <-ctx.Done():
		cancelStreamRead(stream)
		execution.detach()
		return nil, statusFromContextError(ctx.Err())
	case <-idle:
		cancelStreamRead(stream)
		execution.detach()
		return nil, Unavailable(fmt.Sprintf("%s stream idle timeout", direction))
	}
}

func recvByte(stream byteStream, direction string) (body []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			phase := ServerDiagnosticRequestStreamPanic
			if direction == "response" {
				phase = ServerDiagnosticResponseStreamPanic
			}
			err = &serverPanicError{phase: phase, recovered: recovered, stack: debug.Stack()}
		}
	}()
	return stream.Recv()
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
	inner           byteStream
	runtime         *serverRuntime
	request         *wireRequest
	lease           *requestExecutionLease
	service         string
	method          string
	requestBodyLen  int
	responseBodyLen int
	startedAt       time.Time
	ctx             context.Context
	limits          streamLimits
	messages        int
	pendingBody     []byte
	hasPendingBody  bool
	pendingStatus   *Status
	done            bool
	cancel          context.CancelFunc
	finishOnce      sync.Once
}

type responseStreamItem struct {
	body   []byte
	status *Status
}

func (s *serverResponseStream) Recv() (*wireStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	body, err := recvByteWithTimeout(s.ctx, s.inner, s.limits.idleTimeout, "response", s.lease, s.request)
	if err == io.EOF {
		status := s.terminalResponseStatus()
		s.finish(status.Code)
		return statusFrame(status), nil
	}

	if err != nil {
		status := s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request)
		s.finish(status.Code)
		return statusFrame(status), nil
	}

	if err := checkStreamLimits("response", s.limits, &s.messages, &s.responseBodyLen, len(body)); err != nil {
		status := s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request)
		s.finish(status.Code)
		return statusFrame(status), nil
	}

	return messageFrame(body), nil
}

func (s *serverResponseStream) trevrpcWriteNextFrame(ctx context.Context, writer io.Writer, maxFrameSize int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if s.done {
		return true, nil
	}
	if s.pendingStatus != nil {
		return s.writeResponseStatusFrame(writer, maxFrameSize, s.pendingStatus)
	}

	item := s.readNextResponseItem()
	if item.status != nil {
		return s.writeResponseStatusFrame(writer, maxFrameSize, item.status)
	}
	if status := s.acceptResponseBody(item.body); status != nil {
		return s.writeResponseStatusFrame(writer, maxFrameSize, status)
	}

	return false, writeMessageStreamFrame(writer, item.body, maxFrameSize)
}

func (s *serverResponseStream) trevrpcWriteNextFrames(ctx context.Context, writer io.Writer, maxFrameSize int) (bool, error) {
	if !s.trevrpcNonBlockingStream() {
		return s.trevrpcWriteNextFrame(ctx, writer, maxFrameSize)
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if s.done {
		return true, nil
	}
	if s.pendingStatus != nil {
		return s.writeResponseStatusFrame(writer, maxFrameSize, s.pendingStatus)
	}

	var batch [maxMessageFrameBatch][]byte
	batchBytes := 0
	batchByteLimit := responseFrameBatchByteLimit(maxFrameSize)
	count := 0
	for count < len(batch) {
		if count > 0 {
			if err := ctx.Err(); err != nil {
				break
			}
		}

		item := s.readNextResponseItem()
		if item.status != nil {
			if count == 0 {
				return s.writeResponseStatusFrame(writer, maxFrameSize, item.status)
			}
			s.pendingStatus = item.status
			break
		}

		encodedLen := encodedMessageStreamFrameLen(item.body)
		if count > 0 && batchByteLimit > 0 && saturatingAdd(batchBytes, encodedLen) > batchByteLimit {
			s.pendingBody = item.body
			s.hasPendingBody = true
			break
		}

		if status := s.acceptResponseBody(item.body); status != nil {
			if count == 0 {
				return s.writeResponseStatusFrame(writer, maxFrameSize, status)
			}
			s.pendingStatus = status
			break
		}

		batch[count] = item.body
		count++
		batchBytes = saturatingAdd(batchBytes, encodedLen)
		if responseBatchReachedStreamLimit(s.limits, s.messages, s.responseBodyLen) || (batchByteLimit > 0 && batchBytes >= batchByteLimit) {
			break
		}
	}

	if count == 0 {
		return false, nil
	}

	err := writeMessageStreamFrames(writer, batch[:count], maxFrameSize)
	clear(batch[:count])
	return false, err
}

func (s *serverResponseStream) trevrpcNonBlockingStream() bool {
	return s.limits.idleTimeout <= 0 && isNonBlockingStream(s.inner)
}

func (s *serverResponseStream) trevrpcContextCancelsRecv() bool {
	return s.limits.idleTimeout <= 0 && streamContextCancelsRecv(s.inner)
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
		s.runtime.recordFinished(RPCFinished{Service: s.service, Method: s.method, RequestBodyLen: s.requestBodyLen, ResponseBodyLen: s.responseBodyLen, Code: code, Elapsed: time.Since(s.startedAt)}, s.request)
		s.lease.release()
	})
}

func (s *serverResponseStream) readNextResponseItem() responseStreamItem {
	if s.hasPendingBody {
		body := s.pendingBody
		s.pendingBody = nil
		s.hasPendingBody = false
		return responseStreamItem{body: body}
	}

	body, err := recvByteWithTimeout(s.ctx, s.inner, s.limits.idleTimeout, "response", s.lease, s.request)
	if err == io.EOF {
		return responseStreamItem{status: s.terminalResponseStatus()}
	}
	if err != nil {
		return responseStreamItem{status: s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request)}
	}

	return responseStreamItem{body: body}
}

func (s *serverResponseStream) acceptResponseBody(body []byte) *Status {
	if err := checkStreamLimits("response", s.limits, &s.messages, &s.responseBodyLen, len(body)); err != nil {
		return StatusFromError(err)
	}

	return nil
}

func (s *serverResponseStream) terminalResponseStatus() *Status {
	terminal, aware := s.inner.(terminalStatusByteStream)
	if !aware {
		return OK()
	}

	status, present := terminal.trevrpcTerminalStatus()
	if !present || status == nil {
		return s.runtime.wireStatus(Internal("response stream reached EOF without a terminal status"), ServerDiagnosticInternalError, s.request)
	}
	return s.runtime.wireStatus(status, ServerDiagnosticInternalError, s.request)
}

func (s *serverResponseStream) writeResponseStatusFrame(writer io.Writer, maxFrameSize int, status *Status) (bool, error) {
	s.pendingStatus = nil
	s.finish(status.Code)
	return true, writeFrame(writer, statusFrame(status), maxFrameSize)
}

func responseFrameBatchByteLimit(maxFrameSize int) int {
	if maxFrameSize <= 0 {
		return 0
	}

	return maxFrameSize
}

func encodedMessageStreamFrameLen(body []byte) int {
	return saturatingAdd(4, messageStreamFrameBodyLen(body))
}

func responseBatchReachedStreamLimit(limits streamLimits, messages, bodySize int) bool {
	if limits.maxMessages >= 0 && messages >= limits.maxMessages {
		return true
	}
	if limits.maxBodySize >= 0 && bodySize >= limits.maxBodySize {
		return true
	}

	return false
}

func saturatingAdd(left, right int) int {
	if right > 0 && left > math.MaxInt-right {
		return math.MaxInt
	}

	return left + right
}
