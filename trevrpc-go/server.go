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

	"github.com/quic-go/quic-go/quicvarint"
)

// unaryReply is a successful unary handler result: the encoded response
// message with its response headers and trailing metadata.
type unaryReply struct {
	message  encodedMessage
	headers  Metadata
	trailers Metadata
}

// unaryHandler handles an encoded unary request message.
type unaryHandler func(context.Context, []byte) (*unaryReply, error)

// streamResponse is what a streaming handler produces: the encoded response
// messages, the headers to send before the first one, and, for a
// ResponseStream, the source of the terminal status.
type streamResponse struct {
	stream   encodedStream
	headers  Metadata
	terminal func() (*Status, bool)
}

// newStreamResponse prepares a handler's response stream for the server. It
// closes stream and fails when the stream's response headers are invalid.
func newStreamResponse[T ProtoMessage](stream MessageStream[T]) (streamResponse, error) {
	headers := cloneMetadata(responseHeadersOf(stream))
	if err := validateMetadata(headers); err != nil {
		discardMessageStream(stream)
		return streamResponse{}, Internal("handler returned invalid response headers: " + err.Error())
	}
	response := streamResponse{stream: newEncodeStream(stream), headers: headers}
	if terminal, ok := stream.(ResponseStream[T]); ok {
		response.terminal = terminal.TerminalStatus
	}
	return response, nil
}

// streamingHandler handles a streaming request. body is the request message of
// a server-streaming call and nil otherwise; requests carries the message
// stream of a client-streaming or bidirectional call.
type streamingHandler func(context.Context, []byte, byteStream) (streamResponse, error)

type route struct {
	kind      RPCKind
	unary     unaryHandler
	streaming streamingHandler
}

// ErrServerFrozen is the panic value used when a server is mutated after first use.
var ErrServerFrozen = errors.New("trevrpc: server configuration is frozen")

// Server routes and handles TrevRPC requests.
type Server struct {
	mu          sync.Mutex
	routes      map[string]route
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

// Server diagnostic phases delivered to a [ServerDiagnosticHandler].
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
	Kind                   RPCKind
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
		routes:  map[string]route{},
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

// routeUnary registers a unary route handler for a method path.
func (s *Server) routeUnary(method string, handler unaryHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	validateRoute(method, handler != nil)
	s.routes[method] = route{kind: RPCKindUnary, unary: handler}
}

// routeStreaming registers a streaming route handler for a method path and RPC kind.
func (s *Server) routeStreaming(method string, kind RPCKind, handler streamingHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	if kind == RPCKindUnary || !kind.isValid() {
		panic("trevrpc: streaming route requires a streaming RPC kind")
	}
	validateRoute(method, handler != nil)
	s.routes[method] = route{kind: kind, streaming: handler}
}

// RegisterUnary registers a unary handler for method, the full method path
// "/package.Service/Method". It panics if method is not of that form. The
// handler may attach response headers and trailing metadata through the
// returned Response.
func RegisterUnary[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, Req) (*Response[Res], error)) {
	s.routeUnary(method, func(ctx context.Context, body []byte) (*unaryReply, error) {
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

// RegisterClientStreaming registers a client-streaming handler for method, the
// full method path "/package.Service/Method". It panics if method is not of
// that form. The handler may attach response headers and trailing metadata
// through the returned Response.
func RegisterClientStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, MessageStream[Req]) (*Response[Res], error)) {
	s.routeStreaming(method, RPCKindClientStreaming, func(ctx context.Context, _ []byte, requests byteStream) (streamResponse, error) {
		response, err := handler(ctx, newDecodeStream(requests, newRequest))
		if err != nil {
			return streamResponse{}, err
		}
		if err := validateRegisteredResponse(response); err != nil {
			return streamResponse{}, err
		}
		return newStreamResponse(WithResponseHeaders(NewResponseStream(newSliceStream(response.Message), response.Metadata), response.Headers))
	})
}

// RegisterServerStreaming registers a server-streaming handler for method, the
// full method path "/package.Service/Method". It panics if method is not of
// that form. If the returned stream implements ResponseStream (see NewResponseStream), its
// trailing metadata is sent with the final status; WithResponseHeaders adds
// response headers.
func RegisterServerStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, Req) (MessageStream[Res], error)) {
	s.routeStreaming(method, RPCKindServerStreaming, func(ctx context.Context, body []byte, _ byteStream) (streamResponse, error) {
		request, err := decodeRegisteredRequest(body, newRequest)
		if err != nil {
			return streamResponse{}, err
		}

		responses, err := handler(ctx, request)
		if err != nil {
			return streamResponse{}, err
		}
		if isNilRegisteredValue(responses) {
			return streamResponse{}, Internal("handler returned nil response stream")
		}
		return newStreamResponse(responses)
	})
}

// RegisterBidirectionalStreaming registers a bidirectional-streaming handler
// for method, the full method path "/package.Service/Method". It panics if
// method is not of that form. If the returned stream implements ResponseStream (see NewResponseStream), its
// trailing metadata is sent with the final status; WithResponseHeaders adds
// response headers.
func RegisterBidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, MessageStream[Req]) (MessageStream[Res], error)) {
	s.routeStreaming(method, RPCKindBidirectionalStreaming, func(ctx context.Context, _ []byte, requests byteStream) (streamResponse, error) {
		responses, err := handler(ctx, newDecodeStream(requests, newRequest))
		if err != nil {
			return streamResponse{}, err
		}
		if isNilRegisteredValue(responses) {
			return streamResponse{}, Internal("handler returned nil response stream")
		}
		return newStreamResponse(responses)
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

func encodeRegisteredResponse[Res ProtoMessage](response *Response[Res]) (*unaryReply, error) {
	if err := validateRegisteredResponse(response); err != nil {
		return nil, err
	}
	message, err := encodeMessage(response.Message)
	if err != nil {
		return nil, err
	}
	return &unaryReply{message: message, headers: cloneMetadata(response.Headers), trailers: cloneMetadata(response.Metadata)}, nil
}

func validateRegisteredResponse[Res ProtoMessage](response *Response[Res]) error {
	if response == nil {
		return Internal("handler returned nil response")
	}
	if isNilRegisteredValue(response.Message) {
		return Internal("handler returned nil response message")
	}
	if err := validateMetadata(response.Headers); err != nil {
		return Internal("handler returned invalid response headers: " + err.Error())
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
	routes           map[string]route
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
	if uint64(canonical.MaxFrameSize) > quicvarint.Max {
		panic("trevrpc: MaxFrameSize exceeds the frame length limit")
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

func validateRoute(method string, hasHandler bool) {
	if _, _, ok := splitMethodPath(method); !ok {
		panic(fmt.Sprintf("trevrpc: route method %q must be /<service>/<method>", method))
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
	kind    RPCKind
	refs    atomic.Int64
}

func (r *serverRuntime) tryRequestLease(request *wireRequest) (*requestExecutionLease, bool) {
	if !tryAcquire(r.requestLimit) {
		return nil, false
	}
	lease := &requestExecutionLease{runtime: r, limit: r.requestLimit}
	lease.refs.Store(1)
	if request != nil {
		lease.service, lease.method = request.serviceAndMethod()
		lease.kind = request.rpcKind()
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
	if panicErr, ok := errors.AsType[*serverPanicError](err); ok {
		r.emitDiagnostic(ServerDiagnostic{Phase: panicErr.phase, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err, Panic: panicErr.recovered, Stack: panicErr.stack})
		return internalServerStatus()
	}
	if status, ok := errors.AsType[*Status](err); ok && status.Code != CodeInternal {
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
	service, _ := request.serviceAndMethod()
	return service
}
func requestMethod(request *wireRequest) string {
	if request == nil {
		return ""
	}
	_, method := request.serviceAndMethod()
	return method
}
func requestKind(request *wireRequest) RPCKind {
	if request == nil {
		return RPCKindUnary
	}
	return request.rpcKind()
}

// responseWriter writes one RPC's response to its stream.
type responseWriter interface {
	// writeNext writes the next frames of the response. It reports done once
	// the terminal STATUS was written or the response cannot continue.
	// beforeStatus, if not nil, is called once just before the write that
	// carries the terminal STATUS, so the caller can free what a client may
	// reuse as soon as it sees the call complete.
	writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (done bool, err error)
	// Close releases the response without writing the rest of it.
	Close() error
}

// statusResponse is a response that is a lone STATUS frame, the response to
// every failure before a handler produced a response.
type statusResponse struct {
	status *Status
}

func (r statusResponse) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	frames, err := appendStatusFrame(nil, r.status, maxFrameSize)
	if err != nil {
		return true, err
	}
	callBeforeStatus(beforeStatus)
	_, err = writer.Write(frames)
	return true, err
}

func (statusResponse) Close() error { return nil }

// callBeforeStatus runs the beforeStatus hook of writeNext, if there is one.
func callBeforeStatus(beforeStatus func()) {
	if beforeStatus != nil {
		beforeStatus()
	}
}

// unaryResponse is the complete response of a unary call: optional HEADERS,
// then a MESSAGE and an OK STATUS on success, or a lone failure STATUS.
type unaryResponse struct {
	headers    Metadata
	message    encodedMessage
	hasMessage bool
	status     *Status
}

func (r *unaryResponse) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	var frames []byte
	var err error
	if len(r.headers) == 0 && r.hasMessage {
		// The message frame has room for the STATUS behind it.
		if r.message.size > maxFrameSize {
			return true, &FrameTooLargeError{Len: r.message.size, Max: maxFrameSize}
		}
		frames = r.message.frame
	} else {
		frames = make([]byte, 0, len(r.message.frame)+frameTailSlack)
		if len(r.headers) > 0 {
			if frames, err = appendHeadersFrame(frames, r.headers, maxFrameSize); err != nil {
				return true, err
			}
		}
		if r.hasMessage {
			if frames, err = appendEncodedMessage(frames, r.message, maxFrameSize); err != nil {
				return true, err
			}
		}
	}
	if frames, err = appendStatusFrame(frames, r.status, maxFrameSize); err != nil {
		return true, err
	}
	callBeforeStatus(beforeStatus)
	_, err = writer.Write(frames)
	return true, err
}

func (*unaryResponse) Close() error { return nil }

func (r *serverRuntime) handleRequest(ctx context.Context, request *wireRequest, body []byte, lease *requestExecutionLease) *unaryResponse {
	startedAt := time.Now()
	if request == nil {
		r.recordStarted(RPCStarted{}, nil)
		return r.finishResponse("", "", 0, startedAt, failedUnaryResponse(InvalidArgument("RPC request is nil")), nil)
	}
	service, method := request.serviceAndMethod()
	requestBodyLen := len(body)
	r.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen}, request)

	ctx, cancel, err := r.prepareRequest(ctx, request, lease)
	if err != nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, failedUnaryResponse(r.wireStatus(err, ServerDiagnosticInternalError, request)), request)
	}
	defer cancel()

	route, ok := r.routes[request.Method]
	if !ok || route.unary == nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, failedUnaryResponse(Unimplemented(fmt.Sprintf("unknown RPC method %s", request.Method))), request)
	}
	reply, err := invokeUnaryHandler(ctx, route.unary, body, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, failedUnaryResponse(r.wireStatus(err, ServerDiagnosticInternalError, request)), request)
	}
	return r.finishResponse(service, method, requestBodyLen, startedAt, r.unaryResponseFor(reply, request), request)
}

func failedUnaryResponse(status *Status) *unaryResponse {
	return &unaryResponse{status: status}
}

// unaryResponseFor turns a handler's reply into the response to send.
func (r *serverRuntime) unaryResponseFor(reply *unaryReply, request *wireRequest) *unaryResponse {
	if reply == nil {
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: errors.New("handler returned nil response")})
		return failedUnaryResponse(internalServerStatus())
	}
	for _, metadata := range []Metadata{reply.headers, reply.trailers} {
		if err := validateMetadata(metadata); err != nil {
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err})
			return failedUnaryResponse(internalServerStatus())
		}
	}
	return &unaryResponse{
		headers:    reply.headers,
		message:    reply.message,
		hasMessage: true,
		status:     OK().WithMetadata(reply.trailers),
	}
}

func invokeUnaryHandler(
	ctx context.Context,
	handler unaryHandler,
	body []byte,
	raceContext bool,
	lease *requestExecutionLease,
) (reply *unaryReply, err error) {
	if !raceContext {
		defer func() {
			if recovered := recover(); recovered != nil {
				reply = nil
				err = &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}
			}
		}()
		return handler(ctx, body)
	}
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	type unaryResult struct {
		reply *unaryReply
		err   error
	}
	result := make(chan unaryResult, 1)
	execution := lease.retain(ServerDiagnosticHandlerPanic)
	go func() {
		defer execution.finish()
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- unaryResult{err: &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}}
			}
		}()
		reply, err := handler(ctx, body)
		result <- unaryResult{reply: reply, err: err}
	}()

	select {
	case result := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, statusFromContextError(ctxErr)
		}
		return result.reply, result.err
	case <-ctx.Done():
		execution.detach()
		return nil, statusFromContextError(ctx.Err())
	}
}

// handleStreamingRequest dispatches a streaming request. body is the request
// message of a server-streaming call.
func (r *serverRuntime) handleStreamingRequest(ctx context.Context, request *wireRequest, body []byte, requestBody byteStream, lease *requestExecutionLease) responseWriter {
	startedAt := time.Now()
	if request == nil {
		r.recordStarted(RPCStarted{}, nil)
		lease.release()
		return r.finishStreamingStatus("", "", 0, startedAt, InvalidArgument("RPC request is nil"), nil)
	}
	service, method := request.serviceAndMethod()
	requestBodyLen := len(body)
	r.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: requestBodyLen}, request)

	ctx, cancel, err := r.prepareRequest(ctx, request, lease)
	if err != nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request), request)
	}

	limits := streamLimitsFromOptions(r.options)
	requestBody = limitByteStream(ctx, requestBody, limits, "request", lease, request)
	route, ok := r.routes[request.Method]
	if !ok || route.streaming == nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, Unimplemented(fmt.Sprintf("unknown streaming RPC method %s", request.Method)), request)
	}
	if route.kind != request.rpcKind() {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, InvalidArgument(fmt.Sprintf("streaming RPC kind mismatch for %s: expected %d, got %d", request.Method, route.kind, request.rpcKind())), request)
	}

	response, err := invokeStreamingHandler(ctx, route.streaming, body, requestBody, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request), request)
	}
	if response.stream == nil {
		cancel()
		lease.release()
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: service, Method: method, Kind: request.rpcKind(), Err: errors.New("handler returned nil response stream")})
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, internalServerStatus(), request)
	}
	if err := validateMetadata(response.headers); err != nil {
		discardMessageStream(response.stream)
		cancel()
		lease.release()
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: service, Method: method, Kind: request.rpcKind(), Err: err})
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, internalServerStatus(), request)
	}
	response.stream = closeStreamOnContext(ctx, response.stream)
	return &serverResponseStream{response: response, runtime: r, request: request, lease: lease, service: service, method: method, requestBodyLen: requestBodyLen, startedAt: startedAt, ctx: ctx, limits: limits, cancel: cancel}
}

type streamingHandlerResult struct {
	response streamResponse
	err      error
}

func invokeStreamingHandler(
	ctx context.Context,
	handler streamingHandler,
	body []byte,
	requestBody byteStream,
	raceContext bool,
	lease *requestExecutionLease,
) (response streamResponse, err error) {
	if !raceContext {
		defer func() {
			if recovered := recover(); recovered != nil {
				response = streamResponse{}
				err = &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}
			}
		}()
		return handler(ctx, body, requestBody)
	}
	if err := ctx.Err(); err != nil {
		return streamResponse{}, statusFromContextError(err)
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
			result.response, result.err = handler(ctx, body, requestBody)
			return result
		}()
		select {
		case result <- resultValue:
		case <-abandoned:
			discardMessageStream(resultValue.response.stream)
		}
	}()

	select {
	case result := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			discardMessageStream(result.response.stream)
			return streamResponse{}, statusFromContextError(ctxErr)
		}
		return result.response, result.err
	case <-ctx.Done():
		execution.detach()
		close(abandoned)
		return streamResponse{}, statusFromContextError(ctx.Err())
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

func (r *serverRuntime) finishResponse(service, method string, requestBodyLen int, startedAt time.Time, response *unaryResponse, request *wireRequest) *unaryResponse {
	r.recordFinished(RPCFinished{Service: service, Method: method, RequestBodyLen: requestBodyLen, ResponseBodyLen: response.message.size, Code: response.status.Code, Elapsed: time.Since(startedAt)}, request)
	return response
}

func (r *serverRuntime) finishStreamingStatus(service, method string, requestBodyLen int, startedAt time.Time, status *Status, request *wireRequest) responseWriter {
	r.finishStreamingResponse(service, method, requestBodyLen, 0, startedAt, status.Code, request)
	return statusResponse{status: status}
}

func (r *serverRuntime) recordRejectedRequest(startedAt time.Time, request *wireRequest, requestBodyLen int, status *Status) {
	r.recordRequestFailure(startedAt, request, requestBodyLen, status)
}

func (r *serverRuntime) recordRequestFailure(startedAt time.Time, request *wireRequest, requestBodyLen int, status *Status) {
	r.recordStarted(RPCStarted{Service: requestService(request), Method: requestMethod(request), RequestBodyLen: requestBodyLen}, request)
	r.finishStreamingResponse(requestService(request), requestMethod(request), requestBodyLen, 0, startedAt, status.Code, request)
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
	inner     byteStream
	ctx       context.Context
	limits    streamLimits
	direction string
	lease     *requestExecutionLease
	request   *wireRequest
	messages  int
	bodySize  int
	// mu guards done and stopCancelRead: Close may run on another goroutine
	// than Recv, such as the one that closes the stream when ctx ends.
	mu             sync.Mutex
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
	if s.isDone() {
		return nil, io.EOF
	}

	body, err := recvByteWithTimeout(s.ctx, s.inner, s.limits.idleTimeout, s.direction, s.lease)
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
	if s.finish() {
		discardMessageStream(s.inner)
	}

	return nil
}

func (s *limitedByteStream) trevrpcContextCancelsRecv() bool {
	return s.limits.idleTimeout <= 0 && streamContextCancelsRecv(s.inner)
}

func (s *limitedByteStream) isDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// finish ends the stream and reports whether this call did.
func (s *limitedByteStream) finish() bool {
	s.mu.Lock()
	first := !s.done
	s.done = true
	stop := s.stopCancelRead
	s.stopCancelRead = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	return first
}

func recvByteWithTimeout[T any](ctx context.Context, stream MessageStream[T], idleTimeout time.Duration, direction string, lease *requestExecutionLease) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, statusFromContextError(err)
	}
	if idleTimeout <= 0 && (isNonBlockingStream(stream) || streamContextCancelsRecv(stream)) {
		body, err := recvByte(stream, direction)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, statusFromContextError(ctxErr)
		}
		return body, err
	}
	type recvResult struct {
		body T
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
			return zero, statusFromContextError(ctxErr)
		}
		return result.body, result.err
	case <-ctx.Done():
		cancelStreamRead(stream)
		execution.detach()
		return zero, statusFromContextError(ctx.Err())
	case <-idle:
		cancelStreamRead(stream)
		execution.detach()
		return zero, Unavailable(fmt.Sprintf("%s stream idle timeout", direction))
	}
}

func recvByte[T any](stream MessageStream[T], direction string) (body T, err error) {
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

// serverResponseStream writes the response of a streaming call: optional
// HEADERS before the first message, MESSAGE frames pulled from the handler's
// stream, and a terminal STATUS.
type serverResponseStream struct {
	response        streamResponse
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
	headersSent     bool
	pending         encodedMessage
	hasPending      bool
	done            bool
	cancel          context.CancelFunc
	finishOnce      sync.Once
}

type responseStreamItem struct {
	message encodedMessage
	status  *Status
}

// writeNext writes the next frames of the response. HEADERS go out as soon as
// the handler has returned, in a write of their own when pulling the first
// message may block. A stream that never blocks has its messages written in
// batches, and the terminal STATUS joins the last batch.
func (s *serverResponseStream) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if s.done {
		return true, nil
	}

	var frames []byte
	var err error
	if !s.headersSent {
		s.headersSent = true
		if len(s.response.headers) > 0 {
			if frames, err = appendHeadersFrame(frames, s.response.headers, maxFrameSize); err != nil {
				return s.writeFinalFrames(writer, maxFrameSize, nil, s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request), beforeStatus)
			}
			if !s.trevrpcNonBlockingStream() {
				if _, err := writer.Write(frames); err != nil {
					return true, err
				}
				frames = nil
			}
		}
	}

	batchSize := 1
	if s.trevrpcNonBlockingStream() {
		batchSize = maxMessageFrameBatch
	}
	batchByteLimit := responseFrameBatchByteLimit(maxFrameSize)
	for count := 0; count < batchSize; count++ {
		if count > 0 {
			if err := ctx.Err(); err != nil {
				break
			}
		}

		item := s.readNextResponseItem()
		if item.status == nil {
			if count > 0 && batchByteLimit > 0 && saturatingAdd(len(frames), len(item.message.frame)) > batchByteLimit {
				s.pending, s.hasPending = item.message, true
				break
			}
			item.status = s.acceptResponseMessage(item.message)
		}
		if item.status != nil {
			return s.writeFinalFrames(writer, maxFrameSize, frames, item.status, beforeStatus)
		}

		if frames, err = appendEncodedMessage(frames, item.message, maxFrameSize); err != nil {
			return true, err
		}
	}

	if len(frames) == 0 {
		return false, nil
	}
	_, err = writer.Write(frames)
	return false, err
}

// writeFinalFrames writes frames followed by the terminal STATUS and finishes
// the response.
func (s *serverResponseStream) writeFinalFrames(writer io.Writer, maxFrameSize int, frames []byte, status *Status, beforeStatus func()) (bool, error) {
	s.finish(status.Code)
	frames, err := appendStatusFrame(frames, status, maxFrameSize)
	if err != nil {
		return true, err
	}
	callBeforeStatus(beforeStatus)
	_, err = writer.Write(frames)
	return true, err
}

func (s *serverResponseStream) trevrpcNonBlockingStream() bool {
	return s.limits.idleTimeout <= 0 && isNonBlockingStream(s.response.stream)
}

func (s *serverResponseStream) trevrpcContextCancelsRecv() bool {
	return s.limits.idleTimeout <= 0 && streamContextCancelsRecv(s.response.stream)
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
		discardMessageStream(s.response.stream)
		s.runtime.recordFinished(RPCFinished{Service: s.service, Method: s.method, RequestBodyLen: s.requestBodyLen, ResponseBodyLen: s.responseBodyLen, Code: code, Elapsed: time.Since(s.startedAt)}, s.request)
		s.lease.release()
	})
}

func (s *serverResponseStream) readNextResponseItem() responseStreamItem {
	if s.hasPending {
		message := s.pending
		s.pending = encodedMessage{}
		s.hasPending = false
		return responseStreamItem{message: message}
	}

	message, err := recvByteWithTimeout(s.ctx, s.response.stream, s.limits.idleTimeout, "response", s.lease)
	if errors.Is(err, io.EOF) {
		return responseStreamItem{status: s.terminalResponseStatus()}
	}
	if err != nil {
		return responseStreamItem{status: s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request)}
	}

	return responseStreamItem{message: message}
}

func (s *serverResponseStream) acceptResponseMessage(message encodedMessage) *Status {
	if err := checkStreamLimits("response", s.limits, &s.messages, &s.responseBodyLen, message.size); err != nil {
		return StatusFromError(err)
	}

	return nil
}

func (s *serverResponseStream) terminalResponseStatus() *Status {
	if s.response.terminal == nil {
		return OK()
	}

	status, present := s.response.terminal()
	if !present || status == nil {
		return s.runtime.wireStatus(Internal("response stream reached EOF without a terminal status"), ServerDiagnosticInternalError, s.request)
	}
	return s.runtime.wireStatus(status, ServerDiagnosticInternalError, s.request)
}

func responseFrameBatchByteLimit(maxFrameSize int) int {
	if maxFrameSize <= 0 {
		return 0
	}

	return maxFrameSize
}

func saturatingAdd(left, right int) int {
	if right > 0 && left > math.MaxInt-right {
		return math.MaxInt
	}

	return left + right
}
