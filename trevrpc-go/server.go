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

// unaryHandler handles an encoded unary request message and returns the
// encoded response message. Response headers and trailing metadata are set
// through the context (see SetResponseHeader).
type unaryHandler func(context.Context, []byte) (encodedMessage, error)

// streamingHandler handles a streaming request and returns the encoded
// response messages. body is the request message of a server-streaming call
// and nil otherwise; requests carries the message stream of a client-streaming
// or bidirectional call. Response headers and trailing metadata are set
// through the context (see SetResponseHeader).
type streamingHandler func(context.Context, []byte, byteStream) (encodedStream, error)

type route struct {
	kind      RPCKind
	unary     unaryHandler
	streaming streamingHandler
	// unreliable says which directions send their messages as datagrams on
	// calls that have a flow.
	unreliable Unreliable
	// streamCompression says which directions may be stream-compressed.
	streamCompression StreamCompression
	// plainCompression says which directions may be compressed without a
	// dictionary.
	plainCompression PlainCompression
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
	// frozen is runtime, once set, for lock-free reads on the request path.
	frozen atomic.Pointer[serverRuntime]
	// compression is options.Compression digested by SetOptions, and
	// compressionErr why it could not be; Listen reports that error.
	compression    *compressionConfig
	compressionErr error
	// streams is the stream compression budget SetOptions made, nil when it
	// is off.
	streams *streamBudget
}

// ServerOptions configures server limits, timeouts, and the HTTP and browser
// session transports.
//
// EnableHTTP serves the HTTP mapping (POST to HTTPPath) over HTTP/3 and, on
// the TCP listener, HTTP/2. EnableWebTransport serves browser sessions at
// HTTPPath: WebTransport over HTTP/3 and, on the TCP listener, QMux over
// WebSocket. Native QUIC and QMux over TLS are always served.
//
// HTTPFallback serves every other HTTP request on the same sockets, over
// HTTP/3, HTTP/2 and HTTP/1.1, such as the static content of a web
// application. TrevRPC claims HTTPPath while EnableHTTP or EnableWebTransport
// is set; every other path goes to HTTPFallback. Setting HTTPFallback serves
// these HTTP versions even when neither is set. A fallback request takes a
// stream slot of its connection like an RPC, so a connection at
// MaxConcurrentStreamsPerConnection answers it with 503, but admission does
// not apply to it. Its context is cancelled when the listener shuts down.
//
// A stream has no message-count, cumulative-body or idle limit by default, so
// long-lived and quiet streams work: MaxStreamMessages and MaxStreamBodySize
// bound a stream when positive (0 or -1 for none), and StreamIdleTimeout ends a
// stream whose request or response side is quiet for longer when positive.
// ConnectionIdleTimeout (default 30 s) closes a connection the peer stopped
// answering and sends keepalives at half of it, so quiet streams on a live
// connection are not affected; DisableConnectionIdleTimeout turns it off.
//
// Compression lets messages travel compressed with the dictionaries it
// holds; see Compression. Listen fails when it is invalid.
//
// StreamCompressionMemory bounds the memory, in bytes, of the zstd contexts
// that stream compression holds for the server's calls (see
// StreamCompression): about 350 KiB for a direction the server decodes and
// 585 KiB for one it compresses. Calls beyond it are compressed per message.
// Zero means 256 MiB, and a negative value turns stream compression off.
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
	ConnectionIdleTimeout             time.Duration
	DisableConnectionIdleTimeout      bool
	EnableHTTP                        bool
	HTTPPath                          string
	HTTPAdmission                     Admission
	EnableWebTransport                bool
	WebTransportAdmission             Admission
	HTTPFallback                      http.Handler
	Compression                       *Compression
	StreamCompressionMemory           int64
}

// servesHTTP reports whether the listener serves HTTP: HTTP/3 on the UDP
// socket and HTTP/2 or HTTP/1.1 on the TCP socket.
func (o ServerOptions) servesHTTP() bool {
	return o.EnableHTTP || o.EnableWebTransport || o.HTTPFallback != nil
}

// servesHTTP2 reports whether the TCP listener serves HTTP/2: RPCs, or
// HTTPFallback.
func (o ServerOptions) servesHTTP2() bool { return o.EnableHTTP || o.HTTPFallback != nil }

// servesHTTP1 reports whether the TCP listener serves HTTP/1.1: WebSocket
// sessions, or HTTPFallback.
func (o ServerOptions) servesHTTP1() bool { return o.EnableWebTransport || o.HTTPFallback != nil }

// claimsPath reports whether TrevRPC, rather than HTTPFallback, serves HTTP
// requests for path.
func (o ServerOptions) claimsPath(path string) bool {
	return (o.EnableHTTP || o.EnableWebTransport) && path == o.HTTPPath
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
		MaxStreamMessages:                 -1,
		MaxStreamBodySize:                 -1,
		ConnectionIdleTimeout:             30 * time.Second,
		HTTPPath:                          DefaultHTTPPath,
	}
}

// Authorizer authorizes a request after metadata and protocol validation. The
// request body is not available to it. A call the server refuses for
// optimistic stream compression is replayed by the client and authorized
// again (see StreamCompression), so authorizers must not have side effects
// that a second authorization of the same call would repeat.
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
		streams: newStreamBudget(0, defaultServerStreamCompressionMemory, nil),
	}
}

// SetOptions canonicalizes and replaces the server options. It panics after first use.
func (s *Server) SetOptions(options ServerOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.options = canonicalServerOptions(options)
	s.compression, s.compressionErr = newCompressionConfig(options.Compression)
	s.streams = newStreamBudget(options.StreamCompressionMemory, defaultServerStreamCompressionMemory, s.compression)
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
func (s *Server) routeUnary(method string, handler unaryHandler, options ...RouteOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	validateRoute(method, handler != nil)
	config := applyRouteOptions(options)
	s.routes[method] = route{kind: RPCKindUnary, unary: handler, plainCompression: config.plainCompression}
}

// routeStreaming registers a streaming route handler for a method path and RPC kind.
func (s *Server) routeStreaming(method string, kind RPCKind, handler streamingHandler, options ...RouteOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	if kind == RPCKindUnary || !kind.isValid() {
		panic("trevrpc: streaming route requires a streaming RPC kind")
	}
	validateRoute(method, handler != nil)
	config := applyRouteOptions(options)
	s.routes[method] = route{
		kind:              kind,
		streaming:         handler,
		unreliable:        config.unreliable.effective(kind),
		streamCompression: config.streamCompression.effective(kind),
		plainCompression:  config.plainCompression,
	}
}

// RegisterUnary registers a unary handler for method, the full method path
// "/package.Service/Method". It panics if method is not of that form. The
// handler may set response headers and trailing metadata with
// SetResponseHeader and SetResponseMetadata. Generated registration passes the
// method's schema options as RouteOptions; RoutePlainCompression is the only
// one that applies to a unary route.
func RegisterUnary[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, Req) (Res, error), options ...RouteOption) {
	s.routeUnary(method, func(ctx context.Context, body []byte) (encodedMessage, error) {
		request, err := decodeRegisteredRequest(body, newRequest)
		if err != nil {
			return encodedMessage{}, err
		}

		response, err := handler(ctx, request)
		if err != nil {
			return encodedMessage{}, err
		}
		if isNilRegisteredValue(response) {
			return encodedMessage{}, Internal("handler returned nil response message")
		}
		return responseCompressorFromContext(ctx).encode(response)
	}, options...)
}

// RegisterClientStreaming registers a client-streaming handler for method, the
// full method path "/package.Service/Method". It panics if method is not of
// that form. The handler may set response headers and trailing metadata with
// SetResponseHeader and SetResponseMetadata, except that a route whose
// requests may be stream-compressed sends no response headers (see
// SetResponseHeader). Request messages that arrive as
// datagrams are delivered with the others; RouteUnreliable has no effect on a
// method whose response is a single message.
func RegisterClientStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, MessageStream[Req]) (Res, error), options ...RouteOption) {
	s.routeStreaming(method, RPCKindClientStreaming, func(ctx context.Context, _ []byte, requests byteStream) (encodedStream, error) {
		response, err := handler(ctx, newDecodeStream(requests, newRequest))
		if err != nil {
			return nil, err
		}
		if isNilRegisteredValue(response) {
			return nil, Internal("handler returned nil response message")
		}
		return newCompressedEncodeStream(newSliceStream(response), responseCompressorFromContext(ctx)), nil
	}, options...)
}

// RegisterServerStreaming registers a server-streaming handler for method, the
// full method path "/package.Service/Method". It panics if method is not of
// that form. The handler may set response headers with SetResponseHeader
// before it returns, and trailing metadata with SetResponseMetadata until the
// returned stream ends. RouteUnreliable lets the response messages travel as
// datagrams.
func RegisterServerStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, Req) (MessageStream[Res], error), options ...RouteOption) {
	s.routeStreaming(method, RPCKindServerStreaming, func(ctx context.Context, body []byte, _ byteStream) (encodedStream, error) {
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
		return newResponseEncodeStream(ctx, responses), nil
	}, options...)
}

// RegisterBidirectionalStreaming registers a bidirectional-streaming handler
// for method, the full method path "/package.Service/Method". It panics if
// method is not of that form. The handler may set response headers with
// SetResponseHeader before it returns, unless the route's requests may be
// stream-compressed (see SetResponseHeader), and trailing metadata with
// SetResponseMetadata until the returned stream ends. RouteUnreliable lets the
// response messages travel as datagrams; request messages that arrive as
// datagrams are always accepted.
func RegisterBidirectionalStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, MessageStream[Req]) (MessageStream[Res], error), options ...RouteOption) {
	s.routeStreaming(method, RPCKindBidirectionalStreaming, func(ctx context.Context, _ []byte, requests byteStream) (encodedStream, error) {
		responses, err := handler(ctx, newDecodeStream(requests, newRequest))
		if err != nil {
			return nil, err
		}
		if isNilRegisteredValue(responses) {
			return nil, Internal("handler returned nil response stream")
		}
		return newResponseEncodeStream(ctx, responses), nil
	}, options...)
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
	options         ServerOptions
	routes          map[string]route
	authorizer      Authorizer
	metrics         Metrics
	requestLimit    semaphore
	connectionLimit semaphore
	admissionLimit  semaphore
	diagnostics     *serverDiagnosticDispatcher
	active          atomic.Int64
	detached        atomic.Int64
	// waiters counts the waitForExecutions calls in progress. While it is
	// zero, starting and finishing an execution only counts it.
	waiters atomic.Int32
	// executionMu guards executionChangedCh, which is closed, and then reset,
	// when an execution starts or finishes with a waiter present.
	executionMu        sync.Mutex
	executionChangedCh chan struct{}
	// compression holds the server's dictionaries, nil without any.
	compression *compressionConfig
	// streams is the stream compression budget, nil when it is off.
	streams *streamBudget
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
	canonical.StreamIdleTimeout = canonicalDuration("StreamIdleTimeout", options.StreamIdleTimeout, defaults.StreamIdleTimeout, false)
	canonical.ConnectionIdleTimeout = canonicalDuration("ConnectionIdleTimeout", options.ConnectionIdleTimeout, defaults.ConnectionIdleTimeout, options.DisableConnectionIdleTimeout)
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

// compressionError reports why the configured compression is invalid.
func (s *Server) compressionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compressionErr
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
	if runtime := s.frozen.Load(); runtime != nil {
		return runtime
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
		options:         options,
		routes:          routes,
		authorizer:      s.authorizer,
		metrics:         metrics,
		requestLimit:    newSemaphore(options.MaxConcurrentRequests),
		connectionLimit: newSemaphore(options.MaxConcurrentConnections),
		admissionLimit:  newSemaphore(options.MaxConcurrentAdmissionCallbacks),
		compression:     s.compression,
		streams:         s.streams,
	}
	runtime.diagnostics = newServerDiagnosticDispatcher(s.diagnostics, runtime)
	s.options = options
	s.runtime = runtime
	s.frozen.Store(runtime)
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

func (r *serverRuntime) executionStarted() { r.executionChanged(1) }

func (r *serverRuntime) executionFinished() { r.executionChanged(-1) }

// executionChanged counts an execution that started or finished and wakes
// the waitForExecutions calls.
func (r *serverRuntime) executionChanged(delta int64) {
	r.active.Add(delta)
	if r.waiters.Load() == 0 {
		return
	}
	r.executionMu.Lock()
	if r.executionChangedCh != nil {
		close(r.executionChangedCh)
		r.executionChangedCh = nil
	}
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

	r.waiters.Add(1)
	defer r.waiters.Add(-1)
	for {
		r.executionMu.Lock()
		if r.active.Load() == 0 {
			r.executionMu.Unlock()
			return true
		}
		if r.executionChangedCh == nil {
			r.executionChangedCh = make(chan struct{})
		}
		changed := r.executionChangedCh
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
	// workers run the goroutines the request starts, nil to start new ones.
	workers *streamWorkers
}

// goRun runs task on a goroutine of its own: a worker of the listener that
// received the request, whose stack has already grown, or a new goroutine.
func (l *requestExecutionLease) goRun(task func()) {
	if l != nil && l.workers != nil {
		l.workers.run(task)
		return
	}
	go task()
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
	return l.retainAs(&executionReference{}, phase)
}

// retainAs starts the execution that reference stands for. A reference that
// has finished can start another, if nothing uses it any more.
func (l *requestExecutionLease) retainAs(reference *executionReference, phase ServerDiagnosticPhase) *executionReference {
	if l == nil {
		return nil
	}
	l.refs.Add(1)
	l.runtime.executionStarted()
	reference.lease, reference.phase = l, phase
	reference.detached, reference.finished = false, false
	return reference
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

// unaryResponse is the complete response of a unary call: optional ANSWER and
// HEADERS, then a MESSAGE and an OK STATUS on success, or a lone failure
// STATUS.
type unaryResponse struct {
	headers Metadata
	// answer is the call's ANSWER, which goes out when answered is set.
	answer     wireAnswer
	answered   bool
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
	if len(r.headers) == 0 && !r.answered && r.hasMessage {
		// The message frame has room for the STATUS behind it.
		if r.message.size > maxFrameSize {
			return true, &FrameTooLargeError{Len: r.message.size, Max: maxFrameSize}
		}
		frames = r.message.frame
	} else {
		frames = make([]byte, 0, len(r.message.frame)+frameTailSlack)
		if r.answered {
			if frames, err = appendAnswerFrame(frames, &r.answer, maxFrameSize); err != nil {
				return true, err
			}
		}
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
	message, err := invokeUnaryHandler(ctx, route.unary, body, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		return r.finishResponse(service, method, requestBodyLen, startedAt, failedUnaryResponse(r.wireStatus(err, ServerDiagnosticInternalError, request)), request)
	}
	return r.finishResponse(service, method, requestBodyLen, startedAt, successfulUnaryResponse(ctx, message, request), request)
}

func failedUnaryResponse(status *Status) *unaryResponse {
	return &unaryResponse{status: status}
}

// successfulUnaryResponse is the response of a unary handler that returned
// message, with the response metadata it set through ctx.
func successfulUnaryResponse(ctx context.Context, message encodedMessage, request *wireRequest) *unaryResponse {
	response := responseMetadataFromContext(ctx)
	headers := response.takeHeaders()
	unary := &unaryResponse{
		headers:    headers,
		message:    message,
		hasMessage: true,
		status:     okStatus(response.takeMetadata()),
	}
	unary.answer, unary.answered = request.answer()
	return unary
}

func invokeUnaryHandler(
	ctx context.Context,
	handler unaryHandler,
	body []byte,
	raceContext bool,
	lease *requestExecutionLease,
) (message encodedMessage, err error) {
	if !raceContext {
		defer func() {
			if recovered := recover(); recovered != nil {
				message = encodedMessage{}
				err = &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}
			}
		}()
		return handler(ctx, body)
	}
	if err := ctx.Err(); err != nil {
		return encodedMessage{}, statusFromContextError(err)
	}

	type unaryResult struct {
		message encodedMessage
		err     error
	}
	result := make(chan unaryResult, 1)
	execution := lease.retain(ServerDiagnosticHandlerPanic)
	lease.goRun(func() {
		defer execution.finish()
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- unaryResult{err: &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}}
			}
		}()
		message, err := handler(ctx, body)
		result <- unaryResult{message: message, err: err}
	})

	select {
	case result := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return encodedMessage{}, statusFromContextError(ctxErr)
		}
		return result.message, result.err
	case <-ctx.Done():
		execution.detach()
		return encodedMessage{}, statusFromContextError(ctx.Err())
	}
}

// handleStreamingRequest dispatches a streaming request. body is the request
// message of a server-streaming call. On routes whose requests may be
// stream-compressed, HEADERS go to writer before the handler runs.
func (r *serverRuntime) handleStreamingRequest(ctx context.Context, request *wireRequest, body []byte, requestBody byteStream, lease *requestExecutionLease, writer io.Writer) responseWriter {
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
	requests := requestBody
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
	// Stream compression is negotiated once the call is authorized and
	// routed, so that an unauthorized caller can neither probe routes nor
	// hold the server's memory. An optimistic call the server cannot decode
	// is refused here, before the handler runs, with none of its request
	// messages read, so that the client can replay it.
	streams, refused := r.negotiateStreams(request)
	if refused {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, refusedStreamCompression(), request)
	}
	request.streams = streams
	setCallStreams(ctx, streams)
	if stream, ok := requests.(*rpcRequestStream); ok {
		stream.reader.stream = streams.requestInflater()
	}

	dispatched := route.streamCompressesRequests()
	if dispatched {
		if err := r.sendAnswerAtDispatch(writer, request); err != nil {
			cancel()
			lease.release()
			return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request), request)
		}
	}

	responses, err := invokeStreamingHandler(ctx, route.streaming, body, requestBody, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		cancel()
		lease.release()
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, r.wireStatus(err, ServerDiagnosticInternalError, request), request)
	}
	if responses == nil {
		cancel()
		lease.release()
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Service: service, Method: method, Kind: request.rpcKind(), Err: errors.New("handler returned nil response stream")})
		return r.finishStreamingStatus(service, method, requestBodyLen, startedAt, internalServerStatus(), request)
	}
	responses = closeStreamOnContext(ctx, responses)
	metadata := responseMetadataFromContext(ctx)
	stream := &serverResponseStream{responses: responses, headers: metadata.takeHeaders(), answerDispatched: dispatched, metadata: metadata, runtime: r, request: request, flow: responseFlow(request, route), lease: lease, service: service, method: method, requestBodyLen: requestBodyLen, startedAt: startedAt, ctx: ctx, limits: limits, cancel: cancel}
	if isNonBlockingStream(responses) {
		// Such a stream is read where it is written, and drained in batches.
		stream.receiver = newStreamReceiver(ctx, responses, limits.idleTimeout, "response", lease)
	} else {
		stream.readAhead = newResponseReadAhead(ctx, responses, limits.idleTimeout, lease)
	}
	return stream
}

// sendAnswerAtDispatch writes the ANSWER of a call whose requests may be
// stream-compressed, if it has one, before its handler runs, so a
// client-streaming client learns the answer while it is still uploading.
func (r *serverRuntime) sendAnswerAtDispatch(writer io.Writer, request *wireRequest) error {
	answer, answered := request.answer()
	if !answered {
		return nil
	}
	frame, err := appendAnswerFrame(nil, &answer, r.options.MaxFrameSize)
	if err != nil {
		return err
	}
	if _, err := writer.Write(frame); err != nil {
		return Cancelled("response stream closed")
	}
	return nil
}

// responseFlow returns the flow the response messages of a call travel on as
// datagrams, or nil if they stay on the stream.
func responseFlow(request *wireRequest, route route) *datagramFlow {
	if request.flow == nil || !route.unreliable.Responses {
		return nil
	}
	return request.flow
}

type streamingHandlerResult struct {
	responses encodedStream
	err       error
}

func invokeStreamingHandler(
	ctx context.Context,
	handler streamingHandler,
	body []byte,
	requestBody byteStream,
	raceContext bool,
	lease *requestExecutionLease,
) (responses encodedStream, err error) {
	if !raceContext {
		defer func() {
			if recovered := recover(); recovered != nil {
				responses = nil
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
	// A goroutine of its own, not a pooled worker: on a worker, 64 concurrent
	// client-streaming calls ran 7% slower, while a streaming handler is
	// long-lived enough that a fresh stack costs it little.
	go func() {
		defer execution.finish()
		resultValue := func() (result streamingHandlerResult) {
			defer func() {
				if recovered := recover(); recovered != nil {
					result = streamingHandlerResult{err: &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}}
				}
			}()
			result.responses, result.err = handler(ctx, body, requestBody)
			return result
		}()
		select {
		case result <- resultValue:
		case <-abandoned:
			discardMessageStream(resultValue.responses)
		}
	}()

	select {
	case result := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			discardMessageStream(result.responses)
			return nil, statusFromContextError(ctxErr)
		}
		return result.responses, result.err
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
	// A request served from a stream was validated, and ctx already carries
	// its deadline and handler context: building them again cost each call a
	// context and a timer. Its cancel func is the one returned, so the
	// handler's context still ends when the handler is done with it, as it
	// would with a context of its own; a streaming response's Close may wait
	// for that.
	cancel, carried := carriedCall(ctx, request)
	if !carried {
		if err := validateRequest(request); err != nil {
			return ctx, func() {}, err
		}
		var err error
		ctx, cancel, err = requestContext(ctx, request)
		if err != nil {
			return ctx, cancel, err
		}
	}
	if r.authorizer != nil {
		// The authorizer gets a map of its own: the handler's is the request's,
		// and an authorizer that changes it, or one that outlives its deadline,
		// must not touch what the handler reads.
		info := newHandlerContext(request)
		info.Metadata = maps.Clone(info.Metadata)
		if err := invokeAuthorizer(ctx, r.authorizer, info, handlerNeedsDeadlineRace(ctx), lease); err != nil {
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
	lease.goRun(func() { defer execution.finish(); result <- invoke() })
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

// requestLifetimeContext returns the context of a validated request served
// from a stream: canceled when the stream's call ends or its deadline passes,
// and carrying its handler context.
func requestLifetimeContext(ctx context.Context, request *wireRequest) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	if timeout, ok, err := rpcRequestTimeout(request); err == nil && ok {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	ctx = contextWithHandlerContext(ctx, request)
	ctx.Value(handlerContextKey{}).(*callState).cancel = cancel
	return ctx, cancel
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
	receiver  *streamReceiver[[]byte]
	// mu guards done and stopCancelRead: Close may run on another goroutine
	// than Recv, such as the one that closes the stream when ctx ends.
	mu             sync.Mutex
	stopCancelRead func()
	done           bool
}

func limitByteStream(ctx context.Context, inner byteStream, limits streamLimits, direction string, lease *requestExecutionLease, request *wireRequest) byteStream {
	stream := &limitedByteStream{inner: inner, ctx: ctx, limits: limits, direction: direction, lease: lease, request: request}
	stream.receiver = newStreamReceiver(ctx, inner, limits.idleTimeout, direction, lease)
	if cancellable, ok := inner.(contextCancelReadStream); ok {
		stream.stopCancelRead = cancellable.trevrpcCancelReadOnContext(ctx)
	}

	return stream
}

func (s *limitedByteStream) Recv() ([]byte, error) {
	if s.isDone() {
		return nil, io.EOF
	}

	body, err := s.receiver.recv()
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
	s.receiver.close()
	return first
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
// ANSWER and HEADERS before the first message, MESSAGE frames pulled from the
// handler's stream, and a terminal STATUS.
//
// A stream that never blocks is read where it is written. Any other stream is
// read ahead by a goroutine (see responseReadAhead), and each write carries
// all the messages that have queued by then.
type serverResponseStream struct {
	responses encodedStream
	// headers go out before the first message, and metadata is where the
	// trailing metadata of the final OK status comes from.
	headers         Metadata
	metadata        *responseMetadata
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
	// answerDispatched says the call's ANSWER, if any, went out before the
	// handler ran.
	answerDispatched bool
	pending          encodedMessage
	hasPending       bool
	done             bool
	cancel           context.CancelFunc
	// flow is the call's datagram flow when the route sends its responses as
	// datagrams, and nil otherwise.
	flow *datagramFlow
	// receiver reads a stream that never blocks, and readAhead any other.
	receiver  *streamReceiver[encodedMessage]
	readAhead *responseReadAhead
	// batch and buffer are reused by the writes: the messages of one, and
	// the frames joined for it.
	batch      []encodedMessage
	buffer     []byte
	finishOnce sync.Once
}

type responseStreamItem struct {
	message encodedMessage
	status  *Status
}

// headersFrames returns the ANSWER, if the call has one it did not send at
// dispatch, and the HEADERS of a call, or nil if it sends neither. A call that
// sends datagrams flushes HEADERS, empty ones included, before the first one.
func (s *serverResponseStream) headersFrames(maxFrameSize int) ([]byte, error) {
	var frames []byte
	if !s.answerDispatched {
		if answer, answered := s.request.answer(); answered {
			var err error
			if frames, err = appendAnswerFrame(frames, &answer, maxFrameSize); err != nil {
				return nil, err
			}
		}
	}
	if len(s.headers) == 0 && s.flow == nil {
		return frames, nil
	}
	return appendHeadersFrame(frames, s.headers, maxFrameSize)
}

// maxRetainedWriteBuffer is the largest buffer a response keeps between
// writes. A bigger one is rare, and not worth holding for the rest of the call.
const maxRetainedWriteBuffer = 256 * 1024

// writeNext writes the next frames of the response. An ANSWER that did not go
// out at dispatch, and HEADERS, go out as soon as the handler has returned, in
// a write of their own when pulling the first message may block. A write carries every
// message that is ready, up to the batch limits and without waiting for more,
// and the terminal STATUS joins the write of the last messages.
func (s *serverResponseStream) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if s.done {
		return true, nil
	}

	var headers []byte
	if !s.headersSent {
		s.headersSent = true
		if s.readAhead != nil {
			// The handler starts on the first message while HEADERS go out.
			s.readAhead.start()
		}
		var err error
		if headers, err = s.headersFrames(maxFrameSize); err != nil {
			return s.writeFinalFrames(writer, maxFrameSize, nil, nil, s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request), beforeStatus)
		}
		if headers != nil && (s.readAhead != nil || s.flow != nil) {
			if _, err := writer.Write(headers); err != nil {
				return true, err
			}
			headers = nil
		}
	}

	if s.batch == nil {
		s.batch = make([]encodedMessage, 0, handlerReadAheadMessages)
	}
	batch := s.batch[:0]
	defer func() { clear(batch); s.batch = batch[:0] }()
	size := len(headers)
	batchByteLimit := responseFrameBatchByteLimit(maxFrameSize)
	var final *Status
	for len(batch) < handlerReadAheadMessages {
		if len(batch) > 0 && s.readAhead == nil {
			// A stream read ahead checks the call where it hands out a message.
			if err := ctx.Err(); err != nil {
				break
			}
		}

		// Only the first message is waited for. The others are the ones
		// already there.
		item, ok := s.readNextResponseItem(len(batch) == 0)
		if !ok {
			break
		}
		if item.status == nil {
			if len(batch) > 0 && batchByteLimit > 0 && saturatingAdd(size, len(item.message.frame)) > batchByteLimit {
				s.pending, s.hasPending = item.message, true
				break
			}
			item.status = s.acceptResponseMessage(item.message)
		}
		if item.status != nil {
			final = item.status
			break
		}
		if item.message.size > maxFrameSize {
			return true, &FrameTooLargeError{Len: item.message.size, Max: maxFrameSize}
		}

		batch = append(batch, item.message)
		size = saturatingAdd(size, len(item.message.frame))
	}

	if s.flow != nil && len(batch) > 0 {
		// The messages that are ready go out as few datagrams as fit; those
		// that do not travel as datagrams stay for the stream.
		var err error
		if batch, err = s.flow.sendBatch(s.ctx, batch); err != nil {
			return true, err
		}
	}

	if final != nil {
		return s.writeFinalFrames(writer, maxFrameSize, headers, batch, final, beforeStatus)
	}
	switch {
	case len(batch) == 0 && len(headers) == 0:
		return false, nil
	case len(batch) == 1 && len(headers) == 0:
		// A lone message is the write, without a copy.
		_, err := writer.Write(batch[0].frame)
		return false, err
	}
	return false, s.writeJoined(writer, headers, batch, nil, maxFrameSize, nil)
}

// writeFinalFrames writes headers and messages followed by the terminal STATUS
// and finishes the response.
func (s *serverResponseStream) writeFinalFrames(writer io.Writer, maxFrameSize int, headers []byte, messages []encodedMessage, status *Status, beforeStatus func()) (bool, error) {
	s.finish(status.Code)
	return true, s.writeJoined(writer, headers, messages, status, maxFrameSize, beforeStatus)
}

// writeJoined writes headers, messages and then status, if there is one, in
// one write. beforeStatus is called just before the write when there is a
// status.
func (s *serverResponseStream) writeJoined(writer io.Writer, headers []byte, messages []encodedMessage, status *Status, maxFrameSize int, beforeStatus func()) error {
	total := len(headers)
	for _, message := range messages {
		total = saturatingAdd(total, len(message.frame))
	}
	frames := s.buffer[:0]
	if cap(frames) < total+frameTailSlack {
		frames = make([]byte, 0, total+frameTailSlack)
	}
	frames = append(frames, headers...)
	for _, message := range messages {
		frames = append(frames, message.frame...)
	}
	if status != nil {
		var err error
		if frames, err = appendStatusFrame(frames, status, maxFrameSize); err != nil {
			s.buffer = nil
			return err
		}
		callBeforeStatus(beforeStatus)
	}
	_, err := writer.Write(frames)
	if status == nil && cap(frames) <= maxRetainedWriteBuffer {
		s.buffer = frames[:0]
	} else {
		// After the last write, or a very large one, nothing is kept.
		s.buffer = nil
	}
	return err
}

func (s *serverResponseStream) trevrpcNonBlockingStream() bool {
	return s.readAhead == nil
}

func (s *serverResponseStream) trevrpcContextCancelsRecv() bool {
	return s.limits.idleTimeout <= 0 && streamContextCancelsRecv(s.responses)
}

func (s *serverResponseStream) Close() error {
	if !s.done {
		s.finish(CodeCancelled)
	}
	s.buffer = nil

	return nil
}

func (s *serverResponseStream) finish(code Code) {
	s.finishOnce.Do(func() {
		s.done = true
		// The receiver goes first, so it can tell a call the peer or a
		// deadline ended from one that ends here.
		s.closeReceiver()
		s.cancel()
		discardMessageStream(s.responses)
		s.runtime.recordFinished(RPCFinished{Service: s.service, Method: s.method, RequestBodyLen: s.requestBodyLen, ResponseBodyLen: s.responseBodyLen, Code: code, Elapsed: time.Since(s.startedAt)}, s.request)
		s.lease.release()
	})
}

func (s *serverResponseStream) closeReceiver() {
	if s.readAhead != nil {
		s.readAhead.close()
		return
	}
	s.receiver.close()
}

// readNextResponseItem returns the next message or the terminal status of the
// response. If wait is not set it reports false instead of waiting for a
// stream that has nothing ready.
func (s *serverResponseStream) readNextResponseItem(wait bool) (responseStreamItem, bool) {
	if s.hasPending {
		message := s.pending
		s.pending = encodedMessage{}
		s.hasPending = false
		return responseStreamItem{message: message}, true
	}

	var message encodedMessage
	var err error
	if s.readAhead != nil {
		item, ok, waitErr := s.readAhead.next(wait)
		if waitErr != nil {
			return responseStreamItem{status: s.runtime.wireStatus(waitErr, ServerDiagnosticInternalError, s.request)}, true
		}
		if !ok {
			return responseStreamItem{}, false
		}
		message, err = item.message, item.err
	} else {
		message, err = s.receiver.recv()
	}
	if errors.Is(err, io.EOF) {
		return responseStreamItem{status: s.terminalResponseStatus()}, true
	}
	if err != nil {
		return responseStreamItem{status: s.runtime.wireStatus(err, ServerDiagnosticInternalError, s.request)}, true
	}

	return responseStreamItem{message: message}, true
}

func (s *serverResponseStream) acceptResponseMessage(message encodedMessage) *Status {
	if err := checkStreamLimits("response", s.limits, &s.messages, &s.responseBodyLen, message.size); err != nil {
		return StatusFromError(err)
	}

	return nil
}

// terminalResponseStatus is the final OK status of a response stream that
// ended, with the trailing metadata the handler set through its context.
func (s *serverResponseStream) terminalResponseStatus() *Status {
	return okStatus(s.metadata.takeMetadata())
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
