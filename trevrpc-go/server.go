package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
	"google.golang.org/protobuf/reflect/protoreflect"
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
	// method is the descriptor of the route's method, nil for a route
	// registered without one (see RouteMethodDescriptor).
	method protoreflect.MethodDescriptor
	// request is the request message type of a route registered with
	// Register*, and nil for a route registered with a raw handler. Its
	// bindUnary or bindStreaming builds the handler for a validation function,
	// which freeze calls when the server has a validator.
	request       protoreflect.MessageDescriptor
	bindUnary     func(validateFunc) unaryHandler
	bindStreaming func(validateFunc) streamingHandler
}

// typedRoute is the request type and handler builder of a route registered
// with Register*; see route.request.
type typedRoute struct {
	request       protoreflect.MessageDescriptor
	bindUnary     func(validateFunc) unaryHandler
	bindStreaming func(validateFunc) streamingHandler
}

// requestDescriptor returns the message type newRequest makes, or nil when it
// makes nil, which the route reports on every call.
func requestDescriptor[Req ProtoMessage](newRequest func() Req) protoreflect.MessageDescriptor {
	request := newRequest()
	if isNilRegisteredValue(request) {
		return nil
	}
	return protoMessageV2(request).ProtoReflect().Descriptor()
}

// ErrServerFrozen is the panic value used when a server is mutated after first use.
var ErrServerFrozen = errors.New("trevrpc: server configuration is frozen")

// Server routes and handles TrevRPC requests.
type Server struct {
	mu           sync.Mutex
	routes       map[string]route
	options      ServerOptions
	authorizer   Authorizer
	interceptors []Interceptor
	metrics      Metrics
	diagnostics  ServerDiagnosticHandler
	validator    Validator
	runtime      *serverRuntime
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
// DevelopmentCertificate is the certificate of a local development server.
// Listen presents it, so ListenOptions.TLSConfig must be nil, and the server
// answers GET and HEAD requests for HTTPPath + "/certificate-hashes" with its
// hashes over every HTTP version the server speaks, for the JavaScript
// client's fetchServerCertificateHashes option: a web app mounts and proxies
// nothing. The hashes path is claimed whenever the server speaks HTTP.
// Without a DevelopmentCertificate it answers 404,
// which tells the client that the browser trusts the certificate already,
// instead of passing the request to HTTPFallback. Both responses allow any
// origin to read them.
//
// Compression lets messages travel compressed with the dictionaries it
// holds; see Compression. Listen fails when it is invalid.
//
// StreamCompressionMemory bounds the memory, in bytes, of the zstd contexts
// that stream compression holds for the server's calls (see
// StreamCompression): about 350 KiB for a direction the server decodes and
// 585 KiB for one it compresses. Calls beyond it are compressed per message.
// Zero means 256 MiB, and a negative value turns stream compression off.
//
// AddressValidationRate makes the QUIC socket ask a client to prove its
// address with a Retry packet before the server keeps any state for its
// connection, which costs the client a round trip, once more connection
// attempts from addresses QUIC has not validated arrive than that many a
// second, as in a flood of handshakes from spoofed addresses. Bursts of up to
// a second's worth pass. ValidateAllAddresses asks every such attempt. A
// client that kept an address validation token from an earlier connection
// skips it. Zero and false, the defaults, never ask. Once a connection
// completes its handshake its address is proven either way, so RemoteAddr is
// a sound key for per-client limits.
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
	DevelopmentCertificate            *DevelopmentCertificate
	Compression                       *Compression
	StreamCompressionMemory           int64
	AddressValidationRate             int
	ValidateAllAddresses              bool
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

// unclaimedHandler returns the handler of an HTTP request for path that TrevRPC
// does not serve as an RPC or a browser session: the certificate hashes, or a
// 404 for them when there is no DevelopmentCertificate, or HTTPFallback, which
// is nil when there is none.
func (o ServerOptions) unclaimedHandler(path string) http.Handler {
	if base, ok := strings.CutSuffix(path, certificateHashesSuffix); ok && base == strings.TrimSuffix(o.HTTPPath, "/") {
		if o.DevelopmentCertificate == nil {
			return certificateHashesNotFound
		}
		return o.DevelopmentCertificate
	}
	return o.HTTPFallback
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
	// RemoteAddr is the address of the client, as HandlerContext.RemoteAddr
	// reports it.
	RemoteAddr netip.AddrPort
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
	ServerDiagnosticInterceptorPanic       ServerDiagnosticPhase = "interceptor_panic"
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
	ServerDiagnosticProxyHeaderFailed      ServerDiagnosticPhase = "proxy_header_failed"
	ServerDiagnosticInvalidResponse        ServerDiagnosticPhase = "invalid_response"
	ServerDiagnosticInternalError          ServerDiagnosticPhase = "internal_error"
	ServerDiagnosticExecutionDetached      ServerDiagnosticPhase = "execution_detached"
	ServerDiagnosticExecutionFinallyExited ServerDiagnosticPhase = "execution_finally_exited"
	ServerDiagnosticShutdownIncomplete     ServerDiagnosticPhase = "shutdown_incomplete"
)

// ServerDiagnostic describes a local server failure. Request bodies and metadata are never included.
type ServerDiagnostic struct {
	Phase ServerDiagnosticPhase
	// Context is the context of the call the diagnostic is about, for the
	// phases that belong to one call, such as a panic in a handler or an
	// interceptor, an invalid handler response and an error that became
	// Internal; it is nil for the others. It is the context the call's last
	// interceptor returned, so it carries what interceptors put there, such as
	// a tracing span, which lets a handler of the diagnostic log it with the
	// call's trace. Diagnostics are delivered asynchronously, after the call
	// has usually ended, so the context may already be done: use it for its
	// values, not for its deadline or cancellation.
	Context                context.Context
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
	// Registered reports whether Service and Method name a method registered
	// on the server. Otherwise they are whatever the client sent, unchecked,
	// so metrics should not label measurements with them: the authorizer and
	// other checks reject calls before the server looks up their method.
	Registered bool
}

// RPCFinished describes an RPC completion event.
type RPCFinished struct {
	Service         string
	Method          string
	RequestBodyLen  int
	ResponseBodyLen int
	Code            Code
	Elapsed         time.Duration
	// Err is the error the call ended with, nil when it succeeded: what the
	// handler, an interceptor, the authorizer or a request or response stream
	// returned, or the panic that ended the call, as the server saw it. It is
	// not what the caller sees: an error that is not a *Status, and a *Status
	// with code Internal, reach the caller as Internal with the message
	// "internal server error", and Err is the original, so it can say why the
	// call failed. A call the server ends for another reason, such as an
	// unknown method or a deadline, has a *Status as Err. Err stays on the
	// server and is never sent, but it may contain what the caller must not
	// see, so log it rather than return it.
	Err error
	// Registered reports whether Service and Method name a registered method,
	// as for RPCStarted.
	Registered bool
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
// authorizer removes it. The context the authorizer returns is the one that
// interceptors and the handler get.
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
	s.addUnaryRoute(method, handler, typedRoute{}, options)
}

// routeTypedUnary registers a unary route of Register*, whose handler
// typed.bindUnary builds.
func (s *Server) routeTypedUnary(method string, typed typedRoute, options []RouteOption) {
	s.addUnaryRoute(method, typed.bindUnary(nil), typed, options)
}

func (s *Server) addUnaryRoute(method string, handler unaryHandler, typed typedRoute, options []RouteOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	validateRoute(method, handler != nil)
	config := applyRouteOptions(options)
	validateRouteMethod(method, RPCKindUnary, config.method)
	s.routes[method] = route{kind: RPCKindUnary, unary: handler, plainCompression: config.plainCompression, method: config.method, request: typed.request, bindUnary: typed.bindUnary}
}

// routeStreaming registers a streaming route handler for a method path and RPC kind.
func (s *Server) routeStreaming(method string, kind RPCKind, handler streamingHandler, options ...RouteOption) {
	s.addStreamingRoute(method, kind, handler, typedRoute{}, options)
}

// routeTypedStreaming registers a streaming route of Register*, whose handler
// typed.bindStreaming builds.
func (s *Server) routeTypedStreaming(method string, kind RPCKind, typed typedRoute, options []RouteOption) {
	s.addStreamingRoute(method, kind, typed.bindStreaming(nil), typed, options)
}

func (s *Server) addStreamingRoute(method string, kind RPCKind, handler streamingHandler, typed typedRoute, options []RouteOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	if kind == RPCKindUnary || !kind.isValid() {
		panic("trevrpc: streaming route requires a streaming RPC kind")
	}
	validateRoute(method, handler != nil)
	config := applyRouteOptions(options)
	validateRouteMethod(method, kind, config.method)
	s.routes[method] = route{
		kind:              kind,
		streaming:         handler,
		unreliable:        config.unreliable.effective(kind),
		streamCompression: config.streamCompression.effective(kind),
		plainCompression:  config.plainCompression,
		method:            config.method,
		request:           typed.request,
		bindStreaming:     typed.bindStreaming,
	}
}

// bindValidators rebuilds the handlers of routes whose request messages
// validator checks, and returns the first route it could not bind.
func bindValidators(routes map[string]route, validator Validator) error {
	if validator == nil {
		return nil
	}
	var bindErr error
	for method, route := range routes {
		if route.request == nil {
			continue
		}
		validate, err := bindValidator(validator, "route "+method, route.request)
		if bindErr == nil {
			bindErr = err
		}
		if validate == nil {
			continue
		}
		if route.bindUnary != nil {
			route.unary = route.bindUnary(validate)
		} else {
			route.streaming = route.bindStreaming(validate)
		}
		routes[method] = route
	}
	return bindErr
}

// RegisterUnary registers a unary handler for method, the full method path
// "/package.Service/Method". It panics if method is not of that form. The
// handler may set response headers and trailing metadata with
// SetResponseHeader and SetResponseMetadata. Generated registration passes the
// method's schema options as RouteOptions, and its descriptor as
// RouteMethodDescriptor; RoutePlainCompression is the only schema option that
// applies to a unary route.
func RegisterUnary[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, Req) (Res, error), options ...RouteOption) {
	validateRoute(method, handler != nil)
	s.routeTypedUnary(method, typedRoute{
		request: requestDescriptor(newRequest),
		bindUnary: func(validate validateFunc) unaryHandler {
			return func(ctx context.Context, body []byte) (encodedMessage, error) {
				request, err := decodeRegisteredRequest(body, newRequest, validate)
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
			}
		},
	}, options)
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
	validateRoute(method, handler != nil)
	s.routeTypedStreaming(method, RPCKindClientStreaming, typedRoute{
		request: requestDescriptor(newRequest),
		bindStreaming: func(validate validateFunc) streamingHandler {
			return func(ctx context.Context, _ []byte, requests byteStream) (encodedStream, error) {
				response, err := handler(ctx, newDecodeStream(requests, newRequest, validate))
				if err != nil {
					return nil, err
				}
				if isNilRegisteredValue(response) {
					return nil, Internal("handler returned nil response message")
				}
				return newCompressedEncodeStream(StreamOf(response), responseCompressorFromContext(ctx)), nil
			}
		},
	}, options)
}

// RegisterServerStreaming registers a server-streaming handler for method, the
// full method path "/package.Service/Method". It panics if method is not of
// that form. The handler may set response headers with SetResponseHeader
// before it returns, and trailing metadata with SetResponseMetadata until the
// returned stream ends. RouteUnreliable lets the response messages travel as
// datagrams.
func RegisterServerStreaming[Req ProtoMessage, Res ProtoMessage](s *Server, method string, newRequest func() Req, handler func(context.Context, Req) (MessageStream[Res], error), options ...RouteOption) {
	validateRoute(method, handler != nil)
	s.routeTypedStreaming(method, RPCKindServerStreaming, typedRoute{
		request: requestDescriptor(newRequest),
		bindStreaming: func(validate validateFunc) streamingHandler {
			return func(ctx context.Context, body []byte, _ byteStream) (encodedStream, error) {
				request, err := decodeRegisteredRequest(body, newRequest, validate)
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
			}
		},
	}, options)
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
	validateRoute(method, handler != nil)
	s.routeTypedStreaming(method, RPCKindBidirectionalStreaming, typedRoute{
		request: requestDescriptor(newRequest),
		bindStreaming: func(validate validateFunc) streamingHandler {
			return func(ctx context.Context, _ []byte, requests byteStream) (encodedStream, error) {
				responses, err := handler(ctx, newDecodeStream(requests, newRequest, validate))
				if err != nil {
					return nil, err
				}
				if isNilRegisteredValue(responses) {
					return nil, Internal("handler returned nil response stream")
				}
				return newResponseEncodeStream(ctx, responses), nil
			}
		},
	}, options)
}

// decodeRegisteredRequest decodes a request message, and checks it with
// validate when there is one.
func decodeRegisteredRequest[Req ProtoMessage](body []byte, newRequest func() Req, validate validateFunc) (Req, error) {
	request := newRequest()
	if isNilRegisteredValue(request) {
		var zero Req
		return zero, Internal("request factory returned nil")
	}
	if err := unmarshalMessage(body, request); err != nil {
		var zero Req
		return zero, InvalidArgument("failed to decode request: " + err.Error())
	}
	if err := checkRequest(validate, request); err != nil {
		var zero Req
		return zero, err
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
	interceptors    []Interceptor
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
	// validatorErr is why the validator could not check a route's requests;
	// Listen reports it.
	validatorErr error
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
	canonical.AddressValidationRate = canonicalPositiveLimit("AddressValidationRate", options.AddressValidationRate, 0)
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
	validatorErr := bindValidators(routes, s.validator)
	metrics := s.metrics
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	runtime := &serverRuntime{
		options:         options,
		routes:          routes,
		authorizer:      s.authorizer,
		interceptors:    s.interceptors,
		metrics:         metrics,
		requestLimit:    newSemaphore(options.MaxConcurrentRequests),
		connectionLimit: newSemaphore(options.MaxConcurrentConnections),
		admissionLimit:  newSemaphore(options.MaxConcurrentAdmissionCallbacks),
		compression:     s.compression,
		streams:         s.streams,
		validatorErr:    validatorErr,
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

// wireStatus returns the STATUS to send for err, the error a call ended with.
// An error that is not a *Status with a code other than Internal, or whose
// metadata is invalid, becomes Internal with a message that says nothing of
// it, and is reported as a diagnostic of phase (or of the phase of the panic
// that caused it), with the context of the call, ctx or what its
// interceptors derived from it.
func (r *serverRuntime) wireStatus(ctx context.Context, err error, phase ServerDiagnosticPhase, request *wireRequest) *Status {
	if err == nil {
		return OK()
	}
	if panicErr, ok := errors.AsType[*serverPanicError](err); ok {
		r.emitDiagnostic(ServerDiagnostic{Phase: panicErr.phase, Context: diagnosticContext(ctx, request), Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err, Panic: panicErr.recovered, Stack: panicErr.stack})
		return internalServerStatus()
	}
	if status, ok := errors.AsType[*Status](err); ok && status.Code != CodeInternal {
		if validateMetadata(status.Metadata) == nil {
			return status.clone()
		}
	}
	r.emitDiagnostic(ServerDiagnostic{Phase: phase, Context: diagnosticContext(ctx, request), Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Err: err})
	return internalServerStatus()
}

// diagnosticContext returns the context to report a diagnostic of request
// with: the context its last interceptor returned, which carries what the
// interceptors added to ctx, or else ctx, the context the call's handler would
// otherwise get, which may be nil.
func diagnosticContext(ctx context.Context, request *wireRequest) context.Context {
	if intercepted := interceptedContext(request); intercepted != nil {
		return intercepted
	}
	return ctx
}

// interceptedContext returns the context the last interceptor of request
// returned, or nil if none ran.
func interceptedContext(request *wireRequest) context.Context {
	if request == nil || request.intercepted == nil {
		return nil
	}
	return request.intercepted.context()
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
	// cause is the error the call failed with, if it has one.
	cause error
	call  callRecord
}

func (r *statusResponse) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	err := r.write(ctx, writer, maxFrameSize, beforeStatus)
	r.call.finishWritten(ctx, r.status, r.cause, err)
	return true, err
}

func (r *statusResponse) write(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frames, err := appendStatusFrame(nil, r.status, maxFrameSize)
	if err != nil {
		return err
	}
	callBeforeStatus(beforeStatus)
	_, err = writer.Write(frames)
	return err
}

// Close reports a call whose response was never written.
func (r *statusResponse) Close() error {
	r.call.finish(callOutcome{code: CodeCancelled, err: errResponseClosed})
	return nil
}

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
	// cause is the error the call failed with, if it has one.
	cause error
	call  callRecord
}

func (r *unaryResponse) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	err := r.write(ctx, writer, maxFrameSize, beforeStatus)
	r.call.responseBodyLen = r.message.size
	r.call.finishWritten(ctx, r.status, r.cause, err)
	return true, err
}

func (r *unaryResponse) write(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frames, err := r.frames(maxFrameSize)
	if tooLarge, ok := errors.AsType[*FrameTooLargeError](err); ok {
		// The call fails with a lone STATUS instead. The client gets its
		// code, and the call is reported as the failure it is.
		r.status, r.cause = r.call.oversizedResponseStatus(ctx, tooLarge), tooLarge
		r.headers, r.answered, r.message, r.hasMessage = nil, false, encodedMessage{}, false
		frames, err = r.frames(maxFrameSize)
	}
	if err != nil {
		return err
	}
	callBeforeStatus(beforeStatus)
	_, err = writer.Write(frames)
	return err
}

// frames returns the frames of the response, which are the same however often
// it is asked for.
func (r *unaryResponse) frames(maxFrameSize int) ([]byte, error) {
	var frames []byte
	var err error
	if len(r.headers) == 0 && !r.answered && r.hasMessage {
		// The message frame has room for the STATUS behind it.
		if r.message.size > maxFrameSize {
			return nil, &FrameTooLargeError{Len: r.message.size, Max: maxFrameSize}
		}
		frames = r.message.frame
	} else {
		frames = make([]byte, 0, len(r.message.frame)+frameTailSlack)
		if r.answered {
			if frames, err = appendAnswerFrame(frames, &r.answer, maxFrameSize); err != nil {
				return nil, err
			}
		}
		if len(r.headers) > 0 {
			if frames, err = appendHeadersFrame(frames, r.headers, maxFrameSize); err != nil {
				return nil, err
			}
		}
		if r.hasMessage {
			if frames, err = appendEncodedMessage(frames, r.message, maxFrameSize); err != nil {
				return nil, err
			}
		}
	}
	return appendStatusFrame(frames, r.status, maxFrameSize)
}

// Close reports a call whose response was never written.
func (r *unaryResponse) Close() error {
	r.call.finish(callOutcome{code: CodeCancelled, err: errResponseClosed})
	return nil
}

func (r *serverRuntime) handleRequest(ctx context.Context, request *wireRequest, body []byte, lease *requestExecutionLease) *unaryResponse {
	call := callRecord{runtime: r, request: request, requestBodyLen: len(body), startedAt: time.Now()}
	if request == nil {
		r.recordStarted(RPCStarted{}, nil)
		return call.failedUnary(InvalidArgument("RPC request is nil"), nil)
	}
	call.service, call.method = request.serviceAndMethod()
	r.recordStarted(RPCStarted{Service: call.service, Method: call.method, RequestBodyLen: call.requestBodyLen}, request)

	ctx, cancel, err := r.prepareRequest(ctx, request, lease)
	if err != nil {
		return call.failedUnary(r.wireStatus(ctx, err, ServerDiagnosticInternalError, request), err)
	}
	defer cancel()

	route, ok := r.routes[request.Method]
	if !ok || route.unary == nil {
		return call.failedUnary(Unimplemented(fmt.Sprintf("unknown RPC method %s", request.Method)), nil)
	}
	message, err := invokeUnaryHandler(ctx, r.interceptUnary(route.unary, request), body, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		return call.failedUnary(r.wireStatus(ctx, err, ServerDiagnosticInternalError, request), err)
	}
	return call.successfulUnary(ctx, message)
}

// failedUnary is the response of a unary call that failed with status, and
// with err if it has one.
func (c callRecord) failedUnary(status *Status, err error) *unaryResponse {
	return &unaryResponse{status: status, cause: failureCause(err, status), call: c}
}

// successfulUnary is the response of a unary handler that returned message,
// with the response metadata it set through ctx.
func (c callRecord) successfulUnary(ctx context.Context, message encodedMessage) *unaryResponse {
	response := responseMetadataFromContext(ctx)
	headers := response.takeHeaders()
	unary := &unaryResponse{
		headers:    headers,
		message:    message,
		hasMessage: true,
		status:     okStatus(response.takeMetadata()),
		call:       c,
	}
	unary.answer, unary.answered = c.request.answer()
	return unary
}

// invokeUnaryHandler runs handler, racing it against ctx when raceContext is
// set, so that a call that ends is not held up by a handler that does not
// return.
func invokeUnaryHandler(
	ctx context.Context,
	handler unaryHandler,
	body []byte,
	raceContext bool,
	lease *requestExecutionLease,
) (encodedMessage, error) {
	var message encodedMessage
	var err error
	if raceContext {
		message, err = raceUnaryHandler(ctx, handler, body, lease)
	} else {
		message, err = callUnaryHandler(ctx, handler, body)
	}
	if err = afterHandler(ctx, err); err != nil {
		return encodedMessage{}, err
	}
	return message, nil
}

// callUnaryHandler runs handler on the calling goroutine.
func callUnaryHandler(ctx context.Context, handler unaryHandler, body []byte) (message encodedMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message = encodedMessage{}
			err = &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}
		}
	}()
	return handler(ctx, body)
}

// raceUnaryHandler runs handler on a goroutine of its own and stops waiting
// for it when ctx ends.
func raceUnaryHandler(ctx context.Context, handler unaryHandler, body []byte, lease *requestExecutionLease) (encodedMessage, error) {
	if err := ctx.Err(); err != nil {
		return encodedMessage{}, err
	}

	type unaryResult struct {
		message encodedMessage
		err     error
	}
	result := make(chan unaryResult, 1)
	execution := lease.retain(ServerDiagnosticHandlerPanic)
	lease.goRun(func() {
		defer execution.finish()
		message, err := callUnaryHandler(ctx, handler, body)
		result <- unaryResult{message: message, err: err}
	})

	select {
	case result := <-result:
		return result.message, result.err
	case <-ctx.Done():
		execution.detach()
		return encodedMessage{}, ctx.Err()
	}
}

// handleStreamingRequest dispatches a streaming request. body is the request
// message of a server-streaming call. On routes whose requests may be
// stream-compressed, HEADERS go to writer before the handler runs.
func (r *serverRuntime) handleStreamingRequest(ctx context.Context, request *wireRequest, body []byte, requestBody byteStream, lease *requestExecutionLease, writer io.Writer) responseWriter {
	call := callRecord{runtime: r, request: request, requestBodyLen: len(body), startedAt: time.Now()}
	if request == nil {
		r.recordStarted(RPCStarted{}, nil)
		lease.release()
		return call.failedStatus(InvalidArgument("RPC request is nil"), nil)
	}
	call.service, call.method = request.serviceAndMethod()
	r.recordStarted(RPCStarted{Service: call.service, Method: call.method, RequestBodyLen: call.requestBodyLen}, request)

	ctx, cancel, err := r.prepareRequest(ctx, request, lease)
	if err != nil {
		cancel()
		lease.release()
		return call.failedStatus(r.wireStatus(ctx, err, ServerDiagnosticInternalError, request), err)
	}

	limits := streamLimitsFromOptions(r.options)
	requests := requestBody
	requestBody = limitByteStream(ctx, requestBody, limits, "request", lease, request)
	route, ok := r.routes[request.Method]
	if !ok || route.streaming == nil {
		cancel()
		lease.release()
		return call.failedStatus(Unimplemented(fmt.Sprintf("unknown streaming RPC method %s", request.Method)), nil)
	}
	if route.kind != request.rpcKind() {
		cancel()
		lease.release()
		return call.failedStatus(InvalidArgument(fmt.Sprintf("streaming RPC kind mismatch for %s: expected %d, got %d", request.Method, route.kind, request.rpcKind())), nil)
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
		return call.failedStatus(refusedStreamCompression(), nil)
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
			return call.failedStatus(r.wireStatus(ctx, err, ServerDiagnosticInternalError, request), err)
		}
	}

	responses, err := invokeStreamingHandler(ctx, r.interceptStreaming(route.streaming, request), body, requestBody, handlerNeedsDeadlineRace(ctx), lease)
	if err != nil {
		cancel()
		lease.release()
		return call.failedStatus(r.wireStatus(ctx, err, ServerDiagnosticInternalError, request), err)
	}
	if responses == nil {
		cancel()
		lease.release()
		err := errors.New("handler returned nil response stream")
		r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Context: diagnosticContext(ctx, request), Service: call.service, Method: call.method, Kind: request.rpcKind(), Err: err})
		return call.failedStatus(internalServerStatus(), err)
	}
	responses = closeStreamOnContext(ctx, responses)
	metadata := responseMetadataFromContext(ctx)
	stream := &serverResponseStream{callRecord: call, responses: responses, headers: metadata.takeHeaders(), answerDispatched: dispatched, metadata: metadata, flow: responseFlow(request, route), lease: lease, ctx: ctx, limits: limits, cancel: cancel}
	if isNonBlockingStream(responses) {
		// Such a stream is read where it is written, and drained in batches.
		stream.receiver = newStreamReceiver(ctx, responses, limits.idleTimeout, "response", lease)
	} else {
		stream.readAhead = newResponseReadAhead(ctx, responses, limits.idleTimeout, lease)
	}
	return stream
}

// failedStatus is the response of a streaming call that failed with status,
// and with err if it has one, before it had a response.
func (c callRecord) failedStatus(status *Status, err error) *statusResponse {
	return &statusResponse{status: status, cause: failureCause(err, status), call: c}
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
) (encodedStream, error) {
	var result streamingHandlerResult
	if raceContext {
		result = raceStreamingHandler(ctx, handler, body, requestBody, lease)
	} else {
		result = callStreamingHandler(ctx, handler, body, requestBody)
	}
	if err := afterHandler(ctx, result.err); err != nil {
		discardMessageStream(result.responses)
		return nil, err
	}
	return result.responses, nil
}

// callStreamingHandler runs handler on the calling goroutine.
func callStreamingHandler(ctx context.Context, handler streamingHandler, body []byte, requestBody byteStream) (result streamingHandlerResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = streamingHandlerResult{err: &serverPanicError{phase: ServerDiagnosticHandlerPanic, recovered: recovered, stack: debug.Stack()}}
		}
	}()
	result.responses, result.err = handler(ctx, body, requestBody)
	return result
}

// raceStreamingHandler runs handler on a goroutine of its own and stops
// waiting for it when ctx ends.
func raceStreamingHandler(ctx context.Context, handler streamingHandler, body []byte, requestBody byteStream, lease *requestExecutionLease) streamingHandlerResult {
	if err := ctx.Err(); err != nil {
		return streamingHandlerResult{err: err}
	}

	result := make(chan streamingHandlerResult)
	abandoned := make(chan struct{})
	execution := lease.retain(ServerDiagnosticHandlerPanic)
	// A goroutine of its own, not a pooled worker: on a worker, 64 concurrent
	// client-streaming calls ran 7% slower, while a streaming handler is
	// long-lived enough that a fresh stack costs it little.
	go func() {
		defer execution.finish()
		resultValue := callStreamingHandler(ctx, handler, body, requestBody)
		select {
		case result <- resultValue:
		case <-abandoned:
			discardMessageStream(resultValue.responses)
		}
	}()

	select {
	case result := <-result:
		return result
	case <-ctx.Done():
		execution.detach()
		close(abandoned)
		return streamingHandlerResult{err: ctx.Err()}
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

// resolveRoute returns the route registered for request, the zero route when
// there is none, and records the route's method descriptor on request, which
// the call's HandlerContext reports. It runs before the call's handler context
// is built, so the authorizer, the interceptors and the handler all see the
// descriptor, and the lookup is the one the call needs anyway.
func (r *serverRuntime) resolveRoute(request *wireRequest) route {
	route := r.routes[request.Method]
	request.methodDescriptor = route.method
	return route
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
		// Served from a stream, the call resolved its route before building
		// its handler context.
		r.resolveRoute(request)
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
		next, err := invokeAuthorizer(ctx, r.authorizer, info, handlerNeedsDeadlineRace(ctx), lease)
		if err == nil && next != ctx {
			if state, _ := ctx.Value(handlerContextKey{}).(*callState); !carriesCall(next, state) {
				err = foreignContextStatus("authorizer")
			}
		}
		if err != nil {
			cancel()
			return ctx, func() {}, err
		}
		ctx = next
	}
	return ctx, cancel, nil
}

// authorizerResult is what an authorizer returned.
type authorizerResult struct {
	ctx context.Context
	err error
}

// invokeAuthorizer runs authorizer on ctx and returns the context the call
// continues with.
func invokeAuthorizer(ctx context.Context, authorizer Authorizer, info HandlerContext, raceContext bool, lease *requestExecutionLease) (context.Context, error) {
	invoke := func() (result authorizerResult) {
		defer func() {
			if recovered := recover(); recovered != nil {
				result = authorizerResult{err: &serverPanicError{phase: ServerDiagnosticAuthorizerPanic, recovered: recovered, stack: debug.Stack()}}
			}
		}()
		result.ctx, result.err = authorizer.Authorize(ctx, info)
		return result
	}
	if !raceContext {
		result := invoke()
		return result.ctx, result.err
	}
	if ctx.Err() != nil {
		return ctx, contextStatus(ctx)
	}
	results := make(chan authorizerResult, 1)
	execution := lease.retain(ServerDiagnosticAuthorizerPanic)
	lease.goRun(func() { defer execution.finish(); results <- invoke() })
	select {
	case result := <-results:
		if ctx.Err() != nil {
			return ctx, contextStatus(ctx)
		}
		return result.ctx, result.err
	case <-ctx.Done():
		execution.detach()
		return ctx, contextStatus(ctx)
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

// failureCause returns err, the error a call ended with, or else status, the
// STATUS it ends with, for a failure the server decided on itself, such as an
// unknown method. It is nil for a call that succeeded.
func failureCause(err error, status *Status) error {
	if err != nil {
		return err
	}
	if status != nil && status.Code != CodeOK {
		return status
	}
	return nil
}

func (r *serverRuntime) recordRejectedRequest(startedAt time.Time, request *wireRequest, requestBodyLen int, status *Status) {
	r.recordRequestFailure(startedAt, request, requestBodyLen, status, nil)
}

// recordRequestFailure records the start and the end of a call that failed
// with status, and err if it has one, before reaching a handler.
func (r *serverRuntime) recordRequestFailure(startedAt time.Time, request *wireRequest, requestBodyLen int, status *Status, err error) {
	r.recordStarted(RPCStarted{Service: requestService(request), Method: requestMethod(request), RequestBodyLen: requestBodyLen}, request)
	call := callRecord{runtime: r, request: request, service: requestService(request), method: requestMethod(request), requestBodyLen: requestBodyLen, startedAt: startedAt}
	call.finish(callOutcome{code: status.Code, err: failureCause(err, status)})
}

// failRequest is recordRequestFailure for a call that failed with err, whose
// STATUS wireStatus decides and failRequest returns.
func (r *serverRuntime) failRequest(ctx context.Context, startedAt time.Time, request *wireRequest, requestBodyLen int, err error) *Status {
	status := r.wireStatus(ctx, err, ServerDiagnosticInternalError, request)
	r.recordRequestFailure(startedAt, request, requestBodyLen, status, err)
	return status
}

func (r *serverRuntime) recordPreHandlerFailure(startedAt time.Time, status *Status) {
	r.recordStarted(RPCStarted{}, nil)
	call := callRecord{runtime: r, startedAt: startedAt}
	call.finish(callOutcome{code: status.Code, err: status})
}

func (r *serverRuntime) recordStarted(event RPCStarted, request *wireRequest) {
	event.Registered = r.registered(request)
	defer func() {
		if recovered := recover(); recovered != nil {
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticMetricsPanic, Context: interceptedContext(request), Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Panic: recovered, Stack: debug.Stack()})
		}
	}()
	r.metrics.RPCStarted(event)
}

func (r *serverRuntime) recordFinished(event RPCFinished, request *wireRequest) {
	event.Registered = r.registered(request)
	if request != nil && request.intercepted != nil {
		request.intercepted.finish(r, request, event)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticMetricsPanic, Context: interceptedContext(request), Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Panic: recovered, Stack: debug.Stack()})
		}
	}()
	r.metrics.RPCFinished(event)
}

// registered reports whether request names a method registered on the
// server, whatever its kind.
func (r *serverRuntime) registered(request *wireRequest) bool {
	if request == nil {
		return false
	}
	_, ok := r.routes[request.Method]
	return ok
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
	// mu guards done, err and stopCancelRead: Close may run on another
	// goroutine than Recv, such as the one that closes the stream when ctx ends.
	mu             sync.Mutex
	stopCancelRead func()
	done           bool
	// err is how the stream ended, if it did before it was closed: every Recv
	// after that returns it, so a failure is not mistaken for a clean end.
	err error
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
	if done, err := s.ended(); done {
		return nil, err
	}

	body, err := s.receiver.recv()
	if err != nil {
		s.fail(err)
		return nil, err
	}

	if err := checkStreamLimits(s.direction, s.limits, &s.messages, &s.bodySize, len(body)); err != nil {
		s.fail(err)
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

// ended reports whether the stream ended, and the error Recv returns from then
// on: how it ended, or io.EOF for a stream that was closed.
func (s *limitedByteStream) ended() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.done {
		return false, nil
	}
	if s.err != nil {
		return true, s.err
	}
	return true, io.EOF
}

// fail ends the stream with err, which later calls of Recv return.
func (s *limitedByteStream) fail(err error) {
	s.mu.Lock()
	if !s.done {
		s.err = err
	}
	s.mu.Unlock()
	s.finish()
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
	// callRecord reports the end of the call once its response was written.
	callRecord
	responses encodedStream
	// headers go out before the first message, and metadata is where the
	// trailing metadata of the final OK status comes from.
	headers     Metadata
	metadata    *responseMetadata
	lease       *requestExecutionLease
	ctx         context.Context
	limits      streamLimits
	messages    int
	headersSent bool
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
	// status is the terminal STATUS, once the response ends, and err the error
	// it ends with, if it is a failure the response stream returned.
	status *Status
	err    error
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
//
// The end of the call is reported once the write that ends the response
// returned, or once a write failed.
func (s *serverResponseStream) writeNext(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
	done, err := s.write(ctx, writer, maxFrameSize, beforeStatus)
	if err != nil {
		// The response ends here, without the STATUS it was to have. This
		// reports nothing if the failed write was the final one.
		outcome := writeFailure(ctx, err)
		s.release()
		s.finish(outcome)
	}
	return done, err
}

func (s *serverResponseStream) write(ctx context.Context, writer io.Writer, maxFrameSize int, beforeStatus func()) (bool, error) {
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
			status, cause := s.failureStatus(err)
			return s.writeFinalFrames(ctx, writer, maxFrameSize, nil, nil, status, cause, beforeStatus)
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
	var finalErr error
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
			final, finalErr = item.status, item.err
			break
		}
		if item.message.size > maxFrameSize {
			// The messages before it are still sent, and then the call ends
			// with the failure.
			final, finalErr = s.failureStatus(&FrameTooLargeError{Len: item.message.size, Max: maxFrameSize})
			break
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
		return s.writeFinalFrames(ctx, writer, maxFrameSize, headers, batch, final, finalErr, beforeStatus)
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

// failureStatus returns the STATUS a call ends with when err stopped its
// response, and the error to report for it. A response that does not fit the
// frame limit is ResourceExhausted, whatever it was that did not fit; any
// other error is what wireStatus makes of it.
func (s *serverResponseStream) failureStatus(err error) (*Status, error) {
	if tooLarge, ok := errors.AsType[*FrameTooLargeError](err); ok {
		return s.oversizedResponseStatus(s.ctx, tooLarge), tooLarge
	}
	return s.runtime.wireStatus(s.ctx, err, ServerDiagnosticInternalError, s.request), err
}

// writeFinalFrames writes headers and messages followed by the terminal STATUS
// and ends the response, which ended with cause if it failed. The response
// releases what it holds, such as its lease, before the write, so that a
// client that sees the call complete finds them free, and reports the end of
// the call after it, so that a STATUS that did not go out is not reported as
// one that did.
func (s *serverResponseStream) writeFinalFrames(ctx context.Context, writer io.Writer, maxFrameSize int, headers []byte, messages []encodedMessage, status *Status, cause error, beforeStatus func()) (bool, error) {
	s.release()
	err := s.writeJoined(writer, headers, messages, status, maxFrameSize, beforeStatus)
	s.finishWritten(ctx, status, cause, err)
	return true, err
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

// Close ends a response that was not written to its end, such as after a
// failed write. The call is reported as the failure that is: the response
// closed while its call was live, or the call's own end.
func (s *serverResponseStream) Close() error {
	var outcome callOutcome
	if !s.reported {
		// Before release cancels the call's context.
		outcome = writeFailure(s.ctx, errResponseClosed)
	}
	s.release()
	s.finish(outcome)
	s.buffer = nil

	return nil
}

// errResponseClosed is the error of a response that was closed before it
// finished while its call was still live, such as after a failed write.
var errResponseClosed = errors.New("response closed before it finished")

// release ends the response and frees what it holds: its streams, the call's
// context and the lease. It is safe to call repeatedly.
func (s *serverResponseStream) release() {
	s.finishOnce.Do(func() {
		s.done = true
		// The receiver goes first, so it can tell a call the peer or a
		// deadline ended from one that ends here.
		s.closeReceiver()
		s.cancel()
		discardMessageStream(s.responses)
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
			return responseStreamItem{status: s.runtime.wireStatus(s.ctx, waitErr, ServerDiagnosticInternalError, s.request), err: waitErr}, true
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
		return responseStreamItem{status: s.runtime.wireStatus(s.ctx, err, ServerDiagnosticInternalError, s.request), err: err}, true
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
