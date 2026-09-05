package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"slices"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

const (
	cancelledWebTransportStreamCode  webtransport.StreamErrorCode  = 1
	cancelledWebTransportSessionCode webtransport.SessionErrorCode = 1
)

// RawWebTransportDialOptions configures an advanced single-session WebTransport client.
// Deprecated: these options configure only the Legacy backend.
type RawWebTransportDialOptions struct {
	TLSClientConfig         *tls.Config
	QUICConfig              *quic.Config
	RequestHeader           http.Header
	ApplicationProtocols    []string
	StreamReorderingTimeout time.Duration
}

// RawWebTransportClient sends TrevRPC calls over one WebTransport session.
// Construct one through Advanced.
// Deprecated: this client is available only with the Legacy backend.
type RawWebTransportClient struct {
	session *webtransport.Session
	client  *transportStreamClient
}

var _ ClientTransport = (*RawWebTransportClient)(nil)

func dialRawWebTransport(ctx context.Context, url string, options RawWebTransportDialOptions) (*RawWebTransportClient, error) {
	dialer := &webtransport.Transport{
		TLSClientConfig:         options.TLSClientConfig,
		QUICConfig:              options.QUICConfig,
		ApplicationProtocols:    options.ApplicationProtocols,
		StreamReorderingTimeout: options.StreamReorderingTimeout,
		// webtransport-go otherwise defaults to DialAddrEarly and permits 0-RTT.
		DialAddr: quic.DialAddr,
	}
	defer dialer.Close()

	_, session, err := dialer.Dial(ctx, url, options.RequestHeader.Clone())
	if err != nil {
		return nil, webTransportStatus(err)
	}

	return newRawWebTransportClient(session), nil
}

func newRawWebTransportClient(session *webtransport.Session) *RawWebTransportClient {
	return newRawWebTransportClientForEndpoint(
		session,
		newLegacyWebTransportSession(session, TransportBackendAuto),
	)
}

func newRawWebTransportClientForEndpoint(
	session *webtransport.Session,
	endpoint *legacyWebTransportSession,
) *RawWebTransportClient {
	return &RawWebTransportClient{
		session: session,
		client: newTransportStreamClient(
			endpoint,
			DefaultMaxFrameSize,
			webTransportOrContextStatus,
		),
	}
}

// WithMaxFrameSize sets the maximum TrevRPC frame size for the client.
func (t *RawWebTransportClient) WithMaxFrameSize(maxFrameSize int) *RawWebTransportClient {
	t.client.maxFrameSize = maxFrameSize
	return t
}

// Session returns the underlying WebTransport session.
func (t *RawWebTransportClient) Session() *webtransport.Session {
	return t.session
}

// Close closes the underlying WebTransport session.
func (t *RawWebTransportClient) Close() error {
	if t == nil || t.session == nil {
		return nil
	}
	return t.session.CloseWithError(cancelledWebTransportSessionCode, "client closed")
}

// Call sends a unary RPC request over WebTransport and returns its response.
func (t *RawWebTransportClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return t.client.Call(ctx, request)
}

// StreamingCall sends a streaming RPC request over WebTransport and returns response frames.
func (t *RawWebTransportClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	return t.client.StreamingCall(ctx, request, requestBody)
}

type webTransportChannelConnector struct {
	url                     string
	tlsConfig               *tls.Config
	quicConfig              *quic.Config
	requestHeader           http.Header
	applicationProtocols    []string
	streamReorderingTimeout time.Duration
	maxFrameSize            int
	requestedBackend        TransportBackend
}

func newWebTransportChannelConnector(url string, options DialOptions) (*webTransportChannelConnector, error) {
	backend, err := resolveTransportBackend(options.Backend)
	if err != nil {
		return nil, err
	}
	if backend == TransportBackendNative {
		if options.TLSConfig != nil || options.QUICConfig != nil || len(options.WebTransport.RequestHeader) != 0 {
			return nil, InvalidArgument("native WebTransport dial does not accept legacy TLSConfig, QUICConfig, or RequestHeader")
		}
		return nil, nativeBackendUnavailable()
	}
	tlsConfig, err := legacyClientTLSConfig(options.TLSConfig, cloneTransportCredentials(options.Credentials))
	if err != nil {
		return nil, err
	}
	requestHeader, err := legacyRequestHeaders(options.WebTransport.RequestHeaders, options.WebTransport.RequestHeader)
	if err != nil {
		return nil, err
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	tlsConfig.NextProtos = []string{http3.NextProtoH3}
	if tlsConfig.ClientSessionCache == nil {
		tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(defaultChannelSessionCache)
	}
	quicConfig := WebTransportQUICClientConfig(maxFrameSize, options.QUICConfig)
	applyDefaultQUICTransportConfig(quicConfig, options.Transport)
	applyQUICTransportLimits(quicConfig, options.Limits)
	if quicConfig.TokenStore == nil {
		quicConfig.TokenStore = quic.NewLRUTokenStore(defaultChannelTokenOrigins, defaultChannelTokensPerOrigin)
	}

	return &webTransportChannelConnector{
		url:                     url,
		tlsConfig:               tlsConfig,
		quicConfig:              quicConfig,
		requestHeader:           requestHeader,
		applicationProtocols:    slices.Clone(options.WebTransport.ApplicationProtocols),
		streamReorderingTimeout: options.WebTransport.StreamReorderingTimeout,
		maxFrameSize:            maxFrameSize,
		requestedBackend:        options.Backend,
	}, nil
}

func (c *webTransportChannelConnector) Connect(ctx context.Context) (channelGeneration, error) {
	dialer := &webtransport.Transport{
		TLSClientConfig:         c.tlsConfig,
		QUICConfig:              c.quicConfig,
		ApplicationProtocols:    c.applicationProtocols,
		StreamReorderingTimeout: c.streamReorderingTimeout,
		// webtransport-go otherwise defaults to DialAddrEarly and permits 0-RTT.
		DialAddr: quic.DialAddr,
	}
	defer dialer.Close()
	_, session, err := dialer.Dial(ctx, c.url, c.requestHeader.Clone())
	if err != nil {
		return nil, webTransportOrContextStatus(ctx, err)
	}
	endpoint := newLegacyWebTransportSession(session, c.requestedBackend)
	return &webTransportGeneration{
		client: newRawWebTransportClientForEndpoint(session, endpoint).
			WithMaxFrameSize(c.maxFrameSize),
		endpoint: endpoint,
	}, nil
}

type webTransportGeneration struct {
	client   *RawWebTransportClient
	endpoint *legacyWebTransportSession
}

func (g *webTransportGeneration) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return g.client.Call(ctx, request)
}

func (g *webTransportGeneration) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	return g.client.StreamingCall(ctx, request, requestBody)
}

func (g *webTransportGeneration) Close() error {
	return g.client.Close()
}

func (g *webTransportGeneration) Done() <-chan struct{} {
	return g.endpoint.Done()
}

func (g *webTransportGeneration) Err() error {
	return g.endpoint.Err()
}

func (g *webTransportGeneration) Info() ConnectionInfo {
	return g.endpoint.Info()
}

func (g *webTransportGeneration) CloseReason(err error) TransportCloseReason {
	return webTransportCloseReason(err)
}

func (g *webTransportGeneration) RawWebTransportSession() *webtransport.Session {
	return g.endpoint.session
}

func (r *serverRuntime) webTransportAdmitted(
	request *http.Request,
) (bool, error) {
	snapshot := cloneAdmissionRequest(request)
	return r.invokeWebTransportAdmission(WebTransportAdmissionRequest{
		Request:   snapshot,
		Headers:   headerFieldsFromHTTP(snapshot.Header),
		Path:      snapshot.URL.Path,
		Authority: snapshot.Host,
		Origin:    snapshot.Header.Get("Origin"),
		Secure:    snapshot.TLS != nil,
	})
}

func (r *serverRuntime) transportWebTransportAdmitted(
	request transportinternal.WebTransportRequest,
) (bool, error) {
	admission := WebTransportAdmissionRequest{
		Headers:   request.Headers().Clone(),
		Path:      request.Path(),
		Authority: request.Authority(),
		Origin:    request.Origin(),
		Secure:    request.Secure(),
	}
	if legacy, ok := request.(interface{ legacyRequest() *http.Request }); ok {
		admission.Request = cloneAdmissionRequest(legacy.legacyRequest())
	}
	return r.invokeWebTransportAdmission(admission)
}

func (r *serverRuntime) invokeWebTransportAdmission(
	request WebTransportAdmissionRequest,
) (admitted bool, err error) {
	callback := r.options.WebTransportAdmission
	if callback == nil {
		return false, nil
	}
	if !tryAcquire(r.admissionLimit) {
		return false, errAdmissionSaturated
	}
	defer release(r.admissionLimit)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &serverPanicError{
				phase:     ServerDiagnosticAdmissionPanic,
				recovered: recovered,
				stack:     debug.Stack(),
			}
			r.emitDiagnostic(ServerDiagnostic{
				Phase: ServerDiagnosticAdmissionPanic,
				Panic: recovered,
				Stack: debug.Stack(),
				Err:   err,
			})
		}
	}()
	return callback(request), nil
}

func handleWebTransportRequest(
	request transportinternal.WebTransportRequest,
	runtime *serverRuntime,
) (transportinternal.WebTransportSession, bool) {
	info := "path=" + request.Path() +
		" authority=" + request.Authority() +
		" origin=" + request.Origin() +
		" remote=" + request.RemoteAddress().String()
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:   ServerDiagnosticWebTransportConnect,
		Message: info,
	})
	admitted, err := runtime.transportWebTransportAdmitted(request)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:   ServerDiagnosticWebTransportAdmission,
			Message: "error",
			Err:     err,
		})
		request.WriteError("internal server error", status)
		return nil, false
	}
	if !admitted {
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:   ServerDiagnosticWebTransportAdmission,
			Message: "denied",
		})
		request.WriteError(
			"WebTransport admission denied",
			http.StatusForbidden,
		)
		return nil, false
	}
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:   ServerDiagnosticWebTransportAdmission,
		Message: "accepted",
	})

	session, err := request.Upgrade()
	if err != nil {
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase: ServerDiagnosticWebTransportUpgrade,
			Err:   err,
		})
		request.WriteError(
			"WebTransport upgrade failed",
			http.StatusBadRequest,
		)
		return nil, false
	}
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:   ServerDiagnosticWebTransportUpgradeSuccess,
		Message: "accepted",
	})
	return session, true
}

func webTransportCloseReason(err error) TransportCloseReason {
	var sessionErr *webtransport.SessionError
	if errors.As(err, &sessionErr) {
		return TransportCloseReason{
			Peer:            sessionErr.Remote,
			Local:           !sessionErr.Remote,
			Clean:           sessionErr.ErrorCode == 0,
			ApplicationCode: uint64(sessionErr.ErrorCode),
			Message:         sessionErr.Message,
			Err:             err,
		}
	}
	var streamErr *webtransport.StreamError
	if errors.As(err, &streamErr) {
		return TransportCloseReason{
			Peer:            streamErr.Remote,
			Local:           !streamErr.Remote,
			ApplicationCode: uint64(streamErr.ErrorCode),
			Err:             err,
		}
	}
	return legacyQUICCloseReason(err)
}

func describeWebTransportSessionError(err error) string {
	var sessionErr *webtransport.SessionError
	if errors.As(err, &sessionErr) {
		return fmt.Sprintf("type=session remote=%t code=%d message=%q", sessionErr.Remote, sessionErr.ErrorCode, sessionErr.Message)
	}
	var h3Err *http3.Error
	if errors.As(err, &h3Err) {
		return fmt.Sprintf("type=http3 remote=%t code=%#x message=%q", h3Err.Remote, uint64(h3Err.ErrorCode), h3Err.ErrorMessage)
	}
	var wtStreamErr *webtransport.StreamError
	if errors.As(err, &wtStreamErr) {
		return fmt.Sprintf(
			"type=webtransport_stream remote=%t code=%d",
			wtStreamErr.Remote,
			wtStreamErr.ErrorCode,
		)
	}
	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		return fmt.Sprintf("type=quic_stream remote=%t stream=%d code=%d", streamErr.Remote, streamErr.StreamID, streamErr.ErrorCode)
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return fmt.Sprintf("type=quic_application remote=%t code=%d message=%q", appErr.Remote, appErr.ErrorCode, appErr.ErrorMessage)
	}
	var transportErr *quic.TransportError
	if errors.As(err, &transportErr) {
		return fmt.Sprintf(
			"type=quic_transport remote=%t code=%d message=%q",
			transportErr.Remote,
			transportErr.ErrorCode,
			transportErr.ErrorMessage,
		)
	}
	return fmt.Sprintf("type=%T error=%q", err, err.Error())
}

func handleWebTransportSession(
	ctx context.Context,
	session transportinternal.WebTransportSession,
	server *Server,
	requestLimit semaphore,
) {
	runtime := server.freeze()
	handleTransportStreamEndpoint(
		ctx,
		session,
		server,
		requestLimit,
		transportStreamEndpointOptions{
			overloadMessage:          "too many concurrent streams on WebTransport session",
			drainTimeoutMessage:      "server WebTransport stream drain timed out",
			shutdownMessage:          "server shutdown",
			closeImmediatelyOnCancel: true,
			onAcceptError: func(err error) {
				reason := webTransportCloseReason(err)
				if ctx.Err() != nil {
					reason = TransportCloseReason{
						Local:   true,
						Clean:   true,
						Message: "server shutdown",
						Err:     err,
					}
				}
				runtime.emitDiagnostic(ServerDiagnostic{
					Phase:       ServerDiagnosticWebTransportSessionClosed,
					Message:     describeWebTransportSessionError(err),
					Err:         err,
					Connection:  session.Info(),
					CloseReason: reason,
				})
			},
		},
	)
}

func contextWithAdditionalCancel(ctx context.Context, cancelOn context.Context) (context.Context, context.CancelFunc) {
	if cancelOn.Done() == nil {
		return ctx, func() {}
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-cancelOn.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}

func webTransportOrContextStatus(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}

	return webTransportStatus(err)
}

func webTransportStatus(err error) error {
	if err == nil {
		return nil
	}

	var streamError *webtransport.StreamError
	if errors.As(err, &streamError) {
		return Cancelled(err.Error())
	}

	var sessionError *webtransport.SessionError
	if errors.As(err, &sessionError) {
		if sessionError.Remote {
			return Unavailable("transport unavailable: " + err.Error())
		}

		return Cancelled("transport closed locally")
	}

	var requirementsError *webtransport.RequirementsNotMetError
	if errors.As(err, &requirementsError) {
		return Unavailable("transport unavailable: " + err.Error())
	}

	return transportStatus(err)
}
