package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
)

const (
	cancelledWebTransportStreamCode  webtransport.StreamErrorCode  = 1
	cancelledWebTransportSessionCode webtransport.SessionErrorCode = 1
)

// RawWebTransportDialOptions configures an advanced single-session WebTransport client.
type RawWebTransportDialOptions struct {
	TLSClientConfig         *tls.Config
	QUICConfig              *quic.Config
	RequestHeader           http.Header
	ApplicationProtocols    []string
	StreamReorderingTimeout time.Duration
}

// RawWebTransportClient sends TrevRPC calls over one WebTransport session.
// Construct one through Advanced.
type RawWebTransportClient struct {
	session      *webtransport.Session
	maxFrameSize int
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
	return &RawWebTransportClient{session: session, maxFrameSize: DefaultMaxFrameSize}
}

// WithMaxFrameSize sets the maximum TrevRPC frame size for the client.
func (t *RawWebTransportClient) WithMaxFrameSize(maxFrameSize int) *RawWebTransportClient {
	t.maxFrameSize = maxFrameSize
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
	stream, err := t.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, webTransportOrContextStatus(ctx, err)
	}
	defer stream.CancelRead(cancelledWebTransportStreamCode)
	stopCancel := cancelWebTransportStreamOnContext(ctx, stream)
	defer stopCancel()

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		stream.CancelWrite(cancelledWebTransportStreamCode)
		return nil, webTransportOrContextStatus(ctx, err)
	}

	if err := stream.Close(); err != nil {
		return nil, webTransportOrContextStatus(ctx, err)
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, webTransportOrContextStatus(ctx, err)
	}

	return response, nil
}

// StreamingCall sends a streaming RPC request over WebTransport and returns response frames.
func (t *RawWebTransportClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := t.session.OpenStreamSync(streamCtx)
	if err != nil {
		cancel()
		return nil, webTransportOrContextStatus(streamCtx, err)
	}

	writerDone := make(chan error, 1)
	stopCancel := cancelWebTransportStreamOnContext(streamCtx, stream)
	go func() {
		writerDone <- writeWebTransportStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()

	return &webTransportResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, stopCancel: stopCancel, maxFrameSize: t.maxFrameSize}, nil
}

type webTransportChannelConnector struct {
	url                     string
	tlsConfig               *tls.Config
	quicConfig              *quic.Config
	requestHeader           http.Header
	applicationProtocols    []string
	streamReorderingTimeout time.Duration
	maxFrameSize            int
}

func newWebTransportChannelConnector(url string, options DialOptions) (*webTransportChannelConnector, error) {
	if options.TLSConfig == nil {
		return nil, InvalidArgument("WebTransport dial requires TLSConfig")
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	tlsConfig := options.TLSConfig.Clone()
	tlsConfig.NextProtos = []string{http3.NextProtoH3}
	if tlsConfig.ClientSessionCache == nil {
		tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(defaultChannelSessionCache)
	}
	quicConfig := WebTransportQUICClientConfig(maxFrameSize, options.QUICConfig)
	applyDefaultQUICTransportConfig(quicConfig, options.Transport)
	if quicConfig.TokenStore == nil {
		quicConfig.TokenStore = quic.NewLRUTokenStore(defaultChannelTokenOrigins, defaultChannelTokensPerOrigin)
	}

	return &webTransportChannelConnector{
		url:                     url,
		tlsConfig:               tlsConfig,
		quicConfig:              quicConfig,
		requestHeader:           options.WebTransport.RequestHeader.Clone(),
		applicationProtocols:    slices.Clone(options.WebTransport.ApplicationProtocols),
		streamReorderingTimeout: options.WebTransport.StreamReorderingTimeout,
		maxFrameSize:            maxFrameSize,
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
	return &webTransportGeneration{
		client:  newRawWebTransportClient(session).WithMaxFrameSize(c.maxFrameSize),
		session: session,
	}, nil
}

type webTransportGeneration struct {
	client  *RawWebTransportClient
	session *webtransport.Session
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
	return g.session.Context().Done()
}

func (g *webTransportGeneration) Err() error {
	return context.Cause(g.session.Context())
}

func (g *webTransportGeneration) RawWebTransportSession() *webtransport.Session {
	return g.session
}

type webTransportResponseStream struct {
	stream       *webtransport.Stream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
}

func (s *webTransportResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *webTransportResponseStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

func (s *webTransportResponseStream) Recv() (*RpcStreamFrame, error) {
	frame, _, err := s.trevrpcRecvStreamFrameFields()
	if err != nil {
		return nil, err
	}

	return frame.rpcStreamFrame(), nil
}

func (s *webTransportResponseStream) trevrpcRecvStreamFrameFields() (streamFrameFields, func(), error) {
	if s.done {
		return streamFrameFields{}, nil, io.EOF
	}

	frame, read, err := readStreamFrameFieldsOrEOF(s.stream, s.maxFrameSize)
	if err != nil {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, webTransportStatus(err)
	}

	if !read {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, io.EOF
	}

	if frame.kind == RpcStreamFrameKindStatus {
		s.finish(false)
		if frame.statusValue().IsOK() {
			if err := s.writerError(true); err != nil {
				return streamFrameFields{}, nil, err
			}
		} else {
			s.ignoreWriterError()
		}
	}

	return frame, nil, nil
}

func (s *webTransportResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *webTransportResponseStream) finish(cancelRead bool) {
	if s.done {
		return
	}

	s.done = true
	if s.stopCancel != nil {
		s.stopCancel()
		s.stopCancel = nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	if cancelRead {
		s.stream.CancelRead(cancelledWebTransportStreamCode)
	}
	s.stream.CancelWrite(cancelledWebTransportStreamCode)
}

func (s *webTransportResponseStream) writerError(ignoreCancelled bool) error {
	if s.writerDone == nil {
		return nil
	}

	err := <-s.writerDone
	s.writerDone = nil
	if err == nil {
		return nil
	}
	if ignoreCancelled && StatusFromError(err).Code == CodeCancelled {
		return nil
	}

	return err
}

func (s *webTransportResponseStream) ignoreWriterError() {
	s.writerDone = nil
}

func cancelWebTransportStreamOnContext(ctx context.Context, stream *webtransport.Stream) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			stream.CancelRead(cancelledWebTransportStreamCode)
			stream.CancelWrite(cancelledWebTransportStreamCode)
		case <-done:
		}
	}()

	return func() { closeOnce.Do(func() { close(done) }) }
}

func writeWebTransportStreamingRequest(ctx context.Context, stream *webtransport.Stream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	requestBody = closeStreamOnContext(ctx, requestBody)
	defer closeMessageStream(requestBody)

	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledWebTransportStreamCode)
		return webTransportOrContextStatus(ctx, err)
	}

	if err := writeRequestBodyFrames(ctx, stream, requestBody, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledWebTransportStreamCode)
		return webTransportOrContextStatus(ctx, err)
	}

	if err := stream.Close(); err != nil {
		return webTransportOrContextStatus(ctx, err)
	}

	return nil
}

func (r *serverRuntime) webTransportAdmitted(request *http.Request) (admitted bool, err error) {
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
			err = &serverPanicError{phase: ServerDiagnosticAdmissionPanic, recovered: recovered, stack: debug.Stack()}
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticAdmissionPanic, Panic: recovered, Stack: debug.Stack(), Err: err})
		}
	}()
	snapshot := cloneAdmissionRequest(request)
	return callback(WebTransportAdmissionRequest{Request: snapshot, Path: snapshot.URL.Path, Authority: snapshot.Host, Origin: snapshot.Header.Get("Origin"), Secure: snapshot.TLS != nil}), nil
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
	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		return fmt.Sprintf("type=quic_stream remote=%t stream=%d code=%d", streamErr.Remote, streamErr.StreamID, streamErr.ErrorCode)
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return fmt.Sprintf("type=quic_application remote=%t code=%d message=%q", appErr.Remote, appErr.ErrorCode, appErr.ErrorMessage)
	}
	return fmt.Sprintf("type=%T error=%q", err, err.Error())
}

func handleWebTransportSession(ctx context.Context, session *webtransport.Session, server *Server, requestLimit semaphore) {
	runtime := server.freeze()
	streamLimit := newSemaphore(runtime.options.MaxConcurrentStreamsPerConnection)
	var streamTasks sync.WaitGroup

	go func() {
		select {
		case <-ctx.Done():
			_ = session.CloseWithError(cancelledWebTransportSessionCode, "server shutdown")
		case <-session.Context().Done():
		}
	}()

	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			runtime.emitDiagnostic(ServerDiagnostic{
				Phase:   ServerDiagnosticWebTransportSessionClosed,
				Message: describeWebTransportSessionError(err),
				Err:     err,
			})
			break
		}

		if !tryAcquire(streamLimit) {
			writeStatusResponse(stream, Unavailable("too many concurrent streams on WebTransport session"), runtime.options.MaxFrameSize)
			continue
		}

		streamTasks.Go(func() {
			defer release(streamLimit)
			streamCtx, cancel := contextWithAdditionalCancel(stream.Context(), ctx)
			defer cancel()
			handleRPCStream(streamCtx, server, requestLimit, webTransportRPCStream{stream: stream})
		})
	}

	waitForWaitGroup(&streamTasks, runtime.options.GracefulShutdownTimeout, func() {
		runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticShutdownIncomplete})
		_ = session.CloseWithError(cancelledWebTransportSessionCode, "server WebTransport stream drain timed out")
	})
}

type webTransportRPCStream struct {
	stream *webtransport.Stream
}

func (s webTransportRPCStream) Read(data []byte) (int, error) {
	return s.stream.Read(data)
}

func (s webTransportRPCStream) Write(data []byte) (int, error) {
	return s.stream.Write(data)
}

func (s webTransportRPCStream) Close() error {
	return s.stream.Close()
}

func (s webTransportRPCStream) SetReadDeadline(ttl time.Time) error {
	return s.stream.SetReadDeadline(ttl)
}

func (s webTransportRPCStream) trevrpcCancelRead() {
	s.stream.CancelRead(cancelledWebTransportStreamCode)
}

func (s webTransportRPCStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			s.stream.CancelRead(cancelledWebTransportStreamCode)
		case <-done:
		}
	}()

	return func() { closeOnce.Do(func() { close(done) }) }
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
