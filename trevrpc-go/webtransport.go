package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
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

type WebTransportDialOptions struct {
	TLSClientConfig         *tls.Config
	QUICConfig              *quic.Config
	RequestHeader           http.Header
	ApplicationProtocols    []string
	StreamReorderingTimeout time.Duration
}

type WebTransportClient struct {
	session      *webtransport.Session
	maxFrameSize int
}

func DialWebTransport(ctx context.Context, url string, options WebTransportDialOptions) (*WebTransportClient, error) {
	dialer := &webtransport.Dialer{
		TLSClientConfig:         options.TLSClientConfig,
		QUICConfig:              options.QUICConfig,
		ApplicationProtocols:    options.ApplicationProtocols,
		StreamReorderingTimeout: options.StreamReorderingTimeout,
	}

	_, session, err := dialer.Dial(ctx, url, options.RequestHeader)
	if err != nil {
		return nil, webTransportStatus(err)
	}

	return NewWebTransportClient(session), nil
}

func NewWebTransportClient(session *webtransport.Session) *WebTransportClient {
	return &WebTransportClient{session: session, maxFrameSize: DefaultMaxFrameSize}
}

func (t *WebTransportClient) WithMaxFrameSize(maxFrameSize int) *WebTransportClient {
	t.maxFrameSize = maxFrameSize
	return t
}

func (t *WebTransportClient) Session() *webtransport.Session {
	return t.session
}

func (t *WebTransportClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
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

func (t *WebTransportClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := t.session.OpenStreamSync(streamCtx)
	if err != nil {
		cancel()
		return nil, webTransportOrContextStatus(streamCtx, err)
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writeWebTransportStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()

	return &webTransportResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, maxFrameSize: t.maxFrameSize}, nil
}

type webTransportResponseStream struct {
	stream       *webtransport.Stream
	writerDone   <-chan error
	cancel       context.CancelFunc
	maxFrameSize int
	done         bool
}

func (s *webTransportResponseStream) Recv() (*RpcStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	frame := &RpcStreamFrame{}
	read, err := ReadFrameOrEOF(s.stream, frame, s.maxFrameSize)
	if err != nil {
		s.finish(false)
		return nil, webTransportStatus(err)
	}

	if !read {
		s.finish(false)
		return nil, io.EOF
	}

	if frame.Kind == RpcStreamFrameKindStatus {
		s.finish(false)
		if err := s.writerError(true); err != nil {
			return nil, err
		}
	}

	return frame, nil
}

func (s *webTransportResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *webTransportResponseStream) finish(cancelRead bool) {
	if s.cancel != nil {
		s.cancel()
	}

	if s.done {
		return
	}

	s.done = true
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

func cancelWebTransportStreamOnContext(ctx context.Context, stream *webtransport.Stream) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stream.CancelRead(cancelledWebTransportStreamCode)
			stream.CancelWrite(cancelledWebTransportStreamCode)
		case <-done:
		}
	}()

	return func() { close(done) }
}

func writeWebTransportStreamingRequest(ctx context.Context, stream *webtransport.Stream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	defer closeMessageStream(requestBody)

	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		stream.CancelWrite(cancelledWebTransportStreamCode)
		return webTransportStatus(err)
	}

	for {
		body, err := recvRequestBody(ctx, requestBody)
		if err == io.EOF {
			break
		}

		if err != nil {
			stream.CancelWrite(cancelledWebTransportStreamCode)
			return webTransportStatus(err)
		}

		if err := WriteFrame(stream, MessageFrame(body), maxFrameSize); err != nil {
			stream.CancelWrite(cancelledWebTransportStreamCode)
			return webTransportStatus(err)
		}
	}

	return webTransportStatus(stream.Close())
}

func isWebTransportQUICConnection(conn *quic.Conn, options ServerOptions) bool {
	return options.EnableWebTransport && conn.ConnectionState().TLS.NegotiatedProtocol == http3.NextProtoH3
}

func handleWebTransportConnection(ctx context.Context, conn *quic.Conn, server *Server, requestLimit semaphore, closeOnShutdown bool) {
	sessionsCtx, stopSessions := context.WithCancel(ctx)
	defer stopSessions()

	var sessionTasks sync.WaitGroup
	var wtServer *webtransport.Server
	wtServer = &webtransport.Server{
		CheckOrigin: server.options.WebTransportCheckOrigin,
		H3: &http3.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != server.options.WebTransportPath {
					http.NotFound(w, r)
					return
				}

				if server.options.WebTransportCheckOrigin == nil {
					http.Error(w, "WebTransport origin policy is not configured", http.StatusForbidden)
					return
				}

				session, err := wtServer.Upgrade(w, r)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				sessionTasks.Go(func() {
					handleWebTransportSession(sessionsCtx, session, server, requestLimit)
				})
			}),
		},
	}
	webtransport.ConfigureHTTP3Server(wtServer.H3)

	go func() {
		<-ctx.Done()
		_ = wtServer.Close()
	}()

	_ = wtServer.ServeQUICConn(conn)
	stopSessions()
	waitForWaitGroup(&sessionTasks, server.options.GracefulShutdownTimeout, func() {
		conn.CloseWithError(0, "server WebTransport session drain timed out")
	})

	if closeOnShutdown && ctx.Err() != nil {
		conn.CloseWithError(0, "server drained WebTransport connection")
	}
}

func handleWebTransportSession(ctx context.Context, session *webtransport.Session, server *Server, requestLimit semaphore) {
	streamLimit := newSemaphore(server.options.MaxConcurrentStreamsPerConnection)
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
			break
		}

		if !tryAcquire(streamLimit) {
			writeStatusResponse(stream, Unavailable("too many concurrent streams on WebTransport session"), server.options.MaxFrameSize)
			continue
		}

		streamTasks.Go(func() {
			defer release(streamLimit)
			streamCtx, cancel := contextWithAdditionalCancel(stream.Context(), ctx)
			defer cancel()
			handleRPCStream(streamCtx, server, requestLimit, stream)
		})
	}

	waitForWaitGroup(&streamTasks, server.options.GracefulShutdownTimeout, func() {
		_ = session.CloseWithError(cancelledWebTransportSessionCode, "server WebTransport stream drain timed out")
	})
}

func contextWithAdditionalCancel(ctx context.Context, cancelOn context.Context) (context.Context, context.CancelFunc) {
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
