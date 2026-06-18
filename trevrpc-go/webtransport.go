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

type WebTransportTransport struct {
	session      *webtransport.Session
	maxFrameSize int
}

func DialWebTransport(ctx context.Context, url string, options WebTransportDialOptions) (*WebTransportTransport, error) {
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

	return NewWebTransportTransport(session), nil
}

func NewWebTransportTransport(session *webtransport.Session) *WebTransportTransport {
	return &WebTransportTransport{session: session, maxFrameSize: DefaultMaxFrameSize}
}

func (t *WebTransportTransport) WithMaxFrameSize(maxFrameSize int) *WebTransportTransport {
	t.maxFrameSize = maxFrameSize
	return t
}

func (t *WebTransportTransport) Session() *webtransport.Session {
	return t.session
}

func (t *WebTransportTransport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	stream, err := t.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, webTransportStatus(err)
	}
	defer stream.CancelRead(cancelledWebTransportStreamCode)

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		stream.CancelWrite(cancelledWebTransportStreamCode)
		return nil, webTransportStatus(err)
	}

	if err := stream.Close(); err != nil {
		return nil, webTransportStatus(err)
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, webTransportStatus(err)
	}

	return response, nil
}

func (t *WebTransportTransport) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := t.session.OpenStreamSync(streamCtx)
	if err != nil {
		cancel()
		return nil, webTransportStatus(err)
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
	}

	return frame, nil
}

func (s *webTransportResponseStream) Close() error {
	s.finish(true)
	return nil
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

func writeWebTransportStreamingRequest(ctx context.Context, stream *webtransport.Stream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
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

		if !tryAcquire(requestLimit) {
			release(streamLimit)
			writeStatusResponse(stream, Unavailable("too many concurrent RPCs"), server.options.MaxFrameSize)
			continue
		}

		streamTasks.Go(func() {
			defer release(streamLimit)
			defer release(requestLimit)
			handleRPCStream(ctx, server, stream)
		})
	}

	waitForWaitGroup(&streamTasks, server.options.GracefulShutdownTimeout, func() {
		_ = session.CloseWithError(cancelledWebTransportSessionCode, "server WebTransport stream drain timed out")
	})
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
