package trevrpc

import (
	"context"
	"errors"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

var cancelledTransportReason = TransportCloseReason{
	Local:           true,
	ApplicationCode: 1,
	Message:         "RPC cancelled",
}

type legacyQUICListener struct {
	listener         *quic.Listener
	requestedBackend TransportBackend
}

var _ transportinternal.Listener = (*legacyQUICListener)(nil)

func (l *legacyQUICListener) Accept(ctx context.Context) (transportinternal.Connection, error) {
	conn, err := l.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return newLegacyQUICConnection(conn, l.requestedBackend), nil
}

func (l *legacyQUICListener) Address() TransportAddress {
	return transportinternal.AddressFromNet(l.listener.Addr())
}

func (l *legacyQUICListener) Close() error {
	return l.listener.Close()
}

type legacyQUICConnection struct {
	conn             *quic.Conn
	requestedBackend TransportBackend
}

var _ transportinternal.Connection = (*legacyQUICConnection)(nil)

func newLegacyQUICConnection(conn *quic.Conn, requested TransportBackend) *legacyQUICConnection {
	return &legacyQUICConnection{conn: conn, requestedBackend: requested}
}

func (c *legacyQUICConnection) OpenStream(ctx context.Context) (transportinternal.BidirectionalStream, error) {
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &legacyQUICStream{stream: stream}, nil
}

func (c *legacyQUICConnection) AcceptStream(ctx context.Context) (transportinternal.BidirectionalStream, error) {
	stream, err := c.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &legacyQUICStream{stream: stream}, nil
}

func (c *legacyQUICConnection) Done() <-chan struct{} {
	return c.conn.Context().Done()
}

func (c *legacyQUICConnection) Err() error {
	return context.Cause(c.conn.Context())
}

func (c *legacyQUICConnection) Info() ConnectionInfo {
	negotiatedProtocol := c.conn.ConnectionState().TLS.NegotiatedProtocol
	protocol := TransportProtocolNativeQUIC
	if negotiatedProtocol == "h3" {
		protocol = TransportProtocolHTTP3
	}
	return ConnectionInfo{
		RequestedBackend:   c.requestedBackend,
		ResolvedBackend:    TransportBackendLegacy,
		Protocol:           protocol,
		LocalAddress:       transportinternal.AddressFromNet(c.conn.LocalAddr()),
		RemoteAddress:      transportinternal.AddressFromNet(c.conn.RemoteAddr()),
		NegotiatedProtocol: negotiatedProtocol,
		Provider:           "quic-go",
	}
}

func (c *legacyQUICConnection) Close(reason TransportCloseReason) error {
	return c.conn.CloseWithError(
		quic.ApplicationErrorCode(reason.ApplicationCode),
		reason.Message,
	)
}

func legacyQUICCloseReason(err error) TransportCloseReason {
	reason := TransportCloseReason{Err: err}
	var h3Err *http3.Error
	if errors.As(err, &h3Err) {
		reason.Peer = h3Err.Remote
		reason.Local = !h3Err.Remote
		reason.Clean = h3Err.ErrorCode == http3.ErrCodeNoError
		reason.ApplicationCode = uint64(h3Err.ErrorCode)
		reason.Message = h3Err.ErrorMessage
		return reason
	}
	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		reason.Peer = streamErr.Remote
		reason.Local = !streamErr.Remote
		reason.ApplicationCode = uint64(streamErr.ErrorCode)
		return reason
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		reason.Peer = appErr.Remote
		reason.Local = !appErr.Remote
		reason.Clean = appErr.ErrorCode == 0
		reason.ApplicationCode = uint64(appErr.ErrorCode)
		reason.Message = appErr.ErrorMessage
		return reason
	}
	var transportErr *quic.TransportError
	if errors.As(err, &transportErr) {
		reason.Peer = transportErr.Remote
		reason.Local = !transportErr.Remote
		reason.Clean = transportErr.ErrorCode == 0
		reason.TransportCode = uint64(transportErr.ErrorCode)
		reason.Message = transportErr.ErrorMessage
		return reason
	}
	if err != nil {
		reason.Message = err.Error()
	}
	return reason
}

type legacyQUICStream struct {
	stream *quic.Stream
}

func (s *legacyQUICStream) Read(data []byte) (int, error) {
	return s.stream.Read(data)
}

func (s *legacyQUICStream) Write(data []byte) (int, error) {
	return s.stream.Write(data)
}

func (s *legacyQUICStream) Close() error {
	return s.stream.Close()
}

func (s *legacyQUICStream) Context() context.Context {
	return s.stream.Context()
}

func (s *legacyQUICStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

func (s *legacyQUICStream) CancelRead(reason TransportCloseReason) {
	s.stream.CancelRead(quic.StreamErrorCode(reason.ApplicationCode))
}

func (s *legacyQUICStream) CancelWrite(reason TransportCloseReason) {
	s.stream.CancelWrite(quic.StreamErrorCode(reason.ApplicationCode))
}

func (s *legacyQUICStream) trevrpcCancelRead() {
	s.CancelRead(cancelledTransportReason)
}

func (s *legacyQUICStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return cancelTransportStreamOnContext(ctx, s)
}

type legacyWebTransportSession struct {
	session          *webtransport.Session
	requestedBackend TransportBackend
}

var _ transportinternal.WebTransportSession = (*legacyWebTransportSession)(nil)

func newLegacyWebTransportSession(
	session *webtransport.Session,
	requested TransportBackend,
) *legacyWebTransportSession {
	return &legacyWebTransportSession{session: session, requestedBackend: requested}
}

func (s *legacyWebTransportSession) OpenStream(ctx context.Context) (transportinternal.BidirectionalStream, error) {
	stream, err := s.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &legacyWebTransportStream{stream: stream}, nil
}

func (s *legacyWebTransportSession) AcceptStream(ctx context.Context) (transportinternal.BidirectionalStream, error) {
	stream, err := s.session.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &legacyWebTransportStream{stream: stream}, nil
}

func (s *legacyWebTransportSession) Done() <-chan struct{} {
	return s.session.Context().Done()
}

func (s *legacyWebTransportSession) Err() error {
	cause := context.Cause(s.session.Context())
	if cause == nil {
		return nil
	}

	// webtransport-go retains the detailed SessionError privately, but every
	// stream-open path returns it after the session closes. Recover that error
	// without retaining or consuming a stream.
	stream, err := s.session.OpenStream()
	if err != nil {
		return err
	}
	_ = stream.Close()
	return cause
}

func (s *legacyWebTransportSession) Info() ConnectionInfo {
	state := s.session.SessionState()
	return ConnectionInfo{
		RequestedBackend:   s.requestedBackend,
		ResolvedBackend:    TransportBackendLegacy,
		Protocol:           TransportProtocolWebTransport,
		LocalAddress:       transportinternal.AddressFromNet(s.session.LocalAddr()),
		RemoteAddress:      transportinternal.AddressFromNet(s.session.RemoteAddr()),
		NegotiatedProtocol: state.ConnectionState.TLS.NegotiatedProtocol,
		Provider:           "webtransport-go",
	}
}

func (s *legacyWebTransportSession) Close(reason TransportCloseReason) error {
	return s.session.CloseWithError(webtransport.SessionErrorCode(reason.ApplicationCode), reason.Message)
}

type legacyWebTransportStream struct {
	stream *webtransport.Stream
}

func (s *legacyWebTransportStream) Read(data []byte) (int, error) {
	return s.stream.Read(data)
}

func (s *legacyWebTransportStream) Write(data []byte) (int, error) {
	return s.stream.Write(data)
}

func (s *legacyWebTransportStream) Close() error {
	return s.stream.Close()
}

func (s *legacyWebTransportStream) Context() context.Context {
	return s.stream.Context()
}

func (s *legacyWebTransportStream) SetReadDeadline(deadline time.Time) error {
	return s.stream.SetReadDeadline(deadline)
}

func (s *legacyWebTransportStream) CancelRead(reason TransportCloseReason) {
	s.stream.CancelRead(webtransport.StreamErrorCode(reason.ApplicationCode))
}

func (s *legacyWebTransportStream) CancelWrite(reason TransportCloseReason) {
	s.stream.CancelWrite(webtransport.StreamErrorCode(reason.ApplicationCode))
}

func (s *legacyWebTransportStream) trevrpcCancelRead() {
	s.CancelRead(cancelledTransportReason)
}

func (s *legacyWebTransportStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return cancelTransportStreamOnContext(ctx, s)
}
