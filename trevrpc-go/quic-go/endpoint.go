package quicgo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

type quicEndpoint struct {
	conn *quic.Conn
	info trevrpc.ConnectionInfo
}

func newQUICEndpoint(conn *quic.Conn, requested trevrpc.TransportBackend) *quicEndpoint {
	negotiated := conn.ConnectionState().TLS.NegotiatedProtocol
	protocol := trevrpc.TransportProtocolNativeQUIC
	if negotiated == http3.NextProtoH3 {
		protocol = trevrpc.TransportProtocolHTTP3
	}
	return &quicEndpoint{conn: conn, info: trevrpc.ConnectionInfo{
		RequestedBackend:   requested,
		ResolvedBackend:    trevrpc.TransportBackendQUICGo,
		Protocol:           protocol,
		LocalAddress:       transportapi.AddressFromNet(conn.LocalAddr()),
		RemoteAddress:      transportapi.AddressFromNet(conn.RemoteAddr()),
		NegotiatedProtocol: negotiated,
		Provider:           "quic-go",
	}}
}

func (c *quicEndpoint) OpenStream(ctx context.Context) (transportapi.BidirectionalStream, error) {
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &quicStream{stream: stream}, nil
}
func (c *quicEndpoint) AcceptStream(ctx context.Context) (transportapi.BidirectionalStream, error) {
	stream, err := c.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &quicStream{stream: stream}, nil
}
func (c *quicEndpoint) Done() <-chan struct{}        { return c.conn.Context().Done() }
func (c *quicEndpoint) Err() error                   { return context.Cause(c.conn.Context()) }
func (c *quicEndpoint) Info() trevrpc.ConnectionInfo { return c.info }
func (c *quicEndpoint) Close(reason trevrpc.TransportCloseReason) error {
	return c.conn.CloseWithError(quic.ApplicationErrorCode(reason.ApplicationCode), reason.Message)
}
func (c *quicEndpoint) CloseReason(err error) trevrpc.TransportCloseReason { return closeReason(err) }
func (c *quicEndpoint) DescribeError(err error) string                     { return describeError(err) }

var _ transportapi.Connection = (*quicEndpoint)(nil)

// webTransportEndpoint adapts a WebTransport session to the root endpoint SPI.
type webTransportEndpoint struct {
	session *webtransport.Session
	info    trevrpc.ConnectionInfo
}

func newWebTransportEndpoint(session *webtransport.Session, requested trevrpc.TransportBackend) *webTransportEndpoint {
	state := session.SessionState()
	return &webTransportEndpoint{session: session, info: trevrpc.ConnectionInfo{
		RequestedBackend:    requested,
		ResolvedBackend:     trevrpc.TransportBackendQUICGo,
		Protocol:            trevrpc.TransportProtocolWebTransport,
		LocalAddress:        transportapi.AddressFromNet(session.LocalAddr()),
		RemoteAddress:       transportapi.AddressFromNet(session.RemoteAddr()),
		NegotiatedProtocol:  state.ConnectionState.TLS.NegotiatedProtocol,
		Provider:            "webtransport-go",
		WebTransportProfile: state.ApplicationProtocol,
	}}
}
func (s *webTransportEndpoint) OpenStream(ctx context.Context) (transportapi.BidirectionalStream, error) {
	stream, err := s.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &webTransportStream{stream: stream}, nil
}
func (s *webTransportEndpoint) AcceptStream(ctx context.Context) (transportapi.BidirectionalStream, error) {
	stream, err := s.session.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &webTransportStream{stream: stream}, nil
}
func (s *webTransportEndpoint) Done() <-chan struct{} { return s.session.Context().Done() }
func (s *webTransportEndpoint) Err() error {
	cause := context.Cause(s.session.Context())
	if cause == nil {
		return nil
	}
	stream, err := s.session.OpenStream()
	if err != nil {
		return err
	}
	_ = stream.Close()
	return cause
}
func (s *webTransportEndpoint) Info() trevrpc.ConnectionInfo { return s.info }
func (s *webTransportEndpoint) Close(reason trevrpc.TransportCloseReason) error {
	return s.session.CloseWithError(webtransport.SessionErrorCode(reason.ApplicationCode), reason.Message)
}
func (s *webTransportEndpoint) CloseReason(err error) trevrpc.TransportCloseReason {
	return closeReason(err)
}
func (s *webTransportEndpoint) DescribeError(err error) string { return describeError(err) }

var _ transportapi.WebTransportSession = (*webTransportEndpoint)(nil)

type quicStream struct{ stream *quic.Stream }

func (s *quicStream) Read(p []byte) (int, error)        { return s.stream.Read(p) }
func (s *quicStream) Write(p []byte) (int, error)       { return s.stream.Write(p) }
func (s *quicStream) Close() error                      { return s.stream.Close() }
func (s *quicStream) Context() context.Context          { return s.stream.Context() }
func (s *quicStream) SetReadDeadline(t time.Time) error { return s.stream.SetReadDeadline(t) }
func (s *quicStream) CancelRead(reason trevrpc.TransportCloseReason) {
	s.stream.CancelRead(quic.StreamErrorCode(reason.ApplicationCode))
}
func (s *quicStream) CancelWrite(reason trevrpc.TransportCloseReason) {
	s.stream.CancelWrite(quic.StreamErrorCode(reason.ApplicationCode))
}

var _ transportapi.BidirectionalStream = (*quicStream)(nil)
var _ transportapi.StreamContext = (*quicStream)(nil)
var _ transportapi.ReadCanceler = (*quicStream)(nil)
var _ transportapi.WriteCanceler = (*quicStream)(nil)
var _ transportapi.ReadDeadlineSetter = (*quicStream)(nil)

type webTransportStream struct{ stream *webtransport.Stream }

func (s *webTransportStream) Read(p []byte) (int, error)        { return s.stream.Read(p) }
func (s *webTransportStream) Write(p []byte) (int, error)       { return s.stream.Write(p) }
func (s *webTransportStream) Close() error                      { return s.stream.Close() }
func (s *webTransportStream) Context() context.Context          { return s.stream.Context() }
func (s *webTransportStream) SetReadDeadline(t time.Time) error { return s.stream.SetReadDeadline(t) }
func (s *webTransportStream) CancelRead(reason trevrpc.TransportCloseReason) {
	s.stream.CancelRead(webtransport.StreamErrorCode(reason.ApplicationCode))
}
func (s *webTransportStream) CancelWrite(reason trevrpc.TransportCloseReason) {
	s.stream.CancelWrite(webtransport.StreamErrorCode(reason.ApplicationCode))
}

var _ transportapi.BidirectionalStream = (*webTransportStream)(nil)
var _ transportapi.StreamContext = (*webTransportStream)(nil)
var _ transportapi.ReadCanceler = (*webTransportStream)(nil)
var _ transportapi.WriteCanceler = (*webTransportStream)(nil)
var _ transportapi.ReadDeadlineSetter = (*webTransportStream)(nil)

func closeReason(err error) trevrpc.TransportCloseReason {
	reason := trevrpc.TransportCloseReason{Err: err}
	var h3Err *http3.Error
	if errors.As(err, &h3Err) {
		reason.Peer, reason.Local, reason.Clean = h3Err.Remote, !h3Err.Remote, h3Err.ErrorCode == http3.ErrCodeNoError
		reason.ApplicationCode, reason.Message = uint64(h3Err.ErrorCode), h3Err.ErrorMessage
		return reason
	}
	var wtSession *webtransport.SessionError
	if errors.As(err, &wtSession) {
		reason.Peer, reason.Local, reason.Clean = wtSession.Remote, !wtSession.Remote, wtSession.ErrorCode == 0
		reason.ApplicationCode, reason.Message = uint64(wtSession.ErrorCode), wtSession.Message
		return reason
	}
	var wtStream *webtransport.StreamError
	if errors.As(err, &wtStream) {
		reason.Peer, reason.Local = wtStream.Remote, !wtStream.Remote
		reason.ApplicationCode = uint64(wtStream.ErrorCode)
		return reason
	}
	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		reason.Peer, reason.Local = streamErr.Remote, !streamErr.Remote
		reason.ApplicationCode = uint64(streamErr.ErrorCode)
		return reason
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		reason.Peer, reason.Local, reason.Clean = appErr.Remote, !appErr.Remote, appErr.ErrorCode == 0
		reason.ApplicationCode, reason.Message = uint64(appErr.ErrorCode), appErr.ErrorMessage
		return reason
	}
	var transportErr *quic.TransportError
	if errors.As(err, &transportErr) {
		reason.Peer, reason.Local, reason.Clean = transportErr.Remote, !transportErr.Remote, transportErr.ErrorCode == 0
		reason.TransportCode, reason.Message = uint64(transportErr.ErrorCode), transportErr.ErrorMessage
		return reason
	}
	if err != nil {
		reason.Message = err.Error()
	}
	return reason
}

func describeError(err error) string {
	if err == nil {
		return "<nil>"
	}
	var session *webtransport.SessionError
	if errors.As(err, &session) {
		return fmt.Sprintf("type=webtransport_session remote=%t code=%d message=%q", session.Remote, session.ErrorCode, session.Message)
	}
	var h3 *http3.Error
	if errors.As(err, &h3) {
		return fmt.Sprintf("type=http3 remote=%t code=%#x message=%q", h3.Remote, uint64(h3.ErrorCode), h3.ErrorMessage)
	}
	return err.Error()
}
