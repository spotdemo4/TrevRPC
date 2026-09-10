package quicgo

import (
	"context"
	webtransport "github.com/quic-go/webtransport-go"
	"io"
	"net/http"
	"sync"
	"time"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

type http3Request struct {
	request *http.Request
	writer  http.ResponseWriter
	ctrl    *http.ResponseController
	stream  *http3Stream
	headers trevrpc.HeaderFields
	loaded  bool
}

func (r *http3Request) Context() context.Context { return r.request.Context() }
func (r *http3Request) Method() string           { return r.request.Method }
func (r *http3Request) Path() string             { return r.request.URL.Path }
func (r *http3Request) Authority() string        { return r.request.Host }
func (r *http3Request) Secure() bool             { return r.request.TLS != nil }
func (r *http3Request) Headers() trevrpc.HeaderFields {
	if !r.loaded {
		for name, values := range r.request.Header {
			for _, value := range values {
				r.headers = append(r.headers, trevrpc.HeaderField{Name: name, Value: value})
			}
		}
		r.loaded = true
	}
	return r.headers
}
func (r *http3Request) Stream() transportapi.BidirectionalStream {
	if r.stream == nil {
		r.stream = &http3Stream{ctx: r.request.Context(), body: r.request.Body, writer: r.writer, ctrl: r.controller()}
	}
	return r.stream
}
func (r *http3Request) SetResponseHeader(name, value string)  { r.writer.Header().Set(name, value) }
func (r *http3Request) WriteResponseHeader(status int)        { r.writer.WriteHeader(status) }
func (r *http3Request) WriteError(message string, status int) { http.Error(r.writer, message, status) }
func (r *http3Request) Flush() error                          { return r.controller().Flush() }
func (r *http3Request) controller() *http.ResponseController {
	if r.ctrl == nil {
		r.ctrl = http.NewResponseController(r.writer)
	}
	return r.ctrl
}

var _ transportapi.HTTP3Request = (*http3Request)(nil)

type webTransportRequest struct {
	*http3Request
	server    *webtransport.Server
	requested trevrpc.TransportBackend
}

func (r *webTransportRequest) Origin() string { return r.request.Header.Get("Origin") }
func (r *webTransportRequest) RemoteAddress() trevrpc.TransportAddress {
	return transportapi.AddressFromNet(stringAddr{network: "udp", value: r.request.RemoteAddr})
}
func (r *webTransportRequest) Upgrade() (transportapi.WebTransportSession, error) {
	session, err := r.server.Upgrade(r.writer, r.request)
	if err != nil {
		return nil, err
	}
	return newWebTransportEndpoint(session, r.requested), nil
}

var _ transportapi.WebTransportRequest = (*webTransportRequest)(nil)

type stringAddr struct{ network, value string }

func (a stringAddr) Network() string { return a.network }
func (a stringAddr) String() string  { return a.value }

type http3Stream struct {
	ctx    context.Context
	body   io.ReadCloser
	writer http.ResponseWriter
	ctrl   *http.ResponseController
	once   sync.Once
}

func (s *http3Stream) Read(p []byte) (int, error) { return s.body.Read(p) }
func (s *http3Stream) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	if err == nil {
		err = s.ctrl.Flush()
	}
	return n, err
}
func (s *http3Stream) Close() error {
	var err error
	s.once.Do(func() { err = s.body.Close() })
	return err
}
func (s *http3Stream) SetReadDeadline(deadline time.Time) error {
	return s.ctrl.SetReadDeadline(deadline)
}
func (s *http3Stream) CancelRead(trevrpc.TransportCloseReason) { _ = s.Close() }
func (s *http3Stream) Context() context.Context                { return s.ctx }

var _ transportapi.BidirectionalStream = (*http3Stream)(nil)
var _ transportapi.ReadCanceler = (*http3Stream)(nil)
var _ transportapi.ReadDeadlineSetter = (*http3Stream)(nil)
