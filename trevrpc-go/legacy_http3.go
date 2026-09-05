package trevrpc

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

type legacyHTTP3Request struct {
	request       *http.Request
	writer        http.ResponseWriter
	controller    *http.ResponseController
	stream        *legacyHTTP3Stream
	headers       HeaderFields
	headersLoaded bool
}

var _ transportinternal.HTTP3Request = (*legacyHTTP3Request)(nil)

func newLegacyHTTP3Request(
	writer http.ResponseWriter,
	request *http.Request,
) *legacyHTTP3Request {
	return &legacyHTTP3Request{
		request: request,
		writer:  writer,
	}
}

func (r *legacyHTTP3Request) Context() context.Context {
	return r.request.Context()
}

func (r *legacyHTTP3Request) Method() string {
	return r.request.Method
}

func (r *legacyHTTP3Request) Path() string {
	return r.request.URL.Path
}

func (r *legacyHTTP3Request) Authority() string {
	return r.request.Host
}

func (r *legacyHTTP3Request) Secure() bool {
	return r.request.TLS != nil
}

func (r *legacyHTTP3Request) Headers() HeaderFields {
	if !r.headersLoaded {
		r.headers = headerFieldsFromHTTP(r.request.Header)
		r.headersLoaded = true
	}
	return r.headers
}

func (r *legacyHTTP3Request) Stream() transportinternal.BidirectionalStream {
	if r.stream == nil {
		controller := r.responseController()
		r.stream = &legacyHTTP3Stream{
			body:       r.request.Body,
			writer:     r.writer,
			controller: controller,
		}
	}
	return r.stream
}

func (r *legacyHTTP3Request) SetResponseHeader(name, value string) {
	r.writer.Header().Set(name, value)
}

func (r *legacyHTTP3Request) WriteResponseHeader(status int) {
	r.writer.WriteHeader(status)
}

func (r *legacyHTTP3Request) WriteError(message string, status int) {
	http.Error(r.writer, message, status)
}

func (r *legacyHTTP3Request) Flush() error {
	return r.responseController().Flush()
}

func (r *legacyHTTP3Request) responseController() *http.ResponseController {
	if r.controller == nil {
		r.controller = http.NewResponseController(r.writer)
	}
	return r.controller
}

func (r *legacyHTTP3Request) legacyRequest() *http.Request {
	return r.request
}

type legacyWebTransportRequest struct {
	*legacyHTTP3Request
	server           *webtransport.Server
	requestedBackend TransportBackend
}

var _ transportinternal.WebTransportRequest = (*legacyWebTransportRequest)(nil)

func newLegacyWebTransportRequest(
	writer http.ResponseWriter,
	request *http.Request,
	server *webtransport.Server,
	requestedBackend TransportBackend,
) *legacyWebTransportRequest {
	return &legacyWebTransportRequest{
		legacyHTTP3Request: newLegacyHTTP3Request(writer, request),
		server:             server,
		requestedBackend:   requestedBackend,
	}
}

func (r *legacyWebTransportRequest) Origin() string {
	return r.request.Header.Get("Origin")
}

func (r *legacyWebTransportRequest) RemoteAddress() TransportAddress {
	return transportinternal.AddressFromNet(transportStringAddress{
		network: "udp",
		value:   r.request.RemoteAddr,
	})
}

func (r *legacyWebTransportRequest) Upgrade() (
	transportinternal.WebTransportSession,
	error,
) {
	session, err := r.server.Upgrade(r.writer, r.request)
	if err != nil {
		return nil, err
	}
	return newLegacyWebTransportSession(
		session,
		r.requestedBackend,
	), nil
}

type transportStringAddress struct {
	network string
	value   string
}

func (a transportStringAddress) Network() string { return a.network }
func (a transportStringAddress) String() string  { return a.value }

type legacyHTTP3Stream struct {
	body       io.ReadCloser
	writer     http.ResponseWriter
	controller *http.ResponseController
	closeOnce  sync.Once
}

func (s *legacyHTTP3Stream) Read(data []byte) (int, error) {
	return s.body.Read(data)
}

func (s *legacyHTTP3Stream) Write(data []byte) (int, error) {
	written, err := s.writer.Write(data)
	if err != nil {
		return written, err
	}
	if err := s.controller.Flush(); err != nil {
		return written, err
	}
	return written, nil
}

func (s *legacyHTTP3Stream) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.body.Close() })
	return err
}

func (s *legacyHTTP3Stream) SetReadDeadline(deadline time.Time) error {
	return s.controller.SetReadDeadline(deadline)
}

func (s *legacyHTTP3Stream) CancelRead(TransportCloseReason) {
	_ = s.Close()
}

func (s *legacyHTTP3Stream) trevrpcCancelRead() {
	_ = s.Close()
}

func (s *legacyHTTP3Stream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	return watchContextCancellation(ctx, func() { _ = s.Close() })
}
