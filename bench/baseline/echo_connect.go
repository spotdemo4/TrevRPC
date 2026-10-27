package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/quic-go/quic-go/http3"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
	"trev.zip/llc/trevrpc/bench/baseline/echopb/echopbconnect"
)

// newConnectHandler returns the Echo handler with connect-go's defaults, plus
// zstd and CORS. Its handlers also accept gRPC and gRPC-Web, which the browser
// benchmark's gRPC-Web client speaks; the bench client only speaks the Connect
// protocol. They accept gzip (built in) and zstd requests, and compress
// responses with an encoding the client accepts.
func newConnectHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(echopbconnect.NewEchoHandler(connectEcho{}, connectZstdAtFastest))
	return allowCrossOrigin(mux)
}

// newConnectServer returns a net/http server for handler with net/http's
// defaults. Served with TLS, it negotiates HTTP/2 with ALPN, which Connect
// needs for bidirectional streams.
func newConnectServer(certificate tls.Certificate, handler http.Handler) *http.Server {
	return &http.Server{
		Handler:   handler,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}},
	}
}

// newConnectHTTP3Server returns an HTTP/3 server for handler, for browsers:
// the native clients reach Connect over HTTP/2 only.
func newConnectHTTP3Server(certificate tls.Certificate, handler http.Handler) *http3.Server {
	return &http3.Server{
		Handler:   handler,
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{certificate}}),
	}
}

// corsMaxAge is how long, in seconds, a browser may reuse a preflight's
// answer: the most Chromium honours, so a benchmark pays for one preflight.
const corsMaxAge = "7200"

// allowCrossOrigin lets pages from any origin call next: the browser
// benchmark's page is served from the runner's own loopback origin. Requests
// carry no credentials, so the wildcards apply to every header and origin.
func allowCrossOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") == "" {
			next.ServeHTTP(w, r)
			return
		}
		header := w.Header()
		header.Set("Access-Control-Allow-Origin", "*")
		header.Set("Access-Control-Expose-Headers", "*")
		// The page checks which HTTP version a call took in its resource timing.
		header.Set("Timing-Allow-Origin", "*")
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			header.Set("Access-Control-Allow-Methods", "GET, POST")
			header.Set("Access-Control-Allow-Headers", "*")
			header.Set("Access-Control-Max-Age", corsMaxAge)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// connectEcho implements the interop Echo semantics from interop/README.md
// with connect-go.
type connectEcho struct {
	echopbconnect.UnimplementedEchoHandler
}

// connectFailure converts an EchoRequest's fail_code / fail_message into an
// error.
func connectFailure(req *echopb.EchoRequest) error {
	return connect.NewError(connect.Code(req.GetFailCode()), errors.New(req.GetFailMessage()))
}

// connectHeaders sets the response header when the request carries
// interop-header, and returns a function that sets the matching trailer for an
// OK finish. Without that key both are no-ops.
func connectHeaders(request, response, trailer http.Header) func() {
	values := request.Values(headerKey)
	if len(values) == 0 {
		return func() {}
	}
	response.Set(headerKey, values[0])
	return func() { trailer.Set(trailerKey, values[0]) }
}

func (connectEcho) Unary(_ context.Context, req *connect.Request[echopb.EchoRequest]) (*connect.Response[echopb.EchoResponse], error) {
	if req.Msg.GetFailCode() != 0 {
		return nil, connectFailure(req.Msg)
	}
	message := req.Msg.GetMessage()
	if suffix := req.Header().Values(suffixKey); len(suffix) > 0 {
		message += suffix[0]
	}
	resp := connect.NewResponse(&echopb.EchoResponse{Message: message})
	connectHeaders(req.Header(), resp.Header(), resp.Trailer())()
	return resp, nil
}

func (connectEcho) ClientStream(_ context.Context, stream *connect.ClientStream[echopb.EchoRequest]) (*connect.Response[echopb.EchoResponse], error) {
	var message strings.Builder
	var count uint32
	for stream.Receive() {
		req := stream.Msg()
		if req.GetFailCode() != 0 {
			return nil, connectFailure(req)
		}
		message.WriteString(req.GetMessage())
		count++
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	resp := connect.NewResponse(&echopb.EchoResponse{Message: message.String(), Index: count})
	connectHeaders(stream.RequestHeader(), resp.Header(), resp.Trailer())()
	return resp, nil
}

func (connectEcho) ServerStream(_ context.Context, req *connect.Request[echopb.EchoRequest], stream *connect.ServerStream[echopb.EchoResponse]) error {
	finish := connectHeaders(req.Header(), stream.ResponseHeader(), stream.ResponseTrailer())
	// One response value is reused: Send marshals it before returning.
	resp := &echopb.EchoResponse{Message: req.Msg.GetMessage()}
	for i := range req.Msg.GetCount() {
		resp.Index = i
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	if req.Msg.GetFailCode() != 0 {
		return connectFailure(req.Msg)
	}
	finish()
	return nil
}

func (connectEcho) Bidi(_ context.Context, stream *connect.BidiStream[echopb.EchoRequest, echopb.EchoResponse]) error {
	finish := connectHeaders(stream.RequestHeader(), stream.ResponseHeader(), stream.ResponseTrailer())
	resp := &echopb.EchoResponse{}
	for i := uint32(0); ; i++ {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			finish()
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetFailCode() != 0 {
			return connectFailure(req)
		}
		resp.Message, resp.Index = req.GetMessage(), i
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
