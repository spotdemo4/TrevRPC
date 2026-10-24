package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"strings"

	"connectrpc.com/connect"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
	"trev.zip/llc/trevrpc/bench/baseline/echopb/echopbconnect"
)

// newConnectServer returns a net/http server for Echo with connect-go's and
// net/http's defaults, plus zstd. Served with TLS, it negotiates HTTP/2 with
// ALPN, which Connect needs for bidirectional streams. Its handlers also accept
// gRPC and gRPC-Web, though the bench client only speaks the Connect protocol.
// They accept gzip (built in) and zstd requests, and compress responses with an
// encoding the client accepts.
func newConnectServer(certificate tls.Certificate) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(echopbconnect.NewEchoHandler(connectEcho{}, connectZstdAtFastest))
	return &http.Server{
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}},
	}
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
