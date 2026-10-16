package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
)

type serverConfig struct {
	certFile string
	keyFile  string
	host     string
	port     string
}

// listeningLine is the single JSON line printed once the server accepts
// connections. One UDP socket serves all three protocols.
type listeningLine struct {
	Native       int `json:"native"`
	HTTP3        int `json:"http3"`
	WebTransport int `json:"webtransport"`
}

// runServer serves Echo over native QUIC, HTTP/3 and WebTransport until ctx is
// cancelled. It writes the listening line to out once the socket is bound.
func runServer(ctx context.Context, config serverConfig, out io.Writer) error {
	certificate, err := tls.LoadX509KeyPair(config.certFile, config.keyFile)
	if err != nil {
		return fmt.Errorf("load certificate: %w", err)
	}
	listener, err := newServer(net.JoinHostPort(config.host, config.port), &tls.Config{Certificates: []tls.Certificate{certificate}})
	if err != nil {
		return err
	}
	defer listener.Close()

	port := listener.Addr().(*net.UDPAddr).Port
	if err := json.NewEncoder(out).Encode(listeningLine{Native: port, HTTP3: port, WebTransport: port}); err != nil {
		return err
	}
	return listener.Serve(ctx)
}

// newServer binds a listener for the Echo service. Every WebTransport session
// is admitted because browsers connect from arbitrary origins.
func newServer(addr string, tlsConfig *tls.Config) (*trevrpc.Listener, error) {
	server := trevrpc.NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(trevrpc.AdmissionRequest) bool { return true }
	server.SetOptions(options)
	interoppb.RegisterEchoServer(server, echoServer{})
	return trevrpc.Listen(addr, server, trevrpc.ListenOptions{TLSConfig: tlsConfig})
}

// echoServer implements the behavior table in interop/README.md.
type echoServer struct{}

func failure(request *interoppb.EchoRequest) error {
	return trevrpc.NewStatus(trevrpc.CodeFromUint32(request.FailCode), request.FailMessage)
}

func (echoServer) Unary(ctx context.Context, request *interoppb.EchoRequest) (*trevrpc.Response[*interoppb.EchoResponse], error) {
	if request.FailCode != 0 {
		return nil, failure(request)
	}
	suffix := trevrpc.RequestMetadataFromContext(ctx)["interop-suffix"]
	return trevrpc.NewResponse(&interoppb.EchoResponse{Message: request.Message + string(suffix)}), nil
}

func (echoServer) ClientStream(_ context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (*trevrpc.Response[*interoppb.EchoResponse], error) {
	var message strings.Builder
	var count uint32
	for request, err := range trevrpc.Messages(requests) {
		if err != nil {
			return nil, err
		}
		if request.FailCode != 0 {
			return nil, failure(request)
		}
		message.WriteString(request.Message)
		count++
	}
	return trevrpc.NewResponse(&interoppb.EchoResponse{Message: message.String(), Index: count}), nil
}

func (echoServer) ServerStream(_ context.Context, request *interoppb.EchoRequest) (trevrpc.ResponseStream[*interoppb.EchoResponse], error) {
	return trevrpc.NewResponseStream[*interoppb.EchoResponse](&countedEcho{request: request}), nil
}

// countedEcho yields request.Count echoes and then ends with the requested status.
type countedEcho struct {
	request *interoppb.EchoRequest
	sent    uint32
}

func (s *countedEcho) Recv() (*interoppb.EchoResponse, error) {
	if s.sent < s.request.Count {
		response := &interoppb.EchoResponse{Message: s.request.Message, Index: s.sent}
		s.sent++
		return response, nil
	}
	if s.request.FailCode != 0 {
		return nil, failure(s.request)
	}
	return nil, io.EOF
}

func (*countedEcho) Close() error { return nil }

func (echoServer) Bidi(_ context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (trevrpc.ResponseStream[*interoppb.EchoResponse], error) {
	return trevrpc.NewResponseStream[*interoppb.EchoResponse](&bidiEcho{requests: requests}), nil
}

// bidiEcho answers each request in order and ends when the requests end.
type bidiEcho struct {
	requests trevrpc.MessageStream[*interoppb.EchoRequest]
	index    uint32
}

func (s *bidiEcho) Recv() (*interoppb.EchoResponse, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}
	if request.FailCode != 0 {
		return nil, failure(request)
	}
	response := &interoppb.EchoResponse{Message: request.Message, Index: s.index}
	s.index++
	return response, nil
}

func (s *bidiEcho) Close() error { return s.requests.Close() }
