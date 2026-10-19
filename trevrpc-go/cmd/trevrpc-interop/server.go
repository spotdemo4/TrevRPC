package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"strconv"
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
// connections. One UDP socket serves the QUIC protocols and one TCP socket
// the fallbacks.
type listeningLine struct {
	Native       int `json:"native"`
	HTTP3        int `json:"http3"`
	WebTransport int `json:"webtransport"`
	QMux         int `json:"qmux"`
	HTTP2        int `json:"http2"`
	WebSocket    int `json:"websocket"`
}

// runServer serves Echo over every transport until ctx is cancelled. It writes
// the listening line to out once the sockets are bound.
func runServer(ctx context.Context, config serverConfig, out io.Writer) error {
	certificate, err := tls.LoadX509KeyPair(config.certFile, config.keyFile)
	if err != nil {
		return fmt.Errorf("load certificate: %w", err)
	}
	listener, err := newServer(net.JoinHostPort(config.host, config.port), &tls.Config{Certificates: []tls.Certificate{certificate}})
	if err != nil {
		return err
	}
	defer closeQuietly(listener)

	udp := listener.Addr().(*net.UDPAddr).Port
	tcp := listener.TCPAddr().(*net.TCPAddr).Port
	line := listeningLine{Native: udp, HTTP3: udp, WebTransport: udp, QMux: tcp, HTTP2: tcp, WebSocket: tcp}
	if err := json.NewEncoder(out).Encode(line); err != nil {
		return err
	}
	return listener.Serve(ctx)
}

// newServer binds a listener for the Echo and UnreliableEcho services. Every browser session is
// admitted because browsers connect from arbitrary origins. Streams are not
// limited in messages or bytes, because bench/README.md keeps one Bidi stream
// open for a whole run.
func newServer(addr string, tlsConfig *tls.Config) (*trevrpc.Listener, error) {
	server := trevrpc.NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(trevrpc.AdmissionRequest) bool { return true }
	options.MaxStreamMessages = -1
	options.MaxStreamBodySize = -1
	server.SetOptions(options)
	interoppb.RegisterEchoServer(server, echoServer{})
	interoppb.RegisterUnreliableEchoServer(server, unreliableEchoServer{})
	return trevrpc.Listen(addr, server, trevrpc.ListenOptions{TLSConfig: tlsConfig})
}

// echoServer implements the behavior table in interop/README.md.
type echoServer struct{}

func failure(request *interoppb.EchoRequest) error {
	return trevrpc.NewStatus(trevrpc.CodeFromUint32(request.FailCode), request.FailMessage)
}

// headerKey is the request metadata key that asks for response headers.
const headerKey = "interop-header"

// trailerKey is the trailing metadata key of the answer to headerKey.
const trailerKey = "interop-trailer"

// headerMetadata returns the response headers and the trailing metadata that
// answer the interop-header request metadata of ctx: both are empty without it.
func headerMetadata(ctx context.Context) (headers, trailers trevrpc.Metadata) {
	value, ok := trevrpc.RequestMetadataFromContext(ctx)[headerKey]
	if !ok {
		return nil, nil
	}
	return trevrpc.Metadata{headerKey: value}, trevrpc.Metadata{trailerKey: value}
}

// withHeaders adds the response headers and trailing metadata that answer
// interop-header to a response stream.
func withHeaders(ctx context.Context, stream trevrpc.MessageStream[*interoppb.EchoResponse]) trevrpc.MessageStream[*interoppb.EchoResponse] {
	headers, trailers := headerMetadata(ctx)
	if headers == nil {
		return stream
	}
	return trevrpc.WithResponseHeaders(trevrpc.NewResponseStream(stream, trailers), headers)
}

func (echoServer) Unary(ctx context.Context, request *interoppb.EchoRequest) (*trevrpc.Response[*interoppb.EchoResponse], error) {
	if request.FailCode != 0 {
		return nil, failure(request)
	}
	suffix := trevrpc.RequestMetadataFromContext(ctx)["interop-suffix"]
	headers, trailers := headerMetadata(ctx)
	return &trevrpc.Response[*interoppb.EchoResponse]{
		Message:  &interoppb.EchoResponse{Message: request.Message + string(suffix)},
		Headers:  headers,
		Metadata: trailers,
	}, nil
}

func (echoServer) ClientStream(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (*trevrpc.Response[*interoppb.EchoResponse], error) {
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
	headers, trailers := headerMetadata(ctx)
	return &trevrpc.Response[*interoppb.EchoResponse]{
		Message:  &interoppb.EchoResponse{Message: message.String(), Index: count},
		Headers:  headers,
		Metadata: trailers,
	}, nil
}

func (echoServer) ServerStream(ctx context.Context, request *interoppb.EchoRequest) (trevrpc.MessageStream[*interoppb.EchoResponse], error) {
	return withHeaders(ctx, &countedEcho{request: request}), nil
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

func (echoServer) Bidi(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (trevrpc.MessageStream[*interoppb.EchoResponse], error) {
	return withHeaders(ctx, &bidiEcho{requests: requests}), nil
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

// Trailing metadata keys of UnreliableEcho.Bidi.
const (
	receivedKey          = "interop-received"
	datagramsReceivedKey = "interop-datagrams-received"
)

// unreliableEchoServer implements UnreliableEcho in interop/README.md.
type unreliableEchoServer struct{}

func (unreliableEchoServer) Bidi(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (trevrpc.MessageStream[*interoppb.EchoResponse], error) {
	stream := &unreliableEcho{ctx: ctx, requests: requests}
	if headers, _ := headerMetadata(ctx); headers != nil {
		return trevrpc.WithResponseHeaders[*interoppb.EchoResponse](stream, headers), nil
	}
	return stream, nil
}

// unreliableEcho answers each request with request.count echoes, in order, and
// ends with the number of requests received, and how many of them arrived as
// datagrams, in its trailing metadata.
type unreliableEcho struct {
	ctx      context.Context
	requests trevrpc.MessageStream[*interoppb.EchoRequest]
	current  *interoppb.EchoRequest
	sent     uint32
	received int
	terminal *trevrpc.Status
}

func (s *unreliableEcho) Recv() (*interoppb.EchoResponse, error) {
	for {
		if s.current != nil && s.sent < s.current.Count {
			response := &interoppb.EchoResponse{Message: s.current.Message, Index: s.sent}
			s.sent++
			return response, nil
		}
		request, err := s.requests.Recv()
		if errors.Is(err, io.EOF) {
			s.terminal = trevrpc.OK().WithMetadata(s.trailers())
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		s.received++
		s.current, s.sent = request, 0
	}
}

// trailers returns the trailing metadata of a request stream that ended.
func (s *unreliableEcho) trailers() trevrpc.Metadata {
	var datagrams int64
	if info, ok := trevrpc.HandlerContextFromContext(s.ctx); ok {
		datagrams = info.RequestDatagrams()
	}
	trailers := trevrpc.Metadata{
		receivedKey:          []byte(strconv.Itoa(s.received)),
		datagramsReceivedKey: []byte(strconv.FormatInt(datagrams, 10)),
	}
	if _, answer := headerMetadata(s.ctx); answer != nil {
		maps.Copy(trailers, answer)
	}
	return trailers
}

func (s *unreliableEcho) TerminalStatus() (*trevrpc.Status, bool) {
	return s.terminal, s.terminal != nil
}

func (s *unreliableEcho) Close() error { return s.requests.Close() }
