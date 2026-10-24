package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
	// webRoot is a directory served through the HTTP fallback, if set.
	webRoot string
	// dictionary is a compression dictionary file, if set.
	dictionary string
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
	compression, err := interopCompression(config.dictionary)
	if err != nil {
		return err
	}
	listener, err := newServer(net.JoinHostPort(config.host, config.port), &tls.Config{Certificates: []tls.Certificate{certificate}}, config.webRoot, compression)
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
// admitted because browsers connect from arbitrary origins. A non-empty webRoot
// is served through the HTTP fallback on the same ports.
func newServer(addr string, tlsConfig *tls.Config, webRoot string, compression *trevrpc.Compression) (*trevrpc.Listener, error) {
	server := trevrpc.NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(trevrpc.AdmissionRequest) bool { return true }
	options.Compression = compression
	if webRoot != "" {
		options.HTTPFallback = http.FileServer(http.Dir(webRoot))
	}
	server.SetOptions(options)
	interoppb.RegisterEchoServer(server, echoServer{})
	interoppb.RegisterUnreliableEchoServer(server, unreliableEchoServer{})
	interoppb.RegisterCompressedEchoServer(server, compressedEchoServer{})
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

// answerHeader sets the response header and the trailing metadata that answer
// the interop-header request metadata of ctx, if any.
func answerHeader(ctx context.Context) error {
	value, ok := trevrpc.RequestMetadataFromContext(ctx)[headerKey]
	if !ok {
		return nil
	}
	if err := trevrpc.SetResponseHeader(ctx, headerKey, value); err != nil {
		return err
	}
	return trevrpc.SetResponseMetadata(ctx, trailerKey, value)
}

func (echoServer) Unary(ctx context.Context, request *interoppb.EchoRequest) (*interoppb.EchoResponse, error) {
	if request.FailCode != 0 {
		return nil, failure(request)
	}
	if err := answerHeader(ctx); err != nil {
		return nil, err
	}
	suffix := trevrpc.RequestMetadataFromContext(ctx)["interop-suffix"]
	return &interoppb.EchoResponse{Message: request.Message + string(suffix)}, nil
}

func (echoServer) ClientStream(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (*interoppb.EchoResponse, error) {
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
	if err := answerHeader(ctx); err != nil {
		return nil, err
	}
	return &interoppb.EchoResponse{Message: message.String(), Index: count}, nil
}

func (echoServer) ServerStream(ctx context.Context, request *interoppb.EchoRequest) (trevrpc.MessageStream[*interoppb.EchoResponse], error) {
	if err := answerHeader(ctx); err != nil {
		return nil, err
	}
	return &countedEcho{request: request}, nil
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
	if err := answerHeader(ctx); err != nil {
		return nil, err
	}
	return &bidiEcho{requests: requests}, nil
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
	if err := answerHeader(ctx); err != nil {
		return nil, err
	}
	return &unreliableEcho{ctx: ctx, requests: requests}, nil
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
			if err := s.setTrailers(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		s.received++
		s.current, s.sent = request, 0
	}
}

// setTrailers sets the trailing metadata of a request stream that ended.
func (s *unreliableEcho) setTrailers() error {
	var datagrams int64
	if info, ok := trevrpc.HandlerContextFromContext(s.ctx); ok {
		datagrams = info.RequestDatagrams()
	}
	if err := trevrpc.SetResponseMetadata(s.ctx, receivedKey, []byte(strconv.Itoa(s.received))); err != nil {
		return err
	}
	return trevrpc.SetResponseMetadata(s.ctx, datagramsReceivedKey, []byte(strconv.FormatInt(datagrams, 10)))
}

func (s *unreliableEcho) Close() error { return s.requests.Close() }

// streamCompressedReceivedKey is the trailing metadata key of
// CompressedEcho.Bidi.
const streamCompressedReceivedKey = "interop-stream-compressed-received"

// compressedEchoServer implements CompressedEcho in interop/README.md: Echo's
// Bidi, reporting how many requests arrived stream-compressed, and Echo's
// ClientStream.
type compressedEchoServer struct{}

func (compressedEchoServer) ClientStream(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (*interoppb.EchoResponse, error) {
	return echoServer{}.ClientStream(ctx, requests)
}

func (compressedEchoServer) Bidi(ctx context.Context, requests trevrpc.MessageStream[*interoppb.EchoRequest]) (trevrpc.MessageStream[*interoppb.EchoResponse], error) {
	responses, err := echoServer{}.Bidi(ctx, requests)
	if err != nil {
		return nil, err
	}
	return &compressedEcho{ctx: ctx, responses: responses}, nil
}

// compressedEcho is an Echo.Bidi response stream that sets the count of
// stream-compressed requests in its trailing metadata when the requests end.
type compressedEcho struct {
	ctx       context.Context
	responses trevrpc.MessageStream[*interoppb.EchoResponse]
}

func (s *compressedEcho) Recv() (*interoppb.EchoResponse, error) {
	response, err := s.responses.Recv()
	if errors.Is(err, io.EOF) {
		var received int64
		if info, ok := trevrpc.HandlerContextFromContext(s.ctx); ok {
			received = info.StreamCompressedRequests()
		}
		if err := trevrpc.SetResponseMetadata(s.ctx, streamCompressedReceivedKey, []byte(strconv.FormatInt(received, 10))); err != nil {
			return nil, err
		}
	}
	return response, err
}

func (s *compressedEcho) Close() error { return s.responses.Close() }
