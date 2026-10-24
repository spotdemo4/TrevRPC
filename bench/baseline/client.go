package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
	"trev.zip/llc/trevrpc/bench/baseline/echopb/echopbconnect"
)

// echoClient is the Echo API the scenario workers drive, over grpc-go or
// connect-go. Every call carries the bench-pad request metadata when
// --metadata-size is set. Streams report their end with io.EOF, as grpc-go's
// do.
type echoClient interface {
	Unary(ctx context.Context, req *echopb.EchoRequest) (*echopb.EchoResponse, error)
	ServerStream(ctx context.Context, req *echopb.EchoRequest) (responseStream, error)
	ClientStream(ctx context.Context) (requestStream, error)
	Bidi(ctx context.Context) (bidiStream, error)
}

type responseStream interface {
	Recv() (*echopb.EchoResponse, error)
}

type requestStream interface {
	Send(req *echopb.EchoRequest) error
	CloseAndRecv() (*echopb.EchoResponse, error)
}

type bidiStream interface {
	Send(req *echopb.EchoRequest) error
	Recv() (*echopb.EchoResponse, error)
	CloseSend() error
}

// connection is one established client connection that every worker shares.
type connection struct {
	client echoClient
	wire   *wireCounter
	close  func()
}

// dial connects over cfg.transport and waits until the connection is
// established, so connection setup is never part of a measurement.
func dial(ctx context.Context, cfg benchConfig) (connection, error) {
	tlsConfig, err := clientTLS(cfg)
	if err != nil {
		return connection{}, err
	}
	// gRPC metadata values must be printable ASCII, so the pad is made of 'x'.
	pad := strings.Repeat("x", cfg.metadataSize)
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if cfg.transport == "connect" {
		return dialConnect(ctx, cfg.addr, tlsConfig, pad, connectClientOptions(cfg.compression))
	}
	return dialGRPC(ctx, cfg.addr, tlsConfig, pad, grpcCallOptions(cfg.compression))
}

func clientTLS(cfg benchConfig) (*tls.Config, error) {
	pem, err := os.ReadFile(cfg.ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s contains no certificates", cfg.ca)
	}
	return &tls.Config{RootCAs: pool, ServerName: cfg.serverName}, nil
}

// dialGRPC returns a grpc.ClientConn, which grpc-go multiplexes over a single
// HTTP/2 connection with default settings (64 KiB initial flow-control windows
// with dynamic BDP estimation, 32 KiB write buffers). Those defaults cap
// throughput per stream on high-latency links but are irrelevant on loopback;
// no tuning is applied so the numbers are stock gRPC. Every call carries
// options, which select its compression.
func dialGRPC(ctx context.Context, addr string, tlsConfig *tls.Config, pad string, options []grpc.CallOption) (connection, error) {
	counter := &wireCounter{}
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithContextDialer(counter.dial))
	if err != nil {
		return connection{}, err
	}
	if err := waitReady(ctx, conn); err != nil {
		_ = conn.Close()
		return connection{}, err
	}
	client := grpcClient{client: echopb.NewEchoClient(conn), options: options}
	if pad != "" {
		client.md = metadata.Pairs(padKey, pad)
	}
	return connection{client: client, wire: counter, close: func() { _ = conn.Close() }}, nil
}

// waitReady connects and waits for the HTTP/2 connection to be established.
func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		switch state := conn.GetState(); state {
		case connectivity.Ready:
			return nil
		case connectivity.TransientFailure, connectivity.Shutdown:
			return fmt.Errorf("connect to %s: %s", conn.Target(), state)
		}
		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			return fmt.Errorf("connect to %s: %w", conn.Target(), ctx.Err())
		}
	}
}

// grpcClient adapts the generated grpc-go client.
type grpcClient struct {
	client  echopb.EchoClient
	md      metadata.MD // the bench-pad entry, or nil without --metadata-size
	options []grpc.CallOption
}

func (c grpcClient) outgoing(ctx context.Context) context.Context {
	if c.md == nil {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, c.md)
}

func (c grpcClient) Unary(ctx context.Context, req *echopb.EchoRequest) (*echopb.EchoResponse, error) {
	return c.client.Unary(c.outgoing(ctx), req, c.options...)
}

func (c grpcClient) ServerStream(ctx context.Context, req *echopb.EchoRequest) (responseStream, error) {
	return c.client.ServerStream(c.outgoing(ctx), req, c.options...)
}

func (c grpcClient) ClientStream(ctx context.Context) (requestStream, error) {
	return c.client.ClientStream(c.outgoing(ctx), c.options...)
}

func (c grpcClient) Bidi(ctx context.Context) (bidiStream, error) {
	return c.client.Bidi(c.outgoing(ctx), c.options...)
}

// dialConnect returns a connect-go client speaking the Connect protocol with
// its default binary Protobuf codec over one HTTP/2 connection from net/http.
// The defaults are kept except for two deviations that make it comparable with
// the gRPC baseline: options select the compression, off unless a mode asks for
// it (connect-go otherwise gzips every response, since its client accepts gzip
// and handlers compress messages of any size), and the transport waits for a
// free stream rather than opening a second connection once the server's stream
// limit (250 in net/http) is reached, so that every worker shares one
// connection.
func dialConnect(ctx context.Context, addr string, tlsConfig *tls.Config, pad string, options []connect.ClientOption) (connection, error) {
	counter := &wireCounter{}
	var protocols http.Protocols
	protocols.SetHTTP2(true)
	transport := &http.Transport{
		DialContext:     counter.dialNetwork,
		TLSClientConfig: tlsConfig,
		Protocols:       &protocols,
		HTTP2:           &http.HTTP2Config{StrictMaxConcurrentRequests: true},
	}
	client := connectClient{
		client: echopbconnect.NewEchoClient(&http.Client{Transport: transport}, "https://"+addr, options...),
		pad:    pad,
	}
	// One call establishes the connection the workers will share.
	if _, err := client.Unary(ctx, &echopb.EchoRequest{}); err != nil {
		transport.CloseIdleConnections()
		return connection{}, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return connection{client: client, wire: counter, close: transport.CloseIdleConnections}, nil
}

// connectClient adapts the generated connect-go client.
type connectClient struct {
	client echopbconnect.EchoClient
	pad    string // the bench-pad value, or empty without --metadata-size
}

func (c connectClient) setPad(header http.Header) {
	if c.pad != "" {
		header.Set(padKey, c.pad)
	}
}

func (c connectClient) Unary(ctx context.Context, req *echopb.EchoRequest) (*echopb.EchoResponse, error) {
	request := connect.NewRequest(req)
	c.setPad(request.Header())
	resp, err := c.client.Unary(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (c connectClient) ServerStream(ctx context.Context, req *echopb.EchoRequest) (responseStream, error) {
	request := connect.NewRequest(req)
	c.setPad(request.Header())
	stream, err := c.client.ServerStream(ctx, request)
	if err != nil {
		return nil, err
	}
	return connectResponseStream{stream}, nil
}

func (c connectClient) ClientStream(ctx context.Context) (requestStream, error) {
	stream := c.client.ClientStream(ctx)
	c.setPad(stream.RequestHeader())
	return connectRequestStream{stream}, nil
}

func (c connectClient) Bidi(ctx context.Context) (bidiStream, error) {
	stream := c.client.Bidi(ctx)
	c.setPad(stream.RequestHeader())
	return connectBidiStream{stream}, nil
}

type connectResponseStream struct {
	stream *connect.ServerStreamForClient[echopb.EchoResponse]
}

// Recv returns the next response, or io.EOF once the call ended with OK. At
// the end it closes the stream, releasing the HTTP response.
func (s connectResponseStream) Recv() (*echopb.EchoResponse, error) {
	if s.stream.Receive() {
		return s.stream.Msg(), nil
	}
	err := s.stream.Err()
	if closeErr := s.stream.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = io.EOF
	}
	return nil, err
}

type connectRequestStream struct {
	stream *connect.ClientStreamForClient[echopb.EchoRequest, echopb.EchoResponse]
}

func (s connectRequestStream) Send(req *echopb.EchoRequest) error {
	return s.stream.Send(req)
}

func (s connectRequestStream) CloseAndRecv() (*echopb.EchoResponse, error) {
	resp, err := s.stream.CloseAndReceive()
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

type connectBidiStream struct {
	stream *connect.BidiStreamForClient[echopb.EchoRequest, echopb.EchoResponse]
}

func (s connectBidiStream) Send(req *echopb.EchoRequest) error {
	return s.stream.Send(req)
}

func (s connectBidiStream) Recv() (*echopb.EchoResponse, error) {
	return s.stream.Receive()
}

func (s connectBidiStream) CloseSend() error {
	return s.stream.CloseRequest()
}
