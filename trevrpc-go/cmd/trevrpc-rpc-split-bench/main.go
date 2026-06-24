package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

const (
	streamMessageCount = 16
	requestName        = "TrevRPC benchmark"
	idleTimeout        = 10 * time.Minute
	keepAlive          = 5 * time.Second
	shutdownTimeout    = 2 * time.Second
)

var (
	request  = &greeter.HelloRequest{Name: requestName}
	requests = makeRequests()
)

type splitGreeter struct{}

type grpcSplitGreeter struct{}

func main() {
	mode := flag.String("mode", "", "client or server")
	transport := flag.String("transport", "quic", "quic, msquic, webtransport, webtransport-msquic, or grpc")
	addr := flag.String("addr", "127.0.0.1:0", "listen or dial address")
	cert := flag.String("cert", "", "PEM certificate path")
	key := flag.String("key", "", "PEM private key path")
	origin := flag.String("origin", "", "allowed WebTransport origin")
	iterations := flag.Int("iterations", 1000, "benchmark iterations")
	flag.Parse()

	var err error
	switch *mode {
	case "client":
		err = runClient(*transport, *addr, *cert, *iterations)
	case "server":
		err = runServer(*transport, *addr, *cert, *key, *origin)
	default:
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runClient(transportName, addr, certFile string, iterations int) error {
	if iterations <= 0 {
		return fmt.Errorf("iterations must be positive")
	}
	if transportName == "grpc" {
		return runGRPCClient(addr, iterations)
	}

	ctx := context.Background()
	transport, err := dialTransport(ctx, transportName, addr, certFile)
	if err != nil {
		return err
	}
	defer transport.Close()

	client := greeter.NewGreeterClient(transport, trevrpc.WithoutStreamIdleTimeout())
	if err := warmClient(ctx, client); err != nil {
		return err
	}
	if err := runCase("unary_round_trip", iterations, func() error {
		_, err := unary(ctx, client)
		return err
	}); err != nil {
		return err
	}
	if err := runCase("server_stream_16_messages", iterations, func() error {
		_, err := serverStreaming(ctx, client)
		return err
	}); err != nil {
		return err
	}
	if err := runCase("client_stream_16_messages", iterations, func() error {
		_, err := clientStreaming(ctx, client)
		return err
	}); err != nil {
		return err
	}
	if err := runCase("bidi_stream_16_messages", iterations, func() error {
		_, err := bidiStreaming(ctx, client)
		return err
	}); err != nil {
		return err
	}
	return runLongLivedStreamCase("bidi_stream_long_lived_messages", iterations, func() error {
		_, err := bidiLongLived(ctx, client, iterations)
		return err
	})
}

func runServer(transportName, addr, certFile, keyFile, origin string) error {
	if transportName == "grpc" {
		return runGRPCServer(addr)
	}
	if transportName == "webtransport-msquic" {
		return runNativeWebTransportServer(addr, certFile, keyFile, origin)
	}

	server := trevrpc.NewServer()
	options := server.Options()
	options.GracefulShutdownTimeout = time.Second
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = 0
	if transportName == "webtransport" {
		options.EnableWebTransport = true
		options.MaxConcurrentStreamsPerConnection = 65535
		options.WebTransportCheckOrigin = func(*http.Request) bool { return true }
	}
	server.SetOptions(options)
	greeter.RegisterGreeterServer(server, splitGreeter{})

	listener, err := listenTransport(transportName, addr, certFile, keyFile, server)
	if err != nil {
		return err
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- listener.Serve(ctx)
	}()
	fmt.Printf("PORT %d\n", portFromAddr(listener.Addr()))
	if certFile != "" {
		fmt.Printf("CERT %s\n", certFile)
	}
	if err := waitForShutdown(ctx, serveDone); err != nil {
		return err
	}
	return nil
}

func runGRPCClient(addr string, iterations int) error {
	ctx := context.Background()
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := warmGRPCClient(ctx, conn); err != nil {
		return err
	}
	if err := runCase("unary_round_trip", iterations, func() error {
		_, err := grpcUnary(ctx, conn)
		return err
	}); err != nil {
		return err
	}
	if err := runCase("server_stream_16_messages", iterations, func() error {
		_, err := grpcServerStreaming(ctx, conn)
		return err
	}); err != nil {
		return err
	}
	if err := runCase("client_stream_16_messages", iterations, func() error {
		_, err := grpcClientStreaming(ctx, conn)
		return err
	}); err != nil {
		return err
	}
	if err := runCase("bidi_stream_16_messages", iterations, func() error {
		_, err := grpcBidiStreaming(ctx, conn)
		return err
	}); err != nil {
		return err
	}
	return runLongLivedStreamCase("bidi_stream_long_lived_messages", iterations, func() error {
		_, err := grpcBidiLongLived(ctx, conn, iterations)
		return err
	})
}

func runGRPCServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := grpc.NewServer()
	registerGRPCGreeterServer(server, grpcSplitGreeter{})

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("PORT %d\n", portFromAddr(listener.Addr()))

	select {
	case err := <-serveDone:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	server.Stop()
	select {
	case err := <-serveDone:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-time.After(shutdownTimeout):
		return errors.New("timed out waiting for gRPC server shutdown")
	}
}

func dialTransport(ctx context.Context, transportName, addr, certFile string) (trevrpc.ClientTransport, error) {
	switch transportName {
	case "quic":
		return trevrpc.Dial(ctx, addr, trevrpc.DialOptions{
			Kind:       trevrpc.TransportQUICGo,
			TLSConfig:  clientTLSConfig(certFile),
			QUICConfig: quicConfig(),
		})
	case "msquic":
		return trevrpc.Dial(ctx, addr, trevrpc.DialOptions{
			Kind: trevrpc.TransportMsQuic,
			MsQuic: trevrpc.MsQuicConfig{
				MaxIdleTimeout:      idleTimeout,
				KeepAlive:           keepAlive,
				PeerBidiStreamCount: 128,
			},
		})
	default:
		return nil, fmt.Errorf("unsupported transport %q", transportName)
	}
}

func listenTransport(transportName, addr, certFile, keyFile string, server *trevrpc.Server) (trevrpc.ServerListener, error) {
	switch transportName {
	case "quic":
		if certFile == "" || keyFile == "" {
			return nil, errors.New("quic server requires -cert and -key")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return trevrpc.Listen(addr, server, trevrpc.ListenOptions{
			Kind:       trevrpc.TransportQUICGo,
			TLSConfig:  &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN}},
			QUICConfig: quicConfig(),
		})
	case "msquic":
		return trevrpc.Listen(addr, server, trevrpc.ListenOptions{
			Kind: trevrpc.TransportMsQuic,
			MsQuic: trevrpc.MsQuicConfig{
				CertFile:            certFile,
				KeyFile:             keyFile,
				MaxIdleTimeout:      idleTimeout,
				KeepAlive:           keepAlive,
				PeerBidiStreamCount: 128,
			},
		})
	case "webtransport":
		if certFile == "" || keyFile == "" {
			return nil, errors.New("webtransport server requires -cert and -key")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return trevrpc.Listen(addr, server, trevrpc.ListenOptions{
			Kind:       trevrpc.TransportQUICGo,
			TLSConfig:  &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN, http3.NextProtoH3}},
			QUICConfig: quicConfig(),
		})
	default:
		return nil, fmt.Errorf("unsupported transport %q", transportName)
	}
}

func clientTLSConfig(certFile string) *tls.Config {
	_ = certFile
	return &tls.Config{ServerName: "localhost", NextProtos: []string{trevrpc.ALPN}, InsecureSkipVerify: true}
}

func quicConfig() *quic.Config {
	return &quic.Config{MaxIdleTimeout: idleTimeout, KeepAlivePeriod: keepAlive}
}

func waitForShutdown(ctx context.Context, serveDone <-chan error) error {
	select {
	case err := <-serveDone:
		return err
	case <-ctx.Done():
	}

	select {
	case err := <-serveDone:
		return err
	case <-time.After(shutdownTimeout):
		return errors.New("timed out waiting for server shutdown")
	}
}

func portFromAddr(addr net.Addr) int {
	_, portText, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return 0
	}
	return port
}

func warmClient(ctx context.Context, client *greeter.GreeterClient) error {
	if _, err := unary(ctx, client); err != nil {
		return err
	}
	if _, err := serverStreaming(ctx, client); err != nil {
		return err
	}
	if _, err := clientStreaming(ctx, client); err != nil {
		return err
	}
	_, err := bidiStreaming(ctx, client)
	return err
}

func runCase(name string, iterations int, fn func() error) error {
	start := time.Now()
	for range iterations {
		if err := fn(); err != nil {
			return fmt.Errorf("%s failed: %w", name, err)
		}
	}
	elapsed := time.Since(start)
	ops := float64(iterations) / elapsed.Seconds()
	fmt.Printf("%s: %.0f ops/s (%d iterations in %.3fs)\n", name, ops, iterations, elapsed.Seconds())
	return nil
}

func runLongLivedStreamCase(name string, iterations int, fn func() error) error {
	start := time.Now()
	if err := fn(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	elapsed := time.Since(start)
	ops := float64(iterations) / elapsed.Seconds()
	fmt.Printf("%s: %.0f ops/s (%d iterations in %.3fs)\n", name, ops, iterations, elapsed.Seconds())
	return nil
}

func unary(ctx context.Context, client *greeter.GreeterClient) (string, error) {
	response, err := client.SayHello(ctx, request)
	if err != nil {
		return "", err
	}
	if response.Message != requestName {
		return "", fmt.Errorf("unary response = %q, want %q", response.Message, requestName)
	}
	return response.Message, nil
}

func serverStreaming(ctx context.Context, client *greeter.GreeterClient) (int, error) {
	replies, err := client.LotsOfReplies(ctx, request)
	if err != nil {
		return 0, err
	}
	count := 0
	for {
		_, err := replies.Recv()
		if err == io.EOF {
			if count != streamMessageCount {
				_ = replies.Close()
				return 0, fmt.Errorf("server stream count = %d, want %d", count, streamMessageCount)
			}
			return count, replies.Close()
		}
		if err != nil {
			_ = replies.Close()
			return 0, err
		}
		count++
	}
}

func clientStreaming(ctx context.Context, client *greeter.GreeterClient) (string, error) {
	response, err := client.LotsOfGreetingsFromStream(ctx, trevrpc.FromSlice(requests...))
	if err != nil {
		return "", err
	}
	if response.Message != fmt.Sprintf("streamed %d greetings", streamMessageCount) {
		return "", fmt.Errorf("client stream response = %q", response.Message)
	}
	return response.Message, nil
}

func bidiStreaming(ctx context.Context, client *greeter.GreeterClient) (int, error) {
	stream, err := client.BidiHelloFromStream(ctx, trevrpc.FromSlice(requests...))
	if err != nil {
		return 0, err
	}
	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			if count != streamMessageCount {
				_ = stream.Close()
				return 0, fmt.Errorf("bidi stream count = %d, want %d", count, streamMessageCount)
			}
			return count, stream.Close()
		}
		if err != nil {
			_ = stream.Close()
			return 0, err
		}
		count++
	}
}

func bidiLongLived(ctx context.Context, client *greeter.GreeterClient, messageCount int) (int, error) {
	stream, err := client.BidiHelloFromStream(ctx,
		&countedBenchmarkRequests{remaining: messageCount},
		trevrpc.WithMaxResponseMessages(messageCount),
	)
	if err != nil {
		return 0, err
	}

	count := 0
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			if count != messageCount {
				_ = stream.Close()
				return 0, fmt.Errorf("long-lived bidi stream count = %d, want %d", count, messageCount)
			}
			return count, stream.Close()
		}
		if err != nil {
			_ = stream.Close()
			return 0, err
		}
		if response.Message != requestName {
			_ = stream.Close()
			return 0, fmt.Errorf("long-lived bidi response = %q, want %q", response.Message, requestName)
		}
		count++
	}
}

func warmGRPCClient(ctx context.Context, conn *grpc.ClientConn) error {
	if _, err := grpcUnary(ctx, conn); err != nil {
		return err
	}
	if _, err := grpcServerStreaming(ctx, conn); err != nil {
		return err
	}
	if _, err := grpcClientStreaming(ctx, conn); err != nil {
		return err
	}
	_, err := grpcBidiStreaming(ctx, conn)
	return err
}

func grpcUnary(ctx context.Context, conn *grpc.ClientConn) (string, error) {
	response := &greeter.HelloReply{}
	if err := conn.Invoke(ctx, grpcFullMethod(greeter.MethodSayHello), request, response); err != nil {
		return "", err
	}
	if response.Message != requestName {
		return "", fmt.Errorf("grpc unary response = %q, want %q", response.Message, requestName)
	}
	return response.Message, nil
}

func grpcServerStreaming(ctx context.Context, conn *grpc.ClientConn) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfReplies, false, true), grpcFullMethod(greeter.MethodLotsOfReplies))
	if err != nil {
		return 0, err
	}
	if err := stream.SendMsg(request); err != nil {
		return 0, err
	}
	if err := stream.CloseSend(); err != nil {
		return 0, err
	}

	count := 0
	for {
		response := &greeter.HelloReply{}
		err := stream.RecvMsg(response)
		if err == io.EOF {
			if count != streamMessageCount {
				return 0, fmt.Errorf("grpc server stream count = %d, want %d", count, streamMessageCount)
			}
			return count, nil
		}
		if err != nil {
			return 0, err
		}
		count++
	}
}

func grpcClientStreaming(ctx context.Context, conn *grpc.ClientConn) (string, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfGreetings, true, false), grpcFullMethod(greeter.MethodLotsOfGreetings))
	if err != nil {
		return "", err
	}
	for _, request := range requests {
		if err := stream.SendMsg(request); err != nil {
			return "", err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return "", err
	}

	response := &greeter.HelloReply{}
	if err := stream.RecvMsg(response); err != nil {
		return "", err
	}
	if response.Message != fmt.Sprintf("streamed %d greetings", streamMessageCount) {
		return "", fmt.Errorf("grpc client stream response = %q", response.Message)
	}
	return response.Message, nil
}

func grpcBidiStreaming(ctx context.Context, conn *grpc.ClientConn) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodBidiHello, true, true), grpcFullMethod(greeter.MethodBidiHello))
	if err != nil {
		return 0, err
	}
	for _, request := range requests {
		if err := stream.SendMsg(request); err != nil {
			return 0, err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return 0, err
	}

	count := 0
	for {
		response := &greeter.HelloReply{}
		err := stream.RecvMsg(response)
		if err == io.EOF {
			if count != streamMessageCount {
				return 0, fmt.Errorf("grpc bidi stream count = %d, want %d", count, streamMessageCount)
			}
			return count, nil
		}
		if err != nil {
			return 0, err
		}
		count++
	}
}

func grpcBidiLongLived(ctx context.Context, conn *grpc.ClientConn, messageCount int) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodBidiHello, true, true), grpcFullMethod(greeter.MethodBidiHello))
	if err != nil {
		return 0, err
	}
	sendErr := make(chan error, 1)
	go func() {
		for range messageCount {
			if err := stream.SendMsg(request); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- stream.CloseSend()
	}()

	count := 0
	for {
		response := &greeter.HelloReply{}
		err := stream.RecvMsg(response)
		if err == io.EOF {
			if sendErr := <-sendErr; sendErr != nil {
				return 0, sendErr
			}
			if count != messageCount {
				return 0, fmt.Errorf("grpc long-lived bidi stream count = %d, want %d", count, messageCount)
			}
			return count, nil
		}
		if err != nil {
			return 0, err
		}
		if response.Message != requestName {
			return 0, fmt.Errorf("grpc long-lived bidi response = %q, want %q", response.Message, requestName)
		}
		count++
	}
}

type grpcGreeterServer interface {
	SayHello(context.Context, *greeter.HelloRequest) (*greeter.HelloReply, error)
	LotsOfReplies(*greeter.HelloRequest, grpc.ServerStream) error
	LotsOfGreetings(grpc.ServerStream) error
	BidiHello(grpc.ServerStream) error
}

func registerGRPCGreeterServer(server *grpc.Server, implementation grpcGreeterServer) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: greeter.ServiceName,
		HandlerType: (*grpcGreeterServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: greeter.MethodSayHello, Handler: grpcSayHelloHandler},
		},
		Streams: []grpc.StreamDesc{
			{StreamName: greeter.MethodLotsOfReplies, Handler: grpcLotsOfRepliesHandler, ServerStreams: true},
			{StreamName: greeter.MethodLotsOfGreetings, Handler: grpcLotsOfGreetingsHandler, ClientStreams: true},
			{StreamName: greeter.MethodBidiHello, Handler: grpcBidiHelloHandler, ServerStreams: true, ClientStreams: true},
		},
		Metadata: "benchmark.greeter.proto",
	}, implementation)
}

func grpcSayHelloHandler(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := &greeter.HelloRequest{}
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(grpcGreeterServer).SayHello(ctx, request)
	}

	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcFullMethod(greeter.MethodSayHello)}
	handler := func(ctx context.Context, request any) (any, error) {
		return srv.(grpcGreeterServer).SayHello(ctx, request.(*greeter.HelloRequest))
	}
	return interceptor(ctx, request, info, handler)
}

func grpcLotsOfRepliesHandler(srv any, stream grpc.ServerStream) error {
	request := &greeter.HelloRequest{}
	if err := stream.RecvMsg(request); err != nil {
		return err
	}
	return srv.(grpcGreeterServer).LotsOfReplies(request, stream)
}

func grpcLotsOfGreetingsHandler(srv any, stream grpc.ServerStream) error {
	return srv.(grpcGreeterServer).LotsOfGreetings(stream)
}

func grpcBidiHelloHandler(srv any, stream grpc.ServerStream) error {
	return srv.(grpcGreeterServer).BidiHello(stream)
}

func grpcFullMethod(method string) string {
	return "/" + greeter.ServiceName + "/" + method
}

func grpcStreamDesc(method string, clientStreams, serverStreams bool) *grpc.StreamDesc {
	return &grpc.StreamDesc{StreamName: method, ClientStreams: clientStreams, ServerStreams: serverStreams}
}

func makeRequests() []*greeter.HelloRequest {
	requests := make([]*greeter.HelloRequest, streamMessageCount)
	for i := range requests {
		requests[i] = &greeter.HelloRequest{Name: requestName}
	}
	return requests
}

type countedBenchmarkRequests struct {
	remaining int
}

func (s *countedBenchmarkRequests) Recv() (*greeter.HelloRequest, error) {
	if s.remaining <= 0 {
		return nil, io.EOF
	}
	s.remaining--
	return request, nil
}

func (s *countedBenchmarkRequests) Close() error {
	s.remaining = 0
	return nil
}

func (splitGreeter) SayHello(_ context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: request.Name}, nil
}

func (splitGreeter) LotsOfReplies(context.Context, *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	replies := make([]*greeter.HelloReply, streamMessageCount)
	for i := range replies {
		replies[i] = &greeter.HelloReply{Message: "server stream"}
	}
	return trevrpc.FromSlice(replies...), nil
}

func (splitGreeter) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	count := 0
	for {
		_, err := requests.Recv()
		if err == io.EOF {
			return &greeter.HelloReply{Message: fmt.Sprintf("streamed %d greetings", count)}, nil
		}
		if err != nil {
			return nil, err
		}
		count++
	}
}

func (splitGreeter) BidiHello(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return &bidiReplies{requests: requests}, nil
}

type bidiReplies struct {
	requests trevrpc.MessageStream[*greeter.HelloRequest]
}

func (s *bidiReplies) Recv() (*greeter.HelloReply, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}
	return &greeter.HelloReply{Message: request.Name}, nil
}

func (s *bidiReplies) Close() error {
	return s.requests.Close()
}

func (grpcSplitGreeter) SayHello(_ context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: request.Name}, nil
}

func (grpcSplitGreeter) LotsOfReplies(_ *greeter.HelloRequest, stream grpc.ServerStream) error {
	for range streamMessageCount {
		if err := stream.SendMsg(&greeter.HelloReply{Message: "server stream"}); err != nil {
			return err
		}
	}
	return nil
}

func (grpcSplitGreeter) LotsOfGreetings(stream grpc.ServerStream) error {
	count := 0
	for {
		request := &greeter.HelloRequest{}
		err := stream.RecvMsg(request)
		if err == io.EOF {
			return stream.SendMsg(&greeter.HelloReply{Message: fmt.Sprintf("streamed %d greetings", count)})
		}
		if err != nil {
			return err
		}
		count++
	}
}

func (grpcSplitGreeter) BidiHello(stream grpc.ServerStream) error {
	for {
		request := &greeter.HelloRequest{}
		err := stream.RecvMsg(request)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.SendMsg(&greeter.HelloReply{Message: request.Name}); err != nil {
			return err
		}
	}
}

var _ grpcGreeterServer = grpcSplitGreeter{}
