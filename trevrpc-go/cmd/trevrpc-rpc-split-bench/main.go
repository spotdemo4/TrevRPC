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
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

const (
	latencyStreamMessageCount = 1
	requestName               = "TrevRPC benchmark"
	idleTimeout               = 10 * time.Minute
	keepAlive                 = 5 * time.Second
	shutdownTimeout           = 2 * time.Second
)

var (
	request = &greeter.HelloRequest{Name: requestName}
)

type splitGreeter struct{}

type grpcSplitGreeter struct{}

func main() {
	mode := flag.String("mode", "", "client or server")
	transport := flag.String("transport", "quic", "quic, msquic, webtransport, webtransport-msquic, grpc, or connect")
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
	if err := runLatencyCase("unary_latency", iterations, func() error {
		_, err := unary(ctx, client)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("server_stream_latency", iterations, func() error {
		_, err := serverStreaming(ctx, client, latencyStreamMessageCount)
		return err
	}); err != nil {
		return err
	}
	if err := runMessageThroughputCase("server_stream_throughput", iterations, func() error {
		_, err := serverStreaming(ctx, client, iterations)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("client_stream_latency", iterations, func() error {
		_, err := clientStreaming(ctx, client, latencyStreamMessageCount)
		return err
	}); err != nil {
		return err
	}
	if err := runMessageThroughputCase("client_stream_throughput", iterations, func() error {
		_, err := clientStreaming(ctx, client, iterations)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("bidi_stream_latency", iterations, func() error {
		_, err := bidiStreaming(ctx, client, latencyStreamMessageCount)
		return err
	}); err != nil {
		return err
	}
	return runMessageThroughputCase("bidi_stream_throughput", iterations, func() error {
		_, err := bidiStreaming(ctx, client, iterations)
		return err
	})
}

func runServer(transportName, addr, certFile, keyFile, origin string) error {
	if transportName == "grpc" {
		return runGRPCServer(addr)
	}
	if transportName == "connect" {
		return runConnectServer(addr, origin)
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

func runConnectServer(addr, origin string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	mux := http.NewServeMux()
	codec := connect.WithCodec(trevRPCProtoCodec{})
	mux.Handle(grpcFullMethod(greeter.MethodSayHello), withBenchmarkCORS(connect.NewUnaryHandler(
		grpcFullMethod(greeter.MethodSayHello),
		func(_ context.Context, req *connect.Request[greeter.HelloRequest]) (*connect.Response[greeter.HelloReply], error) {
			return connect.NewResponse(&greeter.HelloReply{Message: req.Msg.Name}), nil
		},
		codec,
	), origin))
	mux.Handle(grpcFullMethod(greeter.MethodLotsOfReplies), withBenchmarkCORS(connect.NewServerStreamHandler(
		grpcFullMethod(greeter.MethodLotsOfReplies),
		func(_ context.Context, req *connect.Request[greeter.HelloRequest], stream *connect.ServerStream[greeter.HelloReply]) error {
			count, message := serverStreamSpecFromName(req.Msg.Name)
			for range count {
				if err := stream.Send(&greeter.HelloReply{Message: message}); err != nil {
					return err
				}
			}
			return nil
		},
		codec,
	), origin))

	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("PORT %d\n", portFromAddr(listener.Addr()))

	select {
	case err := <-serveDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	select {
	case err := <-serveDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-time.After(shutdownTimeout):
		return errors.New("timed out waiting for Connect server shutdown")
	}
}

type trevRPCProtoCodec struct{}

func (trevRPCProtoCodec) Name() string {
	return "proto"
}

func (trevRPCProtoCodec) Marshal(msg any) ([]byte, error) {
	message, ok := msg.(trevrpc.ProtoMessage)
	if !ok {
		return nil, fmt.Errorf("unsupported Connect message type %T", msg)
	}
	return trevrpc.MarshalMessage(message)
}

func (trevRPCProtoCodec) Unmarshal(data []byte, msg any) error {
	message, ok := msg.(trevrpc.ProtoMessage)
	if !ok {
		return fmt.Errorf("unsupported Connect message type %T", msg)
	}
	return trevrpc.UnmarshalMessage(data, message)
}

func withBenchmarkCORS(handler http.Handler, origin string) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if origin == "" {
			response.Header().Set("Access-Control-Allow-Origin", "*")
		} else {
			response.Header().Set("Access-Control-Allow-Origin", origin)
			response.Header().Add("Vary", "Origin")
		}
		response.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		response.Header().Set(
			"Access-Control-Allow-Headers",
			"Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, X-User-Agent",
		)
		response.Header().Set(
			"Access-Control-Expose-Headers",
			"Connect-Content-Encoding, Connect-Accept-Encoding, Grpc-Status, Grpc-Message",
		)
		if request.Method == http.MethodOptions {
			response.WriteHeader(http.StatusNoContent)
			return
		}
		handler.ServeHTTP(response, request)
	})
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
	if err := runLatencyCase("unary_latency", iterations, func() error {
		_, err := grpcUnary(ctx, conn)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("server_stream_latency", iterations, func() error {
		_, err := grpcServerStreaming(ctx, conn, latencyStreamMessageCount)
		return err
	}); err != nil {
		return err
	}
	if err := runMessageThroughputCase("server_stream_throughput", iterations, func() error {
		_, err := grpcServerStreaming(ctx, conn, iterations)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("client_stream_latency", iterations, func() error {
		_, err := grpcClientStreaming(ctx, conn, latencyStreamMessageCount)
		return err
	}); err != nil {
		return err
	}
	if err := runMessageThroughputCase("client_stream_throughput", iterations, func() error {
		_, err := grpcClientStreaming(ctx, conn, iterations)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("bidi_stream_latency", iterations, func() error {
		_, err := grpcBidiStreaming(ctx, conn, latencyStreamMessageCount)
		return err
	}); err != nil {
		return err
	}
	return runMessageThroughputCase("bidi_stream_throughput", iterations, func() error {
		_, err := grpcBidiStreaming(ctx, conn, iterations)
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
				MaxIdleTimeout:            idleTimeout,
				KeepAlive:                 keepAlive,
				PeerBidiStreamCount:       128,
				SkipCertificateValidation: true,
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
	if _, err := serverStreaming(ctx, client, latencyStreamMessageCount); err != nil {
		return err
	}
	if _, err := clientStreaming(ctx, client, latencyStreamMessageCount); err != nil {
		return err
	}
	_, err := bidiStreaming(ctx, client, latencyStreamMessageCount)
	return err
}

func runLatencyCase(name string, iterations int, fn func() error) error {
	start := time.Now()
	for range iterations {
		if err := fn(); err != nil {
			return fmt.Errorf("%s failed: %w", name, err)
		}
	}
	elapsed := time.Since(start)
	latencyUs := float64(elapsed.Nanoseconds()) / 1000 / float64(iterations)
	fmt.Printf("%s: %.3f us/op (%d iterations in %.3fs)\n", name, latencyUs, iterations, elapsed.Seconds())
	return nil
}

func runMessageThroughputCase(name string, messages int, fn func() error) error {
	start := time.Now()
	if err := fn(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	elapsed := time.Since(start)
	messagesPerSecond := float64(messages) / elapsed.Seconds()
	fmt.Printf("%s: %.0f messages/s (%d messages in %.3fs)\n", name, messagesPerSecond, messages, elapsed.Seconds())
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

func serverStreaming(ctx context.Context, client *greeter.GreeterClient, messageCount int) (int, error) {
	replies, err := client.LotsOfReplies(ctx,
		&greeter.HelloRequest{Name: strconv.Itoa(messageCount)},
		trevrpc.WithMaxResponseMessages(messageCount),
	)
	if err != nil {
		return 0, err
	}
	count := 0
	for {
		_, err := replies.Recv()
		if err == io.EOF {
			if count != messageCount {
				_ = replies.Close()
				return 0, fmt.Errorf("server stream count = %d, want %d", count, messageCount)
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

func clientStreaming(ctx context.Context, client *greeter.GreeterClient, messageCount int) (string, error) {
	response, err := client.LotsOfGreetingsFromStream(ctx, &countedBenchmarkRequests{remaining: messageCount})
	if err != nil {
		return "", err
	}
	if response.Message != fmt.Sprintf("streamed %d greetings", messageCount) {
		return "", fmt.Errorf("client stream response = %q", response.Message)
	}
	return response.Message, nil
}

func bidiStreaming(ctx context.Context, client *greeter.GreeterClient, messageCount int) (int, error) {
	stream, err := client.BidiHelloFromStream(ctx,
		&countedBenchmarkRequests{remaining: messageCount},
		trevrpc.WithMaxResponseMessages(messageCount),
	)
	if err != nil {
		return 0, err
	}
	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			if count != messageCount {
				_ = stream.Close()
				return 0, fmt.Errorf("bidi stream count = %d, want %d", count, messageCount)
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

func warmGRPCClient(ctx context.Context, conn *grpc.ClientConn) error {
	if _, err := grpcUnary(ctx, conn); err != nil {
		return err
	}
	if _, err := grpcServerStreaming(ctx, conn, latencyStreamMessageCount); err != nil {
		return err
	}
	if _, err := grpcClientStreaming(ctx, conn, latencyStreamMessageCount); err != nil {
		return err
	}
	_, err := grpcBidiStreaming(ctx, conn, latencyStreamMessageCount)
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

func grpcServerStreaming(ctx context.Context, conn *grpc.ClientConn, messageCount int) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfReplies, false, true), grpcFullMethod(greeter.MethodLotsOfReplies))
	if err != nil {
		return 0, err
	}
	if err := stream.SendMsg(&greeter.HelloRequest{Name: strconv.Itoa(messageCount)}); err != nil {
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
			if count != messageCount {
				return 0, fmt.Errorf("grpc server stream count = %d, want %d", count, messageCount)
			}
			return count, nil
		}
		if err != nil {
			return 0, err
		}
		count++
	}
}

func grpcClientStreaming(ctx context.Context, conn *grpc.ClientConn, messageCount int) (string, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfGreetings, true, false), grpcFullMethod(greeter.MethodLotsOfGreetings))
	if err != nil {
		return "", err
	}
	for range messageCount {
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
	if response.Message != fmt.Sprintf("streamed %d greetings", messageCount) {
		return "", fmt.Errorf("grpc client stream response = %q", response.Message)
	}
	return response.Message, nil
}

func grpcBidiStreaming(ctx context.Context, conn *grpc.ClientConn, messageCount int) (int, error) {
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
				return 0, fmt.Errorf("grpc bidi stream count = %d, want %d", count, messageCount)
			}
			return count, nil
		}
		if err != nil {
			return 0, err
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

func messageCountFromName(name string) int {
	count, err := strconv.Atoi(name)
	if err != nil || count <= 0 {
		return latencyStreamMessageCount
	}
	return count
}

func serverStreamSpecFromName(name string) (int, string) {
	countText, payloadText, ok := strings.Cut(name, ":")
	if !ok {
		return messageCountFromName(name), "server stream"
	}
	payloadBytes, err := strconv.Atoi(payloadText)
	if err != nil || payloadBytes <= 0 {
		return messageCountFromName(countText), "server stream"
	}
	return messageCountFromName(countText), strings.Repeat("x", payloadBytes)
}

func (splitGreeter) LotsOfReplies(_ context.Context, request *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	count, message := serverStreamSpecFromName(request.Name)
	replies := make([]*greeter.HelloReply, count)
	for i := range replies {
		replies[i] = &greeter.HelloReply{Message: message}
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

func (grpcSplitGreeter) LotsOfReplies(request *greeter.HelloRequest, stream grpc.ServerStream) error {
	count, message := serverStreamSpecFromName(request.Name)
	for range count {
		if err := stream.SendMsg(&greeter.HelloReply{Message: message}); err != nil {
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
