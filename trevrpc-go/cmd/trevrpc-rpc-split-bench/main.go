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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

const (
	latencyStreamMessageCount = 1
	requestName               = "TrevRPC benchmark"
	payloadProfileEnv         = "TREVRPC_BENCH_PAYLOAD_PROFILE"
	metadataProfileEnv        = "TREVRPC_BENCH_METADATA_PROFILE"
	idleTimeout               = 10 * time.Minute
	keepAlive                 = 5 * time.Second
	shutdownTimeout           = 2 * time.Second
)

var (
	payloadProfile  = loadPayloadProfile()
	metadataProfile = loadMetadataProfile()
)

type splitGreeter struct{}

type grpcSplitGreeter struct{}

func main() {
	mode := flag.String("mode", "", "client or server")
	transport := flag.String("transport", "quic", "quic, webtransport, grpc, or connect")
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
		return runGRPCClient(addr, certFile, iterations)
	}

	ctx := context.Background()
	transport, err := dialTransport(ctx, transportName, addr, certFile)
	if err != nil {
		return err
	}
	defer transport.Close()

	client := greeter.NewGreeterClient(transport, trevrpcClientOptions()...)
	if err := warmClient(ctx, client); err != nil {
		return err
	}
	if err := runLatencyCase("unary_latency", iterations, func(index int) error {
		_, err := unary(ctx, client, index)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("server_stream_latency", iterations, func(_ int) error {
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
	if err := runLatencyCase("client_stream_latency", iterations, func(_ int) error {
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
	if err := runLatencyCase("bidi_stream_latency", iterations, func(_ int) error {
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
		return runGRPCServer(addr, certFile, keyFile)
	}
	if transportName == "connect" {
		return runConnectServer(addr, origin, certFile, keyFile)
	}
	server := trevrpc.NewServer()
	options := server.Options()
	options.GracefulShutdownTimeout = time.Second
	options.MaxStreamMessages = -1
	if transportName == "webtransport" {
		options.EnableWebTransport = true
		options.MaxConcurrentStreamsPerConnection = 65535
		options.WebTransportAdmission = func(request trevrpc.WebTransportAdmissionRequest) bool {
			return request.Path == "/trevrpc"
		}
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

func runConnectServer(addr, origin, certFile, keyFile string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	if certFile == "" || keyFile == "" {
		return errors.New("connect server requires -cert and -key")
	}

	mux := http.NewServeMux()
	codec := connect.WithCodec(trevRPCProtoCodec{})
	mux.Handle(grpcFullMethod(greeter.MethodSayHello), withBenchmarkCORS(connect.NewUnaryHandler(
		grpcFullMethod(greeter.MethodSayHello),
		func(_ context.Context, req *connect.Request[greeter.HelloRequest]) (*connect.Response[greeter.HelloReply], error) {
			if err := validateConnectMetadata(req.Header()); err != nil {
				return nil, err
			}
			response := connect.NewResponse(&greeter.HelloReply{Message: req.Msg.Name})
			setConnectResponseMetadata(response.Header())
			return response, nil
		},
		codec,
	), origin))
	mux.Handle(grpcFullMethod(greeter.MethodLotsOfReplies), withBenchmarkCORS(connect.NewServerStreamHandler(
		grpcFullMethod(greeter.MethodLotsOfReplies),
		func(_ context.Context, req *connect.Request[greeter.HelloRequest], stream *connect.ServerStream[greeter.HelloReply]) error {
			if err := validateConnectMetadata(req.Header()); err != nil {
				return err
			}
			setConnectResponseMetadata(stream.ResponseHeader())
			count := messageCountFromName(req.Msg.Name)
			for index := range count {
				if err := stream.Send(&greeter.HelloReply{Message: benchmarkPayload(index)}); err != nil {
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
		serveDone <- server.ServeTLS(listener, certFile, keyFile)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("PORT %d\n", portFromAddr(listener.Addr()))
	if certFile != "" {
		fmt.Printf("CERT %s\n", certFile)
	}

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
			"Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, X-User-Agent, Benchmark-Client, Benchmark-Profile, Benchmark-Trace-Id",
		)
		response.Header().Set(
			"Access-Control-Expose-Headers",
			"Connect-Content-Encoding, Connect-Accept-Encoding, Grpc-Status, Grpc-Message, Benchmark-Response",
		)
		if request.Method == http.MethodOptions {
			response.WriteHeader(http.StatusNoContent)
			return
		}
		handler.ServeHTTP(response, request)
	})
}

func runGRPCClient(addr, certFile string, iterations int) error {
	ctx := context.Background()
	transportCredentials, err := grpcClientTransportCredentials(certFile)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := warmGRPCClient(ctx, conn); err != nil {
		return err
	}
	if err := runLatencyCase("unary_latency", iterations, func(index int) error {
		_, err := grpcUnary(ctx, conn, index)
		return err
	}); err != nil {
		return err
	}
	if err := runLatencyCase("server_stream_latency", iterations, func(_ int) error {
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
	if err := runLatencyCase("client_stream_latency", iterations, func(_ int) error {
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
	if err := runLatencyCase("bidi_stream_latency", iterations, func(_ int) error {
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

func runGRPCServer(addr, certFile, keyFile string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	if certFile == "" || keyFile == "" {
		return errors.New("gRPC server requires -cert and -key")
	}

	transportCredentials, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.Creds(transportCredentials))
	registerGRPCGreeterServer(server, grpcSplitGreeter{})

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("PORT %d\n", portFromAddr(listener.Addr()))
	if certFile != "" {
		fmt.Printf("CERT %s\n", certFile)
	}

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

func grpcClientTransportCredentials(certFile string) (credentials.TransportCredentials, error) {
	if certFile == "" {
		return nil, errors.New("gRPC client requires -cert")
	}
	return credentials.NewClientTLSFromFile(certFile, "localhost")
}

func dialTransport(ctx context.Context, transportName, addr, certFile string) (trevrpc.ClientTransport, error) {
	switch transportName {
	case "quic":
		conn, err := quic.DialAddr(ctx, addr, clientTLSConfig(certFile), trevrpc.QUICClientConfig(trevrpc.DefaultMaxFrameSize, quicConfig()))
		if err != nil {
			return nil, err
		}
		return trevrpc.Advanced.NewRawQUICClient(conn), nil
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
			TLSConfig:  &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN}},
			QUICConfig: quicConfig(),
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

type benchmarkPayloadProfile struct {
	values []string
}

func loadPayloadProfile() benchmarkPayloadProfile {
	switch name := os.Getenv(payloadProfileEnv); name {
	case "", "tiny":
		return benchmarkPayloadProfile{values: []string{requestName}}
	case "small":
		return benchmarkPayloadProfile{values: []string{strings.Repeat("x", 253)}}
	case "medium":
		return benchmarkPayloadProfile{values: []string{strings.Repeat("x", 4093)}}
	case "large":
		return benchmarkPayloadProfile{values: []string{strings.Repeat("x", 65532)}}
	case "mixed":
		return benchmarkPayloadProfile{values: []string{requestName, strings.Repeat("x", 253), strings.Repeat("x", 4093), strings.Repeat("x", 65532)}}
	default:
		fmt.Fprintf(os.Stderr, "unsupported %s %q\n", payloadProfileEnv, name)
		os.Exit(2)
		return benchmarkPayloadProfile{}
	}
}

func benchmarkPayload(index int) string {
	if index < 0 {
		index = 0
	}
	return payloadProfile.values[index%len(payloadProfile.values)]
}

func benchmarkRequest(index int) *greeter.HelloRequest {
	return &greeter.HelloRequest{Name: benchmarkPayload(index)}
}

func loadMetadataProfile() trevrpc.Metadata {
	switch profile := os.Getenv(metadataProfileEnv); profile {
	case "", "none":
		return trevrpc.Metadata{}
	case "production":
		return trevrpc.Metadata{
			"benchmark-client":   []byte("trevrpc-bench"),
			"benchmark-profile":  []byte("production"),
			"benchmark-trace-id": []byte("trevrpc-benchmark"),
		}
	default:
		fmt.Fprintf(os.Stderr, "unsupported %s %q\n", metadataProfileEnv, profile)
		os.Exit(2)
		return trevrpc.Metadata{}
	}
}

func benchmarkResponseMetadata() trevrpc.Metadata {
	if len(metadataProfile) == 0 {
		return trevrpc.Metadata{}
	}
	return trevrpc.Metadata{"benchmark-response": []byte("ok")}
}

func trevrpcClientOptions() []trevrpc.CallOption {
	var options []trevrpc.CallOption
	if len(metadataProfile) > 0 {
		options = append(options, trevrpc.WithTimeout(idleTimeout), trevrpc.WithMetadataMap(metadataProfile))
	}
	return options
}

func grpcBenchmarkContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if len(metadataProfile) == 0 {
		return ctx, func() {}
	}
	ctx, cancel := context.WithTimeout(ctx, idleTimeout)
	return metadata.NewOutgoingContext(ctx, grpcMetadata()), cancel
}

func grpcMetadata() metadata.MD {
	md := metadata.MD{}
	for key, value := range metadataProfile {
		md.Set(key, string(value))
	}
	return md
}

func validateTrevRPCMetadata(actual trevrpc.Metadata) error {
	for key, expected := range metadataProfile {
		value, ok := actual[key]
		if !ok || string(value) != string(expected) {
			return trevrpc.InvalidArgument(fmt.Sprintf("metadata %q = %q, want %q", key, value, expected))
		}
	}
	return nil
}

func validateGRPCMetadata(ctx context.Context) error {
	if len(metadataProfile) == 0 {
		return nil
	}
	actual, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errors.New("missing incoming gRPC metadata")
	}
	for key, expected := range metadataProfile {
		values := actual.Get(key)
		if len(values) == 0 || values[0] != string(expected) {
			return fmt.Errorf("metadata %q = %q, want %q", key, values, expected)
		}
	}
	return nil
}

func validateGRPCResponseMetadata(md metadata.MD) error {
	if len(metadataProfile) == 0 {
		return nil
	}
	values := md.Get("benchmark-response")
	if len(values) == 0 || values[0] != "ok" {
		return fmt.Errorf("response metadata benchmark-response = %q, want ok", values)
	}
	return nil
}

func validateConnectMetadata(header http.Header) error {
	for key, expected := range metadataProfile {
		if got := header.Get(key); got != string(expected) {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("metadata %q = %q, want %q", key, got, expected))
		}
	}
	return nil
}

func setConnectResponseMetadata(header http.Header) {
	for key, value := range benchmarkResponseMetadata() {
		header.Set(key, string(value))
	}
}

func warmClient(ctx context.Context, client *greeter.GreeterClient) error {
	if _, err := unary(ctx, client, 0); err != nil {
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

func runLatencyCase(name string, iterations int, fn func(int) error) error {
	start := time.Now()
	for index := range iterations {
		if err := fn(index); err != nil {
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

func unary(ctx context.Context, client *greeter.GreeterClient, index int) (string, error) {
	request := benchmarkRequest(index)
	response, err := client.SayHello(ctx, request)
	if err != nil {
		return "", err
	}
	if response.Message != request.Name {
		return "", fmt.Errorf("unary response = %q, want %q", response.Message, request.Name)
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
		reply, err := replies.Recv()
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
		if reply.Message != benchmarkPayload(count) {
			_ = replies.Close()
			return 0, fmt.Errorf("server stream reply %d = %q", count, reply.Message)
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
		reply, err := stream.Recv()
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
		if reply.Message != benchmarkPayload(count) {
			_ = stream.Close()
			return 0, fmt.Errorf("bidi reply %d = %q", count, reply.Message)
		}
		count++
	}
}

func warmGRPCClient(ctx context.Context, conn *grpc.ClientConn) error {
	if _, err := grpcUnary(ctx, conn, 0); err != nil {
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

func grpcUnary(ctx context.Context, conn *grpc.ClientConn, index int) (string, error) {
	ctx, cancel := grpcBenchmarkContext(ctx)
	defer cancel()
	request := benchmarkRequest(index)
	response := &greeter.HelloReply{}
	var header metadata.MD
	if err := conn.Invoke(ctx, grpcFullMethod(greeter.MethodSayHello), request, response, grpc.Header(&header)); err != nil {
		return "", err
	}
	if err := validateGRPCResponseMetadata(header); err != nil {
		return "", err
	}
	if response.Message != request.Name {
		return "", fmt.Errorf("grpc unary response = %q, want %q", response.Message, request.Name)
	}
	return response.Message, nil
}

func grpcServerStreaming(ctx context.Context, conn *grpc.ClientConn, messageCount int) (int, error) {
	ctx, cancel := grpcBenchmarkContext(ctx)
	defer cancel()
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
	if len(metadataProfile) > 0 {
		if header, err := stream.Header(); err != nil {
			return 0, err
		} else if err := validateGRPCResponseMetadata(header); err != nil {
			return 0, err
		}
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
		if response.Message != benchmarkPayload(count) {
			return 0, fmt.Errorf("grpc server stream reply %d = %q", count, response.Message)
		}
		count++
	}
}

func grpcClientStreaming(ctx context.Context, conn *grpc.ClientConn, messageCount int) (string, error) {
	ctx, cancel := grpcBenchmarkContext(ctx)
	defer cancel()
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfGreetings, true, false), grpcFullMethod(greeter.MethodLotsOfGreetings))
	if err != nil {
		return "", err
	}
	for index := range messageCount {
		if err := stream.SendMsg(benchmarkRequest(index)); err != nil {
			return "", err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return "", err
	}
	if len(metadataProfile) > 0 {
		if header, err := stream.Header(); err != nil {
			return "", err
		} else if err := validateGRPCResponseMetadata(header); err != nil {
			return "", err
		}
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
	ctx, cancel := grpcBenchmarkContext(ctx)
	defer cancel()
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodBidiHello, true, true), grpcFullMethod(greeter.MethodBidiHello))
	if err != nil {
		return 0, err
	}
	sendErr := make(chan error, 1)
	go func() {
		for index := range messageCount {
			if err := stream.SendMsg(benchmarkRequest(index)); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- stream.CloseSend()
	}()
	if len(metadataProfile) > 0 {
		if header, err := stream.Header(); err != nil {
			return 0, err
		} else if err := validateGRPCResponseMetadata(header); err != nil {
			return 0, err
		}
	}

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
		if response.Message != benchmarkPayload(count) {
			return 0, fmt.Errorf("grpc bidi reply %d = %q", count, response.Message)
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
	sent      int
}

func (s *countedBenchmarkRequests) Recv() (*greeter.HelloRequest, error) {
	if s.remaining <= 0 {
		return nil, io.EOF
	}
	request := benchmarkRequest(s.sent)
	s.sent++
	s.remaining--
	return request, nil
}

func (s *countedBenchmarkRequests) Close() error {
	s.remaining = 0
	return nil
}

func (splitGreeter) SayHello(ctx context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	if err := validateTrevRPCMetadata(trevrpc.RequestMetadataFromContext(ctx)); err != nil {
		return nil, err
	}
	return &greeter.HelloReply{Message: request.Name}, nil
}

func messageCountFromName(name string) int {
	count, err := strconv.Atoi(name)
	if err != nil || count <= 0 {
		return latencyStreamMessageCount
	}
	return count
}

func (splitGreeter) LotsOfReplies(ctx context.Context, request *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	if err := validateTrevRPCMetadata(trevrpc.RequestMetadataFromContext(ctx)); err != nil {
		return nil, err
	}
	count := messageCountFromName(request.Name)
	replies := make([]*greeter.HelloReply, count)
	for i := range replies {
		replies[i] = &greeter.HelloReply{Message: benchmarkPayload(i)}
	}
	return trevrpc.FromSlice(replies...), nil
}

func (splitGreeter) LotsOfGreetings(ctx context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	if err := validateTrevRPCMetadata(trevrpc.RequestMetadataFromContext(ctx)); err != nil {
		return nil, err
	}
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

func (splitGreeter) BidiHello(ctx context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	if err := validateTrevRPCMetadata(trevrpc.RequestMetadataFromContext(ctx)); err != nil {
		return nil, err
	}
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

func (grpcSplitGreeter) SayHello(ctx context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	if err := validateGRPCMetadata(ctx); err != nil {
		return nil, err
	}
	if len(metadataProfile) > 0 {
		_ = grpc.SetHeader(ctx, metadata.Pairs("benchmark-response", "ok"))
	}
	return &greeter.HelloReply{Message: request.Name}, nil
}

func (grpcSplitGreeter) LotsOfReplies(request *greeter.HelloRequest, stream grpc.ServerStream) error {
	if err := validateGRPCMetadata(stream.Context()); err != nil {
		return err
	}
	if len(metadataProfile) > 0 {
		_ = stream.SetHeader(metadata.Pairs("benchmark-response", "ok"))
	}
	count := messageCountFromName(request.Name)
	for index := range count {
		if err := stream.SendMsg(&greeter.HelloReply{Message: benchmarkPayload(index)}); err != nil {
			return err
		}
	}
	return nil
}

func (grpcSplitGreeter) LotsOfGreetings(stream grpc.ServerStream) error {
	if err := validateGRPCMetadata(stream.Context()); err != nil {
		return err
	}
	if len(metadataProfile) > 0 {
		_ = stream.SetHeader(metadata.Pairs("benchmark-response", "ok"))
	}
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
	if err := validateGRPCMetadata(stream.Context()); err != nil {
		return err
	}
	if len(metadataProfile) > 0 {
		_ = stream.SetHeader(metadata.Pairs("benchmark-response", "ok"))
	}
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
