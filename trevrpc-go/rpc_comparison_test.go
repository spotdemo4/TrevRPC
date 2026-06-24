package trevrpc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

const (
	comparisonRequestName         = "TrevRPC benchmark"
	comparisonLatencyMessageCount = 1
	comparisonQUICIdleTimeout     = 10 * time.Minute
	comparisonQUICKeepAlive       = 5 * time.Second
)

var (
	comparisonUnaryReply = &greeter.HelloReply{Message: "hello TrevRPC benchmark"}
	comparisonRequests   = makeComparisonRequests(comparisonLatencyMessageCount)
	comparisonStringSink string
	comparisonCountSink  int
)

func BenchmarkRPCComparison(b *testing.B) {
	env := startRPCComparisonEnvironment(b)

	b.Run("unary_latency", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCUnary(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCUnary(b, env.grpcConn)
		})
	})

	b.Run("server_stream_latency", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCServerStreamLatency(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCServerStreamLatency(b, env.grpcConn)
		})
	})

	b.Run("server_stream_throughput", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCServerStreamThroughput(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCServerStreamThroughput(b, env.grpcConn)
		})
	})

	b.Run("client_stream_latency", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCClientStreamLatency(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCClientStreamLatency(b, env.grpcConn)
		})
	})

	b.Run("client_stream_throughput", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCClientStreamThroughput(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCClientStreamThroughput(b, env.grpcConn)
		})
	})

	b.Run("bidi_stream_latency", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCBidiStreamLatency(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCBidiStreamLatency(b, env.grpcConn)
		})
	})

	b.Run("bidi_stream_throughput", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCBidiStreamThroughput(b, env.trevrpcQUICClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCBidiStreamThroughput(b, env.grpcConn)
		})
	})
}

func benchmarkTrevRPCUnary(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		response, err := client.SayHello(context.Background(), comparisonRequests[0])
		if err != nil {
			b.Fatal(err)
		}
		comparisonStringSink = response.Message
	}
}

func benchmarkGRPCUnary(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		message, err := grpcUnaryCall(context.Background(), conn)
		if err != nil {
			b.Fatal(err)
		}
		comparisonStringSink = message
	}
}

func benchmarkTrevRPCServerStreamLatency(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := trevrpcServerStreamingCall(context.Background(), client, comparisonLatencyMessageCount)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkGRPCServerStreamLatency(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := grpcServerStreamingCall(context.Background(), conn, comparisonLatencyMessageCount)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkTrevRPCServerStreamThroughput(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	count, err := trevrpcServerStreamingCall(context.Background(), client, b.N)
	if err != nil {
		b.Fatal(err)
	}
	comparisonCountSink = count
}

func benchmarkGRPCServerStreamThroughput(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	count, err := grpcServerStreamingCall(context.Background(), conn, b.N)
	if err != nil {
		b.Fatal(err)
	}
	comparisonCountSink = count
}

func benchmarkTrevRPCClientStreamLatency(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		message, err := trevrpcClientStreamingCall(context.Background(), client, comparisonLatencyMessageCount)
		if err != nil {
			b.Fatal(err)
		}
		comparisonStringSink = message
	}
}

func benchmarkGRPCClientStreamLatency(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		message, err := grpcClientStreamingCall(context.Background(), conn, comparisonLatencyMessageCount)
		if err != nil {
			b.Fatal(err)
		}
		comparisonStringSink = message
	}
}

func benchmarkTrevRPCClientStreamThroughput(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	message, err := trevrpcClientStreamingCall(context.Background(), client, b.N)
	if err != nil {
		b.Fatal(err)
	}
	comparisonStringSink = message
}

func benchmarkGRPCClientStreamThroughput(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	message, err := grpcClientStreamingCall(context.Background(), conn, b.N)
	if err != nil {
		b.Fatal(err)
	}
	comparisonStringSink = message
}

func benchmarkTrevRPCBidiStreamLatency(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := trevrpcBidiStreamingCall(context.Background(), client, comparisonLatencyMessageCount)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkGRPCBidiStreamLatency(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := grpcBidiStreamingCall(context.Background(), conn, comparisonLatencyMessageCount)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkTrevRPCBidiStreamThroughput(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	count, err := trevrpcBidiStreamingCall(context.Background(), client, b.N)
	if err != nil {
		b.Fatal(err)
	}
	comparisonCountSink = count
}

func benchmarkGRPCBidiStreamThroughput(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	count, err := grpcBidiStreamingCall(context.Background(), conn, b.N)
	if err != nil {
		b.Fatal(err)
	}
	comparisonCountSink = count
}

type rpcComparisonEnvironment struct {
	trevrpcQUICClient *greeter.GreeterClient
	grpcConn          *grpc.ClientConn
}

func startRPCComparisonEnvironment(b *testing.B) *rpcComparisonEnvironment {
	b.Helper()

	trevrpcQUICClient := startTrevRPCQUICComparisonClient(b)
	grpcConn := startGRPCComparisonClient(b)
	env := &rpcComparisonEnvironment{trevrpcQUICClient: trevrpcQUICClient, grpcConn: grpcConn}
	warmRPCComparisonEnvironment(b, env)
	return env
}

func warmRPCComparisonEnvironment(b *testing.B, env *rpcComparisonEnvironment) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := env.trevrpcQUICClient.SayHello(ctx, comparisonRequests[0]); err != nil {
		b.Fatalf("warm TrevRPC unary: %v", err)
	}
	if _, err := grpcUnaryCall(ctx, env.grpcConn); err != nil {
		b.Fatalf("warm grpc unary: %v", err)
	}
	if _, err := trevrpcServerStreamingCall(ctx, env.trevrpcQUICClient, comparisonLatencyMessageCount); err != nil {
		b.Fatalf("warm TrevRPC server streaming: %v", err)
	}
	if _, err := grpcServerStreamingCall(ctx, env.grpcConn, comparisonLatencyMessageCount); err != nil {
		b.Fatalf("warm grpc server streaming: %v", err)
	}
	if _, err := trevrpcClientStreamingCall(ctx, env.trevrpcQUICClient, comparisonLatencyMessageCount); err != nil {
		b.Fatalf("warm TrevRPC client streaming: %v", err)
	}
	if _, err := grpcClientStreamingCall(ctx, env.grpcConn, comparisonLatencyMessageCount); err != nil {
		b.Fatalf("warm grpc client streaming: %v", err)
	}
	if _, err := trevrpcBidiStreamingCall(ctx, env.trevrpcQUICClient, comparisonLatencyMessageCount); err != nil {
		b.Fatalf("warm TrevRPC bidi streaming: %v", err)
	}
	if _, err := grpcBidiStreamingCall(ctx, env.grpcConn, comparisonLatencyMessageCount); err != nil {
		b.Fatalf("warm grpc bidi streaming: %v", err)
	}
}

func startTrevRPCQUICComparisonClient(b *testing.B) *greeter.GreeterClient {
	b.Helper()

	serverTLS, clientTLS := comparisonTLSConfig(b)
	server := newComparisonTrevRPCServer()

	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, trevrpc.QUICServerConfig(server.Options(), comparisonQUICConfig()))
	if err != nil {
		b.Fatalf("listen TrevRPC QUIC: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- trevrpc.ServeQUIC(ctx, listener, server)
	}()

	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, trevrpc.QUICClientConfig(trevrpc.DefaultMaxFrameSize, comparisonQUICConfig()))
	if err != nil {
		cancel()
		_ = listener.Close()
		b.Fatalf("dial TrevRPC QUIC: %v", err)
	}

	b.Cleanup(func() {
		conn.CloseWithError(0, "benchmark complete")
		cancel()
		_ = listener.Close()
		select {
		case err := <-serveDone:
			if err != nil {
				b.Logf("TrevRPC server stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			b.Log("timed out waiting for TrevRPC server shutdown")
		}
	})

	return greeter.NewGreeterClient(trevrpc.NewQuicClient(conn), trevrpc.WithoutStreamIdleTimeout())
}

func newComparisonTrevRPCServer() *trevrpc.Server {
	server := trevrpc.NewServer()
	options := server.Options()
	options.GracefulShutdownTimeout = time.Second
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = 0
	server.SetOptions(options)
	greeter.RegisterGreeterServer(server, comparisonGreeter{})
	return server
}

func startGRPCComparisonClient(b *testing.B) *grpc.ClientConn {
	b.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen grpc TCP: %v", err)
	}
	server := grpc.NewServer()
	registerGRPCGreeterServer(server, grpcComparisonGreeter{})

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		_ = listener.Close()
		b.Fatalf("dial grpc TCP: %v", err)
	}

	b.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		select {
		case err := <-serveDone:
			if err != nil {
				b.Logf("grpc server stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			b.Log("timed out waiting for grpc server shutdown")
		}
	})

	return conn
}

func trevrpcServerStreamingCall(ctx context.Context, client *greeter.GreeterClient, messageCount int) (int, error) {
	replies, err := client.LotsOfReplies(ctx,
		comparisonRequestWithCount(messageCount),
		trevrpc.WithMaxResponseMessages(messageCount),
	)
	if err != nil {
		return 0, err
	}

	count := 0
	for {
		_, err := replies.Recv()
		if err == io.EOF {
			if err := replies.Close(); err != nil {
				return 0, err
			}
			if count != messageCount {
				return 0, fmt.Errorf("server stream count = %d, want %d", count, messageCount)
			}
			return count, nil
		}
		if err != nil {
			_ = replies.Close()
			return 0, err
		}
		count++
	}
}

func trevrpcClientStreamingCall(ctx context.Context, client *greeter.GreeterClient, messageCount int) (string, error) {
	response, err := client.LotsOfGreetingsFromStream(ctx, &countedComparisonRequests{remaining: messageCount})
	if err != nil {
		return "", err
	}
	if response.Message != strconv.Itoa(messageCount) {
		return "", fmt.Errorf("client stream response = %q, want %d", response.Message, messageCount)
	}
	return response.Message, nil
}

func trevrpcBidiStreamingCall(ctx context.Context, client *greeter.GreeterClient, messageCount int) (int, error) {
	stream, err := client.BidiHelloFromStream(ctx,
		&countedComparisonRequests{remaining: messageCount},
		trevrpc.WithMaxResponseMessages(messageCount),
	)
	if err != nil {
		return 0, err
	}

	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			if err := stream.Close(); err != nil {
				return 0, err
			}
			if count != messageCount {
				return 0, fmt.Errorf("bidi stream count = %d, want %d", count, messageCount)
			}
			return count, nil
		}
		if err != nil {
			_ = stream.Close()
			return 0, err
		}
		count++
	}
}

func grpcUnaryCall(ctx context.Context, conn *grpc.ClientConn) (string, error) {
	response := &greeter.HelloReply{}
	if err := conn.Invoke(ctx, grpcFullMethod(greeter.MethodSayHello), comparisonRequests[0], response); err != nil {
		return "", err
	}
	return response.Message, nil
}

func grpcServerStreamingCall(ctx context.Context, conn *grpc.ClientConn, messageCount int) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfReplies, false, true), grpcFullMethod(greeter.MethodLotsOfReplies))
	if err != nil {
		return 0, err
	}
	if err := stream.SendMsg(comparisonRequestWithCount(messageCount)); err != nil {
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

func grpcClientStreamingCall(ctx context.Context, conn *grpc.ClientConn, messageCount int) (string, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfGreetings, true, false), grpcFullMethod(greeter.MethodLotsOfGreetings))
	if err != nil {
		return "", err
	}
	for range messageCount {
		if err := stream.SendMsg(comparisonRequests[0]); err != nil {
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
	if response.Message != strconv.Itoa(messageCount) {
		return "", fmt.Errorf("grpc client stream response = %q, want %d", response.Message, messageCount)
	}
	return response.Message, nil
}

func grpcBidiStreamingCall(ctx context.Context, conn *grpc.ClientConn, messageCount int) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodBidiHello, true, true), grpcFullMethod(greeter.MethodBidiHello))
	if err != nil {
		return 0, err
	}
	sendErr := make(chan error, 1)
	go func() {
		for range messageCount {
			if err := stream.SendMsg(comparisonRequests[0]); err != nil {
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

type comparisonGreeter struct{}

func (comparisonGreeter) SayHello(context.Context, *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return comparisonUnaryReply, nil
}

func (comparisonGreeter) LotsOfReplies(_ context.Context, request *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.FromSlice(makeComparisonReplies("server stream", comparisonMessageCount(request.Name))...), nil
}

func (comparisonGreeter) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	count := 0
	for {
		_, err := requests.Recv()
		if err == io.EOF {
			return &greeter.HelloReply{Message: strconv.Itoa(count)}, nil
		}
		if err != nil {
			return nil, err
		}
		count++
	}
}

func (comparisonGreeter) BidiHello(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return &comparisonBidiStream{requests: requests}, nil
}

type comparisonBidiStream struct {
	requests trevrpc.MessageStream[*greeter.HelloRequest]
	next     int
}

func (s *comparisonBidiStream) Recv() (*greeter.HelloReply, error) {
	_, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}

	reply := &greeter.HelloReply{Message: strconv.Itoa(s.next)}
	s.next++
	return reply, nil
}

func (s *comparisonBidiStream) Close() error {
	return s.requests.Close()
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

type grpcComparisonGreeter struct{}

func (grpcComparisonGreeter) SayHello(context.Context, *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return comparisonUnaryReply, nil
}

func (grpcComparisonGreeter) LotsOfReplies(request *greeter.HelloRequest, stream grpc.ServerStream) error {
	for range comparisonMessageCount(request.Name) {
		if err := stream.SendMsg(&greeter.HelloReply{Message: "server stream"}); err != nil {
			return err
		}
	}
	return nil
}

func (grpcComparisonGreeter) LotsOfGreetings(stream grpc.ServerStream) error {
	count := 0
	for {
		request := &greeter.HelloRequest{}
		err := stream.RecvMsg(request)
		if err == io.EOF {
			return stream.SendMsg(&greeter.HelloReply{Message: strconv.Itoa(count)})
		}
		if err != nil {
			return err
		}
		count++
	}
}

func (grpcComparisonGreeter) BidiHello(stream grpc.ServerStream) error {
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

func grpcFullMethod(method string) string {
	return "/" + greeter.ServiceName + "/" + method
}

func grpcStreamDesc(method string, clientStreams, serverStreams bool) *grpc.StreamDesc {
	return &grpc.StreamDesc{StreamName: method, ClientStreams: clientStreams, ServerStreams: serverStreams}
}

func comparisonQUICConfig() *quic.Config {
	return &quic.Config{MaxIdleTimeout: comparisonQUICIdleTimeout, KeepAlivePeriod: comparisonQUICKeepAlive}
}

func comparisonTLSConfig(b *testing.B) (*tls.Config, *tls.Config) {
	b.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatalf("generate TLS key: %v", err)
	}
	notBefore := time.Now().Add(-time.Hour)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		b.Fatalf("create TLS certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		b.Fatalf("marshal TLS key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		b.Fatalf("load TLS key pair: %v", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certPEM) {
		b.Fatal("trust TLS certificate")
	}

	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN}}
	clientTLS := &tls.Config{RootCAs: certPool, ServerName: "localhost", NextProtos: []string{trevrpc.ALPN}}
	return serverTLS, clientTLS
}

func makeComparisonRequests(count int) []*greeter.HelloRequest {
	requests := make([]*greeter.HelloRequest, count)
	for index := range requests {
		requests[index] = &greeter.HelloRequest{Name: comparisonRequestName}
	}
	return requests
}

type countedComparisonRequests struct {
	remaining int
}

func (s *countedComparisonRequests) Recv() (*greeter.HelloRequest, error) {
	if s.remaining <= 0 {
		return nil, io.EOF
	}
	s.remaining--
	return comparisonRequests[0], nil
}

func (s *countedComparisonRequests) Close() error {
	s.remaining = 0
	return nil
}

func comparisonRequestWithCount(count int) *greeter.HelloRequest {
	return &greeter.HelloRequest{Name: strconv.Itoa(count)}
}

func comparisonMessageCount(name string) int {
	count, err := strconv.Atoi(name)
	if err != nil || count <= 0 {
		return comparisonLatencyMessageCount
	}
	return count
}

func makeComparisonReplies(prefix string, count int) []*greeter.HelloReply {
	replies := make([]*greeter.HelloReply, count)
	for index := range replies {
		replies[index] = &greeter.HelloReply{Message: prefix}
	}
	return replies
}

var _ grpcGreeterServer = grpcComparisonGreeter{}
