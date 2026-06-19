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
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

const (
	comparisonRequestName        = "TrevRPC benchmark"
	comparisonStreamMessageCount = 16
	comparisonQUICIdleTimeout    = 10 * time.Minute
	comparisonQUICKeepAlive      = 5 * time.Second
)

var (
	comparisonUnaryReply    = &greeter.HelloReply{Message: "hello TrevRPC benchmark"}
	comparisonSummaryReply  = &greeter.HelloReply{Message: "streamed 16 greetings"}
	comparisonRequests      = makeComparisonRequests()
	comparisonServerReplies = makeComparisonReplies("server stream")
	comparisonBidiReplies   = makeComparisonReplies("bidi stream")
	comparisonStringSink    string
	comparisonCountSink     int
)

func BenchmarkRPCComparison(b *testing.B) {
	env := startRPCComparisonEnvironment(b)

	b.Run("unary_round_trip", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCUnary(b, env.trevrpcClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCUnary(b, env.grpcConn)
		})
	})

	b.Run("server_stream_16_messages", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCServerStreaming(b, env.trevrpcClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCServerStreaming(b, env.grpcConn)
		})
	})

	b.Run("client_stream_16_messages", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCClientStreaming(b, env.trevrpcClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCClientStreaming(b, env.grpcConn)
		})
	})

	b.Run("bidi_stream_16_messages", func(b *testing.B) {
		b.Run("trevrpc_quic", func(b *testing.B) {
			benchmarkTrevRPCBidiStreaming(b, env.trevrpcClient)
		})
		b.Run("grpc_go", func(b *testing.B) {
			benchmarkGRPCBidiStreaming(b, env.grpcConn)
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

func benchmarkTrevRPCServerStreaming(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := trevrpcServerStreamingCall(context.Background(), client)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkGRPCServerStreaming(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := grpcServerStreamingCall(context.Background(), conn)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkTrevRPCClientStreaming(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		message, err := trevrpcClientStreamingCall(context.Background(), client)
		if err != nil {
			b.Fatal(err)
		}
		comparisonStringSink = message
	}
}

func benchmarkGRPCClientStreaming(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		message, err := grpcClientStreamingCall(context.Background(), conn)
		if err != nil {
			b.Fatal(err)
		}
		comparisonStringSink = message
	}
}

func benchmarkTrevRPCBidiStreaming(b *testing.B, client *greeter.GreeterClient) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := trevrpcBidiStreamingCall(context.Background(), client)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

func benchmarkGRPCBidiStreaming(b *testing.B, conn *grpc.ClientConn) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, err := grpcBidiStreamingCall(context.Background(), conn)
		if err != nil {
			b.Fatal(err)
		}
		comparisonCountSink = count
	}
}

type rpcComparisonEnvironment struct {
	trevrpcClient *greeter.GreeterClient
	grpcConn      *grpc.ClientConn
}

func startRPCComparisonEnvironment(b *testing.B) *rpcComparisonEnvironment {
	b.Helper()

	trevrpcClient := startTrevRPCComparisonClient(b)
	grpcConn := startGRPCComparisonClient(b)
	env := &rpcComparisonEnvironment{trevrpcClient: trevrpcClient, grpcConn: grpcConn}
	warmRPCComparisonEnvironment(b, env)
	return env
}

func warmRPCComparisonEnvironment(b *testing.B, env *rpcComparisonEnvironment) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := env.trevrpcClient.SayHello(ctx, comparisonRequests[0]); err != nil {
		b.Fatalf("warm TrevRPC unary: %v", err)
	}
	if _, err := grpcUnaryCall(ctx, env.grpcConn); err != nil {
		b.Fatalf("warm grpc unary: %v", err)
	}
	if _, err := trevrpcServerStreamingCall(ctx, env.trevrpcClient); err != nil {
		b.Fatalf("warm TrevRPC server streaming: %v", err)
	}
	if _, err := grpcServerStreamingCall(ctx, env.grpcConn); err != nil {
		b.Fatalf("warm grpc server streaming: %v", err)
	}
	if _, err := trevrpcClientStreamingCall(ctx, env.trevrpcClient); err != nil {
		b.Fatalf("warm TrevRPC client streaming: %v", err)
	}
	if _, err := grpcClientStreamingCall(ctx, env.grpcConn); err != nil {
		b.Fatalf("warm grpc client streaming: %v", err)
	}
	if _, err := trevrpcBidiStreamingCall(ctx, env.trevrpcClient); err != nil {
		b.Fatalf("warm TrevRPC bidi streaming: %v", err)
	}
	if _, err := grpcBidiStreamingCall(ctx, env.grpcConn); err != nil {
		b.Fatalf("warm grpc bidi streaming: %v", err)
	}
}

func startTrevRPCComparisonClient(b *testing.B) *greeter.GreeterClient {
	b.Helper()

	serverTLS, clientTLS := comparisonTLSConfig(b)
	server := trevrpc.NewServer()
	options := server.Options()
	options.GracefulShutdownTimeout = time.Second
	options.StreamIdleTimeout = 0
	server.SetOptions(options)
	greeter.RegisterGreeterServer(server, comparisonGreeter{})

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

func trevrpcServerStreamingCall(ctx context.Context, client *greeter.GreeterClient) (int, error) {
	replies, err := client.LotsOfReplies(ctx, comparisonRequests[0])
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
			return count, nil
		}
		if err != nil {
			_ = replies.Close()
			return 0, err
		}
		count++
	}
}

func trevrpcClientStreamingCall(ctx context.Context, client *greeter.GreeterClient) (string, error) {
	response, err := client.LotsOfGreetingsFromStream(ctx, trevrpc.FromSlice(comparisonRequests...))
	if err != nil {
		return "", err
	}
	return response.Message, nil
}

func trevrpcBidiStreamingCall(ctx context.Context, client *greeter.GreeterClient) (int, error) {
	stream, err := client.BidiHelloFromStream(ctx, trevrpc.FromSlice(comparisonRequests...))
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

func grpcServerStreamingCall(ctx context.Context, conn *grpc.ClientConn) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfReplies, false, true), grpcFullMethod(greeter.MethodLotsOfReplies))
	if err != nil {
		return 0, err
	}
	if err := stream.SendMsg(comparisonRequests[0]); err != nil {
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
			return count, nil
		}
		if err != nil {
			return 0, err
		}
		count++
	}
}

func grpcClientStreamingCall(ctx context.Context, conn *grpc.ClientConn) (string, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodLotsOfGreetings, true, false), grpcFullMethod(greeter.MethodLotsOfGreetings))
	if err != nil {
		return "", err
	}
	for _, request := range comparisonRequests {
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
	return response.Message, nil
}

func grpcBidiStreamingCall(ctx context.Context, conn *grpc.ClientConn) (int, error) {
	stream, err := conn.NewStream(ctx, grpcStreamDesc(greeter.MethodBidiHello, true, true), grpcFullMethod(greeter.MethodBidiHello))
	if err != nil {
		return 0, err
	}
	for _, request := range comparisonRequests {
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

func (comparisonGreeter) LotsOfReplies(context.Context, *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.FromSlice(comparisonServerReplies...), nil
}

func (comparisonGreeter) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	for {
		_, err := requests.Recv()
		if err == io.EOF {
			return comparisonSummaryReply, nil
		}
		if err != nil {
			return nil, err
		}
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

	reply := comparisonBidiReplies[s.next%len(comparisonBidiReplies)]
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

func (grpcComparisonGreeter) LotsOfReplies(_ *greeter.HelloRequest, stream grpc.ServerStream) error {
	for _, reply := range comparisonServerReplies {
		if err := stream.SendMsg(reply); err != nil {
			return err
		}
	}
	return nil
}

func (grpcComparisonGreeter) LotsOfGreetings(stream grpc.ServerStream) error {
	for {
		request := &greeter.HelloRequest{}
		err := stream.RecvMsg(request)
		if err == io.EOF {
			return stream.SendMsg(comparisonSummaryReply)
		}
		if err != nil {
			return err
		}
	}
}

func (grpcComparisonGreeter) BidiHello(stream grpc.ServerStream) error {
	next := 0
	for {
		request := &greeter.HelloRequest{}
		err := stream.RecvMsg(request)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err := stream.SendMsg(comparisonBidiReplies[next%len(comparisonBidiReplies)]); err != nil {
			return err
		}
		next++
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

func makeComparisonRequests() []*greeter.HelloRequest {
	requests := make([]*greeter.HelloRequest, comparisonStreamMessageCount)
	for index := range requests {
		requests[index] = &greeter.HelloRequest{Name: comparisonRequestName}
	}
	return requests
}

func makeComparisonReplies(prefix string) []*greeter.HelloReply {
	replies := make([]*greeter.HelloReply, comparisonStreamMessageCount)
	for index := range replies {
		replies[index] = &greeter.HelloReply{Message: prefix}
	}
	return replies
}

var _ grpcGreeterServer = grpcComparisonGreeter{}
