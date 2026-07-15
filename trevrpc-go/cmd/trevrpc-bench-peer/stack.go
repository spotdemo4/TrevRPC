package main

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/internal/benchutil"
)

type benchmarkListener interface {
	Addr() net.Addr
	Serve(context.Context) error
	Close() error
}

func listenBenchmarkServer(config serverConfig) (benchmarkListener, error) {
	switch config.stack {
	case stackNativeQUIC:
		return benchutil.ListenNativeQUIC(config.listen, config.certFile, config.keyFile, newNativeBenchmarkServer())
	case stackGRPCHTTP2:
		tlsCredentials, err := credentials.NewServerTLSFromFile(config.certFile, config.keyFile)
		if err != nil {
			return nil, err
		}
		listener, err := net.Listen("tcp", config.listen)
		if err != nil {
			return nil, err
		}
		server := grpc.NewServer(
			grpc.Creds(tlsCredentials),
			grpc.MaxRecvMsgSize(maxBenchmarkFrameSize),
			grpc.MaxSendMsgSize(maxBenchmarkFrameSize),
			grpc.MaxConcurrentStreams(maxBenchmarkConcurrency),
		)
		benchmarkpb.RegisterBenchmarkServiceServer(server, grpcBenchmarkService{})
		return &grpcBenchmarkListener{listener: listener, server: server}, nil
	default:
		return nil, validateStack(config.stack)
	}
}

type grpcBenchmarkListener struct {
	listener net.Listener
	server   *grpc.Server
}

func (l *grpcBenchmarkListener) Addr() net.Addr { return l.listener.Addr() }

func (l *grpcBenchmarkListener) Serve(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			l.server.GracefulStop()
		case <-done:
		}
	}()
	err := l.server.Serve(l.listener)
	close(done)
	if errors.Is(err, grpc.ErrServerStopped) || (ctx.Err() != nil && errors.Is(err, net.ErrClosed)) {
		return nil
	}
	return err
}

func (l *grpcBenchmarkListener) Close() error {
	l.server.Stop()
	err := l.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func dialBenchmarkClient(ctx context.Context, config clientConfig) (benchmarkClient, func() error, error) {
	switch config.stack {
	case stackNativeQUIC:
		tlsConfig, err := benchutil.VerifiedClientTLSConfig(config.certFile, config.address)
		if err != nil {
			return nil, nil, err
		}
		transport, err := benchutil.DialNativeQUICWithMaxFrameSize(ctx, config.address, tlsConfig, maxBenchmarkFrameSize)
		if err != nil {
			return nil, nil, err
		}
		return nativeBenchmarkClient{
			client:              benchmarkpb.NewNativeBenchmarkServiceClient(transport),
			maxResponseMessages: int(config.messagesPerStream),
		}, transport.Close, nil
	case stackGRPCHTTP2:
		tlsConfig, err := benchutil.VerifiedClientTLSConfigForProtocol(config.certFile, config.address, "h2")
		if err != nil {
			return nil, nil, err
		}
		connection, err := grpc.NewClient(
			"passthrough:///"+config.address,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxBenchmarkFrameSize),
				grpc.MaxCallSendMsgSize(maxBenchmarkFrameSize),
			),
		)
		if err != nil {
			return nil, nil, err
		}
		return grpcBenchmarkClient{client: benchmarkpb.NewBenchmarkServiceClient(connection)}, connection.Close, nil
	default:
		return nil, nil, validateStack(config.stack)
	}
}

type nativeBenchmarkClient struct {
	client              *benchmarkpb.NativeBenchmarkServiceClient
	maxResponseMessages int
}

func (c nativeBenchmarkClient) Unary(ctx context.Context, request *benchmarkpb.BenchmarkRequest) (*benchmarkpb.BenchmarkResponse, error) {
	return c.client.Unary(ctx, request, trevrpc.WithMaxResponseBodySize(maxBenchmarkFrameSize))
}

func (c nativeBenchmarkClient) ClientStream(ctx context.Context) (benchmarkClientStream, error) {
	return c.client.ClientStream(ctx)
}

func (c nativeBenchmarkClient) ServerStream(ctx context.Context, request *benchmarkpb.StreamRequest) (benchmarkResponseStream, error) {
	return c.client.ServerStream(ctx, request,
		trevrpc.WithMaxResponseBodySize(maxBenchmarkFrameSize),
		trevrpc.WithMaxResponseMessages(c.maxResponseMessages),
		trevrpc.WithoutMaxResponseStreamBodySize(),
	)
}

func (c nativeBenchmarkClient) Bidi(ctx context.Context) (benchmarkBidiStream, error) {
	return c.client.Bidi(ctx,
		trevrpc.WithMaxResponseBodySize(maxBenchmarkFrameSize),
		trevrpc.WithMaxResponseMessages(c.maxResponseMessages),
		trevrpc.WithoutMaxResponseStreamBodySize(),
	)
}

type grpcBenchmarkClient struct {
	client benchmarkpb.BenchmarkServiceClient
}

func (c grpcBenchmarkClient) Unary(ctx context.Context, request *benchmarkpb.BenchmarkRequest) (*benchmarkpb.BenchmarkResponse, error) {
	return c.client.Unary(ctx, request)
}

func (c grpcBenchmarkClient) ClientStream(ctx context.Context) (benchmarkClientStream, error) {
	return c.client.ClientStream(ctx)
}

func (c grpcBenchmarkClient) ServerStream(ctx context.Context, request *benchmarkpb.StreamRequest) (benchmarkResponseStream, error) {
	return c.client.ServerStream(ctx, request)
}

func (c grpcBenchmarkClient) Bidi(ctx context.Context) (benchmarkBidiStream, error) {
	return c.client.Bidi(ctx)
}
