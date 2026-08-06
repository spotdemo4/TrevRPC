package main

import (
	"context"
	"net"

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
	case stackWebTransport:
		server := newNativeBenchmarkServer()
		options := server.Options()
		options.EnableWebTransport = true
		options.WebTransportAdmission = func(request trevrpc.WebTransportAdmissionRequest) bool {
			return request.Path == trevrpc.DefaultHTTP3Path && request.Origin == config.webTransportOrigin
		}
		server.SetOptions(options)
		return benchutil.ListenWebTransport(config.listen, config.certFile, config.keyFile, server)
	default:
		return nil, validateServerStack(config.stack)
	}
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
	default:
		return nil, nil, validateClientStack(config.stack)
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
