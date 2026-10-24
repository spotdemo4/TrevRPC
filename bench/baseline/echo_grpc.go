package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
)

// newGRPCServer returns a grpc-go server for Echo with grpc-go's defaults (no
// tuning options), so it is the stock gRPC baseline; in particular HTTP/2 flow
// control uses the default initial windows of 64 KiB with grpc-go's dynamic BDP
// estimation enabled. It decompresses gzip and zstd requests, whose
// compressors compression.go registers, and compresses each response with the
// encoding of its request, as grpc-go does by default.
func newGRPCServer(certificate tls.Certificate) *grpc.Server {
	srv := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&certificate)))
	echopb.RegisterEchoServer(srv, echoServer{})
	return srv
}

// echoServer implements the interop Echo semantics from interop/README.md
// with grpc-go.
type echoServer struct {
	echopb.UnimplementedEchoServer
}

// failure converts an EchoRequest's fail_code / fail_message into a status.
func failure(req *echopb.EchoRequest) error {
	return status.Error(codes.Code(req.GetFailCode()), req.GetFailMessage())
}

// headerValue returns the value of the first metadata entry for key.
func headerValue(md metadata.MD, key string) (string, bool) {
	if values := md.Get(key); len(values) > 0 {
		return values[0], true
	}
	return "", false
}

// echoHeaders sends the response headers when the request carries
// interop-header, and returns a function that sets the matching trailer for an
// OK finish. Without that key both are no-ops.
func echoHeaders(ctx context.Context, send func(metadata.MD) error, setTrailer func(metadata.MD)) (func(), error) {
	md, _ := metadata.FromIncomingContext(ctx)
	value, ok := headerValue(md, headerKey)
	if !ok {
		return func() {}, nil
	}
	if err := send(metadata.Pairs(headerKey, value)); err != nil {
		return nil, err
	}
	return func() { setTrailer(metadata.Pairs(trailerKey, value)) }, nil
}

func streamHeaders(stream grpc.ServerStream) (func(), error) {
	return echoHeaders(stream.Context(), stream.SendHeader, stream.SetTrailer)
}

func (echoServer) Unary(ctx context.Context, req *echopb.EchoRequest) (*echopb.EchoResponse, error) {
	if req.GetFailCode() != 0 {
		return nil, failure(req)
	}
	finish, err := echoHeaders(ctx,
		func(md metadata.MD) error { return grpc.SendHeader(ctx, md) },
		func(md metadata.MD) { _ = grpc.SetTrailer(ctx, md) })
	if err != nil {
		return nil, err
	}
	message := req.GetMessage()
	md, _ := metadata.FromIncomingContext(ctx)
	if suffix, ok := headerValue(md, suffixKey); ok {
		message += suffix
	}
	finish()
	return &echopb.EchoResponse{Message: message}, nil
}

func (echoServer) ClientStream(stream grpc.ClientStreamingServer[echopb.EchoRequest, echopb.EchoResponse]) error {
	finish, err := streamHeaders(stream)
	if err != nil {
		return err
	}
	var message strings.Builder
	var count uint32
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if req.GetFailCode() != 0 {
			return failure(req)
		}
		message.WriteString(req.GetMessage())
		count++
	}
	finish()
	return stream.SendAndClose(&echopb.EchoResponse{Message: message.String(), Index: count})
}

func (echoServer) ServerStream(req *echopb.EchoRequest, stream grpc.ServerStreamingServer[echopb.EchoResponse]) error {
	finish, err := streamHeaders(stream)
	if err != nil {
		return err
	}
	// One response value is reused: Send marshals it before returning.
	resp := &echopb.EchoResponse{Message: req.GetMessage()}
	for i := range req.GetCount() {
		resp.Index = i
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	if req.GetFailCode() != 0 {
		return failure(req)
	}
	finish()
	return nil
}

func (echoServer) Bidi(stream grpc.BidiStreamingServer[echopb.EchoRequest, echopb.EchoResponse]) error {
	finish, err := streamHeaders(stream)
	if err != nil {
		return err
	}
	resp := &echopb.EchoResponse{}
	for i := uint32(0); ; i++ {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			finish()
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetFailCode() != 0 {
			return failure(req)
		}
		resp.Message, resp.Index = req.GetMessage(), i
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
