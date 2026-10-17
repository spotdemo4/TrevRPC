package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"trev.zip/llc/trevrpc/bench/grpc-go/echopb"
)

const (
	suffixKey  = "interop-suffix"
	headerKey  = "interop-header"
	trailerKey = "interop-trailer"

	// shutdownGrace bounds how long a graceful stop waits for in-flight calls.
	shutdownGrace = 5 * time.Second
)

type serverConfig struct {
	cert, key string
	host      string
	port      int
}

func parseServerFlags(args []string) (serverConfig, error) {
	var cfg serverConfig
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.StringVar(&cfg.cert, "cert", "", "TLS certificate PEM file")
	fs.StringVar(&cfg.key, "key", "", "TLS private key PEM file")
	fs.StringVar(&cfg.host, "host", "127.0.0.1", "listen host")
	fs.IntVar(&cfg.port, "port", 0, "listen port (0 picks a free port)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if cfg.cert == "" || cfg.key == "" {
		return cfg, errors.New("server needs --cert and --key")
	}
	if cfg.port < 0 || cfg.port > 65535 {
		return cfg, fmt.Errorf("invalid --port %d", cfg.port)
	}
	return cfg, nil
}

// runServer serves Echo over gRPC with TLS until stdin reaches EOF or ctx is
// done. Once the listener is accepting it prints exactly one JSON line,
// {"grpc":<port>}, to stdout (which is unbuffered when it is os.Stdout). The server uses grpc-go's defaults (no tuning
// options), so it is the stock gRPC baseline; in particular HTTP/2 flow control
// uses the default initial windows of 64 KiB with grpc-go's dynamic BDP
// estimation enabled, and the server never logs per RPC.
func runServer(ctx context.Context, cfg serverConfig, stdin io.Reader, stdout io.Writer) error {
	creds, err := credentials.NewServerTLSFromFile(cfg.cert, cfg.key)
	if err != nil {
		return fmt.Errorf("load certificate: %w", err)
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.host, strconv.Itoa(cfg.port)))
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.Creds(creds))
	echopb.RegisterEchoServer(srv, echoServer{})

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	port := lis.Addr().(*net.TCPAddr).Port
	line, err := json.Marshal(map[string]int{"grpc": port})
	if err != nil {
		srv.Stop()
		return err
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", line); err != nil {
		srv.Stop()
		return err
	}

	stdinClosed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, stdin)
		close(stdinClosed)
	}()
	select {
	case <-stdinClosed:
	case <-ctx.Done():
	case err := <-serveErr:
		return err
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(shutdownGrace):
		srv.Stop()
	}
	return nil
}

// echoServer implements the interop Echo semantics from interop/README.md.
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
