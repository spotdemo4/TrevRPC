package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

type greeterService struct{}

func (greeterService) SayHello(_ context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + request.Name}, nil
}

func (greeterService) LotsOfReplies(_ context.Context, request *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.FromSlice(
		&greeter.HelloReply{Message: "hello, " + request.Name},
		&greeter.HelloReply{Message: "goodbye, " + request.Name},
	), nil
}

func (greeterService) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	var names []string
	for {
		request, err := requests.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		names = append(names, request.Name)
	}

	return &greeter.HelloReply{Message: strings.Join(names, ",")}, nil
}

func (greeterService) BidiHello(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return &echoReplies{requests: requests}, nil
}

type echoReplies struct {
	requests trevrpc.MessageStream[*greeter.HelloRequest]
}

func (s *echoReplies) Recv() (*greeter.HelloReply, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}

	return &greeter.HelloReply{Message: "echo, " + request.Name}, nil
}

func (s *echoReplies) Close() error {
	return s.requests.Close()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "", "server or client")
	addr := flag.String("addr", "127.0.0.1:0", "QUIC address")
	certPath := flag.String("cert", "", "PEM certificate path")
	iterations := flag.Int("iterations", 1, "client iteration count")
	token := flag.String("token", "cross-runtime-token", "bearer token")
	flag.Parse()
	if *iterations < 1 {
		return errors.New("-iterations must be positive")
	}

	switch *mode {
	case "server":
		return runServer(*addr, *certPath, *token)
	case "client":
		return runClient(*addr, *certPath, *token, *iterations)
	case "lifecycle-client":
		return runLifecycleClient(*addr, *certPath, *token, *iterations)
	default:
		return fmt.Errorf("unsupported -mode %q", *mode)
	}
}

func runServer(addr, certPath, token string) error {
	if certPath == "" {
		return errors.New("-cert is required")
	}

	tlsConfig, certPEM, err := serverTLSConfig()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return err
	}

	server := trevrpc.NewServer()
	server.SetAuthorizer(trevrpc.BearerAuthorizer(token))
	greeter.RegisterGreeterServer(server, greeterService{})

	listener, err := quic.ListenAddr(addr, tlsConfig, trevrpc.QUICServerConfig(server.Options(), nil))
	if err != nil {
		return err
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("READY %s\n", listener.Addr())

	return trevrpc.ServeQUIC(ctx, listener, server)
}

func runClient(addr, certPath, token string, iterations int) error {
	for iteration := range iterations {
		if err := runClientIteration(addr, certPath, token); err != nil {
			return fmt.Errorf("client iteration %d: %w", iteration+1, err)
		}
	}

	return nil
}

func runClientIteration(addr, certPath, token string) error {
	if addr == "" {
		return errors.New("-addr is required")
	}
	if certPath == "" {
		return errors.New("-cert is required")
	}

	tlsConfig, err := clientTLSConfig(certPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, addr, tlsConfig, trevrpc.QUICClientConfig(trevrpc.DefaultMaxFrameSize, nil))
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "client complete")

	transport := trevrpc.Advanced.NewRawQUICClient(conn)
	client := greeter.NewGreeterClient(
		transport,
		trevrpc.WithTimeout(5*time.Second),
		trevrpc.WithMetadata("authorization", []byte("Bearer "+token)),
	)

	if _, err := greeter.NewGreeterClient(transport, trevrpc.WithTimeout(5*time.Second)).SayHello(ctx, &greeter.HelloRequest{Name: "unauthenticated"}); trevrpc.StatusFromError(err).Code != trevrpc.CodeUnauthenticated {
		return fmt.Errorf("unauthenticated SayHello returned %v, want Unauthenticated", err)
	}

	if response, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "unary"}); err != nil || response.Message != "hello, unary" {
		return fmt.Errorf("SayHello = %#v, %v", response, err)
	}

	replies, err := client.LotsOfReplies(ctx, &greeter.HelloRequest{Name: "server"})
	if err != nil {
		return err
	}
	if err := expectGoStream(replies, []string{"hello, server", "goodbye, server"}); err != nil {
		return err
	}

	greetings, err := client.LotsOfGreetings(ctx)
	if err != nil {
		return err
	}
	for _, name := range []string{"left", "right"} {
		if err := greetings.Send(&greeter.HelloRequest{Name: name}); err != nil {
			return err
		}
	}
	summary, err := greetings.CloseAndRecv()
	if err != nil || summary.Message != "left,right" {
		return fmt.Errorf("LotsOfGreetings = %#v, %v", summary, err)
	}

	bidi, err := client.BidiHello(ctx)
	if err != nil {
		return err
	}
	for _, name := range []string{"one", "two"} {
		if err := bidi.Send(&greeter.HelloRequest{Name: name}); err != nil {
			return err
		}
	}
	if err := bidi.CloseSend(); err != nil {
		return err
	}
	if err := expectGoStream(bidi, []string{"echo, one", "echo, two"}); err != nil {
		return err
	}

	return expectGoProtocolError(ctx, conn)
}

func runLifecycleClient(addr, certPath, token string, iterations int) error {
	for iteration := range iterations {
		if err := runLifecycleClientIteration(addr, certPath, token); err != nil {
			return fmt.Errorf("lifecycle client iteration %d: %w", iteration+1, err)
		}
	}

	return nil
}

func runLifecycleClientIteration(addr, certPath, token string) error {
	if addr == "" {
		return errors.New("-addr is required")
	}
	if certPath == "" {
		return errors.New("-cert is required")
	}

	tlsConfig, err := clientTLSConfig(certPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, addr, tlsConfig, trevrpc.QUICClientConfig(trevrpc.DefaultMaxFrameSize, nil))
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "lifecycle client complete")

	transport := trevrpc.Advanced.NewRawQUICClient(conn)
	client := greeter.NewGreeterClient(
		transport,
		trevrpc.WithTimeout(5*time.Second),
		trevrpc.WithMetadata("authorization", []byte("Bearer "+token)),
	)

	if response, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "lifecycle-unary"}); err != nil || response.Message != "hello, lifecycle-unary" {
		return fmt.Errorf("lifecycle SayHello = %#v, %v", response, err)
	}

	replies, err := client.LotsOfReplies(ctx, &greeter.HelloRequest{Name: "lifecycle-server-stream"})
	if err != nil {
		return err
	}
	if reply, err := replies.Recv(); err != nil || reply.Message != "hello, lifecycle-server-stream" {
		_ = replies.Close()
		return fmt.Errorf("lifecycle server stream first = %#v, %v", reply, err)
	}
	if err := replies.Close(); err != nil {
		return err
	}

	greetings, err := client.LotsOfGreetings(ctx)
	if err != nil {
		return err
	}
	if err := greetings.Send(&greeter.HelloRequest{Name: "cancelled-client-stream"}); err != nil {
		_ = greetings.Close()
		return err
	}
	if err := greetings.Close(); err != nil {
		return err
	}

	bidi, err := client.BidiHello(ctx)
	if err != nil {
		return err
	}
	if err := bidi.Send(&greeter.HelloRequest{Name: "cancelled-bidi"}); err != nil {
		_ = bidi.Close()
		return err
	}
	if reply, err := bidi.Recv(); err != nil || reply.Message != "echo, cancelled-bidi" {
		_ = bidi.Close()
		return fmt.Errorf("lifecycle bidi first = %#v, %v", reply, err)
	}
	if err := bidi.Close(); err != nil {
		return err
	}

	return nil
}

func expectGoStream(stream trevrpc.MessageStream[*greeter.HelloReply], expected []string) error {
	defer stream.Close()
	for index, want := range expected {
		reply, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream message %d: %w", index, err)
		}
		if reply.Message != want {
			return fmt.Errorf("stream message %d = %q, want %q", index, reply.Message, want)
		}
	}
	if reply, err := stream.Recv(); err != io.EOF {
		return fmt.Errorf("stream final = %#v, %v, want EOF", reply, err)
	}

	return nil
}

func expectGoProtocolError(ctx context.Context, conn *quic.Conn) error {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte{0, 0, 0, 2, 0xff, 0xff}); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	response := &trevrpc.RpcResponse{}
	if err := ReadFrameWithTimeout(stream, response, trevrpc.DefaultMaxFrameSize); err != nil {
		return err
	}
	if code := trevrpc.CodeFromUint32(response.Status); code != trevrpc.CodeInvalidArgument {
		return fmt.Errorf("malformed initial frame status = %v, want InvalidArgument", code)
	}

	return nil
}

func serverTLSConfig() (*tls.Config, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, err
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN}}, certPEM, nil
}

func clientTLSConfig(certPath string) (*tls.Config, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certPEM) {
		return nil, errors.New("failed to parse server certificate")
	}

	return &tls.Config{RootCAs: certPool, ServerName: "localhost", NextProtos: []string{trevrpc.ALPN}}, nil
}

func ReadFrameWithTimeout(stream *quic.Stream, message trevrpc.ProtoMessage, maxFrameSize int) error {
	if err := stream.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	defer stream.SetReadDeadline(time.Time{})

	return trevrpc.ReadFrame(stream, message, maxFrameSize)
}
