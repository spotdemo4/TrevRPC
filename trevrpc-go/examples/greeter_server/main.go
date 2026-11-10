package main

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/internal/examplecert"
)

const (
	defaultListenAddr           = "127.0.0.1:50051"
	defaultBrowserExampleOrigin = "http://127.0.0.1:8080"
	defaultAuthToken            = "trevrpc-example-token"
)

var (
	listenAddr              = envOr("TREVRPC_EXAMPLE_ADDR", defaultListenAddr)
	browserExampleOrigin    = envOr("TREVRPC_EXAMPLE_ORIGIN", defaultBrowserExampleOrigin)
	authToken               = envOr("TREVRPC_EXAMPLE_TOKEN", defaultAuthToken)
	webTransportAuthorities = allowedWebTransportAuthorities(listenAddr)
)

type greeterService struct{}

func (greeterService) SayHello(_ context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello " + request.Name}, nil
}

func (greeterService) LotsOfReplies(_ context.Context, request *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.StreamOf(
		&greeter.HelloReply{Message: "hello " + request.Name},
		&greeter.HelloReply{Message: "welcome to TrevRPC over QUIC"},
	), nil
}

func (greeterService) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	var names []string
	for request, err := range trevrpc.Messages(requests) {
		if err != nil {
			return nil, err
		}

		names = append(names, request.Name)
	}

	if len(names) == 0 {
		return &greeter.HelloReply{Message: "hello, nobody"}, nil
	}

	return &greeter.HelloReply{Message: "hello, " + strings.Join(names, ", ")}, nil
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

	return &greeter.HelloReply{Message: "stream hello, " + request.Name}, nil
}

func (s *echoReplies) Close() error {
	return s.requests.Close()
}

func main() {
	certificate, certPath, err := developmentCertificate()
	if err != nil {
		log.Fatal(err)
	}

	server := trevrpc.NewServer()
	server.SetAuthorizer(trevrpc.BearerAuthorizer(authToken))
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = allowBrowserExampleSession
	// The server presents the certificate and publishes its hashes, which
	// browsers trust it by, at /trevrpc/certificate-hashes.
	options.DevelopmentCertificate = certificate
	server.SetOptions(options)
	greeter.RegisterGreeterServer(server, greeterService{})

	listener, err := trevrpc.Listen(listenAddr, server, trevrpc.ListenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("close listener: %v", err)
		}
	}()

	log.Printf("greeter server listening on %s for native QUIC, HTTP/3, and WebTransport", listener.Addr())
	log.Printf("HTTP/3 URL: https://%s/trevrpc", listener.Addr())
	log.Printf("WebTransport URL: https://%s/trevrpc", listener.Addr())
	log.Printf("bearer token: %s", authToken)
	log.Printf("certificate hashes: https://%s/trevrpc/certificate-hashes", listener.Addr())
	log.Printf("wrote client trust certificate to %s", certPath)
	if err := listener.Serve(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func allowBrowserExampleSession(request trevrpc.AdmissionRequest) bool {
	if request.Path != "/trevrpc" {
		return false
	}
	if _, ok := webTransportAuthorities[request.Authority]; !ok {
		return false
	}

	return request.Origin == "" || request.Origin == browserExampleOrigin
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func allowedWebTransportAuthorities(addr string) map[string]struct{} {
	authorities := map[string]struct{}{addr: {}}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return authorities
	}

	switch host {
	case "127.0.0.1":
		authorities[net.JoinHostPort("localhost", port)] = struct{}{}
	case "localhost":
		authorities[net.JoinHostPort("127.0.0.1", port)] = struct{}{}
	}

	return authorities
}

// developmentCertificate generates the server's rotating development
// certificate and writes it where the example client loads its trust from.
func developmentCertificate() (*trevrpc.DevelopmentCertificate, string, error) {
	certificate, err := trevrpc.NewDevelopmentCertificate("localhost", "127.0.0.1")
	if err != nil {
		return nil, "", err
	}
	certPEM, err := certificate.PEM()
	if err != nil {
		return nil, "", err
	}
	certPath, err := examplecert.Path()
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return nil, "", err
	}
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return nil, "", err
	}

	return certificate, certPath, nil
}
