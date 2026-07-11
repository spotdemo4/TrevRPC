package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log"
	"os"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/internal/examplecert"
)

const (
	serverAddr = "127.0.0.1:50051"
	authToken  = "trevrpc-example-token"
)

func main() {
	name := "TrevRPC"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	tlsConfig, err := clientTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport, err := trevrpc.Dial(ctx, serverAddr, trevrpc.DialOptions{TLSConfig: tlsConfig})
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()
	// Calls fail fast during reconnects, and in-flight calls are never retried or replayed.

	client := greeter.NewGreeterClient(
		transport,
		trevrpc.WithTimeout(5*time.Second),
		trevrpc.WithMetadata("authorization", []byte("Bearer "+authToken)),
	)
	request := &greeter.HelloRequest{Name: name}

	response, err := client.SayHello(ctx, request)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("SayHello: %s", response.Message)

	replies, err := client.LotsOfReplies(ctx, request)
	if err != nil {
		log.Fatal(err)
	}
	for reply, err := range trevrpc.Messages(replies) {
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("LotsOfReplies: %s", reply.Message)
	}

	greetings, err := client.LotsOfGreetings(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, suffix := range []string{"client stream 1", "client stream 2"} {
		if err := greetings.Send(&greeter.HelloRequest{Name: name + " " + suffix}); err != nil {
			log.Fatal(err)
		}
	}
	summary, err := greetings.CloseAndRecv()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("LotsOfGreetings: %s", summary.Message)

	bidi, err := client.BidiHello(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer bidi.Close()

	for _, suffix := range []string{"bidi 1", "bidi 2"} {
		if err := bidi.Send(&greeter.HelloRequest{Name: name + " " + suffix}); err != nil {
			log.Fatal(err)
		}

		reply, err := bidi.Recv()
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("BidiHello: %s", reply.Message)
	}
	if err := bidi.CloseSend(); err != nil {
		log.Fatal(err)
	}
	if reply, err := bidi.Recv(); err != io.EOF {
		if err != nil {
			log.Fatal(err)
		}
		log.Fatalf("unexpected extra BidiHello reply: %s", reply.Message)
	}
}

func clientTLSConfig() (*tls.Config, error) {
	certPath, err := examplecert.Path()
	if err != nil {
		return nil, err
	}

	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certPEM) {
		return nil, x509.CertificateInvalidError{Reason: x509.NotAuthorizedToSign, Detail: "failed to parse server certificate"}
	}

	return &tls.Config{RootCAs: certPool, ServerName: "localhost", NextProtos: []string{trevrpc.ALPN}}, nil
}
