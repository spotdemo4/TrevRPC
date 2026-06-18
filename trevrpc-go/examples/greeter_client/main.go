package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/quic-go/quic-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

const serverAddr = "127.0.0.1:50051"

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

	conn, err := quic.DialAddr(ctx, serverAddr, tlsConfig, &quic.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.CloseWithError(0, "client done")

	client := greeter.NewGreeterClient(trevrpc.NewQuinnTransport(conn), trevrpc.WithTimeout(5*time.Second))
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
	for {
		reply, err := replies.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("LotsOfReplies: %s", reply.Message)
	}
}

func clientTLSConfig() (*tls.Config, error) {
	certPath := os.Getenv("TREVRPC_GO_EXAMPLE_CERT")
	if certPath == "" {
		certPath = filepath.Join("target", "trevrpc-go-example-cert.pem")
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
