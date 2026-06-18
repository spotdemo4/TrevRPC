package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/quic-go/quic-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/internal/examplecert"
)

const listenAddr = "127.0.0.1:50051"

type greeterService struct{}

func (greeterService) SayHello(_ context.Context, request *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello " + request.Name}, nil
}

func (greeterService) LotsOfReplies(_ context.Context, request *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.FromSlice(
		&greeter.HelloReply{Message: "hello " + request.Name},
		&greeter.HelloReply{Message: "welcome to TrevRPC over QUIC"},
	), nil
}

func main() {
	tlsConfig, certPath, err := serverTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	listener, err := quic.ListenAddr(listenAddr, tlsConfig, &quic.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	server := trevrpc.NewServer()
	greeter.RegisterGreeterServer(server, greeterService{})

	log.Printf("greeter server listening on %s", listener.Addr())
	log.Printf("wrote client trust certificate to %s", certPath)
	if err := trevrpc.ServeQUIC(context.Background(), listener, server); err != nil {
		log.Fatal(err)
	}
}

func serverTLSConfig() (*tls.Config, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", err
	}

	notBefore := time.Now().Add(-time.Hour)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, "", err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
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

	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN}}, certPath, nil
}
