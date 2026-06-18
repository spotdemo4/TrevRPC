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
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/internal/examplecert"
)

const (
	listenAddr           = "127.0.0.1:50051"
	browserExampleOrigin = "http://127.0.0.1:8080"
	authToken            = "trevrpc-example-token"
)

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

func main() {
	tlsConfig, certPath, err := serverTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	listener, err := quic.ListenAddr(listenAddr, tlsConfig, &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	server := trevrpc.NewServer()
	server.SetAuthorizer(trevrpc.BearerAuthorizer(authToken))
	options := server.Options()
	options.EnableWebTransport = true
	options.WebTransportCheckOrigin = allowBrowserExampleOrigin
	server.SetOptions(options)
	greeter.RegisterGreeterServer(server, greeterService{})

	log.Printf("greeter server listening on %s for native QUIC and WebTransport", listener.Addr())
	log.Printf("WebTransport URL: https://%s/trevrpc", listener.Addr())
	log.Printf("bearer token: %s", authToken)
	log.Printf("wrote client trust certificate to %s", certPath)
	if err := trevrpc.ServeQUIC(context.Background(), listener, server); err != nil {
		log.Fatal(err)
	}
}

func allowBrowserExampleOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	return origin == "" || origin == browserExampleOrigin
}

func serverTLSConfig() (*tls.Config, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
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
		return nil, "", err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, "", err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
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

	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN, http3.NextProtoH3}}, certPath, nil
}
