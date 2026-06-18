package trevrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

type testMessage struct {
	Value string `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *testMessage) Reset()         { *m = testMessage{} }
func (m *testMessage) String() string { return m.Value }
func (*testMessage) ProtoMessage()    {}

type localTransport struct {
	server *Server
}

func (t localTransport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return t.server.HandleRequest(ctx, request), nil
}

func (t localTransport) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	return t.server.HandleStreamingRequest(ctx, request, requestBody), nil
}

func TestFrameRoundTrip(t *testing.T) {
	request := NewRpcRequest("example.Greeter", "SayHello", []byte("trev"))
	request.Metadata["authorization"] = []byte("ok")

	frame, err := EncodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	decoded := &RpcRequest{}
	if err := ReadFrame(bytes.NewReader(frame), decoded, DefaultMaxFrameSize); err != nil {
		t.Fatalf("decode frame: %v", err)
	}

	if decoded.Service != request.Service || decoded.Method != request.Method || !bytes.Equal(decoded.Body, request.Body) {
		t.Fatalf("decoded request mismatch: %#v", decoded)
	}
}

func TestMetadataValidation(t *testing.T) {
	if err := ValidateMetadata(Metadata{"authorization": []byte("ok")}); err != nil {
		t.Fatalf("valid metadata was rejected: %v", err)
	}

	if err := ValidateMetadata(Metadata{"Authorization": []byte("ok")}); err == nil {
		t.Fatal("uppercase metadata key should be rejected")
	}

	if NormalizeMetadataKey("Authorization") != "authorization" {
		t.Fatal("metadata key was not normalized")
	}
}

func TestUnaryClientServer(t *testing.T) {
	server := NewServer()
	server.SetAuthorizer(NewMetadataValueAuthorizer("Authorization", []byte("ok")))
	RegisterUnary(server, "example.Greeter", "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello " + request.Value}, nil
	})

	response, err := Unary(context.Background(), localTransport{server: server}, "example.Greeter", "SayHello", &testMessage{Value: "TrevRPC"}, func() *testMessage { return &testMessage{} }, WithMetadata("Authorization", []byte("ok")), WithTimeout(time.Second))
	if err != nil {
		t.Fatalf("unary RPC failed: %v", err)
	}

	if response.Value != "hello TrevRPC" {
		t.Fatalf("unexpected response: %q", response.Value)
	}

	_, err = Unary(context.Background(), localTransport{server: server}, "example.Greeter", "SayHello", &testMessage{Value: "TrevRPC"}, func() *testMessage { return &testMessage{} })
	status := StatusFromError(err)
	if status.Code != CodeUnauthenticated {
		t.Fatalf("expected unauthenticated status, got %v", err)
	}
}

func TestStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "LotsOfReplies", RpcKindServerStreaming, func(_ context.Context, body []byte, _ ByteStream) (ByteStream, error) {
		request := &testMessage{}
		if err := UnmarshalMessage(body, request); err != nil {
			return nil, err
		}

		return EncodeStream(FromSlice(
			&testMessage{Value: request.Value + " 1"},
			&testMessage{Value: request.Value + " 2"},
		)), nil
	})

	stream, err := ServerStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "LotsOfReplies", &testMessage{Value: "hello"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("server streaming RPC failed: %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first response failed: %v", err)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second response failed: %v", err)
	}
	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("expected final EOF, got %v", err)
	}

	if first.Value != "hello 1" || second.Value != "hello 2" {
		t.Fatalf("unexpected stream responses: %q %q", first.Value, second.Value)
	}
}

func TestClientStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "LotsOfGreetings", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		var combined strings.Builder
		for {
			body, err := requests.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			request := &testMessage{}
			if err := UnmarshalMessage(body, request); err != nil {
				return nil, err
			}

			combined.WriteString(request.Value)
		}

		return SingleMessageStream(&testMessage{Value: combined.String()}), nil
	})

	response, err := ClientStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "LotsOfGreetings", FromSlice(
		&testMessage{Value: "hello"},
		&testMessage{Value: " world"},
	), func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("client streaming RPC failed: %v", err)
	}

	if response.Value != "hello world" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestQuinnTransportUnary(t *testing.T) {
	server := NewServer()
	RegisterUnary(server, "example.Greeter", "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello " + request.Value}, nil
	})

	serverTLS, clientTLS := testTLSConfig(t)
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = ServeQUIC(ctx, listener, server)
	}()

	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "test complete")

	response, err := Unary(ctx, NewQuinnTransport(conn), "example.Greeter", "SayHello", &testMessage{Value: "QUIC"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("quic unary RPC failed: %v", err)
	}

	if response.Value != "hello QUIC" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func testTLSConfig(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load certificate: %v", err)
	}

	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{ALPN}}
	clientTLS := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{ALPN}}
	return serverTLS, clientTLS
}
