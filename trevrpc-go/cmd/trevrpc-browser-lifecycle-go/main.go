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
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	defaultAddr   = "127.0.0.1:0"
	defaultOrigin = "http://127.0.0.1:8080"
	defaultToken  = "trevrpc-example-token"
	serviceName   = "browser.lifecycle.Lifecycle"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := envOr("TREVRPC_EXAMPLE_ADDR", defaultAddr)
	origin := envOr("TREVRPC_EXAMPLE_ORIGIN", defaultOrigin)
	token := envOr("TREVRPC_EXAMPLE_TOKEN", defaultToken)
	certPath := envOr("TREVRPC_EXAMPLE_CERT", filepath.Join(os.TempDir(), "trevrpc-browser-lifecycle.pem"))

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
	options := server.Options()
	options.EnableWebTransport = true
	authorities := allowedAuthorities(addr)
	options.WebTransportCheckOrigin = func(r *http.Request) bool {
		if _, ok := authorities[r.Host]; !ok {
			return false
		}

		requestOrigin := r.Header.Get("Origin")
		return requestOrigin == "" || requestOrigin == origin
	}
	server.SetOptions(options)
	registerLifecycleRoutes(server)

	listener, err := quic.ListenAddr(addr, tlsConfig, trevrpc.QUICServerConfig(server.Options(), nil))
	if err != nil {
		return err
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("READY https://%s/trevrpc\n", listener.Addr())
	fmt.Printf("certificate written to %s\n", certPath)

	return trevrpc.ServeQUIC(ctx, listener, server)
}

func registerLifecycleRoutes(server *trevrpc.Server) {
	server.RouteStreaming(serviceName, "EarlyOk", trevrpc.RpcKindClientStreaming, func(_ context.Context, _ []byte, _ trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		return trevrpc.FromSlice(encodeValue("early ok")), nil
	})
	server.RouteStreaming(serviceName, "EarlyError", trevrpc.RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, _ trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		return nil, trevrpc.NewStatus(trevrpc.CodePermissionDenied, "remote rejected upload")
	})
	server.RouteStreaming(serviceName, "Pending", trevrpc.RpcKindServerStreaming, func(ctx context.Context, _ []byte, _ trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		return &pendingStream{ctx: ctx, event: "EVENT pending_cancelled"}, nil
	})
	server.RouteStreaming(serviceName, "FirstThenPending", trevrpc.RpcKindServerStreaming, func(ctx context.Context, _ []byte, _ trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		return &firstThenPendingStream{ctx: ctx, first: encodeValue("first"), event: "EVENT response_stream_closed"}, nil
	})
}

type pendingStream struct {
	ctx   context.Context
	event string
	once  sync.Once
}

func (s *pendingStream) Recv() ([]byte, error) {
	<-s.ctx.Done()
	s.logEvent()
	return nil, s.ctx.Err()
}

func (s *pendingStream) Close() error {
	s.logEvent()
	return nil
}

func (s *pendingStream) logEvent() {
	s.once.Do(func() { fmt.Println(s.event) })
}

type firstThenPendingStream struct {
	ctx   context.Context
	first []byte
	event string
	sent  bool
	once  sync.Once
}

func (s *firstThenPendingStream) Recv() ([]byte, error) {
	if !s.sent {
		s.sent = true
		return s.first, nil
	}

	<-s.ctx.Done()
	s.logEvent()
	return nil, s.ctx.Err()
}

func (s *firstThenPendingStream) Close() error {
	s.logEvent()
	return nil
}

func (s *firstThenPendingStream) logEvent() {
	s.once.Do(func() { fmt.Println(s.event) })
}

func encodeValue(value string) []byte {
	if len(value) > 127 {
		panic("test value is too long for single-byte protobuf length")
	}

	body := make([]byte, 0, len(value)+2)
	body = append(body, 0x0a, byte(len(value)))
	body = append(body, value...)
	return body
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func allowedAuthorities(addr string) map[string]struct{} {
	authorities := map[string]struct{}{}
	if override := os.Getenv("TREVRPC_EXAMPLE_AUTHORITIES"); override != "" {
		for authority := range strings.SplitSeq(override, ",") {
			if authority = strings.TrimSpace(authority); authority != "" {
				authorities[authority] = struct{}{}
			}
		}
		return authorities
	}

	authorities[addr] = struct{}{}
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
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, err
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{trevrpc.ALPN, http3.NextProtoH3}}, certPEM, nil
}
