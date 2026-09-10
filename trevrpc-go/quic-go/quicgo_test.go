package quicgo

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

func TestDialOptionsCloneCopiesMutableInputs(t *testing.T) {
	cert := &trevrpc.TransportCredentials{CertificateChainPEM: []byte("cert"), PrivateKeyPEM: []byte("key"), RootCAPEM: []byte("root")}
	tlsConfig := &tls.Config{ServerName: "example.test", NextProtos: []string{"custom"}}
	quicConfig := &quic.Config{MaxIdleTimeout: time.Second, Versions: []quic.Version{quic.Version1}}
	options := DialOptions{Credentials: cert, TLSConfig: tlsConfig, QUICConfig: quicConfig, WebTransport: WebTransportOptions{
		RequestHeaders:       trevrpc.HeaderFields{{Name: "X-Test", Value: "one"}},
		ApplicationProtocols: []string{"chat"},
	}}
	clone := options.clone()
	cert.CertificateChainPEM[0] = 'X'
	tlsConfig.NextProtos[0] = "changed"
	quicConfig.Versions[0] = quic.Version2
	options.WebTransport.RequestHeaders[0].Value = "changed"
	options.WebTransport.ApplicationProtocols[0] = "changed"
	if got := string(clone.Credentials.CertificateChainPEM); got != "cert" {
		t.Fatalf("credentials clone = %q", got)
	}
	if got := clone.TLSConfig.NextProtos[0]; got != "custom" {
		t.Fatalf("TLS clone = %q", got)
	}
	if got := clone.QUICConfig.Versions[0]; got != quic.Version1 {
		t.Fatalf("QUIC clone = %v", got)
	}
	if got := clone.WebTransport.RequestHeaders[0].Value; got != "one" {
		t.Fatalf("header clone = %q", got)
	}
	if got := clone.WebTransport.ApplicationProtocols[0]; got != "chat" {
		t.Fatalf("protocol clone = %q", got)
	}
}

func TestQUICConfigDisablesZeroRTTAndCapsLimits(t *testing.T) {
	base := &quic.Config{Allow0RTT: true, InitialStreamReceiveWindow: 1024, MaxStreamReceiveWindow: 2048, MaxIncomingUniStreams: 10}
	config := quicConfig(base, trevrpc.TransportConfig{}, trevrpc.TransportLimits{StreamReceiveWindow: 512, ConnectionReceiveWindow: 1024, IncomingBidirectionalStreams: 2}, 128, false, false)
	if config == base || config.Allow0RTT || config.InitialStreamReceiveWindow != 132 || config.MaxStreamReceiveWindow != 132 || config.MaxIncomingStreams != 2 || config.MaxIncomingUniStreams != -1 {
		t.Fatalf("unexpected QUIC config: %+v", config)
	}
	if base.Allow0RTT != true || base.MaxIncomingUniStreams != 10 {
		t.Fatal("base QUIC config was mutated")
	}
}

func TestCloseReasonMapsQUICApplicationError(t *testing.T) {
	err := &quic.ApplicationError{ErrorCode: 7, ErrorMessage: "peer shutdown", Remote: true}
	reason := closeReason(err)
	if !reason.Peer || reason.Local || reason.Clean || reason.ApplicationCode != 7 || reason.Message != "peer shutdown" || !errors.Is(reason.Err, err) {
		t.Fatalf("close reason = %+v", reason)
	}
}

func TestBackendAdaptersExposeExpectedBackend(t *testing.T) {
	b := &backend{}
	if b.Backend() != trevrpc.TransportBackendQUICGo {
		t.Fatalf("backend = %v", b.Backend())
	}
	if _, err := b.NewConnector("http://example.test", trevrpc.BackendDialOptions{}); trevrpc.StatusFromError(err).Code != trevrpc.CodeInvalidArgument {
		t.Fatalf("unsupported scheme error = %v", err)
	}
	if _, err := b.NewConnector("https://example.test/path", trevrpc.BackendDialOptions{WebTransport: trevrpc.WebTransportOptions{RequestHeaders: trevrpc.HeaderFields{{Name: "X-Test\n", Value: "bad"}}}}); trevrpc.StatusFromError(err).Code != trevrpc.CodeInvalidArgument {
		t.Fatalf("invalid header error = %v", err)
	}
}

func TestWebTransportRoundTripAndShutdown(t *testing.T) {
	certificate, key := testCertificate(t)
	server := trevrpc.NewServer()
	server.Route("test.Service", "Echo", func(_ context.Context, body []byte) ([]byte, error) {
		return append([]byte("reply:"), body...), nil
	})
	options := trevrpc.DefaultServerOptions()
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request trevrpc.WebTransportAdmissionRequest) bool {
		return request.Origin == "https://example.test"
	}
	server.SetOptions(options)

	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Credentials: &trevrpc.TransportCredentials{CertificateChainPEM: certificate, PrivateKeyPEM: key},
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- listener.Serve(serveCtx) }()

	channel, err := Dial(context.Background(), "https://"+listener.Addr().String()+"/trevrpc", DialOptions{
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
		WebTransport: WebTransportOptions{RequestHeaders: trevrpc.HeaderFields{
			{Name: "Origin", Value: "https://example.test"},
		}},
	})
	if err != nil {
		cancelServe()
		t.Fatalf("Dial: %v", err)
	}
	response, err := channel.Call(context.Background(), trevrpc.NewRpcRequest("test.Service", "Echo", []byte("hello")))
	if err != nil {
		channel.Close()
		cancelServe()
		t.Fatalf("Call: %v", err)
	}
	if string(response.Body) != "reply:hello" || response.Status != uint32(trevrpc.CodeOK) {
		t.Fatalf("response = %+v", response)
	}

	started := time.Now()
	cancelServe()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WebTransport server shutdown timed out")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("WebTransport shutdown took %v", elapsed)
	}
	if err := channel.Close(); err != nil {
		t.Fatalf("channel close: %v", err)
	}
}

func TestNativeQUICRoundTrip(t *testing.T) {
	certificate, key := testCertificate(t)
	server := trevrpc.NewServer()
	server.Route("test.Service", "Echo", func(_ context.Context, body []byte) ([]byte, error) { return append([]byte("reply:"), body...), nil })
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{Credentials: &trevrpc.TransportCredentials{CertificateChainPEM: certificate, PrivateKeyPEM: key}})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()
	serveErr := make(chan error, 1)
	go func() { serveErr <- listener.Serve(t.Context()) }()

	channel, err := Dial(context.Background(), listener.Addr().String(), DialOptions{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer channel.Close()
	response, err := channel.Call(context.Background(), trevrpc.NewRpcRequest("test.Service", "Echo", []byte("hello")))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(response.Body) != "reply:hello" || response.Status != uint32(trevrpc.CodeOK) {
		t.Fatalf("response = %+v", response)
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("listener close: %v", err)
	}
	select {
	case <-serveErr:
	case <-time.After(5 * time.Second):
		t.Fatal("listener Serve did not stop")
	}
}

func testCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: new(big.Int).SetInt64(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
}
