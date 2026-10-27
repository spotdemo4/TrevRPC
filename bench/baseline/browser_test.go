package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"maps"
	"net/http"
	"os"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/quic-go/quic-go/http3"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
	"trev.zip/llc/trevrpc/bench/baseline/echopb/echopbconnect"
)

// serverTLS trusts srv's certificate for localhost.
func serverTLS(t *testing.T, srv *testServer) *tls.Config {
	t.Helper()
	pem, err := os.ReadFile(srv.ca)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	return &tls.Config{RootCAs: pool, ServerName: "localhost"}
}

// TestConnectOverHTTP3 calls Echo over HTTP/3, on the Connect port's UDP twin,
// with the Connect and gRPC-Web protocols that the browser benchmark speaks.
func TestConnectOverHTTP3(t *testing.T) {
	srv := startServer(t)
	transport := &http3.Transport{TLSClientConfig: serverTLS(t, srv)}
	t.Cleanup(func() { _ = transport.Close() })
	httpClient := &http.Client{Transport: transport}
	for name, option := range map[string]connecthttp.Option{
		"connect":  connecthttp.WithProtoJSON(),
		"grpc-web": connecthttp.WithGRPCWeb(),
	} {
		t.Run(name, func(t *testing.T) {
			client := echopbconnect.NewEchoClient(connect.NewClient(connecthttp.NewTransport(httpClient, "https://"+srv.addrs["connect"], option)))
			resp, err := client.Unary(t.Context(), &echopb.EchoRequest{Message: "h3"})
			if err != nil || resp.GetMessage() != "h3" {
				t.Fatalf("unary: resp = %v, err = %v", resp, err)
			}
			stream, err := client.ServerStream(t.Context(), &echopb.EchoRequest{Message: "s", Count: 3})
			if err != nil {
				t.Fatal(err)
			}
			received := 0
			for {
				if _, err = stream.Receive(); err != nil {
					break
				}
				received++
			}
			if !errors.Is(err, io.EOF) || received != 3 {
				t.Fatalf("server stream: %d messages, err = %v", received, err)
			}
		})
	}
}

// TestCrossOriginRequests checks that a page from another origin may call
// Echo: preflights are answered, and responses expose their headers.
func TestCrossOriginRequests(t *testing.T) {
	srv := startServer(t)
	transport := &http.Transport{TLSClientConfig: serverTLS(t, srv), ForceAttemptHTTP2: true}
	t.Cleanup(transport.CloseIdleConnections)
	httpClient := &http.Client{Transport: transport}
	url := "https://" + srv.addrs["connect"] + echopbconnect.EchoUnaryProcedure

	preflight, err := http.NewRequestWithContext(t.Context(), http.MethodOptions, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	preflight.Header.Set("Origin", "http://127.0.0.1:1")
	preflight.Header.Set("Access-Control-Request-Method", http.MethodPost)
	preflight.Header.Set("Access-Control-Request-Headers", "content-type, connect-protocol-version, bench-pad")
	resp, err := httpClient.Do(preflight)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent ||
		resp.Header.Get("Access-Control-Allow-Origin") != "*" ||
		resp.Header.Get("Access-Control-Allow-Headers") != "*" ||
		resp.Header.Get("Access-Control-Max-Age") != corsMaxAge {
		t.Fatalf("preflight answered %d with %v", resp.StatusCode, resp.Header)
	}

	client := echopbconnect.NewEchoClient(connect.NewClient(connecthttp.NewTransport(httpClient, "https://"+srv.addrs["connect"])))
	ctx, info := connect.NewClientContext(t.Context())
	info.RequestHeader().Set("Origin", "http://127.0.0.1:1")
	echoed, err := client.Unary(ctx, &echopb.EchoRequest{Message: "cors"})
	if err != nil || echoed.GetMessage() != "cors" {
		t.Fatalf("resp = %v, err = %v", echoed, err)
	}
	header := info.ResponseHeader()
	if header.Get("Access-Control-Allow-Origin") != "*" || header.Get("Access-Control-Expose-Headers") != "*" {
		t.Errorf("response headers %v do not allow the page to read them", maps.Collect(header.All()))
	}
}
