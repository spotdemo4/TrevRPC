package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
	"trev.zip/llc/trevrpc/bench/baseline/echopb/echopbconnect"
)

// TestConnectEchoSemantics checks the interop Echo behavior from
// interop/README.md over the Connect protocol.
func TestConnectEchoSemantics(t *testing.T) {
	srv := startServer(t)
	pem, err := os.ReadFile(srv.ca)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	var protocols http.Protocols
	protocols.SetHTTP2(true)
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"},
		Protocols:       &protocols,
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := echopbconnect.NewEchoClient(connect.NewClient(connecthttp.NewTransport(&http.Client{Transport: transport}, "https://"+srv.addrs["connect"])))
	ctx := t.Context()

	t.Run("unary suffix", func(t *testing.T) {
		req := &echopb.EchoRequest{Message: "hello"}
		ctx, info := connect.NewClientContext(ctx)
		info.RequestHeader().Set(suffixKey, "!")
		resp, err := client.Unary(ctx, req)
		if err != nil || resp.GetMessage() != "hello!" || resp.GetIndex() != 0 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
		if info.ResponseHeader().Has(headerKey) || info.ResponseTrailer().Has(trailerKey) {
			t.Error("unexpected header or trailer without interop-header")
		}
	})
	t.Run("unary headers", func(t *testing.T) {
		req := &echopb.EchoRequest{Message: "hello"}
		ctx, info := connect.NewClientContext(ctx)
		info.RequestHeader().Set(headerKey, "h")
		if _, err := client.Unary(ctx, req); err != nil {
			t.Fatal(err)
		}
		if header, trailer := info.ResponseHeader().Get(headerKey), info.ResponseTrailer().Get(trailerKey); header != "h" || trailer != "h" {
			t.Errorf("header %q, trailer %q", header, trailer)
		}
	})
	t.Run("unary error", func(t *testing.T) {
		_, err := client.Unary(ctx, &echopb.EchoRequest{FailCode: 5, FailMessage: "missing"})
		if connect.CodeOf(err) != connect.CodeNotFound || connectMessage(err) != "missing" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("client stream", func(t *testing.T) {
		stream, err := client.ClientStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range []string{"a", "b", "c"} {
			if err := stream.Send(&echopb.EchoRequest{Message: m}); err != nil {
				t.Fatal(err)
			}
		}
		resp, err := stream.CloseAndReceive()
		if err != nil || resp.GetMessage() != "abc" || resp.GetIndex() != 3 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
	})
	t.Run("server stream headers", func(t *testing.T) {
		req := &echopb.EchoRequest{Message: "s", Count: 2}
		ctx, info := connect.NewClientContext(ctx)
		info.RequestHeader().Set(headerKey, "h")
		stream, err := client.ServerStream(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		for i := range uint32(2) {
			if resp, err := stream.Receive(); err != nil || resp.GetMessage() != "s" || resp.GetIndex() != i {
				t.Fatalf("response %d = %v, err = %v", i, resp, err)
			}
		}
		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("want OK after two responses, err = %v", err)
		}
		if header, trailer := info.ResponseHeader().Get(headerKey), info.ResponseTrailer().Get(trailerKey); header != "h" || trailer != "h" {
			t.Errorf("header %q, trailer %q", header, trailer)
		}
	})
	t.Run("server stream error", func(t *testing.T) {
		stream, err := client.ServerStream(ctx, &echopb.EchoRequest{Message: "s", Count: 2, FailCode: 9, FailMessage: "stop"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		for i := range uint32(2) {
			if resp, err := stream.Receive(); err != nil || resp.GetMessage() != "s" || resp.GetIndex() != i {
				t.Fatalf("response %d = %v, err = %v", i, resp, err)
			}
		}
		if _, err := stream.Receive(); connect.CodeOf(err) != connect.CodeFailedPrecondition || connectMessage(err) != "stop" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bidi error", func(t *testing.T) {
		stream, err := client.Bidi(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		if err := stream.Send(&echopb.EchoRequest{Message: "1"}); err != nil {
			t.Fatal(err)
		}
		if resp, err := stream.Receive(); err != nil || resp.GetMessage() != "1" || resp.GetIndex() != 0 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
		// The server may end the call before this send lands.
		_ = stream.Send(&echopb.EchoRequest{FailCode: 10, FailMessage: "abort"})
		if _, err := stream.Receive(); connect.CodeOf(err) != connect.CodeAborted || connectMessage(err) != "abort" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bidi ok", func(t *testing.T) {
		stream, err := client.Bidi(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		for i, m := range []string{"1", "2", "3"} {
			if err := stream.Send(&echopb.EchoRequest{Message: m}); err != nil {
				t.Fatal(err)
			}
			if resp, err := stream.Receive(); err != nil || resp.GetMessage() != m || int(resp.GetIndex()) != i {
				t.Fatalf("resp = %v, err = %v", resp, err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want EOF", err)
		}
	})
}

// connectMessage returns a connect error's message without its code prefix.
func connectMessage(err error) string {
	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		return connectErr.Message()
	}
	return ""
}
