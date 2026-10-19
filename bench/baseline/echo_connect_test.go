package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"

	"connectrpc.com/connect"

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
	client := echopbconnect.NewEchoClient(&http.Client{Transport: transport}, "https://"+srv.addrs["connect"])
	ctx := t.Context()

	t.Run("unary suffix", func(t *testing.T) {
		req := connect.NewRequest(&echopb.EchoRequest{Message: "hello"})
		req.Header().Set(suffixKey, "!")
		resp, err := client.Unary(ctx, req)
		if err != nil || resp.Msg.GetMessage() != "hello!" || resp.Msg.GetIndex() != 0 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
		if resp.Header().Get(headerKey) != "" || resp.Trailer().Get(trailerKey) != "" {
			t.Errorf("unexpected header %v / trailer %v without interop-header", resp.Header(), resp.Trailer())
		}
	})
	t.Run("unary headers", func(t *testing.T) {
		req := connect.NewRequest(&echopb.EchoRequest{Message: "hello"})
		req.Header().Set(headerKey, "h")
		resp, err := client.Unary(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header().Get(headerKey) != "h" || resp.Trailer().Get(trailerKey) != "h" {
			t.Errorf("header %v, trailer %v", resp.Header(), resp.Trailer())
		}
	})
	t.Run("unary error", func(t *testing.T) {
		_, err := client.Unary(ctx, connect.NewRequest(&echopb.EchoRequest{FailCode: 5, FailMessage: "missing"}))
		if connect.CodeOf(err) != connect.CodeNotFound || connectMessage(err) != "missing" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("client stream", func(t *testing.T) {
		stream := client.ClientStream(ctx)
		for _, m := range []string{"a", "b", "c"} {
			if err := stream.Send(&echopb.EchoRequest{Message: m}); err != nil {
				t.Fatal(err)
			}
		}
		resp, err := stream.CloseAndReceive()
		if err != nil || resp.Msg.GetMessage() != "abc" || resp.Msg.GetIndex() != 3 {
			t.Fatalf("resp = %v, err = %v", resp, err)
		}
	})
	t.Run("server stream headers", func(t *testing.T) {
		req := connect.NewRequest(&echopb.EchoRequest{Message: "s", Count: 2})
		req.Header().Set(headerKey, "h")
		stream, err := client.ServerStream(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		for i := range uint32(2) {
			if !stream.Receive() || stream.Msg().GetMessage() != "s" || stream.Msg().GetIndex() != i {
				t.Fatalf("response %d = %v, err = %v", i, stream.Msg(), stream.Err())
			}
		}
		if stream.Receive() || stream.Err() != nil {
			t.Fatalf("want OK after two responses, err = %v", stream.Err())
		}
		if stream.ResponseHeader().Get(headerKey) != "h" || stream.ResponseTrailer().Get(trailerKey) != "h" {
			t.Errorf("header %v, trailer %v", stream.ResponseHeader(), stream.ResponseTrailer())
		}
	})
	t.Run("server stream error", func(t *testing.T) {
		stream, err := client.ServerStream(ctx, connect.NewRequest(&echopb.EchoRequest{Message: "s", Count: 2, FailCode: 9, FailMessage: "stop"}))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		for i := range uint32(2) {
			if !stream.Receive() || stream.Msg().GetMessage() != "s" || stream.Msg().GetIndex() != i {
				t.Fatalf("response %d = %v, err = %v", i, stream.Msg(), stream.Err())
			}
		}
		if stream.Receive() || connect.CodeOf(stream.Err()) != connect.CodeFailedPrecondition || connectMessage(stream.Err()) != "stop" {
			t.Fatalf("err = %v", stream.Err())
		}
	})
	t.Run("bidi error", func(t *testing.T) {
		stream := client.Bidi(ctx)
		defer func() { _ = stream.CloseResponse() }()
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
		stream := client.Bidi(ctx)
		defer func() { _ = stream.CloseResponse() }()
		for i, m := range []string{"1", "2", "3"} {
			if err := stream.Send(&echopb.EchoRequest{Message: m}); err != nil {
				t.Fatal(err)
			}
			if resp, err := stream.Receive(); err != nil || resp.GetMessage() != m || int(resp.GetIndex()) != i {
				t.Fatalf("resp = %v, err = %v", resp, err)
			}
		}
		if err := stream.CloseRequest(); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want EOF", err)
		}
	})
}

// connectMessage returns a connect error's message without its code prefix.
func connectMessage(err error) string {
	if connectErr := new(connect.Error); errors.As(err, &connectErr) {
		return connectErr.Message()
	}
	return ""
}
