package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
)

const scenarioTimeout = 10 * time.Second

type clientConfig struct {
	addr            string
	caFile          string
	serverName      string
	transport       string
	tcpAddr         string
	expectTransport string
}

var transportPolicies = map[string]trevrpc.TransportPolicy{
	"quic": trevrpc.TransportQUICOnly,
	"tcp":  trevrpc.TransportTCPOnly,
	"auto": trevrpc.TransportAuto,
}

// scenario is one named call from interop/README.md.
type scenario struct {
	name string
	run  func(context.Context, *interoppb.EchoClient) error
}

// runClient runs every scenario against a server over the configured transport
// and prints "ok <name>" or "fail <name>: <reason>" for each. It reports
// whether all scenarios passed.
func runClient(ctx context.Context, config clientConfig, out io.Writer) (bool, error) {
	ca, err := os.ReadFile(config.caFile)
	if err != nil {
		return false, fmt.Errorf("read CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return false, fmt.Errorf("CA file %s contains no certificates", config.caFile)
	}
	dialCtx, cancel := context.WithTimeout(ctx, scenarioTimeout)
	defer cancel()
	channel, err := trevrpc.Dial(dialCtx, config.addr, trevrpc.DialOptions{
		TLSConfig: &tls.Config{RootCAs: roots, ServerName: config.serverName},
		Transport: transportPolicies[config.transport],
		TCPAddr:   config.tcpAddr,
	})
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", config.addr, err)
	}
	defer closeQuietly(channel)
	if want := config.expectTransport; want != "" && string(channel.Transport()) != want {
		return false, fmt.Errorf("connected over %s, want %s", channel.Transport(), want)
	}
	return runScenarios(ctx, channel, out)
}

func runScenarios(ctx context.Context, transport trevrpc.Transport, out io.Writer) (bool, error) {
	client := interoppb.NewEchoClient(transport)
	passed := true
	for _, s := range scenarios {
		scenarioCtx, cancel := context.WithTimeout(ctx, scenarioTimeout)
		err := s.run(scenarioCtx, client)
		cancel()
		line := "ok " + s.name
		if err != nil {
			passed = false
			line = fmt.Sprintf("fail %s: %v", s.name, err)
		}
		if _, err := fmt.Fprintln(out, line); err != nil {
			return false, fmt.Errorf("write result: %w", err)
		}
	}
	return passed, nil
}

var scenarios = []scenario{
	{"unary", func(ctx context.Context, c *interoppb.EchoClient) error {
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: "hello"})
		if err != nil {
			return err
		}
		return expectResponse(response, "hello", 0)
	}},
	{"unary-metadata", func(ctx context.Context, c *interoppb.EchoClient) error {
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: "hello"}, trevrpc.WithMetadata("interop-suffix", []byte("!")))
		if err != nil {
			return err
		}
		return expectResponse(response, "hello!", 0)
	}},
	{"unary-error", func(ctx context.Context, c *interoppb.EchoClient) error {
		_, err := c.Unary(ctx, &interoppb.EchoRequest{FailCode: 5, FailMessage: "missing"})
		return expectStatus(err, trevrpc.CodeNotFound, "missing")
	}},
	{"unary-large", func(ctx context.Context, c *interoppb.EchoClient) error {
		message := strings.Repeat("x", 1<<20)
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: message})
		if err != nil {
			return err
		}
		return expectResponse(response, message, 0)
	}},
	{"client-stream", func(ctx context.Context, c *interoppb.EchoClient) error {
		call, err := c.ClientStream(ctx)
		if err != nil {
			return err
		}
		defer closeQuietly(call)
		for _, message := range []string{"a", "b", "c"} {
			if err := call.Send(&interoppb.EchoRequest{Message: message}); err != nil {
				return err
			}
		}
		response, err := call.CloseAndRecv()
		if err != nil {
			return err
		}
		return expectResponse(response, "abc", 3)
	}},
	{"server-stream", func(ctx context.Context, c *interoppb.EchoClient) error {
		stream, err := c.ServerStream(ctx, &interoppb.EchoRequest{Message: "s", Count: 3})
		if err != nil {
			return err
		}
		defer closeQuietly(stream)
		if err := expectEchoes(stream, "s", 3); err != nil {
			return err
		}
		return expectEnd(stream)
	}},
	{"server-stream-error", func(ctx context.Context, c *interoppb.EchoClient) error {
		stream, err := c.ServerStream(ctx, &interoppb.EchoRequest{Message: "s", Count: 2, FailCode: 9, FailMessage: "stop"})
		if err != nil {
			return err
		}
		defer closeQuietly(stream)
		if err := expectEchoes(stream, "s", 2); err != nil {
			return err
		}
		_, err = stream.Recv()
		return expectStatus(err, trevrpc.CodeFailedPrecondition, "stop")
	}},
	{"bidi", func(ctx context.Context, c *interoppb.EchoClient) error {
		call, err := c.Bidi(ctx)
		if err != nil {
			return err
		}
		defer closeQuietly(call)
		for index, message := range []string{"1", "2", "3"} {
			if err := call.Send(&interoppb.EchoRequest{Message: message}); err != nil {
				return err
			}
			response, err := call.Recv()
			if err != nil {
				return err
			}
			if err := expectResponse(response, message, uint32(index)); err != nil {
				return err
			}
		}
		if err := call.CloseSend(); err != nil {
			return err
		}
		return expectEnd(call)
	}},
	{"bidi-error", func(ctx context.Context, c *interoppb.EchoClient) error {
		call, err := c.Bidi(ctx)
		if err != nil {
			return err
		}
		defer closeQuietly(call)
		if err := call.Send(&interoppb.EchoRequest{Message: "1"}); err != nil {
			return err
		}
		response, err := call.Recv()
		if err != nil {
			return err
		}
		if err := expectResponse(response, "1", 0); err != nil {
			return err
		}
		if err := call.Send(&interoppb.EchoRequest{FailCode: 10, FailMessage: "abort"}); err != nil {
			return err
		}
		_, err = call.Recv()
		return expectStatus(err, trevrpc.CodeAborted, "abort")
	}},
	{"unary-headers", func(ctx context.Context, c *interoppb.EchoClient) error {
		var headers, trailers trevrpc.Metadata
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: "hello"},
			trevrpc.WithMetadata("interop-header", []byte("h")),
			trevrpc.CaptureResponseHeaders(&headers),
			trevrpc.CaptureResponseMetadata(&trailers),
		)
		if err != nil {
			return err
		}
		if err := expectResponse(response, "hello", 0); err != nil {
			return err
		}
		if err := expectMetadata("headers", headers, "interop-header", "h"); err != nil {
			return err
		}
		return expectMetadata("trailing metadata", trailers, "interop-trailer", "h")
	}},
	{"server-stream-headers", func(ctx context.Context, c *interoppb.EchoClient) error {
		stream, err := c.ServerStream(ctx, &interoppb.EchoRequest{Message: "s", Count: 2}, trevrpc.WithMetadata("interop-header", []byte("h")))
		if err != nil {
			return err
		}
		defer closeQuietly(stream)
		headers, err := stream.Headers()
		if err != nil {
			return fmt.Errorf("headers: %w", err)
		}
		if err := expectMetadata("headers", headers, "interop-header", "h"); err != nil {
			return err
		}
		if err := expectEchoes(stream, "s", 2); err != nil {
			return err
		}
		if err := expectEnd(stream); err != nil {
			return err
		}
		status, ok := stream.TerminalStatus()
		if !ok {
			return errors.New("no terminal status after the end of the stream")
		}
		return expectMetadata("trailing metadata", status.Metadata, "interop-trailer", "h")
	}},
}

// expectMetadata checks that metadata holds exactly one entry, key = value.
func expectMetadata(what string, metadata trevrpc.Metadata, key, value string) error {
	if len(metadata) != 1 || string(metadata[key]) != value {
		return fmt.Errorf("%s = %v, want {%s: %q}", what, metadata, key, value)
	}
	return nil
}

func expectResponse(response *interoppb.EchoResponse, message string, index uint32) error {
	if response.Message != message || response.Index != index {
		return fmt.Errorf("got (%.32q, %d), want (%.32q, %d)", response.Message, response.Index, message, index)
	}
	return nil
}

func expectStatus(err error, code trevrpc.Code, message string) error {
	if err == nil {
		return fmt.Errorf("got success, want status %s %q", code, message)
	}
	status := trevrpc.StatusFromError(err)
	if status.Code != code || status.Message != message {
		return fmt.Errorf("got status %s %q, want %s %q", status.Code, status.Message, code, message)
	}
	return nil
}

// echoStream is the receive side shared by server-streaming and bidirectional calls.
type echoStream interface {
	Recv() (*interoppb.EchoResponse, error)
}

func expectEchoes(stream echoStream, message string, count uint32) error {
	for index := range count {
		response, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("response %d: %w", index, err)
		}
		if err := expectResponse(response, message, index); err != nil {
			return fmt.Errorf("response %d: %w", index, err)
		}
	}
	return nil
}

func expectEnd(stream echoStream) error {
	response, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("end of stream: %w", err)
	}
	return fmt.Errorf("unexpected extra response %q", response.Message)
}
