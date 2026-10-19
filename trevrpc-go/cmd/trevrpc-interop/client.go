package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
)

const scenarioTimeout = 10 * time.Second

// connectionConfig holds the flags of the subcommands that dial a server.
type connectionConfig struct {
	addr       string
	caFile     string
	serverName string
	transport  string
	tcpAddr    string
}

// register defines the connection flags. transportUsage describes the values
// --transport accepts.
func (c *connectionConfig) register(flags *flag.FlagSet, transportUsage string) {
	flags.StringVar(&c.addr, "addr", "", "server host:port")
	flags.StringVar(&c.caFile, "ca", "", "trusted server certificate PEM file")
	flags.StringVar(&c.serverName, "server-name", "localhost", "TLS server name")
	flags.StringVar(&c.transport, "transport", "quic", transportUsage)
	flags.StringVar(&c.tcpAddr, "tcp-addr", "", "server TCP host:port (defaults to --addr)")
}

// check reports a usage error for a missing address or CA, or for a transport
// that is not one of allowed. command names the subcommand in the message.
func (c *connectionConfig) check(command string, allowed ...string) error {
	if c.addr == "" || c.caFile == "" {
		return fmt.Errorf("%s: --addr and --ca are required", command)
	}
	if !slices.Contains(allowed, c.transport) {
		return fmt.Errorf("%s: unknown --transport %q", command, c.transport)
	}
	return nil
}

// dial connects to the server with the configured trust and transport policy.
// options carries any further settings; its TLS, transport and TCP address
// fields are overwritten. The context bounds the dial only.
func (c *connectionConfig) dial(ctx context.Context, options trevrpc.DialOptions) (*trevrpc.Channel, error) {
	ca, err := os.ReadFile(c.caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("CA file %s contains no certificates", c.caFile)
	}
	options.TLSConfig = &tls.Config{RootCAs: roots, ServerName: c.serverName}
	options.Transport = transportPolicies[c.transport]
	options.TCPAddr = c.tcpAddr
	dialCtx, cancel := context.WithTimeout(ctx, scenarioTimeout)
	defer cancel()
	channel, err := trevrpc.Dial(dialCtx, c.addr, options)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.addr, err)
	}
	return channel, nil
}

type clientConfig struct {
	connectionConfig
	expectTransport string
}

var transportPolicies = map[string]trevrpc.TransportPolicy{
	"quic": trevrpc.TransportQUICOnly,
	"tcp":  trevrpc.TransportTCPOnly,
	"auto": trevrpc.TransportAuto,
}

// scenarioEnv is what a scenario runs against.
type scenarioEnv struct {
	echo       *interoppb.EchoClient
	unreliable *interoppb.UnreliableEchoClient
	// transport is the transport of the connection.
	transport trevrpc.TransportKind
	// datagramFrames reports how many QUIC DATAGRAM frames the connection has
	// received, or is nil when the client does not watch for them.
	datagramFrames func() uint64
}

// scenario is one named call from interop/README.md.
type scenario struct {
	name string
	run  func(context.Context, *scenarioEnv) error
}

// runClient runs every scenario against a server over the configured transport
// and prints "ok <name>" or "fail <name>: <reason>" for each. It reports
// whether all scenarios passed.
func runClient(ctx context.Context, config clientConfig, out io.Writer) (bool, error) {
	// Over QUIC alone, the connection is watched for DATAGRAM frames, which
	// proves that the server's datagram messages arrived.
	var dialOptions trevrpc.DialOptions
	var frames *wireCounter
	if config.transport == "quic" {
		frames = new(wireCounter)
		dialOptions.QUICConfig = frames.quicConfig()
	}
	channel, err := config.dial(ctx, dialOptions)
	if err != nil {
		return false, err
	}
	defer closeQuietly(channel)
	if want := config.expectTransport; want != "" && string(channel.Transport()) != want {
		return false, fmt.Errorf("connected over %s, want %s", channel.Transport(), want)
	}
	env := &scenarioEnv{
		echo:       interoppb.NewEchoClient(channel),
		unreliable: interoppb.NewUnreliableEchoClient(channel),
		transport:  channel.Transport(),
	}
	if frames != nil {
		env.datagramFrames = frames.datagramFrames.Load
	}
	return runScenarios(ctx, env, out)
}

func runScenarios(ctx context.Context, env *scenarioEnv, out io.Writer) (bool, error) {
	passed := true
	for _, s := range scenarios {
		scenarioCtx, cancel := context.WithTimeout(ctx, scenarioTimeout)
		err := s.run(scenarioCtx, env)
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
	{"unary", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: "hello"})
		if err != nil {
			return err
		}
		return expectResponse(response, "hello", 0)
	}},
	{"unary-metadata", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: "hello"}, trevrpc.WithMetadata("interop-suffix", []byte("!")))
		if err != nil {
			return err
		}
		return expectResponse(response, "hello!", 0)
	}},
	{"unary-error", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
		_, err := c.Unary(ctx, &interoppb.EchoRequest{FailCode: 5, FailMessage: "missing"})
		return expectStatus(err, trevrpc.CodeNotFound, "missing")
	}},
	{"unary-large", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
		message := strings.Repeat("x", 1<<20)
		response, err := c.Unary(ctx, &interoppb.EchoRequest{Message: message})
		if err != nil {
			return err
		}
		return expectResponse(response, message, 0)
	}},
	{"client-stream", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
	{"server-stream", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
	{"server-stream-error", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
	{"bidi", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
	{"bidi-error", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
		// The server may end the call before this send returns; the send then
		// reports the same status.
		if err := call.Send(&interoppb.EchoRequest{FailCode: 10, FailMessage: "abort"}); err != nil {
			if err := expectStatus(err, trevrpc.CodeAborted, "abort"); err != nil {
				return err
			}
		}
		_, err = call.Recv()
		return expectStatus(err, trevrpc.CodeAborted, "abort")
	}},
	{"unary-headers", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
	{"server-stream-headers", func(ctx context.Context, env *scenarioEnv) error {
		c := env.echo
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
	{"unreliable", runUnreliable},
}

// Parameters of the unreliable scenario: the number of small requests, and
// the size of the large one, which cannot travel as a datagram.
const (
	unreliableRequests  = 50
	unreliableLargeSize = 4096
)

// runUnreliable is the unreliable scenario of interop/README.md.
func runUnreliable(ctx context.Context, env *scenarioEnv) error {
	call, err := env.unreliable.Bidi(ctx)
	if err != nil {
		return err
	}
	defer closeQuietly(call)

	// Every message that was sent, and none was echoed yet.
	pending := make(map[string]bool)
	large := strings.Repeat("x", unreliableLargeSize)
	messages := make([]string, 0, unreliableRequests+1)
	for i := range unreliableRequests {
		messages = append(messages, strconv.Itoa(i))
	}
	messages = append(messages, large)
	for _, message := range messages {
		pending[message] = true
		if err := call.Send(&interoppb.EchoRequest{Message: message, Count: 1}); err != nil {
			return err
		}
	}
	if err := call.CloseSend(); err != nil {
		return err
	}

	responses := 0
	for {
		response, err := call.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("response %d: %w", responses, err)
		}
		if !pending[response.Message] || response.Index != 0 {
			return fmt.Errorf("unexpected or repeated response (%.32q, %d)", response.Message, response.Index)
		}
		delete(pending, response.Message)
		responses++
	}
	if pending[large] {
		return errors.New("the 4096-byte echo, which cannot be a datagram, is missing")
	}

	status, ok := call.TerminalStatus()
	if !ok {
		return errors.New("no terminal status after the end of the stream")
	}
	received, err := metadataCount(status.Metadata, receivedKey)
	if err != nil {
		return err
	}
	datagrams, err := metadataCount(status.Metadata, datagramsReceivedKey)
	if err != nil {
		return err
	}
	total := len(messages)
	if env.transport != trevrpc.TransportQUIC {
		if responses != total || received != total || datagrams != 0 {
			return fmt.Errorf("on %s: %d responses, %s = %d, %s = %d; want %d, %d, 0", env.transport, responses, receivedKey, received, datagramsReceivedKey, datagrams, total, total)
		}
		return nil
	}
	if received < 1 || received > total || datagrams < 1 {
		return fmt.Errorf("on QUIC: %s = %d, %s = %d; want 1..%d and at least 1", receivedKey, received, datagramsReceivedKey, datagrams, total)
	}
	if env.datagramFrames != nil && env.datagramFrames() == 0 {
		return errors.New("the connection received no DATAGRAM frames, so the server sent no response as a datagram")
	}
	return nil
}

// metadataCount reads the decimal trailing metadata entry key.
func metadataCount(metadata trevrpc.Metadata, key string) (int, error) {
	value, ok := metadata[key]
	if !ok {
		return 0, fmt.Errorf("trailing metadata has no %s", key)
	}
	count, err := strconv.Atoi(string(value))
	if err != nil || count < 0 {
		return 0, fmt.Errorf("trailing metadata %s = %q, want a decimal count", key, value)
	}
	return count, nil
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
