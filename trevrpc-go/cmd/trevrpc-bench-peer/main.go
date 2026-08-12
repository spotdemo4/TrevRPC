package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type peerError struct {
	phase string
	code  string
	err   error
}

func (e *peerError) Error() string { return e.err.Error() }
func (e *peerError) Unwrap() error { return e.err }

func main() {
	emitter := newEventEmitter(os.Stdout)
	if err := run(os.Args[1:], os.Stdin, emitter); err != nil {
		var protocolError *peerError
		if !errors.As(err, &protocolError) {
			protocolError = &peerError{phase: "startup", code: "peer_failed", err: err}
		}
		_ = emitter.emit(errorEvent{
			SchemaVersion: schemaVersion,
			Event:         "error",
			Peer:          peerName,
			Phase:         protocolError.phase,
			Code:          protocolError.code,
			Message:       protocolError.err.Error(),
		})
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, emitter *eventEmitter) error {
	globalFlags := flag.NewFlagSet("trevrpc-bench-peer", flag.ContinueOnError)
	globalFlags.SetOutput(io.Discard)
	var webTransportDraft07Only bool
	globalFlags.BoolVar(&webTransportDraft07Only, "webtransport-draft07-only", false, "advertise only draft-07 WebTransport settings")
	if err := globalFlags.Parse(args); err != nil {
		return fail("startup", "invalid_config", err)
	}
	args = globalFlags.Args()
	if len(args) == 0 {
		return fail("startup", "invalid_command", errors.New("usage: trevrpc-bench-peer [--webtransport-draft07-only] capabilities|server|client [options]"))
	}
	switch args[0] {
	case "capabilities":
		if len(args) != 1 {
			return fail("startup", "invalid_config", errors.New("capabilities accepts no arguments"))
		}
		return emitter.emit(capabilitiesEvent{
			SchemaVersion: schemaVersion,
			Event:         "capabilities",
			Peer:          peerName,
			Roles: roleCapabilities{
				Client: []string{string(stackNativeQUIC)},
				Server: []string{string(stackNativeQUIC), string(stackWebTransport)},
			},
			RPCKinds:  []string{"unary", "client_stream", "server_stream", "bidi"},
			Histogram: "log_linear_v1",
		})
	case "server":
		config, err := parseServerConfig(args[1:])
		if err != nil {
			return fail("startup", "invalid_config", err)
		}
		config.webTransportDraft07Only = webTransportDraft07Only
		if config.webTransportDraft07Only && config.stack != stackWebTransport {
			return fail("startup", "invalid_config", errors.New("--webtransport-draft07-only is only valid with trevrpc_webtransport"))
		}
		return runServer(config, stdin, emitter)
	case "client":
		if webTransportDraft07Only {
			return fail("startup", "invalid_config", errors.New("--webtransport-draft07-only is only valid with a trevrpc_webtransport server"))
		}
		config, err := parseClientConfig(args[1:])
		if err != nil {
			return fail("startup", "invalid_config", err)
		}
		return runClient(config, stdin, emitter)
	default:
		return fail("startup", "invalid_command", fmt.Errorf("unsupported command %q", args[0]))
	}
}

func runServer(config serverConfig, stdin io.Reader, emitter *eventEmitter) error {
	listener, err := listenBenchmarkServer(config)
	if err != nil {
		return fail("startup", "listen_failed", err)
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(serveCtx) }()
	if err := emitter.emit(readyEvent{
		SchemaVersion: schemaVersion,
		Event:         "ready",
		Peer:          peerName,
		Address:       listener.Addr().String(),
		PID:           os.Getpid(),
	}); err != nil {
		return fail("startup", "event_failed", err)
	}

	commands := scanCommands(stdin)
	for {
		select {
		case err := <-serveDone:
			if err != nil {
				return fail("serve", "serve_failed", err)
			}
			return nil
		case <-ctx.Done():
			cancelServe()
			if err := <-serveDone; err != nil {
				return fail("shutdown", "serve_failed", err)
			}
			return nil
		case command, ok := <-commands:
			if !ok {
				return fail("control", "control_closed", errors.New("stdin closed before SHUTDOWN"))
			}
			if command.err != nil {
				return fail("control", "control_failed", command.err)
			}
			if command.value != "SHUTDOWN" {
				return fail("control", "invalid_control", fmt.Errorf("server expected SHUTDOWN, got %q", command.value))
			}
			cancelServe()
			if err := <-serveDone; err != nil {
				return fail("shutdown", "serve_failed", err)
			}
			if err := emitter.emit(processEvent{SchemaVersion: schemaVersion, Event: "stopped", Peer: peerName}); err != nil {
				return fail("shutdown", "event_failed", err)
			}
			return nil
		}
	}
}

func runClient(config clientConfig, stdin io.Reader, emitter *eventEmitter) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, closeClient, err := dialBenchmarkClient(ctx, config)
	if err != nil {
		return fail("connect", "connect_failed", err)
	}
	defer closeClient()
	operation := newBenchmarkOperation(client, config)
	if _, err := operation(ctx, 0); err != nil {
		return fail("validate", "rpc_failed", err)
	}
	if config.warmup > 0 {
		warmup := preparePhase(ctx, config.concurrency, operation, false).run(config.warmup)
		if warmup.failed != 0 {
			return fail("warmup", "rpc_failed", fmt.Errorf("warmup recorded %d failed operations: %w", warmup.failed, warmup.err))
		}
		if ctx.Err() != nil {
			return fail("warmup", "interrupted", ctx.Err())
		}
	}

	measurement := preparePhase(ctx, config.concurrency, operation, true)
	if err := emitter.emit(processEvent{SchemaVersion: schemaVersion, Event: "armed", Peer: peerName, PID: os.Getpid()}); err != nil {
		return fail("control", "event_failed", err)
	}
	commands := scanCommands(stdin)
	select {
	case <-ctx.Done():
		return fail("control", "interrupted", ctx.Err())
	case command, ok := <-commands:
		if !ok {
			return fail("control", "control_closed", errors.New("stdin closed before START"))
		}
		if command.err != nil {
			return fail("control", "control_failed", command.err)
		}
		if command.value == "SHUTDOWN" {
			return nil
		}
		if command.value != "START" {
			return fail("control", "invalid_control", fmt.Errorf("client expected START or SHUTDOWN, got %q", command.value))
		}
	}

	result := measurement.run(config.measurement)
	if ctx.Err() != nil {
		return fail("measure", "interrupted", ctx.Err())
	}
	if result.failed != 0 {
		return fail("measure", "rpc_failed", fmt.Errorf("measurement recorded %d failed operations: %w", result.failed, result.err))
	}
	if histogramCount(result.histogram) != result.completed {
		return fail("measure", "histogram_mismatch", errors.New("histogram count does not equal completed operations"))
	}
	if err := emitter.emit(sampleFromResult(config, result)); err != nil {
		return fail("measure", "event_failed", err)
	}
	return nil
}

type controlCommand struct {
	value string
	err   error
}

func scanCommands(reader io.Reader) <-chan controlCommand {
	commands := make(chan controlCommand, 1)
	go func() {
		defer close(commands)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			commands <- controlCommand{value: strings.TrimSuffix(scanner.Text(), "\r")}
		}
		if err := scanner.Err(); err != nil {
			commands <- controlCommand{err: err}
		}
	}()
	return commands
}

func fail(phase, code string, err error) error {
	return &peerError{phase: phase, code: code, err: err}
}
