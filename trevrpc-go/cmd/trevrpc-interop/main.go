// Command trevrpc-interop is the Go entry point of the cross-runtime interop
// suite described in interop/README.md. It serves or calls the Echo service.
//
//	trevrpc-interop server --cert cert.pem --key key.pem [--host 127.0.0.1] [--port 0]
//	trevrpc-interop client --addr host:port --ca cert.pem [--server-name localhost]
//	                       [--transport quic|tcp|auto] [--tcp-addr host:port]
//	                       [--expect-transport quic|qmux]
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printf(stderr, "usage: trevrpc-interop server|client [flags]\n")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	switch args[0] {
	case "server":
		var config serverConfig
		flags.StringVar(&config.certFile, "cert", "", "server certificate PEM file")
		flags.StringVar(&config.keyFile, "key", "", "server private key PEM file")
		flags.StringVar(&config.host, "host", "127.0.0.1", "listen host")
		flags.StringVar(&config.port, "port", "0", "listen UDP port (0 picks a free port)")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if config.certFile == "" || config.keyFile == "" {
			printf(stderr, "server: --cert and --key are required\n")
			return 2
		}
		// The server runs until stdin reaches EOF or it is signalled.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			_, _ = io.Copy(io.Discard, stdin)
			cancel()
		}()
		if err := runServer(ctx, config, stdout); err != nil {
			printf(stderr, "server: %v\n", err)
			return 1
		}
		return 0
	case "client":
		var config clientConfig
		flags.StringVar(&config.addr, "addr", "", "server host:port")
		flags.StringVar(&config.caFile, "ca", "", "trusted server certificate PEM file")
		flags.StringVar(&config.serverName, "server-name", "localhost", "TLS server name")
		flags.StringVar(&config.transport, "transport", "quic", "transport policy: quic, tcp (QMux over TLS) or auto (QUIC with TCP fallback)")
		flags.StringVar(&config.tcpAddr, "tcp-addr", "", "server TCP host:port (defaults to --addr)")
		flags.StringVar(&config.expectTransport, "expect-transport", "", "fail unless the channel connects over this transport (quic or qmux)")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if config.addr == "" || config.caFile == "" {
			printf(stderr, "client: --addr and --ca are required\n")
			return 2
		}
		if _, ok := transportPolicies[config.transport]; !ok {
			printf(stderr, "client: unknown --transport %q\n", config.transport)
			return 2
		}
		passed, err := runClient(ctx, config, stdout)
		if err != nil {
			printf(stderr, "client: %v\n", err)
			return 1
		}
		if !passed {
			return 1
		}
		return 0
	default:
		printf(stderr, "unknown subcommand %q\n", args[0])
		return 2
	}
}

// printf writes a diagnostic to w. The command has nowhere else to report a
// failed console write, so the error is dropped.
func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

// closeQuietly closes c at the end of a scenario or command, once its outcome
// is already decided and a close error would only mask it.
func closeQuietly(c io.Closer) {
	_ = c.Close()
}
