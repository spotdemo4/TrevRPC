// Command trevrpc-bench-grpc is the grpc-go baseline for TrevRPC's load
// benchmarks (see ../README.md). Its server subcommand serves the interop Echo
// service over gRPC (HTTP/2 over TLS) and its bench subcommand is a load
// client with the same flags and JSON output as the TrevRPC bench clients.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const usage = "usage: trevrpc-bench-grpc server|bench [flags]"

func main() {
	os.Exit(realMain())
}

func realMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "trevrpc-bench-grpc:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "server":
		cfg, err := parseServerFlags(args[1:])
		if err != nil {
			return err
		}
		return runServer(ctx, cfg, stdin, stdout)
	case "bench":
		cfg, err := parseBenchFlags(args[1:])
		if err != nil {
			return err
		}
		result, err := runBench(ctx, cfg)
		if err != nil {
			return err
		}
		return result.write(stdout)
	default:
		return fmt.Errorf("unknown subcommand %q\n%s", args[0], usage)
	}
}
