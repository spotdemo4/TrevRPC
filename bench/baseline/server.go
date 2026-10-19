package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	suffixKey  = "interop-suffix"
	headerKey  = "interop-header"
	trailerKey = "interop-trailer"

	// shutdownGrace bounds how long a graceful stop waits for in-flight calls.
	shutdownGrace = 5 * time.Second
)

type serverConfig struct {
	cert, key             string
	host                  string
	grpcPort, connectPort int
}

func parseServerFlags(args []string) (serverConfig, error) {
	var cfg serverConfig
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.StringVar(&cfg.cert, "cert", "", "TLS certificate PEM file")
	fs.StringVar(&cfg.key, "key", "", "TLS private key PEM file")
	fs.StringVar(&cfg.host, "host", "127.0.0.1", "listen host")
	fs.IntVar(&cfg.grpcPort, "port", 0, "gRPC listen port (0 picks a free port)")
	fs.IntVar(&cfg.connectPort, "connect-port", 0, "Connect listen port (0 picks a free port)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if cfg.cert == "" || cfg.key == "" {
		return cfg, errors.New("server needs --cert and --key")
	}
	for _, port := range []int{cfg.grpcPort, cfg.connectPort} {
		if port < 0 || port > 65535 {
			return cfg, fmt.Errorf("invalid port %d", port)
		}
	}
	return cfg, nil
}

// runServer serves Echo over gRPC (grpc-go) and over the Connect protocol
// (connect-go on net/http), each on its own TLS listener, until stdin reaches
// EOF or ctx is done. Once both listeners are accepting it prints exactly one
// JSON line, {"grpc":<port>,"connect":<port>}, to stdout (which is unbuffered
// when it is os.Stdout). Neither server logs per RPC.
func runServer(ctx context.Context, cfg serverConfig, stdin io.Reader, stdout io.Writer) error {
	certificate, err := tls.LoadX509KeyPair(cfg.cert, cfg.key)
	if err != nil {
		return fmt.Errorf("load certificate: %w", err)
	}
	var lc net.ListenConfig
	grpcLis, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.host, strconv.Itoa(cfg.grpcPort)))
	if err != nil {
		return err
	}
	connectLis, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.host, strconv.Itoa(cfg.connectPort)))
	if err != nil {
		_ = grpcLis.Close()
		return err
	}
	grpcSrv := newGRPCServer(certificate)
	connectSrv := newConnectServer(certificate)

	serveErr := make(chan error, 2)
	go func() { serveErr <- grpcSrv.Serve(grpcLis) }()
	go func() { serveErr <- connectSrv.ServeTLS(connectLis, "", "") }()
	stop := func() {
		grpcSrv.Stop()
		_ = connectSrv.Close()
	}

	line, err := json.Marshal(map[string]int{
		"grpc":    grpcLis.Addr().(*net.TCPAddr).Port,
		"connect": connectLis.Addr().(*net.TCPAddr).Port,
	})
	if err != nil {
		stop()
		return err
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", line); err != nil {
		stop()
		return err
	}

	stdinClosed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, stdin)
		close(stdinClosed)
	}()
	select {
	case <-stdinClosed:
	case <-ctx.Done():
	case err := <-serveErr:
		stop()
		return err
	}

	// Both servers drain in-flight calls in parallel, and are closed outright
	// once shutdownGrace has passed.
	graceCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		stopped := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-graceCtx.Done():
			grpcSrv.Stop()
		}
	})
	wg.Go(func() {
		if connectSrv.Shutdown(graceCtx) != nil {
			_ = connectSrv.Close()
		}
	})
	wg.Wait()
	return nil
}
