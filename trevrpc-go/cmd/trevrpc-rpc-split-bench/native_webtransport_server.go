//go:build trevrpc_webtransport_native && cgo

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/examples/greeter"
)

func runNativeWebTransportServer(addr, certFile, keyFile, origin string) error {
	if certFile == "" || keyFile == "" {
		return errors.New("webtransport-msquic server requires -cert and -key")
	}

	server := trevrpc.NewServer()
	options := server.Options()
	options.GracefulShutdownTimeout = time.Second
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = 0
	options.MaxConcurrentStreamsPerConnection = 65535
	server.SetOptions(options)
	greeter.RegisterGreeterServer(server, splitGreeter{})

	listener, err := trevrpc.ListenNativeWebTransport(addr, trevrpc.NativeWebTransportConfig{
		Path:                     "/trevrpc",
		Origin:                   origin,
		CertFile:                 certFile,
		KeyFile:                  keyFile,
		MaxSessionsPerConnection: 16,
		MaxStreamsPerSession:     65535,
		IdleTimeout:              idleTimeout,
		HandshakeTimeout:         15 * time.Second,
	})
	if err != nil {
		return err
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- trevrpc.ServeNativeWebTransport(ctx, listener, server)
	}()
	fmt.Printf("PORT %d\n", portFromAddr(listener.Addr()))
	fmt.Printf("CERT %s\n", certFile)
	if err := waitForShutdown(ctx, serveDone); err != nil {
		return err
	}
	return nil
}
