package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

type rpcKind string
type stackKind string

const (
	stackNativeQUIC               stackKind = "trevrpc_native_quic"
	stackWebTransport             stackKind = "trevrpc_webtransport"
	rpcUnary                      rpcKind   = "unary"
	rpcClientStream               rpcKind   = "client_stream"
	rpcServerStream               rpcKind   = "server_stream"
	rpcBidi                       rpcKind   = "bidi"
	maxBenchmarkPayloadBytes                = 64 * 1024 * 1024
	maxBenchmarkFrameSize                   = maxBenchmarkPayloadBytes + 1024
	maxBenchmarkConcurrency                 = 1024
	maxBenchmarkMessagesPerStream           = 1_000_000
)

type serverConfig struct {
	stack              stackKind
	listen             string
	certFile           string
	keyFile            string
	webTransportOrigin string
}

type clientConfig struct {
	stack             stackKind
	address           string
	certFile          string
	rpc               rpcKind
	concurrency       int
	warmup            time.Duration
	measurement       time.Duration
	requestBytes      uint32
	responseBytes     uint32
	messagesPerStream uint32
}

func parseServerConfig(args []string) (serverConfig, error) {
	if err := validateOptionSyntax(args, "stack", "listen", "cert", "key", "webtransport-origin"); err != nil {
		return serverConfig{}, err
	}
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var stackText string
	config := serverConfig{}
	flags.StringVar(&stackText, "stack", "", "RPC stack")
	flags.StringVar(&config.listen, "listen", "", "listen address")
	flags.StringVar(&config.certFile, "cert", "", "PEM certificate path")
	flags.StringVar(&config.keyFile, "key", "", "PEM private key path")
	flags.StringVar(&config.webTransportOrigin, "webtransport-origin", "", "required WebTransport Origin header")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		return config, fmt.Errorf("unexpected server arguments: %v", flags.Args())
	}
	if stackText == "" || config.listen == "" || config.certFile == "" || config.keyFile == "" {
		return config, errors.New("server requires --stack, --listen, --cert, and --key")
	}
	config.stack = stackKind(stackText)
	if err := validateServerStack(config.stack); err != nil {
		return config, err
	}
	if config.stack == stackWebTransport && config.webTransportOrigin == "" {
		return config, errors.New("trevrpc_webtransport server requires --webtransport-origin")
	}
	if config.stack != stackWebTransport && config.webTransportOrigin != "" {
		return config, errors.New("--webtransport-origin is only valid with trevrpc_webtransport")
	}
	return config, nil
}

func parseClientConfig(args []string) (clientConfig, error) {
	if err := validateOptionSyntax(args, "stack", "address", "cert", "rpc", "concurrency", "warmup-ms", "measurement-ms", "request-bytes", "response-bytes", "messages-per-stream"); err != nil {
		return clientConfig{}, err
	}
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var (
		config            clientConfig
		stackText         string
		rpcText           string
		concurrency       uint64
		warmupMS          uint64
		measurementMS     uint64
		requestBytes      uint64
		responseBytes     uint64
		messagesPerStream uint64
	)
	flags.StringVar(&stackText, "stack", "", "RPC stack")
	flags.StringVar(&config.address, "address", "", "server address")
	flags.StringVar(&config.certFile, "cert", "", "PEM CA certificate path")
	flags.StringVar(&rpcText, "rpc", "", "RPC kind")
	flags.Uint64Var(&concurrency, "concurrency", 0, "workload concurrency")
	flags.Uint64Var(&warmupMS, "warmup-ms", 0, "warmup duration in milliseconds")
	flags.Uint64Var(&measurementMS, "measurement-ms", 0, "measurement duration in milliseconds")
	flags.Uint64Var(&requestBytes, "request-bytes", 0, "request payload bytes")
	flags.Uint64Var(&responseBytes, "response-bytes", 0, "response payload bytes")
	flags.Uint64Var(&messagesPerStream, "messages-per-stream", 0, "application messages per stream")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		return config, fmt.Errorf("unexpected client arguments: %v", flags.Args())
	}
	if stackText == "" || config.address == "" || config.certFile == "" {
		return config, errors.New("client requires --stack, --address, and --cert")
	}
	config.stack = stackKind(stackText)
	if err := validateClientStack(config.stack); err != nil {
		return config, err
	}
	config.rpc = rpcKind(rpcText)
	switch config.rpc {
	case rpcUnary, rpcClientStream, rpcServerStream, rpcBidi:
	default:
		return config, fmt.Errorf("unsupported --rpc %q", rpcText)
	}
	if concurrency == 0 || concurrency > maxBenchmarkConcurrency {
		return config, fmt.Errorf("--concurrency must be between 1 and %d", maxBenchmarkConcurrency)
	}
	if measurementMS == 0 {
		return config, errors.New("--measurement-ms must be positive")
	}
	if requestBytes > maxBenchmarkPayloadBytes || responseBytes > maxBenchmarkPayloadBytes {
		return config, fmt.Errorf("payload byte counts must not exceed %d", maxBenchmarkPayloadBytes)
	}
	if messagesPerStream == 0 || messagesPerStream > maxBenchmarkMessagesPerStream {
		return config, fmt.Errorf("--messages-per-stream must be between 1 and %d", maxBenchmarkMessagesPerStream)
	}
	var err error
	config.warmup, err = millisecondsDuration("--warmup-ms", warmupMS)
	if err != nil {
		return config, err
	}
	config.measurement, err = millisecondsDuration("--measurement-ms", measurementMS)
	if err != nil {
		return config, err
	}
	config.concurrency = int(concurrency)
	config.requestBytes = uint32(requestBytes)
	config.responseBytes = uint32(responseBytes)
	config.messagesPerStream = uint32(messagesPerStream)
	return config, nil
}

func validateServerStack(stack stackKind) error {
	switch stack {
	case stackNativeQUIC, stackWebTransport:
		return nil
	default:
		return fmt.Errorf("unsupported --stack %q", stack)
	}
}

func validateClientStack(stack stackKind) error {
	switch stack {
	case stackNativeQUIC:
		return nil
	default:
		return fmt.Errorf("unsupported client --stack %q", stack)
	}
}

func millisecondsDuration(name string, value uint64) (time.Duration, error) {
	if value > uint64(math.MaxInt64)/uint64(time.Millisecond) {
		return 0, fmt.Errorf("%s is too large", name)
	}
	return time.Duration(value) * time.Millisecond, nil
}

func validateOptionSyntax(args []string, names ...string) error {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(names))
	for index := 0; index < len(args); index++ {
		option := args[index]
		if !strings.HasPrefix(option, "--") || len(option) == 2 {
			return fmt.Errorf("expected --name, got %q", option)
		}
		name, _, hasInlineValue := strings.Cut(option[2:], "=")
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("unknown option --%s", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate option --%s", name)
		}
		seen[name] = struct{}{}
		if !hasInlineValue {
			index++
			if index == len(args) {
				return fmt.Errorf("missing value for --%s", name)
			}
		}
	}
	return nil
}
