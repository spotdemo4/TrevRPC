# trevrpc-go

The Go runtime for [TrevRPC](https://trev.zip/llc/TrevRPC), an RPC framework like gRPC built on QUIC instead of HTTP/2: servers that serve native QUIC, HTTP/3 and WebTransport on one UDP port and every TCP fallback on one TCP port, and native QUIC clients that fall back to QMux over TLS when UDP is blocked. It is pure Go, built on [quic-go](https://github.com/quic-go/quic-go), [webtransport-go](https://github.com/quic-go/webtransport-go) and [coder/websocket](https://github.com/coder/websocket), with an in-tree QMux implementation (`internal/qmux`).

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.12.0 works with `trevrpc-go` 1.12.0, `trevrpc-js` 1.12.0, `trevrpc-kotlin`
> 1.12.0, and the Erlang `trevrpc` 1.12.0. Compatibility across different versions isn't guaranteed yet; that's a
> milestone for 2.0.

## Packages

| Package                                                                                                                  | Install                                                                       | Purpose                                                                                           |
| ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| [`trev.zip/llc/trevrpc/trevrpc-go`](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go)                                  | `go get trev.zip/llc/trevrpc/trevrpc-go`                                      | Client and server runtime                                                                         |
| [`…/trevrpc-go/cmd/protoc-gen-trevrpc-go`](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go/cmd/protoc-gen-trevrpc-go) | `go install trev.zip/llc/trevrpc/trevrpc-go/cmd/protoc-gen-trevrpc-go@latest` | `protoc` plugin: service interfaces, clients and registration functions                           |
| [`…/trevrpc-go/ratelimit`](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go/ratelimit)                                 | part of the runtime module                                                    | [Rate-limit interceptor](https://trev.zip/llc/TrevRPC/wiki/Go-Guide#rate-limiting)                |
| [`…/trevrpc-go/oteltrevrpc`](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go/oteltrevrpc)                             | `go get trev.zip/llc/trevrpc/trevrpc-go/oteltrevrpc`                          | [OpenTelemetry](https://trev.zip/llc/TrevRPC/wiki/Go-Guide#opentelemetry) metrics and tracing     |
| [`…/trevrpc-go/protovalidatetrevrpc`](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go/protovalidatetrevrpc)           | `go get trev.zip/llc/trevrpc/trevrpc-go/protovalidatetrevrpc`                 | [protovalidate](https://trev.zip/llc/TrevRPC/wiki/Go-Guide#request-validation) request validation |

The `trevrpc-go` Nix package provides the plugin: `nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-go`.

## Example

Generate code for a service with `protoc-gen-go` and `protoc-gen-trevrpc-go`:

```sh
protoc --go_out=. --go_opt=paths=source_relative \
       --trevrpc-go_out=. --trevrpc-go_opt=paths=source_relative \
       greeter.proto
```

Serve it:

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + r.Name}, nil
}

server := trevrpc.NewServer()
greeter.RegisterGreeterServer(server, greeterService{})

listener, err := trevrpc.Listen("0.0.0.0:50051", server, trevrpc.ListenOptions{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}},
})
if err != nil {
	return err
}
defer listener.Close()
return listener.Serve(ctx)
```

And call it:

```go
channel, err := trevrpc.Dial(ctx, "localhost:50051", trevrpc.DialOptions{
	TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost"},
})
if err != nil {
	return err
}
defer channel.Close()

client := greeter.NewGreeterClient(channel, trevrpc.WithTimeout(5*time.Second))
reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
```

Runnable servers and clients are in [`examples`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-go/examples).

## Documentation

The [Go guide](https://trev.zip/llc/TrevRPC/wiki/Go-Guide) covers generated code, servers, clients, streams, statuses, rate limiting, request validation, and observability. The rest of the [wiki](https://trev.zip/llc/TrevRPC/wiki) covers the transports, the protocol, compression, and operations.

## Development

```sh
go test ./...
golangci-lint run ./...
go run ./examples/greeter_server
go run ./examples/greeter_client TrevRPC   # another shell
```

`cmd/trevrpc-interop` (installed as `trevrpc-interop-go`) implements the `server` and `client` subcommands of the cross-runtime [interop suite](https://trev.zip/llc/TrevRPC/src/branch/main/interop/README.md), and the `bench` load client of the [benchmarks](https://trev.zip/llc/TrevRPC/src/branch/main/bench/README.md). It is a module of its own, so that this one does not depend on protovalidate, which its server uses. Its Echo, UnreliableEcho, CompressedEcho, RefusingEcho, ValidatedEcho and ValidationBench bindings in `cmd/trevrpc-interop/interoppb` are generated from `interop/proto/*.proto`; `go -C cmd/trevrpc-interop test ./interoppb` fails when they drift, and `-update` rewrites them.

See [Local Development](https://trev.zip/llc/TrevRPC/wiki/Local-Development) for the repository-wide checks.
