# trevrpc-go

Go runtime support for TrevRPC clients and servers.

The Go runtime uses `quic-go`. It requires no cgo, and works with `go test ./...` and standard Go cross-compilation.

Generated TrevRPC clients are transport-agnostic. Use `Listen` and `Dial` for a common setup API, then pass the returned client transport to the generated service client. Backend-independent QUIC idle and keepalive settings go in `ListenOptions.Transport` or `DialOptions.Transport`; use `QUICConfig` for quic-go-specific knobs.

## Setup

Server setup:

```go
server := trevrpc.NewServer()
listener, err := trevrpc.Listen("127.0.0.1:50051", server, trevrpc.ListenOptions{
    TLSConfig: tlsConfig,
})
if err != nil {
    return err
}
defer listener.Close()

return listener.Serve(ctx)
```

Client setup:

```go
transport, err := trevrpc.Dial(ctx, "127.0.0.1:50051", trevrpc.DialOptions{
    TLSConfig: tlsConfig,
})
if err != nil {
    return err
}
defer transport.Close()

client := greeter.NewGreeterClient(transport)
```
