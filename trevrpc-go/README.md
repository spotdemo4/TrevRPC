# trevrpc-go

Go runtime support for TrevRPC clients and servers.

## Transport Choice

Use `quic-go` by default. It is the normal Go path, requires no cgo, and works with `go test ./...` and standard Go cross-compilation.

Use native MsQuic when you explicitly want the C/MsQuic transport path, for example to benchmark against `trevrpc-c`, deploy with MsQuic, or compare behavior with other native TrevRPC runtimes. It requires cgo, `libmsquic`, and the `trevrpc_msquic_native` build tag.

| Transport     | Build                         | Best For                                                          |
| ------------- | ----------------------------- | ----------------------------------------------------------------- |
| `quic-go`     | default                       | Portable Go services, easiest builds, default tests               |
| native MsQuic | `-tags trevrpc_msquic_native` | Native MsQuic deployments, C-runtime parity, transport benchmarks |

Generated TrevRPC clients are transport-agnostic. Use `Listen` and `Dial` for a common setup API, then pass the returned client transport to the generated service client.

## quic-go

Server setup:

```go
server := trevrpc.NewServer()
listener, err := trevrpc.Listen("127.0.0.1:50051", server, trevrpc.ListenOptions{
    Kind:      trevrpc.TransportQUICGo,
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
    Kind:      trevrpc.TransportQUICGo,
    TLSConfig: tlsConfig,
})
if err != nil {
    return err
}
defer transport.Close()

client := greeter.NewGreeterClient(transport)
```

## Native MsQuic

Build and test native MsQuic code with:

```sh
go test -tags trevrpc_msquic_native ./...
```

The native path links against `libmsquic` and embeds the `trevrpc-c` MsQuic wrapper through cgo.

Server setup:

```go
server := trevrpc.NewServer()
listener, err := trevrpc.Listen("127.0.0.1:50051", server, trevrpc.ListenOptions{
    Kind: trevrpc.TransportNativeMsQuic,
    NativeMsQuic: trevrpc.NativeMsQuicConfig{
        CertFile: "localhost-cert.pem",
        KeyFile:  "localhost-key.pem",
    },
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
    Kind: trevrpc.TransportNativeMsQuic,
})
if err != nil {
    return err
}
defer transport.Close()

client := greeter.NewGreeterClient(transport)
```

The unified native MsQuic listener derives transport limits from `server.Options()` and then applies non-zero fields from `ListenOptions.NativeMsQuic`. Low-level APIs like `ServeQUIC`, `ListenNativeMsQuic`, and `DialNativeMsQuic` remain available when direct access to transport-specific objects is needed.
