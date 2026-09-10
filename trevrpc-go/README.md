# trevrpc-go

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Protobuf

```proto
syntax = "proto3";

package example.greeter;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
  rpc LotsOfReplies(HelloRequest) returns (stream HelloReply);
  rpc LotsOfGreetings(stream HelloRequest) returns (HelloReply);
  rpc BidiHello(stream HelloRequest) returns (stream HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Generate with `protoc-gen-trevrpc-go`.

## Client

```go
channel, _ := trevrpc.Dial(ctx, "localhost:50051", trevrpc.DialOptions{
    Credentials: &trevrpc.TransportCredentials{
        RootCAPEM:  caPEM,
        ServerName: "localhost",
    },
})
defer channel.Close()
client := greeter.NewGreeterClient(channel)

// Unary
reply, _ := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})

// Server streaming
stream, _ := client.LotsOfReplies(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
for r, err := range trevrpc.Messages(stream) {
	if err != nil { break }
	fmt.Println(r.Message)
}

// Client streaming
call, _ := client.LotsOfGreetings(ctx)
call.Send(&greeter.HelloRequest{Name: "Alice"})
call.Send(&greeter.HelloRequest{Name: "Bob"})
reply, _ = call.CloseAndRecv()

// Bidirectional streaming
bidi, _ := client.BidiHello(ctx)
bidi.Send(&greeter.HelloRequest{Name: "Alice"})
r, _ := bidi.Recv()
fmt.Println(r.Message)
bidi.CloseSend()
```

## Server

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + r.Name}, nil
}
func (greeterService) LotsOfReplies(_ context.Context, r *greeter.HelloRequest) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.FromSlice(
		&greeter.HelloReply{Message: "hello, " + r.Name},
		&greeter.HelloReply{Message: "goodbye, " + r.Name},
	), nil
}
func (greeterService) LotsOfGreetings(_ context.Context, reqs trevrpc.MessageStream[*greeter.HelloRequest]) (*greeter.HelloReply, error) {
	var names []string
	for r, err := range trevrpc.Messages(reqs) {
		if err != nil { return nil, err }
		names = append(names, r.Name)
	}
	return &greeter.HelloReply{Message: strings.Join(names, ", ")}, nil
}
func (greeterService) BidiHello(_ context.Context, reqs trevrpc.MessageStream[*greeter.HelloRequest]) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return &echoReplies{reqs}, nil // Recv() → HelloReply{Message: "hello, " + req.Name}
}

server := trevrpc.NewServer()
greeter.RegisterGreeterServer(server, greeterService{})
listener, _ := trevrpc.Listen("127.0.0.1:50051", server, trevrpc.ListenOptions{
    Credentials: &trevrpc.TransportCredentials{
        CertificateChainPEM: certificatePEM,
        PrivateKeyPEM:       privateKeyPEM,
    },
})
defer listener.Close()
listener.Serve(ctx)
```

## Go transport modules

| Module                                                      | Role                                          | Native dependency behavior                                                                                                                               |
| ----------------------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`trevrpc-go`](.)                                           | Default Go API                                | Bundles the provider-neutral C runtime and patched static MsQuic for cgo on glibc `linux/amd64` and `linux/arm64`; no host MsQuic or `pkg-config` setup. |
| [`trevrpc-go/msquic`](msquic)                               | Explicit endpoint-scoped MsQuic wrapper       | Uses the same bundled provider without global registration.                                                                                              |
| [`trevrpc-go/quic-go`](quic-go)                             | Optional pure-Go QUIC-go/WebTransport backend | Separate nested Go module; importing the base module does not add QUIC-go dependencies.                                                                  |
| [`trevrpc-c/provider/msquic`](../trevrpc-c/provider/msquic) | Low-level `/v2` provider module               | Carries static artifacts, licenses, provenance, and update instructions.                                                                                 |

## Transport backends

### Native backend (default)

The base module uses the native C/MsQuic backend exclusively. `trevrpc.Dial` and
`trevrpc.Listen` do not probe for another implementation and never fall back to
QUIC-go:

- A `host:port` dial target uses native TrevRPC-over-QUIC.
- An `https://` dial target uses native WebTransport.
- A listener serves native QUIC and, when enabled in `ServerOptions`, HTTP/3 and
  WebTransport on the same UDP port.

The base module bundles its patched static MsQuic dependency: it requires no
host TrevRPC installation, `pkg-config`, or system MsQuic package. Native support
activates automatically with cgo on glibc-based `linux/amd64` and `linux/arm64`.
Other builds, including macOS until a bundled Darwin artifact is published, compile
using actionable native-unavailable stubs; `Dial` and `Listen` return
`Unimplemented` when called.

The base module deliberately has no QUIC-go, webtransport-go, or qpack dependency.
Importing only `trev.zip/llc/trevrpc/trevrpc-go` therefore does not add the pure-Go
QUIC stack to an application's module graph.

Backend selection is fixed when an endpoint is created. The shared Go runtime owns
reconnect generations and never retries or replays an in-flight RPC. To select the
bundled provider explicitly for one endpoint, use the same-module wrapper:

```go
import (
    trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
    "trev.zip/llc/trevrpc/trevrpc-go/msquic"
)

channel, err := trevrpc.DialWithBackend(ctx, "127.0.0.1:50051", trevrpc.DialOptions{}, msquic.Backend())
// Or use msquic.Dial(ctx, "127.0.0.1:50051", trevrpc.DialOptions{}).
```

`msquic.Backend` binds an immutable provider value to that endpoint. It does not
use a blank import, `init` hook, mutable registry, or process-global selection.

### Optional QUIC-go backend

Applications that need the pure-Go implementation import the separate nested
module explicitly:

```sh
go get trev.zip/llc/trevrpc/trevrpc-go/quic-go
```

Its import path contains `quic-go`, while its Go package name is `quicgo`.
`quicgo.Dial` and `quicgo.Listen` inject the provider for that endpoint only; no
blank import, `init` registration, or process-global backend selection is used.

This module intentionally uses upstream `webtransport-go` without a fork. Safari
WebTransport may remain unavailable until upstream compatibility is restored.
This does not affect applications that import only the base `trevrpc-go` module.

#### Optional-module releases

`quic-go` is independently versioned and released with tags shaped
`trevrpc-go/quic-go/vX.Y.Z`. Its `go.mod` pins the required base
`trevrpc-go` version, which must already be available from the public Go proxy;
the release job waits for that exact version before seeding the optional module.
Release the base module before—or push its tag alongside—the optional-module tag.
The Nix derivation's `0.1.0` is a package snapshot version, not this module's
release authority; its download page intentionally points to the releases index.

```go
import (
    "crypto/tls"

    "github.com/quic-go/quic-go"
    trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
    quicgo "trev.zip/llc/trevrpc/trevrpc-go/quic-go"
)

channel, err := quicgo.Dial(ctx, "127.0.0.1:50051", quicgo.DialOptions{
    Credentials: &trevrpc.TransportCredentials{RootCAPEM: caPEM},
    TLSConfig:   &tls.Config{ServerName: "localhost"},
    QUICConfig:  &quic.Config{},
})
if err != nil {
    return err
}
defer channel.Close()
```

Use an `https://` target for WebTransport. Request headers, application protocols,
and stream-reordering behavior belong to `quicgo.WebTransportOptions`:

```go
channel, err := quicgo.Dial(ctx, "https://localhost:50051/trevrpc", quicgo.DialOptions{
    Credentials: &trevrpc.TransportCredentials{RootCAPEM: caPEM},
    WebTransport: quicgo.WebTransportOptions{
        RequestHeaders: trevrpc.HeaderFields{
            {Name: "Origin", Value: "https://example.test"},
        },
    },
})
```

The optional listener uses the same `Server` configuration as the native backend:

```go
listener, err := quicgo.Listen("127.0.0.1:50051", server, quicgo.ListenOptions{
    Credentials: &trevrpc.TransportCredentials{
        CertificateChainPEM: certificatePEM,
        PrivateKeyPEM:       privateKeyPEM,
    },
})
if err != nil {
    return err
}
defer listener.Close()
return listener.Serve(ctx)
```

Set `ServerOptions.EnableHTTP3` or `ServerOptions.EnableWebTransport` before
calling `quicgo.Listen` to serve those protocols. `TLSConfig` and `QUICConfig` are
cloned before use, as are credentials and WebTransport option slices.
