# trevrpc-go

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Transports

The Go runtime is pure Go, built on [quic-go](https://github.com/quic-go/quic-go) and [webtransport-go](https://github.com/quic-go/webtransport-go).

| Role   | Protocols                                                                                                            |
| ------ | -------------------------------------------------------------------------------------------------------------------- |
| Client | Native QUIC only (ALPN `trevrpc/1`). Browsers use WebTransport and Android uses HTTP/3 through their own runtimes.   |
| Server | Native QUIC always. Ordinary HTTP/3 (`POST /trevrpc`) and WebTransport (`/trevrpc`) when enabled in `ServerOptions`. |

A `Listener` serves all three on **one UDP socket** and dispatches each connection on its negotiated ALPN protocol (`trevrpc/1` or `h3`).

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

Generate Go code with `protoc-gen-go` and `protoc-gen-trevrpc-go`:

```sh
protoc --go_out=. --go_opt=paths=source_relative \
       --trevrpc-go_out=. --trevrpc-go_opt=paths=source_relative \
       greeter.proto
```

## Client

```go
channel, err := trevrpc.Dial(ctx, "localhost:50051", trevrpc.DialOptions{
	TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost"},
})
if err != nil {
	return err
}
defer channel.Close()
client := greeter.NewGreeterClient(channel)

// Unary
reply, _ := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})

// Server streaming
stream, _ := client.LotsOfReplies(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
for r, err := range trevrpc.Messages(stream) {
	if err != nil {
		break
	}
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

`Dial` returns a `*Channel` that reconnects with backoff after connection loss. Each RPC uses exactly one connection and is never retried or replayed; calls fail fast with `Unavailable` while the channel is reconnecting. `DialOptions.TLSConfig` and `DialOptions.QUICConfig` are cloned before use.

## Server

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*trevrpc.Response[*greeter.HelloReply], error) {
	return trevrpc.NewResponse(&greeter.HelloReply{Message: "hello, " + r.Name}), nil
}

// LotsOfReplies, LotsOfGreetings and BidiHello are implemented the same way:
// see examples/greeter_server.

server := trevrpc.NewServer()
options := server.Options()
options.EnableHTTP3 = true          // POST /trevrpc for Android / Cronet
options.EnableWebTransport = true   // /trevrpc for browsers
options.WebTransportAdmission = func(r trevrpc.AdmissionRequest) bool {
	return r.Origin == "https://app.example.com"
}
server.SetOptions(options)
greeter.RegisterGreeterServer(server, greeterService{})

listener, err := trevrpc.Listen("0.0.0.0:50051", server, trevrpc.ListenOptions{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}},
})
if err != nil {
	return err
}
defer listener.Close()
return listener.Serve(ctx) // returns nil after ctx is cancelled and handlers drain
```

WebTransport sessions are rejected unless `WebTransportAdmission` admits them; HTTP/3 requests are admitted unless `HTTP3Admission` denies them. Cancelling the context passed to `Serve` (or calling `Close`) cancels in-flight RPCs and waits up to `GracefulShutdownTimeout` for handlers to return.

## Interop

`cmd/trevrpc-interop` (installed as `trevrpc-interop-go`) implements the `server` and `client` subcommands of the cross-runtime [interop suite](../interop/README.md). Its Echo bindings in `cmd/trevrpc-interop/interoppb` are generated from `interop/proto/interop.proto`; `go test ./cmd/trevrpc-interop/interoppb` fails when they drift, and `-update` rewrites them.
