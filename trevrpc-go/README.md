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
channel, _ := trevrpc.Dial(ctx, "127.0.0.1:50051", trevrpc.DialOptions{
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

## Transport backends

`DialOptions.Backend` and `ListenOptions.Backend` accept `TransportBackendAuto`,
`TransportBackendLegacy`, or `TransportBackendNative`. `Auto` resolves
deterministically to the quic-go/webtransport-go Legacy backend in this release;
it never probes or falls back at runtime. Explicit `Native` returns an actionable
unavailable error until the native adapter is built. The `tls.Config`,
`quic.Config`, and raw quic-go/webtransport-go APIs remain deprecated
Legacy-only compatibility surfaces.
