# trevrpc-go

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC, with TCP fallbacks for networks that block UDP.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Transports

The Go runtime is pure Go, built on [quic-go](https://github.com/quic-go/quic-go), [webtransport-go](https://github.com/quic-go/webtransport-go), and [coder/websocket](https://github.com/coder/websocket). Its QMux implementation is in-tree (`internal/qmux`).

| Role   | Protocols                                                                                                                                                                                                |
| ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Client | Native QUIC (ALPN `trevrpc/1`), falling back to QMux over TLS (ALPN `qmux-02.trevrpc-1`) when UDP is blocked. Browsers and Android use their own runtimes.                                               |
| Server | Native QUIC and QMux over TLS always. Ordinary HTTP (`POST /trevrpc`, over HTTP/3 and HTTP/2) and browser sessions (WebTransport and QMux over WebSocket on `/trevrpc`) when enabled in `ServerOptions`. |

A `Listener` serves QUIC on **one UDP socket** and dispatches each connection on its negotiated ALPN protocol (`trevrpc/1` or `h3`). Unless disabled, it also binds **one TCP socket** on the same port number and dispatches on the TLS ALPN: `qmux-02.trevrpc-1` (native QMux), `h2` (HTTP/2), or `http/1.1` (WebSocket upgrade). See [TCP fallback](#tcp-fallback).

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

Each RPC has exactly one client method. Per-call options are `trevrpc.WithTimeout`, `WithMetadata`, `WithMaxFrameSize` and `CaptureResponseMetadata`:

```go
var md trevrpc.Metadata
reply, _ := client.SayHello(ctx, request,
	trevrpc.WithTimeout(5*time.Second),
	trevrpc.WithMetadata("authorization", []byte("Bearer "+token)),
	trevrpc.CaptureResponseMetadata(&md), // unary and client streaming
)
```

Server-streaming and bidirectional calls expose the final status, and its metadata, through `TerminalStatus()` once the stream has ended.

`Dial` returns a `*Channel` that reconnects with backoff after connection loss. Each RPC uses exactly one connection and is never retried or replayed; calls fail fast with `Unavailable` while the channel is reconnecting. `DialOptions.TLSConfig` and `DialOptions.QUICConfig` are cloned before use.

### TCP fallback

By default `Dial` prefers QUIC and falls back to QMux over TLS on the server's TCP listener. QUIC starts first; QMux starts after `FallbackDelay` (300 ms) or immediately if QUIC fails, and the first connection to be ready wins. After QMux wins, reconnects use it directly until `ReprobeInterval` (5 minutes) has passed since QUIC was lost, then race again.

```go
channel, err := trevrpc.Dial(ctx, "localhost:50051", trevrpc.DialOptions{
	TLSConfig:       &tls.Config{RootCAs: roots, ServerName: "localhost"},
	Transport:       trevrpc.TransportAuto, // default; or TransportQUICOnly, TransportTCPOnly
	TCPAddr:         "localhost:50051",     // default: the dial address
	FallbackDelay:   300 * time.Millisecond,
	ReprobeInterval: 5 * time.Minute,
})
fmt.Println(channel.Transport()) // trevrpc.TransportQUIC ("quic") or trevrpc.TransportQMux ("qmux")
```

`ChannelEvent.Transport` is set on `ChannelEventReady`. QUIC and TCP keep separate TLS session caches.

## Server

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*trevrpc.Response[*greeter.HelloReply], error) {
	return &trevrpc.Response[*greeter.HelloReply]{Message: &greeter.HelloReply{Message: "hello, " + r.Name}}, nil
}

// LotsOfReplies and BidiHello return a trevrpc.MessageStream; LotsOfGreetings
// returns a *trevrpc.Response like SayHello. See examples/greeter_server.

server := trevrpc.NewServer()
options := server.Options()
options.EnableHTTP = true          // POST /trevrpc over HTTP/3 and HTTP/2, for Android / Cronet
options.EnableWebTransport = true   // /trevrpc for browsers: WebTransport and QMux over WebSocket
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

Unary and client-streaming handlers return a `*trevrpc.Response` whose `Metadata` is sent with the response; streaming handlers may wrap their stream with `trevrpc.NewResponseStream(stream, metadata)` to send terminal metadata.

Browser sessions (WebTransport and QMux over WebSocket) are rejected unless `WebTransportAdmission` admits them; HTTP requests (HTTP/3 and HTTP/2) are admitted unless `HTTPAdmission` denies them. `AdmissionRequest.Proto` reports the HTTP version (`HTTP/3.0`, `HTTP/2.0`, or `HTTP/1.1`) and `Method` is `POST`, `CONNECT` (WebTransport), or `GET` (WebSocket). Cancelling the context passed to `Serve` (or calling `Close`) cancels in-flight RPCs and waits up to `GracefulShutdownTimeout` for handlers to return.

`Listen` binds TCP on the same host and port number as the UDP socket (with port 0 it retries until a port is free for both). `ListenOptions.TCPAddr` overrides the TCP address, `ListenOptions.DisableTCP` serves QUIC only, and `ListenOptions.TLSHandshakeTimeout` (default 10 s) bounds the TLS handshake. `Listener.TCPAddr()` returns the TCP address, or nil when TCP is disabled. The TCP listener shares the connection limit, request limit, and graceful shutdown with QUIC.

`EnableHTTP3`, `HTTP3Path`, and `HTTP3Admission` were renamed `EnableHTTP`, `HTTPPath`, and `HTTPAdmission`, because the HTTP mapping is now also served over HTTP/2.

## Interop

`cmd/trevrpc-interop` (installed as `trevrpc-interop-go`) implements the `server` and `client` subcommands of the cross-runtime [interop suite](../interop/README.md). Its Echo bindings in `cmd/trevrpc-interop/interoppb` are generated from `interop/proto/interop.proto`; `go test ./cmd/trevrpc-interop/interoppb` fails when they drift, and `-update` rewrites them.
