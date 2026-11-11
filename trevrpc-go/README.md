# trevrpc-go

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC, with TCP fallbacks for networks that block UDP.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.8.0 works with `trevrpc-go` 1.8.0, `trevrpc-js` 1.8.0, and `trevrpc-kotlin`
> 1.8.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

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

Each RPC has exactly one client method. Per-call options are `trevrpc.WithTimeout`, `WithMetadata`, `WithMaxFrameSize`, `CaptureResponseHeaders` and `CaptureResponseMetadata`:

```go
var headers, trailers trevrpc.Metadata
reply, _ := client.SayHello(ctx, request,
	trevrpc.WithTimeout(5*time.Second),
	trevrpc.WithMetadata("authorization", []byte("Bearer "+token)),
	trevrpc.CaptureResponseHeaders(&headers), // unary and client streaming
	trevrpc.CaptureResponseMetadata(&trailers), // unary and client streaming
)
```

A response has two kinds of metadata. Response headers arrive before the first response message, in a `HEADERS` frame. Trailing metadata ("response metadata") arrives with the final status. Server-streaming calls (`ServerStreamingCall`) and bidirectional calls expose the headers through `Headers()`, which blocks until the headers, the first message or the final status arrives, returns empty metadata if the server sent none, and does not consume a message. A call that failed or was closed before its first frame makes `Headers()` return the failure, as `Recv` does. `Recv` returns `io.EOF` only after the server's OK status or `Close`, and keeps returning the error that ended a failed call. They expose the final status, and the trailing metadata on it, through `TerminalStatus()` once the stream has ended.

`trevrpc.BearerMetadata(token)` builds the `authorization` metadata of a bearer token, for `WithMetadataEntries` and the connection metadata below, and returns an `InvalidArgument` status for a token that the server's `BearerToken` would reject as malformed (anything but an RFC 6750 `b64token`).

Metadata that every call carries, such as a bearer token, can instead be set once with `DialOptions.ConnectionMetadata` and replaced with `channel.SetConnectionMetadata`. Each connection then sends it in its first calls only, and the server merges it into every call's metadata, a call's own entry winning; see [Connection Metadata](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#connection-metadata).

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

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + r.Name}, nil
}

// LotsOfReplies and BidiHello return a trevrpc.MessageStream, such as
// trevrpc.StreamOf(replies...) or trevrpc.StreamFromSeq(seq); LotsOfGreetings
// returns a *greeter.HelloReply like SayHello. See examples/greeter_server.

server := trevrpc.NewServer()
options := server.Options()
options.EnableHTTP = true          // POST /trevrpc over HTTP/3 and HTTP/2, for Android / Cronet
options.EnableWebTransport = true   // /trevrpc for browsers: WebTransport and QMux over WebSocket
// Pages served by this server, and https://app.example.com.
options.WebTransportAdmission = trevrpc.SameOriginAdmission("https://app.example.com")
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

Handlers set response metadata through their context. `trevrpc.SetResponseHeader(ctx, key, value)` adds a response header, sent before the first response message as soon as the handler returns. `trevrpc.SetResponseMetadata(ctx, key, value)` adds trailing metadata, sent with the final OK status: when a unary or client-streaming handler returns, or when a streaming handler's response stream ends, so the stream may still set it before its `Recv` returns `io.EOF`. Keys are normalized with `trevrpc.NormalizeMetadataKey` and validated; setting a header after the handler has returned, or metadata after the final status, fails. A failed call sends the `Metadata` of its `*trevrpc.Status` instead.

Generated code calls the generic functions `trevrpc.Unary`, `ServerStreaming`, `ClientStreaming` and `BidirectionalStreaming` on the client and `trevrpc.RegisterUnary`, `RegisterServerStreaming`, `RegisterClientStreaming` and `RegisterBidirectionalStreaming` on the server, and you can call them directly. Each identifies the method by its full path, `/package.Service/Method`, which generated code exposes as a constant such as `greeter.Greeter_SayHello_FullMethodName`:

```go
reply, err := trevrpc.Unary(ctx, channel, "/example.greeter.Greeter/SayHello",
	&greeter.HelloRequest{Name: "TrevRPC"}, func() *greeter.HelloReply { return &greeter.HelloReply{} })

trevrpc.RegisterUnary(server, "/example.greeter.Greeter/SayHello",
	func() *greeter.HelloRequest { return &greeter.HelloRequest{} },
	func(ctx context.Context, r *greeter.HelloRequest) (*greeter.HelloReply, error) { /* ... */ })
```

`Register*` panics if the path is not exactly `/<service>/<method>` with both parts non-empty. The server answers a request whose path is malformed with `InvalidArgument` and a well-formed path without a handler with `Unimplemented`. `HandlerContext` carries the `FullMethod` path and its `Service` and `Method` parts, `MethodDescriptor`, the protobuf descriptor of the method (nil for a route registered without `RouteMethodDescriptor`, which generated code passes, and for unknown methods), whose options an authorizer or interceptor reads to apply a policy the schema defines, such as a custom `public` option, and `RemoteAddr`, the client's `netip.AddrPort` (IPv4 clients as IPv4 on every transport). Authorizers see all of them.

On the wire a call is a sequence of frames, each a QUIC variable-length type, a QUIC variable-length length and a payload: `REQUEST MESSAGE*` from the client, ended by its FIN, and `[ANSWER] [HEADERS] MESSAGE* STATUS` from the server, where ANSWER answers the request's negotiation fields (dictionaries, stream compression, connection metadata). `MESSAGE` payloads are the protobuf message itself. Frames of unknown type are skipped everywhere, and count towards `MaxFrameSize` (checked before a payload is read) and, on servers, towards `MaxStreamBodySize`.

Frames carry no wire version: each transport fixes it while negotiating (ALPN `trevrpc/1`, ALPN or subprotocol `qmux-02.trevrpc-1`, `Content-Type: application/trevrpc`). For WebTransport the server selects the protocol `trevrpc-1` in `WT-Protocol`, answers `400` to a CONNECT whose `WT-Available-Protocols` is a valid list without `"trevrpc-1"`, and treats a missing or malformed field as version 1.

Browser sessions (WebTransport and QMux over WebSocket) are rejected unless `WebTransportAdmission` admits them (`SameOriginAdmission` admits the server's own pages and listed origins); HTTP requests (HTTP/3 and HTTP/2) are admitted unless `HTTPAdmission` denies them. `AdmissionRequest.Proto` reports the HTTP version (`HTTP/3.0`, `HTTP/2.0`, or `HTTP/1.1`), `Method` is `POST`, `CONNECT` (WebTransport), or `GET` (WebSocket), and `RemoteAddr` is the client's address. Cancelling the context passed to `Serve` (or calling `Close`) cancels in-flight RPCs and waits up to `GracefulShutdownTimeout` for handlers to return.

`ServerOptions.HTTPFallback` serves every other HTTP request on the same sockets, over HTTP/3, HTTP/2 and HTTP/1.1, such as the static content of the web application that calls the server (`options.HTTPFallback = http.FileServerFS(dist)`). TrevRPC keeps `HTTPPath` while `EnableHTTP` or `EnableWebTransport` is set; see [Serving Other HTTP Content](https://trev.zip/llc/TrevRPC/wiki/HTTP3#serving-other-http-content).

For local development, `trevrpc.NewDevelopmentCertificate(hosts...)` generates a self-signed ECDSA certificate (for `localhost`, `127.0.0.1` and `::1` by default) that rotates itself within the 14-day limit browsers place on certificates they trust by hash. Set `ServerOptions.DevelopmentCertificate` and leave `ListenOptions.TLSConfig` nil: `Listen` presents the certificate and publishes its current and next SHA-256 hashes as JSON at `HTTPPath + "/certificate-hashes"` over every HTTP version it serves. The browser client fetches them with `fetchServerCertificateHashes: true`; without a development certificate, this path answers 404 instead of reaching `HTTPFallback`. As an `http.Handler`, the certificate answers the same hashes elsewhere, and `PEM()` returns both certificates for native clients to trust. See [Development certificates](https://trev.zip/llc/TrevRPC/wiki/WebTransport#development-certificates).

In production, [`autocert`](https://pkg.go.dev/golang.org/x/crypto/acme/autocert) can get certificates from Let's Encrypt: pass `manager.TLSConfig()` as `ListenOptions.TLSConfig`. It lists `acme-tls/1`, which the TCP listener keeps so that `autocert` can answer TLS-ALPN-01 challenges when the TCP listener is on port 443. See [Certificates from ACME](https://trev.zip/llc/TrevRPC/wiki/Operations-Limits-and-Security#certificates-from-acme).

`Listen` binds TCP on the same host and port number as the UDP socket (with port 0 it retries until a port is free for both). `ListenOptions.TCPAddr` overrides the TCP address, `ListenOptions.DisableTCP` serves QUIC only, and `ListenOptions.TLSHandshakeTimeout` (default 10 s) bounds the TLS handshake. Behind a TCP load balancer, `ListenOptions.TrustedTCPProxies` lists its networks: connections from them must start with a PROXY protocol v1 or v2 header, whose source address becomes the client's `RemoteAddr`, and connections from anywhere else are never read for one. Behind a UDP load balancer, `ListenOptions.TrustedUDPProxies` lists its networks: every datagram from them must start with a PROXY protocol v2 header, which the socket strips, and the client it names when the connection completes its handshake becomes the QUIC connection's `RemoteAddr` while QUIC keeps talking to the load balancer; datagrams without a valid header are dropped. `ServerOptions.AddressValidationRate` makes clients prove their address with a QUIC Retry once more than that many connection attempts from unvalidated addresses arrive a second (`ValidateAllAddresses` for every attempt). HTTP/2 and HTTP/1.1 responses advertise HTTP/3 on the UDP socket's port in `Alt-Svc`; `ListenOptions.AltSvcPort` advertises another port when clients reach the socket through a reverse proxy, load balancer or NAT. `Listener.TCPAddr()` returns the TCP address, or nil when TCP is disabled. The TCP listener shares the connection limit, request limit, and graceful shutdown with QUIC.

`EnableHTTP3`, `HTTP3Path`, and `HTTP3Admission` were renamed `EnableHTTP`, `HTTPPath`, and `HTTPAdmission`, because the HTTP mapping is now also served over HTTP/2.

## Observability

`server.SetMetrics` and `server.SetInterceptors` report every call, and `server.SetDiagnostics` reports local failures such as panics; `trevrpc.LogDiagnostics(logger)` logs diagnostics to `log/slog` at the level each phase recommends. The separate module [`oteltrevrpc`](oteltrevrpc) builds OpenTelemetry metrics, server spans and trace propagation on them:

```go
metrics, err := oteltrevrpc.NewMetrics()
if err != nil {
	return err
}
server.SetMetrics(metrics)
server.SetInterceptors(oteltrevrpc.NewInterceptor())
server.SetDiagnostics(trevrpc.LogDiagnostics(slog.Default()))

reply, err := client.SayHello(ctx, request, oteltrevrpc.Propagate(ctx))
```

See [Observability](https://trev.zip/llc/TrevRPC/wiki/Go-Guide#observability).

## Compression

Messages can travel compressed with zstd (see [Compression](https://trev.zip/llc/TrevRPC/wiki/Compression)), in three ways that combine. They call libzstd in binaries built with cgo, and the pure-Go `klauspost/compress` without it or with the `trevrpc_purego` build tag; the two interoperate.

- **Dictionaries:** `DialOptions.Compression` and `ServerOptions.Compression` take dictionaries trained with `zstd --train`, each with the protobuf message types it compresses.
- **Without a dictionary:** a method marked `option (trevrpc.method) = {compression: COMPRESSION_MESSAGE}` (or `COMPRESSION_STREAM`) compresses its messages of 256 bytes or more on their own where no dictionary applies, with nothing to configure. Responses compress from a connection's first call, requests once its answer arrives. `HandlerContext.PlainCompressedRequests` counts the requests that arrived so.
- **Stream compression:** a streaming method marked `option (trevrpc.method) = {compression: COMPRESSION_STREAM}` compresses each streamed direction with one zstd context per call, from its third message, with nothing to configure. `ServerOptions.StreamCompressionMemory` (256 MiB by default) and `DialOptions.StreamCompressionMemory` (64 MiB) bound the memory those contexts hold; a negative value turns it off.

## Interop

`cmd/trevrpc-interop` (installed as `trevrpc-interop-go`) implements the `server` and `client` subcommands of the cross-runtime [interop suite](../interop/README.md), and the `bench` load client of the [benchmarks](../bench/README.md). It is a module of its own, so that this one does not depend on protovalidate, which its server uses. Its Echo, UnreliableEcho, CompressedEcho, RefusingEcho, ValidatedEcho and ValidationBench bindings in `cmd/trevrpc-interop/interoppb` are generated from `interop/proto/*.proto`; `go -C cmd/trevrpc-interop test ./interoppb` fails when they drift, and `-update` rewrites them.
