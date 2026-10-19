# trevrpc-rust

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC, with TCP fallbacks for networks that block UDP.

The Rust crate is built on [Quinn](https://github.com/quinn-rs/quinn):

- **Client:** `client::Channel` speaks native QUIC (ALPN `trevrpc/1`) and reconnects automatically. It can fall back to QMux over TLS (ALPN `qmux-02.trevrpc-1`) when UDP is blocked. Rust has no HTTP/3 or WebTransport client; Android uses HTTP/3 and browsers use WebTransport.
- **Server:** `server::Server` serves native QUIC always, plus ordinary HTTP/3 (`POST /trevrpc`) and WebTransport (`/trevrpc`) when enabled, from one Quinn endpoint dispatching on the negotiated ALPN. With a TCP listener it also serves QMux over TLS, HTTP/2, and QMux over WebSocket; see [TCP fallback](#tcp-fallback).
- **Codegen:** `protoc-gen-trevrpc-rust` generates service traits, `register_*` functions and typed clients.

There are no Cargo features: the HTTP/3, WebTransport, and TCP fallback servers are a handful of small crates next to Quinn (`h3`, `h2`, `tokio-rustls`, and the [`qmux`](https://crates.io/crates/qmux) crate), so a feature matrix would buy almost no compile time.

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

Generate with `protoc-gen-trevrpc-rust`.

## Client

The complete, runnable version is [`examples/greeter_client.rs`](examples/greeter_client.rs).

```rust
use futures_util::StreamExt;

// `ChannelConfig::quic_config` sets the ALPN and sizes the QUIC windows.
let tls = quinn::rustls::ClientConfig::builder()
    .with_root_certificates(roots)
    .with_no_client_auth();
let mut endpoint = quinn::Endpoint::client("[::]:0".parse()?)?;
endpoint.set_default_client_config(trevrpc::client::ChannelConfig::new().quic_config(tls)?);

let channel = trevrpc::client::Channel::connect(endpoint, "127.0.0.1:50051".parse()?, "localhost").await?;
let client = greeter::GreeterClient::new(channel.clone());

// Unary
let reply = client.say_hello(greeter::HelloRequest { name: "TrevRPC".into() }).await?;

// Server streaming
let mut stream = client.lots_of_replies(greeter::HelloRequest { name: "TrevRPC".into() }).await?;
while let Some(reply) = stream.next().await { println!("{}", reply?.message); }

// Client streaming
let greetings = client.lots_of_greetings().await?;
greetings.send(greeter::HelloRequest { name: "Alice".into() }).await?;
greetings.send(greeter::HelloRequest { name: "Bob".into() }).await?;
let reply = greetings.close_and_recv().await?;

// Bidirectional streaming
let call = client.bidi_hello().await?;
let (sender, mut responses) = call.split();
sender.send(greeter::HelloRequest { name: "Alice".into() }).await?;
sender.finish()?;
while let Some(reply) = responses.next().await { println!("{}", reply?.message); }
```

### Response headers and trailing metadata

A response carries two metadata maps. Headers are sent before the first response message, so a streaming call sees them without waiting for a message; trailing metadata arrives with the final status. `ResponseStream::headers()` (also on `BidirectionalCall` and `ClientStreamingCall`) resolves once the first response frame arrives, with empty headers if the server sent none, and never loses a message it read ahead. The envelope methods fill `headers()` and `metadata()` (the trailing metadata) once the call completes.

```rust
let mut replies = client.lots_of_replies(greeter::HelloRequest { name: "Alice".into() }).await?;
let headers = replies.headers().await?;           // before the first message
while let Some(reply) = replies.next().await { reply?; }
let trailers = replies.terminal_metadata();       // after a clean end

let envelope = client.say_hello_envelope(greeter::HelloRequest { name: "Alice".into() }).await?;
let (reply, headers, trailers) = envelope.into_parts();
```

### Method paths

A call names its method by one path, `/package.Service/Method`. The generator emits the paths of each service as constants in a `<service>_paths` module (`greeter::greeter_paths::SAY_HELLO` is `"/example.greeter.Greeter/SayHello"`), and the generated clients and `register_*` functions use them. The lower-level API takes the path directly:

```rust
let reply: greeter::HelloReply =
    trevrpc::client::unary(&channel, greeter::greeter_paths::SAY_HELLO, &request, options).await?;
server.route(greeter::greeter_paths::SAY_HELLO, |body| async move { Ok(body) });
```

`Server::route*` panic when the path is not exactly `/<service>/<method>` with both parts non-empty. A request with a malformed path is rejected with `InvalidArgument`, and a well-formed path with no handler with `Unimplemented`. `RequestContext::service()`, `method()` and `full_method()` return the parts and the whole path.

## Server

The complete, runnable version is [`examples/greeter_server.rs`](examples/greeter_server.rs).

```rust
use futures_util::StreamExt;
use trevrpc::{BoxStream, ResponseEnvelope, Status, server::RequestContext};

struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(&self, _ctx: RequestContext, req: greeter::HelloRequest)
        -> Result<ResponseEnvelope<greeter::HelloReply>, Status> {
        Ok(ResponseEnvelope::new(greeter::HelloReply { message: format!("hello, {}", req.name) }))
    }
    async fn lots_of_replies(&self, _ctx: RequestContext, req: greeter::HelloRequest)
        -> Result<ResponseEnvelope<BoxStream<greeter::HelloReply>>, Status> {
        Ok(ResponseEnvelope::new(trevrpc::stream::from_iter([
            greeter::HelloReply { message: format!("hello, {}", req.name) },
            greeter::HelloReply { message: format!("goodbye, {}", req.name) },
        ])))
    }
    async fn lots_of_greetings(&self, _ctx: RequestContext, mut reqs: BoxStream<greeter::HelloRequest>)
        -> Result<ResponseEnvelope<greeter::HelloReply>, Status> {
        let mut names = Vec::new();
        while let Some(r) = reqs.next().await { names.push(r?.name); }
        Ok(ResponseEnvelope::new(greeter::HelloReply { message: names.join(", ") }))
    }
    async fn bidi_hello(&self, _ctx: RequestContext, reqs: BoxStream<greeter::HelloRequest>)
        -> Result<ResponseEnvelope<BoxStream<greeter::HelloReply>>, Status> {
        let replies: BoxStream<greeter::HelloReply> = Box::pin(reqs.map(|r| {
            r.map(|r| greeter::HelloReply { message: format!("hello, {}", r.name) })
        }));
        Ok(ResponseEnvelope::new(replies))
    }
}

let mut server = trevrpc::server::Server::new();
greeter::register_greeter(&mut server, GreeterService);

// `Server::quic_config` advertises the enabled ALPNs and aligns the QUIC limits with the options.
let tls = quinn::rustls::ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
let endpoint = quinn::Endpoint::server(server.quic_config(tls)?, "[::]:50051".parse()?)?;
server.serve(endpoint).await?;
```

Handlers attach response headers with `ResponseEnvelope::with_headers` and trailing metadata with `with_metadata`; both are validated like request metadata, and an invalid map fails the call with `Internal`. For streaming responses the headers go out as soon as the handler returns its envelope, before the first message is polled.

Native QUIC is always served. HTTP/3 and WebTransport are opt-in through `ServerOptions`; the `h3` ALPN is negotiated only when at least one is enabled:

```rust
server.set_options(
    trevrpc::server::ServerOptions::new()
        .with_http_enabled(true)         // HTTP/3 `POST` RPCs; off by default
        .with_webtransport_enabled(true)  // WebTransport sessions; off by default
        .with_http_path("/trevrpc")      // one path shared by both (the default)
        .with_webtransport_allowed_origins(["https://app.example"]),
);
```

WebTransport policy: a session must use the shared path, then pass the authority and origin allowlists. With no allowlists, requests that carry an `Origin` header (every browser) are denied; requests without one are admitted. `with_webtransport_admission` installs a callback that replaces the allowlists (the path is still checked first). `with_http_admission` is the equivalent hook for HTTP/3 `POST`. A CONNECT whose `WT-Available-Protocols` header offers WebTransport protocols but not `trevrpc-1` gets `400`; when it offers `trevrpc-1`, the response selects it with `WT-Protocol`. A missing, empty or malformed header means version 1.

`with_http_fallback` serves every other HTTP request on the same endpoint and TCP listener, over HTTP/3, HTTP/2 and HTTP/1.1, with any `tower_service::Service`, such as the static content of the web application that calls the server (`HttpFallback::new(tower_http::services::ServeDir::new("dist"))`). `TrevRPC` keeps the HTTP path while HTTP or WebTransport is enabled; see [Serving Other HTTP Content](https://trev.zip/llc/TrevRPC/wiki/HTTP3#serving-other-http-content).

`Server::serve_with_shutdown` drains in-flight RPCs for up to `ServerOptions::graceful_shutdown_timeout` before closing the endpoint.

`http3_enabled`, `http3_path`, and `http3_admission` (and their `with_*` builders) were renamed `http_enabled`, `http_path`, and `http_admission`, and `Http3Admission`/`Http3AdmissionRequest` became `HttpAdmission`/`HttpAdmissionRequest`, because the HTTP mapping is now also served over HTTP/2.

## Unreliable messages

A method can mark its streamed requests, its streamed responses, or both as unreliable with the `trevrpc.method` option from [`proto/trevrpc/options.proto`](../proto/trevrpc/options.proto), imported with the `proto/` directory on the include path:

```proto
import "trevrpc/options.proto";

service Telemetry {
  rpc Track(stream Position) returns (stream Position) {
    option (trevrpc.method) = {unreliable_requests: true, unreliable_responses: true};
  }
}
```

Their messages then travel in QUIC datagrams where the connection has them (native QUIC, and WebTransport on the server) and may be lost or reordered, never duplicated or corrupted. A message that does not fit a datagram, or whose datagram cannot be sent, goes on the stream, and QMux, HTTP/2 and HTTP/3 request streams keep every message on the stream. `protoc-gen-trevrpc-rust` reads the option and fails on a direction that does not stream; generated clients and `register_*` functions apply it.

The runtime options underneath are `trevrpc::Unreliable`, set on a client call with `CallOptions::with_unreliable` and on a server route with `RouteOptions::with_unreliable` and `Server::route_streaming_envelope_with_options`. A handler sees how many request messages arrived as datagrams with `RequestContext::datagram_requests_received`. The QUIC configurations from `Server::quic_config` and `ChannelConfig::quic_config` buffer at most 128 KiB of incoming datagrams per connection.

## TCP fallback

TCP is opt-in. `Server::tcp_tls_config` builds the TLS configuration for a TCP listener, setting the ALPN list from the options (`qmux-02.trevrpc-1` always, `h2` when HTTP is enabled, `http/1.1` for the WebSocket upgrade when WebTransport is enabled). `Server::serve_with_tcp` then serves QUIC and TCP together, sharing the connection and request limits and graceful shutdown:

```rust
let tcp = trevrpc::tcp::TcpFallback {
    listener: tokio::net::TcpListener::bind("[::]:50051").await?,
    tls: server.tcp_tls_config(tls),         // the same rustls config (clone it for `quic_config`); call after the options are final
};
server.serve_with_tcp(endpoint, tcp, shutdown_signal()).await?;
```

The channel falls back to QMux over TLS when given a rustls client configuration for it. QUIC starts first; QMux starts after the fallback delay (300 ms) or immediately if QUIC fails, and the first to connect wins. After QMux wins, reconnects use it until the reprobe interval (5 minutes) has passed since QUIC was lost.

```rust
use trevrpc::client::{ChannelConfig, TransportPolicy};

let config = ChannelConfig::new()
    .with_tcp_fallback(tls.clone())              // enables QMux over TLS
    .with_tcp_addr("127.0.0.1:50051".parse()?)   // default: the QUIC address
    .with_transport_policy(TransportPolicy::Auto) // or QuicOnly, TcpOnly
    .with_fallback_delay(std::time::Duration::from_millis(300))
    .with_reprobe_interval(std::time::Duration::from_secs(300));
let channel = trevrpc::client::Channel::connect_with_config(endpoint, addr, "localhost", config).await?;
println!("{:?}", channel.transport()); // Some(TransportKind::Quic) or Some(TransportKind::Qmux)
```

`ChannelEvent::Ready` carries the `transport` of the new generation. Without `with_tcp_fallback`, `Auto` is QUIC only. `client::QmuxTransport::connect(addr, server_name, tls)` opens a single QMux connection without reconnection. `remote_addr`, `rebind`, and `local_addr` concern the QUIC endpoint only. See [`crates/trevrpc-interop/src/server.rs`](crates/trevrpc-interop/src/server.rs) and [`tests/tcp_integration.rs`](tests/tcp_integration.rs) for complete usage.

## Interop

`trevrpc-interop-rust server|client` is the Rust half of the cross-runtime interop suite in `interop/`. The `Echo` and `UnreliableEcho` services are generated from `interop/proto/interop.proto` and `interop/proto/unreliable.proto`, the single copy of the contract.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --example greeter_server   # then, in another shell:
cargo run --example greeter_client
```
