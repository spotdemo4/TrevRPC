# trevrpc-rust

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC, with TCP fallbacks for networks that block UDP.

The Rust crate is built on [Quinn](https://github.com/quinn-rs/quinn):

- **Client:** `client::Channel` speaks native QUIC (ALPN `trevrpc/1`) and reconnects automatically. It can fall back to QMux over TLS (ALPN `qmux-02.trevrpc-1`) when UDP is blocked. Rust has no HTTP/3 or WebTransport client; Android uses HTTP/3 and browsers use WebTransport.
- **Server:** `server::Server` serves native QUIC always, plus ordinary HTTP/3 (`POST /trevrpc`) and WebTransport (`/trevrpc`) when enabled, from one Quinn endpoint dispatching on the negotiated ALPN. With a TCP listener it also serves QMux over TLS, HTTP/2, and QMux over WebSocket; see [TCP fallback](#tcp-fallback).
- **Codegen:** `protoc-gen-trevrpc-rust` generates service traits, `register_*` functions and typed clients.

The transports need no Cargo features: the HTTP/3, WebTransport, and TCP fallback servers are a handful of small crates next to Quinn (`h3`, `h2`, `tokio-rustls`, and the [`qmux`](https://crates.io/crates/qmux) crate), so a feature matrix would buy almost no compile time. Two features are optional: `webpki-roots` (on by default) gives `Channel::builder` the Mozilla roots to verify servers with, and `dev-certificate` adds the development certificate below and its `rcgen` dependency.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.8.0 works with `trevrpc-go` 1.8.0, `trevrpc-js` 1.8.0, and `trevrpc-kotlin`
> 1.8.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

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

// Resolves the host, tries each of its addresses, and verifies the server with the webpki roots.
let channel = trevrpc::client::Channel::builder("https://example.com:50051")?
    .connect_timeout(std::time::Duration::from_secs(5))
    .connect()
    .await?;
let client = greeter::GreeterClient::new(channel.clone());

// Unary
let reply = client.say_hello(&greeter::HelloRequest { name: "TrevRPC".into() }).await?;

// Server streaming
let mut stream = client.lots_of_replies(&greeter::HelloRequest { name: "TrevRPC".into() }).await?;
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

// Per-call options, and an existing stream of requests
let reply = client
    .say_hello(&greeter::HelloRequest { name: "TrevRPC".into() })
    .timeout(std::time::Duration::from_secs(5))
    .metadata("authorization", "Bearer token")
    .await?;
let reply = client.lots_of_greetings().send_all(trevrpc::stream::from_iter(requests)).await?;
```

`Channel::builder(url)` takes `https://host[:port]` or `host:port` (port 443 by default). It resolves the host before every dial and tries each address Happy Eyeballs style, alternating IPv6 and IPv4 and starting the next attempt 250 ms after the one before, with a Quinn endpoint for each address family. The builder also sets `roots`, `add_root_certificates`, `tls` (for a client certificate), `server_name`, `tcp_fallback` (on by default), `connect_timeout` and `config` (a `ChannelConfig`). For full control over the endpoint and address, build the TLS configuration and endpoint yourself and call `Channel::connect_with_config`:

```rust
// `ChannelConfig::quic_config` sets the ALPN and sizes the QUIC windows.
let tls = quinn::rustls::ClientConfig::builder()
    .with_root_certificates(roots)
    .with_no_client_auth();
let mut endpoint = quinn::Endpoint::client("[::]:0".parse()?)?;
let config = trevrpc::client::ChannelConfig::new();
endpoint.set_default_client_config(config.quic_config(tls)?);
let channel = trevrpc::client::Channel::connect_with_config(endpoint, "127.0.0.1:50051".parse()?, "localhost", config).await?;
```

A client method returns a `trevrpc::client::Call`, and awaiting it makes the call. Its options start from the client's default call options (`GreeterClient::with_default_call_options`); `.timeout(d)`, `.metadata(key, value)` and `.options(|options| ...)` change them for that call only. `.with_response()` also returns the response headers and trailing metadata, and `.send_all(requests)` sends a stream of requests and finishes it.

`trevrpc::bearer_metadata(token)` returns the `authorization` entry of a bearer token (`let (key, value) = trevrpc::bearer_metadata(&token)?;` for `.metadata(key, value)`, or `Metadata::from([trevrpc::bearer_metadata(&token)?])` for the connection metadata below), and fails with `InvalidArgument` for a token that `trevrpc::server::bearer_token` would reject as malformed (anything but an RFC 6750 `b64token`).

Metadata that every call carries, such as a bearer token, can instead be set once with `ChannelConfig::with_connection_metadata` and replaced with `Channel::set_connection_metadata`. Each connection then sends it in its first calls only, and the server merges it into every call's metadata, a call's own entry winning; see [Connection Metadata](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#connection-metadata).

### Response headers and trailing metadata

A response carries two metadata maps. Headers are sent before the first response message, so a streaming call sees them without waiting for a message; trailing metadata arrives with the final status. `ResponseStream::headers()` (also on `BidirectionalCall` and `ClientStreamingCall`) resolves once the headers, the first message or the final status arrives, with empty headers if the server sent none, and never loses a message it read ahead. For unary and client-streaming calls, `.with_response()` (and `ClientStreamingCall::close_and_recv_with_response`) returns a `ResponseEnvelope` whose `headers()` and `metadata()` (the trailing metadata) are filled once the call completes.

```rust
let mut replies = client.lots_of_replies(&greeter::HelloRequest { name: "Alice".into() }).await?;
let headers = replies.headers().await?;           // before the first message
while let Some(reply) = replies.next().await { reply?; }
let trailers = replies.terminal_metadata();       // after a clean end

let envelope = client.say_hello(&greeter::HelloRequest { name: "Alice".into() }).with_response().await?;
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
use trevrpc::{BoxStream, Status, server::RequestContext};

struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(&self, ctx: RequestContext, req: greeter::HelloRequest)
        -> Result<greeter::HelloReply, Status> {
        ctx.set_response_metadata("greeting-language", "en")?; // optional
        Ok(greeter::HelloReply { message: format!("hello, {}", req.name) })
    }
    async fn lots_of_replies(&self, _ctx: RequestContext, req: greeter::HelloRequest)
        -> Result<BoxStream<greeter::HelloReply>, Status> {
        Ok(trevrpc::stream::from_iter([
            greeter::HelloReply { message: format!("hello, {}", req.name) },
            greeter::HelloReply { message: format!("goodbye, {}", req.name) },
        ]))
    }
    async fn lots_of_greetings(&self, _ctx: RequestContext, mut reqs: BoxStream<greeter::HelloRequest>)
        -> Result<greeter::HelloReply, Status> {
        let mut names = Vec::new();
        while let Some(r) = reqs.next().await { names.push(r?.name); }
        Ok(greeter::HelloReply { message: names.join(", ") })
    }
    async fn bidi_hello(&self, _ctx: RequestContext, reqs: BoxStream<greeter::HelloRequest>)
        -> Result<BoxStream<greeter::HelloReply>, Status> {
        Ok(Box::pin(reqs.map(|r| {
            r.map(|r| greeter::HelloReply { message: format!("hello, {}", r.name) })
        })))
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

Handlers set response metadata through their `RequestContext`. `context.set_response_header(key, value)` adds a response header, sent before the first response message as soon as the handler returns. `context.set_response_metadata(key, value)` adds trailing metadata, sent with the final `OK` status: when a unary or client-streaming handler returns, or when a streaming handler's response stream ends, so a clone of the context moved into the stream may still set it until then. Keys are lowercased and validated like request metadata; an invalid entry, a header set after the handler has returned, or metadata set after the final status, is an `Internal` status, ready for `?`. A failed call sends the metadata of its `Status` instead.

`RequestContext::remote_addr()` is the client's address: the QUIC connection's remote address for native QUIC, HTTP/3 and WebTransport calls, and on the TCP listener the TCP peer's, or the client a trusted proxy names in its PROXY header (see [TCP fallback](#tcp-fallback)). An IPv4 client's address is IPv4 even on a dual-stack socket. Authorizers get it in their `AuthorizationRequest`, beside the REQUEST frame, and admission callbacks in `HttpAdmissionRequest::remote_addr` and `WebTransportAdmissionRequest::remote_addr`, so per-client limits, such as on login attempts, can key on it.

Native QUIC is always served. HTTP/3 and WebTransport are opt-in through `ServerOptions`; the `h3` ALPN is negotiated only when at least one is enabled or an HTTP fallback is set:

```rust
server.set_options(
    trevrpc::server::ServerOptions::new()
        .with_http_enabled(true)         // HTTP/3 `POST` RPCs; off by default
        .with_webtransport_enabled(true)  // WebTransport sessions; off by default
        .with_http_path("/trevrpc")      // one path shared by both (the default)
        .with_webtransport_allowed_origins(["https://app.example"]),
);
```

WebTransport policy: a session must use the shared path, then pass the authority and origin allowlists. With no allowlists, requests that carry an `Origin` header (every browser) are denied; requests without one are admitted. `with_webtransport_admission` installs a `WebTransportAdmission` that replaces the allowlists (the path is still checked first): `WebTransportAdmission::new(f)` wraps a closure, and `WebTransportAdmission::same_origin(allowed_origins)?` admits the server's own pages and the listed origins. `with_http_admission` takes the equivalent `HttpAdmission` for HTTP `POST`. A CONNECT whose `WT-Available-Protocols` header offers WebTransport protocols but not `trevrpc-1` gets `400`; when it offers `trevrpc-1`, the response selects it with `WT-Protocol`. A missing, empty or malformed header means version 1.

`with_http_fallback` serves every other HTTP request on the same endpoint and TCP listener, over HTTP/3, HTTP/2 and HTTP/1.1, with any `tower_service::Service`, such as the static content of the web application that calls the server (`HttpFallback::new(tower_http::services::ServeDir::new("dist"))`). `TrevRPC` keeps the HTTP path while HTTP or WebTransport is enabled; see [Serving Other HTTP Content](https://trev.zip/llc/TrevRPC/wiki/HTTP3#serving-other-http-content).

For local development, `server::DevelopmentCertificate::new()` (feature `dev-certificate`) generates a self-signed ECDSA certificate for `localhost`, `127.0.0.1` and `::1` that rotates itself within the 14-day limit browsers place on certificates they trust by hash. Pass its `server_config()` to `quic_config` and `tcp_tls_config`. As a `tower_service::Service` it answers its current and next SHA-256 hashes as JSON, which the browser client's `serverCertificateHashesUrl` reads, and `pem()` returns both certificates for native clients to trust. See [Development certificates](https://trev.zip/llc/TrevRPC/wiki/WebTransport#development-certificates).

In production, [`rustls-acme`](https://crates.io/crates/rustls-acme) (with its `ring` feature) can get certificates from Let's Encrypt: pass its `AcmeState::challenge_rustls_config()` to `quic_config` and `tcp_tls_config`, and poll the state. `tcp_tls_config` keeps the `acme-tls/1` that configuration lists, so `rustls-acme` can answer TLS-ALPN-01 challenges when the TCP listener is on port 443. See [Certificates from ACME](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide#certificates-from-acme).

Interceptors run the rest of a call in a `tracing` span with `RequestContext::set_span`. The separate crate [`trevrpc-opentelemetry`](crates/trevrpc-opentelemetry) builds OpenTelemetry metrics, server spans and trace propagation on the interceptor and metrics hooks:

```rust
server.set_metrics(trevrpc_opentelemetry::Metrics::global());
server.add_interceptor(trevrpc_opentelemetry::Interceptor::new());

let reply = client
    .say_hello(&request)
    .options(trevrpc_opentelemetry::propagate)
    .await?;
```

See [Observability](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide#observability).

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

The runtime options underneath are `trevrpc::Unreliable`, set on a client call with `CallOptions::with_unreliable` and on a server route with `RouteOptions::with_unreliable` and `Server::route_streaming_with_options`. A handler sees how many request messages arrived as datagrams with `RequestContext::datagram_requests_received`. The QUIC configurations from `Server::quic_config` and `ChannelConfig::quic_config` buffer at most 128 KiB of incoming datagrams per connection.

## Compression

Messages can travel compressed with zstd (see [Compression](https://trev.zip/llc/TrevRPC/wiki/Compression)), in two ways that combine:

- **Dictionaries:** `ChannelConfig::with_compression` and `ServerOptions::with_compression` take a `Compression` holding dictionaries trained with `zstd --train`, each with the protobuf message types it compresses. Generated code passes each method's message types, which pick the dictionaries.
- **Stream compression:** a streaming method marked `option (trevrpc.method) = {compression: COMPRESSION_STREAM}` compresses each streamed direction with one zstd context per call, from its third message, with nothing to configure. `ServerOptions::with_stream_compression_memory` (256 MiB by default) and `ChannelConfig::with_stream_compression_memory` (64 MiB) bound the memory those contexts hold; 0 turns it off.

## TCP fallback

TCP is opt-in. `Server::tcp_tls_config` builds the TLS configuration for a TCP listener, setting the ALPN list from the options (`qmux-02.trevrpc-1` always, `h2` when HTTP is enabled, `http/1.1` for the WebSocket upgrade when WebTransport is enabled; an HTTP fallback adds both). `Server::serve_with_tcp` then serves QUIC and TCP together, sharing the connection and request limits and graceful shutdown:

```rust
let tcp = trevrpc::tcp::TcpFallback::new(
    tokio::net::TcpListener::bind("[::]:50051").await?,
    server.tcp_tls_config(tls),              // the same rustls config (clone it for `quic_config`); call after the options are final
);
server.serve_with_tcp(endpoint, tcp, shutdown_signal()).await?;
```

Behind a TCP load balancer, `with_trusted_proxies` names its networks. A connection from one of them must start with a PROXY protocol header, version 1 or 2, and the client it names becomes the calls' `remote_addr`; a connection without one is closed. A version 2 `LOCAL` header (a health check), version 1 `UNKNOWN`, or an address family other than TCP over IPv4 or IPv6 leaves the proxy's own address. Connections from anywhere else are never read for a header. The header must arrive within the TLS handshake timeout (10 s).

```rust
let tcp = tcp.with_trusted_proxies(["10.0.0.0/8".parse::<trevrpc::tcp::IpPrefix>()?]);
```

Behind a UDP load balancer that puts a PROXY protocol version 2 header in front of every datagram, bind the endpoint with `trevrpc::udp::QuicEndpoint::server`, which every `serve` method takes in place of a `quinn::Endpoint`. Its socket strips the header of every datagram from the trusted networks, and the client a load balancer's address names when the connection on it completes its handshake becomes the connection's `remote_addr` (looked up again only when the connection's address changes), while QUIC keeps talking to the load balancer. A datagram from a trusted network without a valid header is dropped and logged as a `tracing` warning, at most once every 10 seconds; a `LOCAL` header, or an address family other than UDP over IPv4 or IPv6, leaves the load balancer's own address. Datagrams from anywhere else are never read for a header. The header takes up to 52 bytes of every datagram, plus any extensions.

`ServerOptions::with_address_validation(AddressValidation::AboveRate(n))` makes clients prove their address with a QUIC Retry, which costs them a round trip, once more than `n` connection attempts from unvalidated addresses arrive a second; `AddressValidation::Always` asks every one.

```rust
let endpoint = trevrpc::udp::QuicEndpoint::server(
    server.quic_config(tls.clone())?,
    "[::]:50051".parse()?,
    ["10.0.0.0/8".parse::<trevrpc::tcp::IpPrefix>()?],
)?;
```

HTTP/2 and HTTP/1.1 responses advertise HTTP/3 on the endpoint's port in `Alt-Svc`. When clients reach the endpoint on another port, such as through a reverse proxy, load balancer or NAT, `with_alt_svc_port(NonZeroU16::new(443))` advertises that port instead.

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

`ChannelEvent::Ready` carries the `transport` of the new generation. Without `with_tcp_fallback`, `Auto` is QUIC only; `Channel::builder` enables it by default, with its own TLS configuration and the URL's host and port. `client::QmuxTransport::connect(addr, server_name, tls)` opens a single QMux connection without reconnection. `local_addr` and `rebind` concern the QUIC endpoint only. See [`crates/trevrpc-interop/src/server.rs`](crates/trevrpc-interop/src/server.rs) and [`tests/tcp_integration.rs`](tests/tcp_integration.rs) for complete usage.

## Interop

`trevrpc-interop-rust server|client` is the Rust half of the cross-runtime interop suite in `interop/`, and `trevrpc-interop-rust bench` is its load client for the [benchmarks](../bench/README.md). The `Echo`, `UnreliableEcho`, `CompressedEcho` and `RefusingEcho` services are generated from `interop/proto/interop.proto`, `interop/proto/unreliable.proto` and `interop/proto/compressed.proto`, the single copy of the contract.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --features dev-certificate --example greeter_server   # then, in another shell:
cargo run --example greeter_client
```
