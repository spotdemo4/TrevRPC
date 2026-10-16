# trevrpc-rust

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

The Rust crate is built on [Quinn](https://github.com/quinn-rs/quinn):

- **Client:** `client::Channel` speaks native QUIC (ALPN `trevrpc/1`) and reconnects automatically. Rust has no HTTP/3 or WebTransport client; Android uses HTTP/3 and browsers use WebTransport.
- **Server:** `server::Server` serves native QUIC always, plus ordinary HTTP/3 (`POST /trevrpc`) and WebTransport (`/trevrpc`) when enabled, from one Quinn endpoint dispatching on the negotiated ALPN.
- **Codegen:** `protoc-gen-trevrpc-rust` generates service traits, `register_*` functions and typed clients.

There are no Cargo features: the HTTP/3 and WebTransport server is a handful of small crates next to Quinn, so a feature matrix would buy almost no compile time.

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

Native QUIC is always served. HTTP/3 and WebTransport are opt-in through `ServerOptions`; the `h3` ALPN is negotiated only when at least one is enabled:

```rust
server.set_options(
    trevrpc::server::ServerOptions::new()
        .with_http3_enabled(true)         // HTTP/3 `POST` RPCs; off by default
        .with_webtransport_enabled(true)  // WebTransport sessions; off by default
        .with_http3_path("/trevrpc")      // one path shared by both (the default)
        .with_webtransport_allowed_origins(["https://app.example"]),
);
```

WebTransport policy: a session must use the shared path, then pass the authority and origin allowlists. With no allowlists, requests that carry an `Origin` header (every browser) are denied; requests without one are admitted. `with_webtransport_admission` installs a callback that replaces the allowlists (the path is still checked first). `with_http3_admission` is the equivalent hook for HTTP/3 `POST`.

`Server::serve_with_shutdown` drains in-flight RPCs for up to `ServerOptions::graceful_shutdown_timeout` before closing the endpoint.

## Interop

`trevrpc-interop-rust server|client` is the Rust half of the cross-runtime interop suite in `interop/`. The `Echo` service is generated from `interop/proto/interop.proto`, the single copy of the contract.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --example greeter_server   # then, in another shell:
cargo run --example greeter_client
```
