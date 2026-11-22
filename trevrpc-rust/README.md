# trevrpc-rust

The Rust runtime for [TrevRPC](https://trev.zip/llc/TrevRPC), an RPC framework like gRPC built on QUIC instead of HTTP/2: servers that serve native QUIC, HTTP/3 and WebTransport from one Quinn endpoint and every TCP fallback on one TCP port, and native QUIC clients that reconnect automatically and fall back to QMux over TLS when UDP is blocked. It is built on [Quinn](https://github.com/quinn-rs/quinn), with the [`qmux`](https://crates.io/crates/qmux) crate, `h2` and `tokio-rustls` for the TCP fallbacks. Rust has no HTTP/3 or WebTransport client.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.12.0 works with `trevrpc-go` 1.12.0, `trevrpc-js` 1.12.0, `trevrpc-kotlin`
> 1.12.0, and the Erlang `trevrpc` 1.12.0. Compatibility across different versions isn't guaranteed yet; that's a
> milestone for 2.0.

## Packages

| Package                                                                                                               | Install                                                                        | Purpose                                                                                             |
| --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| [`trevrpc`](https://crates.io/crates/trevrpc)                                                                         | `cargo add trevrpc`                                                            | Client and server runtime                                                                           |
| [`protoc-gen-trevrpc-rust`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-rust/crates/protoc-gen-trevrpc-rust) | `cargo install --git https://trev.zip/llc/TrevRPC.git protoc-gen-trevrpc-rust` | `protoc` plugin: service traits, `register_*` functions and typed clients                           |
| [`trevrpc-opentelemetry`](https://crates.io/crates/trevrpc-opentelemetry)                                             | `cargo add trevrpc-opentelemetry`                                              | [OpenTelemetry](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide#observability) metrics and tracing     |
| [`trevrpc-protovalidate`](https://crates.io/crates/trevrpc-protovalidate)                                             | `cargo add trevrpc-protovalidate`                                              | [protovalidate](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide#request-validation) request validation |

The `trevrpc-rust` Nix package provides the plugin: `nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-rust`.

## Example

Generate the service code with `protoc-gen-trevrpc-rust`, and the messages with `prost`:

```sh
protoc --trevrpc-rust_out=. greeter.proto
```

Serve it:

```rust
struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(&self, _ctx: trevrpc::server::RequestContext, request: greeter::HelloRequest)
        -> Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply { message: format!("hello, {}", request.name) })
    }
}

let mut server = trevrpc::server::Server::new();
greeter::register_greeter(&mut server, GreeterService);

let tls = quinn::rustls::ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
let endpoint = quinn::Endpoint::server(server.quic_config(tls)?, "[::]:50051".parse()?)?;
server.serve(endpoint).await?;
```

And call it:

```rust
let channel = trevrpc::client::Channel::builder("https://localhost:50051")?
    .connect_timeout(std::time::Duration::from_secs(5))
    .connect()
    .await?;
let client = greeter::GreeterClient::new(channel.clone());
let reply = client.say_hello(&greeter::HelloRequest { name: "TrevRPC".into() }).await?;
channel.close();
```

Runnable servers and clients are in [`examples`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-rust/examples).

## Documentation

The [Rust guide](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide) covers generated code, servers, clients, streams, statuses, rate limiting, request validation, and observability. The rest of the [wiki](https://trev.zip/llc/TrevRPC/wiki) covers the transports, the protocol, compression, and operations.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --features dev-certificate --example greeter_server   # then, in another shell:
cargo run --example greeter_client
```

`crates/trevrpc-interop` (installed as `trevrpc-interop-rust`, not published) implements the `server` and `client` subcommands of the cross-runtime [interop suite](https://trev.zip/llc/TrevRPC/src/branch/main/interop/README.md), and the `bench` load client of the [benchmarks](https://trev.zip/llc/TrevRPC/src/branch/main/bench/README.md). Its `Echo`, `UnreliableEcho`, `CompressedEcho` and `RefusingEcho` services are generated from `interop/proto/interop.proto`, `interop/proto/unreliable.proto` and `interop/proto/compressed.proto`, the single copy of the contract.

See [Local Development](https://trev.zip/llc/TrevRPC/wiki/Local-Development) for the repository-wide checks.
