# TrevRPC

[![check](https://trev.zip/llc/TrevRPC/actions/workflows/check.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=check&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=check.yaml)
[![vulnerable](https://trev.zip/llc/TrevRPC/actions/workflows/vulnerable.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=vulnerable&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=vulnerable.yaml)
[![rust](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2FCargo.toml&query=%24.package.rust-version&logo=rust&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23D34516)](https://releases.rs/)

Protobuf over QUIC & WebTransport.

## requirements

- [nix](https://nixos.org/)

## getting started

```sh
nix develop
```

### run

```sh
nix run .#dev
```

### format

```sh
nix fmt
```

### check

```sh
nix flake check
```

## protobuf generation

`protoc-gen-trevrpc` is a Buf/protoc plugin that generates TrevRPC service traits, clients, and
server registration glue.

Install the plugin locally while developing:

```sh
cargo install --path crates/protoc-gen-trevrpc
```

Example `buf.gen.yaml`:

```yaml
version: v2
plugins:
  - local: protoc-gen-trevrpc
    out: src/generated
    opt:
      - runtime_path=::trevrpc
```

The plugin emits one service file per protobuf package, such as `hello.v1.trevrpc.rs`. Include it
in the same Rust module as the corresponding prost messages:

```rust
pub mod hello {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/hello.v1.rs"));
        include!(concat!(env!("OUT_DIR"), "/hello.v1.trevrpc.rs"));
    }
}
```

Supported plugin options:

- `runtime_path=::trevrpc`
- `file_suffix=.trevrpc.rs`
- `package_root=crate`

## examples

Run the QUIC greeter server:

```sh
cargo run --example greeter_server
```

Then call it from another shell:

```sh
cargo run --example greeter_client -- TrevRPC
```

The server writes a local self-signed certificate to `target/trevrpc-example-cert.der`; the client
reads that certificate before connecting. Override the path with `TREVRPC_EXAMPLE_CERT`.

The examples also show the production-facing defaults:

- QUIC TLS ALPN is set to `trevrpc::ALPN` (`trevrpc/1`).
- Client calls use `trevrpc::client::CallOptions` for deadlines.
- Client calls attach request metadata through `CallOptions::with_metadata`.
- The server enforces metadata auth with `trevrpc::server::MetadataValueAuthorizer`.
- Server calls use `trevrpc::server::ServerOptions` for connection and RPC concurrency limits.
- The server uses `serve_quinn_with_shutdown` to refuse new connections on Ctrl+C, drain active streams, then force-close after the configured timeout.

## production hooks

Authentication can be enforced in two places:

- Transport identity belongs in Quinn/rustls. Configure Quinn's TLS client/server configs for real
  certificates or mTLS before constructing the endpoint.
- Request-level authorization belongs in TrevRPC metadata. Implement `trevrpc::server::Authorizer`
  or use `MetadataValueAuthorizer` for simple fixed metadata checks.
- Metadata keys are normalized by `CallOptions::with_metadata`, then validated on both client and
  server. User metadata keys must be lowercase ASCII using letters, digits, `.`, `_`, or `-`; the
  `trevrpc-` prefix is reserved for protocol use.
- Authorization runs after metadata validation but before route lookup, so unauthenticated callers
  do not learn whether a method exists.

Observability hooks are intentionally small:

- Enable the `tracing` feature for structured RPC lifecycle events.
- Install custom `trevrpc::server::Metrics` to collect RPC start/finish events, status codes,
  latency, and body sizes.
- Metrics callbacks run inline on the RPC task and must be fast/non-blocking. Forward to a channel
  or recorder instead of doing I/O in the callback.

The plugin has an integration test that compiles a real `.proto` fixture and exercises the
Buf/protoc plugin protocol. The fixture includes a `buf.gen.yaml` example for the generated service
path.

### build

```sh
nix build
```

### release

```sh
bumper
```

releases are created automatically for [significant](https://www.conventionalcommits.org/en/v1.0.0/#summary) changes

## use

### cargo

```sh
cargo install trevrpc \
  --index sparse+https://trev.zip/api/packages/llc/cargo/
```

### docker

```sh
docker run trev.zip/llc/trevrpc:latest
```

### nix

```sh
nix run git+https://trev.zip/llc/TrevRPC.git
```

### download

https://trev.zip/llc/TrevRPC/releases
