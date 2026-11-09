# TrevRPC

[![check](https://trev.zip/llc/TrevRPC/actions/workflows/check.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=check&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=check.yaml)
[![vulnerable](https://trev.zip/llc/TrevRPC/actions/workflows/vulnerable.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=vulnerable&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=vulnerable.yaml)
[![android](https://shields.trev.zip/github/actions/workflow/status/spotdemo4/TrevRPC/android.yaml?branch=main&logo=android&logoColor=%23bac2de&label=android&labelColor=%23313244)](https://github.com/spotdemo4/TrevRPC/actions/workflows/android.yaml)
[![safari](https://shields.trev.zip/github/actions/workflow/status/spotdemo4/TrevRPC/safari.yaml?branch=main&logo=safari&logoColor=%23bac2de&label=safari&labelColor=%23313244)](https://github.com/spotdemo4/TrevRPC/actions/workflows/safari.yaml)
[![nixpkgs](https://nix-shield.trev.zip/?url=https://trev.zip/llc/TrevRPC/raw/branch/main/flake.lock&input=nixpkgs&logoColor=%23bac2de&labelColor=%23313244&color=%235277C3)](https://nixos.org/)

[![go](<https://shields.trev.zip/badge/dynamic/regex?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-go/go.mod&search=go%20(.*)&replace=%3E%3D%241&logo=go&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%2300ADD8>)](https://go.dev/doc/devel/release)
[![javascript](https://shields.trev.zip/badge/dynamic/json?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-js/package.json&query=%24.engines.node&logo=javascript&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23F7DF1E)](https://nodejs.org/en/about/previous-releases)
[![kotlin](https://shields.trev.zip/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-kotlin%2Fgradle%2Flibs.versions.toml&query=%24.versions.minimum-kotlin&prefix=%3E%3D&logo=kotlin&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%237F52FF)](https://kotlinlang.org/docs/releases.html)
[![rust](https://shields.trev.zip/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-rust%2FCargo.toml&query=%24.package.rust-version&prefix=%3E%3D&logo=rust&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23D34516)](https://releases.rs/)

[![pkg.go.dev](https://shields.trev.zip/badge/dynamic/json?url=https%3A%2F%2Fpkg.go.dev%2Fv1beta%2Fpackage%2Ftrev.zip%2Fllc%2Ftrevrpc%2Ftrevrpc-go&query=%24.version&logo=go&logoColor=%23bac2de&label=pkg.go.dev&labelColor=%23313244&color=%2300ADD8)](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go)
[![npm](https://shields.trev.zip/npm/v/%40trevrpc%2Ftrevrpc-js?logo=npm&logoColor=%23bac2de&labelColor=%23313244&color=%23F7DF1E)](https://www.npmjs.com/package/@trevrpc/trevrpc-js)
[![maven-central](https://shields.trev.zip/maven-central/v/zip.trev.trevrpc/core?strategy=highestVersion&logo=apachemaven&logoColor=%23bac2de&labelColor=%23313244&color=%237F52FF)](https://central.sonatype.com/search?namespace=zip.trev.trevrpc)
[![crates.io](https://shields.trev.zip/crates/v/trevrpc?labelColor=%23313244&color=%23D34516)](https://crates.io/crates/trevrpc)

Protobuf RPC over QUIC, HTTP/3 & WebTransport, with TCP fallbacks, for Go, Rust, JavaScript, and Kotlin

TrevRPC is an RPC framework like gRPC, but built on QUIC instead of HTTP/2. Write a protobuf service, generate
typed clients and servers, and call it from native apps, browsers, and Android. Each RPC gets its own QUIC
stream, so a lost packet stalls only the call it belongs to, and methods marked unreliable can send their
messages as QUIC datagrams.

> [!NOTE]
> Every TrevRPC package shares one version, and packages of the same version are compatible with each other:
> `trevrpc-rust` 1.7.0 works with `trevrpc-go` 1.7.0, `trevrpc-js` 1.7.0, and `trevrpc-kotlin` 1.7.0. Compatibility
> across different versions isn't guaranteed yet; that's a milestone for 2.0. TrevRPC speaks its own protocol and is
> not wire-compatible with gRPC, gRPC-Web, or Connect. See [current limitations](https://trev.zip/llc/TrevRPC/wiki/Current-Limitations).

## features

- unary, client-streaming, server-streaming, and bidirectional-streaming RPCs
- per-call deadlines and cancellation, request metadata, response headers and trailing metadata, and canonical
  status codes
- [connection metadata](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#connection-metadata):
  metadata such as a bearer token is set once on a channel, then sent in full once per connection and by ID
  after that
- [unreliable methods](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#datagram-messages)
  that send messages as QUIC datagrams, and fall back to the stream wherever datagrams are unavailable
- [zstd compression](https://trev.zip/llc/TrevRPC/wiki/Compression) with dictionaries trained on your own
  messages, and stream compression that compresses each message of a stream against the ones before it, both
  negotiated per connection
- one UDP port for native QUIC, HTTP/3, and WebTransport, and one TCP port for every fallback
- reconnecting client channels that race QUIC against TCP and fall back when UDP is blocked; RPCs are never
  retried, and a handler never runs twice for one call
- server limits, authorization hooks, interceptors, metrics, diagnostics, and graceful shutdown
- OpenTelemetry metrics and tracing for Go ([`oteltrevrpc`](trevrpc-go/oteltrevrpc)) and Rust
  ([`trevrpc-opentelemetry`](trevrpc-rust/crates/trevrpc-opentelemetry))
- built for deployment: certificates from ACME (TLS-ALPN-01 on the TCP port), PROXY protocol from TCP and UDP
  load balancers, QUIC address validation with Retry, and other HTTP content served from the same ports
- development certificates that browsers trust by hash, so local WebTransport needs no trusted CA
- a shared wire format pinned by golden vectors, and an [interop](interop) suite that tests every client
  against every server over every transport it speaks

## support

Servers are written in Go or Rust. Clients prefer QUIC and fall back to TCP for networks that block UDP and
browsers without WebTransport.

| runtime                            | server                                                    | client       | client fallback                     |
| ---------------------------------- | --------------------------------------------------------- | ------------ | ----------------------------------- |
| [Go](trevrpc-go)                   | native QUIC, HTTP/3, WebTransport, and every TCP fallback | native QUIC  | QMux over TLS                       |
| [Rust](trevrpc-rust)               | native QUIC, HTTP/3, WebTransport, and every TCP fallback | native QUIC  | QMux over TLS                       |
| [JavaScript](trevrpc-js) (browser) | —                                                         | WebTransport | QMux over WebSocket                 |
| [Kotlin](trevrpc-kotlin) (Android) | —                                                         | HTTP/3       | HTTP/2 (Cronet's built-in fallback) |

The browser client is supported on Chromium, Firefox, and Safari with both servers. See
[TCP fallback](https://trev.zip/llc/TrevRPC/wiki/TCP-Fallback) for how the fallbacks are served and chosen.

## example

Define a service:

```proto
syntax = "proto3";

package example.greeter;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Serve it from Go or Rust, over native QUIC, HTTP/3, and WebTransport on UDP port 50051, and every fallback on
TCP port 50051:

<details open>
<summary>Go</summary>

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + r.Name}, nil
}

server := trevrpc.NewServer()
options := server.Options()
options.EnableHTTP = true         // HTTP/3 and HTTP/2, for Android
options.EnableWebTransport = true // WebTransport and QMux over WebSocket, for browsers
options.WebTransportAdmission = trevrpc.SameOriginAdmission("https://example.com")
server.SetOptions(options)
greeter.RegisterGreeterServer(server, greeterService{})

// Binds the UDP port and, by default, the TCP port of the same number.
listener, err := trevrpc.Listen("0.0.0.0:50051", server, trevrpc.ListenOptions{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}},
})
if err != nil {
	return err
}
defer listener.Close()
return listener.Serve(ctx)
```

</details>

<details>
<summary>Rust</summary>

```rust
struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(
        &self,
        _ctx: trevrpc::server::RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply { message: format!("hello, {}", request.name) })
    }
}

let mut server = trevrpc::server::Server::new();
server.set_options(
    trevrpc::server::ServerOptions::new()
        .with_http_enabled(true) // HTTP/3 and HTTP/2, for Android
        .with_webtransport_enabled(true) // WebTransport and QMux over WebSocket, for browsers
        .with_webtransport_allowed_origins(["https://example.com"]),
);
greeter::register_greeter(&mut server, GreeterService);

let tls = quinn::rustls::ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
let addr = "[::]:50051".parse()?;
let endpoint = quinn::Endpoint::server(server.quic_config(tls.clone())?, addr)?;
// TCP is opt-in in Rust.
let tcp = trevrpc::tcp::TcpFallback::new(
    tokio::net::TcpListener::bind(addr).await?,
    server.tcp_tls_config(tls),
);
let shutdown = async {
    let _ = tokio::signal::ctrl_c().await;
};
server.serve_with_tcp(endpoint, tcp, shutdown).await?;
```

</details>

And call it from any client:

<details open>
<summary>Go</summary>

```go
// Native QUIC, falling back to QMux over TLS.
channel, err := trevrpc.Dial(ctx, "example.com:50051", trevrpc.DialOptions{TLSConfig: tlsConfig})
if err != nil {
	return err
}
defer channel.Close()

client := greeter.NewGreeterClient(channel, trevrpc.WithTimeout(5*time.Second))
reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
```

</details>

<details>
<summary>Rust</summary>

```rust
// Native QUIC, falling back to QMux over TLS.
let channel = trevrpc::client::Channel::builder("https://example.com:50051")?
    .connect()
    .await?;

let client = greeter::GreeterClient::with_default_call_options(
    channel.clone(),
    trevrpc::client::CallOptions::new().with_timeout(Duration::from_secs(5)),
);
let reply = client
    .say_hello(&greeter::HelloRequest { name: "TrevRPC".into() })
    .await?;
```

</details>

<details>
<summary>JavaScript (browser)</summary>

```ts
import { connect, createClient } from "@trevrpc/trevrpc-js";
import { Greeter } from "./gen/greeter_pb.js";

// WebTransport, falling back to QMux over WebSocket.
const channel = await connect("https://example.com:50051/trevrpc");

const client = createClient(Greeter, channel, { timeoutMs: 5_000 });
const reply = await client.sayHello({ name: "TrevRPC" });
```

</details>

<details>
<summary>Kotlin (Android)</summary>

```kotlin
// HTTP/3 through Cronet, falling back to HTTP/2.
val engine = CronetEngine.Builder(context).enableQuic(true).build()
val channel = CronetRpcChannel.create(engine, "https://example.com:50051", Executors.newSingleThreadExecutor())

val client = GreeterClient(channel, CallOptions(timeout = 5.seconds))
val reply = client.sayHello(HelloRequest.newBuilder().setName("TrevRPC").build())
```

</details>

Runnable greeter servers and clients are in [`trevrpc-go/examples`](trevrpc-go/examples) and
[`trevrpc-rust/examples`](trevrpc-rust/examples). [Getting started](https://trev.zip/llc/TrevRPC/wiki/Getting-Started)
covers code generation, and the runtime guides cover streaming calls.

## using

Each runtime has a library. Go, Rust, and Kotlin also have a `protoc` plugin that generates code from your
services; JavaScript uses the service descriptors that protobuf-es generates.

### go

```sh
go get trev.zip/llc/trevrpc/trevrpc-go
go install trev.zip/llc/trevrpc/trevrpc-go/cmd/protoc-gen-trevrpc-go@latest
```

### rust

```sh
cargo add trevrpc
cargo install --git https://trev.zip/llc/TrevRPC.git protoc-gen-trevrpc-rust
```

### javascript

the client takes the messages and service descriptors that [protobuf-es](https://github.com/bufbuild/protobuf-es) generates with `protoc-gen-es`:

```sh
npm install @trevrpc/trevrpc-js @bufbuild/protobuf
npm install --save-dev @bufbuild/protoc-gen-es
```

for [Effect](https://effect.website), [`@trevrpc/effect`](trevrpc-js/effect) makes clients whose calls are effects and streams:

```sh
npm install @trevrpc/effect effect
```

### kotlin

```kotlin
dependencies {
    implementation("zip.trev.trevrpc:core:<version>")
    implementation("zip.trev.trevrpc:transport-cronet:<version>")
}
```

`protoc-gen-trevrpc-kotlin` is available from [nix](#nix), or from a checkout with
`./gradlew :protoc-gen-trevrpc-kotlin:installDist` in `trevrpc-kotlin/`

### nix

the Go, Rust, and Kotlin packages provide their `protoc` plugins:

```sh
nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-go
nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-rust
nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-kotlin
```

### download

https://trev.zip/llc/TrevRPC/releases

## documentation

the [wiki](https://trev.zip/llc/TrevRPC/wiki) has the full documentation:

- [getting started](https://trev.zip/llc/TrevRPC/wiki/Getting-Started): define a service, generate code, and run
  a server and client
- runtime guides: [Go](https://trev.zip/llc/TrevRPC/wiki/Go-Guide),
  [Rust](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide),
  [JavaScript](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide), and
  [Kotlin](https://trev.zip/llc/TrevRPC/wiki/Kotlin-Guide)
- transports: [HTTP/3 and HTTP/2](https://trev.zip/llc/TrevRPC/wiki/HTTP3),
  [WebTransport](https://trev.zip/llc/TrevRPC/wiki/WebTransport), and
  [TCP fallback](https://trev.zip/llc/TrevRPC/wiki/TCP-Fallback)
- [protocol and wire format](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format)
- [operations, limits, and security](https://trev.zip/llc/TrevRPC/wiki/Operations-Limits-and-Security)
- [benchmarks](https://trev.zip/llc/TrevRPC/wiki/Benchmarks)

## contributing

see [CONTRIBUTING.md](CONTRIBUTING.md) for requirements and getting started
