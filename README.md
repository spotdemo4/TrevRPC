# TrevRPC

[![check](https://trev.zip/llc/TrevRPC/actions/workflows/check.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=check&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=check.yaml)
[![vulnerable](https://trev.zip/llc/TrevRPC/actions/workflows/vulnerable.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=vulnerable&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=vulnerable.yaml)
[![android](https://img.shields.io/github/actions/workflow/status/spotdemo4/TrevRPC/android.yaml?branch=main&logo=android&logoColor=%23bac2de&label=android&labelColor=%23313244)](https://github.com/spotdemo4/TrevRPC/actions/workflows/android.yaml)
[![safari](https://img.shields.io/github/actions/workflow/status/spotdemo4/TrevRPC/safari.yaml?branch=main&logo=safari&logoColor=%23bac2de&label=safari&labelColor=%23313244)](https://github.com/spotdemo4/TrevRPC/actions/workflows/safari.yaml)
[![nixpkgs](https://nix-shield.trev.zip/?url=https://trev.zip/llc/TrevRPC/raw/branch/main/flake.lock&input=nixpkgs&logoColor=%23bac2de&labelColor=%23313244&color=%235277C3)](https://nixos.org/)

[![go](<https://img.shields.io/badge/dynamic/regex?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-go/go.mod&search=go%20(.*)&replace=%241&logo=go&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%2300ADD8>)](https://go.dev/doc/devel/release)
[![javascript](https://img.shields.io/badge/dynamic/json?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-js/package.json&query=%24.engines.node&logo=javascript&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23F7DF1E)](https://nodejs.org/en/about/previous-releases)
[![kotlin](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-kotlin%2Fgradle%2Flibs.versions.toml&query=%24.versions.kotlin&logo=kotlin&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%237F52FF)](https://kotlinlang.org/docs/releases.html)
[![rust](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-rust%2FCargo.toml&query=%24.package.rust-version&logo=rust&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23D34516)](https://releases.rs/)

[![pkg.go.dev](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fpkg.go.dev%2Fv1beta%2Fpackage%2Ftrev.zip%2Fllc%2Ftrevrpc%2Ftrevrpc-go&query=%24.version&logo=go&logoColor=%23bac2de&label=pkg.go.dev&labelColor=%23313244&color=%2300ADD8)](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go)
[![npm](https://img.shields.io/npm/v/%40trevrpc%2Ftrevrpc-js?logo=npm&logoColor=%23bac2de&labelColor=%23313244&color=%23F7DF1E)](https://www.npmjs.com/package/@trevrpc/trevrpc-js)
[![maven-central](https://img.shields.io/maven-central/v/zip.trev.trevrpc/core?strategy=highestVersion&logo=apachemaven&logoColor=%23bac2de&labelColor=%23313244&color=%237F52FF)](https://central.sonatype.com/search?namespace=zip.trev.trevrpc)
[![crates.io](https://img.shields.io/crates/v/trevrpc?labelColor=%23313244&color=%23D34516)](https://crates.io/crates/trevrpc)

Protobuf RPC over QUIC, HTTP/3 & WebTransport, with TCP fallbacks, for Go, Rust, JavaScript, and Kotlin

TrevRPC is an RPC framework like gRPC, but built on QUIC instead of HTTP/2. Write a protobuf service, generate
typed clients and servers, and call it from native apps, browsers, and Android. Each RPC gets its own QUIC
stream, so a lost packet stalls only the call it belongs to, and methods marked unreliable can send their
messages as QUIC datagrams.

> [!WARNING]
> TrevRPC is a very early work in progress with no stability guarantees. It speaks its own protocol and is not
> wire-compatible with gRPC, gRPC-Web, or Connect. See [current limitations](https://trev.zip/llc/TrevRPC/wiki/Current-Limitations).

## features

- unary, client-streaming, server-streaming, and bidirectional-streaming RPCs
- per-call deadlines, request and response metadata, and canonical status codes
- [unreliable methods](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#datagram-messages)
  that send messages as QUIC datagrams, and fall back to the stream wherever datagrams are unavailable
- one UDP port for native QUIC, HTTP/3, and WebTransport, and one TCP port for every fallback
- reconnecting client channels that fall back to TCP when UDP is blocked; RPCs are never retried or replayed
- server limits, authorization hooks, metrics, and graceful shutdown
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
  rpc LotsOfReplies(HelloRequest) returns (stream HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Serve it from Go:

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, r *greeter.HelloRequest) (*trevrpc.Response[*greeter.HelloReply], error) {
	return &trevrpc.Response[*greeter.HelloReply]{Message: &greeter.HelloReply{Message: "hello, " + r.Name}}, nil
}

server := trevrpc.NewServer()
greeter.RegisterGreeterServer(server, greeterService{})

listener, err := trevrpc.Listen("0.0.0.0:50051", server, trevrpc.ListenOptions{
	TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}},
})
if err != nil {
	return err
}
defer listener.Close()
return listener.Serve(ctx)
```

And call it:

```go
channel, err := trevrpc.Dial(ctx, "example.com:50051", trevrpc.DialOptions{TLSConfig: tlsConfig})
if err != nil {
	return err
}
defer channel.Close()

client := greeter.NewGreeterClient(channel, trevrpc.WithTimeout(5*time.Second))
reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
```

Runnable greeter servers and clients are in [`trevrpc-go/examples`](trevrpc-go/examples) and
[`trevrpc-rust/examples`](trevrpc-rust/examples). [Getting started](https://trev.zip/llc/TrevRPC/wiki/Getting-Started)
covers code generation and the JavaScript and Kotlin clients.

## using

Each runtime has a library and a `protoc` plugin that generates code from your services.

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

the package includes `protoc-gen-trevrpc-js`, which runs on Node.js:

```sh
npm install @trevrpc/trevrpc-js
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

each runtime's package provides its `protoc` plugin:

```sh
nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-go
nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-rust
nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-js
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
