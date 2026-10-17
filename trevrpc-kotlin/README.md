# trevrpc-kotlin

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients, and call servers over ordinary HTTP/3.

Kotlin is client-only and targets Android: calls are `POST /trevrpc` over HTTP/3 through [Cronet](https://developer.android.com/develop/connectivity/cronet). When UDP is blocked, Cronet falls back to HTTP/2 over TCP on its own; Go and Rust servers serve the same HTTP mapping over HTTP/2 on their TCP listener, so there is nothing to configure. Servers are written in Go or Rust.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Modules

| Module                      | Purpose                                                            |
| --------------------------- | ------------------------------------------------------------------ |
| `core`                      | Client runtime: calls, framing, metadata, status, wire codec.      |
| `transport-cronet`          | `CronetRpcChannel`: the Cronet HTTP/3 transport.                   |
| `protoc-gen-trevrpc-kotlin` | `protoc` plugin that generates one client class per service.       |
| `interop-android`           | Android app and instrumented test for the cross-runtime scenarios. |

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

Generate Java messages and the client with `protoc`:

```shell
protoc \
  --java_out=build/generated \
  --plugin=protoc-gen-trevrpc-kotlin=path/to/protoc-gen-trevrpc-kotlin \
  --trevrpc-kotlin_out=build/generated \
  greeter.proto
```

## Client

The application owns the `CronetEngine` and callback executor; the channel never shuts them down. `transport-cronet` is a JVM 17 library that compiles against Cronet but does not ship it, so the application adds Cronet and chooses its provider: `org.chromium.net:cronet-bundled` embeds the native library, while `org.chromium.net:cronet` alone uses the platform `HttpEngine` (Android 14+) or a Google Play services provider. Cronet 500.x requires `minSdk` 24 (Android 7.0) and no longer merges its permissions into the app, so the application must declare `android.permission.INTERNET` and `android.permission.ACCESS_NETWORK_STATE`.

```kotlin
val engine = CronetEngine.Builder(context).enableQuic(true).build()
val executor = Executors.newSingleThreadExecutor()
val channel = CronetRpcChannel.create(engine, "https://rpc.example.com", executor)
val client = GreeterClient(channel, CallOptions(timeout = 10.seconds))

fun req(name: String) = HelloRequest.newBuilder().setName(name).build()

// Unary
val reply = client.sayHello(req("TrevRPC"))

// Server streaming
client.lotsOfReplies(req("TrevRPC")).collect { println(it.message) }

// Client streaming
val summary = client.lotsOfGreetings(flowOf(req("Alice"), req("Bob")))

// Bidirectional streaming (Flow)
client.bidiHello(flowOf(req("Alice"), req("Bob"))).collect { println(it.message) }

// Bidi with explicit sends
val bidi = client.bidiHello()
bidi.send(req("Alice"))
println(bidi.receive()?.message)
bidi.closeSend()

// A successful close proves every call has settled; only then release the engine and executor.
channel.close()
engine.shutdown()
executor.shutdown()
```

Cronet decides between HTTP/3 and HTTP/2, so the wire format and the channel API are identical on both. Servers must keep their TCP port reachable for the fallback (Go uses the same port number for TCP and UDP). Their HTTP/2 responses carry an `Alt-Svc` header pointing at the UDP port, so Cronet can move to HTTP/3 when it is available.

Failed calls throw `TrevRpcException` carrying a `Status`. Calls are never retried, and deadlines, idle timeouts, and response size limits are set through `CallOptions`.

## Interop tests

`interop-android` generates its client from the shared [`interop/proto/interop.proto`](../interop/proto/interop.proto) and runs every scenario in [`interop/README.md`](../interop/README.md) from an emulator against a Go or Rust server on the host. It needs KVM, so it is built only with `-PtrevrpcAndroidInterop=true` and exposed as `trevrpc-kotlin.interopAndroid` in the flake:

```shell
nix shell .#trevrpc-kotlin.interopAndroid .#trevrpc-go --command \
  trevrpc-interop-android \
  --server-command "trevrpc-interop-go server" \
  --output out/interop --work-dir /var/tmp/interop
```

`--transport http3` (the default) runs the scenarios over HTTP/3. `--transport http2` runs them with Cronet's QUIC disabled, as on a network that blocks UDP, to exercise the HTTP/2 mapping on the server's TCP listener. The GitHub `android` workflow runs both for each server.

The runner generates a certificate for `10.0.2.2` (the emulator's alias for the host), trusts it in the emulator's system store, appends `--host 0.0.0.0 --cert <pem> --key <pem>` to the server command, and prints `ok <name>` or `fail <name>: <reason>` per scenario. It exits 0 only if every scenario passed.

## Development

```shell
./gradlew check                    # core, transport, and generator tests
./gradlew verifyStagedMavenRepository createCentralBundle
python3 -I -m unittest discover -s interop-android
```
