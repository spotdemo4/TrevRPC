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

// Bidirectional streaming
client.bidiHello(flowOf(req("Alice"), req("Bob"))).collect { println(it.message) }

// Per-call options start from the client's defaults
val quick = client.sayHello(req("TrevRPC")) { copy(timeout = 2.seconds) }

// A successful close proves every call has settled; only then release the engine and executor.
channel.close()
engine.shutdown()
executor.shutdown()
```

Requests use `Content-Type: application/trevrpc`, which names wire version 1; frames carry no version, so a server that does not serve it answers `415`. Every call names its method by one path (`/example.greeter.Greeter/SayHello`), which the generator emits as a `GREETER_SAY_HELLO_PATH` constant per method. Calling `Client` directly takes that path: `client.unary(GREETER_SAY_HELLO_PATH, request, requestCodec, responseCodec)`.

A generated client has one function per RPC, in the shape of the RPC: a unary call is a `suspend` function that returns the response, a server-streaming call returns a `Flow` of responses, a client-streaming call takes a `Flow` of requests and returns the response, and a bidirectional call takes a `Flow` of requests and returns a `Flow` of responses. A flow makes its call when it is collected, and cancelling the collection cancels the call. The last parameter, `options`, changes the client's default `CallOptions` for one call, as in `client.sayHello(request) { copy(timeout = 2.seconds) }`. `Client` also has `openServerStreaming`, `openClientStreaming`, and `openBidirectionalStreaming`, which return call objects for sending and receiving one message at a time.

Cronet decides between HTTP/3 and HTTP/2, so the wire format and the channel API are identical on both. Servers must keep their TCP port reachable for the fallback (Go uses the same port number for TCP and UDP). Their HTTP/2 responses carry an `Alt-Svc` header pointing at the UDP port, so Cronet can move to HTTP/3 when it is available.

### Headers and metadata

A response has two metadata maps. Response headers arrive in an optional `HEADERS` frame before the first message; trailing metadata arrives with the terminal `STATUS` frame. A call reports them to the `onResponseHeaders` and `onResponseMetadata` callbacks of its `CallOptions`: the headers (empty when the server sent none) as soon as the first response frame arrives, and the trailing metadata when the call succeeds, before its result is returned or its flow completes. A failed call carries its metadata on the `TrevRpcException`. A callback that throws fails the call.

```kotlin
var headers = Metadata.EMPTY
var trailers = Metadata.EMPTY
val reply =
    client.sayHello(req("TrevRPC")) {
        copy(onResponseHeaders = { headers = it }, onResponseMetadata = { trailers = it })
    }
println(headers["x-request-id"])   // response headers
println(trailers["x-cost"])        // trailing metadata
```

Frames use HTTP/3's layout (QUIC variable-length integer type and length, then the payload), so `core` skips unknown frame types, including after `STATUS`. Request messages are written raw in `MESSAGE` frames, and unary and server-streaming calls send `REQUEST`, the one `MESSAGE`, and the end of the request stream in a single write. See the [wire format](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format).

Failed calls throw `TrevRpcException` carrying a `Status`. Calls are never retried, and deadlines, idle timeouts, and response size limits are set through `CallOptions`.

### Ending a call early

A server may end a call before it has read the whole request. Client-streaming and bidirectional calls therefore read their response while they upload, in a coroutine of the `Client`'s `responseContext` (`Dispatchers.Default` unless you pass another context to `Client(channel, context)`). As soon as the server's terminal `STATUS` arrives, a `send` that is waiting on flow control is cancelled and later sends fail, even if nobody is calling `receive` and the server did not stop the request side:

| What ended the call                           | `send` throws                                                                  |
| --------------------------------------------- | ------------------------------------------------------------------------------ |
| Non-OK `STATUS`                               | `TrevRpcException` with that status, the same error `receive` reports          |
| OK `STATUS` before the upload finished        | `TrevRpcException` with `Code.CANCELLED`; `receive` still returns the response |
| Response failure (lost connection, bad frame) | `TrevRpcException` carrying that failure, not a generic "closed" error         |
| Response limit exceeded                       | `Code.RESOURCE_EXHAUSTED`, which is also what `receive` reports                |
| `closeSend()` or `close()` was called         | `Code.CANCELLED` ("request stream is closed")                                  |

`closeSend` does nothing once the call has ended. To stop at the first sign that the server is done and let `receive` report the outcome, use `sendAll(flow)`: it sends every request, half-closes, and returns normally when the server ends the call early. The generated functions, which take a `Flow` of requests, use it, so they return the response (or throw the status) rather than a `CANCELLED` error from the upload.

Finding the status can mean reading responses the caller has not asked for yet. While the request side is open, that read-ahead is bounded by the call's own response limits: `maxResponseMessages` and `maxResponseStreamBodySize` (4096 messages and 16 MiB when they are `null`, as they are by default), plus the frame that crosses a limit. A frame that crosses a limit you set ends the call with `RESOURCE_EXHAUSTED`, as it would for `receive`; with no limit set, reaching the bound pauses reading until you receive. After `closeSend` the read-ahead falls back to two frames and the transport's flow control paces the server. The reading coroutine stops at `STATUS` or when the call closes.

If the call's own deadline passes while the client waits for the end of the stream after `STATUS`, the server's status still decides the outcome. Cancelling the caller's coroutine at that point propagates the cancellation, and `terminalStatus` holds the server's status.

## Interop tests

`interop-android` generates its client from the shared [`interop/proto/interop.proto`](../interop/proto/interop.proto) and runs every scenario in [`interop/README.md`](../interop/README.md) from an emulator against a Go or Rust server on the host. It needs KVM, so it is built only with `-PtrevrpcAndroidInterop=true` and exposed as `trevrpc-kotlin.interopAndroid` in the flake:

```shell
nix shell .#trevrpc-kotlin.interopAndroid .#trevrpc-go --command \
  trevrpc-interop-android \
  --server-command trevrpc-interop-go \
  --output out/interop --work-dir /var/tmp/interop
```

`--transport http3` (the default) runs the scenarios over HTTP/3. `--transport http2` runs them with Cronet's QUIC disabled, as on a network that blocks UDP, to exercise the HTTP/2 mapping on the server's TCP listener. The GitHub `android` workflow runs both for each server.

The runner generates a certificate for `10.0.2.2` (the emulator's alias for the host), trusts it in the emulator's system store, and runs the server command with `server --cert <pem> --key <pem> --host 0.0.0.0`. It prints `ok <name>` or `fail <name>: <reason>` per scenario and exits 0 only if every scenario passed and the server exited 0 once its stdin closed.

## Development

```shell
./gradlew check                    # core, transport, and generator tests
./gradlew verifyStagedMavenRepository   # stage and verify the published artifacts
python3 -I -m unittest discover -s interop-android
```
