# trevrpc-kotlin

TrevRPC's Kotlin implementation provides the core RPC runtime, Netty and Cronet channel providers,
and a protobuf code generator. The examples below use the generated Greeter API from
[`examples/src/main/proto/greeter.proto`](examples/src/main/proto/greeter.proto), which defines
unary, server-streaming, client-streaming, and bidirectional-streaming methods.

## Client

Create one application `RpcChannel`, wait for initial readiness, and pass it to generated clients:

```kotlin
import kotlinx.coroutines.flow.flowOf
import zip.trev.trevrpc.examples.greeter.GreeterClient
import zip.trev.trevrpc.examples.greeter.HelloRequest
import zip.trev.trevrpc.netty.NettyClientTls
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyRpcChannel
import java.net.InetSocketAddress

val channel =
    NettyRpcChannel.nativeQuic(
        NettyQuicClientConfig(
            remoteAddress = InetSocketAddress("rpc.example.com", 7443),
            tls = NettyClientTls("rpc.example.com"),
        ),
    )
channel.awaitReady()
val client = GreeterClient(channel)

fun request(name: String): HelloRequest =
    HelloRequest
        .newBuilder()
        .setName(name)
        .build()
```

Use `NettyRpcChannel.http3(config)` for ordinary HTTP/3. Netty channels reconnect future calls after
a connection closes. Calls fail fast while the channel is connecting, and an interrupted call is
never retried or replayed. Channels do not use 0-RTT. Observe `channel.state`, use `awaitReady()`
when readiness is required, and call `channel.close()` when the application is done.

`RpcTransport` remains the generated-client and test integration SPI. Applications should use an
`RpcChannel` factory rather than assembling transport lifecycle components. Deterministic tests
and benchmarks can explicitly own one connection with
`RawNettyQuicRpcTransport` or `RawNettyHttp3RpcTransport` from
`zip.trev.trevrpc.netty.advanced`, then call `shutdown()` when finished.

The [examples guide](examples/README.md) also shows Android Cronet channel setup.

### Unary

```kotlin
val reply = client.sayHello(request("TrevRPC"))
println(reply.message)
```

### Server streaming

The generated method returns a cold `Flow`. Collecting it consumes messages and validates the
terminal RPC status:

```kotlin
client.lotsOfReplies(request("TrevRPC")).collect { reply ->
    println(reply.message)
}
```

### Client streaming

Pass a request `Flow` and await the single response:

```kotlin
val reply =
    client.lotsOfGreetings(
        flowOf(
            request("Alice"),
            request("Bob"),
        ),
    )
println(reply.message)
```

For interactive production, use the generated `client.lotsOfGreetings()` call object:

```kotlin
val call = client.lotsOfGreetings()
try {
    call.send(request("Alice"))
    call.send(request("Bob"))
    val reply = call.receive().message
    println(reply.message)
} finally {
    call.close()
}
```

`receive` half-closes the request side, receives exactly one response, and validates the terminal
status.

### Bidirectional streaming

The `Flow` overload sends requests concurrently while responses are collected:

```kotlin
client
    .bidiHello(
        flowOf(
            request("Alice"),
            request("Bob"),
        ),
    ).collect { reply ->
        println(reply.message)
    }
```

For explicitly interleaved sends and receives, use the call object:

```kotlin
val call = client.bidiHello()
try {
    for (name in listOf("Alice", "Bob")) {
        call.send(request(name))
        val reply = checkNotNull(call.receive()) { "BidiHello ended early" }
        println(reply.message)
    }
    call.closeSend()
    while (true) {
        val reply = call.receive() ?: break
        println(reply.message)
    }
} finally {
    call.close()
}
```

Use independent sender and receiver coroutines when a protocol must continuously read and write
large bidi streams. Interactive `send` calls wait for transport write completion, and `closeSend`
waits for the request-side FIN.

## Server

Implement the generated `GreeterService` interface. Registration wires each method to the matching
RPC shape.

### Unary

```kotlin
class GreeterServiceImpl : GreeterService {
    override suspend fun sayHello(
        context: RequestContext,
        request: HelloRequest,
    ): HelloReply = reply("hello, ${request.name}")

    // Implement the three streaming methods below in the same class.

    private fun reply(message: String): HelloReply =
        HelloReply
            .newBuilder()
            .setMessage(message)
            .build()
}
```

### Server streaming

Return a response `Flow`; `flowOf` is convenient for a fixed sequence:

```kotlin
override suspend fun lotsOfReplies(
    context: RequestContext,
    request: HelloRequest,
): Flow<HelloReply> =
    flowOf(
        reply("hello, ${request.name}"),
        reply("goodbye, ${request.name}"),
    )
```

### Client streaming

```kotlin
override suspend fun lotsOfGreetings(
    context: RequestContext,
    requests: Flow<HelloRequest>,
): HelloReply {
    val names = requests.toList().joinToString(", ") { it.name }
    return reply("hello, $names")
}
```

### Bidirectional streaming

Transforming the request `Flow` preserves coroutine cancellation and backpressure:

```kotlin
override suspend fun bidiHello(
    context: RequestContext,
    requests: Flow<HelloRequest>,
): Flow<HelloReply> = requests.map { request -> reply("stream hello, ${request.name}") }
```

The streaming method snippets belong inside `GreeterServiceImpl`. Register the complete service on
the core server:

```kotlin
val server = Server()
registerGreeter(server, GreeterServiceImpl())
```

See
[`examples/src/main/kotlin/zip/trev/trevrpc/examples/GreeterExample.kt`](examples/src/main/kotlin/zip/trev/trevrpc/examples/GreeterExample.kt)
for the complete service and client exercise. The examples module verifies that generated bindings
remain current and that all four RPC shapes work without installing a cross-runtime executable.

## Benchmark peer

`bench-peer` implements benchmark protocol V4. Its client role supports `trevrpc_native_quic`; its
server role additionally supports `trevrpc_webtransport`. A WebTransport server
requires `--webtransport-origin ORIGIN` and admits only secure WebTransport sessions whose path is
exactly `/trevrpc` and whose `Origin` header exactly matches that value. WebTransport client mode is
not supported.

## Local Maven staging

Milestone 4 packages exactly four local coordinates at version `0.1.5`:

- `zip.trev.trevrpc:core`
- `zip.trev.trevrpc:transport-netty`
- `zip.trev.trevrpc:transport-cronet`
- `zip.trev.trevrpc:protoc-gen-trevrpc-kotlin`

Run `./gradlew stageMavenRepository` to write them only to
`build/staging-repository`. `publishToMavenLocal` is disabled, and the build defines no remote
publishing repository. Every coordinate includes sources and Dokka documentation JARs. The thin
generator JAR declares `zip.trev.trevrpc.generator.MainKt`; the generator also publishes a
self-contained `jdk21` classifier for `java -jar` use.

The Netty transport requires JDK 21 and declares native QUIC JARs for Linux x86-64/AArch64, macOS
x86-64/AArch64, and Windows x86-64. All five are present on the runtime graph; Netty selects the
host implementation. Other hosts are unsupported.

The Cronet transport is a JVM 17 JAR that bundles the Cronet API classes required by its public
surface, together with their Chromium license. It does not select a provider. An application must
choose a compatible Cronet provider, construct and own the `CronetEngine`, and own its callback
executor. TrevRPC neither publishes nor selects `cronet-embedded`, and this milestone does not
claim Android device or instrumentation validation.

The normal Nix package installs the staged repository in `share/maven`, retains the first-party
runtime JARs in `share/java`, and installs `protoc-gen-trevrpc-kotlin` in `bin`. The benchmark peer
remains a separate package output.
