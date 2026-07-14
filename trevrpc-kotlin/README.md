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
`RpcChannel` factory rather than assembling transport lifecycle components. Deterministic tests,
benchmarks, and cross-runtime harnesses can explicitly own one connection with
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
for the complete service and
[`examples/src/main/kotlin/zip/trev/trevrpc/examples/XRuntime.kt`](examples/src/main/kotlin/zip/trev/trevrpc/examples/XRuntime.kt)
for Netty TLS, native QUIC, HTTP/3, and WebTransport serving.
