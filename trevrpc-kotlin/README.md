# trevrpc-kotlin

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

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

Generate with `protoc-gen-trevrpc-kotlin`.

## Client

```kotlin
val channel = NettyRpcChannel.nativeQuic(
    NettyQuicClientConfig(InetSocketAddress("127.0.0.1", 50051), NettyClientTls("localhost"))
)
channel.awaitReady()
val client = GreeterClient(channel)

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

channel.close()
```

## Server

```kotlin
class Greeter : GreeterService {
    override suspend fun sayHello(ctx: RequestContext, req: HelloRequest) =
        HelloReply.newBuilder().setMessage("hello, ${req.name}").build()

    override suspend fun lotsOfReplies(ctx: RequestContext, req: HelloRequest): Flow<HelloReply> =
        flowOf(
            HelloReply.newBuilder().setMessage("hello, ${req.name}").build(),
            HelloReply.newBuilder().setMessage("goodbye, ${req.name}").build(),
        )

    override suspend fun lotsOfGreetings(ctx: RequestContext, reqs: Flow<HelloRequest>): HelloReply {
        val names = reqs.toList().joinToString(", ") { it.name }
        return HelloReply.newBuilder().setMessage("hello, $names").build()
    }

    override suspend fun bidiHello(ctx: RequestContext, reqs: Flow<HelloRequest>): Flow<HelloReply> =
        reqs.map { HelloReply.newBuilder().setMessage("hello, ${it.name}").build() }
}

val server = Server()
registerGreeter(server, Greeter())
val listener = NettyRpcServer.bind(server, NettyRpcServerConfig(
    InetSocketAddress("127.0.0.1", 50051),
    NettyServerTls.Pem(File("key.pem"), File("cert.pem")),
))
```
