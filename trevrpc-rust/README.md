# trevrpc-rust

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

Generate with `protoc-gen-trevrpc-rust`.

## Client

```rust
let channel = trevrpc::client::Channel::connect(endpoint.clone(), "127.0.0.1:50051".parse()?, "localhost").await?;
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

```rust
struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(&self, _ctx: trevrpc::server::RequestContext, req: greeter::HelloRequest)
        -> Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply { message: format!("hello, {}", req.name) })
    }
    async fn lots_of_replies(&self, _ctx: trevrpc::server::RequestContext, req: greeter::HelloRequest)
        -> Result<trevrpc::BoxStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(trevrpc::stream::from_iter([
            greeter::HelloReply { message: format!("hello, {}", req.name) },
            greeter::HelloReply { message: format!("goodbye, {}", req.name) },
        ]))
    }
    async fn lots_of_greetings(&self, _ctx: trevrpc::server::RequestContext, mut reqs: trevrpc::BoxStream<greeter::HelloRequest>)
        -> Result<greeter::HelloReply, trevrpc::Status> {
        let mut names = Vec::new();
        while let Some(r) = reqs.next().await { names.push(r?.name); }
        Ok(greeter::HelloReply { message: names.join(", ") })
    }
    async fn bidi_hello(&self, _ctx: trevrpc::server::RequestContext, reqs: trevrpc::BoxStream<greeter::HelloRequest>)
        -> Result<trevrpc::BoxStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(Box::pin(reqs.map(|r| r.map(|r| greeter::HelloReply { message: format!("hello, {}", r.name) }))))
    }
}

let mut server = trevrpc::server::Server::new();
greeter::register_greeter(&mut server, GreeterService);
server.serve_quinn(endpoint).await?;
```
