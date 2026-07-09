# trevrpc-rust

Minimal unary Greeter flow using `prost` messages and the TrevRPC Rust runtime.

## Client

Create a Quinn connection, wrap it in a TrevRPC client transport, and send a request:

```rust
let connection = endpoint.connect("127.0.0.1:5000".parse()?, "localhost")?.await?;
let transport = trevrpc::quinn::Client::new(connection);

let reply: HelloReply = trevrpc::client::unary(
    &transport,
    "example.greeter.Greeter",
    "SayHello",
    &HelloRequest {
        name: "TrevRPC".to_owned(),
    },
    trevrpc::client::CallOptions::new(),
)
.await?;

println!("{}", reply.message);
```

The Quinn endpoint must be configured with the server certificate and `trevrpc::ALPN`. The examples include full local TLS setup.

## Server

Create a server, register a unary route, decode the received bytes, and return encoded response bytes:

```rust
use prost::Message;

#[derive(Clone, PartialEq, prost::Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: String,
}

let mut server = trevrpc::server::Server::new();
server.route("example.greeter.Greeter", "SayHello", |body| async move {
    let request = HelloRequest::decode(body.as_slice())?;
    let reply = HelloReply {
        message: format!("Hello, {}", request.name),
    };
    Ok(reply.encode_to_vec())
});

server.serve_quinn(endpoint).await?;
```
