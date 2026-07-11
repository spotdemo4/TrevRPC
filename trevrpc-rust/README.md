# trevrpc-rust

The examples use `prost` messages and the generated-style Greeter bindings in
[`examples/shared/greeter.rs`](examples/shared/greeter.rs). Its service defines unary,
server-streaming, client-streaming, and bidirectional-streaming methods.

## Client

Create a generated client from a connected Quinn transport:

```rust
let connection = endpoint
    .connect("127.0.0.1:5000".parse()?, "localhost")?
    .await?;
let transport = trevrpc::quinn::Client::new(connection);
let client = greeter::GreeterClient::new(transport);
let options = trevrpc::client::CallOptions::new();
```

The Quinn endpoint must trust the server certificate and include `trevrpc::ALPN` in its ALPN
protocols. The [complete client example](examples/greeter_client.rs) includes local TLS setup.

### Unary

```rust
let reply = client
    .say_hello(
        greeter::HelloRequest {
            name: "TrevRPC".to_owned(),
        },
        options,
    )
    .await?;
println!("{}", reply.message);
```

### Server streaming

Call `next` until the response stream ends. Each item carries any decode or terminal RPC error:

```rust
let mut replies = client
    .lots_of_replies(
        greeter::HelloRequest {
            name: "TrevRPC".to_owned(),
        },
        trevrpc::client::CallOptions::new(),
    )
    .await?;

while let Some(reply) = replies.next().await {
    println!("{}", reply?.message);
}
```

### Client streaming

```rust
let mut greetings = client
    .lots_of_greetings(trevrpc::client::CallOptions::new())
    .await?;

for name in ["Alice", "Bob"] {
    greetings
        .send(greeter::HelloRequest {
            name: name.to_owned(),
        })
        .await?;
}

let reply = greetings.close_and_recv().await?;
println!("{}", reply.message);
```

### Bidirectional streaming

Requests and responses may be interleaved. `close_send` half-closes the request side; receive until
`recv` returns `None` to consume the terminal status:

```rust
let mut stream = client
    .bidi_hello(trevrpc::client::CallOptions::new())
    .await?;

for name in ["Alice", "Bob"] {
    stream
        .send(greeter::HelloRequest {
            name: name.to_owned(),
        })
        .await?;
    let reply = stream.recv().await?.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "BidiHello ended early")
    })?;
    println!("{}", reply.message);
}

stream.close_send()?;
while let Some(reply) = stream.recv().await? {
    println!("{}", reply.message);
}
```

Use independent sender and receiver tasks when a protocol must continuously read and write large
bidi streams.

## Server

Implement the generated `Greeter` trait. Registration wires each method to the matching RPC shape.

### Unary

```rust
struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(
        &self,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        })
    }

    // Implement the three streaming methods below in the same trait block.
}
```

### Server streaming

Return a boxed `MessageStream`. `trevrpc::stream::from_iter` is convenient for a fixed sequence:

```rust
async fn lots_of_replies(
    &self,
    request: greeter::HelloRequest,
) -> Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
    Ok(trevrpc::stream::from_iter([
        greeter::HelloReply {
            message: format!("hello, {}", request.name),
        },
        greeter::HelloReply {
            message: format!("goodbye, {}", request.name),
        },
    ]))
}
```

### Client streaming

```rust
async fn lots_of_greetings(
    &self,
    mut requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
) -> Result<greeter::HelloReply, trevrpc::Status> {
    let mut names = Vec::new();
    while let Some(request) = requests.next().await {
        names.push(request?.name);
    }

    Ok(greeter::HelloReply {
        message: format!("hello, {}", names.join(", ")),
    })
}
```

### Bidirectional streaming

A response stream can pull from the request stream to preserve backpressure:

```rust
struct EchoReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<greeter::HelloReply> for EchoReplies {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        self.requests.next().await.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("stream hello, {}", request.name),
            })
        })
    }
}

async fn bidi_hello(
    &self,
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
) -> Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
    Ok(Box::new(EchoReplies { requests }))
}
```

The three streaming method snippets belong inside the `impl greeter::Greeter for GreeterService`
block. Register the complete implementation before serving:

```rust
let mut server = trevrpc::server::Server::new();
greeter::register_greeter(&mut server, GreeterService);
server.serve_quinn(endpoint).await?;
```

See the [complete server example](examples/greeter_server.rs) for TLS, authorization, HTTP/3, and
WebTransport setup.
