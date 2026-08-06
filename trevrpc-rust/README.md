# trevrpc-rust

The examples use `prost` messages and the generated-style Greeter bindings in
[`examples/shared/greeter.rs`](examples/shared/greeter.rs). Its service defines unary,
server-streaming, client-streaming, and bidirectional-streaming methods.

## Client

Create a generated client with an application channel:

```rust
let endpoint = make_client_endpoint()?;
let channel = trevrpc::client::Channel::connect(
    endpoint.clone(),
    "127.0.0.1:5000".parse()?,
    "localhost",
)
.await?;
let client = greeter::GreeterClient::new(channel.clone());
let options = trevrpc::client::CallOptions::new();
```

The Quinn endpoint must trust the server certificate and include `trevrpc::ALPN` in its ALPN
protocols. The [complete client example](examples/greeter_client.rs) includes local TLS setup.

`Channel` owns and reuses the supplied Quinn endpoint across connection generations so rustls
TLS session resumption state survives redial. Reconnection only serves future calls; it does not
retry or replay any RPC. In-flight calls fail with their normal transport error when their
generation dies, and new calls fail immediately with `Unavailable` while reconnecting.
`wait_until_ready`, `state`, `subscribe_state`, and `subscribe_events` expose readiness and lifecycle
changes. `close` permanently stops that channel's reconnect loop.

The channel never enables Quinn 0-RTT. TLS session resumption may shorten a handshake, but RPC
application data is sent only after the full handshake completes. The initial `connect` future
covers that complete handshake, so callers can bound or cancel it with their normal future, task,
or request-context deadline semantics.

The same application type manages WebTransport sessions at the conventional `/trevrpc` path:

```rust
let channel = trevrpc::client::Channel::connect_webtransport(
    webtransport_client,
    "https://example.com:443",
)
.await?;
let client = greeter::GreeterClient::new(channel.clone());
```

Custom WebTransport CONNECT paths and headers are intentionally available only through
`trevrpc::advanced::connect_webtransport_channel_with_request`.

Keep the separate channel and endpoint handles for orderly shutdown:

```rust
channel.close();
endpoint.wait_idle().await;
```

Calling `channel.close()` stops reconnecting and closes its current connection. It deliberately
does not close the shared Quinn endpoint. If the application owns the endpoint exclusively, drop it
after `wait_idle`; if the endpoint is shared, keep it alive for its other connections.

### Advanced: low-level established connection

Use `trevrpc::advanced::RawQuinnTransport` when the application already manages an established
Quinn connection and its lifecycle directly. This transport does not reconnect:

```rust
let connection = endpoint
    .connect("127.0.0.1:5000".parse()?, "localhost")?
    .await?;
let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
let client = greeter::GreeterClient::new(transport);

// After all calls and streams have finished:
connection.close(0_u32.into(), b"client done");
endpoint.wait_idle().await;
```

### Unary

```rust
let reply = client
    .say_hello_with_options(
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
    .lots_of_replies_with_options(
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
let greetings = client
    .lots_of_greetings_with_options(trevrpc::client::CallOptions::new())
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

Split the call when request and response work must move to independent tasks. `finish` gracefully
half-closes the request side; dropping an unfinished sender cancels it. Drain `ResponseStream` to
clean EOF before reading successful terminal metadata:

```rust
let call = client
    .bidi_hello_with_options(trevrpc::client::CallOptions::new())
    .await?;
let (sender, mut responses) = call.split();

let send_task = tokio::spawn(async move {
    for name in ["Alice", "Bob"] {
        sender
            .send(greeter::HelloRequest {
                name: name.to_owned(),
            })
            .await?;
    }
    sender.finish()
});

let receive_task = tokio::spawn(async move {
    while let Some(reply) = responses.next().await {
        println!("{}", reply?.message);
    }
    Ok::<_, trevrpc::Error>(responses.terminal_metadata().cloned())
});

send_task.await??;
let terminal_metadata = receive_task.await??;
```

## Server

Implement the generated `Greeter` trait. Registration wires each method to the matching RPC shape.

### Unary

Generated service methods receive a cloneable `RequestContext` and return a
`ResponseEnvelope<T>`. The envelope carries successful response metadata:

```rust
struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(
        &self,
        context: trevrpc::server::RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, trevrpc::Status> {
        if context.cancelled() {
            return Err(trevrpc::Status::cancelled("request cancelled"));
        }
        Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        }))
    }

    // Implement the three streaming methods below in the same trait block.
}
```

### Server streaming

Return `ResponseEnvelope<BoxStream<T>>`. `BoxStream<T>` is a pinned standard
`futures_core::Stream<Item = trevrpc::Result<T>>`, so normal `StreamExt` combinators work:

```rust
async fn lots_of_replies(
    &self,
    _context: trevrpc::server::RequestContext,
    request: greeter::HelloRequest,
) -> Result<
    trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>,
    trevrpc::Status,
> {
    Ok(trevrpc::ResponseEnvelope::new(trevrpc::stream::from_iter([
        greeter::HelloReply {
            message: format!("hello, {}", request.name),
        },
        greeter::HelloReply {
            message: format!("goodbye, {}", request.name),
        },
    ])))
}
```

### Client streaming

```rust
async fn lots_of_greetings(
    &self,
    _context: trevrpc::server::RequestContext,
    mut requests: trevrpc::BoxStream<greeter::HelloRequest>,
) -> Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, trevrpc::Status> {
    let mut names = Vec::new();
    while let Some(request) = requests.next().await {
        names.push(request?.name);
    }

    Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
        message: format!("hello, {}", names.join(", ")),
    }))
}
```

### Bidirectional streaming

A mapped standard stream preserves request-side backpressure without a custom stream trait:

```rust
async fn bidi_hello(
    &self,
    _context: trevrpc::server::RequestContext,
    requests: trevrpc::BoxStream<greeter::HelloRequest>,
) -> Result<
    trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>,
    trevrpc::Status,
> {
    let replies: trevrpc::BoxStream<greeter::HelloReply> = Box::pin(
        requests.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("stream hello, {}", request.name),
            })
        }),
    );
    Ok(trevrpc::ResponseEnvelope::new(replies))
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
