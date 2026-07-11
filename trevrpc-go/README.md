# trevrpc-go

The examples use the generated-style Greeter bindings in
[`examples/greeter`](examples/greeter). Its service defines unary, server-streaming,
client-streaming, and bidirectional-streaming methods.

## Client

Create a generated client with a managed QUIC transport:

```go
transport, err := trevrpc.DialManaged(
	ctx,
	"127.0.0.1:50051",
	trevrpc.ManagedDialOptions{
		DialOptions: trevrpc.DialOptions{TLSConfig: tlsConfig},
	},
)
if err != nil {
	return err
}
defer transport.Close()

client := greeter.NewGreeterClient(transport)
```

`DialManaged` returns a `ManagedQuicClient`. Reconnects serve future calls only; calls are never
retried. `tlsConfig` must trust the server certificate and include `trevrpc.ALPN` in `NextProtos`. The
[complete client example](examples/greeter_client/main.go) includes local certificate setup.

### Advanced: low-level QUIC connection

Use `quic.DialAddr` and `NewQuicClient` when the application needs to own the QUIC connection
lifecycle directly:

```go
conn, err := quic.DialAddr(
	ctx,
	"127.0.0.1:50051",
	tlsConfig,
	trevrpc.QUICClientConfig(trevrpc.DefaultMaxFrameSize, nil),
)
if err != nil {
	return err
}
defer conn.CloseWithError(0, "client done")

client := greeter.NewGreeterClient(trevrpc.NewQuicClient(conn))
```

### Unary

```go
reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
if err != nil {
	return err
}
fmt.Println(reply.Message)
```

### Server streaming

`trevrpc.Messages` adapts a `MessageStream` to Go's range-over-function syntax and reports any
terminal stream error:

```go
replies, err := client.LotsOfReplies(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
if err != nil {
	return err
}
for reply, err := range trevrpc.Messages(replies) {
	if err != nil {
		return err
	}
	fmt.Println(reply.Message)
}
```

### Client streaming

```go
greetings, err := client.LotsOfGreetings(ctx)
if err != nil {
	return err
}
for _, name := range []string{"Alice", "Bob"} {
	if err := greetings.Send(&greeter.HelloRequest{Name: name}); err != nil {
		return err
	}
}

reply, err := greetings.CloseAndRecv()
if err != nil {
	return err
}
fmt.Println(reply.Message)
```

### Bidirectional streaming

Requests and responses may be interleaved. `CloseSend` half-closes the request side; receive until
`io.EOF` to consume the terminal status:

```go
stream, err := client.BidiHello(ctx)
if err != nil {
	return err
}
defer stream.Close()

for _, name := range []string{"Alice", "Bob"} {
	if err := stream.Send(&greeter.HelloRequest{Name: name}); err != nil {
		return err
	}
	reply, err := stream.Recv()
	if err != nil {
		return err
	}
	fmt.Println(reply.Message)
}
if err := stream.CloseSend(); err != nil {
	return err
}
if reply, err := stream.Recv(); err != io.EOF {
	if err == nil {
		return fmt.Errorf("unexpected extra BidiHello reply: %s", reply.Message)
	}
	return err
}
```

Use independent send and receive goroutines when a protocol must continuously read and write large
bidi streams.

## Server

Implement the generated `GreeterServer` interface. Registration wires each method to the matching
RPC shape.

### Unary

```go
type greeterService struct{}

func (greeterService) SayHello(
	_ context.Context,
	request *greeter.HelloRequest,
) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "hello, " + request.Name}, nil
}
```

### Server streaming

Return any `trevrpc.MessageStream`; `FromSlice` is convenient for a fixed sequence:

```go
func (greeterService) LotsOfReplies(
	_ context.Context,
	request *greeter.HelloRequest,
) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return trevrpc.FromSlice(
		&greeter.HelloReply{Message: "hello, " + request.Name},
		&greeter.HelloReply{Message: "goodbye, " + request.Name},
	), nil
}
```

### Client streaming

```go
func (greeterService) LotsOfGreetings(
	_ context.Context,
	requests trevrpc.MessageStream[*greeter.HelloRequest],
) (*greeter.HelloReply, error) {
	var names []string
	for request, err := range trevrpc.Messages(requests) {
		if err != nil {
			return nil, err
		}
		names = append(names, request.Name)
	}
	return &greeter.HelloReply{Message: "hello, " + strings.Join(names, ", ")}, nil
}
```

### Bidirectional streaming

A pull-based response stream naturally applies backpressure while consuming the request stream:

```go
type echoReplies struct {
	requests trevrpc.MessageStream[*greeter.HelloRequest]
}

func (stream *echoReplies) Recv() (*greeter.HelloReply, error) {
	request, err := stream.requests.Recv()
	if err != nil {
		return nil, err
	}
	return &greeter.HelloReply{Message: "stream hello, " + request.Name}, nil
}

func (stream *echoReplies) Close() error {
	return stream.requests.Close()
}

func (greeterService) BidiHello(
	_ context.Context,
	requests trevrpc.MessageStream[*greeter.HelloRequest],
) (trevrpc.MessageStream[*greeter.HelloReply], error) {
	return &echoReplies{requests: requests}, nil
}
```

Register the implementation and serve it on a QUIC listener:

```go
server := trevrpc.NewServer()
greeter.RegisterGreeterServer(server, greeterService{})

listener, err := quic.ListenAddr(
	"127.0.0.1:50051",
	tlsConfig,
	trevrpc.QUICServerConfig(server.Options(), nil),
)
if err != nil {
	return err
}
defer listener.Close()

return trevrpc.ServeQUIC(context.Background(), listener, server)
```

The server TLS configuration must include `trevrpc.ALPN` in `NextProtos`. See the
[complete server example](examples/greeter_server/main.go) for TLS, authorization, HTTP/3, and
WebTransport setup.
