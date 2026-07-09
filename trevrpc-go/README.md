# trevrpc-go

Minimal unary Greeter flow using generated Go bindings.

## Client

Create a transport with `Dial`, create the generated client, and send a request:

```go
ctx := context.Background()
transport, err := trevrpc.Dial(ctx, "127.0.0.1:50051", trevrpc.DialOptions{
	TLSConfig: tlsConfig,
})
if err != nil {
	return err
}
defer transport.Close()

client := greeter.NewGreeterClient(transport)
reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "TrevRPC"})
if err != nil {
	return err
}
fmt.Println(reply.Message)
```

`tlsConfig` must trust the server certificate and use the TrevRPC ALPN. The examples include local certificate setup.

## Server

Create a server, register the generated service, and receive the message in the method implementation:

```go
type greeterService struct{}

func (greeterService) SayHello(_ context.Context, req *greeter.HelloRequest) (*greeter.HelloReply, error) {
	return &greeter.HelloReply{Message: "Hello, " + req.Name}, nil
}

server := trevrpc.NewServer()
greeter.RegisterGreeterServer(server, greeterService{})

listener, err := trevrpc.Listen("127.0.0.1:50051", server, trevrpc.ListenOptions{
	TLSConfig: tlsConfig,
})
if err != nil {
	return err
}
defer listener.Close()

return listener.Serve(context.Background())
```
