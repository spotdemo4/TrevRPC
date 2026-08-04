package greeter

import (
	"context"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	ServiceName           = "example.greeter.Greeter"
	MethodSayHello        = "SayHello"
	MethodLotsOfReplies   = "LotsOfReplies"
	MethodLotsOfGreetings = "LotsOfGreetings"
	MethodBidiHello       = "BidiHello"
)

type HelloRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *HelloRequest) Reset()         { *m = HelloRequest{} }
func (m *HelloRequest) String() string { return m.Name }
func (*HelloRequest) ProtoMessage()    {}

type HelloReply struct {
	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

func (m *HelloReply) Reset()         { *m = HelloReply{} }
func (m *HelloReply) String() string { return m.Message }
func (*HelloReply) ProtoMessage()    {}

type GreeterServer interface {
	SayHello(context.Context, *HelloRequest) (*trevrpc.Response[*HelloReply], error)
	LotsOfReplies(context.Context, *HelloRequest) (trevrpc.ResponseStream[*HelloReply], error)
	LotsOfGreetings(context.Context, trevrpc.MessageStream[*HelloRequest]) (*trevrpc.Response[*HelloReply], error)
	BidiHello(context.Context, trevrpc.MessageStream[*HelloRequest]) (trevrpc.ResponseStream[*HelloReply], error)
}

type GreeterClient struct {
	transport trevrpc.Transport
	options   []trevrpc.CallOption
}

func NewGreeterClient(transport trevrpc.Transport, options ...trevrpc.CallOption) *GreeterClient {
	return &GreeterClient{transport: transport, options: options}
}

func (c *GreeterClient) mergedCallOptions(overrides []trevrpc.CallOption) []trevrpc.CallOption {
	if len(c.options) == 0 {
		return overrides
	}
	if len(overrides) == 0 {
		return c.options
	}
	merged := make([]trevrpc.CallOption, 0, len(c.options)+len(overrides))
	merged = append(merged, c.options...)
	merged = append(merged, overrides...)
	return merged
}

func (c *GreeterClient) SayHello(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (*HelloReply, error) {
	return trevrpc.Unary[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodSayHello, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) SayHelloResponse(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (*trevrpc.Response[*HelloReply], error) {
	return trevrpc.UnaryResponse[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodSayHello, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfReplies(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (trevrpc.MessageStream[*HelloReply], error) {
	return trevrpc.ServerStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfReplies, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfRepliesResponse(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (trevrpc.ResponseStream[*HelloReply], error) {
	return trevrpc.ServerStreamingResponse[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfReplies, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfGreetings(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.ClientStreamingCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.ClientStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfGreetings, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfGreetingsResponse(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.ClientStreamingResponseCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.ClientStreamingResponse[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfGreetings, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfGreetingsFromStream(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest], options ...trevrpc.CallOption) (*HelloReply, error) {
	return trevrpc.ClientStreamingFromStream[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfGreetings, requests, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfGreetingsFromStreamResponse(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest], options ...trevrpc.CallOption) (*trevrpc.Response[*HelloReply], error) {
	return trevrpc.ClientStreamingFromStreamResponse[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfGreetings, requests, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) BidiHello(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.BidirectionalStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodBidiHello, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) BidiHelloResponse(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingResponseCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.BidirectionalStreamingResponse[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodBidiHello, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) BidiHelloFromStream(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest], options ...trevrpc.CallOption) (trevrpc.MessageStream[*HelloReply], error) {
	return trevrpc.BidirectionalStreamingFromStream[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodBidiHello, requests, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) BidiHelloFromStreamResponse(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest], options ...trevrpc.CallOption) (trevrpc.ResponseStream[*HelloReply], error) {
	return trevrpc.BidirectionalStreamingFromStreamResponse[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodBidiHello, requests, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func RegisterGreeterServer(server *trevrpc.Server, implementation GreeterServer) {
	trevrpc.RegisterUnaryResponse[*HelloRequest, *HelloReply](server, ServiceName, MethodSayHello, func() *HelloRequest { return &HelloRequest{} }, implementation.SayHello)
	trevrpc.RegisterServerStreamingResponse[*HelloRequest, *HelloReply](server, ServiceName, MethodLotsOfReplies, func() *HelloRequest { return &HelloRequest{} }, implementation.LotsOfReplies)
	trevrpc.RegisterClientStreamingResponse[*HelloRequest, *HelloReply](server, ServiceName, MethodLotsOfGreetings, func() *HelloRequest { return &HelloRequest{} }, implementation.LotsOfGreetings)
	trevrpc.RegisterBidirectionalStreamingResponse[*HelloRequest, *HelloReply](server, ServiceName, MethodBidiHello, func() *HelloRequest { return &HelloRequest{} }, implementation.BidiHello)
}
