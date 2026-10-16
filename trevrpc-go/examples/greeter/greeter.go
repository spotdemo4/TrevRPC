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
	LotsOfReplies(context.Context, *HelloRequest) (trevrpc.MessageStream[*HelloReply], error)
	LotsOfGreetings(context.Context, trevrpc.MessageStream[*HelloRequest]) (*trevrpc.Response[*HelloReply], error)
	BidiHello(context.Context, trevrpc.MessageStream[*HelloRequest]) (trevrpc.MessageStream[*HelloReply], error)
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

func (c *GreeterClient) LotsOfReplies(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (trevrpc.ResponseStream[*HelloReply], error) {
	return trevrpc.ServerStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfReplies, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) LotsOfGreetings(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.ClientStreamingCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.ClientStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfGreetings, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func (c *GreeterClient) BidiHello(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.BidirectionalStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodBidiHello, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

func RegisterGreeterServer(server *trevrpc.Server, implementation GreeterServer) {
	trevrpc.RegisterUnary[*HelloRequest, *HelloReply](server, ServiceName, MethodSayHello, func() *HelloRequest { return &HelloRequest{} }, implementation.SayHello)
	trevrpc.RegisterServerStreaming[*HelloRequest, *HelloReply](server, ServiceName, MethodLotsOfReplies, func() *HelloRequest { return &HelloRequest{} }, implementation.LotsOfReplies)
	trevrpc.RegisterClientStreaming[*HelloRequest, *HelloReply](server, ServiceName, MethodLotsOfGreetings, func() *HelloRequest { return &HelloRequest{} }, implementation.LotsOfGreetings)
	trevrpc.RegisterBidirectionalStreaming[*HelloRequest, *HelloReply](server, ServiceName, MethodBidiHello, func() *HelloRequest { return &HelloRequest{} }, implementation.BidiHello)
}
