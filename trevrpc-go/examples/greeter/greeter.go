// Package greeter hand-writes the types protoc-gen-go and protoc-gen-trevrpc-go
// would produce for the example Greeter service, so the examples build without
// running protoc.
package greeter

import (
	"context"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// Full method paths ("/package.Service/Method") of the Greeter service.
//
//nolint:staticcheck // mirrors the {Service}_{Method}_FullMethodName names protoc-gen-trevrpc-go generates
const (
	Greeter_SayHello_FullMethodName        = "/example.greeter.Greeter/SayHello"
	Greeter_LotsOfReplies_FullMethodName   = "/example.greeter.Greeter/LotsOfReplies"
	Greeter_LotsOfGreetings_FullMethodName = "/example.greeter.Greeter/LotsOfGreetings"
	Greeter_BidiHello_FullMethodName       = "/example.greeter.Greeter/BidiHello"
)

// HelloRequest is the request message of the Greeter methods.
type HelloRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

// Reset clears the message.
func (m *HelloRequest) Reset() { *m = HelloRequest{} }

// String returns the name.
func (m *HelloRequest) String() string { return m.Name }

// ProtoMessage marks the type as a protobuf message.
func (*HelloRequest) ProtoMessage() {}

// HelloReply is the reply message of the Greeter methods.
type HelloReply struct {
	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

// Reset clears the message.
func (m *HelloReply) Reset() { *m = HelloReply{} }

// String returns the message text.
func (m *HelloReply) String() string { return m.Message }

// ProtoMessage marks the type as a protobuf message.
func (*HelloReply) ProtoMessage() {}

// GreeterServer handles the Greeter service.
//
//nolint:revive // mirrors the {Service}Server name protoc-gen-trevrpc-go generates
type GreeterServer interface {
	SayHello(context.Context, *HelloRequest) (*trevrpc.Response[*HelloReply], error)
	LotsOfReplies(context.Context, *HelloRequest) (trevrpc.MessageStream[*HelloReply], error)
	LotsOfGreetings(context.Context, trevrpc.MessageStream[*HelloRequest]) (*trevrpc.Response[*HelloReply], error)
	BidiHello(context.Context, trevrpc.MessageStream[*HelloRequest]) (trevrpc.MessageStream[*HelloReply], error)
}

// GreeterClient calls the Greeter service.
//
//nolint:revive // mirrors the {Service}Client name protoc-gen-trevrpc-go generates
type GreeterClient struct {
	transport trevrpc.Transport
	options   []trevrpc.CallOption
}

// NewGreeterClient creates a client for the Greeter service.
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

// SayHello calls the SayHello method.
func (c *GreeterClient) SayHello(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (*HelloReply, error) {
	return trevrpc.Unary[*HelloRequest, *HelloReply](ctx, c.transport, Greeter_SayHello_FullMethodName, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

// LotsOfReplies calls the LotsOfReplies method.
func (c *GreeterClient) LotsOfReplies(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (trevrpc.ServerStreamingCall[*HelloReply], error) {
	return trevrpc.ServerStreaming[*HelloRequest, *HelloReply](ctx, c.transport, Greeter_LotsOfReplies_FullMethodName, request, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

// LotsOfGreetings calls the LotsOfGreetings method.
func (c *GreeterClient) LotsOfGreetings(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.ClientStreamingCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.ClientStreaming[*HelloRequest, *HelloReply](ctx, c.transport, Greeter_LotsOfGreetings_FullMethodName, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

// BidiHello calls the BidiHello method.
func (c *GreeterClient) BidiHello(ctx context.Context, options ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*HelloRequest, *HelloReply], error) {
	return trevrpc.BidirectionalStreaming[*HelloRequest, *HelloReply](ctx, c.transport, Greeter_BidiHello_FullMethodName, func() *HelloReply { return &HelloReply{} }, c.mergedCallOptions(options)...)
}

// RegisterGreeterServer registers implementation as the Greeter service on server.
func RegisterGreeterServer(server *trevrpc.Server, implementation GreeterServer) {
	trevrpc.RegisterUnary[*HelloRequest, *HelloReply](server, Greeter_SayHello_FullMethodName, func() *HelloRequest { return &HelloRequest{} }, implementation.SayHello)
	trevrpc.RegisterServerStreaming[*HelloRequest, *HelloReply](server, Greeter_LotsOfReplies_FullMethodName, func() *HelloRequest { return &HelloRequest{} }, implementation.LotsOfReplies)
	trevrpc.RegisterClientStreaming[*HelloRequest, *HelloReply](server, Greeter_LotsOfGreetings_FullMethodName, func() *HelloRequest { return &HelloRequest{} }, implementation.LotsOfGreetings)
	trevrpc.RegisterBidirectionalStreaming[*HelloRequest, *HelloReply](server, Greeter_BidiHello_FullMethodName, func() *HelloRequest { return &HelloRequest{} }, implementation.BidiHello)
}
