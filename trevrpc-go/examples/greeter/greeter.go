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
	SayHello(context.Context, *HelloRequest) (*HelloReply, error)
	LotsOfReplies(context.Context, *HelloRequest) (trevrpc.MessageStream[*HelloReply], error)
	LotsOfGreetings(context.Context, trevrpc.MessageStream[*HelloRequest]) (*HelloReply, error)
	BidiHello(context.Context, trevrpc.MessageStream[*HelloRequest]) (trevrpc.MessageStream[*HelloReply], error)
}

type GreeterClient struct {
	transport trevrpc.Transport
	options   []trevrpc.CallOption
}

func NewGreeterClient(transport trevrpc.Transport, options ...trevrpc.CallOption) *GreeterClient {
	return &GreeterClient{transport: transport, options: options}
}

func (c *GreeterClient) SayHello(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (*HelloReply, error) {
	return trevrpc.Unary[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodSayHello, request, func() *HelloReply { return &HelloReply{} }, mergeOptions(c.options, options)...)
}

func (c *GreeterClient) LotsOfReplies(ctx context.Context, request *HelloRequest, options ...trevrpc.CallOption) (trevrpc.MessageStream[*HelloReply], error) {
	return trevrpc.ServerStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfReplies, request, func() *HelloReply { return &HelloReply{} }, mergeOptions(c.options, options)...)
}

func (c *GreeterClient) LotsOfGreetings(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest], options ...trevrpc.CallOption) (*HelloReply, error) {
	return trevrpc.ClientStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodLotsOfGreetings, requests, func() *HelloReply { return &HelloReply{} }, mergeOptions(c.options, options)...)
}

func (c *GreeterClient) BidiHello(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest], options ...trevrpc.CallOption) (trevrpc.MessageStream[*HelloReply], error) {
	return trevrpc.BidirectionalStreaming[*HelloRequest, *HelloReply](ctx, c.transport, ServiceName, MethodBidiHello, requests, func() *HelloReply { return &HelloReply{} }, mergeOptions(c.options, options)...)
}

func RegisterGreeterServer(server *trevrpc.Server, implementation GreeterServer) {
	server.Route(ServiceName, MethodSayHello, func(ctx context.Context, body []byte) ([]byte, error) {
		request := &HelloRequest{}
		if err := trevrpc.UnmarshalMessage(body, request); err != nil {
			return nil, trevrpc.InvalidArgument("failed to decode request: " + err.Error())
		}

		response, err := implementation.SayHello(ctx, request)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, trevrpc.Internal("handler returned nil response")
		}

		return trevrpc.MarshalMessage(response)
	})

	server.RouteStreaming(ServiceName, MethodLotsOfReplies, trevrpc.RpcKindServerStreaming, func(ctx context.Context, body []byte, _ trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		request := &HelloRequest{}
		if err := trevrpc.UnmarshalMessage(body, request); err != nil {
			return nil, trevrpc.InvalidArgument("failed to decode request: " + err.Error())
		}

		responses, err := implementation.LotsOfReplies(ctx, request)
		if err != nil {
			return nil, err
		}

		return trevrpc.EncodeStream[*HelloReply](responses), nil
	})

	server.RouteStreaming(ServiceName, MethodLotsOfGreetings, trevrpc.RpcKindClientStreaming, func(ctx context.Context, _ []byte, requests trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		requestStream := trevrpc.DecodeStream[*HelloRequest](requests, func() *HelloRequest { return &HelloRequest{} })
		response, err := implementation.LotsOfGreetings(ctx, requestStream)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, trevrpc.Internal("handler returned nil response")
		}

		return trevrpc.SingleMessageStream(response), nil
	})

	server.RouteStreaming(ServiceName, MethodBidiHello, trevrpc.RpcKindBidirectionalStreaming, func(ctx context.Context, _ []byte, requests trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		requestStream := trevrpc.DecodeStream[*HelloRequest](requests, func() *HelloRequest { return &HelloRequest{} })
		responses, err := implementation.BidiHello(ctx, requestStream)
		if err != nil {
			return nil, err
		}

		return trevrpc.EncodeStream[*HelloReply](responses), nil
	})
}

func mergeOptions(base []trevrpc.CallOption, override []trevrpc.CallOption) []trevrpc.CallOption {
	merged := make([]trevrpc.CallOption, 0, len(base)+len(override))
	merged = append(merged, base...)
	merged = append(merged, override...)
	return merged
}
