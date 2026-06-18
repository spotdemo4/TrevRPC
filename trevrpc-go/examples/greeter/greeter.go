package greeter

import (
	"context"

	"github.com/golang/protobuf/proto"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	ServiceName         = "example.greeter.Greeter"
	MethodSayHello      = "SayHello"
	MethodLotsOfReplies = "LotsOfReplies"
)

type HelloRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *HelloRequest) Reset()         { *m = HelloRequest{} }
func (m *HelloRequest) String() string { return proto.CompactTextString(m) }
func (*HelloRequest) ProtoMessage()    {}

type HelloReply struct {
	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

func (m *HelloReply) Reset()         { *m = HelloReply{} }
func (m *HelloReply) String() string { return proto.CompactTextString(m) }
func (*HelloReply) ProtoMessage()    {}

type GreeterServer interface {
	SayHello(context.Context, *HelloRequest) (*HelloReply, error)
	LotsOfReplies(context.Context, *HelloRequest) (trevrpc.MessageStream[*HelloReply], error)
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

func RegisterGreeterServer(server *trevrpc.Server, implementation GreeterServer) {
	server.Route(ServiceName, MethodSayHello, func(ctx context.Context, body []byte) ([]byte, error) {
		request := &HelloRequest{}
		if err := proto.Unmarshal(body, request); err != nil {
			return nil, trevrpc.InvalidArgument("failed to decode request: " + err.Error())
		}

		response, err := implementation.SayHello(ctx, request)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, trevrpc.Internal("handler returned nil response")
		}

		return proto.Marshal(response)
	})

	server.RouteStreaming(ServiceName, MethodLotsOfReplies, trevrpc.RpcKindServerStreaming, func(ctx context.Context, body []byte, _ trevrpc.ByteStream) (trevrpc.ByteStream, error) {
		request := &HelloRequest{}
		if err := proto.Unmarshal(body, request); err != nil {
			return nil, trevrpc.InvalidArgument("failed to decode request: " + err.Error())
		}

		responses, err := implementation.LotsOfReplies(ctx, request)
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
