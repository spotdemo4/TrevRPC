package trevrpc

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// describedInspect returns the descriptors of the example.Inspect service of
// registerInspectService, as protoc-gen-go would embed them, and the option
// "public" that an application defines on its methods, which is set on Unary:
//
//	extend google.protobuf.MethodOptions { bool public = 50001; }
func describedInspect(t *testing.T) (protoreflect.MethodDescriptors, protoreflect.ExtensionType) {
	t.Helper()
	files := new(protoregistry.Files)
	if err := files.RegisterFile(descriptorpb.File_google_protobuf_descriptor_proto); err != nil {
		t.Fatal(err)
	}
	optionsFile, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("inspect_options.proto"), Package: new("example"), Syntax: new("proto3"),
		Dependency: []string{"google/protobuf/descriptor.proto"},
		Extension: []*descriptorpb.FieldDescriptorProto{{
			Name: new("public"), Number: new(int32(50001)), Extendee: new(".google.protobuf.MethodOptions"),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
			JsonName: new("public"),
		}},
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.RegisterFile(optionsFile); err != nil {
		t.Fatal(err)
	}
	public := dynamicpb.NewExtensionType(optionsFile.Extensions().ByName("public"))
	publicOptions := &descriptorpb.MethodOptions{}
	proto.SetExtension(publicOptions, public, true)

	method := func(name string, clientStreaming, serverStreaming bool, options *descriptorpb.MethodOptions) *descriptorpb.MethodDescriptorProto {
		return &descriptorpb.MethodDescriptorProto{
			Name: new(name), InputType: new(".example.Message"), OutputType: new(".example.Message"),
			ClientStreaming: new(clientStreaming), ServerStreaming: new(serverStreaming), Options: options,
		}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("inspect.proto"), Package: new("example"), Syntax: new("proto3"),
		Dependency:  []string{"inspect_options.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: new("Message")}},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: new("Inspect"), Method: []*descriptorpb.MethodDescriptorProto{
			method("Unary", false, false, publicOptions),
			method("ServerStreaming", false, true, nil),
			method("ClientStreaming", true, false, nil),
			method("Bidirectional", true, true, nil),
		}}},
	}, files)
	if err != nil {
		t.Fatal(err)
	}
	return file.Services().ByName("Inspect").Methods(), public
}

// registerDescribedInspectService is registerInspectService with the
// descriptors of methods, the way generated registration passes them.
func registerDescribedInspectService(server *Server, methods protoreflect.MethodDescriptors) {
	reply := func(ctx context.Context) *testMessage {
		info, _ := HandlerContextFromContext(ctx)
		return &testMessage{Value: describe(info)}
	}
	RegisterUnary(server, "/example.Inspect/Unary", newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
		return reply(ctx), nil
	}, RouteMethodDescriptor(methods.ByName("Unary")))
	RegisterServerStreaming(server, "/example.Inspect/ServerStreaming", newTestMessage, func(ctx context.Context, _ *testMessage) (MessageStream[*testMessage], error) {
		return StreamOf(reply(ctx)), nil
	}, RouteMethodDescriptor(methods.ByName("ServerStreaming")))
	RegisterClientStreaming(server, "/example.Inspect/ClientStreaming", newTestMessage, func(ctx context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
		for _, err := range Messages(requests) {
			if err != nil {
				return nil, err
			}
		}
		return reply(ctx), nil
	}, RouteMethodDescriptor(methods.ByName("ClientStreaming")))
	RegisterBidirectionalStreaming(server, "/example.Inspect/Bidirectional", newTestMessage, func(ctx context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
		if err := requests.Close(); err != nil {
			return nil, err
		}
		return StreamOf(reply(ctx)), nil
	}, RouteMethodDescriptor(methods.ByName("Bidirectional")))
}

// describe names the descriptor of info: its full name, or "none".
func describe(info HandlerContext) string {
	if info.MethodDescriptor == nil {
		return "none"
	}
	return string(info.MethodDescriptor.FullName())
}

// descriptorRecorder records what authorizers and interceptors saw by full
// method, safe for the goroutines of concurrent calls.
type descriptorRecorder struct {
	mu   sync.Mutex
	seen map[string]HandlerContext
}

func (r *descriptorRecorder) record(info HandlerContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string]HandlerContext{}
	}
	r.seen[info.FullMethod] = info
}

func (r *descriptorRecorder) get(t *testing.T, fullMethod string) HandlerContext {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.seen[fullMethod]
	if !ok {
		t.Fatalf("no call to %s was recorded", fullMethod)
	}
	return info
}

func TestMethodDescriptorReachesAuthorizersInterceptorsAndHandlers(t *testing.T) {
	methods, public := describedInspect(t)
	pki := newTestPKI(t)
	server := NewServer()
	var authorized, intercepted descriptorRecorder
	server.SetAuthorizer(AuthorizerFunc(func(ctx context.Context, info HandlerContext) (context.Context, error) {
		authorized.record(info)
		return ctx, nil
	}))
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, info HandlerContext) (context.Context, func(RPCFinished), error) {
		intercepted.record(info)
		return ctx, nil, nil
	}))
	registerDescribedInspectService(server, methods)
	// A route registered without a descriptor.
	routeBytes(server, "/example.Hand/Method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server})
	channel := dialNative(t, pki, listener)

	ctx, cancel := context.WithTimeout(t.Context(), rpcTestTimeout)
	defer cancel()
	answers := inspectAll(ctx, t, channel)
	for _, method := range []string{"Unary", "ServerStreaming", "ClientStreaming", "Bidirectional"} {
		want := "example.Inspect." + method
		if answers[method] != want {
			t.Errorf("%s handler saw descriptor %q, want %q", method, answers[method], want)
		}
		fullMethod := "/example.Inspect/" + method
		for who, recorder := range map[string]*descriptorRecorder{"authorizer": &authorized, "interceptor": &intercepted} {
			info := recorder.get(t, fullMethod)
			if info.MethodDescriptor == nil || info.MethodDescriptor.FullName() != protoreflect.FullName(want) {
				t.Errorf("%s saw descriptor %v for %s, want %s", who, info.MethodDescriptor, method, want)
				continue
			}
			// The application's own option reads back from the descriptor.
			isPublic, _ := proto.GetExtension(info.MethodDescriptor.Options(), public).(bool)
			if isPublic != (method == "Unary") {
				t.Errorf("%s read public = %v for %s", who, isPublic, method)
			}
		}
	}

	// Authorizers run before the route lookup, so they see an unknown method
	// too, without a descriptor, and the caller gets the usual status.
	_, err := Unary(ctx, channel, "/example.Inspect/Missing", &testMessage{}, newTestMessage)
	if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodeUnimplemented {
		t.Fatalf("unknown method returned %v, want Unimplemented", err)
	}
	if info := authorized.get(t, "/example.Inspect/Missing"); info.MethodDescriptor != nil {
		t.Errorf("authorizer saw descriptor %v for an unknown method, want nil", info.MethodDescriptor.FullName())
	}
	if _, err := Unary(ctx, channel, "/example.Hand/Method", &testMessage{}, newTestMessage); err != nil {
		t.Fatalf("hand-registered route: %v", err)
	}
	for who, recorder := range map[string]*descriptorRecorder{"authorizer": &authorized, "interceptor": &intercepted} {
		if info := recorder.get(t, "/example.Hand/Method"); info.MethodDescriptor != nil {
			t.Errorf("%s saw descriptor %v for a hand-registered route, want nil", who, info.MethodDescriptor.FullName())
		}
	}
}

func TestMethodDescriptorOfCallsServedWithoutATransport(t *testing.T) {
	methods, _ := describedInspect(t)
	server := NewServer()
	var seen descriptorRecorder
	server.SetAuthorizer(AuthorizerFunc(func(ctx context.Context, info HandlerContext) (context.Context, error) {
		seen.record(info)
		return ctx, nil
	}))
	registerDescribedInspectService(server, methods)

	response := server.handleRequest(context.Background(), newWireRequest("/example.Inspect/Unary"))
	if CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("response = %#v", response)
	}
	if info := seen.get(t, "/example.Inspect/Unary"); describe(info) != "example.Inspect.Unary" {
		t.Errorf("authorizer saw descriptor %s, want example.Inspect.Unary", describe(info))
	}
	if response = server.handleRequest(context.Background(), newWireRequest("/example.Inspect/Missing")); CodeFromUint32(response.Status) != CodeUnimplemented {
		t.Fatalf("unknown method response = %#v, want Unimplemented", response)
	}
	if info := seen.get(t, "/example.Inspect/Missing"); info.MethodDescriptor != nil {
		t.Errorf("authorizer saw descriptor %v for an unknown method, want nil", info.MethodDescriptor.FullName())
	}
}

func TestNewHandlerContextKeepsTheMethodDescriptor(t *testing.T) {
	methods, _ := describedInspect(t)
	ctx := NewHandlerContext(context.Background(), HandlerContext{FullMethod: "/example.Inspect/Unary", MethodDescriptor: methods.ByName("Unary")})
	info, ok := HandlerContextFromContext(ctx)
	if !ok || describe(info) != "example.Inspect.Unary" {
		t.Fatalf("HandlerContextFromContext = %s, %v; want example.Inspect.Unary", describe(info), ok)
	}
}

func TestRouteMethodDescriptorPanicsForAnotherMethod(t *testing.T) {
	methods, _ := describedInspect(t)
	register := func(path string, kind RPCKind, method protoreflect.MethodDescriptor) (recovered any) {
		defer func() { recovered = recover() }()
		server := NewServer()
		switch kind {
		case RPCKindUnary:
			RegisterUnary(server, path, newTestMessage, func(context.Context, *testMessage) (*testMessage, error) { return nil, nil }, RouteMethodDescriptor(method))
		default:
			RegisterServerStreaming(server, path, newTestMessage, func(context.Context, *testMessage) (MessageStream[*testMessage], error) { return nil, nil }, RouteMethodDescriptor(method))
		}
		return nil
	}
	for _, test := range []struct {
		name   string
		path   string
		kind   RPCKind
		method protoreflect.MethodDescriptor
		panics bool
	}{
		{name: "match", path: "/example.Inspect/Unary", kind: RPCKindUnary, method: methods.ByName("Unary")},
		{name: "nil descriptor", path: "/example.Inspect/Unary", kind: RPCKindUnary},
		{name: "another name", path: "/example.Inspect/Other", kind: RPCKindUnary, method: methods.ByName("Unary"), panics: true},
		{name: "another service", path: "/example.Other/Unary", kind: RPCKindUnary, method: methods.ByName("Unary"), panics: true},
		{name: "another kind", path: "/example.Inspect/Unary", kind: RPCKindServerStreaming, method: methods.ByName("Unary"), panics: true},
		{name: "streaming match", path: "/example.Inspect/ServerStreaming", kind: RPCKindServerStreaming, method: methods.ByName("ServerStreaming")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if recovered := register(test.path, test.kind, test.method); (recovered != nil) != test.panics {
				t.Fatalf("registering panicked with %v, want a panic: %v", recovered, test.panics)
			}
		})
	}
}
