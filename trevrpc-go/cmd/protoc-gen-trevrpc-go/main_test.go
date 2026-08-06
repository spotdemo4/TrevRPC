package main

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	descriptor "google.golang.org/protobuf/types/descriptorpb"
	plugin "google.golang.org/protobuf/types/pluginpb"
)

func TestGenerateAllRPCShapes(t *testing.T) {
	response := generate(greeterRequest(""))
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	if len(response.File) != 1 {
		t.Fatalf("expected one generated file, got %d", len(response.File))
	}

	generated := response.File[0]
	if generated.GetName() != "hello/v1/greeter.trevrpc.go" {
		t.Fatalf("unexpected output name: %s", generated.GetName())
	}
	content := generated.GetContent()
	for _, want := range []string{
		"type GreeterServer interface",
		"SayHello(context.Context, *HelloRequest)",
		"LotsOfReplies(context.Context, *HelloRequest)",
		"LotsOfGreetings(context.Context",
		"BidiHello(context.Context",
		"func NewGreeterClient",
		"func (c *GreeterClient) mergedCallOptions",
		"func (c *GreeterClient) LotsOfGreetingsResponse",
		"func (c *GreeterClient) LotsOfGreetingsFromStreamResponse",
		"func (c *GreeterClient) BidiHelloResponse",
		"func (c *GreeterClient) BidiHelloFromStreamResponse",
		"RegisterUnaryResponse[*HelloRequest, *HelloReply]",
		"RegisterClientStreamingResponse[*HelloRequest, *HelloReply]",
		"RegisterServerStreamingResponse[*HelloRequest, *HelloReply]",
		"RegisterBidirectionalStreamingResponse[*HelloRequest, *HelloReply]",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated content missing %q:\n%s", want, content)
		}
	}
	if strings.Contains(content, "func mergeTrevrpcCallOptions") {
		t.Fatalf("generated a package-level option helper:\n%s", content)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), generated.GetName(), content, parser.AllErrors); err != nil {
		t.Fatalf("generated Go is not syntactically valid: %v\n%s", err, content)
	}
}

func TestGeneratedAllRPCShapesCompileAndRoundTripMetadata(t *testing.T) {
	runtimeRoot, root, relativeRoot, importPrefix := newInModuleFixture(t)
	response := generate(greeterRequestWithPackage("", importPrefix+"/hello/v1;hellov1"))
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}

	packageDir := filepath.Join(root, "hello", "v1")
	writeFile(t, filepath.Join(packageDir, "messages.go"), `package hellov1

type HelloRequest struct {
	Value string `+"`"+`protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`+"`"+`
}
func (m *HelloRequest) Reset() { *m = HelloRequest{} }
func (m *HelloRequest) String() string { return m.Value }
func (*HelloRequest) ProtoMessage() {}

type HelloReply struct {
	Value string `+"`"+`protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`+"`"+`
}
func (m *HelloReply) Reset() { *m = HelloReply{} }
func (m *HelloReply) String() string { return m.Value }
func (*HelloReply) ProtoMessage() {}
`)
	writeFile(t, filepath.Join(root, response.File[0].GetName()), response.File[0].GetContent())
	writeFile(t, filepath.Join(packageDir, "service_test.go"), `package hellov1

import (
	"context"
	"io"
	"testing"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type directTransport struct{ server *trevrpc.Server }
func (t directTransport) Call(ctx context.Context, request *trevrpc.RpcRequest) (*trevrpc.RpcResponse, error) {
	return t.server.HandleRequest(ctx, request), nil
}
func (t directTransport) StreamingCall(ctx context.Context, request *trevrpc.RpcRequest, body trevrpc.ByteStream) (trevrpc.FrameStream, error) {
	return t.server.HandleStreamingRequest(ctx, request, body), nil
}

type service struct{}

type terminalResponseStream struct {
	*trevrpc.MessagePipe[*HelloReply]
	status *trevrpc.Status
}
func (s *terminalResponseStream) TerminalStatus() (*trevrpc.Status, bool) { return s.status, true }

func (service) SayHello(context.Context, *HelloRequest) (*trevrpc.Response[*HelloReply], error) {
	return trevrpc.NewResponse(&HelloReply{Value: "unary"}, trevrpc.WithResponseMetadata[*HelloReply]("result", []byte("unary-meta"))), nil
}
func (service) LotsOfReplies(ctx context.Context, _ *HelloRequest) (trevrpc.ResponseStream[*HelloReply], error) {
	responses := trevrpc.NewMessagePipe[*HelloReply](ctx)
	go func() {
		_ = responses.Send(&HelloReply{Value: "server-stream"})
		_ = responses.Close()
	}()
	return trevrpc.WithResponseStreamMetadata(responses, trevrpc.Metadata{"result": []byte("server-meta")}), nil
}
func (service) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*HelloRequest]) (*trevrpc.Response[*HelloReply], error) {
	for {
		_, err := requests.Recv()
		if err == io.EOF { break }
		if err != nil { return nil, err }
	}
	return trevrpc.NewResponse(&HelloReply{Value: "client-stream"}, trevrpc.WithResponseMetadata[*HelloReply]("result", []byte("client-meta"))), nil
}
func (service) BidiHello(ctx context.Context, _ trevrpc.MessageStream[*HelloRequest]) (trevrpc.ResponseStream[*HelloReply], error) {
	responses := trevrpc.NewMessagePipe[*HelloReply](ctx)
	go func() {
		_ = responses.Send(&HelloReply{Value: "bidi"})
		_ = responses.Close()
	}()
	return &terminalResponseStream{
		MessagePipe: responses,
		status: trevrpc.FailedPrecondition("terminal").WithMetadata(trevrpc.Metadata{"result": []byte("bidi-meta")}),
	}, nil
}

func TestGeneratedMetadataAdapters(t *testing.T) {
	server := trevrpc.NewServer()
	RegisterGreeterServer(server, service{})
	client := NewGreeterClient(directTransport{server: server})
	ctx := context.Background()

	unary, err := client.SayHelloResponse(ctx, &HelloRequest{})
	if err != nil || unary.Message.Value != "unary" || string(unary.Metadata["result"]) != "unary-meta" {
		t.Fatalf("unary response = %#v, %v", unary, err)
	}

	clientStreaming, err := client.LotsOfGreetingsFromStreamResponse(ctx, trevrpc.FromSlice(&HelloRequest{}))
	if err != nil || clientStreaming.Message.Value != "client-stream" || string(clientStreaming.Metadata["result"]) != "client-meta" {
		t.Fatalf("client-streaming response = %#v, %v", clientStreaming, err)
	}

	serverStreaming, err := client.LotsOfRepliesResponse(ctx, &HelloRequest{})
	if err != nil { t.Fatal(err) }
	if _, err := serverStreaming.Recv(); err != nil { t.Fatal(err) }
	if _, err := serverStreaming.Recv(); err != io.EOF { t.Fatalf("server stream terminal err = %v", err) }
	serverStatus, ok := serverStreaming.TerminalStatus()
	if !ok || string(serverStatus.Metadata["result"]) != "server-meta" { t.Fatalf("server status = %#v, %v", serverStatus, ok) }

	bidi, err := client.BidiHelloFromStreamResponse(ctx, trevrpc.FromSlice(&HelloRequest{}))
	if err != nil { t.Fatal(err) }
	if _, err := bidi.Recv(); err != nil { t.Fatal(err) }
	if _, err := bidi.Recv(); trevrpc.StatusFromError(err).Code != trevrpc.CodeFailedPrecondition { t.Fatalf("bidi terminal err = %v", err) }
	bidiStatus, ok := bidi.TerminalStatus()
	if !ok || bidiStatus.Code != trevrpc.CodeFailedPrecondition || bidiStatus.Message != "terminal" || string(bidiStatus.Metadata["result"]) != "bidi-meta" { t.Fatalf("bidi status = %#v, %v", bidiStatus, ok) }

	var _ func(context.Context, ...trevrpc.CallOption) (trevrpc.ClientStreamingCall[*HelloRequest, *HelloReply], error) = client.LotsOfGreetings
	var _ func(context.Context, ...trevrpc.CallOption) (trevrpc.ClientStreamingResponseCall[*HelloRequest, *HelloReply], error) = client.LotsOfGreetingsResponse
	var _ func(context.Context, ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*HelloRequest, *HelloReply], error) = client.BidiHello
	var _ func(context.Context, ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingResponseCall[*HelloRequest, *HelloReply], error) = client.BidiHelloResponse
}
`)
	runFixtureTests(t, runtimeRoot, relativeRoot)
}

func TestGeneratedMultiFilePackageAndImportIdentityCompiles(t *testing.T) {
	runtimeRoot, root, relativeRoot, importPrefix := newInModuleFixture(t)
	request := importIdentityRequest(importPrefix)
	response := generate(request)
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	if len(response.File) != 2 {
		t.Fatalf("expected two generated files, got %d", len(response.File))
	}

	writeFile(t, filepath.Join(root, "api", "messages.go"), legacyMessages("apipb", "Local", "Second"))
	writeFile(t, filepath.Join(root, "a", "shared", "messages.go"), legacyMessages("shared", "Outer_Nested", "Outer_Other"))
	writeFile(t, filepath.Join(root, "b", "shared", "messages.go"), legacyMessages("shared", "Item"))
	writeFile(t, filepath.Join(root, "external", "api", "messages.go"), legacyMessages("apipb", "External"))
	for _, file := range response.File {
		writeFile(t, filepath.Join(root, file.GetName()), file.GetContent())
	}
	runFixtureTests(t, runtimeRoot, relativeRoot)
}

func TestGeneratorParameterBehavior(t *testing.T) {
	tests := []struct {
		name      string
		parameter string
		wantName  string
		contains  []string
	}{
		{name: "implicit source relative", wantName: "hello/v1/greeter.trevrpc.go"},
		{name: "explicit source relative", parameter: "paths=source_relative", wantName: "hello/v1/greeter.trevrpc.go"},
		{name: "import paths", parameter: "paths=import", wantName: "example.com/fixture/hello/v1/greeter.trevrpc.go"},
		{name: "module", parameter: "module=example.com/fixture", wantName: "hello/v1/greeter.trevrpc.go"},
		{name: "custom options", parameter: "runtime_import=example.com/custom/runtime,file_suffix=.rpc.go,service_prefix=Native", wantName: "hello/v1/greeter.rpc.go", contains: []string{"type NativeGreeterServer interface", "func NewNativeGreeterClient", "func RegisterNativeGreeterServer", `"example.com/custom/runtime"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := generate(greeterRequest(test.parameter))
			if response.GetError() != "" {
				t.Fatalf("generator returned error: %s", response.GetError())
			}
			if len(response.File) != 1 || response.File[0].GetName() != test.wantName {
				t.Fatalf("generated files = %#v, want %q", response.File, test.wantName)
			}
			for _, want := range test.contains {
				if !strings.Contains(response.File[0].GetContent(), want) {
					t.Fatalf("generated content missing %q", want)
				}
			}
		})
	}

	mapped := greeterRequest("paths=import,Mhello/v1/greeter.proto=example.com/override;alias")
	mappedResponse := generate(mapped)
	if mappedResponse.GetError() != "" {
		t.Fatalf("M mapping failed: %s", mappedResponse.GetError())
	}
	if got := mappedResponse.File[0].GetName(); got != "example.com/override/greeter.trevrpc.go" {
		t.Fatalf("M-mapped output name = %q", got)
	}
	if !strings.Contains(mappedResponse.File[0].GetContent(), "package alias") {
		t.Fatalf("M mapping did not override package name:\n%s", mappedResponse.File[0].GetContent())
	}
}

func TestGeneratorRejectsInvalidInputs(t *testing.T) {
	t.Run("unknown option", func(t *testing.T) {
		response := generate(greeterRequest("bogus=1"))
		if !strings.Contains(response.GetError(), "unknown trevrpc-go option") {
			t.Fatalf("unexpected error: %q", response.GetError())
		}
	})

	t.Run("missing Go import path", func(t *testing.T) {
		request := greeterRequest("")
		request.ProtoFile[0].Options.GoPackage = nil
		response := generate(request)
		if !strings.Contains(response.GetError(), "unable to determine Go import path") {
			t.Fatalf("unexpected error: %q", response.GetError())
		}
	})

	t.Run("convenience method collision", func(t *testing.T) {
		request := greeterRequest("")
		request.ProtoFile[0].Service[0].Method = append(request.ProtoFile[0].Service[0].Method, &descriptor.MethodDescriptorProto{
			Name: new("SayHelloResponse"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply"),
		})
		response := generate(request)
		if !strings.Contains(response.GetError(), "SayHello and SayHelloResponse") || !strings.Contains(response.GetError(), "SayHelloResponse") {
			t.Fatalf("unexpected collision error: %q", response.GetError())
		}
	})

	t.Run("normalized method collision", func(t *testing.T) {
		request := greeterRequest("")
		request.ProtoFile[0].Service[0].Method = []*descriptor.MethodDescriptorProto{
			{Name: new("foo_bar"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply")},
			{Name: new("fooBar"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply")},
		}
		response := generate(request)
		if !strings.Contains(response.GetError(), "hello.v1.Greeter.foo_bar") || !strings.Contains(response.GetError(), "hello.v1.Greeter.fooBar") || !strings.Contains(response.GetError(), "FooBar") {
			t.Fatalf("unexpected normalized collision error: %q", response.GetError())
		}
	})

	t.Run("package-wide service collision", func(t *testing.T) {
		response := generate(packageServiceCollisionRequest("service_prefix=Native"))
		if !strings.Contains(response.GetError(), "NativeEchoServer") || !strings.Contains(response.GetError(), "first.v1.Echo") || !strings.Contains(response.GetError(), "second.v1.Echo") {
			t.Fatalf("unexpected package collision error: %q", response.GetError())
		}
	})
}

func greeterRequest(parameter string) *plugin.CodeGeneratorRequest {
	return greeterRequestWithPackage(parameter, "example.com/fixture/hello/v1;hellov1")
}

func greeterRequestWithPackage(parameter, goPackage string) *plugin.CodeGeneratorRequest {
	return &plugin.CodeGeneratorRequest{
		Parameter:      new(parameter),
		FileToGenerate: []string{"hello/v1/greeter.proto"},
		ProtoFile: []*descriptor.FileDescriptorProto{{
			Name: new("hello/v1/greeter.proto"), Package: new("hello.v1"),
			Options:     &descriptor.FileOptions{GoPackage: new(goPackage)},
			MessageType: []*descriptor.DescriptorProto{{Name: new("HelloRequest")}, {Name: new("HelloReply")}},
			Service: []*descriptor.ServiceDescriptorProto{{
				Name: new("Greeter"),
				Method: []*descriptor.MethodDescriptorProto{
					{Name: new("SayHello"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply")},
					{Name: new("LotsOfReplies"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply"), ServerStreaming: new(true)},
					{Name: new("LotsOfGreetings"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply"), ClientStreaming: new(true)},
					{Name: new("BidiHello"), InputType: new(".hello.v1.HelloRequest"), OutputType: new(".hello.v1.HelloReply"), ClientStreaming: new(true), ServerStreaming: new(true)},
				},
			}},
		}},
	}
}

func importIdentityRequest(importPrefix string) *plugin.CodeGeneratorRequest {
	return &plugin.CodeGeneratorRequest{
		FileToGenerate: []string{"api/one.proto", "api/two.proto"},
		ProtoFile: []*descriptor.FileDescriptorProto{
			{
				Name: new("a/shared/types.proto"), Package: new("external.a"), Options: &descriptor.FileOptions{GoPackage: new(importPrefix + "/a/shared;shared")},
				MessageType: []*descriptor.DescriptorProto{{Name: new("Outer"), NestedType: []*descriptor.DescriptorProto{{Name: new("Nested")}, {Name: new("Other")}}}},
			},
			{
				Name: new("b/shared/types.proto"), Package: new("external.b"), Options: &descriptor.FileOptions{GoPackage: new(importPrefix + "/b/shared;shared")},
				MessageType: []*descriptor.DescriptorProto{{Name: new("Item")}},
			},
			{
				Name: new("external/api/types.proto"), Package: new("external.same"), Options: &descriptor.FileOptions{GoPackage: new(importPrefix + "/external/api;apipb")},
				MessageType: []*descriptor.DescriptorProto{{Name: new("External")}},
			},
			{
				Name: new("api/one.proto"), Package: new("fixture.api"), Options: &descriptor.FileOptions{GoPackage: new(importPrefix + "/api;apipb")},
				Dependency: []string{"a/shared/types.proto"}, MessageType: []*descriptor.DescriptorProto{{Name: new("Local")}},
				Service: []*descriptor.ServiceDescriptorProto{{Name: new("First"), Method: []*descriptor.MethodDescriptorProto{
					{Name: new("Nested"), InputType: new(".external.a.Outer.Nested"), OutputType: new(".external.a.Outer.Other")},
				}}},
			},
			{
				Name: new("api/two.proto"), Package: new("fixture.api"), Options: &descriptor.FileOptions{GoPackage: new(importPrefix + "/api;apipb")},
				Dependency: []string{"api/one.proto", "a/shared/types.proto", "b/shared/types.proto", "external/api/types.proto"}, MessageType: []*descriptor.DescriptorProto{{Name: new("Second")}},
				Service: []*descriptor.ServiceDescriptorProto{{Name: new("SecondService"), Method: []*descriptor.MethodDescriptorProto{
					{Name: new("LocalType"), InputType: new(".fixture.api.Local"), OutputType: new(".fixture.api.Second")},
					{Name: new("DuplicatePackageA"), InputType: new(".external.a.Outer.Nested"), OutputType: new(".external.b.Item")},
					{Name: new("SamePackageName"), InputType: new(".external.same.External"), OutputType: new(".fixture.api.Local")},
				}}},
			},
		},
	}
}

func packageServiceCollisionRequest(parameter string) *plugin.CodeGeneratorRequest {
	return &plugin.CodeGeneratorRequest{
		Parameter:      new(parameter),
		FileToGenerate: []string{"first/service.proto", "second/service.proto"},
		ProtoFile: []*descriptor.FileDescriptorProto{
			{
				Name: new("first/service.proto"), Package: new("first.v1"), Options: &descriptor.FileOptions{GoPackage: new("example.com/fixture/shared;shared")},
				MessageType: []*descriptor.DescriptorProto{{Name: new("Request")}, {Name: new("Response")}},
				Service: []*descriptor.ServiceDescriptorProto{{Name: new("Echo"), Method: []*descriptor.MethodDescriptorProto{{
					Name: new("Call"), InputType: new(".first.v1.Request"), OutputType: new(".first.v1.Response"),
				}}}},
			},
			{
				Name: new("second/service.proto"), Package: new("second.v1"), Options: &descriptor.FileOptions{GoPackage: new("example.com/fixture/shared;shared")},
				MessageType: []*descriptor.DescriptorProto{{Name: new("Request")}, {Name: new("Response")}},
				Service: []*descriptor.ServiceDescriptorProto{{Name: new("Echo"), Method: []*descriptor.MethodDescriptorProto{{
					Name: new("Call"), InputType: new(".second.v1.Request"), OutputType: new(".second.v1.Response"),
				}}}},
			},
		},
	}
}

func newInModuleFixture(t *testing.T) (runtimeRoot, root, relativeRoot, importPrefix string) {
	t.Helper()
	runtimeRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve runtime root: %v", err)
	}
	root, err = os.MkdirTemp(runtimeRoot, "generated-compile-")
	if err != nil {
		t.Fatalf("create generated compile dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	relativeRoot, err = filepath.Rel(runtimeRoot, root)
	if err != nil {
		t.Fatalf("resolve generated compile dir: %v", err)
	}
	importPrefix = "trev.zip/llc/trevrpc/trevrpc-go/" + filepath.ToSlash(relativeRoot)
	return runtimeRoot, root, relativeRoot, importPrefix
}

func runFixtureTests(t *testing.T, runtimeRoot, relativeRoot string) {
	t.Helper()
	command := exec.Command("go", "test", "./"+filepath.ToSlash(relativeRoot)+"/...")
	command.Dir = runtimeRoot
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated fixture should compile and pass: %v\n%s", err, output)
	}
}

func legacyMessages(packageName string, names ...string) string {
	var source strings.Builder
	source.WriteString("package ")
	source.WriteString(packageName)
	source.WriteString("\n\n")
	for _, name := range names {
		source.WriteString("type ")
		source.WriteString(name)
		source.WriteString(" struct{}\n")
		source.WriteString("func (m *")
		source.WriteString(name)
		source.WriteString(") Reset() { *m = ")
		source.WriteString(name)
		source.WriteString("{} }\n")
		source.WriteString("func (*")
		source.WriteString(name)
		source.WriteString(") String() string { return \"\" }\n")
		source.WriteString("func (*")
		source.WriteString(name)
		source.WriteString(") ProtoMessage() {}\n\n")
	}
	return source.String()
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
