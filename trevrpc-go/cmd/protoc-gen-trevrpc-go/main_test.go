package main

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
	plugin "google.golang.org/protobuf/types/pluginpb"

	"trev.zip/llc/trevrpc/trevrpc-go/trevrpcpb"
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
		"SayHello(context.Context, *HelloRequest) (*HelloReply, error)",
		"LotsOfReplies(context.Context, *HelloRequest)",
		"LotsOfGreetings(context.Context, trevrpc_go.MessageStream[*HelloRequest]) (*HelloReply, error)",
		"BidiHello(context.Context",
		"func NewGreeterClient",
		"func (c *GreeterClient) mergedCallOptions",
		"func (c *GreeterClient) SayHello(",
		"func (c *GreeterClient) LotsOfReplies(",
		"func (c *GreeterClient) LotsOfGreetings(",
		"func (c *GreeterClient) BidiHello(",
		`"/hello.v1.Greeter/SayHello"`,
		`"/hello.v1.Greeter/LotsOfReplies"`,
		`"/hello.v1.Greeter/LotsOfGreetings"`,
		`"/hello.v1.Greeter/BidiHello"`,
		"(ctx, c.transport, Greeter_SayHello_FullMethodName, request,",
		"(server, Greeter_SayHello_FullMethodName, func()",
		"RegisterUnary[*HelloRequest, *HelloReply]",
		"RegisterClientStreaming[*HelloRequest, *HelloReply]",
		"RegisterServerStreaming[*HelloRequest, *HelloReply]",
		"RegisterBidirectionalStreaming[*HelloRequest, *HelloReply]",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated content missing %q:\n%s", want, content)
		}
	}
	// Each RPC has exactly one client method; there are no metadata or
	// request-stream variants.
	for _, unwanted := range []string{"Response(ctx", "FromStream", "ResponseCall"} {
		if strings.Contains(content, unwanted) {
			t.Fatalf("generated content contains duplicate-shape variant %q:\n%s", unwanted, content)
		}
	}
	// Handlers return plain messages and set response metadata through their
	// context: there is no response wrapper.
	if strings.Contains(content, ".Response[") {
		t.Fatalf("generated a wrapped handler response:\n%s", content)
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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type service struct{}

// replyThenStatus answers one request with one reply and ends with status.
type replyThenStatus struct {
	requests trevrpc.MessageStream[*HelloRequest]
	answered bool
	status   *trevrpc.Status
}

func (s *replyThenStatus) Recv() (*HelloReply, error) {
	if s.answered {
		return nil, s.status
	}
	if _, err := s.requests.Recv(); err != nil {
		return nil, err
	}
	s.answered = true
	return &HelloReply{Value: "bidi"}, nil
}
func (s *replyThenStatus) Close() error { return s.requests.Close() }

// oneReply yields a single reply and then sets the trailing metadata.
type oneReply struct {
	ctx  context.Context
	sent bool
}

func (s *oneReply) Recv() (*HelloReply, error) {
	if s.sent {
		if err := trevrpc.SetResponseMetadata(s.ctx, "result", []byte("server-meta")); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	s.sent = true
	return &HelloReply{Value: "server-stream"}, nil
}
func (*oneReply) Close() error { return nil }

func (service) SayHello(ctx context.Context, _ *HelloRequest) (*HelloReply, error) {
	if err := trevrpc.SetResponseMetadata(ctx, "result", []byte("unary-meta")); err != nil {
		return nil, err
	}
	return &HelloReply{Value: "unary"}, nil
}
func (service) LotsOfReplies(ctx context.Context, _ *HelloRequest) (trevrpc.MessageStream[*HelloReply], error) {
	return &oneReply{ctx: ctx}, nil
}
func (service) LotsOfGreetings(ctx context.Context, requests trevrpc.MessageStream[*HelloRequest]) (*HelloReply, error) {
	for _, err := range trevrpc.Messages(requests) {
		if err != nil {
			return nil, err
		}
	}
	if err := trevrpc.SetResponseMetadata(ctx, "result", []byte("client-meta")); err != nil {
		return nil, err
	}
	return &HelloReply{Value: "client-stream"}, nil
}
func (service) BidiHello(_ context.Context, requests trevrpc.MessageStream[*HelloRequest]) (trevrpc.MessageStream[*HelloReply], error) {
	return &replyThenStatus{
		requests: requests,
		status:   trevrpc.FailedPrecondition("terminal").WithMetadata(trevrpc.Metadata{"result": []byte("bidi-meta")}),
	}, nil
}

// dialLoopback serves implementation over native QUIC on a loopback port and
// returns a connected channel.
func dialLoopback(t *testing.T, implementation GreeterServer) *trevrpc.Channel {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)

	server := trevrpc.NewServer()
	RegisterGreeterServer(server, implementation)
	listener, err := trevrpc.Listen("127.0.0.1:0", server, trevrpc.ListenOptions{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = listener.Serve(ctx) }()

	channel, err := trevrpc.Dial(ctx, listener.Addr().String(), trevrpc.DialOptions{TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost"}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = channel.Close()
		cancel()
		<-done
		_ = listener.Close()
	})
	return channel
}

func TestGeneratedClientAndServerRoundTripMetadata(t *testing.T) {
	if Greeter_SayHello_FullMethodName != "/hello.v1.Greeter/SayHello" || Greeter_BidiHello_FullMethodName != "/hello.v1.Greeter/BidiHello" {
		t.Fatalf("full method constants = %q, %q", Greeter_SayHello_FullMethodName, Greeter_BidiHello_FullMethodName)
	}
	client := NewGreeterClient(dialLoopback(t, service{}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var unaryMetadata trevrpc.Metadata
	unary, err := client.SayHello(ctx, &HelloRequest{}, trevrpc.CaptureResponseMetadata(&unaryMetadata))
	if err != nil || unary.Value != "unary" || string(unaryMetadata["result"]) != "unary-meta" {
		t.Fatalf("unary response = %#v, metadata %#v, %v", unary, unaryMetadata, err)
	}

	var clientMetadata trevrpc.Metadata
	upload, err := client.LotsOfGreetings(ctx, trevrpc.CaptureResponseMetadata(&clientMetadata))
	if err != nil {
		t.Fatal(err)
	}
	if err := upload.Send(&HelloRequest{}); err != nil {
		t.Fatal(err)
	}
	clientStreaming, err := upload.CloseAndRecv()
	if err != nil || clientStreaming.Value != "client-stream" || string(clientMetadata["result"]) != "client-meta" {
		t.Fatalf("client-streaming response = %#v, metadata %#v, %v", clientStreaming, clientMetadata, err)
	}

	serverStreaming, err := client.LotsOfReplies(ctx, &HelloRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serverStreaming.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := serverStreaming.Recv(); err != io.EOF {
		t.Fatalf("server stream terminal err = %v", err)
	}
	serverStatus, ok := serverStreaming.TerminalStatus()
	if !ok || string(serverStatus.Metadata["result"]) != "server-meta" {
		t.Fatalf("server status = %#v, %v", serverStatus, ok)
	}

	bidi, err := client.BidiHello(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := bidi.Send(&HelloRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := bidi.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := bidi.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := bidi.Recv(); trevrpc.StatusFromError(err).Code != trevrpc.CodeFailedPrecondition {
		t.Fatalf("bidi terminal err = %v", err)
	}
	bidiStatus, ok := bidi.TerminalStatus()
	if !ok || bidiStatus.Code != trevrpc.CodeFailedPrecondition || bidiStatus.Message != "terminal" || string(bidiStatus.Metadata["result"]) != "bidi-meta" {
		t.Fatalf("bidi status = %#v, %v", bidiStatus, ok)
	}

	// Every RPC has exactly one client method.
	var _ func(context.Context, *HelloRequest, ...trevrpc.CallOption) (*HelloReply, error) = client.SayHello
	var _ func(context.Context, *HelloRequest, ...trevrpc.CallOption) (trevrpc.ServerStreamingCall[*HelloReply], error) = client.LotsOfReplies
	var _ func(context.Context, ...trevrpc.CallOption) (trevrpc.ClientStreamingCall[*HelloRequest, *HelloReply], error) = client.LotsOfGreetings
	var _ func(context.Context, ...trevrpc.CallOption) (trevrpc.BidirectionalStreamingCall[*HelloRequest, *HelloReply], error) = client.BidiHello
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
		{name: "custom options", parameter: "runtime_import=example.com/custom/runtime,file_suffix=.rpc.go,service_prefix=Native", wantName: "hello/v1/greeter.rpc.go", contains: []string{"type NativeGreeterServer interface", "func NewNativeGreeterClient", "func RegisterNativeGreeterServer", "NativeGreeter_SayHello_FullMethodName", `"example.com/custom/runtime"`}},
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
	command := exec.CommandContext(t.Context(), "go", "test", "./"+filepath.ToSlash(relativeRoot)+"/...")
	command.Dir = runtimeRoot
	command.Env = os.Environ()
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

// unreliableRequest returns a request with one method per shape, each marked
// with the given options (nil for none), as protoc would send it: the options
// travel as serialized bytes.
func unreliableRequest(t *testing.T, goPackage string, methods ...unreliableMethod) *plugin.CodeGeneratorRequest {
	t.Helper()
	var descriptors []*descriptor.MethodDescriptorProto
	for _, method := range methods {
		input := ".hello.v1.HelloRequest"
		if method.input != "" {
			input = ".hello.v1." + method.input
		}
		method := &descriptor.MethodDescriptorProto{
			Name: new(method.name), InputType: new(input), OutputType: new(".hello.v1.HelloReply"),
			ClientStreaming: new(method.clientStreaming), ServerStreaming: new(method.serverStreaming),
			Options: method.options(t),
		}
		descriptors = append(descriptors, method)
	}
	request := &plugin.CodeGeneratorRequest{
		FileToGenerate: []string{"hello/v1/greeter.proto"},
		ProtoFile: []*descriptor.FileDescriptorProto{{
			Name: new("hello/v1/greeter.proto"), Package: new("hello.v1"),
			Options: &descriptor.FileOptions{GoPackage: new(goPackage)},
			MessageType: []*descriptor.DescriptorProto{{Name: new("HelloRequest")}, {Name: new("HelloReply")}, {
				// Secret holds a redacted field one message down, in Inner.
				Name: new("Secret"),
				Field: []*descriptor.FieldDescriptorProto{{
					Name: new("inner"), Number: new(int32(1)), Label: descriptor.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type: descriptor.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".hello.v1.Inner"), JsonName: new("inner"),
				}},
			}, {
				Name: new("Inner"),
				Field: []*descriptor.FieldDescriptorProto{{
					Name: new("token"), Number: new(int32(1)), Label: descriptor.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type: descriptor.FieldDescriptorProto_TYPE_STRING.Enum(), JsonName: new("token"),
					Options: &descriptor.FieldOptions{DebugRedact: new(true)},
				}, {
					// A recursive field, which the redaction check must not loop on.
					Name: new("next"), Number: new(int32(2)), Label: descriptor.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type: descriptor.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".hello.v1.Inner"), JsonName: new("next"),
				}},
			}},
			Service: []*descriptor.ServiceDescriptorProto{{Name: new("Greeter"), Method: descriptors}},
		}},
	}
	// Round-trip the request, so the option is a serialized extension the
	// plugin has to resolve, not a message already in memory.
	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &plugin.CodeGeneratorRequest{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

type unreliableMethod struct {
	name                             string
	clientStreaming, serverStreaming bool
	// requests, responses and compress are the options; none set means no
	// option.
	requests, responses, compress bool
	// plain sets the compress option.
	plain bool
	// input is the request message type, HelloRequest when empty.
	input string
}

func (m unreliableMethod) options(t *testing.T) *descriptor.MethodOptions {
	t.Helper()
	if !m.requests && !m.responses && !m.compress && !m.plain {
		return nil
	}
	options := &descriptor.MethodOptions{}
	proto.SetExtension(options, trevrpcpb.E_Method, &trevrpcpb.MethodOptions{UnreliableRequests: m.requests, UnreliableResponses: m.responses, CompressStream: m.compress, Compress: m.plain})
	return options
}

func TestGeneratorEmitsStreamCompressionOptions(t *testing.T) {
	request := unreliableRequest(t, "example.test/hello/v1;hellov1",
		unreliableMethod{name: "Bidi", clientStreaming: true, serverStreaming: true, compress: true},
		unreliableMethod{name: "Upload", clientStreaming: true, compress: true},
		unreliableMethod{name: "Download", serverStreaming: true, compress: true, responses: true},
		unreliableMethod{name: "Leaky", clientStreaming: true, serverStreaming: true, compress: true, input: "Secret"},
		unreliableMethod{name: "SecretUpload", clientStreaming: true, compress: true, input: "Secret"},
	)
	response := generate(request)
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	content := response.File[0].GetContent()
	// compress_stream implies compress, for the messages outside the stream.
	plainBoth := "trevrpc_go.PlainCompression{Requests: true, Responses: true}"
	plainResponses := "trevrpc_go.PlainCompression{Requests: false, Responses: true}"
	for _, want := range []string{
		"c.mergedCallOptions(options, trevrpc_go.WithStreamCompression(trevrpc_go.StreamCompression{Requests: true, Responses: true}), trevrpc_go.WithPlainCompression(" + plainBoth + "))...)",
		"c.mergedCallOptions(options, trevrpc_go.WithStreamCompression(trevrpc_go.StreamCompression{Requests: true, Responses: false}), trevrpc_go.WithPlainCompression(" + plainBoth + "))...)",
		"c.mergedCallOptions(options, trevrpc_go.WithUnreliable(trevrpc_go.Unreliable{Requests: false, Responses: true}), trevrpc_go.WithStreamCompression(trevrpc_go.StreamCompression{Requests: false, Responses: true}), trevrpc_go.WithPlainCompression(" + plainBoth + "))...)",
		// A request type with a redacted field keeps its direction out.
		"c.mergedCallOptions(options, trevrpc_go.WithStreamCompression(trevrpc_go.StreamCompression{Requests: false, Responses: true}), trevrpc_go.WithPlainCompression(" + plainResponses + "))...)",
		"implementation.Bidi, trevrpc_go.RouteStreamCompression(trevrpc_go.StreamCompression{Requests: true, Responses: true}), trevrpc_go.RoutePlainCompression(" + plainBoth + "))",
		"implementation.Download, trevrpc_go.RouteUnreliable(trevrpc_go.Unreliable{Requests: false, Responses: true}), trevrpc_go.RouteStreamCompression(trevrpc_go.StreamCompression{Requests: false, Responses: true}), trevrpc_go.RoutePlainCompression(" + plainBoth + "))",
		"implementation.Leaky, trevrpc_go.RouteStreamCompression(trevrpc_go.StreamCompression{Requests: false, Responses: true}), trevrpc_go.RoutePlainCompression(" + plainResponses + "))",
		// Only the unredacted response is left for a client-streaming method
		// of a redacted request type.
		"implementation.SecretUpload, trevrpc_go.RoutePlainCompression(" + plainResponses + "))",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("generated content missing %q:\n%s", want, content)
		}
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "greeter.trevrpc.go", content, parser.AllErrors); err != nil {
		t.Fatalf("generated Go is not syntactically valid: %v\n%s", err, content)
	}
}

func TestGeneratorEmitsPlainCompressionOptions(t *testing.T) {
	request := unreliableRequest(t, "example.test/hello/v1;hellov1",
		unreliableMethod{name: "Call", plain: true},
		unreliableMethod{name: "SecretCall", plain: true, input: "Secret"},
		unreliableMethod{name: "Bare"},
	)
	response := generate(request)
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	content := response.File[0].GetContent()
	for _, want := range []string{
		"c.mergedCallOptions(options, trevrpc_go.WithPlainCompression(trevrpc_go.PlainCompression{Requests: true, Responses: true}))...)",
		"implementation.Call, trevrpc_go.RoutePlainCompression(trevrpc_go.PlainCompression{Requests: true, Responses: true}))",
		"c.mergedCallOptions(options, trevrpc_go.WithPlainCompression(trevrpc_go.PlainCompression{Requests: false, Responses: true}))...)",
		"implementation.SecretCall, trevrpc_go.RoutePlainCompression(trevrpc_go.PlainCompression{Requests: false, Responses: true}))",
		"implementation.Bare)",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("generated content missing %q:\n%s", want, content)
		}
	}
}

func TestGeneratorRejectsStreamCompressionOnUnaryMethods(t *testing.T) {
	response := generate(unreliableRequest(t, "example.test/hello/v1;hellov1", unreliableMethod{name: "Call", compress: true}))
	if want := "hello.v1.Greeter.Call: compress_stream requires a streaming method"; response.GetError() != want {
		t.Fatalf("error = %q, want %q", response.GetError(), want)
	}
}

func TestGeneratorEmitsUnreliableOptions(t *testing.T) {
	request := unreliableRequest(t, "example.test/hello/v1;hellov1",
		unreliableMethod{name: "Plain", clientStreaming: true, serverStreaming: true},
		unreliableMethod{name: "Bidi", clientStreaming: true, serverStreaming: true, requests: true, responses: true},
		unreliableMethod{name: "Upload", clientStreaming: true, requests: true},
		unreliableMethod{name: "Download", serverStreaming: true, responses: true},
	)
	response := generate(request)
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	content := response.File[0].GetContent()
	for _, want := range []string{
		"c.mergedCallOptions(options, trevrpc_go.WithUnreliable(trevrpc_go.Unreliable{Requests: true, Responses: true}))...)",
		"c.mergedCallOptions(options, trevrpc_go.WithUnreliable(trevrpc_go.Unreliable{Requests: true, Responses: false}))...)",
		"c.mergedCallOptions(options, trevrpc_go.WithUnreliable(trevrpc_go.Unreliable{Requests: false, Responses: true}))...)",
		"implementation.Bidi, trevrpc_go.RouteUnreliable(trevrpc_go.Unreliable{Requests: true, Responses: true}))",
		"implementation.Upload, trevrpc_go.RouteUnreliable(trevrpc_go.Unreliable{Requests: true, Responses: false}))",
		"implementation.Download, trevrpc_go.RouteUnreliable(trevrpc_go.Unreliable{Requests: false, Responses: true}))",
		"c.mergedCallOptions(options)...)",
		"implementation.Plain)",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("generated content missing %q:\n%s", want, content)
		}
	}
	if got := strings.Count(content, "WithUnreliable("); got != 3 {
		t.Errorf("generated %d WithUnreliable options, want 3 (unmarked methods get none):\n%s", got, content)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "greeter.trevrpc.go", content, parser.AllErrors); err != nil {
		t.Fatalf("generated Go is not syntactically valid: %v\n%s", err, content)
	}
}

func TestGeneratorRejectsUnreliableDirectionsThatDoNotStream(t *testing.T) {
	for _, tt := range []struct {
		name   string
		method unreliableMethod
		want   string
	}{
		{"unary requests", unreliableMethod{name: "Call", requests: true}, "hello.v1.Greeter.Call: unreliable_requests requires a client-streaming or bidirectional method"},
		{"unary responses", unreliableMethod{name: "Call", responses: true}, "hello.v1.Greeter.Call: unreliable_responses requires a server-streaming or bidirectional method"},
		{"server-streaming requests", unreliableMethod{name: "Watch", serverStreaming: true, requests: true}, "hello.v1.Greeter.Watch: unreliable_requests requires a client-streaming or bidirectional method"},
		{"client-streaming responses", unreliableMethod{name: "Upload", clientStreaming: true, responses: true}, "hello.v1.Greeter.Upload: unreliable_responses requires a server-streaming or bidirectional method"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := generate(unreliableRequest(t, "example.test/hello/v1;hellov1", tt.method))
			if response.GetError() != tt.want {
				t.Fatalf("error = %q, want %q", response.GetError(), tt.want)
			}
		})
	}
}

func TestGeneratedUnreliableOptionsCompile(t *testing.T) {
	runtimeRoot, root, relativeRoot, importPrefix := newInModuleFixture(t)
	response := generate(unreliableRequest(t, importPrefix+"/hello/v1;hellov1",
		unreliableMethod{name: "Bidi", clientStreaming: true, serverStreaming: true, requests: true, responses: true, compress: true},
		unreliableMethod{name: "Upload", clientStreaming: true, requests: true},
		unreliableMethod{name: "Download", serverStreaming: true, responses: true, compress: true},
	))
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	packageDir := filepath.Join(root, "hello", "v1")
	writeFile(t, filepath.Join(packageDir, "messages.go"), legacyMessages("hellov1", "HelloRequest", "HelloReply"))
	writeFile(t, filepath.Join(root, response.File[0].GetName()), response.File[0].GetContent())
	writeFile(t, filepath.Join(packageDir, "service_test.go"), `package hellov1

import (
	"context"
	"testing"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type service struct{}

func (service) Bidi(context.Context, trevrpc.MessageStream[*HelloRequest]) (trevrpc.MessageStream[*HelloReply], error) {
	return nil, nil
}

func (service) Upload(context.Context, trevrpc.MessageStream[*HelloRequest]) (*HelloReply, error) {
	return nil, nil
}

func (service) Download(context.Context, *HelloRequest) (trevrpc.MessageStream[*HelloReply], error) {
	return nil, nil
}

func TestRegistersAndCreatesClient(t *testing.T) {
	RegisterGreeterServer(trevrpc.NewServer(), service{})
	_ = NewGreeterClient(nil)
}
`)
	runFixtureTests(t, runtimeRoot, relativeRoot)
}
