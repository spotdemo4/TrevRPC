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
)

func TestGenerateGreeterService(t *testing.T) {
	request := greeterRequest()

	response := generate(request)
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
		"func NewGreeterClient",
		"func RegisterGreeterServer",
		"trevrpc.Unary[*HelloRequest, *HelloReply]",
		"trevrpc.ServerStreaming[*HelloRequest, *HelloReply]",
		"trevrpc.ClientStreaming[*HelloRequest, *HelloReply]",
		"trevrpc.BidirectionalStreaming[*HelloRequest, *HelloReply]",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated content missing %q:\n%s", want, content)
		}
	}

	if _, err := parser.ParseFile(token.NewFileSet(), generated.GetName(), content, parser.AllErrors); err != nil {
		t.Fatalf("generated Go is not syntactically valid: %v\n%s", err, content)
	}
}

func TestGeneratedGreeterServiceCompiles(t *testing.T) {
	response := generate(greeterRequest())
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	if len(response.File) != 1 {
		t.Fatalf("expected one generated file, got %d", len(response.File))
	}

	runtimeRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve runtime root: %v", err)
	}
	tempDir, err := os.MkdirTemp(runtimeRoot, "generated-compile-")
	if err != nil {
		t.Fatalf("create generated compile dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	writeFile(t, filepath.Join(tempDir, "messages.go"), `package hellov1

type HelloRequest struct {
	Name string `+"`"+`protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`+"`"+`
}

func (m *HelloRequest) Reset()         { *m = HelloRequest{} }
func (m *HelloRequest) String() string { return m.Name }
func (*HelloRequest) ProtoMessage()    {}

type HelloReply struct {
	Message string `+"`"+`protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`+"`"+`
}

func (m *HelloReply) Reset()         { *m = HelloReply{} }
func (m *HelloReply) String() string { return m.Message }
func (*HelloReply) ProtoMessage()    {}
`)
	writeFile(t, filepath.Join(tempDir, "greeter.trevrpc.go"), response.File[0].GetContent())
	writeFile(t, filepath.Join(tempDir, "service_test.go"), `package hellov1

import (
	"context"
	"io"
	"testing"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type service struct{}

func (service) SayHello(context.Context, *HelloRequest) (*HelloReply, error) {
	return &HelloReply{}, nil
}

func (service) LotsOfReplies(context.Context, *HelloRequest) (trevrpc.MessageStream[*HelloReply], error) {
	return trevrpc.FromSlice(&HelloReply{}), nil
}

func (service) LotsOfGreetings(_ context.Context, requests trevrpc.MessageStream[*HelloRequest]) (*HelloReply, error) {
	for {
		_, err := requests.Recv()
		if err == io.EOF {
			return &HelloReply{}, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (service) BidiHello(context.Context, trevrpc.MessageStream[*HelloRequest]) (trevrpc.MessageStream[*HelloReply], error) {
	return trevrpc.FromSlice(&HelloReply{}), nil
}

func TestGeneratedServiceCompiles(t *testing.T) {
	server := trevrpc.NewServer()
	RegisterGreeterServer(server, service{})
}
`)

	relativeDir, err := filepath.Rel(runtimeRoot, tempDir)
	if err != nil {
		t.Fatalf("resolve generated compile dir: %v", err)
	}

	command := exec.Command("go", "test", "./"+filepath.ToSlash(relativeDir))
	command.Dir = runtimeRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated service should compile: %v\n%s", err, output)
	}
}

func greeterRequest() *plugin.CodeGeneratorRequest {
	return &plugin.CodeGeneratorRequest{
		FileToGenerate: []string{"hello/v1/greeter.proto"},
		ProtoFile: []*descriptor.FileDescriptorProto{
			{
				Name:    proto.String("hello/v1/greeter.proto"),
				Package: proto.String("hello.v1"),
				Options: &descriptor.FileOptions{GoPackage: proto.String("example.com/hello/v1;hellov1")},
				MessageType: []*descriptor.DescriptorProto{
					{Name: proto.String("HelloRequest")},
					{Name: proto.String("HelloReply")},
				},
				Service: []*descriptor.ServiceDescriptorProto{
					{
						Name: proto.String("Greeter"),
						Method: []*descriptor.MethodDescriptorProto{
							{
								Name:       proto.String("SayHello"),
								InputType:  proto.String(".hello.v1.HelloRequest"),
								OutputType: proto.String(".hello.v1.HelloReply"),
							},
							{
								Name:            proto.String("LotsOfReplies"),
								InputType:       proto.String(".hello.v1.HelloRequest"),
								OutputType:      proto.String(".hello.v1.HelloReply"),
								ServerStreaming: proto.Bool(true),
							},
							{
								Name:            proto.String("LotsOfGreetings"),
								InputType:       proto.String(".hello.v1.HelloRequest"),
								OutputType:      proto.String(".hello.v1.HelloReply"),
								ClientStreaming: proto.Bool(true),
							},
							{
								Name:            proto.String("BidiHello"),
								InputType:       proto.String(".hello.v1.HelloRequest"),
								OutputType:      proto.String(".hello.v1.HelloReply"),
								ClientStreaming: proto.Bool(true),
								ServerStreaming: proto.Bool(true),
							},
						},
					},
				},
			},
		},
	}
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestRejectsUnknownOption(t *testing.T) {
	response := generate(&plugin.CodeGeneratorRequest{Parameter: proto.String("bogus=1")})
	if !strings.Contains(response.GetError(), "unknown trevrpc-go option") {
		t.Fatalf("unexpected error: %q", response.GetError())
	}
}
