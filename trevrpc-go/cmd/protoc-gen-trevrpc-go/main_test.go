package main

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	descriptor "github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

func TestGenerateGreeterService(t *testing.T) {
	request := &plugin.CodeGeneratorRequest{
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

func TestRejectsUnknownOption(t *testing.T) {
	response := generate(&plugin.CodeGeneratorRequest{Parameter: proto.String("bogus=1")})
	if !strings.Contains(response.GetError(), "unknown trevrpc-go option") {
		t.Fatalf("unexpected error: %q", response.GetError())
	}
}
