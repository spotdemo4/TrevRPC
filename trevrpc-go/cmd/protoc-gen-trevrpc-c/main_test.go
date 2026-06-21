package main

import (
	"strings"
	"testing"

	descriptor "google.golang.org/protobuf/types/descriptorpb"
	plugin "google.golang.org/protobuf/types/pluginpb"
)

func TestGenerateGreeterCService(t *testing.T) {
	response := generate(greeterRequest())
	if response.GetError() != "" {
		t.Fatalf("generator returned error: %s", response.GetError())
	}
	if len(response.File) != 2 {
		t.Fatalf("expected header and source files, got %d", len(response.File))
	}

	header := response.File[0]
	if header.GetName() != "hello/v1/greeter.trevrpc.h" {
		t.Fatalf("unexpected header output name: %s", header.GetName())
	}
	source := response.File[1]
	if source.GetName() != "hello/v1/greeter.trevrpc.c" {
		t.Fatalf("unexpected source output name: %s", source.GetName())
	}

	headerContent := header.GetContent()
	for _, want := range []string{
		"typedef struct hello_v1_greeter_server",
		"int (*say_hello)(void* user_data, const trevrpc_call_context* context, const Hello__V1__HelloRequest* request, Hello__V1__HelloReply** response);",
		"int (*lots_of_replies)(void* user_data, const trevrpc_call_context* context, const Hello__V1__HelloRequest* request, trevrpc_stream* stream);",
		"int (*lots_of_greetings)(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream, Hello__V1__HelloReply** response);",
		"int (*bidi_hello)(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream);",
		"int hello_v1_greeter_send_hello_v1_hello_request",
		"int hello_v1_greeter_recv_hello_v1_hello_request",
		"int hello_v1_greeter_send_hello_v1_hello_reply",
		"int hello_v1_greeter_recv_hello_v1_hello_reply",
		"int hello_v1_greeter_say_hello",
		"int hello_v1_greeter_lots_of_replies",
		"int hello_v1_greeter_lots_of_greetings_start",
		"int hello_v1_greeter_bidi_hello_start",
		"int hello_v1_greeter_register",
	} {
		if !strings.Contains(headerContent, want) {
			t.Fatalf("generated header missing %q:\n%s", want, headerContent)
		}
	}

	sourceContent := source.GetContent()
	for _, want := range []string{
		"trevrpc_client_call_unary(client, \"hello.v1.Greeter\", \"SayHello\"",
		"trevrpc_client_start_stream(client, \"hello.v1.Greeter\", \"LotsOfReplies\", TREVRPC_RPC_KIND_SERVER_STREAMING",
		"trevrpc_client_start_stream(client, \"hello.v1.Greeter\", \"LotsOfGreetings\", TREVRPC_RPC_KIND_CLIENT_STREAMING",
		"trevrpc_client_start_stream(client, \"hello.v1.Greeter\", \"BidiHello\", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING",
		"trevrpc_server_register_unary(server, \"hello.v1.Greeter\", \"SayHello\"",
		"trevrpc_server_register_streaming(server, \"hello.v1.Greeter\", \"LotsOfReplies\", TREVRPC_RPC_KIND_SERVER_STREAMING",
		"trevrpc_server_register_streaming(server, \"hello.v1.Greeter\", \"LotsOfGreetings\", TREVRPC_RPC_KIND_CLIENT_STREAMING",
		"trevrpc_server_register_streaming(server, \"hello.v1.Greeter\", \"BidiHello\", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING",
	} {
		if !strings.Contains(sourceContent, want) {
			t.Fatalf("generated source missing %q:\n%s", want, sourceContent)
		}
	}
}

func greeterRequest() *plugin.CodeGeneratorRequest {
	return &plugin.CodeGeneratorRequest{
		FileToGenerate: []string{"hello/v1/greeter.proto"},
		ProtoFile: []*descriptor.FileDescriptorProto{
			{
				Name:    new("hello/v1/greeter.proto"),
				Package: new("hello.v1"),
				MessageType: []*descriptor.DescriptorProto{
					{Name: new("HelloRequest")},
					{Name: new("HelloReply")},
				},
				Service: []*descriptor.ServiceDescriptorProto{
					{
						Name: new("Greeter"),
						Method: []*descriptor.MethodDescriptorProto{
							{
								Name:       new("SayHello"),
								InputType:  new(".hello.v1.HelloRequest"),
								OutputType: new(".hello.v1.HelloReply"),
							},
							{
								Name:            new("LotsOfReplies"),
								InputType:       new(".hello.v1.HelloRequest"),
								OutputType:      new(".hello.v1.HelloReply"),
								ServerStreaming: new(true),
							},
							{
								Name:            new("LotsOfGreetings"),
								InputType:       new(".hello.v1.HelloRequest"),
								OutputType:      new(".hello.v1.HelloReply"),
								ClientStreaming: new(true),
							},
							{
								Name:            new("BidiHello"),
								InputType:       new(".hello.v1.HelloRequest"),
								OutputType:      new(".hello.v1.HelloReply"),
								ClientStreaming: new(true),
								ServerStreaming: new(true),
							},
						},
					},
				},
			},
		},
	}
}

func TestRejectsUnknownOption(t *testing.T) {
	response := generate(&plugin.CodeGeneratorRequest{Parameter: new("bogus=1")})
	if !strings.Contains(response.GetError(), "unknown trevrpc-c option") {
		t.Fatalf("unexpected error: %q", response.GetError())
	}
}
