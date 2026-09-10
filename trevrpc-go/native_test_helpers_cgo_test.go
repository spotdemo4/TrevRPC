//go:build cgo && linux && (amd64 || arm64)

package trevrpc

import (
	"context"
	"fmt"
	"io"
	"strings"
)

func authenticatedOptions() []CallOption {
	return []CallOption{
		WithTimeout(testTimeout),
		WithMetadata("authorization", []byte("Bearer "+testAuthToken)),
	}
}

const testServiceName = "example.Greeter"

func registerTestGreeter(server *Server) {
	RegisterUnary(server, testServiceName, "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello, " + request.Value}, nil
	})
	server.RouteStreaming(testServiceName, "LotsOfReplies", RpcKindServerStreaming, func(_ context.Context, body []byte, _ ByteStream) (ByteStream, error) {
		request := &testMessage{}
		if err := UnmarshalMessage(body, request); err != nil {
			return nil, err
		}
		return EncodeStream(FromSlice(
			&testMessage{Value: "hello, " + request.Value},
			&testMessage{Value: "goodbye, " + request.Value},
		)), nil
	})
	server.RouteStreaming(testServiceName, "LotsOfGreetings", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		decoded := DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })
		var values []string
		for {
			request, err := decoded.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			values = append(values, request.Value)
		}
		return SingleMessageStream(&testMessage{Value: strings.Join(values, ",")}), nil
	})
	server.RouteStreaming(testServiceName, "BidiHello", RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		return EncodeStream[*testMessage](&echoTestMessages{requests: DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })}), nil
	})
}

func runMixedQUICCallWithOptions(transport Transport, index int, options []CallOption) error {
	switch index % 4 {
	case 0:
		name := fmt.Sprintf("load-unary-%d", index)
		response, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, options...)
		if err != nil {
			return err
		}
		if response.Value != "hello, "+name {
			return fmt.Errorf("unexpected unary response %q", response.Value)
		}
	case 1:
		name := fmt.Sprintf("load-server-%d", index)
		responses, err := ServerStreaming(context.Background(), transport, testServiceName, "LotsOfReplies", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, options...)
		if err != nil {
			return err
		}
		messages := collectTestMessagesNoFatal(responses)
		if messages.err != nil {
			return messages.err
		}
		if !equalStrings(messages.values, []string{"hello, " + name, "goodbye, " + name}) {
			return fmt.Errorf("unexpected server stream responses %#v", messages.values)
		}
	case 2:
		response, err := runTestClientStreaming(context.Background(), transport, testServiceName, "LotsOfGreetings", []string{
			fmt.Sprintf("load-client-%d-a", index),
			fmt.Sprintf("load-client-%d-b", index),
		}, options...)
		if err != nil {
			return err
		}
		expected := fmt.Sprintf("load-client-%d-a,load-client-%d-b", index, index)
		if response.Value != expected {
			return fmt.Errorf("unexpected client stream response %q", response.Value)
		}
	default:
		responses, err := runTestBidiStreaming(context.Background(), transport, testServiceName, "BidiHello", []string{
			fmt.Sprintf("load-bidi-%d-a", index),
			fmt.Sprintf("load-bidi-%d-b", index),
		}, options...)
		if err != nil {
			return err
		}
		messages := collectTestMessagesNoFatal(responses)
		if messages.err != nil {
			return messages.err
		}
		expected := []string{
			fmt.Sprintf("echo, load-bidi-%d-a", index),
			fmt.Sprintf("echo, load-bidi-%d-b", index),
		}
		if !equalStrings(messages.values, expected) {
			return fmt.Errorf("unexpected bidi responses %#v", messages.values)
		}
	}
	return nil
}

type collectedMessages struct {
	values []string
	err    error
}

func collectTestMessagesNoFatal(stream MessageStream[*testMessage]) collectedMessages {
	var messages []string
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return collectedMessages{values: messages}
		}
		if err != nil {
			return collectedMessages{values: messages, err: err}
		}
		messages = append(messages, message.Value)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
