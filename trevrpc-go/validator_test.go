package trevrpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// valueValidator rejects test messages whose value is "bad" with a status
// that has a detail, and those whose value is "plain" with a plain error. It
// records the types it was asked about.
type valueValidator struct {
	asked []protoreflect.FullName
	skip  bool
	err   error
}

func (v *valueValidator) ValidateFunc(desc protoreflect.MessageDescriptor) (func(proto.Message) error, error) {
	v.asked = append(v.asked, desc.FullName())
	if v.err != nil || v.skip {
		return nil, v.err
	}
	return func(message proto.Message) error {
		value := message.ProtoReflect().Get(desc.Fields().ByNumber(1)).String()
		switch value {
		case "bad":
			detail, err := NewDetail(&testMessage{Value: "why"})
			if err != nil {
				return err
			}
			return InvalidArgument("value is bad").WithDetails(detail)
		case "plain":
			return errors.New("value is plain")
		}
		return nil
	}, nil
}

func validatedServer(validator Validator, handled *int) *Server {
	server := NewServer()
	RegisterUnary(server, testPath("Echo"), newTestMessage, func(_ context.Context, request *testMessage) (*testMessage, error) {
		*handled++
		return request, nil
	})
	RegisterClientStreaming(server, testPath("Concat"), newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
		var values []string
		for {
			request, err := requests.Recv()
			if errors.Is(err, io.EOF) {
				return &testMessage{Value: strings.Join(values, ",")}, nil
			}
			if err != nil {
				return nil, err
			}
			*handled++
			values = append(values, request.Value)
		}
	})
	server.SetValidator(validator)
	return server
}

func TestValidatorRejectsRequestsBeforeTheHandler(t *testing.T) {
	var handled int
	server := validatedServer(&valueValidator{}, &handled)

	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "bad")))
	status := requireStatusFrame(t, frames, CodeInvalidArgument).statusValue()
	var detail testMessage
	if status.Message != "value is bad" || handled != 0 {
		t.Fatalf("status = %v, handled = %d; want the validator's status before the handler", status, handled)
	}
	if found, err := status.FindDetail(&detail); !found || err != nil || detail.Value != "why" {
		t.Fatalf("FindDetail = %v, %v, %q; want the validator's detail", found, err, detail.Value)
	}

	_, frames = serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "plain")))
	if status := requireStatusFrame(t, frames, CodeInvalidArgument).statusValue(); status.Message != "value is plain" {
		t.Fatalf("plain error status = %v, want InvalidArgument with its text", status)
	}

	_, frames = serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "good")))
	if len(frames) != 2 || frames[1].Kind != frameTypeStatus || CodeFromUint32(frames[1].Status) != CodeOK || handled != 1 {
		t.Fatalf("frames = %#v, handled = %d; want the valid request handled", frames, handled)
	}
}

func TestValidatorFailsTheStreamedMessageThatBreaksItsRules(t *testing.T) {
	var handled int
	server := validatedServer(&valueValidator{}, &handled)

	request := requestFor("Concat", RPCKindClientStreaming)
	_, frames := serveTestStream(t, server, testRequestBytes(t, request, encodedTestMessage(t, "a"), encodedTestMessage(t, "bad"), encodedTestMessage(t, "c")))
	if status := requireStatusFrame(t, frames, CodeInvalidArgument).statusValue(); status.Message != "value is bad" || handled != 1 {
		t.Fatalf("status = %v, handled = %d; want the second message rejected", status, handled)
	}
}

func TestValidatorSkipsTypesWithoutRules(t *testing.T) {
	var handled int
	validator := &valueValidator{skip: true}
	server := validatedServer(validator, &handled)

	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "bad")))
	if len(frames) != 2 || CodeFromUint32(frames[1].Status) != CodeOK || handled != 1 {
		t.Fatalf("frames = %#v, handled = %d; want the request handled unchecked", frames, handled)
	}
	if len(validator.asked) != 2 {
		t.Fatalf("validator asked about %v, want one type per route", validator.asked)
	}
}

func TestValidatorThatCannotBindFailsClosed(t *testing.T) {
	var handled int
	server := validatedServer(&valueValidator{err: errors.New("bad rule")}, &handled)

	if err := server.freeze().validatorErr; err == nil || !strings.Contains(err.Error(), "bad rule") {
		t.Fatalf("validatorErr = %v, want the binding error", err)
	}
	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "good")))
	requireStatusFrame(t, frames, CodeInternal)
	if handled != 0 {
		t.Fatalf("handled = %d, want no call served", handled)
	}
	if _, err := Listen("127.0.0.1:0", server, ListenOptions{}); err == nil || !strings.Contains(err.Error(), "bad rule") {
		t.Fatalf("Listen = %v, want the binding error", err)
	}
}

func TestMessageValidatorReturnsTheStatusTheServerWouldSend(t *testing.T) {
	validator := &valueValidator{}
	messages := NewMessageValidator(validator)

	err := messages.Validate(protoMessageV2(&testMessage{Value: "bad"}))
	requireInvalidArgument(t, err, "value is bad")

	err = messages.Validate(protoMessageV2(&testMessage{Value: "plain"}))
	if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodeInvalidArgument || status.Message != "value is plain" {
		t.Fatalf("Validate = %v, want a plain error as InvalidArgument", err)
	}
	if err := messages.Validate(protoMessageV2(&testMessage{Value: "good"})); err != nil {
		t.Fatalf("Validate = %v, want a valid message to pass", err)
	}
}

func TestMessageValidatorCompilesEachTypeOnce(t *testing.T) {
	validator := &valueValidator{}
	messages := NewMessageValidator(validator)
	for range 3 {
		_ = messages.Validate(protoMessageV2(&testMessage{Value: "bad"}))
	}
	if len(validator.asked) != 1 {
		t.Fatalf("validator asked about %v, want the type compiled once", validator.asked)
	}
}

func TestMessageValidatorPassesTypesWithoutRules(t *testing.T) {
	validator := &valueValidator{skip: true}
	messages := NewMessageValidator(validator)
	for range 2 {
		if err := messages.Validate(protoMessageV2(&testMessage{Value: "bad"})); err != nil {
			t.Fatalf("Validate = %v, want a type without rules to pass", err)
		}
	}
	if len(validator.asked) != 1 {
		t.Fatalf("validator asked about %v, want the absence of rules cached", validator.asked)
	}
}

func TestMessageValidatorFailsClosedWhenRulesCannotCompile(t *testing.T) {
	messages := NewMessageValidator(&valueValidator{err: errors.New("bad rule")})
	for range 2 {
		err := messages.Validate(protoMessageV2(&testMessage{Value: "good"}))
		if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodeInternal || !strings.Contains(status.Message, "bad rule") {
			t.Fatalf("Validate = %v, want Internal naming the rule error", err)
		}
	}
}

func requireInvalidArgument(t *testing.T, err error, message string) {
	t.Helper()
	status, ok := errors.AsType[*Status](err)
	if !ok || status.Code != CodeInvalidArgument || status.Message != message {
		t.Fatalf("error = %v, want InvalidArgument %q", err, message)
	}
	var detail testMessage
	if found, err := status.FindDetail(&detail); !found || err != nil || detail.Value != "why" {
		t.Fatalf("FindDetail = %v, %v, %q; want the validator's detail", found, err, detail.Value)
	}
}

// clientValidationServer serves Echo, Count, Concat and Chat with an
// authorizer and handlers that count what reaches them, and no validator, so
// that only a client can reject a message.
func clientValidationServer(t *testing.T, calls, handled *atomic.Int32) (testPKI, *Listener) {
	t.Helper()
	pki := newTestPKI(t)
	server := NewServer()
	server.SetAuthorizer(AuthorizerFunc(func(ctx context.Context, _ HandlerContext) (context.Context, error) {
		calls.Add(1)
		return ctx, nil
	}))
	RegisterUnary(server, testPath("Echo"), newTestMessage, func(_ context.Context, request *testMessage) (*testMessage, error) {
		handled.Add(1)
		return request, nil
	})
	RegisterServerStreaming(server, testPath("Count"), newTestMessage, func(_ context.Context, request *testMessage) (MessageStream[*testMessage], error) {
		handled.Add(1)
		return StreamOf(request), nil
	})
	RegisterClientStreaming(server, testPath("Concat"), newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
		var values []string
		for request, err := range Messages(requests) {
			if err != nil {
				return nil, err
			}
			handled.Add(1)
			values = append(values, request.Value)
		}
		return &testMessage{Value: strings.Join(values, ",")}, nil
	})
	RegisterBidirectionalStreaming(server, testPath("Chat"), newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (MessageStream[*testMessage], error) {
		return &upperStream{requests: requests}, nil
	})
	return pki, serveTestListener(t, server, ListenOptions{TLSConfig: pki.server})
}

func dialValidating(t *testing.T, pki testPKI, listener *Listener, validator Validator) *Channel {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := testDial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client, Validator: validator})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	closeOnCleanup(t, channel)
	return channel
}

func TestClientRejectsInvalidRequestsWithoutContactingTheServer(t *testing.T) {
	var calls, handled atomic.Int32
	pki, listener := clientValidationServer(t, &calls, &handled)
	channel := dialValidating(t, pki, listener, &valueValidator{})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()

	_, err := Unary(ctx, channel, testPath("Echo"), &testMessage{Value: "bad"}, newTestMessage)
	requireInvalidArgument(t, err, "value is bad")
	_, err = ServerStreaming(ctx, channel, testPath("Count"), &testMessage{Value: "bad"}, newTestMessage)
	requireInvalidArgument(t, err, "value is bad")
	if calls.Load() != 0 || handled.Load() != 0 {
		t.Fatalf("server saw %d calls and handled %d messages, want the requests rejected locally", calls.Load(), handled.Load())
	}

	response, err := Unary(ctx, channel, testPath("Echo"), &testMessage{Value: "good"}, newTestMessage)
	if err != nil || response.Value != "good" || calls.Load() != 1 || handled.Load() != 1 {
		t.Fatalf("Unary = %v, %v; calls = %d, handled = %d; want the valid request served", response, err, calls.Load(), handled.Load())
	}
}

func TestClientRejectsInvalidStreamedMessagesAndKeepsTheStreamOpen(t *testing.T) {
	var calls, handled atomic.Int32
	pki, listener := clientValidationServer(t, &calls, &handled)
	channel := dialValidating(t, pki, listener, &valueValidator{})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()

	upload, err := ClientStreaming[*testMessage](ctx, channel, testPath("Concat"), newTestMessage)
	if err != nil {
		t.Fatal(err)
	}
	if err := upload.Send(&testMessage{Value: "a"}); err != nil {
		t.Fatal(err)
	}
	requireInvalidArgument(t, upload.Send(&testMessage{Value: "bad"}), "value is bad")
	if err := upload.Send(&testMessage{Value: "b"}); err != nil {
		t.Fatalf("Send after a rejected message = %v, want the stream open", err)
	}
	response, err := upload.CloseAndRecv()
	if err != nil || response.Value != "a,b" || handled.Load() != 2 {
		t.Fatalf("CloseAndRecv = %v, %v; handled = %d; want the valid messages only", response, err, handled.Load())
	}

	chat, err := BidirectionalStreaming[*testMessage](ctx, channel, testPath("Chat"), newTestMessage)
	if err != nil {
		t.Fatal(err)
	}
	requireInvalidArgument(t, chat.Send(&testMessage{Value: "bad"}), "value is bad")
	if err := chat.Send(&testMessage{Value: "hi"}); err != nil {
		t.Fatal(err)
	}
	if reply, err := chat.Recv(); err != nil || reply.Value != "HI" {
		t.Fatalf("Recv = %v, %v; want the valid message answered", reply, err)
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
}

func TestClientWithoutValidatorSendsRequestsUnchecked(t *testing.T) {
	var calls, handled atomic.Int32
	pki, listener := clientValidationServer(t, &calls, &handled)
	channel := dialValidating(t, pki, listener, nil)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()

	if response, err := Unary(ctx, channel, testPath("Echo"), &testMessage{Value: "bad"}, newTestMessage); err != nil || response.Value != "bad" {
		t.Fatalf("Unary = %v, %v; want the request sent", response, err)
	}
}
