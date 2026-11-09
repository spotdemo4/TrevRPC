package trevrpc

import (
	"context"
	"errors"
	"io"
	"strings"
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
