package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestResponseMetadataOutsideAHandlerFails(t *testing.T) {
	if err := SetResponseHeader(context.Background(), "x-h", nil); !errors.Is(err, ErrNoHandlerContext) {
		t.Fatalf("SetResponseHeader = %v, want ErrNoHandlerContext", err)
	}
	if err := SetResponseMetadata(context.Background(), "x-t", nil); !errors.Is(err, ErrNoHandlerContext) {
		t.Fatalf("SetResponseMetadata = %v, want ErrNoHandlerContext", err)
	}
}

func TestUnaryResponseMetadataIsValidatedNormalizedAndCopied(t *testing.T) {
	server := NewServer()
	RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
		for _, key := range []string{"bad key", "trevrpc-reserved", ""} {
			if err := SetResponseHeader(ctx, key, nil); StatusFromError(err).Code != CodeInvalidArgument {
				t.Errorf("SetResponseHeader(%q) = %v, want InvalidArgument", key, err)
			}
		}
		value := []byte("1")
		if err := SetResponseHeader(ctx, "X-H", value); err != nil {
			return nil, err
		}
		value[0] = '2'
		if err := SetResponseMetadata(ctx, "x-t", []byte("3")); err != nil {
			return nil, err
		}
		return &testMessage{Value: "ok"}, nil
	})

	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "")))
	if len(frames) != 3 || frames[0].Kind != frameTypeHeaders || frames[2].Kind != frameTypeStatus {
		t.Fatalf("frames = %#v, want HEADERS, MESSAGE, STATUS", frames)
	}
	requireHeaders(t, "headers", frames[0].Headers, "x-h", "1")
	requireHeaders(t, "trailing metadata", frames[2].Metadata, "x-t", "3")
}

func TestFailedCallSendsTheMetadataOfItsStatus(t *testing.T) {
	server := NewServer()
	RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
		if err := SetResponseHeader(ctx, "x-h", []byte("1")); err != nil {
			return nil, err
		}
		if err := SetResponseMetadata(ctx, "x-t", []byte("1")); err != nil {
			return nil, err
		}
		return nil, NotFound("gone").WithMetadata(Metadata{"x-status": []byte("2")})
	})

	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "")))
	status := requireStatusFrame(t, frames, CodeNotFound)
	requireHeaders(t, "trailing metadata", status.Metadata, "x-status", "2")
}

// metadataStream sets response metadata while the server pulls it: a header,
// too late, with its first message, and trailing metadata before and after
// it ends.
type metadataStream struct {
	ctx    context.Context
	sent   bool
	errors chan error
}

func (s *metadataStream) Recv() (*testMessage, error) {
	if !s.sent {
		s.sent = true
		s.errors <- SetResponseHeader(s.ctx, "x-late", nil)
		return &testMessage{Value: "a"}, nil
	}
	if err := SetResponseMetadata(s.ctx, "x-t", []byte("1")); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *metadataStream) Close() error {
	s.errors <- SetResponseMetadata(s.ctx, "x-after", nil)
	return nil
}

func TestStreamingResponseMetadataIsTakenWhenItIsSent(t *testing.T) {
	errs := make(chan error, 2)
	server := NewServer()
	RegisterServerStreaming(server, "/example.Greeter/Count", newTestMessage, func(ctx context.Context, _ *testMessage) (MessageStream[*testMessage], error) {
		if err := SetResponseHeader(ctx, "x-h", []byte("1")); err != nil {
			return nil, err
		}
		return &metadataStream{ctx: ctx, errors: errs}, nil
	})

	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Count", RPCKindServerStreaming), encodedTestMessage(t, "")))
	if len(frames) != 3 || frames[0].Kind != frameTypeHeaders || frames[2].Kind != frameTypeStatus || CodeFromUint32(frames[2].Status) != CodeOK {
		t.Fatalf("frames = %#v, want HEADERS, MESSAGE, OK STATUS", frames)
	}
	requireHeaders(t, "headers", frames[0].Headers, "x-h", "1")
	requireHeaders(t, "trailing metadata", frames[2].Metadata, "x-t", "1")
	if err := <-errs; !errors.Is(err, ErrResponseHeadersSent) {
		t.Fatalf("header set after the handler returned = %v, want ErrResponseHeadersSent", err)
	}
	if err := <-errs; !errors.Is(err, ErrResponseMetadataSent) {
		t.Fatalf("metadata set after the stream ended = %v, want ErrResponseMetadataSent", err)
	}
}

func TestFailedCallSendsTheDetailsOfItsStatus(t *testing.T) {
	detail, err := NewDetail(&testMessage{Value: "why"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(context.Context, *testMessage) (*testMessage, error) {
		return nil, InvalidArgument("bad").WithDetails(detail)
	})

	_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "")))
	status := requireStatusFrame(t, frames, CodeInvalidArgument).statusValue()
	var got testMessage
	if found, err := status.FindDetail(&got); !found || err != nil || got.Value != "why" {
		t.Fatalf("FindDetail = %v, %v, %q; want the handler's detail", found, err, got.Value)
	}
}

func TestStatusDetails(t *testing.T) {
	detail, err := NewDetail(&testMessage{Value: "why"})
	if err != nil {
		t.Fatal(err)
	}
	status := InvalidArgument("bad").WithMetadata(Metadata{"x-t": []byte("1")}).WithDetails(detail)
	detail.Value[0] ^= 0xff
	if got := status.WithMetadata(nil); len(got.Details) != 1 || got.Details[0].Value[0] == detail.Value[0] {
		t.Fatalf("WithMetadata details = %+v, want an unchanged copy", got.Details)
	}
	if len(status.Metadata) != 1 {
		t.Fatalf("WithDetails metadata = %v, want it kept", status.Metadata)
	}
	var none testMessage
	if found, err := OK().FindDetail(&none); found || err != nil {
		t.Fatalf("FindDetail without details = %v, %v", found, err)
	}
	if err := (Detail{Type: "other.Type"}).UnmarshalTo(&none); err == nil {
		t.Fatal("UnmarshalTo of another type succeeded")
	}
}

func TestStatusTooLargeForAFrameKeepsItsCode(t *testing.T) {
	small := bytes.Repeat([]byte{'x'}, 64)
	for _, tc := range []struct {
		name        string
		status      *Status
		wantMessage string
		wantDetails int
		wantMeta    bool
	}{
		{"details dropped", InvalidArgument("bad").WithMetadata(Metadata{"x-t": small}).WithDetails(Detail{Type: "x.Y", Value: bytes.Repeat([]byte{1}, 4096)}), "bad", 0, true},
		{"message cut", InvalidArgument(strings.Repeat("é", 2048)).WithMetadata(Metadata{"x-t": small}), strings.Repeat("é", (1024-statusOverhead)/2), 0, false},
		{"fits", InvalidArgument("bad").WithDetails(Detail{Type: "x.Y", Value: small}), "bad", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer()
			options := server.Options()
			options.MaxFrameSize = 1024
			server.SetOptions(options)
			RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(context.Context, *testMessage) (*testMessage, error) {
				return nil, tc.status
			})

			_, frames := serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "")))
			status := requireStatusFrame(t, frames, CodeInvalidArgument).statusValue()
			if status.Message != tc.wantMessage || len(status.Details) != tc.wantDetails || (len(status.Metadata) > 0) != tc.wantMeta {
				t.Fatalf("status = %q with %d details and metadata %v", status.Message, len(status.Details), status.Metadata)
			}
		})
	}
}
