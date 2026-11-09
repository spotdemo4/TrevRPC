package trevrpc

import (
	"context"
	"net/netip"
	"testing"
	"time"
)

type handlerContextTestKey struct{}

func TestNewHandlerContextCarriesInfo(t *testing.T) {
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), handlerContextTestKey{}, "v"), time.Hour)
	defer cancel()
	token := []byte("Bearer abc")
	addr := netip.MustParseAddrPort("192.0.2.1:443")
	ctx := NewHandlerContext(parent, HandlerContext{
		Service:    "example.auth.Auth",
		Method:     "Logout",
		Kind:       RPCKindServerStreaming,
		Metadata:   Metadata{"Authorization": token, "x-empty": {}},
		RemoteAddr: addr,
	})
	token[0] = 'X'

	info, ok := HandlerContextFromContext(ctx)
	if !ok {
		t.Fatal("HandlerContextFromContext found no handler context")
	}
	if info.Service != "example.auth.Auth" || info.Method != "Logout" || info.FullMethod != "/example.auth.Auth/Logout" || info.Kind != RPCKindServerStreaming || info.RemoteAddr != addr {
		t.Fatalf("HandlerContextFromContext = %+v", info)
	}
	if info.RequestDatagrams() != 0 || info.StreamCompressedRequests() != 0 || info.PlainCompressedRequests() != 0 {
		t.Fatal("the counters of a handler context made for a test are not zero")
	}
	metadata := RequestMetadataFromContext(ctx)
	if value, ok := metadata["x-empty"]; !ok || value != nil {
		t.Fatalf(`metadata["x-empty"] = %q, %v, want nil, true`, value, ok)
	}
	if got, err := BearerToken(metadata); err != nil || got != "abc" {
		t.Fatalf("BearerToken = %q, %v, want abc (the key normalized and the value copied)", got, err)
	}
	if ctx.Value(handlerContextTestKey{}) != "v" {
		t.Fatal("the handler context lost the values of its parent")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("the handler context lost the deadline of its parent")
	}
	cancel()
	if ctx.Err() == nil {
		t.Fatal("canceling the parent did not cancel the handler context")
	}
}

func TestNewHandlerContextDerivesTheMethodNames(t *testing.T) {
	info, _ := HandlerContextFromContext(NewHandlerContext(context.Background(), HandlerContext{FullMethod: "/example.Greeter/Echo"}))
	if info.Service != "example.Greeter" || info.Method != "Echo" || info.FullMethod != "/example.Greeter/Echo" {
		t.Fatalf("HandlerContextFromContext = %+v, want the service and method of the full method", info)
	}
	if metadata := RequestMetadataFromContext(NewHandlerContext(context.Background(), HandlerContext{})); metadata == nil || len(metadata) != 0 {
		t.Fatalf("RequestMetadataFromContext = %#v, want empty metadata", metadata)
	}
}

func TestNewHandlerContextRecordsResponseMetadata(t *testing.T) {
	ctx := NewHandlerContext(context.Background(), HandlerContext{FullMethod: "/example.Greeter/Echo"})
	// A context the handler derives sets the metadata of the same call.
	derived := context.WithValue(ctx, handlerContextTestKey{}, "v")
	if err := SetResponseHeader(derived, "X-H", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := SetResponseMetadata(derived, "x-t", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := SetResponseHeader(ctx, "bad key", nil); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("SetResponseHeader(bad key) = %v, want InvalidArgument", err)
	}

	headers := ResponseHeadersFromContext(ctx)
	requireHeaders(t, "headers", headers, "x-h", "1")
	requireHeaders(t, "trailing metadata", ResponseMetadataFromContext(ctx), "x-t", "2")
	headers["x-h"][0] = '9'
	requireHeaders(t, "headers after changing a copy", ResponseHeadersFromContext(ctx), "x-h", "1")
}

func TestResponseMetadataFromContextOutsideAHandlerIsEmpty(t *testing.T) {
	for name, metadata := range map[string]Metadata{
		"headers":           ResponseHeadersFromContext(context.Background()),
		"trailing metadata": ResponseMetadataFromContext(context.Background()),
	} {
		if metadata == nil || len(metadata) != 0 {
			t.Fatalf("%s = %#v, want empty metadata", name, metadata)
		}
	}
}

// A served handler reads back the response metadata it set, as does an
// interceptor once the handler has returned and the metadata has been sent.
func TestServedCallResponseMetadataIsReadable(t *testing.T) {
	server := NewServer()
	afterHandler := make(chan [2]Metadata, 1)
	server.SetInterceptors(InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
		return ctx, func(RPCFinished) {
			afterHandler <- [2]Metadata{ResponseHeadersFromContext(ctx), ResponseMetadataFromContext(ctx)}
		}, nil
	}))
	RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(ctx context.Context, _ *testMessage) (*testMessage, error) {
		if err := SetResponseHeader(ctx, "x-h", []byte("1")); err != nil {
			return nil, err
		}
		if err := SetResponseMetadata(ctx, "x-t", []byte("2")); err != nil {
			return nil, err
		}
		requireHeaders(t, "headers in the handler", ResponseHeadersFromContext(ctx), "x-h", "1")
		requireHeaders(t, "trailing metadata in the handler", ResponseMetadataFromContext(ctx), "x-t", "2")
		return &testMessage{Value: "ok"}, nil
	})

	serveTestStream(t, server, testRequestBytes(t, requestFor("Echo", RPCKindUnary), encodedTestMessage(t, "")))
	got := <-afterHandler
	requireHeaders(t, "headers after the handler", got[0], "x-h", "1")
	requireHeaders(t, "trailing metadata after the handler", got[1], "x-t", "2")
}
