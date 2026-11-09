package trevrpc

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestChainAuthorizersRunsInOrderUntilOneFails(t *testing.T) {
	var ran []string
	authorizer := func(name string, err error) Authorizer {
		return AuthorizerFunc(func(context.Context, HandlerContext) error {
			ran = append(ran, name)
			return err
		})
	}

	chain := ChainAuthorizers(authorizer("limiter", nil), nil, authorizer("session", PermissionDenied("no")), authorizer("later", nil))
	err := chain.Authorize(context.Background(), HandlerContext{})
	if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodePermissionDenied {
		t.Fatalf("chain returned %v, want the session authorizer's status", err)
	}
	if want := []string{"limiter", "session"}; !slices.Equal(ran, want) {
		t.Fatalf("ran %v, want %v", ran, want)
	}

	ran = nil
	if err := ChainAuthorizers(authorizer("a", nil), authorizer("b", nil)).Authorize(context.Background(), HandlerContext{}); err != nil {
		t.Fatalf("chain of authorizing authorizers returned %v", err)
	}
	if want := []string{"a", "b"}; !slices.Equal(ran, want) {
		t.Fatalf("ran %v, want %v", ran, want)
	}
	if err := ChainAuthorizers().Authorize(context.Background(), HandlerContext{}); err != nil {
		t.Fatalf("empty chain returned %v", err)
	}
}

type chainValueKey struct{}

// appendingAuthorizer is a ContextAuthorizer that appends name to the
// chainValueKey value of the context.
func appendingAuthorizer(name string) ContextAuthorizer {
	return ContextAuthorizerFunc(func(ctx context.Context, _ HandlerContext) (context.Context, error) {
		value, _ := ctx.Value(chainValueKey{}).(string)
		return context.WithValue(ctx, chainValueKey{}, value+name), nil
	})
}

func TestChainAuthorizersPassesEachContextOnToTheNext(t *testing.T) {
	var sawB bool
	plain := AuthorizerFunc(func(ctx context.Context, _ HandlerContext) error {
		sawB = ctx.Value(chainValueKey{}) == "ab"
		return nil
	})
	chain, ok := ChainAuthorizers(appendingAuthorizer("a"), nil, appendingAuthorizer("b"), plain, appendingAuthorizer("c")).(ContextAuthorizer)
	if !ok {
		t.Fatal("ChainAuthorizers did not return a ContextAuthorizer")
	}
	ctx, err := chain.AuthorizeContext(context.Background(), HandlerContext{})
	if err != nil || ctx.Value(chainValueKey{}) != "abc" {
		t.Fatalf("chain returned a context with %v, %v; want abc", ctx.Value(chainValueKey{}), err)
	}
	if !sawB {
		t.Fatal("the plain authorizer did not get the context of the one before it")
	}
	if err := chain.Authorize(context.Background(), HandlerContext{}); err != nil {
		t.Fatalf("Authorize = %v", err)
	}

	denied, err := ChainAuthorizers(appendingAuthorizer("a"), AuthorizerFunc(func(context.Context, HandlerContext) error {
		return PermissionDenied("no")
	}), appendingAuthorizer("b")).(ContextAuthorizer).AuthorizeContext(context.Background(), HandlerContext{})
	if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodePermissionDenied || denied.Value(chainValueKey{}) != nil {
		t.Fatalf("denying chain returned %v with %v, want PermissionDenied with the context it was given", err, denied.Value(chainValueKey{}))
	}

	var reached bool
	err = ChainAuthorizers(ContextAuthorizerFunc(func(context.Context, HandlerContext) (context.Context, error) {
		return nil, nil
	}), AuthorizerFunc(func(context.Context, HandlerContext) error {
		reached = true
		return nil
	})).Authorize(context.Background(), HandlerContext{})
	if status, ok := errors.AsType[*Status](err); !ok || status.Code != CodeInternal || reached {
		t.Fatalf("chain with a nil context returned %v and reached the next authorizer: %v; want Internal before it", err, reached)
	}
}

func TestContextAuthorizerContextReachesTheHandler(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "no deadline"},
		// A deadline runs the authorizer on a goroutine of its own.
		{name: "deadline", timeout: rpcTestTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			server.SetAuthorizer(ChainAuthorizers(BearerAuthorizer("token"), appendingAuthorizer("user")))
			var interceptorSaw any
			server.SetInterceptors(InterceptorFunc(func(ctx context.Context, _ HandlerContext) (context.Context, func(RPCFinished), error) {
				interceptorSaw = ctx.Value(chainValueKey{})
				return ctx, nil, nil
			}))
			routeBytes(server, "/service/method", func(ctx context.Context, _ []byte) ([]byte, error) {
				user, _ := ctx.Value(chainValueKey{}).(string)
				return []byte(user), nil
			})
			request := newWireRequest("/service/method")
			request.TimeoutNanos = uint64(test.timeout)
			request.Metadata = Metadata{"authorization": []byte("Bearer token")}
			response := server.handleRequest(context.Background(), request)
			if CodeFromUint32(response.Status) != CodeOK || string(response.Body) != "user" || interceptorSaw != "user" {
				t.Fatalf("response = %#v, interceptor saw %v; want the authorizer's user", response, interceptorSaw)
			}
		})
	}
}

func TestContextAuthorizerMustDeriveTheCallContext(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "foreign", ctx: context.Background()},
		{name: "nil"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			server.SetAuthorizer(ContextAuthorizerFunc(func(context.Context, HandlerContext) (context.Context, error) {
				return test.ctx, nil
			}))
			var handled atomic.Int64
			routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) {
				handled.Add(1)
				return nil, nil
			})
			response := server.handleRequest(context.Background(), newWireRequest("/service/method"))
			if CodeFromUint32(response.Status) != CodeInternal || handled.Load() != 0 {
				t.Fatalf("response = %#v, handled %d times; want Internal before the handler", response, handled.Load())
			}
		})
	}
}

func TestBearerToken(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   *string
		token   string
		message string
	}{
		{name: "missing", message: "missing bearer token"},
		{name: "token", value: new("Bearer abc.DEF-123_~+/=="), token: "abc.DEF-123_~+/=="},
		{name: "scheme case", value: new("bEaReR token"), token: "token"},
		{name: "several spaces", value: new("Bearer   token"), token: "token"},
		{name: "empty", value: new(""), message: "malformed bearer token"},
		{name: "no scheme", value: new("token"), message: "malformed bearer token"},
		{name: "scheme only", value: new("Bearer"), message: "malformed bearer token"},
		{name: "empty token", value: new("Bearer "), message: "malformed bearer token"},
		{name: "other scheme", value: new("Basic dXNlcjpwYXNz"), message: "malformed bearer token"},
		{name: "scheme prefix", value: new("Bearertoken"), message: "malformed bearer token"},
		{name: "tab", value: new("Bearer\ttoken"), message: "malformed bearer token"},
		{name: "trailing space", value: new("Bearer token "), message: "malformed bearer token"},
		{name: "inner space", value: new("Bearer to ken"), message: "malformed bearer token"},
		{name: "padding only", value: new("Bearer =="), message: "malformed bearer token"},
		{name: "padding inside", value: new("Bearer a=b"), message: "malformed bearer token"},
		{name: "comma", value: new("Bearer a,b"), message: "malformed bearer token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := Metadata{}
			if test.value != nil {
				metadata["authorization"] = []byte(*test.value)
			}
			token, err := BearerToken(metadata)
			if test.message == "" {
				if err != nil || token != test.token {
					t.Fatalf("BearerToken = %q, %v; want %q", token, err, test.token)
				}
				return
			}
			status, ok := errors.AsType[*Status](err)
			if !ok || status.Code != CodeUnauthenticated || status.Message != test.message || token != "" {
				t.Fatalf("BearerToken = %q, %v; want Unauthenticated %q", token, err, test.message)
			}
		})
	}
}

func TestStatusConstructorsCoverEveryCode(t *testing.T) {
	constructors := map[Code]func(string) *Status{
		CodeCancelled:          Cancelled,
		CodeUnknown:            Unknown,
		CodeInvalidArgument:    InvalidArgument,
		CodeDeadlineExceeded:   DeadlineExceeded,
		CodeNotFound:           NotFound,
		CodeAlreadyExists:      AlreadyExists,
		CodePermissionDenied:   PermissionDenied,
		CodeResourceExhausted:  ResourceExhausted,
		CodeFailedPrecondition: FailedPrecondition,
		CodeAborted:            Aborted,
		CodeOutOfRange:         OutOfRange,
		CodeUnimplemented:      Unimplemented,
		CodeInternal:           Internal,
		CodeUnavailable:        Unavailable,
		CodeDataLoss:           DataLoss,
		CodeUnauthenticated:    Unauthenticated,
	}
	for code := CodeCancelled; code <= CodeUnauthenticated; code++ {
		constructor, ok := constructors[code]
		if !ok {
			t.Errorf("no constructor for %s", code)
			continue
		}
		if status := constructor("message"); status.Code != code || status.Message != "message" {
			t.Errorf("constructor for %s returned %v", code, status)
		}
	}
}
