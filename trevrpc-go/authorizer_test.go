package trevrpc

import (
	"context"
	"errors"
	"slices"
	"testing"
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
