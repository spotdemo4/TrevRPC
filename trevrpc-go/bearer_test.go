package trevrpc

import (
	"context"
	"errors"
	"testing"
)

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

// bearerMetadataTokens lists tokens for the b64token syntax BearerToken and
// BearerMetadata share.
var bearerMetadataTokens = []struct {
	name  string
	token string
	valid bool
}{
	{name: "letters", token: "abc", valid: true},
	{name: "every character", token: "abc.DEF-123_~+/", valid: true},
	{name: "padding", token: "abc.DEF-123_~+/==", valid: true},
	{name: "empty"},
	{name: "padding only", token: "=="},
	{name: "space", token: "to ken"},
	{name: "leading space", token: " token"},
	{name: "trailing space", token: "token "},
	{name: "newline", token: "token\n"},
	{name: "non-ASCII", token: "tökén"},
	{name: "padding inside", token: "a=b"},
	{name: "comma", token: "a,b"},
}

func TestBearerMetadata(t *testing.T) {
	for _, test := range bearerMetadataTokens {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := BearerMetadata(test.token)
			if !test.valid {
				status, ok := errors.AsType[*Status](err)
				if !ok || status.Code != CodeInvalidArgument || metadata != nil {
					t.Fatalf("BearerMetadata(%q) = %v, %v; want InvalidArgument", test.token, metadata, err)
				}
				return
			}
			if err != nil || len(metadata) != 1 || string(metadata["authorization"]) != "Bearer "+test.token {
				t.Fatalf("BearerMetadata(%q) = %q, %v", test.token, metadata, err)
			}
			// The server's parser accepts what the helper produces, and the
			// server's fixed-token authorizer accepts it too.
			if token, err := BearerToken(metadata); err != nil || token != test.token {
				t.Fatalf("BearerToken(BearerMetadata(%q)) = %q, %v", test.token, token, err)
			}
			if _, err := BearerAuthorizer(test.token).Authorize(context.Background(), HandlerContext{Metadata: metadata}); err != nil {
				t.Fatalf("BearerAuthorizer(%q) rejected BearerMetadata: %v", test.token, err)
			}
		})
	}
}

func TestBearerMetadataCallsReachTheAuthorizer(t *testing.T) {
	server := NewServer()
	server.SetAuthorizer(AuthorizerFunc(func(ctx context.Context, info HandlerContext) (context.Context, error) {
		token, err := BearerToken(info.Metadata)
		if err == nil && token != "abc.DEF==" {
			err = Unauthenticated("wrong token")
		}
		return ctx, err
	}))
	routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	metadata, err := BearerMetadata("abc.DEF==")
	if err != nil {
		t.Fatal(err)
	}
	request := newWireRequest("/service/method")
	request.Metadata = metadata
	if response := server.handleRequest(context.Background(), request); CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("response = %#v, want OK", response)
	}
}
