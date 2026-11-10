package trevrpc_test

import (
	"context"
	"fmt"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// userKey is the context key of the user a session belongs to.
type userKey struct{}

// sessions maps session tokens to their users, as a database would.
var sessions = map[string]string{"abc": "ada"}

// An authorizer looks the request's session up to verify it, and passes the
// user it found on to the handler, which need not look the session up again.
func ExampleAuthorizerFunc() {
	authorizer := trevrpc.AuthorizerFunc(func(ctx context.Context, info trevrpc.HandlerContext) (context.Context, error) {
		token, err := trevrpc.BearerToken(info.Metadata)
		if err != nil {
			return nil, err
		}
		user, ok := sessions[token]
		if !ok {
			return nil, trevrpc.Unauthenticated("unknown session")
		}
		return context.WithValue(ctx, userKey{}, user), nil
	})
	server := trevrpc.NewServer()
	server.SetAuthorizer(authorizer)

	// A test of the authorizer calls it with the context of a call.
	ctx := trevrpc.NewHandlerContext(context.Background(), trevrpc.HandlerContext{
		FullMethod: "/example.auth.Auth/Whoami",
		Metadata:   trevrpc.Metadata{"authorization": []byte("Bearer abc")},
	})
	info, _ := trevrpc.HandlerContextFromContext(ctx)
	ctx, err := authorizer.Authorize(ctx, info)
	if err != nil {
		panic(err)
	}
	fmt.Println(ctx.Value(userKey{}))
	// Output: ada
}
