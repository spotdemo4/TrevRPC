package trevrpc_test

import (
	"context"
	"fmt"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// logout is a handler that reads request metadata and sets response metadata.
func logout(ctx context.Context) error {
	token, err := trevrpc.BearerToken(trevrpc.RequestMetadataFromContext(ctx))
	if err != nil {
		return err
	}
	return trevrpc.SetResponseMetadata(ctx, "x-ended-session", []byte(token))
}

// A test calls a handler directly with the context of a call that carries an
// authorization header, then reads back the metadata the handler set.
func ExampleNewHandlerContext() {
	ctx := trevrpc.NewHandlerContext(context.Background(), trevrpc.HandlerContext{
		FullMethod: "/example.auth.Auth/Logout",
		Metadata:   trevrpc.Metadata{"authorization": []byte("Bearer abc")},
	})
	if err := logout(ctx); err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", trevrpc.ResponseMetadataFromContext(ctx)["x-ended-session"])
	// Output: abc
}
