package trevrpc

import (
	"context"
	"crypto/subtle"
)

// Authorizer authorizes a request after metadata and protocol validation. The
// request body is not available to it. A call the server refuses for
// optimistic stream compression is replayed by the client and authorized
// again (see StreamCompression), so authorizers must not have side effects
// that a second authorization of the same call would repeat: put those, such
// as counting calls against a rate limit, in an Interceptor, which runs once
// per handler invocation.
type Authorizer interface {
	// Authorize returns the context that interceptors and the handler get
	// when the request is authorized, and an error when it is not. The
	// context lets an authorizer pass what it verified, such as the
	// authenticated user, on to the handler, so that the handler need not
	// look it up again. An authorizer that has nothing to pass on returns
	// ctx. The returned context must be ctx or derive from it, as an
	// interceptor's must, and must not be nil when the error is nil. It is
	// ignored when the error is not nil. A returned *Status is sent to the
	// caller; any other error becomes Internal.
	//
	// A call the client replays after a refusal of optimistic stream
	// compression is authorized again, and its handler gets the context of
	// that second authorization.
	Authorize(ctx context.Context, info HandlerContext) (context.Context, error)
}

// AuthorizerFunc adapts a function to an Authorizer.
type AuthorizerFunc func(context.Context, HandlerContext) (context.Context, error)

// Authorize calls f.
func (f AuthorizerFunc) Authorize(ctx context.Context, info HandlerContext) (context.Context, error) {
	return f(ctx, info)
}

// chainedAuthorizers runs authorizers in order.
type chainedAuthorizers []Authorizer

// ChainAuthorizers returns an authorizer that runs authorizers in order and
// returns the first error, so a request is authorized only when every one of
// them authorizes it. Nil authorizers are skipped. The returned authorizer
// passes the context each authorizer in the chain returns on to the next, and
// returns the last.
func ChainAuthorizers(authorizers ...Authorizer) Authorizer {
	chain := make(chainedAuthorizers, 0, len(authorizers))
	for _, authorizer := range authorizers {
		if authorizer != nil {
			chain = append(chain, authorizer)
		}
	}
	return chain
}

// Authorize runs each authorizer, on the context the one before it returned,
// until one fails.
func (c chainedAuthorizers) Authorize(ctx context.Context, info HandlerContext) (context.Context, error) {
	next := ctx
	for _, authorizer := range c {
		var err error
		next, err = authorizer.Authorize(next, info)
		if err != nil {
			return ctx, err
		}
		// The next authorizer would get a nil context; the server checks
		// that the last one's derives from the call's.
		if next == nil {
			return ctx, foreignContextStatus("authorizer")
		}
	}
	return next, nil
}

// metadataValueAuthorizer authorizes requests with an exact metadata value.
type metadataValueAuthorizer struct {
	key   string
	value []byte
}

// NewMetadataValueAuthorizer creates an authorizer for an exact metadata key/value pair.
func NewMetadataValueAuthorizer(key string, value []byte) Authorizer {
	return metadataValueAuthorizer{key: NormalizeMetadataKey(key), value: append([]byte(nil), value...)}
}

// BearerAuthorizer creates an authorizer for one fixed bearer token in the
// authorization metadata. To check tokens that vary, such as per-user session
// tokens, parse them with BearerToken in an AuthorizerFunc. The token should
// be a b64token, so that a client can send it with BearerMetadata.
func BearerAuthorizer(token string) Authorizer {
	return NewMetadataValueAuthorizer("authorization", []byte("Bearer "+token))
}

// Authorize checks whether request metadata contains the expected value.
func (a metadataValueAuthorizer) Authorize(ctx context.Context, info HandlerContext) (context.Context, error) {
	if subtle.ConstantTimeCompare(info.Metadata[a.key], a.value) == 1 {
		return ctx, nil
	}

	return ctx, Unauthenticated("request is not authenticated")
}
