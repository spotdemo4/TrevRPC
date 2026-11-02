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
	// Authorize returns nil when the request is authorized. A returned *Status
	// is sent to the caller; any other error becomes Internal.
	Authorize(context.Context, HandlerContext) error
}

// AuthorizerFunc adapts a function to an Authorizer.
type AuthorizerFunc func(context.Context, HandlerContext) error

// Authorize calls f.
func (f AuthorizerFunc) Authorize(ctx context.Context, info HandlerContext) error {
	return f(ctx, info)
}

// chainedAuthorizers runs authorizers in order.
type chainedAuthorizers []Authorizer

// ChainAuthorizers returns an authorizer that runs authorizers in order and
// returns the first error, so a request is authorized only when every one of
// them authorizes it. Nil authorizers are skipped.
func ChainAuthorizers(authorizers ...Authorizer) Authorizer {
	chain := make(chainedAuthorizers, 0, len(authorizers))
	for _, authorizer := range authorizers {
		if authorizer != nil {
			chain = append(chain, authorizer)
		}
	}
	return chain
}

// Authorize runs each authorizer until one fails.
func (c chainedAuthorizers) Authorize(ctx context.Context, info HandlerContext) error {
	for _, authorizer := range c {
		if err := authorizer.Authorize(ctx, info); err != nil {
			return err
		}
	}
	return nil
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
// tokens, parse them with BearerToken in an AuthorizerFunc.
func BearerAuthorizer(token string) Authorizer {
	return NewMetadataValueAuthorizer("authorization", []byte("Bearer "+token))
}

// Authorize checks whether request metadata contains the expected value.
func (a metadataValueAuthorizer) Authorize(_ context.Context, info HandlerContext) error {
	if subtle.ConstantTimeCompare(info.Metadata[a.key], a.value) == 1 {
		return nil
	}

	return Unauthenticated("request is not authenticated")
}

// BearerToken returns the token of the request's authorization metadata, which
// must hold bearer credentials as RFC 6750 defines them: the case-insensitive
// scheme "Bearer", one or more spaces, and the token. It returns an
// Unauthenticated status when the metadata is missing or holds anything else,
// with a message that tells the two apart.
func BearerToken(metadata Metadata) (string, error) {
	value, ok := metadata["authorization"]
	if !ok {
		return "", Unauthenticated("missing bearer token")
	}
	const scheme = "bearer"
	if len(value) <= len(scheme) || !asciiEqualFold(value[:len(scheme)], scheme) || value[len(scheme)] != ' ' {
		return "", Unauthenticated("malformed bearer token")
	}
	token := value[len(scheme)+1:]
	for len(token) > 0 && token[0] == ' ' {
		token = token[1:]
	}
	if !isB64Token(token) {
		return "", Unauthenticated("malformed bearer token")
	}
	return string(token), nil
}

// asciiEqualFold reports whether value equals the lower-case ASCII lower,
// ignoring ASCII case.
func asciiEqualFold(value []byte, lower string) bool {
	for i := range len(lower) {
		c := value[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

// isB64Token reports whether token matches RFC 6750's b64token:
// 1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"=".
func isB64Token(token []byte) bool {
	end := len(token)
	for end > 0 && token[end-1] == '=' {
		end--
	}
	if end == 0 {
		return false
	}
	for _, c := range token[:end] {
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == '-', c == '.', c == '_', c == '~', c == '+', c == '/':
		default:
			return false
		}
	}
	return true
}
