package trevrpc

import "context"

type handlerContextKey struct{}

// HandlerContext contains request information exposed to generated handlers.
type HandlerContext struct {
	Service  string
	Method   string
	Kind     RpcKind
	Metadata Metadata
}

// HandlerContextFromContext returns TrevRPC request information attached to ctx.
func HandlerContextFromContext(ctx context.Context) (HandlerContext, bool) {
	info, ok := ctx.Value(handlerContextKey{}).(HandlerContext)
	return info, ok
}

// RequestMetadataFromContext returns normalized request metadata attached to ctx.
func RequestMetadataFromContext(ctx context.Context) Metadata {
	info, ok := HandlerContextFromContext(ctx)
	if !ok || info.Metadata == nil {
		return Metadata{}
	}

	return info.Metadata
}

func contextWithHandlerContext(ctx context.Context, request *RpcRequest) context.Context {
	if request == nil {
		return ctx
	}

	return context.WithValue(ctx, handlerContextKey{}, HandlerContext{
		Service:  request.Service,
		Method:   request.Method,
		Kind:     request.RPCKind(),
		Metadata: cloneMetadata(request.Metadata),
	})
}

func cloneMetadata(metadata Metadata) Metadata {
	if len(metadata) == 0 {
		return Metadata{}
	}

	clone := make(Metadata, len(metadata))
	for key, value := range metadata {
		clone[key] = append([]byte(nil), value...)
	}
	return clone
}
