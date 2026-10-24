package trevrpc

import "context"

type handlerContextKey struct{}

// callState is what a handler's context carries: the description of the
// request and the response metadata the handler sets.
type callState struct {
	info     HandlerContext
	response responseMetadata
	// compression is the call's negotiation result, nil without one.
	compression *callCompression
	// streams is the call's stream compression, nil without it.
	streams *serverStreams
}

// HandlerContext describes the request being handled. Handlers receive it
// through HandlerContextFromContext and authorizers as an argument. Metadata
// keys are lowercase.
type HandlerContext struct {
	// Service is the fully-qualified service name, such as
	// "example.greeter.Greeter".
	Service string
	// Method is the method name within the service, such as "SayHello".
	Method string
	// FullMethod is the method path "/<Service>/<Method>".
	FullMethod string
	Kind       RPCKind
	Metadata   Metadata

	// flow is the call's datagram flow, nil without one.
	flow *datagramFlow
	// streams is the call's stream compression, nil without it.
	streams *serverStreams
}

// HandlerContextFromContext returns TrevRPC request information attached to ctx.
func HandlerContextFromContext(ctx context.Context) (HandlerContext, bool) {
	state, ok := ctx.Value(handlerContextKey{}).(*callState)
	if !ok {
		return HandlerContext{}, false
	}
	return state.info, true
}

// responseMetadataFromContext returns the response metadata of the call ctx
// belongs to, or nil outside a handler.
func responseMetadataFromContext(ctx context.Context) *responseMetadata {
	state, ok := ctx.Value(handlerContextKey{}).(*callState)
	if !ok {
		return nil
	}
	return &state.response
}

// RequestMetadataFromContext returns normalized request metadata attached to ctx.
func RequestMetadataFromContext(ctx context.Context) Metadata {
	info, ok := HandlerContextFromContext(ctx)
	if !ok || info.Metadata == nil {
		return Metadata{}
	}

	return info.Metadata
}

func newHandlerContext(request *wireRequest) HandlerContext {
	service, method := request.serviceAndMethod()
	return HandlerContext{
		Service:    service,
		Method:     method,
		FullMethod: request.Method,
		Kind:       request.rpcKind(),
		Metadata:   cloneMetadata(request.Metadata),
		flow:       request.flow,
		streams:    request.streams,
	}
}

func contextWithHandlerContext(ctx context.Context, request *wireRequest) context.Context {
	if request == nil {
		return ctx
	}

	return context.WithValue(ctx, handlerContextKey{}, &callState{info: newHandlerContext(request), compression: request.compression, streams: request.streams})
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

// ownedMetadata turns metadata that was just decoded from a frame into the
// metadata to hand out: it is never nil, and an empty value is nil, as
// cloneMetadata makes it. The decoded values are private to the caller, so
// unlike cloneMetadata it does not copy them.
func ownedMetadata(metadata Metadata) Metadata {
	if metadata == nil {
		return Metadata{}
	}
	for key, value := range metadata {
		if len(value) == 0 {
			metadata[key] = nil
		}
	}
	return metadata
}

// StreamCompressedRequests returns how many of the request messages of this
// call have arrived stream-compressed so far (see StreamCompression), which is
// zero when the call did not enable it. It is meant for observing stream
// compression.
func (c HandlerContext) StreamCompressedRequests() int64 {
	if inflater := c.streams.requestInflater(); inflater != nil {
		return inflater.received.Load()
	}
	return 0
}
