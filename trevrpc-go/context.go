package trevrpc

import (
	"context"
	"net/netip"
	"sync/atomic"
)

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
	// request is the request the state describes.
	request *wireRequest
	// cancel cancels the context requestLifetimeContext attached the state
	// to, nil for one that requestContext built.
	cancel context.CancelFunc
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
	// RemoteAddr is the address of the client: the source address of a QUIC
	// connection, or of a TCP connection unless it came from a trusted proxy
	// (see ListenOptions.TrustedProxies), whose PROXY header then names the
	// client. IPv4 clients have IPv4 addresses on every transport. It is the
	// zero AddrPort when the transport does not know it.
	RemoteAddr netip.AddrPort

	// flow is the call's datagram flow, nil without one.
	flow *datagramFlow
	// streams is the call's stream compression, nil without it.
	streams *serverStreams
	// plainRequests counts the request messages that arrived compressed
	// without a dictionary, nil on a server without compression.
	plainRequests *atomic.Int64
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
		// Decoded for this call, connection metadata included, so not copied.
		Metadata:      ownedMetadata(request.Metadata),
		RemoteAddr:    request.remoteAddr,
		flow:          request.flow,
		streams:       request.streams,
		plainRequests: request.plainRequests,
	}
}

func contextWithHandlerContext(ctx context.Context, request *wireRequest) context.Context {
	if request == nil {
		return ctx
	}

	return &handlerCallContext{Context: ctx, state: &callState{info: newHandlerContext(request), compression: request.compression, streams: request.streams, request: request}, serveCtx: request.serveCtx}
}

// handlerCallContext is the context of a call: ctx, carrying the call's state and
// then the values of the context passed to Listener.Serve, which the contexts
// of the transports do not derive from. It takes the place of the
// context.WithValue that would carry the state, so the values cost a call
// nothing.
type handlerCallContext struct {
	context.Context
	state *callState
	// serveCtx is the context passed to Listener.Serve, nil for a call served
	// otherwise.
	serveCtx context.Context
}

func (c *handlerCallContext) Value(key any) any {
	if _, ok := key.(handlerContextKey); ok {
		return c.state
	}
	// ctx always holds the call's cancelation, so the context package finds
	// its own keys there and never in serveCtx.
	if value := c.Context.Value(key); value != nil || c.serveCtx == nil {
		return value
	}
	return c.serveCtx.Value(key)
}

// carriedCall returns the cancel func of ctx when it already carries the
// handler context of request, which requestLifetimeContext attaches with its
// deadline.
func carriedCall(ctx context.Context, request *wireRequest) (context.CancelFunc, bool) {
	state, ok := ctx.Value(handlerContextKey{}).(*callState)
	if !ok || state.request != request || state.cancel == nil {
		return nil, false
	}
	return state.cancel, true
}

// setCallStreams records the call's stream compression, negotiated once the
// call is authorized and routed, in the handler context ctx carries.
func setCallStreams(ctx context.Context, streams *serverStreams) {
	if state, ok := ctx.Value(handlerContextKey{}).(*callState); ok {
		state.streams = streams
		state.info.streams = streams
	}
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

// PlainCompressedRequests returns how many of the request messages of this
// call have arrived compressed without a dictionary so far (see
// PlainCompression). It is meant for observing that compression.
func (c HandlerContext) PlainCompressedRequests() int64 {
	if c.plainRequests == nil {
		return 0
	}
	return c.plainRequests.Load()
}
