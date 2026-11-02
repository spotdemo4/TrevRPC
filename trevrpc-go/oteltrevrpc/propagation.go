package oteltrevrpc

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// Propagate returns a CallOption that sends the trace context of ctx, and its
// baggage, as request metadata, with the propagator of WithPropagator, or the
// global one. Pass it to a call made with ctx:
//
//	reply, err := client.SayHello(ctx, request, oteltrevrpc.Propagate(ctx))
//
// A server whose Interceptor trusts it (see WithTrustRemote) continues the
// trace.
func Propagate(ctx context.Context, options ...Option) trevrpc.CallOption {
	metadata := trevrpc.Metadata{}
	newConfig(options).propagator.Inject(ctx, metadataCarrier(metadata))
	return trevrpc.WithMetadataEntries(metadata)
}

// metadataCarrier carries trace context in TrevRPC metadata.
type metadataCarrier trevrpc.Metadata

var _ propagation.TextMapCarrier = metadataCarrier(nil)

func (c metadataCarrier) Get(key string) string {
	return string(c[trevrpc.NormalizeMetadataKey(key)])
}

func (c metadataCarrier) Set(key, value string) {
	c[trevrpc.NormalizeMetadataKey(key)] = []byte(value)
}

func (c metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}
