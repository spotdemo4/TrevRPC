package oteltrevrpc

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// Interceptor is a trevrpc.Interceptor that traces each call a handler serves
// in a server span, which the handler's context carries, so that the spans the
// handler starts, and the calls it makes with Propagate, belong to it.
//
// The span is named after the method, "package.Service/Method", and has the
// attributes rpc.system.name ("trevrpc"), rpc.method and, when the transport
// knows the client's address, client.address and client.port. When the call
// ends it gets rpc.response.status_code, and an error status and error.type
// when the code means the server failed: UNKNOWN, DEADLINE_EXCEEDED,
// UNIMPLEMENTED, INTERNAL, UNAVAILABLE, DATA_LOSS or a code TrevRPC does not
// define. A streaming call ends when its response stream does.
//
// A client's trace context is extracted from the request metadata with the
// propagator of WithPropagator, or the global one, and only continued with
// WithTrustRemote.
//
// Install it with Server.SetInterceptors, first so that its span covers the
// interceptors after it. Calls the server rejects before handlers run, such as
// those the authorizer denies, have no span; Metrics records them.
type Interceptor struct {
	tracer      trace.Tracer
	propagator  propagation.TextMapPropagator
	trustRemote bool
}

var _ trevrpc.Interceptor = (*Interceptor)(nil)

// NewInterceptor returns an Interceptor that starts spans with the tracer
// provider of WithTracerProvider, or the global one.
func NewInterceptor(options ...Option) *Interceptor {
	config := newConfig(options)
	return &Interceptor{
		tracer:      config.tracerProvider.Tracer(instrumentationName, trace.WithSchemaURL(semconv.SchemaURL)),
		propagator:  config.propagator,
		trustRemote: config.trustRemote,
	}
}

// Intercept starts the span of a call and returns the handler's context, which
// carries it, and the function that ends it.
func (i *Interceptor) Intercept(ctx context.Context, info trevrpc.HandlerContext) (context.Context, func(trevrpc.RPCFinished), error) {
	method := strings.TrimPrefix(info.FullMethod, "/")
	options := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			semconv.RPCSystemNameKey.String(SystemName),
			semconv.RPCMethod(method),
		),
	}
	if info.RemoteAddr.IsValid() {
		options = append(options, trace.WithAttributes(
			semconv.ClientAddress(info.RemoteAddr.Addr().String()),
			semconv.ClientPort(int(info.RemoteAddr.Port())),
		))
	}
	carrier := metadataCarrier(info.Metadata)
	if i.trustRemote {
		ctx = i.propagator.Extract(ctx, carrier)
	} else {
		// Extracted apart from ctx, so that only the client's span is linked.
		options = append(options, trace.WithNewRoot())
		if link := trace.LinkFromContext(i.propagator.Extract(context.Background(), carrier)); link.SpanContext.IsValid() {
			options = append(options, trace.WithLinks(link))
		}
	}
	ctx, span := i.tracer.Start(ctx, method, options...)
	return ctx, func(event trevrpc.RPCFinished) {
		code := statusCode(event.Code)
		span.SetAttributes(semconv.RPCResponseStatusCode(code))
		if serverError(event.Code) {
			span.SetAttributes(semconv.ErrorTypeKey.String(code))
			span.SetStatus(codes.Error, code)
		}
		span.End()
	}, nil
}
