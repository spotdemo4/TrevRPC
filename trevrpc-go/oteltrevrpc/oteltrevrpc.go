// Package oteltrevrpc instruments TrevRPC servers and clients with
// OpenTelemetry, following the semantic conventions for RPC:
//
//   - [Metrics] records the duration of every call a server handles, and the
//     sizes of its request and response bodies;
//   - [Interceptor] traces each call a server's handlers serve in a server span;
//   - [Propagate] sends the trace context of a client call to the server.
//
// It is a separate module so that TrevRPC itself does not depend on
// OpenTelemetry. For server diagnostics, see trevrpc.LogDiagnostics.
package oteltrevrpc

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// SystemName is the value of the rpc.system.name attribute of TrevRPC calls.
const SystemName = "trevrpc"

// instrumentationName names the meter and tracer of this package.
const instrumentationName = "trev.zip/llc/trevrpc/trevrpc-go/oteltrevrpc"

// Option configures Metrics, Interceptor and Propagate. Each uses only the
// options that apply to it.
type Option func(*config)

type config struct {
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
	trustRemote    bool
}

func newConfig(options []Option) config {
	config := config{}
	for _, option := range options {
		option(&config)
	}
	if config.meterProvider == nil {
		config.meterProvider = otel.GetMeterProvider()
	}
	if config.tracerProvider == nil {
		config.tracerProvider = otel.GetTracerProvider()
	}
	if config.propagator == nil {
		config.propagator = otel.GetTextMapPropagator()
	}
	return config
}

// WithMeterProvider sets the meter provider Metrics records with. It defaults
// to the global one, otel.GetMeterProvider().
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(config *config) {
		config.meterProvider = provider
	}
}

// WithTracerProvider sets the tracer provider Interceptor starts spans with. It
// defaults to the global one, otel.GetTracerProvider().
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(config *config) {
		config.tracerProvider = provider
	}
}

// WithPropagator sets the propagator that Propagate injects trace context into
// request metadata with, and that Interceptor extracts it with. It defaults to
// the global one, otel.GetTextMapPropagator(), which propagates nothing until
// otel.SetTextMapPropagator sets one, such as
// propagation.NewCompositeTextMapPropagator(propagation.TraceContext{},
// propagation.Baggage{}).
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return func(config *config) {
		config.propagator = propagator
	}
}

// WithTrustRemote makes Interceptor continue the trace a client propagated:
// each server span becomes a child of the client's span, and the handler's
// context carries the client's baggage. Without it, as suits a public API, a
// server span starts a new trace that links to the client's span, and the
// client's baggage is dropped, so clients cannot choose the trace a server
// records its work in, or its sampling.
func WithTrustRemote() Option {
	return func(config *config) {
		config.trustRemote = true
	}
}
