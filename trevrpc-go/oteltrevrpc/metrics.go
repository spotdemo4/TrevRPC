package oteltrevrpc

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/rpcconv"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// otherMethod is the rpc.method of a call to a method the server does not
// register, whose name the client chose.
const otherMethod = "_OTHER"

// sizeBuckets are the bucket boundaries of the body size histograms, in bytes.
var sizeBuckets = []float64{0, 64, 256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20}

// Metrics is a trevrpc.Metrics that records, for every call a server handles,
// including the calls it rejects before a handler runs:
//
//   - rpc.server.call.duration, the duration of the call in seconds;
//   - rpc.server.request.size and rpc.server.response.size, the sizes in bytes
//     of the request and response bodies TrevRPC reports in
//     trevrpc.RPCFinished, under the names earlier versions of the semantic
//     conventions gave them.
//
// Each measurement has the attributes rpc.system.name ("trevrpc"),
// rpc.method ("package.Service/Method", or "_OTHER" for a method the server
// does not register, so clients cannot add attribute values) and
// rpc.response.status_code (such as "NOT_FOUND"), and error.type, the status
// code, when the code means the server failed (see Interceptor).
//
// Install it with Server.SetMetrics.
type Metrics struct {
	duration     rpcconv.ServerCallDuration
	requestSize  metric.Int64Histogram
	responseSize metric.Int64Histogram
	// attributes caches the attribute set of each method and code.
	attributes sync.Map
}

var _ trevrpc.Metrics = (*Metrics)(nil)

type attributesKey struct {
	service, method string
	code            trevrpc.Code
}

// NewMetrics returns Metrics that record with the meter provider of
// WithMeterProvider, or the global one.
func NewMetrics(options ...Option) (*Metrics, error) {
	config := newConfig(options)
	meter := config.meterProvider.Meter(instrumentationName, metric.WithSchemaURL(semconv.SchemaURL))
	duration, err := rpcconv.NewServerCallDuration(meter)
	if err != nil {
		return nil, err
	}
	requestSize, err := meter.Int64Histogram("rpc.server.request.size",
		metric.WithDescription("Measures the size of RPC request bodies."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(sizeBuckets...))
	if err != nil {
		return nil, err
	}
	responseSize, err := meter.Int64Histogram("rpc.server.response.size",
		metric.WithDescription("Measures the size of RPC response bodies."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(sizeBuckets...))
	if err != nil {
		return nil, err
	}
	return &Metrics{duration: duration, requestSize: requestSize, responseSize: responseSize}, nil
}

// RPCStarted does nothing: Metrics records each call when it finishes.
func (*Metrics) RPCStarted(trevrpc.RPCStarted) {}

// RPCFinished records the duration and body sizes of a call.
func (m *Metrics) RPCFinished(event trevrpc.RPCFinished) {
	ctx := context.Background()
	attributes := metric.WithAttributeSet(m.attributeSet(event))
	m.duration.Inst().Record(ctx, event.Elapsed.Seconds(), attributes)
	m.requestSize.Record(ctx, int64(event.RequestBodyLen), attributes)
	m.responseSize.Record(ctx, int64(event.ResponseBodyLen), attributes)
}

// attributeSet returns the attributes of the measurements of a call.
func (m *Metrics) attributeSet(event trevrpc.RPCFinished) attribute.Set {
	key := attributesKey{code: event.Code}
	if event.Registered {
		key.service, key.method = event.Service, event.Method
	}
	if set, ok := m.attributes.Load(key); ok {
		return set.(attribute.Set)
	}
	method := otherMethod
	if event.Registered {
		method = event.Service + "/" + event.Method
	}
	attributes := []attribute.KeyValue{
		semconv.RPCSystemNameKey.String(SystemName),
		semconv.RPCMethod(method),
		semconv.RPCResponseStatusCode(statusCode(event.Code)),
	}
	if serverError(event.Code) {
		attributes = append(attributes, semconv.ErrorTypeKey.String(statusCode(event.Code)))
	}
	set, _ := m.attributes.LoadOrStore(key, attribute.NewSet(attributes...))
	return set.(attribute.Set)
}
