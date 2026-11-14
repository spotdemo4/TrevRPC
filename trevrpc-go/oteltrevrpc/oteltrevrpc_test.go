package oteltrevrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"maps"
	"net/netip"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	echoMethod = "/example.Echo/Echo"
	traceID    = "0af7651916cd43dd8448eb211c80319c"
	spanID     = "b7ad6b7169203331"
)

var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

func TestMetricsRecordEachCallByMethodAndStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	metrics, err := NewMetrics(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	if err != nil {
		t.Fatal(err)
	}

	metrics.RPCStarted(trevrpc.RPCStarted{Service: "example.Echo", Method: "Echo", Registered: true})
	metrics.RPCFinished(trevrpc.RPCFinished{Service: "example.Echo", Method: "Echo", Registered: true, RequestBodyLen: 3, ResponseBodyLen: 5, Code: trevrpc.CodeOK, Elapsed: 2 * time.Second})
	metrics.RPCFinished(trevrpc.RPCFinished{Service: "example.Echo", Method: "Echo", Registered: true, Code: trevrpc.CodeInternal, Elapsed: time.Second})
	// Methods the server does not register share one attribute value.
	metrics.RPCFinished(trevrpc.RPCFinished{Service: "a", Method: "b", Code: trevrpc.CodePermissionDenied})
	metrics.RPCFinished(trevrpc.RPCFinished{Service: "c", Method: "d", Code: trevrpc.CodePermissionDenied})

	collected := collect(t, reader)
	durations := histogram[float64](t, collected, "rpc.server.call.duration")
	want := map[string]uint64{
		"example.Echo/Echo OK error.type=":               1,
		"example.Echo/Echo INTERNAL error.type=INTERNAL": 1,
		"_OTHER PERMISSION_DENIED error.type=":           2,
	}
	if got := pointCounts(t, durations); !maps.Equal(got, want) {
		t.Fatalf("duration points = %v, want %v", got, want)
	}
	for _, point := range durations {
		if status, _ := point.Attributes.Value("rpc.response.status_code"); status.AsString() == "OK" && point.Sum != 2 {
			t.Fatalf("OK duration sum = %v, want 2", point.Sum)
		}
	}
	for name, want := range map[string]int64{"rpc.server.request.size": 3, "rpc.server.response.size": 5} {
		for _, point := range histogram[int64](t, collected, name) {
			if status, _ := point.Attributes.Value("rpc.response.status_code"); status.AsString() == "OK" && point.Sum != want {
				t.Fatalf("%s OK sum = %d, want %d", name, point.Sum, want)
			}
		}
	}
}

func TestInterceptorStartsAServerSpanEndedWithTheStatus(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	interceptor := NewInterceptor(WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))), WithPropagator(propagator))

	ctx, finish, err := interceptor.Intercept(context.Background(), trevrpc.HandlerContext{
		FullMethod: echoMethod,
		RemoteAddr: netip.MustParseAddrPort("192.0.2.1:4433"),
		Metadata:   trevrpc.Metadata{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("handler context carries no span")
	}
	if len(recorder.Ended()) != 0 {
		t.Fatal("span ended before the call")
	}
	finish(trevrpc.RPCFinished{Code: trevrpc.CodeInternal})

	span := onlySpan(t, recorder)
	if span.Name() != "example.Echo/Echo" || span.SpanKind() != trace.SpanKindServer {
		t.Fatalf("span %q of kind %v", span.Name(), span.SpanKind())
	}
	expectAttributes(t, span.Attributes(), map[attribute.Key]attribute.Value{
		"rpc.system.name":          attribute.StringValue("trevrpc"),
		"rpc.method":               attribute.StringValue("example.Echo/Echo"),
		"client.address":           attribute.StringValue("192.0.2.1"),
		"client.port":              attribute.IntValue(4433),
		"rpc.response.status_code": attribute.StringValue("INTERNAL"),
		"error.type":               attribute.StringValue("INTERNAL"),
	})
	if span.Status().Code != codes.Error {
		t.Fatalf("status = %v, want an error", span.Status())
	}
}

func TestInterceptorLeavesClientErrorsUnset(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	interceptor := NewInterceptor(WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))))

	_, finish, err := interceptor.Intercept(context.Background(), trevrpc.HandlerContext{FullMethod: echoMethod})
	if err != nil {
		t.Fatal(err)
	}
	finish(trevrpc.RPCFinished{Code: trevrpc.CodeNotFound})

	span := onlySpan(t, recorder)
	if span.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", span.Status())
	}
	for _, kv := range span.Attributes() {
		if kv.Key == "client.address" || kv.Key == "error.type" {
			t.Fatalf("unexpected attribute %v", kv)
		}
	}
}

func TestInterceptorRecordsTheCauseOfAServerError(t *testing.T) {
	cause := errors.New("load user: connection refused")
	for _, tt := range []struct {
		name       string
		event      trevrpc.RPCFinished
		wantRecord bool
	}{
		{"internal", trevrpc.RPCFinished{Code: trevrpc.CodeInternal, Err: cause}, true},
		{"internal without a cause", trevrpc.RPCFinished{Code: trevrpc.CodeInternal}, false},
		{"client error", trevrpc.RPCFinished{Code: trevrpc.CodeNotFound, Err: trevrpc.NotFound("no such user")}, false},
		{"success", trevrpc.RPCFinished{Code: trevrpc.CodeOK}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			interceptor := NewInterceptor(WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))))
			_, finish, err := interceptor.Intercept(context.Background(), trevrpc.HandlerContext{FullMethod: echoMethod})
			if err != nil {
				t.Fatal(err)
			}
			finish(tt.event)

			span := onlySpan(t, recorder)
			var recorded []string
			for _, event := range span.Events() {
				if event.Name != "exception" {
					continue
				}
				for _, kv := range event.Attributes {
					if kv.Key == "exception.message" {
						recorded = append(recorded, kv.Value.AsString())
					}
				}
			}
			if tt.wantRecord {
				if len(recorded) != 1 || recorded[0] != cause.Error() {
					t.Fatalf("recorded exceptions %v, want %q", recorded, cause)
				}
				// The status description is all the caller learns.
				if span.Status().Code != codes.Error || span.Status().Description != "INTERNAL" {
					t.Fatalf("status = %v, want an INTERNAL error", span.Status())
				}
			} else if len(recorded) != 0 {
				t.Fatalf("recorded exceptions %v, want none", recorded)
			}
		})
	}
}

func TestInterceptorLinksToAnUntrustedClientSpan(t *testing.T) {
	for _, trust := range []bool{false, true} {
		recorder := tracetest.NewSpanRecorder()
		options := []Option{WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))), WithPropagator(propagator)}
		if trust {
			options = append(options, WithTrustRemote())
		}
		interceptor := NewInterceptor(options...)

		ctx, finish, err := interceptor.Intercept(context.Background(), trevrpc.HandlerContext{
			FullMethod: echoMethod,
			Metadata: trevrpc.Metadata{
				"traceparent": []byte("00-" + traceID + "-" + spanID + "-01"),
				"baggage":     []byte("tenant=a"),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		finish(trevrpc.RPCFinished{})

		span := onlySpan(t, recorder)
		continued := span.SpanContext().TraceID().String() == traceID && span.Parent().SpanID().String() == spanID
		linked := len(span.Links()) == 1 && span.Links()[0].SpanContext.SpanID().String() == spanID
		if continued != trust || linked == trust {
			t.Fatalf("trust %t: continued %t, linked %t", trust, continued, linked)
		}
		if tenant := baggage.FromContext(ctx).Member("tenant").Value(); (tenant == "a") != trust {
			t.Fatalf("trust %t: baggage tenant %q", trust, tenant)
		}
	}
}

func TestServerContinuesTheTraceOfAPropagatingClient(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	metrics, err := NewMetrics(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	if err != nil {
		t.Fatal(err)
	}

	server := trevrpc.NewServer()
	server.SetMetrics(metrics)
	server.SetInterceptors(NewInterceptor(WithTracerProvider(tracerProvider), WithPropagator(propagator), WithTrustRemote()))
	trevrpc.RegisterUnary(server, echoMethod, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if request.Value == "fail" {
				return nil, errors.New("handler failed")
			}
			return request, nil
		})
	channel := serve(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx, clientSpan := tracerProvider.Tracer("test").Start(ctx, "client")
	newReply := func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }
	if _, err := trevrpc.Unary(ctx, channel, echoMethod, wrapperspb.String("hi"), newReply, Propagate(ctx, WithPropagator(propagator))); err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if _, err := trevrpc.Unary(ctx, channel, echoMethod, wrapperspb.String("fail"), newReply); trevrpc.StatusFromError(err).Code != trevrpc.CodeInternal {
		t.Fatalf("failing Echo: %v", err)
	}
	clientSpan.End()

	var serverSpans []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			serverSpans = append(serverSpans, span)
		}
	}
	if len(serverSpans) != 2 {
		t.Fatalf("server spans = %d, want 2", len(serverSpans))
	}
	if parent := serverSpans[0].Parent(); parent.SpanID() != clientSpan.SpanContext().SpanID() {
		t.Fatalf("propagated server span parent = %v, want the client span", parent.SpanID())
	}
	// Without Propagate the second call starts its own trace.
	if serverSpans[1].Parent().IsValid() || serverSpans[1].Status().Code != codes.Error {
		t.Fatalf("unpropagated server span parent %v, status %v", serverSpans[1].Parent(), serverSpans[1].Status())
	}

	want := map[string]uint64{
		"example.Echo/Echo OK error.type=":               1,
		"example.Echo/Echo INTERNAL error.type=INTERNAL": 1,
	}
	if got := pointCounts(t, histogram[float64](t, collect(t, reader), "rpc.server.call.duration")); !maps.Equal(got, want) {
		t.Fatalf("duration points = %v, want %v", got, want)
	}
}

// serve serves server on a local listener until the test ends and returns a
// channel to it.
func serve(t *testing.T, server *trevrpc.Server) *trevrpc.Channel {
	t.Helper()
	certificate, err := trevrpc.NewDevelopmentCertificate()
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM, err := certificate.PEM()
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("PEM holds no certificates")
	}
	listener, err := trevrpc.Listen("127.0.0.1:0", server, trevrpc.ListenOptions{TLSConfig: certificate.TLSConfig(), QUICConfig: testQUICConfig()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- listener.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})

	channel, err := trevrpc.Dial(ctx, listener.Addr().String(), trevrpc.DialOptions{
		TLSConfig:  &tls.Config{RootCAs: roots, ServerName: "localhost"},
		QUICConfig: testQUICConfig(),
		Transport:  trevrpc.TransportQUICOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel
}

func onlySpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	return spans[0]
}

func expectAttributes(t *testing.T, got []attribute.KeyValue, want map[attribute.Key]attribute.Value) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("attributes = %v, want %v", got, want)
	}
	for _, kv := range got {
		if value, ok := want[kv.Key]; !ok || value != kv.Value {
			t.Fatalf("attributes = %v, want %v", got, want)
		}
	}
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	return collected
}

func histogram[N int64 | float64](t *testing.T, collected metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[N] {
	t.Helper()
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric.Data.(metricdata.Histogram[N]).DataPoints
			}
		}
	}
	t.Fatalf("no %s in %v", name, collected)
	return nil
}

// pointCounts returns the count of each point, keyed by its method, status code
// and error type, after checking its system name.
func pointCounts[N int64 | float64](t *testing.T, points []metricdata.HistogramDataPoint[N]) map[string]uint64 {
	t.Helper()
	counts := map[string]uint64{}
	for _, point := range points {
		if system, _ := point.Attributes.Value("rpc.system.name"); system.AsString() != "trevrpc" {
			t.Fatalf("rpc.system.name of %v", point.Attributes.ToSlice())
		}
		method, _ := point.Attributes.Value("rpc.method")
		status, _ := point.Attributes.Value("rpc.response.status_code")
		errorType, _ := point.Attributes.Value("error.type")
		counts[method.AsString()+" "+status.AsString()+" error.type="+errorType.AsString()] += point.Count
	}
	return counts
}

// testQUICConfig is the QUIC configuration of the test's listener and dial.
// quic-go gives up a handshake after 5 s without network activity, which a
// loopback handshake on a starved CI runner has exceeded, and either side
// giving up fails the dial.
func testQUICConfig() *quic.Config {
	return &quic.Config{HandshakeIdleTimeout: 10 * time.Second}
}
