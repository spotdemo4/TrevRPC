//! The metrics, server spans and propagation of trevrpc-opentelemetry, with the in-memory
//! exporters of the `OpenTelemetry` SDK.

use std::collections::BTreeMap;
use std::error::Error;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{SpanId, SpanKind, Status as SpanStatus, TracerProvider as _};
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::SubscriberExt as _;
use trevrpc::client::CallOptions;
use trevrpc::server::Server;
use trevrpc::{Metadata, RpcRequest, Status};
use trevrpc_opentelemetry::{Interceptor, Metrics, propagate_with};

#[macro_use]
#[path = "../../../tests/support/ensure.rs"]
mod ensure;

/// Where the `ensure!` macros find [`ensure::AssertionFailed`].
mod support {
    pub use super::ensure::AssertionFailed;
}

const TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
const SPAN_ID: &str = "b7ad6b7169203331";

/// A server whose `Fail` method fails with `INTERNAL` and whose `SayHello` method records a
/// `work` span.
fn server() -> Server {
    let mut server = Server::new();
    server.route_with_context("/example.Greeter/SayHello", |_context, _body| async move {
        tracing::info_span!("work").in_scope(|| ());
        Ok(b"hello".to_vec())
    });
    server.route_with_context("/example.Greeter/Fail", |_context, _body| async move {
        Err(Status::internal("handler failed").into())
    });
    server
}

fn request(path: &str, metadata: Metadata) -> RpcRequest {
    RpcRequest::new(path).with_metadata(metadata)
}

fn remote_parent() -> Metadata {
    Metadata::from([(
        "traceparent".to_owned(),
        format!("00-{TRACE_ID}-{SPAN_ID}-01").into_bytes(),
    )])
}

/// Exports the spans of the current thread's tracing subscriber to the returned exporter.
fn trace() -> (InMemorySpanExporter, SdkTracerProvider, DefaultGuard) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
    let guard = tracing::subscriber::set_default(subscriber);
    (exporter, provider, guard)
}

type TestResult = Result<(), Box<dyn Error>>;

fn span<'a>(spans: &'a [SpanData], name: &str) -> Result<&'a SpanData, String> {
    spans
        .iter()
        .find(|span| span.name == name)
        .ok_or_else(|| format!("no span {name} in {spans:?}"))
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
}

#[tokio::test]
async fn metrics_record_each_call_by_method_and_status() -> TestResult {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let mut server = server();
    server.set_metrics(Metrics::new(&provider.meter("test")));

    for path in [
        "/example.Greeter/SayHello",
        "/example.Greeter/Fail",
        // Methods the server does not register share one attribute value.
        "/example.Greeter/Missing",
        "/other.Service/Missing",
    ] {
        let _ = server
            .handle_request(RpcRequest::new(path), b"hi".to_vec())
            .await;
    }
    provider.force_flush()?;

    let metrics = exporter.get_finished_metrics()?;
    let durations = histogram_counts(&metrics, "rpc.server.call.duration")?;
    ensure_eq!(
        durations,
        BTreeMap::from([
            (
                "_OTHER UNIMPLEMENTED error.type=UNIMPLEMENTED".to_owned(),
                2
            ),
            (
                "example.Greeter/Fail INTERNAL error.type=INTERNAL".to_owned(),
                1
            ),
            ("example.Greeter/SayHello OK error.type=".to_owned(), 1),
        ])
    );
    let sizes = histogram_sums(&metrics, "rpc.server.response.size")?;
    ensure_eq!(
        sizes.get("example.Greeter/SayHello OK error.type="),
        Some(&5)
    );
    let sizes = histogram_sums(&metrics, "rpc.server.request.size")?;
    ensure_eq!(
        sizes.get("_OTHER UNIMPLEMENTED error.type=UNIMPLEMENTED"),
        Some(&4)
    );
    Ok(())
}

/// The data points of the histogram `name`, keyed by their method, status code and error type,
/// after checking their system name.
fn histogram_points<T: Copy>(
    metrics: &[ResourceMetrics],
    name: &str,
    data: impl Fn(&AggregatedMetrics) -> Option<Vec<(Vec<KeyValue>, u64, T)>>,
) -> Result<Vec<(String, u64, T)>, String> {
    let metric = metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .last()
        .ok_or_else(|| format!("no metric {name}"))?;
    let mut points = Vec::new();
    for (attributes, count, sum) in data(metric.data())
        .ok_or_else(|| format!("{name} is not a histogram of the expected type"))?
    {
        let value = |key: &str| {
            attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == key)
                .map(|attribute| attribute.value.to_string())
                .unwrap_or_default()
        };
        if value("rpc.system.name") != "trevrpc" {
            return Err(format!(
                "{name} point without rpc.system.name trevrpc: {attributes:?}"
            ));
        }
        let key = format!(
            "{} {} error.type={}",
            value("rpc.method"),
            value("rpc.response.status_code"),
            value("error.type")
        );
        points.push((key, count, sum));
    }
    Ok(points)
}

fn histogram_counts(
    metrics: &[ResourceMetrics],
    name: &str,
) -> Result<BTreeMap<String, u64>, String> {
    let points = histogram_points(metrics, name, |data| match data {
        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => Some(
            histogram
                .data_points()
                .map(|point| {
                    (
                        point.attributes().cloned().collect(),
                        point.count(),
                        point.sum(),
                    )
                })
                .collect(),
        ),
        _ => None,
    })?;
    Ok(points
        .into_iter()
        .map(|(key, count, _)| (key, count))
        .collect())
}

fn histogram_sums(
    metrics: &[ResourceMetrics],
    name: &str,
) -> Result<BTreeMap<String, u64>, String> {
    let points = histogram_points(metrics, name, |data| match data {
        AggregatedMetrics::U64(MetricData::Histogram(histogram)) => Some(
            histogram
                .data_points()
                .map(|point| {
                    (
                        point.attributes().cloned().collect(),
                        point.count(),
                        point.sum(),
                    )
                })
                .collect(),
        ),
        _ => None,
    })?;
    Ok(points.into_iter().map(|(key, _, sum)| (key, sum)).collect())
}

#[tokio::test]
async fn an_untrusted_client_span_is_linked() -> TestResult {
    let (exporter, _provider, _guard) = trace();
    let mut server = server();
    server.add_interceptor(Interceptor::new().with_propagator(TraceContextPropagator::new()));

    server
        .handle_request(
            request("/example.Greeter/SayHello", remote_parent()),
            Vec::new(),
        )
        .await?;

    let spans = exporter.get_finished_spans()?;
    let server_span = span(&spans, "example.Greeter/SayHello")?;
    ensure_eq!(server_span.span_kind, SpanKind::Server);
    ensure!(server_span.span_context.trace_id().to_string() != TRACE_ID);
    ensure_eq!(server_span.parent_span_id, SpanId::INVALID);
    let links: Vec<_> = server_span
        .links
        .iter()
        .map(|link| link.span_context.span_id().to_string())
        .collect();
    ensure_eq!(links, [SPAN_ID]);
    for (key, value) in [
        ("rpc.system.name", "trevrpc"),
        ("rpc.method", "example.Greeter/SayHello"),
        ("rpc.response.status_code", "OK"),
    ] {
        ensure_eq!(
            attribute(server_span, key)
                .map(ToString::to_string)
                .as_deref(),
            Some(value),
            "{key}"
        );
    }
    ensure!(attribute(server_span, "error.type").is_none());
    ensure!(attribute(server_span, "client.address").is_none());
    ensure_eq!(server_span.status, SpanStatus::Unset);
    // The handler runs in the server span.
    ensure_eq!(
        span(&spans, "work")?.parent_span_id,
        server_span.span_context.span_id()
    );
    Ok(())
}

#[tokio::test]
async fn a_trusted_client_span_is_the_parent() -> TestResult {
    let (exporter, _provider, _guard) = trace();
    let mut server = server();
    server.add_interceptor(
        Interceptor::new()
            .with_propagator(TraceContextPropagator::new())
            .with_trust_remote(true),
    );

    let Err(status) = server
        .handle_request(
            request("/example.Greeter/Fail", remote_parent()),
            Vec::new(),
        )
        .await
    else {
        return Err("the call should fail".into());
    };
    ensure_eq!(status.code(), trevrpc::Code::Internal);

    let spans = exporter.get_finished_spans()?;
    let server_span = span(&spans, "example.Greeter/Fail")?;
    ensure_eq!(server_span.span_context.trace_id().to_string(), TRACE_ID);
    ensure_eq!(server_span.parent_span_id.to_string(), SPAN_ID);
    ensure!(server_span.links.is_empty());
    ensure_eq!(
        attribute(server_span, "error.type")
            .map(ToString::to_string)
            .as_deref(),
        Some("INTERNAL")
    );
    ensure_eq!(server_span.status, SpanStatus::error("INTERNAL"));
    Ok(())
}

#[tokio::test]
async fn a_server_error_records_its_cause_on_the_span() -> TestResult {
    let (exporter, _provider, _guard) = trace();
    let mut server = server();
    server.add_interceptor(Interceptor::new());

    server
        .handle_request(
            request("/example.Greeter/SayHello", Metadata::new()),
            Vec::new(),
        )
        .await?;
    let Err(status) = server
        .handle_request(
            request("/example.Greeter/Fail", Metadata::new()),
            Vec::new(),
        )
        .await
    else {
        return Err("the call should fail".into());
    };
    // The caller learns nothing of the cause.
    ensure_eq!(
        status,
        Status::internal(trevrpc::server::INTERNAL_SERVER_ERROR)
    );

    let spans = exporter.get_finished_spans()?;
    let causes = |name| -> Result<Vec<String>, String> {
        Ok(span(&spans, name)?
            .events
            .iter()
            .filter(|event| event.name == "exception")
            .flat_map(|event| event.attributes.iter())
            .filter(|attribute| attribute.key.as_str() == "exception.message")
            .map(|attribute| attribute.value.to_string())
            .collect())
    };
    ensure_eq!(
        causes("example.Greeter/Fail")?,
        ["Internal: handler failed"]
    );
    ensure_eq!(
        span(&spans, "example.Greeter/Fail")?.status,
        SpanStatus::error("INTERNAL")
    );
    ensure!(causes("example.Greeter/SayHello")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_propagating_client_continues_its_trace_on_a_trusting_server() -> TestResult {
    let (exporter, provider, _guard) = trace();
    let mut server = server();
    server.add_interceptor(
        Interceptor::new()
            .with_propagator(TraceContextPropagator::new())
            .with_trust_remote(true),
    );

    let client_span = tracing::info_span!("client");
    let options =
        client_span.in_scope(|| propagate_with(&TraceContextPropagator::new())(CallOptions::new()));
    ensure!(options.metadata().contains_key("traceparent"));
    drop(client_span);
    server
        .handle_request(
            request("/example.Greeter/SayHello", options.metadata().clone()),
            Vec::new(),
        )
        .await?;
    provider.force_flush()?;

    let spans = exporter.get_finished_spans()?;
    let client_span = span(&spans, "client")?;
    let server_span = span(&spans, "example.Greeter/SayHello")?;
    ensure_eq!(
        server_span.span_context.trace_id(),
        client_span.span_context.trace_id()
    );
    ensure_eq!(
        server_span.parent_span_id,
        client_span.span_context.span_id()
    );
    Ok(())
}
