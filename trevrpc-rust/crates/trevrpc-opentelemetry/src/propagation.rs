use std::collections::HashMap;

use opentelemetry::global;
use opentelemetry::propagation::TextMapPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use trevrpc::client::CallOptions;

/// Adds the trace context of the current [`tracing`] span, and its baggage, to the request
/// metadata of `options`, with the global propagator. Pass it to a call's `options`:
///
/// ```ignore
/// let reply = client
///     .say_hello(&request)
///     .options(trevrpc_opentelemetry::propagate)
///     .await?;
/// ```
///
/// A server whose [`Interceptor`](crate::Interceptor) trusts it continues the trace. The global
/// propagator propagates nothing until [`opentelemetry::global::set_text_map_propagator`] sets
/// one.
#[must_use]
pub fn propagate(options: CallOptions) -> CallOptions {
    with_entries(
        options,
        global::get_text_map_propagator(|propagator| entries(propagator)),
    )
}

/// Returns a function that adds trace context to call options as [`propagate`] does, with
/// `propagator` instead of the global one.
pub fn propagate_with<P>(propagator: &P) -> impl FnOnce(CallOptions) -> CallOptions + '_
where
    P: TextMapPropagator + ?Sized,
{
    move |options| with_entries(options, entries(propagator))
}

/// The metadata entries `propagator` injects for the current span.
fn entries<P>(propagator: &P) -> HashMap<String, String>
where
    P: TextMapPropagator + ?Sized,
{
    let mut entries = HashMap::new();
    propagator.inject_context(&tracing::Span::current().context(), &mut entries);
    entries
}

fn with_entries(options: CallOptions, entries: HashMap<String, String>) -> CallOptions {
    entries.into_iter().fold(options, |options, (key, value)| {
        options.with_metadata(key, value)
    })
}
