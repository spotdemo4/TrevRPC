use std::sync::Arc;

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{Status as SpanStatus, TraceContextExt as _};
use opentelemetry::{Context, global};
use tracing::field::{Empty, display};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use trevrpc::server::{Finish, RequestContext, RpcFinished};
use trevrpc::{Metadata, Status};

use crate::SYSTEM_NAME;
use crate::status::{server_error, status_code};

/// A [`trevrpc::server::Interceptor`] that traces each call a handler serves in a [`tracing`] span
/// at the `INFO` level, in which the rest of the call runs (see [`RequestContext::set_span`]), so
/// the events and spans the handler records belong to it.
///
/// With a [`tracing-opentelemetry`](tracing_opentelemetry) layer, the span becomes a server span
/// named after the method, `package.Service/Method`, with the attributes `rpc.system.name`
/// (`trevrpc`), `rpc.method` and, when the transport knows the client's address, `client.address`
/// and `client.port`. When the call ends it gets `rpc.response.status_code`, and an error status
/// and `error.type` when the code means the server failed: `UNKNOWN`, `DEADLINE_EXCEEDED`,
/// `UNIMPLEMENTED`, `INTERNAL`, `UNAVAILABLE` or `DATA_LOSS`. A streaming call ends when its
/// response stream does.
///
/// A client's trace context is extracted from the request metadata with the propagator of
/// [`Interceptor::with_propagator`], or the global one, and only continued with
/// [`Interceptor::with_trust_remote`]. Otherwise, as suits a public API, the span links to the
/// client's span instead, so clients cannot choose the trace a server records its work in, or its
/// sampling, and keeps the parent it was created in: the span of an earlier interceptor, if any.
///
/// Install it with [`Server::add_interceptor`](trevrpc::server::Server::add_interceptor), first so
/// that its span covers the interceptors after it. Calls the server rejects before handlers run,
/// such as those the authorizer denies, have no span; [`Metrics`](crate::Metrics) records them.
#[derive(Clone, Default)]
pub struct Interceptor {
    propagator: Option<Arc<dyn TextMapPropagator + Send + Sync>>,
    trust_remote: bool,
}

impl Interceptor {
    /// An interceptor that extracts trace context with the global propagator and does not trust
    /// it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the propagator that extracts trace context from request metadata, instead of the
    /// global one, which propagates nothing until [`opentelemetry::global::set_text_map_propagator`]
    /// sets one.
    #[must_use]
    pub fn with_propagator(
        mut self,
        propagator: impl TextMapPropagator + Send + Sync + 'static,
    ) -> Self {
        self.propagator = Some(Arc::new(propagator));
        self
    }

    /// Sets whether to continue the trace a client propagated: each span becomes a child of the
    /// client's span. The default is `false`.
    #[must_use]
    pub const fn with_trust_remote(mut self, trust: bool) -> Self {
        self.trust_remote = trust;
        self
    }

    /// The trace context of the client, from `metadata`.
    fn extract(&self, metadata: &Metadata) -> Context {
        let carrier = MetadataExtractor(metadata);
        match &self.propagator {
            Some(propagator) => propagator.extract(&carrier),
            None => global::get_text_map_propagator(|propagator| propagator.extract(&carrier)),
        }
    }
}

#[trevrpc::async_trait]
impl trevrpc::server::Interceptor for Interceptor {
    async fn intercept(
        &self,
        context: &mut RequestContext,
    ) -> std::result::Result<Option<Finish>, Status> {
        let method = context.full_method().strip_prefix('/').unwrap_or_default();
        let span = tracing::info_span!(
            "trevrpc",
            otel.name = method,
            otel.kind = "server",
            rpc.system.name = SYSTEM_NAME,
            rpc.method = method,
            client.address = Empty,
            client.port = Empty,
            rpc.response.status_code = Empty,
            "error.type" = Empty,
        );
        if let Some(addr) = context.remote_addr() {
            span.record("client.address", display(addr.ip()));
            span.record("client.port", addr.port());
        }
        let remote = self.extract(context.metadata());
        let remote_span = remote.span().span_context().clone();
        if remote_span.is_valid() {
            if self.trust_remote {
                // Fails only without a tracing-opentelemetry layer, which exports nothing anyway.
                let _ = span.set_parent(remote);
            } else {
                span.add_link(remote_span);
            }
        }
        context.set_span(span.clone());
        Ok(Some(Box::new(move |event: &RpcFinished| {
            let code = status_code(event.code);
            span.record("rpc.response.status_code", code);
            if server_error(event.code) {
                span.record("error.type", code);
                span.set_status(SpanStatus::error(code));
            }
        })))
    }
}

/// Reads trace context from request metadata.
struct MetadataExtractor<'a>(&'a Metadata);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        // Metadata keys are lowercase, as propagators' keys are, so this rarely allocates.
        let value = match self.0.get(key) {
            Some(value) => value,
            None if key.bytes().any(|byte| byte.is_ascii_uppercase()) => {
                self.0.get(&trevrpc::wire::normalize_metadata_key(key))?
            }
            None => return None,
        };
        std::str::from_utf8(value).ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}
