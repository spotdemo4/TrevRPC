use std::collections::HashMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::sync::watch;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::sync::MutexExt as _;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{
    BoxStream, Code, Error, Metadata, ResponseEnvelope, ResponseFrame, Result, RpcKind, RpcRequest,
    Status,
};

type UnaryHandlerFuture =
    Pin<Box<dyn Future<Output = Result<ResponseEnvelope<Vec<u8>>>> + Send + 'static>>;
type UnaryHandler =
    Arc<dyn Fn(RequestContext, Vec<u8>) -> UnaryHandlerFuture + Send + Sync + 'static>;
type StreamingHandlerFuture =
    Pin<Box<dyn Future<Output = Result<ResponseEnvelope<BoxStream<Vec<u8>>>>> + Send + 'static>>;
type StreamingHandler = Arc<
    dyn Fn(RequestContext, Vec<u8>, BoxStream<Vec<u8>>) -> StreamingHandlerFuture + Send + Sync,
>;
type RouteMap = HashMap<String, HashMap<String, Route>>;

#[derive(Clone)]
enum Route {
    Unary(UnaryHandler),
    Streaming {
        kind: RpcKind,
        handler: StreamingHandler,
    },
}

#[derive(Clone)]
pub struct Server {
    routes: Arc<RouteMap>,
    options: ServerOptions,
    authorizer: Option<Arc<dyn Authorizer>>,
    metrics: Arc<dyn Metrics>,
}

/// Identifies the first lifecycle event that cancelled an RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationSource {
    /// The effective RPC deadline elapsed.
    Deadline,
    /// The peer reset or stopped the RPC stream.
    PeerReset,
    /// The underlying transport connection was lost.
    ConnectionLost,
    /// The server began shutting down before the RPC completed.
    ServerShutdown,
}

#[derive(Debug)]
struct CancellationState {
    source: watch::Sender<Option<CancellationSource>>,
    completion_code: Mutex<Option<Code>>,
}

/// A cloneable, first-writer-wins RPC cancellation signal.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    /// Creates an uncancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                source: watch::channel(None).0,
                completion_code: Mutex::new(None),
            }),
        }
    }

    /// Returns the cancellation source once cancellation has been observed.
    #[must_use]
    pub fn source(&self) -> Option<CancellationSource> {
        *self.state.source.borrow()
    }

    /// Returns whether the RPC has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.source().is_some()
    }

    /// Waits until cancellation and returns its source.
    pub async fn cancelled(&self) -> CancellationSource {
        let mut source = self.subscribe();
        loop {
            let cancelled = *source.borrow_and_update();
            if let Some(source) = cancelled {
                return source;
            }
            if source.changed().await.is_err() {
                // `self` retains the sender, so disconnection is unreachable while this
                // future is alive. Preserve the API's wait-until-cancelled contract if
                // that invariant ever changes instead of panicking or spinning.
                std::future::pending::<()>().await;
            }
        }
    }

    fn subscribe(&self) -> watch::Receiver<Option<CancellationSource>> {
        self.state.source.subscribe()
    }

    pub(crate) fn cancel(&self, source: CancellationSource) -> bool {
        self.state.source.send_if_modified(|current| {
            if current.is_some() {
                false
            } else {
                *current = Some(source);
                true
            }
        })
    }

    pub(crate) fn set_completion_code(&self, code: Code) {
        let mut completion_code = self.state.completion_code.lock_unpoisoned();
        if completion_code.is_none() {
            *completion_code = Some(code);
        }
    }

    fn completion_code(&self) -> Option<Code> {
        *self.state.completion_code.lock_unpoisoned()
    }
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    service: String,
    method: String,
    kind: RpcKind,
    metadata: Metadata,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
}

impl RequestContext {
    #[must_use]
    pub fn new(request: &RpcRequest, deadline: Option<Instant>) -> Self {
        Self {
            service: request.service.clone(),
            method: request.method.clone(),
            kind: request.rpc_kind(),
            metadata: request.metadata.clone(),
            deadline,
            cancellation: CancellationToken::new(),
        }
    }

    #[must_use]
    pub fn service(&self) -> &str {
        &self.service
    }

    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    #[must_use]
    pub const fn kind(&self) -> RpcKind {
        self.kind
    }

    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[must_use]
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    #[must_use]
    pub fn time_remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    #[must_use]
    pub fn deadline_expired(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| deadline <= Instant::now())
    }

    /// Returns the call's cloneable cancellation token.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the first cancellation source, if cancellation was observed.
    #[must_use]
    pub fn cancellation_source(&self) -> Option<CancellationSource> {
        if self.cancellation.source().is_none() && self.deadline_expired() {
            self.cancellation.cancel(CancellationSource::Deadline);
        }
        self.cancellation.source()
    }

    /// Returns whether the call was cancelled or its deadline elapsed.
    #[must_use]
    pub fn cancelled(&self) -> bool {
        self.cancellation_source().is_some()
    }

    /// Waits until the call is cancelled and returns the first cancellation source.
    pub async fn cancelled_signal(&self) -> CancellationSource {
        if let Some(source) = self.cancellation_source() {
            return source;
        }

        let Some(deadline) = self.deadline else {
            return self.cancellation.cancelled().await;
        };
        let deadline = tokio::time::Instant::from_std(deadline);

        tokio::select! {
            biased;
            source = self.cancellation.cancelled() => source,
            () = tokio::time::sleep_until(deadline) => {
                self.cancellation.cancel(CancellationSource::Deadline);
                self.cancellation.cancelled().await
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct AdmissionHeader<'a> {
    pub name: &'a str,
    pub value: &'a [u8],
}

#[derive(Clone, Copy)]
pub struct WebTransportAdmissionRequest<'a> {
    pub path: &'a str,
    pub authority: Option<&'a str>,
    pub origin: Option<&'a str>,
    pub secure: bool,
    pub headers: &'a [AdmissionHeader<'a>],
}

pub type WebTransportAdmission = for<'a> fn(&WebTransportAdmissionRequest<'a>) -> bool;
#[derive(Clone, Copy)]
pub struct HttpAdmissionRequest<'a> {
    pub request: &'a http::Request<()>,
    pub path: &'a str,
    pub authority: Option<&'a str>,
    pub secure: bool,
    pub headers: &'a [AdmissionHeader<'a>],
}

pub type HttpAdmission = for<'a> fn(&HttpAdmissionRequest<'a>) -> bool;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    frame_size: usize,
    connections: Option<usize>,
    streams_per_connection: Option<usize>,
    requests: Option<usize>,
    shutdown_timeout: Option<Duration>,
    initial_request_timeout: Option<Duration>,
    stream_messages: Option<usize>,
    stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    http_enabled: bool,
    http_path: String,
    http_admission: Option<HttpAdmission>,
    webtransport_enabled: bool,
    webtransport_allowed_authorities: Vec<String>,
    webtransport_allowed_origins: Vec<String>,
    webtransport_admission: Option<WebTransportAdmission>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            frame_size: DEFAULT_MAX_FRAME_SIZE,
            connections: Some(256),
            streams_per_connection: Some(64),
            requests: Some(1024),
            shutdown_timeout: Some(Duration::from_secs(30)),
            initial_request_timeout: Some(Duration::from_secs(10)),
            stream_messages: Some(4096),
            stream_body_size: Some(16 * 1024 * 1024),
            stream_idle_timeout: Some(Duration::from_secs(30)),
            http_enabled: false,
            http_path: "/trevrpc".to_owned(),
            http_admission: None,
            webtransport_enabled: false,
            webtransport_allowed_authorities: Vec::new(),
            webtransport_allowed_origins: Vec::new(),
            webtransport_admission: None,
        }
    }
}

impl ServerOptions {
    /// Creates server options with the default limits and `WebTransport` settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.frame_size
    }

    /// Returns the maximum number of concurrent transport connections.
    #[must_use]
    pub const fn max_concurrent_connections(&self) -> Option<usize> {
        self.connections
    }

    /// Returns the maximum number of concurrent streams per connection.
    #[must_use]
    pub const fn max_concurrent_streams_per_connection(&self) -> Option<usize> {
        self.streams_per_connection
    }

    /// Returns the maximum number of concurrent RPC requests.
    #[must_use]
    pub const fn max_concurrent_requests(&self) -> Option<usize> {
        self.requests
    }

    /// Returns the timeout used while draining connections during graceful shutdown.
    #[must_use]
    pub const fn graceful_shutdown_timeout(&self) -> Option<Duration> {
        self.shutdown_timeout
    }

    /// Returns the timeout allowed for receiving the initial request frame.
    #[must_use]
    pub const fn initial_request_timeout(&self) -> Option<Duration> {
        self.initial_request_timeout
    }

    /// Returns the maximum number of messages allowed on a stream.
    #[must_use]
    pub const fn max_stream_messages(&self) -> Option<usize> {
        self.stream_messages
    }

    /// Returns the maximum total body bytes allowed on a stream.
    #[must_use]
    pub const fn max_stream_body_size(&self) -> Option<usize> {
        self.stream_body_size
    }

    /// Returns the maximum idle time allowed while waiting for the next stream message.
    #[must_use]
    pub const fn stream_idle_timeout(&self) -> Option<Duration> {
        self.stream_idle_timeout
    }

    /// Returns whether ordinary HTTP/3 POST requests are accepted. Off by default.
    #[must_use]
    pub const fn http_enabled(&self) -> bool {
        self.http_enabled
    }

    /// Returns the path shared by HTTP/3 `POST` RPCs and `WebTransport` sessions, `/trevrpc` by
    /// default.
    #[must_use]
    pub fn http_path(&self) -> &str {
        &self.http_path
    }

    /// Returns the HTTP/3 POST admission callback, if configured.
    #[must_use]
    pub const fn http_admission(&self) -> Option<HttpAdmission> {
        self.http_admission
    }

    /// Returns whether `WebTransport` sessions are accepted. Off by default: every `WebTransport`
    /// CONNECT is answered with 404.
    #[must_use]
    pub const fn webtransport_enabled(&self) -> bool {
        self.webtransport_enabled
    }

    /// Returns the allowed `WebTransport` authorities, or an empty list to allow any authority.
    #[must_use]
    pub fn webtransport_allowed_authorities(&self) -> &[String] {
        &self.webtransport_allowed_authorities
    }

    /// Returns the allowed `WebTransport` origins. A request that carries an `Origin` header, as
    /// every browser does, is rejected unless its origin is listed here or an admission callback
    /// admits it; requests without an `Origin` are not affected.
    #[must_use]
    pub fn webtransport_allowed_origins(&self) -> &[String] {
        &self.webtransport_allowed_origins
    }

    /// Returns the `WebTransport` admission callback, if configured. When set, it replaces the
    /// authority and origin allowlists; the shared path is always checked first.
    #[must_use]
    pub const fn webtransport_admission(&self) -> Option<WebTransportAdmission> {
        self.webtransport_admission
    }

    /// Sets the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.frame_size = max_frame_size;
        self
    }

    /// Sets the maximum number of concurrent transport connections.
    #[must_use]
    pub const fn with_max_concurrent_connections(
        mut self,
        max_concurrent_connections: Option<usize>,
    ) -> Self {
        self.connections = max_concurrent_connections;
        self
    }

    /// Sets the maximum number of concurrent streams per connection.
    #[must_use]
    pub const fn with_max_concurrent_streams_per_connection(
        mut self,
        max_concurrent_streams_per_connection: Option<usize>,
    ) -> Self {
        self.streams_per_connection = max_concurrent_streams_per_connection;
        self
    }

    /// Sets the maximum number of concurrent RPC requests.
    #[must_use]
    pub const fn with_max_concurrent_requests(
        mut self,
        max_concurrent_requests: Option<usize>,
    ) -> Self {
        self.requests = max_concurrent_requests;
        self
    }

    /// Sets the timeout used while draining connections during graceful shutdown.
    #[must_use]
    pub const fn with_graceful_shutdown_timeout(
        mut self,
        graceful_shutdown_timeout: Option<Duration>,
    ) -> Self {
        self.shutdown_timeout = graceful_shutdown_timeout;
        self
    }

    /// Sets the timeout allowed for receiving the initial request frame.
    #[must_use]
    pub const fn with_initial_request_timeout(
        mut self,
        initial_request_timeout: Option<Duration>,
    ) -> Self {
        self.initial_request_timeout = initial_request_timeout;
        self
    }

    /// Sets the maximum number of messages allowed on a stream.
    #[must_use]
    pub const fn with_max_stream_messages(mut self, max_stream_messages: Option<usize>) -> Self {
        self.stream_messages = max_stream_messages;
        self
    }

    /// Sets the maximum total body bytes allowed on a stream.
    #[must_use]
    pub const fn with_max_stream_body_size(mut self, max_stream_body_size: Option<usize>) -> Self {
        self.stream_body_size = max_stream_body_size;
        self
    }

    /// Sets the maximum idle time allowed while waiting for the next stream message.
    #[must_use]
    pub const fn with_stream_idle_timeout(mut self, stream_idle_timeout: Option<Duration>) -> Self {
        self.stream_idle_timeout = stream_idle_timeout;
        self
    }

    /// Enables or disables ordinary HTTP/3 `POST` RPCs (disabled by default).
    ///
    /// The `h3` ALPN is advertised by [`Server::quic_config`] only when HTTP/3 or `WebTransport`
    /// is enabled.
    #[must_use]
    pub const fn with_http_enabled(mut self, http_enabled: bool) -> Self {
        self.http_enabled = http_enabled;
        self
    }

    /// Sets the path shared by HTTP/3 `POST` RPCs and `WebTransport` sessions.
    #[must_use]
    pub fn with_http_path(mut self, http_path: impl Into<String>) -> Self {
        self.http_path = http_path.into();
        self
    }

    /// Sets the HTTP/3 POST admission callback.
    #[must_use]
    pub const fn with_http_admission(mut self, http_admission: Option<HttpAdmission>) -> Self {
        self.http_admission = http_admission;
        self
    }

    /// Enables or disables `WebTransport` sessions (disabled by default).
    ///
    /// Enabled `WebTransport` still applies the shared path, then the authority and origin
    /// allowlists, or the admission callback when one is set. With no allowlists, requests that
    /// carry an `Origin` header (every browser) are denied, so browsers need an allowed origin or
    /// an admission callback.
    #[must_use]
    pub const fn with_webtransport_enabled(mut self, webtransport_enabled: bool) -> Self {
        self.webtransport_enabled = webtransport_enabled;
        self
    }

    /// Sets the `WebTransport` authorities allowed by the server.
    #[must_use]
    pub fn with_webtransport_allowed_authorities<I, S>(
        mut self,
        webtransport_allowed_authorities: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.webtransport_allowed_authorities = webtransport_allowed_authorities
            .into_iter()
            .map(Into::into)
            .collect();
        self
    }

    /// Sets the `WebTransport` origins allowed by the server.
    #[must_use]
    pub fn with_webtransport_allowed_origins<I, S>(
        mut self,
        webtransport_allowed_origins: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.webtransport_allowed_origins = webtransport_allowed_origins
            .into_iter()
            .map(Into::into)
            .collect();
        self
    }

    /// Sets the `WebTransport` admission callback.
    #[must_use]
    pub const fn with_webtransport_admission(
        mut self,
        webtransport_admission: Option<WebTransportAdmission>,
    ) -> Self {
        self.webtransport_admission = webtransport_admission;
        self
    }
}

#[crate::async_trait]
pub trait Authorizer: Send + Sync + 'static {
    /// Authorizes a request after metadata validation but before route lookup.
    /// This intentionally avoids leaking method existence to unauthenticated callers.
    async fn authorize(&self, request: &RpcRequest) -> std::result::Result<(), Status>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataValueAuthorizer {
    key: String,
    value: Vec<u8>,
}

impl MetadataValueAuthorizer {
    /// Creates an authorizer that accepts requests containing an exact metadata value.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        let key = key.into();

        Self {
            key: normalize_metadata_key(&key),
            value: value.into(),
        }
    }

    /// Creates an authorizer that checks the `authorization` metadata for a bearer token.
    #[must_use]
    pub fn bearer(token: impl AsRef<str>) -> Self {
        Self::new(
            "authorization",
            format!("Bearer {}", token.as_ref()).into_bytes(),
        )
    }
}

#[crate::async_trait]
impl Authorizer for MetadataValueAuthorizer {
    async fn authorize(&self, request: &RpcRequest) -> std::result::Result<(), Status> {
        if request
            .metadata
            .get(&self.key)
            .is_some_and(|value| constant_time_eq(value, &self.value))
        {
            Ok(())
        } else {
            Err(Status::unauthenticated("request is not authenticated"))
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();

    for index in 0..max_len {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left ^ right);
    }

    diff == 0
}

pub trait Metrics: Send + Sync + 'static {
    /// Returns false to skip constructing metric events for metrics implementations that ignore them.
    fn enabled(&self) -> bool {
        true
    }

    /// Called inline on the RPC task. Implementations must not block. Panics are isolated.
    fn rpc_started(&self, _event: &RpcStarted) {}

    /// Called inline on the RPC task. Implementations must not block. Panics are isolated.
    fn rpc_finished(&self, _event: &RpcFinished) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn enabled(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcStarted {
    pub service: String,
    pub method: String,
    pub request_body_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcFinished {
    pub service: String,
    pub method: String,
    pub request_body_len: usize,
    pub response_body_len: usize,
    pub code: Code,
    pub elapsed: Duration,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            routes: Arc::new(HashMap::new()),
            options: ServerOptions::default(),
            authorizer: None,
            metrics: Arc::new(NoopMetrics),
        }
    }
}

impl Server {
    /// Creates an empty server with default options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.options.frame_size
    }

    /// Returns the server options.
    #[must_use]
    pub const fn options(&self) -> &ServerOptions {
        &self.options
    }

    /// Replaces all server options.
    pub fn set_options(&mut self, options: ServerOptions) -> &mut Self {
        self.options = options;
        self
    }

    /// Installs an authorizer that runs before route lookup.
    pub fn set_authorizer<A>(&mut self, authorizer: A) -> &mut Self
    where
        A: Authorizer,
    {
        self.authorizer = Some(Arc::new(authorizer));
        self
    }

    /// Removes any configured authorizer.
    pub fn clear_authorizer(&mut self) -> &mut Self {
        self.authorizer = None;
        self
    }

    /// Installs metrics callbacks for RPC lifecycle events.
    pub fn set_metrics<M>(&mut self, metrics: M) -> &mut Self
    where
        M: Metrics,
    {
        self.metrics = Arc::new(metrics);
        self
    }

    /// Returns the number of registered routes.
    #[must_use]
    pub fn route_count(&self) -> usize {
        self.routes.values().map(HashMap::len).sum()
    }

    /// Registers a unary route handler for a service and method.
    pub fn route<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        handler: F,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.route_with_context(service, method, move |_context, body| handler(body));
    }

    /// Registers a unary route handler that receives request context.
    pub fn route_with_context<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.route_envelope_with_context(service, method, move |context, body| {
            let future = handler(context, body);
            async move { future.await.map(ResponseEnvelope::new) }
        });
    }

    /// Registers a unary route handler that can attach response headers and successful trailing
    /// metadata.
    pub fn route_envelope_with_context<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ResponseEnvelope<Vec<u8>>>> + Send + 'static,
    {
        Arc::make_mut(&mut self.routes)
            .entry(service.into())
            .or_default()
            .insert(
                method.into(),
                Route::Unary(Arc::new(move |context, body| {
                    Box::pin(handler(context, body))
                })),
            );
    }

    /// Registers a streaming route handler for a service, method, and RPC kind.
    pub fn route_streaming<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        kind: RpcKind,
        handler: F,
    ) where
        F: Fn(Vec<u8>, BoxStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxStream<Vec<u8>>>> + Send + 'static,
    {
        self.route_streaming_with_context(service, method, kind, move |_context, body, stream| {
            handler(body, stream)
        });
    }

    /// Registers a streaming route handler that receives request context.
    pub fn route_streaming_with_context<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        kind: RpcKind,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>, BoxStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxStream<Vec<u8>>>> + Send + 'static,
    {
        self.route_streaming_envelope_with_context(
            service,
            method,
            kind,
            move |context, body, stream| {
                let future = handler(context, body, stream);
                async move { future.await.map(ResponseEnvelope::new) }
            },
        );
    }

    /// Registers a streaming route handler that can attach response headers, sent before the first
    /// message, and terminal success metadata.
    pub fn route_streaming_envelope_with_context<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        kind: RpcKind,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>, BoxStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ResponseEnvelope<BoxStream<Vec<u8>>>>> + Send + 'static,
    {
        Arc::make_mut(&mut self.routes)
            .entry(service.into())
            .or_default()
            .insert(
                method.into(),
                Route::Streaming {
                    kind,
                    handler: Arc::new(move |context, body, stream| {
                        Box::pin(handler(context, body, stream))
                    }),
                },
            );
    }

    fn route_for(&self, service: &str, method: &str) -> Option<Route> {
        self.routes
            .get(service)
            .and_then(|methods| methods.get(method))
            .cloned()
    }

    /// Handles a unary RPC request whose single message is `body`.
    ///
    /// Returns the response envelope on success, or the status the call failed with.
    pub async fn handle_request(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
    ) -> std::result::Result<ResponseEnvelope<Vec<u8>>, Status> {
        self.handle_request_with_cancellation(request, body, CancellationToken::new())
            .await
    }

    pub(crate) async fn handle_request_with_cancellation(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        cancellation: CancellationToken,
    ) -> std::result::Result<ResponseEnvelope<Vec<u8>>, Status> {
        let started_at = Instant::now();
        let request_body_len = body.len();

        record_rpc_started(
            self.metrics.as_ref(),
            &request.service,
            &request.method,
            request_body_len,
        );

        tracing::info!(
            service = %request.service,
            method = %request.method,
            request_body_len,
            metadata_count = request.metadata.len(),
            "rpc started"
        );

        let deadline = match self.prepare_request(&request, &cancellation).await {
            Ok(deadline) => deadline,
            Err(status) => {
                return self.finish_response(
                    &request.service,
                    &request.method,
                    request_body_len,
                    started_at,
                    Err(status),
                );
            }
        };

        let Some(Route::Unary(handler)) = self.route_for(&request.service, &request.method) else {
            return self.finish_response(
                &request.service,
                &request.method,
                request_body_len,
                started_at,
                Err(Status::unimplemented(format!(
                    "unknown RPC method {}/{}",
                    request.service, request.method
                ))),
            );
        };
        let kind = request.rpc_kind();
        let RpcRequest {
            service,
            method,
            metadata,
            ..
        } = request;
        let context = RequestContext {
            service: service.clone(),
            method: method.clone(),
            kind,
            metadata,
            deadline,
            cancellation: cancellation.clone(),
        };

        let result = match invoke_unary_handler(&handler, context, body) {
            Ok(future) => match run_handler_with_deadline(future, deadline, &cancellation).await {
                Ok(Ok(response)) => validate_response_metadata(response),
                Ok(Err(error)) => Err(error.into_status()),
                Err(status) => Err(status),
            },
            Err(status) => Err(status),
        };

        self.finish_response(&service, &method, request_body_len, started_at, result)
    }

    /// Handles a streaming RPC request and returns response frames.
    ///
    /// `body` is the single request message of a server-streaming call and empty for the other
    /// kinds, whose messages arrive on `request_body`.
    pub async fn handle_streaming_request(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        request_body: BoxStream<Vec<u8>>,
    ) -> BoxStream<ResponseFrame> {
        self.handle_streaming_request_with_cancellation(
            request,
            body,
            request_body,
            CancellationToken::new(),
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn handle_streaming_request_with_cancellation(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        request_body: BoxStream<Vec<u8>>,
        cancellation: CancellationToken,
    ) -> BoxStream<ResponseFrame> {
        let started_at = Instant::now();
        let request_body_len = body.len();

        record_rpc_started(
            self.metrics.as_ref(),
            &request.service,
            &request.method,
            request_body_len,
        );

        tracing::info!(
            service = %request.service,
            method = %request.method,
            request_body_len,
            metadata_count = request.metadata.len(),
            kind = ?request.rpc_kind(),
            "streaming rpc started"
        );

        let deadline = match self.prepare_request(&request, &cancellation).await {
            Ok(deadline) => deadline,
            Err(status) => {
                return self.finish_streaming_status(
                    &request.service,
                    &request.method,
                    request_body_len,
                    started_at,
                    status,
                );
            }
        };
        let stream_limits = StreamLimits::from_options(self.options(), deadline);
        let request_body =
            limit_stream(request_body, stream_limits, "request", cancellation.clone());

        let Some(Route::Streaming { kind, handler }) =
            self.route_for(&request.service, &request.method)
        else {
            let status = Status::unimplemented(format!(
                "unknown streaming RPC method {}/{}",
                request.service, request.method
            ));
            return self.finish_streaming_status(
                &request.service,
                &request.method,
                request_body_len,
                started_at,
                status,
            );
        };

        let request_kind = request.rpc_kind();
        if request_kind != kind {
            let status = Status::invalid_argument(format!(
                "streaming RPC kind mismatch for {}/{}: expected {:?}, got {:?}",
                request.service, request.method, kind, request_kind
            ));
            return self.finish_streaming_status(
                &request.service,
                &request.method,
                request_body_len,
                started_at,
                status,
            );
        }

        let RpcRequest {
            service,
            method,
            metadata,
            ..
        } = request;
        let context = RequestContext {
            service: service.clone(),
            method: method.clone(),
            kind: request_kind,
            metadata,
            deadline,
            cancellation: cancellation.clone(),
        };
        let handler_result = match invoke_streaming_handler(&handler, context, body, request_body) {
            Ok(future) => run_handler_with_deadline(future, deadline, &cancellation).await,
            Err(status) => Err(status),
        };

        match handler_result {
            Err(status) => self.finish_streaming_status(
                &service,
                &method,
                request_body_len,
                started_at,
                status,
            ),
            Ok(Ok(response)) => {
                let response = match validate_response_metadata(response) {
                    Ok(response) => response,
                    Err(status) => {
                        return self.finish_streaming_status(
                            &service,
                            &method,
                            request_body_len,
                            started_at,
                            status,
                        );
                    }
                };
                let (response_body, headers, metadata) = response.into_parts();
                server_response_stream(
                    response_body,
                    headers,
                    metadata,
                    StreamingCompletion {
                        metrics: Arc::clone(&self.metrics),
                        service,
                        method,
                        request_body_len,
                        started_at,
                    },
                    stream_limits,
                    cancellation,
                )
            }
            Ok(Err(error)) => {
                let status = error.into_status();
                self.finish_streaming_status(
                    &service,
                    &method,
                    request_body_len,
                    started_at,
                    status,
                )
            }
        }
    }

    async fn prepare_request(
        &self,
        request: &RpcRequest,
        cancellation: &CancellationToken,
    ) -> std::result::Result<Option<Instant>, Status> {
        validate_metadata(&request.metadata)?;
        request.validate_protocol()?;
        let deadline = request_deadline(request)?;

        if let Some(authorizer) = &self.authorizer {
            match with_deadline(authorizer.authorize(request), deadline).await {
                Ok(result) => result?,
                Err(status) => {
                    if status.code() == Code::DeadlineExceeded {
                        cancellation.cancel(CancellationSource::Deadline);
                    }
                    return Err(status);
                }
            }
        }

        Ok(deadline)
    }

    fn finish_response(
        &self,
        service: &str,
        method: &str,
        request_body_len: usize,
        started_at: Instant,
        result: std::result::Result<ResponseEnvelope<Vec<u8>>, Status>,
    ) -> std::result::Result<ResponseEnvelope<Vec<u8>>, Status> {
        let elapsed = started_at.elapsed();
        let (code, response_body_len) = match &result {
            Ok(response) => (Code::Ok, response.message().len()),
            Err(status) => (status.code(), 0),
        };

        record_rpc_finished(
            self.metrics.as_ref(),
            service,
            method,
            request_body_len,
            response_body_len,
            code,
            elapsed,
        );

        tracing::info!(
            service = %service,
            method = %method,
            request_body_len,
            response_body_len,
            status = ?code,
            elapsed_ms = elapsed.as_millis(),
            "rpc finished"
        );

        result
    }

    fn finish_streaming_response(
        &self,
        service: &str,
        method: &str,
        request_body_len: usize,
        started_at: Instant,
        response_body_len: usize,
        code: Code,
    ) {
        finish_streaming_response(
            self.metrics.as_ref(),
            service,
            method,
            request_body_len,
            started_at,
            response_body_len,
            code,
        );
    }

    fn finish_streaming_status(
        &self,
        service: &str,
        method: &str,
        request_body_len: usize,
        started_at: Instant,
        status: Status,
    ) -> BoxStream<ResponseFrame> {
        self.finish_streaming_response(
            service,
            method,
            request_body_len,
            started_at,
            0,
            status.code(),
        );
        status_stream(status)
    }

    pub(crate) fn record_rejected_request(
        &self,
        request: &RpcRequest,
        request_body_len: usize,
        status: &Status,
    ) {
        let started_at = Instant::now();
        record_rpc_started(
            self.metrics.as_ref(),
            &request.service,
            &request.method,
            request_body_len,
        );
        self.finish_streaming_response(
            &request.service,
            &request.method,
            request_body_len,
            started_at,
            0,
            status.code(),
        );
    }

    pub(crate) fn record_active_request_failure(
        &self,
        request: &RpcRequest,
        request_body_len: usize,
        status: &Status,
    ) {
        self.finish_streaming_response(
            &request.service,
            &request.method,
            request_body_len,
            Instant::now(),
            0,
            status.code(),
        );
    }

    pub(crate) fn record_pre_handler_failure(&self, status: &Status) {
        let started_at = Instant::now();
        record_rpc_started(self.metrics.as_ref(), "", "", 0);
        self.finish_streaming_response("", "", 0, started_at, 0, status.code());
    }
}

async fn with_deadline<T, F>(future: F, deadline: Option<Instant>) -> std::result::Result<T, Status>
where
    F: Future<Output = T>,
{
    match remaining_deadline(deadline)? {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| Status::deadline_exceeded("RPC deadline exceeded")),
        None => Ok(future.await),
    }
}

fn invoke_unary_handler(
    handler: &UnaryHandler,
    context: RequestContext,
    body: Vec<u8>,
) -> std::result::Result<UnaryHandlerFuture, Status> {
    catch_unwind(AssertUnwindSafe(|| handler(context, body)))
        .map_err(|_| Status::internal("RPC handler panicked"))
}

fn invoke_streaming_handler(
    handler: &StreamingHandler,
    context: RequestContext,
    body: Vec<u8>,
    request_body: BoxStream<Vec<u8>>,
) -> std::result::Result<StreamingHandlerFuture, Status> {
    catch_unwind(AssertUnwindSafe(|| handler(context, body, request_body)))
        .map_err(|_| Status::internal("RPC handler panicked"))
}

async fn run_handler_with_deadline<T, F>(
    future: F,
    deadline: Option<Instant>,
    cancellation: &CancellationToken,
) -> std::result::Result<T, Status>
where
    F: Future<Output = T> + Unpin,
{
    let result = with_deadline(CatchUnwindFuture::new(future), deadline).await;
    match result {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(_)) => Err(Status::internal("RPC handler panicked")),
        Err(status) => {
            if status.code() == Code::DeadlineExceeded {
                cancellation.cancel(CancellationSource::Deadline);
            }
            Err(status)
        }
    }
}

struct CatchUnwindFuture<F> {
    future: F,
}

impl<F> CatchUnwindFuture<F> {
    const fn new(future: F) -> Self {
        Self { future }
    }
}

impl<F> Future for CatchUnwindFuture<F>
where
    F: Future + Unpin,
{
    type Output = std::thread::Result<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = &mut self.future;

        match catch_unwind(AssertUnwindSafe(|| Pin::new(future).poll(cx))) {
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

fn request_deadline(request: &RpcRequest) -> std::result::Result<Option<Instant>, Status> {
    if request.timeout_nanos == 0 {
        return Ok(None);
    }

    if request.timeout_nanos > i64::MAX.unsigned_abs() {
        return Err(Status::invalid_argument("RPC timeout is too large"));
    }

    Instant::now()
        .checked_add(Duration::from_nanos(request.timeout_nanos))
        .map(Some)
        .ok_or_else(|| Status::invalid_argument("RPC timeout overflowed"))
}

fn remaining_deadline(deadline: Option<Instant>) -> std::result::Result<Option<Duration>, Status> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };

    deadline
        .checked_duration_since(Instant::now())
        .map(Some)
        .ok_or_else(|| Status::deadline_exceeded("RPC deadline exceeded"))
}

fn status_stream(status: Status) -> BoxStream<ResponseFrame> {
    crate::stream::from_iter([ResponseFrame::Status(status)])
}

/// Rejects a successful response whose header or trailing metadata is invalid.
fn validate_response_metadata<T>(
    response: ResponseEnvelope<T>,
) -> std::result::Result<ResponseEnvelope<T>, Status> {
    validate_metadata(response.headers())
        .and_then(|()| validate_metadata(response.metadata()))
        .map_err(|_| Status::internal("invalid successful response metadata"))?;
    Ok(response)
}

#[derive(Clone, Copy)]
struct StreamLimits {
    max_messages: Option<usize>,
    max_body_size: Option<usize>,
    idle_timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl StreamLimits {
    const fn from_options(options: &ServerOptions, deadline: Option<Instant>) -> Self {
        Self {
            max_messages: options.max_stream_messages(),
            max_body_size: options.max_stream_body_size(),
            idle_timeout: options.stream_idle_timeout(),
            deadline,
        }
    }
}

struct LimitedStream {
    inner: BoxStream<Vec<u8>>,
    limits: StreamLimits,
    direction: &'static str,
    cancellation: CancellationToken,
    messages: usize,
    body_size: usize,
    done: bool,
}

fn limit_stream(
    inner: BoxStream<Vec<u8>>,
    limits: StreamLimits,
    direction: &'static str,
    cancellation: CancellationToken,
) -> BoxStream<Vec<u8>> {
    Box::pin(futures_util::stream::unfold(
        LimitedStream {
            inner,
            limits,
            direction,
            cancellation,
            messages: 0,
            body_size: 0,
            done: false,
        },
        |mut stream| async move {
            if stream.done {
                return None;
            }

            let body = match next_limited_item(
                &mut stream.inner,
                stream.limits,
                stream.direction,
                &stream.cancellation,
            )
            .await
            {
                Ok(Some(body)) => body,
                Ok(None) => return None,
                Err(error) => {
                    stream.done = true;
                    return Some((Err(error), stream));
                }
            };

            let item = match check_stream_limits(
                stream.direction,
                stream.limits,
                &mut stream.messages,
                &mut stream.body_size,
                body.len(),
            ) {
                Ok(()) => Ok(body),
                Err(error) => {
                    stream.done = true;
                    Err(error)
                }
            };

            Some((item, stream))
        },
    ))
}

async fn next_limited_item(
    inner: &mut BoxStream<Vec<u8>>,
    limits: StreamLimits,
    direction: &'static str,
    cancellation: &CancellationToken,
) -> Result<Option<Vec<u8>>> {
    let timeout = match stream_next_timeout(limits.deadline, limits.idle_timeout) {
        Ok(timeout) => timeout,
        Err(status) => {
            cancellation.cancel(CancellationSource::Deadline);
            return Err(Error::from(status));
        }
    };

    match timeout {
        Some((timeout, reason)) => tokio::time::timeout(timeout, inner.next())
            .await
            .map_err(|_| {
                reason.cancel(cancellation);
                Error::from(reason.status(direction))
            })?
            .transpose(),
        None => inner.next().await.transpose(),
    }
}

fn check_stream_limits(
    direction: &str,
    limits: StreamLimits,
    messages: &mut usize,
    body_size: &mut usize,
    item_len: usize,
) -> Result<()> {
    check_stream_message_body_limits(
        direction,
        limits.max_messages,
        limits.max_body_size,
        messages,
        body_size,
        item_len,
    )
}

pub(crate) fn check_stream_message_body_limits(
    direction: &str,
    max_messages: Option<usize>,
    max_body_size: Option<usize>,
    messages: &mut usize,
    body_size: &mut usize,
    item_len: usize,
) -> Result<()> {
    if let Some(max) = max_messages
        && *messages >= max
    {
        return Err(Error::from(Status::resource_exhausted(format!(
            "{direction} stream exceeded maximum of {max} messages"
        ))));
    }

    *messages = messages.saturating_add(1);
    check_stream_body_limit(direction, max_body_size, body_size, item_len)
}

/// Adds `len` bytes to a stream's running body size and checks it against the limit.
pub(crate) fn check_stream_body_limit(
    direction: &str,
    max_body_size: Option<usize>,
    body_size: &mut usize,
    len: usize,
) -> Result<()> {
    *body_size = body_size.saturating_add(len);

    if let Some(max) = max_body_size
        && *body_size > max
    {
        return Err(Error::from(Status::resource_exhausted(format!(
            "{direction} stream exceeded maximum body size of {max} bytes"
        ))));
    }

    Ok(())
}

/// What a request stream may still send: a count of messages and a total of bytes.
///
/// Frames of unknown type count toward the byte total, so they cannot hold a stream open for free.
/// The single message of a unary or server-streaming call is bounded by the frame size instead
/// and is not charged.
#[derive(Debug, Clone)]
pub struct RequestBudget {
    max_messages: Option<usize>,
    max_body_size: Option<usize>,
    messages: usize,
    body_size: usize,
}

impl RequestBudget {
    pub(crate) const fn new(max_messages: Option<usize>, max_body_size: Option<usize>) -> Self {
        Self {
            max_messages,
            max_body_size,
            messages: 0,
            body_size: 0,
        }
    }

    pub(crate) const fn from_options(options: &ServerOptions) -> Self {
        Self::new(
            options.max_stream_messages(),
            options.max_stream_body_size(),
        )
    }

    /// Charges one streamed request message.
    pub(crate) fn charge_message(&mut self, len: usize) -> Result<()> {
        check_stream_message_body_limits(
            "request",
            self.max_messages,
            self.max_body_size,
            &mut self.messages,
            &mut self.body_size,
            len,
        )
    }

    /// Charges a skipped frame by its wire length.
    pub(crate) fn charge_skipped(&mut self, wire_len: usize) -> Result<()> {
        check_stream_body_limit("request", self.max_body_size, &mut self.body_size, wire_len)
    }
}

#[derive(Clone, Copy)]
enum StreamTimeoutReason {
    Deadline,
    Idle,
}

impl StreamTimeoutReason {
    fn status(self, direction: &str) -> Status {
        match self {
            Self::Deadline => Status::deadline_exceeded("RPC deadline exceeded"),
            Self::Idle => Status::unavailable(format!("{direction} stream idle timeout")),
        }
    }

    fn cancel(self, cancellation: &CancellationToken) {
        if matches!(self, Self::Deadline) {
            cancellation.cancel(CancellationSource::Deadline);
        }
    }
}

fn stream_next_timeout(
    deadline: Option<Instant>,
    idle_timeout: Option<Duration>,
) -> std::result::Result<Option<(Duration, StreamTimeoutReason)>, Status> {
    let deadline =
        remaining_deadline(deadline)?.map(|timeout| (timeout, StreamTimeoutReason::Deadline));
    let idle = idle_timeout.map(|timeout| (timeout, StreamTimeoutReason::Idle));

    Ok(match (deadline, idle) {
        (Some(deadline), Some(idle)) if deadline.0 <= idle.0 => Some(deadline),
        (Some(_) | None, Some(idle)) => Some(idle),
        (Some(deadline), None) => Some(deadline),
        (None, None) => None,
    })
}

struct StreamingCompletion {
    metrics: Arc<dyn Metrics>,
    service: String,
    method: String,
    request_body_len: usize,
    started_at: Instant,
}

impl StreamingCompletion {
    fn finish(&self, response_body_len: usize, code: Code) {
        finish_streaming_response(
            self.metrics.as_ref(),
            &self.service,
            &self.method,
            self.request_body_len,
            self.started_at,
            response_body_len,
            code,
        );
    }
}

struct ServerResponseStream {
    inner: BoxStream<Vec<u8>>,
    /// Response headers still to send. Empty headers are never sent.
    headers: Option<Metadata>,
    metadata: Metadata,
    completion: StreamingCompletion,
    response_body_len: usize,
    limits: StreamLimits,
    cancellation: CancellationToken,
    messages: usize,
    done: bool,
}

impl ServerResponseStream {
    fn new(
        inner: BoxStream<Vec<u8>>,
        headers: Metadata,
        metadata: Metadata,
        completion: StreamingCompletion,
        limits: StreamLimits,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            inner,
            headers: (!headers.is_empty()).then_some(headers),
            metadata,
            completion,
            response_body_len: 0,
            limits,
            cancellation,
            messages: 0,
            done: false,
        }
    }

    fn finish(&mut self, code: Code) {
        self.done = true;
        self.completion.finish(self.response_body_len, code);
    }
}

/// Frames a handler's response stream: its headers first, as soon as the handler returns and
/// before the first message is polled, then its messages, then the terminal status.
fn server_response_stream(
    inner: BoxStream<Vec<u8>>,
    headers: Metadata,
    metadata: Metadata,
    completion: StreamingCompletion,
    limits: StreamLimits,
    cancellation: CancellationToken,
) -> BoxStream<ResponseFrame> {
    Box::pin(futures_util::stream::unfold(
        ServerResponseStream::new(inner, headers, metadata, completion, limits, cancellation),
        |mut stream| async move {
            if stream.done {
                return None;
            }

            if let Some(headers) = stream.headers.take() {
                return Some((Ok(ResponseFrame::Headers(headers)), stream));
            }

            let frame = match next_limited_item(
                &mut stream.inner,
                stream.limits,
                "response",
                &stream.cancellation,
            )
            .await
            {
                Ok(Some(body)) => {
                    if let Err(error) = check_stream_limits(
                        "response",
                        stream.limits,
                        &mut stream.messages,
                        &mut stream.response_body_len,
                        body.len(),
                    ) {
                        let status = error.into_status();
                        stream.finish(status.code());
                        ResponseFrame::Status(status)
                    } else {
                        ResponseFrame::message(body)
                    }
                }
                Err(error) => {
                    let status = error.into_status();
                    stream.finish(status.code());
                    ResponseFrame::Status(status)
                }
                Ok(None) => {
                    stream.finish(Code::Ok);
                    ResponseFrame::Status(
                        Status::ok().with_metadata(std::mem::take(&mut stream.metadata)),
                    )
                }
            };

            Some((Ok(frame), stream))
        },
    ))
}

impl Drop for ServerResponseStream {
    fn drop(&mut self) {
        if !self.done {
            self.finish(
                self.cancellation
                    .completion_code()
                    .unwrap_or(Code::Cancelled),
            );
        }
    }
}

fn finish_streaming_response(
    metrics: &dyn Metrics,
    service: &str,
    method: &str,
    request_body_len: usize,
    started_at: Instant,
    response_body_len: usize,
    code: Code,
) {
    let elapsed = started_at.elapsed();

    record_rpc_finished(
        metrics,
        service,
        method,
        request_body_len,
        response_body_len,
        code,
        elapsed,
    );

    tracing::info!(
        service = %service,
        method = %method,
        request_body_len,
        response_body_len,
        status = ?code,
        elapsed_ms = elapsed.as_millis(),
        "streaming rpc finished"
    );
}

fn record_rpc_started(metrics: &dyn Metrics, service: &str, method: &str, request_body_len: usize) {
    if !metrics.enabled() {
        return;
    }

    let event = RpcStarted {
        service: service.to_owned(),
        method: method.to_owned(),
        request_body_len,
    };
    let _ = catch_unwind(AssertUnwindSafe(|| metrics.rpc_started(&event)));
}

fn record_rpc_finished(
    metrics: &dyn Metrics,
    service: &str,
    method: &str,
    request_body_len: usize,
    response_body_len: usize,
    code: Code,
    elapsed: Duration,
) {
    if !metrics.enabled() {
        return;
    }

    let event = RpcFinished {
        service: service.to_owned(),
        method: method.to_owned(),
        request_body_len,
        response_body_len,
        code,
        elapsed,
    };
    let _ = catch_unwind(AssertUnwindSafe(|| metrics.rpc_finished(&event)));
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures_util::StreamExt;

    use crate::{Code, Error, ResponseFrame, RpcKind, RpcRequest, Status};

    use crate::{Metadata, ResponseEnvelope};

    use super::{
        Authorizer, CancellationSource, CancellationToken, MetadataValueAuthorizer, Metrics,
        RequestContext, RpcFinished, Server, ServerOptions, StreamLimits, check_stream_limits,
    };

    #[derive(Clone, Default)]
    struct TestMetrics {
        started: Arc<Mutex<usize>>,
        finished: Arc<Mutex<Vec<Code>>>,
    }

    impl Metrics for TestMetrics {
        fn rpc_started(&self, _event: &super::RpcStarted) {
            let mut started = self
                .started
                .lock()
                .expect("metrics lock should not be poisoned");
            *started = started.saturating_add(1);
        }

        fn rpc_finished(&self, event: &RpcFinished) {
            self.finished
                .lock()
                .expect("metrics lock should not be poisoned")
                .push(event.code);
        }
    }

    impl TestMetrics {
        fn started_count(&self) -> usize {
            *self
                .started
                .lock()
                .expect("metrics lock should not be poisoned")
        }

        fn finished_codes(&self) -> Vec<Code> {
            self.finished
                .lock()
                .expect("metrics lock should not be poisoned")
                .clone()
        }
    }

    struct PanickingMetrics;

    impl Metrics for PanickingMetrics {
        fn rpc_started(&self, _event: &super::RpcStarted) {
            panic!("started");
        }

        fn rpc_finished(&self, _event: &RpcFinished) {
            panic!("finished");
        }
    }

    struct PendingAuthorizer;

    #[crate::async_trait]
    impl Authorizer for PendingAuthorizer {
        async fn authorize(&self, _request: &RpcRequest) -> std::result::Result<(), Status> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn metadata_authorizer_allows_matching_requests() {
        let metrics = TestMetrics::default();
        let finished = Arc::clone(&metrics.finished);
        let mut server = Server::new();
        server.set_authorizer(MetadataValueAuthorizer::new(
            "Authorization",
            b"ok".to_vec(),
        ));
        server.set_metrics(metrics);
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let mut request = RpcRequest::new("example.Greeter", "SayHello");
        request
            .metadata
            .insert("authorization".to_owned(), b"ok".to_vec());

        let response = server
            .handle_request(request, Vec::new())
            .await
            .expect("authorized request should succeed");

        assert_eq!(response.message(), b"hello");
        assert_eq!(
            *finished
                .lock()
                .expect("metrics lock should not be poisoned"),
            vec![Code::Ok]
        );
    }

    #[tokio::test]
    async fn metadata_authorizer_rejects_missing_metadata() {
        let mut server = Server::new();
        server.set_authorizer(MetadataValueAuthorizer::new(
            "authorization",
            b"ok".to_vec(),
        ));
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let status = server
            .handle_request(RpcRequest::new("example.Greeter", "SayHello"), Vec::new())
            .await
            .expect_err("missing metadata should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn metrics_panics_do_not_fail_request() {
        let mut server = Server::new();
        server.set_metrics(PanickingMetrics);
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let response = server
            .handle_request(RpcRequest::new("example.Greeter", "SayHello"), Vec::new())
            .await
            .expect("request should succeed despite panicking metrics");

        assert_eq!(response.message(), b"hello");
    }

    #[tokio::test]
    async fn unary_completion_metrics_are_exactly_once_for_terminal_statuses() {
        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "SayHello", |_| async {
                    Ok(b"hello".to_vec())
                });
            },
            RpcRequest::new("example.Greeter", "SayHello"),
            Code::Ok,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "Denied", |_| async {
                    Err(Error::from(Status::new(Code::PermissionDenied, "denied")))
                });
            },
            RpcRequest::new("example.Greeter", "Denied"),
            Code::PermissionDenied,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "Panic", |_| async {
                    panic!("boom");
                });
            },
            RpcRequest::new("example.Greeter", "Panic"),
            Code::Internal,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.set_authorizer(MetadataValueAuthorizer::bearer("token"));
                server.route("example.Greeter", "SayHello", |_| async {
                    Ok(b"hello".to_vec())
                });
            },
            RpcRequest::new("example.Greeter", "SayHello"),
            Code::Unauthenticated,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "Slow", |_| async {
                    std::future::pending::<crate::Result<Vec<u8>>>().await
                });
            },
            RpcRequest::new("example.Greeter", "Slow").with_timeout_nanos(1_000_000),
            Code::DeadlineExceeded,
        )
        .await;

        let mut invalid_metadata = RpcRequest::new("example.Greeter", "SayHello");
        invalid_metadata
            .metadata
            .insert("Authorization".to_owned(), b"ok".to_vec());
        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "SayHello", |_| async {
                    Ok(b"hello".to_vec())
                });
            },
            invalid_metadata,
            Code::InvalidArgument,
        )
        .await;
    }

    async fn expect_unary_completion(
        configure: impl FnOnce(&mut Server),
        request: RpcRequest,
        expected: Code,
    ) {
        let metrics = TestMetrics::default();
        let observed = metrics.clone();
        let mut server = Server::new();
        server.set_metrics(metrics);
        configure(&mut server);

        let code = server
            .handle_request(request, Vec::new())
            .await
            .map_or_else(|status| status.code(), |_| Code::Ok);

        assert_eq!(code, expected);
        assert_eq!(observed.started_count(), 1);
        assert_eq!(observed.finished_codes(), vec![expected]);
    }

    #[tokio::test]
    async fn unary_handler_panics_return_internal() {
        let mut server = Server::new();
        server.route("example.Greeter", "Panic", |_| async {
            panic!("boom");
        });

        let status = server
            .handle_request(RpcRequest::new("example.Greeter", "Panic"), Vec::new())
            .await
            .expect_err("a panicking handler should fail");

        assert_eq!(status.code(), Code::Internal);
    }

    #[tokio::test]
    async fn unary_handler_panics_after_yield_return_internal() {
        let mut server = Server::new();
        server.route("example.Greeter", "Panic", |_| async {
            tokio::task::yield_now().await;
            panic!("boom");
        });

        let status = server
            .handle_request(RpcRequest::new("example.Greeter", "Panic"), Vec::new())
            .await
            .expect_err("a panicking handler should fail");

        assert_eq!(status.code(), Code::Internal);
    }

    #[tokio::test]
    async fn streaming_handler_panics_return_internal_status() {
        let mut server = Server::new();
        server.route_streaming(
            "example.Greeter",
            "Panic",
            RpcKind::ServerStreaming,
            |_, _| async {
                panic!("boom");
            },
        );
        let request =
            RpcRequest::new("example.Greeter", "Panic").with_kind(RpcKind::ServerStreaming);

        let mut response = server
            .handle_streaming_request(request, Vec::new(), crate::stream::empty())
            .await;
        let frame = response
            .next()
            .await
            .expect("status frame should be emitted")
            .expect("status frame should not be transport error");

        assert_eq!(status_code(&frame), Some(Code::Internal));
    }

    #[tokio::test]
    async fn authorizer_is_bounded_by_rpc_timeout() {
        let metrics = TestMetrics::default();
        let finished = Arc::clone(&metrics.finished);
        let mut server = Server::new();
        server.set_authorizer(PendingAuthorizer);
        server.set_metrics(metrics);
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });
        let request = RpcRequest::new("example.Greeter", "SayHello").with_timeout_nanos(1_000_000);

        let status = server
            .handle_request(request, Vec::new())
            .await
            .expect_err("the authorizer should hit the deadline");

        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(
            *finished
                .lock()
                .expect("metrics lock should not be poisoned"),
            vec![Code::DeadlineExceeded]
        );
    }

    #[tokio::test]
    async fn invalid_metadata_is_rejected_before_authorization() {
        let metrics = TestMetrics::default();
        let finished = Arc::clone(&metrics.finished);
        let mut server = Server::new();
        server.set_authorizer(MetadataValueAuthorizer::new(
            "authorization",
            b"ok".to_vec(),
        ));
        server.set_metrics(metrics);
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let mut request = RpcRequest::new("example.Greeter", "SayHello");
        request
            .metadata
            .insert("Authorization".to_owned(), b"ok".to_vec());

        let status = server
            .handle_request(request, Vec::new())
            .await
            .expect_err("invalid metadata should be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            *finished
                .lock()
                .expect("metrics lock should not be poisoned"),
            vec![Code::InvalidArgument]
        );
    }

    #[tokio::test]
    async fn unauthenticated_requests_do_not_leak_unknown_methods() {
        let mut server = Server::new();
        server.set_authorizer(MetadataValueAuthorizer::new(
            "authorization",
            b"ok".to_vec(),
        ));

        let status = server
            .handle_request(RpcRequest::new("example.Greeter", "Missing"), Vec::new())
            .await
            .expect_err("unauthenticated requests should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn unsupported_wire_versions_are_rejected_before_route_lookup() {
        let mut server = Server::new();
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });
        let mut request = RpcRequest::new("example.Greeter", "Missing");
        request.version = crate::wire::WIRE_VERSION + 1;

        let status = server
            .handle_request(request, Vec::new())
            .await
            .expect_err("the version should be rejected");

        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn unsupported_rpc_kinds_are_rejected_before_route_lookup() {
        let server = Server::new();
        let mut request = RpcRequest::new("example.Greeter", "Missing");
        request.kind = 99;

        let status = server
            .handle_request(request, Vec::new())
            .await
            .expect_err("the kind should be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn cancellation_token_is_cloneable_and_first_writer_wins() {
        let cancellation = CancellationToken::new();
        let observer = cancellation.clone();

        assert!(cancellation.cancel(CancellationSource::PeerReset));
        assert!(!observer.cancel(CancellationSource::ServerShutdown));
        assert_eq!(observer.source(), Some(CancellationSource::PeerReset));
        assert_eq!(observer.cancelled().await, CancellationSource::PeerReset);
    }

    #[tokio::test]
    async fn cancellation_subscription_retains_cancel_before_await() {
        let cancellation = CancellationToken::new();
        let mut observer = cancellation.subscribe();

        assert!(cancellation.cancel(CancellationSource::PeerReset));

        tokio::time::timeout(Duration::from_secs(1), observer.changed())
            .await
            .expect("subscribed cancellation must not be lost before await")
            .expect("cancellation sender is retained by the token");
        assert_eq!(*observer.borrow(), Some(CancellationSource::PeerReset));
    }

    #[tokio::test]
    async fn cancellation_token_wakes_all_subscribers_without_lost_notifications() {
        let cancellation = CancellationToken::new();
        let mut observers = Vec::new();
        for _ in 0..32 {
            let observer = cancellation.clone();
            observers.push(tokio::spawn(async move { observer.cancelled().await }));
        }
        tokio::task::yield_now().await;

        assert!(cancellation.cancel(CancellationSource::ConnectionLost));

        for observer in observers {
            let source = tokio::time::timeout(Duration::from_secs(1), observer)
                .await
                .expect("every cancellation subscriber should wake")
                .expect("cancellation subscriber should not panic");
            assert_eq!(source, CancellationSource::ConnectionLost);
        }
    }

    #[tokio::test]
    async fn cloned_request_context_observes_deadline_cancellation() {
        let request =
            RpcRequest::new("example.Greeter", "Watch").with_kind(RpcKind::ServerStreaming);
        let context = RequestContext::new(
            &request,
            Some(std::time::Instant::now() + Duration::from_millis(1)),
        );
        let observer = context.clone();

        let source = tokio::time::timeout(Duration::from_secs(1), observer.cancelled_signal())
            .await
            .expect("deadline cancellation should be observed promptly");

        assert_eq!(source, CancellationSource::Deadline);
        assert_eq!(
            context.cancellation_source(),
            Some(CancellationSource::Deadline)
        );
    }

    #[test]
    fn observing_an_expired_deadline_commits_the_first_source() {
        let request =
            RpcRequest::new("example.Greeter", "Watch").with_kind(RpcKind::ServerStreaming);
        let context = RequestContext::new(
            &request,
            Some(
                std::time::Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("test deadline should be representable"),
            ),
        );

        assert_eq!(
            context.cancellation_source(),
            Some(CancellationSource::Deadline)
        );
        assert!(!context.cancellation().cancel(CancellationSource::PeerReset));
        assert_eq!(
            context.cancellation_source(),
            Some(CancellationSource::Deadline)
        );
    }

    #[test]
    fn default_options_use_bounded_production_limits() {
        let options = ServerOptions::new();

        assert_eq!(options.max_frame_size(), 4 * 1024 * 1024);
        assert_eq!(options.max_concurrent_connections(), Some(256));
        assert_eq!(options.max_concurrent_streams_per_connection(), Some(64));
        assert_eq!(options.max_concurrent_requests(), Some(1024));
        assert_eq!(
            options.initial_request_timeout(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(options.max_stream_body_size(), Some(16 * 1024 * 1024));
        {
            assert!(!options.http_enabled());
            assert!(!options.webtransport_enabled());
            assert_eq!(options.http_path(), "/trevrpc");
            assert!(options.http_admission().is_none());
        }
    }

    #[test]
    fn options_own_runtime_paths_and_allowlists() {
        let http_path = String::from("/owned-http3");
        let authority = String::from("rpc.example");
        let origin = String::from("https://app.example");
        let options = ServerOptions::new()
            .with_http_path(http_path.clone())
            .with_webtransport_allowed_authorities([authority.clone()])
            .with_webtransport_allowed_origins([origin.clone()]);
        drop((http_path, authority, origin));

        assert_eq!(options.http_path(), "/owned-http3");
        assert_eq!(options.webtransport_allowed_authorities(), ["rpc.example"]);
        assert_eq!(
            options.webtransport_allowed_origins(),
            ["https://app.example"]
        );
    }

    fn allow_any_webtransport(_request: &super::WebTransportAdmissionRequest<'_>) -> bool {
        true
    }

    #[test]
    fn webtransport_admission_callback_is_configurable() {
        let options =
            ServerOptions::new().with_webtransport_admission(Some(allow_any_webtransport));

        assert!(options.webtransport_admission().is_some());
    }

    #[test]
    fn stream_limit_boundary_cases_are_stable() {
        let limits = StreamLimits {
            max_messages: Some(1),
            max_body_size: Some(3),
            idle_timeout: None,
            deadline: None,
        };
        let mut messages = 0;
        let mut body_size = 0;
        check_stream_limits("response", limits, &mut messages, &mut body_size, 3)
            .expect("first message within limits should pass");
        assert_eq!(messages, 1);
        assert_eq!(body_size, 3);
        let status = check_stream_limits("response", limits, &mut messages, &mut body_size, 0)
            .expect_err("second message should exceed message limit")
            .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);

        let limits = StreamLimits {
            max_messages: None,
            max_body_size: Some(3),
            idle_timeout: None,
            deadline: None,
        };
        let mut messages = 0;
        let mut body_size = 0;
        check_stream_limits("response", limits, &mut messages, &mut body_size, 3)
            .expect("exact body size should pass");
        let status = check_stream_limits("response", limits, &mut messages, &mut body_size, 1)
            .expect_err("body size overflow should fail")
            .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);

        let limits = StreamLimits {
            max_messages: None,
            max_body_size: None,
            idle_timeout: None,
            deadline: None,
        };
        let mut messages = usize::MAX;
        let mut body_size = usize::MAX;
        check_stream_limits("response", limits, &mut messages, &mut body_size, 1)
            .expect("unbounded counters should saturate without panicking");
        assert_eq!(messages, usize::MAX);
        assert_eq!(body_size, usize::MAX);
    }

    #[tokio::test]
    async fn oversized_timeouts_are_rejected_before_route_lookup() {
        let server = Server::new();
        let mut request = RpcRequest::new("example.Greeter", "Missing");
        request.timeout_nanos = u64::MAX;

        let status = server
            .handle_request(request, Vec::new())
            .await
            .expect_err("the timeout should be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn streaming_route_emits_messages_and_final_status() {
        let metrics = TestMetrics::default();
        let finished = Arc::clone(&metrics.finished);
        let mut server = Server::new();
        server.set_metrics(metrics);
        server.route_streaming(
            "example.Greeter",
            "StreamHello",
            RpcKind::ServerStreaming,
            |body, _requests| async move {
                assert_eq!(body, b"request".to_vec());
                Ok(crate::stream::from_iter([b"one".to_vec(), b"two".to_vec()]))
            },
        );

        let request =
            RpcRequest::new("example.Greeter", "StreamHello").with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, b"request".to_vec(), crate::stream::empty())
            .await;

        let first = response
            .next()
            .await
            .expect("stream should yield first frame")
            .expect("first frame should be ok");
        assert_eq!(first, ResponseFrame::message(&b"one"[..]));

        let second = response
            .next()
            .await
            .expect("stream should yield second frame")
            .expect("second frame should be ok");
        assert_eq!(second, ResponseFrame::message(&b"two"[..]));

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status, ResponseFrame::Status(Status::ok()));
        assert!(response.next().await.is_none());
        assert_eq!(
            *finished
                .lock()
                .expect("metrics lock should not be poisoned"),
            vec![Code::Ok]
        );
    }

    #[tokio::test]
    async fn dropped_streaming_response_records_cancelled_once() {
        let metrics = TestMetrics::default();
        let observed = metrics.clone();
        let mut server = Server::new();
        server.set_metrics(metrics);
        server.route_streaming(
            "example.Greeter",
            "Download",
            RpcKind::ServerStreaming,
            |_body, _requests| async move {
                let stream: crate::BoxStream<Vec<u8>> = Box::pin(futures_util::stream::pending());
                Ok(stream)
            },
        );

        let request =
            RpcRequest::new("example.Greeter", "Download").with_kind(RpcKind::ServerStreaming);
        let response = server
            .handle_streaming_request(request, Vec::new(), crate::stream::empty())
            .await;

        drop(response);

        assert_eq!(observed.started_count(), 1);
        assert_eq!(observed.finished_codes(), vec![Code::Cancelled]);
    }

    #[tokio::test]
    async fn request_stream_message_limit_returns_resource_exhausted() {
        let mut server = Server::new();
        server.set_options(ServerOptions::new().with_max_stream_messages(Some(1)));
        server.route_streaming(
            "example.Greeter",
            "Upload",
            RpcKind::ClientStreaming,
            |_body, mut requests| async move {
                while let Some(request) = requests.next().await {
                    request?;
                }

                Ok(crate::stream::empty())
            },
        );

        let request =
            RpcRequest::new("example.Greeter", "Upload").with_kind(RpcKind::ClientStreaming);
        let mut response = server
            .handle_streaming_request(
                request,
                Vec::new(),
                crate::stream::from_iter([b"one".to_vec(), b"two".to_vec()]),
            )
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status_code(&status), Some(Code::ResourceExhausted));
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_body_limit_returns_resource_exhausted() {
        let mut server = Server::new();
        server.set_options(ServerOptions::new().with_max_stream_body_size(Some(3)));
        server.route_streaming(
            "example.Greeter",
            "Download",
            RpcKind::ServerStreaming,
            |_body, _requests| async move { Ok(crate::stream::from_iter([b"four".to_vec()])) },
        );

        let request =
            RpcRequest::new("example.Greeter", "Download").with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, Vec::new(), crate::stream::empty())
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status_code(&status), Some(Code::ResourceExhausted));
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn request_stream_idle_timeout_returns_unavailable() {
        let mut server = Server::new();
        server.set_options(
            ServerOptions::new()
                .with_max_stream_messages(None)
                .with_stream_idle_timeout(Some(Duration::from_millis(1))),
        );
        server.route_streaming(
            "example.Greeter",
            "Upload",
            RpcKind::ClientStreaming,
            |_body, mut requests| async move {
                while let Some(request) = requests.next().await {
                    request?;
                }

                Ok(crate::stream::empty())
            },
        );

        let request =
            RpcRequest::new("example.Greeter", "Upload").with_kind(RpcKind::ClientStreaming);
        let mut response = server
            .handle_streaming_request(
                request,
                Vec::new(),
                Box::pin(futures_util::stream::pending()),
            )
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status_code(&status), Some(Code::Unavailable));
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_errors_are_converted_to_status_frames() {
        let mut server = Server::new();
        server.route_streaming(
            "example.Greeter",
            "Download",
            RpcKind::ServerStreaming,
            |_body, _requests| async move {
                let stream: crate::BoxStream<Vec<u8>> =
                    Box::pin(futures_util::stream::iter([Err(Error::from(
                        crate::Status::unavailable("down"),
                    ))]));
                Ok(stream)
            },
        );

        let request =
            RpcRequest::new("example.Greeter", "Download").with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, Vec::new(), crate::stream::empty())
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status_code(&status), Some(Code::Unavailable));
        assert!(matches!(&status, ResponseFrame::Status(status) if status.message() == "down"));
        assert!(response.next().await.is_none());
    }

    fn status_code(frame: &ResponseFrame) -> Option<Code> {
        match frame {
            ResponseFrame::Status(status) => Some(status.code()),
            ResponseFrame::Headers(_) | ResponseFrame::Message(_) => None,
        }
    }

    fn headers_of(key: &str, value: &[u8]) -> Metadata {
        Metadata::from([(key.to_owned(), value.to_vec())])
    }

    #[tokio::test]
    async fn unary_handlers_receive_the_request_message_and_return_headers() {
        let mut server = Server::new();
        server.route_envelope_with_context(
            "example.Greeter",
            "SayHello",
            |_context, body| async move {
                assert_eq!(body, b"request");
                Ok(ResponseEnvelope::new(b"reply".to_vec())
                    .with_headers(headers_of("x-header", b"h"))
                    .with_metadata(headers_of("x-trailer", b"t")))
            },
        );

        let response = server
            .handle_request(
                RpcRequest::new("example.Greeter", "SayHello"),
                b"request".to_vec(),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.message(), b"reply");
        assert_eq!(response.headers(), &headers_of("x-header", b"h"));
        assert_eq!(response.metadata(), &headers_of("x-trailer", b"t"));
    }

    struct LengthMetrics(TestMetrics, Arc<Mutex<Vec<usize>>>);

    impl Metrics for LengthMetrics {
        fn rpc_started(&self, event: &super::RpcStarted) {
            self.1
                .lock()
                .expect("lock should not be poisoned")
                .push(event.request_body_len);
            self.0.rpc_started(event);
        }
    }

    #[tokio::test]
    async fn metrics_see_the_request_message_length() {
        let metrics = TestMetrics::default();
        let started = Arc::new(Mutex::new(Vec::new()));
        let mut server = Server::new();
        server.set_metrics(LengthMetrics(metrics, Arc::clone(&started)));
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        server
            .handle_request(
                RpcRequest::new("example.Greeter", "SayHello"),
                b"four".to_vec(),
            )
            .await
            .expect("request should succeed");

        assert_eq!(*started.lock().expect("lock should not be poisoned"), [4]);
    }

    #[tokio::test]
    async fn invalid_unary_header_or_trailing_metadata_is_internal() {
        for envelope in [
            ResponseEnvelope::new(Vec::new()).with_headers(headers_of("Bad Key", b"h")),
            ResponseEnvelope::new(Vec::new()).with_metadata(headers_of("Bad Key", b"t")),
        ] {
            let mut server = Server::new();
            server.route_envelope_with_context(
                "example.Greeter",
                "SayHello",
                move |_context, _body| {
                    let envelope = envelope.clone();
                    async move { Ok(envelope) }
                },
            );

            let status = server
                .handle_request(RpcRequest::new("example.Greeter", "SayHello"), Vec::new())
                .await
                .expect_err("invalid metadata should fail the call");

            assert_eq!(status.code(), Code::Internal);
        }
    }

    fn headers_route(server: &mut Server, headers: Metadata, messages: crate::BoxStream<Vec<u8>>) {
        let messages = Arc::new(Mutex::new(Some(messages)));
        server.route_streaming_envelope_with_context(
            "example.Greeter",
            "Watch",
            RpcKind::ServerStreaming,
            move |_context, _body, _requests| {
                let headers = headers.clone();
                let messages = Arc::clone(&messages);
                async move {
                    let messages = messages
                        .lock()
                        .expect("lock should not be poisoned")
                        .take()
                        .expect("route should be called once");
                    Ok(ResponseEnvelope::new(messages)
                        .with_headers(headers)
                        .with_metadata(headers_of("x-trailer", b"t")))
                }
            },
        );
    }

    fn watch_request() -> RpcRequest {
        RpcRequest::new("example.Greeter", "Watch").with_kind(RpcKind::ServerStreaming)
    }

    #[tokio::test]
    async fn streaming_headers_precede_the_first_message_poll() {
        let mut server = Server::new();
        // The first message never becomes ready, yet the headers are available at once.
        headers_route(
            &mut server,
            headers_of("x-header", b"h"),
            Box::pin(futures_util::stream::pending()),
        );

        let mut response = server
            .handle_streaming_request(watch_request(), Vec::new(), crate::stream::empty())
            .await;
        let first = response
            .next()
            .await
            .expect("headers should be the first frame")
            .expect("frame should be ok");

        assert_eq!(first, ResponseFrame::Headers(headers_of("x-header", b"h")));
    }

    #[tokio::test]
    async fn streaming_headers_message_and_trailers_are_framed_in_order() {
        let mut server = Server::new();
        headers_route(
            &mut server,
            headers_of("x-header", b"h"),
            crate::stream::from_iter([b"one".to_vec()]),
        );

        let frames: Vec<_> = server
            .handle_streaming_request(watch_request(), Vec::new(), crate::stream::empty())
            .await
            .map(|frame| frame.expect("frame should be ok"))
            .collect()
            .await;

        assert_eq!(
            frames,
            [
                ResponseFrame::Headers(headers_of("x-header", b"h")),
                ResponseFrame::message(&b"one"[..]),
                ResponseFrame::Status(Status::ok().with_metadata(headers_of("x-trailer", b"t"))),
            ]
        );
    }

    #[tokio::test]
    async fn empty_streaming_headers_send_no_headers_frame() {
        let mut server = Server::new();
        headers_route(
            &mut server,
            Metadata::new(),
            crate::stream::from_iter([b"one".to_vec()]),
        );

        let frames: Vec<_> = server
            .handle_streaming_request(watch_request(), Vec::new(), crate::stream::empty())
            .await
            .map(|frame| frame.expect("frame should be ok"))
            .collect()
            .await;

        assert!(matches!(frames.first(), Some(ResponseFrame::Message(_))));
        assert_eq!(frames.len(), 2);
    }

    #[tokio::test]
    async fn invalid_streaming_headers_fail_the_call_with_a_lone_status() {
        let mut server = Server::new();
        headers_route(
            &mut server,
            headers_of("Bad Key", b"h"),
            crate::stream::from_iter([b"one".to_vec()]),
        );

        let frames: Vec<_> = server
            .handle_streaming_request(watch_request(), Vec::new(), crate::stream::empty())
            .await
            .map(|frame| frame.expect("frame should be ok"))
            .collect()
            .await;

        assert_eq!(frames.len(), 1);
        assert_eq!(frames.first().and_then(status_code), Some(Code::Internal));
    }

    #[test]
    fn request_budget_charges_skipped_frames_to_the_body_limit() {
        let mut budget = super::RequestBudget::new(Some(2), Some(10));
        budget.charge_skipped(6).expect("within the limit");
        budget.charge_skipped(4).expect("exactly at the limit");
        let status = budget
            .charge_skipped(1)
            .expect_err("one byte over")
            .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);

        // Skipped frames are not messages.
        let mut budget = super::RequestBudget::new(Some(1), None);
        budget.charge_skipped(100).expect("no byte limit");
        budget.charge_message(5).expect("first message");
        assert!(budget.charge_message(5).is_err(), "second message is over");
    }
}
