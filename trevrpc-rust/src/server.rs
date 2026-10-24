use std::collections::HashMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::sync::watch;

use crate::client::MessageTypes;
use crate::compression::{
    self, Accept, CompressStream, Compression, CompressionConfig, DEFAULT_SERVER_MEMORY, Inflater,
    ServerStream, StreamBudget,
};
use crate::datagram::MessageCount;
use crate::fallback::HttpFallback;
use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::response::ResponseMetadata;
use crate::sync::MutexExt as _;
use crate::wire::{
    normalize_metadata_key, split_method_path, validate_metadata, validate_method_path,
};
use crate::{
    BoxStream, Code, Error, Metadata, ResponseEnvelope, ResponseFrame, Result, RpcKind, RpcRequest,
    Status, Unreliable,
};

type UnaryHandlerFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>>;
type UnaryHandler =
    Arc<dyn Fn(RequestContext, Vec<u8>) -> UnaryHandlerFuture + Send + Sync + 'static>;
type StreamingHandlerFuture =
    Pin<Box<dyn Future<Output = Result<BoxStream<Vec<u8>>>> + Send + 'static>>;
type StreamingHandler = Arc<
    dyn Fn(RequestContext, Vec<u8>, BoxStream<Vec<u8>>) -> StreamingHandlerFuture + Send + Sync,
>;
/// Routes keyed by the full method path, `/<service>/<method>`.
type RouteMap = HashMap<String, Route>;

#[derive(Clone)]
enum Route {
    Unary {
        handler: UnaryHandler,
        options: RouteOptions,
    },
    Streaming {
        kind: RpcKind,
        handler: StreamingHandler,
        options: RouteOptions,
    },
}

/// Settings of a route beyond its handler.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RouteOptions {
    unreliable: Unreliable,
    message_types: Option<MessageTypes>,
    compress_stream: CompressStream,
}

impl RouteOptions {
    /// Creates route options with every setting at its default: reliable, without message types
    /// or stream compression.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            unreliable: Unreliable::NONE,
            message_types: None,
            compress_stream: CompressStream::NONE,
        }
    }

    /// Lets the route's streamed messages be compressed with one zstd context per call in the
    /// directions `directions` sets; see [`CompressStream`]. Generated code passes the schema's
    /// `compress_stream`. Directions the route's kind does not stream are ignored.
    ///
    /// When the requests direction is set, the route streams requests and they are not marked
    /// unreliable, every call sends HEADERS at dispatch, before the handler runs, carrying the
    /// server's answer, so the client can start compressing while it streams. The handlers of
    /// such a route cannot set response headers
    /// ([`RequestContext::set_response_header`] reports them as already sent); they use trailing
    /// metadata instead.
    #[must_use]
    pub const fn with_compress_stream(mut self, directions: CompressStream) -> Self {
        self.compress_stream = directions;
        self
    }

    /// Returns the directions that may use stream compression.
    #[must_use]
    pub const fn compress_stream(&self) -> CompressStream {
        self.compress_stream
    }

    /// Names the route's request and response message types, which pick the dictionary of
    /// [compressed](crate::Compression) response messages. Generated code passes them; a route
    /// without them sends its responses uncompressed.
    #[must_use]
    pub const fn with_message_types(
        mut self,
        request: &'static str,
        response: &'static str,
    ) -> Self {
        self.message_types = Some(MessageTypes::new(request, response));
        self
    }

    /// Returns the route's message types, if they were named.
    #[must_use]
    pub const fn message_types(&self) -> Option<MessageTypes> {
        self.message_types
    }

    /// Marks directions of the route as unreliable, in addition to any marked already.
    ///
    /// The server sends the messages of a streaming response as datagrams, when the client's
    /// connection carries them and gave the call a datagram flow, only if `unreliable.responses`
    /// is set here. It accepts request datagrams on any call with a flow, so `unreliable.requests`
    /// changes nothing on the server. Flags for a direction the route's kind does not stream are
    /// ignored.
    #[must_use]
    pub const fn with_unreliable(mut self, unreliable: Unreliable) -> Self {
        self.unreliable = self.unreliable.union(unreliable);
        self
    }

    /// Returns the directions marked unreliable.
    #[must_use]
    pub const fn unreliable(&self) -> Unreliable {
        self.unreliable
    }
}

#[derive(Clone)]
pub struct Server {
    routes: Arc<RouteMap>,
    options: ServerOptions,
    authorizer: Option<Arc<dyn Authorizer>>,
    metrics: Arc<dyn Metrics>,
    /// The digested dictionaries of `options`, or why they are invalid, which serving reports.
    compression: std::result::Result<Option<Arc<CompressionConfig>>, Status>,
    /// The memory stream compression contexts may hold, `None` when it is disabled.
    stream_budget: Option<Arc<StreamBudget>>,
}

/// The counters of a call's request messages that its handler context reports.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestCounts {
    /// The request messages that arrived as datagrams.
    pub(crate) datagrams: MessageCount,
    /// The request messages that arrived stream-compressed.
    pub(crate) stream_compressed: MessageCount,
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
    /// The method path, `/<service>/<method>`.
    path: Arc<str>,
    /// Byte length of the service name, or `None` when `path` is not a well-formed method path.
    service_len: Option<usize>,
    kind: RpcKind,
    metadata: Metadata,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
    datagrams: MessageCount,
    /// Counts the request messages that arrived stream-compressed.
    stream_compressed: MessageCount,
    /// The response metadata the handler sets, shared by every clone of the context.
    response: ResponseMetadata,
}

impl RequestContext {
    #[must_use]
    pub fn new(request: &RpcRequest, deadline: Option<Instant>) -> Self {
        Self::from_parts(
            Arc::from(request.method.as_str()),
            request.rpc_kind(),
            request.metadata.clone(),
            deadline,
            CancellationToken::new(),
            RequestCounts::default(),
        )
    }

    fn from_parts(
        path: Arc<str>,
        kind: RpcKind,
        metadata: Metadata,
        deadline: Option<Instant>,
        cancellation: CancellationToken,
        counts: RequestCounts,
    ) -> Self {
        let service_len = split_method_path(&path).map(|(service, _)| service.len());
        Self {
            path,
            service_len,
            kind,
            metadata,
            deadline,
            cancellation,
            datagrams: counts.datagrams,
            stream_compressed: counts.stream_compressed,
            response: ResponseMetadata::default(),
        }
    }

    /// Sets a response header of the call, sent in a HEADERS frame ahead of the first response
    /// message as soon as the handler returns. A failed call sends no headers.
    ///
    /// Calls to a method whose streamed requests may be stream-compressed (see
    /// [`RouteOptions::with_compress_stream`]) send their HEADERS at dispatch, before the handler
    /// runs, so on them this always fails as already sent; use
    /// [`set_response_metadata`](Self::set_response_metadata) instead.
    ///
    /// The key is lowercased (see [`normalize_metadata_key`]). Clients read the headers from
    /// `ResponseEnvelope::headers` or the `headers` of a response stream.
    ///
    /// # Errors
    ///
    /// Returns an `INTERNAL` status when the key, or the headers with this entry, are invalid,
    /// and once the headers have been sent.
    pub fn set_response_header(
        &self,
        key: &str,
        value: impl Into<Vec<u8>>,
    ) -> std::result::Result<(), Status> {
        self.response.set_header(key, value.into())
    }

    /// Sets trailing metadata of the call, sent with the final `OK` status: when a unary or
    /// client-streaming handler returns, and when the response stream of a streaming handler ends,
    /// so a clone of the context held by the stream may still set it until then. A failed call
    /// sends the metadata of its [`Status`] instead.
    ///
    /// The key is lowercased (see [`normalize_metadata_key`]). Clients read the metadata from
    /// `ResponseEnvelope::metadata` or the terminal status of a response stream.
    ///
    /// # Errors
    ///
    /// Returns an `INTERNAL` status when the key, or the metadata with this entry, are invalid,
    /// and once the final status has been sent.
    pub fn set_response_metadata(
        &self,
        key: &str,
        value: impl Into<Vec<u8>>,
    ) -> std::result::Result<(), Status> {
        self.response.set_metadata(key, value.into())
    }

    /// Returns the fully-qualified service name, such as `example.greeter.Greeter`.
    ///
    /// Empty when the context was built from a request with a malformed method path, which the
    /// server never dispatches.
    #[must_use]
    pub fn service(&self) -> &str {
        self.service_len
            .and_then(|len| self.path.get(1..len.saturating_add(1)))
            .unwrap_or_default()
    }

    /// Returns the method name, such as `SayHello`.
    ///
    /// Empty when the context was built from a request with a malformed method path, which the
    /// server never dispatches.
    #[must_use]
    pub fn method(&self) -> &str {
        self.service_len
            .and_then(|len| self.path.get(len.saturating_add(2)..))
            .unwrap_or_default()
    }

    /// Returns the full method path, `/<service>/<method>`.
    #[must_use]
    pub fn full_method(&self) -> &str {
        &self.path
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

    /// Returns how many of the call's request messages have arrived as datagrams so far.
    ///
    /// Zero for every call without a datagram flow, such as over `QMux` or HTTP.
    #[must_use]
    pub fn datagram_requests_received(&self) -> u64 {
        self.datagrams.get()
    }

    /// Returns how many of the call's request messages have arrived stream-compressed
    /// (`ZSTD_STREAM_MESSAGE`) so far; see [`CompressStream`].
    #[must_use]
    pub fn stream_compressed_requests_received(&self) -> u64 {
        self.stream_compressed.get()
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

/// Server limits, timeouts, and the HTTP and browser session transports.
///
/// A stream has no message-count, cumulative-body, or idle limit by default, so long-lived and
/// quiet streams work; set them with [`ServerOptions::with_max_stream_messages`],
/// [`ServerOptions::with_max_stream_body_size`], and [`ServerOptions::with_stream_idle_timeout`].
/// The connection idle timeout (30 s by default) closes a connection whose peer stopped answering
/// and sends keep-alives at half of it, so quiet streams on a live connection are not affected.
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
    connection_idle_timeout: Option<Duration>,
    http_enabled: bool,
    http_path: String,
    http_admission: Option<HttpAdmission>,
    webtransport_enabled: bool,
    webtransport_allowed_authorities: Vec<String>,
    webtransport_allowed_origins: Vec<String>,
    webtransport_admission: Option<WebTransportAdmission>,
    http_fallback: Option<HttpFallback>,
    compression: Compression,
    stream_compression_memory: usize,
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
            stream_messages: None,
            stream_body_size: None,
            stream_idle_timeout: None,
            connection_idle_timeout: Some(Duration::from_secs(30)),
            http_enabled: false,
            http_path: "/trevrpc".to_owned(),
            http_admission: None,
            webtransport_enabled: false,
            webtransport_allowed_authorities: Vec::new(),
            webtransport_allowed_origins: Vec::new(),
            webtransport_admission: None,
            http_fallback: None,
            compression: Compression::new(),
            stream_compression_memory: DEFAULT_SERVER_MEMORY,
        }
    }
}

impl ServerOptions {
    /// Creates server options with the default limits and `WebTransport` settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lets messages travel compressed with the dictionaries of `compression`; see
    /// [`Compression`]. Serving fails when they are invalid.
    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Returns the dictionaries for compressed messages.
    #[must_use]
    pub const fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Sets how much memory the zstd contexts of stream compression ([`CompressStream`]) may hold
    /// at once, in bytes. Calls that would exceed it fall back to per-message compression. The
    /// default is 256 MiB; zero disables stream compression.
    #[must_use]
    pub const fn with_stream_compression_memory(mut self, bytes: usize) -> Self {
        self.stream_compression_memory = bytes;
        self
    }

    /// Returns how much memory stream compression may hold, in bytes.
    #[must_use]
    pub const fn stream_compression_memory(&self) -> usize {
        self.stream_compression_memory
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

    /// Returns the maximum time the request or response side of a stream may be quiet.
    #[must_use]
    pub const fn stream_idle_timeout(&self) -> Option<Duration> {
        self.stream_idle_timeout
    }

    /// Returns the time after which a connection whose peer stopped answering is closed. Keep-alives
    /// are sent at half of it.
    #[must_use]
    pub const fn connection_idle_timeout(&self) -> Option<Duration> {
        self.connection_idle_timeout
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

    /// Returns the service for the HTTP requests `TrevRPC` does not claim, if configured.
    #[must_use]
    pub const fn http_fallback(&self) -> Option<&HttpFallback> {
        self.http_fallback.as_ref()
    }

    /// Returns whether the server speaks HTTP: HTTP/3 on the endpoint and HTTP/2 or HTTP/1.1 on
    /// the TCP listener.
    pub(crate) const fn serves_http(&self) -> bool {
        self.http_enabled || self.webtransport_enabled || self.http_fallback.is_some()
    }

    /// Returns whether the TCP listener serves HTTP/2: RPCs, or the HTTP fallback.
    pub(crate) const fn serves_http2(&self) -> bool {
        self.http_enabled || self.http_fallback.is_some()
    }

    /// Returns whether the TCP listener serves HTTP/1.1: WebSocket sessions, or the HTTP fallback.
    pub(crate) const fn serves_http1(&self) -> bool {
        self.webtransport_enabled || self.http_fallback.is_some()
    }

    /// Returns whether `TrevRPC`, rather than the HTTP fallback, serves HTTP requests for `path`.
    pub(crate) fn claims_path(&self, path: &str) -> bool {
        (self.http_enabled || self.webtransport_enabled) && path == self.http_path
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

    /// Sets the maximum number of messages allowed on a stream (none by default).
    #[must_use]
    pub const fn with_max_stream_messages(mut self, max_stream_messages: Option<usize>) -> Self {
        self.stream_messages = max_stream_messages;
        self
    }

    /// Sets the maximum total body bytes allowed on a stream (none by default).
    #[must_use]
    pub const fn with_max_stream_body_size(mut self, max_stream_body_size: Option<usize>) -> Self {
        self.stream_body_size = max_stream_body_size;
        self
    }

    /// Sets the maximum time the request or response side of a stream may be quiet before the call
    /// fails with `Unavailable` (none by default).
    #[must_use]
    pub const fn with_stream_idle_timeout(mut self, stream_idle_timeout: Option<Duration>) -> Self {
        self.stream_idle_timeout = stream_idle_timeout;
        self
    }

    /// Sets the time after which a QUIC or `QMux` connection whose peer stopped answering is
    /// closed (30 s by default); keep-alives are sent at half of it. `None` leaves the transports'
    /// own defaults.
    #[must_use]
    pub const fn with_connection_idle_timeout(
        mut self,
        connection_idle_timeout: Option<Duration>,
    ) -> Self {
        self.connection_idle_timeout = connection_idle_timeout;
        self
    }

    /// Enables or disables ordinary HTTP/3 `POST` RPCs (disabled by default).
    ///
    /// The `h3` ALPN is advertised by [`Server::quic_config`] only when HTTP/3, `WebTransport` or
    /// an HTTP fallback is enabled.
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

    /// Sets the service for every HTTP request `TrevRPC` does not claim, such as the static
    /// content of a web application served from the same port.
    ///
    /// `TrevRPC` claims the HTTP path while HTTP RPCs or `WebTransport` are enabled; every other
    /// path, over HTTP/3, HTTP/2 and HTTP/1.1, goes to the fallback. Setting one serves those
    /// HTTP versions even when neither is enabled: [`Server::quic_config`] then advertises `h3`
    /// and [`Server::tcp_tls_config`] advertises `h2` and `http/1.1`. HTTP/1.1 and HTTP/2
    /// responses advertise HTTP/3 through `Alt-Svc` unless the service sets the header itself.
    ///
    /// A fallback request takes a stream slot of its connection like an RPC, so a connection at
    /// [`ServerOptions::max_concurrent_streams_per_connection`] answers it with 503. It does not
    /// count toward [`ServerOptions::max_concurrent_requests`], and admission does not apply to
    /// it. In-flight requests are drained on shutdown like RPCs.
    #[must_use]
    pub fn with_http_fallback(mut self, http_fallback: Option<HttpFallback>) -> Self {
        self.http_fallback = http_fallback;
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
            compression: Ok(None),
            stream_budget: StreamBudget::new(DEFAULT_SERVER_MEMORY),
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
        self.compression =
            CompressionConfig::new(options.compression()).map_err(Error::into_status);
        self.stream_budget = StreamBudget::new(options.stream_compression_memory());
        self.options = options;
        self
    }

    /// The server's dictionaries, `None` without any.
    pub(crate) fn compression(&self) -> Option<&Arc<CompressionConfig>> {
        self.compression.as_ref().ok().and_then(Option::as_ref)
    }

    /// The memory stream compression may hold, `None` when it is disabled.
    pub(crate) const fn stream_budget(&self) -> Option<&Arc<StreamBudget>> {
        self.stream_budget.as_ref()
    }

    /// Fails when the configured compression is invalid; serving checks this first.
    pub(crate) fn check_compression(&self) -> Result<()> {
        self.compression
            .as_ref()
            .map(|_| ())
            .map_err(|status| Error::from(status.clone()))
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
        self.routes.len()
    }

    /// Registers a unary route handler for a method path, `/<service>/<method>`.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not of the form `/<service>/<method>`.
    pub fn route<F, Fut>(&mut self, path: impl Into<String>, handler: F)
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.route_with_context(path, move |_context, body| handler(body));
    }

    /// Registers a unary route handler that receives request context, through which it may set
    /// response headers and trailing metadata.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not of the form `/<service>/<method>`.
    pub fn route_with_context<F, Fut>(&mut self, path: impl Into<String>, handler: F)
    where
        F: Fn(RequestContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.route_with_options(path, RouteOptions::new(), handler);
    }

    /// Registers a unary route handler like [`Server::route_with_context`], with route options
    /// such as its message types.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not of the form `/<service>/<method>`.
    pub fn route_with_options<F, Fut>(
        &mut self,
        path: impl Into<String>,
        options: RouteOptions,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        self.insert_route(
            path.into(),
            Route::Unary {
                handler: Arc::new(move |context, body| Box::pin(handler(context, body))),
                options,
            },
        );
    }

    /// Registers a streaming route handler for a method path, `/<service>/<method>`, and RPC kind.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not of the form `/<service>/<method>`.
    pub fn route_streaming<F, Fut>(&mut self, path: impl Into<String>, kind: RpcKind, handler: F)
    where
        F: Fn(Vec<u8>, BoxStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxStream<Vec<u8>>>> + Send + 'static,
    {
        self.route_streaming_with_context(path, kind, move |_context, body, stream| {
            handler(body, stream)
        });
    }

    /// Registers a streaming route handler that receives request context, through which it may set
    /// response headers before it returns and trailing metadata until its response stream ends.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not of the form `/<service>/<method>`.
    pub fn route_streaming_with_context<F, Fut>(
        &mut self,
        path: impl Into<String>,
        kind: RpcKind,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>, BoxStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxStream<Vec<u8>>>> + Send + 'static,
    {
        self.route_streaming_with_options(path, kind, RouteOptions::new(), handler);
    }

    /// Registers a streaming route handler like [`Server::route_streaming_with_context`], with
    /// route options such as which directions are [`Unreliable`].
    ///
    /// # Panics
    ///
    /// Panics when `path` is not of the form `/<service>/<method>`.
    pub fn route_streaming_with_options<F, Fut>(
        &mut self,
        path: impl Into<String>,
        kind: RpcKind,
        options: RouteOptions,
        handler: F,
    ) where
        F: Fn(RequestContext, Vec<u8>, BoxStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxStream<Vec<u8>>>> + Send + 'static,
    {
        self.insert_route(
            path.into(),
            Route::Streaming {
                kind,
                handler: Arc::new(move |context, body, stream| {
                    Box::pin(handler(context, body, stream))
                }),
                options,
            },
        );
    }

    /// Returns the directions the streaming route at `path` marks unreliable, or none when there
    /// is no such route.
    pub(crate) fn route_unreliable(&self, path: &str) -> Unreliable {
        match self.routes.get(path) {
            Some(Route::Streaming { kind, options, .. }) => options.unreliable().effective(*kind),
            _ => Unreliable::NONE,
        }
    }

    /// Whether calls to `path` send HEADERS at dispatch, before the handler runs: those whose
    /// requests the server may stream-compress, so the client learns its answer while it still
    /// streams them. Their handlers cannot set response headers.
    pub(crate) fn sends_headers_at_dispatch(&self, path: &str) -> bool {
        self.route_compress_stream(path).requests
    }

    /// Whether the server refuses `request`, which [`Server::admit`] authorized: it is
    /// optimistic, but `stream`, what negotiation decided, does not decode its stream-compressed
    /// requests. A request for an unknown method, or of the wrong kind, gets that status instead,
    /// as without `optimistic`.
    pub(crate) fn refuses_optimistic(&self, request: &RpcRequest, stream: &ServerStream) -> bool {
        compression::refuses_optimistic(request, stream)
            && matches!(
                self.routes.get(request.method.as_str()),
                Some(Route::Streaming { kind, .. }) if *kind == request.rpc_kind()
            )
    }

    /// The directions of the streaming route at `path` that may use stream compression, or none
    /// when there is no such route.
    pub(crate) fn route_compress_stream(&self, path: &str) -> CompressStream {
        match self.routes.get(path) {
            Some(Route::Streaming { kind, options, .. }) => options
                .compress_stream()
                .effective(*kind)
                .without_unreliable(options.unreliable()),
            _ => CompressStream::NONE,
        }
    }

    /// The message types of the route at `path`, if it named them.
    pub(crate) fn route_message_types(&self, path: &str) -> Option<MessageTypes> {
        match self.routes.get(path)? {
            Route::Unary { options, .. } | Route::Streaming { options, .. } => {
                options.message_types()
            }
        }
    }

    /// Inserts a route, replacing any earlier route for the same path.
    #[allow(clippy::panic)] // A malformed path is a programming error found at startup.
    fn insert_route(&mut self, path: String, route: Route) {
        if let Err(status) = validate_method_path(&path) {
            panic!("cannot register route: {}", status.message());
        }
        Arc::make_mut(&mut self.routes).insert(path, route);
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

        record_rpc_started(self.metrics.as_ref(), &request.method, request_body_len);

        let (service, method) = split_or_empty(&request.method);
        tracing::info!(
            service,
            method,
            request_body_len,
            metadata_count = request.metadata.len(),
            "rpc started"
        );

        let deadline = match self.admit(&request, &cancellation).await {
            Ok(Admitted { deadline }) => deadline,
            Err(status) => {
                return self.finish_response(
                    &request.method,
                    request_body_len,
                    started_at,
                    Err(status),
                );
            }
        };

        let Some(Route::Unary { handler, .. }) = self.routes.get(request.method.as_str()) else {
            return self.finish_response(
                &request.method,
                request_body_len,
                started_at,
                Err(Status::unimplemented(format!(
                    "unknown RPC method {}",
                    request.method
                ))),
            );
        };
        let kind = request.rpc_kind();
        let RpcRequest {
            method: path,
            metadata,
            ..
        } = request;
        let path: Arc<str> = Arc::from(path);
        let context = RequestContext::from_parts(
            Arc::clone(&path),
            kind,
            metadata,
            deadline,
            cancellation.clone(),
            RequestCounts::default(),
        );
        let response_metadata = context.response.clone();

        let result = match invoke_unary_handler(handler, context, body) {
            Ok(future) => match run_handler_with_deadline(future, deadline, &cancellation).await {
                Ok(Ok(message)) => {
                    let headers = response_metadata.take_headers();
                    Ok(ResponseEnvelope::new(message)
                        .with_headers(headers)
                        .with_metadata(response_metadata.take_metadata()))
                }
                Ok(Err(error)) => Err(error.into_status()),
                Err(status) => Err(status),
            },
            Err(status) => Err(status),
        };

        self.finish_response(&path, request_body_len, started_at, result)
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
            None,
            CancellationToken::new(),
            RequestCounts::default(),
        )
        .await
    }

    /// `admitted` is set for a call that [`Server::admit`] already validated and authorized;
    /// otherwise that happens here.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn handle_streaming_request_with_cancellation(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        request_body: BoxStream<Vec<u8>>,
        admitted: Option<Admitted>,
        cancellation: CancellationToken,
        counts: RequestCounts,
    ) -> BoxStream<ResponseFrame> {
        let started_at = Instant::now();
        let request_body_len = body.len();

        record_rpc_started(self.metrics.as_ref(), &request.method, request_body_len);

        let (service, method) = split_or_empty(&request.method);
        tracing::info!(
            service,
            method,
            request_body_len,
            metadata_count = request.metadata.len(),
            kind = ?request.rpc_kind(),
            "streaming rpc started"
        );

        let admission = match admitted {
            Some(admitted) => Ok(admitted),
            None => self.admit(&request, &cancellation).await,
        };
        let deadline = match admission {
            Ok(Admitted { deadline }) => deadline,
            Err(status) => {
                return self.finish_streaming_status(
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

        let Some(Route::Streaming { kind, handler, .. }) = self.routes.get(request.method.as_str())
        else {
            let status =
                Status::unimplemented(format!("unknown streaming RPC method {}", request.method));
            return self.finish_streaming_status(
                &request.method,
                request_body_len,
                started_at,
                status,
            );
        };

        let request_kind = request.rpc_kind();
        if request_kind != *kind {
            let status = Status::invalid_argument(format!(
                "streaming RPC kind mismatch for {}: expected {:?}, got {:?}",
                request.method, kind, request_kind
            ));
            return self.finish_streaming_status(
                &request.method,
                request_body_len,
                started_at,
                status,
            );
        }

        let RpcRequest {
            method: path,
            metadata,
            ..
        } = request;
        let path: Arc<str> = Arc::from(path);
        let mut context = RequestContext::from_parts(
            Arc::clone(&path),
            request_kind,
            metadata,
            deadline,
            cancellation.clone(),
            counts,
        );
        if self.sends_headers_at_dispatch(&path) {
            context.response = ResponseMetadata::with_headers_sent();
        }
        let response_metadata = context.response.clone();
        let handler_result = match invoke_streaming_handler(handler, context, body, request_body) {
            Ok(future) => run_handler_with_deadline(future, deadline, &cancellation).await,
            Err(status) => Err(status),
        };

        match handler_result {
            Err(status) => {
                self.finish_streaming_status(&path, request_body_len, started_at, status)
            }
            Ok(Ok(response_body)) => {
                let headers = response_metadata.take_headers();
                server_response_stream(
                    response_body,
                    headers,
                    response_metadata,
                    StreamingCompletion {
                        metrics: Arc::clone(&self.metrics),
                        path,
                        request_body_len,
                        started_at,
                    },
                    stream_limits,
                    cancellation,
                )
            }
            Ok(Err(error)) => {
                let status = error.into_status();
                self.finish_streaming_status(&path, request_body_len, started_at, status)
            }
        }
    }

    /// Validates `request` and runs the authorizer.
    pub(crate) async fn admit(
        &self,
        request: &RpcRequest,
        cancellation: &CancellationToken,
    ) -> std::result::Result<Admitted, Status> {
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

        Ok(Admitted { deadline })
    }

    fn finish_response(
        &self,
        path: &str,
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
            path,
            request_body_len,
            response_body_len,
            code,
            elapsed,
        );

        let (service, method) = split_or_empty(path);
        tracing::info!(
            service,
            method,
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
        path: &str,
        request_body_len: usize,
        started_at: Instant,
        response_body_len: usize,
        code: Code,
    ) {
        finish_streaming_response(
            self.metrics.as_ref(),
            path,
            request_body_len,
            started_at,
            response_body_len,
            code,
        );
    }

    fn finish_streaming_status(
        &self,
        path: &str,
        request_body_len: usize,
        started_at: Instant,
        status: Status,
    ) -> BoxStream<ResponseFrame> {
        self.finish_streaming_response(path, request_body_len, started_at, 0, status.code());
        status_stream(status)
    }

    pub(crate) fn record_rejected_request(
        &self,
        request: &RpcRequest,
        request_body_len: usize,
        status: &Status,
    ) {
        let started_at = Instant::now();
        record_rpc_started(self.metrics.as_ref(), &request.method, request_body_len);
        self.finish_streaming_response(
            &request.method,
            request_body_len,
            started_at,
            0,
            status.code(),
        );
    }

    pub(crate) fn record_active_request_failure(
        &self,
        method: &str,
        request_body_len: usize,
        status: &Status,
    ) {
        self.finish_streaming_response(method, request_body_len, Instant::now(), 0, status.code());
    }

    pub(crate) fn record_pre_handler_failure(&self, status: &Status) {
        let started_at = Instant::now();
        record_rpc_started(self.metrics.as_ref(), "", 0);
        self.finish_streaming_response("", 0, started_at, 0, status.code());
    }
}

/// A call that [`Server::admit`] validated and authorized.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Admitted {
    /// The call's deadline, from its timeout.
    pub(crate) deadline: Option<Instant>,
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
#[derive(Debug)]
pub struct RequestBudget {
    max_messages: Option<usize>,
    max_body_size: Option<usize>,
    messages: usize,
    body_size: usize,
    /// The dictionaries compressed request messages may use.
    pub(crate) accept: Accept,
    /// The decompressing state of stream-compressed request messages, when the server enabled
    /// them.
    pub(crate) stream: Option<Inflater>,
}

impl RequestBudget {
    pub(crate) const fn new(max_messages: Option<usize>, max_body_size: Option<usize>) -> Self {
        Self {
            max_messages,
            max_body_size,
            messages: 0,
            body_size: 0,
            accept: Accept::None,
            stream: None,
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
    path: Arc<str>,
    request_body_len: usize,
    started_at: Instant,
}

impl StreamingCompletion {
    fn finish(&self, response_body_len: usize, code: Code) {
        finish_streaming_response(
            self.metrics.as_ref(),
            &self.path,
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
    /// Where the trailing metadata of the final `OK` status comes from.
    metadata: ResponseMetadata,
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
        metadata: ResponseMetadata,
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
    metadata: ResponseMetadata,
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
                        Status::ok().with_metadata(stream.metadata.take_metadata()),
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
    path: &str,
    request_body_len: usize,
    started_at: Instant,
    response_body_len: usize,
    code: Code,
) {
    let elapsed = started_at.elapsed();

    record_rpc_finished(
        metrics,
        path,
        request_body_len,
        response_body_len,
        code,
        elapsed,
    );

    let (service, method) = split_or_empty(path);
    tracing::info!(
        service,
        method,
        request_body_len,
        response_body_len,
        status = ?code,
        elapsed_ms = elapsed.as_millis(),
        "streaming rpc finished"
    );
}

/// Splits a method path for metrics and logs, yielding empty names for a malformed path.
fn split_or_empty(path: &str) -> (&str, &str) {
    split_method_path(path).unwrap_or_default()
}

fn record_rpc_started(metrics: &dyn Metrics, path: &str, request_body_len: usize) {
    if !metrics.enabled() {
        return;
    }

    let (service, method) = split_or_empty(path);
    let event = RpcStarted {
        service: service.to_owned(),
        method: method.to_owned(),
        request_body_len,
    };
    let _ = catch_unwind(AssertUnwindSafe(|| metrics.rpc_started(&event)));
}

fn record_rpc_finished(
    metrics: &dyn Metrics,
    path: &str,
    request_body_len: usize,
    response_body_len: usize,
    code: Code,
    elapsed: Duration,
) {
    if !metrics.enabled() {
        return;
    }

    let (service, method) = split_or_empty(path);
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

    use crate::Metadata;

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
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let mut request = RpcRequest::new("/example.Greeter/SayHello");
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
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let status = server
            .handle_request(RpcRequest::new("/example.Greeter/SayHello"), Vec::new())
            .await
            .expect_err("missing metadata should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn metrics_panics_do_not_fail_request() {
        let mut server = Server::new();
        server.set_metrics(PanickingMetrics);
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let response = server
            .handle_request(RpcRequest::new("/example.Greeter/SayHello"), Vec::new())
            .await
            .expect("request should succeed despite panicking metrics");

        assert_eq!(response.message(), b"hello");
    }

    #[tokio::test]
    async fn unary_completion_metrics_are_exactly_once_for_terminal_statuses() {
        expect_unary_completion(
            |server| {
                server.route("/example.Greeter/SayHello", |_| async {
                    Ok(b"hello".to_vec())
                });
            },
            RpcRequest::new("/example.Greeter/SayHello"),
            Code::Ok,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("/example.Greeter/Denied", |_| async {
                    Err(Error::from(Status::new(Code::PermissionDenied, "denied")))
                });
            },
            RpcRequest::new("/example.Greeter/Denied"),
            Code::PermissionDenied,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("/example.Greeter/Panic", |_| async {
                    panic!("boom");
                });
            },
            RpcRequest::new("/example.Greeter/Panic"),
            Code::Internal,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.set_authorizer(MetadataValueAuthorizer::bearer("token"));
                server.route("/example.Greeter/SayHello", |_| async {
                    Ok(b"hello".to_vec())
                });
            },
            RpcRequest::new("/example.Greeter/SayHello"),
            Code::Unauthenticated,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("/example.Greeter/Slow", |_| async {
                    std::future::pending::<crate::Result<Vec<u8>>>().await
                });
            },
            RpcRequest::new("/example.Greeter/Slow").with_timeout_nanos(1_000_000),
            Code::DeadlineExceeded,
        )
        .await;

        let mut invalid_metadata = RpcRequest::new("/example.Greeter/SayHello");
        invalid_metadata
            .metadata
            .insert("Authorization".to_owned(), b"ok".to_vec());
        expect_unary_completion(
            |server| {
                server.route("/example.Greeter/SayHello", |_| async {
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
        server.route("/example.Greeter/Panic", |_| async {
            panic!("boom");
        });

        let status = server
            .handle_request(RpcRequest::new("/example.Greeter/Panic"), Vec::new())
            .await
            .expect_err("a panicking handler should fail");

        assert_eq!(status.code(), Code::Internal);
    }

    #[tokio::test]
    async fn unary_handler_panics_after_yield_return_internal() {
        let mut server = Server::new();
        server.route("/example.Greeter/Panic", |_| async {
            tokio::task::yield_now().await;
            panic!("boom");
        });

        let status = server
            .handle_request(RpcRequest::new("/example.Greeter/Panic"), Vec::new())
            .await
            .expect_err("a panicking handler should fail");

        assert_eq!(status.code(), Code::Internal);
    }

    #[tokio::test]
    async fn streaming_handler_panics_return_internal_status() {
        let mut server = Server::new();
        server.route_streaming(
            "/example.Greeter/Panic",
            RpcKind::ServerStreaming,
            |_, _| async {
                panic!("boom");
            },
        );
        let request = RpcRequest::new("/example.Greeter/Panic").with_kind(RpcKind::ServerStreaming);

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
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });
        let request = RpcRequest::new("/example.Greeter/SayHello").with_timeout_nanos(1_000_000);

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
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let mut request = RpcRequest::new("/example.Greeter/SayHello");
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
            .handle_request(RpcRequest::new("/example.Greeter/Missing"), Vec::new())
            .await
            .expect_err("unauthenticated requests should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn malformed_method_paths_are_rejected_before_route_lookup() {
        let mut server = Server::new();
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        for path in [
            "",
            "/",
            "example.Greeter/SayHello",
            "/example.Greeter",
            "/example.Greeter/",
            "//SayHello",
            "/example.Greeter/SayHello/",
            "/example.Greeter/SayHello/Extra",
            "/example.Greeter//SayHello",
        ] {
            let status = server
                .handle_request(RpcRequest::new(path), Vec::new())
                .await
                .expect_err("a malformed path should be rejected");
            assert_eq!(status.code(), Code::InvalidArgument, "{path:?}");

            assert_eq!(
                streaming_status_code(&server, path).await,
                Some(Code::InvalidArgument),
                "{path:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_authorizer_runs_before_the_route_lookup_of_a_well_formed_path() {
        let mut server = Server::new();
        server.set_authorizer(MetadataValueAuthorizer::bearer("secret"));

        let status = server
            .handle_request(RpcRequest::new("/example.Greeter/Missing"), Vec::new())
            .await
            .expect_err("an unauthenticated request should be rejected");

        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn unknown_well_formed_methods_are_unimplemented() {
        let mut server = Server::new();
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        for path in [
            "/example.Greeter/Missing",
            "/other.Service/SayHello",
            "/example/Greeter",
        ] {
            let status = server
                .handle_request(RpcRequest::new(path), Vec::new())
                .await
                .expect_err("an unknown method should fail");
            assert_eq!(status.code(), Code::Unimplemented, "{path:?}");

            assert_eq!(
                streaming_status_code(&server, path).await,
                Some(Code::Unimplemented),
                "{path:?}"
            );
        }
    }

    #[tokio::test]
    async fn request_context_exposes_the_service_method_and_path() {
        let mut server = Server::new();
        server.route_with_context("/example.Greeter/SayHello", |context, _| async move {
            Ok(format!(
                "{}|{}|{}",
                context.service(),
                context.method(),
                context.full_method()
            )
            .into_bytes())
        });

        let response = server
            .handle_request(RpcRequest::new("/example.Greeter/SayHello"), Vec::new())
            .await
            .expect("the call should succeed");

        assert_eq!(
            response.message().as_slice(),
            b"example.Greeter|SayHello|/example.Greeter/SayHello"
        );
    }

    #[test]
    fn request_contexts_of_malformed_paths_have_empty_names() {
        let context = RequestContext::new(&RpcRequest::new("no-slash"), None);

        assert_eq!(context.service(), "");
        assert_eq!(context.method(), "");
        assert_eq!(context.full_method(), "no-slash");
    }

    #[test]
    #[should_panic(expected = "cannot register route")]
    fn registering_a_malformed_path_panics() {
        let mut server = Server::new();
        server.route("example.Greeter/SayHello", |_| async { Ok(Vec::new()) });
    }

    #[tokio::test]
    async fn metrics_events_keep_the_service_and_method() {
        #[derive(Default)]
        struct NamedMetrics {
            names: Arc<Mutex<Vec<(String, String)>>>,
        }

        impl Metrics for NamedMetrics {
            fn rpc_started(&self, event: &super::RpcStarted) {
                self.names
                    .lock()
                    .expect("metrics lock should not be poisoned")
                    .push((event.service.clone(), event.method.clone()));
            }

            fn rpc_finished(&self, event: &RpcFinished) {
                self.names
                    .lock()
                    .expect("metrics lock should not be poisoned")
                    .push((event.service.clone(), event.method.clone()));
            }
        }

        let metrics = NamedMetrics::default();
        let names = Arc::clone(&metrics.names);
        let mut server = Server::new();
        server.set_metrics(metrics);
        server.route("/example.Greeter/SayHello", |_| async { Ok(Vec::new()) });

        server
            .handle_request(RpcRequest::new("/example.Greeter/SayHello"), Vec::new())
            .await
            .expect("the call should succeed");

        let expected = ("example.Greeter".to_owned(), "SayHello".to_owned());
        assert_eq!(
            *names.lock().expect("metrics lock should not be poisoned"),
            [expected.clone(), expected]
        );
    }

    #[tokio::test]
    async fn unsupported_rpc_kinds_are_rejected_before_route_lookup() {
        let server = Server::new();
        let mut request = RpcRequest::new("/example.Greeter/Missing");
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
        let request = RpcRequest::new("/example.Greeter/Watch").with_kind(RpcKind::ServerStreaming);
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
        let request = RpcRequest::new("/example.Greeter/Watch").with_kind(RpcKind::ServerStreaming);
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
        assert_eq!(options.max_stream_messages(), None);
        assert_eq!(options.max_stream_body_size(), None);
        assert_eq!(options.stream_idle_timeout(), None);
        assert_eq!(
            options.connection_idle_timeout(),
            Some(Duration::from_secs(30))
        );
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
        let mut request = RpcRequest::new("/example.Greeter/Missing");
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
            "/example.Greeter/StreamHello",
            RpcKind::ServerStreaming,
            |body, _requests| async move {
                assert_eq!(body, b"request".to_vec());
                Ok(crate::stream::from_iter([b"one".to_vec(), b"two".to_vec()]))
            },
        );

        let request =
            RpcRequest::new("/example.Greeter/StreamHello").with_kind(RpcKind::ServerStreaming);
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
            "/example.Greeter/Download",
            RpcKind::ServerStreaming,
            |_body, _requests| async move {
                let stream: crate::BoxStream<Vec<u8>> = Box::pin(futures_util::stream::pending());
                Ok(stream)
            },
        );

        let request =
            RpcRequest::new("/example.Greeter/Download").with_kind(RpcKind::ServerStreaming);
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
            "/example.Greeter/Upload",
            RpcKind::ClientStreaming,
            |_body, mut requests| async move {
                while let Some(request) = requests.next().await {
                    request?;
                }

                Ok(crate::stream::empty())
            },
        );

        let request =
            RpcRequest::new("/example.Greeter/Upload").with_kind(RpcKind::ClientStreaming);
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
            "/example.Greeter/Download",
            RpcKind::ServerStreaming,
            |_body, _requests| async move { Ok(crate::stream::from_iter([b"four".to_vec()])) },
        );

        let request =
            RpcRequest::new("/example.Greeter/Download").with_kind(RpcKind::ServerStreaming);
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
            "/example.Greeter/Upload",
            RpcKind::ClientStreaming,
            |_body, mut requests| async move {
                while let Some(request) = requests.next().await {
                    request?;
                }

                Ok(crate::stream::empty())
            },
        );

        let request =
            RpcRequest::new("/example.Greeter/Upload").with_kind(RpcKind::ClientStreaming);
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
            "/example.Greeter/Download",
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
            RpcRequest::new("/example.Greeter/Download").with_kind(RpcKind::ServerStreaming);
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

    /// The code of the status a server-streaming call to `path` ends with, when it starts with one.
    async fn streaming_status_code(server: &Server, path: &str) -> Option<Code> {
        let request = RpcRequest::new(path).with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, Vec::new(), crate::stream::empty())
            .await;
        let frame = response
            .next()
            .await
            .expect("a status frame should be emitted")
            .expect("a status frame should not be a transport error");
        status_code(&frame)
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
        server.route_with_context("/example.Greeter/SayHello", |context, body| async move {
            assert_eq!(body, b"request");
            context.set_response_header("X-Header", b"h".to_vec())?;
            context.set_response_metadata("x-trailer", b"t".to_vec())?;
            Ok(b"reply".to_vec())
        });

        let response = server
            .handle_request(
                RpcRequest::new("/example.Greeter/SayHello"),
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
        server.route("/example.Greeter/SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        server
            .handle_request(
                RpcRequest::new("/example.Greeter/SayHello"),
                b"four".to_vec(),
            )
            .await
            .expect("request should succeed");

        assert_eq!(*started.lock().expect("lock should not be poisoned"), [4]);
    }

    #[tokio::test]
    async fn invalid_unary_header_or_trailing_metadata_is_internal() {
        let mut server = Server::new();
        server.route_with_context("/example.Greeter/SayHello", |context, _body| async move {
            for key in ["bad key", "trevrpc-reserved", ""] {
                let header = context.set_response_header(key, Vec::new());
                let trailer = context.set_response_metadata(key, Vec::new());
                for result in [header, trailer] {
                    assert_eq!(result.map_err(|status| status.code()), Err(Code::Internal));
                }
            }
            context.set_response_header("bad key", Vec::new())?;
            Ok(Vec::new())
        });

        let status = server
            .handle_request(RpcRequest::new("/example.Greeter/SayHello"), Vec::new())
            .await
            .expect_err("invalid metadata should fail the call");

        assert_eq!(status.code(), Code::Internal);
    }

    #[tokio::test]
    async fn a_failed_call_sends_the_metadata_of_its_status() {
        let mut server = Server::new();
        server.route_with_context("/example.Greeter/SayHello", |context, _body| async move {
            context.set_response_header("x-header", b"h".to_vec())?;
            context.set_response_metadata("x-trailer", b"t".to_vec())?;
            Err(Status::not_found("gone")
                .with_metadata(headers_of("x-status", b"s"))
                .into())
        });

        let status = server
            .handle_request(RpcRequest::new("/example.Greeter/SayHello"), Vec::new())
            .await
            .expect_err("the handler failed");

        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.metadata(), &headers_of("x-status", b"s"));
    }

    #[tokio::test]
    async fn streaming_response_metadata_is_taken_when_it_is_sent() {
        let late = Arc::new(Mutex::new(Vec::new()));
        let mut server = Server::new();
        let results = Arc::clone(&late);
        server.route_streaming_with_context(
            "/example.Greeter/Watch",
            RpcKind::ServerStreaming,
            move |context, _body, _requests| {
                let results = Arc::clone(&results);
                async move {
                    context.set_response_header("x-header", b"h".to_vec())?;
                    // The stream sets a header too late with its message, and trailing metadata
                    // before and after it ends.
                    let messages = futures_util::stream::unfold(0, move |step| {
                        let context = context.clone();
                        let results = Arc::clone(&results);
                        async move {
                            match step {
                                0 => {
                                    let late = context.set_response_header("x-late", Vec::new());
                                    results
                                        .lock()
                                        .expect("lock should not be poisoned")
                                        .push(late);
                                    Some((Ok(b"one".to_vec()), 1))
                                }
                                1 => {
                                    context
                                        .set_response_metadata("x-trailer", b"t".to_vec())
                                        .ok()?;
                                    None
                                }
                                _ => None,
                            }
                        }
                    });
                    let messages: crate::BoxStream<Vec<u8>> = Box::pin(messages);
                    Ok(messages)
                }
            },
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
        let late = std::mem::take(&mut *late.lock().expect("lock should not be poisoned"));
        assert!(matches!(late.as_slice(), [Err(status)] if status.code() == Code::Internal));
    }

    fn headers_route(server: &mut Server, headers: Metadata, messages: crate::BoxStream<Vec<u8>>) {
        let messages = Arc::new(Mutex::new(Some(messages)));
        server.route_streaming_with_context(
            "/example.Greeter/Watch",
            RpcKind::ServerStreaming,
            move |context, _body, _requests| {
                let headers = headers.clone();
                let messages = Arc::clone(&messages);
                async move {
                    let messages = messages
                        .lock()
                        .expect("lock should not be poisoned")
                        .take()
                        .expect("route should be called once");
                    for (key, value) in headers {
                        context.set_response_header(&key, value)?;
                    }
                    context.set_response_metadata("x-trailer", b"t".to_vec())?;
                    Ok(messages)
                }
            },
        );
    }

    fn watch_request() -> RpcRequest {
        RpcRequest::new("/example.Greeter/Watch").with_kind(RpcKind::ServerStreaming)
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
    async fn handlers_of_request_stream_compressed_routes_cannot_set_headers() {
        let mut server = Server::new();
        let options = super::RouteOptions::new();
        let routes = [
            (
                "/example.Uploads/Compressed",
                options.with_compress_stream(crate::CompressStream::REQUESTS),
                true,
            ),
            ("/example.Uploads/Plain", options, false),
            // Only the requests direction sends HEADERS at dispatch.
            (
                "/example.Uploads/Responses",
                options.with_compress_stream(crate::CompressStream::RESPONSES),
                false,
            ),
            // Unreliable requests are never stream-compressed.
            (
                "/example.Uploads/Unreliable",
                options
                    .with_compress_stream(crate::CompressStream::REQUESTS)
                    .with_unreliable(crate::Unreliable::REQUESTS),
                false,
            ),
        ];
        for (path, options, _) in routes {
            server.route_streaming_with_options(
                path,
                RpcKind::BidirectionalStreaming,
                options,
                |context, _body, _requests| async move {
                    // Deterministic from the start of the handler.
                    let set = context.set_response_header("x-header", b"h".to_vec());
                    let outcome = match set {
                        Ok(()) => "set".to_owned(),
                        Err(status) => status.message().to_owned(),
                    };
                    context.set_response_metadata("x-outcome", outcome)?;
                    Ok(crate::stream::empty())
                },
            );
        }

        for (path, _, sent) in routes {
            let request = RpcRequest::new(path).with_kind(RpcKind::BidirectionalStreaming);
            let frames: Vec<_> = server
                .handle_streaming_request(request, Vec::new(), crate::stream::empty())
                .await
                .map(|frame| frame.expect("frame should be ok"))
                .collect()
                .await;
            let (outcome, expected_frames) = if sent {
                (&b"response headers already sent"[..], 1)
            } else {
                (&b"set"[..], 2)
            };
            assert_eq!(frames.len(), expected_frames, "{path}: {frames:?}");
            let Some(ResponseFrame::Status(status)) = frames.last() else {
                panic!("{path}: no status");
            };
            assert!(status.is_ok(), "{path}: {status:?}");
            assert_eq!(
                status.metadata().get("x-outcome").map(Vec::as_slice),
                Some(outcome),
                "{path}"
            );
        }
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
