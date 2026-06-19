use std::collections::HashMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{
    BoxMessageStream, Code, Error, MessageStream, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, Status,
};

type UnaryHandlerFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>>;
type UnaryHandler = Arc<dyn Fn(Vec<u8>) -> UnaryHandlerFuture + Send + Sync + 'static>;
type StreamingHandlerFuture =
    Pin<Box<dyn Future<Output = Result<BoxMessageStream<Vec<u8>>>> + Send + 'static>>;
type StreamingHandler =
    Arc<dyn Fn(Vec<u8>, BoxMessageStream<Vec<u8>>) -> StreamingHandlerFuture + Send + Sync>;

#[derive(Clone)]
enum Route {
    Unary(UnaryHandler),
    Streaming {
        kind: RpcKind,
        handler: StreamingHandler,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MethodKey {
    service: String,
    method: String,
}

impl MethodKey {
    fn new(service: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            method: method.into(),
        }
    }
}

#[derive(Clone)]
pub struct Server {
    routes: Arc<HashMap<MethodKey, Route>>,
    options: ServerOptions,
    authorizer: Option<Arc<dyn Authorizer>>,
    metrics: Arc<dyn Metrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    webtransport_path: &'static str,
    webtransport_allowed_authorities: &'static [&'static str],
    webtransport_allowed_origins: &'static [&'static str],
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
            webtransport_path: "/trevrpc",
            webtransport_allowed_authorities: &[],
            webtransport_allowed_origins: &[],
        }
    }
}

impl ServerOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.frame_size
    }

    #[must_use]
    pub const fn max_concurrent_connections(&self) -> Option<usize> {
        self.connections
    }

    #[must_use]
    pub const fn max_concurrent_streams_per_connection(&self) -> Option<usize> {
        self.streams_per_connection
    }

    #[must_use]
    pub const fn max_concurrent_requests(&self) -> Option<usize> {
        self.requests
    }

    #[must_use]
    pub const fn graceful_shutdown_timeout(&self) -> Option<Duration> {
        self.shutdown_timeout
    }

    #[must_use]
    pub const fn initial_request_timeout(&self) -> Option<Duration> {
        self.initial_request_timeout
    }

    #[must_use]
    pub const fn max_stream_messages(&self) -> Option<usize> {
        self.stream_messages
    }

    #[must_use]
    pub const fn max_stream_body_size(&self) -> Option<usize> {
        self.stream_body_size
    }

    #[must_use]
    pub const fn stream_idle_timeout(&self) -> Option<Duration> {
        self.stream_idle_timeout
    }

    #[must_use]
    pub const fn webtransport_path(&self) -> &'static str {
        self.webtransport_path
    }

    #[must_use]
    pub const fn webtransport_allowed_authorities(&self) -> &'static [&'static str] {
        self.webtransport_allowed_authorities
    }

    #[must_use]
    pub const fn webtransport_allowed_origins(&self) -> &'static [&'static str] {
        self.webtransport_allowed_origins
    }

    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.frame_size = max_frame_size;
        self
    }

    #[must_use]
    pub const fn with_max_concurrent_connections(
        mut self,
        max_concurrent_connections: Option<usize>,
    ) -> Self {
        self.connections = max_concurrent_connections;
        self
    }

    #[must_use]
    pub const fn with_max_concurrent_streams_per_connection(
        mut self,
        max_concurrent_streams_per_connection: Option<usize>,
    ) -> Self {
        self.streams_per_connection = max_concurrent_streams_per_connection;
        self
    }

    #[must_use]
    pub const fn with_max_concurrent_requests(
        mut self,
        max_concurrent_requests: Option<usize>,
    ) -> Self {
        self.requests = max_concurrent_requests;
        self
    }

    #[must_use]
    pub const fn with_graceful_shutdown_timeout(
        mut self,
        graceful_shutdown_timeout: Option<Duration>,
    ) -> Self {
        self.shutdown_timeout = graceful_shutdown_timeout;
        self
    }

    #[must_use]
    pub const fn with_initial_request_timeout(
        mut self,
        initial_request_timeout: Option<Duration>,
    ) -> Self {
        self.initial_request_timeout = initial_request_timeout;
        self
    }

    #[must_use]
    pub const fn with_max_stream_messages(mut self, max_stream_messages: Option<usize>) -> Self {
        self.stream_messages = max_stream_messages;
        self
    }

    #[must_use]
    pub const fn with_max_stream_body_size(mut self, max_stream_body_size: Option<usize>) -> Self {
        self.stream_body_size = max_stream_body_size;
        self
    }

    #[must_use]
    pub const fn with_stream_idle_timeout(mut self, stream_idle_timeout: Option<Duration>) -> Self {
        self.stream_idle_timeout = stream_idle_timeout;
        self
    }

    #[must_use]
    pub const fn with_webtransport_path(mut self, webtransport_path: &'static str) -> Self {
        self.webtransport_path = webtransport_path;
        self
    }

    #[must_use]
    pub const fn with_webtransport_allowed_authorities(
        mut self,
        webtransport_allowed_authorities: &'static [&'static str],
    ) -> Self {
        self.webtransport_allowed_authorities = webtransport_allowed_authorities;
        self
    }

    #[must_use]
    pub const fn with_webtransport_allowed_origins(
        mut self,
        webtransport_allowed_origins: &'static [&'static str],
    ) -> Self {
        self.webtransport_allowed_origins = webtransport_allowed_origins;
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
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        let key = key.into();

        Self {
            key: normalize_metadata_key(&key),
            value: value.into(),
        }
    }

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
    /// Called inline on the RPC task. Implementations must not block. Panics are isolated.
    fn rpc_started(&self, _event: &RpcStarted) {}

    /// Called inline on the RPC task. Implementations must not block. Panics are isolated.
    fn rpc_finished(&self, _event: &RpcFinished) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {}

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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.options.frame_size
    }

    #[must_use]
    pub const fn options(&self) -> &ServerOptions {
        &self.options
    }

    pub fn set_options(&mut self, options: ServerOptions) -> &mut Self {
        self.options = options;
        self
    }

    pub fn set_max_frame_size(&mut self, max_frame_size: usize) -> &mut Self {
        self.options.frame_size = max_frame_size;
        self
    }

    pub fn set_max_concurrent_connections(
        &mut self,
        max_concurrent_connections: Option<usize>,
    ) -> &mut Self {
        self.options.connections = max_concurrent_connections;
        self
    }

    pub fn set_max_concurrent_streams_per_connection(
        &mut self,
        max_concurrent_streams_per_connection: Option<usize>,
    ) -> &mut Self {
        self.options.streams_per_connection = max_concurrent_streams_per_connection;
        self
    }

    pub fn set_max_concurrent_requests(
        &mut self,
        max_concurrent_requests: Option<usize>,
    ) -> &mut Self {
        self.options.requests = max_concurrent_requests;
        self
    }

    pub fn set_graceful_shutdown_timeout(
        &mut self,
        graceful_shutdown_timeout: Option<Duration>,
    ) -> &mut Self {
        self.options.shutdown_timeout = graceful_shutdown_timeout;
        self
    }

    pub fn set_initial_request_timeout(
        &mut self,
        initial_request_timeout: Option<Duration>,
    ) -> &mut Self {
        self.options.initial_request_timeout = initial_request_timeout;
        self
    }

    pub fn set_max_stream_messages(&mut self, max_stream_messages: Option<usize>) -> &mut Self {
        self.options.stream_messages = max_stream_messages;
        self
    }

    pub fn set_max_stream_body_size(&mut self, max_stream_body_size: Option<usize>) -> &mut Self {
        self.options.stream_body_size = max_stream_body_size;
        self
    }

    pub fn set_stream_idle_timeout(&mut self, stream_idle_timeout: Option<Duration>) -> &mut Self {
        self.options.stream_idle_timeout = stream_idle_timeout;
        self
    }

    pub fn set_webtransport_path(&mut self, webtransport_path: &'static str) -> &mut Self {
        self.options.webtransport_path = webtransport_path;
        self
    }

    pub fn set_webtransport_allowed_authorities(
        &mut self,
        webtransport_allowed_authorities: &'static [&'static str],
    ) -> &mut Self {
        self.options.webtransport_allowed_authorities = webtransport_allowed_authorities;
        self
    }

    pub fn set_webtransport_allowed_origins(
        &mut self,
        webtransport_allowed_origins: &'static [&'static str],
    ) -> &mut Self {
        self.options.webtransport_allowed_origins = webtransport_allowed_origins;
        self
    }

    pub fn set_authorizer<A>(&mut self, authorizer: A) -> &mut Self
    where
        A: Authorizer,
    {
        self.authorizer = Some(Arc::new(authorizer));
        self
    }

    pub fn clear_authorizer(&mut self) -> &mut Self {
        self.authorizer = None;
        self
    }

    pub fn set_metrics<M>(&mut self, metrics: M) -> &mut Self
    where
        M: Metrics,
    {
        self.metrics = Arc::new(metrics);
        self
    }

    #[must_use]
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    pub fn route<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        handler: F,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        Arc::make_mut(&mut self.routes).insert(
            MethodKey::new(service, method),
            Route::Unary(Arc::new(move |body| Box::pin(handler(body)))),
        );
    }

    pub fn route_streaming<F, Fut>(
        &mut self,
        service: impl Into<String>,
        method: impl Into<String>,
        kind: RpcKind,
        handler: F,
    ) where
        F: Fn(Vec<u8>, BoxMessageStream<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BoxMessageStream<Vec<u8>>>> + Send + 'static,
    {
        Arc::make_mut(&mut self.routes).insert(
            MethodKey::new(service, method),
            Route::Streaming {
                kind,
                handler: Arc::new(move |body, stream| Box::pin(handler(body, stream))),
            },
        );
    }

    pub async fn handle_request(&self, request: RpcRequest) -> RpcResponse {
        let started_at = Instant::now();
        let service = request.service.clone();
        let method = request.method.clone();
        let request_body_len = request.body.len();

        record_rpc_started(
            self.metrics.as_ref(),
            &RpcStarted {
                service: service.clone(),
                method: method.clone(),
                request_body_len,
            },
        );

        #[cfg(feature = "tracing")]
        tracing::info!(
            service = %service,
            method = %method,
            request_body_len,
            metadata_count = request.metadata.len(),
            "rpc started"
        );

        let deadline = match self.prepare_request(&request).await {
            Ok(deadline) => deadline,
            Err(status) => {
                return self.finish_response(
                    &service,
                    &method,
                    request_body_len,
                    started_at,
                    status.into_response(Vec::new()),
                );
            }
        };

        let key = MethodKey::new(request.service.clone(), request.method.clone());
        let Some(Route::Unary(handler)) = self.routes.get(&key).cloned() else {
            return self.finish_response(
                &service,
                &method,
                request_body_len,
                started_at,
                Status::unimplemented(format!(
                    "unknown RPC method {}/{}",
                    request.service, request.method
                ))
                .into_response(Vec::new()),
            );
        };

        let response = match invoke_unary_handler(&handler, request.body) {
            Ok(future) => match run_handler_with_deadline(future, deadline).await {
                Ok(Ok(body)) => RpcResponse::ok(body),
                Ok(Err(error)) => error.into_status().into_response(Vec::new()),
                Err(status) => status.into_response(Vec::new()),
            },
            Err(status) => status.into_response(Vec::new()),
        };

        self.finish_response(&service, &method, request_body_len, started_at, response)
    }

    pub async fn handle_streaming_request(
        &self,
        request: RpcRequest,
        request_body: BoxMessageStream<Vec<u8>>,
    ) -> BoxMessageStream<RpcStreamFrame> {
        let started_at = Instant::now();
        let service = request.service.clone();
        let method = request.method.clone();
        let request_body_len = request.body.len();

        record_rpc_started(
            self.metrics.as_ref(),
            &RpcStarted {
                service: service.clone(),
                method: method.clone(),
                request_body_len,
            },
        );

        #[cfg(feature = "tracing")]
        tracing::info!(
            service = %service,
            method = %method,
            request_body_len,
            metadata_count = request.metadata.len(),
            kind = ?request.rpc_kind(),
            "streaming rpc started"
        );

        let deadline = match self.prepare_request(&request).await {
            Ok(deadline) => deadline,
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
        let stream_limits = StreamLimits::from_options(self.options(), deadline);
        let request_body = limit_stream(request_body, stream_limits, "request");

        let key = MethodKey::new(request.service.clone(), request.method.clone());
        let Some(Route::Streaming { kind, handler }) = self.routes.get(&key).cloned() else {
            let status = Status::unimplemented(format!(
                "unknown streaming RPC method {}/{}",
                request.service, request.method
            ));
            return self.finish_streaming_status(
                &service,
                &method,
                request_body_len,
                started_at,
                status,
            );
        };

        if request.rpc_kind() != kind {
            let status = Status::invalid_argument(format!(
                "streaming RPC kind mismatch for {}/{}: expected {:?}, got {:?}",
                request.service,
                request.method,
                kind,
                request.rpc_kind()
            ));
            return self.finish_streaming_status(
                &service,
                &method,
                request_body_len,
                started_at,
                status,
            );
        }

        let handler_result = match invoke_streaming_handler(&handler, request.body, request_body) {
            Ok(future) => run_handler_with_deadline(future, deadline).await,
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
            Ok(Ok(response_body)) => Box::new(ServerResponseStream::new(
                response_body,
                Arc::clone(&self.metrics),
                service,
                method,
                request_body_len,
                started_at,
                stream_limits,
            )),
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
    ) -> std::result::Result<Option<Instant>, Status> {
        validate_metadata(&request.metadata)?;
        request.validate_protocol()?;
        let deadline = request_deadline(request)?;

        if let Some(authorizer) = &self.authorizer {
            with_deadline(authorizer.authorize(request), deadline).await??;
        }

        Ok(deadline)
    }

    fn finish_response(
        &self,
        service: &str,
        method: &str,
        request_body_len: usize,
        started_at: Instant,
        response: RpcResponse,
    ) -> RpcResponse {
        let elapsed = started_at.elapsed();
        let code = Code::from_u32(response.status);

        record_rpc_finished(
            self.metrics.as_ref(),
            &RpcFinished {
                service: service.to_owned(),
                method: method.to_owned(),
                request_body_len,
                response_body_len: response.body.len(),
                code,
                elapsed,
            },
        );

        #[cfg(feature = "tracing")]
        tracing::info!(
            service = %service,
            method = %method,
            request_body_len,
            response_body_len = response.body.len(),
            status = ?code,
            elapsed_ms = elapsed.as_millis(),
            "rpc finished"
        );

        response
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
    ) -> BoxMessageStream<RpcStreamFrame> {
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

    pub(crate) fn record_rejected_request(&self, request: &RpcRequest, status: &Status) {
        let started_at = Instant::now();
        record_rpc_started(
            self.metrics.as_ref(),
            &RpcStarted {
                service: request.service.clone(),
                method: request.method.clone(),
                request_body_len: request.body.len(),
            },
        );
        self.finish_streaming_response(
            &request.service,
            &request.method,
            request.body.len(),
            started_at,
            0,
            status.code(),
        );
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
    body: Vec<u8>,
) -> std::result::Result<UnaryHandlerFuture, Status> {
    catch_unwind(AssertUnwindSafe(|| handler(body)))
        .map_err(|_| Status::internal("RPC handler panicked"))
}

fn invoke_streaming_handler(
    handler: &StreamingHandler,
    body: Vec<u8>,
    request_body: BoxMessageStream<Vec<u8>>,
) -> std::result::Result<StreamingHandlerFuture, Status> {
    catch_unwind(AssertUnwindSafe(|| handler(body, request_body)))
        .map_err(|_| Status::internal("RPC handler panicked"))
}

async fn run_handler_with_deadline<T, F>(
    future: F,
    deadline: Option<Instant>,
) -> std::result::Result<T, Status>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    match tokio::spawn(with_deadline(future, deadline)).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => Err(Status::internal("RPC handler panicked")),
        Err(error) => Err(Status::internal(format!(
            "RPC handler task failed: {error}"
        ))),
    }
}

fn request_deadline(request: &RpcRequest) -> std::result::Result<Option<Instant>, Status> {
    if request.timeout_nanos == 0 {
        return Ok(None);
    }

    if request.timeout_nanos > i64::MAX as u64 {
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

fn status_stream(status: Status) -> BoxMessageStream<RpcStreamFrame> {
    crate::stream::from_iter([RpcStreamFrame::status(status)])
}

#[derive(Clone, Copy)]
struct StreamLimits {
    max_messages: Option<usize>,
    max_body_size: Option<usize>,
    idle_timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl StreamLimits {
    fn from_options(options: &ServerOptions, deadline: Option<Instant>) -> Self {
        Self {
            max_messages: options.max_stream_messages(),
            max_body_size: options.max_stream_body_size(),
            idle_timeout: options.stream_idle_timeout(),
            deadline,
        }
    }
}

struct LimitedStream {
    inner: BoxMessageStream<Vec<u8>>,
    limits: StreamLimits,
    direction: &'static str,
    messages: usize,
    body_size: usize,
    done: bool,
}

fn limit_stream(
    inner: BoxMessageStream<Vec<u8>>,
    limits: StreamLimits,
    direction: &'static str,
) -> BoxMessageStream<Vec<u8>> {
    Box::new(LimitedStream {
        inner,
        limits,
        direction,
        messages: 0,
        body_size: 0,
        done: false,
    })
}

#[crate::async_trait]
impl MessageStream<Vec<u8>> for LimitedStream {
    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        if self.done {
            return None;
        }

        let Some(body) =
            (match next_limited_item(&mut self.inner, self.limits, self.direction).await {
                Ok(body) => body,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            })
        else {
            self.done = true;
            return None;
        };

        if let Err(error) = check_stream_limits(
            self.direction,
            self.limits,
            &mut self.messages,
            &mut self.body_size,
            body.len(),
        ) {
            self.done = true;
            return Some(Err(error));
        }

        Some(Ok(body))
    }
}

async fn next_limited_item(
    inner: &mut BoxMessageStream<Vec<u8>>,
    limits: StreamLimits,
    direction: &'static str,
) -> Result<Option<Vec<u8>>> {
    match stream_next_timeout(limits.deadline, limits.idle_timeout)? {
        Some((timeout, reason)) => tokio::time::timeout(timeout, inner.next())
            .await
            .map_err(|_| Error::from(reason.status(direction)))?
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
    if let Some(max) = limits.max_messages
        && *messages >= max
    {
        return Err(Error::from(Status::resource_exhausted(format!(
            "{direction} stream exceeded maximum of {max} messages"
        ))));
    }

    *messages = messages.saturating_add(1);
    *body_size = body_size.saturating_add(item_len);

    if let Some(max) = limits.max_body_size
        && *body_size > max
    {
        return Err(Error::from(Status::resource_exhausted(format!(
            "{direction} stream exceeded maximum body size of {max} bytes"
        ))));
    }

    Ok(())
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

struct ServerResponseStream {
    inner: BoxMessageStream<Vec<u8>>,
    metrics: Arc<dyn Metrics>,
    service: String,
    method: String,
    request_body_len: usize,
    response_body_len: usize,
    started_at: Instant,
    limits: StreamLimits,
    messages: usize,
    done: bool,
}

impl ServerResponseStream {
    fn new(
        inner: BoxMessageStream<Vec<u8>>,
        metrics: Arc<dyn Metrics>,
        service: String,
        method: String,
        request_body_len: usize,
        started_at: Instant,
        limits: StreamLimits,
    ) -> Self {
        Self {
            inner,
            metrics,
            service,
            method,
            request_body_len,
            response_body_len: 0,
            started_at,
            limits,
            messages: 0,
            done: false,
        }
    }

    fn finish(&mut self, code: Code) {
        self.done = true;
        finish_streaming_response(
            self.metrics.as_ref(),
            &self.service,
            &self.method,
            self.request_body_len,
            self.started_at,
            self.response_body_len,
            code,
        );
    }
}

#[crate::async_trait]
impl MessageStream<RpcStreamFrame> for ServerResponseStream {
    async fn next(&mut self) -> Option<Result<RpcStreamFrame>> {
        if self.done {
            return None;
        }

        match next_limited_item(&mut self.inner, self.limits, "response").await {
            Ok(Some(body)) => {
                if let Err(error) = check_stream_limits(
                    "response",
                    self.limits,
                    &mut self.messages,
                    &mut self.response_body_len,
                    body.len(),
                ) {
                    let status = error.into_status();
                    self.finish(status.code());
                    return Some(Ok(RpcStreamFrame::status(status)));
                }

                Some(Ok(RpcStreamFrame::message(body)))
            }
            Err(error) => {
                let status = error.into_status();
                self.finish(status.code());
                Some(Ok(RpcStreamFrame::status(status)))
            }
            Ok(None) => {
                self.finish(Code::Ok);
                Some(Ok(RpcStreamFrame::status(Status::ok())))
            }
        }
    }
}

impl Drop for ServerResponseStream {
    fn drop(&mut self) {
        if !self.done {
            self.finish(Code::Cancelled);
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
        &RpcFinished {
            service: service.to_owned(),
            method: method.to_owned(),
            request_body_len,
            response_body_len,
            code,
            elapsed,
        },
    );

    #[cfg(feature = "tracing")]
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

fn record_rpc_started(metrics: &dyn Metrics, event: &RpcStarted) {
    let _ = catch_unwind(AssertUnwindSafe(|| metrics.rpc_started(event)));
}

fn record_rpc_finished(metrics: &dyn Metrics, event: &RpcFinished) {
    let _ = catch_unwind(AssertUnwindSafe(|| metrics.rpc_finished(event)));
}

impl From<Error> for RpcResponse {
    fn from(error: Error) -> Self {
        error.into_status().into_response(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::{Code, Error, MessageStream, RpcKind, RpcRequest, RpcStreamFrameKind, Status};

    use super::{
        Authorizer, MetadataValueAuthorizer, Metrics, RpcFinished, Server, ServerOptions,
        StreamLimits, check_stream_limits,
    };

    #[derive(Clone, Default)]
    struct TestMetrics {
        started: Arc<Mutex<usize>>,
        finished: Arc<Mutex<Vec<Code>>>,
    }

    impl Metrics for TestMetrics {
        fn rpc_started(&self, _event: &super::RpcStarted) {
            *self
                .started
                .lock()
                .expect("metrics lock should not be poisoned") += 1;
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

        let mut request = RpcRequest::new("example.Greeter", "SayHello", Vec::new());
        request
            .metadata
            .insert("authorization".to_owned(), b"ok".to_vec());

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), Code::Ok);
        assert_eq!(response.body, b"hello");
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

        let response = server
            .handle_request(RpcRequest::new("example.Greeter", "SayHello", Vec::new()))
            .await;

        assert_eq!(Code::from_u32(response.status), Code::Unauthenticated);
        assert!(response.body.is_empty());
    }

    #[tokio::test]
    async fn metrics_panics_do_not_fail_request() {
        let mut server = Server::new();
        server.set_metrics(PanickingMetrics);
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });

        let response = server
            .handle_request(RpcRequest::new("example.Greeter", "SayHello", Vec::new()))
            .await;

        assert_eq!(Code::from_u32(response.status), Code::Ok);
        assert_eq!(response.body, b"hello");
    }

    #[tokio::test]
    async fn unary_completion_metrics_are_exactly_once_for_terminal_statuses() {
        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "SayHello", |_| async {
                    Ok(b"hello".to_vec())
                });
            },
            RpcRequest::new("example.Greeter", "SayHello", Vec::new()),
            Code::Ok,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "Denied", |_| async {
                    Err(Error::from(Status::new(Code::PermissionDenied, "denied")))
                });
            },
            RpcRequest::new("example.Greeter", "Denied", Vec::new()),
            Code::PermissionDenied,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "Panic", |_| async {
                    panic!("boom");
                });
            },
            RpcRequest::new("example.Greeter", "Panic", Vec::new()),
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
            RpcRequest::new("example.Greeter", "SayHello", Vec::new()),
            Code::Unauthenticated,
        )
        .await;

        expect_unary_completion(
            |server| {
                server.route("example.Greeter", "Slow", |_| async {
                    std::future::pending::<crate::Result<Vec<u8>>>().await
                });
            },
            RpcRequest::new("example.Greeter", "Slow", Vec::new()).with_timeout_nanos(1_000_000),
            Code::DeadlineExceeded,
        )
        .await;

        let mut invalid_metadata = RpcRequest::new("example.Greeter", "SayHello", Vec::new());
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

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), expected);
        assert_eq!(observed.started_count(), 1);
        assert_eq!(observed.finished_codes(), vec![expected]);
    }

    #[tokio::test]
    async fn unary_handler_panics_return_internal() {
        let mut server = Server::new();
        server.route("example.Greeter", "Panic", |_| async {
            panic!("boom");
        });

        let response = server
            .handle_request(RpcRequest::new("example.Greeter", "Panic", Vec::new()))
            .await;

        assert_eq!(Code::from_u32(response.status), Code::Internal);
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
        let request = RpcRequest::new("example.Greeter", "Panic", Vec::new())
            .with_kind(RpcKind::ServerStreaming);

        let mut response = server
            .handle_streaming_request(request, crate::stream::empty())
            .await;
        let frame = response
            .next()
            .await
            .expect("status frame should be emitted")
            .expect("status frame should not be transport error");

        assert_eq!(frame.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(Code::from_u32(frame.status), Code::Internal);
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
        let request = RpcRequest::new("example.Greeter", "SayHello", Vec::new())
            .with_timeout_nanos(1_000_000);

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
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

        let mut request = RpcRequest::new("example.Greeter", "SayHello", Vec::new());
        request
            .metadata
            .insert("Authorization".to_owned(), b"ok".to_vec());

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);
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

        let response = server
            .handle_request(RpcRequest::new("example.Greeter", "Missing", Vec::new()))
            .await;

        assert_eq!(Code::from_u32(response.status), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn unsupported_wire_versions_are_rejected_before_route_lookup() {
        let mut server = Server::new();
        server.route("example.Greeter", "SayHello", |_| async {
            Ok(b"hello".to_vec())
        });
        let mut request = RpcRequest::new("example.Greeter", "Missing", Vec::new());
        request.version = crate::wire::WIRE_VERSION + 1;

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn unsupported_rpc_kinds_are_rejected_before_route_lookup() {
        let server = Server::new();
        let mut request = RpcRequest::new("example.Greeter", "Missing", Vec::new());
        request.kind = 99;

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);
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
        let mut request = RpcRequest::new("example.Greeter", "Missing", Vec::new());
        request.timeout_nanos = u64::MAX;

        let response = server.handle_request(request).await;

        assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);
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

        let request = RpcRequest::new("example.Greeter", "StreamHello", b"request".to_vec())
            .with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, crate::stream::empty())
            .await;

        let first = response
            .next()
            .await
            .expect("stream should yield first frame")
            .expect("first frame should be ok");
        assert_eq!(first.frame_kind(), Some(RpcStreamFrameKind::Message));
        assert_eq!(first.body, b"one".to_vec());

        let second = response
            .next()
            .await
            .expect("stream should yield second frame")
            .expect("second frame should be ok");
        assert_eq!(second.frame_kind(), Some(RpcStreamFrameKind::Message));
        assert_eq!(second.body, b"two".to_vec());

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(Code::from_u32(status.status), Code::Ok);
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
                Ok(Box::new(PendingStream) as crate::BoxMessageStream<Vec<u8>>)
            },
        );

        let request = RpcRequest::new("example.Greeter", "Download", Vec::new())
            .with_kind(RpcKind::ServerStreaming);
        let response = server
            .handle_streaming_request(request, crate::stream::empty())
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

        let request = RpcRequest::new("example.Greeter", "Upload", Vec::new())
            .with_kind(RpcKind::ClientStreaming);
        let mut response = server
            .handle_streaming_request(
                request,
                crate::stream::from_iter([b"one".to_vec(), b"two".to_vec()]),
            )
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(Code::from_u32(status.status), Code::ResourceExhausted);
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

        let request = RpcRequest::new("example.Greeter", "Download", Vec::new())
            .with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, crate::stream::empty())
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(Code::from_u32(status.status), Code::ResourceExhausted);
        assert!(response.next().await.is_none());
    }

    struct PendingStream;

    #[crate::async_trait]
    impl MessageStream<Vec<u8>> for PendingStream {
        async fn next(&mut self) -> Option<crate::Result<Vec<u8>>> {
            std::future::pending().await
        }
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

        let request = RpcRequest::new("example.Greeter", "Upload", Vec::new())
            .with_kind(RpcKind::ClientStreaming);
        let mut response = server
            .handle_streaming_request(request, Box::new(PendingStream))
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(Code::from_u32(status.status), Code::Unavailable);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_errors_are_converted_to_status_frames() {
        struct ErrorStream;

        #[crate::async_trait]
        impl MessageStream<Vec<u8>> for ErrorStream {
            async fn next(&mut self) -> Option<crate::Result<Vec<u8>>> {
                Some(Err(Error::from(crate::Status::unavailable("down"))))
            }
        }

        let mut server = Server::new();
        server.route_streaming(
            "example.Greeter",
            "Download",
            RpcKind::ServerStreaming,
            |_body, _requests| async move {
                Ok(Box::new(ErrorStream) as crate::BoxMessageStream<Vec<u8>>)
            },
        );

        let request = RpcRequest::new("example.Greeter", "Download", Vec::new())
            .with_kind(RpcKind::ServerStreaming);
        let mut response = server
            .handle_streaming_request(request, crate::stream::empty())
            .await;

        let status = response
            .next()
            .await
            .expect("stream should yield status frame")
            .expect("status frame should be ok");
        assert_eq!(status.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(Code::from_u32(status.status), Code::Unavailable);
        assert_eq!(status.message, "down");
        assert!(response.next().await.is_none());
    }
}
