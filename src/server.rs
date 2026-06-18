use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{Code, Error, Result, RpcRequest, RpcResponse, Status};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>>;
type Handler = Arc<dyn Fn(Vec<u8>) -> HandlerFuture + Send + Sync + 'static>;

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
    routes: Arc<HashMap<MethodKey, Handler>>,
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
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            frame_size: DEFAULT_MAX_FRAME_SIZE,
            connections: Some(1024),
            streams_per_connection: Some(128),
            requests: Some(4096),
            shutdown_timeout: Some(Duration::from_secs(30)),
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
        if request.metadata.get(&self.key) == Some(&self.value) {
            Ok(())
        } else {
            Err(Status::unauthenticated("request is not authenticated"))
        }
    }
}

pub trait Metrics: Send + Sync + 'static {
    /// Called inline on the RPC task. Implementations must not block.
    fn rpc_started(&self, _event: &RpcStarted) {}

    /// Called inline on the RPC task. Implementations must not block.
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
            Arc::new(move |body| Box::pin(handler(body))),
        );
    }

    pub async fn handle_request(&self, request: RpcRequest) -> RpcResponse {
        let started_at = Instant::now();
        let service = request.service.clone();
        let method = request.method.clone();
        let request_body_len = request.body.len();

        self.metrics.rpc_started(&RpcStarted {
            service: service.clone(),
            method: method.clone(),
            request_body_len,
        });

        #[cfg(feature = "tracing")]
        tracing::info!(
            service = %service,
            method = %method,
            request_body_len,
            metadata_count = request.metadata.len(),
            "rpc started"
        );

        if let Err(status) = validate_metadata(&request.metadata) {
            return self.finish_response(
                &service,
                &method,
                request_body_len,
                started_at,
                status.into_response(Vec::new()),
            );
        }

        if let Some(authorizer) = &self.authorizer {
            match authorizer.authorize(&request).await {
                Ok(()) => {}
                Err(status) => {
                    return self.finish_response(
                        &service,
                        &method,
                        request_body_len,
                        started_at,
                        status.into_response(Vec::new()),
                    );
                }
            }
        }

        let key = MethodKey::new(request.service.clone(), request.method.clone());
        let Some(handler) = self.routes.get(&key).cloned() else {
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

        let response = match handler(request.body).await {
            Ok(body) => RpcResponse::ok(body),
            Err(error) => error.into_status().into_response(Vec::new()),
        };

        self.finish_response(&service, &method, request_body_len, started_at, response)
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

        self.metrics.rpc_finished(&RpcFinished {
            service: service.to_owned(),
            method: method.to_owned(),
            request_body_len,
            response_body_len: response.body.len(),
            code,
            elapsed,
        });

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
}

impl From<Error> for RpcResponse {
    fn from(error: Error) -> Self {
        error.into_status().into_response(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{Code, RpcRequest};

    use super::{MetadataValueAuthorizer, Metrics, RpcFinished, Server};

    #[derive(Clone, Default)]
    struct TestMetrics {
        finished: Arc<Mutex<Vec<Code>>>,
    }

    impl Metrics for TestMetrics {
        fn rpc_finished(&self, event: &RpcFinished) {
            self.finished
                .lock()
                .expect("metrics lock should not be poisoned")
                .push(event.code);
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
}
