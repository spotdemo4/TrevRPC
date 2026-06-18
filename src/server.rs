use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::{Error, Result, RpcRequest, RpcResponse, Status};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerOptions {
    frame_size: usize,
    connections: Option<usize>,
    streams_per_connection: Option<usize>,
    requests: Option<usize>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            frame_size: DEFAULT_MAX_FRAME_SIZE,
            connections: Some(1024),
            streams_per_connection: Some(128),
            requests: Some(4096),
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
}

impl Default for Server {
    fn default() -> Self {
        Self {
            routes: Arc::new(HashMap::new()),
            options: ServerOptions::default(),
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
        let key = MethodKey::new(request.service.clone(), request.method.clone());
        let Some(handler) = self.routes.get(&key).cloned() else {
            return Status::unimplemented(format!(
                "unknown RPC method {}/{}",
                request.service, request.method
            ))
            .into_response(Vec::new());
        };

        match handler(request.body).await {
            Ok(body) => RpcResponse::ok(body),
            Err(error) => error.into_status().into_response(Vec::new()),
        }
    }
}

impl From<Error> for RpcResponse {
    fn from(error: Error) -> Self {
        error.into_status().into_response(Vec::new())
    }
}
