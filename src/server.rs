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
    max_frame_size: usize,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            routes: Arc::new(HashMap::new()),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
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
        self.max_frame_size
    }

    pub fn set_max_frame_size(&mut self, max_frame_size: usize) -> &mut Self {
        self.max_frame_size = max_frame_size;
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
