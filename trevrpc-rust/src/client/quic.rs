//! Native QUIC call transport over one established Quinn connection.

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::{BoxStream, Result, RpcRequest, RpcResponse, RpcStreamFrame};

use super::native::NativeTransport;
use super::{RpcTransport, StreamingRpcTransport};

/// A native `TrevRPC` transport over one established Quinn connection.
///
/// The transport never reconnects. Most applications should use [`super::Channel`], which
/// supervises a fresh `QuicTransport` for every connection generation.
#[derive(Clone)]
pub struct QuicTransport(NativeTransport<quinn::Connection>);

impl QuicTransport {
    /// Creates a transport over an established connection with the default maximum frame size.
    #[must_use]
    pub const fn new(connection: quinn::Connection) -> Self {
        Self(NativeTransport {
            connection,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        })
    }

    /// Sets the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.0.max_frame_size = max_frame_size;
        self
    }

    /// Returns the established connection.
    #[must_use]
    pub const fn connection(&self) -> &quinn::Connection {
        &self.0.connection
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.0.max_frame_size
    }
}

#[crate::async_trait]
impl RpcTransport for QuicTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        self.0.call(request).await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for QuicTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        self.0.streaming_call(request, request_body).await
    }
}
