//! Native QUIC call transport over one established Quinn connection.

use std::sync::Arc;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::{BoxStream, ResponseEnvelope, ResponseFrame, Result, RpcRequest};

use bytes::Bytes;

use super::native::NativeTransport;
use super::{CallInfo, RpcTransport, StreamingRpcTransport};

/// A native `TrevRPC` transport over one established Quinn connection.
///
/// The transport never reconnects. Most applications should use [`super::Channel`], which
/// supervises a fresh `QuicTransport` for every connection generation.
#[derive(Clone)]
pub struct QuicTransport(NativeTransport<quinn::Connection>);

impl QuicTransport {
    /// Creates a transport over an established connection with the default maximum frame size.
    #[must_use]
    pub fn new(connection: quinn::Connection) -> Self {
        Self(NativeTransport::new(connection, DEFAULT_MAX_FRAME_SIZE))
    }

    /// Sets the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.0.max_frame_size = max_frame_size;
        self
    }

    /// Lets messages travel compressed with the dictionaries of `compression`; see
    /// [`Compression`](crate::Compression). Fails when they are invalid.
    pub fn with_compression(self, compression: &crate::Compression) -> Result<Self> {
        Ok(self.with_compression_config(
            crate::compression::CompressionConfig::new(compression).map(Some)?,
        ))
    }

    pub(crate) fn with_compression_config(
        mut self,
        config: Option<Arc<crate::compression::CompressionConfig>>,
    ) -> Self {
        self.0.set_compression(config);
        self
    }

    pub(crate) fn with_connection_metadata(
        mut self,
        source: Arc<crate::connection_metadata::ConnectionMetadataSource>,
    ) -> Self {
        self.0.set_connection_metadata(source);
        self
    }

    pub(crate) fn with_stream_budget(
        mut self,
        budget: Option<Arc<crate::compression::StreamBudget>>,
    ) -> Self {
        self.0.set_stream_budget(budget);
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
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        self.0.call(request, body).await
    }

    async fn call_with_info(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        info: CallInfo,
    ) -> Result<ResponseEnvelope<Bytes>> {
        self.0.call_with_info(request, body, info).await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for QuicTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.0.streaming_call(request, request_body).await
    }

    async fn streaming_call_with_info(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.0
            .streaming_call_with_info(request, request_body, info)
            .await
    }
}
