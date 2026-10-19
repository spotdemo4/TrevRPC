//! Native `TrevRPC` over `QMux` over TLS: the fallback for networks that block UDP.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::mux::MuxConnection as _;
use crate::qmux_conn::{QmuxConnection, client_config};
use crate::{
    BoxStream, Error, ResponseEnvelope, ResponseFrame, Result, RpcRequest, Status, Unreliable,
};

use bytes::Bytes;

use super::native::NativeTransport;
use super::{RpcTransport, StreamingRpcTransport};

/// Bounds connecting, the TLS handshake and the `QMux` handshake, so a server that accepts TCP but
/// never answers cannot stall a reconnect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A native `TrevRPC` transport over one `QMux` over TLS connection ([`crate::QMUX_ALPN`]).
///
/// The transport never reconnects. Most applications should use [`super::Channel`], which
/// falls back to it when QUIC is unavailable.
#[derive(Clone)]
pub struct QmuxTransport(Arc<NativeTransport<QmuxConnection>>);

impl QmuxTransport {
    /// Connects to the TCP listener of a `TrevRPC` server with the default maximum frame size.
    ///
    /// `tls` verifies the server; its ALPN list is replaced with [`crate::QMUX_ALPN`].
    pub async fn connect(
        remote_addr: SocketAddr,
        server_name: &str,
        tls: quinn::rustls::ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_max_frame_size(remote_addr, server_name, tls, DEFAULT_MAX_FRAME_SIZE)
            .await
    }

    /// Connects like [`QmuxTransport::connect`] with a maximum `TrevRPC` frame size in bytes.
    pub async fn connect_with_max_frame_size(
        remote_addr: SocketAddr,
        server_name: &str,
        tls: quinn::rustls::ClientConfig,
        max_frame_size: usize,
    ) -> Result<Self> {
        tokio::time::timeout(
            CONNECT_TIMEOUT,
            Self::connect_unbounded(remote_addr, server_name, tls, max_frame_size),
        )
        .await
        .map_err(|_| {
            Error::from(Status::unavailable(
                "transport unavailable: QMux over TLS connection timed out",
            ))
        })?
    }

    async fn connect_unbounded(
        remote_addr: SocketAddr,
        server_name: &str,
        mut tls: quinn::rustls::ClientConfig,
        max_frame_size: usize,
    ) -> Result<Self> {
        tls.alpn_protocols = vec![crate::QMUX_ALPN.to_vec()];
        let server_name = quinn::rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|_| Error::from(Status::invalid_argument("invalid TLS server name")))?;
        let stream = TcpStream::connect(remote_addr)
            .await
            .map_err(Error::transport)?;
        let _ = stream.set_nodelay(true);
        let stream = tokio_rustls::TlsConnector::from(Arc::new(tls))
            .connect(server_name, stream)
            .await
            .map_err(Error::transport)?;
        if stream.get_ref().1.alpn_protocol() != Some(crate::QMUX_ALPN) {
            return Err(Error::from(Status::unavailable(
                "transport unavailable: server did not negotiate TrevRPC over QMux",
            )));
        }
        let transport = qmux::transport::Stream::new(
            stream,
            qmux::Version::QMux02,
            crate::qmux_conn::MAX_RECORD_SIZE,
        );
        let session = qmux::Session::connect(transport, client_config(max_frame_size))
            .await
            .map_err(Error::transport)?;
        Ok(Self(Arc::new(NativeTransport::new(
            session.into(),
            max_frame_size,
        ))))
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub fn max_frame_size(&self) -> usize {
        self.0.max_frame_size
    }

    /// Resolves with the reason once the connection has ended.
    pub async fn closed(&self) -> Error {
        self.0.connection.connection_lost().await
    }

    /// Closes the connection.
    pub fn close(&self) {
        self.0.connection.close_connection(b"client closed");
    }
}

#[crate::async_trait]
impl RpcTransport for QmuxTransport {
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        self.0.call(request, body).await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for QmuxTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.0.streaming_call(request, request_body).await
    }

    async fn streaming_call_unreliable(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        unreliable: Unreliable,
    ) -> Result<BoxStream<ResponseFrame>> {
        // `QMux` has no datagrams: the call keeps every message on its stream.
        self.0
            .streaming_call_unreliable(request, request_body, unreliable)
            .await
    }
}
