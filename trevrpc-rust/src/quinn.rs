use std::future::{Future, pending};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::advanced::RawQuinnTransport;
use crate::client::{RpcTransport, StreamingRpcTransport};
use crate::client_transport::{ClientResponseBackend, read_unary_response, streaming_response};
use crate::client_upload::UploadWriter;
use crate::framed::{self, FrameRead, FrameTrace, FrameWrite};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::server::ServerOptions;
use crate::server_transport::{self, ResponseWriter, try_acquire_permit};
use crate::{BoxStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

const CANCELLED_STREAM_CODE: u32 = 1;
const FRAME_HEADER_LEN: u64 = 4;
const HTTP3_MANDATORY_UNI_STREAMS: u64 = 3;

fn trace_quinn_event(event: &'static str, detail: &'static str) {
    #[cfg(feature = "tracing")]
    tracing::trace!(target: "trevrpc::quinn::frames", event, detail);
    #[cfg(not(feature = "tracing"))]
    let _ = (event, detail);
}

fn trace_tx_frame<M: Message>(encoded_len: usize) {
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: "trevrpc::quinn::frames",
        direction = "tx",
        frame = std::any::type_name::<M>(),
        encoded_len,
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (encoded_len, std::marker::PhantomData::<M>);
}

fn trace_rx_frame<M: Message>(encoded_len: usize) {
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: "trevrpc::quinn::frames",
        direction = "rx",
        frame = std::any::type_name::<M>(),
        encoded_len,
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (encoded_len, std::marker::PhantomData::<M>);
}

fn trace_tx_stream_message_frame(body_len: usize, batch_len: usize) {
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: "trevrpc::quinn::frames",
        direction = "tx",
        frame = "RpcStreamFrame",
        kind = "message",
        body_len,
        batch_len,
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (body_len, batch_len);
}

fn trace_tx_stream_frame(frame: &RpcStreamFrame, encoded_len: usize) {
    #[cfg(feature = "tracing")]
    {
        let frame_kind = frame.frame_kind();
        let status = frame.status;
        let body_len = frame.body.len();
        tracing::trace!(
            target: "trevrpc::quinn::frames",
            direction = "tx",
            frame = "RpcStreamFrame",
            ?frame_kind,
            status,
            body_len,
            encoded_len,
        );
    }
    #[cfg(not(feature = "tracing"))]
    let _ = (frame, encoded_len);
}

fn trace_rx_stream_frame(frame: &RpcStreamFrame, encoded_len: usize) {
    #[cfg(feature = "tracing")]
    {
        let frame_kind = frame.frame_kind();
        let status = frame.status;
        let body_len = frame.body.len();
        tracing::trace!(
            target: "trevrpc::quinn::frames",
            direction = "rx",
            frame = "RpcStreamFrame",
            ?frame_kind,
            status,
            body_len,
            encoded_len,
        );
    }
    #[cfg(not(feature = "tracing"))]
    let _ = (frame, encoded_len);
}

struct QuinnFrameTrace;

impl FrameTrace for QuinnFrameTrace {
    fn tx_frame<M: Message>(encoded_len: usize) {
        trace_tx_frame::<M>(encoded_len);
    }

    fn tx_stream_message_frame(body_len: usize, batch_len: usize) {
        trace_tx_stream_message_frame(body_len, batch_len);
    }

    fn tx_stream_frame(frame: &RpcStreamFrame, encoded_len: usize) {
        trace_tx_stream_frame(frame, encoded_len);
    }

    fn rx_frame<M: Message>(encoded_len: usize) {
        trace_rx_frame::<M>(encoded_len);
    }

    fn rx_stream_frame(frame: &RpcStreamFrame, encoded_len: usize) {
        trace_rx_stream_frame(frame, encoded_len);
    }

    fn rx_fin(detail: &'static str) {
        trace_quinn_event("rx_fin", detail);
    }
}

impl FrameWrite for quinn::SendStream {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).await.map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        self.write_all_chunks(chunks)
            .await
            .map_err(Error::transport)
    }
}

impl FrameRead for quinn::RecvStream {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.read(bytes).await.map_err(Error::transport)
    }
}

struct QuinnResponseWriter {
    send: quinn::SendStream,
    max_frame_size: usize,
}

impl QuinnResponseWriter {
    const fn new(send: quinn::SendStream, max_frame_size: usize) -> Self {
        Self {
            send,
            max_frame_size,
        }
    }
}

impl ResponseWriter for QuinnResponseWriter {
    async fn write_response(&mut self, response: &RpcResponse) -> Result<()> {
        framed::write_frame::<_, QuinnFrameTrace, _>(&mut self.send, response, self.max_frame_size)
            .await
    }

    async fn write_message_stream_frames(&mut self, bodies: &mut Vec<Vec<u8>>) -> Result<()> {
        framed::write_message_stream_frames::<_, QuinnFrameTrace>(
            &mut self.send,
            bodies,
            self.max_frame_size,
        )
        .await
    }

    async fn write_stream_frame(&mut self, frame: RpcStreamFrame) -> Result<()> {
        framed::write_stream_frame::<_, QuinnFrameTrace>(&mut self.send, frame, self.max_frame_size)
            .await
    }

    async fn stopped(&mut self) -> Result<()> {
        self.send
            .stopped()
            .await
            .map(|_| ())
            .map_err(Error::transport)
    }

    fn reset(&mut self) {
        let _ = self.send.reset(CANCELLED_STREAM_CODE.into());
    }

    async fn finish(&mut self, detail: &'static str) -> Result<()> {
        let result = self.send.finish().map_err(Error::transport);
        trace_quinn_event("tx_fin", detail);
        result
    }
}

struct QuinnPumpReader(quinn::RecvStream);

impl RequestPumpReader for QuinnPumpReader {
    fn stop_trevrpc(&mut self) {
        trace_quinn_event("tx_stop_sending", "request_pump_stop");
        let _ = self.0.stop(CANCELLED_STREAM_CODE.into());
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        framed::drain_unary_request_end(&mut self.0).await
    }

    async fn read_stream_frame_or_eof(
        &mut self,
        max_frame_size: usize,
    ) -> Result<Option<RpcStreamFrame>> {
        framed::read_stream_frame_or_eof::<_, QuinnFrameTrace>(&mut self.0, max_frame_size).await
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        match self.0.received_reset().await {
            Ok(Some(code)) => Some(RequestTransportEvent::PeerReset(Status::cancelled(
                format!(
                    "request stream reset by peer with code {}",
                    code.into_inner()
                ),
            ))),
            Ok(None) => std::future::pending().await,
            Err(error) => Some(RequestTransportEvent::ConnectionLost(
                Error::transport(error).into_status(),
            )),
        }
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportLimits {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub max_concurrent_bidi_streams: Option<u64>,
    pub max_concurrent_uni_streams: Option<u64>,
}

/// Selects the QUIC stream behavior required by the protocol carried over a Quinn connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportMode {
    /// Native `TrevRPC` uses bidirectional QUIC streams exclusively.
    Native,
    /// Ordinary HTTP/3 uses its three mandatory peer-initiated unidirectional streams.
    Http3,
    /// `WebTransport` requires additional peer-initiated unidirectional HTTP/3 streams.
    WebTransport,
}

impl TransportMode {
    const fn max_concurrent_uni_streams(self) -> Option<u64> {
        match self {
            Self::Native => Some(0),
            Self::Http3 => Some(HTTP3_MANDATORY_UNI_STREAMS),
            Self::WebTransport => None,
        }
    }
}

/// Builds QUIC transport limits from server options.
#[must_use]
pub fn transport_limits_from_server_options(
    options: &ServerOptions,
    mode: TransportMode,
) -> TransportLimits {
    let stream_receive_window = frame_receive_window(options.max_frame_size());

    TransportLimits {
        stream_receive_window,
        connection_receive_window: connection_receive_window(options, stream_receive_window),
        max_concurrent_bidi_streams: options.max_concurrent_streams_per_connection().map(
            |max_streams| {
                // Keep one stream available for an over-limit RPC status. WebTransport also owns
                // one long-lived CONNECT stream, so reserve a second transport-level slot there.
                saturating_usize_to_u64(max_streams).saturating_add(match mode {
                    TransportMode::Native | TransportMode::Http3 => 1,
                    TransportMode::WebTransport => 2,
                })
            },
        ),
        max_concurrent_uni_streams: mode.max_concurrent_uni_streams(),
    }
}

/// Builds QUIC transport limits for a client connection.
#[must_use]
pub fn client_transport_limits(max_frame_size: usize, mode: TransportMode) -> TransportLimits {
    let stream_receive_window = frame_receive_window(max_frame_size);

    TransportLimits {
        stream_receive_window,
        connection_receive_window: stream_receive_window,
        max_concurrent_bidi_streams: Some(0),
        max_concurrent_uni_streams: mode.max_concurrent_uni_streams(),
    }
}

/// Applies `TrevRPC` transport limits to a `Quinn` server config.
pub fn configure_server_config(
    config: &mut quinn::ServerConfig,
    options: &ServerOptions,
    mode: TransportMode,
) {
    let limits = transport_limits_from_server_options(options, mode);
    if let Some(transport) = Arc::get_mut(&mut config.transport) {
        apply_transport_limits(transport, limits);
    } else {
        let mut transport = quinn::TransportConfig::default();
        apply_transport_limits(&mut transport, limits);
        config.transport_config(Arc::new(transport));
    }
}

/// Applies `TrevRPC` transport limits to a `Quinn` client config.
pub fn configure_client_config(
    config: &mut quinn::ClientConfig,
    max_frame_size: usize,
    mode: TransportMode,
) {
    let limits = client_transport_limits(max_frame_size, mode);
    let mut transport = quinn::TransportConfig::default();
    apply_transport_limits(&mut transport, limits);
    config.transport_config(Arc::new(transport));
}

/// Applies concrete transport limits to a `Quinn` transport config.
pub fn apply_transport_limits(config: &mut quinn::TransportConfig, limits: TransportLimits) {
    config.stream_receive_window(varint(limits.stream_receive_window));
    config.receive_window(varint(limits.connection_receive_window));
    if let Some(max_streams) = limits.max_concurrent_bidi_streams {
        config.max_concurrent_bidi_streams(varint(max_streams));
    }
    if let Some(max_streams) = limits.max_concurrent_uni_streams {
        config.max_concurrent_uni_streams(varint(max_streams));
    }
}

fn frame_receive_window(max_frame_size: usize) -> u64 {
    saturating_usize_to_u64(max_frame_size).saturating_add(FRAME_HEADER_LEN)
}

fn connection_receive_window(options: &ServerOptions, stream_receive_window: u64) -> u64 {
    let mut connection_receive_window = stream_receive_window;
    if let Some(max_streams) = options.max_concurrent_streams_per_connection()
        && max_streams > 1
    {
        connection_receive_window =
            stream_receive_window.saturating_mul(saturating_usize_to_u64(max_streams));
    }

    if let Some(max_body_size) = options.max_stream_body_size() {
        let stream_body_window = saturating_usize_to_u64(max_body_size);
        connection_receive_window = connection_receive_window.min(stream_body_window);
        connection_receive_window = connection_receive_window.max(stream_receive_window);
    }

    connection_receive_window
}

fn saturating_usize_to_u64(value: usize) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

fn varint(value: u64) -> quinn::VarInt {
    quinn::VarInt::from_u64(value).unwrap_or(quinn::VarInt::MAX)
}

#[crate::async_trait]
impl RpcTransport for RawQuinnTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self
            .connection()
            .open_bi()
            .await
            .map_err(Error::transport)?;
        let mut streams = CancellableBiStream::new(send, recv, self.max_frame_size());

        write_frame(streams.send_mut(), &request, self.max_frame_size()).await?;
        streams.send_mut().finish().map_err(Error::transport)?;
        trace_quinn_event("tx_fin", "client_unary_request");

        read_unary_response(streams).await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for RawQuinnTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let (send, recv) = self
            .connection()
            .open_bi()
            .await
            .map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size();
        let writer = UploadWriter::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(streaming_response(QuinnResponseBackend::with_writer(
            recv,
            writer,
            self.max_frame_size(),
        )))
    }
}

struct CancellableBiStream {
    send: Option<quinn::SendStream>,
    recv: Option<quinn::RecvStream>,
    max_frame_size: usize,
    complete: bool,
}

impl CancellableBiStream {
    fn new(send: quinn::SendStream, recv: quinn::RecvStream, max_frame_size: usize) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
            max_frame_size,
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn recv_mut(&mut self) -> &mut quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }
}

impl Drop for CancellableBiStream {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        if let Some(send) = &mut self.send {
            trace_quinn_event("tx_reset", "cancellable_bistream_drop");
            let _ = send.reset(CANCELLED_STREAM_CODE.into());
        }

        if let Some(recv) = &mut self.recv {
            trace_quinn_event("tx_stop_sending", "cancellable_bistream_drop");
            let _ = recv.stop(CANCELLED_STREAM_CODE.into());
        }
    }
}

struct CancellableSendStream {
    send: Option<quinn::SendStream>,
    complete: bool,
}

impl CancellableSendStream {
    fn new(send: quinn::SendStream) -> Self {
        Self {
            send: Some(send),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for CancellableSendStream {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        if let Some(send) = &mut self.send {
            trace_quinn_event("tx_reset", "cancellable_send_stream_drop");
            let _ = send.reset(CANCELLED_STREAM_CODE.into());
        }
    }
}

struct QuinnResponseBackend {
    recv: Option<quinn::RecvStream>,
    writer: Option<UploadWriter>,
    max_frame_size: usize,
    complete: bool,
}

impl QuinnResponseBackend {
    const fn with_writer(
        recv: quinn::RecvStream,
        writer: UploadWriter,
        max_frame_size: usize,
    ) -> Self {
        Self {
            recv: Some(recv),
            writer: Some(writer),
            max_frame_size,
            complete: false,
        }
    }

    fn recv_mut(&mut self) -> &mut quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }
}

impl ClientResponseBackend for CancellableBiStream {
    async fn read_unary_response(&mut self) -> Result<RpcResponse> {
        let max_frame_size = self.max_frame_size;
        read_frame(self.recv_mut(), max_frame_size).await
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        if framed::read_raw_frame_or_eof(self.recv_mut(), max_frame_size)
            .await?
            .is_some()
        {
            return Err(Error::from(Status::internal(
                "unary response contained more than one response frame",
            )));
        }
        Ok(())
    }

    async fn read_stream_frame(&mut self) -> Result<Option<RpcStreamFrame>> {
        let max_frame_size = self.max_frame_size;
        read_stream_frame_or_eof(self.recv_mut(), max_frame_size).await
    }

    async fn abort_upload_and_settle(&mut self) -> Result<()> {
        Ok(())
    }

    async fn drain_after_terminal_status(&mut self) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        drain_fin_after_terminal_status(self.recv_mut(), max_frame_size, "response stream").await
    }

    fn mark_complete(&mut self) {
        self.complete = true;
    }
}

impl ClientResponseBackend for QuinnResponseBackend {
    async fn read_unary_response(&mut self) -> Result<RpcResponse> {
        let max_frame_size = self.max_frame_size;
        read_frame(self.recv_mut(), max_frame_size).await
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        if framed::read_raw_frame_or_eof(self.recv_mut(), max_frame_size)
            .await?
            .is_some()
        {
            return Err(Error::from(Status::internal(
                "unary response contained more than one response frame",
            )));
        }
        Ok(())
    }

    async fn read_stream_frame(&mut self) -> Result<Option<RpcStreamFrame>> {
        let max_frame_size = self.max_frame_size;
        read_stream_frame_or_eof(self.recv_mut(), max_frame_size).await
    }

    async fn abort_upload_and_settle(&mut self) -> Result<()> {
        match &mut self.writer {
            Some(writer) => writer.abort_and_settle().await,
            None => Ok(()),
        }
    }

    async fn drain_after_terminal_status(&mut self) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        drain_fin_after_terminal_status(self.recv_mut(), max_frame_size, "response stream").await
    }

    fn mark_complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for QuinnResponseBackend {
    fn drop(&mut self) {
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            trace_quinn_event("tx_stop_sending", "response_stream_drop");
            let _ = recv.stop(CANCELLED_STREAM_CODE.into());
        }
    }
}

/// Writes a length-prefixed protobuf frame to a `Quinn` send stream.
pub async fn write_frame<M>(
    send: &mut quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    framed::write_frame::<_, QuinnFrameTrace, M>(send, message, max_frame_size).await
}

/// Reads and decodes one length-prefixed protobuf frame from a `Quinn` receive stream.
pub async fn read_frame<M>(recv: &mut quinn::RecvStream, max_frame_size: usize) -> Result<M>
where
    M: Message + Default + 'static,
{
    framed::read_frame::<_, QuinnFrameTrace, M>(recv, max_frame_size).await
}

async fn read_stream_frame_or_eof(
    recv: &mut quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>> {
    framed::read_stream_frame_or_eof::<_, QuinnFrameTrace>(recv, max_frame_size).await
}

async fn drain_fin_after_terminal_status(
    recv: &mut quinn::RecvStream,
    max_frame_size: usize,
    stream_name: &'static str,
) -> Result<()> {
    framed::drain_fin_after_terminal_status::<_, QuinnFrameTrace>(recv, max_frame_size, stream_name)
        .await
}

async fn write_streaming_request(
    send: quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    write_request_body_frames(send.send_mut(), &mut request_body, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    trace_quinn_event("tx_fin", "streaming_request");
    send.complete();

    Ok(())
}

async fn write_request_body_frames(
    send: &mut quinn::SendStream,
    request_body: &mut BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_request_body_frames::<_, QuinnFrameTrace>(send, request_body, max_frame_size)
        .await
}

impl crate::server::Server {
    /// Serves `TrevRPC` over a `Quinn` endpoint until the endpoint stops accepting connections.
    pub async fn serve_quinn(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_quinn_with_shutdown(endpoint, pending::<()>())
            .await
    }

    /// Serves `TrevRPC` over a `Quinn` endpoint until the shutdown future completes.
    pub async fn serve_quinn_with_shutdown<S>(
        self,
        endpoint: quinn::Endpoint,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        let connection_limit = self
            .options()
            .max_concurrent_connections()
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let request_limit = self
            .options()
            .max_concurrent_requests()
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connection_tasks = JoinSet::new();

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };

                    let Some(connection_permit) = try_acquire_permit(connection_limit.as_ref()) else {
                        incoming.refuse();
                        continue;
                    };

                    let server = self.clone();
                    let request_limit = request_limit.clone();
                    let shutdown = shutdown_rx.clone();
                    connection_tasks.spawn(async move {
                        if let Ok(connection) = incoming.await {
                            if negotiated_protocol(&connection).as_deref() == Some(crate::ALPN) {
                                let _connection_permit = connection_permit;
                                handle_connection(server, connection, request_limit, shutdown).await;
                            } else {
                                connection.close(0_u32.into(), b"unsupported ALPN");
                            }
                        }
                    });
                }
                () = &mut shutdown => {
                    let _ = shutdown_tx.send(true);
                    break;
                }
                result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    if let Some(Err(error)) = result {
                        let _ = &error;
                        #[cfg(feature = "tracing")]
                        tracing::warn!(%error, "connection task failed");
                    }
                }
            }
        }

        let _ = shutdown_tx.send(true);
        drain_connections(
            &mut connection_tasks,
            self.options().graceful_shutdown_timeout(),
            &endpoint,
        )
        .await;

        Ok(())
    }

    /// Serves native `TrevRPC`, HTTP/3, and WebTransport on the same endpoint.
    ///
    /// The endpoint must advertise both [`crate::ALPN`] and [`crate::HTTP3_ALPN`]. Ordinary HTTP/3
    /// POST RPCs are accepted when enabled in [`crate::server::ServerOptions`].
    #[cfg(feature = "webtransport-server")]
    pub async fn serve_quinn_and_webtransport(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_quinn_and_webtransport_with_shutdown(endpoint, pending::<()>())
            .await
    }

    /// Serves native `TrevRPC`, HTTP/3, and WebTransport until shutdown completes.
    #[cfg(feature = "webtransport-server")]
    pub async fn serve_quinn_and_webtransport_with_shutdown<S>(
        self,
        endpoint: quinn::Endpoint,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        self.serve_quinn_and_http3_with_shutdown(endpoint, shutdown)
            .await
    }
}

fn negotiated_protocol(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol)
}

pub(crate) async fn handle_connection(
    server: crate::server::Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let mut stream_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accepted = connection.accept_bi(), if !*shutdown.borrow() => {
                let Ok((send, recv)) = accepted else {
                    break;
                };

                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    let server = server.clone();
                    stream_tasks.spawn(reject_stream(server, send, recv));
                    continue;
                };

                let server = server.clone();
                let request_limit = request_limit.clone();
                let stream_shutdown = shutdown.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_stream(server, request_limit, send, recv, stream_shutdown).await;
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    let _ = &error;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(%error, "stream task failed");
                }
            }
        }
    }

    drain_streams(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
        &connection,
    )
    .await;

    if *shutdown.borrow() {
        connection.close(0_u32.into(), b"server drained connection");
    }
}

async fn reject_stream(
    server: crate::server::Server,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
) {
    let max_frame_size = server.max_frame_size();
    let mut recv = QuinnPumpReader(recv);
    let request = match read_initial_request(&server, &mut recv.0).await {
        Ok(request) => request,
        Err(error) => {
            let _ = recv.0.stop(CANCELLED_STREAM_CODE.into());
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(QuinnResponseWriter::new(send, max_frame_size), status)
                .await;
            return;
        }
    };
    server_transport::reject_stream(
        server,
        QuinnResponseWriter::new(send, max_frame_size),
        recv,
        request,
    )
    .await;
}

async fn handle_stream(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    shutdown: watch::Receiver<bool>,
) {
    let max_frame_size = server.max_frame_size();
    let mut recv = QuinnPumpReader(recv);
    let request = match read_initial_request(&server, &mut recv.0).await {
        Ok(request) => request,
        Err(error) => {
            let _ = recv.0.stop(CANCELLED_STREAM_CODE.into());
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(QuinnResponseWriter::new(send, max_frame_size), status)
                .await;
            return;
        }
    };
    server_transport::handle_stream(
        server,
        request_limit,
        QuinnResponseWriter::new(send, max_frame_size),
        recv,
        request,
        shutdown,
    )
    .await;
}

async fn read_initial_request(
    server: &crate::server::Server,
    recv: &mut quinn::RecvStream,
) -> Result<RpcRequest> {
    let read = read_frame::<RpcRequest>(recv, server.max_frame_size());
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

async fn drain_streams(
    stream_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    connection: &quinn::Connection,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_stream_tasks(stream_tasks))
            .await
            .is_err()
        {
            connection.close(0_u32.into(), b"server stream drain timed out");
            stream_tasks.abort_all();
            while stream_tasks.join_next().await.is_some() {}
        }
    } else {
        drain_stream_tasks(stream_tasks).await;
    }
}

async fn drain_stream_tasks(stream_tasks: &mut JoinSet<()>) {
    while let Some(result) = stream_tasks.join_next().await {
        if let Err(error) = result {
            let _ = &error;
            #[cfg(feature = "tracing")]
            tracing::warn!(%error, "stream task failed while draining");
        }
    }
}

async fn drain_connections(
    connection_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    endpoint: &quinn::Endpoint,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_connection_tasks(connection_tasks, endpoint))
            .await
            .is_err()
        {
            endpoint.close(0_u32.into(), b"server graceful shutdown timed out");
            connection_tasks.abort_all();
            while connection_tasks.join_next().await.is_some() {}
        } else {
            endpoint.close(0_u32.into(), b"server shutdown complete");
        }
    } else {
        drain_connection_tasks(connection_tasks, endpoint).await;
        endpoint.close(0_u32.into(), b"server shutdown complete");
    }
}

async fn drain_connection_tasks(connection_tasks: &mut JoinSet<()>, endpoint: &quinn::Endpoint) {
    let mut accepting = true;

    while !connection_tasks.is_empty() {
        tokio::select! {
            incoming = endpoint.accept(), if accepting => {
                if let Some(incoming) = incoming {
                    incoming.refuse();
                } else {
                    accepting = false;
                }
            }
            result = connection_tasks.join_next() => {
                if let Some(Err(error)) = result {
                    let _ = &error;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(%error, "connection task failed while draining");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::server::ServerOptions;

    use super::{
        TransportLimits, TransportMode, client_transport_limits,
        transport_limits_from_server_options,
    };

    #[test]
    fn server_transport_limits_align_with_trevrpc_limits() {
        let options = ServerOptions::new()
            .with_max_frame_size(1024)
            .with_max_stream_body_size(Some(4096))
            .with_max_concurrent_streams_per_connection(Some(10));

        assert_eq!(
            transport_limits_from_server_options(&options, TransportMode::Native),
            TransportLimits {
                stream_receive_window: 1028,
                connection_receive_window: 4096,
                max_concurrent_bidi_streams: Some(11),
                max_concurrent_uni_streams: Some(0),
            }
        );
        assert_eq!(
            transport_limits_from_server_options(&options, TransportMode::Http3),
            TransportLimits {
                stream_receive_window: 1028,
                connection_receive_window: 4096,
                max_concurrent_bidi_streams: Some(11),
                max_concurrent_uni_streams: Some(3),
            }
        );
        assert_eq!(
            transport_limits_from_server_options(&options, TransportMode::WebTransport),
            TransportLimits {
                stream_receive_window: 1028,
                connection_receive_window: 4096,
                max_concurrent_bidi_streams: Some(12),
                max_concurrent_uni_streams: None,
            }
        );
    }

    #[test]
    fn client_transport_limits_match_mode_stream_requirements() {
        assert_eq!(
            client_transport_limits(2048, TransportMode::Native),
            TransportLimits {
                stream_receive_window: 2052,
                connection_receive_window: 2052,
                max_concurrent_bidi_streams: Some(0),
                max_concurrent_uni_streams: Some(0),
            }
        );
        assert_eq!(
            client_transport_limits(2048, TransportMode::Http3).max_concurrent_uni_streams,
            Some(3)
        );
        assert_eq!(
            client_transport_limits(2048, TransportMode::WebTransport).max_concurrent_uni_streams,
            None
        );
    }

    #[test]
    fn server_transport_limit_calculation_saturates() {
        let options = ServerOptions::new()
            .with_max_frame_size(usize::MAX)
            .with_max_stream_body_size(None)
            .with_max_concurrent_streams_per_connection(Some(usize::MAX));

        let limits = transport_limits_from_server_options(&options, TransportMode::Native);

        assert_eq!(limits.stream_receive_window, u64::MAX);
        assert_eq!(limits.connection_receive_window, u64::MAX);
        assert_eq!(limits.max_concurrent_bidi_streams, Some(u64::MAX));
    }
}
