use std::fmt;
use std::future::{Future, pending};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::RpcTransport;
use crate::framed::{self, FrameRead, FrameTrace, FrameWrite, MESSAGE_FRAME_BATCH};
use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::server::ServerOptions;
use crate::{
    BoxMessageStream, Error, MessageStream, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, RpcStreamFrameKind, Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;
const FRAME_HEADER_LEN: u64 = 4;

fn trace_quinn_event(event: &'static str, detail: &'static str) {
    trace_quinn_frame_line(format_args!("event={event} detail={detail}"));
    #[cfg(feature = "tracing")]
    tracing::trace!(target: "trevrpc::quinn::frames", event, detail);
}

fn trace_tx_frame<M: Message>(encoded_len: usize) {
    trace_quinn_frame_line(format_args!(
        "direction=tx frame={} encoded_len={encoded_len}",
        std::any::type_name::<M>()
    ));
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: "trevrpc::quinn::frames",
        direction = "tx",
        frame = std::any::type_name::<M>(),
        encoded_len,
    );
}

fn trace_rx_frame<M: Message>(encoded_len: usize) {
    trace_quinn_frame_line(format_args!(
        "direction=rx frame={} encoded_len={encoded_len}",
        std::any::type_name::<M>()
    ));
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: "trevrpc::quinn::frames",
        direction = "rx",
        frame = std::any::type_name::<M>(),
        encoded_len,
    );
}

fn trace_tx_stream_message_frame(body_len: usize, batch_len: usize) {
    trace_quinn_frame_line(format_args!(
        "direction=tx frame=RpcStreamFrame kind=message body_len={body_len} batch_len={batch_len}"
    ));
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: "trevrpc::quinn::frames",
        direction = "tx",
        frame = "RpcStreamFrame",
        kind = "message",
        body_len,
        batch_len,
    );
}

fn trace_tx_stream_frame(frame: &RpcStreamFrame, encoded_len: usize) {
    let frame_kind = frame.frame_kind();
    let status = frame.status;
    let body_len = frame.body.len();
    trace_quinn_frame_line(format_args!(
        "direction=tx frame=RpcStreamFrame kind={frame_kind:?} status={status} body_len={body_len} encoded_len={encoded_len}"
    ));
    #[cfg(feature = "tracing")]
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

fn trace_rx_stream_frame(frame: &RpcStreamFrame, encoded_len: usize) {
    let frame_kind = frame.frame_kind();
    let status = frame.status;
    let body_len = frame.body.len();
    trace_quinn_frame_line(format_args!(
        "direction=rx frame=RpcStreamFrame kind={frame_kind:?} status={status} body_len={body_len} encoded_len={encoded_len}"
    ));
    #[cfg(feature = "tracing")]
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

fn trace_quinn_frame_line(args: fmt::Arguments<'_>) {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    let enabled = *ENABLED.get_or_init(|| {
        std::env::var("TREVRPC_RUST_QUINN_FRAME_TRACE")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
    });
    if enabled {
        eprintln!("trevrpc-quinn-frame {args}");
    }
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

    async fn read_exact_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.read_exact(bytes).await.map_err(Error::transport)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportLimits {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub max_concurrent_bidi_streams: Option<u64>,
    pub max_concurrent_uni_streams: Option<u64>,
}

/// Builds QUIC transport limits from server options.
#[must_use]
pub fn transport_limits_from_server_options(
    options: &ServerOptions,
    allow_incoming_uni_streams: bool,
) -> TransportLimits {
    let stream_receive_window = frame_receive_window(options.max_frame_size());

    TransportLimits {
        stream_receive_window,
        connection_receive_window: connection_receive_window(options, stream_receive_window),
        max_concurrent_bidi_streams: options
            .max_concurrent_streams_per_connection()
            // Keep one extra stream available so over-limit RPCs can receive a TrevRPC status.
            .map(|max_streams| saturating_usize_to_u64(max_streams).saturating_add(1)),
        max_concurrent_uni_streams: if allow_incoming_uni_streams {
            None
        } else {
            Some(0)
        },
    }
}

/// Builds QUIC transport limits for a client connection.
#[must_use]
pub fn client_transport_limits(
    max_frame_size: usize,
    allow_incoming_uni_streams: bool,
) -> TransportLimits {
    let stream_receive_window = frame_receive_window(max_frame_size);

    TransportLimits {
        stream_receive_window,
        connection_receive_window: stream_receive_window,
        max_concurrent_bidi_streams: Some(0),
        max_concurrent_uni_streams: if allow_incoming_uni_streams {
            None
        } else {
            Some(0)
        },
    }
}

/// Applies `TrevRPC` transport limits to a `Quinn` server config.
pub fn configure_server_config(
    config: &mut quinn::ServerConfig,
    options: &ServerOptions,
    allow_incoming_uni_streams: bool,
) {
    let limits = transport_limits_from_server_options(options, allow_incoming_uni_streams);
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
    allow_incoming_uni_streams: bool,
) {
    let limits = client_transport_limits(max_frame_size, allow_incoming_uni_streams);
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

#[derive(Clone)]
pub struct Client {
    connection: quinn::Connection,
    max_frame_size: usize,
}

impl Client {
    /// Creates a `TrevRPC` client over an established `Quinn` connection.
    #[must_use]
    pub const fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Sets the maximum `TrevRPC` frame size in bytes for this client.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Returns the underlying `Quinn` connection.
    #[must_use]
    pub const fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    /// Returns the maximum `TrevRPC` frame size in bytes for this client.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }
}

#[crate::async_trait]
impl RpcTransport for Client {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.connection.open_bi().await.map_err(Error::transport)?;
        let mut streams = CancellableBiStream::new(send, recv);

        write_frame(streams.send_mut(), &request, self.max_frame_size).await?;
        streams.send_mut().finish().map_err(Error::transport)?;
        trace_quinn_event("tx_fin", "client_unary_request");

        let response = read_frame(streams.recv_mut(), self.max_frame_size).await?;
        streams.complete();

        Ok(response)
    }

    async fn streaming_call(
        &self,
        request: RpcRequest,
        mut request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        let (send, recv) = self.connection.open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size;

        if request_body.is_non_blocking() {
            match request_body.next().await {
                None => {
                    write_empty_streaming_request(send, request, max_frame_size).await?;
                    return Ok(Box::new(QuinnResponseStream::new(
                        recv,
                        None,
                        self.max_frame_size,
                    )));
                }
                Some(first) => {
                    request_body = crate::stream::prefixed(first, request_body);
                }
            }
        }

        let write_task = tokio::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(Box::new(QuinnResponseStream::new(
            recv,
            Some(write_task),
            self.max_frame_size,
        )))
    }
}

struct CancellableBiStream {
    send: Option<quinn::SendStream>,
    recv: Option<quinn::RecvStream>,
    complete: bool,
}

impl CancellableBiStream {
    fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
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

    fn complete(mut self) {
        self.complete = true;
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

struct QuinnResponseStream {
    recv: Option<quinn::RecvStream>,
    write_task: Option<JoinHandle<Result<()>>>,
    max_frame_size: usize,
    complete: bool,
}

impl QuinnResponseStream {
    const fn new(
        recv: quinn::RecvStream,
        write_task: Option<JoinHandle<Result<()>>>,
        max_frame_size: usize,
    ) -> Self {
        Self {
            recv: Some(recv),
            write_task,
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

#[crate::async_trait]
impl MessageStream<RpcStreamFrame> for QuinnResponseStream {
    async fn next(&mut self) -> Option<Result<RpcStreamFrame>> {
        if self.complete {
            return None;
        }

        let max_frame_size = self.max_frame_size;
        match read_stream_frame_or_eof(self.recv_mut(), max_frame_size).await {
            Ok(Some(frame)) => {
                if frame.frame_kind() == Some(RpcStreamFrameKind::Status) {
                    self.complete = true;
                    let status = frame.status_value();
                    if status.is_ok() {
                        if let Err(error) = self.stop_writer(true).await {
                            return Some(Err(error));
                        }
                    } else {
                        self.ignore_writer_error();
                    }
                    if let Err(error) = drain_fin_after_terminal_status(
                        self.recv_mut(),
                        max_frame_size,
                        "response stream",
                    )
                    .await
                    {
                        return Some(Err(error));
                    }
                }
                Some(Ok(frame))
            }
            Ok(None) => {
                self.complete = true;
                if let Err(error) = self.stop_writer(false).await {
                    return Some(Err(error));
                }
                None
            }
            Err(error) => {
                self.complete = true;
                let _ = self.stop_writer(true).await;
                Some(Err(error))
            }
        }
    }
}

impl QuinnResponseStream {
    async fn stop_writer(&mut self, ignore_cancelled: bool) -> Result<()> {
        let Some(write_task) = self.write_task.take() else {
            return Ok(());
        };

        if !write_task.is_finished() {
            write_task.abort();
        }

        match write_task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if ignore_cancelled && is_cancelled_error(&error) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(error) if ignore_cancelled && error.is_cancelled() => Ok(()),
            Err(error) => Err(Error::transport(error)),
        }
    }

    fn ignore_writer_error(&mut self) {
        if let Some(write_task) = self.write_task.take()
            && !write_task.is_finished()
        {
            write_task.abort();
        }
    }
}

fn is_cancelled_error(error: &Error) -> bool {
    match error {
        Error::Status(status) => status.code() == crate::Code::Cancelled,
        Error::Transport(_) | Error::Encode(_) | Error::Decode(_) | Error::FrameTooLarge { .. } => {
            false
        }
    }
}

impl Drop for QuinnResponseStream {
    fn drop(&mut self) {
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            trace_quinn_event("tx_stop_sending", "response_stream_drop");
            let _ = recv.stop(CANCELLED_STREAM_CODE.into());
        }

        if let Some(write_task) = &self.write_task {
            write_task.abort();
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

async fn write_message_stream_frames(
    send: &mut quinn::SendStream,
    bodies: &mut Vec<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_message_stream_frames::<_, QuinnFrameTrace>(send, bodies, max_frame_size).await
}

async fn write_stream_frame(
    send: &mut quinn::SendStream,
    frame: RpcStreamFrame,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_stream_frame::<_, QuinnFrameTrace>(send, frame, max_frame_size).await
}

fn is_plain_message_frame(frame: &RpcStreamFrame) -> bool {
    framed::is_plain_message_frame(frame)
}

/// Reads and decodes one length-prefixed protobuf frame from a `Quinn` receive stream.
pub async fn read_frame<M>(recv: &mut quinn::RecvStream, max_frame_size: usize) -> Result<M>
where
    M: Message + Default,
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
    mut request_body: BoxMessageStream<Vec<u8>>,
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

async fn write_empty_streaming_request(
    send: quinn::SendStream,
    request: RpcRequest,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    trace_quinn_event("tx_fin", "empty_streaming_request");
    send.complete();

    Ok(())
}

async fn write_request_body_frames(
    send: &mut quinn::SendStream,
    request_body: &mut BoxMessageStream<Vec<u8>>,
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
                                handle_connection(server, connection, request_limit, connection_permit, shutdown).await;
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

    /// Serves `TrevRPC` over `Quinn` and `WebTransport` on the same endpoint.
    #[cfg(feature = "webtransport")]
    pub async fn serve_quinn_and_webtransport(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_quinn_and_webtransport_with_shutdown(endpoint, pending::<()>())
            .await
    }

    /// Serves `TrevRPC` over `Quinn` and `WebTransport` until the shutdown future completes.
    #[cfg(feature = "webtransport")]
    pub async fn serve_quinn_and_webtransport_with_shutdown<S>(
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
                            match negotiated_protocol(&connection).as_deref() {
                                Some(crate::ALPN) => {
                                    handle_connection(server, connection, request_limit, connection_permit, shutdown).await;
                                }
                                Some(protocol) if protocol == web_transport_quinn::ALPN.as_bytes() => {
                                    let _connection_permit = connection_permit;
                                    handle_webtransport_connection(server, connection, request_limit, shutdown).await;
                                }
                                _ => {
                                    connection.close(0_u32.into(), b"unsupported ALPN");
                                }
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
}

#[cfg(feature = "webtransport")]
async fn handle_webtransport_connection(
    server: crate::server::Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    shutdown: watch::Receiver<bool>,
) {
    let request = match web_transport_quinn::Request::accept(connection).await {
        Ok(request) => request,
        Err(error) => {
            let _ = &error;
            #[cfg(feature = "tracing")]
            tracing::debug!(%error, "WebTransport request failed");
            return;
        }
    };

    if let Some(status) = crate::webtransport::validate_request(&server, &request) {
        let _ = request.reject(status).await;
        return;
    }

    let session = match request.ok().await {
        Ok(session) => session,
        Err(error) => {
            let _ = &error;
            #[cfg(feature = "tracing")]
            tracing::debug!(%error, "WebTransport response failed");
            return;
        }
    };

    crate::webtransport::handle_session(server, session, request_limit, shutdown).await;
}

fn negotiated_protocol(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol)
}

async fn handle_connection(
    server: crate::server::Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    _connection_permit: Permit,
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
                    write_status(
                        send,
                        Status::unavailable("too many concurrent streams on connection"),
                        server.max_frame_size(),
                    )
                    .await;
                    continue;
                };

                let server = server.clone();
                let request_limit = request_limit.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_stream(server, request_limit, send, recv).await;
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

async fn handle_stream(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) {
    let request = match read_initial_request(&server, &mut recv).await {
        Ok(request) => request,
        Err(error) => {
            let _ = recv.stop(CANCELLED_STREAM_CODE.into());
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            write_status(send, status, server.max_frame_size()).await;
            return;
        }
    };

    let Some(_request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        let status = Status::unavailable("too many concurrent RPCs");
        server.record_rejected_request(&request, &status);
        write_rpc_status(send, &request, status, server.max_frame_size()).await;
        return;
    };

    if request.rpc_kind() != RpcKind::Unary {
        handle_streaming_rpc(server, send, recv, request).await;
        return;
    }

    let response = tokio::select! {
        biased;
        response = server.handle_request(request) => response,
        stopped = send.stopped() => {
            let _ = &stopped;
            #[cfg(feature = "tracing")]
            tracing::debug!("client stopped response stream before RPC completed");
            return;
        }
    };

    if write_frame(&mut send, &response, server.max_frame_size())
        .await
        .is_ok()
    {
        let _ = send.finish();
        trace_quinn_event("tx_fin", "server_unary_response");
    }
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

async fn handle_streaming_rpc(
    server: crate::server::Server,
    mut send: quinn::SendStream,
    recv: quinn::RecvStream,
    request: RpcRequest,
) {
    let max_frame_size = server.max_frame_size();
    let request_body = Box::new(QuinnRequestStream::new(recv, max_frame_size));
    let mut response = tokio::select! {
        biased;
        response = server.handle_streaming_request(request, request_body) => response,
        stopped = send.stopped() => {
            let _ = &stopped;
            #[cfg(feature = "tracing")]
            tracing::debug!("client stopped response stream before streaming RPC handler completed");
            return;
        }
    };

    let response_is_non_blocking = response.is_non_blocking();
    let mut message_batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    loop {
        let frame = tokio::select! {
            biased;
            frame = response.next() => frame,
            stopped = send.stopped() => {
                let _ = &stopped;
                #[cfg(feature = "tracing")]
                tracing::debug!("client stopped response stream before streaming RPC completed");
                return;
            }
        };

        let Some(frame) = frame else {
            break;
        };

        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => RpcStreamFrame::status(error.into_status()),
        };

        if response_is_non_blocking && is_plain_message_frame(&frame) {
            message_batch.push(frame.body);
            let mut next_frame = None;
            while message_batch.len() < MESSAGE_FRAME_BATCH {
                match response.next().await {
                    Some(Ok(frame)) if is_plain_message_frame(&frame) => {
                        message_batch.push(frame.body);
                    }
                    Some(Ok(frame)) => {
                        next_frame = Some(frame);
                        break;
                    }
                    Some(Err(error)) => {
                        next_frame = Some(RpcStreamFrame::status(error.into_status()));
                        break;
                    }
                    None => break,
                }
            }

            if write_message_stream_frames(&mut send, &mut message_batch, max_frame_size)
                .await
                .is_err()
            {
                return;
            }

            let Some(frame) = next_frame else {
                continue;
            };
            let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
            if write_stream_frame(&mut send, frame, max_frame_size)
                .await
                .is_err()
            {
                return;
            }
            if is_status {
                break;
            }
            continue;
        }

        let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);

        if write_stream_frame(&mut send, frame, max_frame_size)
            .await
            .is_err()
        {
            return;
        }

        if is_status {
            break;
        }
    }

    let _ = send.finish();
    trace_quinn_event("tx_fin", "server_streaming_response");
}

async fn write_rpc_status(
    mut send: quinn::SendStream,
    request: &RpcRequest,
    status: Status,
    max_frame_size: usize,
) {
    let result = if request.rpc_kind() == RpcKind::Unary {
        write_frame(&mut send, &status.into_response(Vec::new()), max_frame_size).await
    } else {
        write_frame(&mut send, &RpcStreamFrame::status(status), max_frame_size).await
    };

    if result.is_ok() {
        let _ = send.finish();
        trace_quinn_event("tx_fin", "server_rpc_status");
    }
}

struct QuinnRequestStream {
    recv: quinn::RecvStream,
    max_frame_size: usize,
    done: bool,
}

impl QuinnRequestStream {
    const fn new(recv: quinn::RecvStream, max_frame_size: usize) -> Self {
        Self {
            recv,
            max_frame_size,
            done: false,
        }
    }
}

impl Drop for QuinnRequestStream {
    fn drop(&mut self) {
        if !self.done {
            trace_quinn_event("tx_stop_sending", "request_stream_drop");
            let _ = self.recv.stop(CANCELLED_STREAM_CODE.into());
        }
    }
}

#[crate::async_trait]
impl MessageStream<Vec<u8>> for QuinnRequestStream {
    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        if self.done {
            return None;
        }

        match read_stream_frame_or_eof(&mut self.recv, self.max_frame_size).await {
            Ok(Some(frame)) => match frame.frame_kind() {
                Some(RpcStreamFrameKind::Message) => Some(Ok(frame.body)),
                Some(RpcStreamFrameKind::Status) => {
                    self.done = true;
                    let status = frame.status_value();
                    if let Err(error) = drain_fin_after_terminal_status(
                        &mut self.recv,
                        self.max_frame_size,
                        "request stream",
                    )
                    .await
                    {
                        return Some(Err(error));
                    }
                    if status.is_ok() {
                        None
                    } else {
                        Some(Err(Error::from(status)))
                    }
                }
                None => {
                    self.done = true;
                    Some(Err(Error::from(Status::invalid_argument(
                        "request stream contained an unknown frame kind",
                    ))))
                }
            },
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

#[allow(dead_code)]
struct Permit(Option<OwnedSemaphorePermit>);

fn try_acquire_permit(limit: Option<&Arc<Semaphore>>) -> Option<Permit> {
    limit.map_or(Some(Permit(None)), |semaphore| {
        semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| Permit(Some(permit)))
    })
}

async fn write_status(mut send: quinn::SendStream, status: Status, max_frame_size: usize) {
    let response = status.into_response(Vec::new());

    if write_frame(&mut send, &response, max_frame_size)
        .await
        .is_ok()
    {
        let _ = send.finish();
        trace_quinn_event("tx_fin", "server_status");
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

    use super::{TransportLimits, client_transport_limits, transport_limits_from_server_options};

    #[test]
    fn server_transport_limits_align_with_trevrpc_limits() {
        let options = ServerOptions::new()
            .with_max_frame_size(1024)
            .with_max_stream_body_size(Some(4096))
            .with_max_concurrent_streams_per_connection(Some(10));

        assert_eq!(
            transport_limits_from_server_options(&options, false),
            TransportLimits {
                stream_receive_window: 1028,
                connection_receive_window: 4096,
                max_concurrent_bidi_streams: Some(11),
                max_concurrent_uni_streams: Some(0),
            }
        );
        assert_eq!(
            transport_limits_from_server_options(&options, true).max_concurrent_uni_streams,
            None
        );
    }

    #[test]
    fn client_transport_limits_reject_unused_peer_initiated_streams() {
        assert_eq!(
            client_transport_limits(2048, false),
            TransportLimits {
                stream_receive_window: 2052,
                connection_receive_window: 2052,
                max_concurrent_bidi_streams: Some(0),
                max_concurrent_uni_streams: Some(0),
            }
        );
        assert_eq!(
            client_transport_limits(2048, true).max_concurrent_uni_streams,
            None
        );
    }

    #[test]
    fn server_transport_limit_calculation_saturates() {
        let options = ServerOptions::new()
            .with_max_frame_size(usize::MAX)
            .with_max_stream_body_size(None)
            .with_max_concurrent_streams_per_connection(Some(usize::MAX));

        let limits = transport_limits_from_server_options(&options, false);

        assert_eq!(limits.stream_receive_window, u64::MAX);
        assert_eq!(limits.connection_receive_window, u64::MAX);
        assert_eq!(limits.max_concurrent_bidi_streams, Some(u64::MAX));
    }
}
