use std::future::{Future, pending};
use std::io;
use std::sync::Arc;

use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::RpcTransport;
use crate::framing::{DEFAULT_MAX_FRAME_SIZE, decode_frame, encode_frame_with_max, frame_body_len};
use crate::server::ServerOptions;
use crate::{
    BoxMessageStream, Error, MessageStream, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, RpcStreamFrameKind, Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;
const FRAME_READ_CHUNK_LEN: usize = 32 * 1024;
const FRAME_HEADER_LEN: u64 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportLimits {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub max_concurrent_bidi_streams: Option<u64>,
    pub max_concurrent_uni_streams: Option<u64>,
}

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
    #[must_use]
    pub const fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    #[must_use]
    pub const fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

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

        let response = read_frame(streams.recv_mut(), self.max_frame_size).await?;
        streams.complete();

        Ok(response)
    }

    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        let (send, recv) = self.connection.open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size;
        let write_task = tokio::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(Box::new(QuinnResponseStream::new(
            recv,
            write_task,
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
            let _ = send.reset(CANCELLED_STREAM_CODE.into());
        }

        if let Some(recv) = &mut self.recv {
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
        write_task: JoinHandle<Result<()>>,
        max_frame_size: usize,
    ) -> Self {
        Self {
            recv: Some(recv),
            write_task: Some(write_task),
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
        match read_frame_or_eof::<RpcStreamFrame>(self.recv_mut(), max_frame_size).await {
            Ok(Some(frame)) => {
                if frame.frame_kind() == Some(RpcStreamFrameKind::Status) {
                    self.complete = true;
                    if let Err(error) = self.stop_writer(true).await {
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
            let _ = recv.stop(CANCELLED_STREAM_CODE.into());
        }

        if let Some(write_task) = &self.write_task {
            write_task.abort();
        }
    }
}

pub async fn write_frame<M>(
    send: &mut quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    let frame = encode_frame_with_max(message, max_frame_size)?;
    send.write_all(&frame).await.map_err(Error::transport)
}

pub async fn read_frame<M>(recv: &mut quinn::RecvStream, max_frame_size: usize) -> Result<M>
where
    M: Message + Default,
{
    let mut header = [0; 4];
    recv.read_exact(&mut header)
        .await
        .map_err(Error::transport)?;

    let len = frame_body_len(header, max_frame_size)?;
    let body = read_body(recv, len).await?;

    decode_frame(&body)
}

async fn read_frame_or_eof<M>(
    recv: &mut quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<M>>
where
    M: Message + Default,
{
    let mut header = [0; 4];
    if !read_exact_or_eof(recv, &mut header).await? {
        return Ok(None);
    }

    let len = frame_body_len(header, max_frame_size)?;
    let body = read_body(recv, len).await?;

    decode_frame(&body).map(Some)
}

async fn read_body(recv: &mut quinn::RecvStream, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut body = Vec::with_capacity(len.min(FRAME_READ_CHUNK_LEN));
    let mut chunk = vec![0; len.min(FRAME_READ_CHUNK_LEN)];
    while body.len() < len {
        let remaining = len - body.len();
        let read_len = remaining.min(chunk.len());
        match recv
            .read(&mut chunk[..read_len])
            .await
            .map_err(Error::transport)?
        {
            Some(0) => {}
            Some(read) => body.extend_from_slice(&chunk[..read]),
            None => return Err(unexpected_eof()),
        }
    }

    Ok(body)
}

async fn read_exact_or_eof(recv: &mut quinn::RecvStream, buf: &mut [u8]) -> Result<bool> {
    let mut offset = 0;

    while offset < buf.len() {
        match recv
            .read(&mut buf[offset..])
            .await
            .map_err(Error::transport)?
        {
            Some(0) => {}
            Some(read) => offset += read,
            None if offset == 0 => return Ok(false),
            None => return Err(unexpected_eof()),
        }
    }

    Ok(true)
}

fn unexpected_eof() -> Error {
    Error::transport(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "stream ended in the middle of a frame",
    ))
}

async fn write_streaming_request(
    send: quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxMessageStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    while let Some(body) = request_body.next().await.transpose()? {
        write_frame(
            send.send_mut(),
            &RpcStreamFrame::message(body),
            max_frame_size,
        )
        .await?;
    }

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

impl crate::server::Server {
    pub async fn serve_quinn(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_quinn_with_shutdown(endpoint, pending::<()>())
            .await
    }

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

    #[cfg(feature = "webtransport")]
    pub async fn serve_quinn_and_webtransport(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_quinn_and_webtransport_with_shutdown(endpoint, pending::<()>())
            .await
    }

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
            write_status(send, error.into_status(), server.max_frame_size()).await;
            return;
        }
    };

    let Some(_request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        write_rpc_status(
            send,
            &request,
            Status::unavailable("too many concurrent RPCs"),
            server.max_frame_size(),
        )
        .await;
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
        let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);

        if write_frame(&mut send, &frame, max_frame_size)
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

        match read_frame_or_eof::<RpcStreamFrame>(&mut self.recv, self.max_frame_size).await {
            Ok(Some(frame)) => match frame.frame_kind() {
                Some(RpcStreamFrameKind::Message) => Some(Ok(frame.body)),
                Some(RpcStreamFrameKind::Status) => {
                    self.done = true;
                    let status = frame.status_value();
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
