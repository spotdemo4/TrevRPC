use std::future::{Future, pending};
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::RpcTransport;
use crate::framing::{
    DEFAULT_MAX_FRAME_SIZE, STREAM_FRAME_BODY_TAG, decode_frame, decode_stream_frame_body_owned,
    encode_frame_with_max, encode_message_stream_frame_prefix, frame_body_len,
};
use crate::{
    BoxMessageStream, Error, MessageStream, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, RpcStreamFrameKind, Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;
const MESSAGE_FRAME_BATCH: usize = 32;

type ServerEndpoint = web_transport_quinn::Server;

#[derive(Clone)]
pub struct Client {
    session: web_transport_quinn::Session,
    max_frame_size: usize,
}

impl Client {
    /// Creates a `TrevRPC` client over an established `WebTransport` session.
    #[must_use]
    pub const fn new(session: web_transport_quinn::Session) -> Self {
        Self {
            session,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Sets the maximum `TrevRPC` frame size in bytes for this client.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Returns the underlying `WebTransport` session.
    #[must_use]
    pub const fn session(&self) -> &web_transport_quinn::Session {
        &self.session
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
        let (send, recv) = self.session.open_bi().await.map_err(Error::transport)?;
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
        mut request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        let (send, recv) = self.session.open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size;

        if request_body.is_non_blocking() {
            match request_body.next().await {
                None => {
                    write_empty_streaming_request(send, request, max_frame_size).await?;
                    return Ok(Box::new(WebTransportResponseStream::new(
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

        Ok(Box::new(WebTransportResponseStream::new(
            recv,
            Some(write_task),
            self.max_frame_size,
        )))
    }
}

struct CancellableBiStream {
    send: Option<web_transport_quinn::SendStream>,
    recv: Option<web_transport_quinn::RecvStream>,
    complete: bool,
}

impl CancellableBiStream {
    fn new(send: web_transport_quinn::SendStream, recv: web_transport_quinn::RecvStream) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut web_transport_quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn recv_mut(&mut self) -> &mut web_transport_quinn::RecvStream {
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
            let _ = send.reset(cancelled_stream_code());
        }

        if let Some(recv) = &mut self.recv {
            let _ = recv.stop(cancelled_stream_code());
        }
    }
}

struct CancellableSendStream {
    send: Option<web_transport_quinn::SendStream>,
    complete: bool,
}

impl CancellableSendStream {
    fn new(send: web_transport_quinn::SendStream) -> Self {
        Self {
            send: Some(send),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut web_transport_quinn::SendStream {
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
            let _ = send.reset(cancelled_stream_code());
        }
    }
}

struct WebTransportResponseStream {
    recv: Option<web_transport_quinn::RecvStream>,
    write_task: Option<JoinHandle<Result<()>>>,
    max_frame_size: usize,
    complete: bool,
}

impl WebTransportResponseStream {
    const fn new(
        recv: web_transport_quinn::RecvStream,
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

    fn recv_mut(&mut self) -> &mut web_transport_quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }
}

#[crate::async_trait]
impl MessageStream<RpcStreamFrame> for WebTransportResponseStream {
    async fn next(&mut self) -> Option<Result<RpcStreamFrame>> {
        if self.complete {
            return None;
        }

        let max_frame_size = self.max_frame_size;
        match read_stream_frame_or_eof(self.recv_mut(), max_frame_size).await {
            Ok(Some(frame)) => {
                if frame.frame_kind() == Some(RpcStreamFrameKind::Status) {
                    self.complete = true;
                    if frame.status_value().is_ok() {
                        if let Err(error) = self.stop_writer(true).await {
                            return Some(Err(error));
                        }
                    } else {
                        self.ignore_writer_error();
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

impl WebTransportResponseStream {
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

impl Drop for WebTransportResponseStream {
    fn drop(&mut self) {
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            let _ = recv.stop(cancelled_stream_code());
        }

        if let Some(write_task) = &self.write_task {
            write_task.abort();
        }
    }
}

/// Writes a length-prefixed protobuf frame to a `WebTransport` send stream.
pub async fn write_frame<M>(
    send: &mut web_transport_quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    let frame = encode_frame_with_max(message, max_frame_size)?;
    send.write_all(&frame).await.map_err(Error::transport)
}

async fn write_message_stream_frame(
    send: &mut web_transport_quinn::SendStream,
    body: Vec<u8>,
    max_frame_size: usize,
) -> Result<()> {
    let mut prefix = Vec::new();
    encode_message_stream_frame_prefix(body.len(), max_frame_size, &mut prefix)?;
    if body.is_empty() {
        return send.write_all(&prefix).await.map_err(Error::transport);
    }

    send.write_all_chunks(&mut [Bytes::from(prefix), Bytes::from(body)])
        .await
        .map_err(Error::transport)
}

async fn write_message_stream_frames(
    send: &mut web_transport_quinn::SendStream,
    bodies: &mut Vec<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut chunks = Vec::with_capacity(bodies.len().saturating_mul(2));
    let mut prefix = Vec::new();
    for body in bodies.drain(..) {
        encode_message_stream_frame_prefix(body.len(), max_frame_size, &mut prefix)?;
        chunks.push(Bytes::copy_from_slice(&prefix));
        if !body.is_empty() {
            chunks.push(Bytes::from(body));
        }
    }

    send.write_all_chunks(&mut chunks)
        .await
        .map_err(Error::transport)
}

async fn write_stream_frame(
    send: &mut web_transport_quinn::SendStream,
    frame: RpcStreamFrame,
    max_frame_size: usize,
) -> Result<()> {
    if is_plain_message_frame(&frame) {
        write_message_stream_frame(send, frame.body, max_frame_size).await
    } else {
        write_frame(send, &frame, max_frame_size).await
    }
}

fn is_plain_message_frame(frame: &RpcStreamFrame) -> bool {
    frame.frame_kind() == Some(RpcStreamFrameKind::Message)
        && frame.status == crate::Code::Ok.as_u32()
        && frame.message.is_empty()
        && frame.metadata.is_empty()
}

/// Reads and decodes one length-prefixed protobuf frame from a `WebTransport` receive stream.
pub async fn read_frame<M>(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<M>
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

async fn read_stream_frame_or_eof(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>> {
    let mut header = [0; 4];
    if !read_exact_or_eof(recv, &mut header).await? {
        return Ok(None);
    }

    let len = frame_body_len(header, max_frame_size)?;
    if len == 0 {
        return Ok(Some(RpcStreamFrame::message(Vec::new())));
    }

    let mut prefix = [0_u8; 11];
    read_exact_body(recv, &mut prefix[..1]).await?;
    if prefix[0] != STREAM_FRAME_BODY_TAG {
        let body = read_body_with_prefix(recv, len, &prefix[..1]).await?;
        return decode_stream_frame_body_owned(body).map(Some);
    }

    let mut value = 0_u64;
    let mut prefix_len = 1;
    for shift in (0..64).step_by(7) {
        if prefix_len == len {
            let body = prefix[..prefix_len].to_vec();
            return decode_stream_frame_body_owned(body).map(Some);
        }

        read_exact_body(recv, &mut prefix[prefix_len..=prefix_len]).await?;
        let byte = prefix[prefix_len];
        prefix_len += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            let body_len = usize::try_from(value).map_err(|_| {
                Error::from(Status::invalid_argument(
                    "stream frame field length exceeded supported range",
                ))
            })?;
            if prefix_len.checked_add(body_len) == Some(len) {
                return read_body(recv, body_len)
                    .await
                    .map(RpcStreamFrame::message)
                    .map(Some);
            }

            let body = read_body_with_prefix(recv, len, &prefix[..prefix_len]).await?;
            return decode_stream_frame_body_owned(body).map(Some);
        }
    }

    let body = read_body_with_prefix(recv, len, &prefix[..prefix_len]).await?;
    decode_stream_frame_body_owned(body).map(Some)
}

async fn read_body(recv: &mut web_transport_quinn::RecvStream, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut body = vec![0; len];
    read_exact_body(recv, &mut body).await?;

    Ok(body)
}

async fn read_body_with_prefix(
    recv: &mut web_transport_quinn::RecvStream,
    len: usize,
    prefix: &[u8],
) -> Result<Vec<u8>> {
    let remaining = len.checked_sub(prefix.len()).ok_or_else(|| {
        Error::from(Status::invalid_argument(
            "stream frame prefix exceeded frame length",
        ))
    })?;
    let mut body = Vec::with_capacity(len);
    body.extend_from_slice(prefix);
    body.resize(len, 0);
    read_exact_body(recv, &mut body[prefix.len()..prefix.len() + remaining]).await?;

    Ok(body)
}

async fn read_exact_body(recv: &mut web_transport_quinn::RecvStream, buf: &mut [u8]) -> Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        match recv
            .read(&mut buf[offset..])
            .await
            .map_err(Error::transport)?
        {
            Some(0) => {}
            Some(read) => offset += read,
            None => return Err(unexpected_eof()),
        }
    }

    Ok(())
}

async fn read_exact_or_eof(
    recv: &mut web_transport_quinn::RecvStream,
    buf: &mut [u8],
) -> Result<bool> {
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
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxMessageStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    write_request_body_frames(send.send_mut(), &mut request_body, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

async fn write_empty_streaming_request(
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

async fn write_request_body_frames(
    send: &mut web_transport_quinn::SendStream,
    request_body: &mut BoxMessageStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    if !request_body.is_non_blocking() {
        while let Some(body) = request_body.next().await.transpose()? {
            write_message_stream_frame(send, body, max_frame_size).await?;
        }
        return Ok(());
    }

    let mut batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    loop {
        batch.clear();
        let mut done = false;
        while batch.len() < MESSAGE_FRAME_BATCH {
            if request_body.drain_ready(MESSAGE_FRAME_BATCH, &mut batch)? {
                done = true;
                break;
            }
            if batch.len() >= MESSAGE_FRAME_BATCH {
                break;
            }
            let Some(body) = request_body.next().await.transpose()? else {
                done = true;
                break;
            };
            batch.push(body);
        }
        if !batch.is_empty() {
            write_message_stream_frames(send, &mut batch, max_frame_size).await?;
        }
        if done {
            return Ok(());
        }
    }
}

impl crate::server::Server {
    /// Serves `TrevRPC` over a `WebTransport` endpoint until the endpoint stops accepting requests.
    pub async fn serve_webtransport(self, endpoint: ServerEndpoint) -> Result<()> {
        self.serve_webtransport_with_shutdown(endpoint, pending::<()>())
            .await
    }

    /// Serves `TrevRPC` over a `WebTransport` endpoint until the shutdown future completes.
    pub async fn serve_webtransport_with_shutdown<S>(
        self,
        mut endpoint: ServerEndpoint,
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
                request = endpoint.accept() => {
                    let Some(request) = request else {
                        break;
                    };

                    if let Some(status) = validate_request(&self, &request) {
                        let _ = request.reject(status).await;
                        continue;
                    }

                    let Some(connection_permit) = try_acquire_permit(connection_limit.as_ref()) else {
                        let _ = request.reject(web_transport_quinn::http::StatusCode::SERVICE_UNAVAILABLE).await;
                        continue;
                    };

                    let server = self.clone();
                    let request_limit = request_limit.clone();
                    let shutdown = shutdown_rx.clone();
                    connection_tasks.spawn(async move {
                        let _connection_permit = connection_permit;
                        if let Ok(session) = request.ok().await
                        {
                            handle_session(server, session, request_limit, shutdown).await;
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
                        tracing::warn!(%error, "WebTransport connection task failed");
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

pub(crate) fn validate_request(
    server: &crate::server::Server,
    request: &web_transport_quinn::Request,
) -> Option<web_transport_quinn::http::StatusCode> {
    let options = server.options();
    if let Some(admission) = options.webtransport_admission() {
        let origin = request
            .headers
            .get(web_transport_quinn::http::header::ORIGIN)
            .and_then(|origin| origin.to_str().ok());
        let admission_request = crate::server::WebTransportAdmissionRequest {
            request,
            path: request.url.path(),
            authority: request.url.host_str(),
            origin,
            secure: request.url.scheme() == "https",
        };
        return (!admission(&admission_request))
            .then_some(web_transport_quinn::http::StatusCode::FORBIDDEN);
    }

    if request.url.path() != options.webtransport_path() {
        return Some(web_transport_quinn::http::StatusCode::NOT_FOUND);
    }

    let allowed_authorities = options.webtransport_allowed_authorities();
    if !allowed_authorities.is_empty() && !authority_allowed(request, allowed_authorities) {
        return Some(web_transport_quinn::http::StatusCode::FORBIDDEN);
    }

    if let Some(origin) = request
        .headers
        .get(web_transport_quinn::http::header::ORIGIN)
    {
        let Ok(origin) = origin.to_str() else {
            return Some(web_transport_quinn::http::StatusCode::FORBIDDEN);
        };

        if !options.webtransport_allowed_origins().contains(&origin) {
            return Some(web_transport_quinn::http::StatusCode::FORBIDDEN);
        }
    }

    None
}

fn authority_allowed(request: &web_transport_quinn::Request, allowed_authorities: &[&str]) -> bool {
    let Some(host) = request.url.host_str() else {
        return false;
    };

    if allowed_authorities.contains(&host) {
        return true;
    }

    let Some(port) = request.url.port() else {
        return false;
    };
    let authority = format!("{host}:{port}");

    allowed_authorities.contains(&authority.as_str())
}

pub(crate) async fn handle_session(
    server: crate::server::Server,
    session: web_transport_quinn::Session,
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
            accepted = session.accept_bi(), if !*shutdown.borrow() => {
                let Ok((send, recv)) = accepted else {
                    break;
                };

                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    write_status(
                        send,
                        Status::unavailable("too many concurrent streams on WebTransport session"),
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
                    tracing::warn!(%error, "WebTransport stream task failed");
                }
            }
        }
    }

    drain_streams(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
        &session,
    )
    .await;

    if *shutdown.borrow() {
        session.close(
            cancelled_stream_code(),
            b"server drained WebTransport connection",
        );
    }
}

async fn handle_stream(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: web_transport_quinn::SendStream,
    mut recv: web_transport_quinn::RecvStream,
) {
    let request = match read_initial_request(&server, &mut recv).await {
        Ok(request) => request,
        Err(error) => {
            let _ = recv.stop(cancelled_stream_code());
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
            tracing::debug!(?stopped, "client stopped WebTransport response stream before RPC completed");
            return;
        }
    };

    if write_frame(&mut send, &response, server.max_frame_size())
        .await
        .is_ok()
    {
        if let Err(error) = read_unary_request_end(&server, &mut recv).await {
            let _ = &error;
            let _ = recv.stop(cancelled_stream_code());
            #[cfg(feature = "tracing")]
            tracing::debug!(%error, "failed to drain WebTransport unary request stream");
        }
        let _ = send.finish();
    }
}

async fn read_initial_request(
    server: &crate::server::Server,
    recv: &mut web_transport_quinn::RecvStream,
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

async fn read_unary_request_end(
    server: &crate::server::Server,
    recv: &mut web_transport_quinn::RecvStream,
) -> Result<()> {
    let read = drain_unary_request_end(recv);
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read).await.map_err(|_| {
            Error::from(Status::deadline_exceeded(
                "unary request stream finish timeout",
            ))
        })?
    } else {
        read.await
    }
}

async fn drain_unary_request_end(recv: &mut web_transport_quinn::RecvStream) -> Result<()> {
    let mut buf = [0; 1024];
    loop {
        match recv.read(&mut buf).await.map_err(Error::transport)? {
            Some(0) => {}
            Some(_) => {
                return Err(Error::from(Status::invalid_argument(
                    "unary request stream contained data after the initial request frame",
                )));
            }
            None => return Ok(()),
        }
    }
}

async fn handle_streaming_rpc(
    server: crate::server::Server,
    mut send: web_transport_quinn::SendStream,
    recv: web_transport_quinn::RecvStream,
    request: RpcRequest,
) {
    let max_frame_size = server.max_frame_size();
    let request_body = Box::new(WebTransportRequestStream::new(recv, max_frame_size));
    let mut response = tokio::select! {
        biased;
        response = server.handle_streaming_request(request, request_body) => response,
        stopped = send.stopped() => {
            let _ = &stopped;
            #[cfg(feature = "tracing")]
            tracing::debug!(?stopped, "client stopped WebTransport response stream before streaming RPC handler completed");
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
                tracing::debug!(?stopped, "client stopped WebTransport response stream before streaming RPC completed");
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
}

async fn write_rpc_status(
    mut send: web_transport_quinn::SendStream,
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

struct WebTransportRequestStream {
    recv: web_transport_quinn::RecvStream,
    max_frame_size: usize,
    done: bool,
}

impl WebTransportRequestStream {
    const fn new(recv: web_transport_quinn::RecvStream, max_frame_size: usize) -> Self {
        Self {
            recv,
            max_frame_size,
            done: false,
        }
    }
}

impl Drop for WebTransportRequestStream {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.recv.stop(cancelled_stream_code());
        }
    }
}

#[crate::async_trait]
impl MessageStream<Vec<u8>> for WebTransportRequestStream {
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

async fn write_status(
    mut send: web_transport_quinn::SendStream,
    status: Status,
    max_frame_size: usize,
) {
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
    session: &web_transport_quinn::Session,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_stream_tasks(stream_tasks))
            .await
            .is_err()
        {
            session.close(
                cancelled_stream_code(),
                b"server WebTransport stream drain timed out",
            );
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
            tracing::warn!(%error, "WebTransport stream task failed while draining");
        }
    }
}

async fn drain_connections(
    connection_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    endpoint: &ServerEndpoint,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_connection_tasks(connection_tasks))
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
        drain_connection_tasks(connection_tasks).await;
        endpoint.close(0_u32.into(), b"server shutdown complete");
    }
}

async fn drain_connection_tasks(connection_tasks: &mut JoinSet<()>) {
    while let Some(result) = connection_tasks.join_next().await {
        if let Err(error) = result {
            let _ = &error;
            #[cfg(feature = "tracing")]
            tracing::warn!(%error, "WebTransport connection task failed while draining");
        }
    }
}

fn cancelled_stream_code() -> u32 {
    CANCELLED_STREAM_CODE
}
