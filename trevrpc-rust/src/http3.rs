use std::future::{Future, pending};
use std::io;
use std::sync::Arc;

use bytes::{Buf, Bytes};
#[cfg(feature = "webtransport")]
use h3::ext::Protocol;
#[cfg(feature = "webtransport")]
use h3::quic::BidiStream as _;
use http::{Method, Request, Response, StatusCode, header};
use prost::Message;
#[cfg(feature = "webtransport")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::framed::{self, FrameRead, FrameWrite, MESSAGE_FRAME_BATCH, NoopFrameTrace};
use crate::{
    BoxMessageStream, Error, MessageStream, Result, RpcKind, RpcRequest, RpcStreamFrame,
    RpcStreamFrameKind, Status,
};

const MEDIA_TYPE: &str = "application/trevrpc";
const REQUEST_PUMP_CAPACITY: usize = 1;
#[cfg(feature = "webtransport")]
const CANCELLED_STREAM_CODE: u64 = web_transport_quinn::proto::error_to_http3(1);

type H3RequestStream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;
type H3SendStream = h3::server::RequestStream<h3_quinn::SendStream<Bytes>, Bytes>;
type H3RecvStream = h3::server::RequestStream<h3_quinn::RecvStream, Bytes>;

#[cfg(feature = "webtransport")]
type WebTransportSession =
    h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>;
#[cfg(feature = "webtransport")]
type WebTransportSendStream =
    h3_webtransport::stream::SendStream<h3_quinn::SendStream<Bytes>, Bytes>;
#[cfg(feature = "webtransport")]
type WebTransportRecvStream = h3_webtransport::stream::RecvStream<h3_quinn::RecvStream, Bytes>;
/// Serves HTTP/3 POST RPCs on an h3-only Quinn endpoint.
impl crate::server::Server {
    pub async fn serve_http3(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_http3_with_shutdown(endpoint, pending::<()>())
            .await
    }

    /// Serves HTTP/3 POST RPCs until the shutdown future completes.
    pub async fn serve_http3_with_shutdown<S>(
        self,
        endpoint: quinn::Endpoint,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        serve_endpoint(self, endpoint, shutdown, false).await
    }

    #[cfg(feature = "webtransport")]
    pub(crate) async fn serve_quinn_and_http3_with_shutdown<S>(
        self,
        endpoint: quinn::Endpoint,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        serve_endpoint(self, endpoint, shutdown, true).await
    }
}

async fn serve_endpoint<S>(
    server: crate::server::Server,
    endpoint: quinn::Endpoint,
    shutdown: S,
    allow_native: bool,
) -> Result<()>
where
    S: Future<Output = ()> + Send,
{
    let connection_limit = server
        .options()
        .max_concurrent_connections()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let request_limit = server
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

                let server = server.clone();
                let request_limit = request_limit.clone();
                let shutdown = shutdown_rx.clone();
                connection_tasks.spawn(async move {
                    let Ok(connection) = incoming.await else {
                        return;
                    };
                    let _connection_permit = connection_permit;
                    match negotiated_protocol(&connection).as_deref() {
                        Some(crate::HTTP3_ALPN) => {
                            Box::pin(handle_h3_connection(
                                server,
                                connection,
                                request_limit,
                                shutdown,
                            ))
                            .await;
                        }
                        Some(crate::ALPN) if allow_native => {
                            crate::quinn::handle_connection(server, connection, request_limit, shutdown).await;
                        }
                        _ => connection.close(0_u32.into(), b"unsupported ALPN"),
                    }
                });
            }
            () = &mut shutdown => {
                let _ = shutdown_tx.send(true);
                break;
            }
            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                log_task_error(result, "HTTP/3 connection task failed");
            }
        }
    }

    let _ = shutdown_tx.send(true);
    drain_connections(
        &mut connection_tasks,
        server.options().graceful_shutdown_timeout(),
        &endpoint,
    )
    .await;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn handle_h3_connection(
    server: crate::server::Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let raw_connection = connection.clone();
    #[cfg(feature = "webtransport")]
    let mut builder = h3::server::builder();
    #[cfg(not(feature = "webtransport"))]
    let builder = h3::server::builder();
    #[cfg(feature = "webtransport")]
    builder
        .enable_extended_connect(true)
        .enable_datagram(true)
        .enable_webtransport(true)
        .max_webtransport_sessions(1);
    let h3_connection = builder.build(h3_quinn::Connection::new(connection)).await;
    let Ok(mut h3_connection) = h3_connection else {
        raw_connection.close(0_u32.into(), b"failed to initialize HTTP/3");
        return;
    };
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let mut stream_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accepted = h3_connection.accept(), if !*shutdown.borrow() => {
                let Ok(Some(request_resolver)) = accepted else {
                    break;
                };
                let resolve = async {
                    if let Some(timeout) = server.options().initial_request_timeout() {
                        tokio::time::timeout(timeout, request_resolver.resolve_request())
                            .await
                            .ok()
                            .and_then(std::result::Result::ok)
                    } else {
                        request_resolver.resolve_request().await.ok()
                    }
                };
                tokio::pin!(resolve);
                let request_result = tokio::select! {
                    result = &mut resolve => result,
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                };
                let Some((request, stream)) = request_result else {
                    continue;
                };

                #[cfg(feature = "webtransport")]
                if is_webtransport_request(&request) {
                    if let Some(status) = validate_webtransport_request(&server, &request) {
                        reject_h3(stream, status).await;
                        continue;
                    }
                    match WebTransportSession::accept(request, stream, h3_connection).await {
                        Ok(session) => {
                            handle_webtransport_session(
                                server.clone(),
                                session,
                                request_limit,
                                stream_limit,
                                shutdown.clone(),
                            )
                            .await;
                        }
                        Err(error) => {
                            let _ = error;
                        }
                    }
                    break;
                }

                spawn_http_request(
                    &server,
                    request,
                    stream,
                    request_limit.as_ref(),
                    stream_limit.as_ref(),
                    &mut stream_tasks,
                );
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = h3_connection.shutdown(0).await;
                    break;
                }
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                log_task_error(result, "HTTP/3 request task failed");
            }
        }
    }

    drain_streams(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
        &raw_connection,
    )
    .await;
    if *shutdown.borrow() {
        raw_connection.close(0_u32.into(), b"server drained HTTP/3 connection");
    }
}

fn spawn_http_request(
    server: &crate::server::Server,
    request: Request<()>,
    stream: H3RequestStream,
    request_limit: Option<&Arc<Semaphore>>,
    stream_limit: Option<&Arc<Semaphore>>,
    stream_tasks: &mut JoinSet<()>,
) {
    let Some(stream_permit) = try_acquire_permit(stream_limit) else {
        stream_tasks.spawn(reject_h3(stream, StatusCode::SERVICE_UNAVAILABLE));
        return;
    };
    let server = server.clone();
    let request_limit = request_limit.cloned();
    stream_tasks.spawn(async move {
        let _stream_permit = stream_permit;
        handle_http_request(server, request_limit, request, stream).await;
    });
}

async fn handle_http_request(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    request: Request<()>,
    mut stream: H3RequestStream,
) {
    if let Some(status) = validate_http_request(&server, &request) {
        reject_h3(stream, status).await;
        return;
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, MEDIA_TYPE)
        .body(())
        .expect("static HTTP/3 response should be valid");
    if stream.send_response(response).await.is_err() {
        return;
    }
    let (send, recv) = stream.split();
    handle_rpc_stream(
        server,
        request_limit,
        H3BodyWriter(send),
        H3BodyReader::new(recv),
    )
    .await;
}

fn validate_http_request(
    server: &crate::server::Server,
    request: &Request<()>,
) -> Option<StatusCode> {
    let options = server.options();
    if !options.http3_enabled() || request.uri().path() != options.http3_path() {
        return Some(StatusCode::NOT_FOUND);
    }
    if request.method() != Method::POST {
        return Some(StatusCode::METHOD_NOT_ALLOWED);
    }
    if !valid_content_type(request.headers()) {
        return Some(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    if let Some(admission) = options.http3_admission() {
        let headers = admission_headers(request.headers());
        let admission_request = crate::server::Http3AdmissionRequest {
            request,
            path: request.uri().path(),
            authority: request.uri().authority().map(http::uri::Authority::as_str),
            secure: request.uri().scheme_str() == Some("https"),
            headers: &headers,
        };
        if !admission(&admission_request) {
            return Some(StatusCode::FORBIDDEN);
        }
    }
    None
}

fn valid_content_type(headers: &http::HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    values.next().is_none() && value.as_bytes().eq_ignore_ascii_case(MEDIA_TYPE.as_bytes())
}

fn admission_headers(headers: &http::HeaderMap) -> Vec<crate::server::AdmissionHeader<'_>> {
    headers
        .iter()
        .map(|(name, value)| crate::server::AdmissionHeader {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect()
}

async fn reject_h3(mut stream: H3RequestStream, status: StatusCode) {
    let mut response = Response::builder().status(status);
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response = response.header(header::ALLOW, Method::POST.as_str());
    }
    let response = response
        .body(())
        .expect("static HTTP/3 rejection should be valid");
    if stream.send_response(response).await.is_ok() {
        let _ = stream.finish().await;
    }
}

#[cfg(feature = "webtransport")]
fn is_webtransport_request(request: &Request<()>) -> bool {
    request.method() == Method::CONNECT
        && request.extensions().get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT)
}

#[cfg(feature = "webtransport")]
fn validate_webtransport_request(
    server: &crate::server::Server,
    request: &Request<()>,
) -> Option<StatusCode> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    crate::webtransport::validate_admission(
        server,
        request.uri().path(),
        request.uri().authority().map(http::uri::Authority::as_str),
        origin,
        request.uri().scheme_str() == Some("https"),
        &admission_headers(request.headers()),
    )
}

#[cfg(feature = "webtransport")]
async fn handle_webtransport_session(
    server: crate::server::Server,
    session: WebTransportSession,
    request_limit: Option<Arc<Semaphore>>,
    stream_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let session_id = session.session_id();
    let mut stream_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accepted = session.accept_bi(), if !*shutdown.borrow() => {
                match accepted {
                    Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(id, stream)))
                        if id == session_id =>
                    {
                        let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                            let (send, _recv) = stream.split();
                            stream_tasks.spawn(write_transport_status(
                                IoBodyWriter(send),
                                Status::unavailable("too many concurrent streams on WebTransport session"),
                                server.max_frame_size(),
                            ));
                            continue;
                        };
                        let server = server.clone();
                        let request_limit = request_limit.clone();
                        stream_tasks.spawn(async move {
                            let _stream_permit = stream_permit;
                            let (send, recv) = stream.split();
                            handle_rpc_stream(
                                server,
                                request_limit,
                                IoBodyWriter(send),
                                IoBodyReader(recv),
                            )
                            .await;
                        });
                    }
                    Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(_, mut stream))) => {
                        h3::quic::SendStream::reset(&mut stream, CANCELLED_STREAM_CODE);
                    }
                    Ok(Some(h3_webtransport::server::AcceptedBi::Request(request, stream))) => {
                        if is_webtransport_request(&request) {
                            stream_tasks.spawn(reject_h3(stream, StatusCode::CONFLICT));
                        } else {
                            spawn_http_request(
                                &server,
                                request,
                                stream,
                                request_limit.as_ref(),
                                stream_limit.as_ref(),
                                &mut stream_tasks,
                            );
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                log_task_error(result, "unified WebTransport stream task failed");
            }
        }
    }

    if let Some(timeout) = server.options().graceful_shutdown_timeout() {
        if tokio::time::timeout(timeout, drain_stream_tasks(&mut stream_tasks))
            .await
            .is_err()
        {
            stream_tasks.abort_all();
            while stream_tasks.join_next().await.is_some() {}
        }
    } else {
        drain_stream_tasks(&mut stream_tasks).await;
    }
}

trait RpcBodyWriter: FrameWrite + Send {
    async fn finish(&mut self) -> Result<()>;
}

struct H3BodyWriter(H3SendStream);

impl FrameWrite for H3BodyWriter {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.0
            .send_data(Bytes::copy_from_slice(bytes))
            .await
            .map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        for chunk in chunks {
            self.0
                .send_data(chunk.clone())
                .await
                .map_err(Error::transport)?;
        }
        Ok(())
    }
}

impl RpcBodyWriter for H3BodyWriter {
    async fn finish(&mut self) -> Result<()> {
        self.0.finish().await.map_err(Error::transport)
    }
}

struct H3BodyReader {
    stream: H3RecvStream,
    chunk: Bytes,
}

impl H3BodyReader {
    const fn new(stream: H3RecvStream) -> Self {
        Self {
            stream,
            chunk: Bytes::new(),
        }
    }
}

impl FrameRead for H3BodyReader {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        while self.chunk.is_empty() {
            let Some(mut chunk) = self.stream.recv_data().await.map_err(Error::transport)? else {
                return Ok(None);
            };
            self.chunk = chunk.copy_to_bytes(chunk.remaining());
        }
        let len = bytes.len().min(self.chunk.len());
        self.chunk.copy_to_slice(&mut bytes[..len]);
        Ok(Some(len))
    }

    async fn read_exact_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<()> {
        read_exact_frame_bytes(self, bytes).await
    }
}

impl RpcBodyReader for H3BodyReader {
    fn cancel(&mut self) {
        self.stream
            .stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
    }
}

#[cfg(feature = "webtransport")]
struct IoBodyWriter(WebTransportSendStream);

#[cfg(feature = "webtransport")]
impl FrameWrite for IoBodyWriter {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.0.write_all(bytes).await.map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        for chunk in chunks {
            self.0.write_all(chunk).await.map_err(Error::transport)?;
        }
        Ok(())
    }
}

#[cfg(feature = "webtransport")]
impl RpcBodyWriter for IoBodyWriter {
    async fn finish(&mut self) -> Result<()> {
        self.0.shutdown().await.map_err(Error::transport)
    }
}

#[cfg(feature = "webtransport")]
struct IoBodyReader(WebTransportRecvStream);

#[cfg(feature = "webtransport")]
impl FrameRead for IoBodyReader {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        let read = self.0.read(bytes).await.map_err(Error::transport)?;
        Ok((read != 0).then_some(read))
    }

    async fn read_exact_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.0
            .read_exact(bytes)
            .await
            .map(|_| ())
            .map_err(Error::transport)
    }
}

#[cfg(feature = "webtransport")]
impl RpcBodyReader for IoBodyReader {
    fn cancel(&mut self) {
        h3::quic::RecvStream::stop_sending(&mut self.0, CANCELLED_STREAM_CODE);
    }
}

trait RpcBodyReader: FrameRead + Send {
    fn cancel(&mut self);
}

async fn read_exact_frame_bytes<R>(reader: &mut R, bytes: &mut [u8]) -> Result<()>
where
    R: FrameRead,
{
    let mut offset = 0;
    while offset < bytes.len() {
        match reader.read_frame_bytes(&mut bytes[offset..]).await? {
            Some(0) => {}
            Some(read) => offset += read,
            None => {
                return Err(Error::transport(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "HTTP/3 body ended in the middle of a frame",
                )));
            }
        }
    }
    Ok(())
}

async fn handle_rpc_stream<W, R>(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: W,
    mut recv: R,
) where
    W: RpcBodyWriter,
    R: RpcBodyReader + 'static,
{
    let request = match read_initial_request(&server, &mut recv).await {
        Ok(request) => request,
        Err(error) => {
            let status = error.into_status();
            if status.code() != crate::Code::DeadlineExceeded {
                recv.cancel();
            }
            server.record_pre_handler_failure(&status);
            write_transport_status(send, status, server.max_frame_size()).await;
            return;
        }
    };
    let Some(_request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        recv.cancel();
        let status = Status::unavailable("too many concurrent RPCs");
        server.record_rejected_request(&request, &status);
        write_rpc_status(send, &request, status, server.max_frame_size()).await;
        return;
    };

    let (request_body, mut request_pump) = start_request_pump(recv, server.max_frame_size());

    if request.rpc_kind() == RpcKind::Unary {
        let response = server.handle_request(request);
        tokio::pin!(response);
        let response = tokio::select! {
            response = &mut response => response,
            () = request_pump.cancelled() => return,
        };
        if write_frame(&mut send, &response, server.max_frame_size())
            .await
            .is_ok()
        {
            let _ = send.finish().await;
        }
        return;
    }

    let response = server.handle_streaming_request(request, request_body);
    tokio::pin!(response);
    let mut response = tokio::select! {
        response = &mut response => response,
        () = request_pump.cancelled() => return,
    };
    write_streaming_response(
        &mut send,
        &mut response,
        &mut request_pump,
        server.max_frame_size(),
    )
    .await;
}

async fn read_initial_request<R>(server: &crate::server::Server, recv: &mut R) -> Result<RpcRequest>
where
    R: FrameRead,
{
    let read = framed::read_frame::<_, NoopFrameTrace, RpcRequest>(recv, server.max_frame_size());
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

async fn write_frame<W, M>(send: &mut W, message: &M, max_frame_size: usize) -> Result<()>
where
    W: FrameWrite,
    M: Message,
{
    framed::write_frame::<_, NoopFrameTrace, _>(send, message, max_frame_size).await
}

async fn write_rpc_status<W>(
    mut send: W,
    request: &RpcRequest,
    status: Status,
    max_frame_size: usize,
) where
    W: RpcBodyWriter,
{
    let result = if request.rpc_kind() == RpcKind::Unary {
        write_frame(&mut send, &status.into_response(Vec::new()), max_frame_size).await
    } else {
        write_frame(&mut send, &RpcStreamFrame::status(status), max_frame_size).await
    };
    if result.is_ok() {
        let _ = send.finish().await;
    }
}

async fn write_transport_status<W>(mut send: W, status: Status, max_frame_size: usize)
where
    W: RpcBodyWriter,
{
    if write_frame(&mut send, &status.into_response(Vec::new()), max_frame_size)
        .await
        .is_ok()
    {
        let _ = send.finish().await;
    }
}

async fn write_streaming_response<W>(
    send: &mut W,
    response: &mut BoxMessageStream<RpcStreamFrame>,
    request_pump: &mut RequestPump,
    max_frame_size: usize,
) where
    W: RpcBodyWriter,
{
    let response_is_non_blocking = response.is_non_blocking();
    let mut message_batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    loop {
        let frame = tokio::select! {
            frame = response.next() => frame,
            () = request_pump.cancelled() => return,
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = frame.unwrap_or_else(|error| RpcStreamFrame::status(error.into_status()));
        if response_is_non_blocking && framed::is_plain_message_frame(&frame) {
            message_batch.push(frame.body);
            let mut next_frame = None;
            while message_batch.len() < MESSAGE_FRAME_BATCH {
                match response.next().await {
                    Some(Ok(frame)) if framed::is_plain_message_frame(&frame) => {
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
            if framed::write_message_stream_frames::<_, NoopFrameTrace>(
                send,
                &mut message_batch,
                max_frame_size,
            )
            .await
            .is_err()
            {
                return;
            }
            let Some(frame) = next_frame else {
                continue;
            };
            let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
            if framed::write_stream_frame::<_, NoopFrameTrace>(send, frame, max_frame_size)
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
        if framed::write_stream_frame::<_, NoopFrameTrace>(send, frame, max_frame_size)
            .await
            .is_err()
        {
            return;
        }
        if is_status {
            break;
        }
    }
    let _ = send.finish().await;
}

struct PumpedRequestStream {
    recv: mpsc::Receiver<Result<Vec<u8>>>,
}

#[crate::async_trait]
impl MessageStream<Vec<u8>> for PumpedRequestStream {
    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        self.recv.recv().await
    }
}

struct RequestPump {
    cancelled: watch::Receiver<bool>,
    task: JoinHandle<()>,
}

impl RequestPump {
    async fn cancelled(&mut self) {
        loop {
            if *self.cancelled.borrow() {
                return;
            }
            if self.cancelled.changed().await.is_err() {
                pending::<()>().await;
            }
        }
    }
}

impl Drop for RequestPump {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn start_request_pump<R>(
    mut recv: R,
    max_frame_size: usize,
) -> (BoxMessageStream<Vec<u8>>, RequestPump)
where
    R: RpcBodyReader + 'static,
{
    // h3 exposes request RESET_STREAM through reads, but no response STOP_SENDING future.
    // Keep the receive half driven with one-frame read-ahead so resets cancel pending handlers.
    let (body_tx, body_rx) = mpsc::channel(REQUEST_PUMP_CAPACITY);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            let frame =
                framed::read_stream_frame_or_eof::<_, NoopFrameTrace>(&mut recv, max_frame_size)
                    .await;
            let item = match frame {
                Ok(Some(frame)) => match frame.frame_kind() {
                    Some(RpcStreamFrameKind::Message) => Ok(frame.body),
                    Some(RpcStreamFrameKind::Status) => {
                        let status = frame.status_value();
                        if status.is_ok() {
                            break;
                        }
                        Err(Error::from(status))
                    }
                    None => Err(Error::from(Status::invalid_argument(
                        "request stream contained an unknown frame kind",
                    ))),
                },
                Ok(None) => break,
                Err(error) => {
                    let _ = cancel_tx.send(true);
                    let _ = body_tx.try_send(Err(error));
                    break;
                }
            };
            let terminal = item.is_err();
            if body_tx.send(item).await.is_err() || terminal {
                break;
            }
        }
    });
    (
        Box::new(PumpedRequestStream { recv: body_rx }),
        RequestPump {
            cancelled: cancel_rx,
            task,
        },
    )
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

fn negotiated_protocol(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol)
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
            connection.close(0_u32.into(), b"HTTP/3 stream drain timed out");
            stream_tasks.abort_all();
            while stream_tasks.join_next().await.is_some() {}
        }
    } else {
        drain_stream_tasks(stream_tasks).await;
    }
}

async fn drain_stream_tasks(stream_tasks: &mut JoinSet<()>) {
    while let Some(result) = stream_tasks.join_next().await {
        log_task_error(Some(result), "HTTP/3 stream task failed while draining");
    }
}

async fn drain_connections(
    connection_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    endpoint: &quinn::Endpoint,
) {
    let drain = async {
        while let Some(result) = connection_tasks.join_next().await {
            log_task_error(Some(result), "HTTP/3 connection task failed while draining");
        }
    };
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain).await.is_err() {
            endpoint.close(0_u32.into(), b"server graceful shutdown timed out");
            connection_tasks.abort_all();
            while connection_tasks.join_next().await.is_some() {}
        } else {
            endpoint.close(0_u32.into(), b"server shutdown complete");
        }
    } else {
        drain.await;
        endpoint.close(0_u32.into(), b"server shutdown complete");
    }
}

fn log_task_error(
    result: Option<std::result::Result<(), tokio::task::JoinError>>,
    message: &'static str,
) {
    if let Some(Err(error)) = result {
        let _ = (&error, message);
        #[cfg(feature = "tracing")]
        tracing::warn!(%error, "{message}");
    }
}
