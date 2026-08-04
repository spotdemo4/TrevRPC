use std::future::{Future, pending};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures_util::{FutureExt, StreamExt};
#[cfg(feature = "webtransport")]
use h3::ext::Protocol;
#[cfg(feature = "webtransport")]
use h3::quic::BidiStream as _;
use http::{Method, Request, Response, StatusCode, header};
use prost::Message;
#[cfg(feature = "webtransport")]
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;

use crate::framed::{self, FrameRead, FrameWrite, MESSAGE_FRAME_BATCH, NoopFrameTrace};
use crate::request_pump::{
    RequestInputKind, RequestPump, RequestPumpReader, RequestPumpSettle, RequestTransportEvent,
    start_request_pump,
};
use crate::server::{CancellationSource, CancellationToken};
use crate::{
    BoxStream, Error, Result, RpcKind, RpcRequest, RpcStreamFrame, RpcStreamFrameKind, Status,
};

const MEDIA_TYPE: &str = "application/trevrpc";
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
                                raw_connection.clone(),
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
                    shutdown.clone(),
                    raw_connection.clone(),
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

#[allow(clippy::too_many_arguments)]
fn spawn_http_request(
    server: &crate::server::Server,
    request: Request<()>,
    stream: H3RequestStream,
    request_limit: Option<&Arc<Semaphore>>,
    stream_limit: Option<&Arc<Semaphore>>,
    shutdown: watch::Receiver<bool>,
    connection: quinn::Connection,
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
        handle_http_request(server, request_limit, request, stream, connection, shutdown).await;
    });
}

async fn handle_http_request(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    request: Request<()>,
    mut stream: H3RequestStream,
    connection: quinn::Connection,
    shutdown: watch::Receiver<bool>,
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
        H3BodyReader::new(recv, connection.clone()),
        connection,
        shutdown,
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
    connection: quinn::Connection,
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
                        let stream_connection = connection.clone();
                        let stream_shutdown = shutdown.clone();
                        stream_tasks.spawn(async move {
                            let _stream_permit = stream_permit;
                            let (send, recv) = stream.split();
                            handle_rpc_stream(
                                server,
                                request_limit,
                                IoBodyWriter(send),
                                IoBodyReader::new(recv, stream_connection.clone()),
                                stream_connection,
                                stream_shutdown,
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
                                shutdown.clone(),
                                connection.clone(),
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
    fn reset(&mut self);
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

    fn reset(&mut self) {
        self.0.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);
    }
}

struct H3BodyReader {
    stream: H3RecvStream,
    chunk: Bytes,
    connection: quinn::Connection,
}

impl H3BodyReader {
    fn new(stream: H3RecvStream, connection: quinn::Connection) -> Self {
        Self {
            stream,
            chunk: Bytes::new(),
            connection,
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
}

impl RequestPumpReader for H3BodyReader {
    fn stop_trevrpc(&mut self) {
        // h3-quinn 0.0.10 moves its Quinn receive stream into a private
        // reusable read future and its public `stop_sending` unwraps an
        // internal slot that may remain empty even after `recv_data`
        // returns. Calling it after request decoding can therefore panic.
        // Dropping the request stream is bounded, but this dependency API
        // cannot guarantee TrevRPC cancellation code 1 on unified HTTP/3.
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::ConnectionLost(
            Error::transport(self.connection.closed().await).into_status(),
        ))
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        match self
            .stream
            .recv_trailers()
            .await
            .map_err(Error::transport)?
        {
            Some(_) => Err(Error::from(Status::invalid_argument(
                "HTTP/3 request trailers are not supported",
            ))),
            None => Ok(()),
        }
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

    fn reset(&mut self) {
        h3::quic::SendStream::reset(&mut self.0, CANCELLED_STREAM_CODE);
    }
}

#[cfg(feature = "webtransport")]
struct IoBodyReader {
    stream: WebTransportRecvStream,
    chunk: Bytes,
    connection: quinn::Connection,
}

#[cfg(feature = "webtransport")]
impl IoBodyReader {
    fn new(stream: WebTransportRecvStream, connection: quinn::Connection) -> Self {
        Self {
            stream,
            chunk: Bytes::new(),
            connection,
        }
    }
}

#[cfg(feature = "webtransport")]
impl FrameRead for IoBodyReader {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        while self.chunk.is_empty() {
            let chunk = std::future::poll_fn(|context| {
                h3::quic::RecvStream::poll_data(&mut self.stream, context)
            })
            .await
            .map_err(Error::transport)?;
            let Some(mut chunk) = chunk else {
                return Ok(None);
            };
            self.chunk = chunk.copy_to_bytes(chunk.remaining());
        }
        let len = bytes.len().min(self.chunk.len());
        self.chunk.copy_to_slice(&mut bytes[..len]);
        Ok(Some(len))
    }
}

#[cfg(feature = "webtransport")]
impl RequestPumpReader for IoBodyReader {
    fn stop_trevrpc(&mut self) {
        // Like ordinary h3 request streams, h3-WebTransport receive streams
        // delegate to h3-quinn's reusable read future. Its public
        // `stop_sending` can unwrap an empty stream slot after any pending or
        // completed `poll_data` call. Drop the bounded per-request stream
        // instead of risking a task panic until the dependency exposes a safe
        // cancellation operation.
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::ConnectionLost(
            Error::transport(self.connection.closed().await).into_status(),
        ))
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_rpc_stream<W, R>(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: W,
    mut recv: R,
    connection: quinn::Connection,
    mut shutdown: watch::Receiver<bool>,
) where
    W: RpcBodyWriter,
    R: RequestPumpReader,
{
    let request = match read_initial_request(&server, &mut recv).await {
        Ok(request) => request,
        Err(error) => {
            let status = error.into_status();
            if status.code() != crate::Code::DeadlineExceeded {
                recv.stop_trevrpc();
            }
            server.record_pre_handler_failure(&status);
            write_transport_status(send, status, server.max_frame_size()).await;
            return;
        }
    };
    let Some(_request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        recv.stop_trevrpc();
        let status = Status::unavailable("too many concurrent RPCs");
        server.record_rejected_request(&request, &status);
        write_rpc_status(send, &request, status, server.max_frame_size()).await;
        return;
    };

    let cancellation = CancellationToken::new();
    let request_for_failure = request.clone();
    let input_kind = RequestInputKind::for_rpc_kind(
        request.rpc_kind(),
        server.options().max_stream_messages(),
        server.options().max_stream_body_size(),
    );
    let (request_body, mut request_pump) =
        start_request_pump::<_, NoopFrameTrace>(recv, input_kind, server.max_frame_size());

    if request.rpc_kind() == RpcKind::Unary {
        let response = server.handle_request_with_cancellation(request, cancellation.clone());
        tokio::pin!(response);
        let response = tokio::select! {
            biased;
            response = &mut response => response,
            failure = request_pump.failure() => {
                server.record_active_request_failure(&request_for_failure, failure.status());
                if let Some(source) = failure.cancellation_source() {
                    cancellation.cancel(source);
                }
                if !failure.response_writable() {
                    send.reset();
                    let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                    return;
                }
                failure.status().clone().into_response(Vec::new())
            },
            _ = connection.closed() => {
                cancellation.cancel(CancellationSource::ConnectionLost);
                let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
                return;
            },
            changed = shutdown.changed() => {
                let _ = changed;
                cancellation.cancel(CancellationSource::ServerShutdown);
                let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
                return;
            }
        };
        match write_frame(&mut send, &response, server.max_frame_size()).await {
            Ok(()) => {
                let _ = request_pump
                    .settle(RequestPumpSettle::ResponseCommitted)
                    .await;
                let _ = send.finish().await;
            }
            Err(error) => {
                cancel_from_transport_error(&cancellation, &error);
                let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
            }
        }
        return;
    }

    let response = server.handle_streaming_request_with_cancellation(
        request,
        request_body,
        cancellation.clone(),
    );
    tokio::pin!(response);
    let mut response = tokio::select! {
        biased;
        response = &mut response => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&request_for_failure, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                return;
            }
            crate::stream::from_iter([RpcStreamFrame::status(failure.status().clone())])
        },
        _ = connection.closed() => {
            cancellation.cancel(CancellationSource::ConnectionLost);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
            return;
        },
        changed = shutdown.changed() => {
            let _ = changed;
            cancellation.cancel(CancellationSource::ServerShutdown);
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };
    let settle = write_streaming_response(
        &mut send,
        &mut response,
        &mut request_pump,
        &cancellation,
        &connection,
        &mut shutdown,
        server.max_frame_size(),
    )
    .await;
    let finish = matches!(settle, RequestPumpSettle::ResponseCommitted);
    let _ = request_pump.settle(settle).await;
    if finish {
        if let Err(error) = send.finish().await {
            cancel_from_transport_error(&cancellation, &error);
        }
    } else {
        send.reset();
    }
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

fn cancel_from_transport_error(cancellation: &CancellationToken, error: &Error) {
    if let Some(code) = error.transport_code() {
        cancellation.cancel(if code == crate::Code::Cancelled {
            CancellationSource::PeerReset
        } else {
            CancellationSource::ConnectionLost
        });
    }
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
    response: &mut BoxStream<RpcStreamFrame>,
    request_pump: &mut RequestPump,
    cancellation: &CancellationToken,
    connection: &quinn::Connection,
    shutdown: &mut watch::Receiver<bool>,
    max_frame_size: usize,
) -> RequestPumpSettle
where
    W: RpcBodyWriter,
{
    let mut message_batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    loop {
        let frame = tokio::select! {
            biased;
            failure = request_pump.failure() => {
                cancellation.set_completion_code(failure.status().code());
                if let Some(source) = failure.cancellation_source() {
                    cancellation.cancel(source);
                }
                if !failure.response_writable() {
                    send.reset();
                    return RequestPumpSettle::ResponseStopped;
                }
                Some(Ok(RpcStreamFrame::status(failure.status().clone())))
            },
            frame = response.next() => frame,
            _ = connection.closed() => {
                cancellation.cancel(CancellationSource::ConnectionLost);
                return RequestPumpSettle::ConnectionLost;
            },
            changed = shutdown.changed() => {
                let _ = changed;
                cancellation.cancel(CancellationSource::ServerShutdown);
                return RequestPumpSettle::ServerShutdown;
            }
        };
        let Some(frame) = frame else {
            return RequestPumpSettle::ResponseStopped;
        };
        let frame = frame.unwrap_or_else(|error| RpcStreamFrame::status(error.into_status()));
        if framed::is_plain_message_frame(&frame) {
            message_batch.push(frame.body);
            let mut next_frame = None;
            while message_batch.len() < MESSAGE_FRAME_BATCH {
                match response.next().now_or_never() {
                    Some(Some(Ok(frame))) if framed::is_plain_message_frame(&frame) => {
                        message_batch.push(frame.body);
                    }
                    Some(Some(Ok(frame))) => {
                        next_frame = Some(frame);
                        break;
                    }
                    Some(Some(Err(error))) => {
                        next_frame = Some(RpcStreamFrame::status(error.into_status()));
                        break;
                    }
                    Some(None) | None => break,
                }
            }
            if let Err(error) = framed::write_message_stream_frames::<_, NoopFrameTrace>(
                send,
                &mut message_batch,
                max_frame_size,
            )
            .await
            {
                cancel_from_transport_error(cancellation, &error);
                return RequestPumpSettle::ConnectionLost;
            }
            let Some(frame) = next_frame else {
                continue;
            };
            let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
            if let Err(error) =
                framed::write_stream_frame::<_, NoopFrameTrace>(send, frame, max_frame_size).await
            {
                cancel_from_transport_error(cancellation, &error);
                return RequestPumpSettle::ConnectionLost;
            }
            if is_status {
                return RequestPumpSettle::ResponseCommitted;
            }
            continue;
        }

        let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
        if let Err(error) =
            framed::write_stream_frame::<_, NoopFrameTrace>(send, frame, max_frame_size).await
        {
            cancel_from_transport_error(cancellation, &error);
            return RequestPumpSettle::ConnectionLost;
        }
        if is_status {
            return RequestPumpSettle::ResponseCommitted;
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
