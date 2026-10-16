//! Server side of ordinary HTTP/3 (`POST /trevrpc`) and `WebTransport` (`/trevrpc`) over QUIC
//! (ALPN `h3`).

use std::future::Future;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use h3::ext::Protocol;
use h3::quic::BidiStream as _;
use http::{Method, Request, Response, StatusCode, header};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::framed::{self, FrameRead, FrameWrite};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::serve::{drain_streams, drain_tasks, log_task_error};
use crate::server::Server;
use crate::server_transport::{self, ResponseWriter, try_acquire_permit};
use crate::{Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

const MEDIA_TYPE: &str = "application/trevrpc";
// RFC WebTransport reserves this error-code range for application protocol errors. Code 1 maps
// to the first application code plus one; the division term is zero for this value.
const CANCELLED_STREAM_CODE: u64 = 0x52e4_a40f_a8dc;

type H3RequestStream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;
type H3SendStream = h3::server::RequestStream<h3_quinn::SendStream<Bytes>, Bytes>;
type H3RecvStream = h3::server::RequestStream<h3_quinn::RecvStream, Bytes>;
type WebTransportSession =
    h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>;
type WebTransportSendStream =
    h3_webtransport::stream::SendStream<h3_quinn::SendStream<Bytes>, Bytes>;
type WebTransportRecvStream = h3_webtransport::stream::RecvStream<h3_quinn::RecvStream, Bytes>;

/// Serves one HTTP/3 connection: ordinary requests, or a single `WebTransport` session.
#[allow(clippy::too_many_lines)]
pub(crate) async fn handle_connection(
    server: Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let raw_connection = connection.clone();
    let mut builder = h3::server::builder();
    if server.options().webtransport_enabled() {
        builder
            .enable_extended_connect(true)
            .enable_datagram(true)
            .enable_webtransport(true)
            .max_webtransport_sessions(1);
    }
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
        if *shutdown.borrow() {
            break;
        }
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
                if is_webtransport_request(&request) {
                    if let Some(status) = validate_webtransport_request(&server, &request) {
                        reject_h3(stream, status).await;
                        continue;
                    }
                    let accept = WebTransportSession::accept(request, stream, h3_connection);
                    tokio::pin!(accept);
                    let session = tokio::select! {
                        result = &mut accept => result.ok(),
                        changed = shutdown.changed() => {
                            let _ = changed;
                            None
                        }
                    };
                    if let Some(session) = session {
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
    server: &Server,
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
    server: Server,
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
        H3Sink(send),
        H3Source(recv),
        connection,
        shutdown,
    )
    .await;
}

fn validate_http_request(server: &Server, request: &Request<()>) -> Option<StatusCode> {
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

fn is_webtransport_request(request: &Request<()>) -> bool {
    request.method() == Method::CONNECT
        && request.extensions().get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT)
}

fn validate_webtransport_request(server: &Server, request: &Request<()>) -> Option<StatusCode> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    validate_webtransport_admission(
        server,
        request.uri().path(),
        request.uri().authority().map(http::uri::Authority::as_str),
        origin,
        request.uri().scheme_str() == Some("https"),
        &admission_headers(request.headers()),
    )
}

fn validate_webtransport_admission(
    server: &Server,
    path: &str,
    authority: Option<&str>,
    origin: Option<&str>,
    secure: bool,
    headers: &[crate::server::AdmissionHeader<'_>],
) -> Option<StatusCode> {
    let options = server.options();
    if !options.webtransport_enabled() || path != options.http3_path() {
        return Some(StatusCode::NOT_FOUND);
    }
    if let Some(admission) = options.webtransport_admission() {
        let request = crate::server::WebTransportAdmissionRequest {
            path,
            authority,
            origin,
            secure,
            headers,
        };
        return (!admission(&request)).then_some(StatusCode::FORBIDDEN);
    }
    let allowed_authorities = options.webtransport_allowed_authorities();
    if !allowed_authorities.is_empty()
        && !authority.is_some_and(|authority| {
            allowed_authorities
                .iter()
                .any(|allowed| allowed == authority || allowed == authority_host(authority))
        })
    {
        return Some(StatusCode::FORBIDDEN);
    }
    if origin.is_some_and(|origin| {
        !options
            .webtransport_allowed_origins()
            .iter()
            .any(|allowed| allowed == origin)
    }) {
        return Some(StatusCode::FORBIDDEN);
    }
    None
}

fn authority_host(authority: &str) -> &str {
    if let Some(authority) = authority.strip_prefix('[')
        && let Some((host, _)) = authority.split_once(']')
    {
        return host;
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

async fn handle_webtransport_session(
    server: Server,
    session: WebTransportSession,
    request_limit: Option<Arc<Semaphore>>,
    stream_limit: Option<Arc<Semaphore>>,
    connection: quinn::Connection,
    mut shutdown: watch::Receiver<bool>,
) {
    let session_id = session.session_id();
    let mut stream_tasks = JoinSet::new();

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            accepted = session.accept_bi(), if !*shutdown.borrow() => {
                match accepted {
                    Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(id, stream)))
                        if id == session_id =>
                    {
                        let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                            let server = server.clone();
                            let stream_connection = connection.clone();
                            let (send, recv) = stream.split();
                            stream_tasks.spawn(reject_rpc_stream(
                                server,
                                WebTransportSink(send),
                                WebTransportSource(recv),
                                stream_connection,
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
                                WebTransportSink(send),
                                WebTransportSource(recv),
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
                    Ok(None) => break,
                    Err(error) => {
                        let message = error.to_string();
                        connection.close(0_u32.into(), message.as_bytes());
                        break;
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                log_task_error(result, "WebTransport stream task failed");
            }
        }
    }

    drain_tasks(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
    )
    .await;
}

/// The send half of one HTTP/3 or `WebTransport` stream.
trait BodySink: Send + 'static {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<()>> + Send;

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn reset(&mut self);
}

/// The receive half of one HTTP/3 or `WebTransport` stream.
trait BodySource: Send + 'static {
    fn next_chunk(&mut self) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    /// Validates anything that follows the request body, such as HTTP/3 trailers.
    fn validate_end(&mut self) -> impl Future<Output = Result<()>> + Send;
}

struct H3Sink(H3SendStream);

impl BodySink for H3Sink {
    async fn write(&mut self, chunk: &[u8]) -> Result<()> {
        self.0
            .send_data(Bytes::copy_from_slice(chunk))
            .await
            .map_err(Error::transport)
    }

    async fn finish(&mut self) -> Result<()> {
        self.0.finish().await.map_err(Error::transport)
    }

    fn reset(&mut self) {
        self.0.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);
    }
}

struct H3Source(H3RecvStream);

impl BodySource for H3Source {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let chunk = self.0.recv_data().await.map_err(Error::transport)?;
        Ok(chunk.map(|mut chunk| chunk.copy_to_bytes(chunk.remaining())))
    }

    async fn validate_end(&mut self) -> Result<()> {
        match self.0.recv_trailers().await.map_err(Error::transport)? {
            Some(_) => Err(Error::from(Status::invalid_argument(
                "HTTP/3 request trailers are not supported",
            ))),
            None => Ok(()),
        }
    }
}

struct WebTransportSink(WebTransportSendStream);

impl BodySink for WebTransportSink {
    async fn write(&mut self, mut chunk: &[u8]) -> Result<()> {
        while !chunk.is_empty() {
            let written = std::future::poll_fn(|context| {
                h3::quic::SendStreamUnframed::poll_send(&mut self.0, context, &mut chunk)
            })
            .await
            .map_err(Error::transport)?;
            if written == 0 {
                return Err(Error::from(Status::internal(
                    "WebTransport stream write made no progress",
                )));
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        std::future::poll_fn(|context| h3::quic::SendStream::poll_finish(&mut self.0, context))
            .await
            .map_err(Error::transport)
    }

    fn reset(&mut self) {
        h3::quic::SendStream::reset(&mut self.0, CANCELLED_STREAM_CODE);
    }
}

struct WebTransportSource(WebTransportRecvStream);

impl BodySource for WebTransportSource {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let chunk =
            std::future::poll_fn(|context| h3::quic::RecvStream::poll_data(&mut self.0, context))
                .await
                .map_err(Error::transport)?;
        Ok(chunk.map(|mut chunk| chunk.copy_to_bytes(chunk.remaining())))
    }

    async fn validate_end(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Adapts an HTTP/3 or `WebTransport` stream's send half to the server's response writer.
///
/// The underlying libraries cannot report a peer `STOP_SENDING` independently of body polling, so
/// the writer observes only loss of the whole connection.
struct BodyWriter<S> {
    sink: S,
    connection: quinn::Connection,
    max_frame_size: usize,
}

impl<S: BodySink> FrameWrite for BodyWriter<S> {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.sink.write(bytes).await
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        for chunk in chunks {
            self.sink.write(chunk).await?;
        }
        Ok(())
    }
}

impl<S: BodySink> ResponseWriter for BodyWriter<S> {
    async fn write_response(&mut self, response: &RpcResponse) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        framed::write_frame(self, response, max_frame_size).await
    }

    async fn write_message_stream_frames(&mut self, bodies: &mut Vec<Vec<u8>>) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        framed::write_message_stream_frames(self, bodies, max_frame_size).await
    }

    async fn write_stream_frame(&mut self, frame: RpcStreamFrame) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        framed::write_stream_frame(self, frame, max_frame_size).await
    }

    async fn stopped(&mut self) -> Result<()> {
        Err(Error::transport(self.connection.closed().await))
    }

    fn reset(&mut self) {
        self.sink.reset();
    }

    async fn finish(&mut self) -> Result<()> {
        self.sink.finish().await
    }
}

/// Adapts an HTTP/3 or `WebTransport` stream's receive half to the server's request reader.
struct BodyReader<S> {
    source: S,
    chunk: Bytes,
    connection: quinn::Connection,
}

impl<S: BodySource> FrameRead for BodyReader<S> {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        while self.chunk.is_empty() {
            let Some(chunk) = self.source.next_chunk().await? else {
                return Ok(None);
            };
            self.chunk = chunk;
        }
        let len = bytes.len().min(self.chunk.len());
        self.chunk.copy_to_slice(&mut bytes[..len]);
        Ok(Some(len))
    }
}

impl<S: BodySource> RequestPumpReader for BodyReader<S> {
    fn stop_trevrpc(&mut self) {
        // h3-quinn 0.0.10 moves its Quinn receive stream into a private reusable read future, and
        // its public `stop_sending` unwraps an internal slot that may remain empty even after a
        // read returns, so calling it can panic. h3-WebTransport receive streams delegate to the
        // same code. Dropping the bounded per-request stream is safe, but this dependency API
        // cannot guarantee TrevRPC cancellation code 1 over HTTP/3 and WebTransport.
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        framed::drain_unary_request_end(self).await
    }

    async fn read_stream_frame_or_eof(
        &mut self,
        max_frame_size: usize,
    ) -> Result<Option<RpcStreamFrame>> {
        framed::read_stream_frame_or_eof(self, max_frame_size).await
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::ConnectionLost(
            Error::transport(self.connection.closed().await).into_status(),
        ))
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        self.source.validate_end().await
    }
}

/// Reads the initial request frame, replying with its failure status when it is unusable.
async fn read_request<K, S>(
    server: &Server,
    connection: quinn::Connection,
    sink: K,
    source: S,
) -> Option<(BodyWriter<K>, BodyReader<S>, RpcRequest)>
where
    K: BodySink,
    S: BodySource,
{
    let writer = BodyWriter {
        sink,
        connection: connection.clone(),
        max_frame_size: server.max_frame_size(),
    };
    let mut reader = BodyReader {
        source,
        chunk: Bytes::new(),
        connection,
    };
    match server_transport::read_initial_request(server, &mut reader).await {
        Ok(request) => Some((writer, reader, request)),
        Err(error) => {
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(writer, status).await;
            None
        }
    }
}

async fn handle_rpc_stream<K, S>(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    sink: K,
    source: S,
    connection: quinn::Connection,
    shutdown: watch::Receiver<bool>,
) where
    K: BodySink,
    S: BodySource,
{
    if let Some((writer, reader, request)) = read_request(&server, connection, sink, source).await {
        server_transport::handle_stream(server, request_limit, writer, reader, request, shutdown)
            .await;
    }
}

async fn reject_rpc_stream<K, S>(server: Server, sink: K, source: S, connection: quinn::Connection)
where
    K: BodySink,
    S: BodySource,
{
    if let Some((writer, reader, request)) = read_request(&server, connection, sink, source).await {
        server_transport::reject_stream(server, writer, reader, request).await;
    }
}
