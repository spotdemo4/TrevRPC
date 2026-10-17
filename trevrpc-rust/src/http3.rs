//! Server side of ordinary HTTP/3 (`POST /trevrpc`) and `WebTransport` (`/trevrpc`) over QUIC
//! (ALPN `h3`).

use std::sync::Arc;

use bytes::{Buf, Bytes};
use h3::ext::Protocol;
use h3::quic::BidiStream as _;
use http::{Method, Request, Response, StatusCode, header};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::http_admission::{
    MEDIA_TYPE, admission_headers, validate_http_request, validate_webtransport_admission,
};
use crate::http_body::{BodySink, BodySource, handle_rpc_stream, reject_rpc_stream};
use crate::request_pump::RequestTransportEvent;
use crate::serve::{drain_streams, drain_tasks, log_task_error};
use crate::server::Server;
use crate::server_transport::try_acquire_permit;
use crate::{Error, Result, Status};
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
        || raw_connection.close(0_u32.into(), b"server stream drain timed out"),
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
        H3Sink(send, connection.clone()),
        H3Source(recv, connection),
        shutdown,
    )
    .await;
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
                                WebTransportSink(send, stream_connection.clone()),
                                WebTransportSource(recv, stream_connection),
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
                                WebTransportSink(send, stream_connection.clone()),
                                WebTransportSource(recv, stream_connection),
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

struct H3Sink(H3SendStream, quinn::Connection);

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

    /// h3 cannot report a peer `STOP_SENDING` independently of body polling, so only the loss
    /// of the whole connection is observed.
    async fn stopped(&mut self) -> Result<()> {
        Err(Error::transport(self.1.closed().await))
    }
}

struct H3Source(H3RecvStream, quinn::Connection);

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

    async fn transport_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::connection_lost(
            self.1.closed().await,
        ))
    }
}

struct WebTransportSink(WebTransportSendStream, quinn::Connection);

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

    async fn stopped(&mut self) -> Result<()> {
        Err(Error::transport(self.1.closed().await))
    }
}

struct WebTransportSource(WebTransportRecvStream, quinn::Connection);

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

    async fn transport_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::connection_lost(
            self.1.closed().await,
        ))
    }
}
