//! Server side of ordinary HTTP/3 (`POST /trevrpc`) and `WebTransport` (`/trevrpc`) over QUIC
//! (ALPN `h3`).

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use h3::ext::Protocol;
use h3::quic::BidiStream as _;
use h3_webtransport::server::AcceptedBi;
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::datagram::Demux;
use crate::http_admission::{
    admission_headers, rejection_head, rpc_response_head, validate_http_request,
    validate_webtransport_admission,
};
use crate::http_body::{BodySink, BodySource, handle_rpc_stream, reject_rpc_stream};
use crate::quic::SessionDatagrams;
use crate::request_pump::RequestTransportEvent;
use crate::serve::{drain_streams, drain_tasks, log_task_error};
use crate::server::Server;
use crate::server_transport::{Permit, try_acquire_permit};
use crate::{Error, Result, Status};
// RFC WebTransport reserves this error-code range for application protocol errors. Code 1 maps
// to the first application code plus one; the division term is zero for this value.
const CANCELLED_STREAM_CODE: u64 = 0x52e4_a40f_a8dc;

/// How long a server close or drain of a `WebTransport` session waits for the client: to read
/// the capsule, and after a close to close its side of the CONNECT stream. Closing the QUIC
/// connection before that can lose the close.
const SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

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
pub async fn handle_connection(
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
                let resolve = within_initial_request_timeout(
                    server.options().initial_request_timeout(),
                    request_resolver.resolve_request(),
                );
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
                    let response_headers = match validate_webtransport_request(&server, &request) {
                        Ok(headers) => headers,
                        Err(status) => {
                            reject_h3(stream, status).await;
                            continue;
                        }
                    };
                    let accept = WebTransportSession::accept(
                        request,
                        stream,
                        h3_connection,
                        response_headers,
                    );
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
    stream_tasks.spawn(handle_http_request(
        server,
        request_limit,
        stream_permit,
        request,
        stream,
        connection,
        shutdown,
    ));
}

async fn handle_http_request(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    mut stream_permit: Permit,
    request: Request<()>,
    mut stream: H3RequestStream,
    connection: quinn::Connection,
    shutdown: watch::Receiver<bool>,
) {
    if let Some(status) = validate_http_request(&server, &request) {
        stream_permit.release();
        reject_h3(stream, status).await;
        return;
    }

    if stream.send_response(rpc_response_head(None)).await.is_err() {
        return;
    }
    let (send, recv) = stream.split();
    handle_rpc_stream(
        server,
        request_limit,
        stream_permit,
        None,
        H3Sink(send),
        H3Source(recv, connection),
        shutdown,
    )
    .await;
}

/// Resolves `future`, giving up after `timeout`. `None` means it failed or timed out.
async fn within_initial_request_timeout<T, E>(
    timeout: Option<Duration>,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Option<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .ok()
            .and_then(std::result::Result::ok),
        None => future.await.ok(),
    }
}

async fn reject_h3(mut stream: H3RequestStream, status: StatusCode) {
    if stream.send_response(rejection_head(status)).await.is_ok() {
        let _ = stream.finish().await;
    }
}

fn is_webtransport_request(request: &Request<()>) -> bool {
    request.method() == Method::CONNECT
        && request.extensions().get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT)
}

/// The WebTransport protocol that names wire version 1.
const WEBTRANSPORT_PROTOCOL: &str = "trevrpc-1";

/// The CONNECT header listing the WebTransport protocols the client offers, an RFC 9651 list of
/// strings (draft-ietf-webtrans-http3).
const WT_AVAILABLE_PROTOCOLS: &str = "wt-available-protocols";

/// The response header naming the protocol the server selected, an RFC 9651 string.
const WT_PROTOCOL: &str = "wt-protocol";

/// `WEBTRANSPORT_PROTOCOL` as an RFC 9651 string, the value of `WT-Protocol`.
const WT_PROTOCOL_VALUE: &str = "\"trevrpc-1\"";

/// Admits a `WebTransport` CONNECT, returning the headers to add to its response, or the status
/// to reject it with.
fn validate_webtransport_request(
    server: &Server,
    request: &Request<()>,
) -> std::result::Result<HeaderMap, StatusCode> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    if let Some(status) = validate_webtransport_admission(
        server,
        request.uri().path(),
        request.uri().authority().map(http::uri::Authority::as_str),
        origin,
        request.uri().scheme_str() == Some("https"),
        &admission_headers(request.headers()),
    ) {
        return Err(status);
    }
    let mut headers = HeaderMap::new();
    if select_webtransport_protocol(request.headers())? {
        headers.insert(WT_PROTOCOL, HeaderValue::from_static(WT_PROTOCOL_VALUE));
    }
    Ok(headers)
}

/// Decides whether the CONNECT's offer selects `trevrpc-1`: `Ok(true)` when the client offered
/// it, so the response must name it in `WT-Protocol`, and `Ok(false)` when the client offered
/// nothing. A CONNECT that offers protocols but not `trevrpc-1` is a bad request.
///
/// A missing header means version 1, because older browsers cannot send it. A header that is not
/// a list of strings is ignored, and so is an empty list, which RFC 9651 section 3.1 represents by
/// not sending the field.
fn select_webtransport_protocol(headers: &HeaderMap) -> std::result::Result<bool, StatusCode> {
    let Some(offered) = webtransport_offered_protocols(headers) else {
        return Ok(false);
    };
    if offered
        .iter()
        .any(|offered| offered == WEBTRANSPORT_PROTOCOL)
    {
        Ok(true)
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

/// Returns the protocols the CONNECT offers, or `None` when the header is missing, malformed, or
/// an empty list. Repeated header lines are combined into one list, as RFC 9651 specifies.
fn webtransport_offered_protocols(headers: &http::HeaderMap) -> Option<Vec<String>> {
    let mut values = headers.get_all(WT_AVAILABLE_PROTOCOLS).iter().peekable();
    values.peek()?;
    let mut combined = String::new();
    for value in values {
        if !combined.is_empty() {
            combined.push(',');
        }
        combined.push_str(value.to_str().ok()?);
    }
    crate::structured_fields::parse_string_list(&combined).filter(|offered| !offered.is_empty())
}

#[allow(clippy::too_many_lines)]
async fn handle_webtransport_session(
    server: Server,
    session: WebTransportSession,
    request_limit: Option<Arc<Semaphore>>,
    stream_limit: Option<Arc<Semaphore>>,
    connection: quinn::Connection,
    mut shutdown: watch::Receiver<bool>,
) {
    let session_id = session.session_id();
    let initial_request_timeout = server.options().initial_request_timeout();
    let mut stream_tasks = JoinSet::new();
    // Accepted streams whose first frame is still being read, to tell WebTransport streams from
    // HTTP/3 requests without one slow stream holding up the others.
    let mut resolving = JoinSet::new();
    // The session's datagrams are framed here, on the raw connection; see `SessionDatagrams`.
    // Nothing may call `session.datagram_reader()` or `session.datagram_sender()`: quinn's
    // `read_datagram` has a single consumer.
    let demux = SessionDatagrams::new(
        connection.clone(),
        h3::quic::StreamId::from(session_id).into_inner(),
    )
    .map(|datagrams| {
        Arc::new(Demux::server(
            datagrams,
            server.options().max_concurrent_streams_per_connection(),
            server.max_frame_size(),
        ))
    });
    // A close from the client is only noticed while this is polled.
    let closed = session.closed();
    tokio::pin!(closed);
    let mut terminated = false;

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            accepted = session.accept_bi(), if !*shutdown.borrow() => {
                match accepted {
                    Ok(Some(resolver)) => {
                        resolving.spawn(within_initial_request_timeout(
                            initial_request_timeout,
                            resolver.resolve(),
                        ));
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let message = error.to_string();
                        connection.close(0_u32.into(), message.as_bytes());
                        break;
                    }
                }
            }
            Some(resolved) = resolving.join_next(), if !resolving.is_empty() => {
                match resolved {
                    Ok(Some(AcceptedBi::BidiStream(id, stream))) if id == session_id => {
                        let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                            let server = server.clone();
                            let stream_connection = connection.clone();
                            let (send, recv) = stream.split();
                            stream_tasks.spawn(reject_rpc_stream(
                                server,
                                WebTransportSink(send),
                                WebTransportSource(recv, stream_connection),
                            ));
                            continue;
                        };
                        let server = server.clone();
                        let request_limit = request_limit.clone();
                        let stream_connection = connection.clone();
                        let stream_shutdown = shutdown.clone();
                        let stream_demux = demux.clone();
                        stream_tasks.spawn(async move {
                            let (send, recv) = stream.split();
                            handle_rpc_stream(
                                server,
                                request_limit,
                                stream_permit,
                                stream_demux,
                                WebTransportSink(send),
                                WebTransportSource(recv, stream_connection),
                                stream_shutdown,
                            )
                            .await;
                        });
                    }
                    Ok(Some(AcceptedBi::BidiStream(_, mut stream))) => {
                        h3::quic::SendStream::reset(&mut stream, CANCELLED_STREAM_CODE);
                    }
                    Ok(Some(AcceptedBi::Request(request, stream))) => {
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
                    Ok(None) => {}
                    Err(error) => tracing::warn!(%error, "WebTransport stream resolution failed"),
                }
            }
            _ = &mut closed => {
                // The session's streams are reset with WT_SESSION_GONE as they are next used.
                terminated = true;
                break;
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
    resolving.abort_all();

    let shutting_down = *shutdown.borrow() && !terminated;
    if shutting_down {
        // Lets clients that support WebTransport.draining wind down before the close.
        let _ = tokio::time::timeout(SESSION_CLOSE_TIMEOUT, session.drain()).await;
    }
    let drained = drain_tasks(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
    )
    .await;
    if shutting_down {
        let reason = if drained {
            "server shutdown"
        } else {
            "server WebTransport stream drain timed out"
        };
        let close = async {
            if session.close(0, reason).await.is_ok() {
                let _ = (&mut closed).await;
            }
        };
        let _ = tokio::time::timeout(SESSION_CLOSE_TIMEOUT, close).await;
    }
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

    async fn stopped(&mut self) -> Result<()> {
        stopped(self.0.stopped().await).await
    }
}

/// Maps an h3 `stopped()` result to [`BodySink::stopped`]: `Ok(Some(_))` is the peer's
/// `STOP_SENDING`, `Ok(None)` means the stream is closed and can no longer be stopped, and an
/// error means the connection was lost.
async fn stopped<C, E>(result: std::result::Result<Option<C>, E>) -> Result<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match result {
        Ok(Some(_)) => Ok(()),
        Ok(None) => pending().await,
        Err(error) => Err(Error::transport(error)),
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

    fn stop(&mut self) {
        self.0.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
    }

    async fn transport_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::connection_lost(
            self.1.closed().await,
        ))
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

    async fn stopped(&mut self) -> Result<()> {
        stopped(self.0.stopped().await).await
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

    fn validate_end(&mut self) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn stop(&mut self) {
        h3::quic::RecvStream::stop_sending(&mut self.0, CANCELLED_STREAM_CODE);
    }

    async fn transport_event(&mut self) -> Option<RequestTransportEvent> {
        Some(RequestTransportEvent::connection_lost(
            self.1.closed().await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue, StatusCode};

    /// Quinn's `read_datagram` has a single consumer, which is this crate's datagram reader
    /// ([`crate::datagram::Demux`]). h3's own datagram handlers would compete with it for the
    /// connection's datagrams, so no code here may ask a session for them.
    #[test]
    fn nothing_here_asks_h3_for_the_sessions_datagrams() {
        let source = include_str!("http3.rs");
        let forbidden = [
            [".datagram", "_reader("].concat(),
            [".datagram", "_sender("].concat(),
            ["get_datagram", "_reader("].concat(),
            ["get_datagram", "_sender("].concat(),
        ];
        for (number, line) in source.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for needle in &forbidden {
                assert!(
                    !line.contains(needle.as_str()),
                    "http3.rs line {} uses h3's datagram handlers: {line}",
                    number + 1
                );
            }
        }
    }

    use super::{
        WEBTRANSPORT_PROTOCOL, WT_AVAILABLE_PROTOCOLS, WT_PROTOCOL_VALUE,
        select_webtransport_protocol,
    };

    fn headers(lines: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for line in lines {
            headers.append(
                WT_AVAILABLE_PROTOCOLS,
                HeaderValue::from_str(line).expect("test header value should be valid"),
            );
        }
        headers
    }

    fn check(lines: &[&str]) -> Option<StatusCode> {
        select_webtransport_protocol(&headers(lines)).err()
    }

    fn selects(lines: &[&str]) -> bool {
        select_webtransport_protocol(&headers(lines)) == Ok(true)
    }

    #[test]
    fn a_missing_header_means_version_1() {
        assert_eq!(check(&[]), None);
        assert!(!selects(&[]));
    }

    #[test]
    fn an_offer_that_includes_trevrpc_1_is_accepted_and_selected() {
        for lines in [
            &[r#""trevrpc-1""#][..],
            &[r#""trevrpc-2", "trevrpc-1";v=2"#],
            &[r#""trevrpc-2""#, r#""trevrpc-1""#],
        ] {
            assert!(selects(lines), "{lines:?}");
        }
    }

    #[test]
    fn the_wt_protocol_value_is_the_protocol_as_a_structured_string() {
        assert_eq!(WT_PROTOCOL_VALUE, format!("\"{WEBTRANSPORT_PROTOCOL}\""));
    }

    #[test]
    fn an_offer_without_trevrpc_1_is_a_bad_request() {
        for lines in [
            &[r#""trevrpc-2""#][..],
            &[r#""trevrpc-2", "other""#],
            &[r#""trevrpc-2""#, r#""other""#],
            // An empty field line contributes no members.
            &["", r#""trevrpc-2""#],
            &[r#""TREVRPC-1""#],
            &[r#""trevrpc-1 ""#],
            &[r#""trevrpc-11""#],
        ] {
            assert_eq!(check(lines), Some(StatusCode::BAD_REQUEST), "{lines:?}");
        }
    }

    #[test]
    fn an_empty_list_is_treated_as_an_absent_header() {
        for lines in [&[""][..], &["   "], &["", "  "]] {
            assert_eq!(check(lines), None, "{lines:?}");
            assert!(!selects(lines), "{lines:?}");
        }
    }

    #[test]
    fn a_malformed_header_is_ignored() {
        for lines in [
            &["trevrpc-2"][..],
            &[r#""trevrpc-2"#],
            &[r#""trevrpc-2","#],
            &[r#"("trevrpc-2")"#],
            &[r#""trevrpc-2""other""#],
        ] {
            assert_eq!(check(lines), None, "{lines:?}");
            assert!(!selects(lines), "{lines:?}");
        }
    }

    #[test]
    fn a_header_that_is_not_text_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.append(
            WT_AVAILABLE_PROTOCOLS,
            HeaderValue::from_bytes(b"\"trevrpc-2\xff\"").expect("opaque bytes are allowed"),
        );

        assert_eq!(select_webtransport_protocol(&headers), Ok(false));
    }
}
