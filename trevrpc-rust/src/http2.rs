//! Server side of ordinary HTTP/2 (`POST /trevrpc`) on the TCP listener (ALPN `h2`), for HTTP
//! clients such as Cronet that fall back from HTTP/3 when UDP is blocked.

use std::future::{pending, poll_fn};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode, header};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::http_admission::{MEDIA_TYPE, validate_http_request};
use crate::http_body::{BodySink, BodySource, handle_rpc_stream};
use crate::request_pump::RequestTransportEvent;
use crate::serve::{drain_tasks, log_task_error, transport_limits};
use crate::server::Server;
use crate::server_transport::try_acquire_permit;
use crate::{Error, Result, Status};

/// Serves one HTTP/2 connection until it closes or the server shuts down.
pub(crate) async fn handle_connection<T>(
    server: Server,
    io: T,
    alt_svc: Option<HeaderValue>,
    handshake_timeout: Duration,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let options = server.options();
    let limits = transport_limits(options);
    let mut builder = h2::server::Builder::new();
    builder
        .initial_window_size(window_size(limits.stream_receive_window))
        .initial_connection_window_size(window_size(limits.connection_receive_window));
    if let Some(streams) = options.max_concurrent_streams_per_connection() {
        // One spare stream lets an over-limit RPC still receive a status.
        builder
            .max_concurrent_streams(u32::try_from(streams.saturating_add(1)).unwrap_or(u32::MAX));
    }
    let Ok(Ok(mut connection)) =
        tokio::time::timeout(handshake_timeout, builder.handshake::<_, Bytes>(io)).await
    else {
        return;
    };
    let stream_limit = options
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let graceful_shutdown_timeout = options.graceful_shutdown_timeout();
    let (closed_tx, closed_rx) = watch::channel(false);
    let mut stream_tasks = JoinSet::new();
    let mut drain_deadline: Option<Option<Instant>> = None;

    loop {
        let drained = async {
            match drain_deadline {
                Some(Some(deadline)) => tokio::time::sleep_until(deadline).await,
                _ => pending().await,
            }
        };
        tokio::select! {
            // The connection is polled throughout, even while draining: it drives every stream.
            accepted = connection.accept() => {
                let Some(Ok((request, respond))) = accepted else {
                    break;
                };
                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    reject(respond, StatusCode::SERVICE_UNAVAILABLE);
                    continue;
                };
                let server = server.clone();
                let request_limit = request_limit.clone();
                let alt_svc = alt_svc.clone();
                let closed = closed_rx.clone();
                let shutdown = shutdown.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_request(server, request_limit, request, respond, alt_svc, closed, shutdown)
                        .await;
                });
            }
            changed = shutdown.changed(), if drain_deadline.is_none() => {
                if changed.is_err() || *shutdown.borrow() {
                    // GOAWAY: in-flight requests finish, and the connection then ends.
                    connection.graceful_shutdown();
                    drain_deadline = Some(graceful_shutdown_timeout.map(|timeout| Instant::now() + timeout));
                }
            }
            () = drained => break,
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                log_task_error(result, "HTTP/2 request task failed");
            }
        }
    }

    let _ = closed_tx.send(true);
    drop(connection);
    drain_tasks(&mut stream_tasks, graceful_shutdown_timeout).await;
}

/// HTTP/2 windows are at most 2^31 - 1 bytes.
fn window_size(window: u64) -> u32 {
    u32::try_from(window.min((1 << 31) - 1)).unwrap_or(u32::MAX)
}

async fn handle_request(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    request: Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    alt_svc: Option<HeaderValue>,
    closed: watch::Receiver<bool>,
    shutdown: watch::Receiver<bool>,
) {
    let (parts, body) = request.into_parts();
    let head = Request::from_parts(parts, ());
    if let Some(status) = validate_http_request(&server, &head) {
        reject(respond, status);
        return;
    }
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, MEDIA_TYPE);
    if let Some(alt_svc) = alt_svc {
        response = response.header(header::ALT_SVC, alt_svc);
    }
    let response = response
        .body(())
        .expect("static HTTP/2 response should be valid");
    let Ok(send) = respond.send_response(response, false) else {
        return;
    };
    handle_rpc_stream(
        server,
        request_limit,
        H2Sink(send),
        H2Source { body, closed },
        shutdown,
    )
    .await;
}

fn reject(mut respond: h2::server::SendResponse<Bytes>, status: StatusCode) {
    let mut response = Response::builder().status(status);
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response = response.header(header::ALLOW, Method::POST.as_str());
    }
    let response = response
        .body(())
        .expect("static HTTP/2 rejection should be valid");
    let _ = respond.send_response(response, true);
}

struct H2Sink(h2::SendStream<Bytes>);

impl BodySink for H2Sink {
    async fn write(&mut self, mut chunk: &[u8]) -> Result<()> {
        while !chunk.is_empty() {
            self.0.reserve_capacity(chunk.len());
            let capacity = poll_fn(|context| self.0.poll_capacity(context))
                .await
                .ok_or_else(|| Error::from(Status::cancelled("HTTP/2 stream reset by peer")))?
                .map_err(Error::transport)?;
            let len = capacity.min(chunk.len());
            if len == 0 {
                continue;
            }
            self.0
                .send_data(Bytes::copy_from_slice(&chunk[..len]), false)
                .map_err(Error::transport)?;
            chunk = &chunk[len..];
        }
        Ok(())
    }

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(
            self.0
                .send_data(Bytes::new(), true)
                .map_err(Error::transport),
        )
    }

    fn reset(&mut self) {
        self.0.send_reset(h2::Reason::CANCEL);
    }

    /// A peer `RST_STREAM` stops the response, unlike over HTTP/3 where only connection loss is
    /// observable.
    async fn stopped(&mut self) -> Result<()> {
        poll_fn(|context| self.0.poll_reset(context))
            .await
            .map(|_| ())
            .map_err(Error::transport)
    }
}

struct H2Source {
    body: h2::RecvStream,
    closed: watch::Receiver<bool>,
}

impl BodySource for H2Source {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        match self.body.data().await {
            Some(Ok(chunk)) => {
                // The chunk is handed to the request reader, so its credit returns at once.
                let _ = self.body.flow_control().release_capacity(chunk.len());
                Ok(Some(chunk))
            }
            Some(Err(error)) => Err(Error::transport(error)),
            None => Ok(None),
        }
    }

    async fn validate_end(&mut self) -> Result<()> {
        match self.body.trailers().await.map_err(Error::transport)? {
            Some(_) => Err(Error::from(Status::invalid_argument(
                "HTTP/2 request trailers are not supported",
            ))),
            None => Ok(()),
        }
    }

    async fn transport_event(&mut self) -> Option<RequestTransportEvent> {
        let _ = self.closed.wait_for(|closed| *closed).await;
        Some(RequestTransportEvent::ConnectionLost(Status::unavailable(
            "transport unavailable: HTTP/2 connection closed",
        )))
    }
}
