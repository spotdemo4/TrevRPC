//! HTTP/1.1 on the TCP listener (ALPN `http/1.1`): `QMux` over WebSocket browser sessions at the
//! `TrevRPC` path, and every other request served by the HTTP fallback.

use std::convert::Infallible;
use std::future::pending;
use std::sync::Arc;

use http::{HeaderValue, Request, Response, StatusCode};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::Instant;

use crate::fallback::{FallbackBody, ResponseBody, empty_response};
use crate::server::Server;
use crate::websocket;

/// Serves one HTTP/1.1 connection until it closes or the server shuts down. A WebSocket upgrade
/// ends the HTTP/1.1 exchange, and its `QMux` session then runs here, so that it keeps holding
/// the connection's permit.
pub async fn handle_connection<T>(
    server: Server,
    io: T,
    alt_svc: Option<HeaderValue>,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (upgrades, mut pending_upgrade) = mpsc::channel(1);
    let service = {
        let server = server.clone();
        service_fn(move |request| {
            let server = server.clone();
            let alt_svc = alt_svc.clone();
            let upgrades = upgrades.clone();
            async move {
                Ok::<_, Infallible>(handle_request(&server, request, alt_svc, &upgrades).await)
            }
        })
    };
    // The header read timeout also runs while a kept-alive connection waits for its next
    // request, so it bounds idle connections too.
    let connection = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(crate::tcp::HANDSHAKE_TIMEOUT)
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades();
    tokio::pin!(connection);
    let mut drain_deadline: Option<Option<Instant>> = None;
    loop {
        let drained = async {
            match drain_deadline {
                Some(Some(deadline)) => tokio::time::sleep_until(deadline).await,
                _ => pending().await,
            }
        };
        tokio::select! {
            _ = connection.as_mut() => break,
            changed = shutdown.changed(), if drain_deadline.is_none() => {
                if changed.is_err() || *shutdown.borrow() {
                    // The in-flight response finishes, and the connection then closes.
                    connection.as_mut().graceful_shutdown();
                    drain_deadline = Some(
                        server
                            .options()
                            .graceful_shutdown_timeout()
                            .and_then(|timeout| Instant::now().checked_add(timeout)),
                    );
                }
            }
            () = drained => return,
        }
    }
    if let Ok(upgrade) = pending_upgrade.try_recv() {
        websocket::serve_session(server, upgrade, request_limit, shutdown).await;
    }
}

async fn handle_request(
    server: &Server,
    request: Request<Incoming>,
    alt_svc: Option<HeaderValue>,
    upgrades: &mpsc::Sender<OnUpgrade>,
) -> Response<ResponseBody> {
    let options = server.options();
    if !options.claims_path(request.uri().path()) {
        // HTTP/1.1 serves one request at a time, so it needs no stream slot.
        return match options.http_fallback() {
            Some(fallback) => {
                fallback
                    .call(request.map(FallbackBody::http1), alt_svc)
                    .await
            }
            None => empty_response(StatusCode::NOT_FOUND),
        };
    }
    if options.webtransport_enabled() && websocket::is_upgrade(&request) {
        return websocket::accept(server, request, upgrades);
    }
    // `TrevRPC` over HTTP needs HTTP/2 or HTTP/3.
    empty_response(StatusCode::HTTP_VERSION_NOT_SUPPORTED)
}
