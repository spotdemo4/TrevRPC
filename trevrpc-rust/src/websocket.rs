//! Server side of `QMux` over WebSocket (draft-lcurley-qmux-websocket) on the TCP listener (ALPN
//! `http/1.1`): the browser fallback for networks or browsers without `WebTransport`.

use std::sync::Arc;
use std::time::Duration;

use http::{StatusCode, header};
use qmux::ws::tokio_tungstenite::accept_hdr_async_with_config;
use qmux::ws::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use qmux::ws::tungstenite::protocol::WebSocketConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, watch};

use crate::http_admission::{admission_headers, validate_webtransport_admission};
use crate::native;
use crate::qmux_conn::{MAX_RECORD_SIZE, server_config};
use crate::server::Server;

/// The [`crate::QMUX_ALPN`] identifier as a WebSocket subprotocol.
const SUBPROTOCOL: &str = "qmux-02.trevrpc-1";

/// Upgrades one HTTP/1.1 connection to a WebSocket and serves its `QMux` session until it ends.
// The handshake callback must return tungstenite's own, large, error response.
#[allow(clippy::result_large_err)]
pub(crate) async fn handle_connection<T>(
    server: Server,
    io: T,
    handshake_timeout: Duration,
    request_limit: Option<Arc<Semaphore>>,
    shutdown: watch::Receiver<bool>,
) where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let record_size = usize::try_from(MAX_RECORD_SIZE).unwrap_or(usize::MAX);
    let config = WebSocketConfig::default()
        .max_message_size(Some(record_size))
        .max_frame_size(Some(record_size));
    let admit = |request: &Request, response: Response| admit(&server, request, response);
    let Ok(Ok(websocket)) = tokio::time::timeout(
        handshake_timeout,
        accept_hdr_async_with_config(io, admit, Some(config)),
    )
    .await
    else {
        return;
    };
    let session = qmux::ws::Upgraded::new(websocket)
        .with_alpn(SUBPROTOCOL)
        .with_session_config(server_config(server.options()))
        .accept();
    native::handle_connection(server, session, request_limit, shutdown).await;
}

/// Admits a WebSocket upgrade like a `WebTransport` session: same path, same admission policy.
#[allow(clippy::result_large_err)]
fn admit(
    server: &Server,
    request: &Request,
    mut response: Response,
) -> Result<Response, ErrorResponse> {
    let offered = request
        .headers()
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|protocol| protocol.trim() == SUBPROTOCOL);
    if !offered {
        return Err(rejection(StatusCode::BAD_REQUEST));
    }
    let headers = request.headers();
    let authority = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok());
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    if let Some(status) = validate_webtransport_admission(
        server,
        request.uri().path(),
        authority,
        origin,
        true,
        &admission_headers(headers),
    ) {
        return Err(rejection(status));
    }
    response.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        http::HeaderValue::from_static(SUBPROTOCOL),
    );
    Ok(response)
}

fn rejection(status: StatusCode) -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = status;
    response
}
