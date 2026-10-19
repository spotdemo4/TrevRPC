//! Server side of `QMux` over WebSocket (draft-lcurley-qmux-websocket) on the TCP listener (ALPN
//! `http/1.1`): the browser fallback for networks or browsers without `WebTransport`.

use std::sync::Arc;

use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};
use hyper::body::Incoming;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use qmux::ws::tokio_tungstenite::WebSocketStream;
use qmux::ws::tungstenite::handshake::derive_accept_key;
use qmux::ws::tungstenite::protocol::{Role, WebSocketConfig};
use tokio::sync::{Semaphore, mpsc, watch};

use crate::fallback::{ResponseBody, empty_response};
use crate::http_admission::{admission_headers, validate_webtransport_admission};
use crate::native;
use crate::qmux_conn::{MAX_RECORD_SIZE, QmuxConnection, server_config};
use crate::server::Server;

/// The [`crate::QMUX_ALPN`] identifier as a WebSocket subprotocol.
const SUBPROTOCOL: &str = "qmux-02.trevrpc-1";

/// Returns whether `request` asks to upgrade the connection to a WebSocket (RFC 6455 section 4.1).
pub fn is_upgrade(request: &Request<Incoming>) -> bool {
    request.method() == Method::GET
        && header_has_token(request.headers(), &header::CONNECTION, "upgrade")
        && header_has_token(request.headers(), &header::UPGRADE, "websocket")
}

fn header_has_token(headers: &HeaderMap, name: &header::HeaderName, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}

/// Answers a WebSocket upgrade request. An admitted upgrade is answered with 101 and handed to
/// `upgrades`, to run once hyper releases the connection; any other is answered with its
/// rejection.
pub fn accept(
    server: &Server,
    mut request: Request<Incoming>,
    upgrades: &mpsc::Sender<OnUpgrade>,
) -> Response<ResponseBody> {
    if let Err(status) = admit(server, &request) {
        return empty_response(status);
    }
    let headers = request.headers();
    let Some(key) = headers.get(header::SEC_WEBSOCKET_KEY) else {
        return empty_response(StatusCode::BAD_REQUEST);
    };
    if headers.get(header::SEC_WEBSOCKET_VERSION) != Some(&HeaderValue::from_static("13")) {
        let mut response = empty_response(StatusCode::UPGRADE_REQUIRED);
        response.headers_mut().insert(
            header::SEC_WEBSOCKET_VERSION,
            HeaderValue::from_static("13"),
        );
        return response;
    }
    let Ok(accept_key) = HeaderValue::try_from(derive_accept_key(key.as_bytes())) else {
        return empty_response(StatusCode::BAD_REQUEST);
    };
    // The connection carries at most one upgrade: hyper serves no request after it.
    if upgrades.try_send(hyper::upgrade::on(&mut request)).is_err() {
        return empty_response(StatusCode::CONFLICT);
    }
    let mut response = empty_response(StatusCode::SWITCHING_PROTOCOLS);
    let headers = response.headers_mut();
    headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(header::SEC_WEBSOCKET_ACCEPT, accept_key);
    headers.insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(SUBPROTOCOL),
    );
    response
}

/// Admits a WebSocket upgrade like a `WebTransport` session: same path, same admission policy.
fn admit(server: &Server, request: &Request<Incoming>) -> Result<(), StatusCode> {
    let headers = request.headers();
    if !header_has_token(headers, &header::SEC_WEBSOCKET_PROTOCOL, SUBPROTOCOL) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let authority = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok());
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    validate_webtransport_admission(
        server,
        request.uri().path(),
        authority,
        origin,
        true,
        &admission_headers(headers),
    )
    .map_or(Ok(()), Err)
}

/// Serves the `QMux` session of an upgraded WebSocket connection until it ends.
pub async fn serve_session(
    server: Server,
    upgrade: OnUpgrade,
    request_limit: Option<Arc<Semaphore>>,
    shutdown: watch::Receiver<bool>,
) {
    let Ok(upgraded) = upgrade.await else {
        return;
    };
    let record_size = usize::try_from(MAX_RECORD_SIZE).unwrap_or(usize::MAX);
    let config = WebSocketConfig::default()
        .max_message_size(Some(record_size))
        .max_frame_size(Some(record_size));
    let websocket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(config)).await;
    let session = qmux::ws::Upgraded::new(websocket)
        .with_alpn(SUBPROTOCOL)
        .with_session_config(server_config(server.options()))
        .accept();
    native::handle_connection(
        server,
        QmuxConnection::from(session),
        request_limit,
        shutdown,
    )
    .await;
}
