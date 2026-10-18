//! The TCP listener that serves clients that cannot use UDP: `QMux` over TLS for native clients,
//! HTTP/2 for HTTP clients, and `QMux` over WebSocket for browsers, dispatched on the TLS ALPN.

use std::sync::Arc;
use std::time::Duration;

use http::HeaderValue;
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, watch};

use crate::qmux_conn::{MAX_RECORD_SIZE, QmuxConnection, server_config};
use crate::server::Server;
use crate::{http2, native, websocket};

/// Bounds the TLS handshake of a TCP connection and the `QMux` handshake or HTTP request head
/// that follows it.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const H2_ALPN: &[u8] = b"h2";
const HTTP1_ALPN: &[u8] = b"http/1.1";

/// The TCP side of a server: a bound listener and the TLS configuration from
/// [`Server::tcp_tls_config`].
pub struct TcpFallback {
    pub listener: tokio::net::TcpListener,
    pub tls: Arc<quinn::rustls::ServerConfig>,
}

impl Server {
    /// Builds the TLS configuration for the TCP listener of [`Server::serve_with_tcp`].
    ///
    /// It advertises [`crate::QMUX_ALPN`] for native clients, `h2` when
    /// [`crate::server::ServerOptions::http_enabled`] is set, and `http/1.1` (WebSocket) when
    /// [`crate::server::ServerOptions::webtransport_enabled`] is set, replacing any ALPN list
    /// already on `tls`.
    #[must_use]
    pub fn tcp_tls_config(
        &self,
        mut tls: quinn::rustls::ServerConfig,
    ) -> Arc<quinn::rustls::ServerConfig> {
        tls.alpn_protocols = vec![crate::QMUX_ALPN.to_vec()];
        if self.options().http_enabled() {
            tls.alpn_protocols.push(H2_ALPN.to_vec());
        }
        if self.options().webtransport_enabled() {
            tls.alpn_protocols.push(HTTP1_ALPN.to_vec());
        }
        Arc::new(tls)
    }
}

/// Serves one TCP connection on its negotiated ALPN until it ends or the server shuts down.
pub(crate) async fn handle_connection(
    server: Server,
    stream: TcpStream,
    tls: Arc<quinn::rustls::ServerConfig>,
    alt_svc: Option<HeaderValue>,
    request_limit: Option<Arc<Semaphore>>,
    shutdown: watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let Ok(Ok(stream)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
    else {
        return;
    };
    let protocol = stream.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
    let options = server.options();
    match protocol.as_deref() {
        Some(crate::QMUX_ALPN) => {
            let transport =
                qmux::transport::Stream::new(stream, qmux::Version::QMux02, MAX_RECORD_SIZE);
            let mut config = server_config(options);
            config.handshake_timeout = HANDSHAKE_TIMEOUT;
            let Ok(session) = qmux::Session::accept(transport, config).await else {
                return;
            };
            native::handle_connection(
                server,
                QmuxConnection::from(session),
                request_limit,
                shutdown,
            )
            .await;
        }
        Some(H2_ALPN) if options.http_enabled() => {
            http2::handle_connection(
                server,
                stream,
                alt_svc,
                HANDSHAKE_TIMEOUT,
                request_limit,
                shutdown,
            )
            .await;
        }
        Some(HTTP1_ALPN) | None if options.webtransport_enabled() => {
            websocket::handle_connection(
                server,
                stream,
                HANDSHAKE_TIMEOUT,
                request_limit,
                shutdown,
            )
            .await;
        }
        _ => {}
    }
}
