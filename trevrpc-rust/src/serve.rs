//! The server accept loop: one Quinn endpoint serving native QUIC, HTTP/3 and `WebTransport`, and
//! optionally one TCP listener serving the fallbacks.

use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use http::HeaderValue;
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, watch};
use tokio::task::{JoinError, JoinSet};

use crate::quic::{frame_receive_window, negotiated_protocol, saturating_usize_to_u64, varint};
use crate::server::{Server, ServerOptions};
use crate::server_transport::try_acquire_permit;
use crate::tcp::TcpFallback;
use crate::{Error, Result};
use crate::{http3, native, tcp};

/// Pause after a TCP accept failure, such as running out of file descriptors.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

/// Spare bidirectional stream slots above the per-connection RPC limit: one so an over-limit RPC
/// can still receive a status, and one for the long-lived `WebTransport` CONNECT stream.
const RESERVED_BIDI_STREAMS: u64 = 2;

impl Server {
    /// Builds the Quinn server configuration for an endpoint served by [`Server::serve`].
    ///
    /// The configuration advertises [`crate::ALPN`] for native `TrevRPC`, and [`crate::HTTP3_ALPN`]
    /// only when [`ServerOptions::http_enabled`] or [`ServerOptions::webtransport_enabled`] is
    /// set, replacing any ALPN list already on `tls`. It also sizes the QUIC stream and connection
    /// windows from the server's [`ServerOptions`], so call it after the options are final.
    ///
    /// Datagrams ([`crate::Unreliable`] methods) are not flow controlled, so the configuration
    /// bounds what the transport buffers for the datagram reader of each connection: 128 KiB,
    /// after which the oldest datagrams are dropped. A server therefore uses at most that much
    /// memory per connection for datagrams waiting to be read, plus the 64 datagrams and 64 KiB
    /// it holds for calls whose request has not arrived yet.
    pub fn quic_config(&self, mut tls: quinn::rustls::ServerConfig) -> Result<quinn::ServerConfig> {
        tls.alpn_protocols = vec![crate::ALPN.to_vec()];
        if self.options().http_enabled() || self.options().webtransport_enabled() {
            tls.alpn_protocols.push(crate::HTTP3_ALPN.to_vec());
        }
        let crypto =
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).map_err(Error::transport)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        apply_transport_limits(&mut transport, self.options());
        config.transport_config(Arc::new(transport));
        Ok(config)
    }

    /// Serves `TrevRPC` on a Quinn endpoint until the endpoint stops accepting connections.
    ///
    /// Connections are dispatched on their negotiated ALPN: [`crate::ALPN`] runs native `TrevRPC`,
    /// and [`crate::HTTP3_ALPN`] runs the enabled HTTP/3 `POST` RPCs and `WebTransport` sessions.
    pub async fn serve(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_with_shutdown(endpoint, pending::<()>()).await
    }

    /// Serves `TrevRPC` on a Quinn endpoint until the shutdown future completes, then drains
    /// in-flight RPCs for up to the configured graceful shutdown timeout.
    pub async fn serve_with_shutdown<S>(self, endpoint: quinn::Endpoint, shutdown: S) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        self.serve_sources(endpoint, None, shutdown).await
    }

    /// Serves `TrevRPC` on a Quinn endpoint and a TCP listener until the shutdown future
    /// completes, then drains in-flight RPCs for up to the configured graceful shutdown timeout.
    ///
    /// The TCP listener serves clients that cannot use UDP: `QMux` over TLS ([`crate::QMUX_ALPN`])
    /// always, HTTP/2 `POST` RPCs when HTTP is enabled, and `QMux` over WebSocket browser
    /// sessions when `WebTransport` is enabled. Both share the connection and request limits.
    pub async fn serve_with_tcp<S>(
        self,
        endpoint: quinn::Endpoint,
        tcp: TcpFallback,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        self.serve_sources(endpoint, Some(tcp), shutdown).await
    }

    #[allow(clippy::too_many_lines)]
    async fn serve_sources<S>(
        self,
        endpoint: quinn::Endpoint,
        mut tcp: Option<TcpFallback>,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        // HTTP/2 responses advertise HTTP/3 on the endpoint's port.
        let alt_svc = if self.options().http_enabled() {
            endpoint.local_addr().ok().and_then(|addr| {
                HeaderValue::try_from(format!("h3=\":{}\"; ma=86400", addr.port())).ok()
            })
        } else {
            None
        };
        let connection_limit = self
            .options()
            .max_concurrent_connections()
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let request_limit = self
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

                    let server = self.clone();
                    let request_limit = request_limit.clone();
                    let shutdown = shutdown_rx.clone();
                    connection_tasks.spawn(async move {
                        let Ok(connection) = incoming.await else {
                            return;
                        };
                        let _connection_permit = connection_permit;
                        match negotiated_protocol(&connection).as_deref() {
                            Some(crate::ALPN) => {
                                native::handle_connection(server, connection, request_limit, shutdown)
                                    .await;
                            }
                            Some(crate::HTTP3_ALPN)
                                if server.options().http_enabled()
                                    || server.options().webtransport_enabled() =>
                            {
                                Box::pin(http3::handle_connection(
                                    server,
                                    connection,
                                    request_limit,
                                    shutdown,
                                ))
                                .await;
                            }
                            _ => connection.close(0_u32.into(), b"unsupported ALPN"),
                        }
                    });
                }
                accepted = accept_tcp(tcp.as_ref()) => {
                    let (stream, fallback) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            tracing::warn!(%error, "TCP accept failed");
                            tokio::time::sleep(ACCEPT_BACKOFF).await;
                            continue;
                        }
                    };
                    let Some(connection_permit) = try_acquire_permit(connection_limit.as_ref()) else {
                        continue;
                    };
                    let tls = fallback.tls.clone();
                    let connection = tcp::handle_connection(
                        self.clone(),
                        stream,
                        tls,
                        alt_svc.clone(),
                        request_limit.clone(),
                        shutdown_rx.clone(),
                    );
                    connection_tasks.spawn(async move {
                        let _connection_permit = connection_permit;
                        connection.await;
                    });
                }
                () = &mut shutdown => {
                    break;
                }
                result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    log_task_error(result, "connection task failed");
                }
            }
        }

        // Stop accepting TCP connections; the endpoint keeps refusing QUIC ones while draining.
        drop(tcp.take());
        let _ = shutdown_tx.send(true);
        drain_connections(
            &mut connection_tasks,
            self.options().graceful_shutdown_timeout(),
            &endpoint,
        )
        .await;

        Ok(())
    }
}

/// Accepts the next TCP connection, or never when TCP is disabled.
async fn accept_tcp(tcp: Option<&TcpFallback>) -> std::io::Result<(TcpStream, &TcpFallback)> {
    match tcp {
        Some(fallback) => fallback
            .listener
            .accept()
            .await
            .map(|(stream, _)| (stream, fallback)),
        None => pending().await,
    }
}

/// Flow-control limits derived from a server's [`ServerOptions`], shared by QUIC and `QMux`.
#[derive(Debug, Eq, PartialEq)]
pub struct TransportLimits {
    pub(crate) stream_receive_window: u64,
    pub(crate) connection_receive_window: u64,
    max_concurrent_bidi_streams: Option<u64>,
}

/// Aligns the QUIC windows and stream limits with the server's `TrevRPC` limits.
pub fn transport_limits(options: &ServerOptions) -> TransportLimits {
    let stream_receive_window = frame_receive_window(options.max_frame_size());
    TransportLimits {
        stream_receive_window,
        connection_receive_window: connection_receive_window(options, stream_receive_window),
        max_concurrent_bidi_streams: options.max_concurrent_streams_per_connection().map(
            |max_streams| {
                saturating_usize_to_u64(max_streams).saturating_add(RESERVED_BIDI_STREAMS)
            },
        ),
    }
}

fn apply_transport_limits(config: &mut quinn::TransportConfig, options: &ServerOptions) {
    let limits = transport_limits(options);
    config.stream_receive_window(varint(limits.stream_receive_window));
    config.receive_window(varint(limits.connection_receive_window));
    config.datagram_receive_buffer_size(Some(crate::datagram::DATAGRAM_RECEIVE_BUFFER));
    if let Some(max_streams) = limits.max_concurrent_bidi_streams {
        config.max_concurrent_bidi_streams(varint(max_streams));
    }
    // Unidirectional streams stay at Quinn's default: HTTP/3 needs its three control streams and
    // `WebTransport` opens more, and the connection window bounds what native peers can buffer.
}

fn connection_receive_window(options: &ServerOptions, stream_receive_window: u64) -> u64 {
    let mut connection_receive_window = match options.max_concurrent_streams_per_connection() {
        Some(max_streams) if max_streams > 1 => {
            stream_receive_window.saturating_mul(saturating_usize_to_u64(max_streams))
        }
        _ => stream_receive_window,
    };

    if let Some(max_body_size) = options.max_stream_body_size() {
        let stream_body_window = saturating_usize_to_u64(max_body_size);
        connection_receive_window = connection_receive_window.min(stream_body_window);
        connection_receive_window = connection_receive_window.max(stream_receive_window);
    }

    connection_receive_window
}

/// Waits for `tasks` to finish, aborting them if `timeout` elapses first. Returns whether they all
/// finished in time.
pub async fn drain_tasks(tasks: &mut JoinSet<()>, timeout: Option<Duration>) -> bool {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            log_task_error(Some(result), "task failed while draining");
        }
    };
    let finished = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, drain).await.is_ok()
    } else {
        drain.await;
        true
    };
    if !finished {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    finished
}

/// Drains a connection's stream tasks, calling `close` to end the connection if they overrun
/// `timeout`.
pub async fn drain_streams(
    stream_tasks: &mut JoinSet<()>,
    timeout: Option<Duration>,
    close: impl FnOnce(),
) {
    if !drain_tasks(stream_tasks, timeout).await {
        close();
    }
}

/// Drains connection tasks, refusing new connections meanwhile, then closes the endpoint.
async fn drain_connections(
    connection_tasks: &mut JoinSet<()>,
    timeout: Option<Duration>,
    endpoint: &quinn::Endpoint,
) {
    let refuse_new_connections = async {
        while let Some(incoming) = endpoint.accept().await {
            incoming.refuse();
        }
        pending::<bool>().await
    };
    let finished = tokio::select! {
        finished = drain_tasks(connection_tasks, timeout) => finished,
        never = refuse_new_connections => never,
    };
    if finished {
        endpoint.close(0_u32.into(), b"server shutdown complete");
    } else {
        endpoint.close(0_u32.into(), b"server graceful shutdown timed out");
    }
}

pub fn log_task_error(result: Option<std::result::Result<(), JoinError>>, message: &'static str) {
    if let Some(Err(error)) = result {
        tracing::warn!(%error, "{message}");
    }
}

#[cfg(test)]
mod tests {
    use crate::server::ServerOptions;

    use super::{TransportLimits, transport_limits};

    #[test]
    fn transport_limits_align_with_trevrpc_limits() {
        let options = ServerOptions::new()
            .with_max_frame_size(1024)
            .with_max_stream_body_size(Some(4096))
            .with_max_concurrent_streams_per_connection(Some(10));

        assert_eq!(
            transport_limits(&options),
            TransportLimits {
                stream_receive_window: 1040,
                connection_receive_window: 4096,
                max_concurrent_bidi_streams: Some(12),
            }
        );
    }

    #[test]
    fn transport_limit_calculation_saturates() {
        let options = ServerOptions::new()
            .with_max_frame_size(usize::MAX)
            .with_max_stream_body_size(None)
            .with_max_concurrent_streams_per_connection(Some(usize::MAX));

        let limits = transport_limits(&options);

        assert_eq!(limits.stream_receive_window, u64::MAX);
        assert_eq!(limits.connection_receive_window, u64::MAX);
        assert_eq!(limits.max_concurrent_bidi_streams, Some(u64::MAX));
    }
}
