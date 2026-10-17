//! Choosing a transport for each connection generation: QUIC first, `QMux` over TLS as the
//! fallback for networks that block UDP.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::{QmuxTransport, QuicTransport, RpcTransport, StreamingRpcTransport};
use crate::sync::MutexExt as _;
use bytes::Bytes;

use crate::{BoxStream, Error, ResponseEnvelope, ResponseFrame, Result, RpcRequest, Status};

pub const DEFAULT_FALLBACK_DELAY: Duration = Duration::from_millis(300);
pub const DEFAULT_REPROBE_INTERVAL: Duration = Duration::from_mins(5);

/// Which transports a [`super::Channel`] connects with.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TransportPolicy {
    /// Prefer QUIC and fall back to `QMux` over TLS when QUIC does not connect within the
    /// fallback delay. Without [`super::ChannelConfig::with_tcp_fallback`] this is QUIC only.
    #[default]
    Auto,
    /// Connect over QUIC only.
    QuicOnly,
    /// Connect over `QMux` over TLS only. Requires [`super::ChannelConfig::with_tcp_fallback`].
    TcpOnly,
}

/// The transport of one connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    /// Native `TrevRPC` over QUIC ([`crate::ALPN`]).
    Quic,
    /// Native `TrevRPC` over `QMux` over TLS ([`crate::QMUX_ALPN`]).
    Qmux,
}

/// The established transport of one connection generation.
#[derive(Clone)]
pub enum GenerationTransport {
    Quic(QuicTransport),
    Qmux(QmuxTransport),
}

impl GenerationTransport {
    pub(crate) const fn kind(&self) -> TransportKind {
        match self {
            Self::Quic(_) => TransportKind::Quic,
            Self::Qmux(_) => TransportKind::Qmux,
        }
    }

    /// Resolves once the connection has ended.
    pub(crate) async fn closed(&self) {
        match self {
            Self::Quic(transport) => {
                transport.connection().closed().await;
            }
            Self::Qmux(transport) => {
                transport.closed().await;
            }
        }
    }

    pub(crate) fn close(&self, reason: &[u8]) {
        match self {
            Self::Quic(transport) => transport.connection().close(0_u32.into(), reason),
            Self::Qmux(transport) => transport.close(),
        }
    }
}

#[crate::async_trait]
impl RpcTransport for GenerationTransport {
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        match self {
            Self::Quic(transport) => transport.call(request, body).await,
            Self::Qmux(transport) => transport.call(request, body).await,
        }
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for GenerationTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        match self {
            Self::Quic(transport) => transport.streaming_call(request, request_body).await,
            Self::Qmux(transport) => transport.streaming_call(request, request_body).await,
        }
    }
}

/// How to reach a server's TCP listener.
#[derive(Clone)]
pub struct TcpDialer {
    pub(crate) addr: Option<SocketAddr>,
    pub(crate) tls: quinn::rustls::ClientConfig,
}

/// Dials every connection generation of a channel.
#[derive(Clone)]
pub struct Dialer {
    pub(crate) endpoint: quinn::Endpoint,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) server_name: String,
    pub(crate) max_frame_size: usize,
    pub(crate) policy: TransportPolicy,
    pub(crate) tcp: Option<TcpDialer>,
    pub(crate) fallback_delay: Duration,
    pub(crate) reprobe_interval: Duration,
    /// When QUIC last lost a race, or `None` while QUIC is preferred.
    pub(crate) quic_failed_at: Arc<Mutex<Option<Instant>>>,
}

impl Dialer {
    pub(crate) async fn dial(&self) -> Result<GenerationTransport> {
        match (self.policy, &self.tcp) {
            (TransportPolicy::QuicOnly, _) | (TransportPolicy::Auto, None) => {
                self.dial_quic().await
            }
            (TransportPolicy::TcpOnly, Some(tcp)) => self.dial_tcp(tcp).await,
            (TransportPolicy::TcpOnly, None) => Err(Error::from(Status::invalid_argument(
                "TransportPolicy::TcpOnly requires ChannelConfig::with_tcp_fallback",
            ))),
            (TransportPolicy::Auto, Some(tcp)) => {
                let remembered = *self.quic_failed_at.lock_unpoisoned();
                if remembered.is_some_and(|failed| failed.elapsed() < self.reprobe_interval) {
                    return self.dial_tcp(tcp).await;
                }
                let transport = self.race(tcp).await?;
                *self.quic_failed_at.lock_unpoisoned() = match transport.kind() {
                    TransportKind::Quic => None,
                    TransportKind::Qmux => Some(Instant::now()),
                };
                Ok(transport)
            }
        }
    }

    /// Starts QUIC, adds the TCP fallback once QUIC fails or the fallback delay passes, and
    /// returns the first to connect. Dropping the loser abandons its connection attempt.
    async fn race(&self, tcp: &TcpDialer) -> Result<GenerationTransport> {
        let quic = self.dial_quic();
        tokio::pin!(quic);
        let quic_error = tokio::select! {
            result = &mut quic => match result {
                Ok(transport) => return Ok(transport),
                Err(error) => Some(error),
            },
            () = tokio::time::sleep(self.fallback_delay) => None,
        };
        let fallback = self.dial_tcp(tcp);
        tokio::pin!(fallback);
        if let Some(quic_error) = quic_error {
            return fallback
                .await
                .map_err(|tcp_error| both_failed(&quic_error, &tcp_error));
        }
        tokio::select! {
            result = &mut quic => match result {
                Ok(transport) => Ok(transport),
                Err(quic_error) => fallback
                    .await
                    .map_err(|tcp_error| both_failed(&quic_error, &tcp_error)),
            },
            result = &mut fallback => match result {
                Ok(transport) => Ok(transport),
                Err(tcp_error) => quic
                    .await
                    .map_err(|quic_error| both_failed(&quic_error, &tcp_error)),
            },
        }
    }

    async fn dial_quic(&self) -> Result<GenerationTransport> {
        let connection = self
            .endpoint
            .connect(self.remote_addr, &self.server_name)
            .map_err(Error::transport)?
            .await
            .map_err(Error::transport)?;
        Ok(GenerationTransport::Quic(
            QuicTransport::new(connection).with_max_frame_size(self.max_frame_size),
        ))
    }

    async fn dial_tcp(&self, tcp: &TcpDialer) -> Result<GenerationTransport> {
        QmuxTransport::connect_with_max_frame_size(
            tcp.addr.unwrap_or(self.remote_addr),
            &self.server_name,
            tcp.tls.clone(),
            self.max_frame_size,
        )
        .await
        .map(GenerationTransport::Qmux)
    }
}

fn both_failed(quic: &Error, tcp: &Error) -> Error {
    Error::from(Status::unavailable(format!(
        "transport unavailable: QUIC: {quic}; QMux over TLS: {tcp}"
    )))
}
