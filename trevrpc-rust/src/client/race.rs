//! Choosing a transport for each connection generation: QUIC first, `QMux` over TLS as the
//! fallback for networks that block UDP.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::pool::QuicPool;
use super::{CallInfo, QmuxTransport, QuicTransport, RpcTransport, StreamingRpcTransport};
use crate::compression::{CompressionConfig, StreamBudget};
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
    /// One or more QUIC connections; see [`QuicPool`].
    Quic(QuicPool),
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
            Self::Quic(pool) => {
                pool.primary().closed().await;
            }
            Self::Qmux(transport) => {
                transport.closed().await;
            }
        }
    }

    pub(crate) fn close(&self, reason: &[u8]) {
        match self {
            Self::Quic(pool) => pool.close(reason),
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

    async fn call_with_info(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        info: CallInfo,
    ) -> Result<ResponseEnvelope<Bytes>> {
        match self {
            Self::Quic(transport) => transport.call_with_info(request, body, info).await,
            Self::Qmux(transport) => transport.call_with_info(request, body, info).await,
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

    async fn streaming_call_with_info(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        // Whether the call gets a datagram flow, and its dictionaries, is decided by the
        // transport the race chose.
        match self {
            Self::Quic(transport) => {
                transport
                    .streaming_call_with_info(request, request_body, info)
                    .await
            }
            Self::Qmux(transport) => {
                transport
                    .streaming_call_with_info(request, request_body, info)
                    .await
            }
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
    /// The channel's dictionaries, which every connection negotiates afresh.
    pub(crate) compression: Option<Arc<CompressionConfig>>,
    /// The memory the channel's stream compression may hold, shared by every connection.
    pub(crate) stream_budget: Option<Arc<StreamBudget>>,
    pub(crate) max_frame_size: usize,
    pub(crate) policy: TransportPolicy,
    pub(crate) tcp: Option<TcpDialer>,
    pub(crate) fallback_delay: Duration,
    pub(crate) reprobe_interval: Duration,
    /// The most QUIC connections a generation spreads its calls over.
    pub(crate) max_quic_connections: usize,
    /// The QUIC connections a generation keeps open and spreads every call over.
    pub(crate) min_quic_connections: usize,
    /// How long a connection a generation added may go unused before it closes.
    pub(crate) quic_idle_timeout: Duration,
    /// How many calls per second, per connection, a generation spreads and grows at.
    pub(crate) quic_call_rate: u64,
    /// How many bytes per second, per connection, a generation spreads and grows at.
    pub(crate) quic_byte_rate: u64,
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

    /// Dials the generation's QUIC connections, the minimum of them in parallel. The first to
    /// connect is the generation's own; the others join its pool, and those that fail are dialed
    /// again later.
    async fn dial_quic(&self) -> Result<GenerationTransport> {
        let dials = (0..self.min_quic_connections.max(1)).map(|_| self.dial_quic_transport());
        let mut connections = Vec::new();
        let mut failure = None;
        for result in futures_util::future::join_all(dials).await {
            match result {
                Ok(transport) => connections.push(transport),
                Err(error) => failure = failure.or(Some(error)),
            }
        }
        let mut connections = connections.into_iter();
        let Some(primary) = connections.next() else {
            return Err(failure.unwrap_or_else(|| {
                Error::from(Status::unavailable("no QUIC connection was dialed"))
            }));
        };
        Ok(GenerationTransport::Quic(QuicPool::new(
            primary,
            self.clone(),
            connections,
        )))
    }

    /// Dials one QUIC connection, with the channel's settings.
    pub(crate) async fn dial_quic_transport(&self) -> Result<QuicTransport> {
        let connection = self
            .endpoint
            .connect(self.remote_addr, &self.server_name)
            .map_err(Error::transport)?
            .await
            .map_err(Error::transport)?;
        Ok(QuicTransport::new(connection)
            .with_max_frame_size(self.max_frame_size)
            .with_compression_config(self.compression.clone())
            .with_stream_budget(self.stream_budget.clone()))
    }

    async fn dial_tcp(&self, tcp: &TcpDialer) -> Result<GenerationTransport> {
        QmuxTransport::connect_with_max_frame_size(
            tcp.addr.unwrap_or(self.remote_addr),
            &self.server_name,
            tcp.tls.clone(),
            self.max_frame_size,
        )
        .await
        .map(|transport| {
            GenerationTransport::Qmux(
                transport
                    .with_compression_config(self.compression.clone())
                    .with_stream_budget(self.stream_budget.clone()),
            )
        })
    }
}

fn both_failed(quic: &Error, tcp: &Error) -> Error {
    Error::from(Status::unavailable(format!(
        "transport unavailable: QUIC: {quic}; QMux over TLS: {tcp}"
    )))
}
