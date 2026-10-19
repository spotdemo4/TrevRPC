use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use super::race::{
    DEFAULT_FALLBACK_DELAY, DEFAULT_REPROBE_INTERVAL, Dialer, GenerationTransport, TcpDialer,
    TransportKind, TransportPolicy,
};
use super::{RpcTransport, StreamingRpcTransport};
use crate::sync::MutexExt as _;
use bytes::Bytes;

use crate::{
    BoxStream, Error, ResponseEnvelope, ResponseFrame, Result, RpcRequest, Status, Unreliable,
};

const EVENT_CAPACITY: usize = 32;
const CLOSE_REASON: &[u8] = b"channel closed";

static NEXT_BACKOFF_SEED: AtomicU64 = AtomicU64::new(0x6a09_e667_f3bc_c909);

/// Supplies the delay before a channel makes a reconnect attempt.
///
/// Attempts start at one for the first redial after a connection is lost. Implementations must
/// return promptly and must not block.
pub trait ReconnectBackoff: Send + Sync + 'static {
    fn delay(&self, attempt: u32) -> Duration;
}

/// Bounded exponential reconnect backoff with deterministic per-attempt jitter.
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    initial_delay: Duration,
    max_delay: Duration,
    jitter_ratio: f64,
    seed: u64,
}

impl ExponentialBackoff {
    /// Creates a backoff capped at `max_delay`.
    #[must_use]
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay: max_delay.max(initial_delay),
            jitter_ratio: 0.2,
            seed: NEXT_BACKOFF_SEED.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Sets the maximum proportional jitter in either direction.
    #[must_use]
    pub const fn with_jitter_ratio(mut self, jitter_ratio: f64) -> Self {
        self.jitter_ratio = if jitter_ratio.is_nan() {
            0.0
        } else {
            jitter_ratio.clamp(0.0, 1.0)
        };
        self
    }

    /// Sets the deterministic jitter seed, primarily for reproducible tests.
    #[must_use]
    pub const fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    fn base_delay(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        self.initial_delay
            .saturating_mul(1_u32 << exponent)
            .min(self.max_delay)
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(30))
    }
}

impl ReconnectBackoff for ExponentialBackoff {
    fn delay(&self, attempt: u32) -> Duration {
        let base = self.base_delay(attempt);
        if base.is_zero() || self.jitter_ratio == 0.0 {
            return base;
        }

        let random = splitmix64(self.seed ^ u64::from(attempt));
        // The top 32 bits give the jitter far finer resolution than a retry delay needs.
        let [a, b, c, d, ..] = random.to_be_bytes();
        let unit = f64::from(u32::from_be_bytes([a, b, c, d])) / f64::from(u32::MAX);
        let jitter = unit.mul_add(2.0, -1.0) * self.jitter_ratio;
        let delay = if jitter.is_sign_negative() {
            base.saturating_sub(base.mul_f64(-jitter))
        } else {
            base.saturating_add(base.mul_f64(jitter))
        };
        delay.min(self.max_delay)
    }
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Configuration shared by every connection generation of a [`Channel`].
#[derive(Clone)]
pub struct ChannelConfig {
    max_frame_size: usize,
    reconnect_backoff: Arc<dyn ReconnectBackoff>,
    policy: TransportPolicy,
    tcp: Option<TcpDialer>,
    fallback_delay: Duration,
    reprobe_interval: Duration,
}

impl ChannelConfig {
    /// Creates the default channel configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_frame_size: crate::framing::DEFAULT_MAX_FRAME_SIZE,
            reconnect_backoff: Arc::new(ExponentialBackoff::default()),
            policy: TransportPolicy::Auto,
            tcp: None,
            fallback_delay: DEFAULT_FALLBACK_DELAY,
            reprobe_interval: DEFAULT_REPROBE_INTERVAL,
        }
    }

    /// Enables `QMux` over TLS to the server's TCP listener, the fallback for networks that block
    /// UDP. `tls` verifies the server; its ALPN list is replaced with [`crate::QMUX_ALPN`]. The
    /// TCP address defaults to the channel's remote address.
    #[must_use]
    pub fn with_tcp_fallback(mut self, tls: quinn::rustls::ClientConfig) -> Self {
        let addr = self.tcp.as_ref().and_then(|tcp| tcp.addr);
        self.tcp = Some(TcpDialer { addr, tls });
        self
    }

    /// Sets the address of the server's TCP listener when it differs from the remote address.
    #[must_use]
    pub const fn with_tcp_addr(mut self, addr: SocketAddr) -> Self {
        if let Some(tcp) = &mut self.tcp {
            tcp.addr = Some(addr);
        }
        self
    }

    /// Selects which transports the channel connects with. The default is
    /// [`TransportPolicy::Auto`].
    #[must_use]
    pub const fn with_transport_policy(mut self, policy: TransportPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Sets how long QUIC may take to connect before `QMux` over TLS is tried in parallel. The
    /// default is 300 milliseconds.
    #[must_use]
    pub const fn with_fallback_delay(mut self, delay: Duration) -> Self {
        self.fallback_delay = delay;
        self
    }

    /// Sets how long reconnects keep using `QMux` over TLS after it won a race, before QUIC is
    /// tried again. The default is five minutes.
    #[must_use]
    pub const fn with_reprobe_interval(mut self, interval: Duration) -> Self {
        self.reprobe_interval = interval;
        self
    }

    /// Sets the maximum `TrevRPC` frame size for every connection generation.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Replaces the bounded exponential reconnect backoff.
    #[must_use]
    pub fn with_reconnect_backoff(mut self, reconnect_backoff: impl ReconnectBackoff) -> Self {
        self.reconnect_backoff = Arc::new(reconnect_backoff);
        self
    }
}

impl ChannelConfig {
    /// Builds the Quinn client configuration for an endpoint used by a [`Channel`].
    ///
    /// The configuration advertises [`crate::ALPN`] and sizes the QUIC stream windows for this
    /// configuration's maximum frame size. Native clients never accept peer-initiated streams, so
    /// those limits are zero.
    ///
    /// The configuration also bounds the datagrams the transport buffers for the connection's
    /// datagram reader, which datagram messages of [`crate::Unreliable`] methods bypass flow
    /// control for: 128 KiB, after which the oldest datagrams are dropped.
    pub fn quic_config(&self, mut tls: quinn::rustls::ClientConfig) -> Result<quinn::ClientConfig> {
        tls.alpn_protocols = vec![crate::ALPN.to_vec()];
        let crypto =
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).map_err(Error::transport)?;
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        let window = crate::quic::frame_receive_window(self.max_frame_size);
        transport.stream_receive_window(crate::quic::varint(window));
        transport.receive_window(crate::quic::varint(window));
        transport.datagram_receive_buffer_size(Some(crate::datagram::DATAGRAM_RECEIVE_BUFFER));
        transport.max_concurrent_bidi_streams(0_u8.into());
        transport.max_concurrent_uni_streams(0_u8.into());
        config.transport_config(Arc::new(transport));
        Ok(config)
    }
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// High-level lifecycle phase of a channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelPhase {
    Ready,
    Reconnecting,
    Closed,
}

/// Latest observable channel lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChannelState {
    phase: ChannelPhase,
    generation: u64,
    reconnect_attempt: Option<u32>,
}

impl ChannelState {
    /// Returns the current lifecycle phase.
    #[must_use]
    pub const fn phase(self) -> ChannelPhase {
        self.phase
    }

    /// Returns the most recently established connection generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the pending reconnect attempt number while reconnecting.
    #[must_use]
    pub const fn reconnect_attempt(self) -> Option<u32> {
        self.reconnect_attempt
    }

    /// Returns whether calls can currently snapshot a connection.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self.phase, ChannelPhase::Ready)
    }
}

/// Metrics-friendly channel lifecycle event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelEvent {
    ConnectionLost {
        generation: u64,
    },
    ReconnectAttempt {
        generation: u64,
        attempt: u32,
    },
    ReconnectFailed {
        generation: u64,
        attempt: u32,
    },
    Ready {
        generation: u64,
        transport: TransportKind,
    },
    Closed {
        generation: u64,
    },
}

/// A reconnecting native channel for generated clients: QUIC, with `QMux` over TLS as the
/// optional fallback when UDP is blocked (see [`ChannelConfig::with_tcp_fallback`]).
///
/// Each RPC snapshots exactly one established connection generation. RPCs are never retried or
/// replayed. Calls started before connection loss receive the stream or connection error, while
/// calls made during reconnection fail immediately with `Unavailable`.
///
/// The channel retains one Quinn endpoint for every redial, preserving the rustls TLS
/// session-resumption cache. It always awaits a complete handshake before becoming ready and never
/// calls `Connecting::into_0rtt`.
#[derive(Clone)]
pub struct Channel {
    inner: Arc<Inner>,
}

struct Inner {
    dialer: Dialer,
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
}

struct Shared {
    slot: Mutex<Slot>,
    state_tx: watch::Sender<ChannelState>,
    event_tx: broadcast::Sender<ChannelEvent>,
}

struct Slot {
    state: ChannelState,
    transport: Option<GenerationTransport>,
}

impl Channel {
    /// Establishes an initial connection and starts its reconnect supervisor.
    ///
    /// Build the endpoint's client configuration with [`ChannelConfig::quic_config`]. The returned
    /// future covers the complete initial handshake and may be bounded or cancelled with the
    /// caller's normal future, task, or request-context deadline semantics.
    pub async fn connect(
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        Self::connect_with_config(endpoint, remote_addr, server_name, ChannelConfig::new()).await
    }

    /// Establishes an initial connection with explicit channel configuration.
    pub async fn connect_with_config(
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: impl Into<String>,
        config: ChannelConfig,
    ) -> Result<Self> {
        let dialer = Dialer {
            endpoint,
            remote_addr,
            server_name: server_name.into(),
            max_frame_size: config.max_frame_size,
            policy: config.policy,
            tcp: config.tcp.clone(),
            fallback_delay: config.fallback_delay,
            reprobe_interval: config.reprobe_interval,
            quic_failed_at: Arc::default(),
        };
        let transport = dialer.dial().await?;
        let initial_state = ChannelState {
            phase: ChannelPhase::Ready,
            generation: 1,
            reconnect_attempt: None,
        };
        let (state_tx, _) = watch::channel(initial_state);
        let (event_tx, _) = broadcast::channel(EVENT_CAPACITY);
        let shared = Arc::new(Shared {
            slot: Mutex::new(Slot {
                state: initial_state,
                transport: Some(transport.clone()),
            }),
            state_tx,
            event_tx,
        });
        let (shutdown, shutdown_rx) = watch::channel(false);

        tokio::spawn(supervise(
            dialer.clone(),
            config,
            Arc::clone(&shared),
            shutdown_rx,
            transport,
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                dialer,
                shared,
                shutdown,
            }),
        })
    }

    /// Returns the latest lifecycle state.
    #[must_use]
    pub fn state(&self) -> ChannelState {
        self.inner.shared.state()
    }

    /// Returns the transport of the current connection, or `None` while not ready.
    #[must_use]
    pub fn transport(&self) -> Option<TransportKind> {
        self.snapshot().ok().map(|transport| transport.kind())
    }

    /// Returns the current QUIC connection, or `None` while not ready or when the channel is
    /// connected over `QMux`.
    ///
    /// It is for diagnostics, such as [`quinn::Connection::stats`] to see how many DATAGRAM frames
    /// a call used. Closing it ends the channel's generation like any lost connection.
    #[must_use]
    pub fn quic_connection(&self) -> Option<quinn::Connection> {
        match self.snapshot().ok()? {
            GenerationTransport::Quic(transport) => Some(transport.connection().clone()),
            GenerationTransport::Qmux(_) => None,
        }
    }

    /// Returns whether a connection can currently be snapshotted for a new RPC.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state().is_ready()
    }

    /// Waits for this client to become ready and returns the established generation.
    pub async fn wait_until_ready(&self) -> Result<u64> {
        let mut states = self.subscribe_state();
        loop {
            let state = *states.borrow_and_update();
            match state.phase {
                ChannelPhase::Ready => return Ok(state.generation),
                ChannelPhase::Closed => {
                    return Err(unavailable("channel is closed"));
                }
                ChannelPhase::Reconnecting => {}
            }
            states
                .changed()
                .await
                .map_err(|_| unavailable("channel lifecycle observation stopped"))?;
        }
    }

    /// Observes the latest state. The initial value is the state at subscription time.
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ChannelState> {
        self.inner.shared.state_tx.subscribe()
    }

    /// Observes lifecycle transitions suitable for counters and structured telemetry.
    ///
    /// A lagging receiver reports [`broadcast::error::RecvError::Lagged`]; callers should use
    /// [`Self::state`] to resynchronize.
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<ChannelEvent> {
        self.inner.shared.event_tx.subscribe()
    }

    /// Stops reconnecting and closes this channel's current connection.
    pub fn close(&self) {
        let (closed, transport) = self.inner.shared.close();
        if closed {
            let _ = self.inner.shutdown.send(true);
            if let Some(transport) = transport {
                transport.close(CLOSE_REASON);
            }
        }
    }

    /// Returns the remote address this channel dials.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.dialer.remote_addr
    }

    /// Returns the local address of the channel's Quinn endpoint.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.dialer.endpoint.local_addr()
    }

    /// Switches the UDP socket used by the channel's Quinn endpoint, for example after a network
    /// change.
    ///
    /// Rebinding is endpoint-wide and does not itself change the channel generation.
    pub fn rebind(&self, socket: UdpSocket) -> std::io::Result<()> {
        self.inner.dialer.endpoint.rebind(socket)
    }

    fn snapshot(&self) -> Result<GenerationTransport> {
        self.inner.shared.snapshot()
    }
}

#[crate::async_trait]
impl RpcTransport for Channel {
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        self.snapshot()?.call(request, body).await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for Channel {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.snapshot()?.streaming_call(request, request_body).await
    }

    async fn streaming_call_unreliable(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        unreliable: Unreliable,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.snapshot()?
            .streaming_call_unreliable(request, request_body, unreliable)
            .await
    }
}

impl Shared {
    fn state(&self) -> ChannelState {
        self.slot.lock_unpoisoned().state
    }

    fn snapshot(&self) -> Result<GenerationTransport> {
        let slot = self.slot.lock_unpoisoned();
        match slot.state.phase {
            ChannelPhase::Ready => slot
                .transport
                .clone()
                .ok_or_else(|| unavailable("channel has no ready connection")),
            ChannelPhase::Reconnecting => Err(unavailable("channel is reconnecting")),
            ChannelPhase::Closed => Err(unavailable("channel is closed")),
        }
    }

    fn begin_reconnect(&self) -> Option<u64> {
        let state = {
            let mut slot = self.slot.lock_unpoisoned();
            if matches!(slot.state.phase, ChannelPhase::Closed) {
                return None;
            }
            slot.transport = None;
            slot.state.phase = ChannelPhase::Reconnecting;
            slot.state.reconnect_attempt = Some(1);
            slot.state
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ChannelEvent::ConnectionLost {
            generation: state.generation,
        });
        Some(state.generation)
    }

    fn reconnect_attempt(&self, generation: u64, attempt: u32) -> bool {
        let state = {
            let mut slot = self.slot.lock_unpoisoned();
            if !matches!(slot.state.phase, ChannelPhase::Reconnecting)
                || slot.state.generation != generation
            {
                return false;
            }
            slot.state.reconnect_attempt = Some(attempt);
            slot.state
        };
        self.state_tx.send_replace(state);
        true
    }

    fn reconnect_failed(&self, generation: u64, attempt: u32) {
        let _ = self.event_tx.send(ChannelEvent::ReconnectFailed {
            generation,
            attempt,
        });
    }

    fn install(&self, previous_generation: u64, transport: GenerationTransport) -> Option<u64> {
        let kind = transport.kind();
        let state = {
            let mut slot = self.slot.lock_unpoisoned();
            if !matches!(slot.state.phase, ChannelPhase::Reconnecting)
                || slot.state.generation != previous_generation
            {
                return None;
            }
            slot.state = ChannelState {
                phase: ChannelPhase::Ready,
                generation: previous_generation.saturating_add(1),
                reconnect_attempt: None,
            };
            slot.transport = Some(transport);
            slot.state
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ChannelEvent::Ready {
            generation: state.generation,
            transport: kind,
        });
        Some(state.generation)
    }

    fn close(&self) -> (bool, Option<GenerationTransport>) {
        let (state, transport) = {
            let mut slot = self.slot.lock_unpoisoned();
            if matches!(slot.state.phase, ChannelPhase::Closed) {
                return (false, None);
            }
            let transport = slot.transport.take();
            slot.state.phase = ChannelPhase::Closed;
            slot.state.reconnect_attempt = None;
            (slot.state, transport)
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ChannelEvent::Closed {
            generation: state.generation,
        });
        (true, transport)
    }
}

async fn supervise(
    dialer: Dialer,
    config: ChannelConfig,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
    mut transport: GenerationTransport,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            () = transport.closed() => {}
        }

        let Some(generation) = shared.begin_reconnect() else {
            return;
        };
        let mut attempt = 1;

        loop {
            if !shared.reconnect_attempt(generation, attempt) {
                return;
            }
            let delay = config.reconnect_backoff.delay(attempt);
            tokio::select! {
                biased;
                _ = shutdown.changed() => return,
                () = tokio::time::sleep(delay) => {}
            }
            let _ = shared.event_tx.send(ChannelEvent::ReconnectAttempt {
                generation,
                attempt,
            });

            let connected = tokio::select! {
                biased;
                _ = shutdown.changed() => return,
                connected = dialer.dial() => connected,
            };
            if let Ok(new_transport) = connected {
                if shared.install(generation, new_transport.clone()).is_none() {
                    new_transport.close(CLOSE_REASON);
                    return;
                }
                transport = new_transport;
                break;
            }
            shared.reconnect_failed(generation, attempt);
            attempt = attempt.saturating_add(1);
        }
    }
}

fn unavailable(message: &'static str) -> Error {
    Error::from(Status::unavailable(message))
}

#[cfg(test)]
mod tests {
    use super::{ExponentialBackoff, ReconnectBackoff};
    use std::time::Duration;

    #[test]
    fn exponential_backoff_is_bounded_and_reproducible() {
        let backoff = ExponentialBackoff::new(Duration::from_millis(10), Duration::from_millis(50))
            .with_jitter_ratio(0.25)
            .with_seed(7);
        let same = backoff.clone();

        for attempt in 1..=64 {
            assert_eq!(backoff.delay(attempt), same.delay(attempt));
            assert!(backoff.delay(attempt) <= Duration::from_millis(50));
        }
    }

    #[test]
    fn exponential_backoff_jitters_in_both_directions() {
        let base = Duration::from_secs(1);
        let mut below = false;
        let mut above = false;

        for seed in 0..64 {
            let delay = ExponentialBackoff::new(base, Duration::from_secs(2))
                .with_jitter_ratio(0.25)
                .with_seed(seed)
                .delay(1);
            below |= delay < base;
            above |= delay > base;
        }

        assert!(below, "expected at least one delay below the base");
        assert!(above, "expected at least one delay above the base");
    }

    #[test]
    fn exponential_backoff_treats_nan_jitter_as_disabled() {
        let base = Duration::from_millis(10);
        let delay = ExponentialBackoff::new(base, Duration::from_secs(1))
            .with_jitter_ratio(f64::NAN)
            .delay(1);

        assert_eq!(delay, base);
    }

    #[test]
    fn exponential_backoff_jitter_saturates_at_duration_max() {
        for seed in 0..64 {
            let delay = ExponentialBackoff::new(Duration::MAX, Duration::MAX)
                .with_jitter_ratio(1.0)
                .with_seed(seed)
                .delay(1);
            assert!(delay <= Duration::MAX);
        }
    }
}
