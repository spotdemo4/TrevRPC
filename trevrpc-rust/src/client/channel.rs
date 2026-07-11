use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use crate::advanced::RawQuinnTransport;
#[cfg(feature = "webtransport")]
use crate::advanced::RawWebTransport;
use crate::client::RpcTransport;
use crate::{BoxMessageStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

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
    pub fn with_jitter_ratio(mut self, jitter_ratio: f64) -> Self {
        self.jitter_ratio = jitter_ratio.clamp(0.0, 1.0);
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
        #[allow(clippy::cast_precision_loss)]
        let unit = random as f64 / u64::MAX as f64;
        let jitter = (unit * 2.0 - 1.0) * self.jitter_ratio;
        let delay = if jitter.is_sign_negative() {
            base.saturating_sub(base.mul_f64(-jitter))
        } else {
            base.saturating_add(base.mul_f64(jitter))
        };
        delay.min(self.max_delay)
    }
}

fn splitmix64(mut value: u64) -> u64 {
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
}

impl ChannelConfig {
    /// Creates the default channel configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_frame_size: crate::framing::DEFAULT_MAX_FRAME_SIZE,
            reconnect_backoff: Arc::new(ExponentialBackoff::default()),
        }
    }

    /// Sets the maximum `TrevRPC` frame size for every connection generation.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    pub(crate) fn with_reconnect_backoff_advanced(
        mut self,
        reconnect_backoff: impl ReconnectBackoff,
    ) -> Self {
        self.reconnect_backoff = Arc::new(reconnect_backoff);
        self
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
    ConnectionLost { generation: u64 },
    ReconnectAttempt { generation: u64, attempt: u32 },
    ReconnectFailed { generation: u64, attempt: u32 },
    Ready { generation: u64 },
    Closed { generation: u64 },
}

/// A reconnecting `TrevRPC` application channel for generated clients.
///
/// Each RPC snapshots exactly one established connection generation and is delegated once to the
/// raw transport. RPCs are never retried or replayed. Calls started before connection loss receive
/// the transport's normal stream or connection error, while calls made during reconnection fail
/// immediately with `Unavailable`.
///
/// Native QUIC channels retain one [`quinn::Endpoint`] for every redial, preserving the rustls TLS
/// session-resumption cache. All transports await a complete connection handshake before becoming
/// ready, and native QUIC never calls `Connecting::into_0rtt`.
#[derive(Clone)]
pub struct Channel {
    inner: Arc<Inner>,
}

struct Inner {
    backend: Backend,
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
}

#[derive(Clone)]
enum Backend {
    Quinn {
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: String,
    },
    #[cfg(feature = "webtransport")]
    WebTransport {
        client: Arc<web_transport_quinn::Client>,
        request: Arc<web_transport_quinn::proto::ConnectRequest>,
    },
}

struct Shared {
    slot: Mutex<Slot>,
    state_tx: watch::Sender<ChannelState>,
    event_tx: broadcast::Sender<ChannelEvent>,
}

struct Slot {
    state: ChannelState,
    transport: Option<Transport>,
}

#[derive(Clone)]
enum Transport {
    Quinn(RawQuinnTransport),
    #[cfg(feature = "webtransport")]
    WebTransport(RawWebTransport),
}

enum Connection {
    Quinn(quinn::Connection),
    #[cfg(feature = "webtransport")]
    WebTransport(RawWebTransport),
}

impl Channel {
    /// Establishes an initial native QUIC connection and starts its reconnect supervisor.
    ///
    /// The returned future covers the complete initial handshake and may be bounded or cancelled
    /// with the caller's normal future, task, or request-context deadline semantics.
    pub async fn connect(
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        Self::connect_with_config(endpoint, remote_addr, server_name, ChannelConfig::new()).await
    }

    /// Establishes an initial native QUIC connection with explicit channel configuration.
    pub async fn connect_with_config(
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: impl Into<String>,
        config: ChannelConfig,
    ) -> Result<Self> {
        Self::connect_backend(
            Backend::Quinn {
                endpoint,
                remote_addr,
                server_name: server_name.into(),
            },
            config,
        )
        .await
    }

    /// Establishes a WebTransport channel at the conventional `/trevrpc` path.
    ///
    /// `origin` must be an HTTPS origin such as `https://example.com:443`; any supplied path,
    /// query, or fragment is discarded. Use the advanced constructor for custom CONNECT requests.
    #[cfg(feature = "webtransport")]
    pub async fn connect_webtransport(
        client: web_transport_quinn::Client,
        origin: &str,
    ) -> Result<Self> {
        Self::connect_webtransport_with_config(client, origin, ChannelConfig::new()).await
    }

    /// Establishes a WebTransport channel with explicit channel configuration.
    #[cfg(feature = "webtransport")]
    pub async fn connect_webtransport_with_config(
        client: web_transport_quinn::Client,
        origin: &str,
        config: ChannelConfig,
    ) -> Result<Self> {
        let mut url = origin.parse::<url::Url>().map_err(Error::transport)?;
        if url.scheme() != "https" || url.host().is_none() {
            return Err(Error::transport(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WebTransport origin must be an absolute HTTPS URL",
            )));
        }
        url.set_path("/trevrpc");
        url.set_query(None);
        url.set_fragment(None);
        let request = web_transport_quinn::proto::ConnectRequest::new(url);
        Self::connect_webtransport_request(client, request, config).await
    }

    #[cfg(feature = "webtransport")]
    pub(crate) async fn connect_webtransport_request(
        client: web_transport_quinn::Client,
        request: web_transport_quinn::proto::ConnectRequest,
        config: ChannelConfig,
    ) -> Result<Self> {
        Self::connect_backend(
            Backend::WebTransport {
                client: Arc::new(client),
                request: Arc::new(request),
            },
            config,
        )
        .await
    }

    async fn connect_backend(backend: Backend, config: ChannelConfig) -> Result<Self> {
        let (transport, connection) = backend.connect(config.max_frame_size).await?;
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
                transport: Some(transport),
            }),
            state_tx,
            event_tx,
        });
        let (shutdown, shutdown_rx) = watch::channel(false);

        tokio::spawn(supervise(
            backend.clone(),
            config,
            Arc::clone(&shared),
            shutdown_rx,
            connection,
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                backend,
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
        let (closed, connection) = self.inner.shared.close();
        if closed {
            let _ = self.inner.shutdown.send(true);
            if let Some(connection) = connection {
                connection.close();
            }
        }
    }

    fn snapshot(&self) -> Result<Transport> {
        self.inner.shared.snapshot()
    }

    pub(crate) fn advanced_quinn_remote_addr(&self) -> Option<SocketAddr> {
        self.inner.backend.quinn_remote_addr()
    }

    pub(crate) fn advanced_quinn_local_addr(&self) -> std::io::Result<Option<SocketAddr>> {
        self.inner.backend.quinn_local_addr()
    }

    pub(crate) fn advanced_rebind_quinn(&self, socket: UdpSocket) -> std::io::Result<()> {
        self.inner.backend.rebind_quinn(socket)
    }
}

#[crate::async_trait]
impl RpcTransport for Channel {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        self.snapshot()?.call(request).await
    }

    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        self.snapshot()?.streaming_call(request, request_body).await
    }
}

#[crate::async_trait]
impl RpcTransport for Transport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        match self {
            Self::Quinn(transport) => transport.call(request).await,
            #[cfg(feature = "webtransport")]
            Self::WebTransport(transport) => transport.call(request).await,
        }
    }

    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        match self {
            Self::Quinn(transport) => transport.streaming_call(request, request_body).await,
            #[cfg(feature = "webtransport")]
            Self::WebTransport(transport) => transport.streaming_call(request, request_body).await,
        }
    }
}

impl Shared {
    fn state(&self) -> ChannelState {
        self.slot.lock().expect("channel lock poisoned").state
    }

    fn snapshot(&self) -> Result<Transport> {
        let slot = self.slot.lock().expect("channel lock poisoned");
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
            let mut slot = self.slot.lock().expect("channel lock poisoned");
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
            let mut slot = self.slot.lock().expect("channel lock poisoned");
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

    fn install(&self, previous_generation: u64, transport: Transport) -> Option<u64> {
        let state = {
            let mut slot = self.slot.lock().expect("channel lock poisoned");
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
        });
        Some(state.generation)
    }

    fn close(&self) -> (bool, Option<Connection>) {
        let (state, connection) = {
            let mut slot = self.slot.lock().expect("channel lock poisoned");
            if matches!(slot.state.phase, ChannelPhase::Closed) {
                return (false, None);
            }
            let connection = slot
                .transport
                .take()
                .map(|transport| transport.connection());
            slot.state.phase = ChannelPhase::Closed;
            slot.state.reconnect_attempt = None;
            (slot.state, connection)
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ChannelEvent::Closed {
            generation: state.generation,
        });
        (true, connection)
    }
}

async fn supervise(
    backend: Backend,
    config: ChannelConfig,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
    mut connection: Connection,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            () = connection.closed() => {}
        }

        let Some(generation) = shared.begin_reconnect() else {
            return;
        };
        let mut attempt = 1;

        loop {
            if !shared.reconnect_attempt(generation, attempt) {
                return;
            }
            if wait_or_shutdown(config.reconnect_backoff.delay(attempt), &mut shutdown).await {
                return;
            }
            let _ = shared.event_tx.send(ChannelEvent::ReconnectAttempt {
                generation,
                attempt,
            });

            let connected = tokio::select! {
                biased;
                _ = shutdown.changed() => return,
                connected = backend.connect(config.max_frame_size) => connected,
            };
            if let Ok((transport, new_connection)) = connected {
                if shared.install(generation, transport).is_none() {
                    new_connection.close();
                    return;
                }
                connection = new_connection;
                break;
            }
            shared.reconnect_failed(generation, attempt);
            attempt = attempt.saturating_add(1);
        }
    }
}

impl Backend {
    async fn connect(&self, max_frame_size: usize) -> Result<(Transport, Connection)> {
        match self {
            Self::Quinn {
                endpoint,
                remote_addr,
                server_name,
            } => {
                let connection = endpoint
                    .connect(*remote_addr, server_name)
                    .map_err(Error::transport)?
                    .await
                    .map_err(Error::transport)?;
                let transport =
                    RawQuinnTransport::new(connection.clone()).with_max_frame_size(max_frame_size);
                Ok((Transport::Quinn(transport), Connection::Quinn(connection)))
            }
            #[cfg(feature = "webtransport")]
            Self::WebTransport { client, request } => {
                let session = client
                    .connect(request.as_ref().clone())
                    .await
                    .map_err(Error::transport)?;
                let transport = RawWebTransport::new(session).with_max_frame_size(max_frame_size);
                Ok((
                    Transport::WebTransport(transport.clone()),
                    Connection::WebTransport(transport),
                ))
            }
        }
    }

    fn quinn_remote_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Quinn { remote_addr, .. } => Some(*remote_addr),
            #[cfg(feature = "webtransport")]
            Self::WebTransport { .. } => None,
        }
    }

    fn quinn_local_addr(&self) -> std::io::Result<Option<SocketAddr>> {
        match self {
            Self::Quinn { endpoint, .. } => endpoint.local_addr().map(Some),
            #[cfg(feature = "webtransport")]
            Self::WebTransport { .. } => Ok(None),
        }
    }

    fn rebind_quinn(&self, socket: UdpSocket) -> std::io::Result<()> {
        match self {
            Self::Quinn { endpoint, .. } => endpoint.rebind(socket),
            #[cfg(feature = "webtransport")]
            Self::WebTransport { .. } => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "channel does not use a Quinn endpoint",
            )),
        }
    }
}

impl Transport {
    fn connection(&self) -> Connection {
        match self {
            Self::Quinn(transport) => Connection::Quinn(transport.connection().clone()),
            #[cfg(feature = "webtransport")]
            Self::WebTransport(transport) => Connection::WebTransport(transport.clone()),
        }
    }
}

impl Connection {
    async fn closed(&self) {
        match self {
            Self::Quinn(connection) => {
                connection.closed().await;
            }
            #[cfg(feature = "webtransport")]
            Self::WebTransport(transport) => {
                transport.session().closed().await;
            }
        }
    }

    fn close(&self) {
        match self {
            Self::Quinn(connection) => connection.close(0_u32.into(), CLOSE_REASON),
            #[cfg(feature = "webtransport")]
            Self::WebTransport(transport) => transport.session().close(0, CLOSE_REASON),
        }
    }
}

async fn wait_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        biased;
        _ = shutdown.changed() => true,
        () = tokio::time::sleep(delay) => false,
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
