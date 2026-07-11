use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use super::Client;
use crate::client::RpcTransport;
use crate::{BoxMessageStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

const EVENT_CAPACITY: usize = 32;
const CLOSE_REASON: &[u8] = b"managed client closed";
static NEXT_BACKOFF_SEED: AtomicU64 = AtomicU64::new(0x6a09_e667_f3bc_c909);

/// Supplies the delay before a managed client makes a reconnect attempt.
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

/// Configuration for a managed native-QUIC client.
#[derive(Clone)]
pub struct ManagedClientConfig {
    max_frame_size: usize,
    reconnect_backoff: Arc<dyn ReconnectBackoff>,
}

impl ManagedClientConfig {
    /// Creates the default managed-client configuration.
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

    /// Replaces the reconnect delay strategy.
    #[must_use]
    pub fn with_reconnect_backoff(mut self, reconnect_backoff: impl ReconnectBackoff) -> Self {
        self.reconnect_backoff = Arc::new(reconnect_backoff);
        self
    }
}

impl Default for ManagedClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// High-level lifecycle phase of a managed client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedClientPhase {
    Ready,
    Reconnecting,
    Closed,
}

/// Latest observable managed-client lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedClientState {
    phase: ManagedClientPhase,
    generation: u64,
    reconnect_attempt: Option<u32>,
}

impl ManagedClientState {
    /// Returns the current lifecycle phase.
    #[must_use]
    pub const fn phase(self) -> ManagedClientPhase {
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
        matches!(self.phase, ManagedClientPhase::Ready)
    }
}

/// Metrics-friendly managed-client lifecycle event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedClientEvent {
    ConnectionLost { generation: u64 },
    ReconnectAttempt { generation: u64, attempt: u32 },
    ReconnectFailed { generation: u64, attempt: u32 },
    Ready { generation: u64 },
    Closed { generation: u64 },
}

/// A reconnecting native-QUIC `TrevRPC` transport.
///
/// Each RPC snapshots exactly one established connection generation and is delegated once to the
/// low-level [`Client`]. RPCs are never replayed. Calls started before connection loss receive the
/// normal Quinn stream/connection error, while calls made during reconnection fail immediately
/// with `Unavailable`.
///
/// The client retains and reuses one [`quinn::Endpoint`] for every redial, preserving the rustls
/// TLS session-resumption cache. It deliberately awaits the full Quinn handshake and never calls
/// `Connecting::into_0rtt`; application data is therefore never sent as 0-RTT data.
#[derive(Clone)]
pub struct ManagedClient {
    inner: Arc<Inner>,
}

struct Inner {
    endpoint: quinn::Endpoint,
    remote_addr: SocketAddr,
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
}

struct Shared {
    slot: Mutex<Slot>,
    state_tx: watch::Sender<ManagedClientState>,
    event_tx: broadcast::Sender<ManagedClientEvent>,
}

struct Slot {
    state: ManagedClientState,
    client: Option<Client>,
}

impl ManagedClient {
    /// Establishes the initial connection and starts its reconnect supervisor.
    pub async fn connect(
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        Self::connect_with_config(
            endpoint,
            remote_addr,
            server_name,
            ManagedClientConfig::new(),
        )
        .await
    }

    /// Establishes the initial connection with explicit managed-client configuration.
    pub async fn connect_with_config(
        endpoint: quinn::Endpoint,
        remote_addr: SocketAddr,
        server_name: impl Into<String>,
        config: ManagedClientConfig,
    ) -> Result<Self> {
        let server_name = server_name.into();
        let connection = endpoint
            .connect(remote_addr, &server_name)
            .map_err(Error::transport)?
            .await
            .map_err(Error::transport)?;
        let client = Client::new(connection.clone()).with_max_frame_size(config.max_frame_size);
        let initial_state = ManagedClientState {
            phase: ManagedClientPhase::Ready,
            generation: 1,
            reconnect_attempt: None,
        };
        let (state_tx, _) = watch::channel(initial_state);
        let (event_tx, _) = broadcast::channel(EVENT_CAPACITY);
        let shared = Arc::new(Shared {
            slot: Mutex::new(Slot {
                state: initial_state,
                client: Some(client),
            }),
            state_tx,
            event_tx,
        });
        let (shutdown, shutdown_rx) = watch::channel(false);

        tokio::spawn(supervise(
            endpoint.clone(),
            remote_addr,
            server_name,
            config,
            Arc::clone(&shared),
            shutdown_rx,
            connection,
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                endpoint,
                remote_addr,
                shared,
                shutdown,
            }),
        })
    }

    /// Returns the remote address used for redials.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_addr
    }

    /// Returns the endpoint's current local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.endpoint.local_addr()
    }

    /// Returns the latest lifecycle state.
    #[must_use]
    pub fn state(&self) -> ManagedClientState {
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
                ManagedClientPhase::Ready => return Ok(state.generation),
                ManagedClientPhase::Closed => {
                    return Err(unavailable("managed QUIC client is closed"));
                }
                ManagedClientPhase::Reconnecting => {}
            }
            states
                .changed()
                .await
                .map_err(|_| unavailable("managed QUIC client lifecycle observation stopped"))?;
        }
    }

    /// Observes the latest state. The initial value is the state at subscription time.
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ManagedClientState> {
        self.inner.shared.state_tx.subscribe()
    }

    /// Observes lifecycle transitions suitable for counters and structured telemetry.
    ///
    /// A lagging receiver reports [`broadcast::error::RecvError::Lagged`]; callers should use
    /// [`Self::state`] to resynchronize.
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<ManagedClientEvent> {
        self.inner.shared.event_tx.subscribe()
    }

    /// Switches the UDP socket used by this endpoint.
    ///
    /// Quinn rebinding is endpoint-wide: it affects this managed client, every active connection,
    /// and any other user of a clone of the same endpoint. A successful rebind does not itself
    /// change the managed generation. If migration fails and the connection later dies, the normal
    /// reconnect lifecycle establishes the next generation.
    pub fn rebind(&self, socket: UdpSocket) -> io::Result<()> {
        self.inner.endpoint.rebind(socket)
    }

    /// Stops reconnecting and closes this managed client's current connection.
    ///
    /// This does not close the endpoint itself because [`quinn::Endpoint::close`] would also close
    /// unrelated connections sharing the endpoint.
    pub fn close(&self) {
        let (closed, connection) = self.inner.shared.close();
        if closed {
            let _ = self.inner.shutdown.send(true);
            if let Some(connection) = connection {
                connection.close(0_u32.into(), CLOSE_REASON);
            }
        }
    }

    fn snapshot(&self) -> Result<Client> {
        self.inner.shared.snapshot()
    }
}

#[crate::async_trait]
impl RpcTransport for ManagedClient {
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

impl Shared {
    fn state(&self) -> ManagedClientState {
        self.slot
            .lock()
            .expect("managed client lock poisoned")
            .state
    }

    fn snapshot(&self) -> Result<Client> {
        let slot = self.slot.lock().expect("managed client lock poisoned");
        match slot.state.phase {
            ManagedClientPhase::Ready => slot
                .client
                .clone()
                .ok_or_else(|| unavailable("managed QUIC client has no ready connection")),
            ManagedClientPhase::Reconnecting => {
                Err(unavailable("managed QUIC client is reconnecting"))
            }
            ManagedClientPhase::Closed => Err(unavailable("managed QUIC client is closed")),
        }
    }

    fn begin_reconnect(&self) -> Option<u64> {
        let state = {
            let mut slot = self.slot.lock().expect("managed client lock poisoned");
            if matches!(slot.state.phase, ManagedClientPhase::Closed) {
                return None;
            }
            slot.client = None;
            slot.state.phase = ManagedClientPhase::Reconnecting;
            slot.state.reconnect_attempt = Some(1);
            slot.state
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ManagedClientEvent::ConnectionLost {
            generation: state.generation,
        });
        Some(state.generation)
    }

    fn reconnect_attempt(&self, generation: u64, attempt: u32) -> bool {
        let state = {
            let mut slot = self.slot.lock().expect("managed client lock poisoned");
            if !matches!(slot.state.phase, ManagedClientPhase::Reconnecting)
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
        let _ = self.event_tx.send(ManagedClientEvent::ReconnectFailed {
            generation,
            attempt,
        });
    }

    fn install(&self, previous_generation: u64, client: Client) -> Option<u64> {
        let state = {
            let mut slot = self.slot.lock().expect("managed client lock poisoned");
            if !matches!(slot.state.phase, ManagedClientPhase::Reconnecting)
                || slot.state.generation != previous_generation
            {
                return None;
            }
            slot.state = ManagedClientState {
                phase: ManagedClientPhase::Ready,
                generation: previous_generation.saturating_add(1),
                reconnect_attempt: None,
            };
            slot.client = Some(client);
            slot.state
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ManagedClientEvent::Ready {
            generation: state.generation,
        });
        Some(state.generation)
    }

    fn close(&self) -> (bool, Option<quinn::Connection>) {
        let (state, connection) = {
            let mut slot = self.slot.lock().expect("managed client lock poisoned");
            if matches!(slot.state.phase, ManagedClientPhase::Closed) {
                return (false, None);
            }
            let connection = slot.client.take().map(|client| client.connection().clone());
            slot.state.phase = ManagedClientPhase::Closed;
            slot.state.reconnect_attempt = None;
            (slot.state, connection)
        };
        self.state_tx.send_replace(state);
        let _ = self.event_tx.send(ManagedClientEvent::Closed {
            generation: state.generation,
        });
        (true, connection)
    }
}

async fn supervise(
    endpoint: quinn::Endpoint,
    remote_addr: SocketAddr,
    server_name: String,
    config: ManagedClientConfig,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
    mut connection: quinn::Connection,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            _ = connection.closed() => {}
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
            let _ = shared.event_tx.send(ManagedClientEvent::ReconnectAttempt {
                generation,
                attempt,
            });

            let Ok(connecting) = endpoint.connect(remote_addr, &server_name) else {
                shared.reconnect_failed(generation, attempt);
                attempt = attempt.saturating_add(1);
                continue;
            };
            let connected = tokio::select! {
                biased;
                _ = shutdown.changed() => return,
                connected = connecting => connected,
            };
            if let Ok(new_connection) = connected {
                let client =
                    Client::new(new_connection.clone()).with_max_frame_size(config.max_frame_size);
                if shared.install(generation, client).is_none() {
                    new_connection.close(0_u32.into(), CLOSE_REASON);
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
