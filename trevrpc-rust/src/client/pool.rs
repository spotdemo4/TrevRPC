//! Spreading a channel's QUIC calls over several connections to its server.
//!
//! Quinn guards each connection with one lock, which every stream operation and the connection's
//! packet processing take. Calls that share a connection from many threads wait on each other:
//! on 8 threads, 64 concurrent unary calls ran 2.5 times as fast over four connections as over
//! one. Contention follows how fast calls start, not how many are in flight: on a slow link many
//! calls wait a round trip each without touching the lock. Large messages hit a second limit: one
//! connection's packet processing runs on one core, which carried about 500 MB/s on the bench host.
//!
//! A pool starts with its generation's connection. While the channel starts calls faster than its
//! call rate, or moves bytes faster than its byte rate, it places each call on the connection with
//! the fewest in flight, and dials another, up to its limit, when every connection carries one of
//! those rates and two calls. Below both, every call goes to the generation's connection, so the
//! ones it added fall idle and close; they also close when the generation ends, as soon as their
//! last call finishes. A pool with a minimum above one keeps that many open from the start, never
//! idle-closes them, and spreads every call over its connections regardless of the rates.
//!
//! Streaming calls stay open, so the rates miss them once they have started, and a call never moves
//! to another connection: 64 bidirectional streams opened together all stayed on the first, at
//! half the round trips they made over four. A connection whose streaming calls in flight could
//! each make a round trip faster than the call rate allows, at its round trip time, is therefore
//! crowded too: the pool spreads and grows as it does at the rates, and a streaming call that finds
//! every connection crowded waits up to [`CROWDED_DIAL_WAIT`] for the ones it dials. On a slow link
//! many streams fit in a round trip without crowding it (64 over 20 ms make at most 3,200 round
//! trips a second), so such a channel keeps one connection, and one congestion controller, as
//! before. Unary calls end, so the call rate measures them as they are: eight that a server holds
//! for 100 ms each never crowd a connection, however short its round trip.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::{Notify, watch};

use super::race::Dialer;
use super::{CallInfo, QuicTransport, RpcTransport, StreamingRpcTransport};
use crate::sync::RwLockExt as _;
use crate::{BoxStream, ResponseEnvelope, ResponseFrame, Result, RpcRequest};

/// A new call dials another connection only if it finds at least this many calls on every one, so
/// that the calls can use it.
const BUSY_CALLS: usize = 2;

/// How many calls per second, per connection, the pool spreads and grows at, by default. On the
/// bench host (8 threads) four workers on one connection already ran at 23k unary calls per second
/// against 35k on two, while 64 workers over a 20 ms round trip start only 3,200.
pub const DEFAULT_CALL_RATE: u64 = 5_000;

/// How many bytes per second, per connection, the pool spreads and grows at, by default: a fifth of
/// what one connection carried on the bench host, and eight times what a 100 Mbit/s link can.
pub const DEFAULT_BYTE_RATE: u64 = 100_000_000;

/// The shortest window the rates are measured over.
const RATE_WINDOW: Duration = Duration::from_millis(100);

/// How long a streaming call that finds every connection crowded waits for the connection the pool
/// dials before it takes the least crowded one. Only a short round trip crowds a connection, so the
/// handshake takes a few milliseconds.
const CROWDED_DIAL_WAIT: Duration = Duration::from_millis(100);

/// How long a connection the pool added may go unused before it closes, by default.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the pool waits after a failed dial before it dials again.
const DIAL_RETRY: Duration = Duration::from_secs(5);

const IDLE_REASON: &[u8] = b"idle";

/// The default limit on a channel's QUIC connections: half the available parallelism, from one to
/// four. Contention needs threads to contend, and four connections already reach `QMux`'s
/// throughput on 8 threads.
pub fn default_max_connections() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |threads| threads.get() / 2)
        .clamp(1, 4)
}

/// The QUIC connections of one channel generation.
#[derive(Clone)]
pub struct QuicPool {
    inner: Arc<Pool>,
}

struct Pool {
    /// The generation's own connection, which the pool never closes.
    primary: Arc<Member>,
    /// The connections the pool added.
    added: RwLock<Vec<Arc<Member>>>,
    dialer: Dialer,
    max: usize,
    /// The connections the pool keeps open and spreads every call over.
    min: usize,
    idle_timeout: Duration,
    /// Calls per second, per connection, to spread and grow at.
    call_rate: u64,
    /// Bytes per second, per connection, to spread and grow at.
    byte_rate: u64,
    load: Load,
    /// Whether a dial, or the pause after a failed one, is under way.
    dialing: AtomicBool,
    /// Whether a dial itself is under way, which crowded streaming calls wait for.
    dial_in_flight: AtomicBool,
    /// Notified when a dial ends, whether it added a connection or failed.
    dialed: Notify,
    /// Becomes `true` when the pool is dropped, which drains the added connections.
    dropped: watch::Sender<bool>,
}

struct Member {
    transport: QuicTransport,
    /// Calls in flight on the connection.
    calls: AtomicUsize,
    /// Streaming calls in flight on the connection, which crowd it.
    streams: AtomicUsize,
    /// Whether a call started since the last idle check.
    used: AtomicBool,
    /// Notified when the last call in flight ends.
    idle: Notify,
    /// The bytes the connection had sent and received when the pool last looked.
    bytes: AtomicU64,
    /// The connection's smoothed round trip in microseconds, as of when the pool last looked.
    rtt_micros: AtomicU64,
    /// One of the pool's minimum, which never closes for being idle.
    pinned: bool,
}

impl Member {
    fn new(transport: QuicTransport, pinned: bool) -> Arc<Self> {
        let bytes = AtomicU64::new(udp_bytes(transport.connection()));
        let rtt_micros = AtomicU64::new(rtt_micros(transport.connection()));
        Arc::new(Self {
            transport,
            calls: AtomicUsize::new(0),
            streams: AtomicUsize::new(0),
            used: AtomicBool::new(false),
            idle: Notify::new(),
            bytes,
            rtt_micros,
            pinned,
        })
    }

    const fn connection(&self) -> &quinn::Connection {
        self.transport.connection()
    }

    /// The bytes the connection moved since this was last called, refreshing its round trip too.
    fn moved(&self) -> u64 {
        self.rtt_micros
            .store(rtt_micros(self.connection()), Ordering::Relaxed);
        let now = udp_bytes(self.connection());
        now.saturating_sub(self.bytes.swap(now, Ordering::Relaxed))
    }

    /// Whether the connection's streaming calls in flight could each make a round trip faster
    /// than `call_rate` allows, at its last measured round trip time.
    fn crowded(&self, call_rate: u64) -> bool {
        crowded(
            self.streams.load(Ordering::Relaxed),
            self.rtt_micros.load(Ordering::Relaxed),
            call_rate,
        )
    }
}

/// Whether `calls` in flight, each making one round trip of `rtt_micros` at a time, could make
/// round trips faster than `call_rate` a second, with at least [`BUSY_CALLS`] of them.
fn crowded(calls: usize, rtt_micros: u64, call_rate: u64) -> bool {
    let rtt_micros = rtt_micros.max(1);
    calls >= BUSY_CALLS
        && u64::try_from(calls)
            .unwrap_or(u64::MAX)
            .saturating_mul(1_000_000)
            >= call_rate.saturating_mul(rtt_micros)
}

/// A connection's smoothed round trip in microseconds. Reading it takes the connection's lock,
/// so the pool does it once per window, with the bytes.
fn rtt_micros(connection: &quinn::Connection) -> u64 {
    u64::try_from(connection.rtt().as_micros()).unwrap_or(u64::MAX)
}

/// The UDP bytes a connection has sent and received. Reading them takes the connection's lock,
/// which is why the pool does it once per window.
fn udp_bytes(connection: &quinn::Connection) -> u64 {
    let stats = connection.stats();
    stats.udp_tx.bytes.saturating_add(stats.udp_rx.bytes)
}

/// Counts a call, and whether it is streaming, against its connection until it is dropped.
struct CallGuard {
    member: Arc<Member>,
    streaming: bool,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.streaming {
            self.member.streams.fetch_sub(1, Ordering::Relaxed);
        }
        if self.member.calls.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.member.idle.notify_waiters();
        }
    }
}

impl QuicPool {
    /// Creates the pool of a generation whose connection is `primary`, with `others` already
    /// dialed toward its minimum. `dialer` dials the connections it adds, up to its
    /// `max_quic_connections` in all.
    pub(crate) fn new(
        primary: QuicTransport,
        dialer: Dialer,
        others: impl IntoIterator<Item = QuicTransport>,
    ) -> Self {
        let max = dialer.max_quic_connections.max(1);
        let pool = Self {
            inner: Arc::new(Pool {
                primary: Member::new(primary, true),
                added: RwLock::new(Vec::new()),
                max,
                min: dialer.min_quic_connections.clamp(1, max),
                // `tokio::time::interval` rejects a zero period.
                idle_timeout: dialer.quic_idle_timeout.max(Duration::from_millis(1)),
                call_rate: dialer.quic_call_rate.max(1),
                byte_rate: dialer.quic_byte_rate.max(1),
                load: Load::new(),
                dialer,
                dialing: AtomicBool::new(false),
                dial_in_flight: AtomicBool::new(false),
                dialed: Notify::new(),
                dropped: watch::Sender::new(false),
            }),
        };
        for transport in others {
            Pool::add(&pool.inner, transport);
        }
        pool
    }

    /// Returns the generation's own connection.
    pub(crate) fn primary(&self) -> &quinn::Connection {
        self.inner.primary.connection()
    }

    /// Returns every open connection, the generation's own first.
    pub(crate) fn connections(&self) -> Vec<quinn::Connection> {
        let added = self.inner.added.read_unpoisoned();
        std::iter::once(&self.inner.primary)
            .chain(added.iter())
            .map(|member| member.connection().clone())
            .collect()
    }

    /// Closes every connection immediately.
    pub(crate) fn close(&self, reason: &[u8]) {
        for connection in self.connections() {
            connection.close(0_u32.into(), reason);
        }
    }

    /// Picks the connection for a new call: the one with the fewest calls in flight while the
    /// pool keeps more than one connection, reaches its call or byte rate, or finds its own
    /// connection crowded, and the generation's own otherwise. Dials another one when the pool is
    /// below its minimum, or when every connection carries one of those rates and is busy, or is
    /// crowded. A `streaming` call that finds every connection crowded waits for such dials, up to
    /// [`CROWDED_DIAL_WAIT`] in all, since it would stay where it starts.
    async fn pick(&self, streaming: bool) -> (QuicTransport, CallGuard) {
        let pool = &self.inner;
        let rates = pool.load.record(|| pool.moved());
        // One allowance for every dial the call waits for: a burst of streams may need several.
        let started = Instant::now();
        loop {
            let left = CROWDED_DIAL_WAIT.saturating_sub(started.elapsed());
            if let Some(picked) = self.try_pick(rates, streaming, streaming && !left.is_zero()) {
                return picked;
            }
            // Only a call about to wait registers with `dialed`, whose waiter list every call
            // would otherwise lock. It looks at the dial again once registered: the dial clears
            // `dial_in_flight` before it notifies, so one that ended in between is not missed.
            let dialed = pool.dialed.notified();
            tokio::pin!(dialed);
            dialed.as_mut().enable();
            if pool.dial_in_flight.load(Ordering::Acquire) {
                let _ = tokio::time::timeout(left, dialed).await;
            }
        }
    }

    /// Picks as [`QuicPool::pick`] describes, or returns `None` when a call that `may_wait` should
    /// wait for the connection being dialed instead.
    fn try_pick(
        &self,
        rates: Rates,
        streaming: bool,
        may_wait: bool,
    ) -> Option<(QuicTransport, CallGuard)> {
        let pool = &self.inner;
        let (member, least, open, crowded) = {
            let added = pool.added.read_unpoisoned();
            let primary_calls = pool.primary.calls.load(Ordering::Relaxed);
            let spread =
                pool.min > 1 || pool.reaches(rates, 1) || pool.primary.crowded(pool.call_rate);
            let mut member = &pool.primary;
            let mut least = primary_calls;
            for candidate in added.iter().filter(|_| spread) {
                let calls = candidate.calls.load(Ordering::Relaxed);
                if calls < least {
                    member = candidate;
                    least = calls;
                }
            }
            let open = added.len().saturating_add(1);
            // A streaming call finds even the connection with the fewest calls crowded.
            let crowded = streaming && spread && open < pool.max && member.crowded(pool.call_rate);
            if crowded && may_wait {
                self.grow();
                if pool.dial_in_flight.load(Ordering::Acquire) {
                    return None;
                }
            }
            // Counted under the lock, so an idle connection is never retired under a new call.
            member.calls.fetch_add(1, Ordering::AcqRel);
            if streaming {
                member.streams.fetch_add(1, Ordering::Relaxed);
            }
            member.used.store(true, Ordering::Relaxed);
            let member = Arc::clone(member);
            drop(added);
            (member, least, open, crowded)
        };
        let busy = least >= BUSY_CALLS && open < pool.max && pool.reaches(rates, open);
        if open < pool.min || busy || crowded {
            self.grow();
        }
        let transport = member.transport.clone();
        Some((transport, CallGuard { member, streaming }))
    }

    /// Dials another connection in the background, unless a dial is already under way.
    fn grow(&self) {
        if self.inner.dialing.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.dial_in_flight.store(true, Ordering::Release);
        let pool_ref = Arc::downgrade(&self.inner);
        let dialer = self.inner.dialer.clone();
        tokio::spawn(async move {
            let pool = pool_ref.clone();
            let result = dialer.dial_quic_transport().await;
            let failed = result.is_err();
            match (result, pool.upgrade()) {
                (Ok(transport), Some(pool)) => Pool::add(&pool, transport),
                (Ok(transport), None) => transport.connection().close(0_u32.into(), IDLE_REASON),
                (Err(_), _) => {}
            }
            let Some(pool) = pool.upgrade() else { return };
            pool.dial_in_flight.store(false, Ordering::Release);
            if failed {
                pool.dialed.notify_waiters();
                drop(pool);
                tokio::time::sleep(DIAL_RETRY).await;
                if let Some(pool) = pool_ref.upgrade() {
                    pool.dialing.store(false, Ordering::Release);
                }
            } else {
                // Cleared before the calls waiting for this dial look again, so that they can
                // dial the next connection if they find every one crowded still.
                pool.dialing.store(false, Ordering::Release);
                pool.dialed.notify_waiters();
            }
        });
    }
}

/// How fast a pool starts calls and moves bytes, measured over windows of at least
/// [`RATE_WINDOW`]. Every call counts itself, and the call that finds the window over turns it
/// into rates.
struct Load {
    epoch: Instant,
    /// When the current window started, in milliseconds since `epoch`.
    window_start: AtomicU64,
    /// The calls started in the current window.
    window_calls: AtomicU64,
    /// Over the last window that ended: calls started per second.
    calls: AtomicU64,
    /// Over the last window that ended: bytes sent and received per second.
    bytes: AtomicU64,
}

/// The rates of the last window, for the whole pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Rates {
    calls: u64,
    bytes: u64,
}

impl Load {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            window_start: AtomicU64::new(0),
            window_calls: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    /// Counts a call started now and returns the rates. `moved` returns the bytes the pool's
    /// connections moved since it was last asked; it runs once per window.
    fn record(&self, moved: impl FnOnce() -> u64) -> Rates {
        self.record_at(self.epoch.elapsed(), moved)
    }

    fn record_at(&self, now: Duration, moved: impl FnOnce() -> u64) -> Rates {
        self.window_calls.fetch_add(1, Ordering::Relaxed);
        let now = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
        let start = self.window_start.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(start);
        // A window that a pause stretched out yields low rates, which the next window corrects.
        if u128::from(elapsed) >= RATE_WINDOW.as_millis()
            && self
                .window_start
                .compare_exchange(start, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let per_second =
                |count: u64| count.saturating_mul(1000).checked_div(elapsed).unwrap_or(0);
            let calls = self.window_calls.swap(0, Ordering::Relaxed);
            self.calls.store(per_second(calls), Ordering::Relaxed);
            self.bytes.store(per_second(moved()), Ordering::Relaxed);
        }
        Rates {
            calls: self.calls.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

impl Pool {
    /// Whether `rates` reach the call or byte rate for each of `connections`.
    fn reaches(&self, rates: Rates, connections: usize) -> bool {
        let connections = u64::try_from(connections).unwrap_or(u64::MAX);
        rates.calls >= self.call_rate.saturating_mul(connections)
            || rates.bytes >= self.byte_rate.saturating_mul(connections)
    }

    /// The bytes every connection moved since this was last called.
    fn moved(&self) -> u64 {
        let added = self.added.read_unpoisoned();
        std::iter::once(&self.primary)
            .chain(added.iter())
            .map(|member| member.moved())
            .fold(0, u64::saturating_add)
    }

    fn add(pool: &Arc<Self>, transport: QuicTransport) {
        let member = {
            let mut added = pool.added.write_unpoisoned();
            let open = added.len().saturating_add(1);
            if open >= pool.max {
                transport.connection().close(0_u32.into(), IDLE_REASON);
                return;
            }
            let member = Member::new(transport, open < pool.min);
            added.push(Arc::clone(&member));
            member
        };
        tokio::spawn(watch_added(
            Arc::downgrade(pool),
            pool.idle_timeout,
            pool.primary.connection().clone(),
            pool.dropped.subscribe(),
            member,
        ));
    }

    /// Removes `member` if it has no call in flight, and returns whether it did.
    fn retire_if_idle(&self, member: &Arc<Member>) -> bool {
        let mut added = self.added.write_unpoisoned();
        if member.calls.load(Ordering::Acquire) != 0 {
            return false;
        }
        added.retain(|other| !Arc::ptr_eq(other, member));
        true
    }

    fn remove(&self, member: &Arc<Member>) {
        self.added
            .write_unpoisoned()
            .retain(|other| !Arc::ptr_eq(other, member));
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.dropped.send_replace(true);
    }
}

/// Supervises a connection the pool added: forgets it once it closes, closes it once it sits
/// idle, and drains it once the generation ends.
async fn watch_added(
    pool: Weak<Pool>,
    idle_timeout: Duration,
    primary: quinn::Connection,
    mut dropped: watch::Receiver<bool>,
    member: Arc<Member>,
) {
    let mut idle_check = tokio::time::interval(idle_timeout);
    // An interval's first tick completes at once.
    idle_check.tick().await;
    loop {
        tokio::select! {
            _ = member.connection().closed() => {
                if let Some(pool) = pool.upgrade() {
                    pool.remove(&member);
                }
                return;
            }
            _ = idle_check.tick() => {
                let Some(pool) = pool.upgrade() else { break };
                let used = member.used.swap(false, Ordering::Relaxed);
                if !member.pinned && !used && pool.retire_if_idle(&member) {
                    member.connection().close(0_u32.into(), IDLE_REASON);
                    return;
                }
            }
            _ = primary.closed() => break,
            _ = dropped.wait_for(|dropped| *dropped) => break,
        }
    }
    // The generation ended: no new call picks this connection, so close it after its last one.
    if let Some(pool) = pool.upgrade() {
        pool.remove(&member);
    }
    drop(primary);
    loop {
        let idle = member.idle.notified();
        tokio::pin!(idle);
        idle.as_mut().enable();
        if member.calls.load(Ordering::Acquire) == 0 {
            break;
        }
        tokio::select! {
            () = idle => {}
            _ = member.connection().closed() => return,
        }
    }
    member.connection().close(0_u32.into(), IDLE_REASON);
}

/// A response stream that keeps its call counted until it is dropped.
struct Counted {
    inner: BoxStream<ResponseFrame>,
    _guard: CallGuard,
}

impl Stream for Counted {
    type Item = Result<ResponseFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

#[crate::async_trait]
impl RpcTransport for QuicPool {
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        self.call_with_info(request, body, CallInfo::default())
            .await
    }

    async fn call_with_info(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        info: CallInfo,
    ) -> Result<ResponseEnvelope<Bytes>> {
        let (transport, _guard) = self.pick(false).await;
        transport.call_with_info(request, body, info).await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for QuicPool {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.streaming_call_with_info(request, request_body, CallInfo::default())
            .await
    }

    async fn streaming_call_with_info(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        let (transport, guard) = self.pick(true).await;
        let inner = transport
            .streaming_call_with_info(request, request_body, info)
            .await?;
        Ok(Box::pin(Counted {
            inner,
            _guard: guard,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connection_is_crowded_by_calls_that_could_outrun_the_call_rate() {
        // On loopback two calls in flight could already make 10,000 round trips a second.
        assert!(crowded(2, 200, DEFAULT_CALL_RATE));
        assert!(!crowded(1, 1, DEFAULT_CALL_RATE), "one call never crowds");
        // Over a 20 ms round trip, 64 calls make at most 3,200; 100 make 5,000.
        assert!(!crowded(64, 20_000, DEFAULT_CALL_RATE));
        assert!(crowded(100, 20_000, DEFAULT_CALL_RATE));
        // An unmeasured round trip counts as the shortest.
        assert!(crowded(2, 0, DEFAULT_CALL_RATE));
        // The arithmetic saturates instead of overflowing.
        assert!(crowded(usize::MAX, u64::MAX, 1));
    }

    #[test]
    fn rates_are_measured_over_windows() {
        let load = Load::new();
        let ms = Duration::from_millis;
        let none = Rates { calls: 0, bytes: 0 };
        // No window has ended yet, so bytes are not read: they would show in the rates.
        for i in 0..50 {
            assert_eq!(load.record_at(ms(i), || 1 << 40), none);
        }
        // The 51st call ends a 100 ms window of 51 calls and 2 MB.
        assert_eq!(
            load.record_at(ms(100), || 2_000_000),
            Rates {
                calls: 510,
                bytes: 20_000_000
            }
        );
        // A window stretched by a pause reads low.
        assert_eq!(load.record_at(ms(1100), || 0), Rates { calls: 1, bytes: 0 });
        for i in 0..999 {
            load.record_at(ms(1100 + i / 10), || 0);
        }
        assert_eq!(load.record_at(ms(1200), || 0).calls, 10_000);
    }
}
