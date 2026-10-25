//! Spreading a channel's QUIC calls over several connections to its server.
//!
//! Quinn guards each connection with one lock, which every stream operation and the connection's
//! packet processing take. Calls that share a connection from many threads wait on each other:
//! on 8 threads, 64 concurrent unary calls ran 2.5 times as fast over four connections as over
//! one. A pool starts with its generation's connection and dials another, up to its limit,
//! whenever a new call finds every connection busy. The connections it added close once they
//! sit idle, and when the generation ends, as soon as their last call finishes.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::{Notify, watch};

use super::race::Dialer;
use super::{CallInfo, QuicTransport, RpcTransport, StreamingRpcTransport};
use crate::sync::RwLockExt as _;
use crate::{BoxStream, ResponseEnvelope, ResponseFrame, Result, RpcRequest};

/// A new call that finds at least this many calls on every connection dials another one.
const BUSY_CALLS: usize = 2;

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
    idle_timeout: Duration,
    dialing: AtomicBool,
    /// Becomes `true` when the pool is dropped, which drains the added connections.
    dropped: watch::Sender<bool>,
}

struct Member {
    transport: QuicTransport,
    /// Calls in flight on the connection.
    calls: AtomicUsize,
    /// Whether a call started since the last idle check.
    used: AtomicBool,
    /// Notified when the last call in flight ends.
    idle: Notify,
}

impl Member {
    fn new(transport: QuicTransport) -> Arc<Self> {
        Arc::new(Self {
            transport,
            calls: AtomicUsize::new(0),
            used: AtomicBool::new(false),
            idle: Notify::new(),
        })
    }

    const fn connection(&self) -> &quinn::Connection {
        self.transport.connection()
    }
}

/// Counts a call against its connection until it is dropped.
struct CallGuard(Arc<Member>);

impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.0.calls.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

impl QuicPool {
    /// Creates the pool of a generation whose connection is `primary`. `dialer` dials the
    /// connections it adds, up to its `max_quic_connections` in all.
    pub(crate) fn new(primary: QuicTransport, dialer: Dialer) -> Self {
        Self {
            inner: Arc::new(Pool {
                primary: Member::new(primary),
                added: RwLock::new(Vec::new()),
                max: dialer.max_quic_connections.max(1),
                // `tokio::time::interval` rejects a zero period.
                idle_timeout: dialer.quic_idle_timeout.max(Duration::from_millis(1)),
                dialer,
                dialing: AtomicBool::new(false),
                dropped: watch::Sender::new(false),
            }),
        }
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

    /// Picks the connection with the fewest calls in flight for a new call, and dials another one
    /// when even that one is busy.
    fn pick(&self) -> (QuicTransport, CallGuard) {
        let pool = &self.inner;
        let (member, least, open) = {
            let added = pool.added.read_unpoisoned();
            let mut member = &pool.primary;
            let mut least = member.calls.load(Ordering::Relaxed);
            for candidate in added.iter() {
                let calls = candidate.calls.load(Ordering::Relaxed);
                if calls < least {
                    member = candidate;
                    least = calls;
                }
            }
            // Counted under the lock, so an idle connection is never retired under a new call.
            member.calls.fetch_add(1, Ordering::AcqRel);
            member.used.store(true, Ordering::Relaxed);
            (Arc::clone(member), least, added.len().saturating_add(1))
        };
        if least >= BUSY_CALLS && open < pool.max {
            self.grow();
        }
        (member.transport.clone(), CallGuard(member))
    }

    /// Dials another connection in the background, unless a dial is already under way.
    fn grow(&self) {
        if self.inner.dialing.swap(true, Ordering::AcqRel) {
            return;
        }
        let pool = Arc::downgrade(&self.inner);
        let dialer = self.inner.dialer.clone();
        tokio::spawn(async move {
            let result = dialer.dial_quic_transport().await;
            let failed = result.is_err();
            if let Ok(transport) = result {
                match pool.upgrade() {
                    Some(pool) => Pool::add(&pool, transport),
                    None => transport.connection().close(0_u32.into(), IDLE_REASON),
                }
            }
            if failed {
                tokio::time::sleep(DIAL_RETRY).await;
            }
            if let Some(pool) = pool.upgrade() {
                pool.dialing.store(false, Ordering::Release);
            }
        });
    }
}

impl Pool {
    fn add(pool: &Arc<Self>, transport: QuicTransport) {
        let member = Member::new(transport);
        {
            let mut added = pool.added.write_unpoisoned();
            if added.len().saturating_add(1) >= pool.max {
                member.connection().close(0_u32.into(), IDLE_REASON);
                return;
            }
            added.push(Arc::clone(&member));
        }
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
                if !member.used.swap(false, Ordering::Relaxed) && pool.retire_if_idle(&member) {
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
        let (transport, _guard) = self.pick();
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
        let (transport, guard) = self.pick();
        let inner = transport
            .streaming_call_with_info(request, request_body, info)
            .await?;
        Ok(Box::pin(Counted {
            inner,
            _guard: guard,
        }))
    }
}
