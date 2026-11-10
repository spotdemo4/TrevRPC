//! A rate-limit [`Interceptor`]: a token bucket per method and client.
//!
//! [`RateLimit`] is an interceptor, not an [`Authorizer`](crate::server::Authorizer): the server
//! authorizes a call again when the client replays it after the server refused its optimistic
//! stream compression, so an authorizer would charge such a call two tokens, while an interceptor
//! runs once per handler invocation.
//!
//! ```
//! use std::time::Duration;
//! use trevrpc::server::{RateLimit, RateLimitOptions, RatePolicy, Server};
//!
//! let mut server = Server::new();
//! server.add_interceptor(RateLimit::new(
//!     RateLimitOptions::new()
//!         .with_policy("/example.auth.Auth/Login", RatePolicy::new(5, Duration::from_secs(60)))
//!         .with_default(RatePolicy::new(100, Duration::from_secs(1))),
//! ));
//! ```
//!
//! A call that finds its bucket empty fails with `ResourceExhausted` and a `google.rpc.RetryInfo`
//! status detail that says when a token is next available; every `TrevRPC` client reads it (see
//! [`Status::retry_delay`]).

use std::collections::HashMap;
use std::hash::{BuildHasher as _, RandomState};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::Status;
use crate::server::{Finish, Interceptor, RequestContext};
use crate::sync::MutexExt as _;
use crate::wire::validate_method_path;

/// The length of the IPv6 prefix whose addresses share a bucket unless
/// [`RateLimitOptions::with_ipv6_prefix`] says otherwise: the /64 that most residential and cloud
/// networks give a customer.
pub const DEFAULT_IPV6_PREFIX: u8 = 64;

/// The number of buckets a [`RateLimit`] keeps at most, unless
/// [`RateLimitOptions::with_max_buckets`] says otherwise.
pub const DEFAULT_MAX_BUCKETS: usize = 10_000;

/// The number of independently locked bucket tables a [`RateLimit`] splits its buckets over, at
/// most.
const MAX_SHARDS: usize = 16;

/// Marks the end of the list of buckets in a shard.
const NONE: usize = usize::MAX;

/// A token bucket: a client may make `requests` calls at once, and the bucket refills at
/// `requests` per `window`, one token every `window / requests`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RatePolicy {
    requests: u32,
    window: Duration,
}

impl RatePolicy {
    /// Creates a policy of `requests` calls per `window`. `requests` is the size of the bucket,
    /// which is also the burst a client that has been idle may send.
    ///
    /// # Panics
    ///
    /// Panics when `requests` is zero, when `window` is zero, or when `window` is shorter than a
    /// nanosecond for each request.
    #[must_use]
    #[allow(clippy::panic)] // A policy that cannot work is a programming error found at startup.
    pub fn new(requests: u32, window: Duration) -> Self {
        assert!(requests > 0, "rate limit: requests must be positive");
        assert!(!window.is_zero(), "rate limit: window must be positive");
        assert!(
            window.as_nanos() >= u128::from(requests),
            "rate limit: window must be at least a nanosecond for each request"
        );
        Self { requests, window }
    }
}

/// The settings of a [`RateLimit`].
#[derive(Clone)]
pub struct RateLimitOptions {
    policies: HashMap<String, RatePolicy>,
    default: Option<RatePolicy>,
    key: Option<Arc<KeyFn>>,
    ipv6_prefix: u8,
    max_buckets: usize,
    clock: Option<Arc<ClockFn>>,
}

type KeyFn = dyn Fn(&RequestContext) -> String + Send + Sync + 'static;
type ClockFn = dyn Fn() -> Instant + Send + Sync + 'static;

impl Default for RateLimitOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RateLimitOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RateLimitOptions")
            .field("policies", &self.policies)
            .field("default", &self.default)
            .field("key", &self.key.as_ref().map(|_| "<fn>"))
            .field("ipv6_prefix", &self.ipv6_prefix)
            .field("max_buckets", &self.max_buckets)
            .finish_non_exhaustive()
    }
}

impl RateLimitOptions {
    /// Creates options that limit nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
            default: None,
            key: None,
            ipv6_prefix: DEFAULT_IPV6_PREFIX,
            max_buckets: DEFAULT_MAX_BUCKETS,
            clock: None,
        }
    }

    /// Limits the method `method`, a full method name such as `/example.auth.Auth/Login`. Every
    /// method has a bucket of its own for each client, even when several methods have the same
    /// policy.
    #[must_use]
    pub fn with_policy(mut self, method: impl Into<String>, policy: RatePolicy) -> Self {
        self.policies.insert(method.into(), policy);
        self
    }

    /// Limits every method without a policy of its own, with a bucket of its own for each method
    /// and client. Without a default those methods are unlimited.
    #[must_use]
    pub const fn with_default(mut self, policy: RatePolicy) -> Self {
        self.default = Some(policy);
        self
    }

    /// Names the client a call is counted against, in place of the default, the client's network
    /// address. Use it to limit by identity, such as the user an authorizer verified (and put in
    /// [`RequestContext::extensions`]) or an API key in the metadata. The key is kept as long as
    /// the bucket is, so it must not be unbounded text the client chooses without a check. With a
    /// key function the IPv6 prefix is not used.
    #[must_use]
    pub fn with_key(
        mut self,
        key: impl Fn(&RequestContext) -> String + Send + Sync + 'static,
    ) -> Self {
        self.key = Some(Arc::new(key));
        self
    }

    /// Sets the length in bits of the IPv6 prefix whose addresses share a bucket, from 1 to 128;
    /// [`DEFAULT_IPV6_PREFIX`] by default. A client with a /64 can use any of its 2^64 addresses,
    /// so limiting single addresses would not limit it. IPv4 addresses, with IPv4-mapped IPv6
    /// addresses counted as IPv4, always have a bucket of their own.
    #[must_use]
    pub const fn with_ipv6_prefix(mut self, bits: u8) -> Self {
        self.ipv6_prefix = bits;
        self
    }

    /// Sets the number of buckets a [`RateLimit`] keeps at most, [`DEFAULT_MAX_BUCKETS`] by
    /// default. When it is reached the bucket that was used longest ago makes room for a new one,
    /// which starts full.
    #[must_use]
    pub const fn with_max_buckets(mut self, max_buckets: usize) -> Self {
        self.max_buckets = max_buckets;
        self
    }

    /// Sets the clock that measures time, [`Instant::now`] by default, for tests.
    #[must_use]
    pub fn with_clock(mut self, clock: impl Fn() -> Instant + Send + Sync + 'static) -> Self {
        self.clock = Some(Arc::new(clock));
        self
    }
}

/// An [`Interceptor`] that limits calls with a token bucket for each method and client; see the
/// [module documentation](self).
///
/// By default a client is its address, [`RequestContext::remote_addr`], which names the real
/// client behind a proxy only when the server trusts the proxy to say so
/// ([`TcpFallback::with_trusted_proxies`](crate::tcp::TcpFallback::with_trusted_proxies) and the
/// trusted prefixes of [`QuicEndpoint::server`](crate::udp::QuicEndpoint::server)); otherwise every
/// client behind it shares the proxy's address and one set of buckets. A call whose address is
/// not known shares one bucket per method with the others of its kind.
///
/// Buckets live in a table that holds at most [`RateLimitOptions::with_max_buckets`] of them,
/// split over shards by key hash with a lock of its own and a least-recently-used order each. A
/// new client starts with a full bucket, so forgetting a bucket never refuses a call, but a client
/// that makes a bucket forgotten while it is empty, such as by calling from many networks, gets a
/// full one: set the bucket count large enough for the clients you expect, and limit abusive
/// networks at the edge as well.
pub struct RateLimit {
    policies: HashMap<String, Limit>,
    default: Option<Limit>,
    key: Option<Arc<KeyFn>>,
    ipv6_prefix: u8,
    clock: Arc<ClockFn>,
    /// The instant bucket state is measured from.
    epoch: Instant,
    hasher: RandomState,
    shards: Vec<Mutex<Shard>>,
}

impl std::fmt::Debug for RateLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RateLimit")
            .field("policies", &self.policies.len())
            .field("default", &self.default)
            .field("ipv6_prefix", &self.ipv6_prefix)
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

/// A [`RatePolicy`] in the form the buckets use.
#[derive(Debug, Clone)]
struct Limit {
    /// The time one token takes to refill, in nanoseconds.
    interval: u64,
    /// The time a full bucket takes to refill: `interval` times the number of requests.
    burst: u64,
    /// The status message of a rejected call.
    message: Arc<str>,
}

impl Limit {
    fn new(policy: RatePolicy) -> Self {
        let interval = policy
            .window
            .as_nanos()
            .checked_div(u128::from(policy.requests))
            .and_then(|nanos| u64::try_from(nanos).ok())
            .unwrap_or(u64::MAX);
        Self {
            interval,
            burst: interval.saturating_mul(u64::from(policy.requests)),
            message: format!(
                "rate limit exceeded: {} requests per {:?}",
                policy.requests, policy.window
            )
            .into(),
        }
    }
}

/// Who a bucket belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Client {
    /// A call without an address.
    Unknown,
    /// The network of the client's address: an IPv4 address, or an IPv6 address masked to the
    /// configured prefix.
    Network(IpAddr),
    /// The text a key function returned.
    Key(Box<str>),
}

impl RateLimit {
    /// Creates a rate limit from `options`.
    ///
    /// # Panics
    ///
    /// Panics when a policy is for a method that is not of the form `/<service>/<method>`, when
    /// the IPv6 prefix is not between 1 and 128, or when the bucket count is zero.
    #[must_use]
    #[allow(clippy::panic)] // Invalid options are a programming error found at startup.
    pub fn new(options: RateLimitOptions) -> Self {
        for method in options.policies.keys() {
            assert!(
                validate_method_path(method).is_ok(),
                "rate limit: policy method {method:?} must be /<service>/<method>"
            );
        }
        assert!(
            (1..=128).contains(&options.ipv6_prefix),
            "rate limit: the IPv6 prefix must be between 1 and 128"
        );
        assert!(
            options.max_buckets > 0,
            "rate limit: the bucket count must be positive"
        );

        // Split the capacity so that the shards hold exactly `max_buckets` together.
        let shards = options.max_buckets.min(MAX_SHARDS);
        let base = options.max_buckets.checked_div(shards).unwrap_or_default();
        let extra = options.max_buckets.checked_rem(shards).unwrap_or_default();
        let clock: Arc<ClockFn> = options.clock.unwrap_or_else(|| Arc::new(Instant::now));
        Self {
            policies: options
                .policies
                .into_iter()
                .map(|(method, policy)| (method, Limit::new(policy)))
                .collect(),
            default: options.default.map(Limit::new),
            key: options.key,
            ipv6_prefix: options.ipv6_prefix,
            epoch: clock(),
            clock,
            hasher: RandomState::new(),
            shards: (0..shards)
                .map(|index| {
                    Mutex::new(Shard::new(base.saturating_add(usize::from(index < extra))))
                })
                .collect(),
        }
    }

    /// Returns the client a call is counted against.
    fn client(&self, context: &RequestContext) -> Client {
        if let Some(key) = &self.key {
            return Client::Key(key(context).into());
        }
        context.remote_addr().map_or(Client::Unknown, |addr| {
            Client::Network(network(addr.ip(), self.ipv6_prefix))
        })
    }

    /// Takes a token from the bucket of `client` for `method`, or returns how long until one is
    /// available, in nanoseconds.
    fn take(&self, method: &str, client: Client, limit: &Limit) -> Option<u64> {
        let now = u64::try_from(
            (self.clock)()
                .saturating_duration_since(self.epoch)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        let shard = usize::try_from(self.hasher.hash_one((method, &client)))
            .unwrap_or_default()
            .checked_rem(self.shards.len())
            .and_then(|index| self.shards.get(index))?;
        shard.lock_unpoisoned().take(method, client, now, limit)
    }

    /// The number of buckets, for tests.
    #[cfg(test)]
    fn buckets(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock_unpoisoned().nodes.len())
            .sum()
    }
}

/// Returns the network `ip` belongs to: an IPv4 address (an IPv4-mapped IPv6 address as IPv4) as
/// it is, and an IPv6 address masked to its first `prefix` bits.
fn network(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => {
            let mask = u128::MAX
                .checked_shl(128_u32.saturating_sub(u32::from(prefix)))
                .unwrap_or(0);
            IpAddr::V6(Ipv6Addr::from(u128::from(v6) & mask))
        }
        IpAddr::V4(v4) => IpAddr::V4(v4),
    }
}

#[crate::async_trait]
impl Interceptor for RateLimit {
    async fn intercept(
        &self,
        context: &mut RequestContext,
    ) -> std::result::Result<Option<Finish>, Status> {
        let method = context.full_method();
        let Some(limit) = self.policies.get(method).or(self.default.as_ref()) else {
            return Ok(None);
        };
        match self.take(method, self.client(context), limit) {
            None => Ok(None),
            Some(wait) => Err(rejection(limit, wait)),
        }
    }
}

/// Returns the status of a call that has to wait `wait` nanoseconds. The delay is rounded up to a
/// whole millisecond, which is what JavaScript clients count in, so a client that waits it never
/// retries too soon.
fn rejection(limit: &Limit, wait: u64) -> Status {
    Status::resource_exhausted(&*limit.message)
        .with_retry_delay(Duration::from_millis(wait.div_ceil(1_000_000)))
}

/// One lock and table of buckets: a slab of them linked in the order they were used, and an index
/// from method and client.
struct Shard {
    methods: HashMap<Arc<str>, HashMap<Client, usize>>,
    nodes: Vec<Node>,
    /// The most recently used bucket, or [`NONE`].
    head: usize,
    /// The least recently used bucket, or [`NONE`].
    tail: usize,
    capacity: usize,
}

struct Node {
    method: Arc<str>,
    client: Client,
    /// The theoretical arrival time of the generic cell rate algorithm: when the bucket is full
    /// again. A bucket with `tat <= now` is full; one that holds `n` tokens of `requests` has
    /// `tat = now + (requests - n) * interval`.
    tat: u64,
    prev: usize,
    next: usize,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        Self {
            methods: HashMap::new(),
            nodes: Vec::new(),
            head: NONE,
            tail: NONE,
            capacity,
        }
    }

    /// Takes a token from the bucket of `client` for `method` and returns `None`, or leaves the
    /// bucket as it is and returns how long until a token is available, in nanoseconds.
    fn take(&mut self, method: &str, client: Client, now: u64, limit: &Limit) -> Option<u64> {
        let found = self
            .methods
            .get(method)
            .and_then(|clients| clients.get(&client))
            .copied();
        let index = match found {
            Some(index) => {
                if self.head != index {
                    self.unlink(index);
                    self.push_front(index);
                }
                index
            }
            None => self.insert(method, client, now),
        };
        let node = self.nodes.get_mut(index)?;

        // The generic cell rate algorithm: a call advances the bucket's full time by one
        // interval, and is allowed unless that leaves the bucket more than a whole bucket's
        // refill time ahead of now.
        let tat = node.tat.max(now).saturating_add(limit.interval);
        let allow_at = tat.saturating_sub(limit.burst);
        if allow_at > now {
            return Some(allow_at.saturating_sub(now));
        }
        node.tat = tat;
        None
    }

    /// Adds a full bucket, in place of the least recently used one when the shard is full, and
    /// returns its index.
    fn insert(&mut self, method: &str, client: Client, now: u64) -> usize {
        let method: Arc<str> = self
            .methods
            .get_key_value(method)
            .map_or_else(|| Arc::from(method), |(shared, _)| Arc::clone(shared));
        let node = Node {
            method: Arc::clone(&method),
            client: client.clone(),
            tat: now,
            prev: NONE,
            next: NONE,
        };
        let index = if self.nodes.len() < self.capacity {
            self.nodes.push(node);
            self.nodes.len().saturating_sub(1)
        } else {
            let index = self.tail;
            self.unlink(index);
            if let Some(old) = self.nodes.get_mut(index) {
                let old = std::mem::replace(old, node);
                self.forget(&old);
            }
            index
        };
        self.methods
            .entry(method)
            .or_default()
            .insert(client, index);
        self.push_front(index);
        index
    }

    /// Removes `node` from the index.
    fn forget(&mut self, node: &Node) {
        if let Some(clients) = self.methods.get_mut(&node.method) {
            clients.remove(&node.client);
            if clients.is_empty() {
                self.methods.remove(&node.method);
            }
        }
    }

    /// Takes the bucket at `index` out of the list.
    fn unlink(&mut self, index: usize) {
        let Some((prev, next)) = self.nodes.get(index).map(|node| (node.prev, node.next)) else {
            return;
        };
        match self.nodes.get_mut(prev) {
            Some(before) => before.next = next,
            None => self.head = next,
        }
        match self.nodes.get_mut(next) {
            Some(after) => after.prev = prev,
            None => self.tail = prev,
        }
    }

    /// Makes the bucket at `index`, which is not in the list, the most recently used.
    fn push_front(&mut self, index: usize) {
        let old_head = self.head;
        if let Some(node) = self.nodes.get_mut(index) {
            node.prev = NONE;
            node.next = old_head;
        }
        match self.nodes.get_mut(old_head) {
            Some(first) => first.prev = index,
            None => self.tail = index,
        }
        self.head = index;
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::{Code, Metadata, RpcRequest};

    const LOGIN: &str = "/example.auth.Auth/Login";
    const SEARCH: &str = "/example.search.Search/Query";
    const OTHER: &str = "/example.search.Search/Other";

    /// A clock tests move by hand.
    #[derive(Clone)]
    struct Clock {
        base: Instant,
        elapsed: Arc<AtomicU64>,
    }

    impl Clock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                elapsed: Arc::new(AtomicU64::new(0)),
            }
        }

        fn advance(&self, by: Duration) {
            let nanos = u64::try_from(by.as_nanos()).unwrap();
            self.elapsed.fetch_add(nanos, Ordering::SeqCst);
        }

        fn options(&self) -> RateLimitOptions {
            let clock = self.clone();
            RateLimitOptions::new().with_clock(move || {
                clock
                    .base
                    .checked_add(Duration::from_nanos(clock.elapsed.load(Ordering::SeqCst)))
                    .unwrap()
            })
        }
    }

    fn policy(requests: u32, seconds: u64) -> RatePolicy {
        RatePolicy::new(requests, Duration::from_secs(seconds))
    }

    fn context(method: &str, addr: Option<&str>) -> RequestContext {
        let context = RequestContext::new(&RpcRequest::new(method), None);
        match addr {
            Some(addr) => context.with_remote_addr(SocketAddr::new(addr.parse().unwrap(), 4433)),
            None => context,
        }
    }

    async fn call(limit: &RateLimit, method: &str, addr: Option<&str>) -> Result<(), Status> {
        let mut context = context(method, addr);
        limit.intercept(&mut context).await.map(|finish| {
            assert!(finish.is_none());
        })
    }

    async fn allowed(limit: &RateLimit, method: &str, addr: &str, calls: usize) {
        for call_number in 1..=calls {
            if let Err(status) = call(limit, method, Some(addr)).await {
                panic!("call {call_number} to {method} from {addr}: {status}");
            }
        }
    }

    async fn rejected(limit: &RateLimit, method: &str, addr: &str, delay: Duration) {
        let status = call(limit, method, Some(addr))
            .await
            .expect_err("the call should be rejected");
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.retry_delay(), Some(delay), "{method} from {addr}");
    }

    #[tokio::test]
    async fn a_burst_refills_at_the_configured_rate() {
        let clock = Clock::new();
        // One token every second, up to three.
        let limit = RateLimit::new(clock.options().with_default(policy(3, 3)));

        allowed(&limit, SEARCH, "192.0.2.1", 3).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_secs(1)).await;

        // A rejection does not spend anything, and the delay shrinks as time passes.
        clock.advance(Duration::from_millis(400));
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_millis(600)).await;
        clock.advance(Duration::from_millis(600));
        allowed(&limit, SEARCH, "192.0.2.1", 1).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_secs(1)).await;

        // Two tokens refill in two seconds.
        clock.advance(Duration::from_secs(2));
        allowed(&limit, SEARCH, "192.0.2.1", 2).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_secs(1)).await;

        // An idle client gets the burst again, and no more.
        clock.advance(Duration::from_secs(3600));
        allowed(&limit, SEARCH, "192.0.2.1", 3).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn the_retry_delay_rounds_up_to_a_millisecond() {
        let clock = Clock::new();
        // One token every 333,333,333 ns.
        let limit = RateLimit::new(
            clock
                .options()
                .with_default(RatePolicy::new(3, Duration::from_secs(1))),
        );

        allowed(&limit, SEARCH, "192.0.2.1", 3).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_millis(334)).await;
        // Waiting for the delay is always enough.
        clock.advance(Duration::from_millis(334));
        allowed(&limit, SEARCH, "192.0.2.1", 1).await;
    }

    #[tokio::test]
    async fn a_rejection_says_why_and_when_to_retry() {
        let clock = Clock::new();
        let limit = RateLimit::new(clock.options().with_policy(LOGIN, policy(1, 60)));
        allowed(&limit, LOGIN, "192.0.2.1", 1).await;

        let status = call(&limit, LOGIN, Some("192.0.2.1")).await.unwrap_err();

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), "rate limit exceeded: 1 requests per 60s");
        assert_eq!(status.details().len(), 1);
        assert!(status.metadata().is_empty());
    }

    #[tokio::test]
    async fn policies_apply_to_their_methods_and_the_default_to_the_rest() {
        let clock = Clock::new();
        let limit = RateLimit::new(
            clock
                .options()
                .with_policy(LOGIN, policy(1, 60))
                .with_default(policy(2, 60)),
        );

        allowed(&limit, LOGIN, "192.0.2.1", 1).await;
        rejected(&limit, LOGIN, "192.0.2.1", Duration::from_secs(60)).await;

        // Methods under the default have a bucket of their own each.
        allowed(&limit, SEARCH, "192.0.2.1", 2).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_secs(30)).await;
        allowed(&limit, OTHER, "192.0.2.1", 2).await;
        rejected(&limit, OTHER, "192.0.2.1", Duration::from_secs(30)).await;

        // So does every client.
        allowed(&limit, LOGIN, "192.0.2.2", 1).await;
        allowed(&limit, SEARCH, "192.0.2.2", 2).await;
    }

    #[tokio::test]
    async fn methods_without_a_policy_are_unlimited_without_a_default() {
        let limit = RateLimit::new(RateLimitOptions::new().with_policy(LOGIN, policy(1, 60)));

        allowed(&limit, SEARCH, "192.0.2.1", 1000).await;

        assert_eq!(limit.buckets(), 0);
    }

    #[tokio::test]
    async fn ipv4_clients_are_limited_by_address() {
        let clock = Clock::new();
        let limit = RateLimit::new(clock.options().with_default(policy(1, 60)));

        allowed(&limit, SEARCH, "192.0.2.1", 1).await;
        rejected(&limit, SEARCH, "192.0.2.1", Duration::from_secs(60)).await;
        // A neighbour in the same /24 has its own bucket.
        allowed(&limit, SEARCH, "192.0.2.2", 1).await;
        // An IPv4-mapped IPv6 address is the IPv4 address.
        rejected(&limit, SEARCH, "::ffff:192.0.2.1", Duration::from_secs(60)).await;
        rejected(&limit, SEARCH, "::ffff:192.0.2.2", Duration::from_secs(60)).await;
        allowed(&limit, SEARCH, "::ffff:192.0.2.3", 1).await;
        rejected(&limit, SEARCH, "192.0.2.3", Duration::from_secs(60)).await;
    }

    #[tokio::test]
    async fn ipv6_clients_share_a_bucket_per_prefix() {
        let clock = Clock::new();
        let limit = RateLimit::new(clock.options().with_default(policy(1, 60)));
        let minute = Duration::from_secs(60);

        allowed(&limit, SEARCH, "2001:db8:1:2::1", 1).await;
        // Other addresses of the same /64, including ones that differ in the interface identifier
        // only.
        rejected(&limit, SEARCH, "2001:db8:1:2::2", minute).await;
        rejected(&limit, SEARCH, "2001:db8:1:2:ffff:ffff:ffff:ffff", minute).await;
        // A neighbouring /64 has its own bucket.
        allowed(&limit, SEARCH, "2001:db8:1:3::1", 1).await;
        rejected(&limit, SEARCH, "2001:db8:1:3::99", minute).await;
        // IPv4 and IPv6 do not mix.
        allowed(&limit, SEARCH, "192.0.2.1", 1).await;
    }

    #[tokio::test]
    async fn the_ipv6_prefix_is_configurable() {
        let clock = Clock::new();
        let minute = Duration::from_secs(60);
        let limit = RateLimit::new(
            clock
                .options()
                .with_default(policy(1, 60))
                .with_ipv6_prefix(48),
        );

        allowed(&limit, SEARCH, "2001:db8:1:2::1", 1).await;
        rejected(&limit, SEARCH, "2001:db8:1:3::1", minute).await;
        rejected(&limit, SEARCH, "2001:db8:1:ffff::1", minute).await;
        allowed(&limit, SEARCH, "2001:db8:2::1", 1).await;

        // A /128 limits single addresses.
        let limit = RateLimit::new(
            clock
                .options()
                .with_default(policy(1, 60))
                .with_ipv6_prefix(128),
        );
        allowed(&limit, SEARCH, "2001:db8:1:2::1", 1).await;
        allowed(&limit, SEARCH, "2001:db8:1:2::2", 1).await;
        rejected(&limit, SEARCH, "2001:db8:1:2::2", minute).await;
    }

    #[tokio::test]
    async fn calls_without_an_address_share_one_bucket_per_method() {
        let clock = Clock::new();
        let limit = RateLimit::new(clock.options().with_default(policy(2, 60)));

        for _ in 0..2 {
            call(&limit, SEARCH, None).await.unwrap();
        }
        assert!(call(&limit, SEARCH, None).await.is_err());
        call(&limit, OTHER, None).await.unwrap();
        // Known clients are not affected.
        allowed(&limit, SEARCH, "192.0.2.1", 2).await;
    }

    #[tokio::test]
    async fn a_key_function_replaces_the_address() {
        let clock = Clock::new();
        let limit = RateLimit::new(clock.options().with_default(policy(1, 60)).with_key(
            |context| {
                context
                    .metadata()
                    .get("x-api-key")
                    .map(|key| String::from_utf8_lossy(key).into_owned())
                    .unwrap_or_default()
            },
        ));
        let call_as = |key: &'static str, addr: &'static str| {
            let limit = &limit;
            async move {
                let request = RpcRequest::new(SEARCH).with_metadata(Metadata::from([(
                    "x-api-key".to_owned(),
                    key.as_bytes().to_vec(),
                )]));
                let mut context = RequestContext::new(&request, None)
                    .with_remote_addr(SocketAddr::new(addr.parse().unwrap(), 4433));
                limit.intercept(&mut context).await
            }
        };

        call_as("alice", "192.0.2.1").await.unwrap();
        // The same key from another address is the same client; another key from the same address
        // is another.
        assert!(call_as("alice", "192.0.2.2").await.is_err());
        call_as("bob", "192.0.2.1").await.unwrap();
    }

    #[tokio::test]
    async fn buckets_are_bounded() {
        let clock = Clock::new();
        for max_buckets in [1, 5, 16, 100, 1000] {
            let limit = RateLimit::new(
                clock
                    .options()
                    .with_default(policy(1, 60))
                    .with_max_buckets(max_buckets),
            );
            for i in 0_u32..5000 {
                let [_, b, c, d] = i.to_be_bytes();
                allowed(&limit, SEARCH, &format!("10.{b}.{c}.{d}"), 1).await;
            }
            assert!(limit.buckets() <= max_buckets, "{max_buckets}");
            check_invariants(&limit);
        }
    }

    #[tokio::test]
    async fn the_least_recently_used_bucket_makes_room() {
        let clock = Clock::new();
        // One shard with room for two buckets.
        let limit = RateLimit::new(
            clock
                .options()
                .with_default(policy(1, 60))
                .with_max_buckets(1),
        );
        limit.shards[0].lock_unpoisoned().capacity = 2;
        let minute = Duration::from_secs(60);

        allowed(&limit, SEARCH, "192.0.2.1", 1).await;
        allowed(&limit, SEARCH, "192.0.2.2", 1).await;
        // Using the first bucket again makes the second the oldest.
        rejected(&limit, SEARCH, "192.0.2.1", minute).await;
        allowed(&limit, SEARCH, "192.0.2.3", 1).await;
        rejected(&limit, SEARCH, "192.0.2.1", minute).await;
        // The second was forgotten, so its client starts with a full bucket.
        allowed(&limit, SEARCH, "192.0.2.2", 1).await;
        check_invariants(&limit);
    }

    /// Fails the test if a shard's list and index disagree or hold more buckets than the shard
    /// may.
    fn check_invariants(limit: &RateLimit) {
        for shard in &limit.shards {
            let shard = shard.lock_unpoisoned();
            let mut listed: usize = 0;
            let mut index = shard.head;
            let mut previous = NONE;
            while let Some(node) = shard.nodes.get(index) {
                assert_eq!(node.prev, previous);
                assert_eq!(
                    shard
                        .methods
                        .get(&node.method)
                        .and_then(|clients| clients.get(&node.client)),
                    Some(&index)
                );
                listed = listed.saturating_add(1);
                assert!(listed <= shard.nodes.len(), "the list has a cycle");
                previous = index;
                index = node.next;
            }
            assert_eq!(shard.tail, previous);
            assert_eq!(listed, shard.nodes.len());
            assert_eq!(
                listed,
                shard.methods.values().map(HashMap::len).sum::<usize>()
            );
            assert!(listed <= shard.capacity);
            drop(shard);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_calls_share_buckets_safely() {
        const BURST: u32 = 50;
        const CLIENTS: usize = 8;
        let clock = Clock::new();
        let limit = Arc::new(RateLimit::new(
            clock
                .options()
                .with_default(policy(BURST, 3600))
                .with_max_buckets(64),
        ));

        let tasks: Vec<_> = (0..16)
            .map(|task| {
                let limit = Arc::clone(&limit);
                tokio::spawn(async move {
                    let mut allowed = [0_u32; CLIENTS];
                    for i in 0..200 {
                        let client = (task + i) % CLIENTS;
                        let addr = format!("192.0.2.{client}");
                        if call(&limit, SEARCH, Some(&addr)).await.is_ok() {
                            allowed[client] += 1;
                        }
                    }
                    allowed
                })
            })
            .collect();
        let mut totals = [0_u32; CLIENTS];
        for task in tasks {
            for (total, count) in totals.iter_mut().zip(task.await.unwrap()) {
                *total += count;
            }
        }

        // The clock never moved, so every client got exactly its burst.
        assert_eq!(totals, [BURST; CLIENTS]);
        check_invariants(&limit);
    }

    #[test]
    #[should_panic(expected = "requests must be positive")]
    fn a_policy_needs_requests() {
        let _ = RatePolicy::new(0, Duration::from_secs(1));
    }

    #[test]
    #[should_panic(expected = "window must be positive")]
    fn a_policy_needs_a_window() {
        let _ = RatePolicy::new(1, Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "a nanosecond for each request")]
    fn a_window_needs_a_nanosecond_for_each_request() {
        let _ = RatePolicy::new(3, Duration::from_nanos(2));
    }

    #[test]
    #[should_panic(expected = "must be /<service>/<method>")]
    fn a_policy_needs_a_method_path() {
        let _ = RateLimit::new(RateLimitOptions::new().with_policy("Login", policy(1, 1)));
    }

    #[test]
    #[should_panic(expected = "between 1 and 128")]
    fn the_ipv6_prefix_is_checked() {
        let _ = RateLimit::new(RateLimitOptions::new().with_ipv6_prefix(129));
    }

    #[test]
    #[should_panic(expected = "bucket count must be positive")]
    fn the_bucket_count_is_checked() {
        let _ = RateLimit::new(RateLimitOptions::new().with_max_buckets(0));
    }
}
