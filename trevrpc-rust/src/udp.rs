//! The QUIC side of a server: a Quinn endpoint, and the UDP load balancers in front of it that
//! name the client with a PROXY protocol header on every datagram.
//!
//! A UDP load balancer cannot open a connection with a PROXY header, so a trusted one puts a
//! version 2 header in front of every datagram it forwards instead. The endpoint's socket strips
//! it before Quinn reads the datagram, so QUIC keeps talking to the load balancer, and remembers
//! the client it names for the address the datagram came from. The server reports that client as
//! the connection's [`RequestContext::remote_addr`](crate::server::RequestContext::remote_addr).

use std::collections::HashMap;
use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};

use crate::proxy_protocol;
use crate::server::{AddressValidation, canonical_addr};
use crate::sync::{MutexExt as _, RwLockExt as _};
use crate::tcp::{self, IpPrefix};

/// How long the client of an address is remembered after its last datagram. Every datagram names
/// the client again, and the server looks it up as the streams and requests of a datagram arrive,
/// so it only has to outlast their processing.
const FLOW_TIMEOUT: Duration = Duration::from_secs(60);
/// How often the addresses whose datagrams stopped are forgotten.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// How many addresses of trusted proxies are remembered. Datagrams from further addresses are
/// dropped.
const MAX_FLOWS: usize = 1 << 18;
/// How often dropped datagrams are logged.
const DROP_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// A Quinn endpoint for [`Server::serve`](crate::server::Server::serve) and its variants, which
/// also take a bare [`quinn::Endpoint`].
pub struct QuicEndpoint {
    endpoint: quinn::Endpoint,
    /// The clients trusted proxies name, or `None` when none are trusted.
    clients: Option<Arc<ProxiedClients>>,
}

impl QuicEndpoint {
    /// Binds a server endpoint on `addr`, as [`quinn::Endpoint::server`] does, behind the UDP
    /// load balancers in `trusted_proxies`.
    ///
    /// Every datagram from a trusted proxy must start with a PROXY protocol version 2 header,
    /// which is stripped. The source address of the headers from a proxy's address when the
    /// connection on it completes its handshake becomes the connection's
    /// [`RequestContext::remote_addr`](crate::server::RequestContext::remote_addr), looked up
    /// again only when the connection's address changes, while QUIC keeps talking to the proxy. A datagram from a trusted proxy
    /// without a valid header is dropped and logged, at most once every 10 seconds; one from
    /// anywhere else is never read for a header. A `LOCAL` header, or one for an address family
    /// other than UDP over IPv4 or IPv6, leaves the proxy's own address.
    ///
    /// The header takes up to 52 bytes of every datagram, plus any extensions, which the path
    /// between the proxy and the endpoint must have room for. With no trusted proxies, this is
    /// [`quinn::Endpoint::server`].
    ///
    /// # Errors
    ///
    /// When the socket cannot be bound, or there is no async runtime.
    pub fn server(
        config: quinn::ServerConfig,
        addr: SocketAddr,
        trusted_proxies: impl IntoIterator<Item = IpPrefix>,
    ) -> io::Result<Self> {
        let trusted: Vec<IpPrefix> = trusted_proxies.into_iter().collect();
        if trusted.is_empty() {
            return quinn::Endpoint::server(config, addr).map(Self::from);
        }
        let socket = std::net::UdpSocket::bind(addr)?;
        let runtime =
            quinn::default_runtime().ok_or_else(|| io::Error::other("no async runtime found"))?;
        let clients = Arc::new(ProxiedClients::default());
        let socket = Arc::new(ProxiedSocket {
            inner: runtime.wrap_udp_socket(socket)?,
            trusted,
            clients: Arc::clone(&clients),
            flows: Mutex::new(Flows::default()),
        });
        let endpoint = quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            Some(config),
            socket,
            runtime,
        )?;
        Ok(Self {
            endpoint,
            clients: Some(clients),
        })
    }

    /// The Quinn endpoint, for its address and statistics.
    #[must_use]
    pub const fn endpoint(&self) -> &quinn::Endpoint {
        &self.endpoint
    }

    pub(crate) fn into_parts(self) -> (quinn::Endpoint, Option<Arc<ProxiedClients>>) {
        (self.endpoint, self.clients)
    }
}

impl From<quinn::Endpoint> for QuicEndpoint {
    /// An endpoint behind no trusted proxies.
    fn from(endpoint: quinn::Endpoint) -> Self {
        Self {
            endpoint,
            clients: None,
        }
    }
}

impl fmt::Debug for QuicEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuicEndpoint")
            .field("proxied", &self.clients.is_some())
            .finish_non_exhaustive()
    }
}

/// The client of one QUIC connection behind trusted proxies.
pub(crate) struct ProxiedPeer {
    clients: Arc<ProxiedClients>,
    /// The connection's remote address when its client was last looked up, and the client.
    last: Mutex<(SocketAddr, SocketAddr)>,
}

impl ProxiedPeer {
    /// Looks up the client of `connection`, which has completed its handshake.
    pub(crate) fn new(clients: Arc<ProxiedClients>, connection: &quinn::Connection) -> Self {
        let peer = connection.remote_address();
        let client = clients.client_of(peer).unwrap_or(peer);
        Self {
            clients,
            last: Mutex::new((peer, client)),
        }
    }

    /// The client of `connection`: the client the PROXY headers of a trusted proxy at its remote
    /// address name, or that address itself. It is looked up when the connection has completed
    /// its handshake, and again only when its remote address changes, so a header forged later in
    /// the name of the proxy does not move a connection to another client.
    pub(crate) fn client_addr(&self, connection: &quinn::Connection) -> SocketAddr {
        let peer = connection.remote_address();
        let mut last = self.last.lock_unpoisoned();
        if last.0 != peer {
            *last = (peer, self.clients.client_of(peer).unwrap_or(peer));
        }
        let client = last.1;
        drop(last);
        canonical_addr(client)
    }
}

/// Decides which connection attempts from addresses QUIC has not validated must prove them with a
/// Retry first, as [`AddressValidation`] says.
pub(crate) struct RetryPolicy {
    validation: AddressValidation,
    /// The attempts that may still pass, in billionths, for [`AddressValidation::AboveRate`]: a
    /// token bucket that holds a second's worth.
    tokens: u128,
    last: Instant,
}

/// What one attempt costs from [`RetryPolicy::tokens`].
const ATTEMPT: u128 = 1_000_000_000;

impl RetryPolicy {
    pub(crate) fn new(validation: AddressValidation, now: Instant) -> Self {
        let mut policy = Self {
            validation,
            tokens: 0,
            last: now,
        };
        policy.tokens = policy.capacity();
        policy
    }

    fn capacity(&self) -> u128 {
        match self.validation {
            AddressValidation::AboveRate(rate) => u128::from(rate.get()).saturating_mul(ATTEMPT),
            _ => 0,
        }
    }

    /// Whether an attempt at `now` from an address that is not validated must validate it.
    pub(crate) fn should_retry(&mut self, now: Instant) -> bool {
        let rate = match self.validation {
            AddressValidation::Off => return false,
            AddressValidation::Always => return true,
            AddressValidation::AboveRate(rate) => u128::from(rate.get()),
        };
        let elapsed = now.saturating_duration_since(self.last).as_nanos();
        if elapsed > 0 {
            self.tokens = self
                .tokens
                .saturating_add(elapsed.saturating_mul(rate))
                .min(self.capacity());
            self.last = now;
        }
        match self.tokens.checked_sub(ATTEMPT) {
            Some(tokens) => {
                self.tokens = tokens;
                false
            }
            None => true,
        }
    }
}

/// The addresses that trusted proxies forward datagrams from, and the clients their PROXY
/// headers name.
#[derive(Default)]
pub(crate) struct ProxiedClients {
    clients: RwLock<HashMap<SocketAddr, SocketAddr>>,
}

impl ProxiedClients {
    /// The client the datagrams from `peer` were last forwarded for, or `None` when their headers
    /// name none.
    fn client_of(&self, peer: SocketAddr) -> Option<SocketAddr> {
        self.clients.read_unpoisoned().get(&peer).copied()
    }

    /// Records `changes`; one without a client forgets its address.
    fn apply(&self, changes: &mut Vec<(SocketAddr, Option<SocketAddr>)>) {
        let mut clients = self.clients.write_unpoisoned();
        for (peer, client) in changes.drain(..) {
            match client {
                Some(client) => clients.insert(peer, client),
                None => clients.remove(&peer),
            };
        }
    }
}

/// The socket of an endpoint behind trusted proxies. It strips the PROXY header of every
/// datagram from them, and drops the ones without a valid header. Datagrams from anywhere else
/// are not read for one, and sends go to the socket unchanged.
struct ProxiedSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    trusted: Vec<IpPrefix>,
    clients: Arc<ProxiedClients>,
    /// Quinn receives from one task, so this is not contended.
    flows: Mutex<Flows>,
}

impl fmt::Debug for ProxiedSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxiedSocket")
            .field("inner", &self.inner)
            .field("trusted", &self.trusted)
            .finish_non_exhaustive()
    }
}

impl AsyncUdpSocket for ProxiedSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Arc::clone(&self.inner).create_io_poller()
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        self.inner.try_send(transmit)
    }

    fn poll_recv(
        &self,
        context: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let count = ready!(self.inner.poll_recv(context, bufs, meta))?;
        let mut flows = None;
        for (buf, meta) in bufs.iter_mut().zip(meta.iter_mut()).take(count) {
            if !tcp::trusts(&self.trusted, meta.addr.ip()) {
                continue;
            }
            let (flows, now) =
                flows.get_or_insert_with(|| (self.flows.lock_unpoisoned(), Instant::now()));
            flows.strip(buf, meta, *now);
        }
        if let Some((mut flows, now)) = flows {
            flows.finish(now, &self.clients);
        }
        Poll::Ready(Ok(count))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

/// What the socket knows of the addresses of trusted proxies.
#[derive(Default)]
struct Flows {
    by_peer: HashMap<SocketAddr, Flow>,
    /// Clients changed since they were last published, `None` for an address forgotten or whose
    /// header names no client.
    changes: Vec<(SocketAddr, Option<SocketAddr>)>,
    last_sweep: Option<Instant>,
    /// The datagrams dropped since the last report, the last of them from this address for this
    /// reason.
    dropped: u64,
    last_drop: Option<(SocketAddr, &'static str)>,
    last_report: Option<Instant>,
}

/// One address of a trusted proxy.
struct Flow {
    /// The client the last header named, or `None` when it named none.
    client: Option<SocketAddr>,
    seen: Instant,
}

impl Flows {
    /// Strips the PROXY header of every datagram in `buf`, as `meta` describes them, from a
    /// trusted proxy at `now`, and records the clients they name.
    ///
    /// With GRO, the buffer holds several datagrams from one address, each `meta.stride` bytes but
    /// the last, which may be shorter. Their payloads are moved together, and the stride becomes
    /// the payload length of the first. A datagram without a valid header, or whose payload cannot
    /// take that stride, is dropped; one with only a header, such as a health check, has nothing
    /// to keep.
    fn strip(&mut self, buf: &mut [u8], meta: &mut RecvMeta, now: Instant) {
        let len = meta.len.min(buf.len());
        let stride = if meta.stride == 0 { len } else { meta.stride };
        let mut read = 0_usize;
        let mut written = 0_usize;
        let mut new_stride = None;
        while read < len {
            let end = read.saturating_add(stride).min(len);
            let datagram = buf.get(read..end).unwrap_or_default();
            match self.header(datagram, meta.addr, now) {
                Ok(header_len) => {
                    let payload = end.saturating_sub(read).saturating_sub(header_len);
                    let fits = match new_stride {
                        _ if payload == 0 => false,
                        None => true,
                        // Only the last datagram may be shorter than the stride.
                        Some(new_stride) => {
                            payload == new_stride || (end == len && payload < new_stride)
                        }
                    };
                    if fits {
                        buf.copy_within(read.saturating_add(header_len)..end, written);
                        written = written.saturating_add(payload);
                        new_stride.get_or_insert(payload);
                    } else if payload > 0 {
                        self.dropped(meta.addr, "PROXY v2 headers of different lengths");
                    }
                }
                Err(reason) => self.dropped(meta.addr, reason),
            }
            read = end;
        }
        meta.len = written;
        if let Some(new_stride) = new_stride {
            meta.stride = new_stride;
        }
    }

    /// Parses the header of `datagram`, from `peer` at `now`, records the client it names, and
    /// returns its length.
    fn header(
        &mut self,
        datagram: &[u8],
        peer: SocketAddr,
        now: Instant,
    ) -> Result<usize, &'static str> {
        let (client, header_len) = proxy_protocol::parse_datagram(datagram)?;
        let client = client.map(canonical_addr);
        let flows_len = self.by_peer.len();
        match self.by_peer.get_mut(&peer) {
            Some(flow) => {
                if flow.client != client {
                    flow.client = client;
                    self.changes.push((peer, client));
                }
                flow.seen = now;
            }
            None if flows_len >= MAX_FLOWS => {
                return Err("too many addresses of trusted UDP proxies");
            }
            None => {
                self.by_peer.insert(peer, Flow { client, seen: now });
                if client.is_some() {
                    self.changes.push((peer, client));
                }
            }
        }
        Ok(header_len)
    }

    fn dropped(&mut self, peer: SocketAddr, reason: &'static str) {
        self.dropped = self.dropped.saturating_add(1);
        self.last_drop = Some((peer, reason));
    }

    /// Ends a receive whose datagrams from trusted proxies arrived at `now`: forgets the addresses
    /// that went quiet, publishes the clients that changed to `clients`, and logs the dropped
    /// datagrams when a report is due.
    fn finish(&mut self, now: Instant, clients: &ProxiedClients) {
        if self
            .last_sweep
            .is_none_or(|last| now.saturating_duration_since(last) >= SWEEP_INTERVAL)
        {
            let changes = &mut self.changes;
            self.by_peer.retain(|peer, flow| {
                let live = now.saturating_duration_since(flow.seen) <= FLOW_TIMEOUT;
                if !live && flow.client.is_some() {
                    changes.push((*peer, None));
                }
                live
            });
            self.last_sweep = Some(now);
        }
        if !self.changes.is_empty() {
            clients.apply(&mut self.changes);
        }
        if let Some((peer, reason)) = self.last_drop
            && self
                .last_report
                .is_none_or(|last| now.saturating_duration_since(last) >= DROP_REPORT_INTERVAL)
        {
            tracing::warn!(
                dropped = self.dropped,
                %peer,
                reason,
                "dropped datagrams from trusted UDP proxies"
            );
            self.dropped = 0;
            self.last_drop = None;
            self.last_report = Some(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Instant;

    use quinn::udp::RecvMeta;

    use super::{
        AddressValidation, FLOW_TIMEOUT, Flows, MAX_FLOWS, ProxiedClients, RetryPolicy,
        SWEEP_INTERVAL,
    };

    fn addr(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    const PROXY: &str = "127.0.0.1:5000";

    /// A version 2 header for a datagram over UDP from `client` to 127.0.0.1:443, with `extra`
    /// bytes of extensions.
    fn header(client: &str, extra: usize) -> Vec<u8> {
        let client = addr(client);
        let mut header = b"\r\n\r\n\0\r\nQUIT\n\x21".to_vec();
        let mut block = Vec::new();
        match client {
            SocketAddr::V4(v4) => {
                header.push(0x12);
                block.extend_from_slice(&v4.ip().octets());
                block.extend_from_slice(&[127, 0, 0, 1]);
            }
            SocketAddr::V6(v6) => {
                header.push(0x22);
                block.extend_from_slice(&v6.ip().octets());
                block.extend_from_slice(&[0; 15]);
                block.push(1);
            }
        }
        block.extend_from_slice(&client.port().to_be_bytes());
        block.extend_from_slice(&443_u16.to_be_bytes());
        // PP2_TYPE_NOOP.
        if extra > 0 {
            block.extend_from_slice(&[0x04, 0]);
            let value = extra.checked_sub(3).unwrap();
            block.push(u8::try_from(value).unwrap());
            block.resize(block.len().checked_add(value).unwrap(), 0);
        }
        header.extend_from_slice(&u16::try_from(block.len()).unwrap().to_be_bytes());
        header.extend_from_slice(&block);
        header
    }

    fn datagram(client: &str, payload: &[u8]) -> Vec<u8> {
        let mut datagram = header(client, 0);
        datagram.extend_from_slice(payload);
        datagram
    }

    fn meta(len: usize, stride: usize) -> RecvMeta {
        RecvMeta {
            addr: addr(PROXY),
            len,
            stride,
            ..RecvMeta::default()
        }
    }

    /// Strips `buf`, received as one buffer of datagrams `stride` bytes apart, and returns the
    /// datagrams that remain.
    fn strip(flows: &mut Flows, mut buf: Vec<u8>, stride: usize, now: Instant) -> Vec<Vec<u8>> {
        let mut meta = meta(buf.len(), stride);
        flows.strip(&mut buf, &mut meta, now);
        buf.truncate(meta.len);
        buf.chunks(meta.stride.max(1)).map(<[u8]>::to_vec).collect()
    }

    #[test]
    fn strips_one_datagram_and_names_its_client() {
        let clients = ProxiedClients::default();
        let mut flows = Flows::default();
        let now = Instant::now();
        let got = strip(&mut flows, datagram("203.0.113.7:4242", b"quic"), 0, now);
        flows.finish(now, &clients);
        assert_eq!(got, [b"quic".to_vec()]);
        assert_eq!(
            clients.client_of(addr(PROXY)),
            Some(addr("203.0.113.7:4242"))
        );
        assert_eq!(clients.client_of(addr("127.0.0.1:5001")), None);

        // IPv4-mapped clients are reported as IPv4, and a client that moves is followed.
        strip(&mut flows, datagram("[::ffff:203.0.113.8]:1", b"q"), 0, now);
        flows.finish(now, &clients);
        assert_eq!(clients.client_of(addr(PROXY)), Some(addr("203.0.113.8:1")));

        // A LOCAL header leaves the proxy's own address.
        let mut local = b"\r\n\r\n\0\r\nQUIT\n\x20\x00\x00\x00".to_vec();
        local.extend_from_slice(b"quic");
        assert_eq!(strip(&mut flows, local, 0, now), [b"quic".to_vec()]);
        flows.finish(now, &clients);
        assert_eq!(clients.client_of(addr(PROXY)), None);
    }

    #[test]
    fn drops_datagrams_without_a_header() {
        let mut flows = Flows::default();
        let now = Instant::now();
        assert_eq!(
            strip(&mut flows, vec![0xc0, 0, 0, 0, 1], 0, now),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(flows.dropped, 1);
        // A header alone, as a health check may send, leaves nothing but is not an error.
        assert_eq!(
            strip(&mut flows, header("203.0.113.7:1", 0), 0, now),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(flows.dropped, 1);
        flows.finish(now, &ProxiedClients::default());
        assert_eq!(flows.dropped, 0, "the drop was reported");
    }

    #[test]
    fn strips_every_datagram_of_a_gro_buffer() {
        let mut flows = Flows::default();
        let now = Instant::now();
        let client = "203.0.113.7:4242";
        // Three full datagrams and a shorter last one, with a bad one in the middle.
        let mut buf = Vec::new();
        buf.extend(datagram(client, &[1; 100]));
        buf.extend(datagram(client, &[2; 100]));
        buf.extend([0xc0; 128]);
        buf.extend(datagram(client, &[3; 100]));
        buf.extend(datagram(client, &[4; 40]));
        let got = strip(&mut flows, buf, 128, now);
        assert_eq!(got, [vec![1; 100], vec![2; 100], vec![3; 100], vec![4; 40]]);
        assert_eq!(flows.dropped, 1);
    }

    #[test]
    fn drops_gro_datagrams_that_break_the_stride() {
        let mut flows = Flows::default();
        let now = Instant::now();
        let client = "203.0.113.7:4242";
        // The second header is longer, so its payload is shorter than the first's, but it is not
        // the last datagram.
        let mut buf = datagram(client, &[1; 100]);
        let mut longer = header(client, 8);
        longer.extend_from_slice(&[2; 92]);
        buf.extend(longer);
        buf.extend(datagram(client, &[3; 100]));
        let got = strip(&mut flows, buf, 128, now);
        assert_eq!(got, [vec![1; 100], vec![3; 100]]);
        assert_eq!(flows.dropped, 1);

        // A shorter payload is fine for the last datagram.
        let mut buf = datagram(client, &[1; 100]);
        let mut longer = header(client, 8);
        longer.extend_from_slice(&[2; 92]);
        buf.extend(longer);
        assert_eq!(
            strip(&mut flows, buf, 128, now),
            [vec![1; 100], vec![2; 92]]
        );

        // Headers of different lengths in the first datagram set the stride.
        let mut buf = header(client, 8);
        buf.extend_from_slice(&[1; 92]);
        buf.extend(datagram(client, &[2; 100]));
        let got = strip(&mut flows, buf, 128, now);
        assert_eq!(got, [vec![1; 92]]);
    }

    #[test]
    fn forgets_quiet_addresses() {
        let clients = ProxiedClients::default();
        let mut flows = Flows::default();
        let start = Instant::now();
        let quiet = addr("127.0.0.1:5000");
        let busy = addr("127.0.0.1:5001");
        let send = |flows: &mut Flows, peer, client, at| {
            let mut buf = datagram(client, b"q");
            let mut meta = RecvMeta {
                addr: peer,
                len: buf.len(),
                stride: buf.len(),
                ..RecvMeta::default()
            };
            flows.strip(&mut buf, &mut meta, at);
            flows.finish(at, &clients);
        };
        send(&mut flows, quiet, "203.0.113.1:1", start);
        send(&mut flows, busy, "203.0.113.2:2", start);
        let mut elapsed = SWEEP_INTERVAL;
        while elapsed <= FLOW_TIMEOUT + 2 * SWEEP_INTERVAL {
            send(&mut flows, busy, "203.0.113.2:2", start + elapsed);
            elapsed += SWEEP_INTERVAL;
        }
        assert_eq!(clients.client_of(quiet), None);
        assert_eq!(clients.client_of(busy), Some(addr("203.0.113.2:2")));
        assert_eq!(flows.by_peer.len(), 1);
    }

    #[test]
    fn retry_policy_lets_its_rate_through() {
        let start = Instant::now();
        let mut policy = RetryPolicy::new(
            AddressValidation::AboveRate(std::num::NonZeroU32::new(2).unwrap()),
            start,
        );
        let burst: Vec<bool> = (0..3).map(|_| policy.should_retry(start)).collect();
        assert_eq!(burst, [false, false, true]);
        let half = start + std::time::Duration::from_millis(500);
        assert!(!policy.should_retry(half));
        assert!(policy.should_retry(half));
        // Quiet time refills up to a second's worth, not more.
        let later = half + std::time::Duration::from_secs(3600);
        let burst: Vec<bool> = (0..3).map(|_| policy.should_retry(later)).collect();
        assert_eq!(burst, [false, false, true]);

        assert!(!RetryPolicy::new(AddressValidation::Off, start).should_retry(start));
        assert!(RetryPolicy::new(AddressValidation::Always, start).should_retry(start));
    }

    #[test]
    fn bounds_the_addresses() {
        let mut flows = Flows::default();
        let now = Instant::now();
        for port in 0..MAX_FLOWS {
            let [a, b, c, d] = u32::try_from(port).unwrap().to_be_bytes();
            flows.by_peer.insert(
                SocketAddr::from(([127, b, c, d], u16::from(a))),
                super::Flow {
                    client: None,
                    seen: now,
                },
            );
        }
        assert_eq!(
            strip(&mut flows, datagram("203.0.113.7:1", b"q"), 0, now),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(flows.dropped, 1);
    }
}
