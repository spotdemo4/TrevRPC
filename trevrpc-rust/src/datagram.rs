//! Unreliable messages in datagrams: the codec, the per-connection demultiplexer, and the
//! per-call flows and streams built on it.
//!
//! A method can mark its streamed requests, its streamed responses, or both as [`Unreliable`].
//! The sender of each such message then picks, per message, between a datagram and an ordinary
//! MESSAGE frame; receivers accept datagram messages on any call that has a datagram flow. The
//! payload of a datagram is the call's flow ID as a QUIC variable-length integer followed by the
//! raw message bytes; see "Datagram Messages" in the protocol specification.
//!
//! This module is the one place that knows about datagrams. A transport only has to describe how
//! it sends and receives them ([`DatagramTransport`]); everything else is shared:
//!
//! - [`Demux`] reads a connection's datagrams in one task and hands each to the call that owns its
//!   flow, through a bounded queue that drops instead of blocking. A server also holds datagrams
//!   that overtake their REQUEST for a moment, and remembers recently ended flows.
//! - [`FlowSender`] sends one call's messages as datagrams, and reports when a message has to
//!   travel on the stream instead, and how many datagrams the transport took.
//! - [`MergedRequests`] and [`MergedResponses`] merge a call's datagram messages with the MESSAGE
//!   frames of its stream, so the rest of the runtime sees one ordinary message stream. When the
//!   stream's direction ends with a `DATAGRAM_COUNT` that announces more datagrams than the
//!   demultiplexer has received for the flow, they hold the end back for a moment, so datagrams
//!   that a transport hands over after the stream data are not lost.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use futures_util::{FutureExt as _, StreamExt as _};
use tokio::task::JoinHandle;
use tokio::time::{Instant, Sleep};

use crate::compression::{Accept, CallDict, inflate};
use crate::sync::MutexExt as _;
use crate::wake_gate::Gated;
use crate::{BoxStream, ResponseFrame, Result, RpcKind, queue, varint};

/// The most datagrams a server holds for flows it has not seen yet, per connection.
pub const MAX_HELD_DATAGRAMS: usize = 64;
/// The most bytes (of messages) a server holds for flows it has not seen yet, per connection.
pub const MAX_HELD_BYTES: usize = 64 * 1024;
/// How long a server holds a datagram for a flow it has not seen yet.
pub const HELD_DATAGRAM_TTL: Duration = Duration::from_millis(500);
/// The datagram receive buffer, in bytes, that the QUIC configurations built by this crate set on
/// every connection. Datagrams bypass flow control, so this bounds what a peer can make the
/// transport buffer before the demultiplexer reads it.
pub const DATAGRAM_RECEIVE_BUFFER: usize = 128 * 1024;
/// What a call queues of its datagram messages before it drops further ones: more messages than
/// the call's stream queue holds, because a datagram sender is not paced by flow control and
/// bursts of small messages are the common case, but the same bytes.
const QUEUE: queue::Limits = queue::Limits {
    values: queue::MESSAGES.values.saturating_mul(4),
    bytes: queue::MESSAGES.bytes,
};
/// How long a streaming direction that has ended waits for the datagrams its `DATAGRAM_COUNT`
/// announced and the demultiplexer has not received yet, before it ends regardless. A datagram
/// still missing then is lost.
const DATAGRAM_COUNT_WAIT: Duration = Duration::from_millis(10);
/// How long a connection reuses the largest datagram size it read from the transport; see
/// [`Link`].
const MAX_SIZE_REUSE: Duration = Duration::from_millis(100);
/// The fewest ended flows a server remembers per connection.
const MIN_ENDED_FLOWS: usize = 16;
/// The ended flows a server remembers per stream the connection may have open, so a late datagram
/// of a finished call is dropped at once instead of being held for a call that never comes.
const ENDED_FLOWS_PER_STREAM: usize = 2;
/// The stream limit assumed for remembering ended flows when a server sets none.
const UNLIMITED_STREAMS_ASSUMED: usize = 1024;

/// Which directions of a streaming method carry their messages in datagrams when they can.
///
/// Unreliable messages may be lost or reordered, but are never duplicated or corrupted. A
/// direction that does not stream is never unreliable: flags for it are ignored. Over transports
/// without datagrams (`QMux`, HTTP/2, HTTP/3 request streams) every message stays on the stream.
///
/// A client sets this on its calls with [`crate::client::CallOptions::with_unreliable`] and a
/// server on a route with [`crate::server::RouteOptions::with_unreliable`]; generated code does
/// both from the `trevrpc.method` option of the schema. A server always accepts request
/// datagrams, so only its `responses` flag matters.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Unreliable {
    /// The messages the client streams may travel as datagrams.
    pub requests: bool,
    /// The messages the server streams may travel as datagrams.
    pub responses: bool,
}

impl Unreliable {
    /// Neither direction is unreliable.
    pub const NONE: Self = Self {
        requests: false,
        responses: false,
    };
    /// The client's streamed requests are unreliable.
    pub const REQUESTS: Self = Self {
        requests: true,
        responses: false,
    };
    /// The server's streamed responses are unreliable.
    pub const RESPONSES: Self = Self {
        requests: false,
        responses: true,
    };
    /// Both directions are unreliable.
    pub const BOTH: Self = Self {
        requests: true,
        responses: true,
    };

    /// Returns whether either direction is unreliable.
    #[must_use]
    pub const fn any(self) -> bool {
        self.requests || self.responses
    }

    /// Returns the directions marked by either `self` or `other`.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self {
            requests: self.requests || other.requests,
            responses: self.responses || other.responses,
        }
    }

    /// Keeps only the directions that stream messages for a call of `kind`.
    #[must_use]
    pub const fn effective(self, kind: RpcKind) -> Self {
        Self {
            requests: self.requests && kind.streams_requests(),
            responses: self.responses && kind.streams_responses(),
        }
    }
}

/// A datagram could not be sent; the message travels on the stream instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Unsent;

/// How a transport sends and receives the datagrams of one connection (or `WebTransport`
/// session).
pub trait DatagramTransport: Send + Sync + 'static {
    /// The bytes that start every datagram, such as a `WebTransport` session's quarter stream ID.
    fn prefix(&self) -> &[u8] {
        &[]
    }

    /// The largest datagram the transport sends now, prefix included, or `None` when the peer
    /// does not accept datagrams.
    fn max_size(&self) -> Option<usize>;

    /// Queues `datagram`, waiting for room in the transport's send buffer.
    ///
    /// Dropping the future abandons the datagram.
    fn send(&self, datagram: Bytes)
    -> impl Future<Output = std::result::Result<(), Unsent>> + Send;

    /// Receives the next datagram, whole and prefix included, or `None` once the connection has
    /// ended.
    fn recv(&self) -> impl Future<Output = Option<Bytes>> + Send;
}

/// The datagrams of a transport that has none. It cannot be constructed.
pub enum NoDatagrams {}

impl DatagramTransport for NoDatagrams {
    fn max_size(&self) -> Option<usize> {
        None
    }

    fn send(
        &self,
        _datagram: Bytes,
    ) -> impl Future<Output = std::result::Result<(), Unsent>> + Send {
        std::future::ready(Err(Unsent))
    }

    fn recv(&self) -> impl Future<Output = Option<Bytes>> + Send {
        std::future::pending()
    }
}

/// A [`DatagramTransport`] without its type parameter, with the future of a blocked send boxed:
/// what [`FlowSender`] and the demultiplexer's state need of the transport.
trait SendLink: Send + Sync + 'static {
    fn prefix(&self) -> &[u8];

    fn max_size(&self) -> Option<usize>;

    /// Sends `datagram` if the transport has room right now; `None` when it would have to wait.
    fn send_now(&self, datagram: &Bytes) -> Option<std::result::Result<(), Unsent>>;

    fn send_wait(
        &self,
        datagram: Bytes,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), Unsent>> + Send + '_>>;
}

impl<T: DatagramTransport> SendLink for T {
    fn prefix(&self) -> &[u8] {
        DatagramTransport::prefix(self)
    }

    fn max_size(&self) -> Option<usize> {
        DatagramTransport::max_size(self)
    }

    fn send_now(&self, datagram: &Bytes) -> Option<std::result::Result<(), Unsent>> {
        DatagramTransport::send(self, datagram.clone()).now_or_never()
    }

    fn send_wait(
        &self,
        datagram: Bytes,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), Unsent>> + Send + '_>> {
        Box::pin(DatagramTransport::send(self, datagram))
    }
}

/// A connection's datagram transport, with the largest datagram size it last read from it.
///
/// Quinn reads that size under the connection lock, which every send already takes; reading it
/// for every datagram doubled the lock traffic of senders that send one message at a time. The
/// size only moves when path MTU discovery moves it, so it is read again after
/// [`MAX_SIZE_REUSE`], or as soon as the transport refuses a datagram, which may have outgrown a
/// shrunken path. A stale size costs little either way: one too large sends a datagram's messages
/// on the stream, one too small packs datagrams less.
struct Link {
    transport: Arc<dyn SendLink>,
    /// The size last read plus one, or zero when the transport had no datagrams.
    max_size: AtomicUsize,
    /// When `max_size` was read, in milliseconds since `epoch`, or [`Link::UNREAD`].
    read_at: AtomicU64,
    epoch: Instant,
}

impl Link {
    const UNREAD: u64 = u64::MAX;

    fn new(transport: Arc<dyn SendLink>) -> Self {
        Self {
            transport,
            max_size: AtomicUsize::new(0),
            read_at: AtomicU64::new(Self::UNREAD),
            epoch: Instant::now(),
        }
    }

    /// The largest datagram the transport sends, prefix included, as read at most
    /// [`MAX_SIZE_REUSE`] ago; `None` when the peer does not accept datagrams.
    fn max_size(&self) -> Option<usize> {
        let now = u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
        let read_at = self.read_at.load(Ordering::Acquire);
        let reuse = u64::try_from(MAX_SIZE_REUSE.as_millis()).unwrap_or(u64::MAX);
        if read_at != Self::UNREAD && now.saturating_sub(read_at) < reuse {
            return self.max_size.load(Ordering::Relaxed).checked_sub(1);
        }
        let max = self.transport.max_size();
        self.max_size.store(
            max.map_or(0, |max| max.saturating_add(1)),
            Ordering::Relaxed,
        );
        self.read_at.store(now, Ordering::Release);
        max
    }

    /// Makes the next [`Link::max_size`] read the size from the transport.
    fn reread_max_size(&self) {
        self.read_at.store(Self::UNREAD, Ordering::Release);
    }
}

/// The frame type of a MESSAGE frame.
const MESSAGE_FRAME: u64 = crate::framing::frame_type::MESSAGE;
/// The frame type of a `ZSTD_MESSAGE` frame. Every frame type in a datagram other than these two is
/// skipped.
const ZSTD_MESSAGE_FRAME: u64 = crate::framing::frame_type::ZSTD_MESSAGE;

/// The bytes a MESSAGE frame with a payload of `payload_len` bytes occupies in a datagram.
fn message_frame_len(payload_len: usize) -> usize {
    // The type is one byte; the length is a minimal variable-length integer.
    payload_len
        .saturating_add(varint::encoded_len(saturating_u64(payload_len)))
        .saturating_add(1)
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Appends a MESSAGE or `ZSTD_MESSAGE` frame carrying `message`.
fn put_message_frame(datagram: &mut BytesMut, frame_type: u64, message: &[u8]) {
    // A payload length always fits a variable-length integer.
    let _ = varint::push(datagram, frame_type);
    let _ = varint::push(datagram, saturating_u64(message.len()));
    datagram.extend_from_slice(message);
}

/// The bytes that start every datagram of a flow: the transport's prefix and the flow ID.
fn flow_header(prefix: &[u8], flow: u64) -> Option<Bytes> {
    let mut header = BytesMut::with_capacity(prefix.len().saturating_add(varint::MAX_LEN));
    header.extend_from_slice(prefix);
    varint::push(&mut header, flow).ok()?;
    Some(header.freeze())
}

/// Reads the frame at `offset`: its type, the range of its payload, and where the next frame
/// starts. Any valid encoding of the type and the length is accepted; `None` if the frame is
/// truncated or its length passes the end of `buf`.
fn next_frame(buf: &[u8], offset: usize) -> Option<(u64, usize, usize)> {
    let rest = buf.get(offset..)?;
    let (frame_type, type_len) = varint::decode(rest)?;
    let (len, length_len) = varint::decode(rest.get(type_len..)?)?;
    let start = offset.checked_add(type_len)?.checked_add(length_len)?;
    let end = start.checked_add(usize::try_from(len).ok()?)?;
    (end <= buf.len()).then_some((frame_type, start, end))
}

/// The messages of a datagram that [`decode`] has checked: its MESSAGE and `ZSTD_MESSAGE` frames,
/// in order, as their frame type and payload.
#[derive(Clone)]
struct Messages {
    datagram: Bytes,
    offset: usize,
}

impl Iterator for Messages {
    type Item = (u64, Bytes);

    fn next(&mut self) -> Option<(u64, Bytes)> {
        loop {
            // The frames were checked to fill the datagram exactly.
            let (frame_type, start, end) = next_frame(&self.datagram, self.offset)?;
            self.offset = end;
            if frame_type == MESSAGE_FRAME || frame_type == ZSTD_MESSAGE_FRAME {
                return Some((frame_type, self.datagram.slice(start..end)));
            }
        }
    }
}

impl Messages {
    /// The datagram's messages, compressed ones decompressed with the dictionaries `accept`
    /// holds, or `None` when one is rejected, which drops the whole datagram.
    fn inflate(self, accept: &Accept, max_frame_size: usize) -> Option<Vec<Bytes>> {
        self.map(|(frame_type, payload)| {
            if frame_type == ZSTD_MESSAGE_FRAME {
                inflate(accept, &payload, max_frame_size).ok()
            } else {
                Some(payload)
            }
        })
        .collect()
    }
}

/// A datagram, with its transport prefix already removed, split into its flow ID and messages.
struct Decoded {
    flow: u64,
    /// `None` when the datagram is dropped whole; see [`decode`]. It still counts as received for
    /// its flow.
    messages: Option<Messages>,
}

/// Splits a datagram, with its transport prefix already removed, into its flow ID and messages.
///
/// Returns `None` when the datagram does not start with a flow ID or the flow is zero (which
/// means no flow): it belongs to no flow. Otherwise its flow is known, and its messages are
/// `None`, dropping the datagram whole, when its frames do not exactly fill it, when a message
/// is larger than `max_frame_size`, or when it holds a `ZSTD_STREAM_MESSAGE`, which needs the
/// ordered delivery of a stream. Other frames that are not messages are skipped.
fn decode(datagram: Bytes, max_frame_size: usize) -> Option<Decoded> {
    let (flow, flow_len) = varint::decode(&datagram)?;
    if flow == 0 {
        return None;
    }
    let mut offset = flow_len;
    while offset < datagram.len() {
        let Some((frame_type, start, end)) = next_frame(&datagram, offset) else {
            return Some(Decoded {
                flow,
                messages: None,
            });
        };
        if (frame_type == MESSAGE_FRAME && end.saturating_sub(start) > max_frame_size)
            || frame_type == crate::framing::frame_type::ZSTD_STREAM_MESSAGE
        {
            return Some(Decoded {
                flow,
                messages: None,
            });
        }
        offset = end;
    }
    Some(Decoded {
        flow,
        messages: Some(Messages {
            datagram,
            offset: flow_len,
        }),
    })
}

/// What a server remembers about the flows it has finished.
struct EndedFlows {
    capacity: usize,
    order: VecDeque<u64>,
    set: HashSet<u64>,
}

impl EndedFlows {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn contains(&self, flow: u64) -> bool {
        self.set.contains(&flow)
    }

    fn insert(&mut self, flow: u64) {
        if !self.set.insert(flow) {
            return;
        }
        self.order.push_back(flow);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, flow: u64) {
        if self.set.remove(&flow) {
            self.order.retain(|ended| *ended != flow);
        }
    }
}

/// A datagram held for a flow whose REQUEST has not arrived.
struct Held {
    flow: u64,
    /// The datagram's messages, or `None` for one that was dropped whole as malformed: it is held
    /// only to be counted.
    messages: Option<Messages>,
    at: Instant,
    /// The datagrams of the flow that arrived after this one and found no room, which are
    /// counted with it.
    unheld: u64,
}

impl Held {
    /// The bytes the held datagram occupies.
    fn len(&self) -> usize {
        self.messages
            .as_ref()
            .map_or(0, |messages| messages.datagram.len())
    }
}

/// What a flow's REQUEST claims of the datagrams held for it.
struct Claimed {
    /// The messages of the datagrams that were well formed, oldest first.
    messages: Vec<Messages>,
    /// How many datagrams arrived for the flow, those that held no messages included.
    received: u64,
}

/// The datagrams a server holds for flows it has not seen yet, bounded in count, bytes and time.
#[derive(Default)]
struct HeldDatagrams {
    held: VecDeque<Held>,
    bytes: usize,
}

impl HeldDatagrams {
    /// Forgets the datagrams held for longer than [`HELD_DATAGRAM_TTL`].
    fn expire(&mut self, now: Instant) {
        while let Some(front) = self.held.front() {
            if now.saturating_duration_since(front.at) < HELD_DATAGRAM_TTL {
                break;
            }
            self.bytes = self.bytes.saturating_sub(front.len());
            self.held.pop_front();
        }
    }

    /// Holds a datagram, with `None` for its messages if it is malformed, unless that would
    /// exceed the bounds, in which case it is dropped. A dropped datagram is still counted, with
    /// the latest datagram held for its flow, if there is one.
    fn hold(&mut self, flow: u64, messages: Option<Messages>, now: Instant) {
        self.expire(now);
        let len = messages
            .as_ref()
            .map_or(0, |messages| messages.datagram.len());
        if self.held.len() >= MAX_HELD_DATAGRAMS || self.bytes.saturating_add(len) > MAX_HELD_BYTES
        {
            if let Some(held) = self.held.iter_mut().rev().find(|held| held.flow == flow) {
                held.unheld = held.unheld.saturating_add(1);
            }
            return;
        }
        self.bytes = self.bytes.saturating_add(len);
        self.held.push_back(Held {
            flow,
            messages,
            at: now,
            unheld: 0,
        });
    }

    /// Takes the unexpired datagrams held for `flow`, oldest first.
    fn claim(&mut self, flow: u64, now: Instant) -> Claimed {
        self.expire(now);
        let mut claimed = Claimed {
            messages: Vec::new(),
            received: 0,
        };
        if !self.held.iter().any(|held| held.flow == flow) {
            return claimed;
        }
        let mut kept = VecDeque::with_capacity(self.held.len());
        for held in self.held.drain(..) {
            if held.flow == flow {
                self.bytes = self.bytes.saturating_sub(held.len());
                claimed.received = claimed
                    .received
                    .saturating_add(1)
                    .saturating_add(held.unheld);
                claimed.messages.extend(held.messages);
            } else {
                kept.push_back(held);
            }
        }
        self.held = kept;
        claimed
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    /// Flows come from REQUEST frames; early datagrams are held, and ended flows remembered.
    Server,
    /// Flows are allocated locally and registered before their REQUEST is sent.
    Client,
}

/// How many datagrams the demultiplexer has received for a flow, and the call that may wait for
/// the rest of them.
#[derive(Default)]
struct Arrivals {
    received: AtomicU64,
    waiter: AtomicWaker,
}

impl Arrivals {
    /// Counts `datagrams` more as received, and wakes the call waiting for them. Their messages
    /// are queued first, so a call that sees the count also sees the messages.
    fn record(&self, datagrams: u64) {
        if datagrams > 0 {
            self.received.fetch_add(datagrams, Ordering::Release);
            self.waiter.wake();
        }
    }

    fn received(&self) -> u64 {
        self.received.load(Ordering::Acquire)
    }
}

/// A registered flow: where its datagram messages go, if its call receives any.
struct Slot {
    sink: Option<queue::Sender<Bytes>>,
    /// The dictionaries the call accepts on compressed messages.
    accept: Accept,
    /// The datagrams received for the flow, in the direction its call receives.
    arrivals: Arc<Arrivals>,
}

struct State {
    flows: HashMap<u64, Slot>,
    ended: EndedFlows,
    held: HeldDatagrams,
}

struct Shared {
    role: Role,
    link: Arc<Link>,
    state: Mutex<State>,
    /// The largest message a datagram may carry, and the largest this side sends in one.
    max_frame_size: usize,
    /// The next flow ID a client allocates.
    next_flow: AtomicU64,
}

impl Shared {
    fn dispatch(&self, datagram: Bytes) {
        let prefix_len = self.link.transport.prefix().len();
        let datagram = if prefix_len == 0 {
            datagram
        } else {
            // A datagram of another session, or without the prefix, is not ours.
            if datagram.get(..prefix_len) != Some(self.link.transport.prefix()) {
                return;
            }
            datagram.slice(prefix_len..)
        };
        let Some(Decoded { flow, messages }) = decode(datagram, self.max_frame_size) else {
            return;
        };

        let mut state = self.state.lock_unpoisoned();
        if let Some(slot) = state.flows.get(&flow) {
            if let Some(sink) = &slot.sink
                && let Some(messages) = messages
                    .and_then(|messages| messages.inflate(&slot.accept, self.max_frame_size))
            {
                for message in messages {
                    let size = message.len();
                    // A full queue or a finished direction drops the message.
                    let _ = sink.try_send(message, size);
                }
            }
            // Whatever became of its messages, the datagram arrived.
            slot.arrivals.record(1);
            return;
        }
        if self.role == Role::Server && !state.ended.contains(flow) {
            state.held.hold(flow, messages, Instant::now());
        }
    }

    fn unregister(&self, flow: u64) {
        let mut state = self.state.lock_unpoisoned();
        state.flows.remove(&flow);
        if self.role == Role::Server {
            state.ended.insert(flow);
        }
    }
}

/// A flow ID that is already in use on the connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowInUse;

/// Spawns the task that reads a connection's datagrams into its [`Shared`] state.
type ReaderStarter = Box<dyn FnOnce(Arc<Shared>) -> JoinHandle<()> + Send>;

/// The demultiplexer of one connection's (or `WebTransport` session's) datagrams.
///
/// One task reads the connection's datagrams, and stops when the demultiplexer is dropped. A
/// server starts it with the demultiplexer if the peer accepts datagrams; a client starts it with
/// its first flow, so a connection that never has a flow costs no task. Until it starts, the
/// transport buffers the connection's datagrams within its own receive buffer.
pub struct Demux {
    shared: Arc<Shared>,
    starter: Mutex<Option<ReaderStarter>>,
    reader: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Demux {
    fn drop(&mut self) {
        let reader = self.reader.lock_unpoisoned().take();
        if let Some(reader) = reader {
            reader.abort();
        }
    }
}

impl Demux {
    /// Demultiplexes a server connection's datagrams.
    ///
    /// `max_streams` is the connection's stream limit, which sizes the memory of ended flows.
    ///
    /// The reader starts at once when the peer accepts datagrams, so that a datagram is held for
    /// its REQUEST from the moment it arrives and not from the moment its call opens. A peer that
    /// accepts none costs no task.
    #[must_use]
    pub fn server<T: DatagramTransport>(
        transport: T,
        max_streams: Option<usize>,
        max_frame_size: usize,
    ) -> Self {
        let capacity = max_streams
            .unwrap_or(UNLIMITED_STREAMS_ASSUMED)
            .saturating_mul(ENDED_FLOWS_PER_STREAM)
            .max(MIN_ENDED_FLOWS);
        let demux = Self::new(transport, Role::Server, capacity, max_frame_size);
        if demux.supported() {
            demux.ensure_reader();
        }
        demux
    }

    /// Demultiplexes a client connection's datagrams.
    #[must_use]
    pub fn client<T: DatagramTransport>(transport: T, max_frame_size: usize) -> Self {
        Self::new(transport, Role::Client, 0, max_frame_size)
    }

    fn new<T: DatagramTransport>(
        transport: T,
        role: Role,
        ended_capacity: usize,
        max_frame_size: usize,
    ) -> Self {
        let transport = Arc::new(transport);
        let link: Arc<T> = Arc::clone(&transport);
        let link: Arc<dyn SendLink> = link;
        let shared = Arc::new(Shared {
            role,
            link: Arc::new(Link::new(link)),
            state: Mutex::new(State {
                flows: HashMap::new(),
                ended: EndedFlows::new(ended_capacity),
                held: HeldDatagrams::default(),
            }),
            max_frame_size,
            next_flow: AtomicU64::new(1),
        });
        let starter: ReaderStarter =
            Box::new(move |shared| tokio::spawn(read_datagrams(transport, shared)));
        Self {
            shared,
            starter: Mutex::new(Some(starter)),
            reader: Mutex::new(None),
        }
    }

    /// Starts reading the connection's datagrams, if that has not started yet.
    fn ensure_reader(&self) {
        let Some(starter) = self.starter.lock_unpoisoned().take() else {
            return;
        };
        let reader = starter(Arc::clone(&self.shared));
        *self.reader.lock_unpoisoned() = Some(reader);
    }

    /// Returns whether the peer accepts datagrams, so calls can have a flow.
    #[must_use]
    pub fn supported(&self) -> bool {
        self.shared.link.max_size().is_some()
    }

    fn sender(&self, flow: u64) -> Option<FlowSender> {
        Some(FlowSender {
            link: Arc::clone(&self.shared.link),
            header: flow_header(self.shared.link.transport.prefix(), flow)?,
            max_frame_size: self.shared.max_frame_size,
            compress: CallDict::None,
        })
    }

    /// A queue for the datagram messages of a direction that streams, or nothing.
    fn sink(streams: bool) -> (Option<queue::Sender<Bytes>>, Option<queue::Receiver<Bytes>>) {
        if streams {
            let (sender, receiver) = queue::channel(QUEUE);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        }
    }

    /// Allocates a flow for a client call of `kind` and registers it, or returns `None` when the
    /// call gets no flow: it marks no streaming direction unreliable, or the peer accepts no
    /// datagrams.
    ///
    /// Open the flow before writing the call's REQUEST, so no datagram of the server can arrive
    /// before the flow exists. `accept` holds the dictionaries the call accepts on compressed
    /// response messages.
    #[must_use]
    pub(crate) fn open_client_flow(
        &self,
        kind: RpcKind,
        unreliable: Unreliable,
        accept: Accept,
    ) -> Option<ClientFlow> {
        let unreliable = unreliable.effective(kind);
        if !unreliable.any() || !self.supported() {
            return None;
        }
        let flow = self.shared.next_flow.fetch_add(1, Ordering::Relaxed);
        if flow > varint::MAX {
            return None;
        }
        let sender = self.sender(flow)?;
        let (sink, responses) = Self::sink(kind.streams_responses());
        let expected = Expected::default();
        self.shared.state.lock_unpoisoned().flows.insert(
            flow,
            Slot {
                sink,
                accept,
                arrivals: Arc::clone(&expected.arrivals),
            },
        );
        self.ensure_reader();
        Some(ClientFlow {
            flow,
            requests: unreliable.requests.then_some(sender),
            responses,
            expected,
            guard: FlowGuard {
                shared: Arc::clone(&self.shared),
                flow,
            },
        })
    }

    /// Registers the flow of a server call of `kind` and claims the datagrams that arrived
    /// before its REQUEST.
    ///
    /// Fails when the flow is zero, too large, or already live on the connection. `accept` holds
    /// the dictionaries the server accepts on compressed request messages.
    pub(crate) fn open_server_flow(
        &self,
        flow: u64,
        kind: RpcKind,
        accept: &Accept,
    ) -> std::result::Result<ServerFlow, FlowInUse> {
        if flow == 0 || flow > varint::MAX {
            return Err(FlowInUse);
        }
        let sender = self.sender(flow).ok_or(FlowInUse)?;
        let (sink, requests) = Self::sink(kind.streams_requests());
        let expected = Expected::default();
        let held = {
            let mut state = self.shared.state.lock_unpoisoned();
            if state.flows.contains_key(&flow) {
                return Err(FlowInUse);
            }
            state.ended.remove(flow);
            let held = state.held.claim(flow, Instant::now());
            state.flows.insert(
                flow,
                Slot {
                    sink: sink.clone(),
                    accept: accept.clone(),
                    arrivals: Arc::clone(&expected.arrivals),
                },
            );
            held
        };
        if let Some(sink) = &sink {
            let max_frame_size = self.shared.max_frame_size;
            for messages in held.messages {
                for message in messages.inflate(accept, max_frame_size).unwrap_or_default() {
                    let size = message.len();
                    let _ = sink.try_send(message, size);
                }
            }
        }
        // The datagrams that overtook the REQUEST count like the ones that follow it.
        expected.arrivals.record(held.received);
        self.ensure_reader();
        Ok(ServerFlow {
            requests,
            sender,
            expected,
            guard: FlowGuard {
                shared: Arc::clone(&self.shared),
                flow,
            },
        })
    }
}

/// Reads a connection's datagrams until it ends.
async fn read_datagrams<T: DatagramTransport>(transport: Arc<T>, shared: Arc<Shared>) {
    while let Some(datagram) = transport.recv().await {
        shared.dispatch(datagram);
    }
}

/// Unregisters a flow when its call ends, whatever the way it ends.
pub struct FlowGuard {
    shared: Arc<Shared>,
    flow: u64,
}

impl Drop for FlowGuard {
    fn drop(&mut self) {
        self.shared.unregister(self.flow);
    }
}

/// The client side of a call's datagram flow.
pub struct ClientFlow {
    /// The ID to send in REQUEST.
    pub flow: u64,
    /// Sends the call's request messages, if the call marks its requests unreliable.
    pub requests: Option<FlowSender>,
    /// The call's datagram response messages, if its responses stream.
    pub responses: Option<queue::Receiver<Bytes>>,
    /// The datagrams the response direction is owed; see [`Expected`].
    pub expected: Expected,
    /// Unregisters the flow when dropped.
    pub guard: FlowGuard,
}

/// The server side of a call's datagram flow.
pub struct ServerFlow {
    /// The call's datagram request messages, if its requests stream.
    pub requests: Option<queue::Receiver<Bytes>>,
    /// Sends the call's response messages.
    pub sender: FlowSender,
    /// The datagrams the request direction is owed; see [`Expected`].
    pub expected: Expected,
    /// Unregisters the flow when dropped.
    pub guard: FlowGuard,
}

/// The `DATAGRAM_COUNT` a direction ended with, recorded by whatever reads its stream: how many
/// datagrams the peer sent in the direction. It stays unset on a direction that has none, and
/// the default records nothing, for calls without a datagram flow.
#[derive(Clone, Debug, Default)]
pub struct Announced(Option<Arc<OnceLock<u64>>>);

impl Announced {
    /// A cell that records nothing.
    pub(crate) const fn none() -> Self {
        Self(None)
    }

    fn new() -> Self {
        Self(Some(Arc::new(OnceLock::new())))
    }

    /// Records the count. The first one stands.
    pub(crate) fn set(&self, count: u64) {
        if let Some(cell) = &self.0 {
            let _ = cell.set(count);
        }
    }

    pub(crate) fn get(&self) -> Option<u64> {
        self.0.as_ref().and_then(|cell| cell.get().copied())
    }
}

/// What a call knows of the datagrams its receiving direction is owed: the count the stream
/// announced, and how many the demultiplexer has received for the flow so far.
#[derive(Clone)]
pub struct Expected {
    announced: Announced,
    arrivals: Arc<Arrivals>,
}

impl Default for Expected {
    fn default() -> Self {
        Self {
            announced: Announced::new(),
            arrivals: Arc::default(),
        }
    }
}

impl Expected {
    /// Where the reader of the direction's stream records its `DATAGRAM_COUNT`.
    #[must_use]
    pub fn announced(&self) -> Announced {
        self.announced.clone()
    }
}

/// The wait of a direction that has ended, for the datagrams its `DATAGRAM_COUNT` announced and the
/// demultiplexer has not received yet.
///
/// The wait is bounded by [`DATAGRAM_COUNT_WAIT`] from the first poll, which the caller makes
/// when the direction ends. The caller delivers what the flow's queue holds as it waits, and
/// ends the direction once the wait is over.
struct Stragglers {
    expected: Expected,
    timer: Option<Pin<Box<Sleep>>>,
}

impl Stragglers {
    const fn new(expected: Expected) -> Self {
        Self {
            expected,
            timer: None,
        }
    }

    /// Whether no datagram is awaited any more: none was announced, every announced one has
    /// arrived, or the wait has run out. A pending poll wakes when a datagram arrives.
    ///
    /// Take what the flow's queue holds only after this returns, so that every message of the
    /// datagrams counted by now is in it.
    fn poll_over(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(announced) = self.expected.announced.get() else {
            return Poll::Ready(());
        };
        let arrivals = &self.expected.arrivals;
        // Registered before the count is read, so an arrival in between wakes the call.
        arrivals.waiter.register(cx.waker());
        if arrivals.received() >= announced {
            return Poll::Ready(());
        }
        self.timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(DATAGRAM_COUNT_WAIT)))
            .as_mut()
            .poll(cx)
    }
}

/// Sends one call's messages as datagrams.
#[derive(Clone)]
pub struct FlowSender {
    link: Arc<Link>,
    /// The transport prefix and the flow ID.
    header: Bytes,
    /// The largest message sent in a datagram.
    max_frame_size: usize,
    /// Compresses the messages, once it has a dictionary.
    compress: CallDict,
}

impl FlowSender {
    /// Makes the sender compress messages with `dict`, once it has one and when that makes them
    /// shorter.
    #[must_use]
    pub(crate) fn with_compression(mut self, dict: impl Into<CallDict>) -> Self {
        self.compress = dict.into();
        self
    }

    /// Sends the messages in `batch`, draining it, as few datagrams as fit, in order; the messages
    /// that have to travel on the stream are pushed to `on_stream`, in order.
    ///
    /// A message goes on the stream when it alone does not fit a datagram, when datagrams are
    /// unavailable, when the datagram that carries it is refused by the transport, or when
    /// `cancelled` finished while a send was waiting for room in the transport's buffer.
    ///
    /// The messages in `batch` are the ones that are ready now: nothing waits for more. A send
    /// that finds room returns without suspending, and one that has to wait never holds up other
    /// calls.
    ///
    /// Returns how many datagrams the transport accepted, which the call announces in its
    /// `DATAGRAM_COUNT`: a datagram that was refused, whose messages go to `on_stream`, is not one.
    pub async fn send_batch(
        &self,
        batch: &mut Vec<Bytes>,
        on_stream: &mut Vec<Bytes>,
        cancelled: impl Future<Output = ()>,
    ) -> u64 {
        let max = self.link.max_size();
        let mut cancelled = std::pin::pin!(cancelled);
        let mut fired = false;
        let mut sent = 0_u64;
        let total = batch.iter().fold(self.header.len(), |total, message| {
            total.saturating_add(message_frame_len(message.len()))
        });
        let mut datagram = BytesMut::new();
        let mut packed: Vec<Bytes> = Vec::new();
        for message in batch.drain(..) {
            // The datagram carries the compressed frame; a message that goes on the stream after
            // all goes uncompressed, and the stream's writer compresses it again.
            let (frame_type, payload) = self
                .compress
                .get()
                .and_then(|dict| dict.compress(&message))
                .map_or_else(
                    || (MESSAGE_FRAME, message.clone()),
                    |compressed| (ZSTD_MESSAGE_FRAME, compressed),
                );
            let frame = message_frame_len(payload.len());
            let max = max.filter(|max| {
                message.len() <= self.max_frame_size
                    && self.header.len().saturating_add(frame) <= *max
            });
            let Some(max) = max else {
                on_stream.push(message);
                continue;
            };
            if !packed.is_empty() && datagram.len().saturating_add(frame) > max {
                sent = sent.saturating_add(
                    self.flush(
                        &mut datagram,
                        &mut packed,
                        on_stream,
                        cancelled.as_mut(),
                        &mut fired,
                    )
                    .await,
                );
            }
            if packed.is_empty() {
                datagram.reserve(total.min(max));
                datagram.extend_from_slice(&self.header);
            }
            put_message_frame(&mut datagram, frame_type, &payload);
            packed.push(message);
        }
        if !packed.is_empty() {
            sent = sent.saturating_add(
                self.flush(
                    &mut datagram,
                    &mut packed,
                    on_stream,
                    cancelled.as_mut(),
                    &mut fired,
                )
                .await,
            );
        }
        sent
    }

    /// Sends the datagram holding `packed`; its messages go to `on_stream` if that fails.
    /// Returns how many datagrams the transport accepted, one or none.
    async fn flush(
        &self,
        datagram: &mut BytesMut,
        packed: &mut Vec<Bytes>,
        on_stream: &mut Vec<Bytes>,
        cancelled: Pin<&mut impl Future<Output = ()>>,
        fired: &mut bool,
    ) -> u64 {
        let datagram = datagram.split().freeze();
        // `None` when `cancelled` gave up on a send that had to wait.
        let sent = if let Some(sent) = self.link.transport.send_now(&datagram) {
            Some(sent)
        } else if *fired {
            None
        } else {
            tokio::select! {
                biased;
                () = cancelled => {
                    *fired = true;
                    None
                }
                sent = self.link.transport.send_wait(datagram) => Some(sent),
            }
        };
        match sent {
            Some(Ok(())) => {
                packed.clear();
                1
            }
            Some(Err(Unsent)) => {
                // The datagram may have outgrown a path that shrank since the size was read.
                self.link.reread_max_size();
                on_stream.append(packed);
                0
            }
            None => {
                on_stream.append(packed);
                0
            }
        }
    }
}

/// How many request messages of a call arrived as datagrams.
#[derive(Clone, Debug, Default)]
pub struct MessageCount(Option<Arc<AtomicU64>>);

impl MessageCount {
    /// A counter that starts at zero and shares its count with its clones.
    #[must_use]
    pub fn new() -> Self {
        Self(Some(Arc::new(AtomicU64::new(0))))
    }

    pub(crate) fn add(&self) {
        if let Some(count) = &self.0 {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The messages counted so far.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0
            .as_ref()
            .map_or(0, |count| count.load(Ordering::Relaxed))
    }
}

/// The request messages of a call: its stream's MESSAGE frames merged with its datagram
/// messages in arrival order.
///
/// The datagram queue is read until the stream's request side ends. If the request side ended
/// with a `DATAGRAM_COUNT` that announces datagrams that have not arrived yet, the queue is read
/// until they have, or [`DATAGRAM_COUNT_WAIT`] has passed. Then it is drained of what it holds
/// and dropped, so datagrams that arrive later are dropped, and the requests end.
pub struct MergedRequests {
    /// Polled only when it woke, so a datagram does not also cost a poll of the stream.
    requests: Gated<BoxStream<Vec<u8>>>,
    datagrams: Option<queue::Receiver<Bytes>>,
    stragglers: Stragglers,
    count: MessageCount,
    requests_done: bool,
    /// Alternates which source is polled first, so neither starves the other.
    datagrams_first: bool,
}

impl MergedRequests {
    /// Merges `requests` with the datagram messages in `datagrams`, counting the latter in
    /// `count` as they are delivered. The datagrams `expected` says the request side is owed are
    /// waited for when it ends.
    pub fn boxed(
        requests: BoxStream<Vec<u8>>,
        datagrams: queue::Receiver<Bytes>,
        expected: Expected,
        count: MessageCount,
    ) -> BoxStream<Vec<u8>> {
        Box::pin(Self {
            requests: Gated::new(requests),
            datagrams: Some(datagrams),
            stragglers: Stragglers::new(expected),
            count,
            requests_done: false,
            datagrams_first: false,
        })
    }

    fn poll_requests(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        if self.requests_done {
            return Poll::Pending;
        }
        match self.requests.poll_next_unpin(cx) {
            Poll::Ready(Some(Err(error))) => {
                // The call failed: nothing more is delivered.
                self.requests_done = true;
                self.datagrams = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.requests_done = true;
                Poll::Pending
            }
            other => other,
        }
    }

    /// The next datagram message, while the request side is open.
    fn poll_datagrams(&mut self, cx: &Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        let Some(datagrams) = &mut self.datagrams else {
            return Poll::Pending;
        };
        match datagrams.poll_recv(cx) {
            Poll::Ready(Some(message)) => {
                self.count.add();
                Poll::Ready(Some(Ok(message.to_vec())))
            }
            Poll::Ready(None) => {
                self.datagrams = None;
                Poll::Pending
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// The request side has ended: delivers the datagram messages that are queued, and ends once
    /// no announced datagram is awaited any more.
    fn poll_ending(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        let Some(datagrams) = &mut self.datagrams else {
            // The call failed: nothing more is delivered.
            return Poll::Ready(None);
        };
        // The wait is polled first: the messages of every datagram it has counted are queued.
        let over = self.stragglers.poll_over(cx).is_ready();
        if let Some(message) = datagrams.try_recv() {
            self.count.add();
            return Poll::Ready(Some(Ok(message.to_vec())));
        }
        if over {
            self.datagrams = None;
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl Stream for MergedRequests {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if !this.requests_done {
            this.datagrams_first = !this.datagrams_first;
            if this.datagrams_first {
                if let Poll::Ready(item) = this.poll_datagrams(cx) {
                    return Poll::Ready(item);
                }
                if let Poll::Ready(item) = this.poll_requests(cx) {
                    return Poll::Ready(item);
                }
            } else {
                if let Poll::Ready(item) = this.poll_requests(cx) {
                    return Poll::Ready(item);
                }
                if let Poll::Ready(item) = this.poll_datagrams(cx) {
                    return Poll::Ready(item);
                }
            }
        }
        if this.requests_done {
            return this.poll_ending(cx);
        }
        Poll::Pending
    }
}

/// Where a call's response stands.
enum ResponsePhase {
    /// Frames and datagram messages are read as they arrive.
    Open,
    /// The response ended, with this terminal STATUS or none; the queued datagram messages are
    /// delivered before it, after waiting for the ones its `DATAGRAM_COUNT` announced if it has
    /// a STATUS.
    Draining(Option<ResponseFrame>),
    /// The terminal frame has been delivered; only the stream's own end remains.
    Ended,
}

/// The response frames of a call: its stream's frames, with the datagram messages merged in
/// after the HEADERS frame and ahead of the terminal STATUS. When the response ends with a
/// `DATAGRAM_COUNT` that announces datagrams that have not arrived yet, the STATUS waits for them,
/// until [`DATAGRAM_COUNT_WAIT`] has passed.
pub struct MergedResponses {
    /// Polled only when it woke, so a datagram does not also cost a poll of the stream.
    frames: Gated<BoxStream<ResponseFrame>>,
    datagrams: Option<queue::Receiver<Bytes>>,
    stragglers: Stragglers,
    /// Keeps the call's flow registered until the response ends.
    _guard: FlowGuard,
    /// The call's HEADERS frame has been read.
    headers_read: bool,
    phase: ResponsePhase,
    datagrams_first: bool,
}

impl MergedResponses {
    /// Merges `frames` with the datagram messages in `datagrams`, if the call receives any, and
    /// keeps `guard` until the response ends or is dropped. The datagrams `expected` says the
    /// response is owed are waited for when it ends.
    pub fn boxed(
        frames: BoxStream<ResponseFrame>,
        datagrams: Option<queue::Receiver<Bytes>>,
        expected: Expected,
        guard: FlowGuard,
    ) -> BoxStream<ResponseFrame> {
        Box::pin(Self {
            frames: Gated::new(frames),
            datagrams,
            stragglers: Stragglers::new(expected),
            _guard: guard,
            headers_read: false,
            phase: ResponsePhase::Open,
            datagrams_first: false,
        })
    }

    /// The next datagram message, which the call delivers only after its HEADERS.
    fn poll_datagrams(&mut self, cx: &Context<'_>) -> Poll<Option<Result<ResponseFrame>>> {
        if !self.headers_read {
            return Poll::Pending;
        }
        let Some(datagrams) = &mut self.datagrams else {
            return Poll::Pending;
        };
        match datagrams.poll_recv(cx) {
            Poll::Ready(Some(message)) => Poll::Ready(Some(Ok(ResponseFrame::Message(message)))),
            Poll::Ready(None) => {
                self.datagrams = None;
                Poll::Pending
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// The next frame of the stream. A terminal STATUS, or the end of the stream, starts the
    /// draining of the queued datagram messages instead of being returned.
    fn poll_frames(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<ResponseFrame>>> {
        match self.frames.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame {
                ResponseFrame::Status(_) => {
                    self.phase = ResponsePhase::Draining(Some(frame));
                    Poll::Pending
                }
                ResponseFrame::Headers(_) => {
                    self.headers_read = true;
                    Poll::Ready(Some(Ok(frame)))
                }
                ResponseFrame::Message(_) => Poll::Ready(Some(Ok(frame))),
            },
            Poll::Ready(None) => {
                self.phase = ResponsePhase::Draining(None);
                Poll::Pending
            }
            other => other,
        }
    }

    /// Whether the call is to wait for announced datagrams before it ends: its STATUS arrived,
    /// and it delivers datagram messages.
    const fn awaits_stragglers(&self) -> bool {
        matches!(self.phase, ResponsePhase::Draining(Some(_)))
            && self.headers_read
            && self.datagrams.is_some()
    }

    /// The next queued datagram message, if the call delivers them.
    fn next_queued(&mut self) -> Option<Bytes> {
        if self.headers_read {
            self.datagrams.as_mut()?.try_recv()
        } else {
            None
        }
    }
}

impl Stream for MergedResponses {
    type Item = Result<ResponseFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.datagrams_first = !this.datagrams_first;
        loop {
            match this.phase {
                ResponsePhase::Ended => return this.frames.poll_next_unpin(cx),
                ResponsePhase::Draining(_) => {
                    // The wait is polled first: the messages of every datagram it has counted
                    // are queued.
                    let over =
                        !this.awaits_stragglers() || this.stragglers.poll_over(cx).is_ready();
                    if let Some(message) = this.next_queued() {
                        return Poll::Ready(Some(Ok(ResponseFrame::Message(message))));
                    }
                    if !over {
                        return Poll::Pending;
                    }
                    this.datagrams = None;
                    return match std::mem::replace(&mut this.phase, ResponsePhase::Ended) {
                        ResponsePhase::Draining(Some(terminal)) => Poll::Ready(Some(Ok(terminal))),
                        _ => Poll::Ready(None),
                    };
                }
                ResponsePhase::Open => {}
            }

            if this.datagrams_first {
                if let Poll::Ready(item) = this.poll_datagrams(cx) {
                    return Poll::Ready(item);
                }
                if let Poll::Ready(item) = this.poll_frames(cx) {
                    return Poll::Ready(item);
                }
            } else {
                if let Poll::Ready(item) = this.poll_frames(cx) {
                    return Poll::Ready(item);
                }
                if let Poll::Ready(item) = this.poll_datagrams(cx) {
                    return Poll::Ready(item);
                }
            }
            if matches!(this.phase, ResponsePhase::Open) {
                return Poll::Pending;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use tokio::sync::{Notify, mpsc};

    use super::*;
    use crate::{Error, Status};

    /// A datagram transport the tests drive by hand.
    #[derive(Clone)]
    struct Fake(Arc<FakeInner>);

    struct FakeInner {
        prefix: Bytes,
        /// The largest datagram, or zero for a peer without datagrams.
        max: AtomicU64,
        sent: Mutex<Vec<Bytes>>,
        /// Sends wait while this is set.
        blocked: AtomicBool,
        unblocked: Notify,
        fail: AtomicBool,
        /// Datagrams that arrived and were not taken yet.
        incoming: Mutex<VecDeque<Bytes>>,
        arrived: Notify,
    }

    impl Fake {
        fn new(prefix: &[u8], max: u64) -> Self {
            Self(Arc::new(FakeInner {
                prefix: Bytes::copy_from_slice(prefix),
                max: AtomicU64::new(max),
                sent: Mutex::new(Vec::new()),
                blocked: AtomicBool::new(false),
                unblocked: Notify::new(),
                fail: AtomicBool::new(false),
                incoming: Mutex::new(VecDeque::new()),
                arrived: Notify::new(),
            }))
        }

        /// Delivers a datagram that arrived from the peer, prefix included.
        fn arrive(&self, datagram: &[u8]) {
            self.0
                .incoming
                .lock()
                .unwrap()
                .push_back(Bytes::copy_from_slice(datagram));
            self.0.arrived.notify_waiters();
        }

        /// Delivers a datagram of `flow` carrying `message`, as the peer would send it.
        fn arrive_for(&self, flow: u64, message: &[u8]) {
            self.arrive_batch(flow, &[message]);
        }

        /// Delivers a datagram of `flow` carrying `messages`, packed as the peer would send them.
        fn arrive_batch(&self, flow: u64, messages: &[&[u8]]) {
            let mut datagram = BytesMut::from(&flow_header(&self.0.prefix, flow).unwrap()[..]);
            for message in messages {
                put_message_frame(&mut datagram, MESSAGE_FRAME, message);
            }
            self.arrive(&datagram);
        }

        /// Takes the next datagram that arrived, if one did.
        fn take(&self) -> Option<Bytes> {
            self.0.incoming.lock().unwrap().pop_front()
        }

        fn sent(&self) -> Vec<Bytes> {
            self.0.sent.lock().unwrap().clone()
        }

        fn block_sends(&self, blocked: bool) {
            self.0.blocked.store(blocked, Ordering::SeqCst);
            self.0.unblocked.notify_waiters();
        }
    }

    impl DatagramTransport for Fake {
        fn prefix(&self) -> &[u8] {
            &self.0.prefix
        }

        fn max_size(&self) -> Option<usize> {
            match self.0.max.load(Ordering::SeqCst) {
                0 => None,
                max => Some(usize::try_from(max).unwrap()),
            }
        }

        async fn send(&self, datagram: Bytes) -> std::result::Result<(), Unsent> {
            loop {
                let unblocked = self.0.unblocked.notified();
                tokio::pin!(unblocked);
                // Register before checking, so an unblock in between is not missed.
                unblocked.as_mut().enable();
                // Quinn refuses a datagram larger than the path carries.
                if self.0.fail.load(Ordering::SeqCst)
                    || DatagramTransport::max_size(self).is_none_or(|max| datagram.len() > max)
                {
                    return Err(Unsent);
                }
                if !self.0.blocked.load(Ordering::SeqCst) {
                    self.0.sent.lock().unwrap().push(datagram);
                    return Ok(());
                }
                unblocked.await;
            }
        }

        async fn recv(&self) -> Option<Bytes> {
            loop {
                let arrived = self.0.arrived.notified();
                tokio::pin!(arrived);
                arrived.as_mut().enable();
                if let Some(datagram) = self.take() {
                    return Some(datagram);
                }
                arrived.await;
            }
        }
    }

    /// Lets the reader task process everything injected so far.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    const BIDI: RpcKind = RpcKind::BidirectionalStreaming;
    const MAX_FRAME: usize = crate::framing::DEFAULT_MAX_FRAME_SIZE;

    fn server(fake: &Fake) -> Demux {
        Demux::server(fake.clone(), None, MAX_FRAME)
    }

    fn drain(receiver: &mut queue::Receiver<Bytes>) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| receiver.try_recv())
            .map(|message| message.to_vec())
            .collect()
    }

    fn requests_of(flow: ServerFlow) -> (queue::Receiver<Bytes>, ServerFlow) {
        let mut flow = flow;
        (flow.requests.take().unwrap(), flow)
    }

    #[test]
    fn the_codec_matches_the_golden_vectors() {
        let mut entries: HashMap<String, HashMap<String, String>> = HashMap::new();
        for line in include_str!("../../testdata/wire-golden-vectors.txt").lines() {
            let line = line.trim();
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            let Some(rest) = name.trim().strip_prefix("datagram.") else {
                continue;
            };
            let (entry, field) = rest.rsplit_once('.').unwrap();
            entries
                .entry(entry.to_owned())
                .or_default()
                .insert(field.to_owned(), value.trim().to_owned());
        }
        assert!(entries.len() >= 5, "{entries:?}");

        for (name, fields) in entries {
            let flow = hex(&fields["flow"])
                .iter()
                .fold(0_u64, |flow, byte| flow << 8 | u64::from(*byte));
            let messages: Vec<Vec<u8>> = if fields["messages"].is_empty() {
                Vec::new()
            } else {
                fields["messages"].split(',').map(hex).collect()
            };
            let datagram = match (fields.get("datagram"), fields.get("received")) {
                (Some(datagram), None) => {
                    // A sender writes exactly these bytes.
                    let mut encoded = BytesMut::from(&flow_header(&[], flow).unwrap()[..]);
                    for message in &messages {
                        put_message_frame(&mut encoded, MESSAGE_FRAME, message);
                    }
                    assert_eq!(&encoded[..], hex(datagram).as_slice(), "{name}");
                    hex(datagram)
                }
                (None, Some(received)) => hex(received),
                other => panic!("{name} needs exactly one of .datagram and .received: {other:?}"),
            };

            // A receiver reads the same messages, or drops the datagram whole.
            let decoded = messages_of(&datagram, MAX_FRAME);
            if name == "truncated" {
                assert_eq!(decoded, None, "{name}");
            } else {
                assert_eq!(decoded, Some((flow, messages)), "{name}");
            }
        }
    }

    fn hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    /// The flow and messages of a datagram, or `None` when it is dropped whole, whether it has a
    /// flow or not.
    fn messages_of(datagram: &[u8], max_frame_size: usize) -> Option<(u64, Vec<Vec<u8>>)> {
        let Decoded { flow, messages } = decode(Bytes::copy_from_slice(datagram), max_frame_size)?;
        Some((
            flow,
            messages?.map(|(_, message)| message.to_vec()).collect(),
        ))
    }

    #[test]
    fn malformed_datagrams_are_dropped_whole() {
        assert!(messages_of(&[], MAX_FRAME).is_none());
        // Flow zero means no flow.
        assert!(messages_of(&[0x00, 0x00, 0x01, b'x'], MAX_FRAME).is_none());
        // A two-byte flow ID that stops after its first byte.
        assert!(messages_of(&[0x40], MAX_FRAME).is_none());
        // A frame header that stops, a length past the end, and a good message ahead of bad ones.
        assert!(messages_of(&[0x01, 0x00], MAX_FRAME).is_none());
        assert!(messages_of(&[0x01, 0x00, 0x05, b'x'], MAX_FRAME).is_none());
        assert!(messages_of(&[0x01, 0x00, 0x01, b'x', 0x00, 0x05, b'y'], MAX_FRAME).is_none());
        // A message over the maximum frame size drops the whole datagram, even after good ones.
        assert!(messages_of(&[0x01, 0x00, 0x01, b'x', 0x00, 0x03, 1, 2, 3], 2).is_none());
        assert!(messages_of(&[0x01, 0x00, 0x03, 1, 2, 3], 3).is_some());
        // A ZSTD_STREAM_MESSAGE needs a stream's order, so it drops the datagram, good messages
        // and all.
        assert!(messages_of(&[0x01, 0x00, 0x01, b'x', 0x05, 0x01, 0x00], MAX_FRAME).is_none());
    }

    #[test]
    fn non_minimal_encodings_and_other_frame_types_are_accepted() {
        // Flow 1 and a MESSAGE frame as two-byte varints, then an unknown frame.
        let datagram = [0x40, 0x01, 0x40, 0x00, 0x40, 0x01, b'x', 0x21, 0x01, 0xff];
        assert_eq!(
            messages_of(&datagram, MAX_FRAME),
            Some((1, vec![b"x".to_vec()]))
        );
        // A flow with no frames carries no messages.
        assert_eq!(messages_of(&[0x01], MAX_FRAME), Some((1, vec![])));
    }

    #[test]
    fn unreliable_marks_only_count_for_directions_that_stream() {
        assert_eq!(
            Unreliable::BOTH.effective(RpcKind::ServerStreaming),
            Unreliable::RESPONSES
        );
        assert_eq!(
            Unreliable::BOTH.effective(RpcKind::ClientStreaming),
            Unreliable::REQUESTS
        );
        assert_eq!(Unreliable::BOTH.effective(RpcKind::Unary), Unreliable::NONE);
        assert_eq!(Unreliable::BOTH.effective(BIDI), Unreliable::BOTH);
        assert_eq!(
            Unreliable::REQUESTS.union(Unreliable::RESPONSES),
            Unreliable::BOTH
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_server_flow_receives_its_datagrams_and_drops_what_does_not_fit() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(5, BIDI, &Accept::None).unwrap());

        for index in 0..QUEUE.values + 10 {
            fake.arrive_for(5, index.to_string().as_bytes());
        }
        // Other flows and garbage do not reach the call.
        fake.arrive_for(6, b"other");
        fake.arrive(&[]);
        fake.arrive(&[0x00, b'x']);
        settle().await;

        let received = drain(&mut requests);
        assert_eq!(received.len(), QUEUE.values);
        assert_eq!(received.first().unwrap(), b"0");
    }

    #[tokio::test(start_paused = true)]
    async fn datagrams_for_a_direction_that_does_not_stream_are_dropped() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        // A server-streaming call has one request message: no queue for request datagrams.
        let flow = demux
            .open_server_flow(5, RpcKind::ServerStreaming, &Accept::None)
            .unwrap();
        assert!(flow.requests.is_none());

        fake.arrive_for(5, b"ignored");
        settle().await;
        // They are not held for a later claim either: the flow exists.
        assert_eq!(
            demux
                .shared
                .state
                .lock_unpoisoned()
                .held
                .claim(5, Instant::now())
                .received,
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn datagrams_that_overtake_their_request_are_held_and_claimed_in_order() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let _other = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        for message in [&b"a"[..], b"b", b"c"] {
            fake.arrive_for(9, message);
        }
        settle().await;

        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(9, BIDI, &Accept::None).unwrap());

        assert_eq!(
            drain(&mut requests),
            [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn held_datagrams_expire() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let _other = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        fake.arrive_for(9, b"old");
        settle().await;
        tokio::time::sleep(
            HELD_DATAGRAM_TTL
                .checked_sub(Duration::from_millis(100))
                .unwrap(),
        )
        .await;
        fake.arrive_for(9, b"young");
        settle().await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(9, BIDI, &Accept::None).unwrap());

        // The first is past the 500 ms limit by now; the second is not.
        assert_eq!(drain(&mut requests), [b"young".to_vec()]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_holding_area_is_bounded_by_count_and_by_bytes() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let _other = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        for index in 0..MAX_HELD_DATAGRAMS + 20 {
            fake.arrive_for(2, &[u8::try_from(index).unwrap()]);
        }
        settle().await;
        let (mut by_count, _flow) =
            requests_of(demux.open_server_flow(2, BIDI, &Accept::None).unwrap());
        // The queue takes them all; the area kept the first 64.
        assert_eq!(drain(&mut by_count).len(), MAX_HELD_DATAGRAMS);

        let chunk = vec![0_u8; MAX_HELD_BYTES / 2 + 1];
        fake.arrive_for(3, &chunk);
        fake.arrive_for(3, &chunk);
        settle().await;
        let (mut by_bytes, _flow) =
            requests_of(demux.open_server_flow(3, BIDI, &Accept::None).unwrap());
        assert_eq!(drain(&mut by_bytes).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn late_datagrams_of_ended_flows_are_dropped_and_do_not_fill_the_holding_area() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        drop(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());
        for _ in 0..MAX_HELD_DATAGRAMS * 2 {
            fake.arrive_for(1, b"late");
        }
        settle().await;

        // Had the late datagrams been held, flow 2 would find the area full.
        for _ in 0..MAX_HELD_DATAGRAMS {
            fake.arrive_for(2, b"early");
        }
        settle().await;
        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(2, BIDI, &Accept::None).unwrap());
        assert_eq!(drain(&mut requests).len(), MAX_HELD_DATAGRAMS);
    }

    #[tokio::test(start_paused = true)]
    async fn ended_flows_are_remembered_up_to_twice_the_stream_limit() {
        let fake = Fake::new(&[], 1200);
        // A stream limit of 10 remembers 20 flows, but never fewer than the minimum.
        let demux = Demux::server(fake.clone(), Some(10), MAX_FRAME);
        let capacity = (10 * ENDED_FLOWS_PER_STREAM).max(MIN_ENDED_FLOWS);
        let flows = u64::try_from(capacity).unwrap() + 5;
        for flow in 1..=flows {
            drop(demux.open_server_flow(flow, BIDI, &Accept::None).unwrap());
        }
        let _live = demux
            .open_server_flow(flows + 1, BIDI, &Accept::None)
            .unwrap();

        // The oldest five are forgotten and held like unknown flows; the rest are dropped.
        fake.arrive_for(1, b"forgotten");
        fake.arrive_for(flows, b"remembered");
        settle().await;

        let (forgotten, remembered) = {
            let mut state = demux.shared.state.lock_unpoisoned();
            (
                state.held.claim(1, Instant::now()),
                state.held.claim(flows, Instant::now()),
            )
        };
        assert_eq!(forgotten.messages.len(), 1);
        assert_eq!(remembered.received, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_flow_cannot_be_opened_twice_but_an_ended_one_can() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);

        let first = demux.open_server_flow(4, BIDI, &Accept::None).unwrap();
        assert_eq!(
            demux.open_server_flow(4, BIDI, &Accept::None).err(),
            Some(FlowInUse)
        );
        assert_eq!(
            demux.open_server_flow(0, BIDI, &Accept::None).err(),
            Some(FlowInUse)
        );
        assert_eq!(
            demux
                .open_server_flow(varint::MAX + 1, BIDI, &Accept::None)
                .err(),
            Some(FlowInUse)
        );

        drop(first);
        assert!(demux.open_server_flow(4, BIDI, &Accept::None).is_ok());
    }

    /// Sends `messages` as one batch; returns those that went on the stream.
    async fn send_all(sender: &FlowSender, messages: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut batch: Vec<Bytes> = messages.iter().map(|m| Bytes::copy_from_slice(m)).collect();
        let mut on_stream = Vec::new();
        sender
            .send_batch(&mut batch, &mut on_stream, std::future::pending())
            .await;
        assert_eq!(batch, Vec::<Bytes>::new());
        on_stream.iter().map(|message| message.to_vec()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn the_flow_sender_uses_a_datagram_only_when_the_whole_frame_fits() {
        // A `WebTransport`-like prefix of one byte, a one-byte flow and a two-byte frame header
        // leave 95 bytes of message in a datagram of 100: past 63 bytes the length takes two bytes.
        let fake = Fake::new(&[0x2a], 100);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();

        assert_eq!(
            send_all(&flow.sender, &[&[7; 95]]).await,
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(send_all(&flow.sender, &[&[7; 96]]).await, [vec![7; 96]]);
        let sent = fake.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].len(), 100);
        assert_eq!(&sent[0][..5], &[0x2a, 0x01, 0x00, 0x40, 95]);

        // The limit moves with the connection's path MTU. A datagram that outgrew a shrunken path
        // is refused and its message goes on the stream, and the sender reads the limit again.
        fake.0.max.store(50, Ordering::SeqCst);
        assert_eq!(send_all(&flow.sender, &[&[7; 95]]).await.len(), 1);
        assert_eq!(
            send_all(&flow.sender, &[&[7; 46]]).await,
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(fake.sent().len(), 2);
        // A limit that grew is read again once the last reading is old enough.
        fake.0.max.store(100, Ordering::SeqCst);
        assert_eq!(send_all(&flow.sender, &[&[7; 95]]).await.len(), 1);
        tokio::time::advance(MAX_SIZE_REUSE).await;
        assert_eq!(
            send_all(&flow.sender, &[&[7; 95]]).await,
            Vec::<Vec<u8>>::new()
        );

        // No datagrams at all, or a refused send, mean the stream.
        fake.0.max.store(0, Ordering::SeqCst);
        tokio::time::advance(MAX_SIZE_REUSE).await;
        assert_eq!(send_all(&flow.sender, &[b"x"]).await.len(), 1);
        fake.0.max.store(100, Ordering::SeqCst);
        tokio::time::advance(MAX_SIZE_REUSE).await;
        fake.0.fail.store(true, Ordering::SeqCst);
        assert_eq!(send_all(&flow.sender, &[b"x"]).await.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_flow_sender_packs_ready_messages_into_as_few_datagrams_as_fit() {
        let fake = Fake::new(&[], 40);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();

        // Flow (1) + frames of 2 + 10 bytes: three fit in a datagram of 40, the fourth does not.
        let messages: Vec<&[u8]> = vec![&[1; 10], &[2; 10], &[3; 10], &[4; 10], &[5; 10]];
        assert_eq!(
            send_all(&flow.sender, &messages).await,
            Vec::<Vec<u8>>::new()
        );

        let sent = fake.sent();
        assert_eq!(sent.len(), 2);
        let unpack = |datagram: &Bytes| messages_of(datagram, MAX_FRAME).unwrap();
        assert_eq!(
            unpack(&sent[0]),
            (1, vec![vec![1; 10], vec![2; 10], vec![3; 10]])
        );
        assert_eq!(unpack(&sent[1]), (1, vec![vec![4; 10], vec![5; 10]]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_message_too_large_for_a_datagram_goes_on_the_stream_without_splitting_the_batch() {
        let fake = Fake::new(&[], 40);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();

        let on_stream = send_all(&flow.sender, &[b"a", &[9; 60], b"b", &[8; 70], b"c"]).await;

        // Order among the messages that fit is kept, and so is the order of those that do not.
        assert_eq!(on_stream, [vec![9; 60], vec![8; 70]]);
        let sent = fake.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            messages_of(&sent[0], MAX_FRAME),
            Some((1, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_datagram_sends_all_of_its_messages_on_the_stream() {
        let fake = Fake::new(&[], 40);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        fake.0.fail.store(true, Ordering::SeqCst);

        let messages: Vec<&[u8]> = vec![&[1; 10], &[2; 10], &[3; 10], &[4; 10]];
        let on_stream = send_all(&flow.sender, &messages).await;

        assert_eq!(
            on_stream,
            [vec![1; 10], vec![2; 10], vec![3; 10], vec![4; 10]]
        );
        assert_eq!(fake.sent(), Vec::<Bytes>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn messages_over_the_maximum_frame_size_stay_off_datagrams() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::server(fake.clone(), None, 8);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();

        let on_stream = send_all(&flow.sender, &[&[1; 8], &[2; 9]]).await;

        assert_eq!(on_stream, [vec![2; 9]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_datagram_with_several_messages_reaches_the_call_in_order() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(3, BIDI, &Accept::None).unwrap());

        fake.arrive_batch(3, &[b"a", b"", b"c"]);
        settle().await;

        assert_eq!(drain(&mut requests), [b"a".to_vec(), vec![], b"c".to_vec()]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_malformed_datagram_delivers_nothing_and_holds_no_messages() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(3, BIDI, &Accept::None).unwrap());

        // A good message followed by a truncated frame, for a live flow and for an unknown one.
        fake.arrive(&[0x03, 0x00, 0x01, b'a', 0x00, 0x09, b'b']);
        fake.arrive(&[0x09, 0x00, 0x01, b'a', 0x00, 0x09, b'b']);
        settle().await;

        assert_eq!(drain(&mut requests), Vec::<Vec<u8>>::new());
        // Nothing of it is held for the unknown flow, but it is counted for when that flow opens.
        let claimed = demux
            .shared
            .state
            .lock_unpoisoned()
            .held
            .claim(9, Instant::now());
        assert!(claimed.messages.is_empty());
        assert_eq!(claimed.received, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_datagrams_of_a_session_are_filtered_and_stripped_by_its_prefix() {
        let fake = Fake::new(&[0x2a], 1200);
        let demux = server(&fake);
        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());

        fake.arrive_for(1, b"mine");
        // Another session's datagram, and one without any prefix.
        fake.arrive(&[0x2b, 0x01, 0x00, 0x01, b'o']);
        fake.arrive(&[0x01, 0x00, 0x01, b'n']);
        settle().await;

        assert_eq!(drain(&mut requests), [b"mine".to_vec()]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_that_must_wait_ends_with_its_call_and_never_blocks_a_send_with_room() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let first = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        fake.block_sends(true);

        // The send buffer is full: the send waits until the call is cancelled.
        let started = Instant::now();
        let cancelled = tokio::time::sleep(Duration::from_millis(50));
        let mut batch = vec![Bytes::from_static(b"x")];
        let mut on_stream = Vec::new();
        first
            .sender
            .send_batch(&mut batch, &mut on_stream, cancelled)
            .await;
        assert_eq!(on_stream.len(), 1);
        assert_eq!(started.elapsed(), Duration::from_millis(50));
        assert_eq!(fake.sent(), Vec::<Bytes>::new());

        // A send that waits completes when room appears.
        let sender = first.sender.clone();
        let waiting = tokio::spawn(async move { send_all(&sender, &[b"y"]).await });
        settle().await;
        assert!(!waiting.is_finished());
        fake.block_sends(false);
        assert_eq!(waiting.await.unwrap(), Vec::<Vec<u8>>::new());
        assert_eq!(fake.sent().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn client_flows_count_up_from_one_on_every_connection() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake, MAX_FRAME);

        let first = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let second = demux
            .open_client_flow(BIDI, Unreliable::RESPONSES, Accept::None)
            .unwrap();
        assert_eq!((first.flow, second.flow), (1, 2));
        assert!(first.requests.is_some() && second.requests.is_none());
        assert!(first.responses.is_some());

        // A reconnect has a fresh demultiplexer, and so a fresh allocator.
        let reconnected = Demux::client(Fake::new(&[], 1200), MAX_FRAME);
        assert_eq!(
            reconnected
                .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
                .unwrap()
                .flow,
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_call_gets_a_flow_only_when_it_could_use_one() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);

        assert!(
            demux
                .open_client_flow(BIDI, Unreliable::NONE, Accept::None)
                .is_none()
        );
        assert!(
            demux
                .open_client_flow(RpcKind::Unary, Unreliable::BOTH, Accept::None)
                .is_none()
        );
        // Requests do not stream in a server-streaming call, so there is nothing to mark.
        assert!(
            demux
                .open_client_flow(RpcKind::ServerStreaming, Unreliable::REQUESTS, Accept::None)
                .is_none()
        );
        assert!(
            demux
                .open_client_flow(
                    RpcKind::ServerStreaming,
                    Unreliable::RESPONSES,
                    Accept::None
                )
                .is_some()
        );

        // The peer does not accept datagrams.
        fake.0.max.store(0, Ordering::SeqCst);
        tokio::time::advance(MAX_SIZE_REUSE).await;
        assert!(!demux.supported());
        assert!(
            demux
                .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_does_not_hold_datagrams_for_unknown_flows() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let _flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        fake.arrive_for(77, b"stray");
        settle().await;

        assert_eq!(
            demux
                .shared
                .state
                .lock_unpoisoned()
                .held
                .claim(77, Instant::now())
                .received,
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_reader_stops_with_the_demultiplexer() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        settle().await;
        assert!(Arc::strong_count(&fake.0) > 1);

        drop(demux);
        drop(flow);
        settle().await;

        // Nothing holds the transport any more: the reader is gone.
        assert_eq!(Arc::strong_count(&fake.0), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn readers_start_only_where_datagrams_can_arrive() {
        // A server starts reading as soon as the peer accepts datagrams.
        let accepting = server(&Fake::new(&[], 1200));
        assert!(accepting.reader.lock_unpoisoned().is_some());

        // A peer without datagrams costs no task, and neither does a client without flows.
        let refusing = server(&Fake::new(&[], 0));
        assert!(refusing.reader.lock_unpoisoned().is_none());
        let client = Demux::client(Fake::new(&[], 1200), MAX_FRAME);
        assert!(client.reader.lock_unpoisoned().is_none());
        let _flow = client
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        assert!(client.reader.lock_unpoisoned().is_some());
    }

    /// A request stream the test feeds by hand.
    fn channel_stream<T: Send + 'static>() -> (mpsc::UnboundedSender<Result<T>>, BoxStream<T>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        (sender, Box::pin(stream))
    }

    #[tokio::test(start_paused = true)]
    async fn requests_merge_stream_messages_and_datagram_messages_and_count_the_latter() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (requests, flow) = requests_of(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());
        let (stream_tx, stream) = channel_stream::<Vec<u8>>();
        let count = MessageCount::new();
        let mut merged =
            MergedRequests::boxed(stream, requests, flow.expected.clone(), count.clone());

        stream_tx.send(Ok(b"s1".to_vec())).unwrap();
        fake.arrive_for(1, b"d1");
        settle().await;
        let mut got = vec![
            merged.next().await.unwrap().unwrap(),
            merged.next().await.unwrap().unwrap(),
        ];
        got.sort();
        assert_eq!(got, [b"d1".to_vec(), b"s1".to_vec()]);
        assert_eq!(count.get(), 1);

        // A pending read wakes for a datagram.
        let waiting = tokio::spawn(async move { merged.next().await.unwrap().unwrap() });
        settle().await;
        assert!(!waiting.is_finished());
        fake.arrive_for(1, b"d2");
        assert_eq!(waiting.await.unwrap(), b"d2");
        assert_eq!(count.get(), 2);
        drop(flow);
    }

    #[tokio::test(start_paused = true)]
    async fn requests_deliver_the_datagrams_queued_before_the_end_then_drop_later_ones() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (requests, flow) = requests_of(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());
        let (stream_tx, stream) = channel_stream::<Vec<u8>>();
        let mut merged =
            MergedRequests::boxed(stream, requests, flow.expected.clone(), MessageCount::new());

        fake.arrive_for(1, b"queued");
        settle().await;
        stream_tx.send(Ok(b"last".to_vec())).unwrap();
        drop(stream_tx);

        let mut delivered = vec![
            merged.next().await.unwrap().unwrap(),
            merged.next().await.unwrap().unwrap(),
        ];
        delivered.sort();
        assert_eq!(delivered, [b"last".to_vec(), b"queued".to_vec()]);
        assert!(merged.next().await.is_none());

        // The direction has ended: the queue is gone, and datagrams for it are dropped.
        fake.arrive_for(1, b"late");
        settle().await;
        assert!(merged.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_request_stream_ends_the_merged_requests_at_once() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (requests, flow) = requests_of(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());
        let (stream_tx, stream) = channel_stream::<Vec<u8>>();
        let mut merged =
            MergedRequests::boxed(stream, requests, flow.expected.clone(), MessageCount::new());

        stream_tx
            .send(Err(Error::from(Status::cancelled("reset"))))
            .unwrap();

        let error = merged.next().await.unwrap().unwrap_err();
        assert_eq!(error.into_status().code(), crate::Code::Cancelled);
        fake.arrive_for(1, b"never delivered");
        settle().await;
        assert!(merged.next().await.is_none());
    }

    /// A server call with merged requests: the datagram transport, the stream the test ends by
    /// hand, the `DATAGRAM_COUNT` cell the request pump would fill, and the merged requests.
    struct MergedCall {
        fake: Fake,
        _demux: Demux,
        /// Keeps the call's flow registered.
        _flow: ServerFlow,
        stream_tx: mpsc::UnboundedSender<Result<Vec<u8>>>,
        announced: Announced,
        merged: BoxStream<Vec<u8>>,
    }

    fn merged_call() -> MergedCall {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (requests, flow) = requests_of(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());
        let (stream_tx, stream) = channel_stream::<Vec<u8>>();
        let announced = flow.expected.announced();
        let merged =
            MergedRequests::boxed(stream, requests, flow.expected.clone(), MessageCount::new());
        MergedCall {
            fake,
            _demux: demux,
            _flow: flow,
            stream_tx,
            announced,
            merged,
        }
    }

    /// Reads merged requests to their end, sorted, and how long that took.
    async fn read_to_end(merged: &mut BoxStream<Vec<u8>>) -> (Vec<Vec<u8>>, Duration) {
        let started = Instant::now();
        let mut delivered = Vec::new();
        while let Some(message) = merged.next().await {
            delivered.push(message.unwrap());
        }
        delivered.sort();
        (delivered, started.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn requests_end_at_once_when_the_announced_datagrams_have_arrived() {
        let mut call = merged_call();
        // The datagrams and the end of the stream arrive together, and nothing yields to the
        // reader task before the end is seen.
        for index in 0..3_u8 {
            call.fake.arrive_for(1, &[index]);
        }
        call.announced.set(3);
        drop(call.stream_tx);

        // The call waits for the reader to hand over what the count announced, but not for the
        // bound.
        let (delivered, elapsed) = read_to_end(&mut call.merged).await;

        assert_eq!(delivered, [vec![0], vec![1], vec![2]]);
        assert!(elapsed < DATAGRAM_COUNT_WAIT, "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn requests_wait_for_the_datagrams_that_are_still_to_arrive() {
        let mut call = merged_call();
        call.fake.arrive_for(1, b"early");
        settle().await;
        call.announced.set(3);
        drop(call.stream_tx);
        let reading = tokio::spawn(async move { read_to_end(&mut call.merged).await });

        // The end is held back while two of the three datagrams are missing, and what is queued
        // is delivered meanwhile.
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(!reading.is_finished());
        call.fake.arrive_for(1, b"second");
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(!reading.is_finished());
        call.fake.arrive_for(1, b"third");

        let (delivered, elapsed) = reading.await.unwrap();
        assert_eq!(
            delivered,
            [b"early".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
        // It ended when the last datagram arrived, before the bound.
        assert!(elapsed < DATAGRAM_COUNT_WAIT, "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn requests_give_up_on_missing_datagrams_after_the_bound() {
        let mut call = merged_call();
        call.fake.arrive_for(1, b"only");
        settle().await;
        call.announced.set(2);
        drop(call.stream_tx);

        let (delivered, elapsed) = read_to_end(&mut call.merged).await;

        assert_eq!(delivered, [b"only".to_vec()]);
        assert!(elapsed >= DATAGRAM_COUNT_WAIT, "{elapsed:?}");
        assert!(elapsed < DATAGRAM_COUNT_WAIT * 2, "{elapsed:?}");
        // The datagram that is still missing is dropped if it comes later.
        call.fake.arrive_for(1, b"late");
        settle().await;
        assert!(call.merged.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn requests_without_a_count_end_at_once_with_what_is_queued() {
        let mut call = merged_call();
        call.fake.arrive_for(1, b"queued");
        settle().await;
        drop(call.stream_tx);

        let (delivered, elapsed) = read_to_end(&mut call.merged).await;

        assert_eq!(delivered, [b"queued".to_vec()]);
        assert_eq!(elapsed, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_request_stream_does_not_wait_for_announced_datagrams() {
        let mut call = merged_call();
        call.announced.set(5);
        call.stream_tx
            .send(Err(Error::from(Status::cancelled("reset"))))
            .unwrap();

        let started = Instant::now();
        assert!(call.merged.next().await.unwrap().is_err());
        assert!(call.merged.next().await.is_none());
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn datagrams_that_are_dropped_still_count_as_received() {
        let mut call = merged_call();
        // A malformed datagram, and as many good ones as the queue holds plus one that does not
        // fit: every one of them arrived.
        call.fake
            .arrive(&[0x01, 0x00, 0x01, b'a', 0x00, 0x09, b'b']);
        for index in 0..=QUEUE.values {
            call.fake.arrive_for(1, index.to_string().as_bytes());
        }
        settle().await;
        call.announced.set(u64::try_from(QUEUE.values).unwrap() + 2);
        drop(call.stream_tx);

        let (delivered, elapsed) = read_to_end(&mut call.merged).await;

        assert_eq!(delivered.len(), QUEUE.values);
        assert_eq!(elapsed, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn datagrams_held_before_the_request_count_as_received() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let _other = demux.open_server_flow(2, BIDI, &Accept::None).unwrap();
        // A good datagram and a malformed one overtake the REQUEST of flow 9.
        fake.arrive_for(9, b"held");
        fake.arrive(&[0x09, 0x00, 0x09]);
        settle().await;

        let (requests, flow) = requests_of(demux.open_server_flow(9, BIDI, &Accept::None).unwrap());
        let (stream_tx, stream) = channel_stream::<Vec<u8>>();
        flow.expected.announced().set(2);
        let mut merged =
            MergedRequests::boxed(stream, requests, flow.expected.clone(), MessageCount::new());
        drop(stream_tx);

        let (delivered, elapsed) = read_to_end(&mut merged).await;

        assert_eq!(delivered, [b"held".to_vec()]);
        assert_eq!(elapsed, Duration::ZERO);
    }

    #[test]
    fn datagrams_the_holding_area_has_no_room_for_are_counted_with_the_flow() {
        let now = Instant::now();
        let mut held = HeldDatagrams::default();
        let datagram = |flow: u8| {
            decode(Bytes::from(vec![flow, 0x00, 0x01, b'x']), MAX_FRAME)
                .unwrap()
                .messages
        };
        for _ in 0..MAX_HELD_DATAGRAMS {
            held.hold(9, datagram(9), now);
        }
        // Two more for the same flow find no room; one for a flow with nothing held is lost.
        held.hold(9, datagram(9), now);
        held.hold(9, datagram(9), now);
        held.hold(8, datagram(8), now);

        let claimed = held.claim(9, now);

        assert_eq!(claimed.messages.len(), MAX_HELD_DATAGRAMS);
        assert_eq!(
            claimed.received,
            u64::try_from(MAX_HELD_DATAGRAMS).unwrap() + 2
        );
        assert_eq!(held.claim(8, now).received, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn responses_wait_for_the_announced_datagrams_ahead_of_the_status() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let id = flow.flow;
        let announced = flow.expected.announced();
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        frames_tx.send(Ok(headers())).unwrap();
        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        fake.arrive_for(id, b"a");
        settle().await;
        announced.set(3);
        frames_tx.send(Ok(ok())).unwrap();
        let reading = tokio::spawn(async move {
            let started = Instant::now();
            let mut frames = Vec::new();
            loop {
                let frame = merged.next().await.unwrap().unwrap();
                let end = frame == ok();
                frames.push(frame);
                if end {
                    return (frames, started.elapsed());
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(!reading.is_finished(), "the STATUS must wait");
        fake.arrive_for(id, b"b");
        fake.arrive_for(id, b"c");

        let (frames, elapsed) = reading.await.unwrap();
        // The STATUS comes last, after every announced datagram's message.
        assert_eq!(frames.last(), Some(&ok()));
        let mut messages: Vec<_> = frames.iter().take(3).cloned().collect();
        messages.sort_by_key(|frame| format!("{frame:?}"));
        assert_eq!(messages, [message(b"a"), message(b"b"), message(b"c")]);
        assert!(elapsed < DATAGRAM_COUNT_WAIT, "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn responses_end_at_once_when_the_announced_datagrams_have_arrived() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        flow.expected.announced().set(3);
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        frames_tx.send(Ok(headers())).unwrap();
        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        // The datagrams and the STATUS arrive together, and nothing yields to the reader task
        // before the STATUS is seen.
        for index in 0..3_u8 {
            fake.arrive_for(flow.flow, &[index]);
        }
        frames_tx.send(Ok(ok())).unwrap();

        let started = Instant::now();
        let mut messages = Vec::new();
        loop {
            let frame = merged.next().await.unwrap().unwrap();
            if frame == ok() {
                break;
            }
            messages.push(format!("{frame:?}"));
        }
        messages.sort();
        assert_eq!(messages.len(), 3, "{messages:?}");
        assert!(started.elapsed() < DATAGRAM_COUNT_WAIT);
    }

    #[tokio::test(start_paused = true)]
    async fn responses_give_up_on_missing_datagrams_after_the_bound() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let id = flow.flow;
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        flow.expected.announced().set(2);
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        frames_tx.send(Ok(headers())).unwrap();
        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        fake.arrive_for(id, b"only");
        settle().await;
        frames_tx.send(Ok(ok())).unwrap();

        let started = Instant::now();
        assert_eq!(merged.next().await.unwrap().unwrap(), message(b"only"));
        assert_eq!(merged.next().await.unwrap().unwrap(), ok());
        let elapsed = started.elapsed();
        assert!(elapsed >= DATAGRAM_COUNT_WAIT, "{elapsed:?}");
        assert!(elapsed < DATAGRAM_COUNT_WAIT * 2, "{elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn responses_that_do_not_deliver_datagrams_ignore_the_count() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        // A client-streaming call has one response, which has no datagrams to wait for.
        let flow = demux
            .open_client_flow(RpcKind::ClientStreaming, Unreliable::REQUESTS, Accept::None)
            .unwrap();
        flow.expected.announced().set(5);
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);
        frames_tx.send(Ok(headers())).unwrap();
        frames_tx.send(Ok(ok())).unwrap();

        let started = Instant::now();
        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        assert_eq!(merged.next().await.unwrap().unwrap(), ok());
        assert_eq!(started.elapsed(), Duration::ZERO);

        // Nor does a response whose HEADERS were never read: no datagram is delivered.
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        flow.expected.announced().set(5);
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);
        frames_tx.send(Ok(ok())).unwrap();
        let started = Instant::now();
        assert_eq!(merged.next().await.unwrap().unwrap(), ok());
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    fn headers() -> ResponseFrame {
        ResponseFrame::Headers(crate::Metadata::new())
    }

    fn message(body: &'static [u8]) -> ResponseFrame {
        ResponseFrame::message(body)
    }

    fn ok() -> ResponseFrame {
        ResponseFrame::Status(Status::ok())
    }

    #[tokio::test(start_paused = true)]
    async fn responses_deliver_datagram_messages_only_after_the_headers() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        fake.arrive_for(flow.flow, b"early");
        settle().await;
        let waiting = tokio::spawn(async move {
            let first = merged.next().await.unwrap().unwrap();
            let second = merged.next().await.unwrap().unwrap();
            (first, second)
        });
        settle().await;
        // The datagram waits in the call's queue while the headers are unread.
        assert!(!waiting.is_finished());

        frames_tx.send(Ok(headers())).unwrap();
        let (first, second) = waiting.await.unwrap();
        assert_eq!(first, headers());
        assert_eq!(second, message(b"early"));
    }

    #[tokio::test(start_paused = true)]
    async fn responses_deliver_queued_datagram_messages_before_the_status() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        frames_tx.send(Ok(headers())).unwrap();
        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        fake.arrive_for(flow.flow, b"a");
        fake.arrive_for(flow.flow, b"b");
        settle().await;
        frames_tx.send(Ok(message(b"stream"))).unwrap();
        frames_tx.send(Ok(ok())).unwrap();

        let mut messages = Vec::new();
        loop {
            let frame = merged.next().await.unwrap().unwrap();
            if frame == ok() {
                break;
            }
            messages.push(frame);
        }
        messages.sort_by_key(|frame| format!("{frame:?}"));
        assert_eq!(messages, [message(b"a"), message(b"b"), message(b"stream")]);

        // Whatever the transport still sends after the status is the caller's to reject.
        frames_tx.send(Ok(message(b"after"))).unwrap();
        assert_eq!(merged.next().await.unwrap().unwrap(), message(b"after"));
        drop(frames_tx);
        assert!(merged.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn responses_without_headers_or_with_an_error_drop_queued_datagrams() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        fake.arrive_for(flow.flow, b"before headers");
        settle().await;
        frames_tx.send(Ok(ok())).unwrap();

        // No HEADERS frame was read, so nothing the datagrams carry is delivered.
        assert_eq!(merged.next().await.unwrap().unwrap(), ok());
        drop(frames_tx);
        assert!(merged.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn the_response_stream_holds_the_flow_until_it_is_dropped() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake, MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let id = flow.flow;
        let (_frames_tx, frames) = channel_stream::<ResponseFrame>();
        let merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);
        assert!(demux.shared.state.lock_unpoisoned().flows.contains_key(&id));

        drop(merged);

        assert!(!demux.shared.state.lock_unpoisoned().flows.contains_key(&id));
    }

    #[tokio::test(start_paused = true)]
    async fn a_response_stream_that_does_not_stream_still_holds_its_flow() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        // A client-streaming call has one response: only the requests are unreliable.
        let flow = demux
            .open_client_flow(RpcKind::ClientStreaming, Unreliable::REQUESTS, Accept::None)
            .unwrap();
        assert!(flow.responses.is_none() && flow.requests.is_some());
        let id = flow.flow;
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.expected, flow.guard);

        fake.arrive_for(id, b"dropped");
        frames_tx.send(Ok(headers())).unwrap();
        frames_tx.send(Ok(message(b"only"))).unwrap();
        frames_tx.send(Ok(ok())).unwrap();
        settle().await;

        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        assert_eq!(merged.next().await.unwrap().unwrap(), message(b"only"));
        assert_eq!(merged.next().await.unwrap().unwrap(), ok());
    }
}
