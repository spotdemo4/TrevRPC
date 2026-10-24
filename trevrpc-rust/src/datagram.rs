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
//!   travel on the stream instead.
//! - [`MergedRequests`] and [`MergedResponses`] merge a call's datagram messages with the MESSAGE
//!   frames of its stream, so the rest of the runtime sees one ordinary message stream.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use futures_util::FutureExt as _;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::compression::{Accept, Dict, inflate};
use crate::sync::MutexExt as _;
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
/// The most datagrams a call dispatches when its stream ends, before it ends regardless.
const MAX_DRAINED: usize = 1024;
/// How long a call waits for its datagrams to reach the network before it ends regardless.
const FLUSH_WAIT: Duration = Duration::from_millis(2);
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

    /// Takes the next datagram the transport has already received, without waiting.
    fn try_recv(&self) -> Option<Bytes>;

    /// How many datagrams the transport has written to the network so far, or `None` if it cannot
    /// tell. Datagrams queued by [`DatagramTransport::send`] count once they are on the wire.
    fn transmitted(&self) -> Option<u64> {
        None
    }
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

    fn try_recv(&self) -> Option<Bytes> {
        None
    }
}

/// A [`DatagramTransport`] without its type parameter, with the future of a blocked send boxed:
/// what [`FlowSender`] and the demultiplexer's state need of the transport.
trait SendLink: Send + Sync + 'static {
    fn prefix(&self) -> &[u8];

    fn max_size(&self) -> Option<usize>;

    fn try_recv(&self) -> Option<Bytes>;

    fn transmitted(&self) -> Option<u64>;

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

    fn try_recv(&self) -> Option<Bytes> {
        DatagramTransport::try_recv(self)
    }

    fn transmitted(&self) -> Option<u64> {
        DatagramTransport::transmitted(self)
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

/// Splits a datagram, with its transport prefix already removed, into its flow ID and messages.
///
/// Returns `None`, dropping the datagram whole, when it does not start with a flow ID, when the
/// flow is zero (which means no flow), when its frames do not exactly fill it, when a message
/// is larger than `max_frame_size`, or when it holds a `ZSTD_STREAM_MESSAGE`, which needs the
/// ordered delivery of a stream. Other frames that are not messages are skipped.
fn decode(datagram: Bytes, max_frame_size: usize) -> Option<(u64, Messages)> {
    let (flow, flow_len) = varint::decode(&datagram)?;
    if flow == 0 {
        return None;
    }
    let mut offset = flow_len;
    while offset < datagram.len() {
        let (frame_type, start, end) = next_frame(&datagram, offset)?;
        if (frame_type == MESSAGE_FRAME && end.saturating_sub(start) > max_frame_size)
            || frame_type == crate::framing::frame_type::ZSTD_STREAM_MESSAGE
        {
            return None;
        }
        offset = end;
    }
    Some((
        flow,
        Messages {
            datagram,
            offset: flow_len,
        },
    ))
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

/// A datagram message held for a flow whose REQUEST has not arrived.
struct Held {
    flow: u64,
    messages: Messages,
    at: Instant,
}

impl Held {
    /// The bytes the held datagram occupies.
    const fn len(&self) -> usize {
        self.messages.datagram.len()
    }
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

    /// Holds a datagram unless that would exceed the bounds, in which case it is dropped.
    fn hold(&mut self, flow: u64, messages: Messages, now: Instant) {
        self.expire(now);
        let len = messages.datagram.len();
        if self.held.len() >= MAX_HELD_DATAGRAMS || self.bytes.saturating_add(len) > MAX_HELD_BYTES
        {
            return;
        }
        self.bytes = self.bytes.saturating_add(len);
        self.held.push_back(Held {
            flow,
            messages,
            at: now,
        });
    }

    /// Takes the unexpired datagrams held for `flow`, oldest first.
    fn claim(&mut self, flow: u64, now: Instant) -> Vec<Messages> {
        self.expire(now);
        if !self.held.iter().any(|held| held.flow == flow) {
            return Vec::new();
        }
        let mut claimed = Vec::new();
        let mut kept = VecDeque::with_capacity(self.held.len());
        for held in self.held.drain(..) {
            if held.flow == flow {
                self.bytes = self.bytes.saturating_sub(held.len());
                claimed.push(held.messages);
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

/// A registered flow: where its datagram messages go, if its call receives any.
struct Slot {
    sink: Option<queue::Sender<Bytes>>,
    /// The dictionaries the call accepts on compressed messages.
    accept: Accept,
}

struct State {
    flows: HashMap<u64, Slot>,
    ended: EndedFlows,
    held: HeldDatagrams,
}

struct Shared {
    role: Role,
    link: Arc<dyn SendLink>,
    state: Mutex<State>,
    /// The largest message a datagram may carry, and the largest this side sends in one.
    max_frame_size: usize,
    /// The next flow ID a client allocates.
    next_flow: AtomicU64,
    /// How many datagrams the senders of this connection have queued.
    queued: Arc<AtomicU64>,
}

impl Shared {
    fn dispatch(&self, datagram: Bytes) {
        let prefix_len = self.link.prefix().len();
        let datagram = if prefix_len == 0 {
            datagram
        } else {
            // A datagram of another session, or without the prefix, is not ours.
            if datagram.get(..prefix_len) != Some(self.link.prefix()) {
                return;
            }
            datagram.slice(prefix_len..)
        };
        let Some((flow, messages)) = decode(datagram, self.max_frame_size) else {
            return;
        };

        let mut state = self.state.lock_unpoisoned();
        if let Some(slot) = state.flows.get(&flow) {
            if let Some(sink) = &slot.sink
                && let Some(messages) = messages.inflate(&slot.accept, self.max_frame_size)
            {
                for message in messages {
                    let size = message.len();
                    // A full queue or a finished direction drops the message.
                    let _ = sink.try_send(message, size);
                }
            }
            return;
        }
        if self.role == Role::Server && !state.ended.contains(flow) {
            state.held.hold(flow, messages, Instant::now());
        }
    }

    /// Dispatches the datagrams the transport has received already, without waiting.
    ///
    /// A call whose stream has just ended uses this to deliver the datagrams that arrived before
    /// the end: the reader task moves them on its own schedule, which may lag behind the stream.
    fn drain(&self) {
        // A peer that keeps sending cannot keep a call from ending.
        for _ in 0..MAX_DRAINED {
            let Some(datagram) = self.link.try_recv() else {
                return;
            };
            self.dispatch(datagram);
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
            link,
            state: Mutex::new(State {
                flows: HashMap::new(),
                ended: EndedFlows::new(ended_capacity),
                held: HeldDatagrams::default(),
            }),
            max_frame_size,
            next_flow: AtomicU64::new(1),
            queued: Arc::new(AtomicU64::new(0)),
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
            header: flow_header(self.shared.link.prefix(), flow)?,
            max_frame_size: self.shared.max_frame_size,
            queued: Arc::clone(&self.shared.queued),
            compress: None,
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
        self.shared
            .state
            .lock_unpoisoned()
            .flows
            .insert(flow, Slot { sink, accept });
        self.ensure_reader();
        Some(ClientFlow {
            flow,
            requests: unreliable.requests.then_some(sender),
            responses,
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
                },
            );
            held
        };
        if let Some(sink) = &sink {
            let max_frame_size = self.shared.max_frame_size;
            for messages in held {
                for message in messages.inflate(accept, max_frame_size).unwrap_or_default() {
                    let size = message.len();
                    let _ = sink.try_send(message, size);
                }
            }
        }
        self.ensure_reader();
        Ok(ServerFlow {
            requests,
            sender,
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

impl FlowGuard {
    /// A handle that dispatches the datagrams already received, without keeping the flow
    /// registered.
    #[must_use]
    pub fn drain_handle(&self) -> DrainHandle {
        DrainHandle(Arc::clone(&self.shared))
    }

    fn drain(&self) {
        self.shared.drain();
    }
}

/// Dispatches a connection's already received datagrams to their flows; see
/// [`FlowGuard::drain_handle`].
#[derive(Clone)]
pub struct DrainHandle(Arc<Shared>);

impl DrainHandle {
    fn drain(&self) {
        self.0.drain();
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
    /// Unregisters the flow when dropped.
    pub guard: FlowGuard,
}

/// The server side of a call's datagram flow.
pub struct ServerFlow {
    /// The call's datagram request messages, if its requests stream.
    pub requests: Option<queue::Receiver<Bytes>>,
    /// Sends the call's response messages.
    pub sender: FlowSender,
    /// Unregisters the flow when dropped.
    pub guard: FlowGuard,
}

/// Sends one call's messages as datagrams.
#[derive(Clone)]
pub struct FlowSender {
    link: Arc<dyn SendLink>,
    /// The transport prefix and the flow ID.
    header: Bytes,
    /// The largest message sent in a datagram.
    max_frame_size: usize,
    /// Counts the datagrams queued on the connection; see [`FlowSender::flushed`].
    queued: Arc<AtomicU64>,
    /// Compresses the messages, when set.
    compress: Option<Arc<Dict>>,
}

impl FlowSender {
    /// Makes the sender compress messages with `dict`, when set and when that makes them shorter.
    #[must_use]
    pub(crate) fn with_compression(mut self, dict: Option<Arc<Dict>>) -> Self {
        self.compress = dict;
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
    pub async fn send_batch(
        &self,
        batch: &mut Vec<Bytes>,
        on_stream: &mut Vec<Bytes>,
        cancelled: impl Future<Output = ()>,
    ) {
        let max = self.link.max_size();
        let mut cancelled = std::pin::pin!(cancelled);
        let mut fired = false;
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
                .as_ref()
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
                self.flush(
                    &mut datagram,
                    &mut packed,
                    on_stream,
                    cancelled.as_mut(),
                    &mut fired,
                )
                .await;
            }
            if packed.is_empty() {
                datagram.reserve(total.min(max));
                datagram.extend_from_slice(&self.header);
            }
            put_message_frame(&mut datagram, frame_type, &payload);
            packed.push(message);
        }
        if !packed.is_empty() {
            self.flush(
                &mut datagram,
                &mut packed,
                on_stream,
                cancelled.as_mut(),
                &mut fired,
            )
            .await;
        }
    }

    /// Waits until the transport has written every datagram queued on the connection to the
    /// network, for a short while at most.
    ///
    /// A call ends with a stream frame, a STATUS or a FIN, and that frame can overtake the
    /// datagrams queued just before it, which the receiver then drops as arriving after the end.
    /// Calling this first keeps the order the sender intended. It does nothing for transports that
    /// cannot say how many datagrams they have written, and gives up when `cancelled` finishes.
    pub async fn flushed(&self, cancelled: impl Future<Output = ()>) {
        let queued = self.queued.load(Ordering::Relaxed);
        let now = Instant::now();
        let deadline = now.checked_add(FLUSH_WAIT).unwrap_or(now);
        let mut cancelled = std::pin::pin!(cancelled);
        while self
            .link
            .transmitted()
            .is_some_and(|written| written < queued)
            && Instant::now() < deadline
        {
            tokio::select! {
                biased;
                () = cancelled.as_mut() => return,
                () = tokio::task::yield_now() => {}
            }
        }
    }

    /// Sends the datagram holding `packed`; its messages go to `on_stream` if that fails.
    async fn flush(
        &self,
        datagram: &mut BytesMut,
        packed: &mut Vec<Bytes>,
        on_stream: &mut Vec<Bytes>,
        cancelled: Pin<&mut impl Future<Output = ()>>,
        fired: &mut bool,
    ) {
        let datagram = datagram.split().freeze();
        let sent = if let Some(sent) = self.link.send_now(&datagram) {
            sent.is_ok()
        } else if *fired {
            false
        } else {
            tokio::select! {
                biased;
                () = cancelled => {
                    *fired = true;
                    false
                }
                sent = self.link.send_wait(datagram) => sent.is_ok(),
            }
        };
        if sent {
            self.queued.fetch_add(1, Ordering::Relaxed);
            packed.clear();
        } else {
            on_stream.append(packed);
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
/// The datagram queue is read until the stream's request side ends, then drained of what it holds
/// and dropped, so datagrams that arrive later are dropped.
pub struct MergedRequests {
    requests: BoxStream<Vec<u8>>,
    datagrams: Option<queue::Receiver<Bytes>>,
    drain: DrainHandle,
    count: MessageCount,
    requests_done: bool,
    /// Alternates which source is polled first, so neither starves the other.
    datagrams_first: bool,
}

impl MergedRequests {
    /// Merges `requests` with the datagram messages in `datagrams`, counting the latter in
    /// `count` as they are delivered.
    #[must_use]
    pub fn boxed(
        requests: BoxStream<Vec<u8>>,
        datagrams: queue::Receiver<Bytes>,
        drain: DrainHandle,
        count: MessageCount,
    ) -> BoxStream<Vec<u8>> {
        Box::pin(Self {
            requests,
            datagrams: Some(datagrams),
            drain,
            count,
            requests_done: false,
            datagrams_first: false,
        })
    }

    fn poll_requests(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        if self.requests_done {
            return Poll::Pending;
        }
        match self.requests.as_mut().poll_next(cx) {
            Poll::Ready(Some(Err(error))) => {
                // The call failed: nothing more is delivered.
                self.requests_done = true;
                self.datagrams = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.requests_done = true;
                // The datagrams that arrived before the end of the stream are delivered first.
                self.drain.drain();
                Poll::Pending
            }
            other => other,
        }
    }

    fn poll_datagrams(&mut self, cx: &Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        let Some(datagrams) = &mut self.datagrams else {
            return Poll::Pending;
        };
        let message = if self.requests_done {
            datagrams.try_recv()
        } else {
            match datagrams.poll_recv(cx) {
                Poll::Ready(message) => message,
                Poll::Pending => return Poll::Pending,
            }
        };
        if let Some(message) = message {
            self.count.add();
            Poll::Ready(Some(Ok(message.to_vec())))
        } else {
            if self.requests_done {
                self.datagrams = None;
            }
            Poll::Pending
        }
    }
}

impl Stream for MergedRequests {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
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
        if this.requests_done {
            // The request side has just ended: deliver what was queued before it, then end.
            if let Poll::Ready(item) = this.poll_datagrams(cx) {
                return Poll::Ready(item);
            }
            this.datagrams = None;
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}

/// Where a call's response stands.
enum ResponsePhase {
    /// Frames and datagram messages are read as they arrive.
    Open,
    /// The response ended, with this terminal STATUS or none; the queued datagram messages are
    /// delivered before it.
    Draining(Option<ResponseFrame>),
    /// The terminal frame has been delivered; only the stream's own end remains.
    Ended,
}

/// The response frames of a call: its stream's frames, with the datagram messages merged in
/// after the HEADERS frame and ahead of the terminal STATUS.
pub struct MergedResponses {
    frames: BoxStream<ResponseFrame>,
    datagrams: Option<queue::Receiver<Bytes>>,
    /// Keeps the call's flow registered until the response ends.
    guard: FlowGuard,
    /// The call's HEADERS frame has been read.
    headers_read: bool,
    phase: ResponsePhase,
    datagrams_first: bool,
}

impl MergedResponses {
    /// Merges `frames` with the datagram messages in `datagrams`, if the call receives any, and
    /// keeps `guard` until the response ends or is dropped.
    #[must_use]
    pub fn boxed(
        frames: BoxStream<ResponseFrame>,
        datagrams: Option<queue::Receiver<Bytes>>,
        guard: FlowGuard,
    ) -> BoxStream<ResponseFrame> {
        Box::pin(Self {
            frames,
            datagrams,
            guard,
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
        match self.frames.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame {
                ResponseFrame::Status(_) => {
                    self.guard.drain();
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
                self.guard.drain();
                self.phase = ResponsePhase::Draining(None);
                Poll::Pending
            }
            other => other,
        }
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
                ResponsePhase::Ended => return this.frames.as_mut().poll_next(cx),
                ResponsePhase::Draining(_) => {
                    if let Some(message) = this.next_queued() {
                        return Poll::Ready(Some(Ok(ResponseFrame::Message(message))));
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

    use futures_util::StreamExt as _;
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
        /// How many datagrams the transport reports as written to the network, if it reports.
        transmitted: Option<AtomicU64>,
        /// Datagrams that arrived and were not taken yet.
        incoming: Mutex<VecDeque<Bytes>>,
        arrived: Notify,
    }

    impl Fake {
        /// A transport that reports how many datagrams it has written, starting at zero.
        fn reporting(max: u64) -> Self {
            let fake = Self::new(&[], max);
            let mut inner = Arc::try_unwrap(fake.0).ok().unwrap();
            inner.transmitted = Some(AtomicU64::new(0));
            Self(Arc::new(inner))
        }

        fn new(prefix: &[u8], max: u64) -> Self {
            Self(Arc::new(FakeInner {
                prefix: Bytes::copy_from_slice(prefix),
                max: AtomicU64::new(max),
                sent: Mutex::new(Vec::new()),
                blocked: AtomicBool::new(false),
                unblocked: Notify::new(),
                fail: AtomicBool::new(false),
                transmitted: None,
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
                if self.0.fail.load(Ordering::SeqCst) {
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
                if let Some(datagram) = DatagramTransport::try_recv(self) {
                    return Some(datagram);
                }
                arrived.await;
            }
        }

        fn try_recv(&self) -> Option<Bytes> {
            self.0.incoming.lock().unwrap().pop_front()
        }

        fn transmitted(&self) -> Option<u64> {
            self.0
                .transmitted
                .as_ref()
                .map(|written| written.load(Ordering::SeqCst))
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
            let decoded = decode(Bytes::from(datagram), MAX_FRAME).map(|(decoded, messages)| {
                (
                    decoded,
                    messages
                        .map(|(_, message)| message.to_vec())
                        .collect::<Vec<_>>(),
                )
            });
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

    fn messages_of(datagram: &[u8], max_frame_size: usize) -> Option<(u64, Vec<Vec<u8>>)> {
        decode(Bytes::copy_from_slice(datagram), max_frame_size).map(|(flow, messages)| {
            (
                flow,
                messages.map(|(_, message)| message.to_vec()).collect(),
            )
        })
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
        assert!(
            demux
                .shared
                .state
                .lock_unpoisoned()
                .held
                .claim(5, Instant::now())
                .is_empty()
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
        assert_eq!(forgotten.len(), 1);
        assert!(remembered.is_empty());
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
        assert!(batch.is_empty());
        on_stream.iter().map(|message| message.to_vec()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn the_flow_sender_uses_a_datagram_only_when_the_whole_frame_fits() {
        // A `WebTransport`-like prefix of one byte, a one-byte flow and a two-byte frame header
        // leave 95 bytes of message in a datagram of 100: past 63 bytes the length takes two bytes.
        let fake = Fake::new(&[0x2a], 100);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();

        assert!(send_all(&flow.sender, &[&[7; 95]]).await.is_empty());
        assert_eq!(send_all(&flow.sender, &[&[7; 96]]).await, [vec![7; 96]]);
        let sent = fake.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].len(), 100);
        assert_eq!(&sent[0][..5], &[0x2a, 0x01, 0x00, 0x40, 95]);

        // The limit moves with the connection's path MTU.
        fake.0.max.store(50, Ordering::SeqCst);
        assert_eq!(send_all(&flow.sender, &[&[7; 95]]).await.len(), 1);
        assert!(send_all(&flow.sender, &[&[7; 46]]).await.is_empty());

        // No datagrams at all, or a refused send, mean the stream.
        fake.0.max.store(0, Ordering::SeqCst);
        assert_eq!(send_all(&flow.sender, &[b"x"]).await.len(), 1);
        fake.0.max.store(100, Ordering::SeqCst);
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
        assert!(send_all(&flow.sender, &messages).await.is_empty());

        let sent = fake.sent();
        assert_eq!(sent.len(), 2);
        let unpack = |datagram: &Bytes| messages_of(datagram, MAX_FRAME).unwrap();
        assert_eq!(
            unpack(&sent[0]),
            (1, vec![vec![1; 10], vec![2; 10], vec![3; 10]])
        );
        assert_eq!(unpack(&sent[1]), (1, vec![vec![4; 10], vec![5; 10]]));
    }

    #[tokio::test]
    async fn flushed_waits_for_the_transport_to_write_the_queued_datagrams_but_not_for_long() {
        let fake = Fake::reporting(1200);
        let demux = server(&fake);
        let flow = demux.open_server_flow(1, BIDI, &Accept::None).unwrap();
        // Nothing queued yet: nothing to wait for.
        let started = std::time::Instant::now();
        flow.sender.flushed(std::future::pending()).await;
        assert!(started.elapsed() < FLUSH_WAIT);

        assert!(send_all(&flow.sender, &[b"x"]).await.is_empty());

        // The transport writes it while the call waits.
        let writer = fake.clone();
        tokio::spawn(async move {
            writer
                .0
                .transmitted
                .as_ref()
                .unwrap()
                .store(1, Ordering::SeqCst);
        });
        let started = std::time::Instant::now();
        flow.sender.flushed(std::future::pending()).await;
        assert!(started.elapsed() < FLUSH_WAIT);

        // A transport that never writes it holds the call up for the limit and no longer.
        assert!(send_all(&flow.sender, &[b"y"]).await.is_empty());
        let started = std::time::Instant::now();
        flow.sender.flushed(std::future::pending()).await;
        assert!(started.elapsed() >= FLUSH_WAIT);
        assert!(started.elapsed() < FLUSH_WAIT * 50);

        // Cancellation ends the wait at once.
        let started = std::time::Instant::now();
        flow.sender.flushed(std::future::ready(())).await;
        assert!(started.elapsed() < FLUSH_WAIT);
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
        assert!(fake.sent().is_empty());
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
    async fn a_malformed_datagram_delivers_nothing_and_is_not_held() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (mut requests, _flow) =
            requests_of(demux.open_server_flow(3, BIDI, &Accept::None).unwrap());

        // A good message followed by a truncated frame, for a live flow and for an unknown one.
        fake.arrive(&[0x03, 0x00, 0x01, b'a', 0x00, 0x09, b'b']);
        fake.arrive(&[0x09, 0x00, 0x01, b'a', 0x00, 0x09, b'b']);
        settle().await;

        assert!(drain(&mut requests).is_empty());
        assert!(
            demux
                .shared
                .state
                .lock_unpoisoned()
                .held
                .claim(9, Instant::now())
                .is_empty()
        );
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
        assert!(fake.sent().is_empty());

        // A send that waits completes when room appears.
        let sender = first.sender.clone();
        let waiting = tokio::spawn(async move { send_all(&sender, &[b"y"]).await });
        settle().await;
        assert!(!waiting.is_finished());
        fake.block_sends(false);
        assert!(waiting.await.unwrap().is_empty());
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

        assert!(
            demux
                .shared
                .state
                .lock_unpoisoned()
                .held
                .claim(77, Instant::now())
                .is_empty()
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
            MergedRequests::boxed(stream, requests, flow.guard.drain_handle(), count.clone());

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
        let mut merged = MergedRequests::boxed(
            stream,
            requests,
            flow.guard.drain_handle(),
            MessageCount::new(),
        );

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
        let mut merged = MergedRequests::boxed(
            stream,
            requests,
            flow.guard.drain_handle(),
            MessageCount::new(),
        );

        stream_tx
            .send(Err(Error::from(Status::cancelled("reset"))))
            .unwrap();

        let error = merged.next().await.unwrap().unwrap_err();
        assert_eq!(error.into_status().code(), crate::Code::Cancelled);
        fake.arrive_for(1, b"never delivered");
        settle().await;
        assert!(merged.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn requests_deliver_datagrams_that_arrived_before_the_end_even_if_the_reader_lags() {
        let fake = Fake::new(&[], 1200);
        let demux = server(&fake);
        let (requests, flow) = requests_of(demux.open_server_flow(1, BIDI, &Accept::None).unwrap());
        let (stream_tx, stream) = channel_stream::<Vec<u8>>();
        let mut merged = MergedRequests::boxed(
            stream,
            requests,
            flow.guard.drain_handle(),
            MessageCount::new(),
        );

        // The datagrams and the end of the stream arrive together, and nothing yields to the
        // reader task before the end is seen.
        for index in 0..3_u8 {
            fake.arrive_for(1, &[index]);
        }
        drop(stream_tx);

        let mut delivered = Vec::new();
        while let Some(message) = merged.next().await {
            delivered.push(message.unwrap());
        }
        delivered.sort();
        assert_eq!(delivered, [vec![0], vec![1], vec![2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn responses_deliver_datagrams_that_arrived_before_the_status_even_if_the_reader_lags() {
        let fake = Fake::new(&[], 1200);
        let demux = Demux::client(fake.clone(), MAX_FRAME);
        let flow = demux
            .open_client_flow(BIDI, Unreliable::BOTH, Accept::None)
            .unwrap();
        let (frames_tx, frames) = channel_stream::<ResponseFrame>();
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.guard);

        frames_tx.send(Ok(headers())).unwrap();
        assert_eq!(merged.next().await.unwrap().unwrap(), headers());
        for index in 0..3_u8 {
            fake.arrive_for(flow.flow, &[index]);
        }
        frames_tx.send(Ok(ok())).unwrap();

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
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.guard);

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
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.guard);

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
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.guard);

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
        let merged = MergedResponses::boxed(frames, flow.responses, flow.guard);
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
        let mut merged = MergedResponses::boxed(frames, flow.responses, flow.guard);

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
