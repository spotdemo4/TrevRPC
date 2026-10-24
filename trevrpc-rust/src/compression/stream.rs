//! Stream compression: `ZSTD_STREAM_MESSAGE` frames, which carry a message compressed with the zstd
//! context of its direction of a call, so each message compresses against the direction's earlier
//! ones.
//!
//! A method allows it in its schema (`compress_stream`), which generated code passes as
//! [`CompressStream`]. The client offers it on each call and the server answers, per direction;
//! senders start a direction's context from its third message, within a memory budget that every
//! server and channel has, and receivers validate each chunk before decompressing it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use zstd_safe::zstd_sys::ZSTD_EndDirective;
use zstd_safe::{CCtx, CParameter, DCtx, DParameter, FrameFormat, InBuffer, OutBuffer};

use super::{Accept, CallDict, CompressionConfig, Dict, Rejected, zstd_error};
use crate::datagram::MessageCount;
use crate::sync::MutexExt;
use crate::{Result, RpcKind, varint, wire};

/// The directions of a method whose messages may be compressed with one zstd context per call
/// (stream compression), from the `compress_stream` field of the `trevrpc.method` schema option.
///
/// Generated code passes them with [`Call::compress_stream`](crate::client::Call::compress_stream)
/// and [`RouteOptions::with_compress_stream`](crate::server::RouteOptions::with_compress_stream).
/// A direction is set only if the method streams it and its message type has no field marked
/// `debug_redact`.
///
/// A server sends HEADERS at dispatch, before the handler runs, on every call to a method whose
/// requests it may stream-compress (`requests` is set, the method streams requests and they are
/// not unreliable), so the client learns the answer while it still streams. Handlers of such
/// methods cannot set response headers; the runtime reports them as already sent.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct CompressStream {
    /// The messages the client streams may be stream-compressed.
    pub requests: bool,
    /// The messages the server streams may be stream-compressed.
    pub responses: bool,
}

impl CompressStream {
    /// Neither direction.
    pub const NONE: Self = Self {
        requests: false,
        responses: false,
    };
    /// The client's streamed requests.
    pub const REQUESTS: Self = Self {
        requests: true,
        responses: false,
    };
    /// The server's streamed responses.
    pub const RESPONSES: Self = Self {
        requests: false,
        responses: true,
    };
    /// Both directions.
    pub const BOTH: Self = Self {
        requests: true,
        responses: true,
    };

    /// Returns whether either direction is set.
    #[must_use]
    pub const fn any(self) -> bool {
        self.requests || self.responses
    }

    /// Keeps only the directions that stream messages for a call of `kind`.
    #[must_use]
    pub const fn effective(self, kind: RpcKind) -> Self {
        Self {
            requests: self.requests && kind.streams_requests(),
            responses: self.responses && kind.streams_responses(),
        }
    }

    /// Drops the directions marked unreliable: their messages may travel as datagrams, which
    /// stream compression cannot use, so they never hold a context.
    #[must_use]
    pub const fn without_unreliable(self, unreliable: crate::Unreliable) -> Self {
        Self {
            requests: self.requests && !unreliable.requests,
            responses: self.responses && !unreliable.responses,
        }
    }
}

/// The window, as a base-2 logarithm, that stream compression contexts use and receivers decode.
pub const STREAM_WINDOW_LOG: u32 = 16;
/// A peer that advertises a smaller window enables nothing.
const MIN_STREAM_WINDOW_LOG: u32 = 10;
/// A sender starts a direction's context at this message; earlier ones go per message, so short
/// streams cost nothing.
const FIRST_STREAMED_MESSAGE: usize = 3;
/// What stream compression may hold by default on a server, in bytes.
pub const DEFAULT_SERVER_MEMORY: usize = 256 << 20;
/// What stream compression may hold by default on a client channel, in bytes.
pub const DEFAULT_CLIENT_MEMORY: usize = 64 << 20;

/// The negotiation field this runtime sends: the window it decodes.
pub const fn advertised() -> wire::StreamCompression {
    wire::StreamCompression {
        max_window_log: STREAM_WINDOW_LOG,
    }
}

/// The window a sender uses for a receiver that advertised `info`, or `None` when it enables
/// nothing.
pub fn sending_window(info: Option<&wire::StreamCompression>) -> Option<u32> {
    let window = info?.max_window_log;
    (window >= MIN_STREAM_WINDOW_LOG).then(|| window.min(STREAM_WINDOW_LOG))
}

// ---------------------------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------------------------

/// The memory that the stream compression contexts of one server or channel may hold.
#[derive(Debug)]
pub struct StreamBudget {
    available: AtomicUsize,
}

impl StreamBudget {
    /// A budget of `bytes`, or `None` for zero, which disables stream compression.
    pub fn new(bytes: usize) -> Option<Arc<Self>> {
        (bytes > 0).then(|| {
            Arc::new(Self {
                available: AtomicUsize::new(bytes),
            })
        })
    }

    /// Reserves `cost` bytes, or returns `None` when the budget has less left.
    pub fn reserve(self: &Arc<Self>, cost: usize) -> Option<Reservation> {
        self.available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| {
                available.checked_sub(cost)
            })
            .ok()?;
        Some(Reservation {
            budget: Arc::clone(self),
            cost,
        })
    }
}

/// Memory reserved from a [`StreamBudget`], given back when dropped.
#[derive(Debug)]
pub struct Reservation {
    budget: Arc<StreamBudget>,
    cost: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.available.fetch_add(self.cost, Ordering::AcqRel);
    }
}

/// A context's memory when it cannot be measured; libzstd's estimates for a 64 KiB window are
/// about 585 KiB and 350 KiB.
const FALLBACK_ENCODER_COST: usize = 600 << 10;
const FALLBACK_DECODER_COST: usize = 360 << 10;

/// What a compressing context costs at `level`, starting from `dict`: measured once, after it
/// compressed a message, since libzstd allocates its window then.
pub fn encoder_cost(level: i32, dict: Option<&Dict>) -> usize {
    /// Costs by level and dictionary ID.
    type Costs = Mutex<HashMap<(i32, Option<u32>), usize>>;
    static COSTS: OnceLock<Costs> = OnceLock::new();
    let costs = COSTS.get_or_init(Mutex::default);
    let key = (level, dict.map(|dict| dict.id));
    if let Some(cost) = costs.lock_unpoisoned().get(&key) {
        return *cost;
    }
    let cost = StreamEncoder::context(level, STREAM_WINDOW_LOG, dict)
        .ok()
        .and_then(|mut context| {
            compress_chunk(&mut context, b"x", Vec::new()).ok()?;
            Some(context.sizeof())
        })
        .unwrap_or(FALLBACK_ENCODER_COST);
    costs.lock_unpoisoned().insert(key, cost);
    cost
}

/// What a decompressing context costs once it decoded a frame starting from `dict`.
pub fn decoder_cost(dict: Option<&Arc<Dict>>) -> usize {
    static COSTS: OnceLock<Mutex<HashMap<Option<u32>, usize>>> = OnceLock::new();
    let costs = COSTS.get_or_init(Mutex::default);
    let key = dict.map(|dict| dict.id);
    if let Some(cost) = costs.lock_unpoisoned().get(&key) {
        return *cost;
    }
    let accept = dict.map_or(Accept::None, |dict| Accept::One(Arc::clone(dict)));
    let cost = StreamEncoder::context(1, STREAM_WINDOW_LOG, dict.map(AsRef::as_ref))
        .ok()
        .and_then(|mut context| {
            let payload = compress_chunk(&mut context, b"x", Vec::new()).ok()?;
            let mut decoder = Inflater::new(accept, STREAM_WINDOW_LOG);
            decoder.decode(&payload, 1).ok()?;
            decoder.context.as_ref().map(DCtx::sizeof)
        })
        .unwrap_or(FALLBACK_DECODER_COST);
    costs.lock_unpoisoned().insert(key, cost);
    cost
}

/// The most a server's decompressing context can cost: clients may start request frames from any
/// dictionary the server holds.
pub fn server_decoder_cost(config: Option<&Arc<CompressionConfig>>) -> usize {
    config
        .into_iter()
        .flat_map(|config| config.by_id.values())
        .map(|dict| decoder_cost(Some(dict)))
        .fold(decoder_cost(None), usize::max)
}

// ---------------------------------------------------------------------------------------------
// Compressing
// ---------------------------------------------------------------------------------------------

/// One direction's compressing context.
pub struct StreamEncoder {
    context: CCtx<'static>,
    _reservation: Reservation,
}

impl StreamEncoder {
    /// A context that writes the chunks of magicless frames with a `2^window_log` window and no
    /// checksum or content size, starting every frame from `dict` if set.
    fn context(level: i32, window_log: u32, dict: Option<&Dict>) -> Result<CCtx<'static>> {
        let mut context = CCtx::try_create().ok_or_else(|| zstd_error(0))?;
        for parameter in [
            CParameter::CompressionLevel(level),
            CParameter::WindowLog(window_log),
            CParameter::ChecksumFlag(false),
            CParameter::ContentSizeFlag(false),
            CParameter::DictIdFlag(true),
            CParameter::Format(FrameFormat::Magicless),
        ] {
            context.set_parameter(parameter).map_err(zstd_error)?;
        }
        if let Some(dict) = dict {
            context.load_dictionary(dict.data()).map_err(zstd_error)?;
        }
        Ok(context)
    }

    /// Compresses `message` into a `ZSTD_STREAM_MESSAGE` payload.
    fn encode(&mut self, message: &[u8]) -> Result<Bytes> {
        let mut payload = Vec::with_capacity(
            varint::MAX_LEN.saturating_add(zstd_safe::compress_bound(message.len())),
        );
        varint::push(
            &mut payload,
            u64::try_from(message.len()).unwrap_or(u64::MAX),
        )?;
        compress_chunk(&mut self.context, message, payload).map(Bytes::from)
    }
}

/// Appends the chunk of `message`, compressed with `context` and flushed, to `out`.
fn compress_chunk(context: &mut CCtx<'_>, message: &[u8], mut out: Vec<u8>) -> Result<Vec<u8>> {
    let mut input = InBuffer::around(message);
    loop {
        if out.len() == out.capacity() {
            out.reserve(out.len().max(64));
        }
        let start = out.len();
        let mut output = OutBuffer::around_pos(&mut out, start);
        let remaining = context
            .compress_stream2(&mut output, &mut input, ZSTD_EndDirective::ZSTD_e_flush)
            .map_err(zstd_error)?;
        if remaining == 0 && input.pos() == message.len() {
            return Ok(out);
        }
    }
}

/// How a direction learns that the receiver enabled stream compression: the window it decodes,
/// zero until then.
#[derive(Debug, Default)]
pub struct StreamEnable(AtomicU32);

impl StreamEnable {
    /// A direction the receiver enabled already, with `window_log`.
    pub fn enabled(window_log: u32) -> Arc<Self> {
        Arc::new(Self(AtomicU32::new(window_log)))
    }

    /// Records the receiver's answer, if it enables anything.
    pub fn enable(&self, info: Option<&wire::StreamCompression>) {
        if let Some(window) = sending_window(info) {
            self.0.store(window, Ordering::Release);
        }
    }

    fn window_log(&self) -> Option<u32> {
        let window = self.0.load(Ordering::Acquire);
        (window != 0).then_some(window)
    }
}

/// Stream compression of one direction of a call, from its sender's side.
pub struct StreamSender {
    enable: Arc<StreamEnable>,
    budget: Arc<StreamBudget>,
    level: i32,
    /// The messages the direction has sent so far.
    sent: usize,
    state: SenderState,
}

enum SenderState {
    /// No context yet: too few messages, or the receiver has not enabled the direction.
    Waiting,
    Active(StreamEncoder),
    /// The budget was spent or compression failed: every message goes per message.
    Off,
}

impl std::fmt::Debug for StreamSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSender")
            .field("sent", &self.sent)
            .finish_non_exhaustive()
    }
}

impl StreamSender {
    pub const fn new(enable: Arc<StreamEnable>, budget: Arc<StreamBudget>, level: i32) -> Self {
        Self {
            enable,
            budget,
            level,
            sent: 0,
            state: SenderState::Waiting,
        }
    }

    /// The `ZSTD_STREAM_MESSAGE` payload of the next message, or `None` when it goes per message.
    /// The direction's context starts from `dict`, when set.
    fn encode(&mut self, message: &[u8], dict: Option<&Dict>) -> Option<Bytes> {
        self.sent = self.sent.saturating_add(1);
        if matches!(self.state, SenderState::Waiting)
            && self.sent >= FIRST_STREAMED_MESSAGE
            && let Some(window_log) = self.enable.window_log()
        {
            self.state = self
                .start(window_log, dict)
                .map_or(SenderState::Off, SenderState::Active);
        }
        let SenderState::Active(encoder) = &mut self.state else {
            return None;
        };
        let payload = encoder.encode(message);
        // A failed message may have changed the context without reaching the receiver, so the
        // context must not be used again.
        payload.map_err(|_| self.state = SenderState::Off).ok()
    }

    fn start(&self, window_log: u32, dict: Option<&Dict>) -> Option<StreamEncoder> {
        let reservation = self.budget.reserve(encoder_cost(self.level, dict))?;
        let context = StreamEncoder::context(self.level, window_log, dict).ok()?;
        Some(StreamEncoder {
            context,
            _reservation: reservation,
        })
    }
}

/// How one direction of a call encodes its outgoing messages: per message with a dictionary, or
/// with the direction's stream compression context.
#[derive(Debug, Default)]
pub struct MessageEncoder {
    /// The dictionary per-message frames are compressed with, which also starts the stream
    /// compression context.
    dict: CallDict,
    stream: Option<StreamSender>,
}

impl MessageEncoder {
    /// Compresses per message with `dict`, once it has one.
    pub(crate) fn new(dict: impl Into<CallDict>) -> Self {
        Self {
            dict: dict.into(),
            stream: None,
        }
    }

    /// Lets the direction use stream compression, when `stream` is set.
    #[must_use]
    pub fn with_stream(mut self, stream: Option<StreamSender>) -> Self {
        self.stream = stream;
        self
    }

    /// Encodes one outgoing message as its frame type and payload: a `ZSTD_STREAM_MESSAGE` once the
    /// direction's context runs, else a `ZSTD_MESSAGE` when the dictionary makes it shorter, else a
    /// MESSAGE.
    pub fn encode(&mut self, body: Bytes) -> (u64, Bytes) {
        use crate::framing::frame_type;
        let dict = self.dict.get();
        if let Some(payload) = self
            .stream
            .as_mut()
            .and_then(|stream| stream.encode(&body, dict.as_deref()))
        {
            return (frame_type::ZSTD_STREAM_MESSAGE, payload);
        }
        dict.as_ref()
            .and_then(|dict| dict.compress(&body))
            .map_or((frame_type::MESSAGE, body), |compressed| {
                (frame_type::ZSTD_MESSAGE, compressed)
            })
    }
}

// ---------------------------------------------------------------------------------------------
// Decompressing
// ---------------------------------------------------------------------------------------------

/// What validating a chunk reveals about it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamChunk {
    /// The chunk begins with a frame header.
    pub starts_frame: bool,
    /// That frame's `Dictionary_ID`, zero for none.
    pub dict_id: u32,
    /// The chunk's last block ends the frame, so the next chunk starts a new one.
    pub ends_frame: bool,
}

/// Walks the block whose header starts at `pos`, returning where the next block starts and
/// whether this one is the `Last_Block`, or `None` for a reserved block type or a block that does
/// not fit.
pub(super) fn next_block(data: &[u8], pos: usize) -> Option<(usize, bool)> {
    let header = data.get(pos..pos.checked_add(3)?)?;
    let [a, b, c] = <[u8; 3]>::try_from(header).ok()?;
    let header = u32::from_le_bytes([a, b, c, 0]);
    let size = usize::try_from(header >> 3).ok()?;
    let content_len = match (header >> 1) & 0x03 {
        // Raw_Block and Compressed_Block.
        0 | 2 => size,
        // RLE_Block.
        1 => 1,
        _ => return None,
    };
    let next = pos.checked_add(3)?.checked_add(content_len)?;
    (next <= data.len()).then_some((next, header & 1 != 0))
}

/// Validates `chunk`, the zstd data of a `ZSTD_STREAM_MESSAGE`.
///
/// With `at_frame_start`, no frame is open, so a non-empty chunk must start with a frame header:
/// one without `Single_Segment_flag`, `Frame_Content_Size_flag`, `Content_Checksum_flag` or the
/// reserved bit, whose window is at most `2^max_window_log`. The chunk must then be whole blocks,
/// and a `Last_Block` ends it. An empty chunk is an empty message and changes nothing. Block
/// contents are not checked; decompression does that.
pub fn parse_stream_chunk(
    chunk: &[u8],
    at_frame_start: bool,
    max_window_log: u32,
) -> Option<StreamChunk> {
    let mut result = StreamChunk::default();
    if chunk.is_empty() {
        return Some(result);
    }
    let mut pos = if at_frame_start {
        let (header_len, dict_id) = parse_frame_header(chunk, max_window_log)?;
        result.dict_id = dict_id;
        result.starts_frame = true;
        header_len
    } else {
        0
    };
    while pos < chunk.len() {
        let (next, last) = next_block(chunk, pos)?;
        if last && next != chunk.len() {
            return None;
        }
        pos = next;
        result.ends_frame = last;
    }
    Some(result)
}

/// Validates the frame header that opens `chunk` and returns its length and `Dictionary_ID`.
fn parse_frame_header(chunk: &[u8], max_window_log: u32) -> Option<(usize, u32)> {
    let (&descriptor, rest) = chunk.split_first()?;
    // Frame_Content_Size_flag (bits 7-6), Single_Segment_flag (bit 5), the reserved bit (bit 3)
    // and Content_Checksum_flag (bit 2) must be unset.
    if descriptor & 0xEC != 0 {
        return None;
    }
    // The Window_Descriptor: an exponent above 10 and eighths of that power of two.
    let &window = rest.first()?;
    let base = 1_u64.checked_shl(u32::from(window >> 3).checked_add(10)?)?;
    let size = base.checked_add((base / 8).checked_mul(u64::from(window & 7))?)?;
    if size > 1_u64.checked_shl(max_window_log)? {
        return None;
    }
    let dict_id_len: usize = match descriptor & 0x03 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    let header_len = dict_id_len.checked_add(2)?;
    let dict_id = u32::try_from(super::little_endian(chunk.get(2..header_len)?)?).ok()?;
    Some((header_len, dict_id))
}

/// One direction's decompressing state, from its receiver's side.
pub struct Inflater {
    /// The dictionaries a frame may start from.
    accept: Accept,
    max_window_log: u32,
    /// The context of the open frame, created when a frame starts.
    context: Option<DCtx<'static>>,
    at_frame_start: bool,
    /// Counts the messages decoded, for a server's handler context.
    count: MessageCount,
    _reservation: Option<Reservation>,
}

impl std::fmt::Debug for Inflater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inflater")
            .field("at_frame_start", &self.at_frame_start)
            .finish_non_exhaustive()
    }
}

impl Inflater {
    /// A receiver that decodes windows up to `2^max_window_log` and frames starting from the
    /// dictionaries `accept` holds.
    pub fn new(accept: Accept, max_window_log: u32) -> Self {
        Self {
            accept,
            max_window_log,
            context: None,
            at_frame_start: true,
            count: MessageCount::default(),
            _reservation: None,
        }
    }

    /// Holds `reservation` for as long as the receiver lives.
    #[must_use]
    pub fn with_reservation(self, reservation: Reservation) -> Self {
        Self {
            _reservation: Some(reservation),
            ..self
        }
    }

    /// Counts every decoded message in `count`.
    #[must_use]
    pub fn with_count(mut self, count: MessageCount) -> Self {
        self.count = count;
        self
    }

    /// Decompresses the payload of a `ZSTD_STREAM_MESSAGE`.
    ///
    /// A message larger than `max_size` is rejected before it is decompressed. After a rejection
    /// the direction cannot continue.
    pub fn decode(
        &mut self,
        payload: &[u8],
        max_size: usize,
    ) -> std::result::Result<Bytes, Rejected> {
        let invalid = || Rejected::Invalid("invalid ZSTD_STREAM_MESSAGE payload".to_owned());
        let (size, size_len) = varint::decode(payload).ok_or_else(invalid)?;
        let len = usize::try_from(size).unwrap_or(usize::MAX);
        if len > max_size {
            return Err(Rejected::TooLarge { len, max: max_size });
        }
        let chunk = payload.get(size_len..).ok_or_else(invalid)?;
        let parsed = parse_stream_chunk(chunk, self.at_frame_start, self.max_window_log)
            .ok_or_else(invalid)?;
        if chunk.is_empty() {
            return if len == 0 {
                self.count.add();
                Ok(Bytes::new())
            } else {
                Err(invalid())
            };
        }
        if parsed.starts_frame {
            self.context = Some(self.frame_context(parsed.dict_id)?);
        }
        let context = self.context.as_mut().ok_or_else(invalid)?;
        // One spare byte: libzstd holds back output that does not fit, so a message longer than
        // declared shows only as an extra byte.
        let mut message = Vec::with_capacity(len.saturating_add(1));
        let mut input = InBuffer::around(chunk);
        while input.pos() < chunk.len() {
            let before = (input.pos(), message.len());
            let mut output = OutBuffer::around_pos(&mut message, before.1);
            let result = context.decompress_stream(&mut output, &mut input);
            if result.is_err() || (input.pos(), message.len()) == before {
                self.context = None;
                return Err(Rejected::Invalid(
                    "ZSTD_STREAM_MESSAGE chunk is corrupt".to_owned(),
                ));
            }
        }
        if message.len() != len {
            self.context = None;
            return Err(Rejected::Invalid(
                "ZSTD_STREAM_MESSAGE decompressed to the wrong size".to_owned(),
            ));
        }
        self.at_frame_start = parsed.ends_frame;
        self.count.add();
        Ok(Bytes::from(message))
    }

    /// A context for a frame that starts from the dictionary `dict_id`, or none for zero.
    fn frame_context(&self, dict_id: u32) -> std::result::Result<DCtx<'static>, Rejected> {
        let failed = |code| Rejected::Invalid(zstd_error(code).to_string());
        let mut context = DCtx::try_create().ok_or_else(|| failed(0))?;
        for parameter in [
            DParameter::Format(FrameFormat::Magicless),
            DParameter::WindowLogMax(self.max_window_log),
        ] {
            context.set_parameter(parameter).map_err(failed)?;
        }
        if dict_id != 0 {
            let dict = self.accept.lookup(dict_id).ok_or_else(|| {
                Rejected::Invalid(format!(
                    "stream-compressed frame uses dictionary {dict_id}, which is not accepted"
                ))
            })?;
            context.load_dictionary(dict.data()).map_err(failed)?;
        }
        Ok(context)
    }
}

// ---------------------------------------------------------------------------------------------
// Negotiation
// ---------------------------------------------------------------------------------------------

/// What stream compression negotiation decided for one server call.
#[derive(Debug, Default)]
pub struct ServerStream {
    /// The answer the call's HEADERS carry, when the server enabled stream-compressed requests.
    pub answer: Option<wire::StreamCompression>,
    /// The decompressing state of the request messages, when enabled.
    pub requests: Option<Inflater>,
    /// The compressing state of the response messages, when the client enabled them.
    pub responses: Option<StreamSender>,
    /// Counts the request messages that arrive stream-compressed.
    pub count: MessageCount,
}

/// Answers the request's stream compression offer for the directions of the route that may use
/// it, within `budget`. A request that does not offer gets nothing: its client cannot use it.
pub fn negotiate_stream(
    request: &crate::RpcRequest,
    directions: CompressStream,
    budget: Option<&Arc<StreamBudget>>,
    config: Option<&Arc<CompressionConfig>>,
) -> ServerStream {
    let mut stream = ServerStream::default();
    let (Some(window_log), Some(budget)) =
        (sending_window(request.stream_compression.as_ref()), budget)
    else {
        return stream;
    };
    if directions.responses {
        stream.responses = Some(StreamSender::new(
            StreamEnable::enabled(window_log),
            Arc::clone(budget),
            CompressionConfig::level(config),
        ));
    }
    if directions.requests
        && let Some(reservation) = budget.reserve(server_decoder_cost(config))
    {
        stream.count = MessageCount::new();
        stream.requests = Some(
            Inflater::new(Accept::server(config), STREAM_WINDOW_LOG)
                .with_reservation(reservation)
                .with_count(stream.count.clone()),
        );
        stream.answer = Some(advertised());
    }
    stream
}

/// What stream compression negotiation decided for one client call.
#[derive(Debug, Default)]
pub struct ClientStream {
    /// The decompressing state of the response messages, when the request offered them.
    pub responses: Option<Inflater>,
    /// The compressing state of the request messages, enabled by the server's answer.
    pub requests: Option<StreamSender>,
    /// What the server's answer enables.
    pub enable_requests: Option<Arc<StreamEnable>>,
}

/// Offers stream compression on `request` for the directions of the method that may use it,
/// within `budget`. Responses are decoded with the dictionaries `accept` holds.
///
/// An offer says the client decodes stream-compressed responses, so a call whose responses stream
/// offers only if it can reserve their decompressing context.
pub fn offer_stream(
    request: &mut crate::RpcRequest,
    directions: CompressStream,
    budget: Option<&Arc<StreamBudget>>,
    accept: &Accept,
    level: i32,
) -> ClientStream {
    let mut stream = ClientStream::default();
    let Some(budget) = budget.filter(|_| directions.any()) else {
        return stream;
    };
    if directions.responses {
        let dict = match accept {
            Accept::One(dict) => Some(dict),
            Accept::None | Accept::All(_) => None,
        };
        let Some(reservation) = budget.reserve(decoder_cost(dict)) else {
            return stream;
        };
        stream.responses =
            Some(Inflater::new(accept.clone(), STREAM_WINDOW_LOG).with_reservation(reservation));
    }
    if directions.requests {
        let enable = Arc::new(StreamEnable::default());
        stream.requests = Some(StreamSender::new(
            Arc::clone(&enable),
            Arc::clone(budget),
            level,
        ));
        stream.enable_requests = Some(enable);
    }
    request.stream_compression = Some(advertised());
    stream
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;
    use crate::compression::tests::{config, vectors};
    use crate::framing::frame_type;
    use crate::{RpcHeaders, RpcRequest};

    fn inflater() -> Inflater {
        Inflater::new(Accept::All(config()), STREAM_WINDOW_LOG)
    }

    /// The payloads and messages of the golden sequence `name`, in order.
    fn sequence(name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let vectors = vectors();
        (0..)
            .map_while(|i| {
                let payload = vectors.get(&format!("stream.{name}.{i}.payload"))?;
                let message = vectors.get(&format!("stream.{name}.{i}.message"))?;
                Some((payload.clone(), message.clone()))
            })
            .collect()
    }

    #[test]
    fn receivers_follow_the_golden_vectors() {
        for name in ["plain", "seeded", "reset", "empty"] {
            let sequence = sequence(name);
            assert!(sequence.len() >= 2, "{name}");
            let mut receiver = inflater();
            for (i, (payload, message)) in sequence.iter().enumerate() {
                let decoded = receiver
                    .decode(payload, crate::framing::DEFAULT_MAX_FRAME_SIZE)
                    .unwrap_or_else(|error| panic!("{name}.{i}: {error:?}"));
                assert_eq!(decoded.as_ref(), message.as_slice(), "{name}.{i}");
            }
        }
        let vectors = vectors();
        let mut bad = 0;
        for (key, payload) in &vectors {
            if let Some(name) = key.strip_prefix("stream_bad.") {
                bad += 1;
                assert!(
                    matches!(
                        inflater().decode(payload, crate::framing::DEFAULT_MAX_FRAME_SIZE),
                        Err(Rejected::Invalid(_))
                    ),
                    "{name} was accepted"
                );
            } else if let Some(name) = key.strip_prefix("stream_too_large.") {
                assert!(
                    matches!(
                        inflater().decode(payload, crate::framing::DEFAULT_MAX_FRAME_SIZE),
                        Err(Rejected::TooLarge { .. })
                    ),
                    "{name} was not too large"
                );
            }
        }
        assert!(bad >= 9);
    }

    #[test]
    fn a_seeded_frame_needs_an_accepted_dictionary() {
        let (payload, _) = sequence("seeded").swap_remove(0);
        let mut receiver = Inflater::new(Accept::None, STREAM_WINDOW_LOG);
        assert!(matches!(
            receiver.decode(&payload, 1 << 20),
            Err(Rejected::Invalid(_))
        ));
    }

    #[test]
    fn senders_write_the_golden_payloads() {
        // Both runtimes compress with libzstd 1.5.7 at level 1, so they write the same bytes.
        let config = config();
        let dict = config.dict(1).unwrap();
        for (name, dict) in [("plain", None), ("seeded", Some(dict.as_ref()))] {
            let budget = StreamBudget::new(usize::MAX).unwrap();
            let mut encoder = StreamEncoder {
                context: StreamEncoder::context(1, STREAM_WINDOW_LOG, dict).unwrap(),
                _reservation: budget.reserve(0).unwrap(),
            };
            for (i, (payload, message)) in sequence(name).iter().enumerate() {
                assert_eq!(
                    encoder.encode(message).unwrap().as_ref(),
                    payload.as_slice(),
                    "{name}.{i}"
                );
            }
        }
    }

    #[test]
    fn the_negotiation_fields_match_the_golden_vectors() {
        let vectors = vectors();
        let mut request = RpcRequest::new("/svc/m");
        request.stream_compression = Some(advertised());
        assert_eq!(
            request.encode_to_vec(),
            vectors["request.stream_compression.payload"]
        );
        let headers = RpcHeaders {
            stream_compression: Some(advertised()),
            ..RpcHeaders::default()
        };
        assert_eq!(
            headers.encode_to_vec(),
            vectors["headers.stream_compression.payload"]
        );
        assert_eq!(
            crate::framing::decode_payload::<RpcHeaders>(
                &vectors["headers.stream_compression.payload"]
            )
            .unwrap()
            .stream_compression,
            Some(advertised())
        );
    }

    #[test]
    fn windows_below_the_minimum_enable_nothing() {
        let window =
            |max_window_log| sending_window(Some(&wire::StreamCompression { max_window_log }));
        assert_eq!(window(9), None);
        assert_eq!(window(10), Some(10));
        assert_eq!(window(20), Some(STREAM_WINDOW_LOG));
        assert_eq!(sending_window(None), None);
    }

    fn message(i: usize) -> Bytes {
        Bytes::from(format!(
            r#"{{"service":"search","region":"us-east-1","status":404,"latency_ms":{}}}"#,
            i.saturating_add(100)
        ))
    }

    #[test]
    fn senders_start_at_the_third_message_once_enabled_and_receivers_follow() {
        let budget = StreamBudget::new(64 << 20).unwrap();
        let enable = Arc::new(StreamEnable::default());
        let mut encoder = MessageEncoder::new(None).with_stream(Some(StreamSender::new(
            Arc::clone(&enable),
            Arc::clone(&budget),
            1,
        )));
        let mut receiver = inflater();
        let mut types = Vec::new();
        for i in 0..6 {
            if i == 3 {
                enable.enable(Some(&advertised()));
            }
            let (frame_type, payload) = encoder.encode(message(i));
            if frame_type == frame_type::ZSTD_STREAM_MESSAGE {
                assert_eq!(receiver.decode(&payload, 1 << 20).unwrap(), message(i));
            }
            types.push(frame_type);
        }
        // Not enabled until the fourth message, so the context starts there.
        assert_eq!(types, [0, 0, 0, 5, 5, 5].map(|frame_type: u64| frame_type));
        assert_eq!(
            receiver.count.get(),
            0,
            "the default counter counts nothing"
        );
    }

    #[test]
    fn a_spent_budget_keeps_the_direction_per_message() {
        let budget = StreamBudget::new(1).unwrap();
        let mut encoder = MessageEncoder::new(None).with_stream(Some(StreamSender::new(
            StreamEnable::enabled(STREAM_WINDOW_LOG),
            budget,
            1,
        )));
        for i in 0..5 {
            assert_eq!(encoder.encode(message(i)).0, frame_type::MESSAGE);
        }
    }

    #[test]
    fn unreliable_directions_never_use_stream_compression() {
        assert_eq!(
            CompressStream::BOTH.without_unreliable(crate::Unreliable::REQUESTS),
            CompressStream::RESPONSES
        );
        assert_eq!(
            CompressStream::BOTH.without_unreliable(crate::Unreliable::BOTH),
            CompressStream::NONE
        );
    }

    #[test]
    fn an_empty_message_is_an_empty_chunk_even_before_the_frame_header() {
        let budget = StreamBudget::new(64 << 20).unwrap();
        let mut encoder = StreamEncoder {
            context: StreamEncoder::context(1, STREAM_WINDOW_LOG, None).unwrap(),
            _reservation: budget.reserve(0).unwrap(),
        };
        let mut receiver = inflater();
        let empty = encoder.encode(b"").unwrap();
        assert_eq!(empty.as_ref(), [0]);
        assert_eq!(receiver.decode(&empty, 1 << 20).unwrap(), Bytes::new());
        let next = encoder.encode(&message(0)).unwrap();
        assert_eq!(receiver.decode(&next, 1 << 20).unwrap(), message(0));
    }

    #[test]
    fn reservations_return_their_memory() {
        let budget = StreamBudget::new(10).unwrap();
        let first = budget.reserve(6).unwrap();
        assert!(budget.reserve(6).is_none());
        drop(first);
        assert!(budget.reserve(10).is_some());
        assert!(StreamBudget::new(0).is_none());
    }

    #[test]
    fn servers_answer_only_offers_for_the_directions_the_route_allows() {
        let budget = StreamBudget::new(64 << 20);
        let mut offer = RpcRequest::new("/svc/m");
        offer.stream_compression = Some(advertised());

        let both = negotiate_stream(&offer, CompressStream::BOTH, budget.as_ref(), None);
        assert_eq!(both.answer, Some(advertised()));
        assert!(both.requests.is_some() && both.responses.is_some());

        let responses = negotiate_stream(&offer, CompressStream::RESPONSES, budget.as_ref(), None);
        assert!(responses.answer.is_none() && responses.responses.is_some());

        let no_offer = negotiate_stream(
            &RpcRequest::new("/svc/m"),
            CompressStream::BOTH,
            budget.as_ref(),
            None,
        );
        assert!(no_offer.answer.is_none() && no_offer.responses.is_none());

        let disabled = negotiate_stream(&offer, CompressStream::BOTH, None, None);
        assert!(disabled.answer.is_none() && disabled.responses.is_none());

        let spent = StreamBudget::new(1);
        let spent = negotiate_stream(&offer, CompressStream::BOTH, spent.as_ref(), None);
        assert!(spent.answer.is_none(), "no room for the request context");
    }

    #[test]
    fn clients_offer_only_what_they_can_decode() {
        let budget = StreamBudget::new(64 << 20);
        let mut request = RpcRequest::new("/svc/m");
        let stream = offer_stream(
            &mut request,
            CompressStream::BOTH,
            budget.as_ref(),
            &Accept::None,
            1,
        );
        assert_eq!(request.stream_compression, Some(advertised()));
        assert!(stream.responses.is_some() && stream.requests.is_some());

        // A client-streaming method offers without reserving a response context.
        let mut request = RpcRequest::new("/svc/m");
        let stream = offer_stream(
            &mut request,
            CompressStream::REQUESTS,
            budget.as_ref(),
            &Accept::None,
            1,
        );
        assert!(request.stream_compression.is_some() && stream.responses.is_none());

        // Without room for the response context there is no offer at all.
        let spent = StreamBudget::new(1);
        let mut request = RpcRequest::new("/svc/m");
        let stream = offer_stream(
            &mut request,
            CompressStream::BOTH,
            spent.as_ref(),
            &Accept::None,
            1,
        );
        assert!(request.stream_compression.is_none() && stream.requests.is_none());

        let mut request = RpcRequest::new("/svc/m");
        let _ = offer_stream(
            &mut request,
            CompressStream::NONE,
            budget.as_ref(),
            &Accept::None,
            1,
        );
        assert!(request.stream_compression.is_none());
    }
}
