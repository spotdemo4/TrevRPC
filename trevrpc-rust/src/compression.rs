//! Compressed messages: `ZSTD_MESSAGE` frames, which carry a message compressed with zstd and a
//! dictionary both peers hold.
//!
//! Dictionaries are configured with [`Compression`] on a channel
//! ([`ChannelConfig::with_compression`](crate::client::ChannelConfig::with_compression)) and a
//! server ([`ServerOptions::with_compression`](crate::server::ServerOptions::with_compression)),
//! and negotiated per connection, so a message whose dictionary the peer lacks travels
//! uncompressed. A dictionary compresses the protobuf message types it names; generated code tells
//! the runtime each method's types.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use zstd_safe::{CCtx, CParameter, DCtx, DParameter, FrameFormat};

use crate::sync::MutexExt;
use crate::wire::DictionaryInfo;
use crate::{Error, Result, RpcRequest, Status};

mod stream;

pub use stream::CompressStream;
pub(crate) use stream::{
    DEFAULT_CLIENT_MEMORY, DEFAULT_SERVER_MEMORY, Inflater, MessageEncoder, ServerStream,
    StreamBudget, StreamEnable, negotiate_stream, offer_stream,
};

/// The dictionaries a peer holds, for compressed messages.
///
/// Messages travel as zstd frames compressed with a dictionary both peers hold. Dictionaries are
/// negotiated per connection; a message whose dictionary the peer does not hold travels
/// uncompressed.
#[derive(Clone, Debug, Default)]
pub struct Compression {
    dictionaries: Vec<Dictionary>,
    level: i32,
}

impl Compression {
    /// Creates a configuration without dictionaries, which compresses nothing.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            dictionaries: Vec::new(),
            level: 0,
        }
    }

    /// Adds a dictionary this peer holds.
    #[must_use]
    pub fn with_dictionary(mut self, dictionary: Dictionary) -> Self {
        self.dictionaries.push(dictionary);
        self
    }

    /// Sets the zstd compression level of outgoing messages; zero, the default, means 1.
    #[must_use]
    pub const fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        self
    }
}

/// A zstd dictionary and the protobuf message types it compresses.
#[derive(Clone, Debug)]
pub struct Dictionary {
    data: Vec<u8>,
    messages: Vec<String>,
}

impl Dictionary {
    /// A dictionary in zstd's format, as written by `zstd --train`.
    ///
    /// Its header carries the `Dictionary_ID` that identifies it to peers, which must never name
    /// different dictionaries.
    #[must_use]
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            messages: Vec::new(),
        }
    }

    /// Names the protobuf message types the dictionary compresses, by full name, such as
    /// `example.v1.Position`.
    ///
    /// A type may appear in one dictionary only. A dictionary without messages is still offered to
    /// peers, which may compress with it, but this peer never does; adding a new dictionary that
    /// way first lets receivers hold it before senders use it.
    #[must_use]
    pub fn with_messages<I, S>(mut self, messages: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.messages.extend(messages.into_iter().map(Into::into));
        self
    }
}

/// The magic number that opens every zstd-format dictionary (RFC 8878, section 5).
const DICTIONARY_MAGIC: u32 = 0xEC30_A437;
/// Contexts a dictionary keeps for reuse; more are created under load and then dropped.
const MAX_POOLED_CONTEXTS: usize = 64;

/// A digested dictionary, safe to share between calls.
pub(crate) struct Dict {
    pub(crate) id: u32,
    pub(crate) info: DictionaryInfo,
    data: Vec<u8>,
    level: i32,
    compressors: Mutex<Vec<CCtx<'static>>>,
    decompressors: Mutex<Vec<DCtx<'static>>>,
}

impl PartialEq for Dict {
    /// Dictionaries are the same when their IDs and fingerprints are.
    fn eq(&self, other: &Self) -> bool {
        self.info == other.info
    }
}

impl Eq for Dict {}

impl std::fmt::Debug for Dict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dict")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Returns the `Dictionary_ID` of a zstd-format dictionary.
fn dictionary_id(data: &[u8]) -> Option<u32> {
    let magic = data.get(..4)?;
    let id = data.get(4..8)?;
    if u32::from_le_bytes(magic.try_into().ok()?) != DICTIONARY_MAGIC {
        return None;
    }
    let id = u32::from_le_bytes(id.try_into().ok()?);
    (id != 0).then_some(id)
}

/// Returns a dictionary's fingerprint: the first 8 bytes of its SHA-256 digest.
pub(crate) fn fingerprint(data: &[u8]) -> Vec<u8> {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest
        .as_ref()
        .get(..8)
        .map(<[u8]>::to_vec)
        .unwrap_or_default()
}

fn zstd_error(code: usize) -> Error {
    Error::from(Status::internal(format!(
        "zstd: {}",
        zstd_safe::get_error_name(code)
    )))
}

impl Dict {
    fn new(data: Vec<u8>, level: i32) -> Result<Self> {
        let id = dictionary_id(&data).ok_or_else(|| {
            Error::from(Status::invalid_argument(
                "compression dictionary is not a zstd-format dictionary with an ID",
            ))
        })?;
        let dict = Self {
            id,
            info: DictionaryInfo {
                id,
                fingerprint: fingerprint(&data),
            },
            data,
            level,
            compressors: Mutex::new(Vec::new()),
            decompressors: Mutex::new(Vec::new()),
        };
        // Digesting it once proves libzstd accepts it, and leaves a context of each kind ready.
        let compressor = dict.new_compressor()?;
        let decompressor = dict.new_decompressor()?;
        dict.compressors.lock_unpoisoned().push(compressor);
        dict.decompressors.lock_unpoisoned().push(decompressor);
        Ok(dict)
    }

    /// The dictionary itself, which stream compression contexts load.
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    fn new_compressor(&self) -> Result<CCtx<'static>> {
        let mut context = CCtx::create();
        for parameter in [
            CParameter::CompressionLevel(self.level),
            CParameter::ChecksumFlag(false),
            CParameter::DictIdFlag(true),
            CParameter::ContentSizeFlag(true),
            CParameter::Format(FrameFormat::Magicless),
        ] {
            context.set_parameter(parameter).map_err(zstd_error)?;
        }
        // The dictionary is digested once per context and used for every frame after.
        context.load_dictionary(&self.data).map_err(zstd_error)?;
        Ok(context)
    }

    fn new_decompressor(&self) -> Result<DCtx<'static>> {
        let mut context = DCtx::create();
        context
            .set_parameter(DParameter::Format(FrameFormat::Magicless))
            .map_err(zstd_error)?;
        context.load_dictionary(&self.data).map_err(zstd_error)?;
        Ok(context)
    }

    /// Compresses `message` into a `ZSTD_MESSAGE` payload, or returns `None` when that would not be
    /// shorter than the message, which then travels uncompressed.
    pub(crate) fn compress(&self, message: &[u8]) -> Option<Bytes> {
        let pooled = self.compressors.lock_unpoisoned().pop();
        let mut context = match pooled {
            Some(context) => context,
            None => self.new_compressor().ok()?,
        };
        let mut payload = Vec::with_capacity(zstd_safe::compress_bound(message.len()));
        let result = context.compress2(&mut payload, message);
        let mut pool = self.compressors.lock_unpoisoned();
        if pool.len() < MAX_POOLED_CONTEXTS {
            pool.push(context);
        }
        drop(pool);
        match result {
            Ok(len) if len < message.len() => Some(Bytes::from(payload)),
            _ => None,
        }
    }

    /// Decompresses a `ZSTD_MESSAGE` payload whose header was parsed into `header`.
    fn decompress(&self, payload: &[u8], header: Header) -> std::result::Result<Bytes, String> {
        let size = usize::try_from(header.content_size).map_err(|_| "content size".to_owned())?;
        let pooled = self.decompressors.lock_unpoisoned().pop();
        let mut context = match pooled {
            Some(context) => context,
            None => self.new_decompressor().map_err(|error| error.to_string())?,
        };
        let mut message = Vec::with_capacity(size);
        let result = context.decompress(&mut message, payload);
        let mut pool = self.decompressors.lock_unpoisoned();
        if pool.len() < MAX_POOLED_CONTEXTS {
            pool.push(context);
        }
        drop(pool);
        match result {
            Ok(len) if len == size => Ok(Bytes::from(message)),
            Ok(_) => Err("compressed message decompressed to the wrong size".to_owned()),
            Err(code) => Err(format!(
                "compressed message is corrupt: {}",
                zstd_safe::get_error_name(code)
            )),
        }
    }
}

/// A validated [`Compression`].
#[derive(Debug)]
pub(crate) struct CompressionConfig {
    by_id: HashMap<u32, Arc<Dict>>,
    by_type: HashMap<String, Arc<Dict>>,
    /// Every dictionary, as a client's `dictionary_query`.
    query: Vec<DictionaryInfo>,
    /// The zstd level of outgoing messages.
    level: i32,
}

impl CompressionConfig {
    /// Validates and digests `compression`; `None` when it has no dictionaries.
    pub(crate) fn new(compression: &Compression) -> Result<Option<Arc<Self>>> {
        if compression.dictionaries.is_empty() {
            return Ok(None);
        }
        let level = if compression.level == 0 {
            1
        } else {
            compression.level
        };
        let mut config = Self {
            by_id: HashMap::new(),
            by_type: HashMap::new(),
            query: Vec::new(),
            level,
        };
        for dictionary in &compression.dictionaries {
            let dict = Arc::new(Dict::new(dictionary.data.clone(), level)?);
            if config.by_id.contains_key(&dict.id) {
                return Err(Error::from(Status::invalid_argument(format!(
                    "compression dictionaries share ID {}",
                    dict.id
                ))));
            }
            config.query.push(dict.info.clone());
            for name in &dictionary.messages {
                if !valid_full_name(name) {
                    return Err(Error::from(Status::invalid_argument(format!(
                        "invalid message type {name:?} in compression dictionary {}",
                        dict.id
                    ))));
                }
                if config
                    .by_type
                    .insert(name.clone(), Arc::clone(&dict))
                    .is_some()
                {
                    return Err(Error::from(Status::invalid_argument(format!(
                        "message type {name} is in two compression dictionaries"
                    ))));
                }
            }
            config.by_id.insert(dict.id, dict);
        }
        Ok(Some(Arc::new(config)))
    }

    fn dict(&self, id: u32) -> Option<&Arc<Dict>> {
        self.by_id.get(&id)
    }

    /// The zstd level of outgoing messages under `config`: its level, or 1 without one, since
    /// stream compression needs no dictionaries.
    pub(crate) fn level(config: Option<&Arc<Self>>) -> i32 {
        config.map_or(1, |config| config.level)
    }

    fn dict_for_type(&self, name: Option<&str>) -> Option<&Arc<Dict>> {
        self.by_type.get(name?)
    }

    fn matches(&self, info: &DictionaryInfo) -> Option<&Arc<Dict>> {
        self.dict(info.id).filter(|dict| dict.info == *info)
    }
}

/// Whether `name` is a well-formed protobuf full name: dot-separated identifiers.
fn valid_full_name(name: &str) -> bool {
    !name.is_empty()
        && name.split('.').all(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

/// The dictionaries a receiver accepts on `ZSTD_MESSAGE` frames.
#[derive(Clone, Debug, Default)]
pub(crate) enum Accept {
    /// None: every compressed message is rejected.
    #[default]
    None,
    /// The one dictionary a client listed for the call.
    One(Arc<Dict>),
    /// Every dictionary a server holds.
    All(Arc<CompressionConfig>),
}

impl Accept {
    /// What a server accepts: every dictionary it holds.
    pub(crate) fn server(config: Option<&Arc<CompressionConfig>>) -> Self {
        config.map_or(Self::None, |config| Self::All(Arc::clone(config)))
    }

    fn lookup(&self, id: u32) -> Option<&Arc<Dict>> {
        match self {
            Self::None => None,
            Self::One(dict) => (dict.id == id).then_some(dict),
            Self::All(config) => config.dict(id),
        }
    }
}

/// What a receiver needs from a `ZSTD_MESSAGE` payload before decompressing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) dict_id: u32,
    pub(crate) content_size: u64,
}

/// Reads a little-endian integer of up to 8 bytes.
fn little_endian(bytes: &[u8]) -> Option<u64> {
    let mut buffer = [0_u8; 8];
    buffer.get_mut(..bytes.len())?.copy_from_slice(bytes);
    Some(u64::from_le_bytes(buffer))
}

/// Validates that `payload` is exactly one magicless zstd frame with a non-zero `Dictionary_ID`
/// and a `Frame_Content_Size`, and returns both.
///
/// It walks the block headers to find the frame's end, so trailing bytes, including a second
/// frame, are rejected. Block contents are not checked; decompression does that.
pub(crate) fn parse_header(payload: &[u8]) -> Option<Header> {
    let (&descriptor, mut rest) = payload.split_first()?;
    let single_segment = descriptor & 0x20 != 0;
    if descriptor & 0x08 != 0 {
        // The reserved bit.
        return None;
    }
    let checksum = descriptor & 0x04 != 0;
    let dict_id_len = match descriptor & 0x03 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    let content_size_len = match (descriptor >> 6, single_segment) {
        (0, true) => 1,
        (0, false) => 0,
        (1, _) => 2,
        (2, _) => 4,
        _ => 8,
    };
    if dict_id_len == 0 || content_size_len == 0 {
        return None;
    }
    if !single_segment {
        // The Window_Descriptor.
        rest = rest.get(1..)?;
    }
    let (dict_id, after) = rest.split_at_checked(dict_id_len)?;
    let (content_size, after) = after.split_at_checked(content_size_len)?;
    rest = after;
    let dict_id = u32::try_from(little_endian(dict_id)?).ok()?;
    let mut content_size = little_endian(content_size)?;
    if content_size_len == 2 {
        content_size = content_size.checked_add(256)?;
    }
    if dict_id == 0 {
        return None;
    }
    loop {
        let (block_header, after) = rest.split_at_checked(3)?;
        let [a, b, c] = <[u8; 3]>::try_from(block_header).ok()?;
        let block_header = u32::from_le_bytes([a, b, c, 0]);
        let size = usize::try_from(block_header >> 3).ok()?;
        let content_len = match (block_header >> 1) & 0x03 {
            // Raw_Block and Compressed_Block.
            0 | 2 => size,
            // RLE_Block.
            1 => 1,
            _ => return None,
        };
        rest = after.get(content_len..)?;
        if block_header & 1 != 0 {
            break;
        }
    }
    if checksum {
        rest = rest.get(4..)?;
    }
    rest.is_empty().then_some(Header {
        dict_id,
        content_size,
    })
}

/// Why a `ZSTD_MESSAGE` frame was rejected.
#[derive(Debug)]
pub(crate) enum Rejected {
    /// The message is larger than the frame size limit.
    TooLarge { len: usize, max: usize },
    /// Anything else, described.
    Invalid(String),
}

impl Rejected {
    /// The error a receiver reports: `invalid` builds the status of an invalid payload, which is
    /// `InvalidArgument` on a request stream and `Internal` on a response stream.
    pub(crate) fn into_error(self, invalid: fn(String) -> Status) -> Error {
        match self {
            Self::TooLarge { len, max } => Error::FrameTooLarge { len, max },
            Self::Invalid(message) => Error::from(invalid(message)),
        }
    }
}

/// Decompresses the payload of a `ZSTD_MESSAGE` frame with the dictionaries `accept` holds.
///
/// A message larger than `max_size` is rejected before it is decompressed.
pub(crate) fn inflate(
    accept: &Accept,
    payload: &[u8],
    max_size: usize,
) -> std::result::Result<Bytes, Rejected> {
    if matches!(accept, Accept::None) {
        return Err(Rejected::Invalid(
            "compressed message where no dictionary is accepted".to_owned(),
        ));
    }
    let header = parse_header(payload)
        .ok_or_else(|| Rejected::Invalid("invalid ZSTD_MESSAGE payload".to_owned()))?;
    let len = usize::try_from(header.content_size).unwrap_or(usize::MAX);
    if len > max_size {
        return Err(Rejected::TooLarge { len, max: max_size });
    }
    let dict = accept.lookup(header.dict_id).ok_or_else(|| {
        Rejected::Invalid(format!(
            "compressed message uses dictionary {}, which is not accepted",
            header.dict_id
        ))
    })?;
    dict.decompress(payload, header).map_err(Rejected::Invalid)
}

/// The compression state of one client connection.
#[derive(Debug)]
pub(crate) struct ClientCompression {
    config: Arc<CompressionConfig>,
    /// The connection's shared set, from the server's answer to a query.
    shared: OnceLock<Vec<Arc<Dict>>>,
}

/// The dictionary one direction of a call compresses its messages with, which may only become
/// known during the call.
#[derive(Clone, Debug, Default)]
pub(crate) enum CallDict {
    /// No dictionary.
    #[default]
    None,
    /// This dictionary, from the start of the call.
    Known(Arc<Dict>),
    /// The request type's dictionary of a call that started before its connection had the
    /// server's answer: it applies as soon as the answer arrives and confirms it, so a request
    /// stream starts compressing mid-call.
    Pending {
        connection: Arc<ClientCompression>,
        dict: Arc<Dict>,
    },
}

impl CallDict {
    /// The dictionary to compress with now, if any.
    pub(crate) fn get(&self) -> Option<Arc<Dict>> {
        match self {
            Self::None => None,
            Self::Known(dict) => Some(Arc::clone(dict)),
            Self::Pending { connection, dict } => connection
                .shared
                .get()
                .filter(|set| set.contains(dict))
                .map(|_| Arc::clone(dict)),
        }
    }
}

impl From<Option<Arc<Dict>>> for CallDict {
    fn from(dict: Option<Arc<Dict>>) -> Self {
        dict.map_or(Self::None, Self::Known)
    }
}

/// What negotiation decided for one client call.
#[derive(Clone, Debug, Default)]
pub(crate) struct ClientCall {
    /// The dictionaries the call accepts on its response messages.
    pub(crate) accept: Accept,
    /// The dictionary request messages are compressed with.
    pub(crate) requests: CallDict,
    /// The connection to teach the answer in the call's HEADERS, when the request queried.
    pub(crate) learn: Option<Arc<ClientCompression>>,
}

impl ClientCompression {
    pub(crate) fn new(config: Arc<CompressionConfig>) -> Arc<Self> {
        Arc::new(Self {
            config,
            shared: OnceLock::new(),
        })
    }

    /// Fills the negotiation fields of `request` for a call whose message types are `types`.
    pub(crate) fn prepare(
        self: &Arc<Self>,
        request: &mut RpcRequest,
        types: Option<crate::client::MessageTypes>,
    ) -> ClientCall {
        let shared = self.shared.get();
        let usable = |dict: &&Arc<Dict>| shared.is_none_or(|set| set.contains(dict));
        let mut call = ClientCall::default();
        if shared.is_none() {
            request.dictionary_query.clone_from(&self.config.query);
            call.learn = Some(Arc::clone(self));
        }
        // A call that queries may already accept the dictionary it asks about: the server
        // compresses with it only if its answer confirms it.
        if let Some(dict) = self
            .config
            .dict_for_type(types.map(|types| types.response))
            .filter(usable)
        {
            request.accept_dictionaries = vec![dict.id];
            call.accept = Accept::One(Arc::clone(dict));
        }
        if let Some(dict) = self.config.dict_for_type(types.map(|types| types.request)) {
            call.requests = match shared {
                Some(set) if set.contains(dict) => CallDict::Known(Arc::clone(dict)),
                Some(_) => CallDict::None,
                None => CallDict::Pending {
                    connection: Arc::clone(self),
                    dict: Arc::clone(dict),
                },
            };
        }
        call
    }

    /// Records the server's answer to a query as the connection's shared set. The first answer
    /// wins; they all come from one configuration.
    pub(crate) fn learn(&self, answer: &[DictionaryInfo]) {
        let mut shared: Vec<Arc<Dict>> = Vec::new();
        for info in answer {
            if let Some(dict) = self.config.matches(info)
                && !shared.contains(dict)
            {
                shared.push(Arc::clone(dict));
            }
        }
        let _ = self.shared.set(shared);
    }
}

/// What negotiation decided for one server call.
#[derive(Clone, Debug, Default)]
pub(crate) struct ServerCall {
    /// The answer to the request's query; `Some`, if empty, when it queried, which makes HEADERS
    /// go out.
    pub(crate) answer: Option<Vec<DictionaryInfo>>,
    /// The dictionary response messages are compressed with, if any.
    pub(crate) responses: Option<Arc<Dict>>,
}

/// Answers the request's query, if it has one, and picks the dictionary of the response messages:
/// the one for `response_type`, if the client accepts it. A server without compression still
/// answers, so clients stop asking.
pub(crate) fn negotiate(
    config: Option<&Arc<CompressionConfig>>,
    request: &RpcRequest,
    response_type: Option<&str>,
) -> ServerCall {
    let mut call = ServerCall::default();
    if !request.dictionary_query.is_empty() {
        let mut answer: Vec<DictionaryInfo> = Vec::new();
        for info in &request.dictionary_query {
            if let Some(dict) = config.and_then(|config| config.matches(info))
                && !answer.contains(&dict.info)
            {
                answer.push(dict.info.clone());
            }
        }
        call.answer = Some(answer);
    }
    call.responses = config
        .and_then(|config| config.dict_for_type(response_type))
        .filter(|dict| request.accept_dictionaries.contains(&dict.id))
        .filter(|dict| {
            call.answer
                .as_ref()
                .is_none_or(|answer| answer.contains(&dict.info))
        })
        .cloned();
    call
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The entries of `testdata/zstd-golden-vectors.txt`, hex-decoded.
    pub(super) fn vectors() -> HashMap<String, Vec<u8>> {
        include_str!("../../testdata/zstd-golden-vectors.txt")
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .filter_map(|line| line.split_once(" ="))
            .filter(|(key, _)| key.rsplit('.').next() != Some("id"))
            .map(|(key, value)| (key.to_owned(), hex(value.trim())))
            .collect()
    }

    fn hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn test_compression() -> Compression {
        Compression::new().with_dictionary(
            Dictionary::new(vectors()["dict.d1.dictionary"].clone())
                .with_messages(["google.protobuf.StringValue"]),
        )
    }

    pub(super) fn config() -> Arc<CompressionConfig> {
        CompressionConfig::new(&test_compression())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn the_dictionary_vector_has_its_id_and_fingerprint() {
        let vectors = vectors();
        let dictionary = &vectors["dict.d1.dictionary"];
        assert_eq!(dictionary_id(dictionary), Some(1));
        assert_eq!(fingerprint(dictionary), vectors["dict.d1.fingerprint"]);
        assert_eq!(dictionary_id(b"raw content dictionary"), None);
    }

    #[test]
    fn receivers_follow_the_golden_vectors() {
        let vectors = vectors();
        let accept = Accept::All(config());
        let max = crate::framing::DEFAULT_MAX_FRAME_SIZE;
        for (key, payload) in &vectors {
            let Some(name) = key.strip_suffix(".payload") else {
                continue;
            };
            if let Some(case) = name.strip_prefix("ok.") {
                let message = inflate(&accept, payload, max).unwrap_or_else(|error| {
                    panic!("{case}: {error:?}");
                });
                assert_eq!(
                    message.as_ref(),
                    vectors[&format!("{name}.message")],
                    "{case}"
                );
            } else if name.starts_with("bad.") {
                assert!(
                    matches!(inflate(&accept, payload, max), Err(Rejected::Invalid(_))),
                    "{name} was accepted"
                );
            } else if name.starts_with("too_large.") {
                assert!(
                    matches!(
                        inflate(&accept, payload, max),
                        Err(Rejected::TooLarge { .. })
                    ),
                    "{name} was not too large"
                );
            }
        }
        assert!(inflate(&Accept::None, &vectors["ok.small.payload"], max).is_err());
    }

    #[test]
    fn compression_round_trips_and_skips_incompressible_messages() {
        let vectors = vectors();
        let config = config();
        let dict = config.dict(1).unwrap();
        let message = &vectors["ok.small.message"];
        let payload = dict.compress(message).unwrap();
        let header = parse_header(&payload).unwrap();
        assert_eq!((header.dict_id, header.content_size), (1, 71));
        // No magic number, no checksum.
        assert_ne!(payload.get(..4), Some(&[0x28, 0xb5, 0x2f, 0xfd][..]));
        assert_eq!(payload.first().unwrap() & 0x04, 0);
        let accept = Accept::One(Arc::clone(dict));
        assert_eq!(
            inflate(&accept, &payload, 1 << 20).unwrap().as_ref(),
            message
        );
        assert!(dict.compress(&vectors["ok.raw_block.message"]).is_none());
        assert!(dict.compress(b"").is_none());
    }

    #[test]
    fn invalid_configurations_are_rejected() {
        let dictionary = vectors()["dict.d1.dictionary"].clone();
        let mut renumbered = dictionary.clone();
        renumbered.splice(4..8, 2_u32.to_le_bytes());
        for compression in [
            Compression::new().with_dictionary(Dictionary::new(b"not a dictionary".to_vec())),
            Compression::new()
                .with_dictionary(Dictionary::new(dictionary.clone()))
                .with_dictionary(Dictionary::new(dictionary.clone())),
            Compression::new().with_dictionary(
                Dictionary::new(dictionary.clone()).with_messages(["not a name!"]),
            ),
            Compression::new()
                .with_dictionary(Dictionary::new(dictionary).with_messages(["a.B"]))
                .with_dictionary(Dictionary::new(renumbered).with_messages(["a.B"])),
        ] {
            assert!(CompressionConfig::new(&compression).is_err());
        }
        assert!(
            CompressionConfig::new(&Compression::new())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn negotiation_answers_queries_and_confirms_dictionaries() {
        let config = config();
        let types = Some(crate::client::MessageTypes::new(
            "google.protobuf.StringValue",
            "google.protobuf.StringValue",
        ));
        let client = ClientCompression::new(Arc::clone(&config));

        // The first call queries and accepts dictionary 1 unconfirmed.
        let mut request = RpcRequest::new("/svc/m");
        let first = client.prepare(&mut request, types);
        assert_eq!(request.accept_dictionaries, [1]);
        assert_eq!(request.dictionary_query.len(), 1);
        assert!(first.requests.get().is_none() && first.learn.is_some());

        let server = negotiate(Some(&config), &request, Some("google.protobuf.StringValue"));
        assert_eq!(server.answer.as_deref(), Some(&config.query[..]));
        assert!(server.responses.is_some());
        // A server without compression answers too, with nothing.
        let none = negotiate(None, &request, Some("google.protobuf.StringValue"));
        assert_eq!(none.answer.as_deref(), Some(&[][..]));
        assert!(none.responses.is_none());

        // A wrong fingerprint is not confirmed, so the server does not compress with it.
        let mut wrong = request.clone();
        wrong.dictionary_query[0].fingerprint = b"12345678".to_vec();
        let refused = negotiate(Some(&config), &wrong, Some("google.protobuf.StringValue"));
        assert_eq!(refused.answer.as_deref(), Some(&[][..]));
        assert!(refused.responses.is_none());

        // Once the answer is in, calls stop querying and compress requests, and so does the
        // call that queried, from then on.
        client.learn(server.answer.as_deref().unwrap());
        assert!(first.requests.get().is_some());
        let mut next = RpcRequest::new("/svc/m");
        let call = client.prepare(&mut next, types);
        assert!(next.dictionary_query.is_empty() && call.learn.is_none());
        assert_eq!(next.accept_dictionaries, [1]);
        assert!(matches!(call.requests, CallDict::Known(_)));
    }

    #[test]
    fn an_empty_answer_stops_compression() {
        let config = config();
        let client = ClientCompression::new(config);
        client.learn(&[]);
        let mut request = RpcRequest::new("/svc/m");
        let types = Some(crate::client::MessageTypes::new(
            "a.B",
            "google.protobuf.StringValue",
        ));
        let call = client.prepare(&mut request, types);
        assert!(request.dictionary_query.is_empty() && request.accept_dictionaries.is_empty());
        assert!(matches!(call.accept, Accept::None) && matches!(call.requests, CallDict::None));
    }
}
