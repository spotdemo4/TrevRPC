use std::collections::HashMap;

pub type Metadata = HashMap<String, Vec<u8>>;

pub const MAX_METADATA_ENTRIES: usize = 64;
pub const MAX_METADATA_KEY_LEN: usize = 128;
pub const MAX_METADATA_VALUE_LEN: usize = 8 * 1024;
pub const MAX_METADATA_TOTAL_SIZE: usize = 64 * 1024;
pub const RESERVED_METADATA_PREFIX: &str = "trevrpc-";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum RpcKind {
    Unary = 0,
    ClientStreaming = 1,
    ServerStreaming = 2,
    BidirectionalStreaming = 3,
}

/// Normalizes a metadata key to lowercase ASCII.
#[must_use]
pub fn normalize_metadata_key(key: &str) -> String {
    key.to_ascii_lowercase()
}

/// Validates metadata key syntax, value sizes, and total metadata limits.
pub fn validate_metadata(metadata: &Metadata) -> std::result::Result<(), crate::Status> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(crate::Status::invalid_argument(format!(
            "metadata has {} entries, maximum is {MAX_METADATA_ENTRIES}",
            metadata.len()
        )));
    }

    let mut total_size = 0_usize;

    for (key, value) in metadata {
        validate_metadata_key(key)?;

        if value.len() > MAX_METADATA_VALUE_LEN {
            return Err(crate::Status::invalid_argument(format!(
                "metadata value {key:?} is {} bytes, maximum is {MAX_METADATA_VALUE_LEN}",
                value.len()
            )));
        }

        total_size = total_size
            .saturating_add(key.len())
            .saturating_add(value.len());
    }

    if total_size > MAX_METADATA_TOTAL_SIZE {
        return Err(crate::Status::invalid_argument(format!(
            "metadata is {total_size} bytes, maximum is {MAX_METADATA_TOTAL_SIZE}"
        )));
    }

    Ok(())
}

fn validate_metadata_key(key: &str) -> std::result::Result<(), crate::Status> {
    if key.is_empty() {
        return Err(crate::Status::invalid_argument("metadata key is empty"));
    }

    if key.len() > MAX_METADATA_KEY_LEN {
        return Err(crate::Status::invalid_argument(format!(
            "metadata key {key:?} is {} bytes, maximum is {MAX_METADATA_KEY_LEN}",
            key.len()
        )));
    }

    if key.starts_with(RESERVED_METADATA_PREFIX) {
        return Err(crate::Status::invalid_argument(format!(
            "metadata key {key:?} uses reserved prefix {RESERVED_METADATA_PREFIX:?}"
        )));
    }

    if !key.bytes().all(is_metadata_key_byte) {
        return Err(crate::Status::invalid_argument(format!(
            "metadata key {key:?} must use lowercase ASCII letters, digits, '.', '_' or '-'"
        )));
    }

    Ok(())
}

const fn is_metadata_key_byte(byte: u8) -> bool {
    matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-')
}

/// Splits a method path `/<service>/<method>` into its service and method.
///
/// Returns `None` unless the path is a leading slash, a non-empty service, a slash, and a
/// non-empty method with no further slash.
#[must_use]
pub fn split_method_path(path: &str) -> Option<(&str, &str)> {
    let (service, method) = path.strip_prefix('/')?.split_once('/')?;
    (!service.is_empty() && !method.is_empty() && !method.contains('/'))
        .then_some((service, method))
}

/// Validates the syntax of a method path; see [`split_method_path`].
pub fn validate_method_path(path: &str) -> std::result::Result<(), crate::Status> {
    if split_method_path(path).is_some() {
        Ok(())
    } else {
        Err(crate::Status::invalid_argument(format!(
            "method {path:?} is not of the form /<service>/<method>"
        )))
    }
}

/// The REQUEST frame payload: everything about a call except its messages.
///
/// Tags 2, 3 and 6 are reserved and must never be reused: 2 carried the method name, 3 the
/// request body before messages moved into MESSAGE frames, and 6 the wire version, which each
/// transport now negotiates for the whole connection or request instead.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RpcRequest {
    /// The method path `/package.Service/Method`; see [`split_method_path`].
    #[prost(string, tag = "1")]
    pub method: String,
    #[prost(map = "string, bytes", tag = "4")]
    pub metadata: Metadata,
    #[prost(enumeration = "RpcKind", tag = "5")]
    pub kind: i32,
    #[prost(uint64, tag = "7")]
    pub timeout_nanos: u64,
    /// Extensions the client supports. The registry is empty, so clients send none and servers
    /// ignore the field.
    #[prost(uint64, repeated, tag = "8")]
    pub extensions: Vec<u64>,
    /// The call's datagram flow ID, or zero when the call has none. A client sets it only on a
    /// connection that carries datagrams, for a method with an unreliable streaming direction;
    /// connections without datagrams ignore it. See [`crate::Unreliable`].
    #[prost(uint64, tag = "9")]
    pub datagram_flow: u64,
    /// The dictionaries the client accepts on the call's response messages; see
    /// [`crate::Compression`].
    #[prost(uint32, repeated, tag = "10")]
    pub accept_dictionaries: Vec<u32>,
    /// The dictionaries the client asks the server about, on calls until the connection has an
    /// answer.
    #[prost(message, repeated, tag = "11")]
    pub dictionary_query: Vec<DictionaryInfo>,
    /// Set by a client that implements stream compression, on calls to methods that allow it: it
    /// decodes stream-compressed responses up to this window. See [`crate::CompressStream`].
    #[prost(message, optional, tag = "12")]
    pub stream_compression: Option<StreamCompression>,
}

/// A compression dictionary in the negotiation fields: its `Dictionary_ID` and its fingerprint,
/// the first 8 bytes of its SHA-256 digest.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct DictionaryInfo {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub fingerprint: Vec<u8>,
}

/// What a peer decodes in stream compression: the largest zstd window, as a base-2 logarithm.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct StreamCompression {
    #[prost(uint32, tag = "1")]
    pub max_window_log: u32,
    /// REQUEST only: the client stream-compresses its requests without waiting for the server's
    /// answer, which accepts or refuses the call at dispatch.
    #[prost(bool, tag = "2")]
    pub optimistic: bool,
}

impl RpcRequest {
    /// Creates a unary RPC request for the method path `/package.Service/Method`.
    #[must_use]
    pub fn new(method: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            metadata: Metadata::new(),
            kind: i32::from(RpcKind::Unary),
            timeout_nanos: 0,
            extensions: Vec::new(),
            datagram_flow: 0,
            accept_dictionaries: Vec::new(),
            dictionary_query: Vec::new(),
            stream_compression: None,
        }
    }

    /// Validates the method path and the RPC kind.
    pub fn validate_protocol(&self) -> std::result::Result<(), crate::Status> {
        validate_method_path(&self.method)?;

        if RpcKind::try_from(self.kind).is_err() {
            return Err(crate::Status::invalid_argument(format!(
                "unsupported TrevRPC RPC kind {}",
                self.kind
            )));
        }

        Ok(())
    }

    /// Returns the decoded RPC kind, defaulting to unary for invalid values.
    #[must_use]
    pub fn rpc_kind(&self) -> RpcKind {
        RpcKind::try_from(self.kind).unwrap_or(RpcKind::Unary)
    }

    /// Sets the RPC kind encoded in the request.
    #[must_use]
    pub fn with_kind(mut self, kind: RpcKind) -> Self {
        self.kind = i32::from(kind);
        self
    }

    /// Sets the timeout advertised by the request in nanoseconds.
    #[must_use]
    pub const fn with_timeout_nanos(mut self, timeout_nanos: u64) -> Self {
        self.timeout_nanos = timeout_nanos;
        self
    }

    /// Sets the datagram flow ID of the request; zero means the call has no flow.
    #[must_use]
    pub const fn with_datagram_flow(mut self, datagram_flow: u64) -> Self {
        self.datagram_flow = datagram_flow;
        self
    }

    /// Replaces the request metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

impl RpcKind {
    /// Returns whether a request of this kind carries exactly one MESSAGE frame, so the server
    /// reads it before dispatching the handler.
    #[must_use]
    pub const fn has_single_request_message(self) -> bool {
        matches!(self, Self::Unary | Self::ServerStreaming)
    }

    /// Returns whether the client streams request messages: client-streaming and bidirectional
    /// calls.
    #[must_use]
    pub const fn streams_requests(self) -> bool {
        matches!(self, Self::ClientStreaming | Self::BidirectionalStreaming)
    }

    /// Returns whether the server streams response messages: server-streaming and bidirectional
    /// calls.
    #[must_use]
    pub const fn streams_responses(self) -> bool {
        matches!(self, Self::ServerStreaming | Self::BidirectionalStreaming)
    }
}

/// The HEADERS frame payload: response header metadata.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RpcHeaders {
    #[prost(map = "string, bytes", tag = "1")]
    pub metadata: Metadata,
    /// Extensions the server accepts. The registry is empty, so receivers ignore the field.
    #[prost(uint64, repeated, tag = "2")]
    pub extensions: Vec<u64>,
    /// The server's answer to the request's `dictionary_query`.
    #[prost(message, repeated, tag = "3")]
    pub dictionaries: Vec<DictionaryInfo>,
    /// The server's answer to the request's `stream_compression`: it decodes stream-compressed
    /// requests up to this window.
    #[prost(message, optional, tag = "4")]
    pub stream_compression: Option<StreamCompression>,
}

/// The STATUS frame payload: the terminal status and trailing metadata.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RpcStatus {
    #[prost(uint32, tag = "1")]
    pub code: u32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(map = "string, bytes", tag = "3")]
    pub metadata: Metadata,
    /// The server refused an optimistic call before any application code ran, so the client may
    /// replay it without stream compression. Meaningless on any other STATUS.
    #[prost(bool, tag = "4")]
    pub refused_stream_compression: bool,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::fmt::Debug;

    use prost::Message;

    use crate::framed::{next_known_header, read_payload};
    use crate::framing::{
        DEFAULT_MAX_FRAME_SIZE, decode_payload, encode_frame, encode_protobuf_frame, frame_type,
    };
    use crate::test_io::Script;
    use crate::{Code, Metadata, Status};

    use super::{
        MAX_METADATA_ENTRIES, MAX_METADATA_TOTAL_SIZE, MAX_METADATA_VALUE_LEN, RpcHeaders, RpcKind,
        RpcRequest, RpcStatus, normalize_metadata_key, split_method_path, validate_metadata,
    };

    #[test]
    fn validates_well_formed_metadata() {
        let mut metadata = Metadata::new();
        metadata.insert("authorization".to_owned(), b"Bearer token".to_vec());

        assert_eq!(validate_metadata(&metadata), Ok(()));
    }

    #[test]
    fn normalizes_metadata_keys_to_lowercase_ascii() {
        assert_eq!(normalize_metadata_key("Authorization"), "authorization");
    }

    #[test]
    fn rejects_invalid_metadata_keys() {
        for key in ["", "Authorization", "trevrpc-timeout", "bad key"] {
            let mut metadata = Metadata::new();
            metadata.insert(key.to_owned(), Vec::new());

            let status = validate_metadata(&metadata).expect_err("metadata should be invalid");

            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[test]
    fn rejects_metadata_limit_violations() {
        let mut too_many_entries = Metadata::new();
        for index in 0..=MAX_METADATA_ENTRIES {
            too_many_entries.insert(format!("key-{index}"), Vec::new());
        }
        assert!(validate_metadata(&too_many_entries).is_err());

        let mut value_too_large = Metadata::new();
        value_too_large.insert("key".to_owned(), vec![0; MAX_METADATA_VALUE_LEN + 1]);
        assert!(validate_metadata(&value_too_large).is_err());

        let mut total_too_large = Metadata::new();
        total_too_large.insert("key".to_owned(), vec![0; MAX_METADATA_TOTAL_SIZE + 1]);
        assert!(validate_metadata(&total_too_large).is_err());
    }

    #[test]
    fn metadata_boundary_cases_are_stable() {
        let mut max_entries = Metadata::new();
        for index in 0..MAX_METADATA_ENTRIES {
            max_entries.insert(format!("key-{index}"), Vec::new());
        }
        assert_eq!(validate_metadata(&max_entries), Ok(()));

        let mut max_key = Metadata::new();
        max_key.insert("a".repeat(super::MAX_METADATA_KEY_LEN), Vec::new());
        assert_eq!(validate_metadata(&max_key), Ok(()));

        let mut max_value = Metadata::new();
        max_value.insert("key".to_owned(), vec![0; MAX_METADATA_VALUE_LEN]);
        assert_eq!(validate_metadata(&max_value), Ok(()));

        let mut exact_total = Metadata::new();
        for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            exact_total.insert(key.to_owned(), vec![0; 8191]);
        }
        assert_eq!(validate_metadata(&exact_total), Ok(()));

        exact_total.insert("i".to_owned(), Vec::new());
        let status = validate_metadata(&exact_total).expect_err("total metadata size should fail");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn request_defaults_to_unary_kind() {
        let request = RpcRequest::new("/service/method");

        assert_eq!(request.rpc_kind(), RpcKind::Unary);
        assert!(request.extensions.is_empty());
    }

    #[test]
    fn single_request_message_kinds_are_unary_and_server_streaming() {
        assert!(RpcKind::Unary.has_single_request_message());
        assert!(RpcKind::ServerStreaming.has_single_request_message());
        assert!(!RpcKind::ClientStreaming.has_single_request_message());
        assert!(!RpcKind::BidirectionalStreaming.has_single_request_message());
    }

    #[test]
    fn splits_well_formed_method_paths() {
        assert_eq!(split_method_path("/svc/m"), Some(("svc", "m")));
        assert_eq!(
            split_method_path("/example.greeter.Greeter/SayHello"),
            Some(("example.greeter.Greeter", "SayHello"))
        );
    }

    #[test]
    fn rejects_malformed_method_paths() {
        for path in [
            "", "/", "//", "svc/m", "/svc", "/svc/", "//m", "/svc/m/", "/svc/m/x", "svc.m",
            "/svc//m",
        ] {
            assert_eq!(split_method_path(path), None, "{path:?}");

            let status = RpcRequest::new(path)
                .validate_protocol()
                .expect_err("malformed method path should be rejected");
            assert_eq!(status.code(), Code::InvalidArgument, "{path:?}");
        }
    }

    #[test]
    fn rejects_unknown_rpc_kinds() {
        let mut request = RpcRequest::new("/service/method");
        request.kind = 99;

        let status = request
            .validate_protocol()
            .expect_err("unknown RPC kind should be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn rpc_status_round_trips_through_status() {
        let mut metadata = Metadata::new();
        metadata.insert("x-t".to_owned(), b"2".to_vec());
        let status = Status::new(Code::FailedPrecondition, "stop").with_metadata(metadata);

        let wire = RpcStatus::from(status.clone());
        assert_eq!(wire.code, 9);
        assert_eq!(Status::from(wire), status);
        assert_eq!(
            Status::from(RpcStatus {
                code: 9999,
                ..RpcStatus::default()
            })
            .code(),
            Code::Unknown
        );
    }

    /// The parsed fixture: hex values by name, and the `.frames` entries as written.
    struct Vectors {
        bytes: HashMap<String, Vec<u8>>,
        frames: HashMap<String, String>,
    }

    impl Vectors {
        fn load() -> Self {
            let mut vectors = Self {
                bytes: HashMap::new(),
                frames: HashMap::new(),
            };
            for (index, line) in include_str!("../../testdata/wire-golden-vectors.txt")
                .lines()
                .enumerate()
            {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }

                // An empty value has no trailing space, so split on the first `=` and trim.
                let (name, value) = line.split_once('=').unwrap_or_else(|| {
                    panic!(
                        "invalid wire golden vector line {}",
                        index.saturating_add(1)
                    )
                });
                let (name, value) = (name.trim(), value.trim());
                // The datagram entries are not frames; `datagram::tests` checks them.
                if name.starts_with("datagram.") {
                    continue;
                }
                if name.ends_with(".frames") {
                    vectors.frames.insert(name.to_owned(), value.to_owned());
                } else {
                    vectors.bytes.insert(name.to_owned(), decode_hex(value));
                }
            }
            vectors
        }

        fn get(&self, name: &str) -> &[u8] {
            self.bytes.get(name).map_or_else(
                || panic!("missing wire golden vector {name:?}"),
                Vec::as_slice,
            )
        }

        /// Names that have a `.frame` entry.
        fn frame_names(&self) -> BTreeSet<String> {
            self.bytes
                .keys()
                .filter_map(|name| name.strip_suffix(".frame"))
                .map(str::to_owned)
                .collect()
        }

        /// Names that have a `.stream` entry.
        fn stream_names(&self) -> BTreeSet<String> {
            self.bytes
                .keys()
                .filter_map(|name| name.strip_suffix(".stream"))
                .map(str::to_owned)
                .collect()
        }
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0, "invalid hex length for {hex:?}");

        let (pairs, _) = hex.as_bytes().as_chunks::<2>();
        pairs
            .iter()
            .map(|pair| {
                std::str::from_utf8(pair)
                    .ok()
                    .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                    .unwrap_or_else(|| panic!("invalid hex byte in {hex:?}"))
            })
            .collect()
    }

    /// Reads every known frame of `wire` with the real frame reader, in reads of `read_size`.
    async fn read_known_frames(wire: &[u8], read_size: usize) -> Vec<(u64, Vec<u8>)> {
        let mut recv = Script::with_read_size(wire.to_vec(), read_size);
        let mut frames = Vec::new();
        while let Some(header) =
            next_known_header(&mut recv, DEFAULT_MAX_FRAME_SIZE, &mut |_| Ok(()))
                .await
                .expect("stream should parse")
        {
            let payload = read_payload(&mut recv, header.payload_len)
                .await
                .expect("payload should read");
            frames.push((header.frame_type, payload.to_vec()));
        }
        frames
    }

    /// Checks one protobuf frame: the documented logical value encodes to the fixture's payload
    /// and frame bytes, the payload decodes back to the value, and the real frame reader reads
    /// the frame back.
    async fn check_protobuf_frame<M>(
        vectors: &Vectors,
        checked: &mut BTreeSet<String>,
        name: &str,
        frame_type: u64,
        logical: &M,
    ) where
        M: Message + Default + PartialEq + Debug + 'static,
    {
        let payload = logical.encode_to_vec();
        let frame = encode_protobuf_frame(frame_type, logical, DEFAULT_MAX_FRAME_SIZE)
            .expect("frame should encode");

        assert_eq!(payload, vectors.get(&format!("{name}.payload")), "{name}");
        assert_eq!(frame, vectors.get(&format!("{name}.frame")), "{name}");
        assert_eq!(
            &decode_payload::<M>(&payload).expect("payload should decode"),
            logical,
            "{name}"
        );
        assert_eq!(
            read_known_frames(&frame, 2).await,
            [(frame_type, payload)],
            "{name}"
        );
        checked.insert(name.to_owned());
    }

    async fn check_raw_frame(
        vectors: &Vectors,
        checked: &mut BTreeSet<String>,
        name: &str,
        frame_type: u64,
        payload: &[u8],
    ) {
        let frame =
            encode_frame(frame_type, payload, DEFAULT_MAX_FRAME_SIZE).expect("frame should encode");

        assert_eq!(payload, vectors.get(&format!("{name}.payload")), "{name}");
        assert_eq!(frame, vectors.get(&format!("{name}.frame")), "{name}");
        // Grease frames are skipped, so only defined types come back from the reader.
        let expected: Vec<_> = if crate::framing::is_known_frame_type(frame_type) {
            vec![(frame_type, payload.to_vec())]
        } else {
            Vec::new()
        };
        assert_eq!(read_known_frames(&frame, 2).await, expected, "{name}");
        checked.insert(name.to_owned());
    }

    fn metadata(key: &str, value: &[u8]) -> Metadata {
        Metadata::from([(key.to_owned(), value.to_vec())])
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn wire_golden_vectors_stay_stable() {
        let vectors = Vectors::load();
        let mut checked = BTreeSet::new();
        let request = || RpcRequest::new("/svc/m");

        check_protobuf_frame(
            &vectors,
            &mut checked,
            "request.unary",
            frame_type::REQUEST,
            &request(),
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "request.bidi_timeout",
            frame_type::REQUEST,
            &request()
                .with_kind(RpcKind::BidirectionalStreaming)
                .with_timeout_nanos(123_456),
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "request.metadata",
            frame_type::REQUEST,
            &request().with_metadata(metadata("authorization", b"ok")),
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "request.extensions",
            frame_type::REQUEST,
            &RpcRequest {
                extensions: vec![1, 300],
                ..request()
            },
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "request.datagram_flow",
            frame_type::REQUEST,
            &request()
                .with_kind(RpcKind::BidirectionalStreaming)
                .with_datagram_flow(1),
        )
        .await;

        check_raw_frame(
            &vectors,
            &mut checked,
            "message.hi",
            frame_type::MESSAGE,
            b"hi",
        )
        .await;
        check_raw_frame(
            &vectors,
            &mut checked,
            "message.empty",
            frame_type::MESSAGE,
            b"",
        )
        .await;

        check_protobuf_frame(
            &vectors,
            &mut checked,
            "headers.metadata",
            frame_type::HEADERS,
            &RpcHeaders {
                metadata: metadata("x-a", b"1"),
                extensions: Vec::new(),
                dictionaries: Vec::new(),
                stream_compression: None,
            },
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "headers.empty",
            frame_type::HEADERS,
            &RpcHeaders::default(),
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "headers.extensions",
            frame_type::HEADERS,
            &RpcHeaders {
                metadata: Metadata::new(),
                extensions: vec![1],
                dictionaries: Vec::new(),
                stream_compression: None,
            },
        )
        .await;

        check_protobuf_frame(
            &vectors,
            &mut checked,
            "status.ok",
            frame_type::STATUS,
            &RpcStatus::from(Status::ok()),
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "status.unavailable",
            frame_type::STATUS,
            &RpcStatus::from(Status::unavailable("down")),
        )
        .await;
        check_protobuf_frame(
            &vectors,
            &mut checked,
            "status.metadata",
            frame_type::STATUS,
            &RpcStatus::from(
                Status::new(Code::FailedPrecondition, "stop").with_metadata(metadata("x-t", b"2")),
            ),
        )
        .await;

        check_raw_frame(
            &vectors,
            &mut checked,
            "grease.two_byte_type",
            0x40,
            &[0, 1, 2],
        )
        .await;

        assert_eq!(
            checked,
            vectors.frame_names(),
            "every .frame vector needs a logical value check"
        );
    }

    #[tokio::test]
    async fn wire_golden_streams_parse_to_the_documented_frames() {
        let vectors = Vectors::load();
        let names = vectors.stream_names();
        assert!(!names.is_empty());
        let frame_entries: BTreeSet<_> = vectors
            .frames
            .keys()
            .filter_map(|name| name.strip_suffix(".frames"))
            .map(str::to_owned)
            .collect();
        assert_eq!(names, frame_entries, "every .stream needs a .frames");

        for name in names {
            let expected: Vec<(u64, Vec<u8>)> = vectors
                .frames
                .get(&format!("{name}.frames"))
                .map(|frames| {
                    frames
                        .split(',')
                        .filter(|frame| !frame.is_empty())
                        .map(|frame| {
                            let (frame_type, payload) = frame
                                .split_once(':')
                                .unwrap_or_else(|| panic!("invalid frames entry {frame:?}"));
                            (
                                frame_type.parse().expect("frame type should be decimal"),
                                decode_hex(payload),
                            )
                        })
                        .collect()
                })
                .expect("frames entry was checked");
            let wire = vectors.get(&format!("{name}.stream"));

            // The same frames come back however the transport slices the bytes.
            for read_size in [1, 2, 3, 7, 4096] {
                assert_eq!(
                    read_known_frames(wire, read_size).await,
                    expected,
                    "{name} with reads of {read_size}"
                );
            }
        }
    }

    #[tokio::test]
    async fn payloads_in_the_golden_streams_decode_as_their_frame_types() {
        let vectors = Vectors::load();
        let wire = vectors.get("stream.server_stream.stream");
        let frames = read_known_frames(wire, 4096).await;

        let types: Vec<u64> = frames.iter().map(|(frame_type, _)| *frame_type).collect();
        assert_eq!(
            types,
            [
                frame_type::HEADERS,
                frame_type::MESSAGE,
                frame_type::MESSAGE,
                frame_type::STATUS
            ]
        );
        let headers = decode_payload::<RpcHeaders>(&frames[0].1).expect("headers should decode");
        assert_eq!(headers.metadata, metadata("x-a", b"1"));
        assert_eq!(frames[1].1, b"hi");
        assert!(frames[2].1.is_empty());
        let status = Status::from(decode_payload::<RpcStatus>(&frames[3].1).expect("status"));
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), "stop");
        assert_eq!(status.metadata(), &metadata("x-t", b"2"));
    }
}
