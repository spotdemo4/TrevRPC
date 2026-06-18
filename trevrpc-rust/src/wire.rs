use std::collections::HashMap;

pub type Metadata = HashMap<String, Vec<u8>>;

pub const WIRE_VERSION: u32 = 1;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum RpcStreamFrameKind {
    Message = 0,
    Status = 1,
}

#[must_use]
pub fn normalize_metadata_key(key: &str) -> String {
    key.to_ascii_lowercase()
}

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

#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcRequest {
    #[prost(string, tag = "1")]
    pub service: String,
    #[prost(string, tag = "2")]
    pub method: String,
    #[prost(bytes = "vec", tag = "3")]
    pub body: Vec<u8>,
    #[prost(map = "string, bytes", tag = "4")]
    pub metadata: Metadata,
    #[prost(enumeration = "RpcKind", tag = "5")]
    pub kind: i32,
    #[prost(uint32, tag = "6")]
    pub version: u32,
    #[prost(uint64, tag = "7")]
    pub deadline_unix_nanos: u64,
}

impl RpcRequest {
    #[must_use]
    pub fn new(service: impl Into<String>, method: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            service: service.into(),
            method: method.into(),
            body,
            metadata: Metadata::new(),
            kind: RpcKind::Unary as i32,
            version: WIRE_VERSION,
            deadline_unix_nanos: 0,
        }
    }

    pub fn validate_protocol(&self) -> std::result::Result<(), crate::Status> {
        if self.version != WIRE_VERSION {
            return Err(crate::Status::failed_precondition(format!(
                "unsupported TrevRPC wire version {}; expected {WIRE_VERSION}",
                self.version
            )));
        }

        if RpcKind::try_from(self.kind).is_err() {
            return Err(crate::Status::invalid_argument(format!(
                "unsupported TrevRPC RPC kind {}",
                self.kind
            )));
        }

        Ok(())
    }

    #[must_use]
    pub fn rpc_kind(&self) -> RpcKind {
        RpcKind::try_from(self.kind).unwrap_or(RpcKind::Unary)
    }

    #[must_use]
    pub const fn with_kind(mut self, kind: RpcKind) -> Self {
        self.kind = kind as i32;
        self
    }

    #[must_use]
    pub const fn with_deadline_unix_nanos(mut self, deadline_unix_nanos: u64) -> Self {
        self.deadline_unix_nanos = deadline_unix_nanos;
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcResponse {
    #[prost(uint32, tag = "1")]
    pub status: u32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(bytes = "vec", tag = "3")]
    pub body: Vec<u8>,
    #[prost(map = "string, bytes", tag = "4")]
    pub metadata: Metadata,
}

impl RpcResponse {
    #[must_use]
    pub fn ok(body: Vec<u8>) -> Self {
        crate::Status::ok().into_response(body)
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcStreamFrame {
    #[prost(enumeration = "RpcStreamFrameKind", tag = "1")]
    pub kind: i32,
    #[prost(uint32, tag = "2")]
    pub status: u32,
    #[prost(string, tag = "3")]
    pub message: String,
    #[prost(bytes = "vec", tag = "4")]
    pub body: Vec<u8>,
    #[prost(map = "string, bytes", tag = "5")]
    pub metadata: Metadata,
}

impl RpcStreamFrame {
    #[must_use]
    pub fn message(body: Vec<u8>) -> Self {
        Self {
            kind: RpcStreamFrameKind::Message as i32,
            status: crate::Code::Ok.as_u32(),
            message: String::new(),
            body,
            metadata: Metadata::new(),
        }
    }

    #[must_use]
    pub fn status(status: crate::Status) -> Self {
        Self::status_with_metadata(status, Metadata::new())
    }

    #[must_use]
    pub fn status_with_metadata(status: crate::Status, metadata: Metadata) -> Self {
        let (code, message) = status.into_parts();

        Self {
            kind: RpcStreamFrameKind::Status as i32,
            status: code.as_u32(),
            message,
            body: Vec::new(),
            metadata,
        }
    }

    #[must_use]
    pub fn frame_kind(&self) -> Option<RpcStreamFrameKind> {
        RpcStreamFrameKind::try_from(self.kind).ok()
    }

    #[must_use]
    pub fn status_value(&self) -> crate::Status {
        crate::Status::new(crate::Code::from_u32(self.status), self.message.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Code, Metadata};

    use super::{
        MAX_METADATA_ENTRIES, MAX_METADATA_TOTAL_SIZE, MAX_METADATA_VALUE_LEN, RpcKind, RpcRequest,
        RpcStreamFrame, RpcStreamFrameKind, WIRE_VERSION, normalize_metadata_key,
        validate_metadata,
    };

    use prost::Message;

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
    fn request_defaults_to_unary_kind() {
        let request = RpcRequest::new("service", "method", Vec::new());

        assert_eq!(request.rpc_kind(), RpcKind::Unary);
        assert_eq!(request.version, WIRE_VERSION);
    }

    #[test]
    fn rejects_unsupported_wire_versions() {
        let mut request = RpcRequest::new("service", "method", Vec::new());
        request.version = WIRE_VERSION + 1;

        let status = request
            .validate_protocol()
            .expect_err("version should be rejected");

        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[test]
    fn rejects_unknown_rpc_kinds() {
        let mut request = RpcRequest::new("service", "method", Vec::new());
        request.kind = 99;

        let status = request
            .validate_protocol()
            .expect_err("unknown RPC kind should be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn stream_frame_carries_status() {
        let frame = RpcStreamFrame::status(crate::Status::unavailable("retry later"));

        assert_eq!(frame.frame_kind(), Some(RpcStreamFrameKind::Status));
        assert_eq!(frame.status_value().code(), Code::Unavailable);
        assert_eq!(frame.status_value().message(), "retry later");
    }

    #[test]
    fn unary_request_encoding_is_stable() {
        let request = RpcRequest::new("svc", "m", b"hi".to_vec());

        assert_eq!(
            request.encode_to_vec(),
            b"\x0a\x03svc\x12\x01m\x1a\x02hi\x30\x01"
        );
    }

    #[test]
    fn stream_frame_encoding_is_stable() {
        let message = RpcStreamFrame::message(b"hi".to_vec());
        assert_eq!(message.encode_to_vec(), b"\x22\x02hi");

        let status = RpcStreamFrame::status(crate::Status::unavailable("down"));
        assert_eq!(status.encode_to_vec(), b"\x08\x01\x10\x0e\x1a\x04down");
    }
}
