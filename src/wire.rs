use std::collections::HashMap;

pub type Metadata = HashMap<String, Vec<u8>>;

pub const MAX_METADATA_ENTRIES: usize = 64;
pub const MAX_METADATA_KEY_LEN: usize = 128;
pub const MAX_METADATA_VALUE_LEN: usize = 8 * 1024;
pub const MAX_METADATA_TOTAL_SIZE: usize = 64 * 1024;
pub const RESERVED_METADATA_PREFIX: &str = "trevrpc-";

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
}

impl RpcRequest {
    #[must_use]
    pub fn new(service: impl Into<String>, method: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            service: service.into(),
            method: method.into(),
            body,
            metadata: Metadata::new(),
        }
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

#[cfg(test)]
mod tests {
    use crate::{Code, Metadata};

    use super::{
        MAX_METADATA_ENTRIES, MAX_METADATA_TOTAL_SIZE, MAX_METADATA_VALUE_LEN,
        normalize_metadata_key, validate_metadata,
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
}
