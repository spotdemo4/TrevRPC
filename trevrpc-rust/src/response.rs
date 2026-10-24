use std::sync::{Arc, Mutex};

use crate::sync::MutexExt as _;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{Metadata, Status};

/// A successful response value together with its response headers and trailing metadata, as a
/// client receives it.
///
/// Headers travel in the HEADERS frame ahead of the first message, so streaming clients see them
/// before any response arrives. Trailing metadata travels in the final STATUS frame and is
/// visible only once the call completes. Handlers do not return envelopes: they set response
/// metadata through their [`RequestContext`](crate::server::RequestContext).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseEnvelope<T> {
    message: T,
    headers: Metadata,
    metadata: Metadata,
}

impl<T> ResponseEnvelope<T> {
    #[must_use]
    pub fn new(message: T) -> Self {
        Self {
            message,
            headers: Metadata::new(),
            metadata: Metadata::new(),
        }
    }

    /// Sets the response headers, sent before the first response message.
    #[must_use]
    pub fn with_headers(mut self, headers: Metadata) -> Self {
        self.headers = headers;
        self
    }

    /// Sets the trailing metadata, sent with the final `OK` status.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub const fn message(&self) -> &T {
        &self.message
    }

    /// Returns the response headers.
    #[must_use]
    pub const fn headers(&self) -> &Metadata {
        &self.headers
    }

    /// Returns the trailing metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Splits the envelope into the message, the response headers, and the trailing metadata.
    #[must_use]
    pub fn into_parts(self) -> (T, Metadata, Metadata) {
        (self.message, self.headers, self.metadata)
    }

    /// Maps the successful response value while preserving headers and metadata.
    #[must_use]
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> ResponseEnvelope<U> {
        ResponseEnvelope {
            message: map(self.message),
            headers: self.headers,
            metadata: self.metadata,
        }
    }
}

impl<T> From<T> for ResponseEnvelope<T> {
    fn from(message: T) -> Self {
        Self::new(message)
    }
}

/// The response headers and trailing metadata a handler sets through its request context, shared
/// by every clone of the context.
///
/// The server takes the headers when the handler returns and the trailing metadata when the
/// response ends; setting either afterwards fails. On calls that send HEADERS at dispatch the
/// headers count as sent from the start.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResponseMetadata {
    state: Arc<Mutex<ResponseMetadataState>>,
}

#[derive(Debug, Default)]
struct ResponseMetadataState {
    headers: Metadata,
    metadata: Metadata,
    headers_sent: bool,
    metadata_sent: bool,
}

impl ResponseMetadataState {
    fn set(
        &mut self,
        kind: ResponseMetadataKind,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), Status> {
        let (target, sent, what) = match kind {
            ResponseMetadataKind::Headers => {
                (&mut self.headers, self.headers_sent, "response headers")
            }
            ResponseMetadataKind::Trailers => {
                (&mut self.metadata, self.metadata_sent, "response metadata")
            }
        };
        if sent {
            return Err(Status::internal(format!("{what} already sent")));
        }
        let previous = target.insert(key.clone(), value);
        if let Err(status) = validate_metadata(target) {
            match previous {
                Some(previous) => target.insert(key, previous),
                None => target.remove(&key),
            };
            return Err(Status::internal(format!(
                "invalid {what}: {}",
                status.message()
            )));
        }
        Ok(())
    }
}

/// Which of the two kinds of response metadata a value is set on.
#[derive(Clone, Copy)]
enum ResponseMetadataKind {
    Headers,
    Trailers,
}

impl ResponseMetadata {
    /// Response metadata whose headers have already been sent, so setting them fails.
    pub(crate) fn with_headers_sent() -> Self {
        Self {
            state: Arc::new(Mutex::new(ResponseMetadataState {
                headers_sent: true,
                ..ResponseMetadataState::default()
            })),
        }
    }

    pub(crate) fn set_header(&self, key: &str, value: Vec<u8>) -> Result<(), Status> {
        self.set(ResponseMetadataKind::Headers, key, value)
    }

    pub(crate) fn set_metadata(&self, key: &str, value: Vec<u8>) -> Result<(), Status> {
        self.set(ResponseMetadataKind::Trailers, key, value)
    }

    fn set(&self, kind: ResponseMetadataKind, key: &str, value: Vec<u8>) -> Result<(), Status> {
        let key = normalize_metadata_key(key);
        self.state.lock_unpoisoned().set(kind, key, value)
    }

    /// Returns the headers to send and makes later headers fail.
    pub(crate) fn take_headers(&self) -> Metadata {
        let mut state = self.state.lock_unpoisoned();
        state.headers_sent = true;
        std::mem::take(&mut state.headers)
    }

    /// Returns the trailing metadata to send with the final `OK` status and makes later response
    /// metadata of either kind fail.
    pub(crate) fn take_metadata(&self) -> Metadata {
        let mut state = self.state.lock_unpoisoned();
        state.headers_sent = true;
        state.metadata_sent = true;
        std::mem::take(&mut state.metadata)
    }
}
