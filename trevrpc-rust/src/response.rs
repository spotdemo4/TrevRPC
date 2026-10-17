use crate::Metadata;

/// A successful response value together with its response headers and trailing metadata.
///
/// Headers travel in the HEADERS frame ahead of the first message, so streaming clients see them
/// before any response arrives. Trailing metadata travels in the final STATUS frame and is
/// visible only once the call completes.
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
