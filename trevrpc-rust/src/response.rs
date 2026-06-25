use crate::Metadata;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseEnvelope<T> {
    message: T,
    metadata: Metadata,
}

impl<T> ResponseEnvelope<T> {
    #[must_use]
    pub fn new(message: T) -> Self {
        Self {
            message,
            metadata: Metadata::new(),
        }
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub const fn message(&self) -> &T {
        &self.message
    }

    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[must_use]
    pub fn into_parts(self) -> (T, Metadata) {
        (self.message, self.metadata)
    }
}

impl<T> From<T> for ResponseEnvelope<T> {
    fn from(message: T) -> Self {
        Self::new(message)
    }
}
