use std::fmt;

use crate::{Metadata, RpcResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Code {
    Ok,
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

impl Code {
    /// Returns the wire-format numeric status code.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::Ok => 0,
            Self::Cancelled => 1,
            Self::Unknown => 2,
            Self::InvalidArgument => 3,
            Self::DeadlineExceeded => 4,
            Self::NotFound => 5,
            Self::AlreadyExists => 6,
            Self::PermissionDenied => 7,
            Self::ResourceExhausted => 8,
            Self::FailedPrecondition => 9,
            Self::Aborted => 10,
            Self::OutOfRange => 11,
            Self::Unimplemented => 12,
            Self::Internal => 13,
            Self::Unavailable => 14,
            Self::DataLoss => 15,
            Self::Unauthenticated => 16,
        }
    }

    /// Converts a wire-format numeric status code into a `Code`.
    #[must_use]
    pub const fn from_u32(code: u32) -> Self {
        match code {
            0 => Self::Ok,
            1 => Self::Cancelled,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    code: Code,
    message: String,
    metadata: Metadata,
}

impl Status {
    /// Creates a status from a status code and message.
    #[must_use]
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            metadata: Metadata::new(),
        }
    }

    /// Creates an OK status.
    #[must_use]
    pub fn ok() -> Self {
        Self::new(Code::Ok, "")
    }

    /// Creates a cancelled status.
    #[must_use]
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(Code::Cancelled, message)
    }

    /// Creates an internal error status.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Code::Internal, message)
    }

    /// Creates an invalid-argument status.
    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    /// Creates a deadline-exceeded status.
    #[must_use]
    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(Code::DeadlineExceeded, message)
    }

    /// Creates a not-found status.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }

    /// Creates a resource-exhausted status.
    #[must_use]
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(Code::ResourceExhausted, message)
    }

    /// Creates an unavailable status.
    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, message)
    }

    /// Creates a failed-precondition status.
    #[must_use]
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(Code::FailedPrecondition, message)
    }

    /// Creates an unauthenticated status.
    #[must_use]
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(Code::Unauthenticated, message)
    }

    /// Creates an unimplemented status.
    #[must_use]
    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::new(Code::Unimplemented, message)
    }

    /// Returns the status code.
    #[must_use]
    pub const fn code(&self) -> Code {
        self.code
    }

    /// Returns the status message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns terminal or response metadata carried by this status.
    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Attaches metadata to this status.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Splits the status into its code and message.
    #[must_use]
    pub fn into_parts(self) -> (Code, String) {
        (self.code, self.message)
    }

    /// Returns whether this status is OK.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(self.code, Code::Ok)
    }

    /// Converts the status and response body into an RPC response.
    #[must_use]
    pub fn into_response(self, body: Vec<u8>) -> RpcResponse {
        let metadata = self.metadata.clone();
        self.into_response_with_metadata(body, metadata)
    }

    /// Converts the status, response body, and metadata into an RPC response.
    #[must_use]
    pub fn into_response_with_metadata(
        self,
        body: Vec<u8>,
        metadata: crate::Metadata,
    ) -> RpcResponse {
        RpcResponse {
            status: self.code.as_u32(),
            message: self.message,
            body,
            metadata,
        }
    }

    /// Builds a status from an RPC response status code, message, and metadata.
    #[must_use]
    pub fn from_response(response: &RpcResponse) -> Self {
        Self::new(Code::from_u32(response.status), response.message.clone())
            .with_metadata(response.metadata.clone())
    }
}

impl fmt::Display for Status {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(formatter, "{:?}", self.code)
        } else {
            write!(formatter, "{:?}: {}", self.code, self.message)
        }
    }
}

impl std::error::Error for Status {}
