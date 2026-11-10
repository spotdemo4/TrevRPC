use std::fmt;
use std::time::Duration;

use crate::retry_info::{self, RETRY_INFO_TYPE};
use crate::{Metadata, RpcStatus};

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
    details: Vec<Detail>,
    /// Set only on the STATUS that refuses an optimistic call, which the runtime writes itself:
    /// application code cannot set it, and a decoded STATUS never carries it, so a handler that
    /// forwards another call's status can never make its client replay a call.
    refused_stream_compression: bool,
}

impl Status {
    /// Creates a status from a status code and message.
    #[must_use]
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            metadata: Metadata::new(),
            details: Vec::new(),
            refused_stream_compression: false,
        }
    }

    /// The STATUS that refuses an optimistic call: the server does not decode its
    /// stream-compressed requests, and no application code ran.
    pub(crate) fn refused_stream_compression() -> Self {
        Self {
            refused_stream_compression: true,
            // No message, so the frame is the same few bytes on every server.
            ..Self::resource_exhausted("")
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

    /// Creates an unknown status.
    #[must_use]
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(Code::Unknown, message)
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

    /// Creates an already-exists status.
    #[must_use]
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(Code::AlreadyExists, message)
    }

    /// Creates a permission-denied status.
    #[must_use]
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(Code::PermissionDenied, message)
    }

    /// Creates a resource-exhausted status.
    #[must_use]
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(Code::ResourceExhausted, message)
    }

    /// Creates an aborted status.
    #[must_use]
    pub fn aborted(message: impl Into<String>) -> Self {
        Self::new(Code::Aborted, message)
    }

    /// Creates an out-of-range status.
    #[must_use]
    pub fn out_of_range(message: impl Into<String>) -> Self {
        Self::new(Code::OutOfRange, message)
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

    /// Creates a data-loss status.
    #[must_use]
    pub fn data_loss(message: impl Into<String>) -> Self {
        Self::new(Code::DataLoss, message)
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
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Attaches metadata to this status.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Returns the details of this status: protobuf messages that describe a failure in more
    /// depth, such as the `buf.validate.Violations` of a request that broke its validation rules.
    #[must_use]
    pub fn details(&self) -> &[Detail] {
        &self.details
    }

    /// Attaches details to this status in place of any it had.
    #[must_use]
    pub fn with_details(mut self, details: impl IntoIterator<Item = Detail>) -> Self {
        self.details = details.into_iter().collect();
        self
    }

    /// Tells the client to wait `delay` before it retries, with a `google.rpc.RetryInfo` detail.
    /// The detail replaces any `RetryInfo` the status had and leaves its other details alone.
    /// Clients read the delay with [`Status::retry_delay`], or the equivalent in their language.
    #[must_use]
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.details
            .retain(|detail| detail.type_name != RETRY_INFO_TYPE);
        self.details
            .push(Detail::new(RETRY_INFO_TYPE, retry_info::encode(delay)));
        self
    }

    /// Returns how long the status tells the client to wait before it retries, from its first
    /// well-formed `google.rpc.RetryInfo` detail, such as that of a call a server's rate limit
    /// rejected. `None` when it has none. A detail that does not decode, or whose delay is
    /// negative or not a valid `google.protobuf.Duration`, is ignored.
    #[must_use]
    pub fn retry_delay(&self) -> Option<Duration> {
        self.details
            .iter()
            .filter(|detail| detail.type_name == RETRY_INFO_TYPE)
            .find_map(|detail| retry_info::decode(&detail.value))
    }

    /// Returns the first detail whose message type is `type_name`, a full name such as
    /// `buf.validate.Violations`.
    #[must_use]
    pub fn find_detail(&self, type_name: &str) -> Option<&Detail> {
        self.details
            .iter()
            .find(|detail| detail.type_name == type_name)
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
}

impl From<Status> for RpcStatus {
    fn from(status: Status) -> Self {
        Self {
            code: status.code.as_u32(),
            message: status.message,
            metadata: status.metadata,
            refused_stream_compression: status.refused_stream_compression,
            details: status.details,
        }
    }
}

impl From<RpcStatus> for Status {
    fn from(status: RpcStatus) -> Self {
        Self::new(Code::from_u32(status.code), status.message)
            .with_metadata(status.metadata)
            .with_details(status.details)
    }
}

/// One detail of a [`Status`]: a protobuf message, carried as its full name and its encoding so
/// that a receiver decodes only the details it reads.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Detail {
    #[prost(string, tag = "1")]
    type_name: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

impl Detail {
    /// Creates a detail from a message's full name, such as `buf.validate.Violations`, and its
    /// protobuf encoding.
    #[must_use]
    pub fn new(type_name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            type_name: type_name.into(),
            value: value.into(),
        }
    }

    /// Creates a detail that carries `message`.
    #[must_use]
    pub fn from_message<M: prost::Name>(message: &M) -> Self {
        Self::new(M::full_name(), message.encode_to_vec())
    }

    /// Returns the full name of the message the detail carries.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the protobuf encoding of the message the detail carries.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Decodes the message the detail carries. The caller picks `M` by the detail's
    /// [`type_name`](Self::type_name).
    ///
    /// # Errors
    ///
    /// Returns the decode error if the value is not a valid encoding of `M`.
    pub fn decode<M: prost::Message + Default>(&self) -> Result<M, prost::DecodeError> {
        M::decode(self.value.as_slice())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Code, Detail, Status};
    use crate::{Error, RpcStatus};

    #[test]
    fn with_retry_delay_keeps_other_details_and_replaces_itself() {
        let other = Detail::new("x.Y", vec![1]);
        let first = Status::resource_exhausted("slow down")
            .with_details([other.clone()])
            .with_retry_delay(Duration::from_secs(1));
        let second = first.clone().with_retry_delay(Duration::from_secs(5));

        assert_eq!(first.retry_delay(), Some(Duration::from_secs(1)));
        assert_eq!(second.retry_delay(), Some(Duration::from_secs(5)));
        assert_eq!(second.details().len(), 2);
        assert_eq!(second.details().first(), Some(&other));
        assert_eq!(
            second.details().get(1).map(Detail::type_name),
            Some("google.rpc.RetryInfo")
        );
        assert_eq!(second.code(), Code::ResourceExhausted);
        assert_eq!(second.message(), "slow down");
    }

    #[test]
    fn the_retry_delay_is_the_golden_encoding() {
        let status = Status::resource_exhausted("slow down").with_retry_delay(Duration::new(1, 2));

        assert_eq!(
            status
                .find_detail("google.rpc.RetryInfo")
                .map(Detail::value),
            Some([0x0a, 0x04, 0x08, 0x01, 0x10, 0x02].as_slice())
        );
    }

    #[test]
    fn retry_delay_ignores_malformed_details() {
        let status = Status::resource_exhausted("slow down").with_details([
            Detail::new("google.rpc.RetryInfo", vec![0x0a, 0x09]),
            Detail::new("other.Type", vec![0x0a, 0x02, 0x08, 0x09]),
            Detail::new("google.rpc.RetryInfo", vec![0x0a, 0x02, 0x08, 0x03]),
        ]);

        assert_eq!(status.retry_delay(), Some(Duration::from_secs(3)));
        assert_eq!(status.with_details([]).retry_delay(), None);
    }

    #[test]
    fn the_retry_delay_survives_the_wire_and_is_read_from_errors() {
        let status =
            Status::resource_exhausted("slow down").with_retry_delay(Duration::from_millis(1500));
        let decoded = Status::from(RpcStatus::from(status));

        assert_eq!(decoded.retry_delay(), Some(Duration::from_millis(1500)));
        assert_eq!(
            Error::from(decoded).retry_delay(),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(Error::from(Status::internal("x")).retry_delay(), None);
    }
}
