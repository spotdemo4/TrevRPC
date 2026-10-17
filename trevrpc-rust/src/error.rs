use std::error::Error as StdError;
use std::fmt;

use crate::Status;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Status(Status),
    Encode(prost::EncodeError),
    Decode(prost::DecodeError),
    FrameTooLarge { len: usize, max: usize },
    Transport(Box<dyn StdError + Send + Sync>),
}

impl Error {
    /// Wraps a transport-layer error.
    #[must_use]
    pub fn transport(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::Transport(Box::new(error))
    }

    pub(crate) fn transport_code(&self) -> Option<crate::Code> {
        match self {
            Self::Transport(error) => Some(transport_status(error.as_ref()).code()),
            _ => None,
        }
    }

    /// Converts the error into the RPC status that should be returned to callers.
    #[must_use]
    pub fn into_status(self) -> Status {
        match self {
            Self::Status(status) => status,
            Self::FrameTooLarge { len, max } => {
                Status::resource_exhausted(format!("frame length {len} exceeds maximum {max}"))
            }
            Self::Transport(error) => transport_status(error.as_ref()),
            Self::Decode(error) => {
                Status::invalid_argument(format!("failed to decode protobuf message: {error}"))
            }
            Self::Encode(error) => Status::internal(error.to_string()),
        }
    }
}

fn transport_status(error: &(dyn StdError + Send + Sync + 'static)) -> Status {
    crate::transport_error::classify(error).status
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        error.into_status()
    }
}

impl From<prost::EncodeError> for Error {
    fn from(error: prost::EncodeError) -> Self {
        Self::Encode(error)
    }
}

impl From<prost::DecodeError> for Error {
    fn from(error: prost::DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(formatter, "{status}"),
            Self::Encode(error) => write!(formatter, "protobuf encode error: {error}"),
            Self::Decode(error) => write!(formatter, "protobuf decode error: {error}"),
            Self::FrameTooLarge { len, max } => {
                write!(formatter, "frame length {len} exceeds maximum {max}")
            }
            Self::Transport(error) => write!(formatter, "transport error: {error}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::FrameTooLarge { .. } => None,
            Self::Transport(error) => Some(error.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{Code, Error, RpcRequest};

    #[test]
    fn frame_too_large_maps_to_resource_exhausted() {
        let status = Error::FrameTooLarge { len: 11, max: 10 }.into_status();

        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[test]
    fn protobuf_decode_errors_map_to_invalid_argument() {
        let status = crate::framing::decode_frame::<RpcRequest>(&[0xff, 0xff])
            .expect_err("malformed protobuf should fail")
            .into_status();

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn transport_io_failures_map_to_unavailable() {
        let status = Error::transport(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        ))
        .into_status();

        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn quinn_local_close_maps_to_cancelled() {
        let status = Error::transport(quinn::ConnectionError::LocallyClosed).into_status();

        assert_eq!(status.code(), Code::Cancelled);
    }

    #[test]
    fn quinn_remote_reset_maps_to_cancelled() {
        let status = Error::transport(quinn::ReadError::Reset(1_u32.into())).into_status();

        assert_eq!(status.code(), Code::Cancelled);
    }

    #[test]
    fn quinn_connection_timeout_maps_to_unavailable() {
        let status = Error::transport(quinn::ConnectionError::TimedOut).into_status();

        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn h3_quic_stream_reset_maps_to_cancelled() {
        let status =
            Error::transport(h3::quic::StreamErrorIncoming::StreamTerminated { error_code: 1 })
                .into_status();

        assert_eq!(status.code(), Code::Cancelled);
    }
}
