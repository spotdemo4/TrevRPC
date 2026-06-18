use std::error::Error as StdError;
use std::fmt;
use std::io;

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
    #[must_use]
    pub fn transport(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::Transport(Box::new(error))
    }

    #[must_use]
    pub fn into_status(self) -> Status {
        match self {
            Self::Status(status) => status,
            Self::FrameTooLarge { len, max } => {
                Status::resource_exhausted(format!("frame length {len} exceeds maximum {max}"))
            }
            Self::Transport(error) => transport_status(error.as_ref()),
            error => Status::internal(error.to_string()),
        }
    }
}

fn transport_status(error: &(dyn StdError + Send + Sync + 'static)) -> Status {
    #[cfg(feature = "quinn")]
    if let Some(error) = error.downcast_ref::<quinn::ConnectionError>() {
        return quinn_connection_status(error);
    }

    #[cfg(feature = "quinn")]
    if let Some(error) = error.downcast_ref::<quinn::ReadError>() {
        return quinn_read_status(error);
    }

    #[cfg(feature = "quinn")]
    if let Some(error) = error.downcast_ref::<quinn::ReadExactError>() {
        return quinn_read_exact_status(error);
    }

    #[cfg(feature = "quinn")]
    if let Some(error) = error.downcast_ref::<quinn::WriteError>() {
        return quinn_write_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<wtransport::error::ConnectionError>() {
        return webtransport_connection_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<wtransport::error::StreamOpeningError>() {
        return webtransport_stream_opening_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<wtransport::error::StreamReadError>() {
        return webtransport_stream_read_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<wtransport::error::StreamReadExactError>() {
        return webtransport_stream_read_exact_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<wtransport::error::StreamWriteError>() {
        return webtransport_stream_write_status(error);
    }

    if let Some(error) = error.downcast_ref::<io::Error>() {
        return io_status(error);
    }

    transport_unavailable(error)
}

fn io_status(error: &io::Error) -> Status {
    match error.kind() {
        io::ErrorKind::BrokenPipe
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::NotConnected
        | io::ErrorKind::TimedOut
        | io::ErrorKind::UnexpectedEof => transport_unavailable(error),
        _ => Status::internal(error.to_string()),
    }
}

#[cfg(feature = "quinn")]
fn quinn_connection_status(error: &quinn::ConnectionError) -> Status {
    match error {
        quinn::ConnectionError::LocallyClosed => Status::cancelled("transport closed locally"),
        _ => transport_unavailable(error),
    }
}

#[cfg(feature = "quinn")]
fn quinn_read_status(error: &quinn::ReadError) -> Status {
    match error {
        quinn::ReadError::Reset(_)
        | quinn::ReadError::ClosedStream
        | quinn::ReadError::ZeroRttRejected => Status::cancelled(error.to_string()),
        quinn::ReadError::ConnectionLost(error) => quinn_connection_status(error),
        quinn::ReadError::IllegalOrderedRead => Status::internal(error.to_string()),
    }
}

#[cfg(feature = "quinn")]
fn quinn_read_exact_status(error: &quinn::ReadExactError) -> Status {
    match error {
        quinn::ReadExactError::FinishedEarly(_) => transport_unavailable(error),
        quinn::ReadExactError::ReadError(error) => quinn_read_status(error),
    }
}

#[cfg(feature = "quinn")]
fn quinn_write_status(error: &quinn::WriteError) -> Status {
    match error {
        quinn::WriteError::Stopped(_)
        | quinn::WriteError::ClosedStream
        | quinn::WriteError::ZeroRttRejected => Status::cancelled(error.to_string()),
        quinn::WriteError::ConnectionLost(error) => quinn_connection_status(error),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_connection_status(error: &wtransport::error::ConnectionError) -> Status {
    match error {
        wtransport::error::ConnectionError::LocallyClosed => {
            Status::cancelled("transport closed locally")
        }
        _ => transport_unavailable(error),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_opening_status(error: &wtransport::error::StreamOpeningError) -> Status {
    match error {
        wtransport::error::StreamOpeningError::Refused => Status::cancelled(error.to_string()),
        wtransport::error::StreamOpeningError::NotConnected => transport_unavailable(error),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_read_status(error: &wtransport::error::StreamReadError) -> Status {
    match error {
        wtransport::error::StreamReadError::Reset(_) => Status::cancelled(error.to_string()),
        wtransport::error::StreamReadError::NotConnected
        | wtransport::error::StreamReadError::QuicProto => transport_unavailable(error),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_read_exact_status(
    error: &wtransport::error::StreamReadExactError,
) -> Status {
    match error {
        wtransport::error::StreamReadExactError::FinishedEarly(_) => transport_unavailable(error),
        wtransport::error::StreamReadExactError::Read(error) => {
            webtransport_stream_read_status(error)
        }
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_write_status(error: &wtransport::error::StreamWriteError) -> Status {
    match error {
        wtransport::error::StreamWriteError::Closed
        | wtransport::error::StreamWriteError::Stopped(_) => Status::cancelled(error.to_string()),
        wtransport::error::StreamWriteError::NotConnected
        | wtransport::error::StreamWriteError::QuicProto => transport_unavailable(error),
    }
}

fn transport_unavailable(error: &dyn fmt::Display) -> Status {
    Status::unavailable(format!("transport unavailable: {error}"))
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

    use crate::{Code, Error};

    #[test]
    fn frame_too_large_maps_to_resource_exhausted() {
        let status = Error::FrameTooLarge { len: 11, max: 10 }.into_status();

        assert_eq!(status.code(), Code::ResourceExhausted);
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

    #[cfg(feature = "quinn")]
    #[test]
    fn quinn_local_close_maps_to_cancelled() {
        let status = Error::transport(quinn::ConnectionError::LocallyClosed).into_status();

        assert_eq!(status.code(), Code::Cancelled);
    }

    #[cfg(feature = "quinn")]
    #[test]
    fn quinn_remote_reset_maps_to_cancelled() {
        let status = Error::transport(quinn::ReadError::Reset(1_u32.into())).into_status();

        assert_eq!(status.code(), Code::Cancelled);
    }

    #[cfg(feature = "quinn")]
    #[test]
    fn quinn_connection_timeout_maps_to_unavailable() {
        let status = Error::transport(quinn::ConnectionError::TimedOut).into_status();

        assert_eq!(status.code(), Code::Unavailable);
    }
}
