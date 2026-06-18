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
    if let Some(error) = error.downcast_ref::<web_transport_quinn::SessionError>() {
        return webtransport_session_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<web_transport_quinn::WebTransportError>() {
        return webtransport_error_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<web_transport_quinn::ReadError>() {
        return webtransport_stream_read_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<web_transport_quinn::ReadExactError>() {
        return webtransport_stream_read_exact_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<web_transport_quinn::WriteError>() {
        return webtransport_stream_write_status(error);
    }

    #[cfg(feature = "webtransport")]
    if let Some(error) = error.downcast_ref::<web_transport_quinn::ClosedStream>() {
        return Status::cancelled(error.to_string());
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
fn webtransport_session_status(error: &web_transport_quinn::SessionError) -> Status {
    match error {
        web_transport_quinn::SessionError::ConnectionError(error) => quinn_connection_status(error),
        web_transport_quinn::SessionError::WebTransportError(error) => {
            webtransport_error_status(error)
        }
        web_transport_quinn::SessionError::SendDatagramError(_) => transport_unavailable(error),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_error_status(error: &web_transport_quinn::WebTransportError) -> Status {
    match error {
        web_transport_quinn::WebTransportError::Closed(_, _) => {
            Status::cancelled(error.to_string())
        }
        web_transport_quinn::WebTransportError::UnknownSession => transport_unavailable(error),
        web_transport_quinn::WebTransportError::ReadError(error) => quinn_read_exact_status(error),
        web_transport_quinn::WebTransportError::WriteError(error) => quinn_write_status(error),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_read_status(error: &web_transport_quinn::ReadError) -> Status {
    match error {
        web_transport_quinn::ReadError::SessionError(error) => webtransport_session_status(error),
        web_transport_quinn::ReadError::Reset(_) | web_transport_quinn::ReadError::ClosedStream => {
            Status::cancelled(error.to_string())
        }
        web_transport_quinn::ReadError::InvalidReset(_) => transport_unavailable(error),
        web_transport_quinn::ReadError::IllegalOrderedRead => Status::internal(error.to_string()),
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_read_exact_status(error: &web_transport_quinn::ReadExactError) -> Status {
    match error {
        web_transport_quinn::ReadExactError::FinishedEarly(_) => transport_unavailable(error),
        web_transport_quinn::ReadExactError::ReadError(error) => {
            webtransport_stream_read_status(error)
        }
    }
}

#[cfg(feature = "webtransport")]
fn webtransport_stream_write_status(error: &web_transport_quinn::WriteError) -> Status {
    match error {
        web_transport_quinn::WriteError::SessionError(error) => webtransport_session_status(error),
        web_transport_quinn::WriteError::Stopped(_)
        | web_transport_quinn::WriteError::ClosedStream => Status::cancelled(error.to_string()),
        web_transport_quinn::WriteError::InvalidStopped(_) => transport_unavailable(error),
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
