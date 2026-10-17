//! Classification of transport errors from every stack `TrevRPC` runs on.
//!
//! Each transport error type is mapped once, here, to both the status reported to callers and
//! the way it ended a request stream.

use std::error::Error as StdError;
use std::fmt;
use std::io;

use crate::Status;

/// How a transport failure ended a request stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFailureKind {
    PeerReset,
    ConnectionLost,
    Protocol,
}

/// The status and failure kind of a transport error.
pub struct TransportFailure {
    pub(crate) status: Status,
    /// `None` when the error does not say how the stream ended.
    pub(crate) kind: Option<TransportFailureKind>,
}

impl TransportFailure {
    const fn new(status: Status, kind: Option<TransportFailureKind>) -> Self {
        Self { status, kind }
    }
}

/// Classifies a transport error.
pub fn classify(error: &(dyn StdError + Send + Sync + 'static)) -> TransportFailure {
    if let Some(error) = error.downcast_ref::<quinn::ConnectionError>() {
        return TransportFailure::new(quinn_connection_status(error), None);
    }
    if let Some(error) = error.downcast_ref::<quinn::ReadError>() {
        return quinn_read(error);
    }
    if let Some(error) = error.downcast_ref::<quinn::ReadExactError>() {
        let status = match error {
            quinn::ReadExactError::FinishedEarly(_) => transport_unavailable(error),
            quinn::ReadExactError::ReadError(error) => quinn_read(error).status,
        };
        return TransportFailure::new(status, None);
    }
    if let Some(error) = error.downcast_ref::<quinn::WriteError>() {
        return TransportFailure::new(quinn_write_status(error), None);
    }
    if let Some(error) = error.downcast_ref::<h3::error::StreamError>() {
        return h3_stream(error);
    }
    if let Some(error) = error.downcast_ref::<h3::quic::StreamErrorIncoming>() {
        return h3_quic_stream(error);
    }
    if let Some(error) = error.downcast_ref::<qmux::Error>() {
        return qmux_error(error);
    }
    if let Some(error) = error.downcast_ref::<h2::Error>() {
        return h2_error(error);
    }
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return TransportFailure::new(io_status(error), Some(TransportFailureKind::ConnectionLost));
    }

    TransportFailure::new(transport_unavailable(error), None)
}

fn quinn_connection_status(error: &quinn::ConnectionError) -> Status {
    match error {
        quinn::ConnectionError::LocallyClosed => Status::cancelled("transport closed locally"),
        _ => transport_unavailable(error),
    }
}

fn quinn_read(error: &quinn::ReadError) -> TransportFailure {
    match error {
        quinn::ReadError::Reset(_) => TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::PeerReset),
        ),
        quinn::ReadError::ZeroRttRejected => TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::ConnectionLost),
        ),
        quinn::ReadError::ConnectionLost(connection_error) => TransportFailure::new(
            quinn_connection_status(connection_error),
            Some(TransportFailureKind::ConnectionLost),
        ),
        quinn::ReadError::ClosedStream => TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::Protocol),
        ),
        quinn::ReadError::IllegalOrderedRead => TransportFailure::new(
            Status::internal(error.to_string()),
            Some(TransportFailureKind::Protocol),
        ),
    }
}

fn quinn_write_status(error: &quinn::WriteError) -> Status {
    match error {
        quinn::WriteError::Stopped(_)
        | quinn::WriteError::ClosedStream
        | quinn::WriteError::ZeroRttRejected => Status::cancelled(error.to_string()),
        quinn::WriteError::ConnectionLost(error) => quinn_connection_status(error),
    }
}

fn h3_stream(error: &h3::error::StreamError) -> TransportFailure {
    match error {
        h3::error::StreamError::RemoteTerminate { .. } => TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::PeerReset),
        ),
        h3::error::StreamError::StreamError { .. }
        | h3::error::StreamError::HeaderTooBig { .. } => TransportFailure::new(
            Status::internal(error.to_string()),
            Some(TransportFailureKind::Protocol),
        ),
        _ => TransportFailure::new(
            transport_unavailable(error),
            Some(TransportFailureKind::ConnectionLost),
        ),
    }
}

fn h3_quic_stream(error: &h3::quic::StreamErrorIncoming) -> TransportFailure {
    match error {
        h3::quic::StreamErrorIncoming::StreamTerminated { .. } => TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::PeerReset),
        ),
        h3::quic::StreamErrorIncoming::ConnectionErrorIncoming { .. }
        | h3::quic::StreamErrorIncoming::Unknown(_) => TransportFailure::new(
            transport_unavailable(error),
            Some(TransportFailureKind::ConnectionLost),
        ),
    }
}

fn qmux_error(error: &qmux::Error) -> TransportFailure {
    match error {
        qmux::Error::StreamReset(_) | qmux::Error::StreamStop(_) => TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::PeerReset),
        ),
        qmux::Error::StreamClosed => TransportFailure::new(
            Status::internal(error.to_string()),
            Some(TransportFailureKind::Protocol),
        ),
        _ => TransportFailure::new(
            transport_unavailable(error),
            Some(TransportFailureKind::ConnectionLost),
        ),
    }
}

fn h2_error(error: &h2::Error) -> TransportFailure {
    if error.is_reset() {
        return TransportFailure::new(
            Status::cancelled(error.to_string()),
            Some(TransportFailureKind::PeerReset),
        );
    }
    TransportFailure::new(
        transport_unavailable(error),
        Some(TransportFailureKind::ConnectionLost),
    )
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

fn transport_unavailable(error: &dyn fmt::Display) -> Status {
    Status::unavailable(format!("transport unavailable: {error}"))
}
