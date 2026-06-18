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
    #[must_use]
    pub fn transport(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::Transport(Box::new(error))
    }

    #[must_use]
    pub fn into_status(self) -> Status {
        match self {
            Self::Status(status) => status,
            error => Status::internal(error.to_string()),
        }
    }
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
