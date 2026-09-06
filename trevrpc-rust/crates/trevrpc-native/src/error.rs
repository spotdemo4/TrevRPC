use std::fmt;

/// Origin of a native transport failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorOrigin {
    Local,
    Peer,
    Transport,
    Unknown,
}

/// Error returned by the safe native transport layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeError {
    code: i32,
    message: String,
    origin: ErrorOrigin,
    application_code: u64,
    provider_code: u64,
    peer_reset: bool,
    terminal: bool,
}

impl NativeError {
    pub(crate) fn status(code: i32) -> Self {
        let code = if code > 0 { -code } else { code };
        let message = if code == 0 {
            "native transport operation failed".to_owned()
        } else {
            std::io::Error::from_raw_os_error(code.unsigned_abs().cast_signed()).to_string()
        };
        Self {
            code,
            message,
            origin: ErrorOrigin::Unknown,
            application_code: 0,
            provider_code: 0,
            peer_reset: false,
            terminal: false,
        }
    }

    pub(crate) fn event(
        code: i32,
        origin: ErrorOrigin,
        application_code: u64,
        provider_code: u64,
        peer_reset: bool,
    ) -> Self {
        let mut error = Self::status(code);
        error.origin = origin;
        error.application_code = application_code;
        error.provider_code = provider_code;
        error.peer_reset = peer_reset;
        error
    }

    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self {
            code: 0,
            message: message.into(),
            origin: ErrorOrigin::Unknown,
            application_code: 0,
            provider_code: 0,
            peer_reset: false,
            terminal: false,
        }
    }

    pub(crate) fn terminal(mut self) -> Self {
        self.terminal = true;
        self
    }

    pub(crate) const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub(crate) fn closed(reason: CloseReason) -> Self {
        match reason {
            CloseReason::Clean => Self::message("native object closed").terminal(),
            CloseReason::Local { code } => Self {
                code: 0,
                message: "native object closed locally".to_owned(),
                origin: ErrorOrigin::Local,
                application_code: code,
                provider_code: 0,
                peer_reset: false,
                terminal: true,
            },
            CloseReason::Peer { code, peer_reset } => Self {
                code: 0,
                message: "native object closed by peer".to_owned(),
                origin: ErrorOrigin::Peer,
                application_code: code,
                provider_code: 0,
                peer_reset,
                terminal: true,
            },
            CloseReason::Transport {
                status,
                application_code,
                provider_code,
                peer_reset,
                ..
            } => Self::event(
                status,
                ErrorOrigin::Transport,
                application_code,
                provider_code,
                peer_reset,
            )
            .terminal(),
            CloseReason::Failed {
                status,
                application_code,
                provider_code,
                peer,
                local,
                peer_reset,
            } => Self::event(
                status,
                if peer {
                    ErrorOrigin::Peer
                } else if local {
                    ErrorOrigin::Local
                } else {
                    ErrorOrigin::Unknown
                },
                application_code,
                provider_code,
                peer_reset,
            )
            .terminal(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> i32 {
        self.code
    }

    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    #[must_use]
    pub const fn application_code(&self) -> u64 {
        self.application_code
    }

    #[must_use]
    pub const fn provider_code(&self) -> u64 {
        self.provider_code
    }

    #[must_use]
    pub const fn is_peer_reset(&self) -> bool {
        self.peer_reset
    }

    /// Returns whether the native transport rejected a frame body as too large.
    #[must_use]
    pub const fn is_message_too_large(&self) -> bool {
        self.code == -libc::EMSGSIZE
    }

    pub(crate) fn is_would_block(&self) -> bool {
        self.code != 0
            && std::io::Error::from_raw_os_error(self.code.unsigned_abs().cast_signed()).kind()
                == std::io::ErrorKind::WouldBlock
    }

    pub(crate) fn is_retryable_release(&self) -> bool {
        self.is_would_block() || self.code == -libc::EBUSY
    }
}

impl fmt::Display for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.code == 0 {
            return f.write_str(&self.message);
        }
        write!(f, "{} ({})", self.message, self.code)?;
        if self.application_code != 0 {
            write!(f, ", application code {}", self.application_code)?;
        }
        if self.provider_code != 0 {
            write!(f, ", provider code {}", self.provider_code)?;
        }
        Ok(())
    }
}

impl std::error::Error for NativeError {}

pub type Result<T> = std::result::Result<T, NativeError>;

/// Final reason reported for a native semantic object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    Clean,
    Local {
        code: u64,
    },
    Peer {
        code: u64,
        peer_reset: bool,
    },
    Transport {
        status: i32,
        application_code: u64,
        provider_code: u64,
        peer: bool,
        local: bool,
        peer_reset: bool,
    },
    Failed {
        status: i32,
        application_code: u64,
        provider_code: u64,
        peer: bool,
        local: bool,
        peer_reset: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::NativeError;

    #[test]
    fn message_too_large_classification_is_exact() {
        assert!(NativeError::status(-libc::EMSGSIZE).is_message_too_large());
        assert!(!NativeError::status(-libc::EINVAL).is_message_too_large());
    }
}
