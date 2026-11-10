//! Checking the request messages of a call before they are sent.

use std::fmt;
use std::sync::Arc;

use crate::{Error, Result, Status};

/// Checks the encoded request messages of a call before the client sends them.
///
/// A message the check rejects is not sent: the call, or the send of that message, fails locally
/// with the status the check returns, normally `InvalidArgument` with details that say why, and
/// the server never sees it. Generated clients with request validation pass one for each method
/// whose request type carries `buf.validate` rules; see [`Call::request_check`](super::call::Call::request_check).
///
/// The check runs inline on the task that makes the call or sends the message, so it should be
/// cheap. Clones share the check.
#[derive(Clone)]
pub struct RequestCheck {
    check: Arc<CheckFn>,
}

/// The function of a [`RequestCheck`].
type CheckFn = dyn Fn(&[u8]) -> std::result::Result<(), Status> + Send + Sync;

impl RequestCheck {
    /// Creates a check from `check`, which receives the encoded request message and returns the
    /// status the call fails with if it rejects it.
    pub fn new<F>(check: F) -> Self
    where
        F: Fn(&[u8]) -> std::result::Result<(), Status> + Send + Sync + 'static,
    {
        Self {
            check: Arc::new(check),
        }
    }

    /// Checks one encoded request message.
    ///
    /// # Errors
    ///
    /// Returns the error that fails the call, carrying the status the check returned.
    pub fn check(&self, message: &[u8]) -> Result<()> {
        (self.check)(message).map_err(Error::from)
    }
}

impl fmt::Debug for RequestCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestCheck")
    }
}

/// Checks are equal when they are the same check.
impl PartialEq for RequestCheck {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.check, &other.check)
    }
}

impl Eq for RequestCheck {}
