//! Bounds on waits that should finish promptly, failing with where the test was waiting.

use std::error::Error;
use std::fmt;
use std::time::Duration;

/// A [`within!`] wait that ran out of time. Its message names the awaited expression and where
/// it was awaited, which a bare `tokio::time::error::Elapsed` cannot.
pub struct TimedOut(String);

impl TimedOut {
    pub fn new(file: &str, line: u32, awaited: &str, timeout: Duration) -> Self {
        Self(format!(
            "timed out after {timeout:?} waiting for `{awaited}`\n  at {file}:{line}"
        ))
    }
}

impl fmt::Debug for TimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for TimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for TimedOut {}

/// Awaits a future for at most a timeout, evaluating to `Ok` with its output or to a
/// [`TimedOut`] error that names the future and the line that awaited it.
///
/// For waits that only guard against a hang. A test that expects a future *not* to finish
/// within a window uses `tokio::time::timeout` directly.
macro_rules! within {
    ($timeout:expr, $future:expr $(,)?) => {{
        let timeout: ::std::time::Duration = $timeout;
        match ::tokio::time::timeout(timeout, $future).await {
            Ok(output) => Ok(output),
            Err(_) => Err($crate::support::TimedOut::new(
                file!(),
                line!(),
                stringify!($future),
                timeout,
            )),
        }
    }};
}
