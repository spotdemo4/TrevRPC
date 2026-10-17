//! Assertions that fail a `Result`-returning test by returning an error instead of panicking.

use std::error::Error;
use std::fmt;

/// A failed [`ensure!`] or [`ensure_eq!`]; its `Debug` output is the bare message so the test
/// harness prints it unescaped.
pub struct AssertionFailed(String);

impl AssertionFailed {
    pub fn new(file: &str, line: u32, message: fmt::Arguments<'_>) -> Self {
        Self(format!("{message}\n  at {file}:{line}"))
    }
}

impl fmt::Debug for AssertionFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for AssertionFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for AssertionFailed {}

/// Returns an [`AssertionFailed`] error from the enclosing function unless the condition holds.
macro_rules! ensure {
    ($cond:expr $(,)?) => {
        ensure!($cond, "assertion failed: {}", stringify!($cond))
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err($crate::support::AssertionFailed::new(
                file!(),
                line!(),
                format_args!($($arg)+),
            )
            .into());
        }
    };
}

/// Returns an [`AssertionFailed`] error from the enclosing function unless both sides are equal.
macro_rules! ensure_eq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left, right) => {
                if *left != *right {
                    return Err($crate::support::AssertionFailed::new(
                        file!(),
                        line!(),
                        format_args!(
                            "assertion `left == right` failed\n  left: {:?}\n right: {:?}",
                            left, right
                        ),
                    )
                    .into());
                }
            }
        }
    };
    ($left:expr, $right:expr, $($arg:tt)+) => {
        match (&$left, &$right) {
            (left, right) => {
                if *left != *right {
                    return Err($crate::support::AssertionFailed::new(
                        file!(),
                        line!(),
                        format_args!(
                            "assertion `left == right` failed: {}\n  left: {:?}\n right: {:?}",
                            format_args!($($arg)+),
                            left,
                            right
                        ),
                    )
                    .into());
                }
            }
        }
    };
}
