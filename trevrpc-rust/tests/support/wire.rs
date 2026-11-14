//! The wires a test can run over, a macro that defines a test per wire, and the raw streams of
//! either QUIC wire.

use trevrpc::{Error, Result};

use super::{RawRecv, RawSend};

/// Defines `mod $name`, with a test for each wire of the list (`quic`, `qmux` or `webtransport`)
/// named after that wire, which awaits `$test` called with the wire. `$test` is a function, or a
/// closure, of a `Wire` that returns a future of a [`TestResult`](super::TestResult); without one,
/// it is the function named `$name`.
///
/// `Wire` is the calling file's own enum of the wires it can run, with a `Quic`, `Qmux` or
/// `WebTransport` variant for each, so a wire the file cannot run fails to compile.
///
/// The tests run on tokio's current-thread runtime, or with `multi_thread` first, on four worker
/// threads.
macro_rules! over_each_wire {
    (multi_thread, $name:ident, [$($wire:ident),+ $(,)?] $(,)?) => {
        over_each_wire!(multi_thread, $name, [$($wire),+], $name);
    };
    (multi_thread, $name:ident, [$($wire:ident),+ $(,)?], $test:expr $(,)?) => {
        over_each_wire!(
            @module [tokio::test(flavor = "multi_thread", worker_threads = 4)]
            $name, [$($wire),+], $test
        );
    };
    ($name:ident, [$($wire:ident),+ $(,)?] $(,)?) => {
        over_each_wire!($name, [$($wire),+], $name);
    };
    ($name:ident, [$($wire:ident),+ $(,)?], $test:expr $(,)?) => {
        over_each_wire!(@module [tokio::test] $name, [$($wire),+], $test);
    };
    (@module $runtime:tt $name:ident, [$($wire:ident),+], $test:expr) => {
        mod $name {
            use super::*;

            $(over_each_wire!(@test $runtime $wire, $test);)+
        }
    };
    (@test $runtime:tt quic, $test:expr) => {
        #$runtime
        async fn quic() -> $crate::support::TestResult {
            let test = $test;
            test(Wire::Quic).await
        }
    };
    (@test $runtime:tt qmux, $test:expr) => {
        #$runtime
        async fn qmux() -> $crate::support::TestResult {
            let test = $test;
            test(Wire::Qmux).await
        }
    };
    (@test $runtime:tt webtransport, $test:expr) => {
        #$runtime
        async fn webtransport() -> $crate::support::TestResult {
            let test = $test;
            test(Wire::WebTransport).await
        }
    };
}

/// The send side of a raw stream over native QUIC or `WebTransport`.
pub enum WireSend {
    Quic(quinn::SendStream),
    WebTransport(web_transport_quinn::SendStream),
}

impl WireSend {
    /// Ends the stream cleanly.
    pub fn finish(&mut self) -> Result<()> {
        match self {
            Self::Quic(send) => send.finish().map_err(Error::transport),
            Self::WebTransport(send) => send.finish().map_err(Error::transport),
        }
    }

    /// Abandons the stream with the application error `code`.
    pub fn reset(&mut self, code: u32) -> Result<()> {
        match self {
            Self::Quic(send) => send.reset(code.into()).map_err(Error::transport),
            Self::WebTransport(send) => send.reset(code).map_err(Error::transport),
        }
    }
}

impl RawSend for WireSend {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Quic(send) => RawSend::write_all(send, bytes).await,
            Self::WebTransport(send) => RawSend::write_all(send, bytes).await,
        }
    }
}

/// The receive side of a raw stream over native QUIC or `WebTransport`.
pub enum WireRecv {
    Quic(quinn::RecvStream),
    WebTransport(web_transport_quinn::RecvStream),
}

impl RawRecv for WireRecv {
    async fn read_some(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        match self {
            Self::Quic(recv) => recv.read_some(buf).await,
            Self::WebTransport(recv) => recv.read_some(buf).await,
        }
    }
}
