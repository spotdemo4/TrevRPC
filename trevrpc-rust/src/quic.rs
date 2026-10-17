//! Quinn helpers shared by the native client and the server.

use std::time::Duration;

use crate::mux::{MuxConnection, MuxRecvStream, MuxSendStream};
use crate::request_pump::RequestTransportEvent;
use crate::{Error, Result, Status};

/// Bytes of the length prefix that precede every `TrevRPC` frame.
const FRAME_HEADER_LEN: u64 = 4;

/// The stream receive window that fits one maximum-size frame and its length prefix.
pub(crate) fn frame_receive_window(max_frame_size: usize) -> u64 {
    saturating_usize_to_u64(max_frame_size).saturating_add(FRAME_HEADER_LEN)
}

pub(crate) fn saturating_usize_to_u64(value: usize) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

pub(crate) fn varint(value: u64) -> quinn::VarInt {
    quinn::VarInt::from_u64(value).unwrap_or(quinn::VarInt::MAX)
}

/// Returns the ALPN protocol the TLS handshake negotiated for `connection`.
pub(crate) fn negotiated_protocol(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol)
}

impl MuxConnection for quinn::Connection {
    type SendStream = quinn::SendStream;
    type RecvStream = quinn::RecvStream;

    async fn accept_stream(&self) -> Option<(quinn::SendStream, quinn::RecvStream)> {
        self.accept_bi().await.ok()
    }

    async fn open_stream(&self) -> Result<(quinn::SendStream, quinn::RecvStream)> {
        self.open_bi().await.map_err(Error::transport)
    }

    fn close_connection(&self, reason: &'static [u8]) {
        self.close(0_u32.into(), reason);
    }

    async fn connection_lost(&self) -> Error {
        Error::transport(self.closed().await)
    }
}

impl MuxSendStream for quinn::SendStream {
    async fn peer_stopped(&mut self) -> Result<()> {
        self.stopped().await.map(|_| ()).map_err(Error::transport)
    }

    fn reset_stream(&mut self, code: u32) {
        let _ = self.reset(code.into());
    }

    fn finish_stream(&mut self) -> Result<()> {
        self.finish().map_err(Error::transport)
    }
}

/// How often a backpressured request stream with unread buffered data is re-checked for a reset.
const RESET_PROBE_INTERVAL: Duration = Duration::from_millis(250);

impl MuxRecvStream for quinn::RecvStream {
    fn stop_stream(&mut self, code: u32) {
        let _ = self.stop(code.into());
    }

    /// Waits for the peer to reset the stream without consuming any of its data.
    ///
    /// This deliberately avoids `quinn::RecvStream::received_reset`. Quinn 0.11.12 registers the
    /// stream's blocked-reader waker from it and clears that waker only on the next stream event,
    /// a blocked read, or a drop before EOF. When the whole stream has already arrived, nothing
    /// clears it: reading to EOF then leaves a stale waker, which trips the `debug_assert!` in
    /// `RecvStream::drop` and, in release builds, keeps the pump task allocated until the
    /// connection closes. Fixed on quinn's main branch by commit cdd7940 ("cleanup stale wakers
    /// in `blocked_readers`"), which no release includes yet.
    ///
    /// A zero-length chunk read is the one public probe that only registers a waker when the
    /// stream is genuinely blocked, and such a waker is cleared by the data or FIN that unblocks
    /// it. It returns a reset at once, and an empty chunk when unread data is buffered. A reset
    /// discards buffered data, so a buffered stream has not been reset yet, but it cannot wake
    /// this probe when it is. That case is re-checked on a timer instead.
    async fn peer_reset(&mut self) -> Option<RequestTransportEvent> {
        loop {
            match self.read_chunk(0, true).await {
                Ok(Some(_)) => tokio::time::sleep(RESET_PROBE_INTERVAL).await,
                Err(quinn::ReadError::Reset(code)) => {
                    return Some(RequestTransportEvent::PeerReset(Status::cancelled(
                        format!(
                            "request stream reset by peer with code {}",
                            code.into_inner()
                        ),
                    )));
                }
                Err(
                    error @ (quinn::ReadError::ConnectionLost(_)
                    | quinn::ReadError::ZeroRttRejected),
                ) => {
                    return Some(RequestTransportEvent::connection_lost(error));
                }
                // The request ended cleanly, was stopped locally, or cannot be probed: the reads
                // that follow report whatever remains.
                Ok(None)
                | Err(quinn::ReadError::ClosedStream | quinn::ReadError::IllegalOrderedRead) => {
                    return std::future::pending().await;
                }
            }
        }
    }
}
