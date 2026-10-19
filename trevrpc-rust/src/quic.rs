//! Quinn helpers shared by the native client and the server.

use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt as _;

use crate::datagram::{DatagramTransport, Unsent};
use crate::framed::Chunked;
use crate::mux::{MuxConnection, MuxRecvStream, MuxSendStream};
use crate::request_pump::RequestTransportEvent;
use crate::{Error, Result, Status};

/// The longest `TrevRPC` frame header: a type and a length, each a QUIC variable-length integer
/// of at most eight bytes.
const FRAME_HEADER_LEN: u64 = 16;

/// The stream receive window that fits one maximum-size frame and its header.
pub fn frame_receive_window(max_frame_size: usize) -> u64 {
    saturating_usize_to_u64(max_frame_size).saturating_add(FRAME_HEADER_LEN)
}

pub fn saturating_usize_to_u64(value: usize) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

pub fn varint(value: u64) -> quinn::VarInt {
    quinn::VarInt::from_u64(value).unwrap_or(quinn::VarInt::MAX)
}

/// Returns the ALPN protocol the TLS handshake negotiated for `connection`.
pub fn negotiated_protocol(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol)
}

impl MuxConnection for quinn::Connection {
    type SendStream = quinn::SendStream;
    type RecvStream = Chunked<quinn::RecvStream>;
    type Datagrams = Self;

    async fn accept_stream(&self) -> Option<(quinn::SendStream, Chunked<quinn::RecvStream>)> {
        let (send, recv) = self.accept_bi().await.ok()?;
        Some((send, Chunked::new(recv)))
    }

    async fn open_stream(&self) -> Result<(quinn::SendStream, Chunked<quinn::RecvStream>)> {
        let (send, recv) = self.open_bi().await.map_err(Error::transport)?;
        Ok((send, Chunked::new(recv)))
    }

    fn close_connection(&self, reason: &'static [u8]) {
        self.close(0_u32.into(), reason);
    }

    async fn connection_lost(&self) -> Error {
        Error::transport(self.closed().await)
    }

    fn datagrams(&self) -> Option<Self> {
        Some(self.clone())
    }
}

/// Native QUIC datagrams: DATAGRAM frames whose payload is the whole datagram.
impl DatagramTransport for quinn::Connection {
    fn max_size(&self) -> Option<usize> {
        self.max_datagram_size()
    }

    async fn send(&self, datagram: Bytes) -> std::result::Result<(), Unsent> {
        self.send_datagram_wait(datagram).await.map_err(|_| Unsent)
    }

    async fn recv(&self) -> Option<Bytes> {
        self.read_datagram().await.ok()
    }

    fn try_recv(&self) -> Option<Bytes> {
        self.read_datagram().now_or_never()?.ok()
    }

    fn transmitted(&self) -> Option<u64> {
        Some(self.stats().frame_tx.datagram)
    }
}

/// The datagrams of one `WebTransport` session, framed by this crate on the raw QUIC connection.
///
/// A `WebTransport` datagram is a QUIC DATAGRAM frame that starts with the session's quarter
/// stream ID (RFC 9297). This crate adds and checks that prefix itself instead of using the h3
/// crates' datagram support, so that one [`crate::datagram::Demux`] serves native QUIC and
/// `WebTransport` alike and no h3 buffer sits between Quinn and a call.
///
/// Quinn's `read_datagram` has a single consumer, so nothing else may read this connection's
/// datagrams: the HTTP/3 and `WebTransport` code must never call the session's
/// `datagram_reader()` or `datagram_sender()`.
pub struct SessionDatagrams {
    connection: quinn::Connection,
    /// The session's quarter stream ID as a variable-length integer.
    prefix: Bytes,
}

impl SessionDatagrams {
    /// The datagrams of the session whose CONNECT request is stream `session_id`.
    pub fn new(connection: quinn::Connection, session_id: u64) -> Option<Self> {
        Some(Self {
            connection,
            prefix: quarter_stream_id(session_id)?,
        })
    }
}

/// The quarter stream ID that starts the datagrams of the session whose CONNECT request is stream
/// `session_id`: the stream ID divided by four, as a variable-length integer (RFC 9297,
/// section 2.1).
fn quarter_stream_id(session_id: u64) -> Option<Bytes> {
    let mut prefix = bytes::BytesMut::with_capacity(crate::varint::MAX_LEN);
    crate::varint::push(&mut prefix, session_id / 4).ok()?;
    Some(prefix.freeze())
}

impl DatagramTransport for SessionDatagrams {
    fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    fn max_size(&self) -> Option<usize> {
        self.connection.max_datagram_size()
    }

    async fn send(&self, datagram: Bytes) -> std::result::Result<(), Unsent> {
        self.connection
            .send_datagram_wait(datagram)
            .await
            .map_err(|_| Unsent)
    }

    async fn recv(&self) -> Option<Bytes> {
        self.connection.read_datagram().await.ok()
    }

    fn try_recv(&self) -> Option<Bytes> {
        self.connection.read_datagram().now_or_never()?.ok()
    }

    fn transmitted(&self) -> Option<u64> {
        Some(self.connection.stats().frame_tx.datagram)
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

impl MuxRecvStream for Chunked<quinn::RecvStream> {
    fn stop_stream(&mut self, code: u32) {
        let _ = self.source.stop(code.into());
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
            match self.source.read_chunk(0, true).await {
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

#[cfg(test)]
mod tests {
    use super::quarter_stream_id;

    #[test]
    fn a_session_prefix_is_its_stream_id_divided_by_four() {
        assert_eq!(quarter_stream_id(0).as_deref(), Some(&[0x00][..]));
        // The second request stream of a connection, and the first one that is not stream 0.
        assert_eq!(quarter_stream_id(4).as_deref(), Some(&[0x01][..]));
        assert_eq!(quarter_stream_id(8).as_deref(), Some(&[0x02][..]));
        // Past six bits the integer takes two bytes.
        assert_eq!(
            quarter_stream_id(4 * 64).as_deref(),
            Some(&[0x40, 0x40][..])
        );
        // Stream IDs are below 2^62, so their quarters always fit.
        assert!(quarter_stream_id((1 << 62) - 4).is_some());
    }
}
