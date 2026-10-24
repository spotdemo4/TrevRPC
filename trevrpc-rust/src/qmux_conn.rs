//! `QMux` (draft-ietf-quic-qmux-02) sessions from the `qmux` crate as multiplexed connections, so
//! native `TrevRPC` runs over TLS and WebSocket unchanged.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use web_transport_trait::{RecvStream as _, SendStream as _, Session as _};

use crate::datagram::NoDatagrams;
use crate::framed::{ChunkSource, Chunked, FrameWrite};
use crate::mux::{MuxConnection, MuxRecvStream, MuxSendStream};
use crate::request_pump::RequestTransportEvent;
use crate::serve::{stream_credit_limit, transport_limits};
use crate::server::ServerOptions;
use crate::{Error, Result};

/// The application protocol inside [`crate::QMUX_ALPN`], as the `qmux` crate names it.
const APPLICATION_PROTOCOL: &str = "trevrpc-1";
/// The largest `QMux` record a `TrevRPC` endpoint accepts.
pub const MAX_RECORD_SIZE: u64 = 64 * 1024;

/// `QMux` settings shared by every endpoint. The wire version is always pinned: the crate treats
/// an unknown identifier as its legacy `webtransport` format.
fn base_config() -> qmux::Config {
    let mut config =
        qmux::Config::negotiated(qmux::Version::QMux02, Some(APPLICATION_PROTOCOL.to_owned()));
    config.max_streams_uni = 0;
    config.max_stream_data_uni = 0;
    config.max_record_size = MAX_RECORD_SIZE;
    config.max_datagram_frame_size = 0;
    config.rtt_interval = Duration::ZERO;
    config
}

/// The `QMux` transport parameters of a server, mirroring its QUIC limits.
pub fn server_config(options: &ServerOptions) -> qmux::Config {
    let limits = transport_limits(options);
    let mut config = base_config();
    config.max_data = limits.connection_receive_window;
    config.max_stream_data_bidi_remote = limits.stream_receive_window;
    config.max_stream_data_bidi_local = 0;
    config.max_streams_bidi = stream_credit_limit(options.max_concurrent_streams_per_connection());
    config.max_idle_timeout = options.connection_idle_timeout().map_or(0, |timeout| {
        timeout.as_millis().try_into().unwrap_or(u64::MAX)
    });
    config
}

/// The `QMux` transport parameters of a client: like a QUIC client, it receives at most one frame
/// per stream at a time and accepts no streams from the server.
pub fn client_config(max_frame_size: usize) -> qmux::Config {
    let window = crate::quic::frame_receive_window(max_frame_size);
    let mut config = base_config();
    config.max_data = window;
    config.max_stream_data_bidi_local = window;
    config.max_stream_data_bidi_remote = 0;
    config.max_streams_bidi = 0;
    // The server's idle timeout and keep-alives govern the connection.
    config.max_idle_timeout = 0;
    config
}

/// A `QMux` session as a [`MuxConnection`].
///
/// Streams hold the connection, as QUIC streams do, so it stays open while any of them is in use.
#[derive(Clone)]
pub struct QmuxConnection(Arc<qmux::Session>);

impl From<qmux::Session> for QmuxConnection {
    fn from(session: qmux::Session) -> Self {
        Self(Arc::new(session))
    }
}

impl MuxConnection for QmuxConnection {
    type SendStream = QmuxSendStream;
    type RecvStream = QmuxRecvStream;
    type Datagrams = NoDatagrams;

    async fn accept_stream(&self) -> Option<(QmuxSendStream, QmuxRecvStream)> {
        let (send, recv) = self.0.accept_bi().await.ok()?;
        Some((
            QmuxSendStream {
                stream: send,
                _connection: self.clone(),
            },
            Chunked::new(QmuxRecvSource {
                stream: recv,
                _connection: self.clone(),
            }),
        ))
    }

    async fn open_stream(&self) -> Result<(QmuxSendStream, QmuxRecvStream)> {
        let (send, recv) = self.0.open_bi().await.map_err(Error::transport)?;
        Ok((
            QmuxSendStream {
                stream: send,
                _connection: self.clone(),
            },
            Chunked::new(QmuxRecvSource {
                stream: recv,
                _connection: self.clone(),
            }),
        ))
    }

    fn close_connection(&self, reason: &'static [u8]) {
        self.0.close(0, &String::from_utf8_lossy(reason));
    }

    async fn connection_lost(&self) -> Error {
        Error::transport(self.0.closed().await)
    }

    /// `QMux` is configured with `max_datagram_frame_size = 0`: it has no datagrams.
    fn datagrams(&self) -> Option<NoDatagrams> {
        None
    }
}

/// The sending half of a stream of a [`QmuxConnection`].
pub struct QmuxSendStream {
    stream: qmux::SendStream,
    /// Keeps the session open while the stream is in use.
    _connection: QmuxConnection,
}

impl MuxSendStream for QmuxSendStream {
    async fn peer_stopped(&mut self) -> Result<()> {
        match self.stream.closed().await {
            Err(qmux::Error::StreamStop(_)) => Ok(()),
            // Finished or reset locally: the peer can no longer stop the stream.
            Ok(()) | Err(qmux::Error::StreamReset(_)) => pending().await,
            Err(error) => Err(Error::transport(error)),
        }
    }

    fn reset_stream(&mut self, code: u32) {
        self.stream.reset(code);
    }

    fn finish_stream(&mut self) -> Result<()> {
        self.stream.finish().map_err(Error::transport)
    }
}

/// The size from which a chunk is written on its own, so it is never copied. `qmux` makes every
/// chunk at least one STREAM frame with its own queue slot and flow-control claim, so smaller
/// chunks, such as frame headers and short messages, are joined into one write first.
const JOIN_LIMIT: usize = 16 * 1024;

impl QmuxSendStream {
    async fn write_chunk(&mut self, chunk: Bytes) -> Result<()> {
        self.stream
            .write_chunk(chunk)
            .await
            .map_err(Error::transport)
    }
}

impl FrameWrite for QmuxSendStream {
    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        if let [chunk] = chunks {
            return self.write_chunk(std::mem::take(chunk)).await;
        }
        let mut joined = BytesMut::new();
        for chunk in chunks {
            let chunk = std::mem::take(chunk);
            if chunk.len() >= JOIN_LIMIT {
                if !joined.is_empty() {
                    self.write_chunk(joined.split().freeze()).await?;
                }
                self.write_chunk(chunk).await?;
                continue;
            }
            if joined.len().saturating_add(chunk.len()) > JOIN_LIMIT {
                self.write_chunk(joined.split().freeze()).await?;
            }
            joined.extend_from_slice(&chunk);
        }
        if !joined.is_empty() {
            self.write_chunk(joined.freeze()).await?;
        }
        Ok(())
    }
}

/// The receiving half of a stream of a [`QmuxConnection`], read as frames.
pub type QmuxRecvStream = Chunked<QmuxRecvSource>;

/// The chunks of a stream of a [`QmuxConnection`].
pub struct QmuxRecvSource {
    stream: qmux::RecvStream,
    /// Keeps the session open while the stream is in use.
    _connection: QmuxConnection,
}

impl MuxRecvStream for QmuxRecvStream {
    fn stop_stream(&mut self, code: u32) {
        self.source.stream.stop(code);
    }

    /// `RecvStream::closed` buffers data as it waits without consuming it, and is cancel-safe.
    async fn peer_reset(&mut self) -> Option<RequestTransportEvent> {
        match self.source.stream.closed().await {
            Err(qmux::Error::StreamReset(code)) => Some(RequestTransportEvent::peer_reset(code)),
            // The request ended cleanly or was stopped locally: the reads that follow report it.
            Ok(()) | Err(qmux::Error::StreamStop(_)) => pending().await,
            Err(error) => Some(RequestTransportEvent::connection_lost(error)),
        }
    }
}

impl ChunkSource for QmuxRecvSource {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        self.stream
            .read_chunk(usize::MAX)
            .await
            .map_err(Error::transport)
    }

    async fn read_into(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.stream.read(bytes).await.map_err(Error::transport)
    }
}

#[cfg(test)]
mod tests {
    use crate::server::ServerOptions;

    use super::server_config;

    #[test]
    fn server_streams_match_the_quic_credit() {
        let limited = ServerOptions::new().with_max_concurrent_streams_per_connection(Some(10));
        assert_eq!(server_config(&limited).max_streams_bidi, 20);

        let unlimited = ServerOptions::new().with_max_concurrent_streams_per_connection(None);
        assert_eq!(server_config(&unlimited).max_streams_bidi, 1 << 60);
    }
}
