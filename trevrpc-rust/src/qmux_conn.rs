//! `QMux` (draft-ietf-quic-qmux-02) sessions from the `qmux` crate as multiplexed connections, so
//! native `TrevRPC` runs over TLS and WebSocket unchanged.

use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use web_transport_trait::{RecvStream as _, SendStream as _, Session as _};

use crate::framed::{ChunkSource, Chunked, FrameWrite};
use crate::mux::{MuxConnection, MuxRecvStream, MuxSendStream};
use crate::quic::saturating_usize_to_u64;
use crate::request_pump::RequestTransportEvent;
use crate::serve::transport_limits;
use crate::server::ServerOptions;
use crate::{Error, Result, Status};

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
    // One spare stream lets an over-limit RPC still receive a status.
    config.max_streams_bidi = options
        .max_concurrent_streams_per_connection()
        .map_or(u64::MAX >> 4, |streams| {
            saturating_usize_to_u64(streams).saturating_add(1)
        });
    config.max_idle_timeout = options.stream_idle_timeout().map_or(0, |timeout| {
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
/// `qmux` ends a stream's reads and writes when its session dies only as a side effect of how it
/// tears the session down, and an operation can still wait for the stream's peer forever while a
/// session handle is alive. Every wait on the session or one of its streams therefore also waits
/// for the session to close, and fails with its close reason, so a dead session fails every call
/// on it promptly. Operations that can finish at once never look at the session: the close is
/// only polled once the operation itself has to wait.
///
/// Streams hold the connection, as QUIC streams do, so it stays open while any of them is in use.
#[derive(Clone)]
pub struct QmuxConnection(Arc<qmux::Session>);

impl From<qmux::Session> for QmuxConnection {
    fn from(session: qmux::Session) -> Self {
        Self(Arc::new(session))
    }
}

impl QmuxConnection {
    /// Resolves with the close reason once the session has ended.
    async fn lost(&self) -> qmux::Error {
        self.0.closed().await
    }

    /// Runs `operation` on a stream of this session, failing if the session ends first.
    async fn until_lost<T>(
        &self,
        operation: impl Future<Output = std::result::Result<T, qmux::Error>>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            result = operation => result.map_err(Error::transport),
            error = self.lost() => Err(Error::transport(error)),
        }
    }
}

impl MuxConnection for QmuxConnection {
    type SendStream = QmuxSendStream;
    type RecvStream = QmuxRecvStream;

    async fn accept_stream(&self) -> Option<(QmuxSendStream, QmuxRecvStream)> {
        let (send, recv) = self.until_lost(self.0.accept_bi()).await.ok()?;
        Some((
            QmuxSendStream {
                stream: send,
                connection: self.clone(),
            },
            Chunked::new(QmuxRecvSource {
                stream: recv,
                connection: self.clone(),
            }),
        ))
    }

    async fn open_stream(&self) -> Result<(QmuxSendStream, QmuxRecvStream)> {
        let (send, recv) = self.until_lost(self.0.open_bi()).await?;
        Ok((
            QmuxSendStream {
                stream: send,
                connection: self.clone(),
            },
            Chunked::new(QmuxRecvSource {
                stream: recv,
                connection: self.clone(),
            }),
        ))
    }

    fn close_connection(&self, reason: &'static [u8]) {
        self.0.close(0, &String::from_utf8_lossy(reason));
    }

    async fn connection_lost(&self) -> Error {
        Error::transport(self.lost().await)
    }
}

/// The sending half of a stream of a [`QmuxConnection`].
pub struct QmuxSendStream {
    stream: qmux::SendStream,
    connection: QmuxConnection,
}

impl MuxSendStream for QmuxSendStream {
    async fn peer_stopped(&mut self) -> Result<()> {
        let stopped = async {
            match self.stream.closed().await {
                Err(qmux::Error::StreamStop(_)) => Ok(()),
                // Finished or reset locally: the peer can no longer stop the stream.
                Ok(()) | Err(qmux::Error::StreamReset(_)) => pending().await,
                Err(error) => Err(Error::transport(error)),
            }
        };
        tokio::select! {
            biased;
            stopped = stopped => stopped,
            error = self.connection.lost() => Err(Error::transport(error)),
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
        self.connection
            .until_lost(self.stream.write_chunk(chunk))
            .await
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
    connection: QmuxConnection,
}

impl MuxRecvStream for QmuxRecvStream {
    fn stop_stream(&mut self, code: u32) {
        self.source.stream.stop(code);
    }

    /// `RecvStream::closed` buffers data as it waits without consuming it, and is cancel-safe.
    async fn peer_reset(&mut self) -> Option<RequestTransportEvent> {
        match self.source.stream.closed().await {
            Err(qmux::Error::StreamReset(code)) => Some(RequestTransportEvent::PeerReset(
                Status::cancelled(format!("request stream reset by peer with code {code}")),
            )),
            // The request ended cleanly or was stopped locally: the reads that follow report it.
            Ok(()) | Err(qmux::Error::StreamStop(_)) => pending().await,
            Err(error) => Some(RequestTransportEvent::connection_lost(error)),
        }
    }
}

impl ChunkSource for QmuxRecvSource {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        self.connection
            .until_lost(self.stream.read_chunk(usize::MAX))
            .await
    }

    async fn read_into(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.connection.until_lost(self.stream.read(bytes)).await
    }
}
