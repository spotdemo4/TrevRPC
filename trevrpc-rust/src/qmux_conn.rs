//! `QMux` (draft-ietf-quic-qmux-02) sessions from the `qmux` crate as multiplexed connections, so
//! native `TrevRPC` runs over TLS and WebSocket unchanged.

use std::future::pending;
use std::time::Duration;

use bytes::Bytes;
use web_transport_trait::{RecvStream as _, SendStream as _, Session as _};

use crate::framed::{FrameRead, FrameWrite};
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

impl MuxConnection for qmux::Session {
    type SendStream = qmux::SendStream;
    type RecvStream = qmux::RecvStream;

    async fn accept_stream(&self) -> Option<(qmux::SendStream, qmux::RecvStream)> {
        self.accept_bi().await.ok()
    }

    async fn open_stream(&self) -> Result<(qmux::SendStream, qmux::RecvStream)> {
        self.open_bi().await.map_err(Error::transport)
    }

    fn close_connection(&self, reason: &'static [u8]) {
        self.close(0, &String::from_utf8_lossy(reason));
    }

    async fn connection_lost(&self) -> Error {
        Error::transport(self.closed().await)
    }
}

impl MuxSendStream for qmux::SendStream {
    async fn peer_stopped(&mut self) -> Result<()> {
        match self.closed().await {
            Err(qmux::Error::StreamStop(_)) => Ok(()),
            // Finished or reset locally: the peer can no longer stop the stream.
            Ok(()) | Err(qmux::Error::StreamReset(_)) => pending().await,
            Err(error) => Err(Error::transport(error)),
        }
    }

    fn reset_stream(&mut self, code: u32) {
        self.reset(code);
    }

    fn finish_stream(&mut self) -> Result<()> {
        self.finish().map_err(Error::transport)
    }
}

impl FrameWrite for qmux::SendStream {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).await.map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        for chunk in chunks {
            self.write_chunk(std::mem::take(chunk))
                .await
                .map_err(Error::transport)?;
        }
        Ok(())
    }
}

impl MuxRecvStream for qmux::RecvStream {
    fn stop_stream(&mut self, code: u32) {
        self.stop(code);
    }

    /// `RecvStream::closed` buffers data as it waits without consuming it, and is cancel-safe.
    async fn peer_reset(&mut self) -> Option<RequestTransportEvent> {
        match self.closed().await {
            Err(qmux::Error::StreamReset(code)) => Some(RequestTransportEvent::PeerReset(
                Status::cancelled(format!("request stream reset by peer with code {code}")),
            )),
            // The request ended cleanly or was stopped locally: the reads that follow report it.
            Ok(()) | Err(qmux::Error::StreamStop(_)) => pending().await,
            Err(error) => Some(RequestTransportEvent::connection_lost(error)),
        }
    }
}

impl FrameRead for qmux::RecvStream {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.read(bytes).await.map_err(Error::transport)
    }
}
