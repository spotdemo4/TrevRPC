//! Transport-neutral view of a connection that carries one RPC per bidirectional stream.
//!
//! Native QUIC and `QMux` connections both implement these traits, so the native `TrevRPC`
//! connection handler and stream adapters are written once.

use std::future::Future;

use crate::framed::{FrameRead, FrameWrite};
use crate::request_pump::RequestTransportEvent;
use crate::{Error, Result};

/// Application error code for streams a client or server abandons.
pub const CANCELLED_STREAM_CODE: u32 = 1;

/// A multiplexed connection whose peer opens one bidirectional stream per RPC.
pub trait MuxConnection: Clone + Send + Sync + 'static {
    type SendStream: MuxSendStream;
    type RecvStream: MuxRecvStream;

    /// Accepts the next peer-initiated bidirectional stream, or `None` once the connection ends.
    fn accept_stream(
        &self,
    ) -> impl Future<Output = Option<(Self::SendStream, Self::RecvStream)>> + Send;

    /// Opens a bidirectional stream, waiting for the peer's stream limit.
    fn open_stream(
        &self,
    ) -> impl Future<Output = Result<(Self::SendStream, Self::RecvStream)>> + Send;

    /// Closes the connection with application code 0 and `reason`.
    fn close_connection(&self, reason: &'static [u8]);

    /// Resolves with the reason once the connection has ended.
    fn connection_lost(&self) -> impl Future<Output = Error> + Send;
}

/// The sending half of a multiplexed stream.
pub trait MuxSendStream: FrameWrite + Send + 'static {
    /// Resolves `Ok` when the peer stops reading (`STOP_SENDING`), and with an error when the
    /// connection is lost first.
    fn peer_stopped(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Abandons the stream with `RESET_STREAM` and `code`.
    fn reset_stream(&mut self, code: u32);

    /// Ends the stream cleanly (FIN).
    fn finish_stream(&mut self) -> Result<()>;
}

/// The receiving half of a multiplexed stream.
pub trait MuxRecvStream: FrameRead + Send + 'static {
    /// Asks the peer to stop sending with `STOP_SENDING` and `code`.
    fn stop_stream(&mut self, code: u32);

    /// Resolves when the peer resets the stream, or never if it cannot tell.
    ///
    /// It must be cancel-safe and must leave unread data readable; see
    /// [`crate::request_pump::RequestPumpReader::backpressure_event`].
    fn peer_reset(&mut self) -> impl Future<Output = Option<RequestTransportEvent>> + Send;
}
