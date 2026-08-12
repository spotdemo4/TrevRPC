//! Low-level transports and operational controls for applications that manage transport details.

use std::io;
use std::net::{SocketAddr, UdpSocket};
#[cfg(feature = "webtransport-client")]
use std::sync::Arc;

#[cfg(feature = "quinn")]
pub use crate::client::channel::{ExponentialBackoff, ReconnectBackoff};
#[cfg(feature = "quinn")]
use crate::client::{Channel, ChannelConfig};
use crate::framing::DEFAULT_MAX_FRAME_SIZE;

/// A raw `TrevRPC` transport over one established Quinn connection.
///
/// This transport does not reconnect. Most applications should use [`Channel`] instead.
#[cfg(feature = "quinn")]
#[derive(Clone)]
pub struct RawQuinnTransport {
    connection: quinn::Connection,
    max_frame_size: usize,
}

#[cfg(feature = "quinn")]
impl RawQuinnTransport {
    /// Creates a raw transport over an established Quinn connection.
    #[must_use]
    pub const fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Sets the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Returns the established Quinn connection.
    #[must_use]
    pub const fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }
}

/// A raw `TrevRPC` transport over one established WebTransport session.
///
/// This transport does not reconnect. Most applications should use [`Channel`] instead.
#[cfg(feature = "webtransport-client")]
#[derive(Clone)]
pub struct RawWebTransport {
    session: Arc<web_transport_quinn::Session>,
    max_frame_size: usize,
}

#[cfg(feature = "webtransport-client")]
impl RawWebTransport {
    /// Creates a raw transport over an established WebTransport session.
    #[must_use]
    pub fn new(session: web_transport_quinn::Session) -> Self {
        Self {
            session: Arc::new(session),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Sets the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Returns the established WebTransport session.
    #[must_use]
    pub fn session(&self) -> &web_transport_quinn::Session {
        &self.session
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }
}

/// Quinn endpoint controls intentionally excluded from the routine [`Channel`] API.
#[cfg(feature = "quinn")]
pub trait ChannelOperations {
    /// Returns the Quinn remote address, or `None` for a non-Quinn channel.
    fn quinn_remote_addr(&self) -> Option<SocketAddr>;

    /// Returns the Quinn endpoint's local address, or `None` for a non-Quinn channel.
    fn quinn_local_addr(&self) -> io::Result<Option<SocketAddr>>;

    /// Switches the UDP socket used by a Quinn channel's shared endpoint.
    ///
    /// Rebinding is endpoint-wide and does not itself change the channel generation.
    fn rebind_quinn(&self, socket: UdpSocket) -> io::Result<()>;
}

#[cfg(feature = "quinn")]
impl ChannelOperations for Channel {
    fn quinn_remote_addr(&self) -> Option<SocketAddr> {
        self.advanced_quinn_remote_addr()
    }

    fn quinn_local_addr(&self) -> io::Result<Option<SocketAddr>> {
        self.advanced_quinn_local_addr()
    }

    fn rebind_quinn(&self, socket: UdpSocket) -> io::Result<()> {
        self.advanced_rebind_quinn(socket)
    }
}

/// Reconnect-policy controls intentionally excluded from the routine [`ChannelConfig`] API.
#[cfg(feature = "quinn")]
pub trait ChannelConfigOperations {
    /// Replaces the fixed bounded reconnect delay strategy.
    #[must_use]
    fn with_reconnect_backoff(self, reconnect_backoff: impl ReconnectBackoff) -> ChannelConfig;
}

#[cfg(feature = "quinn")]
impl ChannelConfigOperations for ChannelConfig {
    fn with_reconnect_backoff(self, reconnect_backoff: impl ReconnectBackoff) -> ChannelConfig {
        self.with_reconnect_backoff_advanced(reconnect_backoff)
    }
}

/// Connects a WebTransport channel with a custom CONNECT request.
///
/// This advanced entry point permits custom paths, headers, origins, and subprotocols. Routine
/// channels should use [`Channel::connect_webtransport`], which always uses `/trevrpc`.
#[cfg(feature = "webtransport-client")]
pub async fn connect_webtransport_channel_with_request(
    client: web_transport_quinn::Client,
    request: web_transport_quinn::proto::ConnectRequest,
    config: ChannelConfig,
) -> crate::Result<Channel> {
    Channel::connect_webtransport_request(client, request, config).await
}
