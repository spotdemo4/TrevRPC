//// A connection to a TrevRPC server, which reconnects on its own: native
//// QUIC, QMux over TLS where UDP is blocked, or whichever connects first.

import gleam/option.{type Option, None}
import trevrpc/metadata.{type Metadata}
import trevrpc/status.{type Status}

/// A channel: the process that keeps the connection.
pub type Channel

pub type Transport {
  /// QUIC, racing QMux over TLS once QUIC has not connected after a delay.
  Auto
  Quic
  /// QMux over TLS.
  Tcp
}

pub type Config {
  Config(
    host: String,
    port: Int,
    /// The server's TCP listener, by default on the QUIC port number.
    tcp_port: Option(Int),
    transport: Transport,
    /// The name the server's certificate must have, by default `host`.
    server_name: Option(String),
    /// A PEM file of the certificates to trust, which may be the server's
    /// own self-signed one; the system's without.
    ca_file: Option(String),
    verify: Bool,
    max_frame_size: Option(Int),
    connect_timeout: Option(Int),
    idle_timeout: Option(Int),
    /// How many QUIC connections the channel keeps open and spreads its
    /// calls over, by default one; QMux over TLS always uses one.
    min_quic_connections: Option(Int),
    /// How many QUIC connections the channel's pool grows to under load, by
    /// default the schedulers online, from one to four.
    max_quic_connections: Option(Int),
    /// Request metadata every call carries, such as a bearer token, sent
    /// once per connection instead of in every call; none when empty. See
    /// `set_connection_metadata`.
    connection_metadata: Metadata,
  )
}

/// A channel to `host` and `port`, with every other option at its default.
pub fn config(host: String, port: Int) -> Config {
  Config(
    host:,
    port:,
    tcp_port: None,
    transport: Auto,
    server_name: None,
    ca_file: None,
    verify: True,
    max_frame_size: None,
    connect_timeout: None,
    idle_timeout: None,
    min_quic_connections: None,
    max_quic_connections: None,
    connection_metadata: metadata.new(),
  )
}

/// Starts a channel, linked to the calling process, and waits up to
/// `timeout` milliseconds for it to connect. The channel closes when that
/// process exits.
@external(erlang, "trevrpc_gleam_ffi", "connect")
pub fn connect(config: Config, timeout: Int) -> Result(Channel, Status)

/// Closes the channel, ending its calls.
@external(erlang, "trevrpc_gleam_ffi", "close")
pub fn close(channel: Channel) -> Nil

/// Replaces the channel's connection metadata: request metadata every call
/// carries, which crosses each connection in its first calls, until the
/// server confirms it stored it, and is referred to by ID after that. The
/// server merges it into each call's metadata, the call's own entry winning
/// for a name both have. Calls that start afterwards carry the new
/// metadata, and calls in flight keep theirs. Empty metadata removes it.
@external(erlang, "trevrpc_gleam_ffi", "set_connection_metadata")
pub fn set_connection_metadata(
  channel: Channel,
  metadata: Metadata,
) -> Result(Nil, Status)
