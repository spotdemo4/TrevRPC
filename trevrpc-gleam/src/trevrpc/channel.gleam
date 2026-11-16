//// A connection to a TrevRPC server, which reconnects on its own: native
//// QUIC, QMux over TLS where UDP is blocked, or whichever connects first.

import gleam/option.{type Option, None}
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
