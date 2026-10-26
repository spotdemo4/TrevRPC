#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

//! `TrevRPC`: protobuf RPC over QUIC.
//!
//! Clients speak native QUIC through [`client::Channel`]. A [`server::Server`] serves native QUIC
//! (ALPN [`ALPN`]), ordinary HTTP/3 (`POST /trevrpc`), and `WebTransport` (`/trevrpc`) from one
//! Quinn endpoint, dispatching on the negotiated ALPN.
//!
//! Streaming methods can mark their messages [`Unreliable`], so that they travel in QUIC
//! datagrams where the connection has them.

extern crate self as trevrpc;

pub mod client;
pub mod compression;
mod congestion;
mod datagram;
pub mod error;
pub mod fallback;
mod framed;
pub mod framing;
mod http1;
mod http2;
mod http3;
mod http_admission;
mod latest;
mod mux;
mod native;
mod qmux_conn;
mod queue;
mod quic;
mod request_pump;
pub mod response;
mod response_state;
mod rpc_body;
mod serve;
pub mod server;
mod server_transport;
mod shards;
pub mod status;
pub mod stream;
mod structured_fields;
mod sync;
pub mod tcp;
#[cfg(test)]
mod test_io;
mod transport_error;
pub mod varint;
mod wake_gate;
mod websocket;
pub mod wire;

/// ALPN protocol identifier for native `TrevRPC` over QUIC.
pub const ALPN: &[u8] = b"trevrpc/1";
/// ALPN protocol identifier for HTTP/3 and `WebTransport`.
pub const HTTP3_ALPN: &[u8] = b"h3";
/// Identifier of `TrevRPC` over `QMux` (draft-ietf-quic-qmux-02): the TLS ALPN protocol of `QMux`
/// over TLS and the WebSocket subprotocol of `QMux` over WebSocket.
pub const QMUX_ALPN: &[u8] = b"qmux-02.trevrpc-1";

pub use async_trait::async_trait;
pub use compression::{CompressMessages, CompressStream, Compression, Dictionary};
pub use datagram::Unreliable;
pub use error::{Error, Result};
pub use framing::ResponseFrame;
pub use futures_core::Stream;
pub use response::ResponseEnvelope;
pub use status::{Code, Status};
pub use stream::BoxStream;
pub use wire::{Metadata, RpcHeaders, RpcKind, RpcRequest, RpcStatus};
