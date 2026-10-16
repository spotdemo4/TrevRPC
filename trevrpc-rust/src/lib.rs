#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

//! `TrevRPC`: protobuf RPC over QUIC.
//!
//! Clients speak native QUIC through [`client::Channel`]. A [`server::Server`] serves native QUIC
//! (ALPN [`ALPN`]), ordinary HTTP/3 (`POST /trevrpc`), and `WebTransport` (`/trevrpc`) from one
//! Quinn endpoint, dispatching on the negotiated ALPN.

extern crate self as trevrpc;

pub mod client;
pub mod error;
mod framed;
pub mod framing;
mod http3;
mod native;
mod quic;
mod request_pump;
pub mod response;
mod response_state;
mod serve;
pub mod server;
mod server_transport;
pub mod status;
pub mod stream;
pub mod wire;

/// ALPN protocol identifier for native `TrevRPC` over QUIC.
pub const ALPN: &[u8] = b"trevrpc/1";
/// ALPN protocol identifier for HTTP/3 and `WebTransport`.
pub const HTTP3_ALPN: &[u8] = b"h3";

pub use async_trait::async_trait;
pub use error::{Error, Result};
pub use futures_core::Stream;
pub use response::ResponseEnvelope;
pub use status::{Code, Status};
pub use stream::BoxStream;
pub use wire::{Metadata, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind};
