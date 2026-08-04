#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

#[cfg(feature = "client")]
extern crate self as trevrpc;

#[cfg(feature = "client")]
mod client_upload;
pub mod error;
#[cfg(any(feature = "quinn", feature = "webtransport"))]
pub(crate) mod framed;
pub mod framing;
#[cfg(any(feature = "quinn", feature = "webtransport"))]
mod request_pump;
pub mod response;
#[cfg(feature = "client")]
mod response_state;
pub mod status;
pub mod stream;
pub mod wire;

pub const ALPN: &[u8] = b"trevrpc/1";
#[cfg(feature = "http3")]
pub const HTTP3_ALPN: &[u8] = b"h3";

#[cfg(feature = "client")]
pub mod client;

#[cfg(any(feature = "quinn", feature = "webtransport"))]
pub mod advanced;

#[cfg(feature = "quinn")]
pub mod quinn;

#[cfg(feature = "http3")]
pub mod http3;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "webtransport")]
pub mod webtransport;

pub use async_trait::async_trait;
pub use error::{Error, Result};
pub use futures_core::Stream;
pub use response::ResponseEnvelope;
pub use status::{Code, Status};
pub use stream::BoxStream;
pub use wire::{Metadata, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind};
