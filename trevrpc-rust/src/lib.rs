#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod error;
#[cfg(any(feature = "quinn", feature = "webtransport"))]
pub(crate) mod framed;
pub mod framing;
pub mod response;
pub mod status;
pub mod stream;
pub mod wire;

pub const ALPN: &[u8] = b"trevrpc/1";
#[cfg(feature = "http3")]
pub const HTTP3_ALPN: &[u8] = b"h3";

#[cfg(feature = "client")]
pub mod client;

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
pub use response::ResponseEnvelope;
pub use status::{Code, Status};
pub use stream::{BoxMessageStream, MessageStream};
pub use wire::{Metadata, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind};
