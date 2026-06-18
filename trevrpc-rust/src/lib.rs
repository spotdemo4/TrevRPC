#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod error;
pub mod framing;
pub mod status;
pub mod stream;
pub mod wire;

pub const ALPN: &[u8] = b"trevrpc/1";

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "quinn")]
pub mod quinn;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "webtransport")]
pub mod webtransport;

pub use async_trait::async_trait;
pub use error::{Error, Result};
pub use status::{Code, Status};
pub use stream::{BoxMessageStream, MessageStream};
pub use wire::{Metadata, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind};
