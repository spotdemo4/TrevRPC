#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod error;
pub mod framing;
pub mod status;
pub mod wire;

pub const ALPN: &[u8] = b"trevrpc/1";

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "quinn")]
pub mod quinn;

#[cfg(feature = "server")]
pub mod server;

pub use async_trait::async_trait;
pub use error::{Error, Result};
pub use status::{Code, Status};
pub use wire::{Metadata, RpcRequest, RpcResponse};
