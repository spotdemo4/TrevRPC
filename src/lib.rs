#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

pub mod error;
pub mod framing;
pub mod status;
pub mod wire;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "quinn")]
pub mod quinn;

#[cfg(feature = "server")]
pub mod server;

pub use async_trait::async_trait;
pub use error::{Error, Result};
pub use status::{Code, Status};
pub use wire::{RpcRequest, RpcResponse};
