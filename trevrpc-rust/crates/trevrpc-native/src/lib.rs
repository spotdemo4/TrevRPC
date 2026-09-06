#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

#[cfg(all(
    feature = "native-c",
    not(all(
        any(target_os = "linux", target_os = "macos"),
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))
))]
compile_error!(
    "trevrpc-native native-c currently supports only x86_64/aarch64 Linux and macOS targets"
);

#[cfg(feature = "native-c")]
mod config;
#[cfg(feature = "native-c")]
mod error;
#[cfg(feature = "native-c")]
mod ffi;
#[cfg(feature = "testing")]
mod testing;
#[cfg(feature = "native-c")]
mod transport;

#[cfg(feature = "native-c")]
pub use config::{EndpointConfig, MsQuicConfig, Protocol, TransportConfig};
#[cfg(feature = "native-c")]
pub use error::{CloseReason, ErrorOrigin, NativeError, Result};
#[cfg(feature = "native-c")]
pub use transport::{
    Admission, AdmissionRequest, BiStream, Connection, Diagnostics, Listener, ListenerEvent,
    NativeTransport, OwnedReceive, RecvHalf, SendHalf, TransportState,
};
