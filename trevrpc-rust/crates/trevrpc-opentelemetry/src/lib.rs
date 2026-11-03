//! `OpenTelemetry` instrumentation for `TrevRPC` servers and clients, following the semantic
//! conventions for RPC with `rpc.system.name` `trevrpc`:
//!
//! - [`Metrics`] records the duration of every call a server handles, and the sizes of its request
//!   and response bodies, with the `OpenTelemetry` metrics API;
//! - [`Interceptor`] traces each call a server's handlers serve in a [`tracing`] span, which
//!   [`tracing-opentelemetry`](tracing_opentelemetry) exports as a server span;
//! - [`propagate`] sends the trace context of the current span with a client call.
//!
//! It is a separate crate so that `TrevRPC` itself does not depend on `OpenTelemetry`.

mod metrics;
mod propagation;
mod server;
mod status;

pub use metrics::Metrics;
pub use propagation::{propagate, propagate_with};
pub use server::Interceptor;

/// The value of the `rpc.system.name` attribute of `TrevRPC` calls.
pub const SYSTEM_NAME: &str = "trevrpc";

/// The schema of the semantic conventions the attributes follow.
const SCHEMA_URL: &str = "https://opentelemetry.io/schemas/1.43.0";
