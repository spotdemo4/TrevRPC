use opentelemetry::metrics::{Histogram, Meter};
use opentelemetry::{InstrumentationScope, KeyValue, global};
use trevrpc::server::RpcFinished;

use crate::status::{server_error, status_code};
use crate::{SCHEMA_URL, SYSTEM_NAME};

/// The `rpc.method` of a call to a method the server does not register, whose name the client
/// chose.
const OTHER_METHOD: &str = "_OTHER";

/// The bucket boundaries of `rpc.server.call.duration`, in seconds, from the semantic conventions.
const DURATION_BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// The bucket boundaries of the body size histograms, in bytes.
const SIZE_BUCKETS: [f64; 11] = [
    0.0,
    64.0,
    256.0,
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
];

/// A [`trevrpc::server::Metrics`] that records, for every call a server handles, including the
/// calls it rejects before a handler runs:
///
/// - `rpc.server.call.duration`, the duration of the call in seconds;
/// - `rpc.server.request.size` and `rpc.server.response.size`, the sizes in bytes of the request
///   and response bodies [`RpcFinished`] reports, under the names earlier versions of the semantic
///   conventions gave them.
///
/// Each measurement has the attributes `rpc.system.name` (`trevrpc`), `rpc.method`
/// (`package.Service/Method`, or `_OTHER` for a method the server does not register, so clients
/// cannot add attribute values) and `rpc.response.status_code` (such as `NOT_FOUND`), and
/// `error.type`, the status code, when the code means the server failed (see [`Interceptor`]).
///
/// Install it with [`Server::set_metrics`](trevrpc::server::Server::set_metrics).
///
/// [`Interceptor`]: crate::Interceptor
#[derive(Clone, Debug)]
pub struct Metrics {
    duration: Histogram<f64>,
    request_size: Histogram<u64>,
    response_size: Histogram<u64>,
}

impl Metrics {
    /// Records with `meter`.
    #[must_use]
    pub fn new(meter: &Meter) -> Self {
        Self {
            duration: meter
                .f64_histogram("rpc.server.call.duration")
                .with_description(
                    "Measures the duration of an incoming Remote Procedure Call (RPC).",
                )
                .with_unit("s")
                .with_boundaries(DURATION_BUCKETS.to_vec())
                .build(),
            request_size: meter
                .u64_histogram("rpc.server.request.size")
                .with_description("Measures the size of RPC request bodies.")
                .with_unit("By")
                .with_boundaries(SIZE_BUCKETS.to_vec())
                .build(),
            response_size: meter
                .u64_histogram("rpc.server.response.size")
                .with_description("Measures the size of RPC response bodies.")
                .with_unit("By")
                .with_boundaries(SIZE_BUCKETS.to_vec())
                .build(),
        }
    }

    /// Records with a meter of the global meter provider.
    #[must_use]
    pub fn global() -> Self {
        Self::new(&global::meter_with_scope(scope()))
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::global()
    }
}

impl trevrpc::server::Metrics for Metrics {
    fn rpc_finished(&self, event: &RpcFinished) {
        let attributes = attributes(event);
        self.duration
            .record(event.elapsed.as_secs_f64(), &attributes);
        self.request_size
            .record(byte_count(event.request_body_len), &attributes);
        self.response_size
            .record(byte_count(event.response_body_len), &attributes);
    }
}

/// The instrumentation scope of this crate.
pub(crate) fn scope() -> InstrumentationScope {
    InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
        .with_version(env!("CARGO_PKG_VERSION"))
        .with_schema_url(SCHEMA_URL)
        .build()
}

/// The attributes of the measurements of a call.
fn attributes(event: &RpcFinished) -> Vec<KeyValue> {
    let method = if event.registered {
        format!("{}/{}", event.service, event.method)
    } else {
        OTHER_METHOD.to_owned()
    };
    let code = status_code(event.code);
    let mut attributes = Vec::with_capacity(4);
    attributes.extend([
        KeyValue::new("rpc.system.name", SYSTEM_NAME),
        KeyValue::new("rpc.method", method),
        KeyValue::new("rpc.response.status_code", code),
    ]);
    if server_error(event.code) {
        attributes.push(KeyValue::new("error.type", code));
    }
    attributes
}

fn byte_count(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}
