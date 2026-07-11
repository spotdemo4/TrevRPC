use serde::{Deserialize, Serialize};

use crate::config::Stack;
use crate::metrics::ProcessDelta;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadyEvent {
    pub schema_version: u32,
    pub event: String,
    pub run_id: String,
    pub config_hash: String,
    pub stack: Stack,
    pub address: String,
    pub control_path: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ControlRequest {
    Begin {
        run_id: String,
        config_hash: String,
    },
    End {
        run_id: String,
        completed: u64,
        failed: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ControlResponse {
    BeginAck {
        run_id: String,
    },
    EndAck {
        run_id: String,
        completed: u64,
        failed: u64,
        server: ProcessDelta,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HistogramBucket {
    pub highest_equivalent_ns: u64,
    pub count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SampleResult {
    pub schema_version: u32,
    pub event: String,
    pub run_id: String,
    pub config_hash: String,
    pub source_commit: String,
    pub artifact_sha256: String,
    pub stack: Stack,
    pub application_encoding: String,
    pub workload: String,
    pub operation: String,
    pub repetition: u32,
    pub concurrency: usize,
    pub connections: u32,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub elapsed_ns: u64,
    pub drain_ns: u64,
    pub completed: u64,
    pub failed: u64,
    pub throughput_per_s: f64,
    pub latency_p50_ns: u64,
    pub latency_p90_ns: u64,
    pub latency_p95_ns: u64,
    pub latency_p99_ns: u64,
    pub latency_p999_ns: u64,
    pub latency_max_ns: u64,
    pub application_request_bytes: usize,
    pub application_response_bytes: usize,
    pub transport_security_mode: String,
    pub certificate_verification_mode: String,
    pub batching_policy: String,
    pub network_profile: String,
    pub client: ProcessDelta,
    pub server: ProcessDelta,
    pub histogram: Vec<HistogramBucket>,
}
