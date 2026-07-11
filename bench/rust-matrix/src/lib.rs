pub mod config;
pub mod control;
pub mod events;
pub mod fixtures;
pub mod metrics;

pub mod proto {
    tonic::include_proto!("trevrpc.benchmark.v1");
}

pub const SCHEMA_VERSION: u32 = 1;
pub const SERVICE_NAME: &str = "trevrpc.benchmark.v1.BenchmarkService";
pub const METHOD_NAME: &str = "TinyUnary";
pub const TINY_VALUE: &str = "TrevRPC benchmark";

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
