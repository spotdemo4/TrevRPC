pub mod campaign;
pub mod certificate;
pub mod conformance;
pub mod metrics;
pub mod network;
pub mod process;
pub mod protocol;
pub mod report;
pub mod runner;

pub const SCHEMA_VERSION: u32 = 5;
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
