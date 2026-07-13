pub mod campaign;
pub mod certificate;
pub mod metrics;
pub mod protocol;
pub mod report;
pub mod runner;

pub const SCHEMA_VERSION: u32 = 1;
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
