use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::BoxError;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stack {
    TrevrpcQuinn,
    GrpcTonicGenerated,
}

impl Stack {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TrevrpcQuinn => "trevrpc_quinn",
            Self::GrpcTonicGenerated => "grpc_tonic_generated",
        }
    }
}

impl std::str::FromStr for Stack {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "trevrpc_quinn" => Ok(Self::TrevrpcQuinn),
            "grpc_tonic_generated" => Ok(Self::GrpcTonicGenerated),
            _ => Err(format!("unsupported stack {value:?}")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub stack: Stack,
    pub listen: String,
    pub control: PathBuf,
    pub ready_file: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub run_id: String,
    pub config_hash: String,
    pub identity: BenchmarkIdentity,
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub stack: Stack,
    pub address: String,
    pub control: PathBuf,
    pub certificate: PathBuf,
    pub run_id: String,
    pub config_hash: String,
    pub concurrency: usize,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub repetition: u32,
    pub identity: BenchmarkIdentity,
    pub identity_certificate: PathBuf,
}

#[derive(Clone, Debug)]
pub struct BenchmarkIdentity {
    pub stack: Stack,
    pub concurrency: usize,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub certificate_sha256: String,
}

impl ServerConfig {
    pub fn parse() -> Result<Self, BoxError> {
        let args = parse_args()?;
        let identity = BenchmarkIdentity::from_args(&args)?;
        let config = Self {
            stack: identity.stack,
            listen: required(&args, "listen")?.to_owned(),
            control: required(&args, "control")?.into(),
            ready_file: required(&args, "ready-file")?.into(),
            certificate: required(&args, "cert")?.into(),
            private_key: required(&args, "key")?.into(),
            run_id: required(&args, "run-id")?.to_owned(),
            config_hash: required(&args, "config-hash")?.to_owned(),
            identity,
        };
        config.validate_hash()?;
        Ok(config)
    }

    fn validate_hash(&self) -> Result<(), BoxError> {
        if self.config_hash != self.identity.hash() {
            return Err("server configuration hash does not match parsed settings".into());
        }
        if sha256_file(&self.certificate)? != self.identity.certificate_sha256 {
            return Err("server certificate digest does not match parsed settings".into());
        }
        Ok(())
    }
}

impl ClientConfig {
    pub fn parse() -> Result<Self, BoxError> {
        let args = parse_args()?;
        let identity = BenchmarkIdentity::from_args(&args)?;
        let config = Self {
            stack: identity.stack,
            address: required(&args, "address")?.to_owned(),
            control: required(&args, "control")?.into(),
            certificate: required(&args, "cert")?.into(),
            run_id: required(&args, "run-id")?.to_owned(),
            config_hash: required(&args, "config-hash")?.to_owned(),
            concurrency: identity.concurrency,
            warmup_ms: identity.warmup_ms,
            measurement_ms: identity.measurement_ms,
            repetition: positive(required(&args, "repetition")?, "repetition")?,
            identity_certificate: required(&args, "identity-cert")?.into(),
            identity,
        };
        if config.config_hash != config.identity.hash() {
            return Err("client configuration hash does not match parsed settings".into());
        }
        config.validate_certificate_hash()?;
        Ok(config)
    }

    fn validate_certificate_hash(&self) -> Result<(), BoxError> {
        if sha256_file(&self.identity_certificate)? != self.identity.certificate_sha256 {
            return Err("client certificate digest does not match parsed settings".into());
        }
        Ok(())
    }
}

impl BenchmarkIdentity {
    pub fn parse() -> Result<Self, BoxError> {
        Self::from_args(&parse_args()?)
    }

    #[must_use]
    pub fn hash(&self) -> String {
        configuration_hash(&[
            ("application_encoding", "protobuf"),
            ("batching_policy", "one-request-per-lane"),
            ("certificate_sha256", &self.certificate_sha256),
            ("concurrency", &self.concurrency.to_string()),
            ("connections", "1"),
            ("measurement_ms", &self.measurement_ms.to_string()),
            ("network_profile", "loopback"),
            ("operation", "unary_closed_loop"),
            ("stack", self.stack.as_str()),
            ("warmup_ms", &self.warmup_ms.to_string()),
            ("workload", "tiny"),
        ])
    }

    fn from_args(args: &BTreeMap<String, String>) -> Result<Self, BoxError> {
        Ok(Self {
            stack: required(args, "stack")?.parse()?,
            concurrency: positive(required(args, "concurrency")?, "concurrency")?,
            warmup_ms: required(args, "warmup-ms")?.parse()?,
            measurement_ms: positive(required(args, "measurement-ms")?, "measurement-ms")?,
            certificate_sha256: required(args, "certificate-sha256")?.to_owned(),
        })
    }
}

#[must_use]
pub fn configuration_hash(values: &[(&str, &str)]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by_key(|(key, _)| *key);
    let mut hash = Sha256::new();
    for (key, value) in sorted {
        hash.update(key.as_bytes());
        hash.update([0]);
        hash.update(value.as_bytes());
        hash.update([0xff]);
    }
    format!("{:x}", hash.finalize())
}

fn parse_args() -> Result<BTreeMap<String, String>, BoxError> {
    let mut args = env::args().skip(2);
    let mut values = BTreeMap::new();
    while let Some(flag) = args.next() {
        let Some(name) = flag.strip_prefix("--") else {
            return Err(format!("expected --name, got {flag:?}").into());
        };
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if values.insert(name.to_owned(), value).is_some() {
            return Err(format!("duplicate argument {flag}").into());
        }
    }
    Ok(values)
}

fn required<'a>(args: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, BoxError> {
    args.get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("missing --{name}").into())
}

fn positive<T>(value: &str, name: &str) -> Result<T, BoxError>
where
    T: std::str::FromStr + Default + PartialEq,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = value.parse::<T>()?;
    if value == T::default() {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}

fn sha256_file(path: &PathBuf) -> Result<String, BoxError> {
    let mut hash = Sha256::new();
    hash.update(fs::read(path)?);
    Ok(format!("{:x}", hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::configuration_hash;

    #[test]
    fn configuration_hash_is_order_independent() {
        let first = configuration_hash(&[("stack", "trevrpc"), ("concurrency", "8")]);
        let second = configuration_hash(&[("concurrency", "8"), ("stack", "trevrpc")]);
        assert_eq!(first, second);
    }
}
