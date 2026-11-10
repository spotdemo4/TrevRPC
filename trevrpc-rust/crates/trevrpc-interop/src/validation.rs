//! The request the validation scenarios send, and the `ValidationBench` methods each
//! `--validation` value calls; see `bench/README.md`. The validation microbenchmarks share it.

use crate::proto::{BenchRequest, validation_bench_paths};

/// Which rules the request type of the `ValidationBench` methods a validation scenario calls
/// carries. Every request type has `BenchRequest`'s encoding, so each sends a `BenchRequest`,
/// and only the server's work differs between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Validation {
    /// `BenchRequest`, which has no rules.
    None,
    /// `StandardBenchRequest`, which has standard rules only.
    Standard,
    /// `CelBenchRequest`, which has the same standard rules and CEL rules.
    Cel,
}

impl Validation {
    pub const ALL: [Self; 3] = [Self::None, Self::Standard, Self::Cel];

    pub fn parse(name: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|validation| validation.name() == name)
            .ok_or_else(|| format!("unknown --validation {name:?}, want none, standard or cel"))
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Standard => "standard",
            Self::Cel => "cel",
        }
    }

    pub const fn unary_path(self) -> &'static str {
        match self {
            Self::None => validation_bench_paths::UNARY,
            Self::Standard => validation_bench_paths::STANDARD_UNARY,
            Self::Cel => validation_bench_paths::CEL_UNARY,
        }
    }

    pub const fn client_stream_path(self) -> &'static str {
        match self {
            Self::None => validation_bench_paths::CLIENT_STREAM,
            Self::Standard => validation_bench_paths::STANDARD_CLIENT_STREAM,
            Self::Cel => validation_bench_paths::CEL_CLIENT_STREAM,
        }
    }
}

/// The request the validation scenarios send: values that keep every rule of
/// `StandardBenchRequest` and `CelBenchRequest`, and the payload in `message`. The Go client
/// sends the same values.
pub fn bench_request(message: String) -> BenchRequest {
    BenchRequest {
        request_id: "0b6e8f3c-2d4a-4e7b-9c1f-5a8d7e6b3c20".to_owned(),
        email: "alice@example.com".to_owned(),
        region: "eu-west-1".to_owned(),
        quantity: 3,
        tags: vec![
            "checkout".to_owned(),
            "mobile".to_owned(),
            "beta".to_owned(),
        ],
        start_ms: 1_790_000_000_000,
        end_ms: 1_790_000_060_000,
        message,
    }
}
