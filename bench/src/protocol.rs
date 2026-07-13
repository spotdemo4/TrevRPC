use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::campaign::RpcKind;
use crate::{BoxError, SCHEMA_VERSION};

#[derive(Clone, Debug, Deserialize)]
pub struct EventHeader {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Capabilities {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub roles: Vec<String>,
    pub rpc_kinds: Vec<String>,
    pub transports: Vec<String>,
    pub histogram: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Ready {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub address: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Armed {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HistogramBucket {
    pub upper_bound_ns: String,
    pub count: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerSample {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub rpc_kind: RpcKind,
    pub admission_ns: String,
    pub elapsed_ns: String,
    pub drain_ns: String,
    pub completed: String,
    pub failed: String,
    pub request_messages: String,
    pub response_messages: String,
    pub histogram: Vec<HistogramBucket>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ErrorEvent {
    pub phase: String,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Copy, Debug)]
pub struct ValidatedSample {
    pub admission_ns: u64,
    pub elapsed_ns: u64,
    pub drain_ns: u64,
    pub completed: u64,
    pub failed: u64,
    pub request_messages: u64,
    pub response_messages: u64,
    pub latency_p50_ns: u64,
    pub latency_p99_ns: u64,
    pub latency_max_ns: u64,
}

impl Capabilities {
    pub fn validate(&self, expected_peer: &str, required: &[RpcKind]) -> Result<(), BoxError> {
        validate_event(
            self.schema_version,
            &self.event,
            "capabilities",
            &self.peer,
            expected_peer,
        )?;
        let roles = self
            .roles
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if !roles.contains("client") || !roles.contains("server") {
            return Err(format!("peer {expected_peer} does not support both roles").into());
        }
        if self.histogram != "log_linear_v1"
            || !self.transports.iter().any(|item| item == "native_quic")
        {
            return Err(
                format!("peer {expected_peer} lacks the required transport or histogram").into(),
            );
        }
        for kind in required {
            if !self.rpc_kinds.iter().any(|item| item == kind.as_str()) {
                return Err(
                    format!("peer {expected_peer} does not support {}", kind.as_str()).into(),
                );
            }
        }
        Ok(())
    }
}

impl Ready {
    pub fn validate(&self, expected_peer: &str, expected_pid: u32) -> Result<(), BoxError> {
        validate_event(
            self.schema_version,
            &self.event,
            "ready",
            &self.peer,
            expected_peer,
        )?;
        if self.address.is_empty() || self.pid != expected_pid {
            return Err(
                format!("peer {expected_peer} emitted inconsistent readiness metadata").into(),
            );
        }
        Ok(())
    }
}

impl Armed {
    pub fn validate(&self, expected_peer: &str, expected_pid: u32) -> Result<(), BoxError> {
        validate_event(
            self.schema_version,
            &self.event,
            "armed",
            &self.peer,
            expected_peer,
        )?;
        if self.pid != expected_pid {
            return Err(format!("peer {expected_peer} emitted inconsistent armed PID").into());
        }
        Ok(())
    }
}

impl PeerSample {
    pub fn validate(
        &self,
        expected_peer: &str,
        expected_rpc: RpcKind,
        expected_admission_ms: u64,
        messages_per_stream: u32,
    ) -> Result<ValidatedSample, BoxError> {
        validate_event(
            self.schema_version,
            &self.event,
            "sample",
            &self.peer,
            expected_peer,
        )?;
        if self.rpc_kind != expected_rpc {
            return Err(format!("peer {expected_peer} returned the wrong RPC kind").into());
        }
        let admission_ns = parse_u64(&self.admission_ns, "admission_ns")?;
        let elapsed_ns = parse_u64(&self.elapsed_ns, "elapsed_ns")?;
        let drain_ns = parse_u64(&self.drain_ns, "drain_ns")?;
        let completed = parse_u64(&self.completed, "completed")?;
        let failed = parse_u64(&self.failed, "failed")?;
        let request_messages = parse_u64(&self.request_messages, "request_messages")?;
        let response_messages = parse_u64(&self.response_messages, "response_messages")?;
        validate_timing(admission_ns, elapsed_ns, drain_ns, expected_admission_ms).map_err(
            |error| format!("peer {expected_peer} returned inconsistent timing: {error}"),
        )?;
        if completed == 0 || failed != 0 {
            return Err(format!("peer {expected_peer} returned an empty or failed sample").into());
        }
        let expected_messages =
            expected_message_counts(expected_rpc, completed, messages_per_stream)?;
        if (request_messages, response_messages) != expected_messages {
            return Err(format!(
                "peer {expected_peer} returned {request_messages}/{response_messages} request/response messages; expected {}/{}",
                expected_messages.0, expected_messages.1
            )
            .into());
        }
        let (histogram_count, latency_p50_ns, latency_p99_ns, latency_max_ns) =
            histogram_quantiles(&self.histogram)?;
        if histogram_count != completed {
            return Err(format!(
                "peer {expected_peer} histogram count {histogram_count} differs from {completed} completions"
            )
            .into());
        }
        Ok(ValidatedSample {
            admission_ns,
            elapsed_ns,
            drain_ns,
            completed,
            failed,
            request_messages,
            response_messages,
            latency_p50_ns,
            latency_p99_ns,
            latency_max_ns,
        })
    }
}

pub fn validate_timing(
    admission_ns: u64,
    elapsed_ns: u64,
    drain_ns: u64,
    expected_admission_ms: u64,
) -> Result<(), BoxError> {
    let target_admission_ns = expected_admission_ms
        .checked_mul(1_000_000)
        .ok_or("configured admission duration overflows nanoseconds")?;
    if admission_ns == 0
        || admission_ns != target_admission_ns
        || elapsed_ns < admission_ns
        || drain_ns != elapsed_ns - admission_ns
    {
        return Err("admission, elapsed, and drain durations disagree".into());
    }
    Ok(())
}

pub fn expected_message_counts(
    rpc_kind: RpcKind,
    completed: u64,
    messages_per_stream: u32,
) -> Result<(u64, u64), BoxError> {
    let streamed = completed
        .checked_mul(u64::from(messages_per_stream))
        .ok_or("sample message count overflow")?;
    Ok(match rpc_kind {
        RpcKind::Unary => (completed, completed),
        RpcKind::ClientStream => (streamed, completed),
        RpcKind::ServerStream => (completed, streamed),
        RpcKind::Bidi => (streamed, streamed),
    })
}

pub fn parse_header(line: &str) -> Result<EventHeader, BoxError> {
    let header: EventHeader = serde_json::from_str(line)?;
    if header.schema_version != SCHEMA_VERSION {
        return Err(format!("unsupported peer schema version {}", header.schema_version).into());
    }
    Ok(header)
}

pub fn peer_error(line: &str) -> Result<String, BoxError> {
    let event: ErrorEvent = serde_json::from_str(line)?;
    Ok(format!(
        "peer error in {} ({}): {}",
        event.phase, event.code, event.message
    ))
}

fn validate_event(
    schema_version: u32,
    actual_event: &str,
    expected_event: &str,
    actual_peer: &str,
    expected_peer: &str,
) -> Result<(), BoxError> {
    if schema_version != SCHEMA_VERSION
        || actual_event != expected_event
        || actual_peer != expected_peer
    {
        return Err(format!("invalid {expected_event} event from peer {expected_peer}").into());
    }
    Ok(())
}

fn parse_u64(value: &str, field: &str) -> Result<u64, BoxError> {
    value
        .parse()
        .map_err(|error| format!("invalid {field} value {value:?}: {error}").into())
}

pub fn histogram_quantiles(buckets: &[HistogramBucket]) -> Result<(u64, u64, u64, u64), BoxError> {
    if buckets.is_empty() {
        return Err("sample histogram is empty".into());
    }
    let mut parsed = Vec::with_capacity(buckets.len());
    let mut total = 0_u64;
    let mut previous = 0_u64;
    for bucket in buckets {
        let upper = parse_u64(&bucket.upper_bound_ns, "histogram upper bound")?;
        let count = parse_u64(&bucket.count, "histogram count")?;
        if upper == 0 || upper <= previous || count == 0 {
            return Err("histogram buckets must have increasing positive bounds and counts".into());
        }
        total = total.checked_add(count).ok_or("histogram count overflow")?;
        previous = upper;
        parsed.push((upper, count));
    }
    let p50 = quantile(&parsed, total, 50, 100);
    let p99 = quantile(&parsed, total, 99, 100);
    Ok((total, p50, p99, previous))
}

fn quantile(buckets: &[(u64, u64)], total: u64, numerator: u64, denominator: u64) -> u64 {
    let rank = total.saturating_mul(numerator).div_ceil(denominator).max(1);
    let mut cumulative = 0_u64;
    for &(upper, count) in buckets {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= rank {
            return upper;
        }
    }
    buckets.last().map_or(0, |bucket| bucket.0)
}

#[cfg(test)]
mod tests {
    use super::{HistogramBucket, expected_message_counts, histogram_quantiles, validate_timing};
    use crate::campaign::RpcKind;

    #[test]
    fn computes_quantiles_from_sparse_buckets() {
        let buckets = vec![
            HistogramBucket {
                upper_bound_ns: "10".to_owned(),
                count: "50".to_owned(),
            },
            HistogramBucket {
                upper_bound_ns: "20".to_owned(),
                count: "49".to_owned(),
            },
            HistogramBucket {
                upper_bound_ns: "30".to_owned(),
                count: "1".to_owned(),
            },
        ];
        assert_eq!(
            histogram_quantiles(&buckets).expect("histogram"),
            (100, 10, 20, 30)
        );
    }

    #[test]
    fn derives_message_counts_for_every_rpc_kind() {
        assert_eq!(
            expected_message_counts(RpcKind::Unary, 3, 4).unwrap(),
            (3, 3)
        );
        assert_eq!(
            expected_message_counts(RpcKind::ClientStream, 3, 4).unwrap(),
            (12, 3)
        );
        assert_eq!(
            expected_message_counts(RpcKind::ServerStream, 3, 4).unwrap(),
            (3, 12)
        );
        assert_eq!(
            expected_message_counts(RpcKind::Bidi, 3, 4).unwrap(),
            (12, 12)
        );
        assert!(expected_message_counts(RpcKind::Bidi, u64::MAX, 2).is_err());
    }

    #[test]
    fn timing_requires_exact_positive_admission_and_consistent_drain() {
        assert!(validate_timing(1_000_000, 1_000_100, 100, 1).is_ok());
        assert!(validate_timing(0, 1_000_000, 1_000_000, 1).is_err());
        assert!(validate_timing(999_999, 1_000_000, 1, 1).is_err());
        assert!(validate_timing(1_000_000, 1_000_100, 99, 1).is_err());
    }
}
