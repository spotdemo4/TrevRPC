use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::campaign::{RpcKind, Stack};
use crate::{BoxError, SCHEMA_VERSION};

#[derive(Clone, Debug, Deserialize)]
pub struct EventHeader {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub rpc_kinds: Vec<String>,
    pub roles: BTreeMap<Role, Vec<Stack>>,
    pub histogram: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Client,
    Server,
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
pub struct Prepared {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub origin: String,
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
    pub fn validate(
        &self,
        expected_peer: &str,
        required_roles: &BTreeMap<Role, BTreeSet<Stack>>,
        required_rpc_kinds: &[RpcKind],
    ) -> Result<(), BoxError> {
        validate_event(
            self.schema_version,
            &self.event,
            "capabilities",
            &self.peer,
            expected_peer,
        )?;
        if required_roles.contains_key(&Role::Client) && self.histogram != "log_linear_v1" {
            return Err(format!("peer {expected_peer} lacks the required histogram").into());
        }
        for kind in required_rpc_kinds {
            if !self.rpc_kinds.iter().any(|item| item == kind.as_str()) {
                return Err(
                    format!("peer {expected_peer} does not support {}", kind.as_str()).into(),
                );
            }
        }
        for (role, required_stacks) in required_roles {
            let stacks = self.roles.get(role).ok_or_else(|| {
                format!(
                    "peer {expected_peer} does not support role {}",
                    role.as_str()
                )
            })?;
            for stack in required_stacks {
                if !stacks.contains(stack) {
                    return Err(format!(
                        "peer {expected_peer} role {} does not support stack {}",
                        role.as_str(),
                        stack.as_str()
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

impl Role {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
        }
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

impl Prepared {
    pub fn validate(&self, expected_peer: &str, expected_pid: u32) -> Result<(), BoxError> {
        validate_event(
            self.schema_version,
            &self.event,
            "prepared",
            &self.peer,
            expected_peer,
        )?;
        if !valid_origin(&self.origin) || self.pid != expected_pid {
            return Err(
                format!("peer {expected_peer} emitted inconsistent prepared metadata").into(),
            );
        }
        Ok(())
    }
}

fn valid_origin(origin: &str) -> bool {
    origin.split_once("://").is_some_and(|(scheme, authority)| {
        matches!(scheme, "http" | "https")
            && !authority.is_empty()
            && !authority
                .chars()
                .any(|character| character.is_whitespace() || matches!(character, '/' | '?' | '#'))
    })
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
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        Capabilities, HistogramBucket, Prepared, Role, expected_message_counts,
        histogram_quantiles, validate_timing,
    };
    use crate::SCHEMA_VERSION;
    use crate::campaign::{RpcKind, Stack};

    fn capabilities(stacks: Vec<Stack>) -> Capabilities {
        Capabilities {
            schema_version: SCHEMA_VERSION,
            event: "capabilities".to_owned(),
            peer: "rust".to_owned(),
            rpc_kinds: vec!["unary".to_owned()],
            roles: BTreeMap::from([(Role::Client, stacks.clone()), (Role::Server, stacks)]),
            histogram: "log_linear_v1".to_owned(),
        }
    }

    fn required(stacks: &[Stack]) -> BTreeMap<Role, BTreeSet<Stack>> {
        BTreeMap::from([
            (Role::Client, stacks.iter().copied().collect()),
            (Role::Server, stacks.iter().copied().collect()),
        ])
    }

    #[test]
    fn validates_required_roles_rpc_kinds_and_stacks() {
        capabilities(vec![Stack::TrevrpcNativeQuic])
            .validate(
                "rust",
                &required(&[Stack::TrevrpcNativeQuic]),
                &[RpcKind::Unary],
            )
            .expect("matching capabilities");
    }

    #[test]
    fn rejects_schema_v3_peer_events() {
        let mut capabilities = capabilities(vec![Stack::TrevrpcNativeQuic]);
        capabilities.schema_version = 3;
        assert!(
            capabilities
                .validate(
                    "rust",
                    &required(&[Stack::TrevrpcNativeQuic]),
                    &[RpcKind::Unary],
                )
                .is_err()
        );
    }

    #[test]
    fn rejects_missing_or_mismatched_required_stacks() {
        let client_only =
            BTreeMap::from([(Role::Client, BTreeSet::from([Stack::TrevrpcNativeQuic]))]);
        let missing = capabilities(Vec::new()).validate("rust", &client_only, &[RpcKind::Unary]);
        assert!(
            missing
                .unwrap_err()
                .to_string()
                .contains("trevrpc_native_quic")
        );

        let mismatched =
            capabilities(vec![Stack::GrpcHttp2]).validate("rust", &client_only, &[RpcKind::Unary]);
        assert!(
            mismatched
                .unwrap_err()
                .to_string()
                .contains("trevrpc_native_quic")
        );
    }

    #[test]
    fn validates_stacks_independently_for_each_role() {
        let mut capabilities = capabilities(vec![Stack::GrpcHttp2]);
        capabilities
            .roles
            .get_mut(&Role::Server)
            .expect("server capabilities")
            .clone_from(&vec![Stack::TrevrpcWebtransport]);
        let required = BTreeMap::from([
            (Role::Client, BTreeSet::from([Stack::GrpcHttp2])),
            (Role::Server, BTreeSet::from([Stack::TrevrpcWebtransport])),
        ]);
        capabilities
            .validate("rust", &required, &[RpcKind::Unary])
            .expect("role-specific stacks");
    }

    #[test]
    fn rejects_v3_global_capabilities() {
        let input = r#"{
            "schema_version": 3,
            "event": "capabilities",
            "peer": "rust",
            "roles": ["client", "server"],
            "rpc_kinds": ["unary"],
            "stacks": ["trevrpc_native_quic"],
            "histogram": "log_linear_v1"
        }"#;
        assert!(serde_json::from_str::<Capabilities>(input).is_err());
    }

    #[test]
    fn validates_webtransport_prepared_metadata() {
        let mut prepared = Prepared {
            schema_version: SCHEMA_VERSION,
            event: "prepared".to_owned(),
            peer: "chromium".to_owned(),
            origin: "http://127.0.0.1:43117".to_owned(),
            pid: 1234,
        };
        prepared
            .validate("chromium", 1234)
            .expect("valid prepared event");
        prepared.origin = "http://127.0.0.1:43117/path".to_owned();
        assert!(prepared.validate("chromium", 1234).is_err());
    }

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
