use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{BoxError, SCHEMA_VERSION};

pub const MAX_CONCURRENCY: usize = 1024;
pub const MAX_PAYLOAD_BYTES: u32 = 64 * 1024 * 1024;
pub const MAX_MESSAGES_PER_STREAM: u32 = 1_000_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Campaign {
    pub schema_version: u32,
    pub campaign_id: String,
    pub repetitions: u32,
    pub peers: Vec<Peer>,
    pub cells: Vec<Cell>,
    pub rpc_kinds: Vec<RpcKind>,
    pub concurrencies: Vec<usize>,
    pub timing: Timing,
    pub workload: Workload,
    #[serde(default = "default_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub id: String,
    pub command: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub id: String,
    pub client: String,
    pub server: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcKind {
    Unary,
    ClientStream,
    ServerStream,
    Bidi,
}

impl RpcKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unary => "unary",
            Self::ClientStream => "client_stream",
            Self::ServerStream => "server_stream",
            Self::Bidi => "bidi",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Timing {
    pub warmup_ms: u64,
    pub measurement_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    pub request_bytes: u32,
    pub response_bytes: u32,
    pub messages_per_stream: u32,
}

const fn default_startup_timeout_ms() -> u64 {
    30_000
}

const fn default_drain_timeout_ms() -> u64 {
    30_000
}

impl Campaign {
    pub fn read(path: &Path) -> Result<Self, BoxError> {
        let input = fs::read_to_string(path)?;
        serde_json::from_str(&input)
            .map_err(|error| format!("invalid campaign {}: {error}", path.display()).into())
    }

    pub fn validate(&self) -> Result<(), BoxError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(format!(
                "unsupported campaign schema version {}",
                self.schema_version
            )
            .into());
        }
        validate_id(&self.campaign_id, "campaign id")?;
        if self.repetitions == 0 {
            return Err("campaign repetitions must be positive".into());
        }
        if self.peers.is_empty() || self.cells.is_empty() {
            return Err("campaign must define peers and cells".into());
        }
        if self.rpc_kinds.is_empty() || self.concurrencies.is_empty() {
            return Err("campaign must define RPC kinds and concurrencies".into());
        }
        if self.timing.measurement_ms == 0 {
            return Err("measurement_ms must be positive".into());
        }
        if self.timing.measurement_ms > u64::MAX / 1_000_000 {
            return Err("measurement_ms is too large to represent in nanoseconds".into());
        }
        if self.workload.messages_per_stream == 0 {
            return Err("messages_per_stream must be positive".into());
        }
        if self.workload.request_bytes > MAX_PAYLOAD_BYTES
            || self.workload.response_bytes > MAX_PAYLOAD_BYTES
        {
            return Err(format!("payloads must not exceed {MAX_PAYLOAD_BYTES} bytes").into());
        }
        if self.workload.messages_per_stream > MAX_MESSAGES_PER_STREAM {
            return Err(
                format!("messages_per_stream must not exceed {MAX_MESSAGES_PER_STREAM}").into(),
            );
        }
        if self.startup_timeout_ms == 0 || self.drain_timeout_ms == 0 {
            return Err("timeouts must be positive".into());
        }

        let mut peers = BTreeMap::new();
        for peer in &self.peers {
            validate_id(&peer.id, "peer id")?;
            if peer.command.is_empty() || peer.command.iter().any(String::is_empty) {
                return Err(format!("peer {} has an empty command", peer.id).into());
            }
            if peers.insert(peer.id.as_str(), peer).is_some() {
                return Err(format!("duplicate peer id {}", peer.id).into());
            }
        }

        let mut cells = BTreeSet::new();
        for cell in &self.cells {
            validate_id(&cell.id, "cell id")?;
            if !cells.insert(cell.id.as_str()) {
                return Err(format!("duplicate cell id {}", cell.id).into());
            }
            if !peers.contains_key(cell.client.as_str())
                || !peers.contains_key(cell.server.as_str())
            {
                return Err(format!("cell {} references an unknown peer", cell.id).into());
            }
        }
        if self.rpc_kinds.iter().collect::<BTreeSet<_>>().len() != self.rpc_kinds.len() {
            return Err("rpc_kinds contains duplicates".into());
        }
        if self.concurrencies.contains(&0)
            || self
                .concurrencies
                .iter()
                .any(|&concurrency| concurrency > MAX_CONCURRENCY)
            || self.concurrencies.iter().collect::<BTreeSet<_>>().len() != self.concurrencies.len()
        {
            return Err(format!(
                "concurrencies must be positive, unique, and at most {MAX_CONCURRENCY}"
            )
            .into());
        }
        Ok(())
    }

    #[must_use]
    pub fn peer(&self, id: &str) -> Option<&Peer> {
        self.peers.iter().find(|peer| peer.id == id)
    }
}

fn validate_id(value: &str, name: &str) -> Result<(), BoxError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "{name} {value:?} must contain only ASCII letters, digits, '-' or '_'"
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        Campaign, Cell, MAX_CONCURRENCY, MAX_MESSAGES_PER_STREAM, MAX_PAYLOAD_BYTES, Peer, RpcKind,
        Timing, Workload,
    };

    fn campaign() -> Campaign {
        Campaign {
            schema_version: 1,
            campaign_id: "smoke".to_owned(),
            repetitions: 1,
            peers: vec![Peer {
                id: "rust".to_owned(),
                command: vec!["peer".to_owned()],
            }],
            cells: vec![Cell {
                id: "rust".to_owned(),
                client: "rust".to_owned(),
                server: "rust".to_owned(),
            }],
            rpc_kinds: vec![RpcKind::Unary],
            concurrencies: vec![1],
            timing: Timing {
                warmup_ms: 0,
                measurement_ms: 10,
            },
            workload: Workload {
                request_bytes: 16,
                response_bytes: 16,
                messages_per_stream: 4,
            },
            startup_timeout_ms: 1000,
            drain_timeout_ms: 1000,
        }
    }

    #[test]
    fn validates_minimal_campaign() {
        campaign().validate().expect("valid campaign");
    }

    #[test]
    fn rejects_unknown_peers() {
        let mut campaign = campaign();
        campaign.cells[0].server = "missing".to_owned();
        assert!(campaign.validate().is_err());
    }

    #[test]
    fn rejects_workloads_outside_protocol_limits() {
        let mut excessive_concurrency = campaign();
        excessive_concurrency.concurrencies = vec![MAX_CONCURRENCY + 1];
        assert!(excessive_concurrency.validate().is_err());

        let mut excessive_payload = campaign();
        excessive_payload.workload.response_bytes = MAX_PAYLOAD_BYTES + 1;
        assert!(excessive_payload.validate().is_err());

        let mut excessive_stream = campaign();
        excessive_stream.workload.messages_per_stream = MAX_MESSAGES_PER_STREAM + 1;
        assert!(excessive_stream.validate().is_err());
    }
}
