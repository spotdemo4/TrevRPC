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
    #[serde(default)]
    pub network: Network,
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
    pub stack: Stack,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stack {
    TrevrpcNativeQuic,
    TrevrpcWebtransport,
}

impl Stack {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TrevrpcNativeQuic => "trevrpc_native_quic",
            Self::TrevrpcWebtransport => "trevrpc_webtransport",
        }
    }
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Network {
    #[serde(default)]
    pub backend: NetworkBackend,
    #[serde(default)]
    pub client_to_server: LinkCondition,
    #[serde(default)]
    pub server_to_client: LinkCondition,
    #[serde(default = "default_mtu")]
    pub mtu: u32,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            backend: NetworkBackend::Loopback,
            client_to_server: LinkCondition::default(),
            server_to_client: LinkCondition::default(),
            mtu: default_mtu(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkBackend {
    #[default]
    Loopback,
    Netns,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LinkCondition {
    #[serde(default)]
    pub delay_ms: u32,
    #[serde(default)]
    pub jitter_ms: u32,
    #[serde(default)]
    pub loss_percent: f64,
    #[serde(default)]
    pub rate_mbit: Option<u32>,
    #[serde(default)]
    pub queue_packets: Option<u32>,
}

impl LinkCondition {
    #[must_use]
    pub fn is_unrestricted(&self) -> bool {
        self.delay_ms == 0
            && self.jitter_ms == 0
            && self.loss_percent == 0.0
            && self.rate_mbit.is_none()
            && self.queue_packets.is_none()
    }

    fn validate(&self, direction: &str) -> Result<(), BoxError> {
        if self.jitter_ms > 0 && self.delay_ms == 0 {
            return Err(format!("{direction} jitter requires a positive delay").into());
        }
        if !self.loss_percent.is_finite() || self.loss_percent < 0.0 || self.loss_percent > 100.0 {
            return Err(format!("{direction} loss_percent must be between 0 and 100").into());
        }
        if self.rate_mbit == Some(0) {
            return Err(format!("{direction} rate_mbit must be positive").into());
        }
        if self.queue_packets == Some(0) {
            return Err(format!("{direction} queue_packets must be positive").into());
        }
        Ok(())
    }
}

const fn default_mtu() -> u32 {
    1500
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
        self.network.client_to_server.validate("client_to_server")?;
        self.network.server_to_client.validate("server_to_client")?;
        if !(576..=65_535).contains(&self.network.mtu) {
            return Err("network MTU must be between 576 and 65535".into());
        }
        if self.network.backend == NetworkBackend::Loopback
            && (!self.network.client_to_server.is_unrestricted()
                || !self.network.server_to_client.is_unrestricted()
                || self.network.mtu != default_mtu())
        {
            return Err("loopback network backend cannot define link impairments or MTU".into());
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
        Campaign, Cell, LinkCondition, MAX_CONCURRENCY, MAX_MESSAGES_PER_STREAM, MAX_PAYLOAD_BYTES,
        Network, NetworkBackend, Peer, RpcKind, Stack, Timing, Workload,
    };
    use crate::SCHEMA_VERSION;

    fn campaign() -> Campaign {
        Campaign {
            schema_version: SCHEMA_VERSION,
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
                stack: Stack::TrevrpcNativeQuic,
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
            network: Network::default(),
            startup_timeout_ms: 1000,
            drain_timeout_ms: 1000,
        }
    }

    #[test]
    fn validates_minimal_campaign() {
        campaign().validate().expect("valid campaign");
    }

    #[test]
    fn rejects_schema_v3_campaigns() {
        let mut campaign = campaign();
        campaign.schema_version = 3;
        assert!(campaign.validate().is_err());
    }

    #[test]
    fn rejects_unknown_peers() {
        let mut campaign = campaign();
        campaign.cells[0].server = "missing".to_owned();
        assert!(campaign.validate().is_err());
    }

    #[test]
    fn rejects_cells_without_a_stack() {
        let mut value = serde_json::to_value(campaign()).expect("campaign JSON");
        value["cells"][0]
            .as_object_mut()
            .expect("cell object")
            .remove("stack");
        let error = serde_json::from_value::<Campaign>(value).expect_err("missing stack");
        assert!(error.to_string().contains("missing field `stack`"));
    }

    #[test]
    fn stack_values_are_closed_and_stable() {
        assert_eq!(
            serde_json::to_string(&Stack::TrevrpcNativeQuic).unwrap(),
            "\"trevrpc_native_quic\""
        );
        assert_eq!(
            serde_json::to_string(&Stack::TrevrpcWebtransport).unwrap(),
            "\"trevrpc_webtransport\""
        );
        assert!(serde_json::from_str::<Stack>("\"native_quic\"").is_err());
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

    #[test]
    fn defaults_omitted_network_to_loopback() {
        let mut value = serde_json::to_value(campaign()).expect("campaign JSON");
        value
            .as_object_mut()
            .expect("campaign object")
            .remove("network");
        let parsed = serde_json::from_value::<Campaign>(value).expect("campaign without network");
        assert_eq!(parsed.network, Network::default());
    }

    #[test]
    fn validates_network_impairments() {
        let mut campaign = campaign();
        campaign.network = Network {
            backend: NetworkBackend::Netns,
            client_to_server: LinkCondition {
                delay_ms: 10,
                jitter_ms: 2,
                loss_percent: 0.1,
                rate_mbit: Some(100),
                queue_packets: Some(1000),
            },
            server_to_client: LinkCondition::default(),
            mtu: 1400,
        };
        campaign.validate().expect("valid netns profile");

        campaign.network.client_to_server.delay_ms = 0;
        assert!(campaign.validate().is_err());
    }

    #[test]
    fn rejects_impairments_for_loopback() {
        let mut campaign = campaign();
        campaign.network.client_to_server.delay_ms = 1;
        assert!(campaign.validate().is_err());
    }

    #[test]
    fn webtransport_smoke_has_expected_servers_and_samples() {
        for (content, browser, expected_servers, expected_samples) in [
            (
                include_str!("../campaigns/chromium-smoke.example.json"),
                "chromium",
                6,
                24,
            ),
            (
                include_str!("../campaigns/firefox-smoke.example.json"),
                "firefox",
                6,
                24,
            ),
            (
                include_str!("../campaigns/webkit-smoke.example.json"),
                "webkit",
                5,
                20,
            ),
        ] {
            let campaign: Campaign =
                serde_json::from_str(content).expect("WebTransport smoke campaign");
            campaign.validate().expect("valid WebTransport campaign");
            assert_eq!(campaign.cells.len(), expected_servers);
            assert!(
                campaign
                    .cells
                    .iter()
                    .all(|cell| cell.client == browser && cell.stack == Stack::TrevrpcWebtransport)
            );
            if browser == "webkit" {
                // Keep the Go server excluded until upstream Safari compatibility is resolved:
                // https://github.com/quic-go/webtransport-go/issues/355
                assert!(campaign.peer("go").is_none());
                assert!(campaign.cells.iter().all(|cell| cell.server != "go"));
            }
            let sample_count = usize::try_from(campaign.repetitions).unwrap()
                * campaign.cells.len()
                * campaign.rpc_kinds.len()
                * campaign.concurrencies.len();
            assert_eq!(sample_count, expected_samples);
        }
    }
}
