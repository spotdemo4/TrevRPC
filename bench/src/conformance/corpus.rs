use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::BoxError;
use crate::conformance::SCHEMA_VERSION;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Suite {
    pub schema_version: u32,
    pub suite_id: String,
    pub protocol_version: u32,
    pub peer_resolution: PeerResolution,
    pub allowance_policy: AllowancePolicy,
    pub startup_timeout_ms: u64,
    pub case_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub max_command_bytes: usize,
    pub max_event_bytes: usize,
    pub max_stdout_bytes: usize,
    pub max_stderr_bytes: usize,
    pub crash_budget: u32,
    pub golden_path: String,
    pub corpus_paths: Vec<String>,
    pub peers: Vec<Peer>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerResolution {
    PathOrOverride,
    AbsoluteOverrides,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowancePolicy {
    PeerSpecific,
    Forbid,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub id: String,
    pub command: Vec<String>,
    pub required_capabilities: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct LoadedSuite {
    pub suite: Suite,
    pub suite_path: PathBuf,
    pub golden_path: PathBuf,
    pub corpus_paths: Vec<PathBuf>,
    pub corpora: Vec<Corpus>,
    pub goldens: GoldenVectors,
}

#[derive(Clone, Debug)]
pub struct Corpus {
    pub kind: String,
    pub path: PathBuf,
    pub cases: Vec<Case>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusDocument {
    schema_version: u32,
    kind: String,
    cases: Vec<Value>,
}

#[derive(Clone, Debug)]
pub enum Case {
    CodecEncode(CodecEncodeCase),
    CodecDecode(CodecDecodeCase),
    FramingEncode(FramingEncodeCase),
    FramingDecode(FramingDecodeCase),
    StateServer(StateServerCase),
    StateClient(StateClientCase),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodecEncodeCase {
    pub id: String,
    pub operation: OperationCodecEncode,
    pub message_type: MessageType,
    pub message: NormalizedMessage,
    pub golden: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodecDecodeCase {
    pub id: String,
    pub operation: OperationCodecDecode,
    pub message_type: MessageType,
    #[serde(default)]
    pub body_hex: Option<String>,
    #[serde(default)]
    pub golden: Option<String>,
    #[serde(default)]
    pub expected_message: Option<NormalizedMessage>,
    #[serde(default)]
    pub canonical_body_hex: Option<String>,
    #[serde(default)]
    pub expected_error: Option<ExpectedError>,
    #[serde(default)]
    pub peer_allowances: BTreeMap<String, ExpectedError>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FramingEncodeCase {
    pub id: String,
    pub operation: OperationFramingEncode,
    pub message_type: MessageType,
    pub max_frame_size: usize,
    pub message: NormalizedMessage,
    pub expected_error: ExpectedError,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FramingDecodeCase {
    pub id: String,
    pub operation: OperationFramingDecode,
    pub message_type: MessageType,
    pub max_frame_size: usize,
    pub chunks_hex: Vec<String>,
    #[serde(default)]
    pub expected_bodies_hex: Option<Vec<String>>,
    #[serde(default)]
    pub expected_eof: Option<bool>,
    #[serde(default)]
    pub expected_error: Option<ExpectedError>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateServerCase {
    pub id: String,
    pub operation: OperationStateServer,
    pub frames_hex: Vec<String>,
    #[serde(default)]
    pub expected_events: Option<Vec<StateEvent>>,
    #[serde(default)]
    pub expected_terminal_status: Option<NormalizedStatus>,
    pub expected_transport_close_count: String,
    #[serde(default)]
    pub expected_error: Option<ExpectedError>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateClientCase {
    pub id: String,
    pub operation: OperationStateClient,
    pub frames_hex: Vec<String>,
    #[serde(default)]
    pub expected_response_body_hex: Option<String>,
    #[serde(default)]
    pub expected_error: Option<ExpectedError>,
}

macro_rules! operation_marker {
    ($name:ident, $text:literal) => {
        #[derive(Clone, Copy, Debug, Deserialize)]
        pub enum $name {
            #[serde(rename = $text)]
            Value,
        }
    };
}
operation_marker!(OperationCodecEncode, "codec.encode");
operation_marker!(OperationCodecDecode, "codec.decode");
operation_marker!(OperationFramingEncode, "framing.encode");
operation_marker!(OperationFramingDecode, "framing.decode_stream");
operation_marker!(OperationStateServer, "state.server_stream");
operation_marker!(OperationStateClient, "state.client_stream");

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    RpcRequest,
    RpcResponse,
    RpcStreamFrame,
}

impl MessageType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RpcRequest => "rpc_request",
            Self::RpcResponse => "rpc_response",
            Self::RpcStreamFrame => "rpc_stream_frame",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NormalizedMessage {
    RpcRequest(NormalizedRequest),
    RpcResponse(NormalizedResponse),
    RpcStreamFrame(NormalizedStreamFrame),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedRequest {
    pub service_hex: String,
    pub method_hex: String,
    pub body_hex: String,
    pub metadata: Vec<MetadataEntry>,
    pub kind: String,
    pub version: String,
    pub timeout_nanos: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedResponse {
    pub status_raw: String,
    pub status_code: u32,
    pub message_hex: String,
    pub body_hex: String,
    pub metadata: Vec<MetadataEntry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedStreamFrame {
    pub kind: String,
    pub kind_raw: String,
    pub status_raw: String,
    pub status_code: u32,
    pub message_hex: String,
    pub body_hex: String,
    pub metadata: Vec<MetadataEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataEntry {
    pub key_hex: String,
    pub value_hex: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedStatus {
    pub status_raw: String,
    pub status_code: u32,
    pub message_hex: String,
    pub metadata: Vec<MetadataEntry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StateEvent {
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_hex: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedError {
    pub category: String,
    pub status_code: u32,
}

#[derive(Clone, Debug)]
pub struct GoldenVectors {
    entries: BTreeMap<String, Vec<u8>>,
}

impl GoldenVectors {
    pub fn load(path: &Path) -> Result<Self, BoxError> {
        let text = fs::read_to_string(path)?;
        let mut entries = BTreeMap::new();
        for (index, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("invalid golden vector line {}", index + 1))?;
            let key = key.trim().to_owned();
            if entries.contains_key(&key) {
                return Err(format!("duplicate golden vector key {key:?}").into());
            }
            entries.insert(key, decode_hex(value.trim())?);
        }
        Ok(Self { entries })
    }

    pub fn pair(&self, base: &str) -> Result<(&[u8], &[u8]), BoxError> {
        let body_key = format!("{base}.body");
        let frame_key = format!("{base}.frame");
        let body = self
            .entries
            .get(&body_key)
            .ok_or_else(|| format!("missing golden vector {body_key}"))?;
        let frame = self
            .entries
            .get(&frame_key)
            .ok_or_else(|| format!("missing golden vector {frame_key}"))?;
        Ok((body, frame))
    }
}

impl LoadedSuite {
    pub fn load(path: &Path) -> Result<Self, BoxError> {
        let suite_path = absolute(path)?;
        let suite: Suite = serde_json::from_slice(&fs::read(&suite_path)?)?;
        suite.validate()?;
        let directory = suite_path.parent().ok_or("suite path has no parent")?;
        let golden_path = absolute(&directory.join(&suite.golden_path))?;
        let corpus_paths = suite
            .corpus_paths
            .iter()
            .map(|path| absolute(&directory.join(path)))
            .collect::<Result<Vec<_>, _>>()?;
        let goldens = GoldenVectors::load(&golden_path)?;
        let mut ids = BTreeSet::new();
        let mut corpora = Vec::with_capacity(corpus_paths.len());
        for path in &corpus_paths {
            let corpus = Corpus::load(path)?;
            for case in &corpus.cases {
                if !ids.insert(case.id().to_owned()) {
                    return Err(format!("duplicate case ID {:?}", case.id()).into());
                }
                case.validate(&goldens)?;
            }
            corpora.push(corpus);
        }
        let peer_ids = suite
            .peers
            .iter()
            .map(|peer| peer.id.as_str())
            .collect::<BTreeSet<_>>();
        for case in corpora.iter().flat_map(|corpus| &corpus.cases) {
            if let Case::CodecDecode(case) = case {
                if suite.allowance_policy == AllowancePolicy::Forbid
                    && !case.peer_allowances.is_empty()
                {
                    return Err(format!(
                        "suite {} forbids peer allowances, but {} contains them",
                        suite.suite_id, case.id
                    )
                    .into());
                }
                for peer in case.peer_allowances.keys() {
                    if !peer_ids.contains(peer.as_str()) {
                        return Err(format!("{} allows unknown peer {peer:?}", case.id).into());
                    }
                }
            }
        }
        Ok(Self {
            suite,
            suite_path,
            golden_path,
            corpus_paths,
            corpora,
            goldens,
        })
    }

    #[must_use]
    pub fn case_count(&self) -> usize {
        self.corpora.iter().map(|corpus| corpus.cases.len()).sum()
    }
}

impl Suite {
    fn validate(&self) -> Result<(), BoxError> {
        if self.schema_version != SCHEMA_VERSION || self.protocol_version != 1 {
            return Err("unsupported conformance suite version".into());
        }
        validate_id(&self.suite_id)?;
        if self.peers.is_empty() || self.corpus_paths.is_empty() || self.crash_budget == 0 {
            return Err("suite must have peers, corpus paths, and a positive crash budget".into());
        }
        if self.startup_timeout_ms == 0
            || self.case_timeout_ms == 0
            || self.shutdown_timeout_ms == 0
            || self.max_command_bytes == 0
            || self.max_event_bytes == 0
            || self.max_stdout_bytes == 0
            || self.max_stderr_bytes == 0
        {
            return Err("suite time and byte limits must be positive".into());
        }
        if self.max_command_bytes > 262_144 || self.max_event_bytes > 65_536 {
            return Err("suite exceeds process protocol v1 command or event caps".into());
        }
        let mut peer_ids = BTreeSet::new();
        let mut previous_peer: Option<&str> = None;
        for peer in &self.peers {
            if previous_peer.is_some_and(|previous| previous >= peer.id.as_str()) {
                return Err("suite peers must be ID-sorted and unique".into());
            }
            previous_peer = Some(&peer.id);
            validate_id(&peer.id)?;
            if !peer_ids.insert(&peer.id)
                || peer.command.is_empty()
                || peer.command.iter().any(String::is_empty)
            {
                return Err(format!("duplicate peer or empty command for {:?}", peer.id).into());
            }
            let mut sorted = peer.required_capabilities.clone();
            sorted.sort();
            sorted.dedup();
            if sorted != peer.required_capabilities {
                return Err(
                    format!("peer {} capabilities must be sorted and unique", peer.id).into(),
                );
            }
        }
        Ok(())
    }
}

impl Corpus {
    fn load(path: &Path) -> Result<Self, BoxError> {
        let document: CorpusDocument = serde_json::from_slice(&fs::read(path)?)?;
        if document.schema_version != SCHEMA_VERSION {
            return Err(format!("unsupported corpus schema in {}", path.display()).into());
        }
        let mut cases = Vec::with_capacity(document.cases.len());
        for value in document.cases {
            let operation = value
                .get("operation")
                .and_then(Value::as_str)
                .ok_or("case operation must be a string")?;
            let case = match operation {
                "codec.encode" => Case::CodecEncode(serde_json::from_value(value)?),
                "codec.decode" => Case::CodecDecode(serde_json::from_value(value)?),
                "framing.encode" => Case::FramingEncode(serde_json::from_value(value)?),
                "framing.decode_stream" => Case::FramingDecode(serde_json::from_value(value)?),
                "state.server_stream" => Case::StateServer(serde_json::from_value(value)?),
                "state.client_stream" => Case::StateClient(serde_json::from_value(value)?),
                _ => return Err(format!("unknown operation {operation:?}").into()),
            };
            cases.push(case);
        }
        Ok(Self {
            kind: document.kind,
            path: path.to_path_buf(),
            cases,
        })
    }
}

impl Case {
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::CodecEncode(case) => &case.id,
            Self::CodecDecode(case) => &case.id,
            Self::FramingEncode(case) => &case.id,
            Self::FramingDecode(case) => &case.id,
            Self::StateServer(case) => &case.id,
            Self::StateClient(case) => &case.id,
        }
    }

    #[must_use]
    pub const fn operation(&self) -> &'static str {
        match self {
            Self::CodecEncode(_) => "codec.encode",
            Self::CodecDecode(_) => "codec.decode",
            Self::FramingEncode(_) => "framing.encode",
            Self::FramingDecode(_) => "framing.decode_stream",
            Self::StateServer(_) => "state.server_stream",
            Self::StateClient(_) => "state.client_stream",
        }
    }

    fn validate(&self, goldens: &GoldenVectors) -> Result<(), BoxError> {
        validate_id(self.id())?;
        match self {
            Self::CodecEncode(case) => validate_codec_encode(case, goldens),
            Self::CodecDecode(case) => validate_codec_decode(case, goldens),
            Self::FramingEncode(case) => validate_framing_encode(case),
            Self::FramingDecode(case) => validate_framing_decode(case),
            Self::StateServer(case) => validate_state_server(case),
            Self::StateClient(case) => validate_state_client(case),
        }
    }

    pub fn command_fields(&self, goldens: &GoldenVectors) -> Result<Vec<String>, BoxError> {
        match self {
            Self::CodecEncode(case) => {
                let mut fields = vec![case.message_type.as_str().to_owned()];
                append_message_fields(&mut fields, &case.message);
                Ok(fields)
            }
            Self::CodecDecode(case) => {
                let body = if let Some(body) = &case.body_hex {
                    body.clone()
                } else {
                    encode_hex(
                        goldens
                            .pair(case.golden.as_deref().ok_or("missing golden")?)?
                            .0,
                    )
                };
                Ok(vec![case.message_type.as_str().to_owned(), body])
            }
            Self::FramingEncode(case) => {
                let mut fields = vec![
                    case.message_type.as_str().to_owned(),
                    case.max_frame_size.to_string(),
                ];
                append_message_fields(&mut fields, &case.message);
                Ok(fields)
            }
            Self::FramingDecode(case) => {
                let mut fields = vec![
                    case.message_type.as_str().to_owned(),
                    case.max_frame_size.to_string(),
                    case.chunks_hex.len().to_string(),
                ];
                fields.extend(case.chunks_hex.iter().cloned());
                Ok(fields)
            }
            Self::StateServer(case) => Ok(frame_fields(&case.frames_hex)),
            Self::StateClient(case) => Ok(frame_fields(&case.frames_hex)),
        }
    }

    pub fn expected_payload(&self, goldens: &GoldenVectors) -> Result<Value, BoxError> {
        let mut object = Map::new();
        match self {
            Self::CodecEncode(case) => {
                let (body, frame) = goldens.pair(&case.golden)?;
                object.insert("body_hex".to_owned(), Value::String(encode_hex(body)));
                object.insert("frame_hex".to_owned(), Value::String(encode_hex(frame)));
            }
            Self::CodecDecode(case) => {
                if let Some(error) = &case.expected_error {
                    return Ok(error_value(error));
                }
                object.insert(
                    "message".to_owned(),
                    serde_json::to_value(
                        case.expected_message
                            .as_ref()
                            .ok_or("missing expected message")?,
                    )?,
                );
                let canonical = if let Some(body) = &case.canonical_body_hex {
                    body.clone()
                } else {
                    encode_hex(
                        goldens
                            .pair(case.golden.as_deref().ok_or("missing golden")?)?
                            .0,
                    )
                };
                object.insert("canonical_body_hex".to_owned(), Value::String(canonical));
            }
            Self::FramingEncode(case) => return Ok(error_value(&case.expected_error)),
            Self::FramingDecode(case) => {
                if let Some(error) = &case.expected_error {
                    return Ok(error_value(error));
                }
                object.insert(
                    "bodies_hex".to_owned(),
                    serde_json::to_value(
                        case.expected_bodies_hex.as_ref().ok_or("missing bodies")?,
                    )?,
                );
                object.insert(
                    "eof".to_owned(),
                    Value::Bool(case.expected_eof.ok_or("missing eof")?),
                );
            }
            Self::StateServer(case) => {
                if let Some(error) = &case.expected_error {
                    let mut value = error_value(error);
                    value.as_object_mut().ok_or("error is not object")?.insert(
                        "transport_close_count".to_owned(),
                        Value::String(case.expected_transport_close_count.clone()),
                    );
                    return Ok(value);
                }
                object.insert(
                    "events".to_owned(),
                    serde_json::to_value(case.expected_events.as_ref().ok_or("missing events")?)?,
                );
                if let Some(status) = &case.expected_terminal_status {
                    object.insert("terminal_status".to_owned(), serde_json::to_value(status)?);
                }
                object.insert(
                    "transport_close_count".to_owned(),
                    Value::String(case.expected_transport_close_count.clone()),
                );
            }
            Self::StateClient(case) => {
                if let Some(error) = &case.expected_error {
                    return Ok(error_value(error));
                }
                object.insert(
                    "response_body_hex".to_owned(),
                    Value::String(
                        case.expected_response_body_hex
                            .clone()
                            .ok_or("missing response")?,
                    ),
                );
            }
        }
        object.insert("outcome".to_owned(), Value::String("success".to_owned()));
        Ok(Value::Object(object))
    }

    pub fn peer_allowance(&self, peer: &str) -> Result<Option<Value>, BoxError> {
        let Self::CodecDecode(case) = self else {
            return Ok(None);
        };
        Ok(case.peer_allowances.get(peer).map(error_value))
    }
}

fn error_value(error: &ExpectedError) -> Value {
    let mut object = Map::new();
    object.insert("outcome".to_owned(), Value::String("error".to_owned()));
    object.insert("category".to_owned(), Value::String(error.category.clone()));
    object.insert(
        "status_code".to_owned(),
        Value::Number(error.status_code.into()),
    );
    Value::Object(object)
}

fn validate_codec_encode(case: &CodecEncodeCase, goldens: &GoldenVectors) -> Result<(), BoxError> {
    validate_message(case.message_type, &case.message)?;
    goldens.pair(&case.golden)?;
    Ok(())
}

fn validate_codec_decode(case: &CodecDecodeCase, goldens: &GoldenVectors) -> Result<(), BoxError> {
    if case.body_hex.is_some() == case.golden.is_some() {
        return Err(format!("{} must specify exactly one body source", case.id).into());
    }
    if let Some(body) = &case.body_hex {
        decode_hex(body)?;
    }
    if let Some(golden) = &case.golden {
        goldens.pair(golden)?;
    }
    if let Some(message) = &case.expected_message {
        validate_message(case.message_type, message)?;
    }
    if let Some(body) = &case.canonical_body_hex {
        decode_hex(body)?;
    }
    if case.expected_error.is_some()
        == (case.expected_message.is_some() || case.canonical_body_hex.is_some())
    {
        return Err(format!("{} must have either success or error expectations", case.id).into());
    }
    validate_expected_error(case.expected_error.as_ref(), Some(case.message_type))?;
    if !case.peer_allowances.is_empty() && case.expected_error.is_none() {
        return Err(format!(
            "{} cannot allow peer errors without a normative error",
            case.id
        )
        .into());
    }
    for allowance in case.peer_allowances.values() {
        validate_expected_error(Some(allowance), Some(case.message_type))?;
        if case.expected_error.as_ref() == Some(allowance) {
            return Err(
                format!("{} peer allowance duplicates the normative error", case.id).into(),
            );
        }
    }
    Ok(())
}

fn validate_framing_encode(case: &FramingEncodeCase) -> Result<(), BoxError> {
    validate_message(case.message_type, &case.message)?;
    validate_expected_error(Some(&case.expected_error), Some(case.message_type))
}

fn validate_framing_decode(case: &FramingDecodeCase) -> Result<(), BoxError> {
    validate_frames(&case.chunks_hex)?;
    if let Some(bodies) = &case.expected_bodies_hex {
        validate_frames(bodies)?;
    }
    if case.expected_error.is_some()
        == (case.expected_bodies_hex.is_some() || case.expected_eof.is_some())
    {
        return Err(format!("{} must have either success or error expectations", case.id).into());
    }
    validate_expected_error(case.expected_error.as_ref(), Some(case.message_type))
}

fn validate_state_server(case: &StateServerCase) -> Result<(), BoxError> {
    validate_frames(&case.frames_hex)?;
    if canonical_decimal(&case.expected_transport_close_count)? != 1 {
        return Err(format!("{} must require exactly one transport close", case.id).into());
    }
    let has_success = case.expected_events.is_some();
    if case.expected_terminal_status.is_some() && !has_success {
        return Err(format!("{} has terminal status without state events", case.id).into());
    }
    if case.expected_error.is_some() == has_success {
        return Err(format!("{} must have either success or error expectations", case.id).into());
    }
    if let Some(events) = &case.expected_events {
        validate_state_events(events)?;
    }
    if let Some(status) = &case.expected_terminal_status {
        validate_status(status)?;
    }
    validate_expected_error(
        case.expected_error.as_ref(),
        Some(MessageType::RpcStreamFrame),
    )
}

fn validate_state_events(events: &[StateEvent]) -> Result<(), BoxError> {
    for event in events {
        if !matches!(event.event.as_str(), "message" | "eof") {
            return Err(format!("unknown state event {:?}", event.event).into());
        }
        if let Some(body) = &event.body_hex {
            decode_hex(body)?;
        }
    }
    Ok(())
}

fn validate_state_client(case: &StateClientCase) -> Result<(), BoxError> {
    validate_frames(&case.frames_hex)?;
    if let Some(body) = &case.expected_response_body_hex {
        decode_hex(body)?;
    }
    if case.expected_error.is_some() == case.expected_response_body_hex.is_some() {
        return Err(format!("{} must have either response or error", case.id).into());
    }
    validate_expected_error(
        case.expected_error.as_ref(),
        Some(MessageType::RpcStreamFrame),
    )
}

fn validate_message(
    expected_type: MessageType,
    message: &NormalizedMessage,
) -> Result<(), BoxError> {
    let actual_type = match message {
        NormalizedMessage::RpcRequest(request) => {
            for value in [&request.service_hex, &request.method_hex, &request.body_hex] {
                decode_hex(value)?;
            }
            validate_metadata(&request.metadata)?;
            if !matches!(
                request.kind.as_str(),
                "unary" | "client_stream" | "server_stream" | "bidi"
            ) {
                return Err(format!("unknown request kind {:?}", request.kind).into());
            }
            canonical_u32(&request.version)?;
            canonical_decimal(&request.timeout_nanos)?;
            MessageType::RpcRequest
        }
        NormalizedMessage::RpcResponse(response) => {
            let status_raw = canonical_u32(&response.status_raw)?;
            validate_status_code(status_raw, response.status_code)?;
            decode_hex(&response.message_hex)?;
            decode_hex(&response.body_hex)?;
            validate_metadata(&response.metadata)?;
            MessageType::RpcResponse
        }
        NormalizedMessage::RpcStreamFrame(frame) => {
            if !matches!(frame.kind.as_str(), "message" | "status") {
                return Err(format!("unknown stream frame kind {:?}", frame.kind).into());
            }
            let kind_raw = canonical_u32(&frame.kind_raw)?;
            let expected_kind = match kind_raw {
                0 => "message",
                1 => "status",
                _ => return Err(format!("unsupported stream frame kind {kind_raw}").into()),
            };
            if frame.kind != expected_kind {
                return Err(format!(
                    "stream frame kind {:?} contradicts kind_raw {}",
                    frame.kind, frame.kind_raw
                )
                .into());
            }
            let status_raw = canonical_u32(&frame.status_raw)?;
            validate_status_code(status_raw, frame.status_code)?;
            decode_hex(&frame.message_hex)?;
            decode_hex(&frame.body_hex)?;
            validate_metadata(&frame.metadata)?;
            MessageType::RpcStreamFrame
        }
    };
    if actual_type != expected_type {
        return Err("normalized message type does not match case message_type".into());
    }
    Ok(())
}

fn validate_status(status: &NormalizedStatus) -> Result<(), BoxError> {
    let status_raw = canonical_u32(&status.status_raw)?;
    validate_status_code(status_raw, status.status_code)?;
    decode_hex(&status.message_hex)?;
    validate_metadata(&status.metadata)
}

fn validate_status_code(status_raw: u32, status_code: u32) -> Result<(), BoxError> {
    let canonical = match status_raw {
        0 | 1 | 3..=16 => status_raw,
        _ => 2,
    };
    if status_code != canonical {
        return Err(
            format!("status_code {status_code} contradicts status_raw {status_raw}").into(),
        );
    }
    Ok(())
}

fn canonical_u32(value: &str) -> Result<u32, BoxError> {
    let value = canonical_decimal(value)?;
    Ok(u32::try_from(value).map_err(|_| format!("wire scalar {value} exceeds uint32"))?)
}

fn validate_metadata(metadata: &[MetadataEntry]) -> Result<(), BoxError> {
    let mut previous: Option<&str> = None;
    for entry in metadata {
        decode_hex(&entry.key_hex)?;
        decode_hex(&entry.value_hex)?;
        if previous.is_some_and(|key| key >= entry.key_hex.as_str()) {
            return Err("normalized metadata must be key-sorted and unique".into());
        }
        previous = Some(&entry.key_hex);
    }
    Ok(())
}

fn validate_frames(frames: &[String]) -> Result<(), BoxError> {
    for frame in frames {
        decode_hex(frame)?;
    }
    Ok(())
}

fn validate_expected_error(
    error: Option<&ExpectedError>,
    message_type: Option<MessageType>,
) -> Result<(), BoxError> {
    let Some(error) = error else {
        return Ok(());
    };
    let directional = match message_type {
        Some(MessageType::RpcRequest) => 3,
        Some(MessageType::RpcResponse | MessageType::RpcStreamFrame) | None => 13,
    };
    let fixed = match error.category.as_str() {
        "malformed_protobuf" | "invalid_metadata" => Some(directional),
        "unsupported_rpc_kind" | "unsupported_frame_kind" => Some(3),
        "unsupported_wire_version" => Some(9),
        "frame_too_large" => Some(8),
        "incomplete_frame"
        | "missing_terminal_status"
        | "response_cardinality"
        | "trailing_frame" => Some(13),
        "remote_status" => None,
        _ => return Err(format!("unknown error category {:?}", error.category).into()),
    };
    if fixed.is_some_and(|code| code != error.status_code) {
        return Err(format!(
            "wrong status code {} for {} (expected {})",
            error.status_code,
            error.category,
            fixed.unwrap_or(error.status_code)
        )
        .into());
    }
    if error.category == "remote_status" && !matches!(error.status_code, 1..=16) {
        return Err("remote_status must use a canonical non-OK public status code".into());
    }
    Ok(())
}

fn append_message_fields(fields: &mut Vec<String>, message: &NormalizedMessage) {
    match message {
        NormalizedMessage::RpcRequest(request) => {
            fields.extend([
                request.service_hex.clone(),
                request.method_hex.clone(),
                request.body_hex.clone(),
            ]);
            append_metadata(fields, &request.metadata);
            fields.extend([
                request.kind.clone(),
                request.version.clone(),
                request.timeout_nanos.clone(),
            ]);
        }
        NormalizedMessage::RpcResponse(response) => {
            fields.extend([
                response.status_raw.clone(),
                response.message_hex.clone(),
                response.body_hex.clone(),
            ]);
            append_metadata(fields, &response.metadata);
        }
        NormalizedMessage::RpcStreamFrame(frame) => {
            fields.extend([
                frame.kind_raw.clone(),
                frame.status_raw.clone(),
                frame.message_hex.clone(),
                frame.body_hex.clone(),
            ]);
            append_metadata(fields, &frame.metadata);
        }
    }
}

fn append_metadata(fields: &mut Vec<String>, metadata: &[MetadataEntry]) {
    fields.push(metadata.len().to_string());
    for entry in metadata {
        fields.push(entry.key_hex.clone());
        fields.push(entry.value_hex.clone());
    }
}

fn frame_fields(frames: &[String]) -> Vec<String> {
    let mut fields = vec![frames.len().to_string()];
    fields.extend(frames.iter().cloned());
    fields
}

fn validate_id(id: &str) -> Result<(), BoxError> {
    if id.is_empty()
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(format!("invalid identifier {id:?}").into());
    }
    Ok(())
}

pub fn decode_hex(value: &str) -> Result<Vec<u8>, BoxError> {
    if !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid lowercase even-length hex {value:?}").into());
    }
    Ok(base16ct::lower::decode_vec(value)?)
}

#[must_use]
pub fn encode_hex(value: &[u8]) -> String {
    base16ct::lower::encode_string(value)
}

pub fn canonical_decimal(value: &str) -> Result<u64, BoxError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid canonical decimal {value:?}").into());
    }
    Ok(value.parse()?)
}

pub fn sha256_file(path: &Path) -> Result<String, BoxError> {
    Ok(base16ct::lower::encode_string(
        Sha256::digest(fs::read(path)?).as_slice(),
    ))
}

fn absolute(path: &Path) -> Result<PathBuf, BoxError> {
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    }
    .canonicalize()?)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    use crate::conformance::protocol;

    use super::{
        AllowancePolicy, Case, GoldenVectors, LoadedSuite, MessageType, NormalizedMessage,
        NormalizedResponse, NormalizedStreamFrame, canonical_decimal, decode_hex, validate_message,
    };

    #[test]
    fn rejects_noncanonical_hex_and_decimal() {
        assert!(decode_hex("A0").is_err());
        assert!(decode_hex("a").is_err());
        assert!(canonical_decimal("01").is_err());
        assert!(canonical_decimal("-1").is_err());
    }

    #[test]
    fn rejects_contradictory_or_out_of_range_normalized_scalars() {
        let response = NormalizedMessage::RpcResponse(NormalizedResponse {
            status_raw: "999".to_owned(),
            status_code: 999,
            message_hex: String::new(),
            body_hex: String::new(),
            metadata: vec![],
        });
        assert!(validate_message(MessageType::RpcResponse, &response).is_err());

        let frame = NormalizedMessage::RpcStreamFrame(NormalizedStreamFrame {
            kind: "status".to_owned(),
            kind_raw: "0".to_owned(),
            status_raw: "4294967296".to_owned(),
            status_code: 0,
            message_hex: String::new(),
            body_hex: String::new(),
            metadata: vec![],
        });
        assert!(validate_message(MessageType::RpcStreamFrame, &frame).is_err());
    }

    #[test]
    fn golden_loader_requires_pairs_and_rejects_duplicates() {
        let path = std::env::temp_dir().join(format!("trevrpc-goldens-{}", std::process::id()));
        fs::write(&path, "x.body = 00\nx.frame = 0000000100\n").expect("write");
        let vectors = GoldenVectors::load(&path).expect("load");
        assert!(vectors.pair("x").is_ok());
        assert!(vectors.pair("missing").is_err());
        fs::write(&path, "x.body = 00\nx.body = 01\n").expect("rewrite");
        assert!(GoldenVectors::load(&path).is_err());
    }

    #[test]
    fn milestone_totals_adversarial_cases_and_resource_vectors_are_stable() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let m0 = LoadedSuite::load(&manifest.join("../conformance/suites/m0.json"))
            .expect("load M0 suite");
        assert_eq!(m0.case_count(), 63);
        assert_eq!(m0.case_count() * m0.suite.peers.len(), 63);

        let loaded = LoadedSuite::load(&manifest.join("../conformance/suites/m3.json"))
            .expect("load M3 suite");
        assert_eq!(loaded.case_count(), 90);
        assert_eq!(loaded.case_count() * loaded.suite.peers.len(), 540);
        assert_eq!(loaded.suite.allowance_policy, AllowancePolicy::Forbid);

        let ids = loaded
            .corpora
            .iter()
            .flat_map(|corpus| &corpus.cases)
            .map(Case::id)
            .collect::<BTreeSet<_>>();
        for required in [
            "state.server.payload_unknown_field",
            "state.client.payload_unknown_field",
            "state.server.payload_field3_wrong_wire",
            "state.client.payload_field3_wrong_wire",
            "state.server.trailing_malformed_after_status",
            "state.client.trailing_malformed_after_status",
            "state.client.two_responses_remote_status",
            "state.client.two_responses_fin",
        ] {
            assert!(
                ids.contains(required),
                "missing adversarial case {required}"
            );
        }

        let bodies = loaded
            .corpora
            .iter()
            .filter(|corpus| corpus.kind == "resource_limits")
            .flat_map(|corpus| &corpus.cases)
            .filter_map(|case| match case {
                Case::CodecDecode(case) if case.id.starts_with("rpc_request.metadata.") => Some((
                    case.id.as_str(),
                    case.body_hex.as_deref().expect("static body"),
                )),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(bodies.len(), 6);

        let cases = [
            (
                "rpc_request.metadata.entry_count_65",
                (0..65)
                    .map(|index| (format!("k{index:02}").into_bytes(), Vec::new()))
                    .collect::<Vec<_>>(),
            ),
            (
                "rpc_request.metadata.key_bytes_128",
                vec![(vec![b'a'; 128], vec![0])],
            ),
            (
                "rpc_request.metadata.key_bytes_129",
                vec![(vec![b'a'; 129], vec![0])],
            ),
            (
                "rpc_request.metadata.value_bytes_8192",
                vec![(vec![b'a'], vec![0; 8192])],
            ),
            (
                "rpc_request.metadata.value_bytes_8193",
                vec![(vec![b'a'], vec![0; 8193])],
            ),
            (
                "rpc_request.metadata.total_bytes_65537",
                (0..8)
                    .map(|index| (vec![b'a' + index], vec![0; 8191]))
                    .chain(std::iter::once((vec![b'i'], Vec::new())))
                    .collect::<Vec<_>>(),
            ),
        ];
        for (id, metadata) in cases {
            let expected = request_with_metadata(&metadata);
            assert_eq!(bodies.get(id).copied(), Some(expected.as_str()), "{id}");
        }
        assert_eq!((0..8).map(|_| 1 + 8191).sum::<usize>() + 1, 65_537);

        let largest = loaded
            .corpora
            .iter()
            .flat_map(|corpus| &corpus.cases)
            .map(|case| {
                let fields = case
                    .command_fields(&loaded.goldens)
                    .expect("command fields");
                protocol::command(540, case, &fields).len()
            })
            .max()
            .expect("cases");
        assert!(largest < loaded.suite.max_command_bytes);
    }

    fn request_with_metadata(metadata: &[(Vec<u8>, Vec<u8>)]) -> String {
        let mut body = Vec::new();
        for (key, value) in metadata {
            let mut entry = vec![0x0a];
            append_varint(&mut entry, key.len());
            entry.extend(key);
            entry.push(0x12);
            append_varint(&mut entry, value.len());
            entry.extend(value);
            body.push(0x22);
            append_varint(&mut body, entry.len());
            body.extend(entry);
        }
        body.extend([0x30, 0x01]);
        super::encode_hex(&body)
    }

    fn append_varint(output: &mut Vec<u8>, mut value: usize) {
        while value >= 0x80 {
            output.push(((value & 0x7f) as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
}
