use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::BoxError;
use crate::conformance::SCHEMA_VERSION;
use crate::conformance::corpus::{
    Case, NormalizedMessage, NormalizedStatus, StateEvent, canonical_decimal, decode_hex,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ready {
    pub schema_version: u32,
    pub event: String,
    pub peer: String,
    pub pid: u32,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fatal {
    schema_version: u32,
    event: String,
    peer: String,
    message: String,
}

#[derive(Debug)]
pub struct ActualResult {
    pub payload: Value,
}

pub fn parse_ready(line: &str, peer: &str, required: &[String]) -> Result<Ready, BoxError> {
    let ready: Ready = serde_json::from_str(line)?;
    if ready.schema_version != SCHEMA_VERSION
        || ready.event != "ready"
        || ready.peer != peer
        || ready.pid == 0
    {
        return Err(format!("invalid ready event from peer {peer}").into());
    }
    let mut sorted = ready.capabilities.clone();
    sorted.sort();
    let unique = sorted.iter().collect::<BTreeSet<_>>();
    if sorted != ready.capabilities
        || unique.len() != sorted.len()
        || ready.capabilities != required
    {
        return Err(
            format!("peer {peer} capabilities do not exactly match suite requirements").into(),
        );
    }
    Ok(ready)
}

#[must_use]
pub fn command(sequence: u64, case: &Case, fields: &[String]) -> String {
    let mut command = vec![
        "RUN".to_owned(),
        sequence.to_string(),
        case.id().to_owned(),
        case.operation().to_owned(),
    ];
    command.extend(fields.iter().cloned());
    command.join("\t")
}

pub fn parse_result(
    line: &str,
    peer: &str,
    sequence: u64,
    case: &Case,
) -> Result<ActualResult, BoxError> {
    let value: Value = serde_json::from_str(line)?;
    let object = value
        .as_object()
        .ok_or("result event must be a JSON object")?;
    validate_result_envelope(object, peer, sequence, case)?;
    let outcome = object
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or("result outcome must be a string")?;
    let payload_keys = match outcome {
        "error" => error_payload_keys(object, case)?,
        "success" => success_payload_keys(object, case)?,
        _ => return Err(format!("unknown result outcome {outcome:?}").into()),
    };
    validate_result_fields(object, payload_keys)?;

    let mut payload = Map::new();
    payload.insert("outcome".to_owned(), Value::String(outcome.to_owned()));
    for key in payload_keys {
        payload.insert(
            (*key).to_owned(),
            object.get(*key).ok_or("missing result payload")?.clone(),
        );
    }
    Ok(ActualResult {
        payload: Value::Object(payload),
    })
}

fn validate_result_envelope(
    object: &Map<String, Value>,
    peer: &str,
    sequence: u64,
    case: &Case,
) -> Result<(), BoxError> {
    require_number(object, "schema_version", u64::from(SCHEMA_VERSION))?;
    require_string(object, "event", "result")?;
    require_string(object, "peer", peer)?;
    let sequence_value = object
        .get("sequence")
        .and_then(Value::as_str)
        .ok_or("result sequence must be a decimal string")?;
    if canonical_decimal(sequence_value)? != sequence {
        return Err(
            format!("result sequence mismatch: expected {sequence}, got {sequence_value}").into(),
        );
    }
    require_string(object, "case_id", case.id())?;
    require_string(object, "operation", case.operation())?;
    Ok(())
}

fn error_payload_keys(
    object: &Map<String, Value>,
    case: &Case,
) -> Result<&'static [&'static str], BoxError> {
    let category = object
        .get("category")
        .and_then(Value::as_str)
        .ok_or("error result category must be a string")?;
    if !matches!(
        category,
        "malformed_protobuf"
            | "invalid_metadata"
            | "unsupported_wire_version"
            | "unsupported_rpc_kind"
            | "unsupported_frame_kind"
            | "frame_too_large"
            | "incomplete_frame"
            | "remote_status"
            | "missing_terminal_status"
            | "response_cardinality"
            | "trailing_frame"
    ) {
        return Err(format!("unknown error category {category:?}").into());
    }
    object
        .get("status_code")
        .and_then(Value::as_u64)
        .ok_or("error status_code must be numeric")?;
    if case.operation() == "state.server_stream" {
        require_decimal(object, "transport_close_count")?;
        Ok(&["category", "status_code", "transport_close_count"])
    } else {
        Ok(&["category", "status_code"])
    }
}

fn success_payload_keys(
    object: &Map<String, Value>,
    case: &Case,
) -> Result<&'static [&'static str], BoxError> {
    match case.operation() {
        "codec.encode" | "framing.encode" => {
            require_hex(object, "body_hex")?;
            require_hex(object, "frame_hex")?;
            Ok(&["body_hex", "frame_hex"])
        }
        "codec.decode" => {
            let message = object
                .get("message")
                .ok_or("codec decode message is missing")?;
            let _: NormalizedMessage = serde_json::from_value(message.clone())?;
            require_hex(object, "canonical_body_hex")?;
            Ok(&["message", "canonical_body_hex"])
        }
        "framing.decode_stream" => validate_framing_result(object),
        "state.server_stream" => validate_server_state_result(object),
        "state.client_stream" => {
            require_hex(object, "response_body_hex")?;
            Ok(&["response_body_hex"])
        }
        _ => Err("unknown case operation".into()),
    }
}

fn validate_framing_result(
    object: &Map<String, Value>,
) -> Result<&'static [&'static str], BoxError> {
    let bodies = object
        .get("bodies_hex")
        .and_then(Value::as_array)
        .ok_or("bodies_hex must be an array")?;
    for body in bodies {
        decode_hex(body.as_str().ok_or("frame body must be hex string")?)?;
    }
    object
        .get("eof")
        .and_then(Value::as_bool)
        .ok_or("eof must be a boolean")?;
    Ok(&["bodies_hex", "eof"])
}

fn validate_server_state_result(
    object: &Map<String, Value>,
) -> Result<&'static [&'static str], BoxError> {
    let events = object.get("events").ok_or("state events are missing")?;
    let _: Vec<StateEvent> = serde_json::from_value(events.clone())?;
    require_decimal(object, "transport_close_count")?;
    if let Some(status) = object.get("terminal_status") {
        let _: NormalizedStatus = serde_json::from_value(status.clone())?;
        Ok(&["events", "terminal_status", "transport_close_count"])
    } else {
        Ok(&["events", "transport_close_count"])
    }
}

fn validate_result_fields(
    object: &Map<String, Value>,
    payload_keys: &[&str],
) -> Result<(), BoxError> {
    let common = [
        "schema_version",
        "event",
        "peer",
        "sequence",
        "case_id",
        "operation",
        "outcome",
    ];
    let allowed = common
        .into_iter()
        .chain(payload_keys.iter().copied())
        .collect::<BTreeSet<_>>();
    for key in object.keys() {
        if !allowed.contains(key.as_str()) {
            return Err(format!("unknown result field {key:?}").into());
        }
    }
    if object.len() != allowed.len() {
        return Err("result is missing required fields".into());
    }
    Ok(())
}

fn require_decimal(object: &Map<String, Value>, key: &str) -> Result<(), BoxError> {
    canonical_decimal(
        object
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{key} must be a decimal string"))?,
    )?;
    Ok(())
}

pub fn parse_fatal(line: &str, peer: &str) -> Result<bool, BoxError> {
    let value: Value = serde_json::from_str(line)?;
    if value.get("event").and_then(Value::as_str) != Some("fatal") {
        return Ok(false);
    }
    let fatal: Fatal = serde_json::from_value(value)?;
    if fatal.schema_version != SCHEMA_VERSION
        || fatal.event != "fatal"
        || fatal.peer != peer
        || fatal.message.is_empty()
    {
        return Err(format!("invalid fatal event from peer {peer}").into());
    }
    Ok(true)
}

fn require_string(object: &Map<String, Value>, key: &str, expected: &str) -> Result<(), BoxError> {
    let actual = object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{key} must be a string"))?;
    if actual != expected {
        return Err(format!("{key} mismatch: expected {expected:?}, got {actual:?}").into());
    }
    Ok(())
}

fn require_number(object: &Map<String, Value>, key: &str, expected: u64) -> Result<(), BoxError> {
    let actual = object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{key} must be numeric"))?;
    if actual != expected {
        return Err(format!("{key} mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn require_hex(object: &Map<String, Value>, key: &str) -> Result<(), BoxError> {
    decode_hex(
        object
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{key} must be a hex string"))?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_fatal, parse_ready};

    #[test]
    fn ready_rejects_unknown_fields() {
        let line = r#"{"schema_version":1,"event":"ready","peer":"go","pid":1,"capabilities":[],"extra":true}"#;
        assert!(parse_ready(line, "go", &[]).is_err());
    }

    #[test]
    fn fatal_is_strict_and_peer_bound() {
        let valid = r#"{"schema_version":1,"event":"fatal","peer":"go","message":"bad command"}"#;
        assert!(parse_fatal(valid, "go").expect("valid fatal"));
        assert!(parse_fatal(valid, "js").is_err());
        let extra =
            r#"{"schema_version":1,"event":"fatal","peer":"go","message":"bad","extra":true}"#;
        assert!(parse_fatal(extra, "go").is_err());
        assert!(!parse_fatal(r#"{"event":"result"}"#, "go").expect("not fatal"));
    }
}
