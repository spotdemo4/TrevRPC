#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::process::ExitCode;

use prost::Message;
use serde_json::{Map, Value, json};
use trevrpc::framing::{decode_frame, encode_frame_with_max, frame_body_len};
use trevrpc::wire::validate_metadata;
use trevrpc::{
    Code, Error, Metadata, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind,
    Status,
};

#[path = "../../../src/response_state.rs"]
mod response_state;

use response_state::{ResponseState, ResponseStateEvent, ResponseStateFailure};

#[derive(Clone, PartialEq, Message)]
struct StatePayload {
    #[prost(bytes = "vec", tag = "3")]
    body: Vec<u8>,
}

const PROTOCOL_VERSION: u64 = 1;
const MAX_COMMAND_BYTES: usize = 262_144;
const MAX_EVENT_BYTES: usize = 65_536;
const DEFAULT_MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;
const PEER: &str = "rust";
const CAPABILITIES: [&str; 6] = [
    "codec.decode",
    "codec.encode",
    "framing.decode_stream",
    "framing.encode",
    "state.client_stream",
    "state.server_stream",
];

fn main() -> ExitCode {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 3 || args[1] != "--protocol" || args[2] != "1" {
        eprintln!("usage: trevrpc-conformance-rust --protocol 1");
        return ExitCode::from(2);
    }

    let stdin = io::stdin();
    let stdout = io::stdout();
    let stderr = io::stderr();
    let code = run(BufReader::new(stdin.lock()), stdout.lock(), stderr.lock());
    ExitCode::from(code)
}

fn run<R, W, E>(mut input: R, mut output: W, mut diagnostics: E) -> u8
where
    R: BufRead,
    W: Write,
    E: Write,
{
    if emit(
        &mut output,
        &json!({
            "schema_version": PROTOCOL_VERSION,
            "event": "ready",
            "peer": PEER,
            "pid": std::process::id(),
            "capabilities": CAPABILITIES,
        }),
    )
    .is_err()
    {
        let _ = writeln!(diagnostics, "failed to emit ready event");
        return 2;
    }

    loop {
        let line = match read_command_line(&mut input) {
            Ok(line) => line,
            Err(message) => return fatal(&mut output, &mut diagnostics, &message),
        };
        let command = match parse_command(&line) {
            Ok(ParsedCommand::Stop) => return 0,
            Ok(ParsedCommand::Run(command)) => command,
            Err(message) => return fatal(&mut output, &mut diagnostics, &message),
        };

        let operation = command.operation;
        let case_id = command.case_id.clone();
        let sequence = command.sequence.clone();
        let result = dispatch(command);
        let mut event = Map::new();
        event.insert("schema_version".to_owned(), json!(PROTOCOL_VERSION));
        event.insert("event".to_owned(), json!("result"));
        event.insert("peer".to_owned(), json!(PEER));
        event.insert("sequence".to_owned(), json!(sequence));
        event.insert("case_id".to_owned(), json!(case_id));
        event.insert("operation".to_owned(), json!(operation.as_str()));

        match result {
            Ok(payload) => {
                event.insert("outcome".to_owned(), json!("success"));
                event.extend(payload);
            }
            Err(failure) => {
                event.insert("outcome".to_owned(), json!("error"));
                event.insert("category".to_owned(), json!(failure.category));
                event.insert("status_code".to_owned(), json!(failure.status_code));
                event.extend(failure.payload);
                let _ = writeln!(
                    diagnostics,
                    "{} {}: {}",
                    case_id, failure.category, failure.native
                );
            }
        }

        if emit(&mut output, &Value::Object(event)).is_err() {
            let _ = writeln!(diagnostics, "failed to emit result event");
            return 2;
        }
    }
}

fn emit(output: &mut impl Write, event: &Value) -> io::Result<()> {
    let encoded = serde_json::to_vec(event)?;
    if encoded.len() > MAX_EVENT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "protocol event exceeded limit",
        ));
    }
    output.write_all(&encoded)?;
    output.write_all(b"\n")?;
    output.flush()
}

fn fatal(output: &mut impl Write, diagnostics: &mut impl Write, message: &str) -> u8 {
    let _ = emit(
        output,
        &json!({
            "schema_version": PROTOCOL_VERSION,
            "event": "fatal",
            "peer": PEER,
            "message": message,
        }),
    );
    let _ = writeln!(diagnostics, "{message}");
    2
}

fn read_command_line(input: &mut impl BufRead) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    loop {
        let available = input
            .fill_buf()
            .map_err(|error| format!("failed to read controller input: {error}"))?;
        if available.is_empty() {
            return if line.is_empty() {
                Err("controller input ended without STOP".to_owned())
            } else {
                Err("command was not LF-terminated".to_owned())
            };
        }

        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            if line.len().saturating_add(newline) > MAX_COMMAND_BYTES {
                return Err("command line exceeded limit".to_owned());
            }
            line.extend_from_slice(&available[..newline]);
            input.consume(newline + 1);
            return Ok(line);
        }

        if line.len().saturating_add(available.len()) > MAX_COMMAND_BYTES {
            return Err("command line exceeded limit".to_owned());
        }
        let consumed = available.len();
        line.extend_from_slice(available);
        input.consume(consumed);
    }
}

#[derive(Clone, Copy)]
enum Operation {
    CodecEncode,
    CodecDecode,
    FramingEncode,
    FramingDecodeStream,
    StateServerStream,
    StateClientStream,
}

impl Operation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CodecEncode => "codec.encode",
            Self::CodecDecode => "codec.decode",
            Self::FramingEncode => "framing.encode",
            Self::FramingDecodeStream => "framing.decode_stream",
            Self::StateServerStream => "state.server_stream",
            Self::StateClientStream => "state.client_stream",
        }
    }
}

enum ParsedCommand {
    Stop,
    Run(RunCommand),
}

struct RunCommand {
    sequence: String,
    case_id: String,
    operation: Operation,
    data: CommandData,
}

enum CommandData {
    Encode {
        message: MessageInput,
        max_frame_size: usize,
    },
    Decode {
        message_type: MessageType,
        body: Vec<u8>,
    },
    DecodeStream {
        message_type: MessageType,
        max_frame_size: usize,
        chunks: Vec<Vec<u8>>,
    },
    State {
        frames: Vec<Vec<u8>>,
    },
}

#[derive(Clone, Copy)]
enum MessageType {
    Request,
    Response,
    StreamFrame,
}

impl MessageType {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "rpc_request" => Ok(Self::Request),
            "rpc_response" => Ok(Self::Response),
            "rpc_stream_frame" => Ok(Self::StreamFrame),
            _ => Err(format!("unknown message type {value:?}")),
        }
    }

    const fn is_request(self) -> bool {
        matches!(self, Self::Request)
    }
}

struct MetadataInput {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

enum MessageInput {
    Request {
        service: Vec<u8>,
        method: Vec<u8>,
        body: Vec<u8>,
        metadata: MetadataInput,
        kind: RpcKind,
        version: u32,
        timeout_nanos: u64,
    },
    Response {
        status: u32,
        message: Vec<u8>,
        body: Vec<u8>,
        metadata: MetadataInput,
    },
    StreamFrame {
        kind: i32,
        status: u32,
        message: Vec<u8>,
        body: Vec<u8>,
        metadata: MetadataInput,
    },
}

fn parse_command(line: &[u8]) -> Result<ParsedCommand, String> {
    if !line.is_ascii() || line.contains(&b'\r') {
        return Err("command must be LF-terminated tab-delimited ASCII".to_owned());
    }
    let line = std::str::from_utf8(line).map_err(|_| "command must be ASCII".to_owned())?;
    if line == "STOP" {
        return Ok(ParsedCommand::Stop);
    }

    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() < 4 || fields[0] != "RUN" {
        return Err("expected RUN command".to_owned());
    }
    parse_decimal_u64(fields[1]).map_err(|error| format!("invalid sequence: {error}"))?;
    if !valid_id(fields[2]) {
        return Err("invalid case ID".to_owned());
    }

    let operation = match fields[3] {
        "codec.encode" => Operation::CodecEncode,
        "codec.decode" => Operation::CodecDecode,
        "framing.encode" => Operation::FramingEncode,
        "framing.decode_stream" => Operation::FramingDecodeStream,
        "state.server_stream" => Operation::StateServerStream,
        "state.client_stream" => Operation::StateClientStream,
        value => return Err(format!("unknown operation {value:?}")),
    };
    let mut parser = FieldParser::new(&fields[4..]);
    let data = match operation {
        Operation::CodecEncode => {
            let message_type = parser.message_type()?;
            let message = parser.message(message_type)?;
            CommandData::Encode {
                message,
                max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            }
        }
        Operation::CodecDecode => CommandData::Decode {
            message_type: parser.message_type()?,
            body: parser.hex_bytes()?,
        },
        Operation::FramingEncode => {
            let message_type = parser.message_type()?;
            let max_frame_size = parser.usize()?;
            let message = parser.message(message_type)?;
            CommandData::Encode {
                message,
                max_frame_size,
            }
        }
        Operation::FramingDecodeStream => CommandData::DecodeStream {
            message_type: parser.message_type()?,
            max_frame_size: parser.usize()?,
            chunks: parser.hex_list()?,
        },
        Operation::StateServerStream | Operation::StateClientStream => CommandData::State {
            frames: parser.hex_list()?,
        },
    };
    if !parser.is_done() {
        return Err("unexpected command fields".to_owned());
    }

    Ok(ParsedCommand::Run(RunCommand {
        sequence: fields[1].to_owned(),
        case_id: fields[2].to_owned(),
        operation,
        data,
    }))
}

struct FieldParser<'a> {
    fields: &'a [&'a str],
    next: usize,
}

impl<'a> FieldParser<'a> {
    const fn new(fields: &'a [&'a str]) -> Self {
        Self { fields, next: 0 }
    }

    fn token(&mut self) -> Result<&'a str, String> {
        let value = self
            .fields
            .get(self.next)
            .copied()
            .ok_or_else(|| "missing command field".to_owned())?;
        self.next += 1;
        Ok(value)
    }

    fn message_type(&mut self) -> Result<MessageType, String> {
        MessageType::parse(self.token()?)
    }

    fn hex_bytes(&mut self) -> Result<Vec<u8>, String> {
        decode_lower_hex(self.token()?)
    }

    fn usize(&mut self) -> Result<usize, String> {
        let value = parse_decimal_u64(self.token()?)?;
        usize::try_from(value).map_err(|_| "integer exceeded supported range".to_owned())
    }

    fn u32(&mut self) -> Result<u32, String> {
        let value = parse_decimal_u64(self.token()?)?;
        u32::try_from(value).map_err(|_| "integer exceeded uint32 range".to_owned())
    }

    fn hex_list(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let count = self.usize()?;
        if count > self.remaining() {
            return Err("list count exceeded remaining command fields".to_owned());
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.hex_bytes()?);
        }
        Ok(values)
    }

    fn metadata(&mut self) -> Result<MetadataInput, String> {
        let count = self.usize()?;
        if count > self.remaining() / 2 {
            return Err("metadata count exceeded remaining command fields".to_owned());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push((self.hex_bytes()?, self.hex_bytes()?));
        }
        if entries
            .windows(2)
            .any(|pair| pair[0].0.as_slice() >= pair[1].0.as_slice())
        {
            return Err("metadata entries must be key-sorted and unique".to_owned());
        }
        Ok(MetadataInput { entries })
    }

    fn message(&mut self, message_type: MessageType) -> Result<MessageInput, String> {
        match message_type {
            MessageType::Request => {
                let service = self.hex_bytes()?;
                let method = self.hex_bytes()?;
                let body = self.hex_bytes()?;
                let metadata = self.metadata()?;
                let kind = match self.token()? {
                    "unary" => RpcKind::Unary,
                    "client_stream" => RpcKind::ClientStreaming,
                    "server_stream" => RpcKind::ServerStreaming,
                    "bidi" => RpcKind::BidirectionalStreaming,
                    value => return Err(format!("unknown RPC kind {value:?}")),
                };
                let version = self.u32()?;
                let timeout_nanos = parse_decimal_u64(self.token()?)?;
                Ok(MessageInput::Request {
                    service,
                    method,
                    body,
                    metadata,
                    kind,
                    version,
                    timeout_nanos,
                })
            }
            MessageType::Response => Ok(MessageInput::Response {
                status: self.u32()?,
                message: self.hex_bytes()?,
                body: self.hex_bytes()?,
                metadata: self.metadata()?,
            }),
            MessageType::StreamFrame => {
                let kind = self.u32()?.cast_signed();
                Ok(MessageInput::StreamFrame {
                    kind,
                    status: self.u32()?,
                    message: self.hex_bytes()?,
                    body: self.hex_bytes()?,
                    metadata: self.metadata()?,
                })
            }
        }
    }

    const fn remaining(&self) -> usize {
        self.fields.len() - self.next
    }

    const fn is_done(&self) -> bool {
        self.next == self.fields.len()
    }
}

fn parse_decimal_u64(value: &str) -> Result<u64, String> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return Err("non-canonical decimal".to_owned());
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("non-decimal value".to_owned());
    }
    value
        .parse::<u64>()
        .map_err(|_| "decimal exceeded uint64 range".to_owned())
}

fn decode_lower_hex(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex value has odd length".to_owned());
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("hex value must be lowercase".to_owned());
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    for index in (0..value.len()).step_by(2) {
        decoded.push(
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| "invalid hex value".to_owned())?,
        );
    }
    Ok(decoded)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

#[derive(Debug)]
struct ConformanceError {
    category: &'static str,
    status_code: u32,
    native: String,
    payload: Map<String, Value>,
}

impl ConformanceError {
    fn new(category: &'static str, status_code: u32, native: impl Into<String>) -> Self {
        Self {
            category,
            status_code,
            native: native.into(),
            payload: Map::new(),
        }
    }

    fn with_payload(mut self, key: &str, value: Value) -> Self {
        self.payload.insert(key.to_owned(), value);
        self
    }
}

fn dispatch(command: RunCommand) -> Result<Map<String, Value>, ConformanceError> {
    match (command.operation, command.data) {
        (
            Operation::CodecEncode | Operation::FramingEncode,
            CommandData::Encode {
                message,
                max_frame_size,
            },
        ) => codec_encode(message, max_frame_size),
        (Operation::CodecDecode, CommandData::Decode { message_type, body }) => {
            codec_decode(message_type, &body)
        }
        (
            Operation::FramingDecodeStream,
            CommandData::DecodeStream {
                message_type,
                max_frame_size,
                chunks,
            },
        ) => framing_decode_stream(message_type, max_frame_size, chunks),
        (Operation::StateServerStream, CommandData::State { frames }) => run_server_state(frames),
        (Operation::StateClientStream, CommandData::State { frames }) => run_client_state(frames),
        _ => Err(ConformanceError::new(
            "malformed_protobuf",
            Code::Internal.as_u32(),
            "command operation/data mismatch",
        )),
    }
}

fn codec_encode(
    input: MessageInput,
    max_frame_size: usize,
) -> Result<Map<String, Value>, ConformanceError> {
    match input {
        MessageInput::Request {
            service,
            method,
            body,
            metadata,
            kind,
            version,
            timeout_nanos,
        } => {
            let message = RpcRequest {
                service: decode_text(service, MessageType::Request)?,
                method: decode_text(method, MessageType::Request)?,
                body,
                metadata: build_metadata(metadata, MessageType::Request)?,
                kind: kind as i32,
                version,
                timeout_nanos,
            };
            validate_request(&message)?;
            encode_message(&message, max_frame_size)
        }
        MessageInput::Response {
            status,
            message,
            body,
            metadata,
        } => {
            let message = RpcResponse {
                status,
                message: decode_text(message, MessageType::Response)?,
                body,
                metadata: build_metadata(metadata, MessageType::Response)?,
            };
            validate_response(&message)?;
            encode_message(&message, max_frame_size)
        }
        MessageInput::StreamFrame {
            kind,
            status,
            message,
            body,
            metadata,
        } => {
            let message = RpcStreamFrame {
                kind,
                status,
                message: decode_text(message, MessageType::StreamFrame)?,
                body,
                metadata: build_metadata(metadata, MessageType::StreamFrame)?,
            };
            validate_stream_frame(&message)?;
            encode_message(&message, max_frame_size)
        }
    }
}

fn encode_message<M: Message>(
    message: &M,
    max_frame_size: usize,
) -> Result<Map<String, Value>, ConformanceError> {
    let body = message.encode_to_vec();
    let frame = encode_frame_with_max(message, max_frame_size)
        .map_err(|error| classify_frame_error(&error))?;
    Ok(map([
        ("body_hex", json!(encode_hex(&body))),
        ("frame_hex", json!(encode_hex(&frame))),
    ]))
}

fn codec_decode(
    message_type: MessageType,
    body: &[u8],
) -> Result<Map<String, Value>, ConformanceError> {
    match message_type {
        MessageType::Request => {
            let message = decode_frame::<RpcRequest>(body)
                .map_err(|error| malformed_error(message_type, error.to_string()))?;
            validate_request(&message)?;
            Ok(normalized_decode(&message, normalize_request(&message)))
        }
        MessageType::Response => {
            let message = decode_frame::<RpcResponse>(body)
                .map_err(|error| malformed_error(message_type, error.to_string()))?;
            validate_response(&message)?;
            Ok(normalized_decode(&message, normalize_response(&message)))
        }
        MessageType::StreamFrame => {
            let message = decode_frame::<RpcStreamFrame>(body)
                .map_err(|error| malformed_error(message_type, error.to_string()))?;
            validate_stream_frame(&message)?;
            Ok(normalized_decode(
                &message,
                normalize_stream_frame(&message),
            ))
        }
    }
}

fn normalized_decode<M: Message>(message: &M, normalized: Value) -> Map<String, Value> {
    map([
        ("message", normalized),
        (
            "canonical_body_hex",
            json!(encode_hex(&message.encode_to_vec())),
        ),
    ])
}

fn validate_request(message: &RpcRequest) -> Result<(), ConformanceError> {
    if let Err(status) = message.validate_protocol() {
        let category = if status.code() == Code::FailedPrecondition {
            "unsupported_wire_version"
        } else {
            "unsupported_rpc_kind"
        };
        return Err(ConformanceError::new(
            category,
            status.code().as_u32(),
            status.to_string(),
        ));
    }
    validate_message_metadata(&message.metadata, MessageType::Request)
}

fn validate_response(message: &RpcResponse) -> Result<(), ConformanceError> {
    validate_message_metadata(&message.metadata, MessageType::Response)
}

fn validate_stream_frame(message: &RpcStreamFrame) -> Result<(), ConformanceError> {
    if message.frame_kind().is_none() {
        return Err(ConformanceError::new(
            "unsupported_frame_kind",
            Code::InvalidArgument.as_u32(),
            "unsupported stream frame kind",
        ));
    }
    validate_message_metadata(&message.metadata, MessageType::StreamFrame)
}

fn validate_message_metadata(
    metadata: &Metadata,
    message_type: MessageType,
) -> Result<(), ConformanceError> {
    validate_metadata(metadata).map_err(|status| {
        ConformanceError::new(
            "invalid_metadata",
            if message_type.is_request() {
                Code::InvalidArgument.as_u32()
            } else {
                Code::Internal.as_u32()
            },
            status.to_string(),
        )
    })
}

fn build_metadata(
    input: MetadataInput,
    message_type: MessageType,
) -> Result<Metadata, ConformanceError> {
    let mut metadata = HashMap::with_capacity(input.entries.len());
    for (key, value) in input.entries {
        let key = String::from_utf8(key).map_err(|error| {
            ConformanceError::new(
                "invalid_metadata",
                if message_type.is_request() {
                    Code::InvalidArgument.as_u32()
                } else {
                    Code::Internal.as_u32()
                },
                error.to_string(),
            )
        })?;
        metadata.insert(key, value);
    }
    Ok(metadata)
}

fn decode_text(bytes: Vec<u8>, message_type: MessageType) -> Result<String, ConformanceError> {
    String::from_utf8(bytes).map_err(|error| malformed_error(message_type, error.to_string()))
}

fn malformed_error(message_type: MessageType, native: impl Into<String>) -> ConformanceError {
    ConformanceError::new(
        "malformed_protobuf",
        if message_type.is_request() {
            Code::InvalidArgument.as_u32()
        } else {
            Code::Internal.as_u32()
        },
        native,
    )
}

fn classify_frame_error(error: &Error) -> ConformanceError {
    match error {
        Error::FrameTooLarge { .. } => ConformanceError::new(
            "frame_too_large",
            Code::ResourceExhausted.as_u32(),
            error.to_string(),
        ),
        _ => malformed_error(MessageType::Response, error.to_string()),
    }
}

fn normalize_request(message: &RpcRequest) -> Value {
    json!({
        "type": "rpc_request",
        "service_hex": encode_hex(message.service.as_bytes()),
        "method_hex": encode_hex(message.method.as_bytes()),
        "body_hex": encode_hex(&message.body),
        "metadata": normalize_metadata(&message.metadata),
        "kind": match message.rpc_kind() {
            RpcKind::Unary => "unary",
            RpcKind::ClientStreaming => "client_stream",
            RpcKind::ServerStreaming => "server_stream",
            RpcKind::BidirectionalStreaming => "bidi",
        },
        "version": message.version.to_string(),
        "timeout_nanos": message.timeout_nanos.to_string(),
    })
}

fn normalize_response(message: &RpcResponse) -> Value {
    json!({
        "type": "rpc_response",
        "status_raw": message.status.to_string(),
        "status_code": Code::from_u32(message.status).as_u32(),
        "message_hex": encode_hex(message.message.as_bytes()),
        "body_hex": encode_hex(&message.body),
        "metadata": normalize_metadata(&message.metadata),
    })
}

fn normalize_stream_frame(message: &RpcStreamFrame) -> Value {
    let kind = match message.frame_kind() {
        Some(RpcStreamFrameKind::Message) => "message",
        Some(RpcStreamFrameKind::Status) => "status",
        None => "",
    };
    json!({
        "type": "rpc_stream_frame",
        "kind": kind,
        "kind_raw": i32_raw_u32(message.kind).to_string(),
        "status_raw": message.status.to_string(),
        "status_code": Code::from_u32(message.status).as_u32(),
        "message_hex": encode_hex(message.message.as_bytes()),
        "body_hex": encode_hex(&message.body),
        "metadata": normalize_metadata(&message.metadata),
    })
}

fn normalize_metadata(metadata: &Metadata) -> Value {
    let mut entries = metadata.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    Value::Array(
        entries
            .into_iter()
            .map(|(key, value)| {
                json!({
                    "key_hex": encode_hex(key.as_bytes()),
                    "value_hex": encode_hex(value),
                })
            })
            .collect(),
    )
}

const fn i32_raw_u32(value: i32) -> u32 {
    u32::from_ne_bytes(value.to_ne_bytes())
}

fn framing_decode_stream(
    _message_type: MessageType,
    max_frame_size: usize,
    chunks: Vec<Vec<u8>>,
) -> Result<Map<String, Value>, ConformanceError> {
    let mut reader = ChunkReader::new(chunks);
    let mut bodies = Vec::new();
    loop {
        let mut header = [0_u8; 4];
        if !read_exact_or_clean_eof(&mut reader, &mut header)? {
            return Ok(map([("bodies_hex", json!(bodies)), ("eof", json!(true))]));
        }
        let body_len =
            frame_body_len(header, max_frame_size).map_err(|error| classify_frame_error(&error))?;
        let mut body = vec![0_u8; body_len];
        read_exact_required(&mut reader, &mut body)?;
        bodies.push(encode_hex(&body));
    }
}

struct ChunkReader {
    chunks: Vec<Vec<u8>>,
    chunk: usize,
    offset: usize,
}

impl ChunkReader {
    const fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks,
            chunk: 0,
            offset: 0,
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        while let Some(current) = self.chunks.get(self.chunk) {
            if self.offset == current.len() {
                self.chunk += 1;
                self.offset = 0;
                continue;
            }
            let read = destination.len().min(current.len() - self.offset);
            destination[..read].copy_from_slice(&current[self.offset..self.offset + read]);
            self.offset += read;
            return Ok(read);
        }
        Ok(0)
    }
}

fn read_exact_or_clean_eof(
    reader: &mut impl Read,
    destination: &mut [u8],
) -> Result<bool, ConformanceError> {
    let mut offset = 0;
    while offset < destination.len() {
        let read = reader.read(&mut destination[offset..]).map_err(|error| {
            ConformanceError::new(
                "incomplete_frame",
                Code::Internal.as_u32(),
                error.to_string(),
            )
        })?;
        if read == 0 {
            if offset == 0 {
                return Ok(false);
            }
            return Err(ConformanceError::new(
                "incomplete_frame",
                Code::Internal.as_u32(),
                "stream ended in the middle of a frame header",
            ));
        }
        offset += read;
    }
    Ok(true)
}

fn read_exact_required(
    reader: &mut impl Read,
    destination: &mut [u8],
) -> Result<(), ConformanceError> {
    let mut offset = 0;
    while offset < destination.len() {
        let read = reader.read(&mut destination[offset..]).map_err(|error| {
            ConformanceError::new(
                "incomplete_frame",
                Code::Internal.as_u32(),
                error.to_string(),
            )
        })?;
        if read == 0 {
            return Err(ConformanceError::new(
                "incomplete_frame",
                Code::Internal.as_u32(),
                "stream ended in the middle of a frame body",
            ));
        }
        offset += read;
    }
    Ok(())
}

fn run_server_state(frames: Vec<Vec<u8>>) -> Result<Map<String, Value>, ConformanceError> {
    let mut transport = ScriptedTransport::default();
    let outcome = run_server_state_inner(frames);
    transport.close();
    match outcome {
        Ok(mut payload) => {
            payload.insert(
                "transport_close_count".to_owned(),
                json!(transport.close_count.to_string()),
            );
            Ok(payload)
        }
        Err(failure) => Err(failure.with_payload(
            "transport_close_count",
            json!(transport.close_count.to_string()),
        )),
    }
}

fn run_server_state_inner(frames: Vec<Vec<u8>>) -> Result<Map<String, Value>, ConformanceError> {
    let mut state = ResponseState::<StatePayload>::default();
    let mut events = Vec::new();
    for body in frames {
        state
            .ensure_open()
            .map_err(|failure| classify_state_failure(&failure))?;
        let frame = decode_frame::<RpcStreamFrame>(&body)
            .map_err(|error| malformed_error(MessageType::StreamFrame, error.to_string()))?;
        match state
            .accept(&frame)
            .map_err(|failure| classify_state_failure(&failure))?
        {
            ResponseStateEvent::Message(message) => events.push(json!({
                "event": "message",
                "body_hex": encode_hex(&message.encode_to_vec()),
            })),
            ResponseStateEvent::Terminal => {}
        }
    }
    state
        .finish()
        .map_err(|failure| classify_state_failure(&failure))?;
    events.push(json!({"event": "eof"}));
    events.push(json!({"event": "eof"}));

    let mut payload = map([("events", Value::Array(events))]);
    if let Some(status) = state.terminal_status() {
        payload.insert("terminal_status".to_owned(), normalize_status(status));
    }
    Ok(payload)
}

fn run_client_state(frames: Vec<Vec<u8>>) -> Result<Map<String, Value>, ConformanceError> {
    let mut state = ResponseState::<StatePayload>::default();
    let mut responses = Vec::new();
    for body in frames {
        state
            .ensure_open()
            .map_err(|failure| classify_state_failure(&failure))?;
        let frame = decode_frame::<RpcStreamFrame>(&body)
            .map_err(|error| malformed_error(MessageType::StreamFrame, error.to_string()))?;
        match state
            .accept(&frame)
            .map_err(|failure| classify_state_failure(&failure))?
        {
            ResponseStateEvent::Message(message) => responses.push(message),
            ResponseStateEvent::Terminal => {}
        }
    }
    state
        .finish()
        .map_err(|failure| classify_state_failure(&failure))?;

    if responses.len() != 1 {
        return Err(ConformanceError::new(
            "response_cardinality",
            Code::Internal.as_u32(),
            format!(
                "client-streaming RPC returned {} response messages; expected exactly one",
                responses.len()
            ),
        ));
    }
    let response = responses.pop().expect("one response was checked");
    Ok(map([(
        "response_body_hex",
        json!(encode_hex(&response.encode_to_vec())),
    )]))
}

#[derive(Default)]
struct ScriptedTransport {
    closed: bool,
    close_count: usize,
}

impl ScriptedTransport {
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.close_count += 1;
        }
    }
}

fn classify_state_failure(failure: &ResponseStateFailure) -> ConformanceError {
    ConformanceError::new(
        failure.kind().category(),
        failure.status().code().as_u32(),
        failure.status().to_string(),
    )
}

fn normalize_status(status: &Status) -> Value {
    json!({
        "status_raw": status.code().as_u32().to_string(),
        "status_code": status.code().as_u32(),
        "message_hex": encode_hex(status.message().as_bytes()),
        "metadata": normalize_metadata(status.metadata()),
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map<const N: usize>(entries: [(&str, Value); N]) -> Map<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::{
        MAX_COMMAND_BYTES, ParsedCommand, parse_command, read_command_line, run, run_client_state,
        run_server_state,
    };

    #[test]
    fn command_parser_is_strict_and_counts_are_bounded() {
        assert!(matches!(
            parse_command(b"RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001"),
            Ok(ParsedCommand::Run(_))
        ));
        for invalid in [
            &b"RUN\t01\tcase.id\tcodec.decode\trpc_request\t3001"[..],
            &b"RUN\t1\tCase\tcodec.decode\trpc_request\t3001"[..],
            &b"RUN\t1\tcase.id\tcodec.decode\trpc_request\tABC0"[..],
            &b"RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001\textra"[..],
            &b"RUN\t1\thuge\tstate.server_stream\t18446744073709551615"[..],
        ] {
            assert!(parse_command(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn command_reader_enforces_exact_byte_cap() {
        let exact = vec![b'a'; MAX_COMMAND_BYTES];
        let mut input = exact.clone();
        input.push(b'\n');
        let mut reader = BufReader::new(Cursor::new(input));
        assert_eq!(
            read_command_line(&mut reader).unwrap().len(),
            MAX_COMMAND_BYTES
        );

        let mut oversized = exact;
        oversized.extend_from_slice(b"a\n");
        let mut reader = BufReader::new(Cursor::new(oversized));
        assert_eq!(
            read_command_line(&mut reader).unwrap_err(),
            "command line exceeded limit"
        );
    }

    #[test]
    fn process_emits_fatal_and_clean_stop_is_silent() {
        let mut output = Vec::new();
        let mut diagnostics = Vec::new();
        assert_eq!(
            run(
                BufReader::new(Cursor::new(b"STOP\n")),
                &mut output,
                &mut diagnostics
            ),
            0
        );
        let stdout = String::from_utf8(output).unwrap();
        assert_eq!(stdout.lines().count(), 1);
        assert!(stdout.contains("\"event\":\"ready\""));
        assert!(diagnostics.is_empty());

        let mut output = Vec::new();
        assert_eq!(
            run(
                BufReader::new(Cursor::new(b"RUN\t01\tbad\tcodec.decode\trpc_request\t\n")),
                &mut output,
                Vec::new()
            ),
            2
        );
        let stdout = String::from_utf8(output).unwrap();
        assert_eq!(stdout.lines().count(), 2);
        assert!(stdout.contains("\"event\":\"fatal\""));
    }

    #[test]
    fn state_payload_accepts_and_omits_unknown_fields() {
        let message = hex("220808011a0161220178");

        let server = run_server_state(vec![message.clone(), hex("0801")]).unwrap();
        assert_eq!(server["events"][0]["body_hex"], "1a0161");

        let client = run_client_state(vec![message, hex("0801")]).unwrap();
        assert_eq!(client["response_body_hex"], "1a0161");
    }

    #[test]
    fn state_payload_rejects_field_three_with_wrong_wire_type() {
        let server = run_server_state(vec![hex("22021801"), hex("0801")]).unwrap_err();
        assert_eq!(server.category, "malformed_protobuf");
        assert_eq!(server.status_code, 13);

        let client = run_client_state(vec![hex("22021801"), hex("0801")]).unwrap_err();
        assert_eq!(client.category, "malformed_protobuf");
        assert_eq!(client.status_code, 13);
    }

    #[test]
    fn state_precedence_and_close_once_are_stable() {
        let remote = run_client_state(vec![hex("0801100e1a04646f776e")]).unwrap_err();
        assert_eq!(remote.category, "remote_status");
        assert_eq!(remote.status_code, 14);

        let trailing =
            run_client_state(vec![hex("22031a0161"), hex("0801"), hex("22031a0162")]).unwrap_err();
        assert_eq!(trailing.category, "trailing_frame");
        let malformed_trailing = run_client_state(vec![hex("0801"), hex("ff")]).unwrap_err();
        assert_eq!(malformed_trailing.category, "trailing_frame");

        let missing = run_client_state(Vec::new()).unwrap_err();
        assert_eq!(missing.category, "missing_terminal_status");
        let message_then_missing = run_client_state(vec![hex("22031a0161")]).unwrap_err();
        assert_eq!(message_then_missing.category, "missing_terminal_status");

        let no_response = run_client_state(vec![hex("0801")]).unwrap_err();
        assert_eq!(no_response.category, "response_cardinality");
        let two_responses =
            run_client_state(vec![hex("22031a0161"), hex("22031a0162"), hex("0801")]).unwrap_err();
        assert_eq!(two_responses.category, "response_cardinality");

        let server = run_server_state(vec![hex("22031a0161"), hex("0801")]).unwrap();
        assert_eq!(server["transport_close_count"], "1");
        assert_eq!(server["events"].as_array().unwrap().len(), 3);
    }

    fn hex(value: &str) -> Vec<u8> {
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
            .collect()
    }
}
