import { clientStreaming, serverStreaming } from "../src/client.js";
import {
  DefaultMaxFrameSize,
  FrameReader,
  encodeFrame,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import { validateMetadata } from "../src/metadata.js";
import { Code, FrameTooLargeError, TrevRpcError, codeFromNumber } from "../src/status.js";
import {
  RpcKind,
  RpcRequest,
  RpcResponse,
  RpcStreamFrame,
  RpcStreamFrameKind,
  WireValidationError,
  createRoot,
  validateWireMessage,
} from "../src/wire.js";
import { ScriptedTransport } from "./scripted-transport.js";

const textDecoder = new TextDecoder("utf-8", { fatal: true });
const textEncoder = new TextEncoder();
const messageTypes = new Map([
  ["rpc_request", RpcRequest],
  ["rpc_response", RpcResponse],
  ["rpc_stream_frame", RpcStreamFrame],
]);
const requestKinds = new Map([
  ["unary", RpcKind.Unary],
  ["client_stream", RpcKind.ClientStreaming],
  ["server_stream", RpcKind.ServerStreaming],
  ["bidi", RpcKind.BidirectionalStreaming],
]);
const stateRoot = createRoot({
  nested: {
    trevrpc: {
      nested: {
        conformance: {
          nested: {
            StatePayload: {
              fields: {
                body: { type: "bytes", id: 3 },
              },
            },
          },
        },
      },
    },
  },
});
const StatePayload = stateRoot.lookupType("trevrpc.conformance.StatePayload");

export async function dispatch(command) {
  try {
    return { payload: await runOperation(command), error: null };
  } catch (error) {
    return { payload: operationErrorPayload(command, error), error };
  }
}

async function runOperation(command) {
  switch (command.operation) {
    case "codec.encode":
      return encodeOperation(command, DefaultMaxFrameSize);
    case "codec.decode":
      return decodeOperation(command);
    case "framing.encode":
      return encodeOperation(command, command.maxFrameSize);
    case "framing.decode_stream":
      return await decodeFraming(command);
    case "state.server_stream":
      return await runServerState(command.frames);
    case "state.client_stream":
      return await runClientState(command.frames);
    default:
      throw new Error(`unsupported operation ${command.operation}`);
  }
}

function encodeOperation(command, maxFrameSize) {
  const messageType = messageTypes.get(command.messageType);
  const message = materializeMessage(command.messageType, command.message);
  validateMessage(command.messageType, messageType, message);
  const body = marshalMessage(messageType, message);
  const frame = encodeFrame(messageType, message, maxFrameSize);
  return { body_hex: bytesToHex(body), frame_hex: bytesToHex(frame) };
}

function decodeOperation(command) {
  const messageType = messageTypes.get(command.messageType);
  const message = unmarshalMessage(messageType, command.body);
  validateMessage(command.messageType, messageType, message);
  return {
    message: normalizeMessage(command.messageType, message),
    canonical_body_hex: bytesToHex(marshalMessage(messageType, message)),
  };
}

async function decodeFraming(command) {
  const reader = new FrameReader(new ChunkReader(command.chunks));
  const bodies = [];
  for (;;) {
    const body = await reader.readRawFrameBodyOrEOF(command.maxFrameSize);
    if (body == null) {
      return { bodies_hex: bodies, eof: true };
    }
    bodies.push(bytesToHex(body));
  }
}

async function runServerState(frames) {
  const transport = new ScriptedTransport(frames);
  try {
    const stream = await serverStreaming(
      transport,
      "conformance",
      "server",
      StatePayload,
      StatePayload,
      {},
      { streamIdleTimeoutMs: -1 },
    );
    const iterator = stream[Symbol.asyncIterator]();
    const events = [];
    for (;;) {
      const result = await iterator.next();
      if (result.done) {
        events.push({ event: "eof" });
        break;
      }
      events.push({
        event: "message",
        body_hex: bytesToHex(marshalMessage(StatePayload, result.value)),
      });
    }
    const stable = await iterator.next();
    if (!stable.done) {
      throw new Error("terminal receive did not remain at EOF");
    }
    events.push({ event: "eof" });
    const status = await stream.status;
    return {
      events,
      terminal_status: normalizeTerminalStatus(status),
      transport_close_count: String(transport.closeCount),
    };
  } catch (error) {
    throw new StateOperationError(error, transport.closeCount);
  }
}

async function runClientState(frames) {
  const transport = new ScriptedTransport(frames);
  try {
    const call = await clientStreaming(
      transport,
      "conformance",
      "client",
      StatePayload,
      StatePayload,
      { streamIdleTimeoutMs: -1 },
    );
    const response = await call.closeAndRecv();
    return { response_body_hex: bytesToHex(marshalMessage(StatePayload, response)) };
  } catch (error) {
    throw new StateOperationError(error, transport.closeCount);
  }
}

function materializeMessage(messageType, message) {
  switch (messageType) {
    case "rpc_request":
      return {
        service: decodeText(message.service, "request service", Code.InvalidArgument),
        method: decodeText(message.method, "request method", Code.InvalidArgument),
        body: message.body,
        metadata: metadataObject(message.metadata, Code.InvalidArgument),
        kind: requestKinds.get(message.kind),
        version: Number(message.version),
        timeoutNanos: message.timeoutNanos,
      };
    case "rpc_response":
      return {
        status: Number(message.statusRaw),
        message: decodeText(message.message, "response message", Code.Internal),
        body: message.body,
        metadata: metadataObject(message.metadata, Code.Internal),
      };
    case "rpc_stream_frame":
      return {
        kind: Number(message.kindRaw),
        status: Number(message.statusRaw),
        message: decodeText(message.message, "stream status message", Code.Internal),
        body: message.body,
        metadata: metadataObject(message.metadata, Code.Internal),
      };
    default:
      throw new Error(`unknown message type ${messageType}`);
  }
}

function validateMessage(messageTypeToken, messageType, message) {
  validateWireMessage(messageType, message);

  try {
    validateMetadata(message.metadata ?? {});
  } catch (error) {
    throw new SemanticError("invalid_metadata", directionalStatus(messageTypeToken), error);
  }
}

function normalizeMessage(messageType, message) {
  switch (messageType) {
    case "rpc_request":
      return {
        type: "rpc_request",
        service_hex: bytesToHex(textEncoder.encode(message.service ?? "")),
        method_hex: bytesToHex(textEncoder.encode(message.method ?? "")),
        body_hex: bytesToHex(message.body ?? new Uint8Array()),
        metadata: normalizeMetadata(message.metadata),
        kind: requestKindToken(message.kind),
        version: String(message.version ?? 0),
        timeout_nanos: String(message.timeoutNanos ?? 0),
      };
    case "rpc_response":
      return {
        type: "rpc_response",
        status_raw: String(message.status ?? 0),
        status_code: codeFromNumber(message.status ?? 0),
        message_hex: bytesToHex(textEncoder.encode(message.message ?? "")),
        body_hex: bytesToHex(message.body ?? new Uint8Array()),
        metadata: normalizeMetadata(message.metadata),
      };
    case "rpc_stream_frame":
      return {
        type: "rpc_stream_frame",
        kind: message.kind === RpcStreamFrameKind.Status ? "status" : "message",
        kind_raw: String(message.kind ?? 0),
        status_raw: String(message.status ?? 0),
        status_code: codeFromNumber(message.status ?? 0),
        message_hex: bytesToHex(textEncoder.encode(message.message ?? "")),
        body_hex: bytesToHex(message.body ?? new Uint8Array()),
        metadata: normalizeMetadata(message.metadata),
      };
    default:
      throw new Error(`unknown message type ${messageType}`);
  }
}

function normalizeTerminalStatus(status) {
  return {
    status_raw: String(status.code),
    status_code: codeFromNumber(status.code),
    message_hex: bytesToHex(textEncoder.encode(status.message ?? "")),
    metadata: normalizeMetadata(status.metadata),
  };
}

function normalizeMetadata(metadata = {}) {
  return Object.entries(metadata)
    .map(([key, value]) => ({
      key: textEncoder.encode(key),
      value,
    }))
    .sort((left, right) => Buffer.compare(left.key, right.key))
    .map(({ key, value }) => ({ key_hex: bytesToHex(key), value_hex: bytesToHex(value) }));
}

function metadataObject(entries, statusCode) {
  const metadata = Object.create(null);
  for (const { key, value } of entries) {
    let decoded;
    try {
      decoded = textDecoder.decode(key);
    } catch (error) {
      throw new SemanticError("invalid_metadata", statusCode, error);
    }
    metadata[decoded] = value;
  }
  return metadata;
}

function decodeText(bytes, description, statusCode) {
  try {
    return textDecoder.decode(bytes);
  } catch (error) {
    throw new SemanticError(
      "malformed_protobuf",
      statusCode,
      new Error(`${description} was not valid UTF-8`, { cause: error }),
    );
  }
}

function requestKindToken(kind) {
  switch (kind) {
    case RpcKind.Unary:
      return "unary";
    case RpcKind.ClientStreaming:
      return "client_stream";
    case RpcKind.ServerStreaming:
      return "server_stream";
    case RpcKind.BidirectionalStreaming:
      return "bidi";
    default:
      return "";
  }
}

function operationErrorPayload(command, thrown) {
  const stateError = thrown instanceof StateOperationError ? thrown : null;
  const error = stateError?.cause ?? thrown;
  const classified = classifyError(command, error);
  const payload = {
    outcome: "error",
    category: classified.category,
    status_code: classified.statusCode,
  };
  if (command.operation === "state.server_stream") {
    payload.transport_close_count = String(stateError?.closeCount ?? 0);
  }
  return payload;
}

function classifyError(command, error) {
  if (error instanceof SemanticError) {
    return { category: error.category, statusCode: error.statusCode };
  }
  if (error instanceof FrameTooLargeError) {
    return { category: "frame_too_large", statusCode: Code.ResourceExhausted };
  }
  if (error instanceof WireValidationError) {
    return wireReason(error.reason, command.messageType);
  }
  if (error?.reason === "malformed_protobuf") {
    return { category: "malformed_protobuf", statusCode: directionalStatus(command.messageType) };
  }
  if (error?.reason === "incomplete_frame") {
    return { category: "incomplete_frame", statusCode: Code.Internal };
  }
  if (error?.reason === "unsupported_frame_kind") {
    return { category: "unsupported_frame_kind", statusCode: Code.InvalidArgument };
  }
  if (error?.reason === "invalid_metadata") {
    return { category: "invalid_metadata", statusCode: Code.Internal };
  }
  if (error?.reason === "missing_terminal_status") {
    return { category: "missing_terminal_status", statusCode: Code.Internal };
  }
  if (error?.reason === "trailing_frame") {
    return { category: "trailing_frame", statusCode: Code.Internal };
  }
  if (error?.reason === "response_cardinality") {
    return { category: "response_cardinality", statusCode: Code.Internal };
  }
  if (command.operation.startsWith("state.") && error instanceof TrevRpcError) {
    return { category: "remote_status", statusCode: error.code };
  }
  return { category: "malformed_protobuf", statusCode: directionalStatus(command.messageType) };
}

function wireReason(reason, messageType) {
  switch (reason) {
    case "unsupported_wire_version":
      return { category: reason, statusCode: Code.FailedPrecondition };
    case "unsupported_rpc_kind":
    case "unsupported_frame_kind":
      return { category: reason, statusCode: Code.InvalidArgument };
    default:
      return { category: "malformed_protobuf", statusCode: directionalStatus(messageType) };
  }
}

function directionalStatus(messageType) {
  return messageType === "rpc_request" ? Code.InvalidArgument : Code.Internal;
}

function bytesToHex(bytes) {
  return Buffer.from(bytes ?? new Uint8Array()).toString("hex");
}

class ChunkReader {
  constructor(chunks) {
    this.chunks = chunks;
    this.next = 0;
  }

  async read() {
    if (this.next >= this.chunks.length) {
      return { done: true, value: undefined };
    }
    return { done: false, value: this.chunks[this.next++] };
  }
}

class SemanticError extends Error {
  constructor(category, statusCode, cause) {
    super(cause?.message ?? category, { cause });
    this.name = "SemanticError";
    this.category = category;
    this.statusCode = statusCode;
  }
}

class StateOperationError extends Error {
  constructor(cause, closeCount) {
    super(cause?.message ?? String(cause), { cause });
    this.name = "StateOperationError";
    this.closeCount = closeCount;
  }
}
