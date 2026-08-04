export const ProtocolVersion = 1;
export const MaxCommandBytes = 262_144;
export const MaxEventBytes = 65_536;

const MaxUint32 = 0xffffffffn;
const MaxUint64 = 0xffffffffffffffffn;
const MaxSignedInt64 = 0x7fffffffffffffffn;
const MessageTypes = new Set(["rpc_request", "rpc_response", "rpc_stream_frame"]);

export class ProtocolError extends Error {
  constructor(message) {
    super(message);
    this.name = "ProtocolError";
  }
}

export async function* readCommandLines(readable, maxBytes = MaxCommandBytes) {
  let pending = Buffer.alloc(0);
  for await (const chunk of readable) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    pending = pending.length === 0 ? bytes : Buffer.concat([pending, bytes]);

    for (;;) {
      const newline = pending.indexOf(0x0a);
      if (newline < 0) {
        if (pending.byteLength > maxBytes) {
          throw new ProtocolError("command line exceeded limit");
        }
        break;
      }
      if (newline > maxBytes) {
        throw new ProtocolError("command line exceeded limit");
      }

      yield pending.subarray(0, newline);
      pending = pending.subarray(newline + 1);
    }
  }

  if (pending.byteLength > 0) {
    throw new ProtocolError("command was not LF-terminated");
  }
  throw new ProtocolError("controller input ended without STOP");
}

export function parseCommand(bytes) {
  for (const byte of bytes) {
    if (byte > 0x7f || byte === 0x0d) {
      throw new ProtocolError("command must be LF-terminated tab-delimited ASCII");
    }
  }

  const line = Buffer.from(bytes).toString("ascii");
  if (line === "STOP") {
    return { stop: true };
  }

  const fields = line.split("\t");
  if (fields.length < 4 || fields[0] !== "RUN") {
    throw new ProtocolError("expected RUN command");
  }
  parseDecimal(fields[1], MaxUint64, "sequence");
  if (!/^[a-z0-9._-]+$/u.test(fields[2])) {
    throw new ProtocolError("invalid case ID");
  }

  const parser = new FieldParser(fields, 4);
  const command = {
    stop: false,
    sequence: fields[1],
    caseId: fields[2],
    operation: fields[3],
  };

  switch (command.operation) {
    case "codec.encode":
      command.messageType = parser.messageType();
      command.message = parser.message(command.messageType);
      break;
    case "codec.decode":
      command.messageType = parser.messageType();
      command.body = parser.hexBytes();
      break;
    case "framing.encode":
      command.messageType = parser.messageType();
      command.maxFrameSize = parser.maxFrameSize();
      command.message = parser.message(command.messageType);
      break;
    case "framing.decode_stream":
      command.messageType = parser.messageType();
      command.maxFrameSize = parser.maxFrameSize();
      command.chunks = parser.hexList();
      break;
    case "state.server_stream":
    case "state.client_stream":
      command.frames = parser.hexList();
      break;
    default:
      throw new ProtocolError(`unknown operation ${JSON.stringify(command.operation)}`);
  }

  if (!parser.done()) {
    throw new ProtocolError("unexpected command fields");
  }
  return command;
}

class FieldParser {
  constructor(fields, next) {
    this.fields = fields;
    this.next = next;
  }

  done() {
    return this.next === this.fields.length;
  }

  token() {
    if (this.next >= this.fields.length) {
      throw new ProtocolError("missing command field");
    }
    return this.fields[this.next++];
  }

  messageType() {
    const value = this.token();
    if (!MessageTypes.has(value)) {
      throw new ProtocolError(`unknown message type ${JSON.stringify(value)}`);
    }
    return value;
  }

  hexBytes() {
    const value = this.token();
    if (value.length % 2 !== 0) {
      throw new ProtocolError("hex value has odd length");
    }
    if (!/^(?:[0-9a-f]{2})*$/u.test(value)) {
      throw new ProtocolError("hex value must be lowercase");
    }
    return Buffer.from(value, "hex");
  }

  decimal(maximum, name) {
    const value = this.token();
    parseDecimal(value, maximum, name);
    return value;
  }

  maxFrameSize() {
    const value = BigInt(this.decimal(MaxSignedInt64, "frame size"));
    return Number(value > MaxUint32 ? MaxUint32 : value);
  }

  count(fieldWidth = 1) {
    const text = this.decimal(MaxSignedInt64, "count");
    const count = BigInt(text);
    const available = BigInt(Math.floor((this.fields.length - this.next) / fieldWidth));
    if (count > available) {
      throw new ProtocolError("missing command field");
    }
    return Number(count);
  }

  hexList() {
    const count = this.count();
    return Array.from({ length: count }, () => this.hexBytes());
  }

  metadata() {
    const count = this.count(2);
    const entries = [];
    for (let index = 0; index < count; index += 1) {
      const key = this.hexBytes();
      const value = this.hexBytes();
      if (entries.length > 0 && Buffer.compare(entries.at(-1).key, key) >= 0) {
        throw new ProtocolError("metadata entries must be key-sorted and unique");
      }
      entries.push({ key, value });
    }
    return entries;
  }

  message(messageType) {
    switch (messageType) {
      case "rpc_request": {
        const service = this.hexBytes();
        const method = this.hexBytes();
        const body = this.hexBytes();
        const metadata = this.metadata();
        const kind = this.token();
        if (!new Set(["unary", "client_stream", "server_stream", "bidi"]).has(kind)) {
          throw new ProtocolError(`unknown RPC kind ${JSON.stringify(kind)}`);
        }
        const version = this.decimal(MaxUint32, "version");
        const timeoutNanos = this.decimal(MaxUint64, "timeout_nanos");
        return { service, method, body, metadata, kind, version, timeoutNanos };
      }
      case "rpc_response":
        return {
          statusRaw: this.decimal(MaxUint32, "status"),
          message: this.hexBytes(),
          body: this.hexBytes(),
          metadata: this.metadata(),
        };
      case "rpc_stream_frame":
        return {
          kindRaw: this.decimal(MaxUint32, "frame kind"),
          statusRaw: this.decimal(MaxUint32, "status"),
          message: this.hexBytes(),
          body: this.hexBytes(),
          metadata: this.metadata(),
        };
      default:
        throw new ProtocolError(`unknown message type ${JSON.stringify(messageType)}`);
    }
  }
}

function parseDecimal(value, maximum, name) {
  if (!/^(?:0|[1-9][0-9]*)$/u.test(value)) {
    throw new ProtocolError(`invalid ${name}: non-canonical decimal`);
  }
  if (BigInt(value) > maximum) {
    throw new ProtocolError(`invalid ${name}: decimal overflow`);
  }
}
