import protobuf from "protobufjs";

export { protobuf };

export const WireVersion = 1;

export const RpcKind = Object.freeze({
  Unary: 0,
  ClientStreaming: 1,
  ServerStreaming: 2,
  BidirectionalStreaming: 3,
});

export const RpcStreamFrameKind = Object.freeze({
  Message: 0,
  Status: 1,
});

export const wireRoot = protobuf.Root.fromJSON({
  nested: {
    trevrpc: {
      nested: {
        RpcKind: {
          values: {
            Unary: 0,
            ClientStreaming: 1,
            ServerStreaming: 2,
            BidirectionalStreaming: 3,
          },
        },
        RpcStreamFrameKind: {
          values: {
            Message: 0,
            Status: 1,
          },
        },
        RpcRequest: {
          fields: {
            service: { type: "string", id: 1 },
            method: { type: "string", id: 2 },
            body: { type: "bytes", id: 3 },
            metadata: { keyType: "string", type: "bytes", id: 4 },
            kind: { type: "RpcKind", id: 5 },
            version: { type: "uint32", id: 6 },
            timeoutNanos: { type: "uint64", id: 7 },
          },
        },
        RpcResponse: {
          fields: {
            status: { type: "uint32", id: 1 },
            message: { type: "string", id: 2 },
            body: { type: "bytes", id: 3 },
            metadata: { keyType: "string", type: "bytes", id: 4 },
          },
        },
        RpcStreamFrame: {
          fields: {
            kind: { type: "RpcStreamFrameKind", id: 1 },
            status: { type: "uint32", id: 2 },
            message: { type: "string", id: 3 },
            body: { type: "bytes", id: 4 },
            metadata: { keyType: "string", type: "bytes", id: 5 },
          },
        },
      },
    },
  },
});

export const RpcRequest = wireRoot.lookupType("trevrpc.RpcRequest");
export const RpcResponse = wireRoot.lookupType("trevrpc.RpcResponse");
export const RpcStreamFrame = wireRoot.lookupType("trevrpc.RpcStreamFrame");

const MaxFieldNumber = 0x1fffffff;
const MaxUint32 = 0xffffffffn;
const MaxUint64 = 0xffffffffffffffffn;
const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

/** Internal wire-validation failure used by production codec paths. */
export class WireValidationError extends Error {
  constructor(reason, message) {
    super(message);
    this.name = "WireValidationError";
    this.reason = reason;
  }
}

/** Checks protobuf tags and known-field wire types before protobuf.js decoding. */
export function preflightWireMessage(messageType, body) {
  scanMessage(messageType, byteView(body), 0);
}

/** Validates TrevRPC protocol enums and versions after protobuf decoding. */
export function validateWireMessage(messageType, message) {
  if (messageType === RpcRequest) {
    if (message.version !== WireVersion) {
      throw new WireValidationError(
        "unsupported_wire_version",
        `unsupported wire version ${message.version}`,
      );
    }
    if (!Object.values(RpcKind).includes(message.kind)) {
      throw new WireValidationError("unsupported_rpc_kind", `unsupported RPC kind ${message.kind}`);
    }
    return;
  }

  if (
    messageType === RpcStreamFrame &&
    message.kind !== RpcStreamFrameKind.Message &&
    message.kind !== RpcStreamFrameKind.Status
  ) {
    throw new WireValidationError(
      "unsupported_frame_kind",
      `unsupported stream frame kind ${message.kind}`,
    );
  }
}

/** Creates a protobuf root from a JSON namespace definition. */
export function createRoot(json) {
  return protobuf.Root.fromJSON(json);
}

/** Creates a stream message frame carrying a protobuf body. */
export function messageFrame(body) {
  return {
    kind: RpcStreamFrameKind.Message,
    status: 0,
    message: "",
    body,
    metadata: {},
  };
}

function scanMessage(messageType, bytes, depth) {
  if (depth > 64) {
    throw malformedWire("protobuf message nesting exceeded limit");
  }

  const cursor = { offset: 0 };
  while (cursor.offset < bytes.byteLength) {
    const tag = readVarint(bytes, cursor, "truncated protobuf field tag");
    const fieldNumber = Number(tag >> 3n);
    const wireType = Number(tag & 7n);
    if (fieldNumber === 0 || fieldNumber > MaxFieldNumber) {
      throw malformedWire("invalid protobuf field tag");
    }

    const field = messageType.fieldsById?.[fieldNumber];
    if (field == null) {
      skipUnknownField(bytes, cursor, fieldNumber, wireType, depth);
      continue;
    }

    if (!acceptedWireTypes(field).includes(wireType)) {
      throw malformedWire(`field ${field.name} used incompatible protobuf wire type ${wireType}`);
    }
    scanKnownField(field, bytes, cursor, wireType, depth);
  }
}

function scanKnownField(field, bytes, cursor, wireType, depth) {
  if (wireType === 0) {
    const value = readVarint(bytes, cursor, `truncated protobuf field ${field.name}`);
    if (isUint32Field(field) && value > MaxUint32) {
      throw malformedWire(`protobuf field ${field.name} exceeded uint32`);
    }
    return;
  }
  if (wireType === 1) {
    skipFixed(bytes, cursor, 8, field.name);
    return;
  }
  if (wireType === 5) {
    skipFixed(bytes, cursor, 4, field.name);
    return;
  }
  if (wireType !== 2) {
    throw malformedWire(`unsupported protobuf wire type ${wireType}`);
  }

  const value = readLengthDelimited(bytes, cursor, `truncated protobuf field ${field.name}`);
  if (field.map) {
    scanMapEntry(field, value, depth + 1);
  } else if (field.resolvedType?.fieldsById != null) {
    scanMessage(field.resolvedType, value, depth + 1);
  } else if (field.type === "string") {
    validateUtf8(value, field.name);
  } else if (field.repeated && isPackableField(field)) {
    scanPackedField(field, value);
  }
}

function scanMapEntry(field, bytes, depth) {
  if (depth > 64) {
    throw malformedWire("protobuf message nesting exceeded limit");
  }

  const cursor = { offset: 0 };
  while (cursor.offset < bytes.byteLength) {
    const tag = readVarint(bytes, cursor, "truncated protobuf map entry tag");
    const fieldNumber = Number(tag >> 3n);
    const wireType = Number(tag & 7n);
    if (fieldNumber === 0 || fieldNumber > MaxFieldNumber) {
      throw malformedWire("invalid protobuf map entry tag");
    }

    if (fieldNumber === 1) {
      scanSyntheticField(field.keyType, `${field.name}.key`, bytes, cursor, wireType, depth);
    } else if (fieldNumber === 2) {
      scanSyntheticField(field.type, `${field.name}.value`, bytes, cursor, wireType, depth);
    } else {
      skipUnknownField(bytes, cursor, fieldNumber, wireType, depth);
    }
  }
}

function scanSyntheticField(type, name, bytes, cursor, wireType, depth) {
  const expected = scalarWireType(type);
  if (wireType !== expected) {
    throw malformedWire(`field ${name} used incompatible protobuf wire type ${wireType}`);
  }
  if (wireType === 0) {
    const value = readVarint(bytes, cursor, `truncated protobuf field ${name}`);
    if (isUint32Type(type) && value > MaxUint32) {
      throw malformedWire(`protobuf field ${name} exceeded uint32`);
    }
  } else if (wireType === 1) {
    skipFixed(bytes, cursor, 8, name);
  } else if (wireType === 5) {
    skipFixed(bytes, cursor, 4, name);
  } else if (wireType === 2) {
    const value = readLengthDelimited(bytes, cursor, `truncated protobuf field ${name}`);
    if (type === "string") {
      validateUtf8(value, name);
    }
  } else {
    skipUnknownField(bytes, cursor, 0, wireType, depth);
  }
}

function scanPackedField(field, bytes) {
  const cursor = { offset: 0 };
  const wireType = scalarWireType(field.type);
  while (cursor.offset < bytes.byteLength) {
    if (wireType === 0) {
      const value = readVarint(bytes, cursor, `truncated packed protobuf field ${field.name}`);
      if (isUint32Field(field) && value > MaxUint32) {
        throw malformedWire(`protobuf field ${field.name} exceeded uint32`);
      }
    } else if (wireType === 1) {
      skipFixed(bytes, cursor, 8, field.name);
    } else if (wireType === 5) {
      skipFixed(bytes, cursor, 4, field.name);
    } else {
      throw malformedWire(`protobuf field ${field.name} cannot be packed`);
    }
  }
}

function acceptedWireTypes(field) {
  const expected = field.resolvedType?.fieldsById != null ? 2 : scalarWireType(field.type);
  return field.repeated && isPackableField(field) ? [expected, 2] : [expected];
}

function scalarWireType(type) {
  switch (type) {
    case "double":
    case "fixed64":
    case "sfixed64":
      return 1;
    case "string":
    case "bytes":
      return 2;
    case "float":
    case "fixed32":
    case "sfixed32":
      return 5;
    default:
      return 0;
  }
}

function isPackableField(field) {
  return (
    field.type !== "string" && field.type !== "bytes" && field.resolvedType?.fieldsById == null
  );
}

function isUint32Field(field) {
  return isUint32Type(field.type) || field.resolvedType?.values != null;
}

function isUint32Type(type) {
  return type === "uint32" || type === "fixed32";
}

function readVarint(bytes, cursor, message) {
  let value = 0n;
  for (let index = 0; index < 10; index += 1) {
    if (cursor.offset >= bytes.byteLength) {
      throw malformedWire(message);
    }
    const byte = bytes[cursor.offset++];
    if (index === 9 && byte > 1) {
      throw malformedWire("protobuf varint exceeded 64 bits");
    }
    value |= BigInt(byte & 0x7f) << BigInt(index * 7);
    if (byte < 0x80) {
      if (value > MaxUint64) {
        throw malformedWire("protobuf varint exceeded 64 bits");
      }
      return value;
    }
  }
  throw malformedWire("protobuf varint exceeded 64 bits");
}

function readLengthDelimited(bytes, cursor, message) {
  const length = readVarint(bytes, cursor, message);
  if (length > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw malformedWire(message);
  }
  const end = cursor.offset + Number(length);
  if (!Number.isSafeInteger(end) || end > bytes.byteLength) {
    throw malformedWire(message);
  }
  const value = bytes.subarray(cursor.offset, end);
  cursor.offset = end;
  return value;
}

function skipUnknownField(bytes, cursor, fieldNumber, wireType, depth) {
  switch (wireType) {
    case 0:
      readVarint(bytes, cursor, "truncated protobuf varint");
      return;
    case 1:
      skipFixed(bytes, cursor, 8, "unknown");
      return;
    case 2:
      readLengthDelimited(bytes, cursor, "truncated protobuf field");
      return;
    case 3:
      skipGroup(bytes, cursor, fieldNumber, depth + 1);
      return;
    case 5:
      skipFixed(bytes, cursor, 4, "unknown");
      return;
    default:
      throw malformedWire(`unsupported protobuf wire type ${wireType}`);
  }
}

function skipGroup(bytes, cursor, fieldNumber, depth) {
  if (depth > 64) {
    throw malformedWire("protobuf group nesting exceeded limit");
  }
  while (cursor.offset < bytes.byteLength) {
    const tag = readVarint(bytes, cursor, "truncated protobuf group tag");
    const nestedField = Number(tag >> 3n);
    const wireType = Number(tag & 7n);
    if (wireType === 4) {
      if (nestedField !== fieldNumber) {
        throw malformedWire("protobuf group end tag did not match start tag");
      }
      return;
    }
    skipUnknownField(bytes, cursor, nestedField, wireType, depth);
  }
  throw malformedWire("unterminated protobuf group");
}

function skipFixed(bytes, cursor, length, name) {
  const end = cursor.offset + length;
  if (end > bytes.byteLength) {
    throw malformedWire(`truncated protobuf field ${name}`);
  }
  cursor.offset = end;
}

function validateUtf8(bytes, name) {
  try {
    fatalUtf8Decoder.decode(bytes);
  } catch {
    throw malformedWire(`protobuf field ${name} was not valid UTF-8`);
  }
}

function malformedWire(message) {
  return new WireValidationError("malformed_protobuf", message);
}

function byteView(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return new Uint8Array(value ?? 0);
}
