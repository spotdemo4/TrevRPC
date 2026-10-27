import protobuf from "protobufjs";

import { byteView } from "./bytes.js";

export { protobuf };

export const RpcKind = Object.freeze({
  Unary: 0,
  ClientStreaming: 1,
  ServerStreaming: 2,
  BidirectionalStreaming: 3,
});

/** The values of `RpcRequest.stream_compression`. */
export const StreamCompression = Object.freeze({
  None: 0,
  Enabled: 1,
  Optimistic: 2,
});

const wireRoot = protobuf.Root.fromJSON({
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
        StreamCompression: {
          values: {
            STREAM_COMPRESSION_NONE: 0,
            STREAM_COMPRESSION_ENABLED: 1,
            STREAM_COMPRESSION_OPTIMISTIC: 2,
          },
        },
        RpcRequest: {
          fields: {
            method: { type: "string", id: 1 },
            metadata: { keyType: "string", type: "bytes", id: 2 },
            kind: { type: "RpcKind", id: 3 },
            timeoutNanos: { type: "uint64", id: 4 },
            datagramFlow: { type: "uint64", id: 5 },
            acceptDictionaries: { rule: "repeated", type: "uint32", id: 6 },
            dictionaryQuery: { rule: "repeated", type: "DictionaryInfo", id: 7 },
            streamCompression: { type: "StreamCompression", id: 8 },
            connectionMetadata: { type: "ConnectionMetadata", id: 9 },
          },
        },
        RpcHeaders: {
          fields: {
            metadata: { keyType: "string", type: "bytes", id: 1 },
          },
        },
        RpcAnswer: {
          fields: {
            dictionaries: { rule: "repeated", type: "DictionaryInfo", id: 1 },
            streamCompression: { type: "bool", id: 2 },
            connectionMetadata: { type: "uint32", id: 3 },
          },
        },
        ConnectionMetadata: {
          fields: {
            id: { type: "uint32", id: 1 },
            metadata: { keyType: "string", type: "bytes", id: 2 },
          },
        },
        DictionaryInfo: {
          fields: {
            id: { type: "uint32", id: 1 },
            fingerprint: { type: "bytes", id: 2 },
          },
        },
        RpcStatus: {
          fields: {
            code: { type: "uint32", id: 1 },
            message: { type: "string", id: 2 },
            metadata: { keyType: "string", type: "bytes", id: 3 },
            refusedStreamCompression: { type: "bool", id: 4 },
          },
        },
      },
    },
  },
});

export const RpcRequest = wireRoot.lookupType("trevrpc.RpcRequest");
export const RpcHeaders = wireRoot.lookupType("trevrpc.RpcHeaders");
export const RpcAnswer = wireRoot.lookupType("trevrpc.RpcAnswer");
export const RpcStatus = wireRoot.lookupType("trevrpc.RpcStatus");

const MaxFieldNumber = 0x1fffffff;
const MaxUint32 = 0xffffffffn;
const MaxUint64 = 0xffffffffffffffffn;
const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

/** Checks protobuf tags and known-field wire types before protobuf.js decoding. */
export function preflightWireMessage(messageType, body) {
  scanMessage(messageType, byteView(body), 0);
}

/** Creates a protobuf root from a JSON namespace definition. */
export function createRoot(json) {
  return protobuf.Root.fromJSON(json);
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
  return new Error(message);
}
