import protobuf from "protobufjs";

import { byteView } from "./bytes.js";
import { setInt64Representation } from "./int64.js";

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

/**
 * Creates a protobuf root from a JSON namespace definition. `options.int64` sets how calls decode
 * its messages' 64-bit integers (see `setInt64Representation`); without it they stay Long objects.
 */
export function createRoot(json, options = {}) {
  const root = protobuf.Root.fromJSON(json);
  if (options.int64 !== undefined) {
    setInt64Representation(root, options.int64);
  }
  return root;
}

/**
 * Checks a protobuf message body before protobuf.js decodes it, rejecting what protobuf.js would
 * accept: a known field sent with another wire type (protobuf.js skips it as unknown), a uint32
 * above 2^32 - 1 (protobuf.js truncates it), a varint above 64 bits, and nesting deeper than 64.
 * It also rejects truncated and otherwise malformed fields. It leaves UTF-8 validation to
 * protobuf.js, which validates strings while decoding them (in every type except those of proto2
 * roots, whose strings protobuf runtimes don't validate).
 */
export function preflightWireMessage(messageType, body) {
  const bytes = byteView(body);
  new WireScanner(bytes).message(messagePlan(messageType), bytes.byteLength, 0);
}

const MaxNestingDepth = 64;
const MaxUint32 = 0xffffffff;

/** How the scanner checks a known field's value. */
const FieldKind = Object.freeze({ Scalar: 0, Message: 1, Map: 2 });

/**
 * The plan of each message type, keyed by its fields array: protobuf.js replaces that array when
 * the type's fields change, which drops the stale plan.
 */
const messagePlans = new WeakMap();

/** Returns a message type's plan: a Map from each known field number to the field's plan. */
function messagePlan(messageType) {
  const fields = messageType.fieldsArray;
  let plan = messagePlans.get(fields);
  if (plan === undefined) {
    plan = new Map(fields.map((field) => [field.id, fieldPlan(field.resolve())]));
    messagePlans.set(fields, plan);
  }
  return plan;
}

function fieldPlan(field) {
  if (!field.map) {
    return valuePlan(field.name, field.type, field.resolvedType, field.repeated);
  }
  // A map entry is a length-delimited message whose key is field 1 and value field 2.
  const entry = new Map([
    [1, valuePlan(`${field.name}.key`, field.keyType, null, false)],
    [2, valuePlan(`${field.name}.value`, field.type, field.resolvedType, false)],
  ]);
  return newFieldPlan(field.name, FieldKind.Map, 2, false, false, undefined, entry);
}

function valuePlan(name, type, resolvedType, repeated) {
  if (resolvedType?.fieldsById != null) {
    return newFieldPlan(name, FieldKind.Message, 2, false, false, resolvedType, undefined);
  }
  // Enums are int32 varints: negative values are 10 bytes long, so they get no uint32 check.
  const wireType = scalarWireType(type);
  const packed = repeated && wireType !== 2;
  const uint32 = type === "uint32";
  return newFieldPlan(name, FieldKind.Scalar, wireType, packed, uint32, undefined, undefined);
}

/** Every field plan has the same shape so the scanner's property loads stay monomorphic. */
function newFieldPlan(name, kind, wireType, packed, uint32, messageType, entry) {
  return { name, kind, wireType, packed, uint32, messageType, entry };
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

/** Walks a message body once with number arithmetic, never reading past `end`. */
class WireScanner {
  constructor(bytes) {
    this.bytes = bytes;
    this.offset = 0;
  }

  message(plan, end, depth) {
    if (depth > MaxNestingDepth) {
      throw malformedWire("protobuf message nesting exceeded limit");
    }
    while (this.offset < end) {
      const tag = this.tag(end);
      const fieldNumber = tag >>> 3;
      const wireType = tag & 7;
      const field = plan.get(fieldNumber);
      if (field === undefined) {
        this.skipField(fieldNumber, wireType, end, depth);
      } else {
        this.knownField(field, wireType, end, depth);
      }
    }
  }

  knownField(field, wireType, end, depth) {
    if (wireType !== field.wireType) {
      if (wireType === 2 && field.packed) {
        this.packed(field, this.lengthDelimited(end, field.name));
        return;
      }
      throw malformedWire(`field ${field.name} used incompatible protobuf wire type ${wireType}`);
    }

    switch (field.kind) {
      case FieldKind.Message:
        this.message(
          messagePlan(field.messageType),
          this.lengthDelimited(end, field.name),
          depth + 1,
        );
        return;
      case FieldKind.Map:
        this.message(field.entry, this.lengthDelimited(end, field.name), depth + 1);
        return;
      default:
        if (field.uint32) {
          this.uint32(end, field.name);
        } else {
          this.skipValue(wireType, end, field.name);
        }
    }
  }

  packed(field, end) {
    while (this.offset < end) {
      if (field.uint32) {
        this.uint32(end, field.name);
      } else {
        this.skipValue(field.wireType, end, field.name);
      }
    }
  }

  skipField(fieldNumber, wireType, end, depth) {
    if (wireType === 3) {
      this.group(fieldNumber, end, depth + 1);
    } else {
      this.skipValue(wireType, end, "unknown");
    }
  }

  /** Skips a varint, fixed-width or length-delimited value. */
  skipValue(wireType, end, name) {
    switch (wireType) {
      case 0:
        this.skipVarint(end, `truncated protobuf field ${name}`);
        return;
      case 1:
        this.skipFixed(8, end, name);
        return;
      case 2:
        this.offset = this.lengthDelimited(end, name);
        return;
      case 5:
        this.skipFixed(4, end, name);
        return;
      default:
        throw malformedWire(`unsupported protobuf wire type ${wireType}`);
    }
  }

  group(fieldNumber, end, depth) {
    if (depth > MaxNestingDepth) {
      throw malformedWire("protobuf group nesting exceeded limit");
    }
    while (this.offset < end) {
      const tag = this.tag(end);
      const nestedField = tag >>> 3;
      const wireType = tag & 7;
      if (wireType === 4) {
        if (nestedField !== fieldNumber) {
          throw malformedWire("protobuf group end tag did not match start tag");
        }
        return;
      }
      this.skipField(nestedField, wireType, end, depth);
    }
    throw malformedWire("unterminated protobuf group");
  }

  /** Reads a field tag; its field number is at least 1 and fits in 29 bits. */
  tag(end) {
    const tag = this.varint(end, "truncated protobuf field tag");
    if (tag < 8 || tag > MaxUint32) {
      throw malformedWire("invalid protobuf field tag");
    }
    return tag;
  }

  uint32(end, name) {
    if (this.varint(end, `truncated protobuf field ${name}`) > MaxUint32) {
      throw malformedWire(`protobuf field ${name} exceeded uint32`);
    }
  }

  /** Reads a length prefix and returns the offset where the value it prefixes ends. */
  lengthDelimited(end, name) {
    const truncated = `truncated protobuf field ${name}`;
    const length = this.varint(end, truncated);
    if (length > end - this.offset) {
      throw malformedWire(truncated);
    }
    return this.offset + length;
  }

  skipFixed(length, end, name) {
    if (length > end - this.offset) {
      throw malformedWire(`truncated protobuf field ${name}`);
    }
    this.offset += length;
  }

  /**
   * Reads a varint of up to 64 bits. The result is exact up to 2^53 and only rounds above it, so
   * comparisons with 32-bit limits and remaining lengths stay exact.
   */
  varint(end, truncated) {
    const bytes = this.bytes;
    let value = 0;
    let scale = 1;
    for (let index = 0; index < 10; index += 1) {
      if (this.offset >= end) {
        throw malformedWire(truncated);
      }
      const byte = bytes[this.offset++];
      value += (byte & 0x7f) * scale;
      if (byte < 0x80) {
        if (index === 9 && byte > 1) {
          break;
        }
        return value;
      }
      scale *= 128;
    }
    throw malformedWire("protobuf varint exceeded 64 bits");
  }

  /** Skips a varint of up to 64 bits, checking its length but not computing its value. */
  skipVarint(end, truncated) {
    const bytes = this.bytes;
    for (let index = 0; index < 10; index += 1) {
      if (this.offset >= end) {
        throw malformedWire(truncated);
      }
      const byte = bytes[this.offset++];
      if (byte < 0x80) {
        if (index === 9 && byte > 1) {
          break;
        }
        return;
      }
    }
    throw malformedWire("protobuf varint exceeded 64 bits");
  }
}

function malformedWire(message) {
  return new Error(message);
}
