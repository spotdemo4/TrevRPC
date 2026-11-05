// The protobuf codec of the REQUEST, HEADERS, ANSWER and STATUS payloads, written against the wire
// format directly, as the Go and Kotlin runtimes do (see "Protocol and Wire Format" in the wiki).
// Encoding is canonical proto3: fields in number order, zero values left out, and both fields of a
// map entry always written. Decoding accepts only what a conforming sender produces: a known field
// with another wire type, a uint32 above 2^32 - 1, a varint above 64 bits, invalid UTF-8 and
// truncated data are all errors, while unknown fields of every wire type are skipped.
import { BinaryReader, BinaryWriter, WireType } from "@bufbuild/protobuf/wire";

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

/** How deeply unknown groups may nest. */
const MaxGroupDepth = 64;

/**
 * The REQUEST payload: `{ method, metadata, kind, timeoutNanos, datagramFlow, acceptDictionaries,
 * dictionaryQuery, streamCompression, connectionMetadata }`. 64-bit fields encode from a bigint, a
 * number or a decimal string and decode as bigints; `connectionMetadata` is `{ id, metadata }`, or
 * undefined when absent.
 */
export const RpcRequest = Object.freeze({
  name: "RpcRequest",
  encode(request) {
    const writer = new BinaryWriter();
    writeString(writer, 1, request.method);
    writeMetadata(writer, 2, request.metadata);
    writeInt32(writer, 3, request.kind);
    writeUint64(writer, 4, request.timeoutNanos);
    writeUint64(writer, 5, request.datagramFlow);
    const accept = request.acceptDictionaries;
    if (accept != null && accept.length > 0) {
      writer.tag(6, WireType.LengthDelimited).fork();
      for (const id of accept) {
        writer.uint32(id);
      }
      writer.join();
    }
    writeDictionaries(writer, 7, request.dictionaryQuery);
    writeInt32(writer, 8, request.streamCompression);
    const connection = request.connectionMetadata;
    if (connection != null) {
      writer.tag(9, WireType.LengthDelimited).fork();
      writeUint32(writer, 1, connection.id);
      writeMetadata(writer, 2, connection.metadata);
      writer.join();
    }
    return writer.finish();
  },
  decode(bytes) {
    const request = {
      method: "",
      metadata: emptyMetadata(),
      kind: 0,
      timeoutNanos: 0n,
      datagramFlow: 0n,
      acceptDictionaries: [],
      dictionaryQuery: [],
      streamCompression: 0,
      connectionMetadata: undefined,
    };
    const reader = new WireReader(bytes);
    while (reader.more()) {
      const [field, wireType] = reader.tag();
      switch (field) {
        case 1:
          request.method = reader.string(wireType, "method");
          break;
        case 2:
          reader.metadataEntry(wireType, request.metadata, "metadata");
          break;
        case 3:
          request.kind = reader.int32(wireType, "kind");
          break;
        case 4:
          request.timeoutNanos = reader.uint64(wireType, "timeout_nanos");
          break;
        case 5:
          request.datagramFlow = reader.uint64(wireType, "datagram_flow");
          break;
        case 6:
          reader.repeatedUint32(wireType, request.acceptDictionaries, "accept_dictionaries");
          break;
        case 7:
          request.dictionaryQuery.push(reader.dictionaryInfo(wireType, "dictionary_query"));
          break;
        case 8:
          request.streamCompression = reader.int32(wireType, "stream_compression");
          break;
        case 9:
          request.connectionMetadata = reader.connectionMetadata(wireType, "connection_metadata");
          break;
        default:
          reader.skip(field, wireType);
      }
    }
    return request;
  },
});

/** The HEADERS payload: `{ metadata }`. */
export const RpcHeaders = Object.freeze({
  name: "RpcHeaders",
  encode(headers) {
    const writer = new BinaryWriter();
    writeMetadata(writer, 1, headers.metadata);
    return writer.finish();
  },
  decode(bytes) {
    const headers = { metadata: emptyMetadata() };
    const reader = new WireReader(bytes);
    while (reader.more()) {
      const [field, wireType] = reader.tag();
      if (field === 1) {
        reader.metadataEntry(wireType, headers.metadata, "metadata");
      } else {
        reader.skip(field, wireType);
      }
    }
    return headers;
  },
});

/** The ANSWER payload: `{ dictionaries, streamCompression, connectionMetadata }`. */
export const RpcAnswer = Object.freeze({
  name: "RpcAnswer",
  encode(answer) {
    const writer = new BinaryWriter();
    writeDictionaries(writer, 1, answer.dictionaries);
    writeBool(writer, 2, answer.streamCompression);
    writeUint32(writer, 3, answer.connectionMetadata);
    return writer.finish();
  },
  decode(bytes) {
    const answer = { dictionaries: [], streamCompression: false, connectionMetadata: 0 };
    const reader = new WireReader(bytes);
    while (reader.more()) {
      const [field, wireType] = reader.tag();
      switch (field) {
        case 1:
          answer.dictionaries.push(reader.dictionaryInfo(wireType, "dictionaries"));
          break;
        case 2:
          answer.streamCompression = reader.bool(wireType, "stream_compression");
          break;
        case 3:
          answer.connectionMetadata = reader.uint32(wireType, "connection_metadata");
          break;
        default:
          reader.skip(field, wireType);
      }
    }
    return answer;
  },
});

/** The STATUS payload: `{ code, message, metadata, refusedStreamCompression }`. */
export const RpcStatus = Object.freeze({
  name: "RpcStatus",
  encode(status) {
    const writer = new BinaryWriter();
    writeUint32(writer, 1, status.code);
    writeString(writer, 2, status.message);
    writeMetadata(writer, 3, status.metadata);
    writeBool(writer, 4, status.refusedStreamCompression);
    return writer.finish();
  },
  decode(bytes) {
    const status = {
      code: 0,
      message: "",
      metadata: emptyMetadata(),
      refusedStreamCompression: false,
    };
    const reader = new WireReader(bytes);
    while (reader.more()) {
      const [field, wireType] = reader.tag();
      switch (field) {
        case 1:
          status.code = reader.uint32(wireType, "code");
          break;
        case 2:
          status.message = reader.string(wireType, "message");
          break;
        case 3:
          reader.metadataEntry(wireType, status.metadata, "metadata");
          break;
        case 4:
          status.refusedStreamCompression = reader.bool(wireType, "refused_stream_compression");
          break;
        default:
          reader.skip(field, wireType);
      }
    }
    return status;
  },
});

function emptyMetadata() {
  return {};
}

/**
 * Sets a decoded metadata entry as an own property, so that a key such as `__proto__` is an entry
 * like any other and never replaces the object's prototype.
 */
function setEntry(metadata, key, value) {
  Object.defineProperty(metadata, key, {
    value,
    enumerable: true,
    writable: true,
    configurable: true,
  });
}

function writeString(writer, field, value) {
  if (value != null && value !== "") {
    writer.tag(field, WireType.LengthDelimited).string(value);
  }
}

function writeInt32(writer, field, value) {
  if (value != null && value !== 0) {
    writer.tag(field, WireType.Varint).int32(value);
  }
}

function writeUint32(writer, field, value) {
  if (value != null && value !== 0) {
    writer.tag(field, WireType.Varint).uint32(value);
  }
}

function writeUint64(writer, field, value) {
  if (value != null && value !== 0 && value !== 0n && value !== "0") {
    writer.tag(field, WireType.Varint).uint64(value);
  }
}

function writeBool(writer, field, value) {
  if (value) {
    writer.tag(field, WireType.Varint).bool(true);
  }
}

/** Writes a `map<string, bytes>` field; both fields of every entry, even when empty. */
function writeMetadata(writer, field, metadata) {
  if (metadata == null) {
    return;
  }
  for (const key of Object.keys(metadata)) {
    writer
      .tag(field, WireType.LengthDelimited)
      .fork()
      .tag(1, WireType.LengthDelimited)
      .string(key)
      .tag(2, WireType.LengthDelimited)
      .bytes(metadata[key])
      .join();
  }
}

/** Writes a repeated DictionaryInfo field `{ id, fingerprint }`; an empty entry is still sent. */
function writeDictionaries(writer, field, infos) {
  if (infos == null) {
    return;
  }
  for (const info of infos) {
    writer.tag(field, WireType.LengthDelimited).fork();
    writeUint32(writer, 1, info.id);
    if (info.fingerprint != null && info.fingerprint.byteLength > 0) {
      writer.tag(2, WireType.LengthDelimited).bytes(info.fingerprint);
    }
    writer.join();
  }
}

/**
 * Reads the fields of one wire message, checking each known field's wire type and range. Every
 * failure is an Error, which the frame layer reports as InvalidArgument.
 */
class WireReader {
  #bytes;
  #reader;

  constructor(bytes) {
    this.#bytes = bytes;
    this.#reader = new BinaryReader(bytes);
  }

  more() {
    return this.#reader.pos < this.#reader.len;
  }

  /** The next field's number, at least 1, and wire type. */
  tag() {
    return this.#reader.tag();
  }

  string(wireType, name) {
    expectWireType(wireType, WireType.LengthDelimited, name);
    return this.#reader.string(true);
  }

  bytes(wireType, name) {
    expectWireType(wireType, WireType.LengthDelimited, name);
    return this.#reader.bytes();
  }

  bool(wireType, name) {
    expectWireType(wireType, WireType.Varint, name);
    return this.#reader.bool();
  }

  int32(wireType, name) {
    expectWireType(wireType, WireType.Varint, name);
    return this.#reader.int32();
  }

  uint32(wireType, name) {
    expectWireType(wireType, WireType.Varint, name);
    return this.#uint32Value(name);
  }

  uint64(wireType, name) {
    expectWireType(wireType, WireType.Varint, name);
    return BigInt(this.#reader.uint64());
  }

  /** Appends one value, or a packed run of values, of a repeated uint32 field to `values`. */
  repeatedUint32(wireType, values, name) {
    if (wireType === WireType.Varint) {
      values.push(this.#uint32Value(name));
      return;
    }
    const packed = this.#nested(wireType, name);
    while (packed.more()) {
      values.push(packed.#uint32Value(name));
    }
  }

  /** A DictionaryInfo message, `{ id, fingerprint }`. */
  dictionaryInfo(wireType, name) {
    const reader = this.#nested(wireType, name);
    const info = { id: 0, fingerprint: new Uint8Array(0) };
    while (reader.more()) {
      const [field, fieldWireType] = reader.tag();
      if (field === 1) {
        info.id = reader.uint32(fieldWireType, `${name}.id`);
      } else if (field === 2) {
        info.fingerprint = reader.bytes(fieldWireType, `${name}.fingerprint`);
      } else {
        reader.skip(field, fieldWireType);
      }
    }
    return info;
  }

  /** A ConnectionMetadata message, `{ id, metadata }`. */
  connectionMetadata(wireType, name) {
    const reader = this.#nested(wireType, name);
    const connection = { id: 0, metadata: emptyMetadata() };
    while (reader.more()) {
      const [field, fieldWireType] = reader.tag();
      if (field === 1) {
        connection.id = reader.uint32(fieldWireType, `${name}.id`);
      } else if (field === 2) {
        reader.metadataEntry(fieldWireType, connection.metadata, `${name}.metadata`);
      } else {
        reader.skip(field, fieldWireType);
      }
    }
    return connection;
  }

  /** Adds one `map<string, bytes>` entry to `metadata`; a later entry for a key replaces it. */
  metadataEntry(wireType, metadata, name) {
    const reader = this.#nested(wireType, name);
    let key = "";
    let value = new Uint8Array(0);
    while (reader.more()) {
      const [field, fieldWireType] = reader.tag();
      if (field === 1) {
        key = reader.string(fieldWireType, `${name}.key`);
      } else if (field === 2) {
        value = reader.bytes(fieldWireType, `${name}.value`);
      } else {
        reader.skip(field, fieldWireType);
      }
    }
    setEntry(metadata, key, value);
  }

  /** Skips an unknown field of any wire type. */
  skip(field, wireType, depth = 0) {
    switch (wireType) {
      case WireType.Varint:
        this.#skipVarint();
        return;
      case WireType.StartGroup:
        this.#skipGroup(field, depth + 1);
        return;
      case WireType.EndGroup:
        throw new Error("protobuf end group tag without a start group");
      default:
        this.#reader.skip(wireType, field);
    }
  }

  #skipGroup(field, depth) {
    if (depth > MaxGroupDepth) {
      throw new Error("protobuf group nesting exceeded limit");
    }
    while (this.more()) {
      const [nested, wireType] = this.tag();
      if (wireType === WireType.EndGroup) {
        if (nested !== field) {
          throw new Error("protobuf group end tag did not match start tag");
        }
        return;
      }
      this.skip(nested, wireType, depth);
    }
    throw new Error("unterminated protobuf group");
  }

  /** Skips a varint of at most 64 bits (10 bytes, the last at most 1). */
  #skipVarint() {
    const bytes = this.#bytes;
    const reader = this.#reader;
    for (let index = 0; index < 10; index += 1) {
      if (reader.pos >= reader.len) {
        throw new Error("truncated protobuf varint");
      }
      const byte = bytes[reader.pos++];
      if (byte < 0x80) {
        if (index === 9 && byte > 1) {
          break;
        }
        return;
      }
    }
    throw new Error("protobuf varint exceeded 64 bits");
  }

  /** Reads a varint that must fit in 32 bits: at most 5 bytes, the last at most 0x0f. */
  #uint32Value(name) {
    const reader = this.#reader;
    const start = reader.pos;
    const value = reader.uint32();
    const length = reader.pos - start;
    if (length > 5 || (length === 5 && this.#bytes[reader.pos - 1] > 0x0f)) {
      throw new Error(`protobuf field ${name} exceeded uint32`);
    }
    return value;
  }

  /** A reader over the length-delimited value of field `name`. */
  #nested(wireType, name) {
    expectWireType(wireType, WireType.LengthDelimited, name);
    return new WireReader(this.#reader.bytes());
  }
}

function expectWireType(wireType, expected, name) {
  if (wireType !== expected) {
    throw new Error(`field ${name} used incompatible protobuf wire type ${wireType}`);
  }
}
