// The protobuf codec of the REQUEST, HEADERS, ANSWER and STATUS payloads, written against the wire
// format directly, as the Go and Kotlin runtimes do (see "Protocol and Wire Format" in the wiki).
// Encoding is canonical proto3: fields in number order, zero values left out, and both fields of a
// map entry always written. Decoding accepts only what a conforming sender produces: a known field
// with another wire type, a uint32 above 2^32 - 1, a varint above 64 bits, invalid UTF-8 and
// truncated data are all errors, while unknown fields of every wire type are skipped.
//
// Every call encodes a REQUEST and decodes a STATUS, so both avoid per-payload allocations: one
// writer is shared across encodes (making a protobuf-es BinaryWriter costs more than encoding a
// typical REQUEST), and decoding reads nested messages on the payload's one reader.
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
/** The shared writer is dropped after an encode larger than this, which grew its buffer. */
const SharedWriterMax = 64 * 1024;

// Each payload has the codec of the direction the client needs: it encodes REQUEST and decodes
// HEADERS, ANSWER and STATUS. The server direction lives with the tests (test/wire-server.js), which
// play the server, so that pages do not ship it.

/**
 * Encodes the REQUEST payload: `{ method, metadata, kind, timeoutNanos, datagramFlow,
 * acceptDictionaries, dictionaryQuery, streamCompression, connectionMetadata }`. 64-bit fields
 * encode from a bigint, a number or a decimal string; `connectionMetadata` is `{ id, metadata }`,
 * or undefined when absent.
 */
export const RpcRequest = Object.freeze({
  name: "RpcRequest",
  encode: (request) => encodeWith(writeRequest, request),
});

/** Decodes the HEADERS payload: `{ metadata }`. */
export const RpcHeaders = Object.freeze({
  name: "RpcHeaders",
  decode: (bytes) => decodeHeaders(new WireReader(bytes)),
});

/** Decodes the ANSWER payload: `{ dictionaries, streamCompression, connectionMetadata }`. */
export const RpcAnswer = Object.freeze({
  name: "RpcAnswer",
  decode: (bytes) => decodeAnswer(new WireReader(bytes)),
});

/**
 * Decodes the STATUS payload: `{ code, message, metadata, refusedStreamCompression, details }`,
 * where each detail is `{ type, value }`.
 */
export const RpcStatus = Object.freeze({
  name: "RpcStatus",
  decode: (bytes) => decodeStatus(new WireReader(bytes)),
});

function writeRequest(writer, request) {
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
}

function decodeHeaders(reader) {
  const headers = { metadata: emptyMetadata() };
  while (reader.more()) {
    const tag = reader.tag();
    if (tag >>> 3 === 1) {
      reader.metadataEntry(tag & 7, headers.metadata, "metadata");
    } else {
      reader.skip(tag);
    }
  }
  return headers;
}

function decodeAnswer(reader) {
  const answer = { dictionaries: [], streamCompression: false, connectionMetadata: 0 };
  while (reader.more()) {
    const tag = reader.tag();
    const wireType = tag & 7;
    switch (tag >>> 3) {
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
        reader.skip(tag);
    }
  }
  return answer;
}

function decodeStatus(reader) {
  const status = {
    code: 0,
    message: "",
    metadata: emptyMetadata(),
    refusedStreamCompression: false,
    details: [],
  };
  while (reader.more()) {
    const tag = reader.tag();
    const wireType = tag & 7;
    switch (tag >>> 3) {
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
      case 5:
        status.details.push(reader.detail(wireType, "details"));
        break;
      default:
        reader.skip(tag);
    }
  }
  return status;
}

// Encoding never calls back into code that encodes another payload, except through String() of a
// value in a string field; a writer in use is never handed out again, so even that is safe.
let sharedWriter;
let sharedWriterBusy = false;

/**
 * Encodes `value` with `write(writer, value)` on the shared writer, and returns a copy. Exported,
 * like the field writers and WireReader, for the server direction in test/wire-server.js.
 */
export function encodeWith(write, value) {
  if (sharedWriterBusy) {
    const writer = new BinaryWriter();
    write(writer, value);
    return writer.finish();
  }
  sharedWriterBusy = true;
  const writer = (sharedWriter ??= new BinaryWriter());
  try {
    write(writer, value);
    // finish() copies the bytes out and rewinds the writer for the next encode, but keeps its
    // buffer, which a large encode grew: a page must not hold that for its lifetime.
    const bytes = writer.finish();
    if (bytes.length > SharedWriterMax) {
      sharedWriter = undefined;
    }
    return bytes;
  } catch (error) {
    // A failed encode leaves the writer mid-message; the next one starts on a new writer.
    sharedWriter = undefined;
    throw error;
  } finally {
    sharedWriterBusy = false;
  }
}

/** Whether a shared writer is kept for the next encode; for tests. */
export function sharedWriterKept() {
  return sharedWriter !== undefined;
}

function emptyMetadata() {
  return {};
}

/**
 * Sets a decoded metadata entry as an own property, so that a key such as `__proto__` is an entry
 * like any other and never replaces the object's prototype. Assignment does that for every other
 * key, faster than defining the property.
 */
function setEntry(metadata, key, value) {
  if (key === "__proto__") {
    Object.defineProperty(metadata, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  } else {
    metadata[key] = value;
  }
}

export function writeString(writer, field, value) {
  if (value != null && value !== "") {
    writer.tag(field, WireType.LengthDelimited).string(value);
  }
}

function writeInt32(writer, field, value) {
  if (value != null && value !== 0) {
    writer.tag(field, WireType.Varint).int32(value);
  }
}

export function writeUint32(writer, field, value) {
  if (value != null && value !== 0) {
    writer.tag(field, WireType.Varint).uint32(value);
  }
}

function writeUint64(writer, field, value) {
  if (value != null && value !== 0 && value !== 0n && value !== "0") {
    writer.tag(field, WireType.Varint).uint64(value);
  }
}

/** Writes a `map<string, bytes>` field; both fields of every entry, even when empty. */
export function writeMetadata(writer, field, metadata) {
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
export function writeDictionaries(writer, field, infos) {
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
 * Reads the fields of one wire message, checking each known field's wire type and range. A nested
 * message is read by a WireReader over the same BinaryReader that ends where the message does.
 * Every loop over fields calls more() after each one, which fails once a read went past the end,
 * into the parent's bytes; at the top level, BinaryReader fails such a read itself. Every failure
 * is an Error, which the frame layer reports as InvalidArgument.
 */
export class WireReader {
  #bytes;
  #reader;
  #end;

  /** A reader over `bytes`, or, given `reader` and `end`, over the reader's bytes up to `end`. */
  constructor(bytes, reader = new BinaryReader(bytes), end = bytes.length) {
    this.#bytes = bytes;
    this.#reader = reader;
    this.#end = end;
  }

  /** Whether a field follows; throws if the last read went past this reader's end. */
  more() {
    const pos = this.#reader.pos;
    if (pos > this.#end) {
      throw new RangeError("premature EOF");
    }
    return pos < this.#end;
  }

  /**
   * The next field's tag: a field number of at least 1 (`tag >>> 3`) and a wire type of at most 5
   * (`tag & 7`), in at most 32 bits.
   */
  tag() {
    const reader = this.#reader;
    const start = reader.pos;
    const tag = reader.uint32();
    const length = reader.pos - start;
    if (length > 5 || (length === 5 && this.#bytes[reader.pos - 1] > 0x0f)) {
      throw new Error("illegal tag: varint overflows uint32");
    }
    const field = tag >>> 3;
    const wireType = tag & 7;
    if (field <= 0 || wireType > 5) {
      throw new Error(`illegal tag: field no ${field} wire type ${wireType}`);
    }
    return tag;
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

  /** A DictionaryInfo message, `{ id, fingerprint }`. */
  dictionaryInfo(wireType, name) {
    const reader = this.nested(wireType, name);
    const info = { id: 0, fingerprint: new Uint8Array(0) };
    while (reader.more()) {
      const tag = reader.tag();
      const field = tag >>> 3;
      if (field === 1) {
        info.id = reader.uint32(tag & 7, `${name}.id`);
      } else if (field === 2) {
        info.fingerprint = reader.bytes(tag & 7, `${name}.fingerprint`);
      } else {
        reader.skip(tag);
      }
    }
    return info;
  }

  /** A status Detail message, `{ type, value }`. */
  detail(wireType, name) {
    const reader = this.nested(wireType, name);
    const detail = { type: "", value: new Uint8Array(0) };
    while (reader.more()) {
      const tag = reader.tag();
      const field = tag >>> 3;
      if (field === 1) {
        detail.type = reader.string(tag & 7, `${name}.type`);
      } else if (field === 2) {
        detail.value = reader.bytes(tag & 7, `${name}.value`);
      } else {
        reader.skip(tag);
      }
    }
    return detail;
  }

  /** Adds one `map<string, bytes>` entry to `metadata`; a later entry for a key replaces it. */
  metadataEntry(wireType, metadata, name) {
    const reader = this.nested(wireType, name);
    let key = "";
    let value = new Uint8Array(0);
    while (reader.more()) {
      const tag = reader.tag();
      const field = tag >>> 3;
      if (field === 1) {
        key = reader.string(tag & 7, `${name}.key`);
      } else if (field === 2) {
        value = reader.bytes(tag & 7, `${name}.value`);
      } else {
        reader.skip(tag);
      }
    }
    setEntry(metadata, key, value);
  }

  /** Skips the unknown field of `tag`, of any wire type. */
  skip(tag, depth = 0) {
    const field = tag >>> 3;
    switch (tag & 7) {
      case WireType.Varint:
        this.#skipVarint();
        return;
      case WireType.StartGroup:
        this.#skipGroup(field, depth + 1);
        return;
      case WireType.EndGroup:
        throw new Error("protobuf end group tag without a start group");
      default:
        this.#reader.skip(tag & 7, field);
    }
  }

  #skipGroup(field, depth) {
    if (depth > MaxGroupDepth) {
      throw new Error("protobuf group nesting exceeded limit");
    }
    while (this.more()) {
      const tag = this.tag();
      if ((tag & 7) === WireType.EndGroup) {
        if (tag >>> 3 !== field) {
          throw new Error("protobuf group end tag did not match start tag");
        }
        return;
      }
      this.skip(tag, depth);
    }
    throw new Error("unterminated protobuf group");
  }

  /** Skips a varint of at most 64 bits (10 bytes, the last at most 1). */
  #skipVarint() {
    const bytes = this.#bytes;
    const reader = this.#reader;
    for (let index = 0; index < 10; index += 1) {
      if (reader.pos >= this.#end) {
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

  /** A reader over the length-delimited value of field `name`, which this reader skips past. */
  nested(wireType, name) {
    expectWireType(wireType, WireType.LengthDelimited, name);
    const reader = this.#reader;
    const length = reader.uint32();
    const end = reader.pos + length;
    if (end > this.#end) {
      throw new RangeError("premature EOF");
    }
    return new WireReader(this.#bytes, reader, end);
  }
}

function expectWireType(wireType, expected, name) {
  if (wireType !== expected) {
    throw new Error(`field ${name} used incompatible protobuf wire type ${wireType}`);
  }
}
