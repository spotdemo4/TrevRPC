// The runtime of the message codecs that protoc-gen-trevrpc-js generates: wire primitives, and
// defineCodec(), which makes a generated codec's encode() and decode(). Generated code does the
// per-field work, so a message type gets straight-line code instead of protobuf-es's per-field
// dispatch. Only generated code imports this module, so apps without codecs don't bundle it.
//
// A codec reads and writes the same protobuf-es messages that toBinary() and fromBinary() do, with
// the same bytes, the same property order, `$unknown` for unknown fields, and the same errors:
// when a codec fails, encode() and decode() run protobuf-es on the same message, so a failure
// throws exactly what protobuf-es throws (and protobuf-es's result is returned if it succeeds).
//
// Decoding differs in two cases, both of malformed input. A known field with another wire type is
// kept in `$unknown`, as Go and Rust keep it, where fromBinary() reads it as its declared type
// anyway. And a codec rejects a nested message whose last field runs past the message's length,
// which fromBinary() accepts: decode() then returns fromBinary()'s result, after decoding twice.
import { create, fromBinary, mergeFromBinary, protoInt64, toBinary } from "@bufbuild/protobuf";

import { HeadroomMin, allocateWithHeadroom } from "./headroom.js";

/**
 * Strings up to this many bytes are decoded in JS when they are ASCII, instead of calling
 * TextDecoder, and strings up to this many UTF-16 units are encoded in JS instead of calling
 * TextEncoder.encodeInto: each native call costs a few hundred nanoseconds in Chromium, which the
 * JS paths beat up to these lengths.
 */
const DecodeJsMax = 64;
const EncodeJsMax = 256;
/** ASCII strings up to this many bytes are built with one String.fromCharCode call. */
const AsciiExactMax = 32;
/** How deeply messages may nest, as in protobuf-es. */
export const RecursionLimit = 100;
/** The capacity a shared writer starts with and shrinks back to. */
const WriterSize = 64 * 1024;
/** A shared writer that grew past this keeps its buffer only while messages still need it. */
const WriterRetainMax = 1024 * 1024;

// Invalid UTF-8 in a proto3 string is an error, as in protobuf-es, and a leading U+FEFF is text.
const utf8Decoder = /* @__PURE__ */ new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const utf8Encoder = /* @__PURE__ */ new TextEncoder();

/** The zero value of implicit bytes fields, shared like protobuf-es shares it. */
export const EmptyBytes = /* @__PURE__ */ new Uint8Array(0);
/** The zero value of 64-bit integer fields: 0n, or "0" where BigInt is unavailable. */
export const Int64Zero = protoInt64.zero;

/**
 * The codec of the messages of `schema`, which generated code defines with its `read` and `write`
 * functions of a message.
 */
export function defineCodec(schema, read, write) {
  return new Codec(schema, read, write);
}

class Codec {
  constructor(schema, read, write) {
    this.schema = schema;
    this.read = read;
    this.write = write;
  }

  /**
   * Encodes `message` to the bytes toBinary() produces. It does not convert init objects: pass a
   * message that create() made, or that a codec or fromBinary() decoded.
   */
  encode(message) {
    return this.#encode(message, false);
  }

  /**
   * Encodes `message` like encode(), into a buffer with room in front for the frame header, so
   * that framing need not copy it into a frame; for trevrpc-js's framing only.
   */
  encodeBody(message) {
    return this.#encode(message, true);
  }

  #encode(message, headroom) {
    const writer = acquireWriter();
    try {
      this.write(writer, message);
      return writer.finish(headroom);
    } catch {
      // The codec rejected the message: protobuf-es throws its own error for it, field name and
      // all. Should protobuf-es accept it after all, its encoding is the right one.
      return toBinary(this.schema, message);
    } finally {
      releaseWriter(writer);
    }
  }

  /** Decodes a message from `bytes`, as fromBinary() does. */
  decode(bytes) {
    try {
      return this.read(new Reader(bytes), bytes.length, 0, undefined);
    } catch {
      return fromBinary(this.schema, bytes);
    }
  }
}

/**
 * Reads a message of `schema` with protobuf-es, for message types that have no generated codec:
 * well-known types, types from files that weren't generated, and types whose protobuf-es
 * representation the generator does not write code for.
 */
export function readMessage(schema, reader, end, depth, into) {
  const bytes = reader.buf.subarray(reader.pos, end);
  reader.pos = end;
  const options = { recursionLimit: RecursionLimit - depth };
  return into === undefined
    ? fromBinary(schema, bytes, options)
    : mergeFromBinary(schema, into, bytes, options);
}

/** Writes a message of `schema` with protobuf-es, the counterpart of readMessage(). */
export function writeMessage(schema, writer, message) {
  writer.raw(toBinary(schema, message));
}

/** A new message of `schema` with no fields set: the value of a map entry that has none. */
export function emptyMessage(schema) {
  return create(schema);
}

/** Throws the error of a message nested more than RecursionLimit deep. */
export function depthError(typeName) {
  throw new Error(
    `cannot decode message ${typeName} from binary: maximum recursion depth of ${RecursionLimit} reached`,
  );
}

/** Throws the error of a message of another type in a field. */
export function foreignError(typeName, message) {
  throw new Error(`cannot encode message ${message?.$typeName} as ${typeName}`);
}

/** Sets a map entry with a string key, which may be "__proto__", as an own property. */
export function setMapEntry(record, key, value) {
  if (key === "__proto__") {
    Object.defineProperty(record, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  } else {
    record[key] = value;
  }
}

/** Converts a map key, always a string in the record, back to an integer of 32 bits or less. */
export function intKey(key) {
  const n = Number.parseInt(key);
  return Number.isFinite(n) ? n : key;
}

/** Converts a map key back to a bool. */
export function boolKey(key) {
  return key === "true" ? true : key === "false" ? false : key;
}

/** Converts a map key back to a signed 64-bit integer. */
export function int64Key(key) {
  try {
    return protoInt64.parse(key);
  } catch {
    return key;
  }
}

/** Converts a map key back to an unsigned 64-bit integer. */
export function uint64Key(key) {
  try {
    return protoInt64.uParse(key);
  } catch {
    return key;
  }
}

/** Reads the protobuf wire format from a byte array. */
export class Reader {
  constructor(buf) {
    this.buf = buf;
    this.len = buf.length;
    this.pos = 0;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    // The halves of the last 64-bit varint read.
    this.lo = 0;
    this.hi = 0;
  }

  /**
   * Reads a tag. A tag of one byte, the common case, needs no checks; field number 0 and invalid
   * wire types end up in unknown(), which rejects them.
   */
  tag() {
    const b = this.buf[this.pos];
    if (b < 0x80) {
      this.pos++;
      return b;
    }
    const start = this.pos;
    const tag = this.uint32();
    const length = this.pos - start;
    if (length > 5 || (length === 5 && this.buf[this.pos - 1] > 0x0f)) {
      throw new Error("illegal tag: varint overflows uint32");
    }
    return tag;
  }

  /** Reads a varint and keeps its low 32 bits, unsigned. */
  uint32() {
    const buf = this.buf;
    let b = buf[this.pos++];
    let v = b & 0x7f;
    if (b < 0x80) {
      return v;
    }
    b = buf[this.pos++];
    v |= (b & 0x7f) << 7;
    if (b < 0x80) {
      return v;
    }
    b = buf[this.pos++];
    v |= (b & 0x7f) << 14;
    if (b < 0x80) {
      return v;
    }
    b = buf[this.pos++];
    v |= (b & 0x7f) << 21;
    if (b < 0x80) {
      return v;
    }
    b = buf[this.pos++];
    v |= (b & 0x0f) << 28;
    if (b < 0x80) {
      return v >>> 0;
    }
    // Negative int32 values are sign-extended to 10 bytes; the rest is dropped.
    for (let i = 0; i < 5; i++) {
      if (buf[this.pos++] < 0x80) {
        return v >>> 0;
      }
    }
    throw new Error("invalid varint");
  }

  int32() {
    return this.uint32() | 0;
  }

  sint32() {
    const n = this.uint32();
    return (n >>> 1) ^ -(n & 1);
  }

  bool() {
    const b = this.buf[this.pos];
    if (b < 0x80) {
      this.pos++;
      return b !== 0;
    }
    this.varint64();
    return this.lo !== 0 || this.hi !== 0;
  }

  double() {
    const v = this.view.getFloat64(this.pos, true);
    this.pos += 8;
    return v;
  }

  float() {
    const v = this.view.getFloat32(this.pos, true);
    this.pos += 4;
    return v;
  }

  fixed32() {
    const v = this.view.getUint32(this.pos, true);
    this.pos += 4;
    return v;
  }

  sfixed32() {
    const v = this.view.getInt32(this.pos, true);
    this.pos += 4;
    return v;
  }

  /** Reads a 64-bit varint into `lo` and `hi`, as protobuf-es does. */
  varint64() {
    const buf = this.buf;
    let pos = this.pos;
    let lo = 0;
    let hi = 0;
    for (let shift = 0; shift < 28; shift += 7) {
      const b = buf[pos++];
      lo |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) {
        this.end64(pos, lo, hi);
        return;
      }
    }
    const middle = buf[pos++];
    lo |= (middle & 0x0f) << 28;
    hi = (middle & 0x70) >> 4;
    if ((middle & 0x80) === 0) {
      this.end64(pos, lo, hi);
      return;
    }
    for (let shift = 3; shift <= 31; shift += 7) {
      const b = buf[pos++];
      hi |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) {
        this.end64(pos, lo, hi);
        return;
      }
    }
    throw new Error("invalid varint");
  }

  end64(pos, lo, hi) {
    if (pos > this.len) {
      throw new RangeError("premature EOF");
    }
    this.pos = pos;
    this.lo = lo;
    this.hi = hi;
  }

  int64() {
    this.varint64();
    return protoInt64.dec(this.lo, this.hi);
  }

  uint64() {
    this.varint64();
    return protoInt64.uDec(this.lo, this.hi);
  }

  sint64() {
    this.varint64();
    const s = -(this.lo & 1);
    const lo = ((this.lo >>> 1) | ((this.hi & 1) << 31)) ^ s;
    const hi = (this.hi >>> 1) ^ s;
    return protoInt64.dec(lo, hi);
  }

  fixed64() {
    const lo = this.sfixed32();
    return protoInt64.uDec(lo, this.sfixed32());
  }

  sfixed64() {
    const lo = this.sfixed32();
    return protoInt64.dec(lo, this.sfixed32());
  }

  /** Reads a length prefix and returns where the data it delimits ends. */
  lenEnd() {
    const end = this.uint32() + this.pos;
    if (end > this.len) {
      throw new RangeError("premature EOF");
    }
    return end;
  }

  /** Reads a string; invalid UTF-8 is an error. */
  string() {
    const end = this.lenEnd();
    const start = this.pos;
    this.pos = end;
    const n = end - start;
    if (n <= DecodeJsMax) {
      const buf = this.buf;
      let or = 0;
      for (let i = start; i < end; i++) {
        or |= buf[i];
      }
      if (or < 0x80) {
        return n <= AsciiExactMax ? asciiExact(buf, start, n) : asciiChunks(buf, start, end);
      }
    }
    return utf8Decoder.decode(this.buf.subarray(start, end));
  }

  /** Reads bytes as a view into the buffer, as protobuf-es does. */
  bytes() {
    const end = this.lenEnd();
    const start = this.pos;
    this.pos = end;
    return this.buf.subarray(start, end);
  }

  /**
   * Skips a field of wireType; groups are skipped up to `depthLeft` levels deep, and must end with
   * the tag of fieldNo.
   */
  skip(wireType, fieldNo, depthLeft) {
    switch (wireType) {
      case 0:
        while (this.buf[this.pos++] & 0x80) {
          // The continuation bits are all there is to read.
        }
        break;
      case 1:
        this.pos += 8;
        break;
      case 2: {
        // Not `this.pos += this.uint32()`, which would add the length to the position before it.
        const len = this.uint32();
        this.pos += len;
        break;
      }
      case 3:
        if (depthLeft <= 0) {
          throw new Error("maximum recursion depth reached");
        }
        for (;;) {
          const tag = this.checkedTag();
          if ((tag & 7) === 4) {
            if (tag >>> 3 !== fieldNo) {
              throw new Error("invalid end group tag");
            }
            break;
          }
          this.skip(tag & 7, tag >>> 3, depthLeft - 1);
        }
        break;
      case 5:
        this.pos += 4;
        break;
      default:
        throw new Error(`cant skip wire type ${wireType}`);
    }
    if (this.pos > this.len) {
      throw new RangeError("premature EOF");
    }
  }

  /** Reads a tag, rejecting field number 0 and invalid wire types. */
  checkedTag() {
    return checkTag(this.tag());
  }

  /**
   * Skips the field of `tag`, which no field of the message at `depth` has, and records it in the
   * message's `$unknown`, as protobuf-es does.
   */
  unknown(message, tag, depth) {
    checkTag(tag);
    const no = tag >>> 3;
    const wireType = tag & 7;
    const start = this.pos;
    this.skip(wireType, no, RecursionLimit - depth - 1);
    (message.$unknown ??= []).push({ no, wireType, data: this.buf.subarray(start, this.pos) });
  }

  /**
   * Skips a field of a map entry other than its key and value, rejecting the tags that unknown()
   * rejects, as protobuf-es does inside a map entry too.
   */
  skipTag(tag, depth) {
    checkTag(tag);
    this.skip(tag & 7, tag >>> 3, RecursionLimit - depth - 1);
  }

  // Packed repeated fields. Assigning by index into an array sized once is much faster than
  // pushing elements one by one, so fixed-width readers size from the byte length, and varint
  // readers count the bytes that end a varint (each has exactly one below 0x80) first. Malformed
  // data cannot over- or under-run unnoticed: checkEnd() throws if the field doesn't end exactly
  // where its length said.

  packedDouble(a) {
    const end = this.lenEnd();
    const view = this.view;
    let pos = this.pos;
    let i = a.length;
    const n = (end - pos) >>> 3;
    a.length = i + n;
    for (let k = 0; k < n; k++, pos += 8) {
      a[i++] = view.getFloat64(pos, true);
    }
    this.pos = pos;
    this.checkEnd(end);
  }

  packedFloat(a) {
    const end = this.lenEnd();
    const view = this.view;
    let pos = this.pos;
    let i = a.length;
    const n = (end - pos) >>> 2;
    a.length = i + n;
    for (let k = 0; k < n; k++, pos += 4) {
      a[i++] = view.getFloat32(pos, true);
    }
    this.pos = pos;
    this.checkEnd(end);
  }

  packedFixed32(a) {
    const end = this.lenEnd();
    const view = this.view;
    let pos = this.pos;
    let i = a.length;
    const n = (end - pos) >>> 2;
    a.length = i + n;
    for (let k = 0; k < n; k++, pos += 4) {
      a[i++] = view.getUint32(pos, true);
    }
    this.pos = pos;
    this.checkEnd(end);
  }

  packedSfixed32(a) {
    const end = this.lenEnd();
    const view = this.view;
    let pos = this.pos;
    let i = a.length;
    const n = (end - pos) >>> 2;
    a.length = i + n;
    for (let k = 0; k < n; k++, pos += 4) {
      a[i++] = view.getInt32(pos, true);
    }
    this.pos = pos;
    this.checkEnd(end);
  }

  /** Sizes `a` for the varints that end before `end` and returns the index of the first. */
  presize(a, end) {
    const buf = this.buf;
    let n = 0;
    for (let p = this.pos; p < end; p++) {
      if (buf[p] < 0x80) {
        n++;
      }
    }
    const i = a.length;
    a.length = i + n;
    return i;
  }

  packedUint32(a) {
    const end = this.lenEnd();
    let i = this.presize(a, end);
    const buf = this.buf;
    while (this.pos < end) {
      const b = buf[this.pos];
      if (b < 0x80) {
        a[i++] = b;
        this.pos++;
      } else {
        a[i++] = this.uint32();
      }
    }
    this.checkEnd(end);
  }

  packedInt32(a) {
    const end = this.lenEnd();
    let i = this.presize(a, end);
    const buf = this.buf;
    while (this.pos < end) {
      const b = buf[this.pos];
      if (b < 0x80) {
        a[i++] = b;
        this.pos++;
      } else {
        a[i++] = this.int32();
      }
    }
    this.checkEnd(end);
  }

  packedSint32(a) {
    const end = this.lenEnd();
    let i = this.presize(a, end);
    const buf = this.buf;
    while (this.pos < end) {
      const b = buf[this.pos];
      if (b < 0x80) {
        a[i++] = (b >>> 1) ^ -(b & 1);
        this.pos++;
      } else {
        a[i++] = this.sint32();
      }
    }
    this.checkEnd(end);
  }

  packedBool(a) {
    const end = this.lenEnd();
    let i = this.presize(a, end);
    const buf = this.buf;
    while (this.pos < end) {
      const b = buf[this.pos];
      if (b < 0x80) {
        a[i++] = b !== 0;
        this.pos++;
      } else {
        a[i++] = this.bool();
      }
    }
    this.checkEnd(end);
  }

  /**
   * Reads packed 64-bit integers with `read`, a method of this reader, converting each to a string
   * if `asString`.
   */
  packedLong(a, read, asString) {
    const end = this.lenEnd();
    while (this.pos < end) {
      const v = read.call(this);
      a.push(asString ? String(v) : v);
    }
    this.checkEnd(end);
  }

  /** Throws unless a message or packed field ended exactly where its length said. */
  checkEnd(end) {
    if (this.pos !== end) {
      throw new RangeError("premature EOF");
    }
  }
}

/** Returns `tag`, or throws if its field number is 0 or its wire type invalid. */
function checkTag(tag) {
  const fieldNo = tag >>> 3;
  const wireType = tag & 7;
  if (fieldNo <= 0 || wireType > 5) {
    throw new Error(`illegal tag: field no ${fieldNo} wire type ${wireType}`);
  }
  return tag;
}

/**
 * Builds a short ASCII string with one String.fromCharCode call of exactly its length: one
 * allocation and no intermediate strings, which beats appending in Chromium.
 */
function asciiExact(b, i, n) {
  switch (n) {
    case 0:
      return "";
    case 1:
      return String.fromCharCode(b[i]);
    case 2:
      return String.fromCharCode(b[i], b[i + 1]);
    case 3:
      return String.fromCharCode(b[i], b[i + 1], b[i + 2]);
    case 4:
      return String.fromCharCode(b[i], b[i + 1], b[i + 2], b[i + 3]);
    case 5:
      return String.fromCharCode(b[i], b[i + 1], b[i + 2], b[i + 3], b[i + 4]);
    case 6:
      return String.fromCharCode(b[i], b[i + 1], b[i + 2], b[i + 3], b[i + 4], b[i + 5]);
    case 7:
      return String.fromCharCode(b[i], b[i + 1], b[i + 2], b[i + 3], b[i + 4], b[i + 5], b[i + 6]);
    case 8:
      return String.fromCharCode(
        b[i],
        b[i + 1],
        b[i + 2],
        b[i + 3],
        b[i + 4],
        b[i + 5],
        b[i + 6],
        b[i + 7],
      );
    default:
      return asciiExactLong(b, i, n);
  }
}

/** asciiExact() for 9 to AsciiExactMax bytes: eight characters, then the rest. */
function asciiExactLong(b, i, n) {
  const head = String.fromCharCode(
    b[i],
    b[i + 1],
    b[i + 2],
    b[i + 3],
    b[i + 4],
    b[i + 5],
    b[i + 6],
    b[i + 7],
  );
  return n <= 16 ? head + asciiExact(b, i + 8, n - 8) : head + asciiExactLong(b, i + 8, n - 8);
}

/** Decodes ASCII eight characters at a time. */
function asciiChunks(buf, i, end) {
  let str = "";
  for (; i + 7 < end; i += 8) {
    str += String.fromCharCode(
      buf[i],
      buf[i + 1],
      buf[i + 2],
      buf[i + 3],
      buf[i + 4],
      buf[i + 5],
      buf[i + 6],
      buf[i + 7],
    );
  }
  while (i < end) {
    str += String.fromCharCode(buf[i++]);
  }
  return str;
}

function varintSize(v) {
  return v < 0x80 ? 1 : v < 0x4000 ? 2 : v < 0x200000 ? 3 : v < 0x10000000 ? 4 : 5;
}

/**
 * Writes the protobuf wire format. Field values are checked as protobuf-es checks them: integers
 * out of range or not integers at all, and floats too large for 32 bits, are errors.
 */
export class Writer {
  constructor(size = WriterSize) {
    this.buf = new Uint8Array(size);
    this.view = new DataView(this.buf.buffer);
    this.pos = 0;
  }

  ensure(n) {
    if (this.pos + n > this.buf.length) {
      this.grow(n);
    }
  }

  grow(n) {
    let size = this.buf.length * 2;
    while (size < this.pos + n) {
      size *= 2;
    }
    const buf = new Uint8Array(size);
    buf.set(this.buf.subarray(0, this.pos));
    this.buf = buf;
    this.view = new DataView(buf.buffer);
  }

  /**
   * Returns a copy of the bytes written; with `headroom`, after room for a frame header if there
   * are HeadroomMin bytes or more, which is when that is cheaper than framing a plain copy.
   */
  finish(headroom = false) {
    if (!headroom || this.pos < HeadroomMin) {
      return this.buf.slice(0, this.pos);
    }
    const bytes = allocateWithHeadroom(this.pos);
    bytes.set(this.buf.subarray(0, this.pos));
    return bytes;
  }

  /** Writes one byte, such as a tag that fits in one. */
  byte(b) {
    this.ensure(1);
    this.buf[this.pos++] = b;
  }

  /** Writes a varint of up to 32 bits, unchecked: a tag or a length. */
  varint(v) {
    this.ensure(5);
    const buf = this.buf;
    while (v > 0x7f) {
      buf[this.pos++] = (v & 0x7f) | 0x80;
      v >>>= 7;
    }
    buf[this.pos++] = v;
  }

  uint32(v) {
    this.varint(uint32Value(v));
  }

  int32(v) {
    v = int32Value(v);
    if (v >= 0) {
      this.varint(v);
      return;
    }
    // Negative int32 values are sign-extended to 64 bits: always 10 bytes.
    this.ensure(10);
    const buf = this.buf;
    for (let i = 0; i < 9; i++) {
      buf[this.pos++] = (v & 0x7f) | 0x80;
      v >>= 7;
    }
    buf[this.pos++] = 1;
  }

  sint32(v) {
    v = int32Value(v);
    this.varint(((v << 1) ^ (v >> 31)) >>> 0);
  }

  bool(v) {
    this.ensure(1);
    this.buf[this.pos++] = v ? 1 : 0;
  }

  double(v) {
    this.ensure(8);
    this.view.setFloat64(this.pos, v, true);
    this.pos += 8;
  }

  float(v) {
    v = float32Value(v);
    this.ensure(4);
    this.view.setFloat32(this.pos, v, true);
    this.pos += 4;
  }

  fixed32(v) {
    v = uint32Value(v);
    this.ensure(4);
    this.view.setUint32(this.pos, v, true);
    this.pos += 4;
  }

  sfixed32(v) {
    v = int32Value(v);
    this.ensure(4);
    this.view.setInt32(this.pos, v, true);
    this.pos += 4;
  }

  /** Writes a 64-bit varint given as its halves. */
  varint64(lo, hi) {
    this.ensure(10);
    const buf = this.buf;
    while (hi !== 0 || lo > 0x7f || lo < 0) {
      buf[this.pos++] = (lo & 0x7f) | 0x80;
      lo = ((lo >>> 7) | (hi << 25)) >>> 0;
      hi >>>= 7;
    }
    buf[this.pos++] = lo;
  }

  int64(v) {
    const tc = protoInt64.enc(v);
    this.varint64(tc.lo >>> 0, tc.hi >>> 0);
  }

  uint64(v) {
    const tc = protoInt64.uEnc(v);
    this.varint64(tc.lo >>> 0, tc.hi >>> 0);
  }

  sint64(v) {
    const tc = protoInt64.enc(v);
    const sign = tc.hi >> 31;
    const lo = (tc.lo << 1) ^ sign;
    const hi = ((tc.hi << 1) | (tc.lo >>> 31)) ^ sign;
    this.varint64(lo >>> 0, hi >>> 0);
  }

  fixed64(v) {
    const tc = protoInt64.uEnc(v);
    this.fixed64Halves(tc.lo, tc.hi);
  }

  sfixed64(v) {
    const tc = protoInt64.enc(v);
    this.fixed64Halves(tc.lo, tc.hi);
  }

  fixed64Halves(lo, hi) {
    this.ensure(8);
    this.view.setInt32(this.pos, lo, true);
    this.view.setInt32(this.pos + 4, hi, true);
    this.pos += 8;
  }

  bytes(v) {
    const n = v.byteLength;
    this.varint(uint32Value(n));
    this.raw(v);
  }

  /** Writes bytes as they are, without a length prefix. */
  raw(v) {
    this.ensure(v.length);
    this.buf.set(v, this.pos);
    this.pos += v.length;
  }

  string(s) {
    if (typeof s !== "string") {
      // As in protobuf-es.
      s = String(s);
    }
    const n = s.length;
    if (n <= 42) {
      // At most 3 UTF-8 bytes per UTF-16 unit, so the length is at most 126 and its prefix one
      // byte: write the ASCII run directly with locals.
      this.ensure(1 + n * 3);
      const buf = this.buf;
      const at = this.pos;
      let p = at + 1;
      let i = 0;
      for (; i < n; i++) {
        const c = s.charCodeAt(i);
        if (c >= 0x80) {
          break;
        }
        buf[p++] = c;
      }
      if (i < n) {
        p = utf8Write(s, i, buf, p);
      }
      buf[at] = p - at - 1;
      this.pos = p;
      return;
    }
    // At most 3 bytes per UTF-16 unit. Reserve the smallest length prefix the result can have and
    // widen it afterwards if the encoded length needs more.
    this.ensure(5 + n * 3);
    const at = this.pos;
    const reserved = n * 3 < 0x80 ? 1 : varintSize(n);
    const start = at + reserved;
    const len =
      n > EncodeJsMax
        ? utf8Encoder.encodeInto(s, this.buf.subarray(start)).written
        : utf8Write(s, 0, this.buf, start) - start;
    const size = varintSize(len);
    if (size !== reserved) {
      this.buf.copyWithin(at + size, start, start + len);
    }
    this.pos = at;
    this.varint(len);
    this.pos = at + size + len;
  }

  // Packed repeated fields, written with one capacity check and a local position. Varint lists
  // compute their exact byte length first, so the length prefix never needs widening.

  packedDouble(a) {
    const n = a.length;
    const len = n * 8;
    this.varint(len);
    this.ensure(len);
    const view = this.view;
    let pos = this.pos;
    for (let i = 0; i < n; i++, pos += 8) {
      view.setFloat64(pos, a[i], true);
    }
    this.pos = pos;
  }

  packedFloat(a) {
    const n = a.length;
    for (let i = 0; i < n; i++) {
      float32Value(a[i]);
    }
    const len = n * 4;
    this.varint(len);
    this.ensure(len);
    const view = this.view;
    let pos = this.pos;
    for (let i = 0; i < n; i++, pos += 4) {
      view.setFloat32(pos, a[i], true);
    }
    this.pos = pos;
  }

  packedFixed32(a) {
    const n = a.length;
    for (let i = 0; i < n; i++) {
      uint32Value(a[i]);
    }
    const len = n * 4;
    this.varint(len);
    this.ensure(len);
    const view = this.view;
    let pos = this.pos;
    for (let i = 0; i < n; i++, pos += 4) {
      view.setUint32(pos, a[i], true);
    }
    this.pos = pos;
  }

  packedSfixed32(a) {
    const n = a.length;
    for (let i = 0; i < n; i++) {
      int32Value(a[i]);
    }
    const len = n * 4;
    this.varint(len);
    this.ensure(len);
    const view = this.view;
    let pos = this.pos;
    for (let i = 0; i < n; i++, pos += 4) {
      view.setInt32(pos, a[i], true);
    }
    this.pos = pos;
  }

  packedUint32(a) {
    const n = a.length;
    let len = 0;
    for (let i = 0; i < n; i++) {
      len += varintSize(uint32Value(a[i]));
    }
    this.varint(len);
    this.ensure(len);
    const buf = this.buf;
    let pos = this.pos;
    for (let i = 0; i < n; i++) {
      let v = +a[i];
      while (v > 0x7f) {
        buf[pos++] = (v & 0x7f) | 0x80;
        v >>>= 7;
      }
      buf[pos++] = v;
    }
    this.pos = pos;
  }

  packedInt32(a) {
    const n = a.length;
    let len = 0;
    for (let i = 0; i < n; i++) {
      const v = int32Value(a[i]);
      len += v >= 0 ? varintSize(v) : 10;
    }
    this.varint(len);
    // int32() checks capacity itself, and does again here: values are numbers by now.
    for (let i = 0; i < n; i++) {
      this.int32(+a[i]);
    }
  }

  packedSint32(a) {
    const n = a.length;
    let len = 0;
    for (let i = 0; i < n; i++) {
      const v = int32Value(a[i]);
      len += varintSize(((v << 1) ^ (v >> 31)) >>> 0);
    }
    this.varint(len);
    for (let i = 0; i < n; i++) {
      this.sint32(+a[i]);
    }
  }

  packedBool(a) {
    const n = a.length;
    this.varint(n);
    this.ensure(n);
    const buf = this.buf;
    let pos = this.pos;
    for (let i = 0; i < n; i++) {
      buf[pos++] = a[i] ? 1 : 0;
    }
    this.pos = pos;
  }

  /** Writes packed 64-bit integers with `write`, a method of this writer. */
  packedLong(a, write) {
    const at = this.fork();
    for (let i = 0; i < a.length; i++) {
      write.call(this, a[i]);
    }
    this.join(at);
  }

  /** Reserves a 1-byte length prefix for a nested message; returns its position for join(). */
  fork() {
    this.ensure(1);
    return this.pos++;
  }

  /** Writes the length prefix that fork() reserved, widening it if the data is 128 bytes or more. */
  join(at) {
    const len = this.pos - at - 1;
    if (len < 0x80) {
      this.buf[at] = len;
      return;
    }
    const size = varintSize(len);
    this.ensure(size - 1);
    const buf = this.buf;
    buf.copyWithin(at + size, at + 1, this.pos);
    const end = this.pos + size - 1;
    this.pos = at;
    this.varint(len);
    this.pos = end;
  }

  /** Writes the unknown fields of a message, as protobuf-es does. */
  unknown(fields) {
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      this.varint(uint32Value(((field.no << 3) | field.wireType) >>> 0));
      this.raw(field.data);
    }
  }
}

/** Encodes s[i..] as UTF-8 at buf[p], returning the end. Lone surrogates become U+FFFD. */
function utf8Write(s, i, buf, p) {
  for (; i < s.length; i++) {
    let c = s.charCodeAt(i);
    if (c < 0x80) {
      buf[p++] = c;
    } else if (c < 0x800) {
      buf[p++] = 0xc0 | (c >> 6);
      buf[p++] = 0x80 | (c & 0x3f);
    } else if (c >= 0xd800 && c < 0xe000) {
      const c2 = c < 0xdc00 && i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (c2 >= 0xdc00 && c2 < 0xe000) {
        i++;
        c = 0x10000 + ((c - 0xd800) << 10) + (c2 - 0xdc00);
        buf[p++] = 0xf0 | (c >> 18);
        buf[p++] = 0x80 | ((c >> 12) & 0x3f);
        buf[p++] = 0x80 | ((c >> 6) & 0x3f);
        buf[p++] = 0x80 | (c & 0x3f);
      } else {
        buf[p++] = 0xef;
        buf[p++] = 0xbf;
        buf[p++] = 0xbd;
      }
    } else {
      buf[p++] = 0xe0 | (c >> 12);
      buf[p++] = 0x80 | ((c >> 6) & 0x3f);
      buf[p++] = 0x80 | (c & 0x3f);
    }
  }
  return p;
}

// Value checks, as protobuf-es makes them: numbers and numeric strings are accepted, anything
// else, and numbers out of range, are errors. Each returns the value as a number.

function int32Value(v) {
  if ((v | 0) === v) {
    return v;
  }
  const n = typeof v === "string" ? Number(v) : v;
  if (typeof n !== "number" || (n | 0) !== n) {
    throw new Error(`invalid int32: ${typeof v === "number" ? v : typeof v}`);
  }
  return n;
}

function uint32Value(v) {
  if (v >>> 0 === v) {
    return v;
  }
  const n = typeof v === "string" ? Number(v) : v;
  if (typeof n !== "number" || n >>> 0 !== n) {
    throw new Error(`invalid uint32: ${typeof v === "number" ? v : typeof v}`);
  }
  return n;
}

function float32Value(v) {
  let n = v;
  if (typeof v === "string") {
    n = Number(v);
    if (Number.isNaN(n) && v !== "NaN") {
      throw new Error(`invalid float32: ${v}`);
    }
  } else if (typeof v !== "number") {
    throw new Error(`invalid float32: ${typeof v}`);
  }
  // A value too large for a float32 rounds to infinity; NaN and infinity themselves are valid.
  if (!Number.isFinite(Math.fround(n)) && Number.isFinite(n)) {
    throw new Error(`invalid float32: ${n}`);
  }
  return n;
}

// Encoding never calls user code except through String() of a value in a string field, but a
// writer in use is never handed out again, so even that can't corrupt a message.
// It is made on first use, which keeps this module free of side effects.
let sharedWriter;
let sharedWriterBusy = false;

function acquireWriter() {
  if (sharedWriterBusy) {
    return new Writer(1024);
  }
  sharedWriterBusy = true;
  sharedWriter ??= new Writer();
  sharedWriter.pos = 0;
  return sharedWriter;
}

function releaseWriter(writer) {
  if (writer !== sharedWriter) {
    return;
  }
  // One large message must not keep a large buffer alive for the life of the page: the buffer
  // stays only while messages still use a quarter of it.
  if (writer.buf.length > WriterRetainMax && writer.pos < writer.buf.length / 4) {
    sharedWriter = new Writer();
  }
  sharedWriterBusy = false;
}
