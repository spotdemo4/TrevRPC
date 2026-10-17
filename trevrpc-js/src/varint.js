// QUIC variable-length integers (RFC 9000 §16), shared by the frame layer and QMux. Values below
// 2^53 are numbers; larger codepoints are bigints.

const MaxSafe = BigInt(Number.MAX_SAFE_INTEGER);

/** Encoded length of a varint. */
export function varintLength(value) {
  if (typeof value === "number" && value >= 0 && value < 0x4000_0000) {
    return value < 0x40 ? 1 : value < 0x4000 ? 2 : 4;
  }
  const v = typeof value === "bigint" ? value : BigInt(value);
  if (v < 0x40n) {
    return 1;
  }
  if (v < 0x4000n) {
    return 2;
  }
  if (v < 0x4000_0000n) {
    return 4;
  }
  if (v < 0x4000_0000_0000_0000n) {
    return 8;
  }
  throw new RangeError(`varint ${v} exceeds 2^62 - 1`);
}

/** Writes a varint at offset and returns the offset after it. */
export function writeVarint(bytes, offset, value) {
  const length = varintLength(value);
  if (typeof value === "number" && length < 8 && Number.isInteger(value)) {
    // Fast path without BigInt for the common small values.
    let v = value;
    for (let i = length - 1; i >= 0; i -= 1) {
      bytes[offset + i] = v % 256;
      v = Math.floor(v / 256);
    }
    bytes[offset] |= [0, 0x40, 0, 0x80][length - 1];
    return offset + length;
  }
  let v = typeof value === "bigint" ? value : BigInt(value);
  for (let i = length - 1; i >= 0; i -= 1) {
    bytes[offset + i] = Number(v & 0xffn);
    v >>= 8n;
  }
  bytes[offset] |= [0, 0x40, 0, 0x80, 0, 0, 0, 0xc0][length - 1];
  return offset + length;
}

/**
 * Reads a varint at offset. Returns `{ value, next }`, or null when the bytes end first. Values
 * above 2^53 - 1 are bigints.
 */
export function readVarint(bytes, offset) {
  if (offset >= bytes.length) {
    return null;
  }
  const length = 1 << (bytes[offset] >> 6);
  if (offset + length > bytes.length) {
    return null;
  }
  if (length < 8) {
    let value = bytes[offset] & 0x3f;
    for (let i = 1; i < length; i += 1) {
      value = value * 256 + bytes[offset + i];
    }
    return { value, next: offset + length };
  }
  let value = BigInt(bytes[offset] & 0x3f);
  for (let i = 1; i < 8; i += 1) {
    value = (value << 8n) | BigInt(bytes[offset + i]);
  }
  return { value: value <= MaxSafe ? Number(value) : value, next: offset + 8 };
}
