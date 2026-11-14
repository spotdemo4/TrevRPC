// Protobuf wire-format helpers shared by the payload codec in wire.js and the runtime of the
// generated message codecs in codec.js.

/**
 * Sets a map entry with a string key as an own property, so that a key such as `__proto__` is an
 * entry like any other and never replaces the record's prototype. Assignment does that for every
 * other key, faster than defining the property.
 */
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

/**
 * Whether the varint read from `bytes[start..end)` overflows 32 bits: it is longer than 5 bytes,
 * or its fifth byte carries more than the top 4 bits.
 */
export function overflowsUint32(bytes, start, end) {
  const length = end - start;
  return length > 5 || (length === 5 && bytes[end - 1] > 0x0f);
}

/** Returns `tag`, or throws if its field number is 0 or its wire type invalid. */
export function checkTag(tag) {
  const fieldNo = tag >>> 3;
  const wireType = tag & 7;
  if (fieldNo <= 0 || wireType > 5) {
    throw new Error(`illegal tag: field no ${fieldNo} wire type ${wireType}`);
  }
  return tag;
}
