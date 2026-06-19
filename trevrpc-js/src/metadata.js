import { invalidArgument } from "./status.js";

export const MaxMetadataEntries = 64;
export const MaxMetadataKeyLen = 128;
export const MaxMetadataValueLen = 8 * 1024;
export const MaxMetadataTotalSize = 64 * 1024;
export const ReservedMetadataPrefix = "trevrpc-";

const textEncoder = new TextEncoder();

/** Normalizes a metadata key to lowercase ASCII. */
export function normalizeMetadataKey(key) {
  return String(key).toLowerCase();
}

/** Normalizes metadata keys and converts metadata values to bytes. */
export function normalizeMetadata(metadata = {}) {
  const normalized = Object.create(null);
  const entries = metadata instanceof Map ? metadata.entries() : Object.entries(metadata);

  for (const [key, value] of entries) {
    normalized[normalizeMetadataKey(key)] = metadataValueToBytes(value);
  }

  validateMetadata(normalized);
  return normalized;
}

/** Converts a metadata value into bytes. */
export function metadataValueToBytes(value) {
  if (value instanceof Uint8Array) {
    return value;
  }

  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  if (typeof value === "string") {
    return textEncoder.encode(value);
  }

  if (Array.isArray(value)) {
    return new Uint8Array(value);
  }

  throw invalidArgument("metadata values must be bytes or strings");
}

/** Validates metadata key syntax, value sizes, and total metadata limits. */
export function validateMetadata(metadata = {}) {
  const entries = Object.entries(metadata);
  if (entries.length > MaxMetadataEntries) {
    throw invalidArgument(
      `metadata has ${entries.length} entries, maximum is ${MaxMetadataEntries}`,
    );
  }

  let totalSize = 0;
  for (const [key, value] of entries) {
    validateMetadataKey(key);
    const bytes = metadataValueToBytes(value);
    if (bytes.byteLength > MaxMetadataValueLen) {
      throw invalidArgument(
        `metadata value ${JSON.stringify(key)} is ${bytes.byteLength} bytes, maximum is ${MaxMetadataValueLen}`,
      );
    }

    totalSize += key.length + bytes.byteLength;
  }

  if (totalSize > MaxMetadataTotalSize) {
    throw invalidArgument(`metadata is ${totalSize} bytes, maximum is ${MaxMetadataTotalSize}`);
  }
}

function validateMetadataKey(key) {
  if (key === "") {
    throw invalidArgument("metadata key is empty");
  }

  if (key.length > MaxMetadataKeyLen) {
    throw invalidArgument(
      `metadata key ${JSON.stringify(key)} is ${key.length} bytes, maximum is ${MaxMetadataKeyLen}`,
    );
  }

  if (key.startsWith(ReservedMetadataPrefix)) {
    throw invalidArgument(
      `metadata key ${JSON.stringify(key)} uses reserved prefix ${JSON.stringify(ReservedMetadataPrefix)}`,
    );
  }

  for (let index = 0; index < key.length; index += 1) {
    const code = key.charCodeAt(index);
    const valid =
      (code >= 0x61 && code <= 0x7a) ||
      (code >= 0x30 && code <= 0x39) ||
      code === 0x2e ||
      code === 0x5f ||
      code === 0x2d;
    if (!valid) {
      throw invalidArgument(
        `metadata key ${JSON.stringify(key)} must use lowercase ASCII letters, digits, '.', '_' or '-'`,
      );
    }
  }
}
