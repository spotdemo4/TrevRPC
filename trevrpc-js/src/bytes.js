/** Returns a Uint8Array view over bytes, typed-array views, or ArrayBuffers without copying. */
export function byteView(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return new Uint8Array(value ?? 0);
}
