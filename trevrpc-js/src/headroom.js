// Spare bytes in front of an encoded message, which framing fills with the frame header instead of
// copying the message into a new frame. Generated codecs encode streamed request messages of
// HeadroomMin bytes or more into such a buffer (see codec.js). Framing writes into it only when
// this module made the buffer, and only in front of a view of exactly the bytes it handed out
// (the view itself, or another view of the same bytes), so it never writes to memory that isn't
// its own.

/** The frame type and the payload length, as QUIC varints: 1 byte for a type below 64, up to 8. */
export const FrameHeadroom = 9;

/**
 * Messages this size or larger get headroom. Below it, copying into a new frame is cheaper: both
 * Chromium and Firefox keep small typed arrays inline, where an ArrayBuffer of its own costs a few
 * hundred nanoseconds. Measured with encode and framing together, in place against copied, in a
 * microbenchmark: Chromium 156 gained 27-33% from 64 to 512 bytes, but Firefox 157 lost 24-119%
 * there, and both gain from 1 KiB (Chromium 28-37%, Firefox 4-6% at 1-4 KiB).
 */
export const HeadroomMin = 1024;

// Marks the buffers this module made. A property only this module can name costs next to nothing,
// where a WeakSet costs 100-250 ns per message in Chromium and Firefox.
const Owned = Symbol("trevrpc.headroom");

/**
 * A new buffer of FrameHeadroom + `length` bytes, as a view of its last `length` bytes: the
 * message goes there, and its frame header in front of it.
 */
export function allocateWithHeadroom(length) {
  const buffer = new ArrayBuffer(FrameHeadroom + length);
  buffer[Owned] = true;
  return new Uint8Array(buffer, FrameHeadroom, length);
}

/**
 * How many bytes framing may write in front of `bytes`: FrameHeadroom for a view of exactly the
 * bytes allocateWithHeadroom() handed out, 0 for any other bytes.
 */
export function headroomOf(bytes) {
  return bytes.byteOffset === FrameHeadroom &&
    bytes.byteLength + FrameHeadroom === bytes.buffer.byteLength &&
    bytes.buffer[Owned] === true
    ? FrameHeadroom
    : 0;
}
