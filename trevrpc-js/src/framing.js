import { byteView } from "./bytes.js";
import { Code, invalidArgument, resourceExhausted, unavailable } from "./status.js";
import { RpcStreamFrame, RpcStreamFrameKind, preflightWireMessage } from "./wire.js";

export const DefaultMaxFrameSize = 4 * 1024 * 1024;

const HeaderLength = 4;
/** Tag of RpcStreamFrame.body: field 4, length-delimited. */
const StreamFrameBodyTag = (4 << 3) | 2;
const EmptyBytes = new Uint8Array(0);

/** Encodes a protobuf message body. */
export function marshalMessage(messageType, message) {
  try {
    return messageType.encode(messageType.fromObject(message)).finish();
  } catch (error) {
    throw invalidArgument(
      `failed to encode protobuf message: ${error?.message ?? String(error)}`,
      error,
    );
  }
}

/** Decodes a protobuf message body, rejecting malformed wire data. */
export function unmarshalMessage(messageType, body) {
  try {
    preflightWireMessage(messageType, body);
    return messageType.decode(body);
  } catch (error) {
    throw invalidArgument(
      `failed to decode protobuf message: ${error?.message ?? String(error)}`,
      error,
    );
  }
}

/** Encodes a protobuf message into a length-prefixed frame. */
export function encodeFrame(messageType, message, maxFrameSize = DefaultMaxFrameSize) {
  const body = marshalMessage(messageType, message);
  checkFrameLength(body.byteLength, maxFrameSize);
  const frame = new Uint8Array(HeaderLength + body.byteLength);
  new DataView(frame.buffer).setUint32(0, body.byteLength, false);
  frame.set(body, HeaderLength);
  return frame;
}

/**
 * Encodes a streaming message frame carrying an already-encoded protobuf body.
 * This is byte-identical to encodeFrame(RpcStreamFrame, { body }) without building the
 * intermediate message.
 */
export function encodeMessageFrame(body, maxFrameSize = DefaultMaxFrameSize) {
  const frameLength =
    body.byteLength === 0 ? 0 : 1 + varintLength(body.byteLength) + body.byteLength;
  checkFrameLength(frameLength, maxFrameSize);

  const frame = new Uint8Array(HeaderLength + frameLength);
  new DataView(frame.buffer).setUint32(0, frameLength, false);
  if (body.byteLength > 0) {
    let offset = HeaderLength;
    frame[offset++] = StreamFrameBodyTag;
    offset = writeVarint(frame, offset, body.byteLength);
    frame.set(body, offset);
  }
  return frame;
}

/** Decodes the body of a response stream frame, validating its kind. */
export function decodeStreamFrame(body) {
  const bytes = byteView(body);
  const messageBody = bareMessageBody(bytes);
  if (messageBody !== undefined) {
    return { kind: RpcStreamFrameKind.Message, status: Code.Ok, message: "", body: messageBody };
  }

  const frame = unmarshalMessage(RpcStreamFrame, bytes);
  if (frame.kind !== RpcStreamFrameKind.Message && frame.kind !== RpcStreamFrameKind.Status) {
    throw invalidArgument(`unsupported stream frame kind ${frame.kind}`);
  }
  return frame;
}

/** Validates the body length stored in a frame header. */
export function frameBodyLength(header, maxFrameSize = DefaultMaxFrameSize) {
  const length = new DataView(header.buffer, header.byteOffset, header.byteLength).getUint32(
    0,
    false,
  );
  checkFrameLength(length, maxFrameSize);
  return length;
}

/** Reads length-prefixed frames from a byte stream reader. */
export class FrameReader {
  #reader;
  /** Bytes received from the reader but not yet consumed. */
  #pending = EmptyBytes;

  /** Creates a frame reader over a ReadableStream reader. */
  constructor(reader) {
    this.#reader = reader;
  }

  /** Reads and decodes one frame; a stream that ends first is an error. */
  async readFrame(messageType, maxFrameSize = DefaultMaxFrameSize) {
    return unmarshalMessage(messageType, await this.#readBody(maxFrameSize, false));
  }

  /** Reads one streaming RPC frame, or returns null when the stream ended cleanly. */
  async readStreamFrame(maxFrameSize = DefaultMaxFrameSize) {
    const body = await this.#readBody(maxFrameSize, true);
    return body === null ? null : decodeStreamFrame(body);
  }

  /** Resolves when the stream ends cleanly; rejects if more data arrives first. */
  async expectEnd() {
    if (this.#pending.byteLength === 0) {
      const { value, done } = await this.#nextChunk();
      if (done) {
        return;
      }
      this.#pending = value;
    }
    throw unavailable("stream carried data after the final frame");
  }

  async #readBody(maxFrameSize, allowEnd) {
    const header = await this.#readExact(HeaderLength, allowEnd);
    if (header === null) {
      return null;
    }
    return await this.#readExact(frameBodyLength(header, maxFrameSize), false);
  }

  /** Returns exactly size bytes, or null if allowEnd and the stream ended before the first byte. */
  async #readExact(size, allowEnd) {
    if (this.#pending.byteLength >= size) {
      return this.#take(size);
    }

    // Copies are needed only when a read spans chunks; whole frames inside one chunk are views.
    const out = new Uint8Array(size);
    let filled = this.#pending.byteLength;
    out.set(this.#pending);
    this.#pending = EmptyBytes;
    while (filled < size) {
      const { value, done } = await this.#nextChunk();
      if (done) {
        if (allowEnd && filled === 0) {
          return null;
        }
        throw unavailable("stream ended before a complete frame");
      }
      const count = Math.min(value.byteLength, size - filled);
      out.set(value.subarray(0, count), filled);
      filled += count;
      this.#pending = value.subarray(count);
    }
    return out;
  }

  async #nextChunk() {
    for (;;) {
      const { value, done } = await this.#reader.read();
      if (done) {
        return { done: true };
      }
      if (value != null && value.byteLength > 0) {
        return { done: false, value: byteView(value) };
      }
    }
  }

  #take(size) {
    const bytes = this.#pending.subarray(0, size);
    this.#pending = this.#pending.subarray(size);
    return bytes;
  }
}

function checkFrameLength(length, maxFrameSize) {
  if (length > maxFrameSize) {
    throw resourceExhausted(`frame length ${length} exceeds maximum ${maxFrameSize}`);
  }
}

/** Returns the body of a message-only stream frame, or undefined if it carries anything else. */
function bareMessageBody(bytes) {
  if (bytes.byteLength === 0) {
    return EmptyBytes;
  }
  if (bytes[0] !== StreamFrameBodyTag) {
    return undefined;
  }

  let length = 0;
  let offset = 1;
  for (let shift = 0; shift < 35 && offset < bytes.byteLength; shift += 7) {
    const byte = bytes[offset++];
    length += (byte & 0x7f) * 2 ** shift;
    if (byte < 0x80) {
      return offset + length === bytes.byteLength ? bytes.subarray(offset) : undefined;
    }
  }
  return undefined;
}

function writeVarint(output, offset, value) {
  let remaining = value;
  while (remaining >= 0x80) {
    output[offset++] = (remaining % 0x80) | 0x80;
    remaining = Math.floor(remaining / 0x80);
  }
  output[offset++] = remaining;
  return offset;
}

function varintLength(value) {
  let length = 1;
  for (let remaining = value; remaining >= 0x80; remaining = Math.floor(remaining / 0x80)) {
    length += 1;
  }
  return length;
}
