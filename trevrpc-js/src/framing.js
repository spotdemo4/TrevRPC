import { byteView } from "./bytes.js";
import { internal, invalidArgument, resourceExhausted, unavailable } from "./status.js";
import { readVarint, varintLength, writeVarint } from "./varint.js";
import { RpcHeaders, RpcRequest, RpcStatus, preflightWireMessage } from "./wire.js";

export const DefaultMaxFrameSize = 4 * 1024 * 1024;

/** Frame types; every other type is unknown and skipped by receivers. */
export const FrameType = Object.freeze({
  Message: 0,
  Request: 1,
  Headers: 2,
  Status: 3,
});

const KnownFrameTypes = new Set(Object.values(FrameType));
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

/** Encodes one frame carrying an already-encoded payload. */
export function encodeFrame(type, payload, maxFrameSize = DefaultMaxFrameSize) {
  checkFrameLength(payload.byteLength, maxFrameSize);
  const frame = new Uint8Array(frameSize(type, payload.byteLength));
  writeFrame(frame, 0, type, payload);
  return frame;
}

/** Encodes a MESSAGE frame; the payload is the raw protobuf message. */
export function encodeMessageFrame(body, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Message, body, maxFrameSize);
}

/**
 * Encodes a REQUEST frame, followed by a MESSAGE frame when message is given, in one buffer so
 * both go out in a single write.
 */
export function encodeRequestFrame(request, message, maxFrameSize = DefaultMaxFrameSize) {
  const payload = marshalMessage(RpcRequest, request);
  checkFrameLength(payload.byteLength, maxFrameSize);
  if (message === undefined) {
    return encodeFrame(FrameType.Request, payload, maxFrameSize);
  }
  checkFrameLength(message.byteLength, maxFrameSize);
  const requestSize = frameSize(FrameType.Request, payload.byteLength);
  const frame = new Uint8Array(requestSize + frameSize(FrameType.Message, message.byteLength));
  writeFrame(frame, requestSize, FrameType.Message, message);
  writeFrame(frame, 0, FrameType.Request, payload);
  return frame;
}

/** Encodes a HEADERS frame from RpcHeaders fields. */
export function encodeHeadersFrame(headers, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Headers, marshalMessage(RpcHeaders, headers), maxFrameSize);
}

/** Encodes a STATUS frame from RpcStatus fields. */
export function encodeStatusFrame(status, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Status, marshalMessage(RpcStatus, status), maxFrameSize);
}

/**
 * Reads frames from a byte stream reader. Frame headers may be split across chunks; payloads
 * inside one chunk are returned as views without copying. Unknown frame types are skipped.
 */
export class FrameReader {
  #reader;
  /** Bytes received from the reader but not yet consumed. */
  #pending = EmptyBytes;

  /** Creates a frame reader over a ReadableStream reader. */
  constructor(reader) {
    this.#reader = reader;
  }

  /**
   * Reads the next known frame as `{ type, payload }`, or returns null when the stream ended
   * cleanly between frames. The declared length of every frame, unknown ones included, is checked
   * against maxFrameSize before any payload is read or allocated.
   */
  async readFrame(maxFrameSize = DefaultMaxFrameSize) {
    for (;;) {
      const type = await this.#readVarint(true);
      if (type === null) {
        return null;
      }
      const length = await this.#readVarint(false);
      checkFrameLength(length, maxFrameSize);
      if (!KnownFrameTypes.has(type)) {
        // Types of 2^53 and above are bigints, which are never known.
        await this.#skip(length);
        continue;
      }
      return { type, payload: await this.#readExact(length) };
    }
  }

  /**
   * Resolves when the rest of the stream consists of unknown frames and a clean end; rejects if
   * a known frame or a malformed frame arrives first.
   */
  async expectEnd(maxFrameSize = DefaultMaxFrameSize) {
    const frame = await this.readFrame(maxFrameSize);
    if (frame !== null) {
      throw internal(`frame of type ${frame.type} after the final STATUS frame`);
    }
  }

  /** Reads a varint; null if allowEnd and the stream ended before its first byte. */
  async #readVarint(allowEnd) {
    while (this.#pending.byteLength === 0) {
      const { value, done } = await this.#nextChunk();
      if (done) {
        if (allowEnd) {
          return null;
        }
        throw unavailable("stream ended before a complete frame");
      }
      this.#pending = value;
    }
    const bytes = await this.#readExact(1 << (this.#pending[0] >> 6));
    return readVarint(bytes, 0).value;
  }

  /** Returns exactly size bytes; copies only when the read spans chunks. */
  async #readExact(size) {
    if (this.#pending.byteLength >= size) {
      return this.#take(size);
    }

    const out = new Uint8Array(size);
    let filled = this.#pending.byteLength;
    out.set(this.#pending);
    this.#pending = EmptyBytes;
    while (filled < size) {
      const { value, done } = await this.#nextChunk();
      if (done) {
        throw unavailable("stream ended before a complete frame");
      }
      const count = Math.min(value.byteLength, size - filled);
      out.set(value.subarray(0, count), filled);
      filled += count;
      this.#pending = value.subarray(count);
    }
    return out;
  }

  /** Discards size bytes without buffering them. */
  async #skip(size) {
    let left = size;
    while (left > 0) {
      if (this.#pending.byteLength === 0) {
        const { value, done } = await this.#nextChunk();
        if (done) {
          throw unavailable("stream ended before a complete frame");
        }
        this.#pending = value;
      }
      const count = Math.min(this.#pending.byteLength, left);
      this.#pending = this.#pending.subarray(count);
      left -= count;
    }
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

/** Rejects payload lengths above the limit; length may be a bigint for huge varints. */
function checkFrameLength(length, maxFrameSize) {
  if (length > maxFrameSize) {
    throw resourceExhausted(`frame length ${length} exceeds maximum ${maxFrameSize}`);
  }
}

function frameSize(type, length) {
  return varintLength(type) + varintLength(length) + length;
}

function writeFrame(output, offset, type, payload) {
  const next = writeVarint(output, writeVarint(output, offset, type), payload.byteLength);
  output.set(payload, next);
}
