import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

import { byteView } from "./bytes.js";
import { headroomOf } from "./headroom.js";
import { internal, invalidArgument, resourceExhausted, unavailable } from "./status.js";
import { readVarint, varintLength, writeVarint } from "./varint.js";
import { RpcAnswer, RpcHeaders, RpcRequest, RpcStatus } from "./wire.js";

export const DefaultMaxFrameSize = 4 * 1024 * 1024;

/** Frame types; every other type is unknown and skipped by receivers. */
export const FrameType = Object.freeze({
  Message: 0,
  Request: 1,
  Headers: 2,
  Status: 3,
  ZstdMessage: 4,
  ZstdStreamMessage: 5,
  Answer: 6,
});

const KnownFrameTypes = new Set(Object.values(FrameType));
const EmptyBytes = new Uint8Array(0);

/**
 * Encodes a protobuf message body: `message` is a message of `schema` (a protobuf-es message
 * descriptor) or the init object `create()` takes. `codec`, the generated codec of `schema` if
 * there is one, encodes it; protobuf-es does otherwise, to the same bytes. With `headroom`, which
 * only a body that goes out in a MESSAGE frame of its own should ask for, a codec leaves room in
 * front of a large body for encodeFrame() to write the header in place. A message that cannot be
 * encoded fails with InvalidArgument. Once it is encoded, `validate`, if given, checks it and
 * throws to reject it, so that it sees only well-formed messages.
 */
export function marshalMessage(schema, message, validate, codec, headroom = false) {
  let value;
  let body;
  try {
    value = create(schema, message);
    if (codec === undefined) {
      body = toBinary(schema, value);
    } else if (headroom && typeof codec.encodeBody === "function") {
      body = codec.encodeBody(value);
    } else {
      body = codec.encode(value);
    }
  } catch (error) {
    throw invalidArgument(
      `failed to encode protobuf message: ${error?.message ?? String(error)}`,
      error,
    );
  }
  validate?.(value);
  return body;
}

/**
 * Decodes a protobuf message body of `schema`, with `codec`, its generated codec, if there is one,
 * rejecting malformed wire data.
 */
export function unmarshalMessage(schema, body, codec) {
  try {
    const bytes = byteView(body);
    return codec === undefined ? fromBinary(schema, bytes) : codec.decode(bytes);
  } catch (error) {
    throw invalidArgument(
      `failed to decode protobuf message: ${error?.message ?? String(error)}`,
      error,
    );
  }
}

/** Encodes a wire payload with its codec from wire.js, such as RpcStatus. */
export function encodeWireMessage(codec, fields) {
  try {
    return codec.encode(fields);
  } catch (error) {
    throw invalidArgument(
      `failed to encode ${codec.name}: ${error?.message ?? String(error)}`,
      error,
    );
  }
}

/** Decodes a wire payload with its codec from wire.js, rejecting malformed wire data. */
export function decodeWireMessage(codec, payload) {
  try {
    return codec.decode(byteView(payload));
  } catch (error) {
    throw invalidArgument(
      `failed to decode ${codec.name}: ${error?.message ?? String(error)}`,
      error,
    );
  }
}

/**
 * Encodes one frame carrying an already-encoded payload. The MESSAGE frame of a body that a
 * generated codec encoded with room in front (see headroom.js) gets its header written there, and
 * is a view of the same buffer; every other frame copies its payload into a new buffer.
 */
export function encodeFrame(type, payload, maxFrameSize = DefaultMaxFrameSize) {
  const length = payload.byteLength;
  checkFrameLength(length, maxFrameSize);
  const header = varintLength(type) + varintLength(length);
  // Only MESSAGE frames, so that framing a body again, as a resent message is, writes the very
  // header a frame of it still in flight has.
  if (type === FrameType.Message && header <= headroomOf(payload)) {
    const frame = new Uint8Array(payload.buffer, payload.byteOffset - header, header + length);
    writeVarint(frame, writeVarint(frame, 0, type), length);
    return frame;
  }
  const frame = new Uint8Array(header + length);
  writeFrame(frame, 0, type, payload);
  return frame;
}

/** Encodes a MESSAGE frame; the payload is the raw protobuf message. */
export function encodeMessageFrame(body, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Message, body, maxFrameSize);
}

/**
 * Encodes an outgoing message, `{ type, payload, size }` from encodeMessage, as its MESSAGE or
 * ZSTD_MESSAGE frame. The message size counts against the limit, not just the payload's.
 */
export function encodeOutgoingFrame(message, maxFrameSize = DefaultMaxFrameSize) {
  checkFrameLength(message.size, maxFrameSize);
  return encodeFrame(message.type, message.payload, maxFrameSize);
}

/**
 * Encodes a REQUEST frame, followed by the frame of message, an outgoing message from
 * encodeMessage, when it is given, in one buffer so both go out in a single write.
 */
export function encodeRequestFrame(request, message, maxFrameSize = DefaultMaxFrameSize) {
  const payload = encodeWireMessage(RpcRequest, request);
  checkFrameLength(payload.byteLength, maxFrameSize);
  if (message === undefined) {
    return encodeFrame(FrameType.Request, payload, maxFrameSize);
  }
  checkFrameLength(message.size, maxFrameSize);
  const requestSize = frameSize(FrameType.Request, payload.byteLength);
  const frame = new Uint8Array(requestSize + frameSize(message.type, message.payload.byteLength));
  writeFrame(frame, requestSize, message.type, message.payload);
  writeFrame(frame, 0, FrameType.Request, payload);
  return frame;
}

/** Encodes a HEADERS frame from RpcHeaders fields. */
export function encodeHeadersFrame(headers, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Headers, encodeWireMessage(RpcHeaders, headers), maxFrameSize);
}

/** Encodes an ANSWER frame from RpcAnswer fields. */
export function encodeAnswerFrame(answer, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Answer, encodeWireMessage(RpcAnswer, answer), maxFrameSize);
}

/** Encodes a STATUS frame from RpcStatus fields. */
export function encodeStatusFrame(status, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Status, encodeWireMessage(RpcStatus, status), maxFrameSize);
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
    // Frames already buffered in full cost no await.
    return this.tryReadFrame(maxFrameSize) ?? (await this.#readSplitFrame(maxFrameSize));
  }

  /**
   * Reads the next known frame like readFrame() if all of its bytes were received already, and
   * returns undefined, consuming nothing of it, if the reader has to wait for more. It never reads
   * from the underlying reader, so it neither learns of the end of the stream nor cancels a read
   * in flight: call it only while no readFrame() is pending. Unknown frames received in full
   * before it are skipped. A frame whose declared length exceeds maxFrameSize throws, as in
   * readFrame(), and stays unread.
   */
  tryReadFrame(maxFrameSize = DefaultMaxFrameSize) {
    for (;;) {
      const pending = this.#pending;
      const type = readVarint(pending, 0);
      const length = type === null ? null : readVarint(pending, type.next);
      if (length === null) {
        return undefined;
      }
      checkFrameLength(length.value, maxFrameSize);
      const end = length.next + length.value;
      if (pending.byteLength < end) {
        return undefined;
      }
      this.#pending = pending.subarray(end);
      // Types of 2^53 and above are bigints, which are never known.
      if (KnownFrameTypes.has(type.value)) {
        return { type: type.value, payload: pending.subarray(length.next, end) };
      }
    }
  }

  /**
   * Reads the next known frame when it is not buffered in full, waiting for the chunks that
   * complete it; null at a clean end between frames.
   */
  async #readSplitFrame(maxFrameSize) {
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
