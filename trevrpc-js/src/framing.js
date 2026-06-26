import { FrameTooLargeError, invalidArgument, unavailable } from "./status.js";
import { RpcStreamFrame, RpcStreamFrameKind } from "./wire.js";

export const DefaultMaxFrameSize = 4 * 1024 * 1024;

const FrameHeaderLength = 4;
const StreamFrameBodyTag = (4 << 3) | 2;
const EmptyBytes = new Uint8Array(0);
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

/** Encodes a protobuf message body. */
export function marshalMessage(messageType, message) {
  const prepared = prepareMessage(messageType, message);
  return messageType.encode(prepared).finish();
}

/** Decodes a protobuf message body. */
export function unmarshalMessage(messageType, body) {
  try {
    return messageType.decode(body);
  } catch (error) {
    throw invalidArgument(`failed to decode protobuf message: ${error?.message ?? String(error)}`);
  }
}

/** Encodes a protobuf message into a length-prefixed TrevRPC frame. */
export function encodeFrame(messageType, message, maxFrameSize = DefaultMaxFrameSize) {
  const body = marshalMessage(messageType, message);
  if (body.byteLength > maxFrameSize) {
    throw new FrameTooLargeError(body.byteLength, maxFrameSize);
  }

  const frame = new Uint8Array(FrameHeaderLength + body.byteLength);
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  view.setUint32(0, body.byteLength, false);
  frame.set(body, FrameHeaderLength);
  return frame;
}

/** Encodes one streaming message frame carrying an already-encoded protobuf body. */
export function encodeMessageStreamFrame(body, maxFrameSize = DefaultMaxFrameSize) {
  return encodeMessageStreamFrames([body], maxFrameSize);
}

/** Encodes multiple streaming message frames into one contiguous write buffer. */
export function encodeMessageStreamFrames(
  bodies,
  maxFrameSize = DefaultMaxFrameSize,
  start = 0,
  end = undefined,
) {
  const range = bodyRange(bodies, start, end);
  let totalLength = 0;
  const byteBodies = Array.from({ length: range.end - range.start });
  for (let index = range.start; index < range.end; index += 1) {
    const body = byteView(range.bodies[index]);
    byteBodies[index - range.start] = body;
    const bodyLength = messageStreamFrameBodyLength(body.byteLength);
    if (bodyLength > maxFrameSize) {
      throw new FrameTooLargeError(bodyLength, maxFrameSize);
    }
    totalLength += FrameHeaderLength + bodyLength;
  }

  const frames = new Uint8Array(totalLength);
  const view = new DataView(frames.buffer, frames.byteOffset, frames.byteLength);
  let offset = 0;
  for (const body of byteBodies) {
    const bodyLength = messageStreamFrameBodyLength(body.byteLength);
    view.setUint32(offset, bodyLength, false);
    offset += FrameHeaderLength;
    if (body.byteLength > 0) {
      frames[offset++] = StreamFrameBodyTag;
      offset = appendVarint(frames, offset, body.byteLength);
      frames.set(body, offset);
      offset += body.byteLength;
    }
  }

  return frames;
}

/** Encodes each streaming message frame separately for vectored writers. */
export function encodeMessageStreamFrameParts(
  bodies,
  maxFrameSize = DefaultMaxFrameSize,
  start = 0,
  end = undefined,
) {
  const range = bodyRange(bodies, start, end);
  const frames = Array.from({ length: range.end - range.start });
  for (let index = range.start; index < range.end; index += 1) {
    frames[index - range.start] = encodeMessageStreamFrame(range.bodies[index], maxFrameSize);
  }
  return frames;
}

/** Decodes a protobuf message from a TrevRPC frame body. */
export function decodeFrame(messageType, body) {
  return unmarshalMessage(messageType, body);
}

/** Decodes a streaming RPC frame body using a fast path for common message frames. */
export function decodeStreamFrameBody(body) {
  const bytes = byteView(body);
  if (bytes.byteLength === 0) {
    return streamFrameMessage(EmptyBytes);
  }

  const cursor = { offset: 0 };
  let kind = RpcStreamFrameKind.Message;
  let status = 0;
  let message = "";
  let frameBody = new Uint8Array(0);

  while (cursor.offset < bytes.byteLength) {
    const tag = consumeVarint(bytes, cursor, "invalid stream frame field tag");
    if (tag === 0) {
      throw invalidArgument("invalid stream frame field tag");
    }

    const field = Math.floor(tag / 8);
    const wireType = tag % 8;
    switch (field) {
      case 1:
        requireWireType(wireType, 0, "invalid stream frame kind wire type");
        kind = consumeVarint(bytes, cursor, "truncated stream frame kind");
        break;
      case 2:
        requireWireType(wireType, 0, "invalid stream frame status wire type");
        status = consumeVarint(bytes, cursor, "truncated stream frame status");
        break;
      case 3:
        requireWireType(wireType, 2, "invalid stream frame message wire type");
        message = decodeUtf8(consumeLengthDelimited(bytes, cursor, "invalid stream frame message"));
        break;
      case 4:
        requireWireType(wireType, 2, "invalid stream frame body wire type");
        frameBody = consumeLengthDelimited(bytes, cursor, "invalid stream frame body");
        break;
      case 5:
        return decodeFrame(RpcStreamFrame, bytes);
      default:
        skipProtoField(bytes, cursor, wireType);
        break;
    }
  }

  if (kind !== RpcStreamFrameKind.Message && kind !== RpcStreamFrameKind.Status) {
    throw invalidArgument("stream frame contained an unknown frame kind");
  }

  return { kind, status, message, body: frameBody, metadata: {} };
}

/** Writes one length-prefixed protobuf frame. */
export async function writeFrame(writer, messageType, message, maxFrameSize = DefaultMaxFrameSize) {
  await writer.write(encodeFrame(messageType, message, maxFrameSize));
}

/** Writes multiple streaming message frames in one write. */
export async function writeMessageStreamFrames(
  writer,
  bodies,
  maxFrameSize = DefaultMaxFrameSize,
  start = 0,
  end = undefined,
) {
  if (typeof writer.writev === "function") {
    await writer.writev(encodeMessageStreamFrameParts(bodies, maxFrameSize, start, end));
    return;
  }

  await writer.write(encodeMessageStreamFrames(bodies, maxFrameSize, start, end));
}

/** Reads length-prefixed protobuf frames from a byte stream reader. */
export class FrameReader {
  /** Creates a frame reader over a stream reader. */
  constructor(reader) {
    this.reader = reader;
    this.chunks = [];
    this.chunkHead = 0;
    this.buffered = 0;
  }

  /** Reads and decodes one frame. */
  async readFrame(messageType, maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(FrameHeaderLength, false);
    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeFrame(messageType, body);
  }

  /** Reads and decodes one frame, or returns null when already at EOF. */
  async readFrameOrEOF(messageType, maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(FrameHeaderLength, true);
    if (header == null) {
      return null;
    }

    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeFrame(messageType, body);
  }

  /** Reads and decodes one streaming RPC frame, or returns null when already at EOF. */
  async readStreamFrameOrEOF(maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(FrameHeaderLength, true);
    if (header == null) {
      return null;
    }

    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeStreamFrameBody(body);
  }

  /** Reads one streaming RPC frame, then drains complete frames already buffered. */
  async readStreamFrameBatchOrEOF(maxBatch = 32, maxFrameSize = DefaultMaxFrameSize) {
    const first = await this.readStreamFrameOrEOF(maxFrameSize);
    if (first == null) {
      return null;
    }

    const limit = Math.max(1, Math.floor(maxBatch));
    const frames = [first];
    while (frames.length < limit) {
      const body = this.tryReadFrameBody(maxFrameSize);
      if (body === undefined) {
        break;
      }
      frames.push(decodeStreamFrameBody(body));
    }
    return frames;
  }

  /** Reads stream message body batches, with at most one terminal status. */
  async readStreamMessageBodyBatchOrEOF(maxBatch = 32, maxFrameSize = DefaultMaxFrameSize) {
    const body = await this.readRawFrameBodyOrEOF(maxFrameSize);
    if (body == null) {
      return null;
    }

    const limit = Math.max(1, Math.floor(maxBatch));
    const result = { bodies: [], status: null };
    pushStreamBodyOrStatus(result, body);
    while (result.status == null && result.bodies.length < limit) {
      const nextBody = this.tryReadFrameBody(maxFrameSize);
      if (nextBody === undefined) {
        break;
      }
      pushStreamBodyOrStatus(result, nextBody);
    }

    return result;
  }

  /** Reads one raw frame body, or returns null when already at EOF. */
  async readRawFrameBodyOrEOF(maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(FrameHeaderLength, true);
    if (header == null) {
      return null;
    }

    return this.readExact(frameBodyLength(header, maxFrameSize), false);
  }

  /** Reads a complete buffered frame body without waiting, or undefined. */
  tryReadFrameBody(maxFrameSize = DefaultMaxFrameSize) {
    if (this.buffered < FrameHeaderLength) {
      return undefined;
    }

    const header = this.peek(FrameHeaderLength);
    const length = frameBodyLength(header, maxFrameSize);
    if (this.buffered < FrameHeaderLength + length) {
      return undefined;
    }

    this.consume(FrameHeaderLength);
    return this.consume(length);
  }

  /** Reads exactly size bytes from the underlying reader. */
  async readExact(size, allowEofAtStart) {
    if (size === 0) {
      return EmptyBytes;
    }

    while (this.buffered < size) {
      const { value, done } = await this.reader.read();
      if (done) {
        if (allowEofAtStart && this.buffered === 0) {
          return null;
        }

        throw unavailable("transport unavailable: stream ended in the middle of a frame");
      }

      if (value == null || value.byteLength === 0) {
        continue;
      }

      const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
      this.chunks.push(bytes);
      this.buffered += bytes.byteLength;
    }

    return this.consume(size);
  }

  /** Removes size bytes from the buffered data. */
  consume(size) {
    if (size === 0) {
      return EmptyBytes;
    }

    const first = this.chunks[this.chunkHead];
    if (first != null && first.byteLength >= size) {
      const result = first.subarray(0, size);
      if (first.byteLength === size) {
        this.chunkHead += 1;
        this.compactChunks();
      } else {
        this.chunks[this.chunkHead] = first.subarray(size);
      }
      this.buffered -= size;
      return result;
    }

    const result = new Uint8Array(size);
    let offset = 0;

    while (offset < size) {
      const chunk = this.chunks[this.chunkHead];
      const needed = size - offset;
      if (chunk.byteLength <= needed) {
        result.set(chunk, offset);
        offset += chunk.byteLength;
        this.chunkHead += 1;
      } else {
        result.set(chunk.subarray(0, needed), offset);
        this.chunks[this.chunkHead] = chunk.subarray(needed);
        offset += needed;
      }
    }

    this.buffered -= size;
    this.compactChunks();
    return result;
  }

  /** Copies size bytes from the buffered data without removing them. */
  peek(size) {
    if (size === 0) {
      return EmptyBytes;
    }

    const first = this.chunks[this.chunkHead];
    if (first != null && first.byteLength >= size) {
      return first.subarray(0, size);
    }

    const result = new Uint8Array(size);
    let offset = 0;

    for (let index = this.chunkHead; index < this.chunks.length; index += 1) {
      const chunk = this.chunks[index];
      if (offset >= size) {
        break;
      }
      const count = Math.min(chunk.byteLength, size - offset);
      result.set(chunk.subarray(0, count), offset);
      offset += count;
    }

    return result;
  }

  compactChunks() {
    if (this.chunkHead === 0) {
      return;
    }
    if (this.chunkHead === this.chunks.length) {
      this.chunks.length = 0;
      this.chunkHead = 0;
      return;
    }
    if (this.chunkHead >= 32 && this.chunkHead * 2 >= this.chunks.length) {
      this.chunks.splice(0, this.chunkHead);
      this.chunkHead = 0;
    }
  }
}

function pushStreamBodyOrStatus(result, body) {
  const messageBody = tryDecodeStreamMessageBody(body);
  if (messageBody !== undefined) {
    result.bodies.push(messageBody);
    return;
  }

  const frame = decodeStreamFrameBody(body);
  switch (frame.kind) {
    case RpcStreamFrameKind.Message:
      result.bodies.push(frame.body ?? EmptyBytes);
      break;
    case RpcStreamFrameKind.Status:
      result.status = frame;
      break;
    default:
      throw invalidArgument("stream frame contained an unknown frame kind");
  }
}

function tryDecodeStreamMessageBody(body) {
  const bytes = byteView(body);
  if (bytes.byteLength === 0) {
    return EmptyBytes;
  }

  if (bytes[0] !== StreamFrameBodyTag) {
    return undefined;
  }

  const cursor = { offset: 1 };
  const length = consumeVarint(bytes, cursor, "invalid stream frame body");
  const end = cursor.offset + length;
  if (end !== bytes.byteLength) {
    return undefined;
  }
  return bytes.subarray(cursor.offset, end);
}

/** Decodes and validates the body length stored in a TrevRPC frame header. */
export function frameBodyLength(header, maxFrameSize = DefaultMaxFrameSize) {
  const view = new DataView(header.buffer, header.byteOffset, header.byteLength);
  const length = view.getUint32(0, false);
  if (length > maxFrameSize) {
    throw new FrameTooLargeError(length, maxFrameSize);
  }

  return length;
}

function prepareMessage(messageType, message) {
  const prepared =
    typeof messageType.fromObject === "function" ? messageType.fromObject(message) : message;
  const error = typeof messageType.verify === "function" ? messageType.verify(prepared) : null;
  if (error != null) {
    throw invalidArgument(`failed to encode protobuf message: ${error}`);
  }

  return prepared;
}

function streamFrameMessage(body) {
  return {
    kind: RpcStreamFrameKind.Message,
    status: 0,
    message: "",
    body,
    metadata: {},
  };
}

function byteView(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return new Uint8Array(value ?? 0);
}

function bodyRange(bodies, start, end) {
  const source = Array.isArray(bodies) ? bodies : Array.from(bodies);
  const from = clampRangeIndex(start, source.length);
  const to = Math.max(from, clampRangeIndex(end ?? source.length, source.length));
  return { bodies: source, start: from, end: to };
}

function clampRangeIndex(value, length) {
  if (!Number.isFinite(value)) {
    return length;
  }
  return Math.min(length, Math.max(0, Math.floor(value)));
}

function messageStreamFrameBodyLength(bodyLength) {
  return bodyLength === 0 ? 0 : 1 + varintLength(bodyLength) + bodyLength;
}

function appendVarint(output, offset, value) {
  let remaining = value;
  while (remaining >= 0x80) {
    output[offset++] = (remaining % 0x80) | 0x80;
    remaining = Math.floor(remaining / 0x80);
  }
  output[offset++] = remaining;
  return offset;
}

function varintLength(value) {
  let remaining = value;
  let length = 1;
  while (remaining >= 0x80) {
    remaining = Math.floor(remaining / 0x80);
    length += 1;
  }
  return length;
}

function consumeVarint(data, cursor, message) {
  let value = 0;
  let multiplier = 1;
  for (let index = 0; index < 10; index += 1) {
    if (cursor.offset >= data.byteLength) {
      throw invalidArgument(message);
    }
    const byte = data[cursor.offset++];
    value += (byte & 0x7f) * multiplier;
    if (byte < 0x80) {
      return value;
    }
    multiplier *= 0x80;
  }
  throw invalidArgument("stream frame varint exceeded 64 bits");
}

function requireWireType(actual, expected, message) {
  if (actual !== expected) {
    throw invalidArgument(message);
  }
}

function consumeLengthDelimited(data, cursor, message) {
  const length = consumeVarint(data, cursor, message);
  const end = cursor.offset + length;
  if (!Number.isSafeInteger(end) || end > data.byteLength) {
    throw invalidArgument(message);
  }
  const value = data.subarray(cursor.offset, end);
  cursor.offset = end;
  return value;
}

function skipProtoField(data, cursor, wireType) {
  switch (wireType) {
    case 0:
      consumeVarint(data, cursor, "truncated stream frame varint");
      return;
    case 1:
      skipFixed(data, cursor, 8);
      return;
    case 2:
      consumeLengthDelimited(data, cursor, "truncated stream frame field");
      return;
    case 5:
      skipFixed(data, cursor, 4);
      return;
    default:
      throw invalidArgument("stream frame contained an unsupported wire type");
  }
}

function skipFixed(data, cursor, length) {
  const end = cursor.offset + length;
  if (end > data.byteLength) {
    throw invalidArgument("truncated stream frame fixed field");
  }
  cursor.offset = end;
}

function decodeUtf8(bytes) {
  try {
    return utf8Decoder.decode(bytes);
  } catch (error) {
    throw invalidArgument(`stream frame message was not valid UTF-8: ${error.message}`);
  }
}
