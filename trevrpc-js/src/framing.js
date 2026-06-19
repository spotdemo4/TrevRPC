import { FrameTooLargeError, invalidArgument, unavailable } from "./status.js";

export const DefaultMaxFrameSize = 4 * 1024 * 1024;

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

  const frame = new Uint8Array(4 + body.byteLength);
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  view.setUint32(0, body.byteLength, false);
  frame.set(body, 4);
  return frame;
}

/** Decodes a protobuf message from a TrevRPC frame body. */
export function decodeFrame(messageType, body) {
  return unmarshalMessage(messageType, body);
}

/** Writes one length-prefixed protobuf frame. */
export async function writeFrame(writer, messageType, message, maxFrameSize = DefaultMaxFrameSize) {
  await writer.write(encodeFrame(messageType, message, maxFrameSize));
}

/** Reads length-prefixed protobuf frames from a byte stream reader. */
export class FrameReader {
  /** Creates a frame reader over a stream reader. */
  constructor(reader) {
    this.reader = reader;
    this.chunks = [];
    this.buffered = 0;
  }

  /** Reads and decodes one frame. */
  async readFrame(messageType, maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(4, false);
    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeFrame(messageType, body);
  }

  /** Reads and decodes one frame, or returns null when already at EOF. */
  async readFrameOrEOF(messageType, maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(4, true);
    if (header == null) {
      return null;
    }

    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeFrame(messageType, body);
  }

  /** Reads exactly size bytes from the underlying reader. */
  async readExact(size, allowEofAtStart) {
    if (size === 0) {
      return new Uint8Array(0);
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
    const result = new Uint8Array(size);
    let offset = 0;

    while (offset < size) {
      const chunk = this.chunks[0];
      const needed = size - offset;
      if (chunk.byteLength <= needed) {
        result.set(chunk, offset);
        offset += chunk.byteLength;
        this.chunks.shift();
      } else {
        result.set(chunk.subarray(0, needed), offset);
        this.chunks[0] = chunk.subarray(needed);
        offset += needed;
      }
    }

    this.buffered -= size;
    return result;
  }
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
