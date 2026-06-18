import { FrameTooLargeError, invalidArgument, unavailable } from "./status.js";

export const DefaultMaxFrameSize = 16 * 1024 * 1024;

export function marshalMessage(messageType, message) {
  const prepared = prepareMessage(messageType, message);
  return messageType.encode(prepared).finish();
}

export function unmarshalMessage(messageType, body) {
  return messageType.decode(body);
}

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

export function decodeFrame(messageType, body) {
  return unmarshalMessage(messageType, body);
}

export async function writeFrame(writer, messageType, message, maxFrameSize = DefaultMaxFrameSize) {
  await writer.write(encodeFrame(messageType, message, maxFrameSize));
}

export class FrameReader {
  constructor(reader) {
    this.reader = reader;
    this.chunks = [];
    this.buffered = 0;
  }

  async readFrame(messageType, maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(4, false);
    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeFrame(messageType, body);
  }

  async readFrameOrEOF(messageType, maxFrameSize = DefaultMaxFrameSize) {
    const header = await this.readExact(4, true);
    if (header == null) {
      return null;
    }

    const length = frameBodyLength(header, maxFrameSize);
    const body = await this.readExact(length, false);
    return decodeFrame(messageType, body);
  }

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

export function frameBodyLength(header, maxFrameSize = DefaultMaxFrameSize) {
  const view = new DataView(header.buffer, header.byteOffset, header.byteLength);
  const length = view.getUint32(0, false);
  if (length > maxFrameSize) {
    throw new FrameTooLargeError(length, maxFrameSize);
  }

  return length;
}

function prepareMessage(messageType, message) {
  const prepared = typeof messageType.fromObject === "function" ? messageType.fromObject(message) : message;
  const error = typeof messageType.verify === "function" ? messageType.verify(prepared) : null;
  if (error != null) {
    throw invalidArgument(`failed to encode protobuf message: ${error}`);
  }

  return prepared;
}
