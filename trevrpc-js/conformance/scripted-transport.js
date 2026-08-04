import { FrameReader, decodeStreamFrameBody } from "../src/framing.js";

export class ScriptedTransport {
  constructor(frameBodies) {
    this.frameBodies = frameBodies;
    this.closeCount = 0;
    this.closed = false;
  }

  async call() {
    throw new Error("scripted transport received an unexpected unary call");
  }

  async streamingCall() {
    return new ScriptedFrameStream(this, this.frameBodies);
  }

  closeOnce() {
    if (!this.closed) {
      this.closed = true;
      this.closeCount += 1;
    }
  }
}

class ScriptedFrameStream {
  constructor(transport, frameBodies) {
    this.transport = transport;
    this.done = false;
    this.reader = new FrameReader(new ScriptedReader(frameBodies));
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    if (this.done) {
      return { done: true, value: undefined };
    }

    const body = await this.reader.readRawFrameBodyOrEOF();
    if (body == null) {
      this.done = true;
      this.transport.closeOnce();
      return { done: true, value: undefined };
    }
    return { done: false, value: decodeStreamFrameBody(body) };
  }

  async return() {
    this.done = true;
    this.transport.closeOnce();
    return { done: true, value: undefined };
  }
}

class ScriptedReader {
  constructor(frameBodies) {
    this.chunks = frameBodies.map(frameBytes);
    this.next = 0;
  }

  async read() {
    if (this.next >= this.chunks.length) {
      return { done: true, value: undefined };
    }
    return { done: false, value: this.chunks[this.next++] };
  }
}

function frameBytes(body) {
  const frame = new Uint8Array(4 + body.byteLength);
  new DataView(frame.buffer).setUint32(0, body.byteLength, false);
  frame.set(body, 4);
  return frame;
}
