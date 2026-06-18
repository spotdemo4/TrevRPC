import { DefaultMaxFrameSize, FrameReader, writeFrame } from "./framing.js";
import { statusFromTransportError, unavailable } from "./status.js";
import { RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind, messageFrame } from "./wire.js";

const CancelledStreamReason = "TrevRPC stream cancelled";

export class WebTransportTransport {
  constructor(session, options = {}) {
    this.session = session;
    this.maxFrameSize = options.maxFrameSize ?? DefaultMaxFrameSize;
  }

  static async connect(url, options = {}) {
    const WebTransportCtor = options.WebTransport ?? globalThis.WebTransport;
    if (typeof WebTransportCtor !== "function") {
      throw unavailable("WebTransport is not available in this JavaScript runtime");
    }

    const session = new WebTransportCtor(url, options.webTransportOptions ?? {});
    await session.ready;
    return new WebTransportTransport(session, options);
  }

  async ready() {
    await this.session.ready;
  }

  close(closeInfo = {}) {
    if (typeof this.session.close === "function") {
      this.session.close(closeInfo);
    }
  }

  async call(request, options = {}) {
    let writer;
    let reader;
    let complete = false;

    try {
      await this.ready();
      const stream = await this.openBidirectionalStream();
      writer = stream.writable.getWriter();
      reader = stream.readable.getReader();
      const frameReader = new FrameReader(reader);

      await writeFrame(writer, RpcRequest, request, options.maxFrameSize ?? this.maxFrameSize);
      await writer.close();
      const response = await frameReader.readFrame(RpcResponse, options.maxFrameSize ?? this.maxFrameSize);
      complete = true;
      return response;
    } catch (error) {
      throw statusFromTransportError(error);
    } finally {
      if (!complete) {
        await abortWriter(writer);
        await cancelReader(reader);
      }
      releaseLock(writer);
      releaseLock(reader);
    }
  }

  async streamingCall(request, requestBody, options = {}) {
    try {
      await this.ready();
      const stream = await this.openBidirectionalStream();
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const maxFrameSize = options.maxFrameSize ?? this.maxFrameSize;
      const writerTask = writeStreamingRequest(writer, request, requestBody, maxFrameSize);
      return new WebTransportResponseFrameStream(reader, writer, writerTask, maxFrameSize);
    } catch (error) {
      throw statusFromTransportError(error);
    }
  }

  async openBidirectionalStream() {
    if (typeof this.session.createBidirectionalStream !== "function") {
      throw unavailable("WebTransport session does not support bidirectional streams");
    }

    return this.session.createBidirectionalStream();
  }
}

export class WebTransportResponseFrameStream {
  constructor(reader, writer, writerTask, maxFrameSize = DefaultMaxFrameSize) {
    this.reader = reader;
    this.writer = writer;
    this.frameReader = new FrameReader(reader);
    this.maxFrameSize = maxFrameSize;
    this.done = false;
    this.writerError = null;
    this.writerDone = writerTask.catch((error) => {
      this.writerError = statusFromTransportError(error);
    });
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    if (this.done) {
      return { done: true, value: undefined };
    }

    try {
      const frame = await this.frameReader.readFrameOrEOF(RpcStreamFrame, this.maxFrameSize);
      if (frame == null) {
        this.done = true;
        releaseLock(this.reader);
        await this.writerDone;
        if (this.writerError != null) {
          throw this.writerError;
        }
        return { done: true, value: undefined };
      }

      if (frame.kind === RpcStreamFrameKind.Status) {
        this.done = true;
        releaseLock(this.reader);
      }

      return { done: false, value: frame };
    } catch (error) {
      this.done = true;
      throw statusFromTransportError(error);
    }
  }

  async return() {
    this.done = true;
    await cancelReader(this.reader);
    await abortWriter(this.writer);
    releaseLock(this.reader);
    releaseLock(this.writer);
    return { done: true, value: undefined };
  }
}

async function writeStreamingRequest(writer, request, requestBody, maxFrameSize) {
  try {
    await writeFrame(writer, RpcRequest, request, maxFrameSize);
    for await (const body of requestBody) {
      await writeFrame(writer, RpcStreamFrame, messageFrame(body), maxFrameSize);
    }
    await writer.close();
  } catch (error) {
    await abortWriter(writer);
    throw error;
  } finally {
    releaseLock(writer);
  }
}

async function cancelReader(reader) {
  if (reader != null && typeof reader.cancel === "function") {
    try {
      await reader.cancel(CancelledStreamReason);
    } catch {
      // The stream may already be closed or reset by the peer.
    }
  }
}

async function abortWriter(writer) {
  if (writer != null && typeof writer.abort === "function") {
    try {
      await writer.abort(CancelledStreamReason);
    } catch {
      // The stream may already be closed or reset by the peer.
    }
  }
}

function releaseLock(lock) {
  if (lock != null && typeof lock.releaseLock === "function") {
    try {
      lock.releaseLock();
    } catch {
      // Releasing an already-released lock is harmless for cleanup purposes.
    }
  }
}
