import { RequestWriterSettlement } from "./client.js";
import {
  DefaultMaxFrameSize,
  FrameReader,
  writeFrame,
  writeMessageStreamFrames,
} from "./framing.js";
import { Code, TrevRpcError, cancelled, statusFromTransportError, unavailable } from "./status.js";
import { RpcRequest, RpcResponse, RpcStreamFrameKind } from "./wire.js";

const CancelledStreamReason = new DOMException("TrevRPC stream cancelled", "AbortError");
const RequestBodyBatchMaxMessages = 64;
const RequestBodyBatchMaxBytes = 64 * 1024;
const ResponseFrameBatchMaxMessages = 64;
const BrowserWebTransportOptionKeys = [
  "allowPooling",
  "congestionControl",
  "requireUnreliable",
  "serverCertificateHashes",
];

/** Transport implementation for TrevRPC over WebTransport. */
export class RawWebTransport {
  /** Creates a client over an established WebTransport session. */
  constructor(session, options = {}) {
    this.session = session;
    this.maxFrameSize = options.maxFrameSize ?? DefaultMaxFrameSize;
  }

  /** Opens a WebTransport session and wraps it in a TrevRPC client. */
  static async connect(url, options = {}) {
    const session = createWebTransportSession(url, options);
    await session.ready;
    return new RawWebTransport(session, options);
  }

  /** Waits for the underlying WebTransport session to become ready. */
  async ready() {
    await this.session.ready;
  }

  /** Closes the underlying WebTransport session. */
  close(closeInfo = {}) {
    if (typeof this.session.close === "function") {
      this.session.close(closeInfo);
    }
  }

  /** Sends a unary RPC request over WebTransport and returns its response. */
  async call(request, options = {}) {
    let writer;
    let reader;
    let complete = false;
    let cleanupAbort = () => {};
    let writerReleaseInCloseTask = false;

    try {
      throwIfAborted(options.signal);
      await abortable(this.ready(), options.signal);
      const stream = await abortableBidirectionalStream(
        this.openBidirectionalStream(),
        options.signal,
      );
      writer = stream.writable.getWriter();
      reader = stream.readable.getReader();
      cleanupAbort = onAbort(options.signal, () => {
        void abortWriter(writer);
        void cancelReader(reader);
      });
      const frameReader = new FrameReader(reader);

      await abortable(
        writeFrame(writer, RpcRequest, request, options.maxFrameSize ?? this.maxFrameSize),
        options.signal,
      );
      closeUnaryRequestWriter(writer);
      writerReleaseInCloseTask = true;
      const response = await abortable(
        frameReader.readFrame(RpcResponse, options.maxFrameSize ?? this.maxFrameSize),
        options.signal,
      );
      complete = true;
      return response;
    } catch (error) {
      throw statusFromTransportError(error);
    } finally {
      if (!complete) {
        await abortWriter(writer);
        await cancelReader(reader);
      }
      if (!writerReleaseInCloseTask) {
        releaseLock(writer);
      }
      releaseLock(reader);
      cleanupAbort();
    }
  }

  /** Sends a streaming RPC request over WebTransport and returns response frames. */
  async streamingCall(request, requestBody, options = {}) {
    try {
      throwIfAborted(options.signal);
      await abortable(this.ready(), options.signal);
      const stream = await abortableBidirectionalStream(
        this.openBidirectionalStream(),
        options.signal,
      );
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const maxFrameSize = options.maxFrameSize ?? this.maxFrameSize;
      const abortRequestWriter = onceAsync(() => abortWriter(writer));
      const writerSettlement = new RequestWriterSettlement(
        statusFromTransportError,
        isCleanupWriterError,
        isRemoteStopSendingWriterError,
      );
      writerSettlement.track(
        writeStreamingRequest(
          writer,
          request,
          requestBody,
          maxFrameSize,
          abortRequestWriter,
          (error) => writerSettlement.recordError(error),
        ),
      );
      return new WebTransportResponseFrameStream(
        reader,
        writerSettlement,
        abortRequestWriter,
        maxFrameSize,
        options.signal,
      );
    } catch (error) {
      throw statusFromTransportError(error);
    }
  }

  /** Opens a bidirectional WebTransport stream. */
  async openBidirectionalStream() {
    if (typeof this.session.createBidirectionalStream !== "function") {
      throw unavailable("WebTransport session does not support bidirectional streams");
    }

    return this.session.createBidirectionalStream();
  }
}

/** Creates a WebTransport session using only browser constructor options. */
export function createWebTransportSession(url, options = {}) {
  const WebTransportCtor = options.WebTransport ?? globalThis.WebTransport;
  if (typeof WebTransportCtor !== "function") {
    throw unavailable("WebTransport is not available in this JavaScript runtime");
  }

  return new WebTransportCtor(url, browserConstructorOptions(options));
}

function browserConstructorOptions(options) {
  const constructorOptions = {};
  for (const key of BrowserWebTransportOptionKeys) {
    if (Object.hasOwn(options, key) && options[key] !== undefined) {
      constructorOptions[key] = options[key];
    }
  }

  return constructorOptions;
}

class WebTransportResponseFrameStream {
  constructor(
    reader,
    writerSettlement,
    abortRequestWriter,
    maxFrameSize = DefaultMaxFrameSize,
    signal = undefined,
  ) {
    this.reader = reader;
    this.frameReader = new FrameReader(reader);
    this.abortRequestWriter = abortRequestWriter;
    this.maxFrameSize = maxFrameSize;
    this.readBatchMaxMessages = ResponseFrameBatchMaxMessages;
    this.frameQueue = [];
    this.frameQueueHead = 0;
    this.done = false;
    this.readerOwned = true;
    this.cleanupAbort = () => {};
    this.cleanupDone = null;
    this.writerSettlement = writerSettlement;
    this.cleanupAbort = onAbort(signal, () => this.startCleanup());
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    if (this.done) {
      return await this.finishDone();
    }

    try {
      await this.fillFrameQueue();
      const frame = this.takeQueuedFrame();
      if (frame == null) {
        return await this.finishEof();
      }

      if (frame.kind === RpcStreamFrameKind.Status) {
        await this.finishStatus(frame);
      }

      return { done: false, value: frame };
    } catch (error) {
      this.startCleanup();
      throw statusFromTransportError(error);
    }
  }

  async nextBatch(max = this.readBatchMaxMessages) {
    if (this.done) {
      return await this.finishDone();
    }

    try {
      await this.fillFrameQueue(max);
      const terminalOffset = this.findQueuedTerminalOffset();
      if (terminalOffset === 0) {
        const frame = this.takeQueuedFrame();
        if (frame == null) {
          return await this.finishEof();
        }
        await this.finishStatus(frame);
        return { done: false, value: [frame] };
      }

      const count = terminalOffset < 0 ? this.queuedFrameCount() : terminalOffset;
      return { done: false, value: this.takeQueuedFrames(count) };
    } catch (error) {
      this.startCleanup();
      throw statusFromTransportError(error);
    }
  }

  async nextBodyBatch(max = this.readBatchMaxMessages) {
    if (this.done) {
      return await this.finishDone();
    }

    try {
      const batch = await this.frameReader.readStreamMessageBodyBatchOrEOF(max, this.maxFrameSize);
      if (batch == null) {
        return await this.finishEof();
      }
      if (batch.status != null) {
        await this.finishStatus(batch.status);
      }
      return { done: false, value: batch };
    } catch (error) {
      this.startCleanup();
      throw statusFromTransportError(error);
    }
  }

  async return() {
    this.startCleanup();
    return await this.finishDone();
  }

  startCleanup() {
    this.writerSettlement.beginCleanup();
    if (this.cleanupDone != null) {
      return;
    }

    this.done = true;
    this.clearFrameQueue();
    this.cleanupAbort();
    const cancel = this.readerOwned ? cancelReader(this.reader) : Promise.resolve();
    this.cleanupDone = Promise.all([cancel, this.abortRequestWriter()]).then(() => {
      this.releaseReader();
    });
  }

  releaseReader() {
    if (!this.readerOwned) {
      return;
    }
    this.readerOwned = false;
    releaseLock(this.reader);
  }

  async fillFrameQueue(max = this.readBatchMaxMessages) {
    if (this.queuedFrameCount() > 0) {
      return;
    }

    const batch = await this.frameReader.readStreamFrameBatchOrEOF(max, this.maxFrameSize);
    if (batch == null) {
      this.frameQueue = [null];
    } else {
      this.frameQueue = batch;
    }
    this.frameQueueHead = 0;
  }

  async finishEof() {
    this.startCleanup();
    return await this.finishDone();
  }

  async finishDone() {
    await this.cleanupDone;
    await this.writerSettlement.finish();
    return { done: true, value: undefined };
  }

  finishStatus(frame) {
    this.writerSettlement.recordTerminal(frame.status ?? Code.Ok);
  }

  queuedFrameCount() {
    return this.frameQueue.length - this.frameQueueHead;
  }

  takeQueuedFrame() {
    const frame = this.frameQueue[this.frameQueueHead];
    this.frameQueueHead += 1;
    this.compactFrameQueue();
    return frame;
  }

  takeQueuedFrames(count) {
    if (count <= 0) {
      return [];
    }
    const start = this.frameQueueHead;
    const end = start + count;
    if (start === 0 && end === this.frameQueue.length) {
      const frames = this.frameQueue;
      this.frameQueue = [];
      this.frameQueueHead = 0;
      return frames;
    }

    const frames = this.frameQueue.slice(start, end);
    this.frameQueueHead = end;
    this.compactFrameQueue();
    return frames;
  }

  findQueuedTerminalOffset() {
    for (let index = this.frameQueueHead; index < this.frameQueue.length; index += 1) {
      if (isTerminalFrame(this.frameQueue[index])) {
        return index - this.frameQueueHead;
      }
    }
    return -1;
  }

  clearFrameQueue() {
    this.frameQueue.length = 0;
    this.frameQueueHead = 0;
  }

  compactFrameQueue() {
    if (this.frameQueueHead === 0) {
      return;
    }
    if (this.frameQueueHead === this.frameQueue.length) {
      this.clearFrameQueue();
      return;
    }
    if (this.frameQueueHead >= 32 && this.frameQueueHead * 2 >= this.frameQueue.length) {
      this.frameQueue.splice(0, this.frameQueueHead);
      this.frameQueueHead = 0;
    }
  }
}

function isTerminalFrame(frame) {
  return frame == null || frame.kind === RpcStreamFrameKind.Status;
}

function isCleanupWriterError(error) {
  if (error?.code === Code.Cancelled) {
    return true;
  }

  const message = error?.statusMessage ?? error?.message ?? "";
  return error?.code === Code.Unavailable && /stream canceled with error code 0/i.test(message);
}

function isRemoteStopSendingWriterError(error) {
  const message = error?.statusMessage ?? error?.message ?? "";
  return error?.code === Code.Unavailable && /received stop_sending/i.test(message);
}

function closeUnaryRequestWriter(writer) {
  void (async () => {
    try {
      await writer.close();
    } catch {
      // Unary reads do not require waiting for the browser's upload close
      // acknowledgement. Response validation still reports server-side status.
    } finally {
      releaseLock(writer);
    }
  })();
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw signalAbortError(signal);
  }
}

function signalAbortError(signal) {
  const reason = signal?.reason;
  if (reason instanceof TrevRpcError) {
    return reason;
  }
  const error = cancelled("RPC cancelled");
  if (reason !== undefined) {
    Object.defineProperty(error, "cause", { configurable: true, value: reason });
  }
  return error;
}

function abortable(promise, signal) {
  throwIfAborted(signal);
  if (signal == null) {
    return promise;
  }

  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signalAbortError(signal));
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function abortableBidirectionalStream(promise, signal) {
  throwIfAborted(signal);
  if (signal == null) {
    return promise;
  }

  let aborted = false;
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      aborted = true;
      reject(signalAbortError(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (stream) => {
        signal.removeEventListener("abort", onAbort);
        if (aborted || signal.aborted) {
          void cleanupBidirectionalStream(stream);
          reject(signalAbortError(signal));
          return;
        }

        resolve(stream);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function onceAsync(callback) {
  let task;
  return () => {
    if (task == null) {
      try {
        task = Promise.resolve(callback());
      } catch (error) {
        task = Promise.reject(error);
      }
      task.catch(() => {});
    }
    return task;
  };
}

function onAbort(signal, abort) {
  if (signal == null) {
    return () => {};
  }

  if (signal.aborted) {
    abort();
    return () => {};
  }

  signal.addEventListener("abort", abort, { once: true });
  return () => signal.removeEventListener("abort", abort);
}

async function writeStreamingRequest(
  writer,
  request,
  requestBody,
  maxFrameSize,
  abortRequestWriter,
  recordError,
) {
  let iterator;
  try {
    iterator = requestBody[Symbol.asyncIterator]();
    await writeFrame(writer, RpcRequest, request, maxFrameSize);
    for (;;) {
      const result = await nextRequestBodyBatch(iterator, RequestBodyBatchMaxMessages);
      if (result.done) {
        break;
      }
      await writeBodyBatches(writer, result.value, maxFrameSize, RequestBodyBatchMaxBytes);
    }
    await writer.close();
  } catch (error) {
    recordError(error);
    try {
      await iterator?.return?.();
    } catch {
      // Preserve the upload error; aborting the writer releases stream resources.
    }
    await abortRequestWriter();
    throw error;
  } finally {
    releaseLock(writer);
  }
}

async function nextRequestBodyBatch(iterator, maxMessages) {
  if (typeof iterator.nextBatch === "function") {
    return await iterator.nextBatch(maxMessages);
  }
  const result = await iterator.next();
  return result.done ? result : { done: false, value: [result.value] };
}

async function writeBodyBatches(writer, bodies, maxFrameSize, maxBytes) {
  const limit = Math.max(0, maxBytes ?? 0);
  if (limit === 0) {
    await writeMessageStreamFrames(writer, bodies, maxFrameSize);
    return;
  }

  let start = 0;
  let bytes = 0;
  for (let index = 0; index < bodies.length; index += 1) {
    const bodyBytes = bodies[index]?.byteLength ?? 0;
    if (index > start && bytes + bodyBytes > limit) {
      await writeMessageStreamFrames(writer, bodies, maxFrameSize, start, index);
      start = index;
      bytes = 0;
    }
    bytes += bodyBytes;
  }
  if (start < bodies.length) {
    await writeMessageStreamFrames(writer, bodies, maxFrameSize, start, bodies.length);
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

async function cleanupBidirectionalStream(stream) {
  let writer;
  let reader;
  try {
    writer = stream?.writable?.getWriter?.();
    await abortWriter(writer);
  } finally {
    releaseLock(writer);
  }

  try {
    reader = stream?.readable?.getReader?.();
    await cancelReader(reader);
  } finally {
    releaseLock(reader);
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
