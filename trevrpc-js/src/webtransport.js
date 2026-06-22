import { DefaultMaxFrameSize, FrameReader, writeFrame } from "./framing.js";
import { Code, statusFromTransportError, unavailable } from "./status.js";
import {
  RpcRequest,
  RpcResponse,
  RpcStreamFrame,
  RpcStreamFrameKind,
  messageFrame,
} from "./wire.js";

const CancelledStreamReason = new DOMException("TrevRPC stream cancelled", "AbortError");
const BrowserWebTransportOptionKeys = [
  "allowPooling",
  "congestionControl",
  "requireUnreliable",
  "serverCertificateHashes",
];

/** Transport implementation for TrevRPC over WebTransport. */
export class WebTransportClient {
  /** Creates a client over an established WebTransport session. */
  constructor(session, options = {}) {
    this.session = session;
    this.maxFrameSize = options.maxFrameSize ?? DefaultMaxFrameSize;
  }

  /** Opens a WebTransport session and wraps it in a TrevRPC client. */
  static async connect(url, options = {}) {
    const WebTransportCtor = options.WebTransport ?? globalThis.WebTransport;
    if (typeof WebTransportCtor !== "function") {
      throw unavailable("WebTransport is not available in this JavaScript runtime");
    }

    const session = new WebTransportCtor(url, webTransportOptions(options));
    await session.ready;
    return new WebTransportClient(session, options);
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
      await abortable(writer.close(), options.signal);
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
      releaseLock(writer);
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
      const cleanupAbort = onAbort(options.signal, () => {
        void abortWriter(writer);
        void cancelReader(reader);
      });
      const writerTask = writeStreamingRequest(writer, request, requestBody, maxFrameSize);
      return new WebTransportResponseFrameStream(
        reader,
        writer,
        writerTask,
        maxFrameSize,
        cleanupAbort,
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

function webTransportOptions(options) {
  const constructorOptions = {};
  for (const key of BrowserWebTransportOptionKeys) {
    if (Object.hasOwn(options, key) && options[key] !== undefined) {
      constructorOptions[key] = options[key];
    }
  }

  const nested = options.webTransportOptions;
  if (nested == null) {
    return constructorOptions;
  }
  if (typeof nested === "object") {
    return { ...constructorOptions, ...nested };
  }
  return nested;
}

class WebTransportResponseFrameStream {
  constructor(
    reader,
    writer,
    writerTask,
    maxFrameSize = DefaultMaxFrameSize,
    cleanupAbort = () => {},
  ) {
    this.reader = reader;
    this.writer = writer;
    this.frameReader = new FrameReader(reader);
    this.maxFrameSize = maxFrameSize;
    this.done = false;
    this.writerError = null;
    this.writerSettled = false;
    this.returnDone = null;
    this.returnErrorReported = false;
    this.suppressReturnWriterError = false;
    this.cleanupAbort = cleanupAbort;
    this.writerDone = writerTask
      .catch((error) => {
        this.writerError = statusFromTransportError(error);
      })
      .finally(() => {
        this.writerSettled = true;
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
        this.cleanupAbort();
        await abortWriter(this.writer);
        if (this.writerSettled) {
          await this.writerDone;
          const statusCode = frame.status ?? Code.Ok;
          if (
            statusCode === Code.Ok &&
            this.writerError != null &&
            !isTerminalCleanupWriterError(this.writerError)
          ) {
            throw this.writerError;
          }
        } else {
          this.suppressReturnWriterError = true;
        }
      }

      return { done: false, value: frame };
    } catch (error) {
      this.done = true;
      throw statusFromTransportError(error);
    }
  }

  async return() {
    if (this.returnDone == null) {
      this.done = true;
      this.returnDone = (async () => {
        await cancelReader(this.reader);
        await abortWriter(this.writer);
        releaseLock(this.reader);
        releaseLock(this.writer);
        this.cleanupAbort();
        await this.writerDone;
      })();
    }

    await this.returnDone;
    if (
      !this.returnErrorReported &&
      !this.suppressReturnWriterError &&
      this.writerError != null &&
      !isTerminalCleanupWriterError(this.writerError)
    ) {
      this.returnErrorReported = true;
      throw this.writerError;
    }
    return { done: true, value: undefined };
  }
}

function isTerminalCleanupWriterError(error) {
  if (error?.code === Code.Cancelled) {
    return true;
  }

  const message = error?.statusMessage ?? error?.message ?? "";
  return (
    error?.code === Code.Unavailable &&
    /(?:stream canceled with error code 0|received stop_sending)/i.test(message)
  );
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException("RPC cancelled", "AbortError");
  }
}

function abortable(promise, signal) {
  throwIfAborted(signal);
  if (signal == null) {
    return promise;
  }

  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason ?? new DOMException("RPC cancelled", "AbortError"));
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
      reject(signal.reason ?? new DOMException("RPC cancelled", "AbortError"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (stream) => {
        signal.removeEventListener("abort", onAbort);
        if (aborted || signal.aborted) {
          void cleanupBidirectionalStream(stream);
          reject(signal.reason ?? new DOMException("RPC cancelled", "AbortError"));
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
