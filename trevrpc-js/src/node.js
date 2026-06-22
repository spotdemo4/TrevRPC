import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { RpcKind, RpcStreamFrameKind } from "./wire.js";

const require = createRequire(import.meta.url);
const moduleDir = dirname(fileURLToPath(import.meta.url));
const EmptyBody = new Uint8Array(0);
const RpcStatusOk = 0;

/** Native Node transport backed by trevrpc-c and MsQuic. */
export class NodeTransport {
  constructor(nativeClient, options = {}) {
    this.nativeClient = nativeClient;
    this.maxFrameSize = options.maxFrameSize;
  }

  /** Opens a WebTransport TrevRPC client backed by the native C runtime. */
  static async connectWebTransport(urlOrOptions, options = {}) {
    const native = loadNative();
    const connectOptions = normalizeWebTransportOptions(urlOrOptions, options);
    const nativeClient = await native.connectWebTransport(connectOptions);
    return new NodeTransport(nativeClient, connectOptions);
  }

  /** Sends a unary RPC request and returns its response. */
  async call(request) {
    return await this.nativeClient.call(request.service, request.method, byteBody(request.body));
  }

  /** Starts a streaming RPC and returns native response frames. */
  async streamingCall(request, requestBody) {
    const stream = await this.nativeClient.startStream(
      request.service,
      request.method,
      request.kind ?? RpcKind.ServerStreaming,
      byteBody(request.body),
    );
    const writerTask = writeRequestStream(stream, requestBody);
    return new NativeResponseFrameStream(stream, writerTask);
  }

  /** Closes the underlying native client. */
  close() {
    this.nativeClient.close();
  }
}

class NativeResponseFrameStream {
  constructor(stream, writerTask) {
    this.stream = stream;
    this.done = false;
    this.writerError = null;
    this.writerSettled = false;
    this.returnDone = null;
    this.returnErrorReported = false;
    this.suppressReturnWriterError = false;
    this.writerDone = writerTask
      .catch((error) => {
        this.writerError = error;
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

    const frame = await this.stream.recv();
    if (frame == null) {
      this.done = true;
      await this.writerDone;
      if (this.writerError != null) {
        throw this.writerError;
      }
      return { done: true, value: undefined };
    }

    if (frame.kind === RpcStreamFrameKind.Status) {
      this.done = true;
      this.stream.close();
      if (this.writerSettled) {
        await this.writerDone;
        if ((frame.status ?? RpcStatusOk) === RpcStatusOk && this.writerError != null) {
          throw this.writerError;
        }
      } else {
        this.suppressReturnWriterError = true;
      }
    }
    return { done: false, value: frame };
  }

  async return() {
    if (this.returnDone == null) {
      this.done = true;
      this.returnDone = (async () => {
        this.stream.close();
        await this.writerDone;
      })();
    }

    await this.returnDone;
    if (!this.returnErrorReported && !this.suppressReturnWriterError && this.writerError != null) {
      this.returnErrorReported = true;
      throw this.writerError;
    }
    return { done: true, value: undefined };
  }
}

async function writeRequestStream(stream, requestBody) {
  try {
    for await (const body of requestBody) {
      await stream.sendMessage(byteBody(body));
    }
    await stream.finishSend();
  } catch (error) {
    stream.close();
    throw error;
  }
}

function normalizeWebTransportOptions(urlOrOptions, options) {
  if (typeof urlOrOptions === "string" || urlOrOptions instanceof URL) {
    const url = new URL(urlOrOptions);
    const path = `${url.pathname || "/"}${url.search}`;
    return {
      ...options,
      host: url.hostname,
      port: Number(url.port || 443),
      path,
      origin: options.origin ?? url.origin,
    };
  }

  if (urlOrOptions == null || typeof urlOrOptions !== "object") {
    throw new TypeError("connectWebTransport requires a URL or options object");
  }
  return { ...urlOrOptions, ...options };
}

function byteBody(body) {
  if (body == null) {
    return EmptyBody;
  }
  if (body instanceof Uint8Array) {
    return body;
  }
  if (ArrayBuffer.isView(body)) {
    return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
  }
  if (body instanceof ArrayBuffer) {
    return new Uint8Array(body);
  }
  return Uint8Array.from(body);
}

function loadNative() {
  const explicitPath = process.env.TREVRPC_JS_NATIVE;
  if (explicitPath != null && explicitPath !== "") {
    return require(explicitPath);
  }

  return require(join(moduleDir, "..", "build", "native", "trevrpc_native.node"));
}
