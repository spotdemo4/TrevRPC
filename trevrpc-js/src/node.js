import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Code } from "./status.js";
import { RpcKind, RpcStreamFrameKind } from "./wire.js";

const require = createRequire(import.meta.url);
const moduleDir = dirname(fileURLToPath(import.meta.url));
const EmptyBody = new Uint8Array(0);
const RpcStatusOk = 0;
const RecvManyBatchSize = 32;

/** Native Node transport backed by trevrpc-c and MsQuic. */
export class NodeTransport {
  constructor(nativeClient, options = {}) {
    this.nativeClient = nativeClient;
    this.maxFrameSize = options.maxFrameSize;
  }

  /** Opens a TrevRPC client backed by the native C runtime. */
  static async connect(urlOrOptions, options = {}) {
    const native = loadNative();
    const connectOptions = normalizeNodeTransportOptions(urlOrOptions, options);
    const nativeClient = await native.connectMsQuic(connectOptions);
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

/** Native Node server backed by trevrpc-c and MsQuic. */
export class NodeServer {
  constructor(nativeServer, options = {}) {
    this.nativeServer = nativeServer;
    this.port = nativeServer.port;
    this.maxFrameSize = options.maxFrameSize;
    this.closed = null;
  }

  /** Creates a native QUIC TrevRPC server backed by trevrpc-c. */
  static async listen(urlOrOptions, options = {}) {
    const native = loadNative();
    const listenOptions = normalizeNodeTransportOptions(urlOrOptions, options);
    const nativeServer = await native.listenMsQuic(listenOptions);
    return new NodeServer(nativeServer, listenOptions);
  }

  /** Registers one raw RPC handler. */
  register(service, method, kind, handler) {
    if (typeof handler !== "function") {
      throw new TypeError("register requires a handler function");
    }
    this.nativeServer.register(service, method, rpcKindNumber(kind), (nativeCall) => {
      void this.#dispatch(handler, nativeCall);
    });
    return this;
  }

  /** Registers handlers for a generated service descriptor. */
  registerService(service, handlers) {
    for (const [jsName, method] of Object.entries(service.methods)) {
      const handler = handlers[jsName] ?? handlers[method.name];
      if (handler != null) {
        this.register(service.fullName, method.name, method.kind, handler);
      }
    }
    return this;
  }

  /** Starts accepting RPCs. The returned promise resolves after close. */
  serve() {
    this.closed ??= this.nativeServer.serve();
    return this.closed;
  }

  /** Requests server shutdown. */
  close() {
    this.nativeServer.close();
  }

  async #dispatch(handler, nativeCall) {
    const call = new NodeServerCall(nativeCall);
    try {
      const result = await handler(call);
      if (!call.completed && !call.deferred) {
        await completeDefault(call, result);
      }
    } catch (error) {
      if (!call.completed) {
        await completeWithError(call, error);
      }
    }
  }
}

/** Raw server call passed to NodeServer handlers. */
export class NodeServerCall {
  constructor(nativeCall) {
    this.nativeCall = nativeCall;
    this.request = nativeCall.request;
    this.completed = false;
    this.deferred = false;
    this.recvDone = false;
    this.recvQueue = [];
  }

  /** Keeps the call open after the handler returns. */
  defer() {
    this.deferred = true;
    return this;
  }

  /** Sends a unary response and completes the call. */
  async respond(response = {}) {
    await this.nativeCall.respond(responseObject(response));
    this.completed = true;
  }

  /** Sends one streaming response message. */
  sendMessage(body) {
    return this.nativeCall.sendMessage(byteBody(body));
  }

  /** Receives one streaming request frame, or null after EOF. */
  async recv() {
    if (this.recvDone) {
      return null;
    }
    while (this.recvQueue.length === 0) {
      const frames = await this.#recvMany();
      this.recvQueue.push(...frames);
    }
    const frame = this.recvQueue.shift();
    if (frame == null) {
      this.recvDone = true;
      this.recvQueue.length = 0;
      return null;
    }
    return frame;
  }

  /** Sends the terminal streaming status and completes the call. */
  async finishStream(status = Code.Ok, message = "") {
    await this.nativeCall.finishStream(status, message);
    this.completed = true;
  }

  /** Cancels and closes the call. */
  close() {
    this.completed = true;
    this.recvDone = true;
    this.recvQueue.length = 0;
    this.nativeCall.close();
  }

  async #recvMany() {
    if (typeof this.nativeCall.recvMany === "function") {
      return await this.nativeCall.recvMany(RecvManyBatchSize);
    }
    return [await this.nativeCall.recv()];
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
    this.recvQueue = [];
    this.recvTask = null;
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

    await this.#fillRecvQueue();
    const frame = this.recvQueue.shift();
    if (frame == null) {
      return await this.#finishEof();
    }

    if (frame.kind === RpcStreamFrameKind.Status) {
      await this.#finishStatus(frame);
    }
    return { done: false, value: frame };
  }

  async nextBatch() {
    if (this.done) {
      return { done: true, value: undefined };
    }

    await this.#fillRecvQueue();
    const terminalIndex = this.recvQueue.findIndex(isTerminalFrame);
    if (terminalIndex === 0) {
      const frame = this.recvQueue.shift();
      if (frame == null) {
        return await this.#finishEof();
      }
      await this.#finishStatus(frame);
      return { done: false, value: [frame] };
    }

    const count = terminalIndex < 0 ? this.recvQueue.length : terminalIndex;
    const frames = this.recvQueue.splice(0, count);
    return { done: false, value: frames };
  }

  async return() {
    if (this.returnDone == null) {
      this.done = true;
      this.returnDone = (async () => {
        this.recvQueue.length = 0;
        this.stream.close();
        await this.recvTask;
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

  #startRecv() {
    const recv =
      typeof this.stream.recvMany === "function"
        ? this.stream.recvMany(RecvManyBatchSize)
        : this.stream.recv().then((frame) => [frame]);
    return recv.then(
      (frames) => ({ frames }),
      (error) => ({ error }),
    );
  }

  async #fillRecvQueue() {
    while (this.recvQueue.length === 0) {
      this.recvTask ??= this.#startRecv();
      const result = await this.recvTask;
      this.recvTask = null;
      if (result.error != null) {
        this.done = true;
        throw result.error;
      }
      this.recvQueue.push(...result.frames);
      if (this.recvQueue.length > 0 && !hasTerminalFrame(this.recvQueue)) {
        this.recvTask = this.#startRecv();
      }
    }
  }

  async #finishEof() {
    this.done = true;
    this.recvQueue.length = 0;
    await this.writerDone;
    if (this.writerError != null) {
      throw this.writerError;
    }
    return { done: true, value: undefined };
  }

  async #finishStatus(frame) {
    this.done = true;
    this.recvQueue.length = 0;
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
}

function hasTerminalFrame(frames) {
  return frames.some(isTerminalFrame);
}

function isTerminalFrame(frame) {
  return frame == null || frame.kind === RpcStreamFrameKind.Status;
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

async function completeDefault(call, result) {
  if (call.request.kind === RpcKind.Unary) {
    await call.respond(result ?? {});
    return;
  }

  if (isAsyncIterable(result)) {
    for await (const body of result) {
      await call.sendMessage(body);
    }
  }
  await call.finishStream(Code.Ok);
}

async function completeWithError(call, error) {
  const status = Number.isInteger(error?.code) ? error.code : Code.Internal;
  const message = error?.statusMessage ?? error?.message ?? "handler failed";
  try {
    if (call.request.kind === RpcKind.Unary) {
      await call.respond({ status, message });
    } else {
      await call.finishStream(status, message);
    }
  } catch {
    call.close();
  }
}

function responseObject(response) {
  if (response == null) {
    return {};
  }
  if (
    response instanceof Uint8Array ||
    ArrayBuffer.isView(response) ||
    response instanceof ArrayBuffer
  ) {
    return { body: byteBody(response) };
  }
  return {
    ...response,
    body: byteBody(response.body),
  };
}

function rpcKindNumber(kind) {
  if (typeof kind === "number") {
    return kind;
  }
  switch (kind) {
    case "unary":
      return RpcKind.Unary;
    case "clientStreaming":
      return RpcKind.ClientStreaming;
    case "serverStreaming":
      return RpcKind.ServerStreaming;
    case "bidirectionalStreaming":
      return RpcKind.BidirectionalStreaming;
    default:
      throw new TypeError(`unsupported RPC kind ${JSON.stringify(kind)}`);
  }
}

function isAsyncIterable(value) {
  return value != null && typeof value[Symbol.asyncIterator] === "function";
}

function normalizeNodeTransportOptions(urlOrOptions, options) {
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
    throw new TypeError("native transport requires a URL or options object");
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
