import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Code, cancelled, codeFromNumber, invalidArgument } from "./status.js";
import { RpcKind, RpcStreamFrameKind } from "./wire.js";

const require = createRequire(import.meta.url);
const moduleDir = dirname(fileURLToPath(import.meta.url));
const EmptyBody = new Uint8Array(0);
const RpcStatusOk = 0;
const RecvManyBatchSize = 32;
const SendManyBatchSize = 16;

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
  async call(request, options = {}) {
    throwIfAborted(options.signal);
    const cancellation =
      options.signal == null ? undefined : createNativeCancellation(this.nativeClient);
    const cleanupAbort = onAbort(options.signal, () => cancellation?.cancel());
    try {
      const response = await nativeCall(
        this.nativeClient,
        nativeRequest(request, RpcKind.Unary),
        cancellation,
      );
      throwIfAborted(options.signal);
      return response;
    } catch (error) {
      if (options.signal?.aborted) {
        throw signalAbortError(options.signal);
      }
      throw error;
    } finally {
      cleanupAbort();
    }
  }

  /** Starts a streaming RPC and returns native response frames. */
  async streamingCall(request, requestBody, options = {}) {
    throwIfAborted(options.signal);
    const cancellation =
      options.signal == null ? undefined : createNativeCancellation(this.nativeClient);
    const cleanupStartAbort = onAbort(options.signal, () => cancellation?.cancel());
    let stream;
    try {
      stream = await nativeStartStream(
        this.nativeClient,
        nativeRequest(request, RpcKind.ServerStreaming),
        cancellation,
      );
      cleanupStartAbort();
      throwIfAborted(options.signal);
    } catch (error) {
      cleanupStartAbort();
      stream?.close();
      if (options.signal?.aborted) {
        throw signalAbortError(options.signal);
      }
      throw error;
    }

    const cleanupStreamAbort = onAbort(options.signal, () => stream.close());
    const writerTask = writeRequestStream(stream, requestBody);
    return new NativeResponseFrameStream(stream, writerTask, cleanupStreamAbort, options.signal);
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
    this.authorizer = options.authorizer ?? null;
    this.metrics = options.metrics ?? null;
    this.logger = options.logger ?? null;
  }

  /** Creates a native QUIC TrevRPC server backed by trevrpc-c. */
  static async listen(urlOrOptions, options = {}) {
    const native = loadNative();
    const listenOptions = normalizeNodeListenOptions(urlOrOptions, options);
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

  /** Sets an async authorizer that runs before registered handlers. */
  setAuthorizer(authorizer) {
    if (authorizer != null && typeof authorizer !== "function") {
      throw new TypeError("authorizer must be a function");
    }
    this.authorizer = authorizer ?? null;
    return this;
  }

  /** Clears the server authorizer. */
  clearAuthorizer() {
    this.authorizer = null;
    return this;
  }

  /** Sets metrics callbacks. */
  setMetrics(metrics) {
    if (metrics != null && typeof metrics !== "object") {
      throw new TypeError("metrics must be an object");
    }
    this.metrics = metrics ?? null;
    return this;
  }

  /** Clears metrics callbacks. */
  clearMetrics() {
    this.metrics = null;
    return this;
  }

  /** Sets a logger function or logger object. */
  setLogger(logger) {
    if (logger != null && typeof logger !== "function" && typeof logger !== "object") {
      throw new TypeError("logger must be a function or object");
    }
    this.logger = logger ?? null;
    return this;
  }

  /** Clears the logger. */
  clearLogger() {
    this.logger = null;
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
    const call = new NodeServerCall(nativeCall, (completedCall) =>
      this.#recordFinished(completedCall),
    );
    this.#recordStarted(call);
    try {
      const authorization = await this.#authorize(call);
      if (authorization != null) {
        this.#log(
          "warn",
          "rpc.authorization_denied",
          authorization.message,
          call,
          authorization.code,
        );
        await completeWithStatus(call, authorization);
        return;
      }
      const result = await handler(call);
      if (!call.completed && !call.deferred) {
        await completeDefault(call, result);
      }
    } catch (error) {
      this.#log(
        "error",
        "rpc.handler_failed",
        error?.message ?? "handler failed",
        call,
        error?.code,
      );
      if (!call.completed) {
        await completeWithError(call, error);
      }
    }
  }

  async #authorize(call) {
    if (this.authorizer == null) {
      return null;
    }
    try {
      return authorizationStatus(await this.authorizer(call));
    } catch (error) {
      this.#log(
        "error",
        "rpc.authorization_failed",
        error?.message ?? "authorizer failed",
        call,
        error?.code,
      );
      return {
        code: Code.Internal,
        message: error?.statusMessage ?? error?.message ?? "authorizer failed",
        metadata: error?.metadata ?? {},
      };
    }
  }

  #recordStarted(call) {
    const event = rpcStartedEvent(call);
    try {
      this.metrics?.rpcStarted?.(event);
      this.metrics?.started?.(event);
    } catch (error) {
      this.#log(
        "error",
        "metrics.rpc_started_failed",
        error?.message ?? "metrics callback failed",
        call,
      );
    }
  }

  #recordFinished(call) {
    const event = rpcFinishedEvent(call);
    try {
      this.metrics?.rpcFinished?.(event);
      this.metrics?.finished?.(event);
    } catch (error) {
      this.#log(
        "error",
        "metrics.rpc_finished_failed",
        error?.message ?? "metrics callback failed",
        call,
      );
    }
  }

  #log(level, event, message, call, status = undefined) {
    const entry = logEvent(level, event, message, call, status);
    try {
      if (typeof this.logger === "function") {
        this.logger(entry);
      } else {
        this.logger?.log?.(entry);
        this.logger?.[level]?.(entry);
      }
    } catch {
      // Logger failures must not affect RPC handling.
    }
  }
}

/** Raw server call passed to NodeServer handlers. */
export class NodeServerCall {
  constructor(nativeCall, onComplete = () => {}) {
    this.nativeCall = nativeCall;
    this.request = nativeCall.request;
    this.completed = false;
    this.deferred = false;
    this.recvDone = false;
    this.recvQueue = [];
    this.responseBodyLength = 0;
    this.finalStatus = Code.Ok;
    this.startedAt = Date.now();
    this.completedAt = null;
    this.readBatchMaxMessages = RecvManyBatchSize;
    this.writeBatchMaxMessages = SendManyBatchSize;
    this.#onComplete = onComplete;
  }

  #onComplete;

  /** Keeps the call open after the handler returns. */
  defer() {
    this.deferred = true;
    return this;
  }

  /** Sends a unary response and completes the call. */
  async respond(response = {}) {
    const rpcResponse = responseObject(response);
    await this.nativeCall.respond(rpcResponse);
    this.completed = true;
    this.responseBodyLength = rpcResponse.body?.byteLength ?? 0;
    this.finalStatus = codeFromNumber(rpcResponse.status ?? rpcResponse.code ?? Code.Ok);
    this.#completeMetrics();
  }

  /** Sends one streaming response message. */
  sendMessage(body) {
    const bytes = byteBody(body);
    return Promise.resolve(this.nativeCall.sendMessage(bytes)).then(() => {
      this.responseBodyLength += bytes.byteLength;
    });
  }

  /** Sends multiple streaming response messages. */
  sendMany(bodies) {
    const batch = Array.from(bodies, byteBody);
    if (batch.length === 0) {
      return Promise.resolve();
    }
    if (batch.length === 1 || typeof this.nativeCall.sendMessages !== "function") {
      return sendManyIndividually(this, batch);
    }

    let bodyLength = 0;
    for (const body of batch) {
      bodyLength += body.byteLength;
    }
    return Promise.resolve(this.nativeCall.sendMessages(batch)).then(() => {
      this.responseBodyLength += bodyLength;
    });
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
  async finishStream(status = Code.Ok, message = "", metadata = {}) {
    await this.nativeCall.finishStream(status, message, metadata);
    this.completed = true;
    this.finalStatus = codeFromNumber(status);
    this.#completeMetrics();
  }

  /** Cancels and closes the call. */
  close() {
    this.completed = true;
    this.recvDone = true;
    this.recvQueue.length = 0;
    this.nativeCall.close();
    this.finalStatus = Code.Cancelled;
    this.#completeMetrics();
  }

  #completeMetrics() {
    if (this.completedAt != null) {
      return;
    }
    this.completedAt = Date.now();
    this.#onComplete(this);
  }

  async #recvMany() {
    if (typeof this.nativeCall.recvMany === "function") {
      return await this.nativeCall.recvMany(this.readBatchMaxMessages);
    }
    return [await this.nativeCall.recv()];
  }
}

/** Creates an authorizer requiring one metadata key/value pair. */
export function metadataValueAuthorizer(key, value) {
  const normalizedKey = String(key).toLowerCase();
  const expected = metadataBytes(value);
  return (call) => {
    const actual = metadataValue(call.request.metadata, normalizedKey);
    if (actual != null && bytesEqual(actual, expected)) {
      return true;
    }
    return { code: Code.Unauthenticated, message: "request is not authenticated" };
  };
}

/** Creates an authorizer requiring an Authorization: Bearer token metadata value. */
export function bearerAuthorizer(token) {
  const expected = new TextEncoder().encode(`Bearer ${token}`);
  return metadataValueAuthorizer("authorization", expected);
}

class NativeResponseFrameStream {
  constructor(stream, writerTask, cleanupAbort = () => {}, signal = undefined) {
    this.stream = stream;
    this.done = false;
    this.cleanupAbort = cleanupAbort;
    this.signal = signal;
    this.writerError = null;
    this.writerSettled = false;
    this.returnDone = null;
    this.returnErrorReported = false;
    this.suppressReturnWriterError = false;
    this.recvQueue = [];
    this.recvTask = null;
    this.pendingBodyStatus = null;
    this.pendingBodyError = null;
    this.pendingBodyEof = false;
    this.readBatchMaxMessages = RecvManyBatchSize;
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

  async nextBodyBatch(max = this.readBatchMaxMessages) {
    if (this.done) {
      return { done: true, value: undefined };
    }

    if (typeof this.stream.recvBodyBatch !== "function") {
      return await this.#nextBodyBatchFromFrames(max);
    }

    try {
      if (this.pendingBodyStatus != null) {
        const status = this.pendingBodyStatus;
        this.pendingBodyStatus = null;
        await this.#finishStatus(status);
        return { done: false, value: { bodies: [], status } };
      }
      if (this.pendingBodyError != null) {
        const error = this.pendingBodyError;
        this.pendingBodyError = null;
        throw error;
      }
      if (this.pendingBodyEof) {
        this.pendingBodyEof = false;
        return await this.#finishEof();
      }

      const batch = await this.stream.recvBodyBatch(max);
      if (batch == null) {
        return await this.#finishEof();
      }

      const bodies = batch.bodies ?? [];
      if (batch.unknownFrameKind != null) {
        const error = invalidArgument("response stream contained an unknown frame kind");
        if (bodies.length > 0) {
          this.pendingBodyError = error;
          return { done: false, value: { bodies, status: null } };
        }
        throw error;
      }

      const status = batch.status ?? null;
      if (status != null && bodies.length > 0) {
        this.pendingBodyStatus = status;
        return { done: false, value: { bodies, status: null } };
      }
      if (batch.eof && bodies.length === 0) {
        return await this.#finishEof();
      }
      if (batch.eof && bodies.length > 0) {
        this.pendingBodyEof = true;
        return { done: false, value: { bodies, status: null } };
      }
      if (status != null) {
        await this.#finishStatus(status);
      }
      return { done: false, value: { bodies, status } };
    } catch (error) {
      this.done = true;
      this.cleanupAbort();
      if (this.signal?.aborted) {
        throw signalAbortError(this.signal);
      }
      throw error;
    }
  }

  async return() {
    if (this.returnDone == null) {
      this.done = true;
      this.returnDone = (async () => {
        this.cleanupAbort();
        this.recvQueue.length = 0;
        this.pendingBodyStatus = null;
        this.pendingBodyError = null;
        this.pendingBodyEof = false;
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
        ? this.stream.recvMany(this.readBatchMaxMessages)
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
        this.cleanupAbort();
        if (this.signal?.aborted) {
          throw signalAbortError(this.signal);
        }
        throw result.error;
      }
      this.recvQueue.push(...result.frames);
      if (this.recvQueue.length > 0 && !hasTerminalFrame(this.recvQueue)) {
        this.recvTask = this.#startRecv();
      }
    }
  }

  async #nextBodyBatchFromFrames(max) {
    try {
      await this.#fillRecvQueue();
      const frame = this.recvQueue[0];
      if (frame == null) {
        this.recvQueue.shift();
        return await this.#finishEof();
      }
      if (frame.kind === RpcStreamFrameKind.Status) {
        this.recvQueue.shift();
        await this.#finishStatus(frame);
        return { done: false, value: { bodies: [], status: frame } };
      }
      if (frame.kind !== RpcStreamFrameKind.Message) {
        throw invalidArgument("response stream contained an unknown frame kind");
      }

      const limit = Math.max(1, Math.floor(max));
      const bodies = [];
      while (bodies.length < limit && this.recvQueue.length > 0) {
        const queued = this.recvQueue[0];
        if (isTerminalFrame(queued) || queued.kind !== RpcStreamFrameKind.Message) {
          break;
        }
        this.recvQueue.shift();
        bodies.push(queued.body ?? EmptyBody);
      }
      return { done: false, value: { bodies, status: null } };
    } catch (error) {
      this.done = true;
      this.cleanupAbort();
      if (this.signal?.aborted) {
        throw signalAbortError(this.signal);
      }
      throw error;
    }
  }

  async #finishEof() {
    this.done = true;
    this.cleanupAbort();
    this.recvQueue.length = 0;
    await this.writerDone;
    if (this.writerError != null) {
      throw this.writerError;
    }
    return { done: true, value: undefined };
  }

  async #finishStatus(frame) {
    this.done = true;
    this.cleanupAbort();
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
  const iterator = requestBody[Symbol.asyncIterator]();
  try {
    for (;;) {
      const result = await nextRequestBodyBatch(iterator, SendManyBatchSize);
      if (result.done) {
        break;
      }
      const bodies = result.value.map(byteBody);
      if (bodies.length === 1 || typeof stream.sendMessages !== "function") {
        for (const body of bodies) {
          await stream.sendMessage(body);
        }
      } else if (bodies.length > 1) {
        await stream.sendMessages(bodies);
      }
    }
    await stream.finishSend();
  } catch (error) {
    try {
      await iterator.return?.();
    } catch {
      // Preserve the upload error; closing the native stream releases resources.
    }
    stream.close();
    throw error;
  }
}

async function nextRequestBodyBatch(iterator, maxMessages) {
  if (typeof iterator.nextBatch === "function") {
    return await iterator.nextBatch(maxMessages);
  }
  const result = await iterator.next();
  return result.done ? result : { done: false, value: [result.value] };
}

async function completeDefault(call, result) {
  if (call.request.kind === RpcKind.Unary) {
    await call.respond(result ?? {});
    return;
  }

  if (isAsyncIterable(result)) {
    await sendResponseMessages(call, result);
  }
  await call.finishStream(Code.Ok);
}

async function sendResponseMessages(call, messages) {
  const iterator = messages[Symbol.asyncIterator]();
  try {
    for (;;) {
      const result = await nextResponseBodyBatch(iterator, call.writeBatchMaxMessages);
      if (result.done) {
        return;
      }
      await call.sendMany(result.value);
    }
  } catch (error) {
    try {
      await iterator.return?.();
    } catch {
      // Preserve the send or iterator error, matching for-await cleanup behavior.
    }
    throw error;
  }
}

async function nextResponseBodyBatch(iterator, maxMessages) {
  if (typeof iterator.nextBatch === "function") {
    return await iterator.nextBatch(maxMessages);
  }
  const result = await iterator.next();
  return result.done ? result : { done: false, value: [result.value] };
}

async function sendManyIndividually(call, bodies) {
  for (const body of bodies) {
    await call.sendMessage(body);
  }
}

async function completeWithError(call, error) {
  const status = Number.isInteger(error?.code) ? error.code : Code.Internal;
  const message = error?.statusMessage ?? error?.message ?? "handler failed";
  const metadata = error?.metadata ?? {};
  try {
    if (call.request.kind === RpcKind.Unary) {
      await call.respond({ status, message, metadata });
    } else {
      await call.finishStream(status, message, metadata);
    }
  } catch {
    call.close();
  }
}

async function completeWithStatus(call, status) {
  if (call.request.kind === RpcKind.Unary) {
    await call.respond({ status: status.code, message: status.message, metadata: status.metadata });
  } else {
    await call.finishStream(status.code, status.message, status.metadata);
  }
}

function authorizationStatus(result) {
  if (result == null || result === true) {
    return null;
  }
  if (result === false) {
    return { code: Code.PermissionDenied, message: "request denied", metadata: {} };
  }
  if (typeof result === "object") {
    const code = codeFromNumber(result.code ?? result.status ?? Code.PermissionDenied);
    if (code === Code.Ok) {
      return null;
    }
    return {
      code,
      message: result.message ?? result.statusMessage ?? "request denied",
      metadata: result.metadata ?? {},
    };
  }
  return null;
}

function rpcStartedEvent(call) {
  return {
    service: call.request.service,
    method: call.request.method,
    requestBodyLength: call.request.body?.byteLength ?? 0,
    kind: call.request.kind,
  };
}

function rpcFinishedEvent(call) {
  return {
    ...rpcStartedEvent(call),
    responseBodyLength: call.responseBodyLength,
    status: call.finalStatus,
    elapsedMs:
      call.completedAt == null
        ? undefined
        : call.completedAt - (call.startedAt ?? call.completedAt),
  };
}

function logEvent(level, event, message, call, status) {
  return {
    level,
    event,
    message,
    service: call?.request?.service,
    method: call?.request?.method,
    status: status == null ? undefined : codeFromNumber(status),
  };
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
  const status = response.status ?? response.code;
  return {
    ...response,
    status,
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
    return {
      ...options,
      host: url.hostname,
      port: Number(url.port || 443),
    };
  }

  if (urlOrOptions == null || typeof urlOrOptions !== "object") {
    throw new TypeError("native transport requires a URL or options object");
  }
  return { ...urlOrOptions, ...options };
}

function normalizeNodeListenOptions(urlOrOptions, options) {
  if (typeof urlOrOptions === "string" || urlOrOptions instanceof URL) {
    const url = new URL(urlOrOptions);
    return {
      ...options,
      host: url.hostname,
      port: Number(url.port || 443),
      path: `${url.pathname || "/"}${url.search}`,
      origin: options.origin ?? url.origin,
    };
  }

  if (urlOrOptions == null || typeof urlOrOptions !== "object") {
    throw new TypeError("native server requires a URL or options object");
  }
  return { ...urlOrOptions, ...options };
}

function createNativeCancellation(nativeClient) {
  return typeof nativeClient.createCancellation === "function"
    ? nativeClient.createCancellation()
    : undefined;
}

function nativeCall(nativeClient, request, cancellation) {
  return cancellation == null
    ? nativeClient.call(request)
    : nativeClient.call(request, cancellation);
}

function nativeStartStream(nativeClient, request, cancellation) {
  return cancellation == null
    ? nativeClient.startStream(request)
    : nativeClient.startStream(request, cancellation);
}

function onAbort(signal, callback) {
  if (signal == null) {
    return () => {};
  }
  if (signal.aborted) {
    callback();
    return () => {};
  }
  signal.addEventListener("abort", callback, { once: true });
  return () => signal.removeEventListener("abort", callback);
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw signalAbortError(signal);
  }
}

function signalAbortError(signal) {
  const reason = signal?.reason;
  return reason?.name === "TrevRpcError" ? reason : cancelled("RPC cancelled");
}

function nativeRequest(request, defaultKind) {
  const native = {
    service: request.service,
    method: request.method,
    body: byteBody(request.body),
    metadata: request.metadata ?? {},
  };
  if (request.kind != null || defaultKind !== RpcKind.Unary) {
    native.kind = request.kind ?? defaultKind;
  }
  if (request.version != null) {
    native.version = request.version;
  }
  if (request.timeoutNanos != null) {
    native.timeoutNanos = request.timeoutNanos;
  }
  return native;
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

function metadataBytes(value) {
  if (typeof value === "string") {
    return new TextEncoder().encode(value);
  }
  return byteBody(value);
}

function metadataValue(metadata, key) {
  if (metadata == null) {
    return null;
  }
  return metadata[key] ?? metadata[String(key).toLowerCase()] ?? null;
}

function bytesEqual(left, right) {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let i = 0; i < left.byteLength; i += 1) {
    if (left[i] !== right[i]) {
      return false;
    }
  }
  return true;
}

function loadNative() {
  return require(join(moduleDir, "..", "build", "native", "trevrpc_native.node"));
}
