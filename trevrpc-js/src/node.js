import { getSystemErrorName } from "node:util";

import { ChannelStateMachine, stripChannelOptions, waitForInitialReady } from "./channel.js";
import { RequestWriterSettlement } from "./client.js";
import { loadNativeAddon } from "./native-loader.js";
import { Code, TrevRpcError, cancelled, codeFromNumber, invalidArgument } from "./status.js";
import { scheduleTimeout } from "./timer.js";
import { RpcKind, RpcStreamFrameKind } from "./wire.js";

const EmptyBody = new Uint8Array(0);
const RecvManyBatchSize = 32;
const SendManyBatchSize = 16;
const NativeTransportClosed = -1001;
const NativeFrameLimit = -1002;
const NativeMsQuicTimeout = -1003;
const NativeStreamLimit = -1004;
const NativeInvalidFrame = -2001;
const NativeUnsupportedWireVersion = -2002;
const NativeInvalidFrameKind = -2003;
const NativeSendByteLimit = -2005;
const NativeSendCountLimit = -2006;
const NativeObjectClosed = -4001;

/** Native Node transport backed by trevrpc-c and MsQuic. */
export class RawNodeTransport {
  constructor(nativeClient, options = {}) {
    this.nativeClient = nativeClient;
    this.maxFrameSize = options.maxFrameSize;
  }

  /** Opens a TrevRPC client backed by the native C runtime. */
  static async connect(urlOrOptions, options = {}) {
    const connectOptions = normalizeNodeTransportOptions(urlOrOptions, options);
    throwIfAborted(connectOptions.signal);
    const native = loadNative();
    const cancellation = connectOptions.signal == null ? undefined : native.createCancellation();
    const cleanupAbort = onAbort(connectOptions.signal, () => cancellation?.cancel());
    let nativeClient;
    try {
      nativeClient = await nativeConnect(native, connectOptions, cancellation);
      throwIfAborted(connectOptions.signal);
      return new RawNodeTransport(nativeClient, connectOptions);
    } catch (error) {
      if (connectOptions.signal?.aborted) {
        closeNativeObjectQuietly(nativeClient);
        throw signalAbortError(connectOptions.signal);
      }
      throw nativeError(error, "connect");
    } finally {
      cleanupAbort();
    }
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
      throw nativeError(error, "unary call");
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
      closeNativeObjectQuietly(stream);
      if (options.signal?.aborted) {
        throw signalAbortError(options.signal);
      }
      throw nativeError(error, "start streaming call");
    }

    const closeStream = once(() => closeNativeObject(stream, "close response stream"));
    const writerSettlement = new RequestWriterSettlement(
      (error) => error,
      isNativeCleanupWriterError,
      isNativeCleanupWriterError,
      true,
    );
    writerSettlement.track(
      writeRequestStream(stream, requestBody, closeStream, (error) =>
        writerSettlement.recordError(error),
      ),
    );
    return new NativeResponseFrameStream(stream, writerSettlement, closeStream, options.signal);
  }

  /** Closes the underlying native client. */
  close() {
    try {
      this.nativeClient.close();
    } catch (error) {
      throw nativeError(error, "close client");
    }
  }

  /** Resolves when the underlying native connection closes. */
  get closed() {
    return this.nativeClient.closed;
  }
}

/** Native Node channel with background connection reconnection. */
export class Channel extends ChannelStateMachine {
  constructor(target, options = {}) {
    const normalized = normalizeNodeChannelOptions(target, options);
    const transportOptions = stripChannelOptions(normalized.options);
    super(
      (signal) => RawNodeTransport.connect(normalized.endpoint, { ...transportOptions, signal }),
      normalized.options,
      (transport) => transport.closed,
      (error) => error?.nativeCode === NativeTransportClosed,
    );
    this.endpoint = normalized.endpoint;
    this.options = normalized.options;
  }

  /** Creates a native channel and waits for its first ready generation. */
  static async connect(target, options = {}) {
    const channel = new Channel(target, options);
    await waitForInitialReady(channel, channel.options);
    return channel;
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
    this.activeCalls = new Set();
  }

  /** Creates a native QUIC TrevRPC server backed by trevrpc-c. */
  static async listen(urlOrOptions, options = {}) {
    try {
      const native = loadNative();
      const listenOptions = normalizeNodeListenOptions(urlOrOptions, options);
      const nativeServer = await native.listenMsQuic(listenOptions);
      return new NodeServer(nativeServer, listenOptions);
    } catch (error) {
      throw nativeError(error, "listen");
    }
  }

  /** Registers one raw RPC handler. */
  register(service, method, kind, handler) {
    if (typeof handler !== "function") {
      throw new TypeError("register requires a handler function");
    }
    try {
      this.nativeServer.register(service, method, rpcKindNumber(kind), (nativeCall) => {
        void this.#dispatch(handler, nativeCall);
      });
      return this;
    } catch (error) {
      throw nativeError(error, "register server handler");
    }
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
    this.closed ??= Promise.resolve()
      .then(() => this.nativeServer.serve())
      .catch((error) => {
        throw nativeError(error, "serve");
      });
    return this.closed;
  }

  /** Requests graceful server shutdown and drains already admitted calls. */
  close() {
    try {
      this.nativeServer.close();
    } catch (error) {
      throw nativeError(error, "close server");
    }
  }

  async #dispatch(handler, nativeCall) {
    const call = new NodeServerCall(nativeCall, (completedCall) => {
      this.activeCalls.delete(completedCall);
      this.#recordFinished(completedCall);
    });
    this.activeCalls.add(call);
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
    this.#abortController = new AbortController();
    this.signal = this.#abortController.signal;

    const context = nativeCall.context ?? {};
    const remainingNanos = context.hasDeadline ? bigintValue(context.timeRemainingNanos) : null;
    const remainingMs = remainingNanos == null ? null : Number(remainingNanos) / 1_000_000;
    this.deadline = remainingMs == null ? null : new Date(this.startedAt + remainingMs);
    if (context.cancelled) {
      queueMicrotask(() => this.#cancel(cancelled("RPC cancelled by peer")));
    } else if (remainingMs != null) {
      const deadlineError = new TrevRpcError(Code.DeadlineExceeded, "RPC deadline exceeded");
      this.#cancelDeadline = scheduleTimeout(
        () => this.#completeDeadline(deadlineError),
        Math.max(0, remainingMs),
      );
    }
  }

  #abortController;
  #cancelDeadline = () => {};
  #onComplete;
  #terminalError = null;
  #terminalSettled = false;

  /** Keeps the call open after the handler returns. */
  defer() {
    this.#throwIfTerminal();
    this.deferred = true;
    return this;
  }

  /** Sends a unary response and completes the call. */
  async respond(response = {}) {
    this.#throwIfTerminal();
    const rpcResponse = responseObject(response);
    const status = codeFromNumber(rpcResponse.status ?? rpcResponse.code ?? Code.Ok);
    this.responseBodyLength = rpcResponse.body?.byteLength ?? 0;
    this.#claimTerminal(status);
    try {
      await this.nativeCall.respond(rpcResponse);
      if (!this.#settleTerminalSuccess()) {
        throw this.#terminalError;
      }
    } catch (error) {
      throw this.#settleTerminalFailure(error, "send unary response");
    }
  }

  /** Sends one streaming response message. */
  async sendMessage(body) {
    this.#throwIfTerminal();
    const bytes = byteBody(body);
    try {
      await this.nativeCall.sendMessage(bytes);
      this.#throwIfTerminal();
      this.responseBodyLength += bytes.byteLength;
    } catch (error) {
      if (this.#terminalError != null) {
        throw this.#terminalError;
      }
      throw this.#observeNativeIoError(error, "send streaming response");
    }
  }

  /** Sends multiple streaming response messages. */
  async sendMany(bodies) {
    this.#throwIfTerminal();
    const batch = Array.from(bodies, byteBody);
    if (batch.length === 0) {
      return;
    }
    if (batch.length === 1 || typeof this.nativeCall.sendMessages !== "function") {
      await sendManyIndividually(this, batch);
      return;
    }

    let bodyLength = 0;
    for (const body of batch) {
      bodyLength += body.byteLength;
    }
    try {
      await this.nativeCall.sendMessages(batch);
      this.#throwIfTerminal();
      this.responseBodyLength += bodyLength;
    } catch (error) {
      if (this.#terminalError != null) {
        throw this.#terminalError;
      }
      throw this.#observeNativeIoError(error, "send streaming response batch");
    }
  }

  /** Receives one streaming request frame, or null after EOF. */
  async recv() {
    this.#throwIfTerminal();
    if (this.recvDone) {
      return null;
    }
    while (this.recvQueue.length === 0) {
      const frames = await this.#recvMany();
      this.#throwIfTerminal();
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
    this.#throwIfTerminal();
    const normalizedStatus = codeFromNumber(status);
    this.#claimTerminal(normalizedStatus);
    try {
      await this.nativeCall.finishStream(normalizedStatus, message, metadata);
      if (!this.#settleTerminalSuccess()) {
        throw this.#terminalError;
      }
    } catch (error) {
      throw this.#settleTerminalFailure(error, "finish response stream");
    }
  }

  /** Cancels and closes the call. */
  close(reason = cancelled("RPC cancelled")) {
    if (this.#terminalSettled) {
      return;
    }
    const error = cancellationError(reason);
    if (!this.completed) {
      this.#claimTerminal(error.code, error);
    }
    this.#settleLocalTerminal(error);
    closeNativeObject(this.nativeCall, "close server call");
  }

  #cancel(error) {
    if (this.#terminalSettled) {
      return;
    }
    if (!this.completed) {
      this.#claimTerminal(error.code, error);
    }
    this.#settleLocalTerminal(error);
    closeNativeObjectQuietly(this.nativeCall);
  }

  #completeDeadline(error) {
    if (this.#terminalSettled) {
      return;
    }
    const sendStatus = !this.completed;
    if (sendStatus) {
      this.#claimTerminal(error.code, error);
    }
    this.#settleLocalTerminal(error);
    if (sendStatus) {
      void this.#sendAutomaticDeadline(error);
    } else {
      closeNativeObjectQuietly(this.nativeCall);
    }
  }

  async #sendAutomaticDeadline(error) {
    try {
      if (this.request.kind === RpcKind.Unary) {
        await this.nativeCall.respond({
          status: error.code,
          message: error.statusMessage,
          metadata: error.metadata,
        });
      } else {
        await this.nativeCall.finishStream(error.code, error.statusMessage, error.metadata);
      }
    } catch {
      // The deadline remains the primary terminal reason.
    } finally {
      closeNativeObjectQuietly(this.nativeCall);
    }
  }

  #claimTerminal(status, error = null) {
    if (this.completed) {
      return false;
    }
    this.completed = true;
    this.finalStatus = codeFromNumber(status);
    this.#terminalError = error;
    return true;
  }

  #settleTerminalSuccess() {
    if (this.#terminalSettled) {
      return false;
    }
    this.#terminalSettled = true;
    this.#clearDeadline();
    this.#completeMetrics();
    return true;
  }

  #settleTerminalFailure(error, operation) {
    if (this.#terminalSettled) {
      return this.#terminalError ?? nativeError(error, operation);
    }
    const normalized = nativeError(error, operation);
    this.#terminalSettled = true;
    this.#clearDeadline();
    this.finalStatus = normalized.code;
    this.#terminalError = normalized;
    this.#abortController.abort(normalized);
    this.recvDone = true;
    this.recvQueue.length = 0;
    closeNativeObjectQuietly(this.nativeCall);
    this.#completeMetrics();
    return normalized;
  }

  #settleLocalTerminal(error) {
    this.#terminalSettled = true;
    this.#clearDeadline();
    this.finalStatus = error.code;
    this.#terminalError = error;
    this.#abortController.abort(error);
    this.recvDone = true;
    this.recvQueue.length = 0;
    this.#completeMetrics();
  }

  #clearDeadline() {
    this.#cancelDeadline();
    this.#cancelDeadline = () => {};
  }

  #throwIfTerminal() {
    if (this.#terminalError != null) {
      throw this.#terminalError;
    }
    if (this.completed) {
      throw new TrevRpcError(Code.FailedPrecondition, "server call already completed");
    }
  }

  #completeMetrics() {
    if (this.completedAt != null) {
      return;
    }
    this.completedAt = Date.now();
    this.#onComplete(this);
  }

  async #recvMany() {
    try {
      if (typeof this.nativeCall.recvMany === "function") {
        return await this.nativeCall.recvMany(this.readBatchMaxMessages);
      }
      return [await this.nativeCall.recv()];
    } catch (error) {
      if (this.#terminalError != null) {
        throw this.#terminalError;
      }
      throw this.#observeNativeIoError(error, "receive request stream");
    }
  }

  #observeNativeIoError(error, operation) {
    const normalized = nativeError(error, operation);
    if (
      !this.completed &&
      (normalized.code === Code.Cancelled || normalized.code === Code.DeadlineExceeded)
    ) {
      this.#cancel(normalized);
    }
    return this.#terminalError ?? normalized;
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
  constructor(stream, writerSettlement, closeStream, signal = undefined) {
    this.stream = stream;
    this.done = false;
    this.closeStream = closeStream;
    this.signal = signal;
    this.cleanupAbort = () => {};
    this.cleanupDone = null;
    this.recvQueue = [];
    this.recvTask = null;
    this.pendingBodyError = null;
    this.pendingBodyEof = false;
    this.readBatchMaxMessages = RecvManyBatchSize;
    this.writerSettlement = writerSettlement;
    this.cleanupAbort = onAbort(signal, () => this.startCleanup());
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    if (this.done) {
      return await this.#finishDone();
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
      return await this.#finishDone();
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
      return await this.#finishDone();
    }

    if (typeof this.stream.recvBodyBatch !== "function") {
      return await this.#nextBodyBatchFromFrames(max);
    }

    try {
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
      this.startCleanup();
      if (this.signal?.aborted) {
        throw signalAbortError(this.signal);
      }
      throw nativeError(error, "receive response stream");
    }
  }

  async return() {
    this.startCleanup();
    return await this.#finishDone();
  }

  startCleanup() {
    this.writerSettlement.beginCleanup();
    if (this.cleanupDone != null) {
      return;
    }

    this.done = true;
    this.cleanupAbort();
    this.recvQueue.length = 0;
    this.pendingBodyError = null;
    this.pendingBodyEof = false;
    const recvTask = this.recvTask;
    let closeError;
    try {
      this.closeStream();
    } catch (error) {
      closeError = error;
    }
    this.cleanupDone = (async () => {
      await recvTask;
      if (closeError != null) {
        throw closeError;
      }
    })();
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
        this.startCleanup();
        if (this.signal?.aborted) {
          throw signalAbortError(this.signal);
        }
        throw nativeError(result.error, "receive response stream");
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
      const status = this.recvQueue[0];
      if (status?.kind === RpcStreamFrameKind.Status) {
        this.recvQueue.shift();
        await this.#finishStatus(status);
        return { done: false, value: { bodies, status } };
      }
      return { done: false, value: { bodies, status: null } };
    } catch (error) {
      this.startCleanup();
      if (this.signal?.aborted) {
        throw signalAbortError(this.signal);
      }
      throw nativeError(error, "receive response stream");
    }
  }

  async #finishEof() {
    this.startCleanup();
    return await this.#finishDone();
  }

  async #finishDone() {
    await this.cleanupDone;
    await this.writerSettlement.finish();
    return { done: true, value: undefined };
  }

  #finishStatus(frame) {
    this.writerSettlement.recordTerminal(frame.status ?? Code.Ok);
  }
}

function hasTerminalFrame(frames) {
  return frames.some(isTerminalFrame);
}

function isTerminalFrame(frame) {
  return frame == null || frame.kind === RpcStreamFrameKind.Status;
}

function isNativeCleanupWriterError(error) {
  return (
    error?.code === Code.Cancelled ||
    error?.nativeCode === NativeTransportClosed ||
    error?.nativeCode === NativeObjectClosed ||
    nativeErrorName(error?.nativeCode) === "ECANCELED"
  );
}

function nativeErrorName(code) {
  if (!Number.isInteger(code)) {
    return null;
  }
  try {
    return getSystemErrorName(code);
  } catch {
    return null;
  }
}

async function writeRequestStream(stream, requestBody, closeStream, recordError) {
  let iterator;
  try {
    iterator = requestBody[Symbol.asyncIterator]();
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
    const normalized = nativeError(error, "send request stream");
    recordError(normalized);
    try {
      await iterator?.return?.();
    } catch {
      // Preserve the upload error; closing the native stream releases resources.
    }
    closeStream();
    throw normalized;
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

  if (call.request.kind === RpcKind.ClientStreaming) {
    if (result == null) {
      await call.finishStream(Code.Ok);
      return;
    }
    const response = responseObject(result);
    await call.sendMessage(response.body);
    await call.finishStream(
      response.status ?? Code.Ok,
      response.message ?? "",
      response.metadata ?? {},
    );
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
    response instanceof ArrayBuffer ||
    Array.isArray(response)
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

function normalizeNodeTransportOptions(target, options) {
  const normalized =
    typeof target === "string" || target instanceof URL
      ? endpointFromUrl(target, options)
      : target != null && typeof target === "object"
        ? { port: 443, ...target, ...options }
        : null;
  if (normalized == null) {
    throw new TypeError("native transport requires a URL or endpoint object");
  }
  validateNodeEndpoint(normalized);
  return normalized;
}

function normalizeNodeChannelOptions(target, options) {
  const normalized = normalizeNodeTransportOptions(target, options);
  const endpoint = Object.freeze({ host: normalized.host, port: normalized.port });
  const effectiveOptions = { ...normalized };
  delete effectiveOptions.host;
  delete effectiveOptions.port;
  return Object.freeze({ endpoint, options: Object.freeze(effectiveOptions) });
}

function endpointFromUrl(target, options) {
  const url = new URL(target);
  return {
    ...options,
    host: url.hostname,
    port: Number(url.port || 443),
  };
}

function validateNodeEndpoint(endpoint) {
  if (typeof endpoint.host !== "string" || endpoint.host.length === 0) {
    throw new TypeError("native endpoint host must be a non-empty string");
  }
  if (!Number.isInteger(endpoint.port) || endpoint.port < 1 || endpoint.port > 65_535) {
    throw new TypeError("native endpoint port must be an integer from 1 through 65535");
  }
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

function nativeConnect(native, options, cancellation) {
  return cancellation == null
    ? native.connectMsQuic(options)
    : native.connectMsQuic(options, cancellation);
}

function once(callback) {
  let called = false;
  return () => {
    if (!called) {
      called = true;
      callback();
    }
  };
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
  return cancellationError(signal?.reason);
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
  try {
    return loadNativeAddon();
  } catch (error) {
    throw new TrevRpcError(
      Code.Unavailable,
      error?.message ?? "native addon unavailable",
      {},
      {
        cause: error?.cause ?? error,
      },
    );
  }
}

function nativeError(error, operation) {
  if (error instanceof TrevRpcError) {
    return error;
  }
  const nativeCode = Number.isInteger(error?.nativeCode)
    ? error.nativeCode
    : Number.isInteger(error?.code) && (error.code < 0 || error.code > Code.Unauthenticated)
      ? error.code
      : undefined;
  const code = nativeStatusCode(error, nativeCode);
  const detail = error?.message ?? String(error);
  return new TrevRpcError(code, `${operation} failed: ${detail}`, error?.metadata ?? {}, {
    cause: error,
    nativeCode,
  });
}

function nativeStatusCode(error, nativeCode) {
  if (Number.isInteger(error?.status)) {
    return codeFromNumber(error.status);
  }
  if (
    Number.isInteger(error?.code) &&
    error.code >= Code.Ok &&
    error.code <= Code.Unauthenticated
  ) {
    return codeFromNumber(error.code);
  }
  if (nativeCode === NativeObjectClosed || nativeErrorName(nativeCode) === "ECANCELED") {
    return Code.Cancelled;
  }
  if (nativeCode === NativeMsQuicTimeout || nativeErrorName(nativeCode) === "ETIMEDOUT") {
    return Code.DeadlineExceeded;
  }
  if (
    nativeCode === NativeFrameLimit ||
    nativeCode === NativeStreamLimit ||
    nativeCode === NativeSendByteLimit ||
    nativeCode === NativeSendCountLimit
  ) {
    return Code.ResourceExhausted;
  }
  if (nativeCode === NativeInvalidFrame || nativeCode === NativeInvalidFrameKind) {
    return Code.InvalidArgument;
  }
  if (nativeCode === NativeUnsupportedWireVersion) {
    return Code.FailedPrecondition;
  }
  return Code.Unavailable;
}

function cancellationError(reason) {
  if (reason instanceof TrevRpcError) {
    return reason;
  }
  const error = cancelled(typeof reason === "string" ? reason : "RPC cancelled");
  if (reason !== undefined) {
    Object.defineProperty(error, "cause", { configurable: true, value: reason });
  }
  return error;
}

function closeNativeObject(value, operation) {
  if (value == null) {
    return;
  }
  try {
    value.close();
  } catch (error) {
    throw nativeError(error, operation);
  }
}

function closeNativeObjectQuietly(value) {
  try {
    value?.close();
  } catch {
    // Preserve the primary operation error.
  }
}

function bigintValue(value) {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return BigInt(Math.floor(value));
  }
  if (typeof value === "string" && /^\d+$/u.test(value)) {
    return BigInt(value);
  }
  return 0n;
}
