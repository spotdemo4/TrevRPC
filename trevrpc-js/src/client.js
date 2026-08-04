import { DefaultMaxFrameSize, marshalMessage, unmarshalMessage } from "./framing.js";
import { normalizeMetadata, validateMetadata } from "./metadata.js";
import {
  Code,
  FrameTooLargeError,
  TrevRpcError,
  cancelled,
  deadlineExceeded,
  internal,
  invalidArgument,
  isOkStatus,
  resourceExhausted,
  statusFromResponse,
  unavailable,
} from "./status.js";
import { scheduleTimeout } from "./timer.js";
import { RpcKind, RpcStreamFrameKind, WireVersion } from "./wire.js";

const RequestStreamBatchSize = 16;
const PreparedCallOptions = Symbol("trevrpc.preparedCallOptions");
const EmptyMetadata = Object.freeze(Object.create(null));
const WriterTerminalNone = 0;
const WriterTerminalOk = 1;
const WriterTerminalNonOk = 2;

/** Internal stream-state failure with a stable diagnostic reason. */
class StreamStateError extends TrevRpcError {
  constructor(reason, message) {
    super(Code.Internal, message);
    this.name = "StreamStateError";
    this.reason = reason;
  }
}

/** Coordinates request writer completion with response stream cleanup. */
export class RequestWriterSettlement {
  constructor(
    normalizeError = (error) => error,
    isCleanupError = () => false,
    isTerminalError = () => false,
    terminalErrorRequiresTerminalFirst = false,
  ) {
    this._terminal = WriterTerminalNone;
    this._cleanupStarted = false;
    this._hasWriterError = false;
    this._writerError = undefined;
    this._errorAfterCleanup = false;
    this._errorAfterTerminal = false;
    this._errorReported = false;
    this._normalizeError = normalizeError;
    this._isCleanupError = isCleanupError;
    this._isTerminalError = isTerminalError;
    this._terminalErrorRequiresTerminalFirst = terminalErrorRequiresTerminalFirst;
    this.done = Promise.resolve();
  }

  track(writerTask) {
    this.done = Promise.resolve(writerTask).then(
      () => {},
      (error) => this.recordError(error),
    );
  }

  recordError(error) {
    if (this._hasWriterError) {
      return;
    }
    this._hasWriterError = true;
    this._writerError = this._normalizeError(error);
    this._errorAfterCleanup = this._cleanupStarted;
    this._errorAfterTerminal = this._terminal !== WriterTerminalNone;
  }

  recordTerminal(statusCode) {
    if (this._terminal === WriterTerminalNone) {
      this._terminal = statusCode === Code.Ok ? WriterTerminalOk : WriterTerminalNonOk;
    }
  }

  beginCleanup() {
    this._cleanupStarted = true;
  }

  async finish() {
    await this.done;
    if (
      !this._hasWriterError ||
      this._terminal === WriterTerminalNonOk ||
      (this._terminal === WriterTerminalOk &&
        (!this._terminalErrorRequiresTerminalFirst || this._errorAfterTerminal) &&
        this._isTerminalError(this._writerError)) ||
      (this._errorAfterCleanup && this._isCleanupError(this._writerError)) ||
      this._errorReported
    ) {
      return;
    }

    this._errorReported = true;
    throw this._writerError;
  }
}

/** Returns the default client call options. */
export function defaultCallOptions() {
  return {
    timeoutMs: undefined,
    maxResponseBodySize: DefaultMaxFrameSize,
    maxResponseMessages: 4096,
    maxResponseStreamBodySize: 16 * 1024 * 1024,
    streamIdleTimeoutMs: 30_000,
    metadata: {},
    signal: undefined,
  };
}

/** Merges call options and normalizes metadata. */
export function mergeCallOptions(base = {}, override = {}) {
  if (isPreparedCallOptions(base) && !hasDefinedOptions(override)) {
    return base;
  }

  const merged = { ...defaultCallOptions() };
  copyDefined(merged, base);
  copyDefined(merged, override);
  merged.metadata = mergeMetadata(base, override);
  return markPreparedCallOptions(merged);
}

function preparedCallOptions(options = {}) {
  return isPreparedCallOptions(options) ? options : mergeCallOptions(options);
}

/** Calls a unary RPC and decodes the protobuf response. */
export async function unary(
  transport,
  service,
  method,
  requestType,
  responseType,
  request,
  options = {},
) {
  return (
    await unaryWithResponse(transport, service, method, requestType, responseType, request, options)
  ).message;
}

/** Calls a unary RPC and returns the decoded response envelope. */
export async function unaryWithResponse(
  transport,
  service,
  method,
  requestType,
  responseType,
  request,
  options = {},
) {
  const callOptions = preparedCallOptions(options);
  const abortScope = callAbortScope(callOptions);

  try {
    throwIfAborted(abortScope.signal);
    const requestBody = marshalMessage(requestType, request);
    const rpcRequest = prepareClientRequest(
      service,
      method,
      RpcKind.Unary,
      requestBody,
      abortScope.options,
    );
    const response = await waitForOperation(
      transport.call(rpcRequest, abortScope.options),
      abortScope.signal,
    );
    const responseStatus = validateResponse(response, callOptions.maxResponseBodySize);
    abortScope.terminal();
    if (!isOkStatus(responseStatus)) {
      throw responseStatus;
    }
    return {
      message: unmarshalMessage(responseType, response.body ?? new Uint8Array(0)),
      metadata: response.metadata ?? {},
    };
  } finally {
    abortScope.cleanup();
  }
}

/** Calls a server-streaming RPC and returns a decoded response stream. */
export async function serverStreaming(
  transport,
  service,
  method,
  requestType,
  responseType,
  request,
  options = {},
) {
  const callOptions = preparedCallOptions(options);
  const abortScope = callAbortScope(callOptions);
  try {
    throwIfAborted(abortScope.signal);
    const requestBody = marshalMessage(requestType, request);
    const rpcRequest = prepareClientRequest(
      service,
      method,
      RpcKind.ServerStreaming,
      requestBody,
      abortScope.options,
    );
    const frames = await waitForOperation(
      transport.streamingCall(rpcRequest, emptyAsyncIterable(), abortScope.options),
      abortScope.signal,
    );
    return responseMessageStream(frames, responseType, abortScope.options, abortScope);
  } catch (error) {
    abortScope.cleanup();
    throw error;
  }
}

/** Calls a client-streaming RPC and returns a sendable call object. */
export async function clientStreaming(
  transport,
  service,
  method,
  requestType,
  responseType,
  options = {},
) {
  const callOptions = preparedCallOptions(options);
  const abortScope = callAbortScope(callOptions, { requestCancellation: true });
  const requests = new RequestQueue(abortScope.signal);
  try {
    const rpcRequest = prepareClientRequest(
      service,
      method,
      RpcKind.ClientStreaming,
      new Uint8Array(0),
      abortScope.options,
    );
    const frames = await waitForOperation(
      transport.streamingCall(
        rpcRequest,
        encodeRequestStream(requestType, requests, abortScope.signal),
        abortScope.options,
      ),
      abortScope.signal,
    );
    return new ClientStreamingCall(
      requests,
      responseMessageStream(frames, responseType, abortScope.options, abortScope, (error) =>
        requests.close(error),
      ),
    );
  } catch (error) {
    requests.close(error);
    abortScope.cleanup();
    throw error;
  }
}

/** Calls a bidirectional-streaming RPC and returns a sendable call object. */
export async function bidirectionalStreaming(
  transport,
  service,
  method,
  requestType,
  responseType,
  options = {},
) {
  const callOptions = preparedCallOptions(options);
  const abortScope = callAbortScope(callOptions, { requestCancellation: true });
  const requests = new RequestQueue(abortScope.signal);
  try {
    const rpcRequest = prepareClientRequest(
      service,
      method,
      RpcKind.BidirectionalStreaming,
      new Uint8Array(0),
      abortScope.options,
    );
    const frames = await waitForOperation(
      transport.streamingCall(
        rpcRequest,
        encodeRequestStream(requestType, requests, abortScope.signal),
        abortScope.options,
      ),
      abortScope.signal,
    );
    return new BidirectionalStreamingCall(
      requests,
      responseMessageStream(frames, responseType, abortScope.options, abortScope, (error) =>
        requests.close(error),
      ),
    );
  } catch (error) {
    requests.close(error);
    abortScope.cleanup();
    throw error;
  }
}

/** A client-streaming RPC call. */
export class ClientStreamingCall {
  constructor(requests, responses) {
    this._requests = requests;
    this._responses = responses;
  }

  /** Sends one request message. */
  send(request) {
    return this._requests.send(request);
  }

  /** Sends multiple request messages. */
  sendMany(requests) {
    return this._requests.sendMany(requests);
  }

  /** Closes the request stream. */
  closeSend() {
    this._requests.close();
    return Promise.resolve();
  }

  /** Closes the request stream and returns the final response. */
  async closeAndRecv() {
    this._requests.close();
    return await readUnaryResponseFromStream(this._responses);
  }

  /** Closes the request stream and returns the final response envelope. */
  async closeAndRecvWithResponse() {
    this._requests.close();
    return await readUnaryResponseEnvelopeFromStream(this._responses);
  }

  /** Releases call resources without waiting for the response. */
  async close(reason) {
    const error = cancellationReason(reason, "request stream cancelled");
    this._requests.close(error);
    await this._responses.close(error);
  }
}

/** A bidirectional-streaming RPC call. */
export class BidirectionalStreamingCall {
  constructor(requests, responses) {
    this._requests = requests;
    this._responses = responses;
    this.status = responses.status;
    this._iterator = responses[Symbol.asyncIterator]();
  }

  /** Sends one request message. */
  send(request) {
    return this._requests.send(request);
  }

  /** Sends multiple request messages. */
  sendMany(requests) {
    return this._requests.sendMany(requests);
  }

  /** Receives one response message, or undefined after the response stream completes. */
  async recv() {
    const result = await this._iterator.next();
    return result.done ? undefined : result.value;
  }

  /** Closes the request stream while keeping the response stream readable. */
  closeSend() {
    this._requests.close();
    return Promise.resolve();
  }

  /** Releases call resources without waiting for more responses. */
  async close(reason) {
    const error = cancellationReason(reason, "request stream cancelled");
    this._requests.close(error);
    await this._responses.close(error);
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const value = await this.recv();
        return value === undefined ? { done: true, value: undefined } : { done: false, value };
      },
      return: async () => {
        await this.close();
        return { done: true, value: undefined };
      },
    };
  }
}

/** Creates a service client from a generated service descriptor. */
export function createServiceClient(transport, service, root, options = {}) {
  const client = {};
  const baseOptions = mergeCallOptions(options);

  for (const [jsName, method] of Object.entries(service.methods)) {
    const requestType = root.lookupType(method.inputType);
    const responseType = root.lookupType(method.outputType);
    const callOptions = (override) => mergeCallOptions(baseOptions, override ?? {});

    switch (method.kind) {
      case "unary":
        client[jsName] = (request, override) =>
          unary(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            request,
            callOptions(override),
          );
        client[`${jsName}WithResponse`] = (request, override) =>
          unaryWithResponse(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            request,
            callOptions(override),
          );
        break;
      case "serverStreaming":
        client[jsName] = (request, override) =>
          serverStreaming(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            request,
            callOptions(override),
          );
        break;
      case "clientStreaming":
        client[jsName] = (override) =>
          clientStreaming(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            callOptions(override),
          );
        break;
      case "bidirectionalStreaming":
        client[jsName] = (override) =>
          bidirectionalStreaming(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            callOptions(override),
          );
        break;
      default:
        throw internal(`unsupported generated RPC kind ${JSON.stringify(method.kind)}`);
    }
  }

  return client;
}

function prepareClientRequest(service, method, kind, body, options) {
  const metadata = isPreparedCallOptions(options)
    ? (options.metadata ?? EmptyMetadata)
    : normalizeMetadata(options.metadata ?? {});
  const request = {
    service,
    method,
    body,
    metadata,
    version: WireVersion,
  };
  const timeout = timeoutNanos(options.timeoutMs);

  if (kind !== RpcKind.Unary) {
    request.kind = kind;
  }

  if (timeout != null) {
    request.timeoutNanos = timeout;
  }

  return request;
}

function validateResponse(response, maxBodySize) {
  if (response == null) {
    throw internal("missing RPC response");
  }

  validateResponseMetadata(response.metadata ?? {});

  const body = response.body ?? new Uint8Array(0);
  if (body.byteLength > maxBodySize) {
    throw new FrameTooLargeError(body.byteLength, maxBodySize);
  }

  return statusFromResponse(response);
}

function responseMessageStream(
  frameStream,
  responseType,
  options,
  abortScope,
  onTerminal = () => {},
) {
  return new ResponseStreamController(frameStream, responseType, options, abortScope, onTerminal);
}

class ResponseStreamController {
  constructor(frameStream, responseType, options, abortScope, onTerminal) {
    this._iterator = frameStream[Symbol.asyncIterator]();
    this._responseType = responseType;
    this._options = options;
    this._abortScope = abortScope;
    this._onTerminal = onTerminal;
    this._useBodyBatches = typeof this._iterator.nextBodyBatch === "function";
    this._counters = { messages: 0, streamBodySize: 0 };
    this._messages = [];
    this._pendingError = null;
    this._pendingStatus = null;
    this._complete = false;
    this._failure = null;
    this._cleanupPromise = null;
    this._statusSettled = false;
    this._operation = Promise.resolve();
    this.status = new Promise((resolve, reject) => {
      this._resolveStatus = resolve;
      this._rejectStatus = reject;
    });
    this.status.catch(() => {});
    this._onAbort = () => {
      const error = signalAbortError(this._options.signal);
      this._recordFailure(error);
      void this._cleanup(error);
    };
    if (this._options.signal?.aborted) {
      this._onAbort();
    } else {
      this._options.signal?.addEventListener?.("abort", this._onAbort, { once: true });
    }
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  next() {
    return this._serialize(() => this._next());
  }

  async return() {
    await this.close();
    return { done: true, value: undefined };
  }

  async close(reason) {
    if (this._complete) {
      await this._cleanupPromise;
      return;
    }
    const error = cancellationReason(reason, "response stream cancelled");
    this._abortScope?.abort(error);
    this._recordFailure(error);
    await this._cleanup(error);
  }

  _serialize(operation) {
    const result = this._operation.then(operation, operation);
    this._operation = result.catch(() => {});
    return result;
  }

  async _next() {
    if (this._failure != null) {
      throw this._failure;
    }
    if (this._complete) {
      return { done: true, value: undefined };
    }

    try {
      for (;;) {
        if (this._messages.length > 0) {
          return { done: false, value: this._messages.shift() };
        }
        if (this._pendingError != null) {
          const error = this._pendingError;
          this._pendingError = null;
          throw error;
        }
        if (this._pendingStatus != null) {
          const frame = this._pendingStatus;
          this._pendingStatus = null;
          await this._finishStatus(frame);
          if (this._failure != null) {
            throw this._failure;
          }
          return { done: true, value: undefined };
        }

        const result = await this._readNext();
        if (result.done) {
          throw new StreamStateError(
            "missing_terminal_status",
            "response stream ended before final status",
          );
        }
        this._consumeBatch(result.value);
      }
    } catch (error) {
      const primary = this._recordFailure(error);
      await this._cleanup(primary);
      throw primary;
    }
  }

  _consumeBatch(value) {
    if (this._useBodyBatches) {
      for (const body of value.bodies) {
        validateResponseMessageBody(body, this._options, this._counters);
        this._messages.push(unmarshalMessage(this._responseType, body));
      }
      if (value.status != null) {
        this._pendingStatus = this._claimStatus(value.status);
      }
      return;
    }

    for (let index = 0; index < value.length; index += 1) {
      const frame = value[index];
      if (frame == null) {
        const error = new StreamStateError(
          "missing_terminal_status",
          "response stream ended before final status",
        );
        if (this._messages.length > 0) {
          this._pendingError = error;
          return;
        }
        throw error;
      }
      if (frame.kind === RpcStreamFrameKind.Message) {
        const body = frame.body ?? new Uint8Array(0);
        validateResponseMessageBody(body, this._options, this._counters);
        this._messages.push(unmarshalMessage(this._responseType, body));
        continue;
      }
      if (frame.kind === RpcStreamFrameKind.Status) {
        if (index + 1 < value.length) {
          throw new StreamStateError(
            "trailing_frame",
            "response stream contained a frame after final status",
          );
        }
        this._pendingStatus = this._claimStatus(frame);
        continue;
      }
      const error = invalidArgument("response stream contained an unknown frame kind");
      error.reason = "unsupported_frame_kind";
      throw error;
    }
  }

  _claimStatus(frame) {
    validateResponseMetadata(frame.metadata ?? {});
    const status = {
      code: frame.status ?? Code.Ok,
      message: frame.message ?? "",
      metadata: frame.metadata ?? {},
    };
    const terminalError =
      status.code === Code.Ok
        ? null
        : statusFromResponse({
            status: status.code,
            message: status.message,
            metadata: status.metadata,
          });

    if (!this._abortScope?.terminal()) {
      throw signalAbortError(this._options.signal);
    }
    this._onTerminal(terminalError);
    return { status, terminalError };
  }

  async _finishStatus(terminal) {
    const { status, terminalError } = terminal;
    let end;
    try {
      end = await this._readNext();
    } catch (error) {
      if (!isMalformedTrailingRead(error)) {
        throw error;
      }
      const trailing = new StreamStateError(
        "trailing_frame",
        "response stream did not end cleanly after final status",
      );
      trailing.cause = error;
      throw trailing;
    }
    if (!end.done) {
      throw new StreamStateError(
        "trailing_frame",
        "response stream contained a frame after final status",
      );
    }

    await this._cleanup(terminalError);
    if (terminalError != null) {
      this._recordFailure(terminalError);
      throw terminalError;
    }
    this._complete = true;
    this._settleStatus(true, status);
  }

  _readNext() {
    return this._useBodyBatches
      ? nextBodyBatchWithTimeout(
          this._iterator,
          this._options.streamIdleTimeoutMs,
          this._options.signal,
          this._abortScope?.abort,
        )
      : nextFrameBatchWithTimeout(
          this._iterator,
          this._options.streamIdleTimeoutMs,
          this._options.signal,
          this._abortScope?.abort,
        );
  }

  _recordFailure(error) {
    if (this._failure == null && !this._complete) {
      this._failure = error;
      this._settleStatus(false, error);
    }
    return this._failure ?? error;
  }

  _settleStatus(success, value) {
    if (this._statusSettled) {
      return;
    }
    this._statusSettled = true;
    if (success) {
      this._resolveStatus(value);
    } else {
      this._rejectStatus(value);
    }
  }

  _cleanup(primaryError) {
    if (this._cleanupPromise != null) {
      return this._cleanupPromise;
    }
    this._cleanupPromise = (async () => {
      this._options.signal?.removeEventListener?.("abort", this._onAbort);
      try {
        if (typeof this._iterator.return === "function") {
          await this._iterator.return();
        }
      } catch (error) {
        if (primaryError == null) {
          throw error;
        }
      } finally {
        this._abortScope?.cleanup();
      }
    })();
    return this._cleanupPromise;
  }
}

function validateResponseMessageBody(body, options, counters) {
  if (options.maxResponseMessages >= 0 && counters.messages >= options.maxResponseMessages) {
    throw resourceExhausted(
      `response stream exceeded maximum of ${options.maxResponseMessages} messages`,
    );
  }

  if (body.byteLength > options.maxResponseBodySize) {
    throw new FrameTooLargeError(body.byteLength, options.maxResponseBodySize);
  }

  counters.messages += 1;
  counters.streamBodySize = saturatingAdd(counters.streamBodySize, body.byteLength);
  if (
    options.maxResponseStreamBodySize >= 0 &&
    counters.streamBodySize > options.maxResponseStreamBodySize
  ) {
    throw resourceExhausted(
      `response stream exceeded maximum body size of ${options.maxResponseStreamBodySize} bytes`,
    );
  }
}

async function nextBodyBatchWithTimeout(iterator, idleTimeoutMs, signal, onTimeout) {
  return await withTimeout(
    iterator.nextBodyBatch(),
    idleTimeoutMs,
    () => unavailable("response stream idle timeout"),
    onTimeout,
    signal,
  );
}

async function nextFrameBatchWithTimeout(iterator, idleTimeoutMs, signal, onTimeout) {
  const hasBatchReader = typeof iterator.nextBatch === "function";
  const result = await withTimeout(
    hasBatchReader ? iterator.nextBatch() : iterator.next(),
    idleTimeoutMs,
    () => unavailable("response stream idle timeout"),
    onTimeout,
    signal,
  );
  return hasBatchReader || result.done ? result : { done: false, value: [result.value] };
}

function isMalformedTrailingRead(error) {
  return (
    error instanceof FrameTooLargeError ||
    error?.reason === "malformed_protobuf" ||
    error?.reason === "incomplete_frame"
  );
}

function validateResponseMetadata(metadata) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw new StreamStateError(
      "invalid_metadata",
      `invalid response metadata: ${error.statusMessage ?? error.message}`,
    );
  }
}

async function readUnaryResponseFromStream(stream) {
  return (await readUnaryResponseEnvelopeFromStream(stream)).message;
}

async function readUnaryResponseEnvelopeFromStream(stream) {
  let firstMessage;
  let messageCount = 0;
  for await (const message of stream) {
    if (messageCount === 0) {
      firstMessage = message;
    }
    messageCount += 1;
  }

  const status = stream.status == null ? { metadata: {} } : await stream.status;
  if (messageCount === 0) {
    throw new StreamStateError(
      "response_cardinality",
      "response stream ended without a response message",
    );
  }
  if (messageCount !== 1) {
    throw new StreamStateError(
      "response_cardinality",
      "client-streaming RPC returned more than one response message",
    );
  }

  return { message: firstMessage, metadata: status.metadata ?? {} };
}

function timeoutNanos(timeoutMs) {
  if (timeoutMs == null) {
    return null;
  }

  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }

  const nanos = BigInt(Math.ceil(timeoutMs)) * 1_000_000n;
  return String(nanos === 0n ? 1n : nanos);
}

function withTimeout(promise, timeoutMs, makeError, onTimeout, signal) {
  if (timeoutMs == null && signal == null) {
    return promise;
  }

  if (timeoutMs != null && !Number.isFinite(timeoutMs)) {
    throw invalidArgument("RPC timeout must be a finite number of milliseconds");
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    let cancelTimer = () => {};
    const cleanup = () => {
      cancelTimer();
      signal?.removeEventListener?.("abort", onAbort);
    };
    const settle = (complete, value) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      complete(value);
    };
    const onAbort = () => settle(reject, signalAbortError(signal));

    if (timeoutMs != null && timeoutMs >= 0) {
      cancelTimer = scheduleTimeout(() => {
        const error = makeError();
        onTimeout?.(error);
        settle(reject, error);
      }, timeoutMs);
    }

    if (signal != null) {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }

    promise.then(
      (value) => {
        settle(resolve, value);
      },
      (error) => {
        settle(reject, error);
      },
    );
  });
}

function signalAbortError(signal) {
  const reason = signal?.reason;
  return reason instanceof TrevRpcError ? reason : cancelled("RPC cancelled");
}

function cancellationReason(reason, fallback) {
  if (reason instanceof TrevRpcError && reason.code === Code.Cancelled) {
    return reason;
  }
  const error = cancelled(fallback);
  if (reason !== undefined) {
    Object.defineProperty(error, "cause", { configurable: true, value: reason });
  }
  return error;
}

function normalizeAbortReason(reason) {
  return reason instanceof TrevRpcError ? reason : cancellationReason(reason, "RPC cancelled");
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw signalAbortError(signal);
  }
}

function waitForOperation(promise, signal) {
  return withTimeout(promise, null, null, null, signal);
}

function callAbortScope(options, { requestCancellation = false } = {}) {
  const needsTransportSignal = options.signal != null || options.timeoutMs != null;
  if (!needsTransportSignal && !requestCancellation) {
    return {
      options,
      signal: undefined,
      abort() {
        return false;
      },
      terminal() {
        return true;
      },
      cleanup() {},
    };
  }

  if (options.timeoutMs != null && (!Number.isFinite(options.timeoutMs) || options.timeoutMs < 0)) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }

  const controller = new AbortController();
  const parentSignal = options.signal;
  const deadlineError =
    options.timeoutMs == null ? null : deadlineExceeded("RPC deadline exceeded");
  let settlement = "active";
  let cleaned = false;
  let cancelDeadline = () => {};

  const clearDeadline = () => {
    cancelDeadline();
    cancelDeadline = () => {};
  };
  const removeParent = () => parentSignal?.removeEventListener?.("abort", onParentAbort);
  const abort = (reason) => {
    if (settlement !== "active") {
      return false;
    }
    settlement = "aborted";
    clearDeadline();
    removeParent();
    controller.abort(normalizeAbortReason(reason));
    return true;
  };
  const terminal = () => {
    if (settlement !== "active") {
      return false;
    }
    settlement = "terminal";
    clearDeadline();
    removeParent();
    return true;
  };
  const onParentAbort = () => abort(parentSignal.reason);

  if (parentSignal != null) {
    if (parentSignal.aborted) {
      onParentAbort();
    } else {
      parentSignal.addEventListener("abort", onParentAbort, { once: true });
    }
  }
  if (deadlineError != null && settlement === "active") {
    cancelDeadline = scheduleTimeout(() => abort(deadlineError), options.timeoutMs);
  }

  return {
    options: markPreparedCallOptions({ ...options, signal: controller.signal }),
    signal: controller.signal,
    abort,
    terminal,
    cleanup() {
      if (cleaned) {
        return;
      }
      cleaned = true;
      clearDeadline();
      removeParent();
    },
  };
}

class RequestQueue {
  constructor(signal) {
    this._pending = [];
    this._waiter = undefined;
    this._closed = false;
    this._error = undefined;
    this._signal = signal;
    this._onAbort = () => this.close(signalAbortError(signal));
    if (signal?.aborted) {
      this._onAbort();
    } else {
      signal?.addEventListener?.("abort", this._onAbort, { once: true });
    }
  }

  send(value) {
    return this.sendMany([value]);
  }

  sendMany(values) {
    if (this._closed) {
      return Promise.reject(this._error ?? cancelled("request stream is closed"));
    }

    const batch = Array.from(values);
    if (batch.length === 0) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      let remaining = batch.length;
      const resolveOne = () => {
        remaining -= 1;
        if (remaining === 0) {
          resolve();
        }
      };
      for (const value of batch) {
        this._pending.push({ value, resolve: resolveOne, reject });
      }
      this._wakeWaiter();
    });
  }

  close(error) {
    if (this._closed) {
      return;
    }

    this._closed = true;
    this._error = error;
    this._signal?.removeEventListener?.("abort", this._onAbort);
    if (error != null) {
      for (const pending of this._pending.splice(0)) {
        pending.reject(error);
      }
    }
    if (this._waiter != null) {
      const waiter = this._waiter;
      this._waiter = undefined;
      if (error == null) {
        waiter.resolve({ done: true, value: undefined });
      } else {
        waiter.reject(error);
      }
    }
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  next() {
    if (this._pending.length > 0) {
      const pending = this._pending.shift();
      pending.resolve();
      return Promise.resolve({ done: false, value: pending.value });
    }
    if (this._error != null) {
      return Promise.reject(this._error);
    }
    if (this._closed) {
      return Promise.resolve({ done: true, value: undefined });
    }

    return new Promise((resolve, reject) => {
      this._waiter = { resolve, reject };
    });
  }

  nextBatch(max = RequestStreamBatchSize) {
    if (this._pending.length > 0) {
      return Promise.resolve({ done: false, value: this._takePendingBatch(max) });
    }
    if (this._error != null) {
      return Promise.reject(this._error);
    }
    if (this._closed) {
      return Promise.resolve({ done: true, value: undefined });
    }

    return new Promise((resolve, reject) => {
      this._waiter = { resolve, reject, batch: true, max };
    });
  }

  return() {
    this.close(cancelled("request stream cancelled"));
    return Promise.resolve({ done: true, value: undefined });
  }

  _takePendingBatch(max) {
    const count = Math.min(Math.max(1, max), this._pending.length);
    const values = Array.from({ length: count });
    for (let i = 0; i < count; i++) {
      const pending = this._pending.shift();
      pending.resolve();
      values[i] = pending.value;
    }
    return values;
  }

  _wakeWaiter() {
    if (this._waiter == null || this._pending.length === 0) {
      return;
    }

    const waiter = this._waiter;
    this._waiter = undefined;
    if (waiter.batch) {
      queueMicrotask(() => this._resolveBatchWaiter(waiter));
      return;
    }

    const pending = this._pending.shift();
    pending.resolve();
    waiter.resolve({ done: false, value: pending.value });
  }

  _resolveBatchWaiter(waiter) {
    if (this._pending.length > 0) {
      waiter.resolve({ done: false, value: this._takePendingBatch(waiter.max) });
    } else if (this._error != null) {
      waiter.reject(this._error);
    } else if (this._closed) {
      waiter.resolve({ done: true, value: undefined });
    } else {
      this._waiter = waiter;
    }
  }
}

function encodeRequestStream(requestType, requests, signal) {
  const iterator = requestIterator(requests);
  let done = false;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    async next() {
      if (done) {
        return { done: true, value: undefined };
      }
      const result = await nextRequest(iterator, signal);
      if (result.done) {
        done = true;
        return { done: true, value: undefined };
      }

      return { done: false, value: marshalMessage(requestType, result.value) };
    },
    async nextBatch(max = RequestStreamBatchSize) {
      if (done) {
        return { done: true, value: undefined };
      }
      const result = await nextRequestBatch(iterator, signal, max);
      if (result.done) {
        done = true;
        return { done: true, value: undefined };
      }

      return {
        done: false,
        value: result.value.map((value) => marshalMessage(requestType, value)),
      };
    },
    async return() {
      done = true;
      if (signal?.aborted && typeof iterator.return === "function") {
        await iterator.return();
      }
      return { done: true, value: undefined };
    },
  };
}

function requestIterator(requests) {
  if (typeof requests?.[Symbol.asyncIterator] === "function") {
    return requests[Symbol.asyncIterator]();
  }

  if (typeof requests?.[Symbol.iterator] === "function") {
    const iterator = requests[Symbol.iterator]();
    return {
      next() {
        return Promise.resolve(iterator.next());
      },
      return() {
        return Promise.resolve(
          typeof iterator.return === "function"
            ? iterator.return()
            : { done: true, value: undefined },
        );
      },
    };
  }

  throw invalidArgument("request stream must be an async iterable or iterable");
}

function nextRequest(iterator, signal) {
  return nextRequestResult(() => iterator.next(), signal);
}

async function nextRequestBatch(iterator, signal, max) {
  if (typeof iterator.nextBatch === "function") {
    return await nextRequestResult(() => iterator.nextBatch(max), signal);
  }

  const result = await nextRequest(iterator, signal);
  return result.done ? result : { done: false, value: [result.value] };
}

function nextRequestResult(next, signal) {
  if (signal?.aborted) {
    return Promise.reject(signal.reason ?? cancelled("request stream cancelled"));
  }
  if (signal == null) {
    return next();
  }

  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason ?? cancelled("request stream cancelled"));
    signal.addEventListener("abort", onAbort, { once: true });
    next().then(
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

async function* emptyAsyncIterable() {}

function copyDefined(target, source) {
  for (const [key, value] of Object.entries(source ?? {})) {
    if (value !== undefined && key !== "metadata") {
      target[key] = value;
    }
  }
}

function mergeMetadata(base, override) {
  const baseHasMetadata = hasMetadataEntries(base?.metadata);
  const overrideHasMetadata = hasMetadataEntries(override?.metadata);

  if (!baseHasMetadata && !overrideHasMetadata) {
    return EmptyMetadata;
  }

  if (!overrideHasMetadata) {
    return isPreparedCallOptions(base)
      ? (base.metadata ?? EmptyMetadata)
      : normalizeMetadata(base.metadata);
  }

  if (!baseHasMetadata) {
    return isPreparedCallOptions(override)
      ? (override.metadata ?? EmptyMetadata)
      : normalizeMetadata(override.metadata);
  }

  const metadata = Object.assign(
    Object.create(null),
    isPreparedCallOptions(base) ? base.metadata : normalizeMetadata(base.metadata),
    isPreparedCallOptions(override) ? override.metadata : normalizeMetadata(override.metadata),
  );
  validateMetadata(metadata);
  return metadata;
}

function hasMetadataEntries(metadata) {
  if (metadata == null) {
    return false;
  }
  if (metadata instanceof Map) {
    return metadata.size > 0;
  }
  for (const _key in metadata) {
    return true;
  }
  return false;
}

function hasDefinedOptions(options) {
  for (const [key, value] of Object.entries(options ?? {})) {
    if (key === "metadata") {
      if (hasMetadataEntries(value)) {
        return true;
      }
    } else if (value !== undefined) {
      return true;
    }
  }
  return false;
}

function markPreparedCallOptions(options) {
  Object.defineProperty(options, PreparedCallOptions, { value: true });
  return options;
}

function isPreparedCallOptions(options) {
  return options?.[PreparedCallOptions] === true;
}

function saturatingAdd(left, right) {
  const sum = left + right;
  return Number.isSafeInteger(sum) ? sum : Number.MAX_SAFE_INTEGER;
}
