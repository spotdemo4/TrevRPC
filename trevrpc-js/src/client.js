import { DefaultMaxFrameSize, marshalMessage, unmarshalMessage } from "./framing.js";
import { normalizeMetadata, validateMetadata } from "./metadata.js";
import {
  Code,
  FrameTooLargeError,
  cancelled,
  deadlineExceeded,
  internal,
  invalidArgument,
  isOkStatus,
  resourceExhausted,
  statusFromResponse,
  unavailable,
} from "./status.js";
import { RpcKind, RpcStreamFrameKind, WireVersion } from "./wire.js";

const RequestStreamBatchSize = 16;

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
  const merged = { ...defaultCallOptions() };
  copyDefined(merged, base);
  copyDefined(merged, override);
  merged.metadata = {
    ...normalizeMetadata(base.metadata ?? {}),
    ...normalizeMetadata(override.metadata ?? {}),
  };
  return merged;
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
  const callOptions = mergeCallOptions(options);
  const abortScope = callAbortScope(callOptions);
  const requestBody = marshalMessage(requestType, request);
  const rpcRequest = prepareClientRequest(
    service,
    method,
    RpcKind.Unary,
    requestBody,
    abortScope.options,
  );

  try {
    const response = await withTimeout(
      transport.call(rpcRequest, abortScope.options),
      callOptions.timeoutMs,
      () => deadlineExceeded("RPC deadline exceeded"),
      abortScope.abort,
      abortScope.options.signal,
    );

    validateResponse(response, callOptions.maxResponseBodySize);
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
  const callOptions = mergeCallOptions(options);
  const abortScope = callAbortScope(callOptions);
  const requestBody = marshalMessage(requestType, request);
  const rpcRequest = prepareClientRequest(
    service,
    method,
    RpcKind.ServerStreaming,
    requestBody,
    abortScope.options,
  );
  const deadlineAt = localDeadline(callOptions.timeoutMs);
  let frames;
  try {
    frames = await withTimeout(
      transport.streamingCall(rpcRequest, emptyAsyncIterable(), abortScope.options),
      callOptions.timeoutMs,
      () => deadlineExceeded("RPC deadline exceeded"),
      abortScope.abort,
      abortScope.options.signal,
    );
  } catch (error) {
    abortScope.cleanup();
    throw error;
  }

  return responseMessageStream(frames, responseType, abortScope.options, deadlineAt, abortScope);
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
  const callOptions = mergeCallOptions(options);
  const abortScope = callAbortScope(callOptions);
  const requests = new RequestQueue();
  const rpcRequest = prepareClientRequest(
    service,
    method,
    RpcKind.ClientStreaming,
    new Uint8Array(0),
    abortScope.options,
  );
  const deadlineAt = localDeadline(callOptions.timeoutMs);
  let frames;
  try {
    frames = await withTimeout(
      transport.streamingCall(
        rpcRequest,
        encodeRequestStream(requestType, requests, abortScope.options.signal),
        abortScope.options,
      ),
      callOptions.timeoutMs,
      () => deadlineExceeded("RPC deadline exceeded"),
      abortScope.abort,
      abortScope.options.signal,
    );
  } catch (error) {
    abortScope.cleanup();
    requests.close(error);
    throw error;
  }

  return new ClientStreamingCall(
    requests,
    responseMessageStream(frames, responseType, abortScope.options, deadlineAt, abortScope),
  );
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
  const callOptions = mergeCallOptions(options);
  const abortScope = callAbortScope(callOptions);
  const requests = new RequestQueue();
  const rpcRequest = prepareClientRequest(
    service,
    method,
    RpcKind.BidirectionalStreaming,
    new Uint8Array(0),
    abortScope.options,
  );
  const deadlineAt = localDeadline(callOptions.timeoutMs);
  let frames;
  try {
    frames = await withTimeout(
      transport.streamingCall(
        rpcRequest,
        encodeRequestStream(requestType, requests, abortScope.options.signal),
        abortScope.options,
      ),
      callOptions.timeoutMs,
      () => deadlineExceeded("RPC deadline exceeded"),
      abortScope.abort,
      abortScope.options.signal,
    );
  } catch (error) {
    abortScope.cleanup();
    requests.close(error);
    throw error;
  }

  return new BidirectionalStreamingCall(
    requests,
    responseMessageStream(frames, responseType, abortScope.options, deadlineAt, abortScope),
  );
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
  async close() {
    this._requests.close(cancelled("request stream cancelled"));
    const iterator = this._responses[Symbol.asyncIterator]();
    await iterator.return?.();
  }
}

/** A bidirectional-streaming RPC call. */
export class BidirectionalStreamingCall {
  constructor(requests, responses) {
    this._requests = requests;
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
  async close() {
    this._requests.close(cancelled("request stream cancelled"));
    await this._iterator.return?.();
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

  for (const [jsName, method] of Object.entries(service.methods)) {
    const requestType = root.lookupType(method.inputType);
    const responseType = root.lookupType(method.outputType);
    const callOptions = (override) => mergeCallOptions(options, override ?? {});

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
  const metadata = normalizeMetadata(options.metadata ?? {});
  validateMetadata(metadata);
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

  const status = statusFromResponse(response);
  if (!isOkStatus(status)) {
    throw status;
  }
}

function responseMessageStream(frameStream, responseType, options, deadlineAt, abortScope) {
  const iterator = frameStream[Symbol.asyncIterator]();
  const useBodyBatches = typeof iterator.nextBodyBatch === "function";
  let resolveStatus;
  let rejectStatus;
  const status = new Promise((resolve, reject) => {
    resolveStatus = resolve;
    rejectStatus = reject;
  });
  status.catch(() => {});

  async function* run() {
    const counters = { messages: 0, streamBodySize: 0 };
    let complete = false;

    try {
      for (;;) {
        const result = useBodyBatches
          ? await nextBodyBatchWithTimeout(
              iterator,
              deadlineAt,
              options.streamIdleTimeoutMs,
              options.signal,
              abortScope?.abort,
            )
          : await nextFrameBatchWithTimeout(
              iterator,
              deadlineAt,
              options.streamIdleTimeoutMs,
              options.signal,
              abortScope?.abort,
            );
        if (result.done) {
          throw internal("response stream ended before final status");
        }

        if (useBodyBatches) {
          const batch = result.value;
          for (const body of batch.bodies) {
            validateResponseMessageBody(body, options, counters);
            yield unmarshalMessage(responseType, body);
          }
          if (batch.status != null) {
            complete = true;
            resolveStatus(await finishResponseStatus(iterator, batch.status, abortScope));
            return;
          }
          continue;
        }

        for (const frame of result.value) {
          if (frame == null) {
            throw internal("response stream ended before final status");
          }

          switch (frame.kind) {
            case RpcStreamFrameKind.Message: {
              const body = frame.body ?? new Uint8Array(0);
              validateResponseMessageBody(body, options, counters);
              yield unmarshalMessage(responseType, body);
              break;
            }
            case RpcStreamFrameKind.Status: {
              complete = true;
              resolveStatus(await finishResponseStatus(iterator, frame, abortScope));
              return;
            }
            default:
              throw invalidArgument("response stream contained an unknown frame kind");
          }
        }
      }
    } catch (error) {
      rejectStatus(error);
      throw error;
    } finally {
      if (!complete) {
        rejectStatus(cancelled("response stream cancelled"));
        abortScope?.abort(cancelled("response stream cancelled"));
      }
      if (!complete && typeof iterator.return === "function") {
        await iterator.return();
      }
      abortScope?.cleanup();
    }
  }

  const stream = run();
  stream.status = status;
  return stream;
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

async function finishResponseStatus(iterator, frame, abortScope) {
  validateResponseMetadata(frame.metadata ?? {});
  abortScope?.abort(cancelled("response stream completed"));
  let cleanupError;
  if (typeof iterator.return === "function") {
    try {
      await iterator.return();
    } catch (error) {
      cleanupError = error;
    }
  }
  const status = {
    code: frame.status ?? Code.Ok,
    statusMessage: frame.message ?? "",
    metadata: frame.metadata ?? {},
  };
  if (status.code === Code.Ok) {
    if (cleanupError != null) {
      throw cleanupError;
    }
    return status;
  }

  throw statusFromResponse({
    status: status.code,
    message: status.statusMessage,
    metadata: status.metadata,
  });
}

async function nextBodyBatchWithTimeout(iterator, deadlineAt, idleTimeoutMs, signal, onTimeout) {
  const timeout = nextTimeout(deadlineAt, idleTimeoutMs);
  const promise = iterator.nextBodyBatch();
  return timeout == null
    ? await withTimeout(promise, null, null, null, signal)
    : await withTimeout(promise, timeout.ms, () => timeout.error, onTimeout, signal);
}

async function nextFrameBatchWithTimeout(iterator, deadlineAt, idleTimeoutMs, signal, onTimeout) {
  const timeout = nextTimeout(deadlineAt, idleTimeoutMs);
  const promise = typeof iterator.nextBatch === "function" ? iterator.nextBatch() : iterator.next();
  const result =
    timeout == null
      ? await withTimeout(promise, null, null, null, signal)
      : await withTimeout(promise, timeout.ms, () => timeout.error, onTimeout, signal);

  if (typeof iterator.nextBatch === "function" || result.done) {
    return result;
  }
  return { done: false, value: [result.value] };
}

function validateResponseMetadata(metadata) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response metadata: ${error.statusMessage ?? error.message}`);
  }
}

async function readUnaryResponseFromStream(stream) {
  return (await readUnaryResponseEnvelopeFromStream(stream)).message;
}

async function readUnaryResponseEnvelopeFromStream(stream) {
  const iterator = stream[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) {
    throw internal("response stream ended without a response message");
  }

  const second = await iterator.next();
  if (second.done) {
    const status = stream.status == null ? { metadata: {} } : await stream.status;
    return { message: first.value, metadata: status.metadata ?? {} };
  }

  if (typeof iterator.return === "function") {
    await iterator.return();
  }
  throw internal("client-streaming RPC returned more than one response message");
}

function nextTimeout(deadlineAt, idleTimeoutMs) {
  const timeouts = [];
  if (deadlineAt != null) {
    timeouts.push({
      ms: deadlineAt - Date.now(),
      error: deadlineExceeded("RPC deadline exceeded"),
    });
  }
  if (idleTimeoutMs != null && idleTimeoutMs > 0) {
    timeouts.push({ ms: idleTimeoutMs, error: unavailable("response stream idle timeout") });
  }

  if (timeouts.length === 0) {
    return null;
  }

  const timeout = timeouts.reduce((best, candidate) => (candidate.ms < best.ms ? candidate : best));
  return { ...timeout, ms: Math.max(0, timeout.ms) };
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

function localDeadline(timeoutMs) {
  if (timeoutMs == null) {
    return null;
  }

  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }

  return Date.now() + timeoutMs;
}

function withTimeout(promise, timeoutMs, makeError, onTimeout, signal) {
  if (timeoutMs == null && signal == null) {
    return promise;
  }

  if (timeoutMs != null && (!Number.isFinite(timeoutMs) || timeoutMs < 0)) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    let timer;
    const cleanup = () => {
      if (timer != null) {
        clearTimeout(timer);
      }
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

    if (timeoutMs != null) {
      timer = setTimeout(() => {
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
  return reason?.name === "TrevRpcError" ? reason : cancelled("RPC cancelled");
}

function callAbortScope(options) {
  const controller = new AbortController();
  const parentSignal = options.signal;
  const abort = (reason) => {
    if (!controller.signal.aborted) {
      controller.abort(reason);
    }
  };
  const onParentAbort = () => abort(parentSignal.reason ?? cancelled("RPC cancelled"));

  if (parentSignal != null) {
    if (parentSignal.aborted) {
      onParentAbort();
    } else {
      parentSignal.addEventListener("abort", onParentAbort, { once: true });
    }
  }

  return {
    options: { ...options, signal: controller.signal },
    abort,
    cleanup() {
      parentSignal?.removeEventListener?.("abort", onParentAbort);
    },
  };
}

class RequestQueue {
  constructor() {
    this._pending = [];
    this._waiter = undefined;
    this._closed = false;
    this._error = undefined;
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

function saturatingAdd(left, right) {
  const sum = left + right;
  return Number.isSafeInteger(sum) ? sum : Number.MAX_SAFE_INTEGER;
}
