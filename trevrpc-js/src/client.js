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

export async function unary(
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
    );

    validateResponse(response, callOptions.maxResponseBodySize);
    return unmarshalMessage(responseType, response.body ?? new Uint8Array(0));
  } finally {
    abortScope.cleanup();
  }
}

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
    );
  } catch (error) {
    abortScope.cleanup();
    throw error;
  }

  return responseMessageStream(frames, responseType, abortScope.options, deadlineAt, abortScope);
}

export async function clientStreaming(
  transport,
  service,
  method,
  requestType,
  responseType,
  requests,
  options = {},
) {
  const callOptions = mergeCallOptions(options);
  const abortScope = callAbortScope(callOptions);
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
    );
  } catch (error) {
    abortScope.cleanup();
    throw error;
  }

  return readUnaryResponseFromStream(
    responseMessageStream(frames, responseType, abortScope.options, deadlineAt, abortScope),
  );
}

export async function bidirectionalStreaming(
  transport,
  service,
  method,
  requestType,
  responseType,
  requests,
  options = {},
) {
  const callOptions = mergeCallOptions(options);
  const abortScope = callAbortScope(callOptions);
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
    );
  } catch (error) {
    abortScope.cleanup();
    throw error;
  }

  return responseMessageStream(frames, responseType, abortScope.options, deadlineAt, abortScope);
}

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
        client[jsName] = (requests, override) =>
          clientStreaming(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            requests,
            callOptions(override),
          );
        break;
      case "bidirectionalStreaming":
        client[jsName] = (requests, override) =>
          bidirectionalStreaming(
            transport,
            service.fullName,
            method.name,
            requestType,
            responseType,
            requests,
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

  return {
    service,
    method,
    body,
    metadata,
    kind,
    version: WireVersion,
    timeoutNanos: timeoutNanos(options.timeoutMs),
  };
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

async function* responseMessageStream(frameStream, responseType, options, deadlineAt, abortScope) {
  const iterator = frameStream[Symbol.asyncIterator]();
  let messages = 0;
  let streamBodySize = 0;
  let complete = false;

  try {
    for (;;) {
      const result = await nextFrameWithTimeout(
        iterator,
        deadlineAt,
        options.streamIdleTimeoutMs,
        abortScope?.abort,
      );
      if (result.done) {
        throw internal("response stream ended before final status");
      }

      const frame = result.value;
      switch (frame.kind) {
        case RpcStreamFrameKind.Message: {
          const body = frame.body ?? new Uint8Array(0);
          if (options.maxResponseMessages >= 0 && messages >= options.maxResponseMessages) {
            throw resourceExhausted(
              `response stream exceeded maximum of ${options.maxResponseMessages} messages`,
            );
          }

          if (body.byteLength > options.maxResponseBodySize) {
            throw new FrameTooLargeError(body.byteLength, options.maxResponseBodySize);
          }

          messages += 1;
          streamBodySize = saturatingAdd(streamBodySize, body.byteLength);
          if (
            options.maxResponseStreamBodySize >= 0 &&
            streamBodySize > options.maxResponseStreamBodySize
          ) {
            throw resourceExhausted(
              `response stream exceeded maximum body size of ${options.maxResponseStreamBodySize} bytes`,
            );
          }

          yield unmarshalMessage(responseType, body);
          break;
        }
        case RpcStreamFrameKind.Status: {
          complete = true;
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
            return;
          }

          throw statusFromResponse({
            status: status.code,
            message: status.statusMessage,
            metadata: status.metadata,
          });
        }
        default:
          throw internal("response stream contained an unknown frame kind");
      }
    }
  } finally {
    if (!complete) {
      abortScope?.abort(cancelled("response stream cancelled"));
    }
    if (!complete && typeof iterator.return === "function") {
      await iterator.return();
    }
    abortScope?.cleanup();
  }
}

function validateResponseMetadata(metadata) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response metadata: ${error.statusMessage ?? error.message}`);
  }
}

async function readUnaryResponseFromStream(stream) {
  const iterator = stream[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) {
    throw internal("response stream ended without a response message");
  }

  const second = await iterator.next();
  if (second.done) {
    return first.value;
  }

  if (typeof iterator.return === "function") {
    await iterator.return();
  }
  throw internal("client-streaming RPC returned more than one response message");
}

async function nextFrameWithTimeout(iterator, deadlineAt, idleTimeoutMs, onTimeout) {
  const timeout = nextTimeout(deadlineAt, idleTimeoutMs);
  if (timeout == null) {
    return iterator.next();
  }

  return withTimeout(iterator.next(), timeout.ms, () => timeout.error, onTimeout);
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

  return timeouts.reduce((best, candidate) => (candidate.ms < best.ms ? candidate : best));
}

function timeoutNanos(timeoutMs) {
  if (timeoutMs == null) {
    return "0";
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

function withTimeout(promise, timeoutMs, makeError, onTimeout) {
  if (timeoutMs == null) {
    return promise;
  }

  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      const error = makeError();
      onTimeout?.(error);
      reject(error);
    }, timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
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

async function* encodeRequestStream(requestType, requests, signal) {
  const iterator = requests[Symbol.asyncIterator]();
  try {
    for (;;) {
      const result = await nextRequest(iterator, signal);
      if (result.done) {
        return;
      }

      yield marshalMessage(requestType, result.value);
    }
  } finally {
    if (signal?.aborted && typeof iterator.return === "function") {
      await iterator.return();
    }
  }
}

function nextRequest(iterator, signal) {
  if (signal?.aborted) {
    return Promise.reject(signal.reason ?? cancelled("request stream cancelled"));
  }
  if (signal == null) {
    return iterator.next();
  }

  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason ?? cancelled("request stream cancelled"));
    signal.addEventListener("abort", onAbort, { once: true });
    iterator.next().then(
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
