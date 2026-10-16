import { DefaultMaxFrameSize, FrameReader, encodeFrame, encodeMessageFrame } from "./framing.js";
import { normalizeMetadata, validateMetadata } from "./metadata.js";
import {
  Code,
  TrevRpcError,
  cancelled,
  deadlineExceeded,
  failedPrecondition,
  invalidArgument,
  statusError,
  statusFromTransportError,
} from "./status.js";
import { RpcRequest, WireVersion } from "./wire.js";

/** Longest delay setTimeout can represent; it fires immediately beyond this. */
const MaxTimerDelayMs = 2_147_483_647;
const MaxUint64 = 0xffffffffffffffffn;

/** Returns the default call options. */
export function defaultCallOptions() {
  return {
    timeoutMs: undefined,
    maxFrameSize: DefaultMaxFrameSize,
    metadata: {},
    signal: undefined,
  };
}

/** Merges override options over resolved base options; metadata merges per key. */
export function mergeCallOptions(base, override = {}) {
  const merged = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (value !== undefined && key !== "metadata") {
      merged[key] = value;
    }
  }
  if (override.metadata != null) {
    merged.metadata = Object.assign(
      Object.create(null),
      base.metadata,
      normalizeMetadata(override.metadata),
    );
    validateMetadata(merged.metadata);
  }
  return merged;
}

/**
 * Cancellation and deadline for one call. The signal aborts with a TrevRpcError reason: the
 * failure that ended the call, the caller's own status error, DeadlineExceeded, or Cancelled
 * wrapping any other abort reason.
 */
class CallScope {
  #controller = new AbortController();
  #parent;
  #timer;

  /** timeoutMs must be validated by the caller. */
  constructor({ signal, timeoutMs }) {
    if (signal?.aborted) {
      this.abort(signal.reason);
      return;
    }
    if (signal != null) {
      this.#parent = { signal, listener: () => this.abort(signal.reason) };
      signal.addEventListener("abort", this.#parent.listener, { once: true });
    }
    if (timeoutMs != null && timeoutMs <= MaxTimerDelayMs) {
      this.#timer = setTimeout(
        () => this.abort(deadlineExceeded("RPC deadline exceeded")),
        timeoutMs,
      );
    }
  }

  get signal() {
    return this.#controller.signal;
  }

  /** Aborts the call; only the first reason counts. Returns the effective reason. */
  abort(reason) {
    if (!this.signal.aborted) {
      this.settle();
      this.#controller.abort(
        reason instanceof TrevRpcError ? reason : cancelled("RPC cancelled", reason),
      );
    }
    return this.signal.reason;
  }

  /** Stops the deadline and detaches from the caller's signal once the outcome is known. */
  settle() {
    clearTimeout(this.#timer);
    this.#parent?.signal.removeEventListener("abort", this.#parent.listener);
    this.#parent = undefined;
  }
}

/**
 * One RPC over one bidirectional stream: the request writer, the response frame reader, and the
 * call's cancellation scope. Transport failures surface as TrevRpcError, and once the call is
 * aborted every operation fails with the abort reason.
 */
export class Call {
  #scope;
  #writer;
  #cancelReader;
  #frames;
  #maxFrameSize;
  #sendClosed = false;

  /**
   * Opens a stream on the transport and sends the request header. The request carries service,
   * method, kind and body; version, metadata and the wire timeout come from the options.
   */
  static async open(transport, request, options) {
    const header = encodeFrame(
      RpcRequest,
      { ...request, version: WireVersion, metadata: options.metadata, ...timeoutField(options) },
      options.maxFrameSize,
    );
    const scope = new CallScope(options);
    try {
      const stream = await openStream(transport, scope.signal);
      const call = new Call(stream, scope, options.maxFrameSize);
      await call.#write(header);
      return call;
    } catch (error) {
      throw scope.abort(failureStatus(error));
    }
  }

  constructor(stream, scope, maxFrameSize) {
    const reader = stream.readable.getReader();
    this.#scope = scope;
    this.#writer = stream.writable.getWriter();
    this.#cancelReader = (reason) => reader.cancel(reason);
    this.#frames = new FrameReader(reader);
    this.#maxFrameSize = maxFrameSize;
    scope.signal.addEventListener("abort", () => this.#teardown(), { once: true });
    if (scope.signal.aborted) {
      this.#teardown();
    }
  }

  /** The call's abort signal. */
  get signal() {
    return this.#scope.signal;
  }

  /** Sends one streaming request message; writes are ordered and apply backpressure. */
  sendMessage(body) {
    if (this.#sendClosed) {
      return Promise.reject(failedPrecondition("request stream is closed"));
    }
    return this.#write(encodeMessageFrame(body, this.#maxFrameSize));
  }

  /** Finishes the request stream gracefully. */
  closeSend() {
    if (this.#sendClosed) {
      return Promise.resolve();
    }
    this.#sendClosed = true;
    return this.#guard(this.#writer.close());
  }

  /** Reads the unary response frame. */
  readFrame(messageType) {
    return this.#guard(this.#frames.readFrame(messageType, this.#maxFrameSize));
  }

  /** Reads the next response stream frame, or null if the stream ended cleanly. */
  readStreamFrame() {
    return this.#guard(this.#frames.readStreamFrame(this.#maxFrameSize));
  }

  /**
   * Ends the call after its outcome (a final response or status) was received. An unfinished
   * request side is reset, and the rest of the response stream is drained in the background.
   */
  finish() {
    this.#scope.settle();
    if (!this.#sendClosed) {
      this.#sendClosed = true;
      this.#writer.abort(cancelled("RPC completed")).catch(() => {});
    }
    this.#frames.expectEnd().catch(() => this.#cancelReader().catch(() => {}));
  }

  /** Cancels the call on behalf of the caller; any reason becomes a Cancelled status. */
  cancel(reason) {
    this.#scope.abort(reason);
  }

  /** Fails the call, tearing down both stream directions. Returns the effective error. */
  fail(error) {
    return this.#scope.abort(failureStatus(error));
  }

  async #write(bytes) {
    await this.#guard(this.#writer.write(bytes));
  }

  async #guard(operation) {
    let result;
    try {
      result = await operation;
    } catch (error) {
      throw this.signal.aborted ? this.signal.reason : statusFromTransportError(error);
    }
    // Cancelling the reader completes pending reads as end-of-stream instead of rejecting them.
    throwIfAborted(this.signal);
    return result;
  }

  #teardown() {
    const reason = this.signal.reason;
    this.#writer.abort(reason).catch(() => {});
    this.#cancelReader(reason).catch(() => {});
  }
}

function openStream(transport, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });

    let opening;
    try {
      opening = Promise.resolve(transport.createBidirectionalStream());
    } catch (error) {
      opening = Promise.reject(error);
    }
    opening.then(
      (stream) => {
        signal.removeEventListener("abort", onAbort);
        if (signal.aborted) {
          // The caller gave up while the stream was opening; do not leak it.
          stream.writable.abort(signal.reason).catch(() => {});
          stream.readable.cancel(signal.reason).catch(() => {});
        } else {
          resolve(stream);
        }
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(statusFromTransportError(error));
      },
    );
  });
}

function throwIfAborted(signal) {
  if (signal.aborted) {
    throw signal.reason;
  }
}

/** Status errors describe themselves; anything else reaching the call is an internal bug. */
function failureStatus(error) {
  return error instanceof TrevRpcError
    ? error
    : statusError(Code.Internal, error?.message ?? String(error), {}, error);
}

/**
 * Validates a call timeout and returns the request field for it. Zero means unset on the wire, so
 * a zero timeout is sent as 1ns.
 */
function timeoutField({ timeoutMs }) {
  if (timeoutMs == null) {
    return {};
  }
  if (!(timeoutMs >= 0 && Number.isFinite(timeoutMs))) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }
  const nanos = BigInt(Math.ceil(timeoutMs)) * 1_000_000n;
  if (nanos > MaxUint64) {
    throw invalidArgument("RPC timeout does not fit in 64-bit nanoseconds");
  }
  return { timeoutNanos: String(nanos === 0n ? 1n : nanos) };
}
