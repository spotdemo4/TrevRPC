import { cancelled, deadlineExceeded, unavailable } from "./status.js";

const ChannelOptionKeys = new Set(["onStateChange", "signal", "timeoutMs"]);

const DefaultChannelOptions = Object.freeze({
  initialDelayMs: 100,
  maxDelayMs: 30_000,
  multiplier: 2,
  jitter: 0.2,
});
const MaxTimerDelayMs = 2_147_483_647;

/** Shared state machine for reconnecting channels. */
export class ChannelStateMachine extends EventTarget {
  #connect;
  #connectAbort = null;
  #generation = 0;
  #observeClosed;
  #isConnectionFailure;
  #onStateChange;
  #options;
  #state = "connecting";
  #transport = null;
  #waiters = [];

  constructor(connect, options = {}, observeClosed = null, isConnectionFailure = () => false) {
    super();
    this.#connect = connect;
    this.#observeClosed = observeClosed;
    this.#isConnectionFailure = isConnectionFailure;
    this.#onStateChange = options.onStateChange;
    this.#options = normalizeChannelOptions(options);
    queueMicrotask(() => this.#runConnectLoop(false));
  }

  /** Reports whether a generation is currently ready to accept new calls. */
  get ready() {
    return this.#state === "ready";
  }

  /** Reports the channel lifecycle state. */
  get state() {
    return this.#state;
  }

  /** Identifies the current successful connection generation, starting at one. */
  get generation() {
    return this.#generation;
  }

  /** Waits for the current or next connection generation to become ready. */
  waitUntilReady() {
    if (this.ready) {
      return Promise.resolve();
    }
    if (this.#state === "closed") {
      return Promise.reject(unavailable("channel is closed"));
    }

    return new Promise((resolve, reject) => {
      this.#waiters.push({ resolve, reject });
    });
  }

  /** Sends a unary call on the current generation without replaying it. */
  async call(request, options = {}) {
    const transport = this.#readyTransport();
    try {
      return await transport.call(request, options);
    } catch (error) {
      this.#operationFailed(transport, error);
      throw error;
    }
  }

  /** Starts a streaming call on the current generation without resuming it later. */
  async streamingCall(request, requestBody, options = {}) {
    const transport = this.#readyTransport();
    try {
      const stream = await transport.streamingCall(request, requestBody, options);
      return observedStream(stream, (error) => this.#operationFailed(transport, error));
    } catch (error) {
      this.#operationFailed(transport, error);
      throw error;
    }
  }

  /** Stops reconnecting and closes the current generation. */
  close(closeInfo = {}) {
    if (this.#state === "closed") {
      return;
    }

    const transport = this.#transport;
    this.#transport = null;
    this.#connectAbort?.abort();
    this.#connectAbort = null;
    this.#transition("closed");
    this.#rejectWaiters(unavailable("channel is closed"));
    safeClose(transport, closeInfo);
  }

  #readyTransport() {
    if (!this.ready || this.#transport == null) {
      throw unavailable(`channel is ${this.#state}`);
    }
    return this.#transport;
  }

  async #runConnectLoop(reconnecting, initialError) {
    if (this.#state === "closed" || this.#connectAbort != null) {
      return;
    }

    const controller = new AbortController();
    this.#connectAbort = controller;
    let attempt = 0;
    let error = initialError;

    while (!controller.signal.aborted) {
      attempt += 1;
      const delayed = reconnecting || attempt > 1;
      const delayMs = delayed
        ? reconnectDelay(this.#options, reconnecting ? attempt - 1 : attempt - 2)
        : 0;
      this.#transition(reconnecting ? "reconnecting" : "connecting", {
        attempt,
        delayMs,
        error,
      });

      if (delayed) {
        await waitForDelay(delayMs, controller.signal);
        if (controller.signal.aborted) {
          return;
        }
      }

      let transport;
      try {
        transport = await this.#connect(controller.signal);
      } catch (connectError) {
        if (controller.signal.aborted) {
          return;
        }
        error = connectError;
        continue;
      }

      if (controller.signal.aborted || this.#state === "closed") {
        safeClose(transport);
        return;
      }

      this.#connectAbort = null;
      this.#transport = transport;
      this.#generation += 1;
      this.#transition("ready", { attempt });
      this.#resolveWaiters();
      this.#watchTransport(transport);
      return;
    }
  }

  #watchTransport(transport) {
    const closed = this.#observeClosed?.(transport);
    if (closed == null || typeof closed.then !== "function") {
      return;
    }

    Promise.resolve(closed).then(
      (closeInfo) => this.#transportFailed(transport, closeInfo),
      (error) => this.#transportFailed(transport, error),
    );
  }

  #transportFailed(transport, error) {
    if (this.#state !== "ready" || transport !== this.#transport) {
      return;
    }

    this.#transport = null;
    safeClose(transport);
    void this.#runConnectLoop(true, error);
  }

  #operationFailed(transport, error) {
    if (this.#isConnectionFailure(error)) {
      this.#transportFailed(transport, error);
    }
  }

  #transition(state, extra = {}) {
    const previousState = this.#state;
    this.#state = state;
    const detail = Object.freeze({
      state,
      previousState,
      generation: this.#generation,
      ...extra,
    });

    if (typeof this.#onStateChange === "function") {
      try {
        this.#onStateChange(detail);
      } catch (error) {
        globalThis.reportError?.(error);
      }
    }
    this.dispatchEvent(lifecycleEvent("statechange", detail));
    this.dispatchEvent(lifecycleEvent(state, detail));
  }

  #resolveWaiters() {
    for (const waiter of this.#waiters.splice(0)) {
      waiter.resolve();
    }
  }

  #rejectWaiters(error) {
    for (const waiter of this.#waiters.splice(0)) {
      waiter.reject(error);
    }
  }
}

/** Returns transport options with channel lifecycle settings removed. */
export function stripChannelOptions(options = {}) {
  return Object.fromEntries(Object.entries(options).filter(([key]) => !ChannelOptionKeys.has(key)));
}

/** Waits for initial readiness with caller-controlled cancellation and deadline. */
export async function waitForInitialReady(channel, options = {}) {
  const timeoutMs = options.timeoutMs;
  if (timeoutMs != null && (!Number.isFinite(timeoutMs) || timeoutMs < 0)) {
    channel.close();
    throw new TypeError("timeoutMs must be a non-negative finite number");
  }

  const signal = options.signal;
  if (signal?.aborted) {
    channel.close();
    throw initialAbortError(signal);
  }
  if (timeoutMs === 0) {
    channel.close();
    throw deadlineExceeded("initial connection deadline exceeded");
  }

  const timeoutAbort = timeoutMs == null ? null : new AbortController();
  let removeAbort = () => {};
  try {
    const waits = [channel.waitUntilReady()];
    if (signal != null) {
      waits.push(
        new Promise((_, reject) => {
          const rejectCancelled = () => reject(initialAbortError(signal));
          if (signal.aborted) {
            rejectCancelled();
            return;
          }
          signal.addEventListener("abort", rejectCancelled, { once: true });
          removeAbort = () => signal.removeEventListener("abort", rejectCancelled);
        }),
      );
    }
    if (timeoutMs != null) {
      waits.push(
        waitForDelay(timeoutMs, timeoutAbort.signal).then(() => {
          throw deadlineExceeded("initial connection deadline exceeded");
        }),
      );
    }
    await Promise.race(waits);
  } catch (error) {
    channel.close();
    throw error;
  } finally {
    timeoutAbort?.abort();
    removeAbort();
  }
}

function normalizeChannelOptions(options) {
  void options;
  return DefaultChannelOptions;
}

function initialAbortError(signal) {
  if (signal.reason?.name === "TrevRpcError") {
    return signal.reason;
  }
  const error = cancelled("initial connection cancelled");
  if (signal.reason !== undefined) {
    Object.defineProperty(error, "cause", { configurable: true, value: signal.reason });
  }
  return error;
}

function reconnectDelay(options, exponent) {
  const exponential = options.initialDelayMs * options.multiplier ** exponent;
  const base = Math.min(options.maxDelayMs, exponential);
  const jitter = 1 + (Math.random() * 2 - 1) * options.jitter;
  return Math.min(options.maxDelayMs, Math.max(0, base * jitter));
}

async function waitForDelay(delayMs, signal) {
  let remaining = delayMs;
  while (remaining > 0 && !signal.aborted) {
    const chunk = Math.min(remaining, MaxTimerDelayMs);
    await waitForTimer(chunk, signal);
    remaining -= chunk;
  }
}

function waitForTimer(delayMs, signal) {
  return new Promise((resolve) => {
    const timer = setTimeout(done, delayMs);
    signal.addEventListener("abort", done, { once: true });

    function done() {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
  });
}

function lifecycleEvent(type, detail) {
  const event = new Event(type);
  Object.defineProperty(event, "detail", { value: detail });
  return event;
}

function observedStream(stream, onFailure) {
  const iterator = stream[Symbol.asyncIterator]();
  const observed = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next(...args) {
      return observeResult(() => iterator.next(...args), onFailure);
    },
  };

  for (const method of ["nextBatch", "nextBodyBatch"]) {
    if (typeof iterator[method] === "function") {
      observed[method] = (...args) => observeResult(() => iterator[method](...args), onFailure);
    }
  }
  if (typeof iterator.return === "function") {
    observed.return = (...args) => iterator.return(...args);
  }
  if (typeof iterator.throw === "function") {
    observed.throw = (...args) => iterator.throw(...args);
  }
  return observed;
}

async function observeResult(operation, onFailure) {
  try {
    return await operation();
  } catch (error) {
    onFailure(error);
    throw error;
  }
}

function safeClose(transport, closeInfo) {
  try {
    transport?.close?.(closeInfo);
  } catch {
    // The failed generation may already have released its transport resources.
  }
}
