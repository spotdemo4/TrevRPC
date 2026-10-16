import { cancelled, statusFromTransportError, unavailable } from "./status.js";

/** WebTransport constructor options passed through unchanged. */
const WebTransportOptionKeys = [
  "allowPooling",
  "congestionControl",
  "requireUnreliable",
  "serverCertificateHashes",
];

const Backoff = Object.freeze({ initialDelayMs: 100, maxDelayMs: 30_000, jitter: 0.2 });

/**
 * A WebTransport session that reconnects in the background after it is lost. Calls open their
 * streams on the current session and fail fast with Unavailable while it reconnects; a call that
 * was in flight when the session was lost is never replayed.
 */
export class Channel {
  #dial;
  #onStateChange;
  #session;
  #state = "ready";
  #waiters = [];
  #stopped = new AbortController();

  /** Opens a channel and waits for its first session; use connect() instead. */
  static async connect(url, options = {}) {
    const dial = (signal) => dialSession(url, options, signal);
    return new Channel(await dial(options.signal), dial, options.onStateChange);
  }

  constructor(session, dial, onStateChange) {
    this.#dial = dial;
    this.#onStateChange = onStateChange;
    this.#adopt(session);
  }

  /** "ready", "reconnecting" while the session is being re-established, or "closed". */
  get state() {
    return this.#state;
  }

  /** Opens a bidirectional stream on the current session. */
  createBidirectionalStream() {
    if (this.#state !== "ready") {
      return Promise.reject(unavailable(`channel is ${this.#state}`));
    }
    return this.#session.createBidirectionalStream();
  }

  /** Resolves once the channel is ready, immediately if it already is. */
  waitUntilReady() {
    if (this.#state === "ready") {
      return Promise.resolve();
    }
    if (this.#state === "closed") {
      return Promise.reject(unavailable("channel is closed"));
    }
    return new Promise((resolve, reject) => this.#waiters.push({ resolve, reject }));
  }

  /** Stops reconnecting and closes the current session. */
  close(closeInfo) {
    if (this.#state === "closed") {
      return;
    }
    const session = this.#session;
    this.#session = undefined;
    this.#stopped.abort();
    this.#transition("closed");
    for (const waiter of this.#waiters.splice(0)) {
      waiter.reject(unavailable("channel is closed"));
    }
    closeQuietly(session, closeInfo);
  }

  #adopt(session) {
    this.#session = session;
    const lost = (reason) => this.#lost(session, reason);
    session.closed.then(lost, lost);
  }

  #lost(session, reason) {
    if (session !== this.#session) {
      return;
    }
    this.#session = undefined;
    closeQuietly(session);
    void this.#reconnect(reason);
  }

  async #reconnect(error) {
    const { signal } = this.#stopped;
    for (let attempt = 1; !signal.aborted; attempt += 1) {
      const delayMs = backoffDelay(attempt);
      this.#transition("reconnecting", { attempt, delayMs, error });
      await sleep(delayMs, signal);
      if (signal.aborted) {
        return;
      }

      try {
        const session = await this.#dial(signal);
        if (signal.aborted) {
          closeQuietly(session);
          return;
        }
        this.#adopt(session);
        this.#transition("ready", { attempt });
        for (const waiter of this.#waiters.splice(0)) {
          waiter.resolve();
        }
        return;
      } catch (dialError) {
        error = dialError;
      }
    }
  }

  #transition(state, extra = {}) {
    const previousState = this.#state;
    this.#state = state;
    try {
      this.#onStateChange?.({ state, previousState, ...extra });
    } catch (error) {
      globalThis.reportError?.(error);
    }
  }
}

/** Opens a channel to a WebTransport server and waits for its session to be ready. */
export function connect(url, options = {}) {
  return Channel.connect(url, options);
}

/** Establishes a ready WebTransport session; aborting the signal abandons the attempt. */
async function dialSession(url, options, signal) {
  const WebTransportConstructor = options.WebTransport ?? globalThis.WebTransport;
  if (typeof WebTransportConstructor !== "function") {
    throw unavailable("WebTransport is not available in this JavaScript runtime");
  }
  if (signal?.aborted) {
    throw abortError(signal);
  }

  const session = new WebTransportConstructor(url, transportOptions(options));
  const abandon = () => closeQuietly(session);
  signal?.addEventListener("abort", abandon, { once: true });
  try {
    // A session that fails to connect settles both promises; one that closes first never becomes
    // ready, so readiness must not wait for it.
    await Promise.race([
      session.ready,
      session.closed.then(() => {
        throw unavailable("WebTransport session closed before becoming ready");
      }),
    ]);
  } catch (error) {
    closeQuietly(session);
    throw signal?.aborted ? abortError(signal) : statusFromTransportError(error);
  } finally {
    signal?.removeEventListener("abort", abandon);
  }
  if (signal?.aborted) {
    closeQuietly(session);
    throw abortError(signal);
  }
  return session;
}

function transportOptions(options) {
  const picked = {};
  for (const key of WebTransportOptionKeys) {
    if (options[key] !== undefined) {
      picked[key] = options[key];
    }
  }
  return picked;
}

function abortError(signal) {
  return cancelled("connection cancelled", signal.reason);
}

/** Exponential backoff with jitter, capped at maxDelayMs. */
function backoffDelay(attempt) {
  const { initialDelayMs, maxDelayMs, jitter } = Backoff;
  const base = Math.min(maxDelayMs, initialDelayMs * 2 ** (attempt - 1));
  return Math.min(maxDelayMs, base * (1 + (Math.random() * 2 - 1) * jitter));
}

function sleep(delayMs, signal) {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, delayMs);
    signal.addEventListener("abort", done, { once: true });
  });
}

function closeQuietly(session, closeInfo) {
  try {
    session?.close(closeInfo);
  } catch {
    // The session may already be gone.
  }
}
