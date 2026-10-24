import { QMuxWebSocketSession } from "./qmux/session.js";
import { dialRace } from "./race.js";
import { cancelled, failedPrecondition, statusFromTransportError, unavailable } from "./status.js";

/**
 * The WebTransport protocol that names wire version 1, offered through the `protocols` constructor
 * option. It is the protocol version, so callers cannot change it.
 */
const WebTransportProtocol = "trevrpc-1";

/** WebTransport constructor options passed through unchanged. */
const WebTransportOptionKeys = [
  "allowPooling",
  "congestionControl",
  "requireUnreliable",
  "serverCertificateHashes",
];

const Backoff = Object.freeze({ initialDelayMs: 100, maxDelayMs: 30_000, jitter: 0.2 });

const DefaultTransports = Object.freeze(["webtransport", "websocket"]);
const DefaultFallbackDelayMs = 300;
const DefaultReprobeIntervalMs = 5 * 60_000;

/**
 * A session that reconnects in the background after it is lost: WebTransport, or QMux over
 * WebSocket when WebTransport is unavailable or blocked. Calls open their streams on the current
 * session and fail fast with Unavailable while it reconnects; a call that was in flight when the
 * session was lost is never replayed.
 */
export class Channel {
  #dial;
  #compression;
  #onStateChange;
  #session;
  #transport;
  #state = "ready";
  #waiters = [];
  #stopped = new AbortController();

  /** Opens a channel and waits for its first session; use connect() instead. */
  static async connect(url, options = {}) {
    const dial = sessionDialer(url, options);
    return new Channel(await dial(options.signal), dial, options);
  }

  constructor(dialed, dial, { compression, onStateChange } = {}) {
    this.#dial = dial;
    this.#compression = compression;
    this.#onStateChange = onStateChange;
    this.#adopt(dialed);
  }

  /** "ready", "reconnecting" while the session is being re-established, or "closed". */
  get state() {
    return this.#state;
  }

  /** The transport of the current or last session: "webtransport" or "websocket". */
  get transport() {
    return this.#transport;
  }

  /** The compression settings from the connect options, or undefined. */
  get compression() {
    return this.#compression;
  }

  /** Opens a bidirectional stream on the current session. */
  createBidirectionalStream() {
    if (this.#state !== "ready") {
      return Promise.reject(unavailable(`channel is ${this.#state}`));
    }
    return this.#session.createBidirectionalStream();
  }

  /**
   * Opens a bidirectional stream on the current session, together with that session: the
   * WebTransport or QMux over WebSocket session, whose `datagrams` are undefined for QMux. Calls
   * whose setup depends on their session (dictionary negotiation, datagram flows) open their stream
   * here, so that setup belongs to the session that carries them even if the channel reconnects
   * meanwhile.
   */
  createBidirectionalStreamWithSession() {
    if (this.#state !== "ready") {
      return Promise.reject(unavailable(`channel is ${this.#state}`));
    }
    const session = this.#session;
    return session.createBidirectionalStream().then((stream) => ({ stream, session }));
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

  #adopt({ session, transport }) {
    this.#session = session;
    this.#transport = transport;
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
        const dialed = await this.#dial(signal);
        if (signal.aborted) {
          closeQuietly(dialed.session);
          return;
        }
        this.#adopt(dialed);
        this.#transition("ready", { attempt, transport: dialed.transport });
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

/** Opens a channel to a TrevRPC server and waits for its session to be ready. */
export function connect(url, options = {}) {
  return Channel.connect(url, options);
}

/**
 * Returns `dial(signal)`, which establishes the next session as `{ session, transport }`.
 *
 * WebTransport is preferred. QMux over WebSocket starts in parallel once WebTransport fails or has
 * not connected within `fallbackDelayMs`, and the first ready session wins. After WebSocket wins,
 * reconnects use it directly until `reprobeIntervalMs` has passed.
 */
function sessionDialer(url, options) {
  const webSocketUrl = options.webSocketUrl ?? defaultWebSocketUrl(url);
  const candidates = {
    webtransport: {
      transport: "webtransport",
      dial: (signal) => dialWebTransport(url, options, signal),
    },
    websocket: {
      transport: "websocket",
      dial: (signal) => dialWebSocket(webSocketUrl, options, signal),
    },
  };
  const [primary, fallback, ...rest] = (options.transports ?? DefaultTransports).map((name) => {
    const candidate = candidates[name];
    if (candidate === undefined) {
      throw new TypeError(`unknown transport "${name}"`);
    }
    return candidate;
  });
  if (primary === undefined || rest.length > 0 || primary === fallback) {
    throw new TypeError("transports must list one or two distinct transports");
  }
  const delayMs = options.fallbackDelayMs ?? DefaultFallbackDelayMs;
  const reprobeIntervalMs = options.reprobeIntervalMs ?? DefaultReprobeIntervalMs;
  let fallbackWonAt = null;

  const dialOne = async (candidate, signal) => ({
    session: await candidate.dial(signal),
    transport: candidate.transport,
  });
  return async (signal) => {
    if (fallback === undefined) {
      return dialOne(primary, signal);
    }
    if (fallbackWonAt !== null && Date.now() - fallbackWonAt < reprobeIntervalMs) {
      return dialOne(fallback, signal);
    }
    const dialed = await dialRace(primary, fallback, { delayMs, signal });
    fallbackWonAt = dialed.transport === fallback.transport ? Date.now() : null;
    return dialed;
  };
}

/** The `wss://` URL of the same endpoint as an `https://` URL. */
function defaultWebSocketUrl(url) {
  const webSocketUrl = new URL(url);
  webSocketUrl.protocol = webSocketUrl.protocol === "http:" ? "ws:" : "wss:";
  return webSocketUrl.href;
}

/** Establishes a ready WebTransport session; aborting the signal abandons the attempt. */
function dialWebTransport(url, options, signal) {
  const WebTransportConstructor = options.WebTransport ?? globalThis.WebTransport;
  if (typeof WebTransportConstructor !== "function") {
    throw unavailable("WebTransport is not available in this JavaScript runtime");
  }
  if (signal?.aborted) {
    throw abortError(signal);
  }
  return establish(
    new WebTransportConstructor(url, transportOptions(options)),
    "WebTransport",
    signal,
    requireWireVersion,
  );
}

/**
 * Fails a ready WebTransport session whose server selected another protocol than version 1. An
 * empty or missing `protocol` means version 1: older browsers do not support the option, and the
 * Rust server cannot echo a selection. A mismatch is FailedPrecondition rather than Unavailable
 * because retrying the same server cannot fix it; the channel still treats it like any failed
 * dial, so the WebSocket fallback and the reconnect loop carry on.
 */
function requireWireVersion(session) {
  const { protocol } = session;
  if (typeof protocol === "string" && protocol !== "" && protocol !== WebTransportProtocol) {
    throw failedPrecondition(
      `server selected WebTransport protocol "${protocol}", but this client speaks "${WebTransportProtocol}"`,
    );
  }
}

/** Establishes a ready QMux over WebSocket session; aborting the signal abandons the attempt. */
function dialWebSocket(url, options, signal) {
  if (signal?.aborted) {
    throw abortError(signal);
  }
  let session;
  try {
    session = new QMuxWebSocketSession(url, { WebSocket: options.WebSocket });
  } catch (error) {
    throw statusFromTransportError(error);
  }
  return establish(session, "WebSocket", signal);
}

/**
 * Waits for a new session to become ready, closing it if it fails or the signal aborts. `verify`
 * may reject the ready session by throwing.
 */
async function establish(session, name, signal, verify) {
  const abandon = () => closeQuietly(session);
  signal?.addEventListener("abort", abandon, { once: true });
  try {
    // A session that fails to connect settles both promises; one that closes first never becomes
    // ready, so readiness must not wait for it.
    await Promise.race([
      session.ready,
      session.closed.then(() => {
        throw unavailable(`${name} session closed before becoming ready`);
      }),
    ]);
    verify?.(session);
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
  const picked = { protocols: [WebTransportProtocol] };
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
