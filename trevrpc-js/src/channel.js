import { ConnectionMetadataSource } from "./connection-metadata.js";
import { interceptorList } from "./interceptor.js";
import { QMuxWebSocketSession } from "./qmux/session.js";
import { dialRace } from "./race.js";
import { cancelled, failedPrecondition, statusFromTransportError, unavailable } from "./status.js";
import { StreamBudget } from "./stream-compression.js";

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
 * session was lost is never replayed. Calls started while the channel first connects wait for
 * that attempt, and fail with its error if it fails.
 */
export class Channel {
  #dial;
  #compression;
  #streamCompressionBudget;
  #connectionMetadata;
  #interceptors;
  #onStateChange;
  #session;
  #transport;
  #state = "connecting";
  /**
   * Settles once the first attempt to connect did: with undefined if it established a session,
   * and with its error otherwise.
   */
  #connecting;
  #waiters = [];
  #stopped = new AbortController();

  /** Opens a channel and waits for its first session; use connect() instead. */
  static async connect(url, options = {}) {
    const { signal } = options;
    if (signal?.aborted) {
      throw abortError(signal);
    }
    const channel = new Channel(url, options, { retry: false });
    const abandon = () => channel.close();
    signal?.addEventListener("abort", abandon, { once: true });
    try {
      const failure = await channel.#connecting;
      if (signal?.aborted) {
        throw abortError(signal);
      }
      if (failure !== undefined) {
        throw failure;
      }
      return channel;
    } finally {
      signal?.removeEventListener("abort", abandon);
    }
  }

  /**
   * Starts connecting; use createChannel() or connect() instead. Unless `retry` is false, a first
   * attempt that fails is retried like a reconnect; otherwise it closes the channel.
   */
  constructor(url, options = {}, { retry = true } = {}) {
    // Checked before dialing, so an invalid option never leaves a session behind.
    this.#dial = sessionDialer(url, options);
    this.#interceptors = interceptorList(options.interceptors, "interceptors");
    this.#streamCompressionBudget = new StreamBudget(options.streamCompressionMemory);
    this.#connectionMetadata = new ConnectionMetadataSource(options.connectionMetadata);
    this.#compression = options.compression;
    this.#onStateChange = options.onStateChange;
    this.#connecting = this.#connect(retry);
  }

  /**
   * "connecting" until the first session is ready, "ready", "reconnecting" while the session is
   * being re-established, or "closed".
   */
  get state() {
    return this.#state;
  }

  /**
   * The transport of the current or last session: "webtransport" or "websocket"; undefined before
   * the first.
   */
  get transport() {
    return this.#transport;
  }

  /** The compression settings from the connect options, or undefined. */
  get compression() {
    return this.#compression;
  }

  /** The memory budget of the stream compression contexts of this channel's calls. */
  get streamCompressionBudget() {
    return this.#streamCompressionBudget;
  }

  /** The connection metadata every call of this channel carries. */
  get connectionMetadata() {
    return this.#connectionMetadata;
  }

  /** The interceptors that every client created on this channel runs around its calls. */
  get interceptors() {
    return this.#interceptors;
  }

  /**
   * Replaces the channel's connection metadata, which every call carries; empty or missing
   * metadata removes it. Calls started after this use the new metadata. Throws InvalidArgument if
   * it is invalid, and then keeps the old.
   */
  setConnectionMetadata(metadata) {
    this.#connectionMetadata.set(metadata);
  }

  /** Opens a bidirectional stream on the current session. */
  createBidirectionalStream() {
    return this.#withSession((session) => session.createBidirectionalStream());
  }

  /**
   * Opens a bidirectional stream on the current session, together with that session: the
   * WebTransport or QMux over WebSocket session, whose `datagrams` are undefined for QMux. Calls
   * whose setup depends on their session (dictionary negotiation, datagram flows) open their stream
   * here, so that setup belongs to the session that carries them even if the channel reconnects
   * meanwhile.
   */
  createBidirectionalStreamWithSession() {
    return this.#withSession((session) =>
      session.createBidirectionalStream().then((stream) => ({ stream, session })),
    );
  }

  /**
   * Calls `open` with the current session. While the channel first connects, that waits for the
   * attempt, and a failed attempt fails the call with its error; otherwise a channel without a
   * session fails it with Unavailable.
   */
  #withSession(open) {
    if (this.#state === "ready") {
      return open(this.#session);
    }
    if (this.#state === "connecting") {
      // The state has moved on once the attempt settled, so this does not wait again.
      return this.#connecting.then((failure) =>
        failure === undefined || this.#state === "closed"
          ? this.#withSession(open)
          : Promise.reject(failure),
      );
    }
    return Promise.reject(unavailable(`channel is ${this.#state}`));
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

  /**
   * Makes the first attempt to connect, and returns its error, or undefined once it established a
   * session. A failure starts reconnecting if `retry` is true and closes the channel otherwise.
   */
  async #connect(retry) {
    const { signal } = this.#stopped;
    let dialed;
    try {
      dialed = await this.#dial(signal);
    } catch (error) {
      if (!signal.aborted) {
        if (retry) {
          void this.#reconnect(error);
        } else {
          this.close();
        }
      }
      return error;
    }
    if (signal.aborted) {
      closeQuietly(dialed.session);
      return abortError(signal);
    }
    this.#ready(dialed);
    return undefined;
  }

  /** Adopts a new session and wakes up whoever waits for the channel to be ready. */
  #ready(dialed, extra) {
    this.#adopt(dialed);
    this.#transition("ready", { ...extra, transport: dialed.transport });
    for (const waiter of this.#waiters.splice(0)) {
      waiter.resolve();
    }
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
        this.#ready(dialed, { attempt });
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

/**
 * Opens a channel to a TrevRPC server and waits for its session to be ready. It rejects, leaving
 * nothing behind, if the first attempt fails.
 */
export function connect(url, options = {}) {
  return Channel.connect(url, options);
}

/**
 * Returns a channel to a TrevRPC server that connects in the background, so that clients can be
 * created on it right away. Calls started before its first session is ready wait for that attempt;
 * a failed attempt fails them and the channel goes on reconnecting with backoff. The `signal`
 * option does not apply: close the channel instead.
 */
export function createChannel(url, options = {}) {
  return new Channel(url, options);
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
