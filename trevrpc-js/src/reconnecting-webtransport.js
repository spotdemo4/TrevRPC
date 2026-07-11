import { ReconnectingTransport, stripReconnectOptions } from "./reconnecting.js";
import { unavailable } from "./status.js";
import { WebTransportClient, createWebTransportSession } from "./webtransport.js";

/** Managed browser WebTransport client with background session reconnection. */
export class ReconnectingWebTransportClient extends ReconnectingTransport {
  constructor(url, options = {}) {
    const retainedUrl = url instanceof URL ? url.href : url;
    const retainedOptions = Object.freeze({ ...options });
    const transportOptions = stripReconnectOptions(retainedOptions);
    super(
      (signal) => connectWebTransport(retainedUrl, transportOptions, signal),
      retainedOptions,
      (transport) => transport.session.closed,
    );
    this.url = retainedUrl;
    this.options = retainedOptions;
  }

  /** Creates a managed client and waits for its first ready generation. */
  static async connect(url, options = {}) {
    const client = new ReconnectingWebTransportClient(url, options);
    await client.waitUntilReady();
    return client;
  }
}

async function connectWebTransport(url, options, signal) {
  const session = createWebTransportSession(url, options);
  const onAbort = () => session.close?.();
  signal.addEventListener("abort", onAbort, { once: true });

  try {
    await session.ready;
    if (signal.aborted) {
      throw unavailable("managed transport is closed");
    }
    return new WebTransportClient(session, options);
  } catch (error) {
    session.close?.();
    throw error;
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}
