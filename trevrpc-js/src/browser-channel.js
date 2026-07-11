import { ChannelStateMachine, stripChannelOptions, waitForInitialReady } from "./channel.js";
import { unavailable } from "./status.js";
import { RawWebTransport, createWebTransportSession } from "./webtransport.js";

/** Browser channel with background session reconnection. */
export class Channel extends ChannelStateMachine {
  constructor(url, options = {}) {
    const retainedUrl = url instanceof URL ? url.href : url;
    const retainedOptions = Object.freeze({ ...options });
    const transportOptions = stripChannelOptions(retainedOptions);
    super(
      (signal) => connectWebTransport(retainedUrl, transportOptions, signal),
      retainedOptions,
      (transport) => transport.session.closed,
    );
    this.url = retainedUrl;
    this.options = retainedOptions;
  }

  /** Creates a channel and waits for its first ready generation. */
  static async connect(url, options = {}) {
    const channel = new Channel(url, options);
    await waitForInitialReady(channel, options);
    return channel;
  }
}

async function connectWebTransport(url, options, signal) {
  const session = createWebTransportSession(url, options);
  const onAbort = () => session.close?.();
  signal.addEventListener("abort", onAbort, { once: true });

  try {
    await session.ready;
    if (signal.aborted) {
      throw unavailable("channel is closed");
    }
    return new RawWebTransport(session, options);
  } catch (error) {
    session.close?.();
    throw error;
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}
