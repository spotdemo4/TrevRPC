// Unreliable RPC messages over WebTransport datagrams. One DatagramEndpoint per session owns the
// session's single datagram reader loop, its single shared writer, the flow ID allocator, and the
// demultiplexer from flow IDs to calls. Calls that stream an unreliable direction open a
// DatagramFlow on the endpoint of the session that carries them. QMux over WebSocket has no
// datagrams, so those calls never get a flow and everything stays on the stream.
import { byteView } from "./bytes.js";
import { FrameType } from "./framing.js";
import { readVarint, varintLength, writeVarint } from "./varint.js";

/**
 * Largest datagram assumed when the browser does not report `maxDatagramSize`. Browsers silently
 * drop datagrams above the real limit, so this stays below a typical path MTU.
 */
export const FallbackMaxDatagramSize = 1100;

/**
 * How much a flow buffers for a call that is not ready to take datagrams yet: at most this many
 * messages or bytes, then further datagrams are dropped.
 */
export const FlowQueueMessages = 32;
export const FlowQueueBytes = 1 << 20;

/**
 * Encoded size of one outgoing message (`{ type, payload }` from encodeMessage) as a MESSAGE or
 * ZSTD_MESSAGE frame inside a datagram.
 */
export function messageFrameSize({ payload }) {
  return 1 + varintLength(payload.byteLength) + payload.byteLength;
}

/**
 * Encodes a datagram payload: the flow ID as a QUIC varint, then the frame of each outgoing
 * message, in order.
 */
export function encodeDatagram(flow, messages) {
  let size = varintLength(flow);
  for (const message of messages) {
    size += messageFrameSize(message);
  }
  const datagram = new Uint8Array(size);
  let offset = writeVarint(datagram, 0, flow);
  for (const { type, payload } of messages) {
    datagram[offset] = type;
    offset = writeVarint(datagram, offset + 1, payload.byteLength);
    datagram.set(payload, offset);
    offset += payload.byteLength;
  }
  return datagram;
}

/**
 * Decodes a datagram payload as `{ flow, messages }`, or null if it has no complete flow ID. The
 * messages are its MESSAGE and ZSTD_MESSAGE frames as `{ type, payload }`; every other frame is
 * skipped. A datagram whose frames do not exactly fill it decodes to no messages at all.
 */
export function decodeDatagram(datagram) {
  const bytes = byteView(datagram);
  const flow = readVarint(bytes, 0);
  if (flow === null) {
    return null;
  }
  const messages = [];
  let offset = flow.next;
  while (offset < bytes.byteLength) {
    const type = readVarint(bytes, offset);
    const length = type === null ? null : readVarint(bytes, type.next);
    if (length === null || typeof length.value !== "number") {
      return { flow: flow.value, messages: [] };
    }
    const end = length.next + length.value;
    if (end > bytes.byteLength) {
      return { flow: flow.value, messages: [] };
    }
    if (type.value === FrameType.Message || type.value === FrameType.ZstdMessage) {
      messages.push({ type: type.value, payload: bytes.subarray(length.next, end) });
    }
    offset = end;
  }
  return { flow: flow.value, messages };
}

const endpoints = new WeakMap();

/**
 * The shared endpoint of a session's `datagrams` object, or undefined if the session has none or
 * its datagram reader is unusable. The endpoint lives as long as the object does.
 */
export function datagramEndpoint(datagrams) {
  if (datagrams == null || typeof datagrams !== "object") {
    return undefined;
  }
  let endpoint = endpoints.get(datagrams);
  if (endpoint === undefined) {
    endpoint = new DatagramEndpoint(datagrams);
    endpoints.set(datagrams, endpoint);
  }
  return endpoint.usable ? endpoint : undefined;
}

class DatagramEndpoint {
  #datagrams;
  /** Flows that accept datagrams, by ID. */
  #flows = new Map();
  #nextFlow = 1;
  #reading = false;
  #closed;
  /** The shared writer: undefined until first needed, null once unavailable. */
  #writer;

  constructor(datagrams) {
    this.#datagrams = datagrams;
    this.#closed = typeof datagrams.readable?.getReader !== "function";
  }

  get usable() {
    return !this.#closed;
  }

  /**
   * Allocates the next flow of this session. A flow that `receive`s is registered with the
   * reader, so datagrams for its ID reach it; `inflate` maps each received `{ type, payload }`
   * message to its payload, decompressed, and throws if it is invalid. Returns undefined if the
   * session cannot read datagrams.
   */
  openFlow({ receive, inflate }) {
    if (!this.#start()) {
      return undefined;
    }
    const flow = new DatagramFlow(this, this.#nextFlow, inflate);
    this.#nextFlow += 1;
    if (receive) {
      this.#flows.set(flow.id, flow);
    }
    return flow;
  }

  /** Stops routing datagrams to a flow; its ID is never reused. */
  closeFlow(id) {
    this.#flows.delete(id);
  }

  /**
   * How many bytes of MESSAGE frames fit in a datagram of `flow` right now, or 0 if this session
   * cannot send datagrams.
   */
  frameRoom(flow) {
    return this.#writerOrNull() === null
      ? 0
      : Math.max(0, this.maxDatagramSize - varintLength(flow));
  }

  /**
   * Sends messages as one datagram of `flow`. Resolves true once the datagram was handed to the
   * transport, and false if the messages have to go on the stream instead: they do not fit, there
   * is no writer, or the write failed. Waits for send-buffer space until `signal` aborts, then
   * rejects with the abort reason.
   */
  async send(flow, messages, signal) {
    const writer = this.#writerOrNull();
    const datagram = writer === null ? null : encodeDatagram(flow, messages);
    if (datagram === null || datagram.byteLength > this.maxDatagramSize) {
      return false;
    }
    try {
      if (!(writer.desiredSize > 0)) {
        await abortable(writer.ready, signal);
      }
      await abortable(writer.write(datagram), signal);
      return true;
    } catch {
      if (signal.aborted) {
        throw signal.reason;
      }
      return false;
    }
  }

  /** The largest datagram payload the session accepts right now, flow ID included. */
  get maxDatagramSize() {
    const size = this.#datagrams.maxDatagramSize;
    return Number.isFinite(size) && size > 0 ? size : FallbackMaxDatagramSize;
  }

  #start() {
    if (this.#closed) {
      return false;
    }
    if (!this.#reading) {
      let reader;
      try {
        reader = this.#datagrams.readable.getReader();
      } catch {
        this.#closed = true;
        return false;
      }
      this.#reading = true;
      void this.#read(reader);
    }
    return true;
  }

  /** The reader loop. It never blocks on a call: delivery is a synchronous, bounded push. */
  async #read(reader) {
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        const datagram = decodeDatagram(value);
        const flow = datagram === null ? undefined : this.#flows.get(datagram.flow);
        // Unknown and ended flows drop the datagram, and so do malformed ones (no messages).
        flow?.deliverAll(datagram.messages);
      }
    } catch {
      // A failed session ends its calls through their streams.
    } finally {
      this.#closed = true;
      this.#flows.clear();
    }
  }

  #writerOrNull() {
    if (this.#writer === undefined) {
      this.#writer = this.#openWriter();
    }
    return this.#writer;
  }

  /** Locks the session's datagram writable once; a lost writer disables sending datagrams. */
  #openWriter() {
    try {
      const datagrams = this.#datagrams;
      const writable =
        typeof datagrams.createWritable === "function"
          ? datagrams.createWritable()
          : datagrams.writable;
      const writer = writable.getWriter();
      writer.closed.catch(() => {
        this.#writer = null;
      });
      return writer;
    } catch {
      return null;
    }
  }
}

/**
 * One call's datagram flow. Datagrams for the flow wait in a bounded buffer until the call
 * connects, then go straight to its handler. The call releases the flow when it ends.
 */
export class DatagramFlow {
  #endpoint;
  #inflate;
  #handler;
  #buffered = [];
  #bufferedBytes = 0;
  #released = false;

  constructor(endpoint, id, inflate) {
    this.#endpoint = endpoint;
    this.#inflate = inflate;
    this.id = id;
  }

  /**
   * Delivers the messages of one received datagram, decompressed. A datagram with an invalid
   * message is dropped whole.
   */
  deliverAll(messages) {
    let payloads;
    try {
      payloads = messages.map(this.#inflate);
    } catch {
      return;
    }
    for (const payload of payloads) {
      this.#deliver(payload);
    }
  }

  /**
   * Delivers one received message. It must not block or throw. Without a handler it is buffered,
   * and dropped when the buffer is full.
   */
  #deliver(message) {
    if (this.#handler !== undefined) {
      this.#handler(message);
    } else if (
      this.#buffered.length < FlowQueueMessages &&
      this.#bufferedBytes + message.byteLength <= FlowQueueBytes
    ) {
      this.#buffered.push(message);
      this.#bufferedBytes += message.byteLength;
    }
  }

  /** Hands the buffered messages, then all later ones, to `handler`. Only the first call counts. */
  connect(handler) {
    if (this.#handler !== undefined || this.#released) {
      return;
    }
    this.#handler = handler;
    const buffered = this.#buffered;
    this.#buffered = [];
    this.#bufferedBytes = 0;
    for (const message of buffered) {
      handler(message);
    }
  }

  /** See DatagramEndpoint.frameRoom; 0 once the flow is released. */
  get frameRoom() {
    return this.#released ? 0 : this.#endpoint.frameRoom(this.id);
  }

  /** See DatagramEndpoint.send. */
  send(messages, signal) {
    return this.#released ? Promise.resolve(false) : this.#endpoint.send(this.id, messages, signal);
  }

  /** Ends the flow: later datagrams for it are dropped. */
  release() {
    if (!this.#released) {
      this.#released = true;
      this.#handler = undefined;
      this.#buffered = [];
      this.#endpoint.closeFlow(this.id);
    }
  }
}

/** Settles like `promise`, or rejects with the abort reason as soon as `signal` aborts. */
function abortable(promise, signal) {
  if (signal.aborted) {
    return Promise.reject(signal.reason);
  }
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
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
