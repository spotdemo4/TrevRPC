// Test peers: a scripted QMux peer behind a fake WebSocket, and pairs of real sessions connected
// by in-memory WebSockets.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import { FrameParser, FrameType, FrameWriter, writePing, writeStreamFrame } from "../src/frames.js";
import { writeTransportParameters } from "../src/params.js";
import { QMuxSession } from "../src/session.js";

export const Protocol = "test-qmux-02";

/** The shared golden vectors in testdata/, as bytes by name. */
export async function goldenVectors() {
  const vectors = new Map();
  const file = new URL("../../../testdata/qmux-golden-vectors.txt", import.meta.url);
  const text = await readFile(file, "utf8");
  for (const [index, rawLine] of text.split(/\r?\n/u).entries()) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) {
      continue;
    }
    const separator = line.indexOf("=");
    assert.ok(separator > 0, `invalid golden vector on line ${index + 1}`);
    const value = line.slice(separator + 1).trim();
    assert.match(value, /^(?:[0-9a-f]{2})*$/u, `invalid golden vector on line ${index + 1}`);
    vectors.set(line.slice(0, separator).trim(), Uint8Array.from(Buffer.from(value, "hex")));
  }
  return vectors;
}

/** The bytes of the frames `write` appends to a FrameWriter. */
export function frames(write) {
  const writer = new FrameWriter();
  write(writer);
  return writer.take();
}

/** Lets queued microtasks, such as in-memory WebSocket deliveries, run. */
export const flushMicrotasks = () => new Promise((resolve) => setImmediate(resolve));

export async function readAll(readable) {
  const chunks = [];
  for await (const chunk of readable) {
    chunks.push(...chunk);
  }
  return Uint8Array.from(chunks);
}

/** The transport parameters of the scripted peer, a server that allows a few client streams. */
export const PeerParameters = {
  maxIdleTimeoutMs: 0,
  initialMaxData: 1 << 20,
  initialMaxStreamDataBidiRemote: 256 << 10,
  initialMaxStreamsBidi: 4,
  maxRecordSize: 64 << 10,
};

/**
 * A WebSocket whose server side is a scripted QMux peer: it answers the client's transport
 * parameters with `parameters`, echoes every stream's data back with the FIN, and records the
 * frames it receives. `onFrame` returning true skips the default handling.
 */
export function scriptedPeer({
  parameters = PeerParameters,
  protocol = Protocol,
  onFrame,
  opens = true,
  maxDatagramFrameSize = 0,
} = {}) {
  const sockets = [];
  class ScriptedWebSocket {
    binaryType = "blob";
    bufferedAmount = 0;
    readyState = 0;
    protocol = "";
    received = [];
    #echoed = new Map();
    #waiters = [];
    #pingSequence = 1000;

    constructor(url, protocols) {
      this.url = url;
      this.protocols = protocols;
      sockets.push(this);
      if (!opens) {
        return;
      }
      queueMicrotask(() => {
        this.readyState = 1;
        this.protocol = protocol;
        this.onopen?.();
      });
    }

    send(data) {
      const parser = new FrameParser(new Uint8Array(data), { maxDatagramFrameSize });
      for (let frame = parser.next(); frame !== null; frame = parser.next()) {
        this.received.push(frame);
        this.#waiters = this.#waiters.filter(({ predicate, resolve }) => {
          if (predicate(frame)) {
            resolve(frame);
            return false;
          }
          return true;
        });
        if (onFrame?.(this, frame)) {
          continue;
        }
        if (frame.type === FrameType.TransportParameters) {
          this.deliver(frames((w) => writeTransportParameters(w, parameters)));
        } else if (frame.data !== undefined) {
          const offset = this.#echoed.get(frame.streamId) ?? 0;
          this.#echoed.set(frame.streamId, offset + frame.data.length);
          this.deliver(
            frames((w) => writeStreamFrame(w, frame.streamId, offset, frame.data, frame.fin)),
          );
        }
      }
    }

    /** Resolves with the first frame the session sent that matches, waiting for it if need be. */
    waitFor(predicate) {
      const frame = this.received.find(predicate);
      if (frame !== undefined) {
        return Promise.resolve(frame);
      }
      return new Promise((resolve) => this.#waiters.push({ predicate, resolve }));
    }

    /**
     * Sends a QX_PING and waits for its response: the session handles records in order, so it has
     * handled everything delivered before.
     */
    roundTrip() {
      const sequence = this.#pingSequence;
      this.#pingSequence += 1;
      this.deliver(frames((w) => writePing(w, FrameType.Ping, sequence)));
      return this.waitFor(
        (frame) => frame.type === FrameType.PingResponse && frame.sequence === sequence,
      );
    }

    /** Delivers frames from the peer. */
    deliver(bytes) {
      queueMicrotask(() => {
        if (this.readyState === 1) {
          this.onmessage?.({
            data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
          });
        }
      });
    }

    close(code = 1000) {
      if (this.readyState === 3) {
        return;
      }
      this.readyState = 3;
      queueMicrotask(() => this.onclose?.({ code }));
    }
  }
  return { WebSocket: ScriptedWebSocket, sockets };
}

/** A client session to a scripted peer, once ready. */
export async function openScripted(peer, options = {}) {
  const session = new QMuxSession("wss://example.test/qmux", {
    protocols: [Protocol],
    WebSocket: peer.WebSocket,
    ...options,
  });
  await session.ready;
  return session;
}

/** One end of an in-memory WebSocket connection. */
class MemoryWebSocket {
  binaryType = "blob";
  bufferedAmount = 0;
  readyState = 0;
  protocol = "";
  peer = null;
  /** Records sent, for tests that inspect the wire. */
  sent = [];

  send(data) {
    assert.equal(this.readyState, 1, "send on a WebSocket that is not open");
    const bytes = Uint8Array.from(data);
    this.sent.push(bytes);
    const { peer } = this;
    queueMicrotask(() => {
      if (peer.readyState === 1) {
        peer.onmessage?.({ data: bytes.buffer });
      }
    });
  }

  /** Closes both ends after the messages already sent, as a WebSocket closing handshake does. */
  close(code = 1000) {
    if (this.readyState >= 2) {
      return;
    }
    this.readyState = 2;
    queueMicrotask(() => {
      for (const end of [this, this.peer]) {
        if (end.readyState < 3) {
          end.readyState = 3;
          end.onclose?.({ code });
        }
      }
    });
  }

  open(protocol) {
    this.readyState = 1;
    this.protocol = protocol;
    queueMicrotask(() => this.onopen?.());
  }
}

/** Two connected in-memory WebSockets that open with `protocol` selected. */
export function memorySockets(protocol = Protocol) {
  const client = new MemoryWebSocket();
  const server = new MemoryWebSocket();
  client.peer = server;
  server.peer = client;
  client.open(protocol);
  server.open(protocol);
  return { client, server };
}

/**
 * A client session dialing through `WebSocket` to a server session over the accepted socket, with
 * their options. Both are returned once ready.
 */
export async function sessionPair(clientOptions = {}, serverOptions = {}) {
  let accepted;
  class DialingWebSocket extends MemoryWebSocket {
    constructor(url, protocols) {
      super();
      const server = new MemoryWebSocket();
      this.peer = server;
      server.peer = this;
      this.open(protocols[0]);
      server.open(protocols[0]);
      accepted = new QMuxSession(server, { role: "server", ...serverOptions });
    }
  }
  const client = new QMuxSession("wss://example.test/qmux", {
    protocols: [Protocol],
    WebSocket: DialingWebSocket,
    ...clientOptions,
  });
  const server = accepted;
  await Promise.all([client.ready, server.ready]);
  return { client, server };
}
