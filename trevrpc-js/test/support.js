// Test doubles: an in-memory server for TrevRPC streams built on web streams, so the real client
// code runs end to end without a network. Cancelling or aborting one side of a stream errors the
// other side exactly like a QUIC reset would.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  FrameReader,
  FrameType,
  encodeHeadersFrame,
  encodeMessageFrame,
  encodeStatusFrame,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import { Code } from "../src/status.js";
import { RpcRequest } from "../src/wire.js";

/**
 * Splits a wire method path into `{ service, method }`, or returns null unless it is exactly
 * `/<service>/<method>` with both parts non-empty.
 */
export function parseMethodPath(path) {
  const parts = path.split("/");
  if (parts.length !== 3 || parts[0] !== "" || parts[1] === "" || parts[2] === "") {
    return null;
  }
  return { service: parts[1], method: parts[2] };
}

/** The server end of one bidirectional stream. */
export class ServerStream {
  constructor({ readable, writable }) {
    this.reader = readable.getReader();
    this.writer = writable.getWriter();
    this.frames = new FrameReader(this.reader);
    // Settles when the client cancels its read side (STOP_SENDING from the server's view).
    this.responseCancelled = this.writer.closed.then(
      () => false,
      () => true,
    );
  }

  /** Reads the REQUEST frame and returns the decoded RpcRequest. */
  async readRequest() {
    const frame = await this.frames.readFrame();
    if (frame?.type !== FrameType.Request) {
      throw new Error(`expected a REQUEST frame, got ${frame?.type}`);
    }
    return unmarshalMessage(RpcRequest, frame.payload);
  }

  /** Reads the next known frame from the client as `{ type, payload }`, null at the end. */
  readFrame() {
    return this.frames.readFrame();
  }

  /** Reads the next request message, null at the end of the request stream. */
  async readMessage(messageType) {
    const frame = await this.readFrame();
    if (frame === null) {
      return null;
    }
    if (frame.type !== FrameType.Message) {
      throw new Error(`client sent a frame of type ${frame.type} instead of a message`);
    }
    return messageType.decode(frame.payload);
  }

  writeHeaders(metadata = {}) {
    return this.write(encodeHeadersFrame({ metadata }));
  }

  writeMessage(messageType, message) {
    return this.write(encodeMessageFrame(marshalMessage(messageType, message)));
  }

  writeStatus(code = Code.Ok, message = "", metadata = {}) {
    return this.write(encodeStatusFrame({ code, message, metadata }));
  }

  write(bytes) {
    return this.writer.write(bytes);
  }

  /** Finishes the response stream gracefully. */
  finish() {
    return this.writer.close();
  }

  /** Resets the response stream. */
  reset(reason = new Error("reset by server")) {
    return this.writer.abort(reason);
  }

  /** Stops reading the request stream, like servers do after a final status. */
  stopSending(reason = new Error("stop sending")) {
    return this.reader.cancel(reason);
  }
}

/**
 * A client transport whose streams are served by handler(serverStream). A handler that throws, for
 * instance because its stream was reset by a cancelled call, does not fail the test by itself;
 * settled() waits for all handlers and rethrows the first failure, so tests that assert inside
 * handlers must await it.
 */
export function fakeTransport(handler, { highWaterMark = 16 } = {}) {
  const handlers = [];
  const failures = [];
  const transport = {
    opened: 0,
    async createBidirectionalStream() {
      transport.opened += 1;
      const strategy = { highWaterMark };
      const requests = new TransformStream({}, strategy, strategy);
      const responses = new TransformStream({}, strategy, strategy);
      const server = new ServerStream({
        readable: requests.readable,
        writable: responses.writable,
      });
      handlers.push(
        Promise.resolve()
          .then(() => handler(server))
          .catch((error) => failures.push(error)),
      );
      return { readable: responses.readable, writable: requests.writable };
    },
    async settled() {
      await Promise.all(handlers);
      if (failures.length > 0) {
        throw failures[0];
      }
    },
  };
  return transport;
}

export function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

/** Resolves after the event loop turned a few times. */
export function turn(count = 3) {
  return new Promise((resolve) => {
    const step = (left) => (left === 0 ? resolve() : setImmediate(() => step(left - 1)));
    step(count);
  });
}

/** Asserts a promise is still pending after the event loop settled. */
export async function isPending(promise) {
  let settled = false;
  promise.then(
    () => (settled = true),
    () => (settled = true),
  );
  await turn();
  return !settled;
}

export function hex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function concatBytes(parts) {
  const output = new Uint8Array(parts.reduce((sum, part) => sum + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}

/** A reader yielding the given chunks, then end of stream. */
export function readerOf(chunks) {
  const pending = [...chunks];
  return {
    async read() {
      const value = pending.shift();
      return value === undefined ? { done: true, value: undefined } : { done: false, value };
    },
  };
}

export function chunked(bytes, size) {
  const chunks = [];
  for (let offset = 0; offset < bytes.byteLength; offset += size) {
    chunks.push(bytes.subarray(offset, offset + size));
  }
  return chunks;
}

/**
 * The `datagrams` object of a fake WebTransport session. `push(bytes)` delivers a datagram to the
 * client; datagrams the client writes are collected in `sent` and passed to `onSend`. `gate` holds
 * back writes while it is a pending promise, and `failWrites` makes them fail.
 */
export function fakeDatagrams({ maxDatagramSize, outgoingHighWaterMark = 16 } = {}) {
  let controller;
  const datagrams = {
    readable: new ReadableStream(
      {
        start(readableController) {
          controller = readableController;
        },
      },
      { highWaterMark: 1000 },
    ),
    sent: [],
    writables: 0,
    gate: /** @type {Promise<unknown> | undefined} */ (undefined),
    onSend: undefined,
    failWrites: false,
    createWritable() {
      datagrams.writables += 1;
      return new WritableStream(
        {
          async write(chunk) {
            if (datagrams.gate !== undefined) {
              await datagrams.gate;
            }
            if (datagrams.failWrites) {
              throw new Error("datagram write failed");
            }
            datagrams.sent.push(chunk);
            datagrams.onSend?.(chunk);
          },
        },
        { highWaterMark: outgoingHighWaterMark },
      );
    },
    push(bytes) {
      controller.enqueue(bytes);
    },
  };
  if (maxDatagramSize !== undefined) {
    datagrams.maxDatagramSize = maxDatagramSize;
  }
  return datagrams;
}

/**
 * Reads a shared golden vector file from testdata/ as a map from vector name to its raw text.
 * Values are hex, except `.frames` and `.messages` lists and decimal `.id`s.
 */
export async function readGoldenVectors(file) {
  const vectors = new Map();
  const text = await readFile(new URL(`../../testdata/${file}`, import.meta.url), "utf8");
  for (const [index, rawLine] of text.split(/\r?\n/u).entries()) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) {
      continue;
    }
    const separator = line.indexOf("=");
    assert.ok(separator > 0, `invalid golden vector on line ${index + 1} of ${file}`);
    const name = line.slice(0, separator).trim();
    const value = line.slice(separator + 1).trim();
    if (![".frames", ".messages", ".id"].some((suffix) => name.endsWith(suffix))) {
      assert.match(
        value,
        /^(?:[0-9a-f]{2})*$/u,
        `invalid golden vector on line ${index + 1} of ${file}`,
      );
    }
    vectors.set(name, value);
  }
  return vectors;
}
