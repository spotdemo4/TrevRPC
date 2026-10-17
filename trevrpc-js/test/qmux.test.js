import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { connect } from "../src/channel.js";
import {
  FrameParser,
  FrameType,
  FrameWriter,
  QMuxProtocolError,
  TransportError,
  writeConnectionClose,
  writeMaxData,
  writeMaxStreamData,
  writePing,
  writeResetStream,
  writeStopSending,
  writeStreamFrame,
} from "../src/qmux/frames.js";
import { decodeTransportParameters, writeTransportParameters } from "../src/qmux/params.js";
import { QMuxProtocol, QMuxWebSocketSession } from "../src/qmux/session.js";
import { readVarint, writeVarint } from "../src/qmux/varint.js";
import { Code } from "../src/status.js";

const goldenPath = new URL("../../testdata/qmux-golden-vectors.txt", import.meta.url);

async function goldenVectors() {
  const vectors = new Map();
  for (const line of (await readFile(goldenPath, "utf8")).split(/\r?\n/u)) {
    const text = line.trim();
    if (text === "" || text.startsWith("#")) {
      continue;
    }
    const [name, value] = text.split("=").map((part) => part.trim());
    vectors.set(name, Uint8Array.from(Buffer.from(value, "hex")));
  }
  return vectors;
}

function frames(write) {
  const writer = new FrameWriter();
  write(writer);
  return writer.take();
}

test("varints match RFC 9000", async () => {
  const vectors = await goldenVectors();
  for (const [name, value] of [
    ["varint.1byte", 37],
    ["varint.2byte", 15293],
    ["varint.4byte", 494878333],
    ["varint.8byte", 151288809941952652n],
  ]) {
    const bytes = new Uint8Array(8);
    const length = writeVarint(bytes, 0, value);
    assert.deepEqual(bytes.subarray(0, length), vectors.get(name), name);
    assert.equal(readVarint(vectors.get(name), 0).value, value, name);
  }
});

test("frames match the shared golden vectors", async () => {
  const vectors = await goldenVectors();
  const encoded = {
    "frame.stream.fin": frames((w) => writeStreamFrame(w, 0, 0, Buffer.from("hi"), true)),
    "frame.stream.offset": frames((w) => writeStreamFrame(w, 4, 16, Buffer.from("hi"), false)),
    "frame.reset_stream": frames((w) => writeResetStream(w, 0, 1, 5)),
    "frame.stop_sending": frames((w) => writeStopSending(w, 0, 1)),
    "frame.max_data": frames((w) => writeMaxData(w, 65)),
    "frame.max_stream_data": frames((w) => writeMaxStreamData(w, 4, 1028)),
    "frame.max_streams_bidi": frames((w) => w.varint(FrameType.MaxStreamsBidi).varint(65)),
    "frame.application_close": frames((w) => writeConnectionClose(w, { reason: "bye" })),
    "frame.transport_close": frames((w) =>
      writeConnectionClose(w, {
        protocolError: new QMuxProtocolError(TransportError.ProtocolViolation, "", 0x08),
      }),
    ),
    "frame.ping": frames((w) => writePing(w, FrameType.Ping, 1)),
    "frame.ping_response": frames((w) => writePing(w, FrameType.PingResponse, 1)),
    "message.stream_and_max_data": frames((w) => {
      writeStreamFrame(w, 0, 0, Buffer.from("hi"), true);
      writeMaxData(w, 65);
    }),
    "params.client": frames((w) =>
      writeTransportParameters(w, {
        initialMaxData: 4194308,
        initialMaxStreamDataBidiLocal: 4194308,
        maxRecordSize: 65536,
      }),
    ),
  };
  for (const [name, bytes] of Object.entries(encoded)) {
    assert.deepEqual(bytes, vectors.get(name), name);
    const parser = new FrameParser(bytes);
    while (parser.next() !== null) {
      // Every golden frame decodes.
    }
  }
});

test("server transport parameters decode", async () => {
  const vectors = await goldenVectors();
  const frame = new FrameParser(vectors.get("params.server")).next();
  assert.deepEqual(decodeTransportParameters(frame.params), {
    maxIdleTimeoutMs: 30000,
    initialMaxData: 16777216,
    initialMaxStreamDataBidiLocal: 0,
    initialMaxStreamDataBidiRemote: 4194308,
    initialMaxStreamDataUni: 0,
    initialMaxStreamsBidi: 65,
    initialMaxStreamsUni: 0,
    maxRecordSize: 65536,
  });
});

test("invalid frames and parameters are rejected", async () => {
  const vectors = await goldenVectors();
  const codes = {
    frame_encoding: TransportError.FrameEncodingError,
    transport_parameter: TransportError.TransportParameterError,
  };
  let tested = 0;
  for (const [name, bytes] of vectors) {
    if (!name.startsWith("invalid.")) {
      continue;
    }
    tested += 1;
    const code = codes[name.split(".")[1]];
    assert.throws(
      () => {
        const parser = new FrameParser(bytes);
        for (let frame = parser.next(); frame !== null; frame = parser.next()) {
          if (frame.type === FrameType.TransportParameters) {
            decodeTransportParameters(frame.params);
          }
        }
      },
      (error) => error instanceof QMuxProtocolError && error.code === code,
      name,
    );
  }
  assert.ok(tested > 0);
});

const ServerParameters = {
  maxIdleTimeoutMs: 0,
  initialMaxData: 1 << 20,
  initialMaxStreamDataBidiRemote: 256 << 10,
  initialMaxStreamsBidi: 4,
  maxRecordSize: 64 << 10,
};

/**
 * A WebSocket whose server side is a scripted QMux peer: it answers the client's transport
 * parameters, echoes every stream's data back with the FIN, and records the frames it receives.
 */
function fakeServer({ parameters = ServerParameters, protocol = QMuxProtocol, onFrame } = {}) {
  const sockets = [];
  class FakeWebSocket {
    binaryType = "blob";
    bufferedAmount = 0;
    readyState = 0;
    protocol = "";
    received = [];
    #echoed = new Map();

    constructor(url, protocols) {
      this.url = url;
      this.protocols = protocols;
      sockets.push(this);
      queueMicrotask(() => {
        this.readyState = 1;
        this.protocol = protocol;
        this.onopen?.();
      });
    }

    send(data) {
      const parser = new FrameParser(new Uint8Array(data));
      for (let frame = parser.next(); frame !== null; frame = parser.next()) {
        this.received.push(frame);
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

    /** Delivers frames from the server. */
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
  return { WebSocket: FakeWebSocket, sockets };
}

async function readAll(readable) {
  const chunks = [];
  for await (const chunk of readable) {
    chunks.push(...chunk);
  }
  return Uint8Array.from(chunks);
}

async function openSession(server) {
  const session = new QMuxWebSocketSession("wss://example.test/trevrpc", {
    WebSocket: server.WebSocket,
  });
  await session.ready;
  return session;
}

test("a session sends its transport parameters first and echoes streams", async () => {
  const server = fakeServer();
  const session = await openSession(server);
  const [socket] = server.sockets;
  assert.deepEqual(socket.protocols, [QMuxProtocol]);
  assert.equal(socket.binaryType, "arraybuffer");
  assert.equal(socket.received[0].type, FrameType.TransportParameters);

  const payload = Uint8Array.from({ length: 200_000 }, (_, i) => i % 251);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const written = writer.write(payload).then(() => writer.close());
  assert.deepEqual(await readAll(stream.readable), payload);
  await written;
  session.close();
  assert.deepEqual(await session.closed, { closeCode: 0, reason: "" });
});

test("writes wait for flow control credit", async () => {
  let limit = 1000;
  let received = 0;
  const server = fakeServer({
    parameters: { ...ServerParameters, initialMaxStreamDataBidiRemote: limit },
    onFrame(socket, frame) {
      if (frame.data === undefined) {
        return false;
      }
      assert.ok(frame.offset + frame.data.length <= limit, "data beyond the granted limit");
      received += frame.data.length;
      // The server reads everything at once and grants another window.
      limit = received + 1000;
      socket.deliver(frames((w) => writeMaxStreamData(w, frame.streamId, limit)));
      return true;
    },
  });
  const session = await openSession(server);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(new Uint8Array(10_000));
  await writer.close();
  assert.equal(received, 10_000);
  session.close();
});

test("cancelling the reader sends STOP_SENDING and aborting the writer sends RESET_STREAM", async () => {
  const server = fakeServer({ onFrame: (_socket, frame) => frame.data !== undefined });
  const session = await openSession(server);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(Uint8Array.of(1, 2, 3));
  await stream.readable.getReader().cancel(new Error("done"));
  await writer.abort(new Error("done"));
  await new Promise((resolve) => setTimeout(resolve, 0));
  const received = server.sockets[0].received;
  assert.ok(received.some((frame) => frame.type === FrameType.StopSending && frame.code === 1));
  assert.ok(
    received.some(
      (frame) => frame.type === FrameType.ResetStream && frame.code === 1 && frame.finalSize === 3,
    ),
  );
  session.close();
});

test("a peer reset errors the readable and STOP_SENDING errors the writable", async () => {
  const server = fakeServer({
    onFrame(socket, frame) {
      if (frame.data !== undefined) {
        socket.deliver(
          frames((w) => {
            writeResetStream(w, frame.streamId, 7, 0);
            writeStopSending(w, frame.streamId, 9);
          }),
        );
        return true;
      }
      return false;
    },
  });
  const session = await openSession(server);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(Uint8Array.of(1));
  await assert.rejects(readAll(stream.readable), (error) => error.streamErrorCode === 7);
  await assert.rejects(writer.closed, (error) => error.streamErrorCode === 9);
  const resets = server.sockets[0].received.filter((frame) => frame.type === FrameType.ResetStream);
  assert.deepEqual(
    resets.map((frame) => frame.code),
    [9],
    "STOP_SENDING is answered with a reset",
  );
  session.close();
});

test("a text message is a protocol error", async () => {
  const server = fakeServer();
  const session = await openSession(server);
  server.sockets[0].onmessage({ data: "hello" });
  await assert.rejects(session.closed, /protocol error/);
  const close = server.sockets[0].received.at(-1);
  assert.equal(close.type, FrameType.ConnectionClose);
  assert.equal(close.code, TransportError.ProtocolViolation);
});

test("pings are answered", async () => {
  const server = fakeServer();
  const session = await openSession(server);
  server.sockets[0].deliver(frames((w) => writePing(w, FrameType.Ping, 3)));
  await new Promise((resolve) => setTimeout(resolve, 0));
  const reply = server.sockets[0].received.at(-1);
  assert.equal(reply.type, FrameType.PingResponse);
  assert.equal(reply.sequence, 3);
  session.close();
});

test("a server that selects another subprotocol is refused", async () => {
  const server = fakeServer({ protocol: "chat" });
  const session = new QMuxWebSocketSession("wss://example.test/trevrpc", {
    WebSocket: server.WebSocket,
  });
  await assert.rejects(session.ready, /subprotocol "chat"/);
});

/** A WebTransport constructor whose sessions never become ready, or fail at once. */
function stalledWebTransport({ fail = false } = {}) {
  const sessions = [];
  return {
    sessions,
    WebTransport: class {
      constructor() {
        sessions.push(this);
        this.ready = fail ? Promise.reject(new Error("UDP blocked")) : new Promise(() => {});
        this.closed = new Promise((resolve) => {
          this.close = () => resolve();
        });
        this.ready.catch(() => {});
      }
    },
  };
}

test("the channel falls back to WebSocket when WebTransport does not connect in time", async () => {
  const server = fakeServer();
  const { WebTransport, sessions } = stalledWebTransport();
  const states = [];
  const channel = await connect("https://example.test/trevrpc", {
    WebTransport,
    WebSocket: server.WebSocket,
    fallbackDelayMs: 20,
    onStateChange: (change) => states.push(change),
  });
  assert.equal(channel.transport, "websocket");
  assert.equal(server.sockets[0].url, "wss://example.test/trevrpc");
  assert.equal(sessions.length, 1);
  const stream = await channel.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(Uint8Array.of(42));
  await writer.close();
  assert.deepEqual(await readAll(stream.readable), Uint8Array.of(42));
  channel.close();
});

test("the channel falls back at once when WebTransport fails or is missing", async () => {
  for (const WebTransport of [stalledWebTransport({ fail: true }).WebTransport, undefined]) {
    const server = fakeServer();
    const started = Date.now();
    const channel = await connect("https://example.test/trevrpc", {
      WebTransport,
      WebSocket: server.WebSocket,
      fallbackDelayMs: 10_000,
    });
    assert.equal(channel.transport, "websocket");
    assert.ok(Date.now() - started < 1000);
    channel.close();
  }
});

test("the channel uses WebTransport when it connects in time", async () => {
  const server = fakeServer();
  class ReadyWebTransport {
    ready = Promise.resolve();
    closed = new Promise(() => {});
    close() {}
  }
  const channel = await connect("https://example.test/trevrpc", {
    WebTransport: ReadyWebTransport,
    WebSocket: server.WebSocket,
  });
  assert.equal(channel.transport, "webtransport");
  assert.equal(server.sockets.length, 0, "the fallback never starts");
  channel.close();
});

test("an explicit WebSocket URL and transport list are honoured", async () => {
  const server = fakeServer();
  const channel = await connect("https://example.test/trevrpc", {
    transports: ["websocket"],
    webSocketUrl: "wss://fallback.example.test/rpc",
    WebSocket: server.WebSocket,
  });
  assert.equal(channel.transport, "websocket");
  assert.equal(server.sockets[0].url, "wss://fallback.example.test/rpc");
  channel.close();
  await assert.rejects(
    connect("https://example.test/trevrpc", { transports: ["carrier-pigeon"] }),
    TypeError,
  );
});

test("both transports failing reports both", async () => {
  class RefusingWebSocket {
    constructor() {
      queueMicrotask(() => this.onclose?.({ code: 1006 }));
    }
    close() {}
  }
  await assert.rejects(
    connect("https://example.test/trevrpc", {
      WebTransport: stalledWebTransport({ fail: true }).WebTransport,
      WebSocket: RefusingWebSocket,
    }),
    (error) =>
      error.code === Code.Unavailable &&
      /webtransport: .*UDP blocked/.test(error.statusMessage) &&
      /websocket: .*1006/.test(error.statusMessage),
  );
});
