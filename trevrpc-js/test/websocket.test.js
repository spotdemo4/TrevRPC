// The channel's QMux over WebSocket fallback, against a server session of @trevrpc/qmux over
// in-memory WebSockets. The QMux protocol itself is tested in the qmux/ package.
import assert from "node:assert/strict";
import test from "node:test";

import { QMuxSession } from "@trevrpc/qmux";

import { connect } from "../src/channel.js";
import { Code } from "../src/status.js";

const QMuxProtocol = "qmux-02.trevrpc-1";

async function readAll(readable) {
  const chunks = [];
  for await (const chunk of readable) {
    chunks.push(...chunk);
  }
  return Uint8Array.from(chunks);
}

/**
 * A WebSocket constructor for the channel whose server end runs a QMux server session that echoes
 * every stream. `opens: false` leaves the sockets opening, `handshake: false` opens them with no
 * session behind them, and `protocol` is the subprotocol the server selects.
 */
function qmuxServer({ opens = true, handshake = true, protocol = QMuxProtocol } = {}) {
  const sockets = [];
  class MemoryWebSocket {
    binaryType = "blob";
    bufferedAmount = 0;
    readyState = 0;
    protocol = "";
    peer = null;
    sent = [];

    send(data) {
      const bytes = Uint8Array.from(data);
      this.sent.push(bytes);
      const { peer } = this;
      queueMicrotask(() => {
        if (peer.readyState === 1) {
          peer.onmessage?.({ data: bytes.buffer });
        }
      });
    }

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
  }
  class ChannelWebSocket extends MemoryWebSocket {
    constructor(url, protocols) {
      super();
      this.url = url;
      this.protocols = protocols;
      sockets.push(this);
      const server = new MemoryWebSocket();
      this.peer = server;
      server.peer = this;
      if (!opens) {
        return;
      }
      for (const end of [this, server]) {
        end.readyState = 1;
        end.protocol = protocol;
      }
      if (handshake) {
        echo(new QMuxSession(server, { role: "server" }));
      }
      queueMicrotask(() => this.onopen?.());
    }
  }
  return { WebSocket: ChannelWebSocket, sockets };
}

function echo(session) {
  void (async () => {
    for await (const stream of session.incomingBidirectionalStreams) {
      stream.readable.pipeTo(stream.writable).catch(() => {});
    }
  })().catch(() => {});
}

test("the channel's QMux session offers TrevRPC's subprotocol and transport parameters", async () => {
  const server = qmuxServer();
  const channel = await connect("https://example.test/trevrpc", {
    transports: ["websocket"],
    WebSocket: server.WebSocket,
  });
  const [socket] = server.sockets;
  assert.deepEqual(socket.protocols, [QMuxProtocol]);
  assert.equal(socket.binaryType, "arraybuffer");
  // The first record holds the transport parameters alone: initial_max_data and
  // initial_max_stream_data_bidi_local of 4194320 (a 4 MiB frame and its header) and a
  // max_record_size of 65536. The server may open no streams and send no datagrams.
  assert.equal(
    Buffer.from(socket.sent[0]).toString("hex"),
    "ff5153300d0a0d0a19040480400010050480400010c571c59429cd08450480010000",
  );
  channel.close();
});

test("a server that selects another subprotocol is refused", async () => {
  const server = qmuxServer({ protocol: "chat" });
  await assert.rejects(
    connect("https://example.test/trevrpc", {
      transports: ["websocket"],
      WebSocket: server.WebSocket,
    }),
    (error) => error.code === Code.Unavailable && /subprotocol "chat"/.test(error.statusMessage),
  );
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
  const server = qmuxServer();
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
    const server = qmuxServer();
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

test("the channel falls back to WebSocket when WebTransport selects another protocol", async () => {
  const server = qmuxServer();
  const closed = [];
  class MismatchedWebTransport {
    ready = Promise.resolve();
    closed = new Promise(() => {});
    protocol = "trevrpc-2";
    close() {
      closed.push(this);
    }
  }
  const channel = await connect("https://example.test/trevrpc", {
    WebTransport: MismatchedWebTransport,
    WebSocket: server.WebSocket,
    fallbackDelayMs: 10_000,
  });
  assert.equal(channel.transport, "websocket");
  assert.equal(closed.length, 1, "the mismatched WebTransport session is closed");
  channel.close();
});

test("the channel uses WebTransport when it connects in time", async () => {
  const server = qmuxServer();
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
  const server = qmuxServer();
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

test("a cancelled connection says how far the WebSocket session got", async () => {
  const stalls = {
    "the WebSocket was still opening": qmuxServer({ opens: false }),
    "the server had not sent its QMux transport parameters": qmuxServer({ handshake: false }),
  };
  for (const [progress, server] of Object.entries(stalls)) {
    const controller = new AbortController();
    const connecting = connect("https://example.test/trevrpc", {
      transports: ["websocket"],
      WebSocket: server.WebSocket,
      signal: controller.signal,
    });
    await new Promise((resolve) => setTimeout(resolve, 5));
    controller.abort(new Error("gave up"));
    await assert.rejects(connecting, (error) => {
      assert.equal(error.code, Code.Cancelled, progress);
      assert.match(error.statusMessage, /^connection cancelled after \d+ ms: /, progress);
      assert.ok(error.statusMessage.endsWith(progress), error.statusMessage);
      assert.equal(error.cause.message, "gave up", progress);
      return true;
    });
    assert.equal(server.sockets[0].readyState, 3, `${progress}: the WebSocket is closed`);
  }
});
