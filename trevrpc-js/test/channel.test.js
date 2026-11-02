import assert from "node:assert/strict";
import test from "node:test";

import { EchoClient } from "../build/interop/gen/interop.trevrpc.js";
import { Channel, connect, createChannel } from "../src/channel.js";
import { Code } from "../src/status.js";
import { echoHandler } from "./echo-server.js";
import { deferred, fakeTransport, isPending, turn } from "./support.js";

// These tests cover the WebTransport lifecycle; the WebSocket fallback has its own tests.
const WebTransportOnly = ["webtransport"];

/** A WebTransport constructor whose sessions the test controls. */
function fakeWebTransport() {
  const sessions = [];
  class FakeWebTransport {
    constructor(url, options) {
      this.url = url;
      this.options = options;
      this.ready = new Promise((resolve, reject) => {
        this.becomeReady = resolve;
        this.failToConnect = reject;
      });
      this.closed = new Promise((resolve, reject) => {
        this.lose = resolve;
        this.crash = reject;
      });
      this.closed.catch(() => {});
      this.closeCalls = [];
      this.streams = fakeTransport(echoHandler);
      sessions.push(this);
    }

    close(closeInfo) {
      this.closeCalls.push(closeInfo);
      this.lose(closeInfo);
      this.failToConnect(new Error("closed"));
    }

    createBidirectionalStream() {
      return this.streams.createBidirectionalStream();
    }
  }
  return { WebTransport: FakeWebTransport, sessions };
}

async function waitFor(condition) {
  for (let attempt = 0; attempt < 500 && !condition(); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.ok(condition(), "timed out waiting for condition");
}

test("connect resolves with a channel once the session is ready", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const hashes = [{ algorithm: "sha-256", value: new Uint8Array(32) }];
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    serverCertificateHashes: hashes,
    allowPooling: true,
    signal: undefined,
    onStateChange() {},
  });
  assert.equal(await isPending(connecting), true);

  assert.equal(sessions.length, 1);
  assert.equal(sessions[0].url, "https://example.test/trevrpc");
  assert.deepEqual(sessions[0].options, {
    protocols: ["trevrpc-1"],
    serverCertificateHashes: hashes,
    allowPooling: true,
  });
  sessions[0].becomeReady();

  const channel = await connecting;
  assert.ok(channel instanceof Channel);
  assert.equal(channel.state, "ready");
  await channel.waitUntilReady();

  const echo = new EchoClient(channel);
  assert.equal((await echo.unary({ message: "hi" })).message, "hi");
  channel.close({ closeCode: 7, reason: "bye" });
  assert.deepEqual(sessions[0].closeCalls, [{ closeCode: 7, reason: "bye" }]);
  assert.equal(channel.state, "closed");
});

test("the protocols option is the protocol version and cannot be overridden", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    protocols: ["trevrpc-99"],
  });
  assert.deepEqual(sessions[0].options, { protocols: ["trevrpc-1"] });
  sessions[0].becomeReady();
  (await connecting).close();
});

test("connect accepts the selected protocol trevrpc-1 and an empty or missing selection", async () => {
  for (const protocol of ["trevrpc-1", "", undefined]) {
    const { WebTransport, sessions } = fakeWebTransport();
    const connecting = connect("https://example.test/trevrpc", {
      transports: WebTransportOnly,
      WebTransport,
    });
    sessions[0].protocol = protocol;
    sessions[0].becomeReady();
    const channel = await connecting;
    assert.equal(channel.state, "ready", JSON.stringify(protocol));
    assert.equal(sessions[0].closeCalls.length, 0, JSON.stringify(protocol));
    channel.close();
  }
});

test("connect fails and closes the session when the server selects another protocol", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  sessions[0].protocol = "trevrpc-2";
  sessions[0].becomeReady();
  await assert.rejects(connecting, (error) => {
    return (
      error.code === Code.FailedPrecondition &&
      /"trevrpc-2"/.test(error.statusMessage) &&
      /"trevrpc-1"/.test(error.statusMessage)
    );
  });
  assert.equal(sessions[0].closeCalls.length, 1, "the mismatched session is closed");
});

test("the channel keeps reconnecting when a reconnect selects another protocol", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  sessions[0].becomeReady();
  const channel = await connecting;

  sessions[0].lose();
  await waitFor(() => sessions.length === 2);
  sessions[1].protocol = "trevrpc-2";
  sessions[1].becomeReady();
  await waitFor(() => sessions.length === 3);
  assert.equal(channel.state, "reconnecting");
  assert.equal(sessions[1].closeCalls.length, 1);
  const mismatch = changes.filter((change) => change.state === "reconnecting").at(-1);
  assert.equal(mismatch.error.code, Code.FailedPrecondition);

  sessions[2].protocol = "trevrpc-1";
  sessions[2].becomeReady();
  await channel.waitUntilReady();
  assert.equal(channel.state, "ready");
  channel.close();
});

test("connect fails when the session cannot be established", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  sessions[0].failToConnect(new Error("handshake failed"));
  await assert.rejects(connecting, (error) => {
    return error.code === Code.Unavailable && /handshake failed/.test(error.statusMessage);
  });
  assert.equal(sessions[0].closeCalls.length, 1);
});

test("connect fails when the session closes before it is ready", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  sessions[0].lose({ closeCode: 1 });
  await assert.rejects(connecting, (error) => error.code === Code.Unavailable);
});

test("connect fails without a WebTransport implementation", async () => {
  await assert.rejects(
    connect("https://example.test/trevrpc", {
      transports: WebTransportOnly,
      WebTransport: undefined,
    }),
    (error) => error.code === Code.Unavailable && /not available/.test(error.statusMessage),
  );
});

test("aborting the signal abandons the initial connection", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const controller = new AbortController();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    signal: controller.signal,
  });
  controller.abort(new Error("gave up"));

  await assert.rejects(connecting, (error) => {
    return error.code === Code.Cancelled && error.cause.message === "gave up";
  });
  assert.ok(sessions[0].closeCalls.length >= 1, "the abandoned session is closed");

  await assert.rejects(
    connect("https://example.test/trevrpc", {
      transports: WebTransportOnly,
      WebTransport,
      signal: AbortSignal.abort(),
    }),
    (error) => error.code === Code.Cancelled,
  );
  assert.equal(sessions.length, 1, "an aborted signal must not start a session");
});

test("the channel reconnects after its session is lost", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  sessions[0].becomeReady();
  const channel = await connecting;
  assert.deepEqual(changes, [
    { state: "ready", previousState: "connecting", transport: "webtransport" },
  ]);

  sessions[0].lose({ closeCode: 1, reason: "restart" });
  await waitFor(() => channel.state === "reconnecting");
  assert.equal(changes[1].state, "reconnecting");
  assert.equal(changes[1].previousState, "ready");
  assert.equal(changes[1].attempt, 1);
  assert.equal(changes[1].error.reason, "restart");
  assert.ok(changes[1].delayMs >= 80 && changes[1].delayMs <= 120);

  // Calls fail fast while there is no session, and are not queued.
  await assert.rejects(new EchoClient(channel).unary({}), (error) => {
    return error.code === Code.Unavailable && /reconnecting/.test(error.statusMessage);
  });
  const ready = channel.waitUntilReady();

  await waitFor(() => sessions.length === 2);
  assert.equal(channel.state, "reconnecting");
  sessions[1].becomeReady();
  await ready;

  assert.equal(channel.state, "ready");
  assert.deepEqual(changes.at(-1).state, "ready");
  assert.equal((await new EchoClient(channel).unary({ message: "back" })).message, "back");
  channel.close();
});

test("the channel keeps retrying with backoff while reconnecting fails", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  sessions[0].becomeReady();
  const channel = await connecting;

  sessions[0].crash(new Error("network down"));
  await waitFor(() => sessions.length === 2);
  sessions[1].failToConnect(new Error("still down"));
  await waitFor(() => sessions.length === 3);

  const attempts = changes.filter((change) => change.state === "reconnecting");
  assert.deepEqual(
    attempts.map((change) => change.attempt),
    [1, 2],
  );
  assert.match(attempts[0].error.message, /network down/);
  assert.match(attempts[1].error.statusMessage, /still down/);
  assert.ok(attempts[1].delayMs > attempts[0].delayMs, "backoff grows");
  channel.close();
});

test("closing the channel stops reconnecting and rejects waiters", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  sessions[0].becomeReady();
  const channel = await connecting;

  sessions[0].lose();
  await waitFor(() => channel.state === "reconnecting");
  const waiting = channel.waitUntilReady();
  channel.close();
  channel.close();

  await assert.rejects(waiting, (error) => error.code === Code.Unavailable);
  await assert.rejects(channel.waitUntilReady(), (error) => error.code === Code.Unavailable);
  await assert.rejects(channel.createBidirectionalStream(), (error) => {
    return error.code === Code.Unavailable && /closed/.test(error.statusMessage);
  });
  await turn(5);
  assert.equal(sessions.length, 1, "no reconnect after close");
});

test("closing during a reconnect attempt abandons the new session", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  sessions[0].becomeReady();
  const channel = await connecting;

  sessions[0].lose();
  await waitFor(() => sessions.length === 2);
  channel.close();

  assert.equal(sessions[1].closeCalls.length, 1);
  assert.equal(channel.state, "closed");
});

test("a failing state callback does not break the channel", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange() {
      throw new Error("observer bug");
    },
  });
  sessions[0].becomeReady();
  const channel = await connecting;
  const reported = deferred();
  globalThis.reportError = reported.resolve;
  try {
    sessions[0].lose();
    assert.equal((await reported.promise).message, "observer bug");
    await waitFor(() => channel.state === "reconnecting");
  } finally {
    delete globalThis.reportError;
    channel.close();
  }
});

test("connect closes the channel when the first attempt fails, without reconnecting", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  sessions[0].failToConnect(new Error("handshake failed"));
  await assert.rejects(connecting, (error) => error.code === Code.Unavailable);
  assert.deepEqual(
    changes.map((change) => [change.previousState, change.state]),
    [["connecting", "closed"]],
  );
  await turn(5);
  assert.equal(sessions.length, 1, "no reconnect after a failed connect");
});

test("createChannel returns at once, and calls wait for the first session", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  assert.ok(channel instanceof Channel);
  assert.equal(channel.state, "connecting");
  assert.equal(channel.transport, undefined);
  assert.equal(sessions.length, 1, "the first attempt starts right away");

  const client = new EchoClient(channel);
  const first = client.unary({ message: "early" });
  const ready = channel.waitUntilReady();
  assert.equal(await isPending(first), true);
  assert.equal(await isPending(ready), true);

  sessions[0].becomeReady();
  assert.equal((await first).message, "early");
  await ready;
  assert.equal(channel.state, "ready");
  assert.equal(channel.transport, "webtransport");
  assert.deepEqual(changes, [
    { state: "ready", previousState: "connecting", transport: "webtransport" },
  ]);
  channel.close();
});

test("calls waiting for the first session fail with its error, and the channel reconnects", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  const client = new EchoClient(channel);
  const waiting = client.unary({ message: "early" });
  const ready = channel.waitUntilReady();

  sessions[0].failToConnect(new Error("handshake failed"));
  await assert.rejects(waiting, (error) => {
    return error.code === Code.Unavailable && /handshake failed/.test(error.statusMessage);
  });
  assert.equal(channel.state, "reconnecting");
  assert.equal(changes[0].previousState, "connecting");
  assert.equal(changes[0].attempt, 1);
  assert.match(changes[0].error.statusMessage, /handshake failed/);

  // Once the first attempt failed, calls fail fast like during any reconnect.
  await assert.rejects(client.unary({}), (error) => {
    return error.code === Code.Unavailable && /reconnecting/.test(error.statusMessage);
  });
  assert.equal(await isPending(ready), true, "waitUntilReady waits across attempts");

  await waitFor(() => sessions.length === 2);
  sessions[1].becomeReady();
  await ready;
  assert.equal((await client.unary({ message: "later" })).message, "later");
  channel.close();
});

test("a call waiting for the first session ends at its deadline or signal", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  const client = new EchoClient(channel);
  await assert.rejects(
    client.unary({}, { timeoutMs: 10 }),
    (error) => error.code === Code.DeadlineExceeded,
  );
  const controller = new AbortController();
  const aborted = client.unary({}, { signal: controller.signal });
  controller.abort(new Error("gave up"));
  await assert.rejects(aborted, (error) => error.code === Code.Cancelled);
  assert.equal(channel.state, "connecting");

  sessions[0].becomeReady();
  await channel.waitUntilReady();
  assert.equal((await client.unary({ message: "after" })).message, "after");
  channel.close();
});

test("closing a connecting channel abandons the attempt and fails waiting calls", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
  });
  const waiting = new EchoClient(channel).unary({});
  const ready = channel.waitUntilReady();
  channel.close();

  await assert.rejects(waiting, (error) => {
    return error.code === Code.Unavailable && /closed/.test(error.statusMessage);
  });
  await assert.rejects(ready, (error) => error.code === Code.Unavailable);
  assert.equal(channel.state, "closed");
  assert.ok(sessions[0].closeCalls.length >= 1, "the pending session is closed");
  await turn(5);
  assert.equal(sessions.length, 1, "no reconnect after close");
});

test("a channel's interceptors run around the calls of every client on it", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const events = [];
  const intercept = (name) => (call) => {
    events.push(`${name} ${call.method}`);
    call.metadata["interop-suffix"] = `+${name}`;
    return ({ code }) => events.push(`${name} finished ${code}`);
  };
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    interceptors: [intercept("channel")],
  });
  assert.equal(channel.interceptors.length, 1);
  assert.ok(Object.isFrozen(channel.interceptors));
  await waitFor(() => sessions.length === 1);
  sessions[0].becomeReady();

  const plain = new EchoClient(channel);
  const intercepted = new EchoClient(channel, { interceptors: [intercept("client")] });
  assert.equal((await plain.unary({ message: "a" })).message, "a+channel");
  assert.equal((await intercepted.unary({ message: "b" })).message, "b+client");
  assert.deepEqual(events, [
    "channel /trevrpc.interop.v1.Echo/Unary",
    `channel finished ${Code.Ok}`,
    "channel /trevrpc.interop.v1.Echo/Unary",
    "client /trevrpc.interop.v1.Echo/Unary",
    `client finished ${Code.Ok}`,
    `channel finished ${Code.Ok}`,
  ]);
  channel.close();
});

test("createChannel throws on invalid options without starting a session", () => {
  const { WebTransport, sessions } = fakeWebTransport();
  assert.throws(
    () =>
      createChannel("https://example.test/trevrpc", {
        transports: WebTransportOnly,
        WebTransport,
        connectionMetadata: { "trevrpc-x": "1" },
      }),
    (error) => error.code === Code.InvalidArgument,
  );
  assert.throws(() => createChannel("https://example.test/trevrpc", { transports: [] }), TypeError);
  assert.throws(
    () =>
      createChannel("https://example.test/trevrpc", {
        transports: WebTransportOnly,
        WebTransport,
        interceptors: [null],
      }),
    (error) => error.code === Code.InvalidArgument,
  );
  assert.equal(sessions.length, 0);
});
