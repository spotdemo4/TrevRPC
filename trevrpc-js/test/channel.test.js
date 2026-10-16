import assert from "node:assert/strict";
import test from "node:test";

import { EchoClient } from "../build/interop/gen/interop.trevrpc.js";
import { Channel, connect } from "../src/channel.js";
import { Code } from "../src/status.js";
import { echoHandler } from "./echo-server.js";
import { deferred, fakeTransport, isPending, turn } from "./support.js";

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
    WebTransport,
    serverCertificateHashes: hashes,
    allowPooling: true,
    signal: undefined,
    onStateChange() {},
  });
  assert.equal(await isPending(connecting), true);

  assert.equal(sessions.length, 1);
  assert.equal(sessions[0].url, "https://example.test/trevrpc");
  assert.deepEqual(sessions[0].options, { serverCertificateHashes: hashes, allowPooling: true });
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

test("connect fails when the session cannot be established", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", { WebTransport });
  sessions[0].failToConnect(new Error("handshake failed"));
  await assert.rejects(connecting, (error) => {
    return error.code === Code.Unavailable && /handshake failed/.test(error.statusMessage);
  });
  assert.equal(sessions[0].closeCalls.length, 1);
});

test("connect fails when the session closes before it is ready", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", { WebTransport });
  sessions[0].lose({ closeCode: 1 });
  await assert.rejects(connecting, (error) => error.code === Code.Unavailable);
});

test("connect fails without a WebTransport implementation", async () => {
  await assert.rejects(
    connect("https://example.test/trevrpc", { WebTransport: undefined }),
    (error) => error.code === Code.Unavailable && /not available/.test(error.statusMessage),
  );
});

test("aborting the signal abandons the initial connection", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const controller = new AbortController();
  const connecting = connect("https://example.test/trevrpc", {
    WebTransport,
    signal: controller.signal,
  });
  controller.abort(new Error("gave up"));

  await assert.rejects(connecting, (error) => {
    return error.code === Code.Cancelled && error.cause.message === "gave up";
  });
  assert.ok(sessions[0].closeCalls.length >= 1, "the abandoned session is closed");

  await assert.rejects(
    connect("https://example.test/trevrpc", { WebTransport, signal: AbortSignal.abort() }),
    (error) => error.code === Code.Cancelled,
  );
  assert.equal(sessions.length, 1, "an aborted signal must not start a session");
});

test("the channel reconnects after its session is lost", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const changes = [];
  const connecting = connect("https://example.test/trevrpc", {
    WebTransport,
    onStateChange: (change) => changes.push(change),
  });
  sessions[0].becomeReady();
  const channel = await connecting;

  sessions[0].lose({ closeCode: 1, reason: "restart" });
  await waitFor(() => channel.state === "reconnecting");
  assert.equal(changes[0].state, "reconnecting");
  assert.equal(changes[0].previousState, "ready");
  assert.equal(changes[0].attempt, 1);
  assert.equal(changes[0].error.reason, "restart");
  assert.ok(changes[0].delayMs >= 80 && changes[0].delayMs <= 120);

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
  const connecting = connect("https://example.test/trevrpc", { WebTransport });
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
  const connecting = connect("https://example.test/trevrpc", { WebTransport });
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
