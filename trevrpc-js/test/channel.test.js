import assert from "node:assert/strict";
import test from "node:test";

import { Echo } from "../build/interop/gen/interop_pb.js";
import { Channel, connect, createChannel } from "../src/channel.js";
import { createClient } from "../src/client.js";
import { Code } from "../src/status.js";
import { createCompression } from "../src/zstd.js";
import { echoHandler } from "./echo-server.js";
import { deferred, fakeTransport, isPending, turn } from "./support.js";

// These tests cover the WebTransport lifecycle; the WebSocket fallback has its own tests.
const WebTransportOnly = ["webtransport"];

/** A WebTransport constructor whose sessions the test controls, serving streams with `handler`. */
function fakeWebTransport(handler = echoHandler) {
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
      this.streams = fakeTransport(handler);
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

  const echo = createClient(Echo, channel);
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

test("each attempt fetches serverCertificateHashesUrl and adds its hashes", async (t) => {
  const published = [["11"], ["22"]];
  const fetch = t.mock.method(globalThis, "fetch", async () =>
    Response.json(
      published.shift().map((byte) => ({ algorithm: "sha-256", value: byte.repeat(32) })),
    ),
  );
  const { WebTransport, sessions } = fakeWebTransport();
  const fixed = { algorithm: "sha-256", value: new Uint8Array(32) };
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    serverCertificateHashes: [fixed],
    serverCertificateHashesUrl: "https://app.example.test/certificate-hashes",
  });
  await waitFor(() => sessions.length === 1);
  assert.equal(fetch.mock.calls[0].arguments[0], "https://app.example.test/certificate-hashes");
  assert.deepEqual(sessions[0].options.serverCertificateHashes, [
    fixed,
    { algorithm: "sha-256", value: new Uint8Array(32).fill(0x11) },
  ]);
  sessions[0].becomeReady();
  const channel = await connecting;

  // The certificate rotated: the reconnect fetches the new hashes.
  sessions[0].lose();
  await waitFor(() => sessions.length === 2);
  assert.deepEqual(sessions[1].options.serverCertificateHashes, [
    fixed,
    { algorithm: "sha-256", value: new Uint8Array(32).fill(0x22) },
  ]);
  channel.close();
});

test("a 404 from serverCertificateHashesUrl adds no hashes, and a failed fetch fails the attempt", async (t) => {
  let status = 404;
  t.mock.method(globalThis, "fetch", async () => new Response("", { status }));
  const { WebTransport, sessions } = fakeWebTransport();
  const options = {
    transports: WebTransportOnly,
    WebTransport,
    serverCertificateHashesUrl: "/certificate-hashes",
  };
  const connecting = connect("https://example.test/trevrpc", options);
  await waitFor(() => sessions.length === 1);
  assert.deepEqual(sessions[0].options, { protocols: ["trevrpc-1"] });
  sessions[0].becomeReady();
  (await connecting).close();

  status = 500;
  await assert.rejects(connect("https://example.test/trevrpc", options), (error) => {
    return error.code === Code.Unavailable && /HTTP 500/.test(error.statusMessage);
  });
  assert.equal(sessions.length, 1, "no session is opened without the hashes");
});

test("fetchServerCertificateHashes derives the hashes URL from the channel URL", async (t) => {
  const fetch = t.mock.method(globalThis, "fetch", async () =>
    Response.json([{ algorithm: "sha-256", value: "33".repeat(32) }]),
  );
  const { WebTransport, sessions } = fakeWebTransport();
  const requested = [];
  for (const [url, expected] of [
    ["https://localhost:4433/trevrpc", "https://localhost:4433/trevrpc/certificate-hashes"],
    ["https://localhost:4433/trevrpc/", "https://localhost:4433/trevrpc/certificate-hashes"],
    ["https://localhost:4433", "https://localhost:4433/certificate-hashes"],
    ["https://localhost:4433/", "https://localhost:4433/certificate-hashes"],
    ["https://localhost:4433/a/b?token=1#x", "https://localhost:4433/a/b/certificate-hashes"],
    [new URL("https://[::1]:4433/trevrpc"), "https://[::1]:4433/trevrpc/certificate-hashes"],
  ]) {
    const connecting = connect(url, {
      transports: WebTransportOnly,
      WebTransport,
      fetchServerCertificateHashes: true,
    });
    requested.push(expected);
    await waitFor(() => sessions.length === requested.length);
    assert.equal(fetch.mock.calls.at(-1).arguments[0], expected);
    assert.deepEqual(sessions.at(-1).options.serverCertificateHashes, [
      { algorithm: "sha-256", value: new Uint8Array(32).fill(0x33) },
    ]);
    sessions.at(-1).becomeReady();
    (await connecting).close();
  }
});

test("fetchServerCertificateHashes is off by default and excludes serverCertificateHashesUrl", async (t) => {
  const fetch = t.mock.method(globalThis, "fetch", async () => new Response("", { status: 404 }));
  const { WebTransport, sessions } = fakeWebTransport();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    fetchServerCertificateHashes: false,
  });
  await waitFor(() => sessions.length === 1);
  assert.equal(fetch.mock.callCount(), 0);
  sessions[0].becomeReady();
  (await connecting).close();

  await assert.rejects(
    connect("https://example.test/trevrpc", {
      transports: WebTransportOnly,
      WebTransport,
      fetchServerCertificateHashes: true,
      serverCertificateHashesUrl: "/certificate-hashes",
    }),
    (error) => error instanceof TypeError && /mutually exclusive/.test(error.message),
  );
  assert.equal(sessions.length, 1);

  // Turned off explicitly, it leaves a custom URL in effect.
  const custom = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    fetchServerCertificateHashes: false,
    serverCertificateHashesUrl: "https://example.test/hashes",
  });
  await waitFor(() => sessions.length === 2);
  assert.equal(fetch.mock.calls.at(-1).arguments[0], "https://example.test/hashes");
  sessions[1].becomeReady();
  (await custom).close();
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
    return (
      error.code === Code.Cancelled &&
      error.cause.message === "gave up" &&
      /^connection cancelled after \d+ ms: the WebTransport session was not ready$/.test(
        error.statusMessage,
      )
    );
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
  await assert.rejects(createClient(Echo, channel).unary({}), (error) => {
    return error.code === Code.Unavailable && /reconnecting/.test(error.statusMessage);
  });
  const ready = channel.waitUntilReady();

  await waitFor(() => sessions.length === 2);
  assert.equal(channel.state, "reconnecting");
  sessions[1].becomeReady();
  await ready;

  assert.equal(channel.state, "ready");
  assert.deepEqual(changes.at(-1).state, "ready");
  assert.equal((await createClient(Echo, channel).unary({ message: "back" })).message, "back");
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

  const client = createClient(Echo, channel);
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
  const client = createClient(Echo, channel);
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
  const client = createClient(Echo, channel);
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
  const waiting = createClient(Echo, channel).unary({});
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

  const plain = createClient(Echo, channel);
  const intercepted = createClient(Echo, channel, { interceptors: [intercept("client")] });
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

/** The echo handler, recording the REQUEST of every call in `requests`. */
function recordingEcho(requests) {
  return (stream, session) => {
    const readRequest = stream.readRequest.bind(stream);
    stream.readRequest = async () => {
      const request = await readRequest();
      requests.push(request);
      return request;
    };
    return echoHandler(stream, session);
  };
}

/** Compression settings without dictionaries: every call queries the server about ID 0 alone. */
const compression = await createCompression();

test("calls started before a pending compression promise resolves are compressed", async () => {
  const requests = [];
  const { WebTransport, sessions } = fakeWebTransport(recordingEcho(requests));
  const settings = deferred();
  const changes = [];
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: settings.promise,
    onStateChange: (change) => changes.push(change),
  });
  assert.equal(channel.compression, undefined);
  assert.equal(sessions.length, 1, "the first attempt does not wait for the compression settings");

  const client = createClient(Echo, channel);
  const first = client.unary({ message: "early" });
  const ready = channel.waitUntilReady();
  sessions[0].becomeReady();
  assert.equal(await isPending(first), true, "calls wait for the compression settings");
  assert.equal(await isPending(ready), true);
  assert.equal(channel.state, "connecting");

  settings.resolve(compression);
  assert.equal((await first).message, "early");
  await ready;
  assert.equal(channel.compression, compression);
  assert.equal(requests[0].dictionaryQuery.length, 1, "the early call negotiated compression");
  assert.deepEqual(changes, [
    { state: "ready", previousState: "connecting", transport: "webtransport" },
  ]);

  assert.equal((await client.unary({ message: "later" })).message, "later");
  assert.equal(requests[1].dictionaryQuery.length, 0, "the session's answer is cached");
  channel.close();
});

test("compression settings that resolve before the session do not delay it", async () => {
  const requests = [];
  const { WebTransport, sessions } = fakeWebTransport(recordingEcho(requests));
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: Promise.resolve(compression),
  });
  const first = createClient(Echo, channel).unary({ message: "early" });
  await turn();
  assert.equal(channel.compression, compression);
  sessions[0].becomeReady();
  assert.equal((await first).message, "early");
  assert.equal(requests[0].dictionaryQuery.length, 1);
  channel.close();
});

test("a rejected compression promise leaves the channel uncompressed and reports the error", async () => {
  const requests = [];
  const { WebTransport, sessions } = fakeWebTransport(recordingEcho(requests));
  const settings = deferred();
  const changes = [];
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: settings.promise,
    onStateChange: (change) => changes.push(change),
  });
  const first = createClient(Echo, channel).unary({ message: "early" });
  sessions[0].becomeReady();
  const failure = new Error("wasm blocked");
  settings.reject(failure);

  assert.equal((await first).message, "early");
  assert.equal(channel.compression, undefined);
  assert.equal(requests[0].dictionaryQuery.length, 0, "the call travels uncompressed");
  assert.deepEqual(changes, [
    {
      state: "ready",
      previousState: "connecting",
      transport: "webtransport",
      compressionError: failure,
    },
  ]);

  // Only the first ready transition after the rejection reports it.
  sessions[0].lose();
  await waitFor(() => sessions.length === 2);
  sessions[1].becomeReady();
  await waitFor(() => channel.state === "ready");
  assert.equal("compressionError" in changes.at(-1), false);
  channel.close();
});

test("a rejected compression promise goes to reportError without onStateChange", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const failure = new Error("wasm blocked");
  const reported = deferred();
  globalThis.reportError = reported.resolve;
  try {
    const connecting = connect("https://example.test/trevrpc", {
      transports: WebTransportOnly,
      WebTransport,
      compression: Promise.reject(failure),
    });
    sessions[0].becomeReady();
    const channel = await connecting;
    assert.equal(await reported.promise, failure);
    assert.equal(channel.compression, undefined);
    channel.close();
  } finally {
    delete globalThis.reportError;
  }
});

test("connect resolves once both the session and a compression promise are ready", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const settings = deferred();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: settings.promise,
  });
  sessions[0].becomeReady();
  assert.equal(await isPending(connecting), true);
  settings.resolve(compression);
  const channel = await connecting;
  assert.equal(channel.state, "ready");
  assert.equal(channel.compression, compression);
  channel.close();
});

test("closing a channel waiting for compression settings closes its new session", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const controller = new AbortController();
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: new Promise(() => {}),
    signal: controller.signal,
  });
  sessions[0].becomeReady();
  await turn();
  assert.equal(sessions[0].closeCalls.length, 0);
  controller.abort();
  await assert.rejects(connecting, (error) => error.code === Code.Cancelled);
  assert.equal(sessions[0].closeCalls.length, 1, "the ready session is closed");
});

test("a reconnect that starts before a compression promise settles waits for it", async () => {
  const requests = [];
  const { WebTransport, sessions } = fakeWebTransport(recordingEcho(requests));
  const settings = deferred();
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: settings.promise,
  });
  sessions[0].failToConnect(new Error("handshake failed"));
  await waitFor(() => sessions.length === 2);
  sessions[1].becomeReady();
  await turn(5);
  assert.equal(channel.state, "reconnecting", "the new session waits for the settings");

  settings.resolve(compression);
  await waitFor(() => channel.state === "ready");
  assert.equal((await createClient(Echo, channel).unary({ message: "a" })).message, "a");
  assert.equal(requests[0].dictionaryQuery.length, 1, "the call negotiated compression");
  channel.close();
});

test("a channel that closes before becoming ready reports a rejected compression promise", async () => {
  const { WebTransport, sessions } = fakeWebTransport();
  const failure = new Error("wasm blocked");
  const changes = [];
  const connecting = connect("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: Promise.reject(failure),
    onStateChange: (change) => changes.push(change),
  });
  await turn();
  sessions[0].failToConnect(new Error("handshake failed"));
  await assert.rejects(connecting, (error) => /handshake failed/.test(error.statusMessage));
  assert.deepEqual(
    changes.map(({ state, compressionError }) => [state, compressionError]),
    [["closed", failure]],
  );
});

test("a compression promise that rejects after the channel closed goes to reportError", async () => {
  const { WebTransport } = fakeWebTransport();
  const settings = deferred();
  const changes = [];
  const channel = createChannel("https://example.test/trevrpc", {
    transports: WebTransportOnly,
    WebTransport,
    compression: settings.promise,
    onStateChange: (change) => changes.push(change),
  });
  channel.close();
  const failure = new Error("wasm blocked");
  const reported = deferred();
  globalThis.reportError = reported.resolve;
  try {
    settings.reject(failure);
    assert.equal(await reported.promise, failure);
    assert.equal("compressionError" in changes[0], false);
  } finally {
    delete globalThis.reportError;
  }
});
