import assert from "node:assert/strict";
import test from "node:test";

import {
  Code,
  ReconnectingWebTransportClient,
  RpcKind,
  RpcRequest,
  WireVersion,
} from "../src/index.js";

test("managed WebTransport reconnects after session.closed and exposes lifecycle state", async () => {
  const sessions = [];
  const callbacks = [];
  const events = [];

  class FakeWebTransport {
    constructor(url, options) {
      this.url = url;
      this.options = options;
      this.readyDeferred = deferred();
      this.ready = this.readyDeferred.promise;
      this.closedDeferred = deferred();
      this.closed = this.closedDeferred.promise;
      this.closeCalls = [];
      sessions.push(this);
    }

    close(closeInfo) {
      this.closeCalls.push(closeInfo);
    }
  }

  const client = new ReconnectingWebTransportClient("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    allowPooling: true,
    reconnectInitialDelayMs: 0,
    reconnectMaxDelayMs: 0,
    reconnectJitter: 0,
    onStateChange(event) {
      callbacks.push(event);
    },
  });
  client.addEventListener("statechange", (event) => events.push(event.detail));

  await waitFor(() => sessions.length === 1);
  assert.deepEqual(sessions[0].options, { allowPooling: true });
  sessions[0].readyDeferred.resolve();
  await client.waitUntilReady();

  assert.equal(client.ready, true);
  assert.equal(client.state, "ready");
  assert.equal(client.generation, 1);
  assert.equal(client.url, "https://example.test/trevrpc");
  assert.equal(client.options.reconnectInitialDelayMs, 0);

  sessions[0].closedDeferred.resolve({ closeCode: 1, reason: "restart" });
  await waitFor(() => client.state === "reconnecting");
  const nextReady = client.waitUntilReady();
  await assert.rejects(
    client.call({}),
    (error) => error.code === Code.Unavailable && /reconnecting/u.test(error.statusMessage),
  );

  await waitFor(() => sessions.length === 2);
  sessions[1].readyDeferred.resolve();
  await nextReady;

  assert.equal(client.generation, 2);
  assert.equal(client.ready, true);
  assert.ok(callbacks.some((event) => event.state === "reconnecting"));
  assert.ok(events.some((event) => event.state === "ready" && event.generation === 2));

  client.close({ closeCode: 0, reason: "done" });
  assert.equal(client.state, "closed");
  assert.equal(client.ready, false);
  assert.deepEqual(sessions[1].closeCalls, [{ closeCode: 0, reason: "done" }]);
  await assert.rejects(client.waitUntilReady(), (error) => error.code === Code.Unavailable);
});

test("managed WebTransport bounds exponential reconnect delays", async () => {
  const sessions = [];
  const transitions = [];

  class FakeWebTransport {
    constructor() {
      this.closed = new Promise(() => {});
      this.ready =
        sessions.length < 2 ? Promise.reject(new Error("not ready")) : new Promise(() => {});
      sessions.push(this);
    }

    close() {}
  }

  const client = new ReconnectingWebTransportClient("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    reconnectInitialDelayMs: 5,
    reconnectMaxDelayMs: 6,
    reconnectMultiplier: 2,
    reconnectJitter: 0,
    onStateChange(event) {
      transitions.push(event);
    },
  });

  await waitFor(() => sessions.length === 3);
  assert.deepEqual(
    transitions
      .filter((event) => event.state === "connecting" && event.attempt > 1)
      .map((event) => event.delayMs),
    [5, 6],
  );
  client.close();
});

test("managed WebTransport does not infer connection loss or replay from a failed call", async () => {
  const sessions = [];

  class FakeWebTransport {
    constructor() {
      this.ready = Promise.resolve();
      this.closedDeferred = deferred();
      this.closed = this.closedDeferred.promise;
      this.opens = 0;
      sessions.push(this);
    }

    createBidirectionalStream() {
      this.opens += 1;
      return Promise.reject(new Error("generation failed"));
    }

    close() {}
  }

  const client = await ReconnectingWebTransportClient.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    reconnectInitialDelayMs: 0,
    reconnectMaxDelayMs: 0,
    reconnectJitter: 0,
  });

  await assert.rejects(client.call(rpcRequest()), (error) => error.code === Code.Unavailable);
  assert.equal(client.generation, 1);
  assert.equal(client.state, "ready");
  assert.equal(sessions[0].opens, 1);
  assert.equal(sessions.length, 1);

  sessions[0].closedDeferred.resolve({ closeCode: 1, reason: "connection lost" });
  await waitFor(() => client.generation === 2);
  assert.equal(sessions[1].opens, 0);
  client.close();
});

test("managed WebTransport does not reconnect for a cancelled call", async () => {
  const sessions = [];
  const pendingOpen = deferred();

  class FakeWebTransport {
    constructor() {
      this.ready = Promise.resolve();
      this.closed = new Promise(() => {});
      sessions.push(this);
    }

    createBidirectionalStream() {
      return pendingOpen.promise;
    }

    close() {}
  }

  const client = await ReconnectingWebTransportClient.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
  });
  const controller = new AbortController();
  const call = client.call(rpcRequest(), { signal: controller.signal });
  controller.abort();

  await assert.rejects(call, (error) => error.code === Code.Cancelled);
  assert.equal(client.generation, 1);
  assert.equal(client.state, "ready");
  assert.equal(sessions.length, 1);

  pendingOpen.resolve(fakePendingStream(Promise.resolve({ done: true, value: undefined })));
  client.close();
});

test("managed WebTransport keeps in-flight streams on their failed generation", async () => {
  const sessions = [];
  const oldRead = deferred();

  class FakeWebTransport {
    constructor() {
      this.ready = Promise.resolve();
      this.closedDeferred = deferred();
      this.closed = this.closedDeferred.promise;
      this.opens = 0;
      sessions.push(this);
    }

    createBidirectionalStream() {
      this.opens += 1;
      return Promise.resolve(fakePendingStream(oldRead.promise));
    }

    close() {}
  }

  const client = await ReconnectingWebTransportClient.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    reconnectInitialDelayMs: 0,
    reconnectMaxDelayMs: 0,
    reconnectJitter: 0,
  });
  const frames = await client.streamingCall(rpcRequest(RpcKind.ServerStreaming), emptyBody());
  const pendingFrame = frames.next();

  sessions[0].closedDeferred.resolve({ closeCode: 1, reason: "restart" });
  await waitFor(() => client.generation === 2);
  oldRead.reject(new Error("old generation closed"));

  await assert.rejects(pendingFrame, (error) => error.code === Code.Unavailable);
  assert.equal(client.generation, 2);
  assert.equal(sessions[0].opens, 1);
  assert.equal(sessions[1].opens, 0);
  client.close();
});

function rpcRequest(kind = RpcKind.Unary) {
  return RpcRequest.create({
    service: "hello.v1.Greeter",
    method: "SayHello",
    body: new Uint8Array(),
    metadata: {},
    kind,
    version: WireVersion,
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function fakePendingStream(readPromise) {
  return {
    readable: {
      getReader() {
        return {
          read() {
            return readPromise;
          },
          cancel() {
            return Promise.resolve();
          },
          releaseLock() {},
        };
      },
    },
    writable: {
      getWriter() {
        return {
          write() {
            return Promise.resolve();
          },
          close() {
            return Promise.resolve();
          },
          abort() {
            return Promise.resolve();
          },
          releaseLock() {},
        };
      },
    },
  };
}

async function* emptyBody() {}

async function waitFor(predicate) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  assert.fail("condition was not met");
}
