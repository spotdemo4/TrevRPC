import assert from "node:assert/strict";
import test from "node:test";

import { Code, Channel, RpcKind, RpcRequest, WireVersion } from "../src/index.js";

test("WebTransport channel reconnects after session.closed and exposes lifecycle state", async () => {
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

  const client = new Channel("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    allowPooling: true,
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

test("WebTransport channel uses bounded exponential reconnect delays", async () => {
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

  const client = new Channel("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    onStateChange(event) {
      transitions.push(event);
    },
  });

  await waitFor(() => sessions.length === 3);
  const delays = transitions
    .filter((event) => event.state === "connecting" && event.attempt > 1)
    .map((event) => event.delayMs);
  assert.equal(delays.length, 2);
  assert.ok(delays[0] >= 80 && delays[0] <= 120);
  assert.ok(delays[1] >= 160 && delays[1] <= 240);
  client.close();
});

test("Channel.connect bounds and closes a stalled initial connection", async () => {
  const sessions = [];
  class FakeWebTransport {
    constructor() {
      this.ready = new Promise(() => {});
      this.closed = new Promise(() => {});
      this.closeCalls = 0;
      sessions.push(this);
    }

    close() {
      this.closeCalls += 1;
    }
  }

  await assert.rejects(
    Channel.connect("https://example.test/trevrpc", {
      WebTransport: FakeWebTransport,
      timeoutMs: 1,
    }),
    (error) => error.code === Code.DeadlineExceeded,
  );
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0].closeCalls, 1);
});

test("Channel.connect honors an initial AbortSignal", async () => {
  const controller = new AbortController();
  class FakeWebTransport {
    constructor() {
      this.ready = new Promise(() => {});
      this.closed = new Promise(() => {});
    }

    close() {}
  }

  const connecting = Channel.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    signal: controller.signal,
  });
  controller.abort();

  await assert.rejects(connecting, (error) => error.code === Code.Cancelled);
});

test("Channel.connect does not start a pre-cancelled initial connection", async () => {
  const controller = new AbortController();
  controller.abort();
  let sessions = 0;

  class FakeWebTransport {
    constructor() {
      sessions += 1;
      this.ready = new Promise(() => {});
    }

    close() {}
  }

  await assert.rejects(
    Channel.connect("https://example.test/trevrpc", {
      WebTransport: FakeWebTransport,
      signal: controller.signal,
    }),
    (error) => error.code === Code.Cancelled,
  );
  await Promise.resolve();
  assert.equal(sessions, 0);
});

test("Channel.connect treats a zero initial timeout as an immediate deadline", async () => {
  let sessions = 0;

  class FakeWebTransport {
    constructor() {
      sessions += 1;
      this.ready = new Promise(() => {});
    }

    close() {}
  }

  await assert.rejects(
    Channel.connect("https://example.test/trevrpc", {
      WebTransport: FakeWebTransport,
      timeoutMs: 0,
    }),
    (error) => error.code === Code.DeadlineExceeded,
  );
  await Promise.resolve();
  assert.equal(sessions, 0);
});

test("Channel.connect supports initial timeouts above the platform timer limit", async () => {
  const controller = new AbortController();
  const sessions = [];

  class FakeWebTransport {
    constructor() {
      this.ready = new Promise(() => {});
      this.closeCalls = 0;
      sessions.push(this);
    }

    close() {
      this.closeCalls += 1;
    }
  }

  const connecting = Channel.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    signal: controller.signal,
    timeoutMs: 2_147_483_648,
  });
  await waitFor(() => sessions.length === 1);
  controller.abort();

  await assert.rejects(connecting, (error) => error.code === Code.Cancelled);
  assert.equal(sessions[0].closeCalls, 1);
});

test("WebTransport channel does not infer connection loss or replay from a failed call", async () => {
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

  const client = await Channel.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
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

test("WebTransport channel does not reconnect for a cancelled call", async () => {
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

  const client = await Channel.connect("https://example.test/trevrpc", {
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

test("WebTransport channel keeps in-flight streams on their failed generation", async () => {
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

  const client = await Channel.connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
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
  for (let attempt = 0; attempt < 500; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  assert.fail("condition was not met");
}
