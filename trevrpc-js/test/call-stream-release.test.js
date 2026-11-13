// A call must release every stream it opened: the replacement stream of a replayed optimistic call
// when the call is aborted while it opens, and the response stream of a finished call whose server
// never ends it.
import assert from "node:assert/strict";
import test from "node:test";

import { CompressedEcho } from "../build/interop/gen/compressed_pb.js";
import { Echo, EchoRequestSchema, EchoResponseSchema } from "../build/interop/gen/interop_pb.js";
import { createClient } from "../src/client.js";
import { Code } from "../src/status.js";
import { StreamCompression } from "../src/wire.js";
import { createCompression } from "../src/zstd.js";
import { deferred, fakeTransport, turn } from "./support.js";

const plain = await createCompression();

/**
 * How long a stream may stay unreleased before a test calls it leaked. A release takes a few turns
 * of the event loop, so this only has to outlast a starved machine; a passing test never waits it
 * out.
 */
const LeakMs = 5000;

/**
 * A transport whose first stream refuses the optimistic call. The replacement stream's open is held
 * by the test: `release(after)` resolves it and then runs `after()`. `replayed` settles once the
 * call has written its REQUEST on the replacement stream, so the stream is attached to the call.
 * `outcome` says how the server saw the replacement stream: "cancelled" if the client reset it,
 * "leaked" if nothing happened for LeakMs.
 */
function replayHarness() {
  const prepared = deferred();
  const replayed = deferred();
  const outcome = deferred();
  let served = 0;
  const transport = fakeTransport(async (stream) => {
    served += 1;
    const replacement = served === 2;
    const request = await stream.readRequest().catch((error) => {
      // The client reset the stream before it wrote the REQUEST.
      if (replacement) {
        outcome.resolve("cancelled");
      }
      throw error;
    });
    if (request.streamCompression === StreamCompression.Optimistic) {
      await stream.refuse();
      return;
    }
    replayed.resolve();
    const leak = deferred();
    const timer = setTimeout(() => leak.resolve("leaked"), LeakMs);
    const requestSideReset = stream.readFrame().then(
      () => "cancelled",
      () => "cancelled",
    );
    const responseSideCancelled = stream.responseCancelled.then((cancelled) =>
      cancelled ? "cancelled" : leak.promise,
    );
    outcome.resolve(await Promise.race([leak.promise, requestSideReset, responseSideCancelled]));
    clearTimeout(timer);
  });
  transport.compression = plain;
  const create = transport.createBidirectionalStream.bind(transport);
  let opens = 0;
  transport.createBidirectionalStream = () => {
    opens += 1;
    if (opens === 1) {
      return create();
    }
    // Hand out the real stream only when the test releases it.
    const gate = deferred();
    prepared.resolve((after) => {
      void create().then((stream) => {
        gate.resolve(stream);
        after?.();
      });
    });
    return gate.promise;
  };
  return {
    transport,
    prepared: prepared.promise,
    replayed: replayed.promise,
    outcome: outcome.promise,
  };
}

/** Starts a refused optimistic call, which is about to open its replacement stream. */
async function startReplay() {
  const parts = replayHarness();
  const controller = new AbortController();
  const call = await createClient(CompressedEcho, parts.transport).clientStream({
    signal: controller.signal,
  });
  await call.send({ message: "x" });
  const echo = call.closeAndRecv().catch((error) => error);
  return { ...parts, controller, echo };
}

test("replay: an abort while the replacement stream is opening resets it", async () => {
  const { prepared, outcome, controller, echo } = await startReplay();
  const release = await prepared;
  controller.abort();
  release();
  await echo;
  assert.equal(await outcome, "cancelled");
});

test("replay: an abort after the replacement stream is attached resets it", async () => {
  const { prepared, replayed, outcome, controller, echo } = await startReplay();
  const release = await prepared;
  release();
  await replayed;
  controller.abort();
  await echo;
  assert.equal(await outcome, "cancelled");
});

test("replay: an abort right after the replacement stream opened resets it unused", async () => {
  const { prepared, outcome, controller, echo } = await startReplay();
  const release = await prepared;
  // The open resolves, then the abort runs before the replay's continuation, when the stream is
  // not attached to the call yet.
  release(() => queueMicrotask(() => controller.abort()));
  const error = await echo;
  assert.equal(error.code, Code.Cancelled);
  assert.equal(await outcome, "cancelled", "the replacement stream was left open");
});

/** Serves one call and then `ending` decides what the server does after the OK STATUS. */
function serveUnary(ending) {
  const served = deferred();
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await stream.readMessage(EchoRequestSchema);
    await stream.writeMessage(EchoResponseSchema, { message: "ok" });
    await stream.writeStatus(Code.Ok);
    if (ending === "fin") {
      await stream.finish();
    }
    served.resolve(stream);
  });
  return { echo: createClient(Echo, transport), served: served.promise };
}

/** Whether the client has cancelled its read side of `stream`, once the event loop settled. */
async function released(stream) {
  let cancelled = false;
  stream.responseCancelled.then((value) => {
    cancelled = value;
  });
  await turn();
  return cancelled;
}

/** Whether the client cancels its read side of `stream`, waiting for it no longer than LeakMs. */
async function releases(stream) {
  let timer;
  const leaked = new Promise((resolve) => {
    timer = setTimeout(() => resolve(false), LeakMs);
  });
  try {
    return await Promise.race([stream.responseCancelled, leaked]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Puts the call's deadline in the test's hands: timers only fire when the test ticks, so that a
 * starved machine cannot let a deadline pass before the test looks.
 */
function holdTimers(t) {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  return (ms) => t.mock.timers.tick(ms);
}

test("after STATUS: a known frame past it resets the response side", async () => {
  const { echo, served } = serveUnary("open");
  const response = await echo.unary({ message: "hi" });
  assert.equal(response.message, "ok");
  const stream = await served;
  await stream.writeHeaders({});
  assert.equal(await releases(stream), true);
});

test("after STATUS: a server that never ends the stream is released at the deadline", async (t) => {
  const tick = holdTimers(t);
  const { echo, served } = serveUnary("open");
  const response = await echo.unary({ message: "hi" }, { timeoutMs: 100 });
  assert.equal(response.message, "ok");
  const stream = await served;
  // The outcome was reported at STATUS, and the deadline has not passed yet.
  assert.equal(await released(stream), false);
  tick(100);
  assert.equal(await released(stream), true);
});

test("after STATUS: a server that never ends the stream is held while there is time left", async (t) => {
  const tick = holdTimers(t);
  const { echo, served } = serveUnary("open");
  await echo.unary({ message: "hi" }, { timeoutMs: 60_000 });
  const stream = await served;
  tick(30_000);
  assert.equal(await released(stream), false);
  // Ending the stream ends the wait, and with it the deadline watch.
  await stream.reset();
});

test("after STATUS: a stream the server ended is not cancelled at the deadline", async (t) => {
  const tick = holdTimers(t);
  const { echo, served } = serveUnary("fin");
  await echo.unary({ message: "hi" }, { timeoutMs: 50 });
  const stream = await served;
  // The server has ended the stream; let the client read that end, which stops its deadline
  // watch, before the deadline passes.
  await turn();
  tick(50);
  assert.equal(await released(stream), false);
});

test("after STATUS: close() releases a server that never ends the stream", async () => {
  const served = deferred();
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponseSchema, { message: "a" });
    await stream.writeStatus(Code.Ok);
    served.resolve(stream);
  });
  const call = await createClient(Echo, transport).serverStream({ message: "x" });
  const received = [];
  for await (const response of call) {
    received.push(response.message);
  }
  assert.deepEqual(received, ["a"]);
  const stream = await served.promise;
  assert.equal(await released(stream), false, "no deadline: held until close()");
  await call.close();
  assert.equal(await releases(stream), true);
  assert.equal((await call.status).code, Code.Ok, "the outcome does not change");
});

test("after STATUS: a stream that fails during the wait raises no unhandled rejection", async (t) => {
  const tick = holdTimers(t);
  const { echo, served } = serveUnary("open");
  await echo.unary({ message: "hi" }, { timeoutMs: 100 });
  const stream = await served;
  await stream.reset();
  tick(100);
  await turn();
});
