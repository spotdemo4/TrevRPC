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
import { deferred, fakeTransport } from "./support.js";

const plain = await createCompression();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A transport whose first stream refuses the optimistic call. The replacement stream's open is held
 * by the test: `release(after)` resolves it and then runs `after()`. `outcome` says how the server
 * saw the replacement stream: "cancelled" if the client reset it, "leaked" if nothing happened
 * for 300 ms.
 */
function replayHarness() {
  const prepared = deferred();
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
    const timer = sleep(300).then(() => "leaked");
    const requestSideReset = stream.readFrame().then(
      () => "cancelled",
      () => "cancelled",
    );
    const responseSideCancelled = stream.responseCancelled.then((cancelled) =>
      cancelled ? "cancelled" : timer,
    );
    outcome.resolve(await Promise.race([timer, requestSideReset, responseSideCancelled]));
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
  return { transport, prepared: prepared.promise, outcome: outcome.promise };
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
  const { prepared, outcome, controller, echo } = await startReplay();
  const release = await prepared;
  release();
  await sleep(50);
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
    served.resolve(stream);
    if (ending === "fin") {
      await stream.finish();
    }
  });
  return { echo: createClient(Echo, transport), served: served.promise };
}

/** Whether the client cancelled its read side of `stream` within `ms`. */
async function clientReleased(stream, ms) {
  let released = false;
  stream.responseCancelled.then((cancelled) => {
    released = cancelled;
  });
  await sleep(ms);
  return released;
}

test("after STATUS: a known frame past it resets the response side", async () => {
  const { echo, served } = serveUnary("open");
  const response = await echo.unary({ message: "hi" });
  assert.equal(response.message, "ok");
  const stream = await served;
  await stream.writeHeaders({});
  assert.equal(await clientReleased(stream, 100), true);
});

test("after STATUS: a server that never ends the stream is released at the deadline", async () => {
  const { echo, served } = serveUnary("open");
  const response = await echo.unary({ message: "hi" }, { timeoutMs: 100 });
  assert.equal(response.message, "ok");
  const stream = await served;
  // The outcome was reported at STATUS, and the deadline has not passed yet.
  assert.equal(await clientReleased(stream, 20), false);
  assert.equal(await clientReleased(stream, 300), true);
});

test("after STATUS: a server that never ends the stream is held while there is time left", async () => {
  const { echo, served } = serveUnary("open");
  await echo.unary({ message: "hi" }, { timeoutMs: 60_000 });
  const stream = await served;
  assert.equal(await clientReleased(stream, 100), false);
  // Ending the stream ends the wait, and with it the deadline watch.
  await stream.reset();
});

test("after STATUS: a stream the server ended is not cancelled at the deadline", async () => {
  const { echo, served } = serveUnary("fin");
  await echo.unary({ message: "hi" }, { timeoutMs: 50 });
  const stream = await served;
  await sleep(150);
  assert.equal(await clientReleased(stream, 0), false);
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
  assert.equal(await clientReleased(stream, 50), false, "no deadline: held until close()");
  await call.close();
  assert.equal(await clientReleased(stream, 100), true);
  assert.equal((await call.status).code, Code.Ok, "the outcome does not change");
});

test("after STATUS: a stream that fails during the wait raises no unhandled rejection", async () => {
  const { echo, served } = serveUnary("open");
  await echo.unary({ message: "hi" }, { timeoutMs: 100 });
  const stream = await served;
  await stream.reset();
  await sleep(150);
});
