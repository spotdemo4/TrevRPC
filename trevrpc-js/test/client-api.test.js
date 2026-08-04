import assert from "node:assert/strict";
import test from "node:test";

import {
  Code,
  RpcStreamFrameKind,
  bidirectionalStreaming,
  createRoot,
  serverStreaming,
} from "../src/index.js";

const Message = createRoot({
  nested: { test: { nested: { Message: { fields: { message: { type: "string", id: 1 } } } } } },
}).lookupType("test.Message");

test("response status has exact public keys after trailing EOF", async () => {
  let finish;
  const eof = new Promise((resolve) => {
    finish = resolve;
  });
  const stream = await serverStreaming(
    transportFromIterator({
      index: 0,
      async next() {
        this.index += 1;
        if (this.index === 1) {
          return {
            done: false,
            value: {
              kind: RpcStreamFrameKind.Status,
              status: Code.Ok,
              message: "peer text",
              metadata: { trailer: new Uint8Array([1]) },
            },
          };
        }
        await eof;
        return { done: true, value: undefined };
      },
    }),
    "test.Service",
    "Stream",
    Message,
    Message,
    { message: "request" },
    { streamIdleTimeoutMs: undefined },
  );
  let settled = false;
  stream.status.finally(() => {
    settled = true;
  });
  const completed = stream[Symbol.asyncIterator]().next();
  await Promise.resolve();
  assert.equal(settled, false);
  finish();
  assert.deepEqual(await completed, { done: true, value: undefined });
  const status = await stream.status;
  assert.deepEqual(Object.keys(status), ["code", "message", "metadata"]);
  assert.equal(status.message, "peer text");
  assert.equal("statusMessage" in status, false);
});

test("a batched terminal status cancels the deadline before queued messages are consumed", async () => {
  let finishEof;
  const eof = new Promise((resolve) => {
    finishEof = resolve;
  });
  let batch = true;
  const transport = {
    async streamingCall() {
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        nextBodyBatch() {
          if (batch) {
            batch = false;
            return Promise.resolve({
              done: false,
              value: {
                bodies: [Message.encode({ message: "queued" }).finish()],
                status: {
                  kind: RpcStreamFrameKind.Status,
                  status: Code.Ok,
                  message: "complete",
                  metadata: {},
                },
              },
            });
          }
          return eof.then(() => ({ done: true, value: undefined }));
        },
        return() {
          return Promise.resolve({ done: true, value: undefined });
        },
      };
    },
  };
  const stream = await serverStreaming(
    transport,
    "test.Service",
    "Stream",
    Message,
    Message,
    {},
    { timeoutMs: 5, streamIdleTimeoutMs: undefined },
  );
  const iterator = stream[Symbol.asyncIterator]();

  const first = await iterator.next();
  assert.equal(first.done, false);
  assert.equal(first.value.message, "queued");
  await new Promise((resolve) => setTimeout(resolve, 15));
  finishEof();

  assert.deepEqual(await iterator.next(), { done: true, value: undefined });
  assert.equal((await stream.status).message, "complete");
});

test("a valid terminal status wins before trailing EOF validation", async () => {
  let trailingReadStarted;
  const trailingRead = new Promise((resolve) => {
    trailingReadStarted = resolve;
  });
  let finish;
  const eof = new Promise((resolve) => {
    finish = resolve;
  });
  const controller = new AbortController();
  const stream = await serverStreaming(
    transportFromIterator({
      index: 0,
      next() {
        this.index += 1;
        if (this.index === 1) {
          return Promise.resolve({
            done: false,
            value: { kind: RpcStreamFrameKind.Status, status: Code.Ok, message: "complete" },
          });
        }
        trailingReadStarted();
        return eof.then(() => ({ done: true, value: undefined }));
      },
      return() {
        return Promise.resolve({ done: true, value: undefined });
      },
    }),
    "test.Service",
    "Stream",
    Message,
    Message,
    {},
    { signal: controller.signal, timeoutMs: 5, streamIdleTimeoutMs: undefined },
  );
  const next = stream[Symbol.asyncIterator]().next();
  await trailingRead;
  await new Promise((resolve) => setTimeout(resolve, 15));

  controller.abort(new Error("late cancellation"));
  finish();

  assert.deepEqual(await next, { done: true, value: undefined });
  assert.equal((await stream.status).message, "complete");
});

test("non-OK iteration and status reject with the same error", async () => {
  const stream = await serverStreaming(
    transportFromFrames([
      {
        kind: RpcStreamFrameKind.Status,
        status: Code.Unavailable,
        message: "down",
        metadata: {},
      },
    ]),
    "test.Service",
    "Stream",
    Message,
    Message,
    {},
    { streamIdleTimeoutMs: undefined },
  );
  let iterationError;
  await assert.rejects(stream[Symbol.asyncIterator]().next(), (error) => {
    iterationError = error;
    return error.code === Code.Unavailable && error.statusMessage === "down";
  });
  await assert.rejects(stream.status, (error) => error === iterationError);
});

test("never-read response streams are cleaned up by the absolute deadline", async () => {
  let returns = 0;
  const stream = await serverStreaming(
    transportFromIterator({
      next: () => new Promise(() => {}),
      return() {
        returns += 1;
        return Promise.resolve({ done: true, value: undefined });
      },
    }),
    "test.Service",
    "Stream",
    Message,
    Message,
    {},
    { timeoutMs: 5, streamIdleTimeoutMs: undefined },
  );
  await assert.rejects(stream.status, (error) => error.code === Code.DeadlineExceeded);
  assert.equal(returns, 1);
});

test("idle bidi deadline closes uploads and rejects later sends", async () => {
  let requestIterator;
  const transport = {
    async streamingCall(_request, requestBody) {
      requestIterator = requestBody[Symbol.asyncIterator]();
      void requestIterator.next().catch(() => {});
      return {
        [Symbol.asyncIterator]() {
          return {
            next: () => new Promise(() => {}),
            return: () => Promise.resolve({ done: true, value: undefined }),
          };
        },
      };
    },
  };
  const call = await bidirectionalStreaming(transport, "test.Service", "Bidi", Message, Message, {
    timeoutMs: 5,
    streamIdleTimeoutMs: undefined,
  });
  await assert.rejects(call.status, (error) => error.code === Code.DeadlineExceeded);
  await assert.rejects(
    call.send({ message: "late" }),
    (error) => error.code === Code.DeadlineExceeded,
  );
  await assert.rejects(requestIterator.next(), (error) => error.code === Code.DeadlineExceeded);
});

test("explicit close and iterator return cancel exactly once", async () => {
  for (const mode of ["close", "return"]) {
    let returns = 0;
    const stream = await serverStreaming(
      transportFromIterator({
        next: () => new Promise(() => {}),
        return() {
          returns += 1;
          return Promise.resolve({ done: true, value: undefined });
        },
      }),
      "test.Service",
      "Stream",
      Message,
      Message,
      {},
      { streamIdleTimeoutMs: undefined },
    );
    if (mode === "close") {
      await stream.close("done");
    } else {
      await stream[Symbol.asyncIterator]().return();
    }
    await assert.rejects(stream.status, (error) => error.code === Code.Cancelled);
    await stream.close();
    assert.equal(returns, 1);
  }
});

function transportFromFrames(frames) {
  let index = 0;
  return transportFromIterator({
    next() {
      if (index >= frames.length) {
        return Promise.resolve({ done: true, value: undefined });
      }
      return Promise.resolve({ done: false, value: frames[index++] });
    },
  });
}

function transportFromIterator(iterator) {
  return {
    async streamingCall() {
      return {
        [Symbol.asyncIterator]() {
          return iterator;
        },
      };
    },
  };
}
