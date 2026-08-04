import assert from "node:assert/strict";
import test from "node:test";

import { Code } from "../src/index.js";
import { NodeServer, NodeServerCall } from "../src/node.js";

test("Node server call exposes an absolute deadline and aborts pending native I/O", async () => {
  const native = pendingNativeCall({ hasDeadline: true, timeRemainingNanos: 5_000_000n });
  const call = new NodeServerCall(native);

  assert.ok(call.deadline instanceof Date);
  assert.equal(call.signal.aborted, false);
  await assert.rejects(call.recv(), (error) => error.code === Code.DeadlineExceeded);
  assert.equal(call.signal.aborted, true);
  assert.equal(call.signal.reason.code, Code.DeadlineExceeded);
  assert.equal(native.closeCount, 1);
  await assert.rejects(
    call.sendMessage(new Uint8Array([1])),
    (error) => error === call.signal.reason,
  );
});

test("captured peer cancellation aborts the call before handler work settles", async () => {
  const native = resolvedNativeCall({ cancelled: true });
  const call = new NodeServerCall(native);
  await Promise.resolve();

  assert.equal(call.signal.aborted, true);
  assert.equal(call.signal.reason.code, Code.Cancelled);
  assert.equal(native.closeCount, 1);
  await assert.rejects(
    call.respond({ body: new Uint8Array() }),
    (error) => error === call.signal.reason,
  );
});

test("terminal response wins over a later deadline", async () => {
  const native = resolvedNativeCall({ hasDeadline: true, timeRemainingNanos: 20_000_000n });
  const call = new NodeServerCall(native);
  await call.respond({ body: new Uint8Array([1]) });
  await new Promise((resolve) => setTimeout(resolve, 30));

  assert.equal(call.completed, true);
  assert.equal(call.finalStatus, Code.Ok);
  assert.equal(call.signal.aborted, false);
  assert.equal(native.closeCount, 0);
  assert.equal(native.responses.length, 1);
});

test("deadline cancels a claimed but unsettled terminal response", async () => {
  let rejectResponse;
  const native = resolvedNativeCall({ hasDeadline: true, timeRemainingNanos: 5_000_000n });
  native.respond = () =>
    new Promise((_resolve, reject) => {
      rejectResponse = reject;
    });
  native.close = function close() {
    this.closeCount += 1;
    rejectResponse(Object.assign(new Error("native call closed"), { nativeCode: -4001 }));
  };
  const call = new NodeServerCall(native);
  const response = call.respond({ body: new Uint8Array([1]) });

  assert.equal(call.completed, true);
  assert.equal(call.completedAt, null);
  await assert.rejects(response, (error) => error === call.signal.reason);
  assert.equal(call.signal.reason.code, Code.DeadlineExceeded);
  assert.equal(call.finalStatus, Code.DeadlineExceeded);
  assert.equal(native.closeCount, 1);
  assert.notEqual(call.completedAt, null);
});

test("explicit close settles cancellation once and suppresses late completion", async () => {
  const native = resolvedNativeCall({});
  const call = new NodeServerCall(native);
  call.close("handler stopped");
  call.close("again");

  assert.equal(call.signal.aborted, true);
  assert.equal(call.signal.reason.statusMessage, "handler stopped");
  assert.equal(native.closeCount, 1);
  await assert.rejects(call.finishStream(), (error) => error === call.signal.reason);
  assert.equal(native.statuses.length, 0);
});

test("server call close cancels a claimed but unsettled terminal response", async () => {
  let rejectResponse;
  const native = resolvedNativeCall({});
  native.respond = () =>
    new Promise((_resolve, reject) => {
      rejectResponse = reject;
    });
  const call = new NodeServerCall(native);
  const response = call.respond({ body: new Uint8Array([1]) });
  await Promise.resolve();

  assert.equal(call.completed, true);
  assert.equal(call.completedAt, null);
  call.close("server shutdown");
  rejectResponse(Object.assign(new Error("native call closed"), { nativeCode: -4001 }));

  await assert.rejects(response, (error) => error === call.signal.reason);
  assert.equal(call.signal.reason.code, Code.Cancelled);
  assert.equal(native.closeCount, 1);
  assert.notEqual(call.completedAt, null);
});

test("failed terminal responses close ownership and preserve the native cause", async () => {
  const cause = Object.assign(new Error("response failed"), { nativeCode: -1001 });
  const native = resolvedNativeCall({});
  native.respond = () => Promise.reject(cause);
  const call = new NodeServerCall(native);

  await assert.rejects(call.respond({ body: new Uint8Array([1]) }), (error) => {
    assert.equal(error.code, Code.Unavailable);
    assert.equal(error.nativeCode, -1001);
    assert.equal(error.cause, cause);
    return true;
  });
  assert.equal(call.signal.aborted, true);
  assert.equal(call.signal.reason.cause, cause);
  assert.equal(native.closeCount, 1);
  assert.notEqual(call.completedAt, null);
});

test("graceful server shutdown drains active handlers and preserves their result", async () => {
  let dispatch;
  const nativeServer = {
    port: 50051,
    register(_service, _method, _kind, handler) {
      dispatch = handler;
    },
    closeCount: 0,
    close() {
      this.closeCount += 1;
    },
  };
  const server = new NodeServer(nativeServer);
  let observedCall;
  let releaseHandler;
  const handlerReleased = new Promise((resolve) => {
    releaseHandler = resolve;
  });
  server.register("test.Service", "Call", 0, async (call) => {
    observedCall = call;
    await handlerReleased;
    return { body: new Uint8Array([1]) };
  });

  const nativeCall = resolvedNativeCall({});
  dispatch(nativeCall);
  await Promise.resolve();
  server.close();

  assert.equal(observedCall.signal.aborted, false);
  assert.equal(nativeCall.closeCount, 0);
  assert.equal(nativeCall.responses.length, 0);
  assert.equal(nativeServer.closeCount, 1);

  releaseHandler();
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(observedCall.signal.aborted, false);
  assert.equal(nativeCall.closeCount, 0);
  assert.deepEqual(Array.from(nativeCall.responses[0].body), [1]);
});

function pendingNativeCall(context) {
  let rejectRecv;
  const recv = new Promise((_resolve, reject) => {
    rejectRecv = reject;
  });
  return {
    ...resolvedNativeCall(context),
    recvMany: () => recv,
    close() {
      this.closeCount += 1;
      const error = Object.assign(new Error("native call closed"), { nativeCode: -4001 });
      rejectRecv(error);
    },
  };
}

function resolvedNativeCall(context) {
  return {
    request: {
      service: "test.Service",
      method: "Call",
      body: new Uint8Array(),
      metadata: {},
      kind: 0,
      version: 1,
    },
    context,
    closeCount: 0,
    responses: [],
    statuses: [],
    respond(response) {
      this.responses.push(response);
      return Promise.resolve();
    },
    sendMessage: () => Promise.resolve(),
    recvMany: () => Promise.resolve([null]),
    finishStream(status, message, metadata) {
      this.statuses.push({ status, message, metadata });
      return Promise.resolve();
    },
    close() {
      this.closeCount += 1;
    },
  };
}
