import assert from "node:assert/strict";
import { constants as osConstants } from "node:os";
import test from "node:test";

import { Code, TrevRpcError } from "../src/index.js";
import { Channel, RawNodeTransport } from "../src/node.js";

for (const operation of ["call", "startStream", "close"]) {
  test(`native ${operation} errors become TrevRpcError values`, async () => {
    const cause = Object.assign(new Error(`${operation} exploded`), { nativeCode: -1001 });
    const nativeClient = {
      call: () => Promise.reject(cause),
      startStream: () => Promise.reject(cause),
      close() {
        throw cause;
      },
    };
    const transport = new RawNodeTransport(nativeClient);

    const invoke =
      operation === "call"
        ? () => transport.call({ service: "test.Service", method: "Call", body: new Uint8Array() })
        : operation === "startStream"
          ? () =>
              transport.streamingCall(
                { service: "test.Service", method: "Call", body: new Uint8Array() },
                emptyBodies(),
              )
          : () => Promise.resolve().then(() => transport.close());

    await assert.rejects(invoke(), (error) => {
      assert.ok(error instanceof TrevRpcError);
      assert.equal(error.code, Code.Unavailable);
      assert.equal(error.nativeCode, -1001);
      assert.equal(error.cause, cause);
      assert.match(error.statusMessage, new RegExp(operation));
      return true;
    });
  });
}

test("native error codes map to the complete public status contract", async () => {
  const cases = [
    [-osConstants.errno.ECANCELED, Code.Cancelled],
    [-4001, Code.Cancelled],
    [-osConstants.errno.ETIMEDOUT, Code.DeadlineExceeded],
    [-1003, Code.DeadlineExceeded],
    [-1002, Code.ResourceExhausted],
    [-1004, Code.ResourceExhausted],
    [-2005, Code.ResourceExhausted],
    [-2006, Code.ResourceExhausted],
    [-2001, Code.InvalidArgument],
    [-2003, Code.InvalidArgument],
    [-2002, Code.FailedPrecondition],
    [-1001, Code.Unavailable],
    [-9999, Code.Unavailable],
  ];

  for (const [nativeCode, code] of cases) {
    const cause = Object.assign(new Error(`native ${nativeCode}`), { nativeCode });
    const transport = new RawNodeTransport({ call: () => Promise.reject(cause) });
    await assert.rejects(
      transport.call({ service: "test.Service", method: "Call", body: new Uint8Array() }),
      (error) =>
        error instanceof TrevRpcError &&
        error.code === code &&
        error.nativeCode === nativeCode &&
        error.cause === cause,
    );
  }
});

test("native status errors retain an explicit TrevRPC status", async () => {
  const cause = Object.assign(new Error("denied"), {
    code: Code.PermissionDenied,
    nativeCode: -2007,
    metadata: { trailer: new Uint8Array([1]) },
  });
  const transport = new RawNodeTransport({ call: () => Promise.reject(cause) });

  await assert.rejects(
    transport.call({ service: "test.Service", method: "Call", body: new Uint8Array() }),
    (error) =>
      error instanceof TrevRpcError &&
      error.code === Code.PermissionDenied &&
      error.nativeCode === -2007 &&
      error.cause === cause &&
      error.metadata === cause.metadata,
  );
});

test("Node channels expose normalized frozen endpoint and options", () => {
  const channel = new Channel("https://example.test:7443/ignored", {
    maxFrameSize: 4096,
  });
  try {
    assert.deepEqual(channel.endpoint, { host: "example.test", port: 7443 });
    assert.deepEqual(channel.options, { maxFrameSize: 4096 });
    assert.equal(Object.isFrozen(channel.endpoint), true);
    assert.equal(Object.isFrozen(channel.options), true);
    assert.equal("urlOrOptions" in channel, false);
  } finally {
    channel.close();
  }
});

async function* emptyBodies() {}
