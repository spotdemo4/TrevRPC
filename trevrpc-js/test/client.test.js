import assert from "node:assert/strict";
import test from "node:test";

import { EchoClient, root } from "../build/interop/gen/interop.trevrpc.js";
import { createServiceClient } from "../src/client.js";
import { encodeFrame, encodeMessageFrame } from "../src/framing.js";
import { Code, TrevRpcError } from "../src/status.js";
import { RpcKind, RpcStreamFrame, WireVersion, createRoot } from "../src/wire.js";
import { echoHandler } from "./echo-server.js";
import { concatBytes, deferred, fakeTransport, turn } from "./support.js";

const EchoRequest = root.lookupType("trevrpc.interop.v1.EchoRequest");
const EchoResponse = root.lookupType("trevrpc.interop.v1.EchoResponse");
const text = new TextDecoder();

/** An Echo client whose streams are served by handler; returns the client and the transport. */
function setup(handler, options) {
  const transport = fakeTransport(handler);
  return { transport, echo: new EchoClient(transport, options) };
}

const isStatus = (code, message) => (error) =>
  error instanceof TrevRpcError &&
  error.code === code &&
  (message === undefined || error.statusMessage === message);

async function reply(stream, fields) {
  await stream.writeResponse({ body: EchoResponse.encode(fields).finish() });
}

test("unary calls send the request header and decode the response", async () => {
  const { echo, transport } = setup(async (stream) => {
    const request = await stream.readRequest();
    assert.equal(request.service, "trevrpc.interop.v1.Echo");
    assert.equal(request.method, "Unary");
    assert.equal(request.kind, RpcKind.Unary);
    assert.equal(request.version, WireVersion);
    assert.equal(text.decode(request.metadata.trace), "abc");
    assert.equal(EchoRequest.decode(request.body).message, "hi");
    assert.equal(await stream.readFrame(), null, "request side is finished after the request");
    await stream.writeResponse({
      body: EchoResponse.encode({ message: "hello", index: 3 }).finish(),
      metadata: { served: new Uint8Array([1]) },
    });
    await stream.finish();
  });

  const response = await echo.unaryWithResponse({ message: "hi" }, { metadata: { Trace: "abc" } });
  assert.equal(response.message.message, "hello");
  assert.equal(response.message.index, 3);
  assert.deepEqual([...response.metadata.served], [1]);
  await transport.settled();
});

test("unary calls fail with the status of a non-OK response", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeResponse({
      status: Code.NotFound,
      message: "missing",
      metadata: { why: new Uint8Array([1]) },
    });
  });

  await assert.rejects(echo.unary({ message: "x" }), (error) => {
    assert.ok(isStatus(Code.NotFound, "missing")(error));
    assert.deepEqual([...error.metadata.why], [1]);
    return true;
  });
});

test("unary calls return as soon as the response arrives", async () => {
  // The server never reads the request or finishes the stream; the response alone decides.
  const { echo } = setup(async (stream) => {
    await reply(stream, { message: "early" });
  });
  assert.equal((await echo.unary({ message: "x" })).message, "early");
});

test("invalid requests fail before a stream is opened", async () => {
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  const echo = new EchoClient(transport);

  await assert.rejects(
    echo.unary({}, { metadata: { "trevrpc-reserved": "x" } }),
    isStatus(Code.InvalidArgument),
  );
  await assert.rejects(echo.unary({}, { maxFrameSize: 4 }), isStatus(Code.ResourceExhausted));
  assert.equal(transport.opened, 0);
});

test("timeouts are sent on the wire", async () => {
  const seen = [];
  const { echo } = setup(async (stream) => {
    const request = await stream.readRequest();
    seen.push(String(request.timeoutNanos));
    await reply(stream, {});
  });

  await echo.unary({});
  await echo.unary({}, { timeoutMs: 1500 });
  await echo.unary({}, { timeoutMs: 0 });
  assert.deepEqual(seen, ["0", "1500000000", "1"]);
});

test("invalid timeouts are rejected", async () => {
  const { echo } = setup(() => assert.fail("no stream expected"));
  for (const timeoutMs of [-1, NaN, Infinity, 1.9e13]) {
    await assert.rejects(echo.unary({}, { timeoutMs }), isStatus(Code.InvalidArgument), timeoutMs);
  }
});

test("deadlines longer than the timer limit do not fire immediately", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await turn();
    await reply(stream, { message: "ok" });
  });
  assert.equal((await echo.unary({}, { timeoutMs: 3_000_000_000 })).message, "ok");
});

test("an expired deadline fails the call and resets its stream", async () => {
  let server;
  const { echo } = setup(async (stream) => {
    server = stream;
    await stream.readRequest();
    await stream.responseCancelled;
  });

  await assert.rejects(echo.unary({}, { timeoutMs: 20 }), isStatus(Code.DeadlineExceeded));
  assert.equal(await server.responseCancelled, true);
});

test("aborting the signal cancels a call and keeps the reason as cause", async () => {
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const controller = new AbortController();
  const call = echo.unary({}, { signal: controller.signal });
  await turn();
  controller.abort(new Error("user stopped"));

  await assert.rejects(call, (error) => {
    assert.ok(isStatus(Code.Cancelled)(error));
    assert.equal(error.cause.message, "user stopped");
    return true;
  });
  await transport.settled();
});

test("an abort reason that is a status error is reported as is", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const controller = new AbortController();
  const call = echo.unary({}, { signal: controller.signal });
  await turn();
  const reason = new TrevRpcError(Code.Aborted, "superseded");
  controller.abort(reason);
  await assert.rejects(call, (error) => error === reason);
});

test("an already aborted signal never opens a stream", async () => {
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  const echo = new EchoClient(transport);
  await assert.rejects(echo.unary({}, { signal: AbortSignal.abort() }), isStatus(Code.Cancelled));
  assert.equal(transport.opened, 0);
});

test("a stream that finishes opening after the call was cancelled is reset", async () => {
  const opening = deferred();
  const requests = new TransformStream();
  const responses = new TransformStream();
  const controller = new AbortController();
  const call = new EchoClient({ createBidirectionalStream: () => opening.promise }).unary(
    {},
    { signal: controller.signal },
  );
  await turn();
  controller.abort();
  await assert.rejects(call, isStatus(Code.Cancelled));

  opening.resolve({ readable: responses.readable, writable: requests.writable });
  await turn();
  await assert.rejects(requests.readable.getReader().read()); // request side reset
  await assert.rejects(responses.writable.getWriter().write(new Uint8Array(1))); // reads stopped
});

test("failures to open a stream are unavailable", async () => {
  const failing = [
    { createBidirectionalStream: () => Promise.reject(new Error("session gone")) },
    {
      createBidirectionalStream() {
        throw new Error("sync failure");
      },
    },
  ];
  for (const transport of failing) {
    await assert.rejects(new EchoClient(transport).unary({}), isStatus(Code.Unavailable));
  }
});

test("streams that fail mid-response are unavailable", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.reset(new Error("connection lost"));
  });
  await assert.rejects(echo.unary({}), (error) => {
    assert.ok(isStatus(Code.Unavailable)(error));
    assert.match(error.statusMessage, /connection lost/);
    return true;
  });
});

test("a stream that ends before the response is unavailable", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.finish();
  });
  await assert.rejects(echo.unary({}), isStatus(Code.Unavailable));
});

test("malformed or oversized responses fail the call", async () => {
  const cases = [
    [Code.InvalidArgument, new Uint8Array([0, 0, 0, 2, 0x0a, 0xff])],
    [Code.ResourceExhausted, new Uint8Array([0, 0, 1, 0])],
  ];
  for (const [code, bytes] of cases) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await stream.write(bytes);
    });
    await assert.rejects(echo.unary({}, { maxFrameSize: 100 }), isStatus(code));
  }
});

test("a response with invalid metadata is an internal error", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeResponse({ metadata: { "trevrpc-x": new Uint8Array() } });
  });
  await assert.rejects(echo.unary({}), isStatus(Code.Internal));
});

test("server streaming yields responses until the final status", async () => {
  const { echo, transport } = setup(echoHandler);
  const stream = await echo.serverStream({ message: "s", count: 3 });
  const received = [];
  for await (const response of stream) {
    received.push([response.message, response.index]);
  }
  assert.deepEqual(received, [
    ["s", 0],
    ["s", 1],
    ["s", 2],
  ]);
  assert.equal((await stream.status).code, Code.Ok);
  await transport.settled();
});

test("server streaming surfaces the final error status after the messages", async () => {
  const { echo } = setup(echoHandler);
  const stream = await echo.serverStream({
    message: "s",
    count: 2,
    failCode: Code.FailedPrecondition,
    failMessage: "stop",
  });
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.index, 0);
  assert.equal((await iterator.next()).value.index, 1);
  await assert.rejects(iterator.next(), isStatus(Code.FailedPrecondition, "stop"));
  await assert.rejects(iterator.next(), isStatus(Code.FailedPrecondition, "stop"));
  await assert.rejects(stream.status, isStatus(Code.FailedPrecondition, "stop"));
});

test("trailing metadata is available on the final status", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.Ok, "done", { checksum: new Uint8Array([7]) });
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  for await (const _ of stream) {
    assert.fail("no messages expected");
  }
  const status = await stream.status;
  assert.equal(status.message, "done");
  assert.deepEqual([...status.metadata.checksum], [7]);
});

test("leaving a response loop early cancels the call", async () => {
  let server;
  const { echo } = setup(async (stream) => {
    server = stream;
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "first" });
  });
  const stream = await echo.serverStream({});
  for await (const _ of stream) {
    break;
  }
  assert.equal(await server.responseCancelled, true);
  await assert.rejects(stream.status, isStatus(Code.Cancelled));
});

test("close() cancels with the given reason and is a no-op after completion", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const stream = await echo.serverStream({});
  const next = stream[Symbol.asyncIterator]().next();
  await stream.close(new Error("enough"));
  await assert.rejects(next, (error) => {
    assert.ok(isStatus(Code.Cancelled)(error));
    assert.equal(error.cause.message, "enough");
    return true;
  });

  const finished = setup(echoHandler);
  const complete = await finished.echo.serverStream({ count: 1 });
  for await (const _ of complete) {
    // drain
  }
  await complete.close();
  assert.equal((await complete.status).code, Code.Ok);
});

test("a deadline settles the status even while nobody is receiving", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const stream = await echo.serverStream({}, { timeoutMs: 20 });
  await assert.rejects(stream.status, isStatus(Code.DeadlineExceeded));
  await assert.rejects(stream[Symbol.asyncIterator]().next(), isStatus(Code.DeadlineExceeded));
});

test("aborting cancels a pending receive", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const controller = new AbortController();
  const stream = await echo.serverStream({}, { signal: controller.signal });
  const next = stream[Symbol.asyncIterator]().next();
  await turn();
  controller.abort();
  await assert.rejects(next, isStatus(Code.Cancelled));
});

test("a stream that ends without a final status is an internal error", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "only" });
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.message, "only");
  await assert.rejects(iterator.next(), isStatus(Code.Internal));
  await assert.rejects(stream.status, isStatus(Code.Internal));
});

test("unknown frame kinds and undecodable messages fail the stream", async () => {
  const frames = [
    [Code.InvalidArgument, encodeFrame(RpcStreamFrame, { kind: 9 })],
    [Code.InvalidArgument, encodeMessageFrame(new Uint8Array([0x0a, 0xff]))],
    [Code.Internal, encodeFrame(RpcStreamFrame, { kind: 1, metadata: { "trevrpc-x": [] } })],
  ];
  for (const [code, bytes] of frames) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await stream.write(bytes);
    });
    const stream = await echo.serverStream({});
    await assert.rejects(stream[Symbol.asyncIterator]().next(), isStatus(code));
    await assert.rejects(stream.status, isStatus(code));
  }
});

test("a failure while draining after the final status does not change the outcome", async () => {
  const finished = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "m" });
    await stream.writeStatus();
    await finished.promise;
    await stream.reset();
  });
  const stream = await echo.serverStream({});
  const received = [];
  for await (const response of stream) {
    received.push(response.message);
  }
  finished.resolve();
  await transport.settled();
  await turn();
  assert.deepEqual(received, ["m"]);
  assert.equal((await stream.status).code, Code.Ok);
});

test("data after the final status is ignored and the stream is cancelled", async () => {
  const cancelled = deferred();
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(
      concatBytes([
        encodeFrame(RpcStreamFrame, { kind: 1, status: 0 }),
        encodeMessageFrame(new Uint8Array([1])),
      ]),
    );
    cancelled.resolve(stream.responseCancelled);
  });
  const stream = await echo.serverStream({});
  const received = [];
  for await (const response of stream) {
    received.push(response);
  }
  assert.deepEqual(received, []);
  assert.equal(await cancelled.promise, true);
});

test("client streaming sends ordered messages and returns the single response", async () => {
  const { echo, transport } = setup(echoHandler);
  const call = await echo.clientStream();
  await Promise.all([call.send({ message: "a" }), call.send({ message: "b" })]);
  await call.send({ message: "c" });
  const response = await call.closeAndRecvWithResponse();
  assert.equal(response.message.message, "abc");
  assert.equal(response.message.index, 3);
  await transport.settled();
});

test("client streaming ends early on a final error status", async () => {
  const { echo } = setup(echoHandler);
  const call = await echo.clientStream();
  await call.send({ message: "a" });
  await call.send({ failCode: Code.PermissionDenied, failMessage: "no" });
  await assert.rejects(call.closeAndRecv(), isStatus(Code.PermissionDenied, "no"));
  await assert.rejects(call.send({ message: "late" }));
});

test("client streaming requires exactly one response message", async () => {
  for (const count of [0, 2]) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      for (let index = 0; index < count; index += 1) {
        await stream.writeMessage(EchoResponse, { index });
      }
      await stream.writeStatus();
    });
    const call = await echo.clientStream();
    await assert.rejects(call.closeAndRecv(), isStatus(Code.Internal), `${count} responses`);
  }
});

test("bidirectional streaming interleaves sends and receives", async () => {
  const { echo, transport } = setup(echoHandler);
  const call = await echo.bidi();
  for (const [index, message] of ["1", "2", "3"].entries()) {
    await call.send({ message });
    const response = await call.recv();
    assert.deepEqual([response.message, response.index], [message, index]);
  }
  await call.closeSend();
  assert.equal(await call.recv(), undefined);
  assert.equal((await call.status).code, Code.Ok);
  assert.equal(await call.recv(), undefined);
  await transport.settled();
});

test("bidirectional streaming reports a server error raised mid-conversation", async () => {
  const { echo } = setup(echoHandler);
  const call = await echo.bidi();
  await call.send({ message: "1" });
  assert.equal((await call.recv()).message, "1");
  await call.send({ failCode: Code.Aborted, failMessage: "abort" });
  await assert.rejects(call.recv(), isStatus(Code.Aborted, "abort"));
  await assert.rejects(call.status, isStatus(Code.Aborted, "abort"));
  await assert.rejects(call.send({ message: "late" }));
});

test("sending after the request stream was closed is a failed precondition", async () => {
  const { echo } = setup(echoHandler);
  const call = await echo.bidi();
  await call.closeSend();
  await call.closeSend();
  await assert.rejects(call.send({ message: "late" }), isStatus(Code.FailedPrecondition));
  assert.equal(await call.recv(), undefined);
});

test("a message that cannot be encoded does not break the call", async () => {
  const Nested = createRoot({
    nested: {
      t: {
        nested: {
          Inner: { fields: { id: { type: "uint32", id: 1 } } },
          Outer: { fields: { inner: { type: "Inner", id: 1 } } },
        },
      },
    },
  });
  const service = {
    fullName: "t.Service",
    methods: {
      put: {
        name: "Put",
        kind: "bidirectionalStreaming",
        inputType: "t.Outer",
        outputType: "t.Outer",
      },
    },
  };
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    for (let frame; (frame = await stream.readFrame()) !== null;) {
      await stream.write(encodeMessageFrame(frame.body));
    }
    await stream.writeStatus();
  });
  const call = await createServiceClient(transport, service, Nested).put();

  await assert.rejects(call.send({ inner: 5 }), isStatus(Code.InvalidArgument));
  await call.send({ inner: { id: 7 } });
  assert.equal((await call.recv()).inner.id, 7);
  await call.closeSend();
  assert.equal(await call.recv(), undefined);
});

test("concurrent receives are served in order", async () => {
  const { echo } = setup(echoHandler);
  const call = await echo.bidi();
  const receives = [call.recv(), call.recv(), call.recv()];
  for (const message of ["a", "b", "c"]) {
    await call.send({ message });
  }
  assert.deepEqual(
    (await Promise.all(receives)).map((response) => response.message),
    ["a", "b", "c"],
  );
  await call.close();
});

test("the response decides when the server stops reading the request", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.stopSending(new Error("not interested"));
    await stream.writeMessage(EchoResponse, { message: "answer" });
    await stream.writeStatus(Code.ResourceExhausted, "enough");
    await stream.finish();
  });
  const call = await echo.bidi();
  await turn();
  await assert.rejects(call.send({ message: "x" })); // the upload side is gone
  assert.equal((await call.recv()).message, "answer");
  await assert.rejects(call.recv(), isStatus(Code.ResourceExhausted, "enough"));
});

test("an upload failure is reported when no response explains it", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.stopSending(new Error("go away"));
    await stream.finish();
  });
  const call = await echo.bidi();
  await turn();
  await assert.rejects(call.send({ message: "x" }), isStatus(Code.Unavailable));
  await assert.rejects(call.recv(), isStatus(Code.Unavailable));
});

test("the unfinished request side is reset when the final status arrives", async () => {
  const requestReset = deferred();
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus();
    await stream.finish();
    requestReset.resolve(
      stream.readFrame().then(
        () => false,
        () => true,
      ),
    );
  });
  const call = await echo.bidi();
  assert.equal(await call.recv(), undefined);
  assert.equal(await requestReset.promise.then((promise) => promise), true);
});

test("service clients merge default and per-call options", async () => {
  const seen = [];
  const transport = fakeTransport(async (stream) => {
    const request = await stream.readRequest();
    seen.push(
      Object.fromEntries(
        Object.entries(request.metadata).map(([key, value]) => [key, text.decode(value)]),
      ),
    );
    await reply(stream, {});
  });
  const echo = new EchoClient(transport, { metadata: { Authorization: "base", trace: "t" } });

  await echo.unary({});
  await echo.unary(
    {},
    {
      metadata: new Map([
        ["authorization", "override"],
        ["request-id", "abc"],
      ]),
    },
  );
  await assert.rejects(echo.unary({}, { metadata: { "trevrpc-x": "no" } }));
  assert.deepEqual(seen, [
    { authorization: "base", trace: "t" },
    { authorization: "override", trace: "t", "request-id": "abc" },
  ]);
});

test("createServiceClient rejects unknown method kinds", () => {
  const service = {
    fullName: "trevrpc.interop.v1.Echo",
    methods: { odd: { name: "Unary", kind: "weird", inputType: "x", outputType: "y" } },
  };
  assert.throws(
    () => createServiceClient({}, service, { lookupType: () => ({}) }),
    isStatus(Code.Internal),
  );
});

test("the whole Echo service works against a fake server", async () => {
  const { echo } = setup(echoHandler);
  const big = "x".repeat(1 << 20);
  assert.equal((await echo.unary({ message: big })).message, big);
  assert.equal(
    (await echo.unary({ message: "hello" }, { metadata: { "interop-suffix": "!" } })).message,
    "hello!",
  );
  await assert.rejects(
    echo.unary({ failCode: Code.NotFound, failMessage: "missing" }),
    isStatus(Code.NotFound, "missing"),
  );
});
