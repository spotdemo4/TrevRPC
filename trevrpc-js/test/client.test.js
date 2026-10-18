import assert from "node:assert/strict";
import test from "node:test";

import { EchoClient, root } from "../build/interop/gen/interop.trevrpc.js";
import { createServiceClient } from "../src/client.js";
import {
  FrameType,
  encodeFrame,
  encodeHeadersFrame,
  encodeMessageFrame,
  encodeRequestFrame,
  encodeStatusFrame,
} from "../src/framing.js";
import { Code, TrevRpcError } from "../src/status.js";
import { RpcKind, createRoot } from "../src/wire.js";
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

/** Answers a unary call: one response message and an OK status. */
async function reply(stream, fields, trailers) {
  await stream.writeMessage(EchoResponse, fields);
  await stream.writeStatus(Code.Ok, "", trailers);
}

const bytes = (value) => new TextEncoder().encode(value);
const metadataText = (metadata) =>
  Object.fromEntries(Object.entries(metadata).map(([key, value]) => [key, text.decode(value)]));

test("unary calls send the request header and decode the response", async () => {
  const { echo, transport } = setup(async (stream) => {
    const request = await stream.readRequest();
    assert.equal(request.method, "/trevrpc.interop.v1.Echo/Unary");
    assert.equal(request.kind, RpcKind.Unary);
    assert.equal(text.decode(request.metadata.trace), "abc");
    assert.equal((await stream.readMessage(EchoRequest)).message, "hi");
    assert.equal(await stream.readFrame(), null, "request side is finished after the request");
    await stream.writeHeaders({ hdr: new Uint8Array([2]) });
    await reply(stream, { message: "hello", index: 3 }, { served: new Uint8Array([1]) });
    await stream.finish();
  });

  const response = await echo.unaryWithResponse({ message: "hi" }, { metadata: { Trace: "abc" } });
  assert.equal(response.message.message, "hello");
  assert.equal(response.message.index, 3);
  assert.deepEqual([...response.metadata.served], [1]);
  assert.deepEqual([...response.headers.hdr], [2]);
  await transport.settled();
});

test("unary calls fail with the status of a non-OK response", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.NotFound, "missing", { why: new Uint8Array([1]) });
  });

  await assert.rejects(echo.unary({ message: "x" }), (error) => {
    assert.ok(isStatus(Code.NotFound, "missing")(error));
    assert.deepEqual([...error.metadata.why], [1]);
    return true;
  });
});

test("unary calls return as soon as the response arrives", async () => {
  // The server never reads the request or finishes the stream; the final status alone decides.
  const { echo } = setup(async (stream) => {
    await reply(stream, { message: "early" });
  });
  const response = await echo.unaryWithResponse({ message: "x" });
  assert.equal(response.message.message, "early");
  assert.deepEqual(response.headers, {});
  assert.deepEqual(response.metadata, {});
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

test("a stream that ends before the final status is an internal error", async () => {
  for (const write of [
    async () => {},
    (stream) => stream.writeHeaders(),
    (stream) => stream.writeMessage(EchoResponse, { message: "m" }),
  ]) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await write(stream);
      await stream.finish();
    });
    await assert.rejects(echo.unary({}), isStatus(Code.Internal));
  }
});

test("a stream that ends inside a frame header is unavailable", async () => {
  const partial = [
    [0x40], // first byte of a two-byte type
    [0x21], // a type without its length
    [0x21, 0x40], // half of a two-byte length
    [0x00, 0x05, 1], // payload cut short
  ];
  for (const wire of partial) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await stream.write(Uint8Array.from(wire));
      await stream.finish();
    });
    await assert.rejects(echo.unary({}), isStatus(Code.Unavailable), wire.join(","));
  }
});

test("malformed or oversized responses fail the call", async () => {
  const cases = [
    [Code.InvalidArgument, Uint8Array.of(FrameType.Status, 2, 0x0a, 0xff)],
    [Code.InvalidArgument, Uint8Array.of(FrameType.Headers, 2, 0x0a, 0xff)],
    [Code.ResourceExhausted, Uint8Array.of(FrameType.Message, 0x40, 200)],
    [Code.ResourceExhausted, Uint8Array.of(0x21, 0x40, 200)], // unknown frames are limited too
    [
      Code.ResourceExhausted,
      Uint8Array.of(FrameType.Message, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
    ],
  ];
  for (const [code, wire] of cases) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await stream.write(wire);
    });
    await assert.rejects(echo.unary({}, { maxFrameSize: 100 }), isStatus(code));
  }
});

test("a response with invalid metadata is an internal error", async () => {
  const invalid = { "trevrpc-x": new Uint8Array() };
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, {});
    await stream.writeStatus(Code.Ok, "", invalid);
  });
  await assert.rejects(echo.unary({}), isStatus(Code.Internal));

  const headers = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeHeaders(invalid);
  });
  await assert.rejects(headers.echo.unary({}), isStatus(Code.Internal));
});

test("unary and server-streaming calls send the request message with the REQUEST frame", async () => {
  for (const [method, kind] of [
    ["unary", RpcKind.Unary],
    ["serverStream", RpcKind.ServerStreaming],
  ]) {
    const first = deferred();
    const { echo } = setup(async (stream) => {
      first.resolve((await stream.reader.read()).value);
      await stream.writeStatus(Code.Unimplemented, "stop");
    });
    await assert.rejects(
      (async () => {
        const result = await echo[method]({ message: "hi" });
        for await (const _ of result) {
          // drain
        }
      })(),
      isStatus(Code.Unimplemented),
    );
    const expected = encodeRequestFrame(
      {
        method: `/trevrpc.interop.v1.Echo/${method === "unary" ? "Unary" : "ServerStream"}`,
        kind,
      },
      EchoRequest.encode({ message: "hi" }).finish(),
    );
    assert.deepEqual(await first.promise, expected, method);
  }
});

test("streaming-request calls send REQUEST alone and their messages later", async () => {
  const first = deferred();
  const { echo } = setup(async (stream) => {
    first.resolve((await stream.reader.read()).value);
    await stream.readMessage(EchoRequest);
    await stream.writeStatus();
  });
  const call = await echo.bidi();
  await call.send({ message: "a" });
  const chunk = await first.promise;
  assert.equal(chunk[0], FrameType.Request);
  assert.equal(chunk.byteLength, 2 + chunk[1], "no message in the first write");
  await call.close();
});

test("a unary call needs exactly one response message", async () => {
  for (const count of [0, 2]) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      for (let index = 0; index < count; index += 1) {
        await stream.writeMessage(EchoResponse, { index });
      }
      await stream.writeStatus();
    });
    await assert.rejects(echo.unary({}), isStatus(Code.Internal), `${count} responses`);
  }
});

test("a lone status fails a unary call before any message", async () => {
  const { echo } = setup(async (stream) => {
    await stream.writeStatus(Code.Unimplemented, "no such method");
  });
  await assert.rejects(echo.unary({}), isStatus(Code.Unimplemented, "no such method"));
});

test("unary response headers are returned next to the trailing metadata", async () => {
  const { echo } = setup(echoHandler);
  const marked = await echo.unaryWithResponse(
    { message: "hi" },
    { metadata: { "interop-header": "h" } },
  );
  assert.deepEqual(metadataText(marked.headers), { "interop-header": "h" });
  assert.deepEqual(metadataText(marked.metadata), { "interop-trailer": "h" });

  const plain = await echo.unaryWithResponse({ message: "hi" });
  assert.deepEqual(plain.headers, {});
  assert.deepEqual(plain.metadata, {});
});

test("headers are visible before the first streamed message", async () => {
  const release = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeHeaders({ early: bytes("yes") });
    await release.promise;
    await stream.writeMessage(EchoResponse, { message: "late" });
    await stream.writeStatus();
  });
  const stream = await echo.serverStream({});
  const headers = await stream.headers; // nobody receives yet
  assert.deepEqual(metadataText(headers), { early: "yes" });
  release.resolve();

  const received = [];
  for await (const response of stream) {
    received.push(response.message);
  }
  assert.deepEqual(received, ["late"]);
  assert.equal(await stream.headers, headers);
  await transport.settled();
});

test("headers resolve empty when the server sends none", async () => {
  const messages = setup(echoHandler);
  const stream = await messages.echo.serverStream({ message: "s", count: 1 });
  assert.deepEqual(await stream.headers, {});
  assert.equal((await stream[Symbol.asyncIterator]().next()).value.message, "s");

  // The first frame may be a bare status, and headers still settle.
  const empty = await setup(echoHandler).echo.serverStream({ count: 0 });
  assert.deepEqual(await empty.headers, {});
  for await (const _ of empty) {
    assert.fail("no messages expected");
  }
  assert.equal((await empty.status).code, Code.Ok);

  const failed = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.NotFound, "nope");
  });
  const lone = await failed.echo.serverStream({});
  assert.deepEqual(await lone.headers, {});
  await assert.rejects(lone[Symbol.asyncIterator]().next(), isStatus(Code.NotFound, "nope"));
});

test("headers are read when the first receive comes first", async () => {
  const { echo } = setup(echoHandler);
  const stream = await echo.serverStream(
    { message: "s", count: 1 },
    { metadata: { "interop-header": "h" } },
  );
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.message, "s");
  assert.deepEqual(metadataText(await stream.headers), { "interop-header": "h" });
  assert.equal((await iterator.next()).done, true);
  assert.deepEqual(metadataText((await stream.status).metadata), { "interop-trailer": "h" });
});

test("bidirectional and client-streaming calls expose headers too", async () => {
  const { echo } = setup(echoHandler);
  const call = await echo.bidi({ metadata: { "interop-header": "b" } });
  await call.send({ message: "1" });
  assert.deepEqual(metadataText(await call.headers), { "interop-header": "b" });
  assert.equal((await call.recv()).message, "1");
  await call.closeSend();
  assert.equal(await call.recv(), undefined);
  assert.deepEqual(metadataText((await call.status).metadata), { "interop-trailer": "b" });

  const upload = await echo.clientStream({ metadata: { "interop-header": "c" } });
  await upload.send({ message: "x" });
  const response = await upload.closeAndRecvWithResponse();
  assert.equal(response.message.message, "x");
  assert.deepEqual(metadataText(response.headers), { "interop-header": "c" });
  assert.deepEqual(metadataText(response.metadata), { "interop-trailer": "c" });
});

test("headers reject with the call error when the call ends first", async () => {
  const reset = setup(async (stream) => {
    await stream.readRequest();
    await stream.reset(new Error("connection lost"));
  });
  const lost = await reset.echo.serverStream({});
  await assert.rejects(lost.headers, isStatus(Code.Unavailable));

  const silent = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const timed = await silent.echo.serverStream({}, { timeoutMs: 20 });
  await assert.rejects(timed.headers, isStatus(Code.DeadlineExceeded));

  const eof = setup(async (stream) => {
    await stream.readRequest();
    await stream.finish();
  });
  await assert.rejects((await eof.echo.serverStream({})).headers, isStatus(Code.Internal));

  const stopped = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const cancelled = await stopped.echo.bidi();
  const pending = cancelled.headers;
  await cancelled.close();
  await assert.rejects(pending, isStatus(Code.Cancelled));
});

test("an unobserved rejected headers promise causes no unhandled rejection", async () => {
  const unhandled = [];
  const listener = (reason) => unhandled.push(reason);
  process.on("unhandledRejection", listener);
  try {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await stream.reset(new Error("connection lost"));
    });
    const quiet = await echo.serverStream({});
    await assert.rejects(quiet[Symbol.asyncIterator]().next(), isStatus(Code.Unavailable));
    const held = (await echo.bidi()).headers; // taken, never awaited
    await turn(10);
    assert.ok(held instanceof Promise);
    await new Promise((resolve) => setTimeout(resolve, 20));
  } finally {
    process.off("unhandledRejection", listener);
  }
  assert.deepEqual(unhandled, []);
});

test("unknown frames are skipped everywhere in a response", async () => {
  const unknown = (type) => encodeFrame(type, bytes("zz"));
  const message = (index) => encodeMessageFrame(EchoResponse.encode({ index }).finish());
  const wire = concatBytes([
    unknown(0x21),
    encodeHeadersFrame({ metadata: { h: bytes("1") } }),
    unknown(0x40),
    message(1),
    unknown(2n ** 62n - 1n),
    message(2),
    unknown(0x5f),
    encodeStatusFrame({ code: Code.Ok, metadata: { t: bytes("2") } }),
    unknown(0x7e),
    unknown(4),
  ]);

  for (const size of [1, 5, wire.byteLength]) {
    const { echo, transport } = setup(async (stream) => {
      await stream.readRequest();
      for (let offset = 0; offset < wire.byteLength; offset += size) {
        await stream.write(wire.subarray(offset, offset + size));
      }
      await stream.finish();
    });
    const stream = await echo.serverStream({});
    const received = [];
    for await (const response of stream) {
      received.push(response.index);
    }
    assert.deepEqual(received, [1, 2]);
    assert.deepEqual(metadataText(await stream.headers), { h: "1" });
    assert.deepEqual(metadataText((await stream.status).metadata), { t: "2" });
    await transport.settled();
  }

  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(
      concatBytes([unknown(0x21), message(7), unknown(0x40), encodeStatusFrame({ code: 0 })]),
    );
  });
  const response = await echo.unary({});
  assert.equal(response.index, 7);
});

test("a second HEADERS frame fails the call", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(
      concatBytes([encodeHeadersFrame({}), encodeHeadersFrame({ metadata: { b: bytes("2") } })]),
    );
  });
  const stream = await echo.serverStream({});
  await assert.rejects(stream[Symbol.asyncIterator]().next(), isStatus(Code.Internal));
  await assert.rejects(stream.status, isStatus(Code.Internal));
  assert.deepEqual(await stream.headers, {}, "the first HEADERS frame was already delivered");

  const unary = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeHeaders({});
    await stream.writeHeaders({});
    await stream.writeMessage(EchoResponse, {});
    await stream.writeStatus();
  });
  await assert.rejects(unary.echo.unary({}), isStatus(Code.Internal));
});

test("HEADERS after a MESSAGE fails the call", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "m" });
    await stream.writeHeaders({});
  });
  const stream = await echo.serverStream({});
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.message, "m");
  await assert.rejects(iterator.next(), isStatus(Code.Internal));
  await assert.rejects(stream.status, isStatus(Code.Internal));

  const unary = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "m" });
    await stream.writeHeaders({});
    await stream.writeStatus();
  });
  await assert.rejects(unary.echo.unary({}), isStatus(Code.Internal));
});

test("request frames are not valid on a response stream", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(encodeRequestFrame({ method: "/s/m" }));
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

test("misplaced frames and undecodable messages fail the stream", async () => {
  const frames = [
    [Code.InvalidArgument, encodeFrame(FrameType.Status, new Uint8Array([0x0a, 0xff]))],
    [Code.InvalidArgument, encodeMessageFrame(new Uint8Array([0x0a, 0xff]))],
    [Code.Internal, encodeStatusFrame({ metadata: { "trevrpc-x": [] } })],
    [Code.Internal, encodeRequestFrame({ method: "/s/m" })],
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

test("a known frame after the final status is ignored and the stream is cancelled", async () => {
  const cancelled = deferred();
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(
      concatBytes([encodeStatusFrame({ code: 0 }), encodeMessageFrame(new Uint8Array([1]))]),
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
  assert.equal((await stream.status).code, Code.Ok);
});

test("unknown frames after the final status are drained without cancelling", async () => {
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus();
    await stream.write(encodeFrame(0x21, bytes("grease")));
    await turn();
    await stream.write(encodeFrame(2n ** 62n - 1n, bytes("late"))); // fails if the client cancelled
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  for await (const _ of stream) {
    assert.fail("no messages expected");
  }
  await transport.settled();
  assert.equal((await stream.status).code, Code.Ok);
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
      await stream.write(encodeMessageFrame(frame.payload));
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
    seen.push(metadataText(request.metadata));
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

test("the client sends the full method path and the Echo fake server routes on it", async () => {
  const unaryOf = (service) =>
    createServiceClient(fakeTransport(echoHandler), service, root).unary({ message: "hi" });
  const method = (name) => ({
    unary: {
      name,
      kind: "unary",
      inputType: "trevrpc.interop.v1.EchoRequest",
      outputType: "trevrpc.interop.v1.EchoResponse",
    },
  });

  const echoService = { fullName: "trevrpc.interop.v1.Echo", methods: method("Unary") };
  assert.equal((await unaryOf(echoService)).message, "hi");

  // A well-formed path without a handler is Unimplemented, for an unknown method or service.
  const missingMethod = { fullName: "trevrpc.interop.v1.Echo", methods: method("Missing") };
  await assert.rejects(
    unaryOf(missingMethod),
    isStatus(Code.Unimplemented, "unknown method /trevrpc.interop.v1.Echo/Missing"),
  );
  const missingService = { fullName: "trevrpc.interop.v1.Other", methods: method("Unary") };
  await assert.rejects(unaryOf(missingService), isStatus(Code.Unimplemented));

  // Anything that is not exactly /<service>/<method> is InvalidArgument.
  for (const [fullName, name] of [
    ["", "Unary"],
    ["trevrpc.interop.v1.Echo", ""],
    ["a/b", "Unary"],
    ["trevrpc.interop.v1.Echo", "Un/ary"],
  ]) {
    await assert.rejects(
      unaryOf({ fullName, methods: method(name) }),
      isStatus(Code.InvalidArgument),
      `${fullName}/${name}`,
    );
  }
});

// Call termination: how a call ends when the server answers before the upload is done.

/** Fails the test if the promise does not settle in time; the caller is stuck otherwise. */
async function within(milliseconds, promise, what = "operation") {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${what} did not finish in ${milliseconds} ms`)),
      milliseconds,
    );
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/** Sends until a send fails, which must happen long before the sends run out. */
async function sendUntilRejected(call, limit = 500) {
  for (let index = 0; index < limit; index += 1) {
    try {
      await call.send({ message: "x".repeat(64) });
    } catch (error) {
      return error;
    }
  }
  assert.fail(`${limit} sends did not fail`);
}

const largeDeadline = 60_000;

test("a client-streaming upload fails with an early error status although the server never reads", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.NotFound, "gone", { why: new Uint8Array([9]) });
    // Neither reads the request side nor stops it.
  });
  const call = await echo.clientStream({ timeoutMs: largeDeadline });
  try {
    const error = await within(1000, sendUntilRejected(call), "sends");
    assert.ok(isStatus(Code.NotFound, "gone")(error), String(error));
    assert.deepEqual([...error.metadata.why], [9]);
    await assert.rejects(call.send({ message: "late" }), isStatus(Code.NotFound, "gone"));
    await assert.rejects(call.closeAndRecv(), isStatus(Code.NotFound, "gone"));
  } finally {
    await call.close();
  }
});

test("a bidirectional upload fails with an early error status although nobody receives", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeHeaders({ h: new Uint8Array([1]) });
    await stream.writeStatus(Code.ResourceExhausted, "enough");
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  try {
    const error = await within(1000, sendUntilRejected(call), "sends");
    assert.ok(isStatus(Code.ResourceExhausted, "enough")(error), String(error));
    await assert.rejects(within(1000, call.status), isStatus(Code.ResourceExhausted, "enough"));
    await assert.rejects(call.recv(), isStatus(Code.ResourceExhausted, "enough"));
    assert.deepEqual([...(await call.headers).h], [1]);
    await call.closeSend(); // the request side is gone; closing it is not an error
  } finally {
    await call.close();
  }
});

test("a blocked send fails when the server's error status arrives", async () => {
  const release = deferred();
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await release.promise;
    await stream.writeStatus(Code.Aborted, "late");
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  try {
    // Fill the stream's buffers so that the next sends are blocked on backpressure.
    const sends = [];
    for (let index = 0; index < 100; index += 1) {
      sends.push(call.send({ message: "y" }).catch((error) => error));
    }
    await turn();
    release.resolve();
    const errors = await within(1000, Promise.all(sends), "blocked sends");
    assert.ok(errors.some((error) => isStatus(Code.Aborted, "late")(error)));
    assert.ok(errors.every((error) => error === undefined || isStatus(Code.Aborted)(error)));
  } finally {
    await call.close();
  }
});

test("a send blocked on backpressure fails promptly when the call is cancelled", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.responseCancelled;
  });
  const controller = new AbortController();
  const call = await echo.bidi({ signal: controller.signal });
  const sends = [];
  for (let index = 0; index < 100; index += 1) {
    sends.push(call.send({ message: "z" }).catch((error) => error));
  }
  await turn();
  controller.abort();
  const errors = await within(1000, Promise.all(sends), "blocked sends");
  assert.ok(errors.some((error) => isStatus(Code.Cancelled)(error)));
});

test("a send after an early OK status reports that the call is already over", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "enough", index: 7 });
    await stream.writeStatus(Code.Ok, "done", { t: new Uint8Array([3]) });
  });
  const call = await echo.clientStream({ timeoutMs: largeDeadline });
  const error = await within(1000, sendUntilRejected(call), "sends");
  assert.ok(isStatus(Code.Cancelled)(error), String(error));
  assert.match(error.statusMessage, /server completed the call/u);
  // The outcome is the server's OK response; the failed send does not change it.
  const response = await call.closeAndRecvWithResponse();
  assert.equal(response.message.message, "enough");
  assert.deepEqual([...response.metadata.t], [3]);
});

test("an early OK status ends a bidirectional upload without an error status", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.Ok);
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  try {
    const error = await within(1000, sendUntilRejected(call), "sends");
    assert.ok(isStatus(Code.Cancelled)(error), String(error));
    assert.equal(await call.recv(), undefined);
    assert.equal((await call.status).code, Code.Ok);
  } finally {
    await call.close();
  }
});

test("the server's status decides when the deadline expires after STATUS but before the end of the stream", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.PermissionDenied, "no");
    // The response stream is never finished.
  });
  const call = await echo.clientStream({ timeoutMs: 30 });
  await new Promise((resolve) => setTimeout(resolve, 100)); // past the deadline
  await assert.rejects(call.send({ message: "x" }), isStatus(Code.PermissionDenied, "no"));
  await assert.rejects(call.closeAndRecv(), isStatus(Code.PermissionDenied, "no"));

  const bidi = await setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.Ok, "fine");
  }).echo.bidi({ timeoutMs: 30 });
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(await bidi.recv(), undefined);
  assert.equal((await bidi.status).message, "fine");
});

test("the server's status decides when the call is aborted after STATUS but before the end of the stream", async () => {
  const controller = new AbortController();
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.DataLoss, "lost");
  });
  const call = await echo.bidi({ signal: controller.signal });
  await assert.rejects(call.status, isStatus(Code.DataLoss, "lost"));
  controller.abort();
  await assert.rejects(call.recv(), isStatus(Code.DataLoss, "lost"));
  await assert.rejects(call.send({ message: "x" }), isStatus(Code.DataLoss, "lost"));
});

test("a response-side transport error after the upload stopped is what send reports", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.stopSending(new Error("stop requested"));
    await stream.reset(new Error("connection lost"));
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  try {
    await turn();
    const error = await within(1000, sendUntilRejected(call), "sends");
    assert.ok(isStatus(Code.Unavailable)(error), String(error));
    assert.match(error.statusMessage, /connection lost/u);
    await assert.rejects(call.recv(), (recvError) => recvError === error);
  } finally {
    await call.close();
  }
});

test("send reports the status that follows the server stopping the request side", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.stopSending(new Error("stop requested"));
    await turn(10);
    await stream.writeStatus(Code.ResourceExhausted, "too much");
    await stream.finish();
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  try {
    await assert.rejects(
      within(1000, sendUntilRejected(call), "sends").then((error) => {
        throw error;
      }),
      isStatus(Code.ResourceExhausted, "too much"),
    );
  } finally {
    await call.close();
  }
});

test("responses before a transport error are delivered in order", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponse, { message: "a" });
    await stream.writeMessage(EchoResponse, { message: "b" });
    await turn();
    await stream.reset(new Error("connection lost"));
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  await call.send({ message: "x" }).catch(() => {});
  await turn(10);
  assert.equal((await call.recv()).message, "a");
  assert.equal((await call.recv()).message, "b");
  await assert.rejects(call.recv(), isStatus(Code.Unavailable));
  await assert.rejects(call.status, isStatus(Code.Unavailable));
});

test("responses are read ahead only up to a bound while the upload runs", async () => {
  const total = 2000;
  let written = 0;
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    for (let index = 0; index < total; index += 1) {
      await stream.writeMessage(EchoResponse, { message: "r", index });
      written += 1;
    }
    await stream.writeStatus();
    await stream.finish();
  });
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  await call.send({ message: "x" });
  await turn(20);
  assert.ok(written < 200, `server wrote ${written} responses to a client that was not receiving`);
  let received = 0;
  for (; ; received += 1) {
    const response = await call.recv();
    if (response === undefined) {
      break;
    }
    assert.equal(response.index, received);
  }
  assert.equal(received, total);
  await transport.settled();
});
