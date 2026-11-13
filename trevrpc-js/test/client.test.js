import assert from "node:assert/strict";
import test from "node:test";

import { Echo, EchoRequestSchema, EchoResponseSchema } from "../build/interop/gen/interop_pb.js";
import { createClient } from "../src/client.js";
import { encodeMessage } from "../src/compression.js";
import {
  FrameType,
  encodeAnswerFrame,
  encodeFrame,
  encodeHeadersFrame,
  encodeMessageFrame,
  encodeRequestFrame,
  encodeStatusFrame,
  marshalMessage,
} from "../src/framing.js";
import { CancelledStreamErrorCode, Code, TrevRpcError } from "../src/status.js";
import { RpcKind } from "../src/wire.js";
import { echoHandler } from "./echo-server.js";
import { concatBytes, deferred, fakeTransport, turn } from "./support.js";

const text = new TextDecoder();

/** An Echo client whose streams are served by handler; returns the client and the transport. */
function setup(handler, options) {
  const transport = fakeTransport(handler);
  return { transport, echo: createClient(Echo, transport, options) };
}

/** Call options whose response callbacks record the headers and trailing metadata into seen. */
function captureResponse(seen) {
  return {
    onResponseHeaders: (headers) => {
      seen.headers = headers;
    },
    onResponseMetadata: (metadata) => {
      seen.metadata = metadata;
    },
  };
}

const isStatus = (code, message) => (error) =>
  error instanceof TrevRpcError &&
  error.code === code &&
  (message === undefined || error.statusMessage === message);

/** Answers a unary call: one response message and an OK status. */
async function reply(stream, fields, trailers) {
  await stream.writeMessage(EchoResponseSchema, fields);
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
    assert.equal((await stream.readMessage(EchoRequestSchema)).message, "hi");
    assert.equal(await stream.readFrame(), null, "request side is finished after the request");
    await stream.writeHeaders({ hdr: new Uint8Array([2]) });
    await reply(stream, { message: "hello", index: 3 }, { served: new Uint8Array([1]) });
    await stream.finish();
  });

  const seen = {};
  const response = await echo.unary(
    { message: "hi" },
    { metadata: { Trace: "abc" }, ...captureResponse(seen) },
  );
  assert.equal(response.message, "hello");
  assert.equal(response.index, 3);
  assert.deepEqual([...seen.metadata.served], [1]);
  assert.deepEqual([...seen.headers.hdr], [2]);
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

test("status errors carry the details of the status and decode them on request", async () => {
  const detail = marshalMessage(EchoResponseSchema, { message: "why" });
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(
      encodeStatusFrame({
        code: Code.InvalidArgument,
        message: "bad",
        details: [
          { type: "other.Type", value: Uint8Array.of(1) },
          { type: EchoResponseSchema.typeName, value: detail },
        ],
      }),
    );
  });

  await assert.rejects(echo.unary({ message: "x" }), (error) => {
    assert.ok(isStatus(Code.InvalidArgument, "bad")(error));
    assert.equal(error.details.length, 2);
    assert.deepEqual(
      error.findDetails(EchoResponseSchema).map((response) => response.message),
      ["why"],
    );
    assert.deepEqual(error.findDetails(EchoRequestSchema), []);
    return true;
  });
});

test("status errors read the retry delay of a google.rpc.RetryInfo detail", async () => {
  const retryInfo = (...bytes) => ({ type: "google.rpc.RetryInfo", value: Uint8Array.from(bytes) });
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(
      encodeStatusFrame({
        code: Code.ResourceExhausted,
        message: "slow down",
        // The golden RetryInfo{retry_delay: Duration{seconds: 1, nanos: 2}} of every runtime,
        // after details to ignore.
        details: [
          { type: "other.Type", value: Uint8Array.of(0x0a, 0x02, 0x08, 0x09) },
          retryInfo(0x0a, 0x09),
          retryInfo(0x0a, 0x04, 0x08, 0x01, 0x10, 0x02),
        ],
      }),
    );
  });

  await assert.rejects(echo.unary({ message: "x" }), (error) => {
    assert.ok(isStatus(Code.ResourceExhausted, "slow down")(error));
    assert.equal(error.retryDelayMs, 1000.000002);
    return true;
  });
});

test("retry delays follow the protobuf rules of google.rpc.RetryInfo", () => {
  const delay = (...bytes) =>
    new TrevRpcError(
      Code.ResourceExhausted,
      "",
      {},
      {
        details: [{ type: "google.rpc.RetryInfo", value: Uint8Array.from(bytes) }],
      },
    ).retryDelayMs;
  const hex = (text) => text.match(/../g)?.map((pair) => Number.parseInt(pair, 16)) ?? [];
  const cases = [
    ["0a02081e", 30_000],
    ["0a061080cab5ee01", 500],
    ["0a0808011080cab5ee01", 1500],
    ["0a00", 0],
    // Unknown fields are skipped, at either level, and the last value wins.
    ["1a036162630a04080110022000", 1000.000002],
    ["0a061a0008011002", 1000.000002],
    ["0a02080a0a02080b", 11_000],
    // No delay, a malformed one, or an invalid one.
    ["", undefined],
    ["1a03616263", undefined],
    ["0a", undefined],
    ["0a040801", undefined],
    ["0a0108", undefined],
    ["0801", undefined],
    ["0a050d00000000", undefined],
    ["0a0b08ffffffffffffffffff01", undefined],
    ["0a0d080110ffffffffffffffffff01", undefined],
    ["0a080801108094ebdc03", undefined],
  ];
  for (const [text, expected] of cases) {
    assert.equal(delay(...hex(text)), expected, text);
  }
  assert.equal(new TrevRpcError(Code.NotFound, "missing").retryDelayMs, undefined);
});

test("unary calls return as soon as the response arrives", async () => {
  // The server never reads the request or finishes the stream; the final status alone decides.
  const { echo } = setup(async (stream) => {
    await reply(stream, { message: "early" });
  });
  const seen = {};
  const response = await echo.unary({ message: "x" }, captureResponse(seen));
  assert.equal(response.message, "early");
  assert.deepEqual(seen.headers, {});
  assert.deepEqual(seen.metadata, {});
});

test("invalid requests fail before a stream is opened", async () => {
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  const echo = createClient(Echo, transport);

  await assert.rejects(
    echo.unary({}, { metadata: { "trevrpc-reserved": "x" } }),
    isStatus(Code.InvalidArgument),
  );
  await assert.rejects(echo.unary({}, { maxFrameSize: 4 }), isStatus(Code.ResourceExhausted));
  assert.equal(transport.opened, 0);
});

/**
 * A request validator that records the types it is asked about and rejects request messages whose
 * `message` is "bad" with what `reject` returns.
 */
function rejectBad(reject = () => new TrevRpcError(Code.InvalidArgument, "bad request")) {
  const asked = [];
  const validator = {
    asked,
    validateFunc(schema) {
      asked.push(schema.typeName);
      if (schema.typeName !== EchoRequestSchema.typeName) {
        return undefined;
      }
      return (message) => {
        assert.equal(message.$typeName, EchoRequestSchema.typeName);
        if (message.message === "bad") {
          throw reject();
        }
      };
    },
  };
  return validator;
}

test("the validator is asked once per method for its request type", () => {
  const validator = rejectBad();
  createClient(
    Echo,
    fakeTransport(() => {}),
    { validator },
  );
  assert.deepEqual(
    validator.asked,
    Echo.methods.map((method) => method.input.typeName),
  );
});

test("requests the validator rejects fail with its error before a stream is opened", async () => {
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  const ends = [];
  const echo = createClient(Echo, transport, {
    validator: rejectBad(),
    interceptors: [() => (end) => ends.push(end.code)],
  });

  await assert.rejects(
    echo.unary({ message: "bad" }),
    isStatus(Code.InvalidArgument, "bad request"),
  );
  await assert.rejects(echo.serverStream({ message: "bad" }), isStatus(Code.InvalidArgument));
  assert.equal(transport.opened, 0);
  // The interceptors ran and saw the call fail.
  assert.deepEqual(ends, [Code.InvalidArgument, Code.InvalidArgument]);
});

test("requests the validator accepts are sent", async () => {
  const { echo, transport } = setup(echoHandler, { validator: rejectBad() });
  assert.equal((await echo.unary({ message: "good" })).message, "good");
  await transport.settled();
});

test("a validator's other errors fail requests with InvalidArgument", async () => {
  const thrown = new RangeError("out of range");
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  const echo = createClient(Echo, transport, { validator: rejectBad(() => thrown) });
  await assert.rejects(echo.unary({ message: "bad" }), (error) => {
    assert.ok(isStatus(Code.InvalidArgument, "out of range")(error));
    assert.equal(error.cause, thrown);
    return true;
  });
});

test("requests that cannot be encoded fail before the validator sees them", async () => {
  const validator = {
    validateFunc: () => () => assert.fail("the validator must not see it"),
  };
  const echo = createClient(
    Echo,
    fakeTransport(() => {}),
    { validator },
  );
  await assert.rejects(echo.unary({ count: -1 }), isStatus(Code.InvalidArgument));
});

test("a rejected send is not sent and the call goes on", async () => {
  const { echo, transport } = setup(echoHandler, { validator: rejectBad() });
  const call = await echo.clientStream();
  await call.send({ message: "a" });
  await assert.rejects(
    call.send({ message: "bad" }),
    isStatus(Code.InvalidArgument, "bad request"),
  );
  await call.send({ message: "b" });
  const response = await call.closeAndRecv();
  assert.equal(response.message, "ab");
  assert.equal(response.index, 2);
  await transport.settled();
});

test("createClient rejects validators that are not request validators", () => {
  const transport = fakeTransport(() => {});
  for (const validator of [{}, () => {}, { validateFunc: () => 1 }]) {
    assert.throws(
      () => createClient(Echo, transport, { validator }),
      isStatus(Code.InvalidArgument),
    );
  }
  const broken = new Error("broken rules");
  assert.throws(
    () =>
      createClient(Echo, transport, {
        validator: {
          validateFunc() {
            throw broken;
          },
        },
      }),
    (error) => error === broken,
  );
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

test("status errors are tagged without an own property", () => {
  const error = new TrevRpcError(Code.NotFound, "missing");
  assert.equal(error._tag, "TrevRpcError");
  assert.equal(Object.hasOwn(error, "_tag"), false);
  assert.deepEqual(Object.keys(error).sort(), [
    "code",
    "details",
    "metadata",
    "name",
    "statusMessage",
  ]);
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
  const echo = createClient(Echo, transport);
  await assert.rejects(echo.unary({}, { signal: AbortSignal.abort() }), isStatus(Code.Cancelled));
  assert.equal(transport.opened, 0);
});

test("a stream that finishes opening after the call was cancelled is reset", async () => {
  const opening = deferred();
  const requests = new TransformStream();
  const responses = new TransformStream();
  const controller = new AbortController();
  const call = createClient(Echo, { createBidirectionalStream: () => opening.promise }).unary(
    {},
    { signal: controller.signal },
  );
  await turn();
  controller.abort();
  await assert.rejects(call, isStatus(Code.Cancelled));

  opening.resolve({ readable: responses.readable, writable: requests.writable });
  await turn();
  // Both directions reset with the cancel code: the request side, and reads stopped.
  const reset = (error) => error.streamErrorCode === CancelledStreamErrorCode;
  await assert.rejects(requests.readable.getReader().read(), reset);
  await assert.rejects(responses.writable.getWriter().write(new Uint8Array(1)), reset);
});

test("a cancelled call resets both directions with the cancel code", async () => {
  // A plain abort reason would reset a WebTransport stream with code 0, which servers took for a
  // transport failure rather than a cancelled call.
  const events = [];
  const received = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.readFrame();
    received.resolve();
    const response = stream.writer.closed.catch((error) => {
      events.push(["response", error.streamErrorCode]);
    });
    const request = stream.readFrame().catch((error) => {
      events.push(["request", error.streamErrorCode]);
    });
    await Promise.all([response, request]);
  });
  const controller = new AbortController();
  const call = await echo.clientStream({ signal: controller.signal });
  await call.send({ message: "first" });
  await received.promise;
  controller.abort();
  await assert.rejects(call.closeAndRecv(), isStatus(Code.Cancelled));
  await transport.settled();
  assert.deepEqual(
    events.toSorted(([a], [b]) => a.localeCompare(b)),
    [
      ["request", CancelledStreamErrorCode],
      ["response", CancelledStreamErrorCode],
    ],
  );
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
    await assert.rejects(createClient(Echo, transport).unary({}), isStatus(Code.Unavailable));
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
    (stream) => stream.writeMessage(EchoResponseSchema, { message: "m" }),
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
    await stream.writeMessage(EchoResponseSchema, {});
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
      encodeMessage(marshalMessage(EchoRequestSchema, { message: "hi" })),
    );
    assert.deepEqual(await first.promise, expected, method);
  }
});

test("streaming-request calls send REQUEST alone and their messages later", async () => {
  const first = deferred();
  const { echo } = setup(async (stream) => {
    first.resolve((await stream.reader.read()).value);
    await stream.readMessage(EchoRequestSchema);
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
        await stream.writeMessage(EchoResponseSchema, { index });
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

test("unary response headers and trailing metadata reach the call's callbacks", async () => {
  const { echo } = setup(echoHandler);
  const marked = {};
  await echo.unary(
    { message: "hi" },
    { metadata: { "interop-header": "h" }, ...captureResponse(marked) },
  );
  assert.deepEqual(metadataText(marked.headers), { "interop-header": "h" });
  assert.deepEqual(metadataText(marked.metadata), { "interop-trailer": "h" });

  const plain = {};
  await echo.unary({ message: "hi" }, captureResponse(plain));
  assert.deepEqual(plain.headers, {});
  assert.deepEqual(plain.metadata, {});
});

test("response callbacks of the client's options apply to every call", async () => {
  const { transport } = setup(echoHandler);
  const seen = {};
  const echo = createClient(Echo, transport, captureResponse(seen));
  const stream = await echo.serverStream(
    { message: "s", count: 1 },
    { metadata: { "interop-header": "s" } },
  );
  for await (const response of stream) {
    assert.equal(response.message, "s");
  }
  assert.deepEqual(metadataText(seen.headers), { "interop-header": "s" });
  assert.deepEqual(metadataText(seen.metadata), { "interop-trailer": "s" });
});

test("a response callback that throws fails the call", async () => {
  const { echo } = setup(echoHandler);
  await assert.rejects(
    echo.unary(
      { message: "hi" },
      {
        onResponseMetadata() {
          throw new Error("rejected by the callback");
        },
      },
    ),
    (error) => error instanceof TrevRpcError,
  );
});

test("headers are visible before the first streamed message", async () => {
  const release = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeHeaders({ early: bytes("yes") });
    await release.promise;
    await stream.writeMessage(EchoResponseSchema, { message: "late" });
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

  const seen = {};
  const upload = await echo.clientStream({
    metadata: { "interop-header": "c" },
    ...captureResponse(seen),
  });
  await upload.send({ message: "x" });
  assert.equal((await upload.closeAndRecv()).message, "x");
  assert.deepEqual(metadataText(seen.headers), { "interop-header": "c" });
  assert.deepEqual(metadataText(seen.metadata), { "interop-trailer": "c" });
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
  const message = (index) => encodeMessageFrame(marshalMessage(EchoResponseSchema, { index }));
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
    await stream.writeMessage(EchoResponseSchema, {});
    await stream.writeStatus();
  });
  await assert.rejects(unary.echo.unary({}), isStatus(Code.Internal));
});

test("HEADERS after a MESSAGE fails the call", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponseSchema, { message: "m" });
    await stream.writeHeaders({});
  });
  const stream = await echo.serverStream({});
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.message, "m");
  await assert.rejects(iterator.next(), isStatus(Code.Internal));
  await assert.rejects(stream.status, isStatus(Code.Internal));

  const unary = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponseSchema, { message: "m" });
    await stream.writeHeaders({});
    await stream.writeStatus();
  });
  await assert.rejects(unary.echo.unary({}), isStatus(Code.Internal));
});

test("ANSWER is not the headers: they settle with the frame after it", async () => {
  const reported = [];
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeAnswer({ connectionMetadata: 1 });
    await stream.writeHeaders({ h: bytes("1") });
    await stream.writeMessage(EchoResponseSchema, { index: 3 });
    await stream.writeStatus();
    await stream.finish();
  });
  const stream = await echo.serverStream(
    {},
    { onResponseHeaders: (headers) => reported.push(metadataText(headers)) },
  );
  assert.deepEqual(metadataText(await stream.headers), { h: "1" });
  const received = [];
  for await (const response of stream) {
    received.push(response.index);
  }
  assert.deepEqual(received, [3]);
  assert.deepEqual(reported, [{ h: "1" }]);
  await transport.settled();
});

test("ANSWER twice, or after HEADERS or a MESSAGE, fails the call", async () => {
  const answer = encodeAnswerFrame({});
  for (const wire of [
    [answer, answer],
    [encodeHeadersFrame({}), answer],
    [encodeMessageFrame(marshalMessage(EchoResponseSchema, {})), answer],
  ]) {
    const { echo } = setup(async (stream) => {
      await stream.readRequest();
      await stream.write(concatBytes([...wire, encodeStatusFrame({ code: Code.Ok })]));
    });
    await assert.rejects(echo.unary({}), isStatus(Code.Internal));
  }
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
    await stream.writeMessage(EchoResponseSchema, { message: "first" });
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
    await stream.writeMessage(EchoResponseSchema, { message: "only" });
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.message, "only");
  await assert.rejects(iterator.next(), isStatus(Code.Internal));
  await assert.rejects(stream.status, isStatus(Code.Internal));
});

test("an undecodable streamed MESSAGE fails the stream", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.write(encodeMessageFrame(new Uint8Array([0x0a, 0xff])));
  });
  const stream = await echo.serverStream({});
  await assert.rejects(stream[Symbol.asyncIterator]().next(), isStatus(Code.InvalidArgument));
  await assert.rejects(stream.status, isStatus(Code.InvalidArgument));
});

test("a failure while draining after the final status does not change the outcome", async () => {
  const finished = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(EchoResponseSchema, { message: "m" });
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
  const response = await call.closeAndRecv();
  assert.equal(response.message, "abc");
  assert.equal(response.index, 3);
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
        await stream.writeMessage(EchoResponseSchema, { index });
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
  const { echo } = setup(echoHandler);
  const call = await echo.bidi();

  // A uint32 field cannot hold a negative number.
  await assert.rejects(call.send({ count: -1 }), isStatus(Code.InvalidArgument));
  await call.send({ message: "after" });
  assert.equal((await call.recv()).message, "after");
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
    await stream.writeMessage(EchoResponseSchema, { message: "answer" });
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
  const echo = createClient(Echo, transport, { metadata: { Authorization: "base", trace: "t" } });

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

test("createClient takes only protobuf-es service descriptors", () => {
  for (const service of [undefined, {}, EchoRequestSchema, { kind: "service" }]) {
    assert.throws(
      () => createClient(service, fakeTransport(echoHandler)),
      isStatus(Code.InvalidArgument),
    );
  }
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
  // Echo's Unary method under another service and method name.
  const unaryOf = ({ fullName, methods }) =>
    createClient(
      {
        kind: "service",
        typeName: fullName,
        methods: [{ ...Echo.method.unary, name: methods.unary.name, parent: undefined }],
      },
      fakeTransport(echoHandler),
    ).unary({ message: "hi" });
  const method = (name) => ({ unary: { name } });

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

test("sendAll sends every request and closes the request stream", async () => {
  const { echo } = setup(echoHandler);
  const upload = await echo.clientStream();
  await upload.sendAll([{ message: "a" }, { message: "b" }, { message: "c" }]);
  const reply = await upload.closeAndRecv();
  assert.equal(reply.message, "abc");
  assert.equal(reply.index, 3);

  async function* requests() {
    yield { message: "1" };
    yield { message: "2" };
  }
  const call = await echo.bidi();
  const sending = call.sendAll(requests());
  const echoed = [];
  for await (const response of call) {
    echoed.push(response.message);
  }
  await sending;
  assert.deepEqual(echoed, ["1", "2"]);
  assert.equal((await call.status).code, Code.Ok);
});

test("sendAll stops quietly when the server ends the call first", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await stream.writeStatus(Code.NotFound, "gone");
    // Ends the stream as servers do: a call keeps a response stream that stays open until its
    // deadline, and that timer would keep this process alive for the whole of largeDeadline.
    await stream.finish();
  });
  async function* endless() {
    for (;;) {
      yield { message: "x".repeat(64) };
    }
  }
  const call = await echo.clientStream({ timeoutMs: largeDeadline });
  await within(1000, call.sendAll(endless()), "sendAll");
  await assert.rejects(call.closeAndRecv(), isStatus(Code.NotFound, "gone"));
});

test("sendAll cancels the call when the requests throw", async () => {
  const release = deferred();
  const { echo } = setup(async (stream) => {
    await stream.readRequest();
    await release.promise;
  });
  const failure = new Error("no more requests");
  async function* failing() {
    yield { message: "a" };
    throw failure;
  }
  const call = await echo.bidi({ timeoutMs: largeDeadline });
  try {
    await assert.rejects(call.sendAll(failing()), (error) => error === failure);
    await assert.rejects(within(1000, call.status), isStatus(Code.Cancelled));
  } finally {
    release.resolve();
    await call.close();
  }
});

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
    await stream.writeMessage(EchoResponseSchema, { message: "enough", index: 7 });
    await stream.writeStatus(Code.Ok, "done", { t: new Uint8Array([3]) });
    // As above: the stream ends, so the call does not hold it until largeDeadline.
    await stream.finish();
  });
  const seen = {};
  const call = await echo.clientStream({ timeoutMs: largeDeadline, ...captureResponse(seen) });
  const error = await within(1000, sendUntilRejected(call), "sends");
  assert.ok(isStatus(Code.Cancelled)(error), String(error));
  assert.match(error.statusMessage, /server completed the call/u);
  // The outcome is the server's OK response; the failed send does not change it.
  const response = await call.closeAndRecv();
  assert.equal(response.message, "enough");
  assert.deepEqual([...seen.metadata.t], [3]);
});

test("a completed call leaves the request side it closed to finish instead of resetting it", async () => {
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await reply(stream, { message: "done" });
  });
  // Records every reset of a request side, which a browser's WebTransport pays for per call.
  const resets = [];
  const open = transport.createBidirectionalStream.bind(transport);
  transport.createBidirectionalStream = async () => {
    const stream = await open();
    const getWriter = stream.writable.getWriter.bind(stream.writable);
    stream.writable.getWriter = () => {
      const writer = getWriter();
      const abort = writer.abort.bind(writer);
      writer.abort = (reason) => {
        resets.push(reason);
        return abort(reason);
      };
      return writer;
    };
    return stream;
  };
  const echo = createClient(Echo, transport);

  assert.equal((await echo.unary({ message: "hi" })).message, "done");
  const responses = [];
  for await (const response of await echo.serverStream({ message: "hi" })) {
    responses.push(response.message);
  }
  assert.deepEqual(responses, ["done"]);
  await turn();
  assert.deepEqual(resets, []);
  await transport.settled();
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
    await stream.writeMessage(EchoResponseSchema, { message: "a" });
    await stream.writeMessage(EchoResponseSchema, { message: "b" });
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
      await stream.writeMessage(EchoResponseSchema, { message: "r", index });
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

/** Chromium's refusal of a new WebTransport stream while the server's stream credit is used up. */
function streamRefusal() {
  return new DOMException("Failed to create bidirectional stream.", "NetworkError");
}

/** A transport that refuses its first `refusals` streams with `error`, then serves Echo. */
function refusingTransport(refusals, error = streamRefusal) {
  const inner = fakeTransport(echoHandler);
  const transport = {
    attempts: 0,
    createBidirectionalStream() {
      transport.attempts += 1;
      if (transport.attempts <= refusals) {
        return Promise.reject(error());
      }
      return inner.createBidirectionalStream();
    },
  };
  return transport;
}

test("a stream the browser refuses for want of stream credit is opened again", async () => {
  const transport = refusingTransport(3);
  const echo = createClient(Echo, transport);
  const response = await echo.unary({ message: "hi" });
  assert.equal(response.message, "hi");
  assert.equal(transport.attempts, 4);
});

test("other failures to open a stream fail the call at once", async () => {
  const transport = refusingTransport(1, () => new Error("boom"));
  const echo = createClient(Echo, transport);
  await assert.rejects(
    echo.unary({ message: "hi" }),
    isStatus(Code.Unavailable, "transport unavailable: boom"),
  );
  assert.equal(transport.attempts, 1);
});

test("a refusal that lasts fails the call with Unavailable", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const transport = refusingTransport(Infinity);
  const echo = createClient(Echo, transport);
  const call = echo.unary({ message: "hi" });
  let settled = false;
  call.then(
    () => (settled = true),
    () => (settled = true),
  );
  for (let elapsed = 0; elapsed <= 3000 && !settled; elapsed += 16) {
    await turn();
    t.mock.timers.tick(16);
  }
  await assert.rejects(
    call,
    isStatus(Code.Unavailable, "transport unavailable: Failed to create bidirectional stream."),
  );
  const attempts = transport.attempts;
  // Doubling from 1 ms to at most 64 ms, it gave up after about 2 s.
  assert.ok(attempts > 20 && attempts < 50, `${attempts} attempts`);
  t.mock.timers.tick(1000);
  await turn();
  assert.equal(transport.attempts, attempts, "no attempts after giving up");
});

test("cancelling a call stops opening a refused stream again", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const transport = refusingTransport(Infinity);
  const echo = createClient(Echo, transport);
  const controller = new AbortController();
  const call = echo.unary({ message: "hi" }, { signal: controller.signal });
  await turn();
  t.mock.timers.tick(10);
  await turn();
  controller.abort();
  await assert.rejects(
    call,
    (error) => error instanceof TrevRpcError && error.code === Code.Cancelled,
  );
  const attempts = transport.attempts;
  t.mock.timers.tick(1000);
  await turn();
  assert.equal(transport.attempts, attempts, "no attempts after cancelling");
});
