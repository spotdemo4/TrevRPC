import assert from "node:assert/strict";
import test from "node:test";

import { Code, TrevRpcError } from "@trevrpc/trevrpc-js";
import { Cause, Effect, Exit, Stream } from "effect";

// The in-memory server of trevrpc-js's own tests, which runs the real client end to end.
import { deferred, fakeTransport } from "../../test/support.js";
import { Echo, EchoRequestSchema, EchoResponseSchema } from "../build/test/gen/echo_pb.js";
import { createEffectClient, toTrevRpcError } from "../src/index.js";

const EchoRequest = EchoRequestSchema;
const EchoResponse = EchoResponseSchema;

/** An Effect client whose calls handler(serverStream) serves after reading the REQUEST frame. */
function setup(handler) {
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await handler(stream);
  });
  return { transport, echo: createEffectClient(Echo, transport) };
}

/** Ends a call with `code`, as servers do: STATUS, then the end of both directions. */
async function finish(stream, code = Code.Ok, message = "") {
  await stream.writeStatus(code, message);
  await stream.finish();
  await stream.stopSending();
}

/** Echoes every request message of a bidirectional call until the request stream ends. */
async function echoAll(stream) {
  for (let request; (request = await stream.readMessage(EchoRequest)) !== null;) {
    await stream.writeMessage(EchoResponse, { message: request.message });
  }
  await finish(stream);
}

/** Resolves with true once the client cancels the call's response stream. */
function watchCancellation(stream, cancelled) {
  stream.responseCancelled.then(cancelled.resolve);
}

/** The typed error a failed exit carries. */
function failure(exit) {
  assert.ok(Exit.isFailure(exit), "expected a failure");
  const error = Cause.findErrorOption(exit.cause);
  assert.ok(error._tag === "Some", `expected a typed error, got ${Cause.pretty(exit.cause)}`);
  return error.value;
}

function assertStatus(error, code) {
  assert.ok(error instanceof TrevRpcError, `expected a TrevRpcError, got ${error}`);
  assert.equal(error.code, code);
}

/** A request stream that emits `messages` and then waits forever. */
function thenIdle(...messages) {
  return Stream.concat(Stream.fromIterable(messages.map((message) => ({ message }))), Stream.never);
}

/** A request stream that never ends and always has the next message ready. */
function endless() {
  return Stream.iterate(0, (index) => index + 1).pipe(
    Stream.map((index) => ({ message: String(index) })),
  );
}

class UploadError {
  _tag = "UploadError";
}

test("unary calls return the response", async () => {
  const { echo, transport } = setup(async (stream) => {
    const request = await stream.readMessage(EchoRequest);
    await stream.writeMessage(EchoResponse, { message: `${request.message}!` });
    await finish(stream);
  });
  const response = await Effect.runPromise(echo.unary({ message: "hi" }));
  assert.equal(response.message, "hi!");
  await transport.settled();
});

test("unary errors are tagged TrevRpcErrors", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readMessage(EchoRequest);
    await finish(stream, Code.NotFound, "missing");
  });
  const code = await Effect.runPromise(
    echo.unary({}).pipe(
      Effect.as(Code.Ok),
      Effect.catchTag("TrevRpcError", (error) => Effect.succeed(error.code)),
    ),
  );
  assert.equal(code, Code.NotFound);
});

test("interrupting a unary call cancels it", async () => {
  const cancelled = deferred();
  const started = deferred();
  const { echo } = setup(async (stream) => {
    watchCancellation(stream, cancelled);
    await stream.readMessage(EchoRequest);
    started.resolve();
  });
  const exit = await Effect.runPromiseExit(
    Effect.raceFirst(
      echo.unary({}),
      Effect.promise(() => started.promise),
    ),
  );
  assert.ok(Exit.isSuccess(exit));
  assert.equal(await cancelled.promise, true);
});

test("the caller's signal still cancels a call", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readMessage(EchoRequest);
  });
  const controller = new AbortController();
  const call = Effect.runPromiseExit(echo.unary({}, { signal: controller.signal }));
  controller.abort();
  assertStatus(failure(await call), Code.Cancelled);
});

test("server streams emit every response and end with the call", async () => {
  const { echo, transport } = setup(async (stream) => {
    const request = await stream.readMessage(EchoRequest);
    for (let index = 0; index < 3; index += 1) {
      await stream.writeMessage(EchoResponse, { message: `${request.message}${index}` });
    }
    await finish(stream);
  });
  const responses = await Effect.runPromise(Stream.runCollect(echo.serverStream({ message: "m" })));
  assert.deepEqual(
    Array.from(responses, (response) => response.message),
    ["m0", "m1", "m2"],
  );
  await transport.settled();
});

test("server streams fail with the final status after the responses", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readMessage(EchoRequest);
    await stream.writeMessage(EchoResponse, { message: "a" });
    await finish(stream, Code.Aborted, "stop");
  });
  const seen = [];
  const exit = await Effect.runPromiseExit(
    Stream.runForEach(echo.serverStream({}), (response) =>
      Effect.sync(() => seen.push(response.message)),
    ),
  );
  assert.deepEqual(seen, ["a"]);
  assertStatus(failure(exit), Code.Aborted);
});

test("taking part of a server stream cancels the call", async () => {
  const cancelled = deferred();
  const { echo } = setup(async (stream) => {
    watchCancellation(stream, cancelled);
    await stream.readMessage(EchoRequest);
    try {
      for (let index = 0; ; index += 1) {
        await stream.writeMessage(EchoResponse, { message: String(index) });
      }
    } catch {
      // The client cancelled the call.
    }
  });
  const responses = await Effect.runPromise(
    Stream.runCollect(Stream.take(echo.serverStream({}), 2)),
  );
  assert.deepEqual(
    Array.from(responses, (response) => response.message),
    ["0", "1"],
  );
  assert.equal(await cancelled.promise, true);
});

test("client streams upload the request stream and return the response", async () => {
  const { echo, transport } = setup(async (stream) => {
    const messages = [];
    for (let request; (request = await stream.readMessage(EchoRequest)) !== null;) {
      messages.push(request.message);
    }
    await stream.writeMessage(EchoResponse, { message: messages.join(",") });
    await finish(stream);
  });
  const requests = Stream.fromIterable(["a", "b", "c"]).pipe(
    Stream.map((message) => ({ message })),
  );
  const response = await Effect.runPromise(echo.clientStream(requests));
  assert.equal(response.message, "a,b,c");
  await transport.settled();
});

test("a client stream returns a response the server sends before the upload ends", async () => {
  const { echo, transport } = setup(async (stream) => {
    const first = await stream.readMessage(EchoRequest);
    await stream.writeMessage(EchoResponse, { message: first.message });
    await finish(stream);
  });
  // The request stream never ends, so only the server's status can end the upload.
  const response = await Effect.runPromise(echo.clientStream(thenIdle("first")));
  assert.equal(response.message, "first");
  await transport.settled();
});

test("a client stream succeeds when the server answers while requests keep coming", async () => {
  const { echo, transport } = setup(async (stream) => {
    const first = await stream.readMessage(EchoRequest);
    await stream.writeMessage(EchoResponse, { message: first.message });
    await finish(stream);
  });
  // Sends are rejected with Code.Cancelled once the server finished; that is not the outcome.
  const response = await Effect.runPromise(echo.clientStream(endless()));
  assert.equal(response.message, "0");
  await transport.settled();
});

test("a client stream fails with an early error status", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readMessage(EchoRequest);
    await finish(stream, Code.PermissionDenied, "no");
  });
  const exit = await Effect.runPromiseExit(echo.clientStream(thenIdle("a")));
  assertStatus(failure(exit), Code.PermissionDenied);
});

test("a failing request stream fails a client stream and cancels the call", async () => {
  const cancelled = deferred();
  const { echo } = setup(async (stream) => {
    watchCancellation(stream, cancelled);
    while ((await stream.readMessage(EchoRequest)) !== null);
  });
  const requests = Stream.concat(Stream.make({ message: "a" }), Stream.fail(new UploadError()));
  const exit = await Effect.runPromiseExit(echo.clientStream(requests));
  assert.ok(failure(exit) instanceof UploadError);
  assert.equal(await cancelled.promise, true);
});

test("interrupting a client stream cancels the call", async () => {
  const cancelled = deferred();
  const received = deferred();
  const { echo } = setup(async (stream) => {
    watchCancellation(stream, cancelled);
    await stream.readMessage(EchoRequest);
    received.resolve();
  });
  await Effect.runPromise(
    Effect.raceFirst(
      echo.clientStream(thenIdle("a")),
      Effect.promise(() => received.promise),
    ),
  );
  assert.equal(await cancelled.promise, true);
});

test("bidirectional streams echo the request stream and end with the call", async () => {
  const { echo, transport } = setup(echoAll);
  const requests = Stream.fromIterable(["1", "2", "3"]).pipe(
    Stream.map((message) => ({ message })),
  );
  const responses = await Effect.runPromise(Stream.runCollect(echo.bidi(requests)));
  assert.deepEqual(
    Array.from(responses, (response) => response.message),
    ["1", "2", "3"],
  );
  await transport.settled();
});

test("a bidirectional stream ends when the server does, even mid-upload", async () => {
  const { echo, transport } = setup(async (stream) => {
    const first = await stream.readMessage(EchoRequest);
    await stream.writeMessage(EchoResponse, { message: first.message });
    await finish(stream);
  });
  const responses = await Effect.runPromise(Stream.runCollect(echo.bidi(thenIdle("only"))));
  assert.deepEqual(
    Array.from(responses, (response) => response.message),
    ["only"],
  );
  await transport.settled();
});

test("a bidirectional stream succeeds when the server ends it while requests keep coming", async () => {
  const { echo, transport } = setup(async (stream) => {
    for (let index = 0; index < 3; index += 1) {
      const request = await stream.readMessage(EchoRequest);
      await stream.writeMessage(EchoResponse, { message: request.message });
    }
    await finish(stream);
  });
  const responses = await Effect.runPromise(Stream.runCollect(echo.bidi(endless())));
  assert.deepEqual(
    Array.from(responses, (response) => response.message),
    ["0", "1", "2"],
  );
  await transport.settled();
});

test("a bidirectional stream fails with the server's error status", async () => {
  const { echo } = setup(async (stream) => {
    await stream.readMessage(EchoRequest);
    await finish(stream, Code.ResourceExhausted, "full");
  });
  const exit = await Effect.runPromiseExit(Stream.runDrain(echo.bidi(thenIdle("a"))));
  assertStatus(failure(exit), Code.ResourceExhausted);
});

test("a failing request stream fails a bidirectional stream and cancels the call", async () => {
  const cancelled = deferred();
  const { echo } = setup(async (stream) => {
    watchCancellation(stream, cancelled);
    try {
      await echoAll(stream);
    } catch {
      // The client cancelled the call.
    }
  });
  const requests = Stream.concat(Stream.make({ message: "a" }), Stream.fail(new UploadError()));
  const exit = await Effect.runPromiseExit(Stream.runDrain(echo.bidi(requests)));
  assert.ok(failure(exit) instanceof UploadError);
  assert.equal(await cancelled.promise, true);
});

/** A request validator that rejects request messages whose `message` is "bad". */
const rejectBad = {
  validateFunc: () => (request) => {
    if (request.message === "bad") {
      throw new TrevRpcError(Code.InvalidArgument, "bad request");
    }
  },
};

test("a request the validator rejects fails a client stream and cancels the call", async () => {
  const cancelled = deferred();
  const received = [];
  const transport = fakeTransport(async (stream) => {
    watchCancellation(stream, cancelled);
    await stream.readRequest();
    try {
      for (let request; (request = await stream.readMessage(EchoRequest)) !== null;) {
        received.push(request.message);
      }
    } catch {
      // The client cancelled the call.
    }
  });
  const echo = createEffectClient(Echo, transport, { validator: rejectBad });
  const exit = await Effect.runPromiseExit(echo.clientStream(thenIdle("a", "bad", "b")));
  assertStatus(failure(exit), Code.InvalidArgument);
  assert.equal(await cancelled.promise, true);
  // Nothing after the rejected request is sent; the cancellation may discard what was.
  assert.ok(
    received.every((message) => message === "a"),
    `received ${received.join(",")}`,
  );
});

test("a request the validator rejects fails a bidirectional stream and cancels the call", async () => {
  const cancelled = deferred();
  const transport = fakeTransport(async (stream) => {
    watchCancellation(stream, cancelled);
    await stream.readRequest();
    try {
      await echoAll(stream);
    } catch {
      // The client cancelled the call.
    }
  });
  const echo = createEffectClient(Echo, transport, { validator: rejectBad });
  const exit = await Effect.runPromiseExit(Stream.runDrain(echo.bidi(thenIdle("a", "bad"))));
  assertStatus(failure(exit), Code.InvalidArgument);
  assert.equal(await cancelled.promise, true);
});

test("taking part of a bidirectional stream cancels the call", async () => {
  const cancelled = deferred();
  const { echo } = setup(async (stream) => {
    watchCancellation(stream, cancelled);
    try {
      await echoAll(stream);
    } catch {
      // The client cancelled the call.
    }
  });
  const responses = await Effect.runPromise(
    Stream.runCollect(Stream.take(echo.bidi(endless()), 3)),
  );
  assert.deepEqual(
    Array.from(responses, (response) => response.message),
    ["0", "1", "2"],
  );
  assert.equal(await cancelled.promise, true);
});

test("anything but a service descriptor is rejected, as createClient rejects it", () => {
  assert.throws(
    () =>
      createEffectClient(
        EchoRequestSchema,
        fakeTransport(() => {}),
      ),
    (error) => error instanceof TrevRpcError && error.code === Code.InvalidArgument,
  );
});

test("other failures become TrevRpcErrors with Code.Unknown", async () => {
  const cause = new Error("boom");
  // An interceptor that throws fails the call with what it threw.
  const echo = createEffectClient(
    Echo,
    fakeTransport(() => {}),
    {
      interceptors: [
        () => {
          throw cause;
        },
      ],
    },
  );
  const error = failure(await Effect.runPromiseExit(echo.unary({})));
  assertStatus(error, Code.Unknown);
  assert.equal(error.statusMessage, "boom");
  assert.equal(error.cause, cause);

  const status = new TrevRpcError(Code.NotFound);
  assert.equal(toTrevRpcError(status), status);
  assert.equal(toTrevRpcError("text").statusMessage, "text");
});
