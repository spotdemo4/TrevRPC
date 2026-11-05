import assert from "node:assert/strict";
import test from "node:test";

import { Echo } from "../build/interop/gen/interop_pb.js";
import { createClient } from "../src/client.js";
import { Code, TrevRpcError } from "../src/status.js";
import { echoHandler } from "./echo-server.js";
import { fakeTransport, isPending, turn } from "./support.js";

const text = new TextDecoder();

/** Records what interceptors see and how their calls end, as "name event" strings. */
function recorder() {
  const events = [];
  const interceptor =
    (name, intercept = async () => {}) =>
    async (call) => {
      events.push(`${name} ${call.name}`);
      await intercept(call);
      return ({ code, error }) => {
        assert.equal(code === Code.Ok, error === undefined);
        events.push(`${name} finished ${code}`);
      };
    };
  return { events, interceptor };
}

test("interceptors run in order, change metadata and finish innermost first", async () => {
  const transport = fakeTransport(echoHandler);
  const { events, interceptor } = recorder();
  const seen = [];
  transport.interceptors = [
    interceptor("channel", (call) => {
      seen.push({ ...call, metadata: undefined });
      call.metadata["interop-suffix"] = "?";
    }),
  ];
  const echo = createClient(Echo, transport, {
    metadata: { removed: "x" },
    interceptors: [
      interceptor("client", async (call) => {
        // An interceptor may wait, such as for a refreshed token, and sees the earlier changes.
        await turn(1);
        assert.equal(call.metadata["interop-suffix"], "?");
        assert.equal(text.decode(call.metadata.removed), "x");
        call.metadata["interop-suffix"] = "!";
        delete call.metadata.removed;
      }),
    ],
  });

  const response = await echo.unary({ message: "hi" }, { timeoutMs: 5000 });
  assert.equal(response.message, "hi!");
  assert.deepEqual(events, [
    "channel Unary",
    "client Unary",
    `client finished ${Code.Ok}`,
    `channel finished ${Code.Ok}`,
  ]);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].method, "/trevrpc.interop.v1.Echo/Unary");
  assert.equal(seen[0].service, "trevrpc.interop.v1.Echo");
  assert.equal(seen[0].name, "Unary");
  assert.equal(seen[0].kind, "unary");
  assert.equal(seen[0].timeoutMs, 5000);
  await transport.settled();
});

test("a failed call finishes with its status", async () => {
  let finished;
  const echo = createClient(Echo, fakeTransport(echoHandler), {
    interceptors: [() => (end) => (finished = end)],
  });

  await assert.rejects(
    echo.unary({ message: "x", failCode: Code.NotFound, failMessage: "missing" }),
    (error) => error instanceof TrevRpcError && error.code === Code.NotFound,
  );
  assert.equal(finished.code, Code.NotFound);
  assert.ok(finished.error instanceof TrevRpcError);
  assert.equal(finished.error.statusMessage, "missing");
});

test("a streaming call finishes when its final status arrives", async () => {
  const { events, interceptor } = recorder();
  const echo = createClient(Echo, fakeTransport(echoHandler), { interceptors: [interceptor("a")] });

  const stream = await echo.serverStream({ message: "m", count: 2 });
  assert.deepEqual(events, ["a ServerStream"], "the call has not ended yet");
  const messages = [];
  for await (const response of stream) {
    messages.push(response.index);
  }
  assert.deepEqual(messages, [0, 1]);
  await turn(1);
  assert.deepEqual(events, ["a ServerStream", `a finished ${Code.Ok}`]);

  events.length = 0;
  const failing = await echo.serverStream({
    message: "m",
    count: 1,
    failCode: Code.PermissionDenied,
  });
  await assert.rejects(async () => {
    for await (const _ of failing) {
      // Iterating throws the final status.
    }
  });
  await turn(1);
  assert.deepEqual(events, ["a ServerStream", `a finished ${Code.PermissionDenied}`]);
});

test("client-streaming and bidirectional calls finish when they end", async () => {
  const { events, interceptor } = recorder();
  const echo = createClient(Echo, fakeTransport(echoHandler), { interceptors: [interceptor("a")] });

  const upload = await echo.clientStream();
  await upload.send({ message: "a" });
  await upload.send({ message: "b" });
  assert.equal((await upload.closeAndRecv()).message, "ab");
  await upload.status;
  await turn(1);
  assert.deepEqual(events, ["a ClientStream", `a finished ${Code.Ok}`]);

  events.length = 0;
  const chat = await echo.bidi();
  await chat.send({ message: "x" });
  assert.equal((await chat.recv()).message, "x");
  assert.ok(await isPending(chat.status));
  assert.deepEqual(events, ["a Bidi"]);
  await chat.close();
  await turn(1);
  assert.deepEqual(events, ["a Bidi", `a finished ${Code.Cancelled}`]);
});

test("an interceptor that throws fails the call before it starts", async () => {
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  const { events, interceptor } = recorder();
  const refusal = new TrevRpcError(Code.Unauthenticated, "signed out");
  const echo = createClient(Echo, transport, {
    interceptors: [
      interceptor("outer"),
      () => {
        throw refusal;
      },
      interceptor("later"),
    ],
  });

  await assert.rejects(echo.unary({ message: "x" }), (error) => error === refusal);
  await assert.rejects(echo.bidi(), (error) => error === refusal);
  assert.deepEqual(events, [
    "outer Unary",
    `outer finished ${Code.Unauthenticated}`,
    "outer Bidi",
    `outer finished ${Code.Unauthenticated}`,
  ]);
  assert.equal(transport.opened, 0);
});

test("invalid metadata from an interceptor fails the call before it starts", async () => {
  const transport = fakeTransport(() => assert.fail("no stream expected"));
  let finished;
  const echo = createClient(Echo, transport, {
    interceptors: [
      (call) => {
        call.metadata["trevrpc-reserved"] = "x";
        return (end) => (finished = end);
      },
    ],
  });
  await assert.rejects(
    echo.unary({ message: "x" }),
    (error) => error instanceof TrevRpcError && error.code === Code.InvalidArgument,
  );
  assert.equal(finished.code, Code.InvalidArgument);
  assert.equal(transport.opened, 0);
});

test("a finish function that throws does not affect the call or other finishes", async (t) => {
  const reported = [];
  t.mock.method(console, "error", (error) => reported.push(error));
  const failure = new Error("finish bug");
  let outerFinished = false;
  const echo = createClient(Echo, fakeTransport(echoHandler), {
    interceptors: [
      () => () => {
        outerFinished = true;
      },
      () => () => {
        throw failure;
      },
    ],
  });
  assert.equal((await echo.unary({ message: "ok" })).message, "ok");
  assert.ok(outerFinished);
  assert.deepEqual(reported, [failure]);
});

test("an interceptor must return a function or nothing", async () => {
  const echo = createClient(Echo, fakeTransport(echoHandler), { interceptors: [() => 1] });
  await assert.rejects(echo.unary({ message: "x" }), TypeError);
});

test("invalid interceptor lists are rejected when the client is created", () => {
  for (const interceptors of [1, "x", [1], [() => {}, null]]) {
    assert.throws(
      () => createClient(Echo, fakeTransport(echoHandler), { interceptors }),
      (error) => error instanceof TrevRpcError && error.code === Code.InvalidArgument,
    );
  }
  const transport = fakeTransport(echoHandler);
  transport.interceptors = {};
  assert.throws(() => createClient(Echo, transport), TrevRpcError);
});
