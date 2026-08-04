import assert from "node:assert/strict";
import test from "node:test";

import { registerGreeterServer } from "../examples/greeter/greeter.node.trevrpc.js";
import { root } from "../examples/greeter/greeter.trevrpc.js";
import {
  Code,
  RpcStreamFrameKind,
  createStreamingResponse,
  createUnaryResponse,
} from "../src/index.js";

const HelloRequest = root.lookupType("example.greeter.HelloRequest");
const HelloReply = root.lookupType("example.greeter.HelloReply");

test("direct protobuf objects with message fields are not envelopes", async () => {
  const { handler } = registered("SayHello", {
    sayHello: () => ({ message: "hello" }),
  });
  const call = fakeCall("SayHello", "unary");
  await handler(call);
  assert.equal(HelloReply.decode(call.responses[0].body).message, "hello");
  assert.deepEqual(call.responses[0].metadata, {});
});

test("iterables carrying messages properties remain ordinary iterables", async () => {
  const replies = [{ message: "one" }, { message: "two" }];
  replies.messages = [{ message: "wrong" }];
  const { handler } = registered("LotsOfReplies", {
    lotsOfReplies: () => replies,
  });
  const call = fakeCall("LotsOfReplies", "serverStreaming");
  await handler(call);
  assert.deepEqual(
    call.messages.map((body) => HelloReply.decode(body).message),
    ["one", "two"],
  );
  assert.equal(call.status.status, Code.Ok);
});

test("branded unary and streaming envelopes carry metadata and status", async () => {
  const unary = registered("SayHello", {
    sayHello: () => createUnaryResponse({ message: "hello" }, { trailer: "unary" }),
  });
  const unaryCall = fakeCall("SayHello", "unary");
  await unary.handler(unaryCall);
  assert.deepEqual(unaryCall.responses[0].metadata.trailer, new TextEncoder().encode("unary"));

  const streaming = registered("LotsOfReplies", {
    lotsOfReplies: () =>
      createStreamingResponse([{ message: "hello" }], {
        code: Code.PermissionDenied,
        message: "denied",
        metadata: { trailer: "stream" },
      }),
  });
  const streamingCall = fakeCall("LotsOfReplies", "serverStreaming");
  await streaming.handler(streamingCall);
  assert.deepEqual(streamingCall.status.metadata.trailer, new TextEncoder().encode("stream"));
  assert.equal(streamingCall.status.status, Code.PermissionDenied);
  assert.equal(streamingCall.status.message, "denied");
});

test("client streaming sends one message then one terminal status without respond", async () => {
  const { handler } = registered("LotsOfGreetings", {
    async lotsOfGreetings(requests) {
      const names = [];
      for await (const request of requests) {
        names.push(request.name);
      }
      return createUnaryResponse({ message: names.join(",") }, { trailer: "done" });
    },
  });
  const call = fakeCall("LotsOfGreetings", "clientStreaming", [
    { kind: RpcStreamFrameKind.Message, body: HelloRequest.encode({ name: "one" }).finish() },
    { kind: RpcStreamFrameKind.Message, body: HelloRequest.encode({ name: "two" }).finish() },
    { kind: RpcStreamFrameKind.Status, status: Code.Ok },
  ]);
  await handler(call);
  assert.equal(call.responses.length, 0);
  assert.equal(call.messages.length, 1);
  assert.equal(HelloReply.decode(call.messages[0]).message, "one,two");
  assert.equal(call.status.status, Code.Ok);
  assert.deepEqual(call.status.metadata.trailer, new TextEncoder().encode("done"));
});

test("server-streaming and bidi handlers accept promised iterables", async () => {
  const serverStreaming = registered("LotsOfReplies", {
    lotsOfReplies: async () => [{ message: "promised" }],
  });
  const serverCall = fakeCall("LotsOfReplies", "serverStreaming");
  await serverStreaming.handler(serverCall);
  assert.equal(HelloReply.decode(serverCall.messages[0]).message, "promised");

  const bidi = registered("BidiHello", {
    bidiHello: async (requests) => {
      for await (const _request of requests) {
        return [{ message: "bidi" }];
      }
      return [];
    },
  });
  const bidiCall = fakeCall("BidiHello", "bidirectionalStreaming", [
    { kind: RpcStreamFrameKind.Message, body: HelloRequest.encode({ name: "one" }).finish() },
    { kind: RpcStreamFrameKind.Status, status: Code.Ok },
  ]);
  await bidi.handler(bidiCall);
  assert.equal(HelloReply.decode(bidiCall.messages[0]).message, "bidi");
});

function registered(methodName, handlers) {
  const registrations = [];
  registerGreeterServer(
    {
      register(service, method, kind, handler) {
        registrations.push({ service, method, kind, handler });
      },
    },
    handlers,
  );
  return registrations.find(({ method }) => method === methodName);
}

function fakeCall(method, kind, requestFrames = []) {
  return {
    request: {
      service: "example.greeter.Greeter",
      method,
      kind,
      body: HelloRequest.encode({ name: "Trev" }).finish(),
      metadata: {},
    },
    responses: [],
    messages: [],
    status: null,
    writeBatchMaxMessages: 16,
    respond(response) {
      this.responses.push(response);
      return Promise.resolve();
    },
    sendMessage(body) {
      this.messages.push(body);
      return Promise.resolve();
    },
    sendMany(bodies) {
      this.messages.push(...bodies);
      return Promise.resolve();
    },
    recv() {
      return Promise.resolve(requestFrames.shift() ?? null);
    },
    finishStream(status, message, metadata) {
      this.status = { status, message, metadata };
      return Promise.resolve();
    },
  };
}
