import assert from "node:assert/strict";
import test from "node:test";

import { Code, RpcKind } from "../src/index.js";
import { NodeServer, bearerAuthorizer, metadataValueAuthorizer } from "../src/node.js";

class FakeNativeServer {
  constructor() {
    this.port = 0;
    this.handlers = [];
  }

  register(service, method, kind, handler) {
    this.handlers.push({ service, method, kind, handler });
  }

  serve() {
    return Promise.resolve();
  }

  close() {}
}

class FakeNativeCall {
  constructor(request) {
    this.request = request;
    this.responses = [];
    this.streamStatus = null;
    this.done = new Promise((resolve) => {
      this.resolveDone = resolve;
    });
  }

  respond(response) {
    this.responses.push(response);
    this.resolveDone();
    return Promise.resolve();
  }

  finishStream(status, message, metadata) {
    this.streamStatus = { status, message, metadata };
    this.resolveDone();
    return Promise.resolve();
  }

  sendMessage() {
    return Promise.resolve();
  }

  close() {
    this.resolveDone();
  }
}

async function waitForDispatch(call) {
  await call.done;
  await Promise.resolve();
  await Promise.resolve();
}

function unaryRequest(metadata = {}) {
  return {
    service: "hello.v1.Greeter",
    method: "SayHello",
    body: new Uint8Array([1, 2, 3]),
    metadata,
    kind: RpcKind.Unary,
    version: 1,
  };
}

test("Node server authorizer rejects before handler", async () => {
  const native = new FakeNativeServer();
  let handled = false;
  const finished = [];
  const logs = [];
  const server = new NodeServer(native, {
    authorizer: bearerAuthorizer("secret"),
    metrics: { rpcFinished: (event) => finished.push(event) },
    logger: (event) => logs.push(event),
  });

  server.register("hello.v1.Greeter", "SayHello", RpcKind.Unary, () => {
    handled = true;
  });

  const call = new FakeNativeCall(unaryRequest());
  native.handlers[0].handler(call);
  await waitForDispatch(call);

  assert.equal(handled, false);
  assert.equal(call.responses[0].status, Code.Unauthenticated);
  assert.equal(finished[0].status, Code.Unauthenticated);
  assert.equal(logs[0].event, "rpc.authorization_denied");
});

test("Node server metadata authorizer accepts and metrics record success", async () => {
  const native = new FakeNativeServer();
  const started = [];
  const finished = [];
  const server = new NodeServer(native)
    .setAuthorizer(metadataValueAuthorizer("authorization", "Bearer secret"))
    .setMetrics({
      rpcStarted: (event) => started.push(event),
      rpcFinished: (event) => finished.push(event),
    });

  server.register("hello.v1.Greeter", "SayHello", RpcKind.Unary, () => new Uint8Array([9, 8]));

  const call = new FakeNativeCall(
    unaryRequest({ authorization: new TextEncoder().encode("Bearer secret") }),
  );
  native.handlers[0].handler(call);
  await waitForDispatch(call);

  assert.deepEqual(call.responses[0].body, new Uint8Array([9, 8]));
  assert.equal(started[0].service, "hello.v1.Greeter");
  assert.equal(finished[0].status, Code.Ok);
  assert.equal(finished[0].responseBodyLength, 2);
});

test("Node server handler errors are logged and finished", async () => {
  const native = new FakeNativeServer();
  const finished = [];
  const logs = [];
  const server = new NodeServer(native, {
    metrics: { finished: (event) => finished.push(event) },
    logger: { error: (event) => logs.push(event) },
  });

  server.register("hello.v1.Greeter", "SayHello", RpcKind.Unary, () => {
    throw new Error("boom");
  });

  const call = new FakeNativeCall(unaryRequest());
  native.handlers[0].handler(call);
  await waitForDispatch(call);

  assert.equal(call.responses[0].status, Code.Internal);
  assert.equal(finished[0].status, Code.Internal);
  assert.equal(logs[0].event, "rpc.handler_failed");
});
