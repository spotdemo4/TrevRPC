import assert from "node:assert/strict";
import test from "node:test";

import { Code, RpcKind } from "../src/index.js";
import {
  NodeServer,
  NodeServerCall,
  bearerAuthorizer,
  metadataValueAuthorizer,
} from "../src/node-index.js";

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
    this.messages = [];
    this.messageBatches = [];
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

  sendMessage(body) {
    this.messages.push(body);
    return Promise.resolve();
  }

  sendMessages(bodies) {
    this.messageBatches.push(bodies);
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

test("Node server batches explicit streaming response batches", async () => {
  const native = new FakeNativeServer();
  const finished = [];
  const server = new NodeServer(native, {
    metrics: { rpcFinished: (event) => finished.push(event) },
  });

  server.register("hello.v1.Greeter", "LotsOfReplies", RpcKind.ServerStreaming, () =>
    batchedBodies([[new Uint8Array([1]), new Uint8Array([2, 3])], [new Uint8Array([4])]]),
  );

  const call = new FakeNativeCall({ ...unaryRequest(), kind: RpcKind.ServerStreaming });
  native.handlers[0].handler(call);
  await waitForDispatch(call);

  assert.deepEqual(
    call.messageBatches.map((batch) => batch.map((body) => Array.from(body))),
    [[[1], [2, 3]]],
  );
  assert.deepEqual(
    call.messages.map((body) => Array.from(body)),
    [[4]],
  );
  assert.deepEqual(call.streamStatus, { status: Code.Ok, message: "", metadata: {} });
  assert.equal(finished[0].responseBodyLength, 4);
});

test("NodeServerCall sendMany falls back to single-message sends", async () => {
  const sent = [];
  const call = new NodeServerCall({
    request: { ...unaryRequest(), kind: RpcKind.ServerStreaming },
    sendMessage(body) {
      sent.push(Array.from(body));
      return Promise.resolve();
    },
    finishStream() {
      return Promise.resolve();
    },
    close() {},
  });

  await call.sendMany([new Uint8Array([1]), new Uint8Array([2, 3])]);

  assert.deepEqual(sent, [[1], [2, 3]]);
  assert.equal(call.responseBodyLength, 3);
});

test("NodeServerCall commits response byte metrics only after successful sends", async () => {
  let singleAttempts = 0;
  let batchAttempts = 0;
  const call = new NodeServerCall({
    request: { ...unaryRequest(), kind: RpcKind.ServerStreaming },
    sendMessage() {
      singleAttempts += 1;
      return singleAttempts === 1 ? Promise.reject(new Error("single failed")) : Promise.resolve();
    },
    sendMessages() {
      batchAttempts += 1;
      return batchAttempts === 1 ? Promise.reject(new Error("batch failed")) : Promise.resolve();
    },
    finishStream() {
      return Promise.resolve();
    },
    close() {},
  });

  await assert.rejects(call.sendMessage(new Uint8Array([1, 2])), /single failed/u);
  assert.equal(call.responseBodyLength, 0);
  await call.sendMessage(new Uint8Array([1, 2]));
  assert.equal(call.responseBodyLength, 2);

  const batch = [new Uint8Array([3]), new Uint8Array([4, 5])];
  await assert.rejects(call.sendMany(batch), /batch failed/u);
  assert.equal(call.responseBodyLength, 2);
  await call.sendMany(batch);
  assert.equal(call.responseBodyLength, 5);
});

test("NodeServerCall uses native unary and batched send methods", async () => {
  const used = [];
  const nativeCall = {
    request: { ...unaryRequest(), kind: RpcKind.ServerStreaming },
    respond() {
      used.push("respond");
      return Promise.resolve();
    },
    sendMessage() {
      used.push("sendMessage");
      return Promise.resolve();
    },
    sendMessages() {
      used.push("sendMessages");
      return Promise.resolve();
    },
    finishStream() {
      return Promise.resolve();
    },
    close() {},
  };
  const call = new NodeServerCall(nativeCall);

  await call.respond({ body: new Uint8Array([1]) });
  await call.sendMessage(new Uint8Array([2]));
  await call.sendMany([new Uint8Array([3]), new Uint8Array([4])]);

  assert.deepEqual(used, ["respond", "sendMessage", "sendMessages"]);
});

function batchedBodies(batches) {
  let index = 0;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    nextBatch() {
      if (index >= batches.length) {
        return Promise.resolve({ done: true, value: undefined });
      }
      return Promise.resolve({ done: false, value: batches[index++] });
    },
    next() {
      return Promise.reject(new Error("single-message iteration should not be used"));
    },
  };
}
