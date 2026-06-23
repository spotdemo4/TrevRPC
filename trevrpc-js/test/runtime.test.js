import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { generate as generateBindings } from "../src/generator.js";
import {
  Code,
  FrameReader,
  FrameTooLargeError,
  MaxMetadataEntries,
  MaxMetadataKeyLen,
  MaxMetadataTotalSize,
  MaxMetadataValueLen,
  RpcKind,
  RpcRequest,
  RpcResponse,
  RpcStreamFrame,
  RpcStreamFrameKind,
  WebTransportClient,
  WireVersion,
  bidirectionalStreaming,
  clientStreaming,
  connect,
  createRoot,
  decodeFrame,
  encodeFrame,
  frameBodyLength,
  invalidArgument,
  marshalMessage,
  normalizeMetadata,
  serverStreaming,
  unary,
  validateMetadata,
} from "../src/index.js";

const require = createRequire(import.meta.url);
const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");

test("frames round-trip TrevRPC requests", () => {
  const frame = encodeFrame(RpcRequest, {
    service: "hello.v1.Greeter",
    method: "SayHello",
    body: new Uint8Array([1, 2, 3]),
    metadata: normalizeMetadata({ Authorization: "Bearer token" }),
    kind: RpcKind.Unary,
    version: WireVersion,
    timeoutNanos: "0",
  });

  const decoded = decodeFrame(RpcRequest, frame.subarray(4));

  assert.equal(decoded.service, "hello.v1.Greeter");
  assert.equal(decoded.method, "SayHello");
  assert.deepEqual(decoded.body, new Uint8Array([1, 2, 3]));
  assert.equal(decoded.kind, RpcKind.Unary);
  assert.equal(decoded.version, WireVersion);
  assert.deepEqual(
    decoded.metadata.authorization,
    new Uint8Array([66, 101, 97, 114, 101, 114, 32, 116, 111, 107, 101, 110]),
  );
});

test("Node native transport subpath exports without loading the addon", async () => {
  const { NodeServer, NodeServerCall, NodeTransport } = await import("../src/node.js");

  assert.equal(typeof NodeTransport, "function");
  assert.equal(typeof NodeTransport.connect, "function");
  assert.equal(typeof NodeServer, "function");
  assert.equal(typeof NodeServer.listen, "function");
  assert.equal(typeof NodeServerCall, "function");
});

test("browser root connect flattens browser WebTransport options", async () => {
  let observed;
  const certificateHash = { algorithm: "sha-256", value: new Uint8Array([1]) };
  class FakeWebTransport {
    constructor(url, options) {
      observed = { url, options };
      this.ready = Promise.resolve();
    }

    close(closeInfo) {
      observed.closeInfo = closeInfo;
    }
  }

  const transport = await connect("https://example.test/trevrpc", {
    WebTransport: FakeWebTransport,
    allowPooling: true,
    serverCertificateHashes: [certificateHash],
    webTransportOptions: { allowPooling: false, requireUnreliable: true },
  });

  assert.ok(transport instanceof WebTransportClient);
  assert.equal(observed.url, "https://example.test/trevrpc");
  assert.deepEqual(observed.options, {
    allowPooling: false,
    requireUnreliable: true,
    serverCertificateHashes: [certificateHash],
  });

  transport.close({ closeCode: 0, reason: "done" });

  assert.deepEqual(observed.closeInfo, { closeCode: 0, reason: "done" });
});

test("package root connect uses native transport under Node", async () => {
  const directory = await mkdtemp(join(tmpdir(), "trevrpc-js-native-"));
  const fakeNativePath = join(directory, "fake-native.cjs");
  const previousNativePath = process.env.TREVRPC_JS_NATIVE;

  await writeFile(
    fakeNativePath,
    `module.exports = {
  async connectMsQuic(options) {
    return {
      options,
      async call() {
        return { status: 0, body: new Uint8Array(0) };
      },
      async startStream() {
        throw new Error("unused");
      },
      close() {
        this.closed = true;
      },
    };
  },
};
`,
  );

  try {
    process.env.TREVRPC_JS_NATIVE = fakeNativePath;
    const runtime = await import("trevrpc-js");
    const transport = await runtime.connect("https://example.test:444/trevrpc?mode=test", {
      skipCertificateValidation: true,
    });

    assert.equal(transport.constructor.name, "NodeTransport");
    assert.equal(transport.nativeClient.options.host, "example.test");
    assert.equal(transport.nativeClient.options.port, 444);
    assert.equal(transport.nativeClient.options.path, "/trevrpc?mode=test");
    assert.equal(transport.nativeClient.options.origin, "https://example.test:444");
    assert.equal(transport.nativeClient.options.skipCertificateValidation, true);
  } finally {
    if (previousNativePath == null) {
      delete process.env.TREVRPC_JS_NATIVE;
    } else {
      process.env.TREVRPC_JS_NATIVE = previousNativePath;
    }
    await rm(directory, { force: true, recursive: true });
  }
});

test("Node native addon loads when built", { skip: !existsSync(nativeAddonPath) }, () => {
  const native = require(nativeAddonPath);

  assert.equal(typeof native.connectMsQuic, "function");
  assert.equal(typeof native.listenMsQuic, "function");
});

test("frame length boundary cases are stable", () => {
  for (const length of [0, 1, 15, 16]) {
    const header = new Uint8Array(4);
    new DataView(header.buffer).setUint32(0, length, false);

    assert.equal(frameBodyLength(header, 16), length);
  }

  for (const length of [17, 0xffffffff]) {
    const header = new Uint8Array(4);
    new DataView(header.buffer).setUint32(0, length, false);

    assert.throws(() => frameBodyLength(header, 16), FrameTooLargeError);
  }
});

test("frame reader maps malformed protobuf bodies to invalid argument", async () => {
  for (const body of deterministicByteVectors()) {
    const frame = new Uint8Array(4 + body.byteLength);
    new DataView(frame.buffer).setUint32(0, body.byteLength, false);
    frame.set(body, 4);
    const reader = new FrameReader(fakeReaderFromChunks(chunkBytes(frame, 3)));

    try {
      await reader.readFrame(RpcRequest);
    } catch (error) {
      assert.equal(error.code, Code.InvalidArgument);
    }
  }
});

test("frame reader rejects large partial body without buffering advertised length", async () => {
  const header = new Uint8Array(4);
  new DataView(header.buffer).setUint32(0, 1 << 30, false);
  const reader = new FrameReader(fakeReaderFromChunks([header]));

  await assert.rejects(
    reader.readFrame(RpcRequest, 1 << 30),
    (error) => error.code === Code.Unavailable,
  );
});

test("metadata validation boundary cases are stable", () => {
  const maxEntries = {};
  for (let index = 0; index < MaxMetadataEntries; index += 1) {
    maxEntries[`key-${index}`] = new Uint8Array();
  }
  assert.doesNotThrow(() => validateMetadata(maxEntries));

  const tooManyEntries = { ...maxEntries, overflow: new Uint8Array() };
  assert.throws(
    () => validateMetadata(tooManyEntries),
    (error) => {
      assert.equal(error.code, Code.InvalidArgument);
      return true;
    },
  );

  assert.doesNotThrow(() => validateMetadata({ ["a".repeat(MaxMetadataKeyLen)]: [] }));
  assert.throws(
    () => validateMetadata({ ["a".repeat(MaxMetadataKeyLen + 1)]: [] }),
    (error) => {
      assert.equal(error.code, Code.InvalidArgument);
      return true;
    },
  );

  assert.doesNotThrow(() => validateMetadata({ key: new Uint8Array(MaxMetadataValueLen) }));
  assert.throws(
    () => validateMetadata({ key: new Uint8Array(MaxMetadataValueLen + 1) }),
    (error) => {
      assert.equal(error.code, Code.InvalidArgument);
      return true;
    },
  );

  const exactTotal = {};
  for (const key of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
    exactTotal[key] = new Uint8Array(8191);
  }
  assert.equal(
    Object.entries(exactTotal).reduce(
      (total, [key, value]) => total + key.length + value.length,
      0,
    ),
    MaxMetadataTotalSize,
  );
  assert.doesNotThrow(() => validateMetadata(exactTotal));

  exactTotal.i = new Uint8Array();
  assert.throws(
    () => validateMetadata(exactTotal),
    (error) => {
      assert.equal(error.code, Code.InvalidArgument);
      return true;
    },
  );
});

test("WebTransport malformed protobuf response frame maps to invalid argument", async () => {
  const stream = fakeBidirectionalStream({
    readableChunks: [new Uint8Array([0, 0, 0, 2, 0xff, 0xff])],
  });
  const client = new WebTransportClient({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });

  await assert.rejects(
    client.call(
      RpcRequest.create({
        service: "hello.v1.Greeter",
        method: "SayHello",
        body: new Uint8Array(),
        metadata: {},
        version: WireVersion,
      }),
    ),
    (error) => error.code === Code.InvalidArgument,
  );
});

test("WebTransport unary reads response before upload close settles", async () => {
  let closeCalled = false;
  let resolveClose;
  const closePromise = new Promise((resolve) => {
    resolveClose = resolve;
  });
  const stream = fakeBidirectionalStream({
    closePromise,
    onClose() {
      closeCalled = true;
    },
    readableChunks: [
      encodeFrame(
        RpcResponse,
        RpcResponse.create({
          status: Code.Ok,
          body: new Uint8Array([1, 2, 3]),
          metadata: {},
        }),
      ),
    ],
  });
  const client = new WebTransportClient({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });

  const response = await client.call(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "SayHello",
      body: new Uint8Array(),
      metadata: {},
      version: WireVersion,
    }),
  );

  assert.equal(closeCalled, true);
  assert.deepEqual(response.body, new Uint8Array([1, 2, 3]));
  resolveClose();
});

test("unknown response stream frame kind maps to invalid argument", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  });
  const Hello = root.lookupType("hello.v1.Hello");
  const transport = {
    async streamingCall() {
      return {
        async *[Symbol.asyncIterator]() {
          yield RpcStreamFrame.create({ kind: 99 });
        },
      };
    },
  };

  const stream = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
  );

  await assert.rejects(
    stream[Symbol.asyncIterator]().next(),
    (error) => error.code === Code.InvalidArgument,
  );
});

test("response streams consume transport frame batches", async () => {
  const Hello = helloTestType();
  let returned = false;
  const transport = {
    async streamingCall() {
      return batchedFrameStream(
        [
          [
            RpcStreamFrame.create({ body: Hello.encode({ value: "one" }).finish() }),
            RpcStreamFrame.create({ body: Hello.encode({ value: "two" }).finish() }),
            RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok }),
          ],
        ],
        () => {
          returned = true;
        },
      );
    },
  };

  const stream = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
    { streamIdleTimeoutMs: undefined },
  );
  const values = [];
  for await (const reply of stream) {
    values.push(reply.value);
  }

  assert.deepEqual(values, ["one", "two"]);
  assert.equal(returned, true);
});

test("request streams expose queued request batches", async () => {
  const Hello = helloTestType();
  let batchPromise;
  let requestIterator;
  const transport = {
    async streamingCall(_request, requestBody) {
      requestIterator = requestBody[Symbol.asyncIterator]();
      batchPromise = requestIterator.nextBatch(16);
      return batchedFrameStream([
        [
          RpcStreamFrame.create({ body: Hello.encode({ value: "done" }).finish() }),
          RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok }),
        ],
      ]);
    },
  };

  const call = await clientStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfGreetings",
    Hello,
    Hello,
    { streamIdleTimeoutMs: undefined },
  );
  const sends = [call.send({ value: "one" }), call.send({ value: "two" })];
  const batch = await batchPromise;
  await Promise.all(sends);
  await call.closeSend();
  const done = await requestIterator.nextBatch(16);
  const reply = await call.closeAndRecv();

  assert.equal(batch.done, false);
  assert.deepEqual(
    batch.value.map((body) => Hello.decode(body).value),
    ["one", "two"],
  );
  assert.equal(reply.value, "done");
  assert.equal(done.done, true);
});

test("Node transport writes request body batches with native sendMessages", async () => {
  const { NodeTransport } = await import("../src/node.js");
  const sentBatches = [];
  let finishSend;
  const finishDone = new Promise((resolve) => {
    finishSend = resolve;
  });
  const nativeClient = {
    async startStream() {
      return {
        async sendMessage() {
          throw new Error("single-message send should not be used for request batches");
        },
        async sendMessages(bodies) {
          sentBatches.push(bodies.map((body) => Array.from(body)));
        },
        async finishSend() {
          finishSend();
        },
        async recvMany() {
          await finishDone;
          return [RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok })];
        },
        close() {},
      };
    },
  };
  const transport = new NodeTransport(nativeClient);

  const responses = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfGreetings",
      kind: RpcKind.ClientStreaming,
      body: new Uint8Array(),
    },
    batchedFrameStream([[new Uint8Array([1]), new Uint8Array([2])]]),
  );
  const result = await responses[Symbol.asyncIterator]().next();

  assert.equal(result.done, false);
  assert.equal(result.value.kind, RpcStreamFrameKind.Status);
  assert.deepEqual(sentBatches, [[[1], [2]]]);
});

test("batched response stream reports EOF before final status after queued messages", async () => {
  const Hello = helloTestType();
  let returned = false;
  const transport = {
    async streamingCall() {
      return batchedFrameStream(
        [[RpcStreamFrame.create({ body: Hello.encode({ value: "one" }).finish() }), null]],
        () => {
          returned = true;
        },
      );
    },
  };

  const stream = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
    { streamIdleTimeoutMs: undefined },
  );
  const iterator = stream[Symbol.asyncIterator]();

  assert.equal((await iterator.next()).value.value, "one");
  await assert.rejects(iterator.next(), (error) => error.code === Code.Internal);
  assert.equal(returned, true);
});

test("batched response stream reports terminal error after queued messages", async () => {
  const Hello = helloTestType();
  let returned = false;
  const transport = {
    async streamingCall() {
      return batchedFrameStream(
        [
          [
            RpcStreamFrame.create({ body: Hello.encode({ value: "one" }).finish() }),
            RpcStreamFrame.create({
              kind: RpcStreamFrameKind.Status,
              status: Code.Unavailable,
              message: "down",
            }),
          ],
        ],
        () => {
          returned = true;
        },
      );
    },
  };

  const stream = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
    { streamIdleTimeoutMs: undefined },
  );
  const iterator = stream[Symbol.asyncIterator]();

  assert.equal((await iterator.next()).value.value, "one");
  await assert.rejects(iterator.next(), (error) => error.code === Code.Unavailable);
  assert.equal(returned, true);
});

test("response stream limits map to resource exhausted at boundaries", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  });
  const Hello = root.lookupType("hello.v1.Hello");
  const first = Hello.encode({ value: "one" }).finish();
  const second = Hello.encode({ value: "two" }).finish();
  const transport = {
    async streamingCall() {
      return {
        async *[Symbol.asyncIterator]() {
          yield RpcStreamFrame.create({ body: first });
          yield RpcStreamFrame.create({ body: second });
          yield RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok });
        },
      };
    },
  };

  const messageLimited = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
    { maxResponseMessages: 1 },
  );
  const messageIterator = messageLimited[Symbol.asyncIterator]();
  assert.equal((await messageIterator.next()).value.value, "one");
  await assert.rejects(messageIterator.next(), (error) => error.code === Code.ResourceExhausted);

  const bodyLimited = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
    { maxResponseStreamBodySize: first.byteLength },
  );
  const bodyIterator = bodyLimited[Symbol.asyncIterator]();
  assert.equal((await bodyIterator.next()).value.value, "one");
  await assert.rejects(bodyIterator.next(), (error) => error.code === Code.ResourceExhausted);
});

test("wire golden vectors stay stable", async () => {
  const vectors = await readWireGoldenVectors();
  const timeoutRequest = RpcRequest.create({
    service: "svc",
    method: "m",
    body: new Uint8Array([104, 105]),
    metadata: {},
    version: WireVersion,
    timeoutNanos: 123456,
  });
  const metadataRequest = RpcRequest.create({
    service: "svc",
    method: "m",
    body: new Uint8Array([104, 105]),
    metadata: { authorization: new Uint8Array([111, 107]) },
    version: WireVersion,
  });

  const tests = [
    {
      name: "rpc_request.unary",
      messageType: RpcRequest,
      message: RpcRequest.create({
        service: "svc",
        method: "m",
        body: new Uint8Array([104, 105]),
        metadata: {},
        version: WireVersion,
      }),
    },
    { name: "rpc_request.timeout", messageType: RpcRequest, message: timeoutRequest },
    { name: "rpc_request.metadata", messageType: RpcRequest, message: metadataRequest },
    {
      name: "rpc_stream_frame.message",
      messageType: RpcStreamFrame,
      message: RpcStreamFrame.create({
        body: new Uint8Array([104, 105]),
      }),
    },
    {
      name: "rpc_stream_frame.status",
      messageType: RpcStreamFrame,
      message: RpcStreamFrame.create({
        kind: RpcStreamFrameKind.Status,
        status: Code.Unavailable,
        message: "down",
      }),
    },
    {
      name: "rpc_response.ok_body",
      messageType: RpcResponse,
      message: RpcResponse.create({
        body: new Uint8Array([104, 105]),
      }),
    },
    {
      name: "rpc_response.unavailable",
      messageType: RpcResponse,
      message: RpcResponse.create({
        status: Code.Unavailable,
        message: "down",
      }),
    },
  ];

  for (const { name, messageType, message } of tests) {
    assert.equal(
      bytesToHex(marshalMessage(messageType, message)),
      vectors.get(`${name}.body`),
      name,
    );
    assert.equal(bytesToHex(encodeFrame(messageType, message)), vectors.get(`${name}.frame`), name);
  }
});

test("unary decodes protobuf.js response bodies", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  });
  const Hello = root.lookupType("hello.v1.Hello");

  const transport = {
    async call(request) {
      const decoded = Hello.decode(request.body);
      return RpcResponse.create({
        status: Code.Ok,
        body: Hello.encode({ value: `hello ${decoded.value}` }).finish(),
        metadata: {},
      });
    },
  };

  const response = await unary(transport, "hello.v1.Greeter", "SayHello", Hello, Hello, {
    value: "Trev",
  });

  assert.equal(response.value, "hello Trev");
});

test("unary timeout aborts transport signal", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  });
  const Hello = root.lookupType("hello.v1.Hello");
  let signal;

  const transport = {
    call(_request, options) {
      signal = options.signal;
      return new Promise((resolve, reject) => {
        options.signal.addEventListener("abort", () => reject(options.signal.reason), {
          once: true,
        });
      });
    },
  };

  await assert.rejects(
    unary(
      transport,
      "hello.v1.Greeter",
      "SayHello",
      Hello,
      Hello,
      { value: "Trev" },
      { timeoutMs: 1 },
    ),
    (error) => error.code === Code.DeadlineExceeded,
  );
  assert.equal(signal.aborted, true);
});

test("unary abort signal rejects when transport ignores cancellation", async () => {
  const Hello = helloTestType();
  let signal;
  const transport = {
    call(_request, options) {
      signal = options.signal;
      return new Promise(() => {});
    },
  };
  const controller = new AbortController();

  const call = unary(
    transport,
    "hello.v1.Greeter",
    "SayHello",
    Hello,
    Hello,
    { value: "Trev" },
    { signal: controller.signal },
  );
  while (signal == null) {
    await Promise.resolve();
  }
  controller.abort(new DOMException("user cancelled", "AbortError"));

  await assert.rejects(call, (error) => error.code === Code.Cancelled);
  assert.equal(signal.aborted, true);
});

test("server streaming abort signal rejects pending response read", async () => {
  const Hello = helloTestType();
  let returned = false;
  const transport = {
    async streamingCall() {
      return {
        [Symbol.asyncIterator]() {
          return {
            next() {
              return new Promise(() => {});
            },
            return() {
              returned = true;
              return Promise.resolve({ done: true, value: undefined });
            },
          };
        },
      };
    },
  };
  const controller = new AbortController();
  const stream = await serverStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfReplies",
    Hello,
    Hello,
    { value: "Trev" },
    { signal: controller.signal, streamIdleTimeoutMs: undefined },
  );

  const next = stream[Symbol.asyncIterator]().next();
  controller.abort(new DOMException("user cancelled", "AbortError"));

  await assert.rejects(next, (error) => error.code === Code.Cancelled);
  assert.equal(returned, true);
});

test("client streaming deadline cancels pending request upload", async () => {
  const Hello = helloTestType();
  let uploadDone;
  const transport = {
    async streamingCall(_request, requestBody) {
      uploadDone = (async () => {
        try {
          for await (const _body of requestBody) {
            // The request stream intentionally never yields in this test.
          }
        } catch {
          // The deadline cancels the upload side.
        }
      })();
      return {
        [Symbol.asyncIterator]() {
          return {
            next() {
              return new Promise(() => {});
            },
            return() {
              return Promise.resolve({ done: true, value: undefined });
            },
          };
        },
      };
    },
  };

  const call = await clientStreaming(
    transport,
    "hello.v1.Greeter",
    "LotsOfGreetings",
    Hello,
    Hello,
    {
      timeoutMs: 1,
      streamIdleTimeoutMs: undefined,
    },
  );
  await new Promise((resolve) => setTimeout(resolve, 5));
  await assert.rejects(call.closeAndRecv(), (error) => error.code === Code.DeadlineExceeded);
  await uploadDone;
});

test("bidirectional streaming abort signal cancels pending upload and response read", async () => {
  const Hello = helloTestType();
  let frameReturned = false;
  let uploadDone;
  const transport = {
    async streamingCall(_request, requestBody) {
      uploadDone = (async () => {
        try {
          for await (const _body of requestBody) {
            // The request stream intentionally never yields in this test.
          }
        } catch {
          // The caller's abort signal cancels the upload side.
        }
      })();
      return {
        [Symbol.asyncIterator]() {
          return {
            next() {
              return new Promise(() => {});
            },
            return() {
              frameReturned = true;
              return Promise.resolve({ done: true, value: undefined });
            },
          };
        },
      };
    },
  };
  const controller = new AbortController();

  const stream = await bidirectionalStreaming(
    transport,
    "hello.v1.Greeter",
    "BidiHello",
    Hello,
    Hello,
    { signal: controller.signal, streamIdleTimeoutMs: undefined },
  );
  const next = stream.recv();
  controller.abort(new DOMException("user cancelled", "AbortError"));

  await assert.rejects(next, (error) => error.code === Code.Cancelled);
  await uploadDone;
  assert.equal(frameReturned, true);
});

test("terminal streaming status cancels pending request upload", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  });
  const Hello = root.lookupType("hello.v1.Hello");
  let writerDone;
  const transport = {
    async streamingCall(_request, requestBody) {
      writerDone = (async () => {
        try {
          for await (const _body of requestBody) {
            // The request stream intentionally never yields in this test.
          }
        } catch {
          // The terminal response cancels the upload side.
        }
      })();
      return {
        async *[Symbol.asyncIterator]() {
          yield RpcStreamFrame.create({
            kind: RpcStreamFrameKind.Status,
            status: Code.Ok,
            metadata: {},
          });
        },
      };
    },
  };

  const stream = await bidirectionalStreaming(
    transport,
    "hello.v1.Greeter",
    "BidiHello",
    Hello,
    Hello,
  );
  const result = await stream[Symbol.asyncIterator]().next();
  await writerDone;

  assert.equal(result.done, true);
});

test("WebTransport timeout cleans up stream opened after abort", async () => {
  let resolveStream;
  let abortDone;
  let cancelDone;
  let abortReason;
  let cancelReason;
  const streamCleanup = Promise.all([
    new Promise((resolve) => {
      abortDone = resolve;
    }),
    new Promise((resolve) => {
      cancelDone = resolve;
    }),
  ]);
  const stream = fakeBidirectionalStream({
    onAbort(reason) {
      abortReason = reason;
      abortDone();
    },
    onCancel(reason) {
      cancelReason = reason;
      cancelDone();
    },
  });
  const client = new WebTransportClient({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return new Promise((resolve) => {
        resolveStream = resolve;
      });
    },
  });
  const controller = new AbortController();
  const call = client.call(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "SayHello",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.Unary,
      version: WireVersion,
    }),
    { signal: controller.signal },
  );

  while (resolveStream == null) {
    await Promise.resolve();
  }
  controller.abort(new DOMException("timeout", "AbortError"));
  await assert.rejects(call, (error) => error.code === Code.Cancelled);
  resolveStream(stream);
  await Promise.race([
    streamCleanup,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error("stream cleanup did not finish")), 100);
    }),
  ]);

  assert.equal(abortReason?.name, "AbortError");
  assert.equal(cancelReason?.name, "AbortError");
});

test("WebTransport terminal OK does not hide local upload error", async () => {
  const uploadError = invalidArgument("local upload failed");
  const stream = fakeBidirectionalStream({
    readableChunks: [
      encodeFrame(
        RpcStreamFrame,
        RpcStreamFrame.create({
          kind: RpcStreamFrameKind.Status,
          status: Code.Ok,
          metadata: {},
        }),
      ),
    ],
  });
  const client = new WebTransportClient({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });
  const requestBody = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          return Promise.reject(uploadError);
        },
      };
    },
  };

  const responses = await client.streamingCall(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "BidiHello",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.BidirectionalStreaming,
      version: WireVersion,
    }),
    requestBody,
  );

  await assert.rejects(
    responses[Symbol.asyncIterator]().next(),
    (error) => error.code === Code.InvalidArgument,
  );
});

test("WebTransport terminal OK ignores browser upload cleanup transport closes", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  });
  const Hello = root.lookupType("hello.v1.Hello");
  for (const closeError of [
    new Error("stream canceled with error code 0"),
    new Error("Received STOP_SENDING."),
  ]) {
    const stream = fakeBidirectionalStream({
      closeError,
      readableChunks: [
        encodeFrame(
          RpcStreamFrame,
          RpcStreamFrame.create({
            kind: RpcStreamFrameKind.Status,
            status: Code.Ok,
            metadata: {},
          }),
        ),
      ],
    });
    const client = new WebTransportClient({
      ready: Promise.resolve(),
      createBidirectionalStream() {
        return Promise.resolve(stream);
      },
    });

    const responses = await bidirectionalStreaming(
      client,
      "hello.v1.Greeter",
      "BidiHello",
      Hello,
      Hello,
    );

    assert.equal(await responses.recv(), undefined);
  }
});

test("WebTransport terminal error wins over local upload error", async () => {
  const stream = fakeBidirectionalStream({
    readableChunks: [
      encodeFrame(
        RpcStreamFrame,
        RpcStreamFrame.create({
          kind: RpcStreamFrameKind.Status,
          status: Code.PermissionDenied,
          message: "remote rejected upload",
          metadata: {},
        }),
      ),
    ],
  });
  const client = new WebTransportClient({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });
  const requestBody = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          return Promise.reject(invalidArgument("local upload failed"));
        },
      };
    },
  };

  const responses = await client.streamingCall(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "BidiHello",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.BidirectionalStreaming,
      version: WireVersion,
    }),
    requestBody,
  );

  const result = await responses[Symbol.asyncIterator]().next();
  assert.equal(result.value.status, Code.PermissionDenied);
});

test("WebTransport response stream return is idempotent after local upload error", async () => {
  const uploadError = invalidArgument("local upload failed");
  let aborts = 0;
  let cancels = 0;
  const stream = fakeBidirectionalStream({
    onAbort() {
      aborts += 1;
    },
    onCancel() {
      cancels += 1;
    },
  });
  const client = new WebTransportClient({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });
  const requestBody = {
    [Symbol.asyncIterator]() {
      let first = true;
      return {
        next() {
          if (first) {
            first = false;
            return Promise.resolve({ done: false, value: new Uint8Array([1]) });
          }
          return Promise.reject(uploadError);
        },
      };
    },
  };

  const responses = await client.streamingCall(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "BidiHello",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.BidirectionalStreaming,
      version: WireVersion,
    }),
    requestBody,
  );

  await assert.rejects(responses.return(), (error) => error.code === Code.InvalidArgument);
  const abortsAfterFirstReturn = aborts;
  const cancelsAfterFirstReturn = cancels;

  assert.deepEqual(await responses.return(), { done: true, value: undefined });
  assert.equal(aborts, abortsAfterFirstReturn);
  assert.equal(cancels, cancelsAfterFirstReturn);
});

test("generator emits JavaScript service clients", () => {
  const response = generateBindings(greeterRequest());
  const generatedJavaScript = generatedFile(response, "hello/v1/greeter.trevrpc.js");
  const generatedTypes = generatedFile(response, "hello/v1/greeter.trevrpc.d.ts");

  assert.equal(response.error, undefined);
  assert.equal(response.file.length, 2);
  assert.match(generatedJavaScript.content, /export class GreeterClient/);
  assert.match(generatedJavaScript.content, /sayHello\(request, options = \{\}\)/);
  assert.match(generatedJavaScript.content, /lotsOfReplies\(request, options = \{\}\)/);
  assert.match(generatedJavaScript.content, /lotsOfGreetings\(options = \{\}\)/);
  assert.match(generatedJavaScript.content, /bidiHello\(options = \{\}\)/);
  assert.match(generatedTypes.content, /export interface HelloRequest/);
  assert.match(generatedTypes.content, /name\?: string;/);
  assert.match(generatedTypes.content, /export interface HelloReply/);
  assert.match(
    generatedTypes.content,
    /sayHello\(request: HelloRequest, options\?: CallOptions\): Promise<HelloReply>;/,
  );
  assert.match(
    generatedTypes.content,
    /lotsOfReplies\(request: HelloRequest, options\?: CallOptions\): Promise<AsyncIterable<HelloReply>>;/,
  );
  assert.match(
    generatedTypes.content,
    /lotsOfGreetings\(options\?: CallOptions\): Promise<ClientStreamingCall<HelloRequest, HelloReply>>;/,
  );
  assert.match(
    generatedTypes.content,
    /bidiHello\(options\?: CallOptions\): Promise<BidirectionalStreamingCall<HelloRequest, HelloReply>>;/,
  );
});

test("generated JavaScript clients call the runtime", async () => {
  const runtimeImport = pathToFileURL(join(process.cwd(), "src", "index.js")).href;
  const response = generateBindings(greeterRequest(`runtime_import=${runtimeImport}`));
  assert.equal(response.error, undefined);

  const directory = await mkdtemp(join(tmpdir(), "trevrpc-js-generated-"));
  const generatedPath = join(directory, "greeter.trevrpc.js");
  await writeFile(generatedPath, generatedFile(response, "hello/v1/greeter.trevrpc.js").content);

  const generated = await import(pathToFileURL(generatedPath).href);
  const HelloRequest = generated.root.lookupType("hello.v1.HelloRequest");
  const HelloReply = generated.root.lookupType("hello.v1.HelloReply");
  const transport = {
    async call(request) {
      const decoded = HelloRequest.decode(request.body);
      assert.equal(request.service, "hello.v1.Greeter");
      assert.equal(request.method, "SayHello");
      return RpcResponse.create({
        status: Code.Ok,
        body: HelloReply.encode({ message: `hello ${decoded.name}` }).finish(),
        metadata: {},
      });
    },
  };

  const client = new generated.GreeterClient(transport);
  const reply = await client.sayHello({ name: "Trev" });

  assert.equal(reply.message, "hello Trev");
});

test("generated TypeScript declarations include imported message types", () => {
  const response = generateBindings(importedTypeRequest());
  const generatedTypes = generatedFile(response, "hello/v1/greeter.trevrpc.d.ts");

  assert.equal(response.error, undefined);
  assert.match(generatedTypes.content, /export interface SharedRequest/);
  assert.match(generatedTypes.content, /sayHello\(request: SharedRequest/);
});

test("checked-in greeter example binding targets the shared service", async () => {
  const example = await import("../examples/greeter/greeter.trevrpc.js");

  assert.equal(example.GreeterService.fullName, "example.greeter.Greeter");
  assert.deepEqual(Object.keys(example.GreeterService.methods), [
    "sayHello",
    "lotsOfReplies",
    "lotsOfGreetings",
    "bidiHello",
  ]);
  assert.equal(example.root.lookupType("example.greeter.HelloRequest").fields.name.id, 1);
  assert.equal(example.root.lookupType("example.greeter.HelloReply").fields.message.id, 1);
});

function helloTestType() {
  return createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  }).lookupType("hello.v1.Hello");
}

function batchedFrameStream(batches, onReturn) {
  let index = 0;
  return {
    [Symbol.asyncIterator]() {
      return {
        next() {
          throw new Error("single-frame next should not be used for batched streams");
        },
        nextBatch() {
          if (index >= batches.length) {
            return Promise.resolve({ done: true, value: undefined });
          }
          return Promise.resolve({ done: false, value: batches[index++] });
        },
        return() {
          onReturn?.();
          return Promise.resolve({ done: true, value: undefined });
        },
      };
    },
  };
}

function greeterRequest(parameter = "") {
  return {
    parameter,
    fileToGenerate: ["hello/v1/greeter.proto"],
    protoFile: [
      {
        name: "hello/v1/greeter.proto",
        package: "hello.v1",
        messageType: [
          {
            name: "HelloRequest",
            field: [{ name: "name", number: 1, label: 1, type: 9, jsonName: "name" }],
          },
          {
            name: "HelloReply",
            field: [{ name: "message", number: 1, label: 1, type: 9, jsonName: "message" }],
          },
        ],
        service: [
          {
            name: "Greeter",
            method: [
              {
                name: "SayHello",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
              },
              {
                name: "LotsOfReplies",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
                serverStreaming: true,
              },
              {
                name: "LotsOfGreetings",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
                clientStreaming: true,
              },
              {
                name: "BidiHello",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
                clientStreaming: true,
                serverStreaming: true,
              },
            ],
          },
        ],
      },
    ],
  };
}

function importedTypeRequest() {
  return {
    fileToGenerate: ["hello/v1/greeter.proto"],
    protoFile: [
      {
        name: "shared/v1/request.proto",
        package: "shared.v1",
        messageType: [
          {
            name: "SharedRequest",
            field: [{ name: "name", number: 1, label: 1, type: 9, jsonName: "name" }],
          },
        ],
      },
      {
        name: "hello/v1/greeter.proto",
        package: "hello.v1",
        dependency: ["shared/v1/request.proto"],
        messageType: [
          {
            name: "HelloReply",
            field: [{ name: "message", number: 1, label: 1, type: 9, jsonName: "message" }],
          },
        ],
        service: [
          {
            name: "Greeter",
            method: [
              {
                name: "SayHello",
                inputType: ".shared.v1.SharedRequest",
                outputType: ".hello.v1.HelloReply",
              },
            ],
          },
        ],
      },
    ],
  };
}

async function readWireGoldenVectors() {
  const text = await readFile(
    new URL("../../testdata/wire-golden-vectors.txt", import.meta.url),
    "utf8",
  );
  const vectors = new Map();

  for (const [index, rawLine] of text.split(/\r?\n/u).entries()) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) {
      continue;
    }

    const separator = line.indexOf("=");
    assert.notEqual(separator, -1, `invalid wire golden vector line ${index + 1}`);

    const value = line
      .slice(separator + 1)
      .trim()
      .toLowerCase();
    assert.match(value, /^(?:[0-9a-f]{2})*$/u, `invalid wire golden vector line ${index + 1}`);

    vectors.set(line.slice(0, separator).trim(), value);
  }

  return vectors;
}

function bytesToHex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function deterministicByteVectors() {
  const vectors = [new Uint8Array(), new Uint8Array([0xff]), new Uint8Array([0xff, 0xff])];
  let state = 0x12345678;
  for (let length = 1; length <= 32; length += 1) {
    const bytes = new Uint8Array(length);
    for (let index = 0; index < length; index += 1) {
      state = (state * 1664525 + 1013904223) >>> 0;
      bytes[index] = state & 0xff;
    }
    vectors.push(bytes);
  }

  return vectors;
}

function chunkBytes(bytes, chunkSize) {
  const chunks = [];
  for (let offset = 0; offset < bytes.byteLength; offset += chunkSize) {
    chunks.push(bytes.subarray(offset, offset + chunkSize));
  }
  return chunks;
}

function fakeReaderFromChunks(chunks) {
  const pending = [...chunks];
  return {
    read() {
      const value = pending.shift();
      return Promise.resolve(
        value == null ? { done: true, value: undefined } : { done: false, value },
      );
    },
  };
}

function fakeBidirectionalStream({
  closeError = null,
  closePromise = null,
  onAbort = () => {},
  onCancel = () => {},
  onClose = () => {},
  readableChunks = [],
} = {}) {
  const chunks = [...readableChunks];
  return {
    writable: {
      getWriter() {
        return {
          write() {
            return Promise.resolve();
          },
          close() {
            onClose();
            if (closePromise != null) {
              return closePromise;
            }
            return closeError == null ? Promise.resolve() : Promise.reject(closeError);
          },
          abort(reason) {
            onAbort(reason);
            return Promise.resolve();
          },
          releaseLock() {},
        };
      },
    },
    readable: {
      getReader() {
        return {
          read() {
            const value = chunks.shift();
            return Promise.resolve(
              value == null ? { done: true, value: undefined } : { done: false, value },
            );
          },
          cancel(reason) {
            onCancel(reason);
            return Promise.resolve();
          },
          releaseLock() {},
        };
      },
    },
  };
}

function generatedFile(response, name) {
  const file = response.file.find((candidate) => candidate.name === name);
  assert.ok(file, `missing generated file ${name}`);
  return file;
}
