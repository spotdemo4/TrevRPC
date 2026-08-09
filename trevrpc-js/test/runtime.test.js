import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { constants as osConstants, tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { RawWebTransport } from "@trevrpc/trevrpc-js/advanced";

import { generate as generateBindings } from "../src/generator.js";
import {
  Channel,
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
  TrevRpcError,
  WireVersion,
  bidirectionalStreaming,
  clientStreaming,
  connect,
  createRoot,
  createServiceClient,
  decodeFrame,
  decodeStreamFrameBody,
  encodeFrame,
  encodeMessageStreamFrame,
  encodeMessageStreamFrames,
  frameBodyLength,
  invalidArgument,
  marshalMessage,
  normalizeMetadata,
  serverStreaming,
  unary,
  validateMetadata,
} from "../src/index.js";
import { loadNativeAddon } from "../src/native-loader.js";
import { waitForWebTransportReady } from "../src/webtransport.js";

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
  const node = await import("../src/node-index.js");
  const advanced = await import("@trevrpc/trevrpc-js/node/advanced");

  assert.equal(typeof node.Channel, "function");
  assert.equal(typeof node.Channel.connect, "function");
  assert.equal(typeof node.NodeServer, "function");
  assert.equal(typeof node.NodeServer.listen, "function");
  assert.equal(typeof node.NodeServerCall, "function");
  assert.equal(node.RawNodeTransport, undefined);
  assert.equal(typeof advanced.RawNodeTransport, "function");
  assert.equal(typeof advanced.RawNodeTransport.connect, "function");
});

test("package entry points separate channels and raw transports", async () => {
  const root = await import("@trevrpc/trevrpc-js");
  const node = await import("@trevrpc/trevrpc-js/node");
  const browserAdvanced = await import("@trevrpc/trevrpc-js/advanced");
  const nodeAdvanced = await import("@trevrpc/trevrpc-js/node/advanced");

  assert.equal(typeof root.connect, "function");
  assert.equal(typeof root.Channel, "function");
  assert.equal(typeof node.Channel, "function");
  assert.equal(root.Channel, node.Channel);
  assert.equal(root.RawWebTransport, undefined);
  assert.equal(root.RawNodeTransport, undefined);
  assert.equal(root.RequestWriterSettlement, undefined);
  assert.equal(node.RawNodeTransport, undefined);
  assert.equal(typeof browserAdvanced.RawWebTransport, "function");
  assert.equal(typeof nodeAdvanced.RawNodeTransport, "function");
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
    requireUnreliable: true,
    serverCertificateHashes: [certificateHash],
  });

  assert.ok(transport instanceof Channel);
  assert.equal(observed.url, "https://example.test/trevrpc");
  assert.deepEqual(observed.options, {
    allowPooling: true,
    requireUnreliable: true,
    serverCertificateHashes: [certificateHash],
  });

  transport.close({ closeCode: 0, reason: "done" });

  assert.deepEqual(observed.closeInfo, { closeCode: 0, reason: "done" });
});

test("browser WebTransport connect reports unsupported runtime", async () => {
  await assert.rejects(
    RawWebTransport.connect("https://example.test/trevrpc", { WebTransport: {} }),
    (error) => error.code === Code.Unavailable,
  );
});

test("browser WebTransport readiness rejects when the session closes first", async () => {
  const closedError = new Error("session failed");
  await assert.rejects(
    waitForWebTransportReady({
      ready: new Promise(() => {}),
      closed: Promise.reject(closedError),
    }),
    (error) => error === closedError,
  );

  await assert.rejects(
    waitForWebTransportReady({
      ready: new Promise(() => {}),
      closed: Promise.resolve(),
    }),
    (error) =>
      error.code === Code.Unavailable && /closed before becoming ready/u.test(error.statusMessage),
  );
});

test("browser raw transport maps explicit abort reasons to cancellation with cause", async () => {
  const reason = new Error("stop browser call");
  const controller = new AbortController();
  controller.abort(reason);
  const client = new RawWebTransport({ ready: Promise.resolve() });

  await assert.rejects(client.call({}, { signal: controller.signal }), (error) => {
    assert.ok(error instanceof TrevRpcError);
    assert.equal(error.code, Code.Cancelled);
    assert.equal(error.cause, reason);
    return true;
  });
});

test("browser WebTransport stream open reports unsupported bidirectional streams", async () => {
  const client = new RawWebTransport({ ready: Promise.resolve() });

  await assert.rejects(
    client.openBidirectionalStream(),
    (error) => error.code === Code.Unavailable,
  );
});

test("Node transport forwards metadata, version, and timeout to native client", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  const metadata = { authorization: new Uint8Array([1, 2, 3]) };
  let unaryRequest;
  let streamRequest;
  let finishedSend = false;
  let finishSend;
  const finishSendDone = new Promise((resolve) => {
    finishSend = resolve;
  });
  const nativeClient = {
    async call(request) {
      unaryRequest = request;
      return { status: Code.Ok, body: new Uint8Array([9]), metadata: {} };
    },
    async startStream(request) {
      streamRequest = request;
      return {
        async sendMessage() {
          throw new Error("server-streaming request should not send body messages");
        },
        async finishSend() {
          finishedSend = true;
          finishSend();
        },
        async recvMany() {
          await finishSendDone;
          return [RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok })];
        },
        close() {},
      };
    },
  };
  const transport = new RawNodeTransport(nativeClient);

  const unaryResponse = await transport.call({
    service: "hello.v1.Greeter",
    method: "SayHello",
    body: new Uint8Array([4]),
    metadata,
    version: WireVersion,
    timeoutNanos: "123000000",
  });
  const stream = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      kind: RpcKind.ServerStreaming,
      body: new Uint8Array([5]),
      metadata,
      version: WireVersion,
      timeoutNanos: "456000000",
    },
    emptyAsyncIterable(),
  );
  const firstFrame = await stream[Symbol.asyncIterator]().next();

  assert.equal(unaryResponse.status, Code.Ok);
  assert.equal(unaryRequest.service, "hello.v1.Greeter");
  assert.equal(unaryRequest.method, "SayHello");
  assert.deepEqual(unaryRequest.body, new Uint8Array([4]));
  assert.deepEqual(unaryRequest.metadata, metadata);
  assert.equal(unaryRequest.version, WireVersion);
  assert.equal(unaryRequest.timeoutNanos, "123000000");
  assert.equal(streamRequest.kind, RpcKind.ServerStreaming);
  assert.deepEqual(streamRequest.metadata, metadata);
  assert.equal(streamRequest.version, WireVersion);
  assert.equal(streamRequest.timeoutNanos, "456000000");
  assert.equal(firstFrame.done, false);
  assert.equal(firstFrame.value.kind, RpcStreamFrameKind.Status);
  assert.equal(finishedSend, true);
});

test("Node transport aborts native unary calls without closing the client", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  let cancellation;
  let clientClosed = false;
  let callCancellation;
  const nativeClient = {
    createCancellation() {
      cancellation = {
        cancelled: false,
        cancel() {
          this.cancelled = true;
        },
      };
      return cancellation;
    },
    async call(_request, observedCancellation) {
      callCancellation = observedCancellation;
      while (!observedCancellation.cancelled) {
        await Promise.resolve();
      }
      throw new Error("native call cancelled");
    },
    close() {
      clientClosed = true;
    },
  };
  const transport = new RawNodeTransport(nativeClient);
  const controller = new AbortController();
  const call = transport.call(
    {
      service: "hello.v1.Greeter",
      method: "SayHello",
      body: new Uint8Array(),
      metadata: {},
      version: WireVersion,
    },
    { signal: controller.signal },
  );

  await Promise.resolve();
  controller.abort();

  await assert.rejects(call, (error) => error.code === Code.Cancelled);
  assert.equal(cancellation.cancelled, true);
  assert.equal(callCancellation, cancellation);
  assert.equal(clientClosed, false);
});

test("Node transport aborts native stream setup and established streams", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  let setupCancellation;
  let nativeStream;
  let setupCancelled;
  const setupCancelledDone = new Promise((resolve) => {
    setupCancelled = resolve;
  });
  const nativeClient = {
    createCancellation() {
      setupCancellation = {
        cancelled: false,
        cancel() {
          this.cancelled = true;
          setupCancelled();
        },
      };
      return setupCancellation;
    },
    async startStream(_request, cancellation) {
      await setupCancelledDone;
      if (cancellation.cancelled) {
        throw new Error("stream setup cancelled");
      }
      throw new Error("unexpected setup completion");
    },
  };
  const transport = new RawNodeTransport(nativeClient);
  const setupController = new AbortController();
  const setup = transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      kind: RpcKind.ServerStreaming,
      body: new Uint8Array(),
      metadata: {},
      version: WireVersion,
    },
    emptyAsyncIterable(),
    { signal: setupController.signal },
  );

  await Promise.resolve();
  setupController.abort();
  await assert.rejects(setup, (error) => error.code === Code.Cancelled);
  assert.equal(setupCancellation.cancelled, true);

  const streamController = new AbortController();
  const establishedClient = {
    createCancellation() {
      return { cancel() {} };
    },
    async startStream() {
      nativeStream = {
        closed: false,
        async finishSend() {},
        async recvMany() {
          while (!this.closed) {
            await Promise.resolve();
          }
          throw new Error("stream closed");
        },
        close() {
          this.closed = true;
        },
      };
      return nativeStream;
    },
  };
  const established = await new RawNodeTransport(establishedClient).streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      kind: RpcKind.ServerStreaming,
      body: new Uint8Array(),
      metadata: {},
      version: WireVersion,
    },
    emptyAsyncIterable(),
    { signal: streamController.signal },
  );
  const next = established[Symbol.asyncIterator]().next();

  await Promise.resolve();
  streamController.abort();

  await assert.rejects(next, (error) => error.code === Code.Cancelled);
  assert.equal(nativeStream.closed, true);
});

test(
  "native loader loads the source-checkout addon when built",
  { skip: !existsSync(nativeAddonPath) },
  () => {
    const native = loadNativeAddon({
      loadOptionalPackage: false,
      loadBundledAddon: false,
    });

    assert.equal(typeof native.connectMsQuic, "function");
    assert.equal(typeof native.listenMsQuic, "function");
  },
);

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

test("frame size failures use the public TrevRpcError hierarchy", () => {
  const error = new FrameTooLargeError(17, 16);

  assert.ok(error instanceof TrevRpcError);
  assert.equal(error.code, Code.ResourceExhausted);
  assert.equal(error.statusMessage, "frame length 17 exceeds maximum 16");
});

test("stream message fast path matches protobuf encoding", () => {
  const body = new Uint8Array([1, 2, 3]);
  const fast = encodeMessageStreamFrame(body);
  const generic = encodeFrame(RpcStreamFrame, RpcStreamFrame.create({ body }));

  assert.deepEqual(fast, generic);
  const decoded = decodeStreamFrameBody(fast.subarray(4));
  assert.equal(decoded.kind, RpcStreamFrameKind.Message);
  assert.equal(decoded.status, Code.Ok);
  assert.deepEqual(decoded.body, body);
});

test("batched stream message fast path concatenates frames", () => {
  const bodies = [new Uint8Array([1]), new Uint8Array([2, 3])];
  const batched = encodeMessageStreamFrames(bodies);
  const expected = concatBytes(bodies.map((body) => encodeMessageStreamFrame(body)));

  assert.deepEqual(batched, expected);
});

test("frame reader drains buffered stream frame batches", async () => {
  const bodies = [new Uint8Array([1]), new Uint8Array([2]), new Uint8Array([3])];
  const reader = new FrameReader(fakeReaderFromChunks([encodeMessageStreamFrames(bodies)]));

  const batch = await reader.readStreamFrameBatchOrEOF(8);

  assert.deepEqual(
    batch.map((frame) => Array.from(frame.body)),
    [[1], [2], [3]],
  );
});

test("frame reader reads stream message body batches without frame objects", async () => {
  const bodies = [new Uint8Array([1]), new Uint8Array([2]), new Uint8Array([3])];
  const status = encodeFrame(
    RpcStreamFrame,
    RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok, metadata: {} }),
  );
  const wire = concatBytes([encodeMessageStreamFrames(bodies), status]);
  const reader = new FrameReader(fakeReaderFromChunks([wire]));

  const batch = await reader.readStreamMessageBodyBatchOrEOF(8);

  assert.deepEqual(
    batch.bodies.map((body) => Array.from(body)),
    [[1], [2], [3]],
  );
  assert.equal(batch.status.kind, RpcStreamFrameKind.Status);
  assert.equal(batch.status.status, Code.Ok);
});

test("frame reader body batches handle split chunks", async () => {
  const bodies = [new Uint8Array([1]), new Uint8Array([2, 3]), new Uint8Array([4])];
  const reader = new FrameReader(
    fakeReaderFromChunks(chunkBytes(encodeMessageStreamFrames(bodies), 2)),
  );
  const decoded = [];

  for (;;) {
    const batch = await reader.readStreamMessageBodyBatchOrEOF(8);
    if (batch == null) {
      break;
    }
    decoded.push(...batch.bodies.map((body) => Array.from(body)));
  }

  assert.deepEqual(decoded, [[1], [2, 3], [4]]);
});

test("frame reader body batch keeps contiguous message bodies as subarrays", async () => {
  const frame = encodeMessageStreamFrame(new Uint8Array([1, 2, 3]));
  const reader = new FrameReader(fakeReaderFromChunks([frame]));

  const batch = await reader.readStreamMessageBodyBatchOrEOF(1);

  assert.equal(batch.bodies[0].buffer, frame.buffer);
  assert.deepEqual(batch.bodies[0], new Uint8Array([1, 2, 3]));
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
  const client = new RawWebTransport({
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
  const client = new RawWebTransport({
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

test("WebTransport writes request body batches", async () => {
  const writes = [];
  const bodies = [new Uint8Array([1]), new Uint8Array([2, 3])];
  const stream = fakeBidirectionalStream({
    onWrite(chunk) {
      writes.push(chunk);
    },
  });
  const client = new RawWebTransport({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });
  const requestBody = {
    [Symbol.asyncIterator]() {
      let sent = false;
      return {
        next() {
          throw new Error("single request body reads should not be used");
        },
        nextBatch() {
          if (sent) {
            return Promise.resolve({ done: true, value: undefined });
          }
          sent = true;
          return Promise.resolve({ done: false, value: bodies });
        },
      };
    },
  };

  const responses = await client.streamingCall(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "LotsOfGreetings",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.ClientStreaming,
      version: WireVersion,
    }),
    requestBody,
  );
  for (let attempt = 0; attempt < 10 && writes.length < 2; attempt += 1) {
    await Promise.resolve();
  }

  assert.equal(writes.length, 2);
  assert.deepEqual(writes[1], encodeMessageStreamFrames(bodies));
  await responses[Symbol.asyncIterator]().return();
});

test("WebTransport splits request body batches at the fixed byte limit", async () => {
  const writes = [];
  const bodies = [new Uint8Array(32 * 1024), new Uint8Array(32 * 1024), new Uint8Array([1])];
  const stream = fakeBidirectionalStream({
    onWrite(chunk) {
      writes.push(chunk);
    },
  });
  const client = new RawWebTransport({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });
  const requestBody = {
    [Symbol.asyncIterator]() {
      let sent = false;
      return {
        nextBatch() {
          if (sent) {
            return Promise.resolve({ done: true, value: undefined });
          }
          sent = true;
          return Promise.resolve({ done: false, value: bodies });
        },
      };
    },
  };

  const responses = await client.streamingCall(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "LotsOfGreetings",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.ClientStreaming,
      version: WireVersion,
    }),
    requestBody,
  );
  for (let attempt = 0; attempt < 10 && writes.length < 3; attempt += 1) {
    await Promise.resolve();
  }

  assert.equal(writes.length, 3);
  assert.deepEqual(writes[1], encodeMessageStreamFrames(bodies.slice(0, 2)));
  assert.deepEqual(writes[2], encodeMessageStreamFrames(bodies.slice(2)));
  await responses[Symbol.asyncIterator]().return();
});

test("WebTransport response frame queue drains messages before terminal status", async () => {
  const bodies = [new Uint8Array([1]), new Uint8Array([2])];
  const status = RpcStreamFrame.create({
    kind: RpcStreamFrameKind.Status,
    status: Code.Ok,
    metadata: {},
  });
  const stream = fakeBidirectionalStream({
    readableChunks: [
      concatBytes([encodeMessageStreamFrames(bodies), encodeFrame(RpcStreamFrame, status)]),
    ],
  });
  const client = new RawWebTransport({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });

  const responses = await client.streamingCall(
    RpcRequest.create({
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      body: new Uint8Array(),
      metadata: {},
      kind: RpcKind.ServerStreaming,
      version: WireVersion,
    }),
    emptyAsyncIterable(),
  );
  const iterator = responses[Symbol.asyncIterator]();
  const first = await iterator.nextBatch(8);
  const second = await iterator.nextBatch(8);

  assert.equal(first.done, false);
  assert.deepEqual(
    first.value.map((frame) => Array.from(frame.body)),
    [[1], [2]],
  );
  assert.equal(second.done, false);
  assert.equal(second.value.length, 1);
  assert.equal(second.value[0].kind, RpcStreamFrameKind.Status);
  assert.equal(second.value[0].status, Code.Ok);
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

test("response stream classifies malformed reads after terminal status as trailing", async () => {
  const Hello = helloTestType();
  const malformed = invalidArgument("malformed trailing frame");
  malformed.reason = "malformed_protobuf";
  const transport = {
    async streamingCall() {
      return {
        async *[Symbol.asyncIterator]() {
          yield RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok });
          throw malformed;
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
    { streamIdleTimeoutMs: undefined },
  );

  await assert.rejects(
    stream[Symbol.asyncIterator]().next(),
    (error) => error.code === Code.Internal && error.reason === "trailing_frame",
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

test("response streams consume transport body batches", async () => {
  const Hello = helloTestType();
  let returned = false;
  let index = 0;
  const batches = [
    {
      bodies: [marshalMessage(Hello, { value: "one" }), marshalMessage(Hello, { value: "two" })],
      status: null,
    },
    {
      bodies: [],
      status: RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok }),
    },
  ];
  const transport = {
    async streamingCall() {
      return {
        [Symbol.asyncIterator]() {
          return {
            next() {
              throw new Error("frame next should not be used for body-batched streams");
            },
            nextBatch() {
              throw new Error("frame batches should not be used for body-batched streams");
            },
            nextBodyBatch() {
              if (index >= batches.length) {
                return Promise.resolve({ done: true, value: undefined });
              }
              return Promise.resolve({ done: false, value: batches[index++] });
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

test("request streams expose sendMany batches", async () => {
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
  const sent = call.sendMany([{ value: "one" }, { value: "two" }]);
  const batch = await batchPromise;
  await sent;
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
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
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
  const transport = new RawNodeTransport(nativeClient);

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

test("Node transport uses native unary and stream send methods", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  const used = [];
  const makeStream = () => {
    let finished = false;
    return {
      sendMessage() {
        used.push("sendMessage");
        return Promise.resolve();
      },
      sendMessages() {
        used.push("sendMessages");
        return Promise.resolve();
      },
      finishSend() {
        finished = true;
        return Promise.resolve();
      },
      async recvMany() {
        while (!finished) {
          await Promise.resolve();
        }
        return [RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok })];
      },
      close() {},
    };
  };
  const nativeClient = {
    call() {
      used.push("call");
      return Promise.resolve({ status: Code.Ok, body: new Uint8Array() });
    },
    startStream() {
      used.push("startStream");
      return Promise.resolve(makeStream());
    },
  };
  const transport = new RawNodeTransport(nativeClient);

  await transport.call({ service: "svc", method: "copy", body: new Uint8Array([1]) });
  await transport.call({ service: "svc", method: "second", body: new Uint8Array([2]) });
  const single = await transport.streamingCall(
    { service: "svc", method: "single", kind: RpcKind.BidirectionalStreaming },
    batchedFrameStream([[new Uint8Array([3])]]),
  );
  await single.next();
  const batch = await transport.streamingCall(
    { service: "svc", method: "batch", kind: RpcKind.BidirectionalStreaming },
    batchedFrameStream([[new Uint8Array([4]), new Uint8Array([5])]]),
  );
  await batch.next();

  assert.deepEqual(used, [
    "call",
    "call",
    "startStream",
    "sendMessage",
    "startStream",
    "sendMessages",
  ]);
});

test("Node transport returns native response bodies with terminal status", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  const status = RpcStreamFrame.create({
    kind: RpcStreamFrameKind.Status,
    status: Code.Ok,
    metadata: { trailer: new Uint8Array([7]) },
  });
  let closed = false;
  let observedMax;
  let recvBodyBatchCalls = 0;
  const nativeClient = {
    async startStream() {
      return {
        async sendMessage() {
          throw new Error("server-streaming request should not send body messages");
        },
        async finishSend() {},
        async recv() {
          throw new Error("single-frame recv should not be used for native body batches");
        },
        async recvMany() {
          throw new Error("frame batches should not be used for native body batches");
        },
        async recvBodyBatch(max) {
          observedMax = max;
          recvBodyBatchCalls += 1;
          return recvBodyBatchCalls === 1
            ? {
                bodies: [new Uint8Array([1]), new Uint8Array([2, 3])],
                status,
              }
            : null;
        },
        close() {
          closed = true;
        },
      };
    },
  };
  const transport = new RawNodeTransport(nativeClient);

  const responses = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      kind: RpcKind.ServerStreaming,
      body: new Uint8Array(),
    },
    emptyAsyncIterable(),
  );
  const iterator = responses[Symbol.asyncIterator]();
  const first = await iterator.nextBodyBatch();

  assert.equal(first.done, false);
  assert.deepEqual(
    first.value.bodies.map((body) => Array.from(body)),
    [[1], [2, 3]],
  );
  assert.equal(first.value.status.status, Code.Ok);
  assert.deepEqual(first.value.status.metadata.trailer, new Uint8Array([7]));
  assert.equal(closed, false);

  assert.deepEqual(await iterator.nextBodyBatch(), { done: true, value: undefined });
  assert.equal(closed, true);
  assert.equal(observedMax, 32);
  assert.equal(recvBodyBatchCalls, 2);
});

test("Node transport terminal OK reports already-settled local upload errors", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  const uploadError = invalidArgument("local upload failed");
  let sendAttempted;
  const sendAttemptedDone = new Promise((resolve) => {
    sendAttempted = resolve;
  });
  const nativeClient = {
    async startStream() {
      return {
        sendMessage() {
          sendAttempted();
          return Promise.reject(uploadError);
        },
        async finishSend() {},
        async recvMany() {
          await sendAttemptedDone;
          await Promise.resolve();
          await Promise.resolve();
          return [RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok })];
        },
        close() {},
      };
    },
  };
  const transport = new RawNodeTransport(nativeClient);
  const responses = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "BidiHello",
      kind: RpcKind.BidirectionalStreaming,
      body: new Uint8Array(),
    },
    batchedFrameStream([[new Uint8Array([1])]]),
  );

  const iterator = responses[Symbol.asyncIterator]();
  const terminal = await iterator.next();
  assert.equal(terminal.value.status, Code.Ok);
  await assert.rejects(iterator.return(), (error) => error.code === Code.InvalidArgument);
});

test("Node transport terminal error wins over local upload errors", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  const uploadError = invalidArgument("local upload failed");
  let closeCalls = 0;
  let closed = false;
  let sendAttempted;
  const sendAttemptedDone = new Promise((resolve) => {
    sendAttempted = resolve;
  });
  const nativeClient = {
    async startStream() {
      return {
        sendMessage() {
          sendAttempted();
          return Promise.reject(uploadError);
        },
        async finishSend() {},
        async recvMany() {
          await sendAttemptedDone;
          return [
            RpcStreamFrame.create({
              kind: RpcStreamFrameKind.Status,
              status: Code.PermissionDenied,
              message: "remote rejected upload",
            }),
          ];
        },
        close() {
          if (!closed) {
            closed = true;
            closeCalls += 1;
          }
        },
      };
    },
  };
  const transport = new RawNodeTransport(nativeClient);
  const responses = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "BidiHello",
      kind: RpcKind.BidirectionalStreaming,
      body: new Uint8Array(),
    },
    batchedFrameStream([[new Uint8Array([1])]]),
  );
  const result = await responses[Symbol.asyncIterator]().next();

  assert.equal(result.done, false);
  assert.equal(result.value.status, Code.PermissionDenied);
  assert.equal(closeCalls, 1);
});

test("Node transport return cleans up partial streams with a pending native receive", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  let closeCalls = 0;
  let resolvePendingRecv;
  const nativeClient = {
    async startStream() {
      let recvCalls = 0;
      return {
        async finishSend() {},
        recvMany() {
          recvCalls += 1;
          if (recvCalls === 1) {
            return Promise.resolve([RpcStreamFrame.create({ body: new Uint8Array([1]) })]);
          }
          return new Promise((resolve) => {
            resolvePendingRecv = resolve;
          });
        },
        close() {
          closeCalls += 1;
          resolvePendingRecv?.([null]);
        },
      };
    },
  };
  const transport = new RawNodeTransport(nativeClient);
  const responses = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      kind: RpcKind.ServerStreaming,
      body: new Uint8Array(),
    },
    emptyAsyncIterable(),
  );
  const iterator = responses[Symbol.asyncIterator]();

  assert.equal((await iterator.next()).value.body[0], 1);
  const pendingNext = iterator.next();
  await Promise.resolve();

  assert.deepEqual(await iterator.return(), { done: true, value: undefined });
  assert.deepEqual(await pendingNext, { done: true, value: undefined });
  assert.equal(closeCalls, 1);
  assert.deepEqual(await iterator.return(), { done: true, value: undefined });
  assert.equal(closeCalls, 1);
});

test("Node transport native response body batches delay EOF until queued bodies drain", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  const Hello = helloTestType();
  let recvBodyBatchCalls = 0;
  const nativeClient = {
    async startStream() {
      return {
        async finishSend() {},
        async recvBodyBatch() {
          recvBodyBatchCalls += 1;
          return {
            bodies: [marshalMessage(Hello, { value: "one" })],
            status: null,
            eof: true,
          };
        },
        close() {},
      };
    },
  };
  const transport = new RawNodeTransport(nativeClient);

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
  assert.equal(recvBodyBatchCalls, 1);
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

test("unary response body limits are enforced at exact byte boundaries", async () => {
  const Hello = helloTestType();
  const body = Hello.encode({ value: "ok" }).finish();
  const transport = {
    async call() {
      return { status: Code.Ok, body, metadata: {} };
    },
  };

  const response = await unary(
    transport,
    "hello.v1.Greeter",
    "SayHello",
    Hello,
    Hello,
    { value: "Trev" },
    { maxResponseBodySize: body.byteLength },
  );
  assert.equal(response.value, "ok");

  await assert.rejects(
    unary(
      transport,
      "hello.v1.Greeter",
      "SayHello",
      Hello,
      Hello,
      { value: "Trev" },
      { maxResponseBodySize: body.byteLength - 1 },
    ),
    (error) =>
      error instanceof FrameTooLargeError &&
      error.length === body.byteLength &&
      error.max === body.byteLength - 1,
  );
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

test("unary without signal or deadline leaves transport abort signal unset", async () => {
  const Hello = helloTestType();
  let observedOptions;
  let observedRequest;
  const transport = {
    async call(request, options) {
      observedRequest = request;
      observedOptions = options;
      return RpcResponse.create({
        status: Code.Ok,
        body: marshalMessage(Hello, { value: "ok" }),
        metadata: {},
      });
    },
  };

  const response = await unary(transport, "hello.v1.Greeter", "SayHello", Hello, Hello, {
    value: "Trev",
  });

  assert.equal(response.value, "ok");
  assert.equal(observedOptions.signal, undefined);
  assert.equal(observedRequest.timeoutNanos, undefined);
});

test("Node transport skips native cancellation without signal", async () => {
  const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
  let cancellationCalls = 0;
  let callArgumentCount = 0;
  let startStreamArgumentCount = 0;
  let finishSend;
  const finishDone = new Promise((resolve) => {
    finishSend = resolve;
  });
  const nativeClient = {
    createCancellation() {
      cancellationCalls += 1;
      return { cancel() {} };
    },
    async call(...args) {
      callArgumentCount = args.length;
      return { status: Code.Ok, body: new Uint8Array(), metadata: {} };
    },
    async startStream(...args) {
      startStreamArgumentCount = args.length;
      return {
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
  const transport = new RawNodeTransport(nativeClient);

  await transport.call({
    service: "hello.v1.Greeter",
    method: "SayHello",
    body: new Uint8Array(),
    metadata: {},
    version: WireVersion,
  });
  const stream = await transport.streamingCall(
    {
      service: "hello.v1.Greeter",
      method: "LotsOfReplies",
      kind: RpcKind.ServerStreaming,
      body: new Uint8Array(),
      metadata: {},
      version: WireVersion,
    },
    emptyAsyncIterable(),
  );
  await stream[Symbol.asyncIterator]().next();

  assert.equal(cancellationCalls, 0);
  assert.equal(callArgumentCount, 1);
  assert.equal(startStreamArgumentCount, 1);
});

test("client deadlines above the platform timer limit do not fire immediately", async () => {
  const Hello = helloTestType();
  let signal;
  const response = await unary(
    {
      call(_request, options) {
        signal = options.signal;
        return new Promise((resolve) =>
          setTimeout(
            () => resolve({ status: Code.Ok, body: Hello.encode({ value: "done" }).finish() }),
            5,
          ),
        );
      },
    },
    "hello.v1.Greeter",
    "SayHello",
    Hello,
    Hello,
    { value: "Trev" },
    { timeoutMs: 2_147_483_648 },
  );

  assert.equal(response.value, "done");
  assert.equal(signal.aborted, false);
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
              return Promise.reject(invalidArgument("deadline cleanup failed"));
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
  const client = new RawWebTransport({
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
  const client = new RawWebTransport({
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

  const iterator = responses[Symbol.asyncIterator]();
  const terminal = await iterator.next();
  assert.equal(terminal.value.status, Code.Ok);
  await assert.rejects(iterator.return(), (error) => error.code === Code.InvalidArgument);
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
    const client = new RawWebTransport({
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
  const client = new RawWebTransport({
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
  const client = new RawWebTransport({
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

test("Node and WebTransport join delayed FIN with terminal precedence", async (t) => {
  for (const runtime of ["node", "webtransport"]) {
    await t.test(`${runtime} local FIN failure beats terminal OK`, async () => {
      const uploadError = invalidArgument("delayed local FIN failed");
      const scenario = await requestWriterScenario(runtime, Code.Ok);
      const terminal = await scenario.iterator.next();

      assert.equal(terminal.value.status, Code.Ok);
      const returned = scenario.iterator.return();
      await assertPending(returned);
      scenario.writer.reject(uploadError);
      await assert.rejects(returned, (error) => error.code === Code.InvalidArgument);
      assert.equal(scenario.closeCount(), 1);
    });

    await t.test(`${runtime} terminal non-OK beats delayed FIN failure`, async () => {
      const scenario = await requestWriterScenario(runtime, Code.PermissionDenied);
      const terminal = await scenario.iterator.next();

      assert.equal(terminal.value.status, Code.PermissionDenied);
      const returned = scenario.iterator.return();
      await assertPending(returned);
      scenario.writer.reject(cleanupWriterError(runtime));
      assert.deepEqual(await returned, { done: true, value: undefined });
      assert.equal(scenario.closeCount(), 1);
    });
  }
});

test("Node and WebTransport RPC completion waits for delayed FIN failure", async (t) => {
  const Hello = helloTestType();
  for (const runtime of ["node", "webtransport"]) {
    await t.test(runtime, async () => {
      const scenario = await requestWriterScenario(runtime, Code.Ok);
      const stream = await serverStreaming(
        {
          async streamingCall() {
            return scenario.iterator;
          },
        },
        "hello.v1.Greeter",
        "LotsOfReplies",
        Hello,
        Hello,
        { value: "Trev" },
        { streamIdleTimeoutMs: undefined },
      );
      const completed = stream[Symbol.asyncIterator]().next();

      await scenario.writerStarted;
      await flushMicrotasks();
      await assertPending(completed);
      scenario.writer.reject(invalidArgument("delayed FIN failed"));
      await assert.rejects(completed, (error) => error.code === Code.InvalidArgument);
    });
  }
});

test("Node and WebTransport surface remote non-OK over cleanup cancellation", async (t) => {
  const Hello = helloTestType();
  for (const runtime of ["node", "webtransport"]) {
    await t.test(runtime, async () => {
      const scenario = await requestWriterScenario(runtime, Code.PermissionDenied);
      const stream = await serverStreaming(
        {
          async streamingCall() {
            return scenario.iterator;
          },
        },
        "hello.v1.Greeter",
        "LotsOfReplies",
        Hello,
        Hello,
        { value: "Trev" },
        { streamIdleTimeoutMs: undefined },
      );
      const completed = stream[Symbol.asyncIterator]().next();

      await scenario.writerStarted;
      await flushMicrotasks();
      scenario.writer.reject(cleanupWriterError(runtime));
      await assert.rejects(completed, (error) => error.code === Code.PermissionDenied);
    });
  }
});

test("Node and WebTransport classify only cleanup-committed writer failures", async (t) => {
  for (const runtime of ["node", "webtransport"]) {
    await t.test(`${runtime} suppresses cleanup writer failure after terminal OK`, async () => {
      const scenario = await requestWriterScenario(runtime, Code.Ok);
      const terminal = await scenario.iterator.next();
      assert.equal(terminal.value.status, Code.Ok);

      const returned = scenario.iterator.return();
      scenario.writer.reject(cleanupWriterError(runtime));
      assert.deepEqual(await returned, { done: true, value: undefined });
    });

    await t.test(
      `${runtime} reports matching writer failure committed before cleanup`,
      async () => {
        const scenario = await requestWriterScenario(runtime, Code.Ok, { holdResponse: true });
        await scenario.writerStarted;
        scenario.writer.reject(cleanupWriterError(runtime));
        await flushMicrotasks();
        scenario.releaseResponse();

        const terminal = await scenario.iterator.next();
        assert.equal(terminal.value.status, Code.Ok);
        await assert.rejects(scenario.iterator.return());
        assert.deepEqual(await scenario.iterator.return(), { done: true, value: undefined });
      },
    );
  }
});

test("Node and WebTransport preserve writer failure provenance across delayed cleanup", async (t) => {
  for (const runtime of ["node", "webtransport"]) {
    await t.test(runtime, async () => {
      const scenario = await requestWriterScenario(runtime, Code.Ok, {
        holdResponse: true,
        holdRequestReturn: true,
      });
      await scenario.writerStarted;
      scenario.writer.reject(cleanupWriterError(runtime));
      await scenario.requestReturnStarted;
      scenario.releaseResponse();

      const terminal = await scenario.iterator.next();
      assert.equal(terminal.value.status, Code.Ok);
      const returned = scenario.iterator.return();
      await assertPending(returned);
      scenario.releaseRequestReturn();

      await assert.rejects(returned);
    });
  }
});

test("WebTransport terminal OK wins over an earlier remote STOP_SENDING", async () => {
  const scenario = await requestWriterScenario("webtransport", Code.Ok, {
    holdResponse: true,
    holdRequestReturn: true,
  });
  await scenario.writerStarted;
  scenario.writer.reject(new Error("Received STOP_SENDING."));
  await scenario.requestReturnStarted;
  scenario.releaseResponse();

  const terminal = await scenario.iterator.next();
  assert.equal(terminal.value.status, Code.Ok);
  const returned = scenario.iterator.return();
  scenario.releaseRequestReturn();

  assert.deepEqual(await returned, { done: true, value: undefined });
});

test("Node and WebTransport normal exhaustion joins delayed writer failure", async (t) => {
  for (const runtime of ["node", "webtransport"]) {
    await t.test(`${runtime} raw iterator`, async () => {
      const scenario = await requestWriterScenario(runtime, Code.Ok);
      const terminal = await scenario.iterator.next();
      assert.equal(terminal.value.status, Code.Ok);

      const completed = scenario.iterator.next();
      await assertPending(completed);
      scenario.writer.reject(invalidArgument("delayed FIN failed"));

      await assert.rejects(completed, (error) => error.code === Code.InvalidArgument);
    });

    await t.test(`${runtime} for await`, async () => {
      const scenario = await requestWriterScenario(runtime, Code.Ok);
      const terminalSeen = deferredPromise();
      const frames = {
        [Symbol.asyncIterator]() {
          return {
            async next() {
              const result = await scenario.iterator.next();
              if (!result.done && result.value.kind === RpcStreamFrameKind.Status) {
                terminalSeen.resolve();
              }
              return result;
            },
          };
        },
      };
      const completed = (async () => {
        for await (const _frame of frames) {
          // Consume through normal iterator exhaustion.
        }
      })();

      await terminalSeen.promise;
      await assertPending(completed);
      scenario.writer.reject(invalidArgument("delayed FIN failed"));

      await assert.rejects(completed, (error) => error.code === Code.InvalidArgument);
    });
  }
});

test("Node cleanup cancellation uses the platform ECANCELED value", async () => {
  const scenario = await requestWriterScenario("node", Code.Ok);
  const terminal = await scenario.iterator.next();
  assert.equal(terminal.value.status, Code.Ok);

  const completed = scenario.iterator.next();
  await assertPending(completed);
  const error = new Error("finishSend failed: operation cancelled");
  error.nativeCode = -osConstants.errno.ECANCELED;
  scenario.writer.reject(error);

  assert.deepEqual(await completed, { done: true, value: undefined });
});

test("Node terminal status suppresses transport-closed request cleanup", async () => {
  const scenario = await requestWriterScenario("node", Code.Ok);
  const terminal = await scenario.iterator.next();
  assert.equal(terminal.value.status, Code.Ok);

  const completed = scenario.iterator.next();
  await assertPending(completed);
  const error = new Error("finishSend failed: connection closed");
  error.nativeCode = -1001;
  scenario.writer.reject(error);

  assert.deepEqual(await completed, { done: true, value: undefined });
});

test("Node and WebTransport abandonment reports writer failure once without duplicate cleanup", async (t) => {
  for (const runtime of ["node", "webtransport"]) {
    await t.test(runtime, async () => {
      const scenario = await requestWriterScenario(runtime, Code.Ok, { noResponse: true });
      await scenario.writerStarted;
      const returned = scenario.iterator.return();
      scenario.writer.reject(invalidArgument("abandoned upload failed"));

      await assert.rejects(returned, (error) => error.code === Code.InvalidArgument);
      assert.deepEqual(await scenario.iterator.return(), { done: true, value: undefined });
      assert.equal(scenario.closeCount(), 1);
    });
  }
});

test("Node and WebTransport terminal status finishes blocked request iteration", async (t) => {
  const Hello = helloTestType();
  for (const runtime of ["node", "webtransport"]) {
    await t.test(runtime, async () => {
      const scenario = await blockedRequestWriterScenario(runtime);
      const call = await bidirectionalStreaming(
        scenario.transport,
        "hello.v1.Greeter",
        "BidiHello",
        Hello,
        Hello,
        { streamIdleTimeoutMs: undefined },
      );

      assert.equal(await call.recv(), undefined);
      assert.equal(scenario.returnCount(), 0);
      assert.equal(scenario.closeCount(), 1);
    });
  }
});

test("request writer rejection is caught before delayed observation", async () => {
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on("unhandledRejection", onUnhandled);
  try {
    const scenario = await requestWriterScenario("webtransport", Code.Ok);
    const terminal = await scenario.iterator.next();
    assert.equal(terminal.value.status, Code.Ok);

    scenario.writer.reject(invalidArgument("late FIN failed"));
    await flushMicrotasks();
    await assert.rejects(
      scenario.iterator.return(),
      (error) => error.code === Code.InvalidArgument,
    );
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(unhandled, []);
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});

test("explicit response abort preserves the primary error over writer cleanup failure", async () => {
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
              return Promise.reject(invalidArgument("writer cleanup failed"));
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

test("generator emits browser-safe clients and Node server companions", () => {
  const response = generateBindings(greeterRequest());
  const generatedJavaScript = generatedFile(response, "hello/v1/greeter.trevrpc.js");
  const generatedTypes = generatedFile(response, "hello/v1/greeter.trevrpc.d.ts");
  const generatedNodeJavaScript = generatedFile(response, "hello/v1/greeter.node.trevrpc.js");
  const generatedNodeTypes = generatedFile(response, "hello/v1/greeter.node.trevrpc.d.ts");

  assert.equal(response.error, undefined);
  assert.equal(response.file.length, 4);
  assert.match(generatedJavaScript.content, /export class GreeterClient/);
  assert.match(generatedJavaScript.content, /sayHello\(request, options = \{\}\)/);
  assert.match(generatedJavaScript.content, /lotsOfReplies\(request, options = \{\}\)/);
  assert.match(generatedJavaScript.content, /lotsOfGreetings\(options = \{\}\)/);
  assert.match(generatedJavaScript.content, /bidiHello\(options = \{\}\)/);
  assert.doesNotMatch(
    generatedJavaScript.content,
    /node\/generated|NodeServer|registerTypedService/,
  );
  assert.match(generatedNodeJavaScript.content, /registerTypedService/);
  assert.match(generatedNodeJavaScript.content, /registerGreeterServer/);
  assert.match(generatedTypes.content, /export interface HelloRequest/);
  assert.match(generatedTypes.content, /name\?: string;/);
  assert.match(generatedTypes.content, /export interface HelloReply/);
  assert.match(
    generatedTypes.content,
    /sayHello\(request: HelloRequest, options\?: CallOptions\): Promise<HelloReply>;/,
  );
  assert.match(
    generatedTypes.content,
    /lotsOfReplies[\s\S]*Promise<ResponseAsyncIterable<HelloReply>>;/,
  );
  assert.match(
    generatedTypes.content,
    /lotsOfGreetings\(options\?: CallOptions\): Promise<ClientStreamingCall<HelloRequest, HelloReply>>;/,
  );
  assert.match(
    generatedTypes.content,
    /bidiHello\(options\?: CallOptions\): Promise<BidirectionalStreamingCall<HelloRequest, HelloReply>>;/,
  );
  assert.match(generatedNodeTypes.content, /export interface GreeterHandlers/);
  assert.match(generatedNodeTypes.content, /UnaryServerResponse<HelloReply>/);
  assert.match(generatedNodeTypes.content, /StreamingServerResponse<HelloReply>/);
});

test("generated JavaScript clients call the runtime", async () => {
  const runtimeImport = new URL("../src/index.js", import.meta.url).href;
  const response = generateBindings(
    greeterRequest(`runtime_import=${runtimeImport},runtime_type_import=${runtimeImport}`),
  );
  assert.equal(response.error, undefined);

  const directory = await mkdtemp(join(tmpdir(), "trevrpc-js-generated-"));
  const generatedPath = join(directory, "greeter.trevrpc.js");
  await writeFile(join(directory, "package.json"), '{"type":"module"}\n');
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

test("generated client-streaming closeAndRecv waits for FIN and preserves status precedence", async () => {
  const runtimeImport = new URL("../src/index.js", import.meta.url).href;
  const response = generateBindings(
    greeterRequest(`runtime_import=${runtimeImport},runtime_type_import=${runtimeImport}`),
  );
  assert.equal(response.error, undefined);

  const directory = await mkdtemp(join(tmpdir(), "trevrpc-js-generated-stream-"));
  const generatedPath = join(directory, "greeter.trevrpc.js");
  await writeFile(join(directory, "package.json"), '{"type":"module"}\n');
  await writeFile(generatedPath, generatedFile(response, "hello/v1/greeter.trevrpc.js").content);
  const generated = await import(pathToFileURL(generatedPath).href);
  const replyBody = generated.root
    .lookupType("hello.v1.HelloReply")
    .encode({ message: "done" })
    .finish();

  for (const status of [Code.Ok, Code.PermissionDenied]) {
    const writer = deferredPromise();
    const writerStarted = deferredPromise();
    const stream = fakeBidirectionalStream({
      beforeRead: () => writerStarted.promise,
      closePromise: writer.promise,
      onClose: writerStarted.resolve,
      readableChunks: [
        concatBytes([
          encodeMessageStreamFrame(replyBody),
          encodeFrame(
            RpcStreamFrame,
            RpcStreamFrame.create({
              kind: RpcStreamFrameKind.Status,
              status,
              message: status === Code.Ok ? "" : "remote rejected upload",
            }),
          ),
        ]),
      ],
    });
    const transport = new RawWebTransport({
      ready: Promise.resolve(),
      createBidirectionalStream() {
        return Promise.resolve(stream);
      },
    });
    const client = new generated.GreeterClient(transport);
    const call = await client.lotsOfGreetings();
    const result = call.closeAndRecv();

    await writerStarted.promise;
    await assertPending(result);
    writer.reject(invalidArgument("delayed generated FIN failed"));
    if (status === Code.Ok) {
      await assert.rejects(result, (error) => error.code === Code.InvalidArgument);
    } else {
      await assert.rejects(
        result,
        (error) =>
          error.code === Code.PermissionDenied && error.statusMessage === "remote rejected upload",
      );
    }
  }
});

test("service clients merge static and override metadata", async () => {
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
  const service = {
    fullName: "hello.v1.Greeter",
    methods: {
      sayHello: {
        name: "SayHello",
        inputType: "hello.v1.Hello",
        outputType: "hello.v1.Hello",
        kind: "unary",
      },
    },
  };
  let observedRequest;
  const transport = {
    async call(request) {
      observedRequest = request;
      return RpcResponse.create({
        status: Code.Ok,
        body: marshalMessage(Hello, { value: "ok" }),
        metadata: {},
      });
    },
  };
  const client = createServiceClient(transport, service, root, {
    metadata: { Authorization: "base", trace: new Uint8Array([1]) },
  });

  const response = await client.sayHello(
    { value: "Trev" },
    {
      metadata: new Map([
        ["authorization", "override"],
        ["request-id", "abc"],
      ]),
    },
  );

  assert.equal(response.value, "ok");
  assert.deepEqual(observedRequest.metadata.authorization, new TextEncoder().encode("override"));
  assert.deepEqual(observedRequest.metadata.trace, new Uint8Array([1]));
  assert.deepEqual(observedRequest.metadata["request-id"], new TextEncoder().encode("abc"));
  await assert.rejects(
    async () => {
      await client.sayHello({ value: "Trev" }, { metadata: { "trevrpc-reserved": "no" } });
    },
    (error) => error.code === Code.InvalidArgument,
  );
});

test("generated TypeScript declarations match protobuf.js long and map values", () => {
  const response = generateBindings(longAndMapRequest());
  const generatedTypes = generatedFile(response, "types/v1/values.trevrpc.d.ts");

  assert.equal(response.error, undefined);
  assert.match(generatedTypes.content, /import type \{ Long, Root \} from "protobufjs";/);
  assert.match(generatedTypes.content, /count\?: number \| string \| Long;/);
  assert.match(generatedTypes.content, /labels\?: Record<string, string>;/);
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

async function* emptyAsyncIterable() {}

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

function longAndMapRequest() {
  return {
    fileToGenerate: ["types/v1/values.proto"],
    protoFile: [
      {
        name: "types/v1/values.proto",
        package: "types.v1",
        messageType: [
          {
            name: "Values",
            field: [
              { name: "count", number: 1, label: 1, type: 3, jsonName: "count" },
              {
                name: "labels",
                number: 2,
                label: 3,
                type: 11,
                typeName: ".types.v1.Values.LabelsEntry",
                jsonName: "labels",
              },
            ],
            nestedType: [
              {
                name: "LabelsEntry",
                options: { mapEntry: true },
                field: [
                  { name: "key", number: 1, label: 1, type: 5, jsonName: "key" },
                  { name: "value", number: 2, label: 1, type: 9, jsonName: "value" },
                ],
              },
            ],
          },
        ],
        service: [
          {
            name: "ValuesService",
            method: [
              {
                name: "GetValues",
                inputType: ".types.v1.Values",
                outputType: ".types.v1.Values",
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

function concatBytes(parts) {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const output = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
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

async function requestWriterScenario(
  runtime,
  status,
  { holdResponse = false, holdRequestReturn = false, noResponse = false } = {},
) {
  const writer = deferredPromise();
  const writerStarted = deferredPromise();
  const response = deferredPromise();
  const requestReturn = deferredPromise();
  const requestReturnStarted = deferredPromise();
  const requestBody = holdRequestReturn
    ? {
        [Symbol.asyncIterator]() {
          return {
            next() {
              return Promise.resolve({ done: true, value: undefined });
            },
            async return() {
              requestReturnStarted.resolve();
              await requestReturn.promise;
              return { done: true, value: undefined };
            },
          };
        },
      }
    : emptyAsyncIterable();
  if (!holdResponse) {
    response.resolve();
  }

  if (runtime === "node") {
    const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
    let closes = 0;
    const transport = new RawNodeTransport({
      async startStream() {
        let recvCalls = 0;
        return {
          async finishSend() {
            writerStarted.resolve();
            await writer.promise;
          },
          async recvMany() {
            await writerStarted.promise;
            await response.promise;
            if (noResponse) {
              return await new Promise(() => {});
            }
            recvCalls += 1;
            return recvCalls === 1
              ? [RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status })]
              : [null];
          },
          close() {
            closes += 1;
          },
        };
      },
    });
    const frames = await transport.streamingCall(
      { service: "svc", method: "stream", kind: RpcKind.BidirectionalStreaming },
      requestBody,
    );
    return {
      iterator: frames[Symbol.asyncIterator](),
      writer,
      writerStarted: writerStarted.promise,
      requestReturnStarted: requestReturnStarted.promise,
      releaseRequestReturn: requestReturn.resolve,
      releaseResponse: response.resolve,
      closeCount: () => closes,
    };
  }

  let aborts = 0;
  let cancels = 0;
  const statusFrame = encodeFrame(
    RpcStreamFrame,
    RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status }),
  );
  const stream = fakeBidirectionalStream({
    beforeRead: async () => {
      await writerStarted.promise;
      await response.promise;
    },
    closePromise: writer.promise,
    onAbort() {
      aborts += 1;
    },
    onCancel() {
      cancels += 1;
    },
    onClose() {
      writerStarted.resolve();
    },
    readableChunks: noResponse ? [] : [statusFrame],
  });
  const transport = new RawWebTransport({
    ready: Promise.resolve(),
    createBidirectionalStream() {
      return Promise.resolve(stream);
    },
  });
  const frames = await transport.streamingCall(
    RpcRequest.create({
      service: "svc",
      method: "stream",
      kind: RpcKind.BidirectionalStreaming,
      version: WireVersion,
    }),
    requestBody,
  );
  return {
    iterator: frames[Symbol.asyncIterator](),
    writer,
    writerStarted: writerStarted.promise,
    requestReturnStarted: requestReturnStarted.promise,
    releaseRequestReturn: requestReturn.resolve,
    releaseResponse: response.resolve,
    closeCount: () => Math.max(aborts, cancels),
  };
}

async function blockedRequestWriterScenario(runtime) {
  const iterationStarted = deferredPromise();
  let returns = 0;
  let closes = 0;
  let rawTransport;

  if (runtime === "node") {
    const { RawNodeTransport } = await import("@trevrpc/trevrpc-js/node/advanced");
    rawTransport = new RawNodeTransport({
      async startStream() {
        let recvCalls = 0;
        return {
          async finishSend() {},
          async recvMany() {
            await iterationStarted.promise;
            recvCalls += 1;
            return recvCalls === 1
              ? [RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok })]
              : [null];
          },
          close() {
            closes += 1;
          },
        };
      },
    });
  } else {
    const stream = fakeBidirectionalStream({
      beforeRead: () => iterationStarted.promise,
      onAbort() {
        closes += 1;
      },
      readableChunks: [
        encodeFrame(
          RpcStreamFrame,
          RpcStreamFrame.create({ kind: RpcStreamFrameKind.Status, status: Code.Ok }),
        ),
      ],
    });
    rawTransport = new RawWebTransport({
      ready: Promise.resolve(),
      createBidirectionalStream() {
        return Promise.resolve(stream);
      },
    });
  }

  return {
    transport: {
      async streamingCall(request, requestBody, options) {
        const iterator = requestBody[Symbol.asyncIterator]();
        const wrappedRequestBody = {
          [Symbol.asyncIterator]() {
            return this;
          },
          next() {
            iterationStarted.resolve();
            return iterator.next();
          },
          nextBatch(max) {
            iterationStarted.resolve();
            return typeof iterator.nextBatch === "function"
              ? iterator.nextBatch(max)
              : iterator.next();
          },
          async return() {
            returns += 1;
            return typeof iterator.return === "function"
              ? await iterator.return()
              : { done: true, value: undefined };
          },
        };
        return await rawTransport.streamingCall(request, wrappedRequestBody, options);
      },
    },
    returnCount: () => returns,
    closeCount: () => closes,
  };
}

function cleanupWriterError(runtime) {
  const error = new Error(
    runtime === "node" ? "native stream closed" : "stream canceled with error code 0",
  );
  if (runtime === "node") {
    error.nativeCode = -1001;
  }
  return error;
}

function deferredPromise() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function assertPending(promise) {
  let settled = false;
  promise.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await flushMicrotasks();
  assert.equal(settled, false);
}

async function flushMicrotasks() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function fakeBidirectionalStream({
  beforeRead = null,
  closeError = null,
  closePromise = null,
  onAbort = () => {},
  onCancel = () => {},
  onClose = () => {},
  onWrite = () => {},
  readableChunks = [],
} = {}) {
  const chunks = [...readableChunks];
  return {
    writable: {
      getWriter() {
        return {
          write(chunk) {
            onWrite(chunk);
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
          async read() {
            await beforeRead?.();
            const value = chunks.shift();
            return value == null ? { done: true, value: undefined } : { done: false, value };
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
