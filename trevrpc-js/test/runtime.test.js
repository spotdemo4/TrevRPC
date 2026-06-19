import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { generate as generateBindings } from "../src/generator.js";
import {
  Code,
  RpcKind,
  RpcRequest,
  RpcResponse,
  RpcStreamFrame,
  RpcStreamFrameKind,
  WebTransportClient,
  WireVersion,
  bidirectionalStreaming,
  createRoot,
  decodeFrame,
  encodeFrame,
  invalidArgument,
  marshalMessage,
  normalizeMetadata,
  serverStreaming,
  unary,
} from "../src/index.js";

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

test("terminal streaming status cancels pending request iterable", async () => {
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
  let returned = false;
  let writerDone;
  const requests = {
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
    requests,
  );
  const result = await stream[Symbol.asyncIterator]().next();
  await writerDone;

  assert.equal(result.done, true);
  assert.equal(returned, true);
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

  const responses = await bidirectionalStreaming(
    client,
    "hello.v1.Greeter",
    "BidiHello",
    Hello,
    Hello,
    requestBody,
  );

  await assert.rejects(
    responses[Symbol.asyncIterator]().next(),
    (error) => error.code === Code.InvalidArgument,
  );
});

test("WebTransport terminal error wins over local upload error", async () => {
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

  const responses = await bidirectionalStreaming(
    client,
    "hello.v1.Greeter",
    "BidiHello",
    Hello,
    Hello,
    requestBody,
  );

  await assert.rejects(
    responses[Symbol.asyncIterator]().next(),
    (error) => error.code === Code.PermissionDenied,
  );
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
  assert.match(generatedJavaScript.content, /lotsOfGreetings\(request, options = \{\}\)/);
  assert.match(generatedJavaScript.content, /bidiHello\(request, options = \{\}\)/);
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
    /lotsOfGreetings\(requests: AsyncIterable<HelloRequest>, options\?: CallOptions\): Promise<HelloReply>;/,
  );
  assert.match(
    generatedTypes.content,
    /bidiHello\(requests: AsyncIterable<HelloRequest>, options\?: CallOptions\): Promise<AsyncIterable<HelloReply>>;/,
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

function fakeBidirectionalStream({
  onAbort = () => {},
  onCancel = () => {},
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
            return Promise.resolve();
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
