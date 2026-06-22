import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

import {
  Code,
  RpcKind,
  RpcStreamFrameKind,
  createRoot,
  createServiceClient,
  marshalMessage,
  unmarshalMessage,
} from "../src/index.js";
import { NodeTransport } from "../src/node.js";

const StreamMessageCount = 16;
const BenchmarkRequest = Object.freeze({ name: "TrevRPC benchmark" });
const ServerStartupTimeoutMs = 10_000;
const ServerShutdownTimeoutMs = 5_000;
const RecvManyBatchSize = 32;

const root = createRoot({
  nested: {
    hello: {
      nested: {
        v1: {
          nested: {
            HelloRequest: {
              fields: {
                name: { type: "string", id: 1 },
              },
            },
            HelloReply: {
              fields: {
                message: { type: "string", id: 1 },
              },
            },
          },
        },
      },
    },
  },
});

const HelloRequest = root.lookupType("hello.v1.HelloRequest");
const HelloReply = root.lookupType("hello.v1.HelloReply");
const GreeterService = Object.freeze({
  name: "Greeter",
  fullName: "hello.v1.Greeter",
  exportName: "GreeterService",
  methods: {
    sayHello: {
      name: "SayHello",
      kind: "unary",
      inputType: "hello.v1.HelloRequest",
      outputType: "hello.v1.HelloReply",
    },
    lotsOfReplies: {
      name: "LotsOfReplies",
      kind: "serverStreaming",
      inputType: "hello.v1.HelloRequest",
      outputType: "hello.v1.HelloReply",
    },
    lotsOfGreetings: {
      name: "LotsOfGreetings",
      kind: "clientStreaming",
      inputType: "hello.v1.HelloRequest",
      outputType: "hello.v1.HelloReply",
    },
    bidiHello: {
      name: "BidiHello",
      kind: "bidirectionalStreaming",
      inputType: "hello.v1.HelloRequest",
      outputType: "hello.v1.HelloReply",
    },
  },
});

const iterations = positiveInteger(process.argv[2] ?? process.env.JS_PROFILE_ITERATIONS ?? "1000");
const warmupIterations = nonNegativeInteger(process.env.JS_PROFILE_WARMUP_ITERATIONS ?? "50");
const serverBinary = process.argv[3] ?? process.env.TREVRPC_BENCH_SERVER;
if (serverBinary == null || serverBinary === "") {
  throw new Error("usage: node bench/rpc_profile_native.js <iterations> <server-binary>");
}

const service = GreeterService.fullName;
const requestBody = marshalMessage(HelloRequest, BenchmarkRequest);
const unaryReplyBody = marshalMessage(HelloReply, { message: BenchmarkRequest.name });
const streamReplyBody = marshalMessage(HelloReply, { message: "server stream" });
const emptyAsyncIterable = Object.freeze({
  async *[Symbol.asyncIterator]() {},
});

let byteSink = 0;
let countSink = 0;

const server = spawn(serverBinary, ["--serve"], {
  stdio: ["pipe", "pipe", "pipe"],
});
server.stderr.on("data", (chunk) => process.stderr.write(chunk));

let transport;
try {
  const port = await waitForServerPort(server);
  transport = await NodeTransport.connect({
    host: "127.0.0.1",
    port,
    maxStreamsPerSession: 128,
    idleTimeoutMs: 600_000,
  });
  const nativeClient = transport.nativeClient;
  const client = createServiceClient(transport, GreeterService, root);
  const noIdleTimeoutClient = createServiceClient(transport, GreeterService, root, {
    streamIdleTimeoutMs: undefined,
  });

  await warmClient(client);
  await warmClient(noIdleTimeoutClient);

  console.log("# JS native RPC profile");
  console.log(`iterations: ${iterations}`);
  console.log(`warmupIterations: ${warmupIterations}`);
  console.log("");

  await runProfileCase("js.await_resolved_1", iterations, async () => {
    await Promise.resolve();
  });
  await runProfileCase("js.await_resolved_16", iterations, async () => {
    for (let i = 0; i < StreamMessageCount; i++) {
      await Promise.resolve();
    }
  });
  await runProfileCase("protobuf.marshal_request", iterations, () => {
    consumeBody(marshalMessage(HelloRequest, BenchmarkRequest));
  });
  await runProfileCase("protobuf.unmarshal_unary_reply", iterations, () => {
    consumeReply(unmarshalMessage(HelloReply, unaryReplyBody), BenchmarkRequest.name);
  });
  await runProfileCase("protobuf.marshal_16_requests", iterations, () => {
    for (let i = 0; i < StreamMessageCount; i++) {
      consumeBody(marshalMessage(HelloRequest, BenchmarkRequest));
    }
  });
  await runProfileCase("protobuf.unmarshal_16_replies", iterations, () => {
    for (let i = 0; i < StreamMessageCount; i++) {
      consumeReply(unmarshalMessage(HelloReply, streamReplyBody), "server stream");
    }
  });
  await runProfileCase("node.native_unary", iterations, async () => {
    const response = await nativeClient.call(service, "SayHello", requestBody);
    validateUnaryResponse(response);
  });
  await runProfileCase("node.transport_unary", iterations, async () => {
    const response = await transport.call({ service, method: "SayHello", body: requestBody });
    validateUnaryResponse(response);
  });
  await runProfileCase("client.unary_full", iterations, () => unary(client));
  await runProfileCase("node.native_server_stream", iterations, async () => {
    const stream = await nativeClient.startStream(
      service,
      "LotsOfReplies",
      RpcKind.ServerStreaming,
      requestBody,
    );
    await drainNativeStream(stream, StreamMessageCount);
  });
  await runProfileCase("node.transport_server_stream", iterations, async () => {
    const frames = await transport.streamingCall(
      {
        service,
        method: "LotsOfReplies",
        kind: RpcKind.ServerStreaming,
        body: requestBody,
      },
      emptyAsyncIterable,
    );
    await drainFrameStream(frames, StreamMessageCount);
  });
  await runProfileCase("node.transport_server_stream_decode", iterations, async () => {
    const frames = await transport.streamingCall(
      {
        service,
        method: "LotsOfReplies",
        kind: RpcKind.ServerStreaming,
        body: requestBody,
      },
      emptyAsyncIterable,
    );
    await drainDecodedFrameStream(frames, StreamMessageCount, "server stream");
  });
  await runProfileCase("client.server_stream_full", iterations, () => serverStreaming(client));
  await runProfileCase("client.server_stream_no_idle_timeout", iterations, () =>
    serverStreaming(noIdleTimeoutClient),
  );
  await runProfileCase("node.native_client_stream", iterations, async () => {
    const stream = await nativeClient.startStream(
      service,
      "LotsOfGreetings",
      RpcKind.ClientStreaming,
      new Uint8Array(0),
    );
    await sendNativeMessages(stream);
    await stream.finishSend();
    await drainNativeStream(stream, 1);
  });
  await runProfileCase("node.native_client_stream_batch", iterations, async () => {
    const stream = await nativeClient.startStream(
      service,
      "LotsOfGreetings",
      RpcKind.ClientStreaming,
      new Uint8Array(0),
    );
    await sendNativeMessagesBatch(stream);
    await stream.finishSend();
    await drainNativeStream(stream, 1);
  });
  await runProfileCase("node.transport_client_stream", iterations, async () => {
    const frames = await transport.streamingCall(
      {
        service,
        method: "LotsOfGreetings",
        kind: RpcKind.ClientStreaming,
        body: new Uint8Array(0),
      },
      requestBodies(),
    );
    await drainFrameStream(frames, 1);
  });
  await runProfileCase("node.transport_client_stream_decode", iterations, async () => {
    const frames = await transport.streamingCall(
      {
        service,
        method: "LotsOfGreetings",
        kind: RpcKind.ClientStreaming,
        body: new Uint8Array(0),
      },
      requestBodies(),
    );
    await drainDecodedFrameStream(frames, 1, `streamed ${StreamMessageCount} greetings`);
  });
  await runProfileCase("client.client_stream_full", iterations, () => clientStreaming(client));
  await runProfileCase("client.client_stream_no_idle_timeout", iterations, () =>
    clientStreaming(noIdleTimeoutClient),
  );
  await runProfileCase("node.native_bidi_stream", iterations, async () => {
    const stream = await nativeClient.startStream(
      service,
      "BidiHello",
      RpcKind.BidirectionalStreaming,
      new Uint8Array(0),
    );
    await sendNativeMessages(stream);
    await stream.finishSend();
    await drainNativeStream(stream, StreamMessageCount);
  });
  await runProfileCase("node.native_bidi_stream_batch", iterations, async () => {
    const stream = await nativeClient.startStream(
      service,
      "BidiHello",
      RpcKind.BidirectionalStreaming,
      new Uint8Array(0),
    );
    await sendNativeMessagesBatch(stream);
    await stream.finishSend();
    await drainNativeStream(stream, StreamMessageCount);
  });
  await runProfileCase("node.transport_bidi_stream", iterations, async () => {
    const frames = await transport.streamingCall(
      {
        service,
        method: "BidiHello",
        kind: RpcKind.BidirectionalStreaming,
        body: new Uint8Array(0),
      },
      requestBodies(),
    );
    await drainFrameStream(frames, StreamMessageCount);
  });
  await runProfileCase("node.transport_bidi_stream_decode", iterations, async () => {
    const frames = await transport.streamingCall(
      {
        service,
        method: "BidiHello",
        kind: RpcKind.BidirectionalStreaming,
        body: new Uint8Array(0),
      },
      requestBodies(),
    );
    await drainDecodedFrameStream(frames, StreamMessageCount, BenchmarkRequest.name);
  });
  await runProfileCase("client.bidi_stream_full", iterations, () => bidiStreaming(client));
  await runProfileCase("client.bidi_stream_no_idle_timeout", iterations, () =>
    bidiStreaming(noIdleTimeoutClient),
  );

  console.log("");
  console.log(`sink: bytes=${byteSink} count=${countSink}`);
} finally {
  transport?.close();
  await stopServer(server);
}

async function runProfileCase(name, count, fn) {
  for (let i = 0; i < warmupIterations; i++) {
    await fn();
  }

  const start = process.hrtime.bigint();
  for (let i = 0; i < count; i++) {
    await fn();
  }
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const latencyUs = (elapsedSeconds * 1_000_000) / count;
  const opsPerSecond = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
  console.log(
    `${name}: ${latencyUs.toFixed(3)} us/op (${opsPerSecond.toFixed(0)} ops/s, ${count} iterations in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function warmClient(client) {
  await unary(client);
  await serverStreaming(client);
  await clientStreaming(client);
  await bidiStreaming(client);
}

async function unary(client) {
  const reply = await client.sayHello(BenchmarkRequest);
  consumeReply(reply, BenchmarkRequest.name);
}

async function serverStreaming(client) {
  const replies = await client.lotsOfReplies(BenchmarkRequest);
  let count = 0;
  for await (const reply of replies) {
    consumeReply(reply, "server stream");
    count++;
  }
  assertCount(count, StreamMessageCount, "server stream replies");
}

async function clientStreaming(client) {
  const call = await client.lotsOfGreetings();
  for (let i = 0; i < StreamMessageCount; i++) {
    await call.send(BenchmarkRequest);
  }
  const reply = await call.closeAndRecv();
  consumeReply(reply, `streamed ${StreamMessageCount} greetings`);
}

async function bidiStreaming(client) {
  const call = await client.bidiHello();
  for (let i = 0; i < StreamMessageCount; i++) {
    await call.send(BenchmarkRequest);
  }
  await call.closeSend();

  let count = 0;
  for (;;) {
    const reply = await call.recv();
    if (reply === undefined) {
      break;
    }
    consumeReply(reply, BenchmarkRequest.name);
    count++;
  }
  assertCount(count, StreamMessageCount, "bidi replies");
}

async function sendNativeMessages(stream) {
  for (let i = 0; i < StreamMessageCount; i++) {
    await stream.sendMessage(requestBody);
  }
}

async function sendNativeMessagesBatch(stream) {
  if (typeof stream.sendMessages === "function") {
    await stream.sendMessages(Array.from({ length: StreamMessageCount }, () => requestBody));
    return;
  }

  const sends = [];
  for (let i = 0; i < StreamMessageCount; i++) {
    sends.push(stream.sendMessage(requestBody));
  }
  await Promise.all(sends);
}

async function drainNativeStream(stream, expectedMessages) {
  let messages = 0;
  try {
    for (;;) {
      const frames = await stream.recvMany(RecvManyBatchSize);
      for (const frame of frames) {
        if (frame == null) {
          assertCount(messages, expectedMessages, "native stream messages");
          return;
        }
        if (frame.kind === RpcStreamFrameKind.Status) {
          validateStatusFrame(frame);
          assertCount(messages, expectedMessages, "native stream messages");
          return;
        }
        validateMessageFrame(frame);
        messages++;
      }
    }
  } finally {
    stream.close();
  }
}

async function drainFrameStream(frames, expectedMessages) {
  let messages = 0;
  for await (const frame of frames) {
    if (frame.kind === RpcStreamFrameKind.Status) {
      validateStatusFrame(frame);
      assertCount(messages, expectedMessages, "transport stream messages");
      return;
    }
    validateMessageFrame(frame);
    messages++;
  }
  throw new Error("stream ended before terminal status");
}

async function drainDecodedFrameStream(frames, expectedMessages, expectedMessage) {
  let messages = 0;
  for await (const frame of frames) {
    if (frame.kind === RpcStreamFrameKind.Status) {
      validateStatusFrame(frame);
      assertCount(messages, expectedMessages, "transport stream messages");
      return;
    }
    validateDecodedMessageFrame(frame, expectedMessage);
    messages++;
  }
  throw new Error("stream ended before terminal status");
}

async function* requestBodies() {
  for (let i = 0; i < StreamMessageCount; i++) {
    yield requestBody;
  }
}

function validateUnaryResponse(response) {
  if (response == null || (response.status ?? Code.Ok) !== Code.Ok) {
    throw new Error(`unexpected unary response: ${JSON.stringify(response)}`);
  }
  consumeBody(response.body ?? new Uint8Array(0));
}

function validateMessageFrame(frame) {
  if (frame.kind !== RpcStreamFrameKind.Message) {
    throw new Error(`unexpected stream frame: ${JSON.stringify(frame)}`);
  }
  consumeBody(frame.body ?? new Uint8Array(0));
}

function validateDecodedMessageFrame(frame, expectedMessage) {
  if (frame.kind !== RpcStreamFrameKind.Message) {
    throw new Error(`unexpected stream frame: ${JSON.stringify(frame)}`);
  }
  consumeReply(unmarshalMessage(HelloReply, frame.body ?? new Uint8Array(0)), expectedMessage);
}

function validateStatusFrame(frame) {
  if ((frame.status ?? Code.Ok) !== Code.Ok) {
    throw new Error(`unexpected stream status: ${JSON.stringify(frame)}`);
  }
}

function consumeReply(reply, expectedMessage) {
  if (reply?.message !== expectedMessage) {
    throw new Error(`unexpected reply: ${JSON.stringify(reply)}`);
  }
  byteSink += reply.message.length;
  countSink++;
}

function consumeBody(body) {
  byteSink += body.byteLength;
  countSink++;
}

function assertCount(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(`expected ${expected} ${label}, got ${actual}`);
  }
  countSink += actual;
}

function positiveInteger(value) {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`expected a positive integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}

function nonNegativeInteger(value) {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(`expected a non-negative integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}

function waitForServerPort(serverProcess) {
  return new Promise((resolve, reject) => {
    let output = "";
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for benchmark server port"));
    }, ServerStartupTimeoutMs);
    const cleanup = () => {
      clearTimeout(timeout);
      serverProcess.stdout.off("data", onData);
      serverProcess.off("exit", onExit);
      serverProcess.off("error", onError);
    };
    const onData = (chunk) => {
      output += chunk.toString("utf8");
      const match = /^PORT ([0-9]+)$/m.exec(output);
      if (match == null) {
        return;
      }
      cleanup();
      resolve(Number(match[1]));
    };
    const onExit = (code, signal) => {
      cleanup();
      reject(
        new Error(`benchmark server exited before reporting a port: code=${code} signal=${signal}`),
      );
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };

    serverProcess.stdout.on("data", onData);
    serverProcess.once("exit", onExit);
    serverProcess.once("error", onError);
  });
}

async function stopServer(serverProcess) {
  if (serverProcess.exitCode != null || serverProcess.signalCode != null) {
    return;
  }

  serverProcess.stdin.end();
  const exited = new Promise((resolve) => serverProcess.once("exit", resolve));
  const timedOut = delay(ServerShutdownTimeoutMs, "timeout");
  if ((await Promise.race([exited, timedOut])) === "timeout") {
    serverProcess.kill("SIGTERM");
    await Promise.race([exited, delay(1000)]);
  }
}
