import { RawNodeTransport } from "trevrpc-js/node/advanced";

import { createRoot, createServiceClient } from "../src/index.js";
import { NodeServer } from "../src/node-index.js";

const LatencyStreamMessageCount = 1;
const RequestName = "TrevRPC benchmark";
const PayloadValues = loadPayloadProfile();
const Metadata = loadMetadataProfile();
const ResponseMetadata = Object.keys(Metadata).length === 0 ? {} : { "benchmark-response": "ok" };
const IdleTimeoutMs = 600_000;
const SendManyBatchSize = 16;

const root = createRoot({
  nested: {
    example: {
      nested: {
        greeter: {
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

const HelloRequest = root.lookupType("example.greeter.HelloRequest");
const HelloReply = root.lookupType("example.greeter.HelloReply");
const GreeterService = Object.freeze({
  name: "Greeter",
  fullName: "example.greeter.Greeter",
  exportName: "GreeterService",
  methods: {
    sayHello: {
      name: "SayHello",
      kind: "unary",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
    lotsOfReplies: {
      name: "LotsOfReplies",
      kind: "serverStreaming",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
    lotsOfGreetings: {
      name: "LotsOfGreetings",
      kind: "clientStreaming",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
    bidiHello: {
      name: "BidiHello",
      kind: "bidirectionalStreaming",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
  },
});

const mode = process.argv[2];
if (mode === "client") {
  const host = process.argv[3] ?? "127.0.0.1";
  const port = positiveInteger(process.argv[4]);
  const iterations = positiveInteger(process.argv[5] ?? "1000");
  await runClient(host, port, iterations);
} else if (mode === "server") {
  const certFile = requiredArg(3, "certFile");
  const keyFile = requiredArg(4, "keyFile");
  await runServer(certFile, keyFile);
} else {
  throw new Error(
    "usage: node bench/rpc_split_native.js client <host> <port> <iterations> | server <certFile> <keyFile>",
  );
}

async function runClient(host, port, iterations) {
  const transport = await RawNodeTransport.connect({
    host,
    port,
    skipCertificateValidation: true,
    maxStreamsPerSession: 128,
    idleTimeoutMs: IdleTimeoutMs,
  });
  try {
    const client = createServiceClient(transport, GreeterService, root, callOptions());
    await warmClient(client);
    await runLatencyCase("unary_latency", iterations, () => unary(client));
    await runLatencyCase("server_stream_latency", iterations, () =>
      serverStreaming(client, LatencyStreamMessageCount),
    );
    await runMessageThroughputCase("server_stream_throughput", iterations, () =>
      serverStreaming(client, iterations),
    );
    await runLatencyCase("client_stream_latency", iterations, () =>
      clientStreaming(client, LatencyStreamMessageCount),
    );
    await runMessageThroughputCase("client_stream_throughput", iterations, () =>
      clientStreaming(client, iterations),
    );
    await runLatencyCase("bidi_stream_latency", iterations, () =>
      bidiStreaming(client, LatencyStreamMessageCount),
    );
    await runMessageThroughputCase("bidi_stream_throughput", iterations, () =>
      bidiStreaming(client, iterations),
    );
  } finally {
    transport.close();
  }
}

async function runServer(certFile, keyFile) {
  const server = await NodeServer.listen({
    host: "127.0.0.1",
    port: 0,
    certFile,
    keyFile,
    path: "/trevrpc",
    origin: process.env.TREVRPC_WEBTRANSPORT_ORIGIN,
    maxSessionsPerConnection: 16,
    maxStreamsPerSession: 65_535,
    maxStreamMessages: -1,
    idleTimeoutMs: IdleTimeoutMs,
  });
  server.registerService(GreeterService, {
    sayHello: (call) => {
      validateMetadata(call.request.metadata);
      return {
        body: encodeReply(decodeRequest(call.request.body).name),
        metadata: ResponseMetadata,
      };
    },
    lotsOfReplies: serverStreamReplies,
    lotsOfGreetings: clientStreamReply,
    bidiHello: bidiReplies,
  });

  const serveDone = server.serve();
  console.log(`PORT ${server.port}`);
  console.log(`CERT ${certFile}`);
  const stop = async () => {
    server.close();
    await serveDone;
  };
  process.once("SIGTERM", () => void stop());
  process.once("SIGINT", () => void stop());
  await serveDone;
}

async function warmClient(client) {
  await unary(client);
  await serverStreaming(client, LatencyStreamMessageCount);
  await clientStreaming(client, LatencyStreamMessageCount);
  await bidiStreaming(client, LatencyStreamMessageCount);
}

async function runLatencyCase(name, iterations, fn) {
  const start = process.hrtime.bigint();
  for (let i = 0; i < iterations; i++) {
    await fn();
  }
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const latencyUs = (elapsedSeconds * 1_000_000) / iterations;
  console.log(
    `${name}: ${latencyUs.toFixed(3)} us/op (${iterations} iterations in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function runMessageThroughputCase(name, iterations, fn) {
  const start = process.hrtime.bigint();
  await fn();
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const messagesPerSecond = elapsedSeconds > 0 ? iterations / elapsedSeconds : 0;
  console.log(
    `${name}: ${messagesPerSecond.toFixed(0)} messages/s (${iterations} messages in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function unary(client) {
  const expected = payloadForIndex(0);
  const reply = await client.sayHello({ name: expected });
  if (reply.message !== expected) {
    throw new Error(`unexpected unary response: ${JSON.stringify(reply)}`);
  }
}

async function serverStreaming(client, messageCount) {
  const replies = await client.lotsOfReplies(
    { name: String(messageCount) },
    { maxResponseMessages: messageCount },
  );
  let count = 0;
  for await (const reply of replies) {
    if (reply.message !== payloadForIndex(count)) {
      throw new Error(`unexpected server-stream response: ${JSON.stringify(reply)}`);
    }
    count++;
  }
  if (count !== messageCount) {
    throw new Error(`expected ${messageCount} server-stream responses, got ${count}`);
  }
}

async function clientStreaming(client, messageCount) {
  const call = await client.lotsOfGreetings();
  await sendRequestMessages(call, messageCount);
  const reply = await call.closeAndRecv();
  if (reply.message !== `streamed ${messageCount} greetings`) {
    throw new Error(`unexpected client-stream response: ${JSON.stringify(reply)}`);
  }
}

async function bidiStreaming(client, messageCount) {
  const call = await client.bidiHello({ maxResponseMessages: messageCount });
  let received = 0;
  await Promise.all([sendMessages(), recvMessages()]);

  async function sendMessages() {
    await sendRequestMessages(call, messageCount);
    await call.closeSend();
  }

  async function recvMessages() {
    for (;;) {
      const reply = await call.recv();
      if (reply === undefined) {
        break;
      }
      if (reply.message !== payloadForIndex(received)) {
        throw new Error(`unexpected bidi response: ${JSON.stringify(reply)}`);
      }
      received++;
    }
    if (received !== messageCount) {
      throw new Error(`expected ${messageCount} bidi responses, got ${received}`);
    }
  }
}

function serverStreamReplies(call) {
  validateMetadata(call.request.metadata);
  const count = messageCountFromName(decodeRequest(call.request.body).name);
  return repeatedReplies(count);
}

async function* clientStreamReply(call) {
  validateMetadata(call.request.metadata);
  let count = 0;
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      yield encodeReply(`streamed ${count} greetings`);
      return;
    }
    decodeRequest(frame.body);
    count++;
  }
}

async function* bidiReplies(call) {
  validateMetadata(call.request.metadata);
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      return;
    }
    yield encodeReply(decodeRequest(frame.body).name);
  }
}

function encodeReply(message) {
  return HelloReply.encode({ message }).finish();
}

function callOptions() {
  return Object.keys(Metadata).length === 0 ? {} : { metadata: Metadata, timeoutMs: IdleTimeoutMs };
}

async function sendRequestMessages(call, messageCount) {
  if (messageCount <= 0) {
    return;
  }
  if (messageCount === 1 || typeof call.sendMany !== "function" || SendManyBatchSize <= 1) {
    for (let i = 0; i < messageCount; i++) {
      await call.send({ name: payloadForIndex(i) });
    }
    return;
  }

  let remaining = messageCount;
  while (remaining > 0) {
    const count = Math.min(remaining, SendManyBatchSize);
    await call.sendMany(
      Array.from({ length: count }, (_, offset) => ({
        name: payloadForIndex(messageCount - remaining + offset),
      })),
    );
    remaining -= count;
  }
}

function repeatedReplies(count) {
  let sent = 0;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (sent >= count) {
        return Promise.resolve({ done: true, value: undefined });
      }
      const reply = encodeReply(payloadForIndex(sent));
      sent++;
      return Promise.resolve({ done: false, value: reply });
    },
    nextBatch(max = SendManyBatchSize) {
      if (sent >= count) {
        return Promise.resolve({ done: true, value: undefined });
      }
      const batchCount = Math.min(count - sent, Math.max(1, max));
      const batch = Array.from({ length: batchCount }, () => {
        const reply = encodeReply(payloadForIndex(sent));
        sent++;
        return reply;
      });
      return Promise.resolve({ done: false, value: batch });
    },
  };
}

function decodeRequest(body) {
  return HelloRequest.decode(body);
}

function loadPayloadProfile() {
  switch (process.env.TREVRPC_BENCH_PAYLOAD_PROFILE ?? "tiny") {
    case "tiny":
      return [RequestName];
    case "small":
      return ["x".repeat(253)];
    case "medium":
      return ["x".repeat(4093)];
    case "large":
      return ["x".repeat(65_532)];
    case "mixed":
      return [RequestName, "x".repeat(253), "x".repeat(4093), "x".repeat(65_532)];
    default:
      throw new Error(
        `unsupported TREVRPC_BENCH_PAYLOAD_PROFILE ${JSON.stringify(process.env.TREVRPC_BENCH_PAYLOAD_PROFILE)}`,
      );
  }
}

function payloadForIndex(index) {
  return PayloadValues[index % PayloadValues.length];
}

function loadMetadataProfile() {
  switch (process.env.TREVRPC_BENCH_METADATA_PROFILE ?? "none") {
    case "none":
      return {};
    case "production":
      return {
        "benchmark-client": "trevrpc-bench",
        "benchmark-profile": "production",
        "benchmark-trace-id": "trevrpc-benchmark",
      };
    default:
      throw new Error(
        `unsupported TREVRPC_BENCH_METADATA_PROFILE ${JSON.stringify(process.env.TREVRPC_BENCH_METADATA_PROFILE)}`,
      );
  }
}

function validateMetadata(actual = {}) {
  for (const [key, expected] of Object.entries(Metadata)) {
    const value = actual[key] ?? actual[key.toLowerCase()];
    const text =
      value instanceof Uint8Array ? new TextDecoder().decode(value) : String(value ?? "");
    if (text !== expected) {
      throw new Error(
        `metadata ${JSON.stringify(key)} = ${JSON.stringify(text)}, want ${JSON.stringify(expected)}`,
      );
    }
  }
}

function messageCountFromName(name) {
  const count = Number(name);
  return Number.isInteger(count) && count > 0 ? count : LatencyStreamMessageCount;
}

function positiveInteger(value) {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`expected a positive integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}

function requiredArg(index, name) {
  const value = process.argv[index];
  if (value == null || value === "") {
    throw new Error(`missing ${name}`);
  }
  return value;
}
