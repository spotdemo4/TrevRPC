import { createRoot, createServiceClient } from "../src/index.js";
import { NodeServer, NodeTransport } from "../src/node.js";

const LatencyStreamMessageCount = 1;
const RequestName = "TrevRPC benchmark";
const IdleTimeoutMs = 600_000;
const SendManyBatchSize = positiveInteger(process.env.TREVRPC_JS_SEND_MANY_BATCH ?? "16");

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
  const transport = await NodeTransport.connect({
    host,
    port,
    skipCertificateValidation: true,
    maxStreamsPerSession: 128,
    idleTimeoutMs: IdleTimeoutMs,
    streamWriteBatchMaxMessages: SendManyBatchSize,
  });
  try {
    const client = createServiceClient(transport, GreeterService, root);
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
    streamWriteBatchMaxMessages: SendManyBatchSize,
  });
  server.registerService(GreeterService, {
    sayHello: (call) => encodeReply(decodeRequest(call.request.body).name),
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
  const reply = await client.sayHello({ name: RequestName });
  if (reply.message !== RequestName) {
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
    if (reply.message !== "server stream") {
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
      if (reply.message !== RequestName) {
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
  const count = messageCountFromName(decodeRequest(call.request.body).name);
  return repeatedReplies(count, "server stream");
}

async function* clientStreamReply(call) {
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

async function sendRequestMessages(call, messageCount) {
  if (messageCount <= 0) {
    return;
  }
  if (messageCount === 1 || typeof call.sendMany !== "function" || SendManyBatchSize <= 1) {
    for (let i = 0; i < messageCount; i++) {
      await call.send({ name: RequestName });
    }
    return;
  }

  let remaining = messageCount;
  while (remaining > 0) {
    const count = Math.min(remaining, SendManyBatchSize);
    await call.sendMany(Array.from({ length: count }, () => ({ name: RequestName })));
    remaining -= count;
  }
}

function repeatedReplies(count, message) {
  let sent = 0;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (sent >= count) {
        return Promise.resolve({ done: true, value: undefined });
      }
      sent++;
      return Promise.resolve({ done: false, value: encodeReply(message) });
    },
    nextBatch(max = SendManyBatchSize) {
      if (sent >= count) {
        return Promise.resolve({ done: true, value: undefined });
      }
      const batchCount = Math.min(count - sent, Math.max(1, max));
      const batch = Array.from({ length: batchCount }, () => {
        sent++;
        return encodeReply(message);
      });
      return Promise.resolve({ done: false, value: batch });
    },
  };
}

function decodeRequest(body) {
  return HelloRequest.decode(body);
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
