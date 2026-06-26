import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

import { createRoot, createServiceClient } from "../src/index.js";
import { NodeTransport } from "../src/node.js";

const LatencyStreamMessageCount = 1;
const BenchmarkRequest = Object.freeze({ name: "TrevRPC benchmark" });
const ServerStartupTimeoutMs = 10_000;
const ServerShutdownTimeoutMs = 5_000;
const SendManyBatchSize = positiveInteger(process.env.TREVRPC_JS_SEND_MANY_BATCH ?? "16");

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

const iterations = positiveInteger(process.argv[2] ?? process.env.JS_ITERATIONS ?? "1000");
const serverBinary = process.argv[3] ?? process.env.TREVRPC_BENCH_SERVER;
if (serverBinary == null || serverBinary === "") {
  throw new Error("usage: node bench/rpc_comparison_native.js <iterations> <server-binary>");
}

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
    streamWriteBatchMaxMessages: SendManyBatchSize,
  });
  const client = createServiceClient(transport, GreeterService, root);

  await warmClient(client);
  await runLatencyCase("unary_latency/trevrpc_js_msquic", iterations, () => unaryRoundTrip(client));
  await runLatencyCase("server_stream_latency/trevrpc_js_msquic", iterations, () =>
    serverStreaming(client, LatencyStreamMessageCount),
  );
  await runMessageThroughputCase("server_stream_throughput/trevrpc_js_msquic", iterations, () =>
    serverStreaming(client, iterations),
  );
  await runLatencyCase("client_stream_latency/trevrpc_js_msquic", iterations, () =>
    clientStreaming(client, LatencyStreamMessageCount),
  );
  await runMessageThroughputCase("client_stream_throughput/trevrpc_js_msquic", iterations, () =>
    clientStreaming(client, iterations),
  );
  await runLatencyCase("bidi_stream_latency/trevrpc_js_msquic", iterations, () =>
    bidiStreaming(client, LatencyStreamMessageCount),
  );
  await runMessageThroughputCase("bidi_stream_throughput/trevrpc_js_msquic", iterations, () =>
    bidiStreaming(client, iterations),
  );
} finally {
  transport?.close();
  await stopServer(server);
}

function positiveInteger(value) {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`iterations must be a positive integer, got ${JSON.stringify(value)}`);
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

async function warmClient(client) {
  await unaryRoundTrip(client);
  await serverStreaming(client, LatencyStreamMessageCount);
  await clientStreaming(client, LatencyStreamMessageCount);
  await bidiStreaming(client, LatencyStreamMessageCount);
}

async function runLatencyCase(name, count, fn) {
  const start = process.hrtime.bigint();
  for (let i = 0; i < count; i++) {
    await fn();
  }
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const latencyUs = (elapsedSeconds * 1_000_000) / count;
  console.log(
    `${name}: ${latencyUs.toFixed(3)} us/op (${count} iterations in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function runMessageThroughputCase(name, count, fn) {
  const start = process.hrtime.bigint();
  await fn();
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const messagesPerSecond = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
  console.log(
    `${name}: ${messagesPerSecond.toFixed(0)} messages/s (${count} messages in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function unaryRoundTrip(client) {
  const reply = await client.sayHello(BenchmarkRequest);
  if (reply.message !== BenchmarkRequest.name) {
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
      if (reply.message !== BenchmarkRequest.name) {
        throw new Error(`unexpected bidi response: ${JSON.stringify(reply)}`);
      }
      received++;
    }
    if (received !== messageCount) {
      throw new Error(`expected ${messageCount} bidi responses, got ${received}`);
    }
  }
}

async function sendRequestMessages(call, messageCount) {
  if (messageCount <= 0) {
    return;
  }
  if (messageCount === 1 || typeof call.sendMany !== "function" || SendManyBatchSize <= 1) {
    for (let i = 0; i < messageCount; i++) {
      await call.send(BenchmarkRequest);
    }
    return;
  }

  let remaining = messageCount;
  while (remaining > 0) {
    const count = Math.min(remaining, SendManyBatchSize);
    await call.sendMany(Array.from({ length: count }, () => BenchmarkRequest));
    remaining -= count;
  }
}
