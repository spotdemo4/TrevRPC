import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

import { createRoot, createServiceClient } from "../src/index.js";
import { NodeTransport } from "../src/node.js";

const BenchmarkStreamMessageCount = 16;
const BenchmarkRequest = Object.freeze({ name: "TrevRPC benchmark" });
const ServerStartupTimeoutMs = 10_000;
const ServerShutdownTimeoutMs = 5_000;

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
  transport = await NodeTransport.connect(`https://127.0.0.1:${port}/trevrpc`, {
    skipCertificateValidation: true,
    maxStreamsPerSession: 128,
    idleTimeoutMs: 600_000,
  });
  const client = createServiceClient(transport, GreeterService, root);

  await warmClient(client);
  await runBenchmarkCase("unary_round_trip/trevrpc_js_native", iterations, () =>
    unaryRoundTrip(client),
  );
  await runBenchmarkCase("server_stream_16_messages/trevrpc_js_native", iterations, () =>
    serverStreaming(client),
  );
  await runBenchmarkCase("client_stream_16_messages/trevrpc_js_native", iterations, () =>
    clientStreaming(client),
  );
  await runBenchmarkCase("bidi_stream_16_messages/trevrpc_js_native", iterations, () =>
    bidiStreaming(client),
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
  await serverStreaming(client);
  await clientStreaming(client);
  await bidiStreaming(client);
}

async function runBenchmarkCase(name, count, fn) {
  const start = process.hrtime.bigint();
  for (let i = 0; i < count; i++) {
    await fn();
  }
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const opsPerSecond = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
  console.log(
    `${name}: ${opsPerSecond.toFixed(0)} ops/s (${count} iterations in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function unaryRoundTrip(client) {
  const reply = await client.sayHello(BenchmarkRequest);
  if (reply.message !== BenchmarkRequest.name) {
    throw new Error(`unexpected unary response: ${JSON.stringify(reply)}`);
  }
}

async function serverStreaming(client) {
  const replies = await client.lotsOfReplies(BenchmarkRequest);
  let count = 0;
  for await (const reply of replies) {
    if (reply.message !== "server stream") {
      throw new Error(`unexpected server-stream response: ${JSON.stringify(reply)}`);
    }
    count++;
  }
  if (count !== BenchmarkStreamMessageCount) {
    throw new Error(
      `expected ${BenchmarkStreamMessageCount} server-stream responses, got ${count}`,
    );
  }
}

async function clientStreaming(client) {
  const call = await client.lotsOfGreetings();
  for (let i = 0; i < BenchmarkStreamMessageCount; i++) {
    await call.send(BenchmarkRequest);
  }
  const reply = await call.closeAndRecv();
  if (reply.message !== `streamed ${BenchmarkStreamMessageCount} greetings`) {
    throw new Error(`unexpected client-stream response: ${JSON.stringify(reply)}`);
  }
}

async function bidiStreaming(client) {
  const call = await client.bidiHello();
  for (let i = 0; i < BenchmarkStreamMessageCount; i++) {
    await call.send(BenchmarkRequest);
  }
  await call.closeSend();

  let count = 0;
  for (;;) {
    const reply = await call.recv();
    if (reply === undefined) {
      break;
    }
    if (reply.message !== BenchmarkRequest.name) {
      throw new Error(`unexpected bidi response: ${JSON.stringify(reply)}`);
    }
    count++;
  }
  if (count !== BenchmarkStreamMessageCount) {
    throw new Error(`expected ${BenchmarkStreamMessageCount} bidi responses, got ${count}`);
  }
}
