import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

import { createRoot, createServiceClient } from "../src/index.js";
import { NodeTransport } from "../src/node.js";

const ServerStartupTimeoutMs = 10_000;
const ServerShutdownTimeoutMs = 5_000;

const serverBinary = requiredArg(2, "server binary");
const profileCase = process.argv[3] ?? "unary";
const sendMode = process.argv[4] ?? "copy";
const concurrency = positiveInteger(process.argv[5] ?? "16");
const iterations = positiveInteger(process.argv[6] ?? "100");
const payloadBytes = positiveInteger(process.argv[7] ?? "64");
const warmupIterations = nonNegativeInteger(process.env.JS_COMPLETION_PROFILE_WARMUP ?? "0");
const timeoutMs = positiveInteger(process.env.JS_COMPLETION_PROFILE_TIMEOUT_MS ?? "5000");
const recvLeadMs = nonNegativeInteger(process.env.JS_COMPLETION_PROFILE_RECV_LEAD_MS ?? "25");
const sample = process.env.JS_COMPLETION_PROFILE_SAMPLE ?? "unspecified";

if (profileCase !== "unary" && profileCase !== "bidi-duplex") {
  throw new Error(`unsupported profile case ${JSON.stringify(profileCase)}`);
}
if (sendMode !== "copy" && sendMode !== "zero-copy") {
  throw new Error(`unsupported send mode ${JSON.stringify(sendMode)}`);
}

const root = createRoot({
  nested: {
    hello: {
      nested: {
        v1: {
          nested: {
            HelloRequest: { fields: { name: { type: "string", id: 1 } } },
            HelloReply: { fields: { message: { type: "string", id: 1 } } },
          },
        },
      },
    },
  },
});
const service = Object.freeze({
  fullName: "hello.v1.Greeter",
  methods: {
    sayHello: {
      name: "SayHello",
      kind: "unary",
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
const request = Object.freeze({ name: "x".repeat(payloadBytes) });
const callOptions = Object.freeze({
  outboundZeroCopy: sendMode === "zero-copy",
  streamIdleTimeoutMs: undefined,
});

const server = spawn(serverBinary, ["--serve"], { stdio: ["pipe", "pipe", "pipe"] });
server.stderr.on("data", (chunk) => process.stderr.write(chunk));

let transport;
let failure;
try {
  const port = await waitForServerPort(server);
  transport = await NodeTransport.connect({
    host: "127.0.0.1",
    port,
    skipCertificateValidation: true,
    maxStreamsPerSession: Math.max(128, concurrency * 2),
    idleTimeoutMs: 600_000,
  });
  const client = createServiceClient(transport, service, root, callOptions);

  if (warmupIterations > 0) {
    await runUnary(client, 1, warmupIterations);
    await runBidiDuplex(client, 1, warmupIterations, timeoutMs, 0);
  }

  const started = process.hrtime.bigint();
  const operations =
    profileCase === "unary"
      ? await runUnary(client, concurrency, iterations)
      : await runBidiDuplex(client, concurrency, iterations, timeoutMs, recvLeadMs);
  const elapsedSeconds = Number(process.hrtime.bigint() - started) / 1_000_000_000;

  console.log(
    JSON.stringify({
      profileCase,
      sample,
      sendMode,
      concurrency,
      iterations,
      operations,
      payloadBytes,
      warmupIterations,
      timeoutMs,
      recvLeadMs,
      elapsedSeconds,
      operationsPerSecond: operations / elapsedSeconds,
      averageLatencyMs: (elapsedSeconds * 1000 * concurrency) / operations,
      status: "ok",
    }),
  );
} catch (error) {
  failure = error;
  console.log(
    JSON.stringify({
      profileCase,
      sample,
      sendMode,
      concurrency,
      iterations,
      payloadBytes,
      warmupIterations,
      timeoutMs,
      recvLeadMs,
      status: "failed",
      error: error?.message ?? String(error),
    }),
  );
} finally {
  transport?.close();
  await stopServer(server);
}

if (failure != null) {
  process.exitCode = 2;
}

async function runUnary(client, lanes, iterationsPerLane) {
  await Promise.all(
    Array.from({ length: lanes }, async () => {
      for (let i = 0; i < iterationsPerLane; i += 1) {
        const reply = await client.sayHello(request);
        if (reply.message !== request.name) {
          throw new Error("unexpected unary reply");
        }
      }
    }),
  );
  return lanes * iterationsPerLane;
}

async function runBidiDuplex(client, lanes, iterationsPerLane, operationTimeoutMs, leadMs) {
  let operations = 0;
  for (let iteration = 0; iteration < iterationsPerLane; iteration += 1) {
    const calls = await Promise.all(Array.from({ length: lanes }, () => client.bidiHello()));
    try {
      const receives = calls.map((call) => call.recv());
      if (leadMs > 0) {
        await delay(leadMs);
      }
      await withTimeout(
        Promise.all(calls.map((call) => call.send(request))),
        operationTimeoutMs,
        "request sends starved behind pending response reads",
      );
      await Promise.all(calls.map((call) => call.closeSend()));
      const replies = await withTimeout(
        Promise.all(receives),
        operationTimeoutMs,
        "response reads did not complete",
      );
      for (const reply of replies) {
        if (reply?.message !== request.name) {
          throw new Error("unexpected bidirectional reply");
        }
      }
      await Promise.all(calls.map((call) => call.recv()));
      operations += lanes;
    } finally {
      await Promise.race([
        Promise.allSettled(calls.map((call) => call.close())),
        delay(1000, undefined, { ref: false }),
      ]);
    }
  }
  return operations;
}

function withTimeout(promise, milliseconds, message) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    }),
  ]).finally(() => clearTimeout(timer));
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
  if ((await Promise.race([exited, delay(ServerShutdownTimeoutMs, "timeout")])) === "timeout") {
    serverProcess.kill("SIGTERM");
    await Promise.race([exited, delay(1000)]);
  }
}

function requiredArg(index, name) {
  const value = process.argv[index];
  if (value == null || value === "") {
    throw new Error(`missing ${name}`);
  }
  return value;
}

function positiveInteger(value) {
  if (!/^[1-9][0-9]*$/u.test(value)) {
    throw new Error(`expected a positive integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}

function nonNegativeInteger(value) {
  if (!/^(?:0|[1-9][0-9]*)$/u.test(value)) {
    throw new Error(`expected a non-negative integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}
