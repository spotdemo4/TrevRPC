import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { chromium } from "playwright";

if (process.argv[2] === "--browser-version") {
  const browser = await launchBrowser();
  try {
    console.log(await browser.version());
  } finally {
    await closeBrowser(browser);
  }
  process.exit(0);
}

const connectMode = process.argv[2] === "--connect";
const pageURL = requiredArg(connectMode ? 3 : 2, "page-url");
const endpointURL = requiredArg(
  connectMode ? 4 : 3,
  connectMode ? "connect-url" : "webtransport-url",
);
if (connectMode && !endpointURL.startsWith("https://")) {
  throw new Error("ConnectRPC benchmark endpoint must use https://");
}
const certFile = connectMode ? null : requiredArg(4, "cert-file");
const iterations = positiveInteger(
  process.argv[connectMode ? 5 : 5] ?? process.env.WEBTRANSPORT_ITERATIONS ?? "1000",
);
const throughputMessages = positiveInteger(
  process.env.WEBTRANSPORT_THROUGHPUT_MESSAGES ?? String(iterations),
);
const certificateSha256Base64 = certFile == null ? null : await certificateHash(certFile);
const benchmarkConfig = benchmarkConfigFromEnv();

let browser;
let context;
let page;
const pageErrors = [];
let shuttingDown = false;

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    void (async () => {
      if (browser != null) {
        await closeBrowser(browser);
      }
      process.exit(signal === "SIGINT" ? 130 : 143);
    })();
  });
}

try {
  browser = await launchBrowser();
  context = await browser.newContext({
    ignoreHTTPSErrors:
      connectMode &&
      endpointURL.startsWith("https://") &&
      envFlag("TREVRPC_CONNECT_INSECURE_SKIP_VERIFY"),
  });
  page = await context.newPage();
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") {
      pageErrors.push(message.text());
    }
  });

  await page.goto(pageURL, { waitUntil: "domcontentloaded" });
  const results = connectMode
    ? await page.evaluate(runConnectBenchmarks, {
        benchmarkConfig,
        connectURL: endpointURL,
        iterations,
      })
    : await page.evaluate(runBrowserBenchmarks, {
        benchmarkConfig,
        certificateSha256Base64,
        iterations,
        throughputMessages,
        webTransportURL: endpointURL,
      });
  if (pageErrors.length > 0) {
    throw new Error(pageErrors.join("\n"));
  }

  for (const result of results) {
    if (result.unsupported) {
      console.log(`${result.name}: N/A (unsupported)`);
    } else {
      console.log(
        result.metric === "throughput"
          ? `${result.name}: ${result.value.toFixed(0)} messages/s (${result.iterations} messages in ${result.elapsedSeconds.toFixed(3)}s)`
          : `${result.name}: ${result.value.toFixed(3)} us/op (${result.iterations} iterations in ${result.elapsedSeconds.toFixed(3)}s)`,
      );
    }
  }
} finally {
  if (browser != null) {
    await closeBrowser(browser);
  }
}

async function runBrowserBenchmarks({
  benchmarkConfig,
  certificateSha256Base64,
  iterations,
  throughputMessages,
  webTransportURL,
}) {
  const LatencyStreamMessageCount = 1;
  const PayloadValues = payloadValues(benchmarkConfig.payloadProfile);
  const RequestName = payloadForIndex(0);
  const { connect, createRoot, createServiceClient } = await import("/src/index.js");
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

  const connectOptions = {
    serverCertificateHashes: [
      {
        algorithm: "sha-256",
        value: base64Bytes(certificateSha256Base64),
      },
    ],
  };
  if (benchmarkConfig.congestionControl != null) {
    connectOptions.congestionControl = benchmarkConfig.congestionControl;
  }

  const transport = await connect(webTransportURL, connectOptions);
  const callOptions = {};
  if (Object.keys(benchmarkConfig.metadata).length > 0) {
    callOptions.metadata = benchmarkConfig.metadata;
  }
  callOptions.maxResponseStreamBodySize = -1;
  const client = createServiceClient(transport, GreeterService, root, callOptions);

  try {
    await warmClient(client);
    return [
      await runLatencyCase("unary_latency", iterations, () => unary(client)),
      await runLatencyCase("server_stream_latency", iterations, () =>
        serverStreaming(client, LatencyStreamMessageCount),
      ),
      await runThroughputCase("server_stream_throughput", throughputMessages, () =>
        serverStreaming(client, throughputMessages),
      ),
      await runLatencyCase("client_stream_latency", iterations, () =>
        clientStreaming(client, LatencyStreamMessageCount),
      ),
      await runThroughputCase("client_stream_throughput", throughputMessages, () =>
        clientStreaming(client, throughputMessages),
      ),
      await runLatencyCase("bidi_stream_latency", iterations, () =>
        bidiStreaming(client, LatencyStreamMessageCount),
      ),
      await runThroughputCase("bidi_stream_throughput", throughputMessages, () =>
        bidiStreaming(client, throughputMessages),
      ),
      ...(benchmarkConfig.concurrentStreams > 1
        ? [
            await runThroughputCase(
              `server_stream_concurrent_${benchmarkConfig.concurrentStreams}_throughput`,
              throughputMessages * benchmarkConfig.concurrentStreams,
              () =>
                runConcurrent(benchmarkConfig.concurrentStreams, () =>
                  serverStreaming(client, throughputMessages),
                ),
            ),
            await runThroughputCase(
              `client_stream_concurrent_${benchmarkConfig.concurrentStreams}_throughput`,
              throughputMessages * benchmarkConfig.concurrentStreams,
              () =>
                runConcurrent(benchmarkConfig.concurrentStreams, () =>
                  clientStreaming(client, throughputMessages),
                ),
            ),
            await runThroughputCase(
              `bidi_stream_concurrent_${benchmarkConfig.concurrentStreams}_throughput`,
              throughputMessages * benchmarkConfig.concurrentStreams,
              () =>
                runConcurrent(benchmarkConfig.concurrentStreams, () =>
                  bidiStreaming(client, throughputMessages),
                ),
            ),
          ]
        : []),
    ];
  } finally {
    transport.close({ closeCode: 0, reason: "browser WebTransport benchmark complete" });
  }

  function base64Bytes(value) {
    const binary = atob(value);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  }

  function serverStreamRequestName(messageCount) {
    return String(messageCount);
  }

  async function runConcurrent(count, fn) {
    await Promise.all(Array.from({ length: count }, () => fn()));
  }

  async function warmClient(client) {
    await unary(client);
    await serverStreaming(client, LatencyStreamMessageCount);
    await clientStreaming(client, LatencyStreamMessageCount);
    await bidiStreaming(client, LatencyStreamMessageCount);
  }

  async function runLatencyCase(name, count, fn) {
    const start = performance.now();
    for (let index = 0; index < count; index += 1) {
      await fn();
    }
    const elapsedSeconds = (performance.now() - start) / 1000;
    return {
      elapsedSeconds,
      iterations: count,
      metric: "latency",
      name,
      value: (elapsedSeconds * 1_000_000) / count,
    };
  }

  async function runThroughputCase(name, count, fn) {
    const start = performance.now();
    await fn();
    const elapsedSeconds = (performance.now() - start) / 1000;
    return {
      elapsedSeconds,
      iterations: count,
      metric: "throughput",
      name,
      value: elapsedSeconds > 0 ? count / elapsedSeconds : 0,
    };
  }

  async function unary(client) {
    const reply = await client.sayHello({ name: RequestName });
    if (reply.message !== RequestName) {
      throw new Error(`unexpected unary response: ${JSON.stringify(reply)}`);
    }
  }

  async function serverStreaming(client, messageCount) {
    const replies = await client.lotsOfReplies(
      { name: serverStreamRequestName(messageCount) },
      { maxResponseMessages: messageCount },
    );
    let count = 0;
    for await (const reply of replies) {
      if (reply.message !== payloadForIndex(count)) {
        throw new Error(`unexpected server-stream response: ${JSON.stringify(reply)}`);
      }
      count += 1;
    }
    if (count !== messageCount) {
      throw new Error(`expected ${messageCount} server-stream responses, got ${count}`);
    }
  }

  async function clientStreaming(client, messageCount) {
    const call = await client.lotsOfGreetings();
    await sendRequests(call, messageCount);
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
      await sendRequests(call, messageCount);
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
        received += 1;
      }
      if (received !== messageCount) {
        throw new Error(`expected ${messageCount} bidi responses, got ${received}`);
      }
    }
  }

  async function sendRequests(call, messageCount) {
    for (let index = 0; index < messageCount; index += 1) {
      await call.send({ name: payloadForIndex(index) });
    }
  }

  function payloadValues(profile) {
    switch (profile) {
      case "tiny":
        return ["TrevRPC benchmark"];
      case "small":
        return ["x".repeat(253)];
      case "medium":
        return ["x".repeat(4093)];
      case "large":
        return ["x".repeat(65_532)];
      case "mixed":
        return ["TrevRPC benchmark", "x".repeat(253), "x".repeat(4093), "x".repeat(65_532)];
      default:
        throw new Error(`unsupported payload profile ${JSON.stringify(profile)}`);
    }
  }

  function payloadForIndex(index) {
    return PayloadValues[index % PayloadValues.length];
  }
}

async function runConnectBenchmarks({ benchmarkConfig, connectURL, iterations }) {
  const PayloadValues = payloadValues(benchmarkConfig.payloadProfile);
  const RequestName = payloadForIndex(0);
  const { createRoot } = await import("/src/index.js");
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
  const baseURL = connectURL.replace(/\/+$/, "");

  await warmClient();
  return [
    await runLatencyCase("unary_latency", iterations, unary),
    await runLatencyCase("server_stream_latency", iterations, () => serverStreaming(1)),
    await runThroughputCase("server_stream_throughput", iterations, () =>
      serverStreaming(iterations),
    ),
    unsupportedCase("client_stream_latency", iterations, "latency"),
    unsupportedCase("client_stream_throughput", iterations, "throughput"),
    unsupportedCase("bidi_stream_latency", iterations, "latency"),
    unsupportedCase("bidi_stream_throughput", iterations, "throughput"),
  ];

  async function warmClient() {
    await unary();
    await serverStreaming(1);
  }

  async function runLatencyCase(name, count, fn) {
    const start = performance.now();
    for (let index = 0; index < count; index += 1) {
      await fn();
    }
    const elapsedSeconds = (performance.now() - start) / 1000;
    return {
      elapsedSeconds,
      iterations: count,
      metric: "latency",
      name,
      value: (elapsedSeconds * 1_000_000) / count,
    };
  }

  async function runThroughputCase(name, count, fn) {
    const start = performance.now();
    await fn();
    const elapsedSeconds = (performance.now() - start) / 1000;
    return {
      elapsedSeconds,
      iterations: count,
      metric: "throughput",
      name,
      value: elapsedSeconds > 0 ? count / elapsedSeconds : 0,
    };
  }

  function unsupportedCase(name, count, metric) {
    return {
      elapsedSeconds: 0,
      iterations: count,
      metric,
      name,
      unsupported: true,
      value: null,
    };
  }

  async function unary() {
    const response = await fetch(`${baseURL}/example.greeter.Greeter/SayHello`, {
      body: encodeMessage(HelloRequest, { name: RequestName }),
      headers: connectHeaders("application/proto"),
      method: "POST",
    });
    await assertOK(response);
    assertConnectResponseMetadata(response);
    const reply = HelloReply.decode(new Uint8Array(await response.arrayBuffer()));
    if (reply.message !== RequestName) {
      throw new Error(`unexpected Connect unary response: ${JSON.stringify(reply)}`);
    }
  }

  async function serverStreaming(messageCount) {
    const response = await fetch(`${baseURL}/example.greeter.Greeter/LotsOfReplies`, {
      body: encodeEnvelope(encodeMessage(HelloRequest, { name: String(messageCount) })),
      headers: connectHeaders("application/connect+proto"),
      method: "POST",
    });
    await assertOK(response);
    assertConnectResponseMetadata(response);

    let count = 0;
    for await (const body of connectEnvelopeBodies(response.body)) {
      const reply = HelloReply.decode(body);
      if (reply.message !== payloadForIndex(count)) {
        throw new Error(`unexpected Connect server-stream response: ${JSON.stringify(reply)}`);
      }
      count += 1;
    }
    if (count !== messageCount) {
      throw new Error(`expected ${messageCount} Connect server-stream responses, got ${count}`);
    }
  }

  function encodeMessage(messageType, message) {
    return messageType.encode(messageType.fromObject(message)).finish();
  }

  function connectHeaders(contentType) {
    const headers = {
      "Connect-Protocol-Version": "1",
      "Connect-Timeout-Ms": "600000",
      "Content-Type": contentType,
      ...benchmarkConfig.metadata,
    };
    return headers;
  }

  function payloadValues(profile) {
    switch (profile) {
      case "tiny":
        return ["TrevRPC benchmark"];
      case "small":
        return ["x".repeat(253)];
      case "medium":
        return ["x".repeat(4093)];
      case "large":
        return ["x".repeat(65_532)];
      case "mixed":
        return ["TrevRPC benchmark", "x".repeat(253), "x".repeat(4093), "x".repeat(65_532)];
      default:
        throw new Error(`unsupported payload profile ${JSON.stringify(profile)}`);
    }
  }

  function payloadForIndex(index) {
    return PayloadValues[index % PayloadValues.length];
  }

  function encodeEnvelope(body) {
    const frame = new Uint8Array(5 + body.byteLength);
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    view.setUint8(0, 0);
    view.setUint32(1, body.byteLength, false);
    frame.set(body, 5);
    return frame;
  }

  async function assertOK(response) {
    if (!response.ok) {
      throw new Error(`Connect request failed with ${response.status}: ${await response.text()}`);
    }
  }

  function assertConnectResponseMetadata(response) {
    if (Object.keys(benchmarkConfig.metadata).length === 0) {
      return;
    }
    const value = response.headers.get("benchmark-response");
    if (value !== "ok") {
      throw new Error(`Connect response metadata benchmark-response = ${JSON.stringify(value)}`);
    }
  }

  async function* connectEnvelopeBodies(stream) {
    if (stream == null) {
      throw new Error("Connect response did not include a body stream");
    }
    const reader = stream.getReader();
    const chunks = [];
    let buffered = 0;
    try {
      for (;;) {
        while (buffered < 5) {
          const chunk = await readChunk(reader);
          if (chunk == null) {
            if (buffered === 0) {
              return;
            }
            throw new Error("truncated Connect envelope header");
          }
          chunks.push(chunk);
          buffered += chunk.byteLength;
        }
        const header = take(5);
        buffered -= 5;
        const view = new DataView(header.buffer, header.byteOffset, header.byteLength);
        const flags = view.getUint8(0);
        const length = view.getUint32(1, false);
        while (buffered < length) {
          const chunk = await readChunk(reader);
          if (chunk == null) {
            throw new Error("truncated Connect envelope body");
          }
          chunks.push(chunk);
          buffered += chunk.byteLength;
        }
        const body = take(length);
        buffered -= length;
        if ((flags & 0x02) !== 0) {
          continue;
        }
        if (flags !== 0) {
          throw new Error(`unsupported Connect envelope flags ${flags}`);
        }
        yield body;
      }
    } finally {
      reader.releaseLock();
    }

    function take(size) {
      const output = new Uint8Array(size);
      let offset = 0;
      while (offset < size) {
        const chunk = chunks.shift();
        const needed = size - offset;
        if (chunk.byteLength <= needed) {
          output.set(chunk, offset);
          offset += chunk.byteLength;
        } else {
          output.set(chunk.subarray(0, needed), offset);
          chunks.unshift(chunk.subarray(needed));
          offset += needed;
        }
      }
      return output;
    }
  }

  async function readChunk(reader) {
    const { value, done } = await reader.read();
    if (done) {
      return null;
    }
    return value;
  }
}

async function certificateHash(certFile) {
  const certificate = await readFile(certFile);
  return createHash("sha256").update(certificateDer(certificate)).digest("base64");
}

function certificateDer(certificate) {
  const text = certificate.toString("utf8");
  const match = /-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\s]+)-----END CERTIFICATE-----/.exec(
    text,
  );
  if (match == null) {
    return certificate;
  }
  return Buffer.from(match[1].replace(/\s/g, ""), "base64");
}

function positiveInteger(value) {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`iterations must be a positive integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}

function optionalPositiveInteger(name, defaultValue) {
  const value = process.env[name];
  return value == null || value === "" ? defaultValue : positiveInteger(value);
}

function envFlag(name) {
  return process.env[name] === "1" || process.env[name] === "true";
}

function benchmarkConfigFromEnv() {
  const congestionControl = process.env.WEBTRANSPORT_CONGESTION_CONTROL || null;
  if (
    congestionControl != null &&
    !["default", "low-latency", "throughput"].includes(congestionControl)
  ) {
    throw new Error(
      `unsupported WEBTRANSPORT_CONGESTION_CONTROL ${JSON.stringify(congestionControl)}`,
    );
  }

  return {
    concurrentStreams: optionalPositiveInteger("WEBTRANSPORT_CONCURRENT_STREAMS", 1),
    congestionControl,
    metadata: metadataProfile(process.env.TREVRPC_BENCH_METADATA_PROFILE ?? "none"),
    payloadProfile: payloadProfile(process.env.TREVRPC_BENCH_PAYLOAD_PROFILE ?? "tiny"),
  };
}

function payloadProfile(profile) {
  if (["tiny", "small", "medium", "large", "mixed"].includes(profile)) {
    return profile;
  }
  throw new Error(`unsupported TREVRPC_BENCH_PAYLOAD_PROFILE ${JSON.stringify(profile)}`);
}

function metadataProfile(profile) {
  switch (profile) {
    case "none":
      return {};
    case "production":
      return {
        "benchmark-client": "trevrpc-bench",
        "benchmark-profile": "production",
        "benchmark-trace-id": "trevrpc-benchmark",
      };
    default:
      throw new Error(`unsupported TREVRPC_BENCH_METADATA_PROFILE ${JSON.stringify(profile)}`);
  }
}

function requiredArg(index, name) {
  const value = process.argv[index];
  if (value == null || value === "") {
    throw new Error(
      `usage: node trevrpc-js/bench/webtransport_browser.js [--connect] <page-url> <endpoint-url> [cert-file] <iterations>; missing ${name}`,
    );
  }
  return value;
}

async function launchBrowser() {
  const launchOptions = {
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    chromiumSandbox: false,
    dumpio: process.env.TREVRPC_BROWSER_DUMPIO === "1",
  };
  if (process.env.TREVRPC_BROWSER_NETLOG != null && process.argv[2] !== "--browser-version") {
    launchOptions.args.push(`--log-net-log=${process.env.TREVRPC_BROWSER_NETLOG}`);
  }
  if (process.env.TREVRPC_BROWSER_ARGS != null) {
    launchOptions.args.push(...process.env.TREVRPC_BROWSER_ARGS.split(/\s+/).filter(Boolean));
  }
  if (process.env.TREVRPC_BROWSER_CHROMIUM != null) {
    launchOptions.executablePath = process.env.TREVRPC_BROWSER_CHROMIUM;
  }
  return chromium.launch(launchOptions);
}

async function closeBrowser(browser) {
  const closed = await Promise.race([
    browser.close().then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 5_000)),
  ]);
  if (!closed) {
    browser.process()?.kill("SIGKILL");
  }
}
