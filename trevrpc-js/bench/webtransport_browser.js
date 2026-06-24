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

const pageURL = requiredArg(2, "page-url");
const webTransportURL = requiredArg(3, "webtransport-url");
const certFile = requiredArg(4, "cert-file");
const iterations = positiveInteger(
  process.argv[5] ?? process.env.WEBTRANSPORT_ITERATIONS ?? "1000",
);
const certificateSha256Base64 = await certificateHash(certFile);

let browser;
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
  page = await browser.newPage();
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") {
      pageErrors.push(message.text());
    }
  });

  await page.goto(pageURL, { waitUntil: "domcontentloaded" });
  const results = await page.evaluate(runBrowserBenchmarks, {
    certificateSha256Base64,
    iterations,
    webTransportURL,
  });
  if (pageErrors.length > 0) {
    throw new Error(pageErrors.join("\n"));
  }

  for (const result of results) {
    console.log(
      `${result.name}: ${result.opsPerSecond.toFixed(0)} ops/s (${result.iterations} iterations in ${result.elapsedSeconds.toFixed(3)}s)`,
    );
  }
} finally {
  if (browser != null) {
    await closeBrowser(browser);
  }
}

async function runBrowserBenchmarks({ certificateSha256Base64, iterations, webTransportURL }) {
  const ConcurrentUnaryCount = 16;
  const StreamMessageCount = 16;
  const LongLivedStreamOptions = Object.freeze({ maxResponseMessages: iterations });
  const RequestName = "TrevRPC benchmark";
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

  const transport = await connect(webTransportURL, {
    serverCertificateHashes: [
      {
        algorithm: "sha-256",
        value: base64Bytes(certificateSha256Base64),
      },
    ],
  });
  const client = createServiceClient(transport, GreeterService, root, {
    streamIdleTimeoutMs: 600_000,
    timeoutMs: 600_000,
  });

  try {
    await warmClient(client);
    return [
      await runCase("unary_round_trip", iterations, () => unary(client)),
      await runConcurrentCase(
        "unary_round_trip_concurrent_16",
        iterations,
        ConcurrentUnaryCount,
        () => unary(client),
      ),
      await runCase("server_stream_16_messages", iterations, () => serverStreaming(client)),
      await runCase("client_stream_16_messages", iterations, () => clientStreaming(client)),
      await runCase("bidi_stream_16_messages", iterations, () => bidiStreaming(client)),
      await runLongLivedStreamCase("bidi_stream_long_lived_messages", iterations, () =>
        bidiLongLived(client, iterations),
      ),
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

  async function warmClient(client) {
    await unary(client);
    await serverStreaming(client);
    await clientStreaming(client);
    await bidiStreaming(client);
  }

  async function runCase(name, count, fn) {
    const start = performance.now();
    for (let index = 0; index < count; index += 1) {
      await fn();
    }
    const elapsedSeconds = (performance.now() - start) / 1000;
    const opsPerSecond = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
    return { elapsedSeconds, iterations: count, name, opsPerSecond };
  }

  async function runConcurrentCase(name, count, concurrency, fn) {
    let next = 0;
    const workerCount = Math.min(count, concurrency);
    const start = performance.now();
    await Promise.all(
      Array.from({ length: workerCount }, async () => {
        for (;;) {
          const index = next;
          next += 1;
          if (index >= count) {
            return;
          }
          await fn();
        }
      }),
    );
    const elapsedSeconds = (performance.now() - start) / 1000;
    const opsPerSecond = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
    return { elapsedSeconds, iterations: count, name, opsPerSecond };
  }

  async function runLongLivedStreamCase(name, count, fn) {
    const start = performance.now();
    await fn();
    const elapsedSeconds = (performance.now() - start) / 1000;
    const opsPerSecond = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
    return { elapsedSeconds, iterations: count, name, opsPerSecond };
  }

  async function unary(client) {
    const reply = await client.sayHello({ name: RequestName });
    if (reply.message !== RequestName) {
      throw new Error(`unexpected unary response: ${JSON.stringify(reply)}`);
    }
  }

  async function serverStreaming(client) {
    const replies = await client.lotsOfReplies({ name: RequestName });
    let count = 0;
    for await (const reply of replies) {
      if (reply.message !== "server stream") {
        throw new Error(`unexpected server-stream response: ${JSON.stringify(reply)}`);
      }
      count += 1;
    }
    if (count !== StreamMessageCount) {
      throw new Error(`expected ${StreamMessageCount} server-stream responses, got ${count}`);
    }
  }

  async function clientStreaming(client) {
    const call = await client.lotsOfGreetings();
    for (let index = 0; index < StreamMessageCount; index += 1) {
      await call.send({ name: RequestName });
    }
    const reply = await call.closeAndRecv();
    if (reply.message !== `streamed ${StreamMessageCount} greetings`) {
      throw new Error(`unexpected client-stream response: ${JSON.stringify(reply)}`);
    }
  }

  async function bidiStreaming(client) {
    const call = await client.bidiHello();
    for (let index = 0; index < StreamMessageCount; index += 1) {
      await call.send({ name: RequestName });
    }
    await call.closeSend();

    let count = 0;
    for (;;) {
      const reply = await call.recv();
      if (reply === undefined) {
        break;
      }
      if (reply.message !== RequestName) {
        throw new Error(`unexpected bidi response: ${JSON.stringify(reply)}`);
      }
      count += 1;
    }
    if (count !== StreamMessageCount) {
      throw new Error(`expected ${StreamMessageCount} bidi responses, got ${count}`);
    }
  }

  async function bidiLongLived(client, messageCount) {
    const call = await client.bidiHello(LongLivedStreamOptions);
    let received = 0;
    await Promise.all([sendMessages(), recvMessages()]);

    async function sendMessages() {
      for (let index = 0; index < messageCount; index += 1) {
        await call.send({ name: RequestName });
      }
      await call.closeSend();
    }

    async function recvMessages() {
      for (;;) {
        const reply = await call.recv();
        if (reply === undefined) {
          break;
        }
        if (reply.message !== RequestName) {
          throw new Error(`unexpected long-lived bidi response: ${JSON.stringify(reply)}`);
        }
        received += 1;
      }
      if (received !== messageCount) {
        throw new Error(`expected ${messageCount} long-lived bidi responses, got ${received}`);
      }
    }
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

function requiredArg(index, name) {
  const value = process.argv[index];
  if (value == null || value === "") {
    throw new Error(
      `usage: node trevrpc-js/bench/webtransport_browser.js <page-url> <webtransport-url> <cert-file> <iterations>; missing ${name}`,
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
