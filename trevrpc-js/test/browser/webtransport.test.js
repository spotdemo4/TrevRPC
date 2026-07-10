import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { chromium, firefox, webkit } from "playwright";

const jsRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const repoRoot = resolve(jsRoot, "..");
const goRoot = join(repoRoot, "trevrpc-go");
const rustRoot = join(repoRoot, "trevrpc-rust");
const browserTypes = { chromium, firefox, webkit };
const selectedBrowserName = (process.env.TREVRPC_BROWSER ?? "chromium").toLowerCase();
const stableWebTransportBrowsers = new Set(["chromium"]);
if (browserTypes[selectedBrowserName] == null) {
  throw new Error(`unsupported TREVRPC_BROWSER ${JSON.stringify(selectedBrowserName)}`);
}
const browserSkipReason = browserCoverageSkipReason(selectedBrowserName);

browserTest(
  "browser WebTransport client calls Go greeter example",
  { timeout: 120_000 },
  async () => {
    await runBrowserGreeterScenario({
      server: servers.go,
      name: "Go Playwright",
      assertOutput(output) {
        assert.match(output, /SayHello: hello Go Playwright/);
        assert.match(output, /LotsOfReplies: hello Go Playwright/);
        assert.match(output, /LotsOfReplies: welcome to TrevRPC over QUIC/);
        assert.match(
          output,
          /LotsOfGreetings: hello, Go Playwright client stream 1, Go Playwright client stream 2/,
        );
        assert.match(output, /BidiHello: stream hello, Go Playwright bidi 1/);
        assert.match(output, /BidiHello: stream hello, Go Playwright bidi 2/);
      },
    });
  },
);

browserTest(
  "browser WebTransport client calls Rust greeter example",
  { timeout: 120_000 },
  async () => {
    await runBrowserGreeterScenario({
      server: servers.rust,
      name: "Rust Playwright",
      assertOutput(output) {
        assert.match(output, /SayHello: hello, Rust Playwright/);
        assert.match(output, /LotsOfReplies: hello, Rust Playwright/);
        assert.match(output, /LotsOfReplies: hello again, Rust Playwright/);
        assert.match(output, /LotsOfReplies: goodbye, Rust Playwright/);
        assert.match(
          output,
          /LotsOfGreetings: hello, Rust Playwright client stream 1, Rust Playwright client stream 2/,
        );
        assert.match(output, /BidiHello: stream hello, Rust Playwright bidi 1/);
        assert.match(output, /BidiHello: stream hello, Rust Playwright bidi 2/);
      },
    });
  },
);

browserTest("browser WebTransport rejects an unexpected path", { timeout: 120_000 }, async () => {
  await runBrowserGreeterScenario({
    server: servers.go,
    path: "/wrong",
    expectError: true,
  });
});

browserTest("browser WebTransport rejects an unexpected origin", { timeout: 120_000 }, async () => {
  await runBrowserGreeterScenario({
    server: servers.go,
    allowedOrigin: "http://127.0.0.1:1",
    expectError: true,
  });
});

browserTest(
  "browser WebTransport rejects an unexpected authority",
  { timeout: 120_000 },
  async () => {
    await runBrowserGreeterScenario({
      server: servers.rust,
      authorities: ["expected.example"],
      expectError: true,
    });
  },
);

const lifecycleCases = [
  {
    name: "terminal OK completes while upload is pending",
    scenario: "early-ok",
    assertResult(result) {
      assert.deepEqual(result, { value: "early ok" });
    },
  },
  {
    name: "terminal error wins over pending upload",
    scenario: "early-error",
    assertResult(result) {
      assert.equal(result.code, 7);
      assert.equal(result.message, "remote rejected upload");
    },
  },
  {
    name: "local abort cancels pending response read",
    eventPattern: /EVENT pending_cancelled/,
    scenario: "local-abort",
    assertResult(result) {
      assert.equal(result.code, 1);
    },
  },
  {
    name: "response stream return cancels server work",
    eventPattern: /EVENT response_stream_closed/,
    scenario: "early-return",
    assertResult(result) {
      assert.deepEqual(result, { first: "first" });
    },
  },
  {
    name: "long server stream preserves count and order",
    scenario: "long-server-stream",
    assertResult(result) {
      assert.deepEqual(result.values, expectedSequence("reply", 256));
    },
  },
  {
    name: "long bidi stream preserves count and order",
    scenario: "long-bidi",
    assertResult(result) {
      assert.deepEqual(result.values, expectedSequence("echo", 256));
    },
  },
  {
    name: "surfaces terminal error after streamed messages",
    scenario: "error-after-messages",
    assertResult(result) {
      assert.deepEqual(result.values, expectedSequence("before-error", 32));
      assert.equal(result.code, 7);
      assert.equal(result.message, "stream failed after messages");
    },
  },
];

for (const lifecycleCase of lifecycleCases) {
  browserTest(`browser WebTransport ${lifecycleCase.name}`, { timeout: 120_000 }, async () => {
    await runBrowserLifecycleScenario(lifecycleCase);
  });
}

browserTest(
  "browser WebTransport Rust lifecycle server matches Go lifecycle scenarios",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleSuite({
      cases: lifecycleCases,
      server: servers.lifecycleRust,
    });
  },
);

browserTest(
  "browser WebTransport page close cancels open response stream",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleTeardownScenario({
      action: async ({ page }) => {
        await page.close();
      },
    });
  },
);

browserTest(
  "browser WebTransport navigation cancels open response stream",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleTeardownScenario({
      action: async ({ page }) => {
        await page.goto("about:blank", { waitUntil: "domcontentloaded" });
      },
    });
  },
);

browserTest(
  "browser WebTransport context close cancels open response stream",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleTeardownScenario({
      action: async ({ page }) => {
        await page.context().close();
      },
    });
  },
);

browserTest(
  "browser WebTransport stream-open abort cancels pending stream creation",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleScenario({
      scenario: "stream-open-abort",
      assertResult(result) {
        assert.equal(result.code, 1);
      },
    });
  },
);

browserTest(
  "browser WebTransport Go server shutdown mid-stream fails active response stream",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleServerShutdownScenario({ server: servers.lifecycle });
  },
);

browserTest(
  "browser WebTransport Rust server shutdown mid-stream fails active response stream",
  { timeout: 120_000 },
  async () => {
    await runBrowserLifecycleServerShutdownScenario({ server: servers.lifecycleRust });
  },
);

if (envFlag("TREVRPC_BROWSER_SOAK")) {
  browserTest(
    "browser WebTransport lifecycle soak across Go and Rust servers",
    { timeout: positiveEnv("TREVRPC_BROWSER_SOAK_TIMEOUT_MS", 1_800_000) },
    async () => {
      const iterations = positiveEnv("TREVRPC_BROWSER_SOAK_ITERATIONS", 10);
      for (let iteration = 0; iteration < iterations; iteration += 1) {
        await runBrowserLifecycleSuite({ cases: lifecycleCases, server: servers.lifecycle });
        await runBrowserLifecycleSuite({ cases: lifecycleCases, server: servers.lifecycleRust });
        await runBrowserLifecycleServerShutdownScenario({ server: servers.lifecycle });
        await runBrowserLifecycleServerShutdownScenario({ server: servers.lifecycleRust });
      }
    },
  );
}

const servers = {
  go: {
    readyPattern: /WebTransport URL: https:\/\/[^\s]+\/trevrpc/,
    certificatePattern: /wrote client trust certificate/,
    spawn({ addr, origin, certPath }) {
      const serverPath = process.env.TREVRPC_BROWSER_GO_SERVER;
      const options = {
        cwd: serverPath == null || serverPath === "" ? goRoot : repoRoot,
        env: serverEnv({ addr, origin, certPath }),
      };
      if (serverPath != null && serverPath !== "") {
        return spawnManaged(serverPath, [], options);
      }

      return spawnManaged("go", ["run", "./examples/greeter_server"], options);
    },
  },
  rust: {
    readyPattern: /TrevRPC greeter WebTransport server listening on https:\/\/[^\s]+\/trevrpc/,
    certificatePattern: /certificate written to/,
    spawn({ addr, authorities, origin, certPath }) {
      const serverPath = process.env.TREVRPC_BROWSER_RUST_SERVER;
      const options = {
        cwd: serverPath == null || serverPath === "" ? rustRoot : repoRoot,
        env: serverEnv({ addr, authorities, origin, certPath }),
      };
      if (serverPath != null && serverPath !== "") {
        return spawnManaged(serverPath, [addr], options);
      }

      return spawnManaged(
        "cargo",
        ["run", "--quiet", "--example", "greeter_server", "--", addr],
        options,
      );
    },
  },
  lifecycle: {
    readyPattern: /READY https:\/\/[^\s]+\/trevrpc/,
    certificatePattern: /certificate written to/,
    spawn({ addr, authorities, origin, certPath, maxStreams }) {
      const serverPath = process.env.TREVRPC_BROWSER_LIFECYCLE_GO_SERVER;
      const options = {
        cwd: serverPath == null || serverPath === "" ? goRoot : repoRoot,
        env: serverEnv({ addr, authorities, origin, certPath, maxStreams }),
      };
      if (serverPath != null && serverPath !== "") {
        return spawnManaged(serverPath, [], options);
      }

      return spawnManaged("go", ["run", "./cmd/trevrpc-browser-lifecycle-go"], options);
    },
  },
  lifecycleRust: {
    readyPattern: /READY https:\/\/[^\s]+\/trevrpc/,
    certificatePattern: /certificate written to/,
    spawn({ addr, authorities, origin, certPath, maxStreams }) {
      const serverPath = process.env.TREVRPC_BROWSER_LIFECYCLE_RUST_SERVER;
      const options = {
        cwd: serverPath == null || serverPath === "" ? rustRoot : repoRoot,
        env: serverEnv({ addr, authorities, origin, certPath, maxStreams }),
      };
      if (serverPath != null && serverPath !== "") {
        return spawnManaged(serverPath, [addr], options);
      }

      return spawnManaged(
        "cargo",
        ["run", "--quiet", "--example", "browser_lifecycle_server", "--", addr],
        options,
      );
    },
  },
};

async function runBrowserLifecycleScenario({
  assertResult,
  eventPattern,
  scenario,
  server = servers.lifecycle,
  serverOptions,
}) {
  await runBrowserLifecycleSuite({
    cases: [{ assertResult, eventPattern, scenario }],
    server,
    serverOptions,
  });
}

async function runBrowserLifecycleSuite({ cases, server = servers.lifecycle, serverOptions }) {
  await withBrowserLifecycleEnvironment(
    { server, serverOptions },
    async ({ page, rpcServer, webTransportURL }) => {
      for (const lifecycleCase of cases) {
        const result = await page.evaluate(runLifecycleBrowserScenario, {
          scenario: lifecycleCase.scenario,
          webTransportURL,
        });
        lifecycleCase.assertResult(result);
        if (lifecycleCase.eventPattern != null) {
          await waitForOutput(rpcServer, lifecycleCase.eventPattern, 10_000);
        }
      }
    },
  );
}

async function runBrowserLifecycleTeardownScenario({ action, server = servers.lifecycle }) {
  await withBrowserLifecycleEnvironment(
    { server },
    async ({ browser, page, rpcServer, webTransportURL }) => {
      const result = await page.evaluate(runLifecycleBrowserScenario, {
        scenario: "hold-first-then-pending",
        webTransportURL,
      });
      assert.deepEqual(result, { first: "first" });

      await action({ browser, page });
      await waitForOutput(rpcServer, /EVENT response_stream_closed/, 10_000);
    },
  );
}

async function runBrowserLifecycleServerShutdownScenario({ server }) {
  await runBrowserLifecycleScenario({
    scenario: "server-shutdown-after-first",
    server,
    assertResult(result) {
      assert.equal(result.first, "first");
      assertShutdownStatus(result);
    },
  });
}

async function withBrowserLifecycleEnvironment(
  { server = servers.lifecycle, serverOptions = {} },
  run,
) {
  const tempDir = await mkdtemp(join(tmpdir(), "trevrpc-browser-"));
  const serverPort = await freePort();
  const staticPort = await freePort();
  const staticOrigin = `http://127.0.0.1:${staticPort}`;
  const webTransportURL = `https://127.0.0.1:${serverPort}/trevrpc`;
  const staticURL = `${staticOrigin}/examples/greeter/`;
  const certPath = join(tempDir, "server.pem");
  const children = [];
  let browser;
  let page;
  let pageErrors = [];

  try {
    const rpcServer = server.spawn({
      addr: `127.0.0.1:${serverPort}`,
      origin: staticOrigin,
      certPath,
      ...serverOptions,
    });
    children.push(rpcServer);
    await waitForOutput(rpcServer, server.readyPattern, 60_000);
    await waitForOutput(rpcServer, server.certificatePattern, 10_000);

    const staticServer = spawnManaged(process.execPath, ["examples/greeter/static-server.js"], {
      cwd: jsRoot,
      env: {
        PORT: String(staticPort),
        TREVRPC_EXAMPLE_CERT: certPath,
      },
    });
    children.push(staticServer);
    await waitForOutput(staticServer, /serving trevrpc-js from http:\/\/127\.0\.0\.1:/, 10_000);

    browser = await launchBrowser();
    page = await browser.newPage();
    pageErrors = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") {
        pageErrors.push(message.text());
      }
    });

    await page.goto(staticURL, { waitUntil: "domcontentloaded" });
    await run({ browser, page, rpcServer, webTransportURL });
    assert.deepEqual(pageErrors, []);
  } catch (error) {
    console.error(error?.stack ?? error);
    console.error(pageErrors.join("\n"));
    for (const child of children) {
      console.error(child.output);
    }
    throw error;
  } finally {
    if (browser != null) {
      await closeBrowser(browser);
    }
    await Promise.all(children.map((child) => stopProcess(child)));
    await rm(tempDir, { force: true, recursive: true });
  }
}

async function runLifecycleBrowserScenario({ scenario, webTransportURL }) {
  const { bidirectionalStreaming, clientStreaming, connect, createRoot, serverStreaming } =
    await import("/src/index.js");
  const service = "browser.lifecycle.Lifecycle";
  const Message = createRoot({
    nested: {
      browser: {
        nested: {
          lifecycle: {
            nested: {
              Message: {
                fields: {
                  value: { type: "string", id: 1 },
                },
              },
            },
          },
        },
      },
    },
  }).lookupType("browser.lifecycle.Message");
  const transport = await connect(webTransportURL, await browserConnectOptions());
  const options = {
    metadata: { authorization: "Bearer trevrpc-example-token" },
    streamIdleTimeoutMs: 10_000,
    timeoutMs: 10_000,
  };
  let closeTransport = true;

  try {
    switch (scenario) {
      case "early-ok": {
        const call = await clientStreaming(
          transport,
          service,
          "EarlyOk",
          Message,
          Message,
          options,
        );
        const response = await call.closeAndRecv();
        return { value: response.value };
      }
      case "early-error": {
        const stream = await bidirectionalStreaming(
          transport,
          service,
          "EarlyError",
          Message,
          Message,
          options,
        );
        try {
          await stream.recv();
        } catch (error) {
          return { code: error.code, message: error.statusMessage };
        }
        throw new Error("expected terminal error");
      }
      case "local-abort": {
        const controller = new AbortController();
        const stream = await serverStreaming(
          transport,
          service,
          "Pending",
          Message,
          Message,
          {},
          { ...options, signal: controller.signal },
        );
        const next = stream[Symbol.asyncIterator]().next();
        setTimeout(() => controller.abort(new DOMException("user cancelled", "AbortError")), 50);
        try {
          await next;
        } catch (error) {
          return { code: error.code };
        }
        throw new Error("expected local cancellation");
      }
      case "early-return": {
        const stream = await serverStreaming(
          transport,
          service,
          "FirstThenPending",
          Message,
          Message,
          {},
          options,
        );
        const iterator = stream[Symbol.asyncIterator]();
        const first = await iterator.next();
        await iterator.return();
        return { first: first.value.value };
      }
      case "hold-first-then-pending": {
        const stream = await serverStreaming(
          transport,
          service,
          "FirstThenPending",
          Message,
          Message,
          {},
          options,
        );
        const iterator = stream[Symbol.asyncIterator]();
        const first = await iterator.next();
        if (first.done) {
          throw new Error("expected first response before holding stream");
        }

        globalThis.__trevrpcLifecycleHold = { iterator, transport };
        closeTransport = false;
        return { first: first.value.value };
      }
      case "long-server-stream": {
        const stream = await serverStreaming(
          transport,
          service,
          "LongReplies",
          Message,
          Message,
          {},
          options,
        );
        const values = [];
        for await (const response of stream) {
          values.push(response.value);
        }
        return { values };
      }
      case "long-bidi": {
        const stream = await bidirectionalStreaming(
          transport,
          service,
          "BidiEchoMany",
          Message,
          Message,
          options,
        );
        for await (const request of sequenceRequests("echo", 256)) {
          await stream.send(request);
        }
        await stream.closeSend();
        const values = [];
        for await (const response of stream) {
          values.push(response.value);
        }
        return { values };
      }
      case "error-after-messages": {
        const stream = await serverStreaming(
          transport,
          service,
          "ErrorAfterMessages",
          Message,
          Message,
          {},
          options,
        );
        const values = [];
        try {
          for await (const response of stream) {
            values.push(response.value);
          }
        } catch (error) {
          return { code: error.code, message: error.statusMessage, values };
        }
        throw new Error("expected terminal error after streamed messages");
      }
      case "server-shutdown-after-first": {
        const values = [];
        try {
          const stream = await serverStreaming(
            transport,
            service,
            "ShutdownAfterFirst",
            Message,
            Message,
            {},
            options,
          );
          for await (const response of stream) {
            values.push(response.value);
          }
        } catch (error) {
          return { code: error.code, first: values[0], message: error.statusMessage };
        }
        throw new Error("expected server shutdown to fail the stream");
      }
      case "stream-open-abort": {
        const originalOpen = transport.openBidirectionalStream.bind(transport);
        transport.openBidirectionalStream = async () => {
          const stream = await originalOpen();
          await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
          return stream;
        };
        const controller = new AbortController();
        const stream = serverStreaming(
          transport,
          service,
          "Pending",
          Message,
          Message,
          {},
          { ...options, signal: controller.signal },
        );
        setTimeout(() => controller.abort(new DOMException("user cancelled", "AbortError")), 50);
        try {
          await stream;
        } catch (error) {
          return { code: error.code, message: error.statusMessage };
        }
        throw new Error("expected pending stream open to be cancelled");
      }
      default:
        throw new Error(`unknown lifecycle scenario ${scenario}`);
    }
  } finally {
    if (closeTransport) {
      transport.close({ closeCode: 0, reason: "browser lifecycle scenario complete" });
    }
  }

  function sequenceRequests(prefix, count) {
    return Array.from({ length: count }, (_, index) => ({ value: sequenceValue(prefix, index) }));
  }

  function sequenceValue(prefix, index) {
    return `${prefix}-${String(index).padStart(3, "0")}`;
  }

  async function browserConnectOptions() {
    const response = await fetch("./certificate-hash.json", { cache: "no-store" });
    const config = await response.json();
    return {
      serverCertificateHashes: [
        {
          algorithm: "sha-256",
          value: base64Bytes(config.sha256Base64),
        },
      ],
    };
  }

  function base64Bytes(value) {
    const binary = atob(value);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  }
}

function expectedSequence(prefix, count) {
  return Array.from({ length: count }, (_, index) => `${prefix}-${String(index).padStart(3, "0")}`);
}

function assertShutdownStatus(result) {
  assert.ok(
    result.code === 1 || result.code === 14,
    `expected shutdown to surface Cancelled or Unavailable, got ${JSON.stringify(result)}`,
  );
}

async function runBrowserGreeterScenario({
  server,
  allowedOrigin,
  assertOutput = () => {},
  authorities,
  expectError = false,
  name = "Playwright",
  path = "/trevrpc",
}) {
  const tempDir = await mkdtemp(join(tmpdir(), "trevrpc-browser-"));
  const serverPort = await freePort();
  const staticPort = await freePort();
  const staticOrigin = `http://127.0.0.1:${staticPort}`;
  const webTransportURL = `https://127.0.0.1:${serverPort}${path}`;
  const staticURL = `${staticOrigin}/examples/greeter/`;
  const certPath = join(tempDir, "server.pem");
  const children = [];
  let browser;
  let page;
  let pageErrors = [];

  try {
    const rpcServer = server.spawn({
      addr: `127.0.0.1:${serverPort}`,
      authorities,
      origin: allowedOrigin ?? staticOrigin,
      certPath,
    });
    children.push(rpcServer);
    await waitForOutput(rpcServer, server.readyPattern, 60_000);
    await waitForOutput(rpcServer, server.certificatePattern, 10_000);

    const staticServer = spawnManaged(process.execPath, ["examples/greeter/static-server.js"], {
      cwd: jsRoot,
      env: {
        PORT: String(staticPort),
        TREVRPC_EXAMPLE_CERT: certPath,
      },
    });
    children.push(staticServer);
    await waitForOutput(staticServer, /serving trevrpc-js from http:\/\/127\.0\.0\.1:/, 10_000);

    browser = await launchBrowser();
    page = await browser.newPage();
    pageErrors = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") {
        pageErrors.push(message.text());
      }
    });

    await page.goto(staticURL, { waitUntil: "domcontentloaded" });
    await page.fill('input[name="url"]', webTransportURL);
    await page.fill('input[name="name"]', name);
    await page.waitForFunction(
      () => document.querySelector('input[name="certificate-hash"]')?.value?.length > 0,
      null,
      { timeout: 10_000 },
    );
    await page.click("#submit");
    await page.waitForFunction(
      (shouldError) => {
        const output = document.querySelector("#output")?.textContent ?? "";
        return shouldError
          ? /(?:^|\n)(?:error|RPC error):/i.test(output)
          : output.includes("complete");
      },
      expectError,
      { timeout: 30_000 },
    );

    const output = await page.textContent("#output");
    if (expectError) {
      assert.match(output, /(?:^|\n)(?:error|RPC error):/i);
      assert.doesNotMatch(output, /(?:^|\n)complete\n?$/i);
      return;
    }

    assertOutput(output);
    assert.doesNotMatch(output, /(?:^|\n)(?:error|RPC error):/i);
    assert.deepEqual(pageErrors, []);
  } catch (error) {
    console.error(error?.stack ?? error);
    console.error(pageErrors.join("\n"));
    if (page != null) {
      console.error(await page.textContent("#output").catch(() => ""));
    }
    for (const child of children) {
      console.error(child.output);
    }
    throw error;
  } finally {
    if (browser != null) {
      await closeBrowser(browser);
    }
    await Promise.all(children.map((child) => stopProcess(child)));
    await rm(tempDir, { force: true, recursive: true });
  }
}

function browserTest(name, options, fn) {
  if (browserSkipReason == null) {
    test(name, options, fn);
    return;
  }

  test(name, { ...options, skip: browserSkipReason }, fn);
}

function browserCoverageSkipReason(browserName) {
  if (stableWebTransportBrowsers.has(browserName) || envFlag("TREVRPC_BROWSER_ALLOW_UNSTABLE")) {
    return null;
  }

  return `${browserName} WebTransport coverage is manual-only until non-Chromium browser support is stable enough`;
}

async function launchBrowser() {
  const launchOptions = {
    dumpio: process.env.TREVRPC_BROWSER_DUMPIO === "1",
  };
  if (selectedBrowserName === "chromium") {
    launchOptions.args = [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
    ];
    launchOptions.chromiumSandbox = false;
  }

  const executablePath = process.env[`TREVRPC_BROWSER_${selectedBrowserName.toUpperCase()}`];
  if (executablePath != null && executablePath !== "") {
    launchOptions.executablePath = executablePath;
  }

  return browserTypes[selectedBrowserName].launch(launchOptions);
}

async function closeBrowser(browser) {
  const closed = await Promise.race([
    browser.close().then(() => true),
    new Promise((resolvePromise) => setTimeout(() => resolvePromise(false), 5_000)),
  ]);
  if (!closed) {
    browser.process()?.kill("SIGKILL");
  }
}

function serverEnv({ addr, authorities, origin, certPath, maxStreams }) {
  const env = {
    TREVRPC_EXAMPLE_ADDR: addr,
    TREVRPC_EXAMPLE_ORIGIN: origin,
    TREVRPC_EXAMPLE_CERT: certPath,
  };
  if (authorities != null) {
    env.TREVRPC_EXAMPLE_AUTHORITIES = authorities.join(",");
  }
  if (maxStreams != null) {
    env.TREVRPC_EXAMPLE_MAX_STREAMS = String(maxStreams);
  }
  return env;
}

function spawnManaged(command, args, options) {
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: { ...process.env, ...options.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.output = "";
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", (chunk) => {
      child.output += chunk;
    });
  }
  return child;
}

function waitForOutput(child, pattern, timeoutMs) {
  return new Promise((resolvePromise, reject) => {
    const check = () => {
      const match = pattern.exec(child.output);
      if (match != null) {
        cleanup();
        resolvePromise(match);
      }
    };
    const onExit = (code, signal) => {
      cleanup();
      reject(new Error(`process exited before ${pattern}: ${code ?? signal}\n${child.output}`));
    };
    const onData = () => check();
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for ${pattern}\n${child.output}`));
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timeout);
      child.stdout.off("data", onData);
      child.stderr.off("data", onData);
      child.off("exit", onExit);
    };

    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.once("exit", onExit);
    check();
  });
}

async function stopProcess(child) {
  try {
    if (child.exitCode != null || child.signalCode != null) {
      return;
    }

    const exited = new Promise((resolvePromise) => child.once("exit", resolvePromise));
    if (!child.kill("SIGTERM")) {
      return;
    }
    const stopped = await Promise.race([
      exited.then(() => true),
      new Promise((resolvePromise) => setTimeout(() => resolvePromise(false), 5_000)),
    ]);
    if (!stopped && child.kill("SIGKILL")) {
      await exited;
    }
  } finally {
    child.stdout.destroy();
    child.stderr.destroy();
    child.removeAllListeners();
  }
}

function freePort() {
  return new Promise((resolvePromise, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close((error) => {
        if (error != null) {
          reject(error);
          return;
        }

        resolvePromise(address.port);
      });
    });
  });
}

function envFlag(name) {
  const value = process.env[name];
  return value === "1" || value === "true" || value === "TRUE" || value === "yes";
}

function positiveEnv(name, fallback) {
  const value = process.env[name] ?? String(fallback);
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`${name} must be a positive integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}
