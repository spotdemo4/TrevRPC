import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { chromium, firefox, webkit } from "playwright-core";
import { RpcKinds, SchemaVersion, parseWorkloadOptions } from "trevrpc-bench-peer-js/common";

const Stack = "trevrpc_webtransport";
const BrowserCloseTimeoutMs = 5_000;
const BrowserLaunchTimeoutMs = 20_000;
const OriginNavigationTimeoutMs = 5_000;
const moduleDirectory = dirname(fileURLToPath(import.meta.url));

const BrowserEngines = {
  chromium: chromium,
  firefox: firefox,
  webkit: webkit,
};

const PeerToEngine = {
  chromium: "chromium",
  firefox: "firefox",
  webkit: "webkit",
};

export function browserRoles(peerName, platform) {
  // Playwright uses WPE on Linux, where WebKit has no WebTransport network backend.
  if (peerName === "webkit" && platform === "linux") {
    return {};
  }
  return { client: [Stack] };
}

export class PeerError extends Error {
  constructor(phase, code, message, options = {}) {
    super(message, options);
    this.name = "PeerError";
    this.phase = phase;
    this.code = code;
  }
}

export function createPeerMain(peerName) {
  const engineName = PeerToEngine[peerName];
  if (engineName == null) {
    throw new Error(`unknown peer ${peerName}`);
  }
  return async function main(argv = process.argv.slice(2), io = process, env = process.env) {
    const config = parseCommandLine(argv, peerName);
    if (config.command === "capabilities") {
      await writeEvent(
        io.stdout,
        {
          event: "capabilities",
          roles: browserRoles(peerName, process.platform),
          rpc_kinds: RpcKinds,
          histogram: "log_linear_v1",
        },
        peerName,
      );
      return;
    }
    await runClient(config, io, env, peerName, engineName);
  };
}

export function parseCommandLine(argv, peerName = "chromium") {
  const [command, ...args] = argv;
  if (command === "capabilities") {
    if (args.length !== 0) {
      throw new PeerError("configure", "invalid_arguments", "capabilities takes no options");
    }
    return { command };
  }
  if (command !== "client") {
    throw new PeerError("configure", "invalid_arguments", usage(peerName));
  }

  const options = parseOptions(
    args,
    [
      "stack",
      "cert",
      "rpc",
      "concurrency",
      "warmup-ms",
      "measurement-ms",
      "request-bytes",
      "response-bytes",
      "messages-per-stream",
    ],
    peerName,
  );
  const stack = requiredOption(options, "stack");
  if (stack !== Stack) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--stack must be ${Stack}; got ${JSON.stringify(stack)}`,
    );
  }
  let workload;
  try {
    workload = parseWorkloadOptions(options, requiredOption, parseIntegerOption);
  } catch (error) {
    throw wrapError("configure", "invalid_arguments", error);
  }
  return {
    command,
    stack,
    cert: requiredOption(options, "cert"),
    ...workload,
  };
}

export function parseConnectCommand(line) {
  const match = /^CONNECT ([^\s]+)$/u.exec(line.trim());
  if (match == null) {
    throw new PeerError(
      "control",
      "invalid_command",
      `expected CONNECT HOST:PORT; got ${JSON.stringify(line.trim())}`,
    );
  }
  parseAddress(match[1]);
  return match[1];
}

async function runClient(config, io, env, peerName, engineName) {
  const browserType = BrowserEngines[engineName];
  if (browserType == null) {
    throw new PeerError("configure", "invalid_arguments", `unknown browser engine ${engineName}`);
  }

  let originServer;
  let browser;
  let context;
  let page;
  let trustArtifacts = null;
  const control = createInterface({ input: io.stdin, crlfDelay: Infinity });
  const lines = control[Symbol.asyncIterator]();
  const termination = terminationSignal();
  try {
    originServer = await startOriginServer();
    try {
      const launchResult = await launchBrowser(browserType, engineName, env, trustArtifacts);
      browser = launchResult.browser;
      context = launchResult.context;
      page = launchResult.page;
      // Keep trust cleanup handle from launchResult if any.
      if (launchResult.trustArtifacts != null) {
        trustArtifacts = launchResult.trustArtifacts;
      }
      await page.goto(originServer.origin, {
        waitUntil: "domcontentloaded",
        timeout: OriginNavigationTimeoutMs,
      });
    } catch (error) {
      throw wrapError("prepare", "browser_failed", error);
    }

    await writeEvent(
      io.stdout,
      {
        event: "prepared",
        origin: originServer.origin,
        pid: process.pid,
      },
      peerName,
    );
    const connectLine = await nextControlOrSignal(lines, termination.promise);
    if (connectLine == null || connectLine.trim() === "SHUTDOWN") {
      return;
    }
    const address = parseConnectCommand(connectLine);
    const certificateHash = await leafCertificateHash(config.cert);

    try {
      await page.evaluate(
        async (input) => (await import("/browser-workload.js")).connectAndPrepare(input),
        { address, certificateHash, config: workloadConfig(config) },
      );
    } catch (error) {
      throw wrapError("connect", "connect_failed", error);
    }

    await writeEvent(io.stdout, { event: "armed", pid: process.pid }, peerName);
    const command = await nextControlOrSignal(lines, termination.promise);
    if (command == null || command.trim() === "SHUTDOWN") {
      return;
    }
    if (command.trim() !== "START") {
      throw new PeerError(
        "control",
        "invalid_command",
        `expected START or SHUTDOWN; got ${JSON.stringify(command.trim())}`,
      );
    }

    let sample;
    try {
      sample = await page.evaluate(async () =>
        (await import("/browser-workload.js")).startMeasurement(),
      );
    } catch (error) {
      throw wrapError("measure", "rpc_failed", error);
    }
    await writeEvent(io.stdout, sample, peerName);
  } finally {
    termination.cleanup();
    control.close();
    if (page != null) {
      await settleWithTimeout(
        page.evaluate(async () => (await import("/browser-workload.js")).close()),
        BrowserCloseTimeoutMs,
      );
    }
    if (context != null) {
      // For persistent contexts, close is same as browser close.
      await settleWithTimeout(context.close(), BrowserCloseTimeoutMs);
    }
    if (browser != null) {
      const closed = await settleWithTimeout(browser.close(), BrowserCloseTimeoutMs);
      if (!closed) {
        const browserProcess =
          typeof browser.process === "function" ? browser.process() : undefined;
        browserProcess?.kill("SIGKILL");
      }
    }
    if (trustArtifacts != null) {
      try {
        await trustArtifacts.cleanup();
      } catch {
        // Ignore cleanup errors.
      }
    }
    await originServer?.close();
  }
}

export function webkitLaunchOptions(env) {
  const executablePath = env.TREVRPC_BROWSER_WEBKIT ?? "";
  const launchOptions = {
    headless: true,
    timeout: BrowserLaunchTimeoutMs,
  };
  if (executablePath !== "") {
    launchOptions.executablePath = executablePath;
  }
  return launchOptions;
}

export function webkitRuntimeDiagnostics(env) {
  const entries = [
    ["executable", env.TREVRPC_BROWSER_WEBKIT],
    ["browsers_path", env.PLAYWRIGHT_BROWSERS_PATH],
    ["launch_timeout_ms", BrowserLaunchTimeoutMs],
    ["EGL_PLATFORM", env.EGL_PLATFORM],
    ["WPE_FDO_HEADLESS", env.WPE_FDO_HEADLESS],
    ["LIBGL_ALWAYS_SOFTWARE", env.LIBGL_ALWAYS_SOFTWARE],
    ["LIBGL_DRIVERS_PATH", env.LIBGL_DRIVERS_PATH],
    ["__EGL_VENDOR_LIBRARY_FILENAMES", env.__EGL_VENDOR_LIBRARY_FILENAMES],
    ["GBM_BACKEND", env.GBM_BACKEND],
    ["GBM_BACKENDS_PATH", env.GBM_BACKENDS_PATH],
  ];
  return entries
    .map(([name, value]) => `${name}=${value == null || value === "" ? "<unset>" : value}`)
    .join(", ");
}

export function webkitLaunchError(error, env) {
  return new Error(
    `${error?.message ?? String(error)}\nWebKit runtime: ${webkitRuntimeDiagnostics(env)}`,
    { cause: error },
  );
}

async function launchBrowser(browserType, engineName, env, trustArtifacts) {
  if (engineName === "chromium") {
    const executablePath = env.TREVRPC_BROWSER_CHROMIUM;
    if (executablePath == null || executablePath === "") {
      // Fall back to PLAYWRIGHT_BROWSERS_PATH discovery; still require env for nix.
      // If not set, try to launch without explicit path and let playwright discover.
      // For backward compat, error if not set is disabled when PLAYWRIGHT_BROWSERS_PATH is set.
      if (env.PLAYWRIGHT_BROWSERS_PATH == null || env.PLAYWRIGHT_BROWSERS_PATH === "") {
        throw new PeerError(
          "configure",
          "missing_browser",
          "TREVRPC_BROWSER_CHROMIUM must name the Chromium executable",
        );
      }
    }
    const launchOptions = {
      headless: true,
      chromiumSandbox: false,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    };
    if (executablePath != null && executablePath !== "") {
      launchOptions.executablePath = executablePath;
    }
    const browser = await browserType.launch(launchOptions);
    const context = await browser.newContext();
    const page = await context.newPage();
    return { browser, context, page };
  }

  if (engineName === "firefox") {
    const executablePath = env.TREVRPC_BROWSER_FIREFOX ?? "";
    const launchOptions = {
      headless: true,
    };
    if (executablePath !== "") {
      launchOptions.executablePath = executablePath;
    }
    const browser = await browserType.launch({
      ...launchOptions,
      firefoxUserPrefs: {
        "network.http.http3.enable": true,
      },
    });
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    const page = await context.newPage();
    return { browser, context, page, trustArtifacts };
  }

  if (engineName === "webkit") {
    let browser;
    try {
      browser = await browserType.launch(webkitLaunchOptions(env));
    } catch (error) {
      throw webkitLaunchError(error, env);
    }
    const context = await browser.newContext();
    const page = await context.newPage();
    return { browser, context, page, trustArtifacts };
  }

  throw new PeerError("configure", "invalid_arguments", `unsupported engine ${engineName}`);
}

async function startOriginServer() {
  const trevrpcEntry = fileURLToPath(import.meta.resolve("@trevrpc/trevrpc-js"));
  const trevrpcSource = dirname(trevrpcEntry);
  const commonPath = fileURLToPath(import.meta.resolve("trevrpc-bench-peer-js/common"));
  const protobufBundle = resolve(trevrpcSource, "../node_modules/protobufjs/dist/protobuf.min.js");
  const browserWorkload = resolve(moduleDirectory, "browser-workload.js");
  const server = createServer(async (request, response) => {
    try {
      const pathname = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
      if (pathname === "/") {
        writeText(
          response,
          "text/html; charset=utf-8",
          `<!doctype html><meta charset="utf-8"><script type="importmap">${JSON.stringify({ imports: { protobufjs: "/vendor/protobufjs.js", "@trevrpc/trevrpc-js": "/trevrpc-js/index.js", "trevrpc-bench-peer-js/common": "/bench-common.js" } })}</script>`,
        );
        return;
      }
      if (pathname === "/vendor/protobufjs.js") {
        writeText(
          response,
          "text/javascript; charset=utf-8",
          `await new Promise((resolve, reject) => {
  const script = document.createElement("script");
  script.src = "/vendor/protobuf.min.js";
  script.onload = resolve;
  script.onerror = () => reject(new Error("failed to load protobuf.js"));
  document.head.append(script);
});
export default globalThis.protobuf;
export const protobuf = globalThis.protobuf;
`,
        );
        return;
      }
      if (pathname === "/vendor/protobuf.min.js") {
        await writeFileResponse(response, protobufBundle, "text/javascript; charset=utf-8");
        return;
      }
      if (pathname === "/bench-common.js") {
        await writeFileResponse(response, commonPath, "text/javascript; charset=utf-8");
        return;
      }
      if (pathname === "/browser-workload.js") {
        await writeFileResponse(response, browserWorkload, "text/javascript; charset=utf-8");
        return;
      }
      if (pathname.startsWith("/trevrpc-js/") && pathname.endsWith(".js")) {
        const relative = pathname.slice("/trevrpc-js/".length);
        if (relative.includes("..") || relative.includes("/")) {
          notFound(response);
          return;
        }
        await writeFileResponse(
          response,
          resolve(trevrpcSource, relative),
          "text/javascript; charset=utf-8",
        );
        return;
      }
      notFound(response);
    } catch (error) {
      response.writeHead(error?.code === "ENOENT" ? 404 : 500, {
        "content-type": "text/plain; charset=utf-8",
      });
      response.end(error?.message ?? String(error));
    }
  });
  await new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", rejectListen);
      resolveListen();
    });
  });
  const address = server.address();
  if (address == null || typeof address === "string") {
    server.close();
    throw new Error("origin server did not expose a TCP address");
  }
  return {
    origin: `http://127.0.0.1:${address.port}`,
    async close() {
      server.closeAllConnections();
      await new Promise((resolveClose) => server.close(() => resolveClose()));
    },
  };
}

async function writeFileResponse(response, path, contentType) {
  const body = await readFile(path);
  response.writeHead(200, { "content-type": contentType, "cache-control": "no-store" });
  response.end(body);
}

function writeText(response, contentType, body) {
  response.writeHead(200, { "content-type": contentType, "cache-control": "no-store" });
  response.end(body);
}

function notFound(response) {
  response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  response.end("not found");
}

async function leafCertificateHash(path) {
  const certificate = await readFile(path);
  const text = certificate.toString("utf8");
  const match = /-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\s]+)-----END CERTIFICATE-----/u.exec(
    text,
  );
  const der = match == null ? certificate : Buffer.from(match[1].replace(/\s/gu, ""), "base64");
  return createHash("sha256").update(der).digest("base64");
}

function workloadConfig(config) {
  return {
    rpcKind: config.rpcKind,
    concurrency: config.concurrency,
    warmupMs: config.warmupMs,
    measurementMs: config.measurementMs,
    requestBytes: config.requestBytes,
    responseBytes: config.responseBytes,
    messagesPerStream: config.messagesPerStream,
  };
}

function parseOptions(args, allowed, peerName = "chromium") {
  const allowedOptions = new Set(allowed);
  const options = new Map();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (typeof flag !== "string" || !flag.startsWith("--") || value == null) {
      throw new PeerError(
        "configure",
        "invalid_arguments",
        `invalid option list: ${usage(peerName)}`,
      );
    }
    const name = flag.slice(2);
    if (!allowedOptions.has(name)) {
      throw new PeerError("configure", "invalid_arguments", `unknown option ${flag}`);
    }
    if (options.has(name)) {
      throw new PeerError("configure", "invalid_arguments", `duplicate option ${flag}`);
    }
    options.set(name, value);
  }
  return options;
}

function requiredOption(options, name) {
  const value = options.get(name);
  if (value == null || value === "") {
    throw new PeerError("configure", "invalid_arguments", `missing required option --${name}`);
  }
  return value;
}

function parseIntegerOption(options, name, minimum, maximum) {
  const value = requiredOption(options, name);
  const number = Number(value);
  if (
    !/^(0|[1-9][0-9]*)$/u.test(value) ||
    !Number.isSafeInteger(number) ||
    number < minimum ||
    number > maximum
  ) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--${name} must be an integer from ${minimum} through ${maximum}; got ${JSON.stringify(value)}`,
    );
  }
  return number;
}

function parseAddress(value) {
  if (/^\[[^\]]+\]:[1-9][0-9]{0,4}$/u.test(value)) {
    const port = Number(value.slice(value.lastIndexOf(":") + 1));
    if (port <= 65_535) {
      return;
    }
  }
  const match = /^([^:\s]+):([1-9][0-9]{0,4})$/u.exec(value);
  if (match != null && Number(match[2]) <= 65_535) {
    return;
  }
  throw new PeerError(
    "control",
    "invalid_command",
    `CONNECT address must be HOST:PORT with a port from 1 through 65535; got ${JSON.stringify(value)}`,
  );
}

async function nextControlOrSignal(lines, signal) {
  for (;;) {
    const next = lines.next().then(({ done, value }) => {
      if (done) {
        throw new PeerError("control", "stdin_closed", "standard input closed before a command");
      }
      return value;
    });
    const result = await Promise.race([next, signal.then(() => null)]);
    if (result == null || result.trim() !== "") {
      return result;
    }
  }
}

function terminationSignal() {
  let resolveSignal;
  const promise = new Promise((resolvePromise) => {
    resolveSignal = resolvePromise;
  });
  const onSignal = () => resolveSignal();
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  return {
    promise,
    cleanup() {
      process.removeListener("SIGINT", onSignal);
      process.removeListener("SIGTERM", onSignal);
    },
  };
}

async function settleWithTimeout(promise, timeoutMs) {
  if (promise == null) {
    return true;
  }
  try {
    return await Promise.race([
      Promise.resolve(promise).then(
        () => true,
        () => true,
      ),
      new Promise((resolveTimeout) => setTimeout(() => resolveTimeout(false), timeoutMs)),
    ]);
  } catch {
    return true;
  }
}

async function writeEvent(stdout, event, peerName) {
  const line = `${JSON.stringify({ schema_version: SchemaVersion, ...event, peer: peerName })}\n`;
  await new Promise((resolveWrite, rejectWrite) => {
    stdout.write(line, (error) => (error == null ? resolveWrite() : rejectWrite(error)));
  });
}

function wrapError(phase, code, error) {
  if (error instanceof PeerError) {
    return error;
  }
  return new PeerError(phase, code, error?.message ?? String(error), { cause: error });
}

function usage(peerName) {
  const bin = `trevrpc-bench-peer-${peerName}`;
  return `usage: ${bin} capabilities | client --stack trevrpc_webtransport --cert FILE --rpc KIND --concurrency N --warmup-ms N --measurement-ms N --request-bytes N --response-bytes N --messages-per-stream N`;
}
