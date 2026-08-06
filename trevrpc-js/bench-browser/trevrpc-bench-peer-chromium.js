#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright-core";
import { RpcKinds, SchemaVersion, parseWorkloadOptions } from "trevrpc-bench-peer-js/common";

const Peer = "chromium";
const Stack = "trevrpc_webtransport";
const BrowserCloseTimeoutMs = 5_000;
const moduleDirectory = dirname(fileURLToPath(import.meta.url));

export class PeerError extends Error {
  constructor(phase, code, message, options = {}) {
    super(message, options);
    this.name = "PeerError";
    this.phase = phase;
    this.code = code;
  }
}

export async function main(argv = process.argv.slice(2), io = process, env = process.env) {
  const config = parseCommandLine(argv);
  if (config.command === "capabilities") {
    await writeEvent(io.stdout, {
      event: "capabilities",
      roles: { client: [Stack] },
      rpc_kinds: RpcKinds,
      histogram: "log_linear_v1",
    });
    return;
  }
  await runClient(config, io, env);
}

export function parseCommandLine(argv) {
  const [command, ...args] = argv;
  if (command === "capabilities") {
    if (args.length !== 0) {
      throw new PeerError("configure", "invalid_arguments", "capabilities takes no options");
    }
    return { command };
  }
  if (command !== "client") {
    throw new PeerError("configure", "invalid_arguments", usage());
  }

  const options = parseOptions(args, [
    "stack",
    "cert",
    "rpc",
    "concurrency",
    "warmup-ms",
    "measurement-ms",
    "request-bytes",
    "response-bytes",
    "messages-per-stream",
  ]);
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

async function runClient(config, io, env) {
  const executablePath = env.TREVRPC_BROWSER_CHROMIUM;
  if (executablePath == null || executablePath === "") {
    throw new PeerError(
      "configure",
      "missing_browser",
      "TREVRPC_BROWSER_CHROMIUM must name the Chromium executable",
    );
  }

  let originServer;
  let browser;
  let context;
  let page;
  const control = createInterface({ input: io.stdin, crlfDelay: Infinity });
  const lines = control[Symbol.asyncIterator]();
  const termination = terminationSignal();
  try {
    originServer = await startOriginServer();
    try {
      browser = await chromium.launch({
        executablePath,
        headless: true,
        chromiumSandbox: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-gpu",
        ],
      });
      context = await browser.newContext();
      page = await context.newPage();
      await page.goto(originServer.origin, { waitUntil: "domcontentloaded" });
    } catch (error) {
      throw wrapError("prepare", "browser_failed", error);
    }

    await writeEvent(io.stdout, {
      event: "prepared",
      origin: originServer.origin,
      pid: process.pid,
    });
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

    await writeEvent(io.stdout, { event: "armed", pid: process.pid });
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
    await writeEvent(io.stdout, sample);
  } finally {
    termination.cleanup();
    control.close();
    if (page != null) {
      await settleWithTimeout(
        page.evaluate(async () => (await import("/browser-workload.js")).close()),
        BrowserCloseTimeoutMs,
      );
    }
    await settleWithTimeout(context?.close(), BrowserCloseTimeoutMs);
    if (browser != null) {
      const closed = await settleWithTimeout(browser.close(), BrowserCloseTimeoutMs);
      if (!closed) {
        const browserProcess =
          typeof browser.process === "function" ? browser.process() : undefined;
        browserProcess?.kill("SIGKILL");
      }
    }
    await originServer?.close();
  }
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
        await writeFile(response, protobufBundle, "text/javascript; charset=utf-8");
        return;
      }
      if (pathname === "/bench-common.js") {
        await writeFile(response, commonPath, "text/javascript; charset=utf-8");
        return;
      }
      if (pathname === "/browser-workload.js") {
        await writeFile(response, browserWorkload, "text/javascript; charset=utf-8");
        return;
      }
      if (pathname.startsWith("/trevrpc-js/") && pathname.endsWith(".js")) {
        const relative = pathname.slice("/trevrpc-js/".length);
        if (relative.includes("..") || relative.includes("/")) {
          notFound(response);
          return;
        }
        await writeFile(
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

async function writeFile(response, path, contentType) {
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

function parseOptions(args, allowed) {
  const allowedOptions = new Set(allowed);
  const options = new Map();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (typeof flag !== "string" || !flag.startsWith("--") || value == null) {
      throw new PeerError("configure", "invalid_arguments", `invalid option list: ${usage()}`);
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

async function writeEvent(stdout, event) {
  const line = `${JSON.stringify({ schema_version: SchemaVersion, ...event, peer: Peer })}\n`;
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

function usage() {
  return "usage: trevrpc-bench-peer-chromium capabilities | client --stack trevrpc_webtransport --cert FILE --rpc KIND --concurrency N --warmup-ms N --measurement-ms N --request-bytes N --response-bytes N --messages-per-stream N";
}

function isMainModule() {
  return process.argv[1] != null && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
}

if (isMainModule()) {
  try {
    await main();
  } catch (error) {
    const peerError = wrapError("run", "peer_failed", error);
    console.error(`trevrpc-bench-peer-chromium: ${peerError.message}`);
    try {
      await writeEvent(process.stdout, {
        event: "error",
        phase: peerError.phase,
        code: peerError.code,
        message: peerError.message,
      });
    } catch (writeError) {
      console.error(
        `trevrpc-bench-peer-chromium: could not write error event: ${writeError.message}`,
      );
    }
    process.exitCode = 1;
  }
}
