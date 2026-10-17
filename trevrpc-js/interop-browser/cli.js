#!/usr/bin/env node
// trevrpc-interop-browser: runs the interop scenarios of interop/README.md in a real browser over
// WebTransport or QMux over WebSocket and prints `ok <name>` / `fail <name>: <reason>` per
// scenario. Exits 0 only if every scenario passed.
//
//   trevrpc-interop-browser --browser chromium|firefox|safari --url <url> --cert-hash <hex>
//                           [--transport webtransport|websocket|auto] [--ws-url <url>]
//                           [--expect-transport webtransport|websocket]
//
// --cert-hash is the SHA-256 of the server certificate's DER encoding in hex, trusted through
// WebTransport serverCertificateHashes. WebSocket connections cannot pin a certificate hash, so the
// Chromium and Firefox contexts ignore certificate errors instead; Safari needs the certificate
// trusted by the system. Chromium and Firefox come from PLAYWRIGHT_BROWSERS_PATH unless
// TREVRPC_BROWSER_CHROMIUM or TREVRPC_BROWSER_FIREFOX name an executable; Safari is driven through
// safaridriver (see safari.js).
//
// The CLI serves the page from a loopback origin; the page fetches its input from there and posts
// each result back, so every browser driver only has to open it.
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { parseArgs } from "node:util";

import { chromium, firefox } from "playwright";

import { ScenarioNames } from "./names.js";
import { openSafari } from "./safari.js";

const Usage =
  "usage: trevrpc-interop-browser --browser chromium|firefox|safari --url <url> --cert-hash <hex>" +
  " [--transport webtransport|websocket|auto] [--ws-url <url>]" +
  " [--expect-transport webtransport|websocket]";
const Transports = ["webtransport", "websocket", "auto"];
const LaunchTimeoutMs = 20_000;
const SetupTimeoutMs = 30_000;
const CloseTimeoutMs = 5_000;
/** Nine scenarios of at most ten seconds each, plus connecting, with slack. */
const RunTimeoutMs = 120_000;
const PageBundle = new URL("../build/interop/page.js", import.meta.url);
const PageHtml =
  '<!doctype html><meta charset="utf-8"><title>trevrpc interop</title><script type="module" src="/page.js"></script>';

const Browsers = {
  chromium: playwrightBrowser(chromium, "TREVRPC_BROWSER_CHROMIUM", {
    chromiumSandbox: false,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
  }),
  firefox: playwrightBrowser(firefox, "TREVRPC_BROWSER_FIREFOX", {
    firefoxUserPrefs: { "network.http.http3.enable": true },
  }),
  safari: {
    name: "safari",
    open: (url) =>
      openSafari(url, { launchTimeoutMs: LaunchTimeoutMs, closeTimeoutMs: CloseTimeoutMs }),
  },
};

async function main() {
  const args = parseArguments();
  const results = await runInBrowser(args).catch((error) =>
    Object.fromEntries(ScenarioNames.map((name) => [name, failure(describe(error))])),
  );

  let passed = true;
  for (const name of ScenarioNames) {
    const result = results[name] ?? failure("did not complete");
    passed &&= result.ok;
    console.log(result.ok ? `ok ${name}` : `fail ${name}: ${result.reason}`);
  }
  return passed ? 0 : 1;
}

function parseArguments() {
  let values;
  try {
    ({ values } = parseArgs({
      options: {
        browser: { type: "string" },
        url: { type: "string" },
        "cert-hash": { type: "string" },
        transport: { type: "string", default: "webtransport" },
        "ws-url": { type: "string" },
        "expect-transport": { type: "string" },
      },
      strict: true,
    }));
  } catch (error) {
    usageError(error.message);
  }

  const browser = Browsers[values.browser];
  if (browser === undefined) {
    usageError("--browser must be chromium, firefox or safari");
  }
  if (!values.url) {
    usageError("--url is required");
  }
  const certHash = values["cert-hash"] ?? "";
  if (!/^(?:[0-9a-fA-F]{2}){32}$/.test(certHash)) {
    usageError("--cert-hash must be the hex SHA-256 of the certificate DER (64 hex digits)");
  }
  if (!Transports.includes(values.transport)) {
    usageError("--transport must be webtransport, websocket or auto");
  }
  return {
    browser,
    url: values.url,
    certHash: [...Buffer.from(certHash, "hex")],
    transport: values.transport,
    webSocketUrl: values["ws-url"],
    expectTransport: values["expect-transport"],
  };
}

function usageError(message) {
  console.error(`trevrpc-interop-browser: ${message}\n${Usage}`);
  process.exit(2);
}

/** Runs the page's scenarios, resolving with a result per scenario name that completed. */
async function runInBrowser({ browser, ...input }) {
  const origin = await serveInterop(input);
  const opening = browser.open(origin.url);
  try {
    await withTimeout(
      opening,
      SetupTimeoutMs,
      `${browser.name} did not open the interop page in time`,
    );
    await withTimeout(origin.done, RunTimeoutMs, "scenarios did not finish in time");
  } catch (error) {
    // Scenarios that completed before the failure keep their results.
    for (const name of ScenarioNames) {
      origin.results[name] ??= failure(describe(error));
    }
  } finally {
    // A browser that opens after its deadline still has to be closed.
    await withTimeout(
      opening.then((session) => session.close()),
      CloseTimeoutMs * 2,
      "close timed out",
    ).catch(() => {});
    await origin.close();
  }
  return origin.results;
}

/** A browser driven by Playwright: launched headless, with certificate errors ignored. */
function playwrightBrowser(type, executableVariable, launchOptions) {
  return {
    name: type.name(),
    async open(url) {
      const instance = await type.launch({
        ...launchOptions,
        executablePath: process.env[executableVariable] || type.executablePath(),
        headless: true,
        timeout: LaunchTimeoutMs,
      });
      const close = () => closePlaywright(instance);
      try {
        // The interop certificate is self-signed: WebTransport trusts it by hash, WebSocket cannot.
        const page = await (await instance.newContext({ ignoreHTTPSErrors: true })).newPage();
        page.on("console", (message) => {
          if (message.type() === "error" || message.type() === "warning") {
            console.error(`[browser ${message.type()}] ${message.text()}`);
          }
        });
        await page.goto(url, { waitUntil: "load" });
      } catch (error) {
        await close();
        throw error;
      }
      return { close };
    },
  };
}

async function closePlaywright(instance) {
  const closed = await withTimeout(instance.close(), CloseTimeoutMs, "close timed out").then(
    () => true,
    () => false,
  );
  if (!closed) {
    instance.process?.()?.kill("SIGKILL");
  }
}

/**
 * Serves the bundled page and its input from a loopback origin, which browsers treat as a secure
 * context, and collects the results the page posts back. done settles when the page finishes.
 */
async function serveInterop(input) {
  const results = {};
  let finish;
  const done = new Promise((resolve, reject) => {
    finish = ({ error }) => (error === undefined ? resolve() : reject(new Error(error)));
  });
  // Rejections surface through withTimeout in runInBrowser; this only keeps an early one handled.
  done.catch(() => {});

  const server = createServer(async (request, response) => {
    const path = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    const route = `${request.method} ${path}`;
    try {
      if (route === "GET /") {
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" }).end(PageHtml);
      } else if (route === "GET /page.js") {
        const bundle = await readFile(PageBundle);
        response.writeHead(200, { "content-type": "text/javascript; charset=utf-8" }).end(bundle);
      } else if (route === "GET /input") {
        response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(input));
      } else if (route === "POST /result") {
        const { name, ok, reason } = await readJson(request);
        results[name] = ok ? { ok } : failure(reason);
        response.writeHead(204).end();
      } else if (route === "POST /log") {
        console.error(`[browser] ${(await readJson(request)).message}`);
        response.writeHead(204).end();
      } else if (route === "POST /done") {
        finish(await readJson(request));
        response.writeHead(204).end();
      } else {
        response.writeHead(path === "/favicon.ico" ? 204 : 404).end();
      }
    } catch (error) {
      response.writeHead(500, { "content-type": "text/plain" }).end(describe(error));
    }
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return {
    url: `http://127.0.0.1:${server.address().port}/`,
    results,
    done,
    close: () => new Promise((resolve) => (server.closeAllConnections(), server.close(resolve))),
  };
}

async function readJson(request) {
  let body = "";
  for await (const chunk of request) {
    body += chunk;
  }
  return JSON.parse(body);
}

function withTimeout(promise, timeoutMs, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function failure(reason) {
  return { ok: false, reason: String(reason).replace(/\s+/g, " ").trim() };
}

function describe(error) {
  return error?.message ?? String(error);
}

process.exit(await main());
