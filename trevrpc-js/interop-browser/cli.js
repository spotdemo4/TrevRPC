#!/usr/bin/env node
// trevrpc-interop-browser: runs the interop scenarios of interop/README.md in a real browser over
// WebTransport and prints `ok <name>` / `fail <name>: <reason>` per scenario. Exits 0 only if
// every scenario passed.
//
//   trevrpc-interop-browser --browser chromium|firefox|webkit --url <url> --cert-hash <hex>
//
// --cert-hash is the SHA-256 of the server certificate's DER encoding in hex, trusted through
// WebTransport serverCertificateHashes. Browsers come from PLAYWRIGHT_BROWSERS_PATH unless
// TREVRPC_BROWSER_CHROMIUM, TREVRPC_BROWSER_FIREFOX or TREVRPC_BROWSER_WEBKIT name an executable.
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { parseArgs } from "node:util";

import { chromium, firefox, webkit } from "playwright";

import { ScenarioNames } from "./names.js";

const Usage =
  "usage: trevrpc-interop-browser --browser chromium|firefox|webkit --url <url> --cert-hash <hex>";
const LaunchTimeoutMs = 20_000;
const SetupTimeoutMs = 30_000;
const CloseTimeoutMs = 5_000;
/** Nine scenarios of at most ten seconds each, plus connecting, with slack. */
const RunTimeoutMs = 120_000;
const PageBundle = new URL("../build/interop/page.js", import.meta.url);
const PageHtml =
  '<!doctype html><meta charset="utf-8"><title>trevrpc interop</title><script type="module" src="/page.js"></script>';

const Browsers = {
  chromium: {
    name: "chromium",
    type: chromium,
    executableVariable: "TREVRPC_BROWSER_CHROMIUM",
    launchOptions: {
      chromiumSandbox: false,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    },
  },
  firefox: {
    name: "firefox",
    type: firefox,
    executableVariable: "TREVRPC_BROWSER_FIREFOX",
    launchOptions: { firefoxUserPrefs: { "network.http.http3.enable": true } },
    contextOptions: { ignoreHTTPSErrors: true },
  },
  // Playwright's WebKit is WebKitGTK/WPE on Linux, which has no WebTransport, and Cocoa WebKit on
  // macOS. Without WebTransport every scenario fails cleanly.
  webkit: {
    name: "webkit",
    type: webkit,
    executableVariable: "TREVRPC_BROWSER_WEBKIT",
    launchOptions: {},
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
      },
      strict: true,
    }));
  } catch (error) {
    usageError(error.message);
  }

  const browser = Browsers[values.browser];
  if (browser === undefined) {
    usageError("--browser must be chromium, firefox or webkit");
  }
  if (!values.url) {
    usageError("--url is required");
  }
  const certHash = values["cert-hash"] ?? "";
  if (!/^(?:[0-9a-fA-F]{2}){32}$/.test(certHash)) {
    usageError("--cert-hash must be the hex SHA-256 of the certificate DER (64 hex digits)");
  }
  return { browser, url: values.url, certHash: [...Buffer.from(certHash, "hex")] };
}

function usageError(message) {
  console.error(`trevrpc-interop-browser: ${message}\n${Usage}`);
  process.exit(2);
}

/** Runs the page's scenarios, resolving with a result per scenario name that completed. */
async function runInBrowser({ browser, url, certHash }) {
  const results = {};
  const origin = await serveBundle();
  let instance;
  try {
    const executablePath = process.env[browser.executableVariable] || browser.type.executablePath();
    instance = await browser.type.launch({
      ...browser.launchOptions,
      executablePath,
      headless: true,
      timeout: LaunchTimeoutMs,
    });
    const page = await withTimeout(
      openPage(instance, browser, origin.url, results),
      SetupTimeoutMs,
      `${browser.name} did not open the interop page in time`,
    );
    const run = page.evaluate(
      (input) =>
        globalThis.trevrpcInterop.run({ ...input, onResult: globalThis.trevrpcInteropResult }),
      { url, certHash },
    );
    await withTimeout(run, RunTimeoutMs, "scenarios did not finish in time");
  } catch (error) {
    // Scenarios that completed before the failure keep their results.
    for (const name of ScenarioNames) {
      results[name] ??= failure(describe(error));
    }
  } finally {
    await closeBrowser(instance);
    await origin.close();
  }
  return results;
}

async function openPage(instance, browser, url, results) {
  const page = await (await instance.newContext(browser.contextOptions)).newPage();
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning") {
      console.error(`[browser ${message.type()}] ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => console.error(`[browser pageerror] ${error.message}`));
  await page.exposeFunction("trevrpcInteropResult", ({ name, ok, reason }) => {
    results[name] = ok ? { ok } : failure(reason);
  });
  await page.goto(url, { waitUntil: "load" });
  return page;
}

/** Serves the bundled page from a loopback origin, which browsers treat as a secure context. */
async function serveBundle() {
  const server = createServer(async (request, response) => {
    const path = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    if (path === "/") {
      response.writeHead(200, { "content-type": "text/html; charset=utf-8" }).end(PageHtml);
    } else if (path === "/page.js") {
      try {
        const bundle = await readFile(PageBundle);
        response.writeHead(200, { "content-type": "text/javascript; charset=utf-8" }).end(bundle);
      } catch (error) {
        response.writeHead(500, { "content-type": "text/plain" }).end(describe(error));
      }
    } else {
      response.writeHead(path === "/favicon.ico" ? 204 : 404).end();
    }
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return {
    url: `http://127.0.0.1:${server.address().port}/`,
    close: () => new Promise((resolve) => (server.closeAllConnections(), server.close(resolve))),
  };
}

async function closeBrowser(instance) {
  if (instance === undefined) {
    return;
  }
  const closed = await withTimeout(instance.close(), CloseTimeoutMs, "close timed out").then(
    () => true,
    () => false,
  );
  if (!closed) {
    instance.process?.()?.kill("SIGKILL");
  }
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
