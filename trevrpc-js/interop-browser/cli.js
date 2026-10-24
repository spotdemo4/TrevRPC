#!/usr/bin/env node
// trevrpc-interop-browser: runs the interop scenarios of interop/README.md in a real browser over
// WebTransport or QMux over WebSocket and prints `ok <name>` / `fail <name>: <reason>` per
// scenario. Exits 0 only if every scenario passed.
//
//   trevrpc-interop-browser --browser chromium|firefox|safari --url <url> --cert-hash <hex>
//                           [--transport webtransport|websocket|auto] [--ws-url <url>]
//                           [--expect-transport webtransport|websocket] [--dictionary <file>]
//                           [--page-url <url> --cert <pem> --key <pem> [--page-protocol h2|h3]]
//   trevrpc-interop-browser --write-page <dir>
//
// --dictionary makes the channel compress Echo messages with that zstd dictionary, the one the
// interop servers hold.
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
//
// --write-page writes the page (index.html and page.js) to a directory, for a TrevRPC server to
// serve through its HTTP fallback. --page-url then opens the page from that server instead, so the
// scenarios run from the server's own origin: the CLI passes its origin to the page in the `report`
// query parameter, and serves the input and results over HTTPS with the --cert and --key pair, so
// that the HTTPS page may reach it, and with CORS. --page-protocol has the browser load the page
// over HTTP/2 or HTTP/3, and fails every scenario unless the page and its script arrived over it.
// Chromium loads it over HTTP/3 when it trusts the certificate by its public key and is forced to
// use QUIC for the page's origin; it ignores Alt-Svc from a connection with certificate errors.
// Firefox does not use HTTP/3 with an overridden certificate, so it loads the page over HTTP/2
// only; Safari cannot be told either way.
import { X509Certificate, createHash } from "node:crypto";
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { chromium, firefox } from "playwright";

import { ScenarioNames } from "./names.js";
import { openSafari } from "./safari.js";

const Usage =
  "usage: trevrpc-interop-browser --browser chromium|firefox|safari --url <url> --cert-hash <hex>" +
  " [--transport webtransport|websocket|auto] [--ws-url <url>]" +
  " [--expect-transport webtransport|websocket] [--dictionary <file>]" +
  " [--page-url <url> --cert <pem> --key <pem> [--page-protocol h2|h3]]\n" +
  "       trevrpc-interop-browser --write-page <dir>";
const Transports = ["webtransport", "websocket", "auto"];
const LaunchTimeoutMs = 20_000;
const SetupTimeoutMs = 30_000;
const CloseTimeoutMs = 5_000;
/** Thirteen scenarios of at most ten seconds each, plus connecting, with slack. */
const RunTimeoutMs = 180_000;
const PageBundle = new URL("../build/interop/page.js", import.meta.url);
// The script path is relative, so the page also works from a TrevRPC server's web root.
const PageHtml =
  '<!doctype html><meta charset="utf-8"><title>trevrpc interop</title><script type="module" src="page.js"></script>';

const Browsers = {
  chromium: playwrightBrowser(
    chromium,
    "TREVRPC_BROWSER_CHROMIUM",
    {
      chromiumSandbox: false,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    },
    {
      // With certificate errors ignored, Chromium never acts on Alt-Svc.
      h2: () => ({}),
      h3: async (page) => ({
        args: [
          `--ignore-certificate-errors-spki-list=${await publicKeyHash(page.cert)}`,
          `--origin-to-force-quic-on=${new URL(page.url).host}`,
        ],
      }),
    },
  ),
  firefox: playwrightBrowser(
    firefox,
    "TREVRPC_BROWSER_FIREFOX",
    { firefoxUserPrefs: { "network.http.http3.enable": true } },
    { h2: () => ({ firefoxUserPrefs: { "network.http.altsvc.enabled": false } }) },
  ),
  safari: {
    name: "safari",
    open: (url) =>
      openSafari(url, { launchTimeoutMs: LaunchTimeoutMs, closeTimeoutMs: CloseTimeoutMs }),
  },
};

async function main() {
  const args = parseArguments();
  if (args.writePage !== undefined) {
    await writePage(args.writePage);
    return 0;
  }
  const { dictionaryFile, ...run } = args;
  const results = await readDictionary(dictionaryFile)
    .then((dictionary) => runInBrowser({ ...run, dictionary }))
    .catch((error) =>
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
        dictionary: { type: "string" },
        "page-url": { type: "string" },
        "page-protocol": { type: "string" },
        cert: { type: "string" },
        key: { type: "string" },
        "write-page": { type: "string" },
      },
      strict: true,
    }));
  } catch (error) {
    usageError(error.message);
  }
  if (values["write-page"] !== undefined) {
    if (Object.keys(values).some((name) => !["write-page", "transport"].includes(name))) {
      usageError("--write-page takes no other options");
    }
    return { writePage: values["write-page"] };
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
  const pageUrl = values["page-url"];
  if (pageUrl !== undefined && (!URL.canParse(pageUrl) || !values.cert || !values.key)) {
    usageError("--page-url must be an absolute URL and needs --cert and --key");
  }
  if (pageUrl === undefined && (values.cert || values.key)) {
    usageError("--cert and --key are only used with --page-url");
  }
  const pageProtocol = values["page-protocol"];
  if (pageProtocol !== undefined) {
    if (pageUrl === undefined) {
      usageError("--page-protocol is only used with --page-url");
    }
    if (!["h2", "h3"].includes(pageProtocol)) {
      usageError("--page-protocol must be h2 or h3");
    }
    if (browser.pageProtocols?.[pageProtocol] === undefined) {
      usageError(`${browser.name} cannot be made to load the page over ${pageProtocol}`);
    }
  }
  return {
    browser,
    page: pageUrl && { url: pageUrl, cert: values.cert, key: values.key, protocol: pageProtocol },
    pageProtocol,
    url: values.url,
    certHash: [...Buffer.from(certHash, "hex")],
    transport: values.transport,
    webSocketUrl: values["ws-url"],
    expectTransport: values["expect-transport"],
    dictionaryFile: values.dictionary,
  };
}

function usageError(message) {
  console.error(`trevrpc-interop-browser: ${message}\n${Usage}`);
  process.exit(2);
}

/** The dictionary in file, in base64 for the page, or undefined without one. */
async function readDictionary(file) {
  return file === undefined ? undefined : (await readFile(file)).toString("base64");
}

/** Writes the page to dir: index.html, and the page.js bundle it loads. */
async function writePage(dir) {
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "index.html"), PageHtml);
  await copyFile(PageBundle, join(dir, "page.js"));
}

/** Runs the page's scenarios, resolving with a result per scenario name that completed. */
async function runInBrowser({ browser, page, ...input }) {
  const origin = await serveInterop(input, page);
  const opening = browser.open(origin.pageUrl, page);
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

/**
 * A browser driven by Playwright: launched headless, with certificate errors ignored.
 * pageProtocols maps each HTTP version the browser can be made to load a server's page over to
 * the extra launch options that do it.
 */
function playwrightBrowser(type, executableVariable, launchOptions, pageProtocols) {
  return {
    name: type.name(),
    pageProtocols,
    async open(url, page) {
      const extra = page?.protocol ? await pageProtocols[page.protocol](page) : {};
      const instance = await type.launch({
        ...launchOptions,
        ...(extra.args && { args: [...(launchOptions.args ?? []), ...extra.args] }),
        ...(extra.firefoxUserPrefs && {
          firefoxUserPrefs: { ...launchOptions.firefoxUserPrefs, ...extra.firefoxUserPrefs },
        }),
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

/** The base64 SHA-256 of the certificate's public key, as Chromium's SPKI allowlist takes it. */
async function publicKeyHash(certFile) {
  const spki = new X509Certificate(await readFile(certFile)).publicKey.export({
    type: "spki",
    format: "der",
  });
  return createHash("sha256").update(spki).digest("base64");
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
 *
 * With page, the page comes from page.url instead, and the origin is HTTPS with page.cert and
 * page.key and answers cross-origin requests.
 */
async function serveInterop(input, page) {
  const results = {};
  let finish;
  const done = new Promise((resolve, reject) => {
    finish = ({ error }) => (error === undefined ? resolve() : reject(new Error(error)));
  });
  // Rejections surface through withTimeout in runInBrowser; this only keeps an early one handled.
  done.catch(() => {});

  const handler = async (request, response) => {
    const path = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    const route = `${request.method} ${path}`;
    if (page) {
      response.setHeader("access-control-allow-origin", new URL(page.url).origin);
    }
    try {
      if (page && request.method === "OPTIONS") {
        response
          .writeHead(204, {
            "access-control-allow-methods": "GET, POST",
            "access-control-allow-headers": "content-type",
          })
          .end();
      } else if (route === "GET /") {
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
  };
  const server = page
    ? createHttpsServer({ cert: await readFile(page.cert), key: await readFile(page.key) }, handler)
    : createHttpServer(handler);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const origin = `${page ? "https" : "http"}://127.0.0.1:${server.address().port}`;
  let pageUrl = `${origin}/`;
  if (page) {
    const url = new URL(page.url);
    url.searchParams.set("report", origin);
    pageUrl = url.href;
  }
  return {
    pageUrl,
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
