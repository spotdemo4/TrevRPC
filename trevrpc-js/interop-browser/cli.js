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
//   trevrpc-interop-browser bench ...   (the load benchmark; see bench-cli.js)
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
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { Browsers, describe, pageHtml, runPage } from "./driver.js";
import { ScenarioNames } from "./names.js";

const Usage =
  "usage: trevrpc-interop-browser --browser chromium|firefox|safari --url <url> --cert-hash <hex>" +
  " [--transport webtransport|websocket|auto] [--ws-url <url>]" +
  " [--expect-transport webtransport|websocket] [--dictionary <file>]" +
  " [--page-url <url> --cert <pem> --key <pem> [--page-protocol h2|h3]]\n" +
  "       trevrpc-interop-browser --write-page <dir>\n" +
  "       trevrpc-interop-browser bench ...";
const Transports = ["webtransport", "websocket", "auto"];
/** Seventeen scenarios of at most ten seconds each, plus connecting, with slack. */
const RunTimeoutMs = 180_000;
const PageBundle = new URL("../build/interop/page.js", import.meta.url);
const PageHtml = pageHtml("page.js");

async function main() {
  if (process.argv[2] === "bench") {
    const { runBenchCommand } = await import("./bench-cli.js");
    return runBenchCommand(process.argv.slice(3));
  }
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
  const results = {};
  try {
    await runPage({
      browser,
      extra: page?.protocol ? await browser.pageProtocols[page.protocol](page) : {},
      page,
      html: PageHtml,
      bundles: { "/page.js": PageBundle },
      input,
      routes: {
        "/result": ({ name, ok, reason }) => {
          results[name] = ok ? { ok } : failure(reason);
        },
      },
      runTimeoutMs: RunTimeoutMs,
      timeoutMessage: "scenarios did not finish in time",
    });
  } catch (error) {
    // Scenarios that completed before the failure keep their results.
    for (const name of ScenarioNames) {
      results[name] ??= failure(describe(error));
    }
  }
  return results;
}

function failure(reason) {
  return { ok: false, reason: String(reason).replace(/\s+/g, " ").trim() };
}

process.exit(await main());
