// Drives a browser through a page this module serves: launches Chromium or Firefox through
// Playwright, or Safari through safaridriver, serves the page with its input from a loopback
// origin, and collects what the page posts back. The interop scenarios (cli.js) and the load
// benchmark (bench-cli.js) both run this way.
import { X509Certificate, createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";

import { chromium, firefox } from "playwright";

import { openSafari } from "./safari.js";

export const LaunchTimeoutMs = 20_000;
export const SetupTimeoutMs = 30_000;
export const CloseTimeoutMs = 5_000;

/** The page that loads bundle, a script beside it: the path is relative, so the page also works
 * from a TrevRPC server's web root. */
export function pageHtml(bundle) {
  return `<!doctype html><meta charset="utf-8"><title>trevrpc interop</title><script type="module" src="${bundle}"></script>`;
}

/**
 * The browsers by name. Each opens a URL with extra launch options ({ args, firefoxUserPrefs },
 * which only Playwright's browsers take) and resolves with a session to close.
 *
 * pageProtocols maps each HTTP version a browser can be made to load a server's page over to the
 * extra launch options that do it, given the page ({ url, cert }). forceQuic(host, cert), where a
 * browser has it, returns the options that make it reach host over HTTP/3 and trust cert there.
 */
export const Browsers = {
  chromium: {
    ...playwrightBrowser(chromium, "TREVRPC_BROWSER_CHROMIUM", {
      chromiumSandbox: false,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    }),
    pageProtocols: {
      // With certificate errors ignored, Chromium never acts on Alt-Svc.
      h2: () => ({}),
      h3: (page) => chromiumForceQuic(new URL(page.url).host, page.cert),
    },
    forceQuic: chromiumForceQuic,
  },
  firefox: {
    ...playwrightBrowser(firefox, "TREVRPC_BROWSER_FIREFOX", {
      firefoxUserPrefs: { "network.http.http3.enable": true },
    }),
    pageProtocols: {
      h2: () => ({ firefoxUserPrefs: { "network.http.altsvc.enabled": false } }),
    },
  },
  safari: {
    name: "safari",
    open: (url) =>
      openSafari(url, { launchTimeoutMs: LaunchTimeoutMs, closeTimeoutMs: CloseTimeoutMs }),
  },
};

/** Chromium trusts cert by its public key, and reaches host over QUIC without waiting for
 * Alt-Svc, which it ignores from a connection with certificate errors. */
async function chromiumForceQuic(host, cert) {
  return {
    args: [
      `--ignore-certificate-errors-spki-list=${await publicKeyHash(cert)}`,
      `--origin-to-force-quic-on=${host}`,
    ],
  };
}

/**
 * Opens a page that browser loads from a loopback origin serving html, the bundles it loads (each
 * path mapped to its file) and input, and resolves with what the page posts to /done once it
 * does. The page fails the run by posting { error }. extra are the browser's extra launch options.
 *
 * With page ({ url, cert, key }), the page comes from page.url instead, and the origin is HTTPS
 * with page.cert and page.key and answers cross-origin requests. isolated makes the page
 * cross-origin isolated, which sharpens its timers. setupTimeoutMs bounds opening the page.
 */
export async function runPage({
  browser,
  extra = {},
  page,
  html,
  bundles,
  input,
  isolated = false,
  routes = {},
  setupTimeoutMs = SetupTimeoutMs,
  runTimeoutMs,
  timeoutMessage,
}) {
  const origin = await serve({ input, page, html, bundles, isolated, routes });
  const opening = browser.open(origin.pageUrl, extra);
  try {
    await withTimeout(opening, setupTimeoutMs, `${browser.name} did not open the page in time`);
    return await withTimeout(origin.done, runTimeoutMs, timeoutMessage);
  } finally {
    // A browser that opens after its deadline still has to be closed.
    await withTimeout(
      opening.then((session) => session.close()),
      CloseTimeoutMs * 2,
      "close timed out",
    ).catch(() => {});
    await origin.close();
  }
}

/** A browser driven by Playwright: launched headless, with certificate errors ignored. */
function playwrightBrowser(type, executableVariable, launchOptions) {
  return {
    name: type.name(),
    async open(url, extra = {}) {
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
 * Serves html at /, the bundles and input at /input, and collects what the page posts: logs at
 * /log, a body for each of routes (a handler per path), and the final body at /done, which done
 * settles with. Loopback is a secure context for browsers.
 */
async function serve({ input, page, html, bundles, isolated, routes }) {
  let finish;
  const done = new Promise((resolve, reject) => {
    finish = (body) => (body.error === undefined ? resolve(body) : reject(new Error(body.error)));
  });
  // Rejections surface through withTimeout in runPage; this only keeps an early one handled.
  done.catch(() => {});

  // Cross-origin isolation: the page and its scripts opt in, and every cross-origin resource it
  // fetches must allow it, which CORS responses do.
  const isolation = isolated
    ? {
        "cross-origin-opener-policy": "same-origin",
        "cross-origin-embedder-policy": "require-corp",
      }
    : {};
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
        response
          .writeHead(200, { "content-type": "text/html; charset=utf-8", ...isolation })
          .end(html);
      } else if (request.method === "GET" && Object.hasOwn(bundles, path)) {
        const bundle = await readFile(bundles[path]);
        response
          .writeHead(200, { "content-type": "text/javascript; charset=utf-8", ...isolation })
          .end(bundle);
      } else if (route === "GET /input") {
        response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(input));
      } else if (route === "POST /log") {
        console.error(`[browser] ${(await readJson(request)).message}`);
        response.writeHead(204).end();
      } else if (route === "POST /done") {
        finish(await readJson(request));
        response.writeHead(204).end();
      } else if (request.method === "POST" && Object.hasOwn(routes, path)) {
        routes[path](await readJson(request));
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

export function withTimeout(promise, timeoutMs, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

export function describe(error) {
  return error?.message ?? String(error);
}
