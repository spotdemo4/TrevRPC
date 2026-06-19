import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const jsRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const repoRoot = resolve(jsRoot, "..");
const goRoot = join(repoRoot, "trevrpc-go");

test("browser WebTransport client calls Go greeter example", { timeout: 120_000 }, async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "trevrpc-browser-"));
  const goPort = await freePort();
  const staticPort = await freePort();
  const webTransportURL = `https://127.0.0.1:${goPort}/trevrpc`;
  const staticURL = `http://127.0.0.1:${staticPort}/examples/greeter/`;
  const certPath = join(tempDir, "server.pem");
  const children = [];
  let browser;
  let page;
  let pageErrors = [];

  try {
    const goServer = spawnGoServer({
      addr: `127.0.0.1:${goPort}`,
      origin: `http://127.0.0.1:${staticPort}`,
      certPath,
    });
    children.push(goServer);
    await waitForOutput(goServer, /WebTransport URL: https:\/\/[^\s]+\/trevrpc/, 60_000);
    await waitForOutput(goServer, /wrote client trust certificate/, 10_000);

    const staticServer = spawnManaged(process.execPath, ["examples/greeter/static-server.js"], {
      cwd: jsRoot,
      env: {
        PORT: String(staticPort),
        TREVRPC_EXAMPLE_CERT: certPath,
      },
    });
    children.push(staticServer);
    await waitForOutput(staticServer, /serving trevrpc-js from http:\/\/127\.0\.0\.1:/, 10_000);

    const launchOptions = {
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
      chromiumSandbox: false,
      dumpio: process.env.TREVRPC_BROWSER_DUMPIO === "1",
    };
    if (process.env.TREVRPC_BROWSER_CHROMIUM != null) {
      launchOptions.executablePath = process.env.TREVRPC_BROWSER_CHROMIUM;
    }
    browser = await chromium.launch(launchOptions);
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
    await page.fill('input[name="name"]', "Playwright");
    await page.waitForFunction(
      () => document.querySelector('input[name="certificate-hash"]')?.value?.length > 0,
      null,
      { timeout: 10_000 },
    );
    await page.click("#submit");
    await page.waitForFunction(
      () => document.querySelector("#output")?.textContent?.includes("complete"),
      null,
      { timeout: 30_000 },
    );

    const output = await page.textContent("#output");
    assert.match(output, /SayHello: hello Playwright/);
    assert.match(output, /LotsOfReplies: hello Playwright/);
    assert.match(output, /LotsOfReplies: welcome to TrevRPC over QUIC/);
    assert.match(
      output,
      /LotsOfGreetings: hello, Playwright client stream 1, Playwright client stream 2/,
    );
    assert.match(output, /BidiHello: stream hello, Playwright bidi 1/);
    assert.match(output, /BidiHello: stream hello, Playwright bidi 2/);
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
});

async function closeBrowser(browser) {
  const closed = await Promise.race([
    browser.close().then(() => true),
    new Promise((resolvePromise) => setTimeout(() => resolvePromise(false), 5_000)),
  ]);
  if (!closed) {
    browser.process()?.kill("SIGKILL");
  }
}

function spawnGoServer({ addr, origin, certPath }) {
  const serverPath = process.env.TREVRPC_BROWSER_GO_SERVER;
  if (serverPath != null && serverPath !== "") {
    return spawnManaged(serverPath, [], {
      cwd: repoRoot,
      env: goServerEnv({ addr, origin, certPath }),
    });
  }

  return spawnManaged("go", ["run", "./examples/greeter_server"], {
    cwd: goRoot,
    env: goServerEnv({ addr, origin, certPath }),
  });
}

function goServerEnv({ addr, origin, certPath }) {
  return {
    TREVRPC_EXAMPLE_ADDR: addr,
    TREVRPC_EXAMPLE_ORIGIN: origin,
    TREVRPC_EXAMPLE_CERT: certPath,
  };
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
