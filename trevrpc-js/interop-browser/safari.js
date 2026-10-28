// Drives Safari through safaridriver over W3C WebDriver. Playwright's WebKit is not Safari: its
// builds decide on their own whether to expose WebTransport, so only Safari itself shows what Safari
// users get. Safari cannot be told to ignore certificate errors, so the interop certificate must be
// trusted by the system (run.py does this) and remote automation must be enabled once with
// `sudo safaridriver --enable`.
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";

const DefaultDriver = "/usr/bin/safaridriver";
const PollIntervalMs = 100;
const LaunchAttempts = 2;

/**
 * Starts safaridriver (TREVRPC_BROWSER_SAFARI, or the system one) within launchTimeoutMs and opens
 * url in a new Safari automation session, giving launching Safari and loading the page timeoutMs
 * each. Resolves with a handle whose close() ends the session and the driver.
 *
 * A driver that exits before it has a session is started once more: on CI runners safaridriver
 * occasionally exits right after a previous one closed, and one more launch costs far less than a
 * failed run.
 */
export async function openSafari(url, options) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await openSafariOnce(url, options);
    } catch (error) {
      if (!(error instanceof DriverExited) || attempt >= LaunchAttempts) {
        throw error;
      }
      console.error(`${error.message}; starting it again`);
    }
  }
}

/** safaridriver exited before it had a session. */
class DriverExited extends Error {}

async function openSafariOnce(url, { launchTimeoutMs, timeoutMs, closeTimeoutMs }) {
  const port = await freePort();
  // The driver may say why it fails on either stream; both go to stderr, which keeps this process's
  // stdout for its own results.
  const driver = spawn(process.env.TREVRPC_BROWSER_SAFARI || DefaultDriver, ["--port", port], {
    stdio: ["ignore", 2, 2],
  });
  let sessionId;
  const exited = once(driver, "exit").then(
    ([code, signal]) => {
      const message = `safaridriver exited (${signal ?? code})`;
      throw sessionId === undefined ? new DriverExited(message) : new Error(message);
    },
    (error) => {
      throw new Error(`cannot start safaridriver: ${error.message}`);
    },
  );
  // Keeps an early exit from going unhandled; every wait below races it.
  exited.catch(() => {});
  const base = `http://127.0.0.1:${port}`;

  const close = async () => {
    if (sessionId !== undefined) {
      await webDriver(base, "DELETE", `/session/${sessionId}`, undefined, closeTimeoutMs).catch(
        () => {},
      );
    }
    if (driver.exitCode === null && driver.signalCode === null) {
      driver.kill();
      await Promise.race([exited.catch(() => {}), sleep(closeTimeoutMs)]);
      driver.kill("SIGKILL");
    }
  };

  try {
    await Promise.race([waitForReady(base, launchTimeoutMs), exited]);
    ({ sessionId } = await Promise.race([
      webDriver(
        base,
        "POST",
        "/session",
        { capabilities: { alwaysMatch: { browserName: "safari" } } },
        timeoutMs,
      ),
      exited,
    ]));
    await Promise.race([
      webDriver(base, "POST", `/session/${sessionId}/url`, { url }, timeoutMs),
      exited,
    ]);
  } catch (error) {
    await close();
    throw error;
  }
  return { close };
}

async function waitForReady(base, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const ready = await webDriver(base, "GET", "/status", undefined, PollIntervalMs * 10).then(
      (status) => status?.ready === true,
      () => false,
    );
    if (ready) {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error("safaridriver did not become ready in time");
    }
    await sleep(PollIntervalMs);
  }
}

/** Sends one WebDriver command and resolves with its value, or rejects with the WebDriver error. */
async function webDriver(base, method, path, body, timeoutMs) {
  const init = { method, signal: AbortSignal.timeout(timeoutMs) };
  if (body !== undefined) {
    init.headers = { "content-type": "application/json" };
    init.body = JSON.stringify(body);
  }
  const response = await fetch(`${base}${path}`, init);
  const { value } = await response.json();
  if (!response.ok) {
    const reason = [value?.error, value?.message].filter(Boolean).join(": ");
    throw new Error(`safaridriver ${method} ${path}: ${reason || `HTTP ${response.status}`}`);
  }
  return value;
}

/** A loopback TCP port that was free a moment ago. */
async function freePort() {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return String(port);
}
