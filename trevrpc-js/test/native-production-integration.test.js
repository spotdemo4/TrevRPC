import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const addonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const greeterFixturePath =
  process.env.TREVRPC_GREETER_SERVER ??
  join(import.meta.dirname, "..", "..", "trevrpc-c", "build", "trevrpc_greeter_server");
const fixturePath = join(
  import.meta.dirname,
  "fixtures",
  "native-production-integration.fixture.test.js",
);
const available = existsSync(addonPath) && existsSync(greeterFixturePath);
const nativeTestHooksAvailable =
  existsSync(addonPath) && readFileSync(addonPath).includes(Buffer.from("trevrpc-node-test:"));
const requiredFixtureTests = [
  "production ABI1 WebTransport listener and client negotiate path and origin",
  "legacy listener selectors map to one canonical transport",
  "production ABI1 endpoint, calls, streams, cancellation, and teardown",
  "send and FIN admission failures reject deterministically",
  "connected idle endpoint keeps client.closed live until remote fixture closure",
];

if (!available) {
  const unavailableReason = `missing addon or fixture: ${greeterFixturePath}`;
  const fixtureRequired = process.env.TREVRPC_REQUIRE_GREETER_FIXTURE === "1";
  test(
    "production ABI1 integration fixture",
    fixtureRequired ? {} : { skip: unavailableReason },
    () => assert.equal(fixtureRequired, false, unavailableReason),
  );
} else {
  test("production ABI1 integration fixture", { timeout: 120_000 }, async () => {
    const temporaryDirectory = await mkdtemp(
      join(tmpdir(), "trevrpc-js-native-integration-parent-"),
    );
    const tracePath = join(temporaryDirectory, "native-lifetime.trace");
    writeFileSync(tracePath, "");
    try {
      const result = await runFixture({
        env: {
          ...process.env,
          TREVRPC_NODE_TRACE_FILE: tracePath,
        },
      });
      const details = [
        `fixture code: ${String(result.code)}`,
        `fixture signal: ${String(result.signal)}`,
        `fixture timed out: ${String(result.timedOut)}`,
        "fixture stdout:",
        result.stdout,
        "fixture stderr:",
        result.stderr,
        "full native trace:",
        result.trace,
      ].join("\n");
      assert.equal(result.timedOut, false, details);
      assert.equal(result.code, 0, details);
      assert.equal(result.signal, null, details);
      const expectedPasses = nativeTestHooksAvailable ? 5 : 4;
      const expectedSkips = nativeTestHooksAvailable ? 0 : 1;
      assert.match(
        result.stdout,
        new RegExp(`# pass ${expectedPasses}(?:\\r?\\n|$)`, "u"),
        details,
      );
      assert.match(result.stdout, /# fail 0(?:\r?\n|$)/u, details);
      assert.match(
        result.stdout,
        new RegExp(`# skipped ${expectedSkips}(?:\\r?\\n|$)`, "u"),
        details,
      );
      for (const name of requiredFixtureTests) assert.ok(result.stdout.includes(name), details);
    } finally {
      await rm(temporaryDirectory, { force: true, recursive: true });
    }
  });
}

function terminateFixture(child, signal) {
  try {
    if (process.platform !== "win32" && child.pid != null) process.kill(-child.pid, signal);
    else child.kill(signal);
  } catch {
    // The fixture or its process group has already exited.
  }
}

function runFixture({ env }) {
  return new Promise((resolve, reject) => {
    const fixtureEnvironment = { ...env };
    delete fixtureEnvironment.NODE_TEST_CONTEXT;
    const child = spawn(
      process.execPath,
      [
        "--test",
        "--test-isolation=none",
        "--test-concurrency=1",
        "--test-reporter=tap",
        fixturePath,
      ],
      {
        detached: process.platform !== "win32",
        env: fixtureEnvironment,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      timedOut = true;
      terminateFixture(child, "SIGKILL");
    }, 100_000);
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      terminateFixture(child, "SIGKILL");
      reject(error);
    });
    child.once("close", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (timedOut || code !== 0 || signal !== null) terminateFixture(child, "SIGKILL");
      let trace = "";
      try {
        trace = readFileSync(env.TREVRPC_NODE_TRACE_FILE, "utf8");
      } catch (error) {
        trace = `unable to read native trace: ${error.message}`;
      }
      resolve({ code, signal, stderr, stdout, timedOut, trace });
    });
  });
}
