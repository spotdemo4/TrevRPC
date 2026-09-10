import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  ChildExitTimeoutError,
  stopDetachedChildGroup,
  trackChild,
  waitForChild,
} from "./child-process-supervisor.js";

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
const fixtureTimeoutMs = 150_000;
const fixtureKillWaitMs = 10_000;
const integrationTimeoutMs = fixtureTimeoutMs + fixtureKillWaitMs + 5_000;

if (!available) {
  const unavailableReason = `missing addon or fixture: ${greeterFixturePath}`;
  const fixtureRequired = process.env.TREVRPC_REQUIRE_GREETER_FIXTURE === "1";
  test(
    "production ABI1 integration fixture",
    fixtureRequired ? {} : { skip: unavailableReason },
    () => assert.equal(fixtureRequired, false, unavailableReason),
  );
} else {
  test("production ABI1 integration fixture", { timeout: integrationTimeoutMs }, async () => {
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
      assert.deepEqual(
        { code: result.code, signal: result.signal, timedOut: result.timedOut },
        { code: 0, signal: null, timedOut: false },
        details,
      );
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

async function runFixture({ env }) {
  const fixtureEnvironment = { ...env };
  delete fixtureEnvironment.NODE_TEST_CONTEXT;
  const child = trackChild(
    spawn(
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
    ),
  );
  let stdout = "";
  let stderr = "";
  let timedOut = false;
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  let result;
  try {
    result = await waitForChild(child, fixtureTimeoutMs, "production integration fixture");
  } catch (error) {
    if (!(error instanceof ChildExitTimeoutError)) {
      await stopDetachedChildGroup(child, {
        graceMs: 0,
        killWaitMs: fixtureKillWaitMs,
        label: "production integration fixture after error",
      }).catch(() => {});
      throw error;
    }
    timedOut = true;
    result = await stopDetachedChildGroup(child, {
      graceMs: 0,
      killWaitMs: fixtureKillWaitMs,
      label: "production integration fixture",
    });
  }

  if (result.code !== 0 || result.signal !== null) {
    await stopDetachedChildGroup(child, {
      graceMs: 0,
      killWaitMs: fixtureKillWaitMs,
      label: "failed production integration fixture",
    });
  }

  let trace = "";
  try {
    trace = readFileSync(env.TREVRPC_NODE_TRACE_FILE, "utf8");
  } catch (error) {
    trace = `unable to read native trace: ${error.message}`;
  }
  return { ...result, stderr, stdout, timedOut, trace };
}
