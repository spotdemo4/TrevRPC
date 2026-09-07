import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import test from "node:test";
import { Worker } from "node:worker_threads";

const require = createRequire(import.meta.url);
const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const nativeSourcePath = join(import.meta.dirname, "..", "native", "trevrpc_node.c");
const nativeTestHooksAvailable =
  existsSync(nativeAddonPath) &&
  readFileSync(nativeAddonPath).includes(Buffer.from("trevrpc-node-test:"));

function nativeOrSkip(t) {
  if (!existsSync(nativeAddonPath)) {
    t.skip("focused native addon has not been built");
    return null;
  }
  return require(nativeAddonPath);
}

function runNativeChild(source, extraEnvironment = {}, args = []) {
  return spawnSync(process.execPath, [...args, "--input-type=commonjs", "-e", source], {
    encoding: "utf8",
    timeout: 10_000,
    env: { ...process.env, ...extraEnvironment },
  });
}

function traceLines(result) {
  return `${result.stderr ?? ""}`
    .split("\n")
    .filter((line) => line.startsWith("trevrpc-node-test:"))
    .map((line) => line.slice("trevrpc-node-test:".length));
}

function assertInOrder(values, expected) {
  let cursor = -1;
  for (const value of expected) {
    const next = values.indexOf(value, cursor + 1);
    assert.notEqual(next, -1, `missing ordered trace ${value}: ${values.join(",")}`);
    cursor = next;
  }
}

test("native addon import is process-liveness neutral", (t) => {
  if (!existsSync(nativeAddonPath)) {
    t.skip("focused native addon has not been built");
    return;
  }
  const result = runNativeChild(`require(${JSON.stringify(nativeAddonPath)})`);
  assert.equal(result.error, undefined, result.stderr);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.signal, null, result.stderr);
});

test("one environment runtime is shared by cancellation wrappers and Worker teardown is bounded", async (t) => {
  const native = nativeOrSkip(t);
  if (native == null) return;
  const first = native.createCancellation();
  const second = native.createCancellation();
  assert.equal(typeof first.cancel, "function");
  assert.equal(typeof second.cancel, "function");
  first.cancel();
  second.cancel();

  const worker = new Worker(
    `
      const { parentPort } = require("node:worker_threads");
      const native = require(${JSON.stringify(nativeAddonPath)});
      const first = native.createCancellation();
      const second = native.createCancellation();
      first.cancel();
      second.cancel();
      parentPort.postMessage("ready");
      setInterval(() => {}, 1000);
    `,
    { eval: true },
  );
  let terminationPromise;
  const terminateWorker = () => {
    if (terminationPromise == null) {
      let timeout;
      terminationPromise = Promise.race([
        worker.terminate(),
        new Promise((_, reject) => {
          timeout = setTimeout(() => reject(new Error("Worker cleanup did not settle")), 3_000);
        }),
      ]).finally(() => clearTimeout(timeout));
    }
    return terminationPromise;
  };
  try {
    await waitForWorkerReady(worker, 3_000);
    await terminateWorker();
  } finally {
    await terminateWorker().catch(() => {});
  }
});

test("test-hook traces prove drain, STOPPED, release, poll close, and free ordering", (t) => {
  if (!nativeTestHooksAvailable) {
    t.skip("native addon was built without lifecycle test hooks");
    return;
  }
  const result = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      const cancellation = native.createCancellation();
      cancellation.cancel();
      cancellation.cancel();
      setTimeout(() => {}, 30);
    `,
  );
  assert.equal(result.error, undefined, result.stderr);
  assert.equal(result.status, 0, result.stderr);
  assertInOrder(traceLines(result), [
    "poll-init",
    "poll-start",
    "close-submitted",
    "stopped",
    "release",
    "poll-closed",
    "free",
  ]);
  assert.ok(traceLines(result).includes("drain-eagain"));
});

test("construction and operation failure injections remain bounded and release in order", (t) => {
  if (!nativeTestHooksAvailable) {
    t.skip("native addon was built without lifecycle test hooks");
    return;
  }
  for (const failure of [
    "TREVRPC_NODE_FAIL_WAKE_SOURCE",
    "TREVRPC_NODE_FAIL_UV_POLL_INIT",
    "TREVRPC_NODE_FAIL_UV_POLL_START",
    "TREVRPC_NODE_FAIL_NAPI_INSTANCE_DATA",
    "TREVRPC_NODE_FAIL_NAPI_CLEANUP_HOOK",
    "TREVRPC_NODE_FAIL_POLL_ERROR",
    "TREVRPC_NODE_FAIL_INVALID_EVENT",
  ]) {
    const result = runNativeChild(
      `
        const native = require(${JSON.stringify(nativeAddonPath)});
        try { native.createCancellation(); } catch (error) { console.log(error.nativeCode); }
      `,
      { [failure]: "1" },
    );
    assert.equal(result.error, undefined, `${failure}: ${result.stderr}`);
    assert.equal(result.status, 0, `${failure}: ${result.stderr}`);
    assertInOrder(traceLines(result), ["close-submitted", "stopped", "release", "free"]);
  }

  const allocation = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      const cancellation = native.createCancellation();
      try { cancellation.cancel(); } catch (error) { console.log(error.nativeCode); }
    `,
    { TREVRPC_NODE_FAIL_OPERATION_ALLOCATION: "1" },
  );
  assert.equal(allocation.error, undefined, allocation.stderr);
  assert.equal(allocation.status, 0, allocation.stderr);
  assertInOrder(traceLines(allocation), ["close-submitted", "stopped", "release", "free"]);

  const closeFailure = runNativeChild(
    `require(${JSON.stringify(nativeAddonPath)}).createCancellation();`,
    { TREVRPC_NODE_FAIL_CLOSE_SUBMISSION: "1" },
  );
  assert.equal(closeFailure.error, undefined, closeFailure.stderr);
  assert.notEqual(closeFailure.status, 0, closeFailure.stderr);
  assert.ok(traceLines(closeFailure).includes("close-failed-bounded"));

  const transientCloseFailure = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      native.createCancellation();
      setTimeout(() => {}, 80);
    `,
    { TREVRPC_NODE_FAIL_CLOSE_SUBMISSION_ONCE: "1" },
  );
  assert.equal(transientCloseFailure.error, undefined, transientCloseFailure.stderr);
  assert.equal(transientCloseFailure.status, 0, transientCloseFailure.stderr);
  assertInOrder(traceLines(transientCloseFailure), [
    "close-submitted",
    "stopped",
    "release",
    "free",
  ]);

  const delayedCloseFailure = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      native.createCancellation();
    `,
    { TREVRPC_NODE_FAIL_CLOSE_SUBMISSION_FIVE: "1" },
  );
  assert.equal(delayedCloseFailure.error, undefined, delayedCloseFailure.stderr);
  assert.equal(delayedCloseFailure.status, 0, delayedCloseFailure.stderr);
  const delayedTraces = traceLines(delayedCloseFailure);
  assert.ok(
    delayedTraces.filter((event) => event === "close-admission-retry").length >= 5,
    delayedCloseFailure.stderr,
  );
  assertInOrder(delayedTraces, ["close-submitted", "stopped", "release", "free"]);
});

test("GC after cancellation completion releases the native cancellation handle", (t) => {
  if (!nativeTestHooksAvailable) {
    t.skip("native addon was built without lifecycle test hooks");
    return;
  }
  const result = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      let cancellation = native.createCancellation();
      cancellation.cancel();
      setTimeout(() => { cancellation = null; global.gc(); }, 50);
      setTimeout(() => {}, 120);
    `,
    {},
    ["--expose-gc"],
  );
  assert.equal(result.error, undefined, result.stderr);
  assert.equal(result.status, 0, result.stderr);
  assert.ok(traceLines(result).includes("cancellation-release"), result.stderr);
});

test("GC before cancellation releases an uncancelled native handle", (t) => {
  if (!nativeTestHooksAvailable) {
    t.skip("native addon was built without lifecycle test hooks");
    return;
  }
  const result = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      let cancellation = native.createCancellation();
      cancellation = null;
      global.gc();
      setTimeout(() => {}, 120);
    `,
    {},
    ["--expose-gc"],
  );
  assert.equal(result.error, undefined, result.stderr);
  assert.equal(result.status, 0, result.stderr);
  assert.ok(traceLines(result).includes("cancellation-release"), result.stderr);
});

test("EBUSY cancellation release is retried after terminal cleanup", (t) => {
  if (!nativeTestHooksAvailable) {
    t.skip("native addon was built without lifecycle test hooks");
    return;
  }
  const result = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      let cancellation = native.createCancellation();
      cancellation.cancel();
      setTimeout(() => { cancellation = null; global.gc(); }, 30);
      setTimeout(() => {}, 120);
    `,
    { TREVRPC_NODE_FAIL_CANCELLATION_RELEASE_BUSY: "1" },
    ["--expose-gc"],
  );
  assert.equal(result.error, undefined, result.stderr);
  assert.equal(result.status, 0, result.stderr);
  assert.ok(traceLines(result).includes("cancellation-release"), result.stderr);
});

test("persistent release retry eventually succeeds after delayed passes", (t) => {
  if (!nativeTestHooksAvailable) {
    t.skip("native addon was built without lifecycle test hooks");
    return;
  }
  const result = runNativeChild(
    `
      const native = require(${JSON.stringify(nativeAddonPath)});
      const cancellation = native.createCancellation();
      cancellation.cancel();
    `,
    { TREVRPC_NODE_FAIL_CANCELLATION_RELEASE_FIVE: "1" },
  );
  assert.equal(result.error, undefined, result.stderr);
  assert.equal(result.status, 0, result.stderr);
  const traces = traceLines(result);
  const passes = traces.filter((event) => event === "failure-progress-pass").length;
  const busyResponses = traces.filter((event) => event === "cancellation-release-busy").length;
  assert.ok(passes >= 2, result.stderr);
  assert.ok(busyResponses >= 5, result.stderr);
  assert.ok(traces.includes("cancellation-release"), result.stderr);
});

test("native source contains one embedded poll and typed full-key routing", async () => {
  const source = await (await import("node:fs/promises")).readFile(nativeSourcePath, "utf8");
  assert.equal((source.match(/uv_poll_t\s+poll\b/gu) ?? []).length, 1);
  assert.match(source, /uv_timer_t\s+failure_progress\b/u);
  assert.match(source, /node_runtime_start_failure_progress\(runtime\)/u);
  assert.match(source, /node_key_equal\(operation->subject, event_key\)/u);
  assert.match(source, /napi_set_instance_data\(runtime->env, NULL, NULL, NULL\)/u);
  assert.match(source, /napi_remove_async_cleanup_hook\(hook\)/u);
});

function waitForWorkerReady(worker, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timeout = setTimeout(() => finish(new Error("Worker did not become ready")), timeoutMs);
    const onMessage = (message) => {
      finish(
        message === "ready" ? null : new Error(`unexpected Worker message: ${String(message)}`),
      );
    };
    const onError = (error) => finish(error);
    const onExit = (code) => finish(new Error(`Worker exited before ready: ${code}`));
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
      if (error == null) resolve();
      else reject(error);
    };
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
  });
}
