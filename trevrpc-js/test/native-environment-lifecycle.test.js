import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

const require = createRequire(import.meta.url);
const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const thisFile = fileURLToPath(import.meta.url);
const childMode = process.env.TREVRPC_NATIVE_ENVIRONMENT_LIFECYCLE_CHILD;
const childMarker = "native environment lifecycle child completed";
const lifecycleFields = [
  "allocations",
  "closes",
  "destroys",
  "tsfnFinalizers",
  "instanceFinalizers",
  "tsfnAcceptances",
  "envNullAbandons",
  "callbackEnvNullAbandons",
  "stoppingAbandons",
  "finalizerAbandons",
  "gatedAllocations",
  "gatedAbandons",
  "gatedFrees",
];

if (childMode == null) {
  const native = existsSync(nativeAddonPath) ? require(nativeAddonPath) : null;
  const nativeTestHooks =
    typeof native?._debugCompletionRuntimeStats === "function" &&
    typeof native?._debugQueueGatedCompletion === "function" &&
    typeof native?._debugCompletionGateReached === "function" &&
    typeof native?._debugReleaseCompletionGate === "function" &&
    typeof native?._debugCloseGatedCompletionRuntime === "function" &&
    typeof native?._debugBlockCompletionJs === "function" &&
    typeof native?._debugCompletionJsBlocked === "function" &&
    typeof native?._debugReleaseCompletionJs === "function" &&
    typeof native?._debugResetCompletionGate === "function";

  for (const [mode, name, timeout] of [
    ["idle", "idle native completion runtime tears down exactly once", 15_000],
    ["queued", "Worker termination abandons an accepted blocked completion exactly once", 30_000],
  ]) {
    test(name, { skip: !nativeTestHooks, timeout }, () => {
      const childEnvironment = {
        ...process.env,
        TREVRPC_NATIVE_ENVIRONMENT_LIFECYCLE_CHILD: mode,
      };
      delete childEnvironment.NODE_TEST_CONTEXT;
      const result = spawnSync(process.execPath, [thisFile], {
        encoding: "utf8",
        env: childEnvironment,
        timeout: timeout - 1_000,
      });
      const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;

      assert.equal(result.error, undefined, output);
      assert.equal(result.signal, null, output);
      assert.equal(result.status, 0, output);
      assert.match(output, new RegExp(`${childMarker}: ${mode}`, "u"));
    });
  }
} else {
  const native = require(nativeAddonPath);

  if (childMode === "idle") {
    test("idle Worker runtime lifecycle", { timeout: 10_000 }, async () => {
      const before = native._debugCompletionRuntimeStats();
      const worker = new Worker(
        `
const { parentPort, workerData } = require("node:worker_threads");
require(workerData.nativeAddonPath);
parentPort.postMessage("ready");
setInterval(() => {}, 1_000);
`,
        { eval: true, workerData: { nativeAddonPath } },
      );

      try {
        await settlesWithin(
          waitForWorkerMessage(worker, (message) => message === "ready"),
          3_000,
        );
        await settlesWithin(worker.terminate(), 3_000);
      } finally {
        await terminateWorker(worker, 3_000);
      }

      const expected = lifecycleDelta({
        allocations: 1,
        closes: 1,
        destroys: 1,
        tsfnFinalizers: 1,
        instanceFinalizers: 1,
      });
      await waitForLifecycleDelta(native, before, expected, 3_000);
      await assertStableLifecycleDelta(native, before, expected);
      console.log(`${childMarker}: idle`);
    });
  } else if (childMode === "queued") {
    test(
      "accepted blocked Worker completions abandon during teardown",
      { timeout: 25_000 },
      async () => {
        const iterations = 4;
        for (let iteration = 0; iteration < iterations; iteration++) {
          native._debugResetCompletionGate();
          const before = native._debugCompletionRuntimeStats();
          const worker = new Worker(
            `
const { parentPort, workerData } = require("node:worker_threads");
const native = require(workerData.nativeAddonPath);
const pending = native._debugQueueGatedCompletion();
pending.catch(() => {});
globalThis.heldPending = pending;
parentPort.postMessage("queued");
native._debugBlockCompletionJs();
`,
            { eval: true, workerData: { nativeAddonPath } },
          );

          try {
            await settlesWithin(
              waitForWorkerMessage(worker, (message) => message === "queued"),
              3_000,
            );
            await waitUntil(() => native._debugCompletionGateReached(), 3_000);
            await waitUntil(() => native._debugCompletionJsBlocked(), 3_000);
            native._debugReleaseCompletionGate();
            await waitUntil(
              () =>
                native._debugCompletionRuntimeStats().tsfnAcceptances ===
                before.tsfnAcceptances + 1,
              3_000,
            );
            const termination = worker.terminate();
            assert.equal(native._debugCloseGatedCompletionRuntime(), true);
            native._debugReleaseCompletionJs();
            await settlesWithin(termination, 3_000);
          } finally {
            native._debugCloseGatedCompletionRuntime();
            native._debugReleaseCompletionGate();
            native._debugReleaseCompletionJs();
            await terminateWorker(worker, 3_000);
          }

          const expected = lifecycleDelta({
            allocations: 1,
            closes: 1,
            destroys: 1,
            tsfnFinalizers: 1,
            instanceFinalizers: 1,
            tsfnAcceptances: 1,
            envNullAbandons: 1,
            callbackEnvNullAbandons: 1,
            gatedAllocations: 1,
            gatedAbandons: 1,
            gatedFrees: 1,
          });
          await waitForLifecycleDelta(native, before, expected, 3_000);
          await assertStableLifecycleDelta(native, before, expected);
        }
        console.log(`${childMarker}: queued`);
      },
    );
  } else {
    throw new Error(`unknown lifecycle child mode: ${childMode}`);
  }
}

function lifecycleDelta(overrides) {
  return Object.fromEntries(lifecycleFields.map((field) => [field, overrides[field] ?? 0]));
}

async function waitForLifecycleDelta(native, before, expected, timeoutMs) {
  await waitUntil(() => {
    const current = native._debugCompletionRuntimeStats();
    return lifecycleFields.every((field) => current[field] >= before[field] + expected[field]);
  }, timeoutMs);
}

async function assertStableLifecycleDelta(native, before, expected) {
  assertLifecycleDelta(native._debugCompletionRuntimeStats(), before, expected);
  await delay(25, undefined, { ref: false });
  assertLifecycleDelta(native._debugCompletionRuntimeStats(), before, expected);
}

function assertLifecycleDelta(current, before, expected) {
  for (const field of lifecycleFields) {
    assert.equal(
      current[field] - before[field],
      expected[field],
      `${field} delta from ${JSON.stringify(before)} to ${JSON.stringify(current)}`,
    );
  }
}

async function waitUntil(predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error(`condition did not become true within ${timeoutMs}ms`);
    }
    await delay(10, undefined, { ref: false });
  }
}

async function settlesWithin(promise, timeoutMs) {
  return await Promise.race([
    promise,
    delay(timeoutMs, undefined, { ref: false }).then(() => {
      throw new Error(`operation did not settle within ${timeoutMs}ms`);
    }),
  ]);
}

async function terminateWorker(worker, timeoutMs) {
  if (worker.threadId !== -1) {
    await settlesWithin(worker.terminate(), timeoutMs);
  }
}

function waitForWorkerMessage(worker, predicate) {
  return new Promise((resolve, reject) => {
    const onMessage = (message) => {
      if (!predicate(message)) {
        return;
      }
      cleanup();
      resolve(message);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onExit = (code) => {
      cleanup();
      reject(new Error(`Worker exited before the expected message: ${code}`));
    };
    const cleanup = () => {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    };
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
  });
}
