import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import test from "node:test";
import { setImmediate, setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const thisFile = fileURLToPath(import.meta.url);
const gcChild = process.env.TREVRPC_NATIVE_ZERO_COPY_GC_CHILD === "1";

if (typeof global.gc !== "function" && !gcChild) {
  test(
    "native zero-copy ownership tests run with forced GC",
    { skip: !existsSync(nativeAddonPath) },
    () => {
      const result = spawnSync(process.execPath, ["--expose-gc", "--test", thisFile], {
        encoding: "utf8",
        env: { ...process.env, TREVRPC_NATIVE_ZERO_COPY_GC_CHILD: "1" },
      });

      assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
    },
  );
} else {
  const native = existsSync(nativeAddonPath) ? require(nativeAddonPath) : null;
  const hasGc = typeof global.gc === "function";

  test(
    "native response external body owner survives C cleanup until GC finalizer",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const expected = new Uint8Array([1, 2, 3, 4, 5]);
      let response = native._debugMakeBorrowedResponse(expected);
      let body = response.body;

      assert.deepEqual(Array.from(body), Array.from(expected));
      response = null;
      await forceGcCycles();

      assert.deepEqual(Array.from(body), Array.from(expected));
      assert.equal(finalizerCount(native), before);

      body = null;
      await waitForFinalizers(native, before + 1);
    },
  );

  test(
    "native stream frame external body owner survives C cleanup until GC finalizer",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const expected = new Uint8Array([9, 8, 7, 6]);
      let frame = native._debugMakeBorrowedStreamFrame(expected);
      let body = frame.body;

      assert.deepEqual(Array.from(body), Array.from(expected));
      frame = null;
      await forceGcCycles();

      assert.deepEqual(Array.from(body), Array.from(expected));
      assert.equal(finalizerCount(native), before);

      body = null;
      await waitForFinalizers(native, before + 1);
    },
  );

  test(
    "native stream body batches transfer borrowed frame owners",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const expected = new Uint8Array([42, 43, 44]);
      let batch = native._debugMakeBorrowedStreamBodyBatch(expected);
      let body = batch.bodies[0];

      assert.deepEqual(Array.from(body), Array.from(expected));
      batch = null;
      await forceGcCycles();

      assert.deepEqual(Array.from(body), Array.from(expected));
      assert.equal(finalizerCount(native), before);

      body = null;
      await waitForFinalizers(native, before + 1);
    },
  );

  test(
    "native outbound body refs survive forced GC until async completion",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      let body = new Uint8Array([17, 34, 51, 68]);
      const expected = checksum(body);
      const retained = native._debugRetainBodyUntilAsyncComplete(body);

      body = null;
      await forceGcCycles(5);

      assert.equal(await retained, expected);
    },
  );

  test(
    "native evented completion defers close until pending operation releases native ref",
    {
      skip: native == null,
    },
    async () => {
      const before = resourceCloseCount(native);
      const resource = native._debugCreatePendingResource();
      const pending = resource.wait(75);

      await waitForCondition(() => resource.refs() === 1, "pending resource was not acquired");
      resource.close();

      assert.equal(resource.closed(), false);
      await pending;
      assert.equal(resource.closed(), true);
      assert.equal(resourceCloseCount(native), before + 1);
      await assert.rejects(resource.wait(1), (error) => error.nativeCode === -4001);
    },
  );

  test(
    "native evented completion receiver ref prevents GC finalizer while operation is pending",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = resourceFinalizerCount(native);
      let resource = native._debugCreatePendingResource();
      const pending = resource.wait(75);

      await waitForCondition(() => resource.refs() === 1, "pending resource was not acquired");
      resource = null;
      await forceGcCycles(5);

      assert.equal(resourceFinalizerCount(native), before);
      await pending;
      await waitForResourceFinalizers(native, before + 1);
    },
  );
}

function finalizerCount(native) {
  return native._debugExternalArrayBufferFinalizers();
}

function resourceCloseCount(native) {
  return native._debugPendingResourceCloses();
}

function resourceFinalizerCount(native) {
  return native._debugPendingResourceFinalizers();
}

async function forceGcCycles(cycles = 4) {
  for (let i = 0; i < cycles; i += 1) {
    global.gc();
    await setImmediate();
  }
}

async function waitForFinalizers(native, target) {
  for (let i = 0; i < 50; i += 1) {
    await forceGcCycles(1);
    if (finalizerCount(native) >= target) {
      return;
    }
  }
  assert.fail(
    `expected at least ${target} external ArrayBuffer finalizers, saw ${finalizerCount(native)}`,
  );
}

async function waitForResourceFinalizers(native, target) {
  for (let i = 0; i < 50; i += 1) {
    await forceGcCycles(1);
    if (resourceFinalizerCount(native) >= target) {
      return;
    }
  }
  assert.fail(
    `expected at least ${target} debug resource finalizers, saw ${resourceFinalizerCount(native)}`,
  );
}

async function waitForCondition(predicate, message) {
  for (let i = 0; i < 100; i += 1) {
    if (predicate()) {
      return;
    }
    await delay(1);
  }
  assert.fail(message);
}

function checksum(bytes) {
  let value = 0;
  for (const byte of bytes) {
    value = value * 131 + byte;
  }
  return value;
}
