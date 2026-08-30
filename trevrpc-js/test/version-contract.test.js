import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const packageRoot = new URL("..", import.meta.url);

async function readJson(path) {
  return JSON.parse(await readFile(new URL(path, packageRoot), "utf8"));
}

function assertLockMatchesManifest(manifest, lock, label) {
  assert.equal(lock.name, manifest.name, `${label} lock name`);
  assert.equal(lock.version, manifest.version, `${label} lock version`);
  assert.equal(lock.packages[""].name, manifest.name, `${label} lock root name`);
  assert.equal(lock.packages[""].version, manifest.version, `${label} lock root version`);
}

test("JavaScript release metadata has one canonical version per package", async () => {
  const [core, coreLock, bench, benchLock, browser, browserLock, nativeTemplate] =
    await Promise.all([
      readJson("package.json"),
      readJson("package-lock.json"),
      readJson("bench/package.json"),
      readJson("bench/package-lock.json"),
      readJson("bench-browser/package.json"),
      readJson("bench-browser/package-lock.json"),
      readJson("npm/native-linux-x64-gnu/package.template.json"),
    ]);

  assertLockMatchesManifest(core, coreLock, "core");
  assertLockMatchesManifest(bench, benchLock, "bench");
  assertLockMatchesManifest(browser, browserLock, "browser");

  const nativeName = "@trevrpc/trevrpc-js-native-linux-x64-gnu";
  assert.equal(nativeTemplate.name, nativeName);
  assert.equal(nativeTemplate.version, undefined);
  assert.equal(core.optionalDependencies?.[nativeName], undefined);
  assert.equal(coreLock.packages[""].optionalDependencies?.[nativeName], undefined);
  assert.equal(coreLock.packages[`node_modules/${nativeName}`], undefined);

  assert.deepEqual(bench.peerDependencies, { [core.name]: "*" });
  assert.deepEqual(browser.peerDependencies, {
    [core.name]: "*",
    [bench.name]: "*",
  });
  assert.deepEqual(benchLock.packages[""].peerDependencies, bench.peerDependencies);
  assert.deepEqual(browserLock.packages[""].peerDependencies, browser.peerDependencies);
});
