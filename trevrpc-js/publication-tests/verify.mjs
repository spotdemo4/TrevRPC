import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const stage = process.argv[2];
if (stage == null) throw new Error("usage: verify.mjs STAGE_DIRECTORY");

const core = join(stage, "trevrpc-trevrpc-js-0.1.0.tgz");
const native = join(stage, "trevrpc-trevrpc-js-native-linux-x64-gnu-0.1.0.tgz");

const coreEntries = await tarEntries(core);
const nativeEntries = await tarEntries(native);
for (const required of [
  "package/package.json",
  "package/README.md",
  "package/LICENSE",
  "package/bin/protoc-gen-trevrpc-js.js",
]) {
  assert(coreEntries.includes(required), `core tarball is missing ${required}`);
}
assert(coreEntries.some((entry) => entry.startsWith("package/src/")));
assert(coreEntries.some((entry) => entry.startsWith("package/examples/greeter/")));
for (const entry of coreEntries) {
  assert.match(
    entry,
    /^package\/(?:src\/|bin\/protoc-gen-trevrpc-js\.js$|examples\/greeter\/|README\.md$|LICENSE$|package\.json$)/u,
    `unexpected core tar entry ${entry}`,
  );
  assert(!entry.endsWith(".node"));
  assert(!entry.includes("/native/"));
  assert(!entry.includes("/build/"));
  assert(!entry.includes("/test/"));
  assert(!entry.includes("/publication-tests/"));
}

assert.deepEqual(
  new Set(nativeEntries),
  new Set([
    "package/package.json",
    "package/README.md",
    "package/LICENSE",
    "package/THIRD_PARTY_NOTICES.md",
    "package/trevrpc_native.node",
    "package/libmsquic.so.2",
  ]),
);

const extract = await mkdtemp(join(tmpdir(), "trevrpc-js-stage-"));
await execFileAsync("tar", ["-xzf", core, "-C", extract]);
const coreManifest = JSON.parse(await readFile(join(extract, "package/package.json"), "utf8"));
assert.equal(coreManifest.name, "@trevrpc/trevrpc-js");
assert.equal(coreManifest.version, "0.1.0");
assert.equal(coreManifest.license, "MIT");
assert.equal(coreManifest.main, undefined);
assert.equal(coreManifest.types, undefined);
assert.equal(coreManifest.bin["protoc-gen-trevrpc-js"], "bin/protoc-gen-trevrpc-js.js");
assert.equal(coreManifest.sideEffects, false);
assert.equal(
  coreManifest.optionalDependencies["@trevrpc/trevrpc-js-native-linux-x64-gnu"],
  "0.1.0",
);
assert.ok(coreManifest.exports["./node/generated"]);

const nativeExtract = await mkdtemp(join(tmpdir(), "trevrpc-js-native-stage-"));
await execFileAsync("tar", ["-xzf", native, "-C", nativeExtract]);
const nativeManifest = JSON.parse(
  await readFile(join(nativeExtract, "package/package.json"), "utf8"),
);
assert.equal(nativeManifest.name, "@trevrpc/trevrpc-js-native-linux-x64-gnu");
assert.equal(nativeManifest.version, "0.1.0");
assert.equal(nativeManifest.license, "MIT");
assert.deepEqual(nativeManifest.os, ["linux"]);
assert.deepEqual(nativeManifest.cpu, ["x64"]);
assert.deepEqual(nativeManifest.libc, ["glibc"]);

const nativeAddon = join(nativeExtract, "package/trevrpc_native.node");
const nativeExports = Object.getOwnPropertyNames(
  createRequire(import.meta.url)(nativeAddon),
).sort();
assert.deepEqual(nativeExports, ["connectMsQuic", "createCancellation", "listenMsQuic"]);
const nativeBytes = await readFile(nativeAddon);
assert.equal(nativeBytes.includes(Buffer.from("_debug")), false);

for (const file of [nativeAddon, join(nativeExtract, "package/libmsquic.so.2")]) {
  const bytes = await readFile(file);
  for (const forbidden of ["/nix/store", "/home/", "/build/"]) {
    assert.equal(bytes.includes(Buffer.from(forbidden)), false, `${file} contains ${forbidden}`);
  }
}
const { stdout: dynamic } = await execFileAsync("readelf", [
  "-d",
  join(nativeExtract, "package/trevrpc_native.node"),
]);
assert.match(dynamic, /(?:RPATH|RUNPATH).*\$ORIGIN/u);

await execFileAsync("npm", ["publish", "--dry-run", "--json", core]);
await execFileAsync("npm", ["publish", "--dry-run", "--json", native]);

async function tarEntries(path) {
  const { stdout } = await execFileAsync("tar", ["-tzf", path]);
  return stdout
    .trim()
    .split("\n")
    .filter((entry) => entry !== "" && !entry.endsWith("/"));
}
