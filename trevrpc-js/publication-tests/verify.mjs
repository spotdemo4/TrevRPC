import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const [mode, target, expectedVersion] = process.argv.slice(2);
if (!mode || !target || !expectedVersion) {
  throw new Error(
    "usage: verify.mjs <core CORE_TGZ|native NATIVE_TGZ|stage|pair STAGE_DIRECTORY> EXPECTED_VERSION",
  );
}

switch (mode) {
  case "core":
    await verifyCore(target, expectedVersion);
    break;
  case "native":
    await verifyNative(target, expectedVersion);
    break;
  case "stage":
    await writeStageMetadata(target, expectedVersion);
    break;
  case "pair":
    await verifyPair(target, expectedVersion);
    break;
  default:
    throw new Error(`unknown verification mode ${mode}`);
}

async function verifyCore(core, version) {
  assert.equal(basename(core), `trevrpc-trevrpc-js-${version}.tgz`);

  const entries = await tarEntries(core);
  for (const required of [
    "package/package.json",
    "package/README.md",
    "package/LICENSE",
    "package/bin/protoc-gen-trevrpc-js.js",
  ]) {
    assert(entries.includes(required), `core tarball is missing ${required}`);
  }
  assert(entries.some((entry) => entry.startsWith("package/src/")));
  assert(entries.some((entry) => entry.startsWith("package/examples/greeter/")));
  for (const entry of entries) {
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

  await withExtractedTarball(core, "trevrpc-js-core-", async (root) => {
    const packageRoot = join(root, "package");
    const manifest = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8"));
    assert.equal(manifest.name, "@trevrpc/trevrpc-js");
    assert.equal(manifest.version, version);
    assert.equal(manifest.license, "MIT");
    assert.equal(manifest.main, undefined);
    assert.equal(manifest.types, undefined);
    assert.equal(manifest.bin["protoc-gen-trevrpc-js"], "bin/protoc-gen-trevrpc-js.js");
    assert.equal(manifest.sideEffects, false);
    assert.equal(typeof manifest.dependencies.protobufjs, "string");
    assert.equal(manifest.engines.node, ">=24 <25");
    assert.equal(
      manifest.optionalDependencies["@trevrpc/trevrpc-js-native-linux-x64-gnu"],
      version,
    );
    assert.ok(manifest.exports["./node/generated"]);
    assert.notEqual(manifest.publishConfig?.provenance, true);
    for (const exportedPath of stringLeaves(manifest.exports)) {
      assert(
        entries.includes(`package/${exportedPath.replace(/^\.\//u, "")}`),
        `core tarball is missing exported path ${exportedPath}`,
      );
    }

    const generator = await readFile(join(packageRoot, "bin/protoc-gen-trevrpc-js.js"), "utf8");
    assert.equal(generator.split(/\r?\n/u, 1)[0], "#!/usr/bin/env node");

    for (const file of await regularFiles(packageRoot)) {
      const bytes = await readFile(file);
      assert.equal(bytes.includes(Buffer.from("/nix/store")), false, `${file} contains /nix/store`);
    }
  });

  await publishDryRun(core);
}

async function verifyNative(native, version) {
  assert.equal(basename(native), `trevrpc-trevrpc-js-native-linux-x64-gnu-${version}.tgz`);

  assert.deepEqual(
    new Set(await tarEntries(native)),
    new Set([
      "package/package.json",
      "package/README.md",
      "package/LICENSE",
      "package/THIRD_PARTY_NOTICES.md",
      "package/trevrpc_native.node",
      "package/libmsquic.so.2",
    ]),
  );

  await withExtractedTarball(native, "trevrpc-js-native-", async (root) => {
    const packageRoot = join(root, "package");
    await regularFiles(packageRoot);
    const manifest = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8"));
    assert.equal(manifest.name, "@trevrpc/trevrpc-js-native-linux-x64-gnu");
    assert.equal(manifest.version, version);
    assert.equal(manifest.license, "MIT");
    assert.equal(manifest.main, "trevrpc_native.node");
    assert.deepEqual(manifest.os, ["linux"]);
    assert.deepEqual(manifest.cpu, ["x64"]);
    assert.deepEqual(manifest.libc, ["glibc"]);
    assert.equal(manifest.engines.node, ">=24 <25");
    assert.notEqual(manifest.publishConfig?.provenance, true);

    const nativeAddon = join(packageRoot, "trevrpc_native.node");
    const msquic = join(packageRoot, "libmsquic.so.2");
    const nativeExports = Object.getOwnPropertyNames(
      createRequire(import.meta.url)(nativeAddon),
    ).sort();
    assert.deepEqual(nativeExports, ["connectMsQuic", "createCancellation", "listenMsQuic"]);

    const nativeBytes = await readFile(nativeAddon);
    assert.equal(nativeBytes.includes(Buffer.from("_debug")), false);
    for (const file of [nativeAddon, msquic]) {
      const bytes = await readFile(file);
      for (const forbidden of ["/nix/store", "/home/", "/build/"]) {
        assert.equal(
          bytes.includes(Buffer.from(forbidden)),
          false,
          `${file} contains ${forbidden}`,
        );
      }
    }

    const { stdout: addonDynamic } = await execFileAsync("readelf", ["-d", nativeAddon]);
    const addonRunpath = /\((?:RPATH|RUNPATH)\).*Library r(?:un)?path: \[([^\]]*)\]/u.exec(
      addonDynamic,
    );
    assert(addonRunpath, "native addon has no RPATH or RUNPATH");
    assert.equal(addonRunpath[1], "$ORIGIN");
    assert.deepEqual(
      new Set(neededLibraries(addonDynamic)),
      new Set(["libmsquic.so.2", "libc.so.6"]),
    );

    const { stdout: msquicDynamic } = await execFileAsync("readelf", ["-d", msquic]);
    assert.doesNotMatch(msquicDynamic, /\b(?:RPATH|RUNPATH)\b/u);
    assert.deepEqual(new Set(neededLibraries(msquicDynamic)), new Set(["libdl.so.2", "libc.so.6"]));

    const versionInfo = await Promise.all(
      [nativeAddon, msquic].map(async (file) => {
        const { stdout } = await execFileAsync("readelf", ["--version-info", file]);
        return stdout;
      }),
    );
    const glibcVersions = versionInfo
      .flatMap((output) => [...output.matchAll(/GLIBC_([0-9]+(?:\.[0-9]+)*)/gu)])
      .map((match) => match[1]);
    assert(glibcVersions.length > 0, "native package has no GLIBC symbol versions");
    glibcVersions.sort(compareVersions);
    const maxGlibc = glibcVersions.at(-1);
    assert(
      compareVersions(maxGlibc, "2.42") <= 0,
      `native package requires GLIBC_${maxGlibc}, expected at most GLIBC_2.42`,
    );
  });

  await publishDryRun(native);
}

async function writeStageMetadata(stage, version) {
  const coreName = `trevrpc-trevrpc-js-${version}.tgz`;
  const nativeName = `trevrpc-trevrpc-js-native-linux-x64-gnu-${version}.tgz`;
  const coreHash = await sha256(join(stage, coreName));
  const nativeHash = await sha256(join(stage, nativeName));
  await writeFile(
    join(stage, "sha256sums.txt"),
    `${coreHash}  ${coreName}\n${nativeHash}  ${nativeName}\n`,
  );
  await writeFile(
    join(stage, "manifest.json"),
    `${JSON.stringify(stageManifest(version, coreName, nativeName, coreHash, nativeHash), null, 2)}\n`,
  );
}

async function verifyPair(stage, version) {
  const coreName = `trevrpc-trevrpc-js-${version}.tgz`;
  const nativeName = `trevrpc-trevrpc-js-native-linux-x64-gnu-${version}.tgz`;
  const expectedFiles = new Set([coreName, nativeName, "sha256sums.txt", "manifest.json"]);
  const entries = await readdir(stage, { withFileTypes: true });
  assert(
    entries.every((entry) => entry.isFile()),
    "stage must contain only regular files",
  );
  assert.deepEqual(new Set(entries.map((entry) => entry.name)), expectedFiles);

  const core = join(stage, coreName);
  const native = join(stage, nativeName);
  const checksumLines = (await readFile(join(stage, "sha256sums.txt"), "utf8")).trim().split("\n");
  assert.equal(checksumLines.length, 2);
  const checksums = new Map(
    checksumLines.map((line) => {
      const match = /^([0-9a-f]{64})\s+(.+)$/u.exec(line);
      assert(match, `invalid checksum line ${line}`);
      return [match[2], match[1]];
    }),
  );
  assert.deepEqual(new Set(checksums.keys()), new Set([coreName, nativeName]));

  const coreHash = await sha256(core);
  const nativeHash = await sha256(native);
  assert.equal(checksums.get(coreName), coreHash);
  assert.equal(checksums.get(nativeName), nativeHash);

  const manifest = JSON.parse(await readFile(join(stage, "manifest.json"), "utf8"));
  assert.deepEqual(manifest, stageManifest(version, coreName, nativeName, coreHash, nativeHash));

  const [coreManifest, nativeManifest] = await Promise.all([
    manifestFromTarball(core, "trevrpc-js-pair-core-"),
    manifestFromTarball(native, "trevrpc-js-pair-native-"),
  ]);
  assert.equal(coreManifest.name, "@trevrpc/trevrpc-js");
  assert.equal(coreManifest.version, version);
  assert.equal(coreManifest.engines.node, ">=24 <25");
  assert.equal(nativeManifest.name, "@trevrpc/trevrpc-js-native-linux-x64-gnu");
  assert.equal(nativeManifest.version, version);
  assert.equal(nativeManifest.engines.node, ">=24 <25");
  assert.equal(
    coreManifest.optionalDependencies["@trevrpc/trevrpc-js-native-linux-x64-gnu"],
    nativeManifest.version,
  );
}

function stageManifest(version, coreName, nativeName, coreHash, nativeHash) {
  return {
    version,
    publication: "local-stage-only",
    native_target: "linux/x64/glibc>=2.42",
    node_versions: [24],
    rpc_shapes: ["unary", "client-streaming", "server-streaming", "bidirectional-streaming"],
    browser: { bundler_types: true, esbuild: true, chromium_unary: true },
    sha256: { [coreName]: coreHash, [nativeName]: nativeHash },
  };
}

async function manifestFromTarball(path, prefix) {
  return withExtractedTarball(path, prefix, async (root) =>
    JSON.parse(await readFile(join(root, "package/package.json"), "utf8")),
  );
}

async function withExtractedTarball(path, prefix, callback) {
  const extract = await mkdtemp(join(tmpdir(), prefix));
  try {
    await execFileAsync("tar", ["-xzf", path, "-C", extract]);
    return await callback(extract);
  } finally {
    await rm(extract, { recursive: true, force: true });
  }
}

async function regularFiles(root) {
  const files = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) files.push(...(await regularFiles(path)));
    else if (entry.isFile()) files.push(path);
    else assert.fail(`package contains non-regular entry ${path}`);
  }
  return files;
}

function stringLeaves(value) {
  if (typeof value === "string") return [value];
  if (value == null || typeof value !== "object") return [];
  return Object.values(value).flatMap(stringLeaves);
}

function neededLibraries(dynamicSection) {
  return [...dynamicSection.matchAll(/\(NEEDED\).*Shared library: \[([^\]]+)\]/gu)].map(
    (match) => match[1],
  );
}

async function publishDryRun(path) {
  await execFileAsync("npm", ["publish", "--dry-run", "--json", path]);
}

async function sha256(path) {
  return createHash("sha256")
    .update(await readFile(path))
    .digest("hex");
}

function compareVersions(left, right) {
  const leftParts = left.split(".").map(Number);
  const rightParts = right.split(".").map(Number);
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const difference = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

async function tarEntries(path) {
  const { stdout } = await execFileAsync("tar", ["-tzf", path]);
  return stdout
    .trim()
    .split("\n")
    .filter((entry) => entry !== "" && !entry.endsWith("/"));
}
