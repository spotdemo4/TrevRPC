import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";

const [mode, path, version, nativeTemplatePath] = process.argv.slice(2);

assert.ok(mode === "core" || mode === "native", `unsupported package manifest mode ${mode}`);
assert.ok(path, "package manifest path is required");
assert.ok(version, "package version is required");

const source = JSON.parse(await readFile(path, "utf8"));
let manifest;

if (mode === "core") {
  assert.ok(nativeTemplatePath, "native package template path is required for the core manifest");
  const nativeTemplate = JSON.parse(await readFile(nativeTemplatePath, "utf8"));
  assert.ok(nativeTemplate.name, "native package template name is required");
  assert.equal(source.version, version, "core source and release versions must match");
  manifest = {
    ...source,
    optionalDependencies: {
      ...source.optionalDependencies,
      [nativeTemplate.name]: version,
    },
  };
} else {
  assert.ok(source.name, "native package template name is required");
  assert.equal(source.version, undefined, "native manifest template must not contain a version");
  const { name, ...metadata } = source;
  manifest = { name, version, ...metadata };
}

await writeFile(path, JSON.stringify(manifest, null, 2) + "\n");
