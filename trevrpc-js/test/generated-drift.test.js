import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const packageRoot = new URL("..", import.meta.url);

test("checked-in greeter bindings match real protoc output", async () => {
  const output = await mkdtemp(join(tmpdir(), "trevrpc-js-drift-"));
  const plugin = new URL("bin/protoc-gen-trevrpc-js.js", packageRoot);
  const protoDir = new URL("examples/greeter/", packageRoot);
  const parameter = [
    "runtime_import=../../src/index.js",
    "runtime_type_import=../../src/index.js",
    "node_runtime_import=../../src/node-generated.js",
    "node_runtime_type_import=../../src/node-generated.js",
  ].join(",");

  await execFileAsync("protoc", [
    `--plugin=protoc-gen-trevrpc-js=${plugin.pathname}`,
    `--trevrpc-js_out=${parameter}:${output}`,
    `--proto_path=${protoDir.pathname}`,
    "greeter.proto",
  ]);
  await execFileAsync("oxfmt", ["--write", output]);

  for (const file of [
    "greeter.trevrpc.js",
    "greeter.trevrpc.d.ts",
    "greeter.node.trevrpc.js",
    "greeter.node.trevrpc.d.ts",
  ]) {
    const [actual, expected] = await Promise.all([
      readFile(join(output, file), "utf8"),
      readFile(new URL(`examples/greeter/${file}`, packageRoot), "utf8"),
    ]);
    assert.equal(actual, expected, `${file} has drifted from protoc output`);
  }
});
