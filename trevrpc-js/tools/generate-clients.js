// Runs protoc-gen-trevrpc-js for the build scripts, on clients that import the runtime straight
// from this checkout.
import { execFile } from "node:child_process";
import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const plugin = fileURLToPath(new URL("../bin/protoc-gen-trevrpc-js.js", import.meta.url));

/**
 * Generates clients for `files`, found in `protoDirs`, into `outDir`, which must be two levels
 * below the package root, as `build/<name>/<dir>` is. `options` are further plugin parameters,
 * such as `{ int64: "number" }`.
 */
export async function generateClients({ protoDirs, files, outDir, options = {} }) {
  // The generated files import the runtime from src/, relative to outDir.
  const runtime = "../../../src/index.js";
  const parameters = Object.entries({
    runtime_import: runtime,
    runtime_type_import: runtime,
    ...options,
  }).map(([key, value]) => `${key}=${value}`);
  await mkdir(outDir, { recursive: true });
  await run("protoc", [
    `--plugin=protoc-gen-trevrpc-js=${plugin}`,
    `--trevrpc-js_out=${parameters.join(",")}:${outDir}`,
    ...protoDirs.map((dir) => `--proto_path=${dir}`),
    ...files,
  ]);
}
