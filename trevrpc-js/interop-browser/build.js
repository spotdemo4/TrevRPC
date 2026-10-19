// Generates the Echo and UnreliableEcho clients from the shared interop protos with
// protoc-gen-trevrpc-js and bundles the browser page that runs the interop scenarios. Output goes
// to build/interop/.
import { execFile } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { rolldown } from "rolldown";

const run = promisify(execFile);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const protoDir = fileURLToPath(new URL("../../interop/proto", import.meta.url));
// The shared schema options (trevrpc/options.proto) that unreliable.proto imports.
const optionsProtoDir = fileURLToPath(new URL("../../proto", import.meta.url));
const plugin = fileURLToPath(new URL("../bin/protoc-gen-trevrpc-js.js", import.meta.url));
const outDir = fileURLToPath(new URL("../build/interop", import.meta.url));

/** Generates build/interop/gen/ and bundles build/interop/page.js. */
export async function buildInterop() {
  const genDir = `${outDir}/gen`;
  await rm(outDir, { recursive: true, force: true });
  await mkdir(genDir, { recursive: true });

  // The generated client imports the runtime straight from this checkout.
  const runtime = "../../../src/index.js";
  await run("protoc", [
    `--plugin=protoc-gen-trevrpc-js=${plugin}`,
    `--trevrpc-js_out=runtime_import=${runtime},runtime_type_import=${runtime}:${genDir}`,
    `--proto_path=${protoDir}`,
    `--proto_path=${optionsProtoDir}`,
    "interop.proto",
    "unreliable.proto",
  ]);

  const bundle = await rolldown({
    cwd: packageRoot,
    input: "interop-browser/page.js",
    platform: "browser",
    transform: { target: "es2022" },
    logLevel: "warn",
  });
  try {
    await bundle.write({ file: `${outDir}/page.js`, format: "esm" });
  } finally {
    await bundle.close();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await buildInterop();
}
