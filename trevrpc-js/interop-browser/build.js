// Generates the Echo, UnreliableEcho, CompressedEcho and ValidatedEcho services, and the
// buf.validate messages of ValidatedEcho's violations, from the shared interop protos with
// protoc-gen-es, for the TrevRPC client and connect-web alike, and bundles the browser pages
// that run the interop scenarios and the load benchmark. Output goes to build/interop/.
import { mkdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { rolldown } from "rolldown";

import { generateEs } from "../tools/generate-es.js";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const protoDir = fileURLToPath(new URL("../../interop/proto", import.meta.url));
// The shared schema options (trevrpc/options.proto) that unreliable.proto imports.
const optionsProtoDir = fileURLToPath(new URL("../../proto", import.meta.url));
const outDir = fileURLToPath(new URL("../build/interop", import.meta.url));

/** Generates build/interop/gen/, and bundles build/interop/page.js and build/interop/bench.js. */
export async function buildInterop() {
  await rm(outDir, { recursive: true, force: true });
  await mkdir(outDir, { recursive: true });
  await generateEs({
    protoDirs: [protoDir, optionsProtoDir],
    files: [
      "interop.proto",
      "unreliable.proto",
      "compressed.proto",
      "validated.proto",
      "buf/validate/validate.proto",
      "trevrpc/options.proto",
    ],
    outDir: `${outDir}/gen`,
  });

  // The bench page also loads over the benchmark's emulated links, so it is minified.
  for (const [page, minify] of [
    ["page", false],
    ["bench-page", true],
  ]) {
    const bundle = await rolldown({
      cwd: packageRoot,
      input: `interop-browser/${page}.js`,
      platform: "browser",
      transform: { target: "es2022" },
      logLevel: "warn",
    });
    try {
      await bundle.write({
        file: `${outDir}/${page.replace("-page", "")}.js`,
        format: "esm",
        minify,
      });
    } finally {
      await bundle.close();
    }
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await buildInterop();
}
