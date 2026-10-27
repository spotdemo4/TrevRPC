// Generates the Echo, UnreliableEcho and CompressedEcho clients from the shared interop protos
// with protoc-gen-trevrpc-js, and the Echo service for connect-web with protoc-gen-es, and bundles
// the browser pages that run the interop scenarios and the load benchmark. Output goes to
// build/interop/.
import { execFile } from "node:child_process";
import { chmod, mkdir, rm, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { rolldown } from "rolldown";

const run = promisify(execFile);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const protoDir = fileURLToPath(new URL("../../interop/proto", import.meta.url));
// The shared schema options (trevrpc/options.proto) that unreliable.proto imports.
const optionsProtoDir = fileURLToPath(new URL("../../proto", import.meta.url));
const plugin = fileURLToPath(new URL("../bin/protoc-gen-trevrpc-js.js", import.meta.url));
const esPlugin = fileURLToPath(
  new URL("../node_modules/@bufbuild/protoc-gen-es/bin/protoc-gen-es", import.meta.url),
);
const outDir = fileURLToPath(new URL("../build/interop", import.meta.url));

/**
 * Generates build/interop/gen/ and build/interop/gen-es/, and bundles build/interop/page.js and
 * build/interop/bench.js.
 */
export async function buildInterop() {
  const genDir = `${outDir}/gen`;
  const esGenDir = `${outDir}/gen-es`;
  await rm(outDir, { recursive: true, force: true });
  await mkdir(genDir, { recursive: true });
  await mkdir(esGenDir, { recursive: true });

  // The generated client imports the runtime straight from this checkout.
  const runtime = "../../../src/index.js";
  await run("protoc", [
    `--plugin=protoc-gen-trevrpc-js=${plugin}`,
    `--trevrpc-js_out=runtime_import=${runtime},runtime_type_import=${runtime}:${genDir}`,
    `--proto_path=${protoDir}`,
    `--proto_path=${optionsProtoDir}`,
    "interop.proto",
    "unreliable.proto",
    "compressed.proto",
  ]);

  // protoc runs plugins directly; this one runs with the node running the build, whatever the
  // shebang of the package's script says.
  const esLauncher = `${outDir}/protoc-gen-es`;
  await writeFile(
    esLauncher,
    `#!${process.execPath}\nawait import(${JSON.stringify(esPlugin)});\n`,
  );
  await chmod(esLauncher, 0o755);
  await run("protoc", [
    `--plugin=protoc-gen-es=${esLauncher}`,
    `--es_out=${esGenDir}`,
    "--es_opt=target=js+dts,import_extension=js",
    `--proto_path=${protoDir}`,
    "interop.proto",
  ]);
  await rm(esLauncher);

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
