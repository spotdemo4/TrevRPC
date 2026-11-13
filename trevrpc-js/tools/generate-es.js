// Runs protoc-gen-es for the build scripts: generates protobuf-es messages and service descriptors,
// which createClient() takes, as JavaScript with declarations, and, when asked, the message codecs
// of protoc-gen-trevrpc-js in this checkout.
import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);

const codecPlugin = fileURLToPath(
  new URL("../protoc-gen-trevrpc-js/bin/protoc-gen-trevrpc-js", import.meta.url),
);

/**
 * Generates `files`, found in `protoDirs`, into `outDir` with the protoc-gen-es that `from` (a
 * file or directory URL, by default this package) resolves, and with protoc-gen-trevrpc-js too if
 * `codecs` is set, for `target`: JavaScript with declarations, or TypeScript ("ts").
 */
export async function generateEs({
  protoDirs,
  files,
  outDir,
  from = import.meta.url,
  codecs = false,
  target = "js+dts",
}) {
  const plugin = createRequire(from).resolve("@bufbuild/protoc-gen-es/bin/protoc-gen-es");
  // protoc runs plugins directly; these run with the node running the build, whatever the shebang
  // of the package's script says.
  const launcherDir = await mkdtemp(join(tmpdir(), "protoc-gen-es-"));
  const launcher = async (name, script) => {
    const path = join(launcherDir, name);
    await writeFile(path, `#!${process.execPath}\nawait import(${JSON.stringify(script)});\n`);
    await chmod(path, 0o755);
    return path;
  };
  try {
    const args = [
      `--plugin=protoc-gen-es=${await launcher("protoc-gen-es", plugin)}`,
      `--es_out=${outDir}`,
      `--es_opt=target=${target},import_extension=js`,
    ];
    if (codecs) {
      args.push(
        `--plugin=protoc-gen-trevrpc-js=${await launcher("protoc-gen-trevrpc-js", codecPlugin)}`,
        `--trevrpc-js_out=${outDir}`,
        `--trevrpc-js_opt=target=${target},import_extension=js`,
      );
    }
    await mkdir(outDir, { recursive: true });
    await run("protoc", [...args, ...protoDirs.map((dir) => `--proto_path=${dir}`), ...files]);
  } finally {
    await rm(launcherDir, { recursive: true, force: true });
  }
}
