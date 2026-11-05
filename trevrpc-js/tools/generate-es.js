// Runs protoc-gen-es for the build scripts: generates protobuf-es messages and service descriptors,
// which createClient() takes, as JavaScript with declarations.
import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);

/**
 * Generates `files`, found in `protoDirs`, into `outDir` with the protoc-gen-es that `from` (a
 * file or directory URL, by default this package) resolves.
 */
export async function generateEs({ protoDirs, files, outDir, from = import.meta.url }) {
  const plugin = createRequire(from).resolve("@bufbuild/protoc-gen-es/bin/protoc-gen-es");
  // protoc runs plugins directly; this one runs with the node running the build, whatever the
  // shebang of the package's script says.
  const launcherDir = await mkdtemp(join(tmpdir(), "protoc-gen-es-"));
  const launcher = join(launcherDir, "protoc-gen-es");
  try {
    await writeFile(launcher, `#!${process.execPath}\nawait import(${JSON.stringify(plugin)});\n`);
    await chmod(launcher, 0o755);
    await mkdir(outDir, { recursive: true });
    await run("protoc", [
      `--plugin=protoc-gen-es=${launcher}`,
      `--es_out=${outDir}`,
      "--es_opt=target=js+dts,import_extension=js",
      ...protoDirs.map((dir) => `--proto_path=${dir}`),
      ...files,
    ]);
  } finally {
    await rm(launcherDir, { recursive: true, force: true });
  }
}
