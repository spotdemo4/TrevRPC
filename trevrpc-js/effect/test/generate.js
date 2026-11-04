// Generates build/test/gen/ from test/proto/echo.proto with the protoc-gen-trevrpc-js of the
// installed @trevrpc/trevrpc-js, for the runtime and type tests.
import { execFile } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const protoDir = fileURLToPath(new URL("proto", import.meta.url));
const outDir = fileURLToPath(new URL("../build/test/gen", import.meta.url));
const plugin = fileURLToPath(
  new URL("../node_modules/.bin/protoc-gen-trevrpc-js", import.meta.url),
);

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });
await run("protoc", [
  `--plugin=protoc-gen-trevrpc-js=${plugin}`,
  `--trevrpc-js_out=${outDir}`,
  `--proto_path=${protoDir}`,
  "echo.proto",
]);
