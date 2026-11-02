// Generates build/test/gen/ (the default int64 representation, bigint) and build/test/gen-number/
// (int64=number) from test/proto/types.proto, for the generator's runtime and type tests.
import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { generateClients } from "../tools/generate-clients.js";

const protoDir = fileURLToPath(new URL("proto", import.meta.url));
const outDir = fileURLToPath(new URL("../build/test", import.meta.url));

await rm(outDir, { recursive: true, force: true });
for (const { dir, options } of [
  { dir: "gen", options: {} },
  { dir: "gen-number", options: { int64: "number" } },
]) {
  await generateClients({
    protoDirs: [protoDir],
    files: ["types.proto"],
    outDir: `${outDir}/${dir}`,
    options,
  });
}
