// Generates build/test/gen/ from test/proto/echo.proto with the protoc-gen-es of this package, for
// the runtime and type tests.
import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { generateEs } from "../../tools/generate-es.js";

const outDir = fileURLToPath(new URL("../build/test/gen", import.meta.url));

await rm(outDir, { recursive: true, force: true });
await generateEs({
  protoDirs: [fileURLToPath(new URL("proto", import.meta.url))],
  files: ["echo.proto"],
  outDir,
  from: import.meta.url,
});
