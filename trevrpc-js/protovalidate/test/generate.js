// Generates build/test/gen/ from test/proto/rules.proto, and the buf/validate/validate.proto it
// imports, with the protoc-gen-es of this package, for the runtime and type tests.
import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { generateEs } from "../../tools/generate-es.js";

const outDir = fileURLToPath(new URL("../build/test/gen", import.meta.url));

await rm(outDir, { recursive: true, force: true });
await generateEs({
  protoDirs: [
    fileURLToPath(new URL("proto", import.meta.url)),
    // The buf.validate schema the interop protos vendor.
    fileURLToPath(new URL("../../../interop/proto", import.meta.url)),
  ],
  files: ["rules.proto", "buf/validate/validate.proto"],
  outDir,
  from: import.meta.url,
});
