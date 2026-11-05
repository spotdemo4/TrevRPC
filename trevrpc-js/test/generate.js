// Generates build/test/gen/ with protoc-gen-es from test/proto/: types.proto, a message with every
// kind of field, and methods.proto, whose services set the trevrpc.method option in every way the
// runtime checks.
import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { generateEs } from "../tools/generate-es.js";

const protoDir = fileURLToPath(new URL("proto", import.meta.url));
// The interop messages and shared schema options (trevrpc/options.proto) that methods.proto imports.
const interopProtoDir = fileURLToPath(new URL("../../interop/proto", import.meta.url));
const optionsProtoDir = fileURLToPath(new URL("../../proto", import.meta.url));
const outDir = fileURLToPath(new URL("../build/test/gen", import.meta.url));

await rm(fileURLToPath(new URL("../build/test", import.meta.url)), {
  recursive: true,
  force: true,
});
await generateEs({
  protoDirs: [protoDir, interopProtoDir, optionsProtoDir],
  files: ["types.proto", "methods.proto", "interop.proto", "trevrpc/options.proto"],
  outDir,
});
