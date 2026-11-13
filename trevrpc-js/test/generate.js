// Generates build/test/gen/ with protoc-gen-es and protoc-gen-trevrpc-js from test/proto/:
// types.proto, a message with every kind of field, methods.proto, whose services set the
// trevrpc.method option in every way the runtime checks, and codec.proto with codec_editions.proto,
// which the codec tests check against protobuf-es.
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
  files: [
    "types.proto",
    "methods.proto",
    "codec.proto",
    "codec_editions.proto",
    "interop.proto",
    "trevrpc/options.proto",
  ],
  outDir,
  codecs: true,
});

// The codecs as TypeScript too, which the type check compiles.
await generateEs({
  protoDirs: [protoDir, interopProtoDir, optionsProtoDir],
  files: ["codec.proto", "codec_editions.proto", "interop.proto"],
  outDir: fileURLToPath(new URL("../build/test/gen-ts", import.meta.url)),
  codecs: true,
  target: "ts",
});
