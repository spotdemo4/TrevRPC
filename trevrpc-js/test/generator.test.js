import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { generate, parseOptions, runGenerator } from "../src/generator.js";
import { protobuf } from "../src/wire.js";

const run = promisify(execFile);
const packageRoot = new URL("..", import.meta.url);

test("the generator emits a JavaScript client and its declarations, nothing else", () => {
  const response = generate(greeterRequest());
  assert.equal(response.error, undefined);
  assert.deepEqual(
    response.file.map((file) => file.name),
    ["hello/v1/greeter.trevrpc.js", "hello/v1/greeter.trevrpc.d.ts"],
  );

  const js = fileContent(response, "hello/v1/greeter.trevrpc.js");
  assert.match(js, /import \{ createRoot, createServiceClient \} from "@trevrpc\/trevrpc-js";/);
  assert.match(js, /export class GreeterClient/);
  assert.match(js, /export function createGreeterClient/);
  assert.match(js, /sayHello\(request, options = \{\}\)/);
  assert.doesNotMatch(js, /WithResponse/);
  assert.match(js, /lotsOfReplies\(request, options = \{\}\)/);
  assert.match(js, /lotsOfGreetings\(options = \{\}\)/);
  assert.match(js, /bidiHello\(options = \{\}\)/);
  assert.match(js, /"fullName": "hello\.v1\.Greeter"/);
  assert.doesNotMatch(js, /register|NodeServer|node\/generated/);
});

test("generated declarations type every call shape", () => {
  const types = fileContent(generate(greeterRequest()), "hello/v1/greeter.trevrpc.d.ts");

  // The decoded shape reads every field; the input shape may leave any out.
  assert.match(types, /export interface HelloRequest \{\s*name: string;\s*\}/);
  assert.match(types, /export interface HelloRequestInit \{\s*name\?: string;\s*\}/);
  assert.match(types, /export interface HelloReply \{/);
  assert.match(
    types,
    /sayHello\(request: HelloRequestInit, options\?: CallOptions\): Promise<HelloReply>;/,
  );
  assert.doesNotMatch(types, /WithResponse|UnaryResponse/);
  assert.match(types, /lotsOfReplies\([\s\S]*Promise<ResponseStream<HelloReply>>;/);
  assert.match(
    types,
    /lotsOfGreetings\(options\?: CallOptions\): Promise<ClientStreamingCall<HelloRequestInit, HelloReply>>;/,
  );
  assert.match(
    types,
    /bidiHello\(options\?: CallOptions\): Promise<BidirectionalStreamingCall<HelloRequestInit, HelloReply>>;/,
  );
  assert.match(
    types,
    /import type \{ BidirectionalStreamingCall, CallOptions, ClientOptions, ClientStreamingCall, ResponseStream, RpcServiceDescriptor, Transport \} from "@trevrpc\/trevrpc-js";/,
  );
  assert.match(types, /import type \{ Root \} from "protobufjs";/);
});

test("declarations import only the types they use", () => {
  const types = fileContent(generate(longAndMapRequest()), "types/v1/values.trevrpc.d.ts");

  assert.match(types, /import type \{ Root \} from "protobufjs";/);
  assert.match(types, /count: bigint;/);
  assert.match(types, /count\?: bigint \| number \| string;/);
  assert.match(types, /labels: Record<string, string>;/);
  assert.match(types, /labels\?: Record<string, string>;/);
  assert.match(
    types,
    /import type \{ CallOptions, ClientOptions, RpcServiceDescriptor, Transport \} from/,
  );
  assert.match(types, /getValues\(request: ValuesInit, options\?: CallOptions\): Promise<Values>;/);

  const long = fileContent(
    generate(longAndMapRequest("int64=long")),
    "types/v1/values.trevrpc.d.ts",
  );
  assert.match(long, /import type \{ Long, Root \} from "protobufjs";/);
  assert.match(long, /count: Long;/);
  assert.match(long, /count\?: bigint \| number \| string \| Long;/);
});

test("declarations include the types of imported files that the file uses, and no others", () => {
  const request = importedTypeRequest();
  request.protoFile[0].messageType.push({ name: "Unused", field: [field("x", 1, 9)] });
  const types = fileContent(generate(request), "hello/v1/greeter.trevrpc.d.ts");

  assert.match(types, /export interface SharedRequest \{/);
  assert.match(types, /sayHello\(request: SharedRequestInit/);
  assert.doesNotMatch(types, /Unused/);
});

test("enums are exported with their values, without the enum's prefix when every value has it", () => {
  const request = greeterRequest();
  const file = request.protoFile[0];
  file.enumType = [
    {
      name: "TimeInterval",
      value: [
        { name: "TIME_INTERVAL_UNSPECIFIED", number: 0 },
        { name: "TIME_INTERVAL_HOUR", number: 1 },
      ],
    },
    // The remainder would start with a digit, so the names stay whole.
    { name: "Size", value: [{ name: "SIZE_UNSPECIFIED" }, { name: "SIZE_2X", number: 2 }] },
  ];
  file.messageType[0].enumType = [
    {
      name: "Mood",
      value: [
        { name: "HAPPY", number: 0 },
        { name: "SAD", number: 1 },
      ],
    },
  ];
  file.messageType[0].field.push(
    field("interval", 2, 14, { typeName: ".hello.v1.TimeInterval" }),
    field("mood", 3, 14, { typeName: ".hello.v1.HelloRequest.Mood" }),
  );
  const response = generate(request);
  const js = fileContent(response, "hello/v1/greeter.trevrpc.js");
  const types = fileContent(response, "hello/v1/greeter.trevrpc.d.ts");

  assert.match(
    js,
    /export const TimeInterval = Object\.freeze\(\{\n  UNSPECIFIED: 0,\n  HOUR: 1,\n\}\);/,
  );
  assert.match(
    js,
    /export const Size = Object\.freeze\(\{\n  SIZE_UNSPECIFIED: 0,\n  SIZE_2X: 2,\n\}\);/,
  );
  assert.match(js, /export const Mood = Object\.freeze\(\{\n  HAPPY: 0,\n  SAD: 1,\n\}\);/);
  assert.match(
    types,
    /export declare const TimeInterval: Readonly<\{ UNSPECIFIED: 0; HOUR: 1 \}>;\n.*\nexport type TimeInterval = \(typeof TimeInterval\)\[keyof typeof TimeInterval\];/,
  );
  assert.match(types, /interval: TimeInterval;/);
  assert.match(types, /interval\?: TimeInterval;/);
  assert.match(types, /mood: Mood;/);
});

test("the int64 option and the jstype field option choose how 64-bit integers decode", () => {
  const request = longAndMapRequest();
  const [values] = request.protoFile[0].messageType;
  values.field.push(
    field("exact", 3, 4, { options: { jstype: 1 } }),
    field("small", 4, 18, { options: { jstype: 2 } }),
    field("history", 5, 6, { label: 3 }),
  );
  const response = generate(request);
  const js = fileContent(response, "types/v1/values.trevrpc.js");
  const types = fileContent(response, "types/v1/values.trevrpc.d.ts");

  assert.match(js, /\}, \{ int64: "bigint" \}\);/);
  const fields = rootJson(js).nested.types.nested.v1.nested.Values.fields;
  assert.deepEqual(fields.exact.options, { jstype: "JS_STRING" });
  assert.deepEqual(fields.small.options, { jstype: "JS_NUMBER" });
  assert.match(types, /exact: string;/);
  assert.match(types, /small: number;/);
  assert.match(types, /history: bigint\[\];/);
  assert.match(types, /history\?: \(bigint \| number \| string\)\[\];/);

  for (const representation of ["number", "string", "long"]) {
    const option = `int64=${representation}`;
    const generated = fileContent(
      generate(longAndMapRequest(option)),
      "types/v1/values.trevrpc.js",
    );
    assert.match(generated, new RegExp(`\\{ int64: "${representation}" \\}`), option);
  }
  assert.match(generate(longAndMapRequest("int64=double")).error, /invalid trevrpc-js int64/);
});

test("well-known types the runtime declares are imported, not declared", () => {
  const request = longAndMapRequest();
  request.protoFile.unshift(
    {
      name: "google/protobuf/timestamp.proto",
      package: "google.protobuf",
      messageType: [{ name: "Timestamp", field: [field("seconds", 1, 3), field("nanos", 2, 5)] }],
    },
    {
      name: "google/protobuf/empty.proto",
      package: "google.protobuf",
      messageType: [{ name: "Empty" }],
    },
  );
  const file = request.protoFile[2];
  file.messageType[0].field.push(field("at", 3, 11, { typeName: ".google.protobuf.Timestamp" }));
  file.service[0].method.push({
    name: "Ping",
    inputType: ".google.protobuf.Empty",
    outputType: ".google.protobuf.Empty",
  });

  const types = fileContent(generate(request), "types/v1/values.trevrpc.d.ts");
  assert.match(
    types,
    /import type \{ CallOptions, ClientOptions, Empty, EmptyInit, RpcServiceDescriptor, Timestamp, TimestampInit, Transport \} from/,
  );
  assert.doesNotMatch(types, /interface Timestamp|interface Empty/);
  assert.match(types, /at: Timestamp \| null;/);
  assert.match(types, /at\?: TimestampInit \| null;/);
  assert.match(types, /ping\(request: EmptyInit, options\?: CallOptions\): Promise<Empty>;/);

  // The runtime declares Timestamp's seconds as a bigint, so other representations declare their own.
  request.parameter = "int64=number";
  const numbers = fileContent(generate(request), "types/v1/values.trevrpc.d.ts");
  assert.match(numbers, /export interface Timestamp \{\n  seconds: number;\n  nanos: number;\n\}/);
  assert.doesNotMatch(numbers, /interface Empty/);
});

test("the generated protobuf root describes nested messages, enums, maps and oneofs", () => {
  const js = fileContent(generate(longAndMapRequest()), "types/v1/values.trevrpc.js");
  const root = protobuf.Root.fromJSON(rootJson(js));
  const Values = root.lookupType("types.v1.Values");

  assert.equal(Values.fields.count.type, "int64");
  assert.equal(Values.fields.labels.keyType, "int32");
  assert.equal(Values.fields.labels.type, "string");
});

test("method names are valid, unique JavaScript identifiers", () => {
  const request = greeterRequest();
  request.protoFile[0].service[0].method = ["delete", "Delete", "get_value", "GetValue"].map(
    (name) => ({ name, inputType: ".hello.v1.HelloRequest", outputType: ".hello.v1.HelloReply" }),
  );
  const js = fileContent(generate(request), "hello/v1/greeter.trevrpc.js");

  for (const name of ["delete_", "delete_2", "getValue", "getValue2"]) {
    assert.match(js, new RegExp(`\\n  ${name}\\(request, options = \\{\\}\\)`), name);
  }
});

test("plugin options select imports and file names", () => {
  const options = parseOptions("runtime_import=../src/index.js,file_suffix=_rpc.js");
  assert.equal(options.runtimeImport, "../src/index.js");
  assert.equal(options.runtimeTypeImport, "../src/index.js");

  const files = generate(greeterRequest("runtime_import=../x.js,file_suffix=_rpc.js")).file;
  assert.deepEqual(
    files.map((file) => file.name),
    ["hello/v1/greeter_rpc.js", "hello/v1/greeter_rpc.d.ts"],
  );

  assert.equal(
    parseOptions("runtime_import=https://esm.sh/x,runtime_type_import=@trevrpc/trevrpc-js")
      .runtimeTypeImport,
    "@trevrpc/trevrpc-js",
  );
  for (const bad of ["unknown=1", "novalue", "runtime_import=https://esm.sh/x"]) {
    assert.match(generate(greeterRequest(bad)).error, /trevrpc-js|runtime_type_import/, bad);
  }
  assert.match(
    generate(greeterRequest("node_runtime_import=x")).error,
    /unknown trevrpc-js option/,
  );
});

test("files without services produce no output", () => {
  const request = longAndMapRequest();
  request.protoFile[0].service = [];
  assert.deepEqual(generate(request).file, []);
});

test("protoc runs the plugin and its output works against the runtime", async () => {
  const output = new URL("build/test-generated/", packageRoot);
  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });
  await run("protoc", [
    `--plugin=protoc-gen-trevrpc-js=${fileURLToPath(new URL("bin/protoc-gen-trevrpc-js.js", packageRoot))}`,
    `--trevrpc-js_out=${fileURLToPath(output)}`,
    `--proto_path=${fileURLToPath(new URL("../interop/proto", packageRoot))}`,
    "interop.proto",
  ]);

  const generated = await import(pathToFileURL(fileURLToPath(output) + "interop.trevrpc.js").href);
  assert.equal(generated.EchoService.fullName, "trevrpc.interop.v1.Echo");
  assert.deepEqual(Object.keys(generated.EchoService.methods), [
    "unary",
    "clientStream",
    "serverStream",
    "bidi",
  ]);
  assert.equal(generated.root.lookupType("trevrpc.interop.v1.EchoRequest").fields.failCode.id, 3);

  const requests = [];
  const client = generated.createEchoClient({
    createBidirectionalStream() {
      requests.push("opened");
      return Promise.reject(new Error("stop here"));
    },
  });
  await assert.rejects(client.unary({ message: "x" }), (error) => error.code === 14);
  assert.deepEqual(requests, ["opened"]);

  const types = await readFile(new URL("interop.trevrpc.d.ts", output), "utf8");
  assert.match(types, /from "@trevrpc\/trevrpc-js";/);
});

test("protoc accepts proto3 optional fields and the root keeps their presence", async () => {
  const output = new URL("build/test-optional/", packageRoot);
  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });
  await writeFile(
    new URL("optional.proto", output),
    `syntax = "proto3";
package optional.v1;
message Sub { string x = 1; }
message Values {
  optional string name = 1;
  optional int32 count = 2;
  string plain = 3;
  Sub sub = 4;
  oneof choice {
    int32 a = 5;
    string b = 6;
  }
}
service OptionalService { rpc Get(Values) returns (Values); }
`,
  );
  await run("protoc", [
    `--plugin=protoc-gen-trevrpc-js=${fileURLToPath(new URL("bin/protoc-gen-trevrpc-js.js", packageRoot))}`,
    `--trevrpc-js_out=${fileURLToPath(output)}`,
    `--proto_path=${fileURLToPath(output)}`,
    "optional.proto",
  ]);

  const js = await readFile(new URL("optional.trevrpc.js", output), "utf8");
  const Values = protobuf.Root.fromJSON(rootJson(js)).lookupType("optional.v1.Values");
  // Fields outside a oneof stay out of every oneof, including synthetic ones.
  assert.deepEqual(
    Object.fromEntries(Values.oneofsArray.map((oneof) => [oneof.name, oneof.oneof])),
    { choice: ["a", "b"], _name: ["name"], _count: ["count"] },
  );

  const decoded = Values.decode(Values.encode({ name: "", plain: "p", a: 1 }).finish());
  assert.equal(decoded.name, "");
  assert.equal(decoded.count, null);
  assert.equal(decoded.plain, "p");
  assert.equal(decoded.a, 1);

  const types = await readFile(new URL("optional.trevrpc.d.ts", output), "utf8");
  assert.match(types, /name: string \| null;/);
  assert.match(types, /name\?: string \| null;/);
  assert.match(types, /plain: string;/);
  assert.match(types, /plain\?: string;/);
  assert.match(types, /sub: Sub \| null;/);
  assert.match(types, /sub\?: SubInit \| null;/);
});

test("the response declares proto3 optional support, with or without an error", () => {
  assert.equal(generate(greeterRequest()).supportedFeatures, 1);
  assert.equal(generate(greeterRequest("unknown=1")).supportedFeatures, 1);
});

test("the generator reads the trevrpc.method option into the method descriptors", () => {
  const request = greeterRequest();
  const [unary, serverStream, clientStream, bidi] = request.protoFile[0].service[0].method;
  assert.equal(unary.name, "SayHello");
  unary.options = { trevrpcMethod: {} };
  serverStream.options = { trevrpcMethod: { unreliableResponses: true } };
  clientStream.options = { trevrpcMethod: { unreliableRequests: true } };
  bidi.options = { trevrpcMethod: { unreliableRequests: true, unreliableResponses: true } };

  const js = fileContent(generate(request), "hello/v1/greeter.trevrpc.js");
  const service = JSON.parse(
    js.slice(js.indexOf("Object.freeze(") + 14, js.indexOf(");\n\n/** Client")),
  );
  const flags = Object.fromEntries(
    Object.entries(service.methods).map(([name, method]) => [
      name,
      [method.unreliableRequests, method.unreliableResponses],
    ]),
  );
  assert.deepEqual(flags, {
    sayHello: [undefined, undefined],
    lotsOfReplies: [undefined, true],
    lotsOfGreetings: [true, undefined],
    bidiHello: [true, true],
  });
});

test("the option is rejected on a direction that does not stream", () => {
  const requests = "unreliable_requests requires a client-streaming or bidirectional method";
  const responses = "unreliable_responses requires a server-streaming or bidirectional method";
  const cases = [
    { name: "SayHello", kind: {}, flags: { unreliableRequests: true }, message: requests },
    { name: "SayHello", kind: {}, flags: { unreliableResponses: true }, message: responses },
    {
      name: "LotsOfReplies",
      kind: { serverStreaming: true },
      flags: { unreliableRequests: true },
      message: requests,
    },
    {
      name: "LotsOfGreetings",
      kind: { clientStreaming: true },
      flags: { unreliableResponses: true },
      message: responses,
    },
  ];
  for (const { name, kind, flags, message } of cases) {
    const request = greeterRequest();
    request.protoFile[0].service[0].method = [
      method(name, { ...kind, options: { trevrpcMethod: flags } }),
    ];
    assert.equal(generate(request).error, `hello.v1.Greeter.${name}: ${message}`, name);
  }
});

test("COMPRESSION_STREAM applies to the streamed directions whose types have no redacted field", () => {
  const request = greeterRequest();
  const file = request.protoFile[0];
  // Secret is redacted; Wrapper holds one two levels down; Loop refers to itself.
  file.messageType.push(
    { name: "Secret", field: [field("token", 1, 9, { options: { debugRedact: true } })] },
    { name: "Middle", field: [field("secret", 1, 11, { typeName: ".hello.v1.Secret" })] },
    { name: "Wrapper", field: [field("middle", 1, 11, { typeName: ".hello.v1.Middle" })] },
    { name: "Loop", field: [field("next", 1, 11, { typeName: ".hello.v1.Loop" })] },
  );
  const compress = { trevrpcMethod: { compression: 2 } };
  file.service[0].method = [
    method("Plain", { clientStreaming: true, serverStreaming: true, options: compress }),
    method("Server", { serverStreaming: true, options: compress }),
    method("Client", { clientStreaming: true, options: compress }),
    method("Redacted", {
      clientStreaming: true,
      serverStreaming: true,
      inputType: ".hello.v1.Wrapper",
      outputType: ".hello.v1.Loop",
      options: compress,
    }),
    method("Unmarked", { clientStreaming: true, serverStreaming: true }),
  ];

  const js = fileContent(generate(request), "hello/v1/greeter.trevrpc.js");
  const service = JSON.parse(
    js.slice(js.indexOf("Object.freeze(") + 14, js.indexOf(");\n\n/** Client")),
  );
  const flags = Object.fromEntries(
    Object.entries(service.methods).map(([name, method]) => [
      name,
      [method.compressStreamRequests, method.compressStreamResponses],
    ]),
  );
  assert.deepEqual(flags, {
    plain: [true, true],
    server: [undefined, true],
    client: [true, undefined],
    redacted: [undefined, true],
    unmarked: [undefined, undefined],
  });

  file.service[0].method = [method("SayHello", { options: compress })];
  assert.equal(
    generate(request).error,
    "hello.v1.Greeter.SayHello: COMPRESSION_STREAM requires a streaming method",
  );

  file.service[0].method = [
    method("Upload", { clientStreaming: true, options: { trevrpcMethod: { compression: 7 } } }),
  ];
  assert.equal(generate(request).error, "hello.v1.Greeter.Upload: unknown compression 7");
});

test("COMPRESSION_MESSAGE and COMPRESSION_STREAM allow compression without a dictionary except for redacted types", () => {
  const request = greeterRequest();
  const file = request.protoFile[0];
  file.messageType.push({
    name: "Secret",
    field: [field("token", 1, 9, { options: { debugRedact: true } })],
  });
  file.service[0].method = [
    method("Unary", { options: { trevrpcMethod: { compression: 1 } } }),
    method("Stream", {
      clientStreaming: true,
      serverStreaming: true,
      options: { trevrpcMethod: { compression: 2 } },
    }),
    method("Redacted", {
      inputType: ".hello.v1.Secret",
      options: { trevrpcMethod: { compression: 1 } },
    }),
    method("Unmarked"),
  ];

  const js = fileContent(generate(request), "hello/v1/greeter.trevrpc.js");
  const service = JSON.parse(
    js.slice(js.indexOf("Object.freeze(") + 14, js.indexOf(");\n\n/** Client")),
  );
  const flags = Object.fromEntries(
    Object.entries(service.methods).map(([name, method]) => [
      name,
      [method.compressRequests, method.compressResponses],
    ]),
  );
  assert.deepEqual(flags, {
    unary: [true, true],
    stream: [true, true],
    redacted: [undefined, true],
    unmarked: [undefined, undefined],
  });
});

test("protoc passes the trevrpc.method option through to the generated descriptor", async () => {
  const output = new URL("build/test-generated-unreliable/", packageRoot);
  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });
  const protoc = (...files) =>
    run("protoc", [
      `--plugin=protoc-gen-trevrpc-js=${fileURLToPath(new URL("bin/protoc-gen-trevrpc-js.js", packageRoot))}`,
      `--trevrpc-js_out=${fileURLToPath(output)}`,
      `--proto_path=${fileURLToPath(new URL("../interop/proto", packageRoot))}`,
      `--proto_path=${fileURLToPath(new URL("../proto", packageRoot))}`,
      `--proto_path=${fileURLToPath(output)}`,
      ...files,
    ]);

  await protoc("compressed.proto");
  const compressed = await import(
    pathToFileURL(fileURLToPath(output) + "compressed.trevrpc.js").href
  );
  const compressedBidi = compressed.CompressedEchoService.methods.bidi;
  assert.equal(compressedBidi.compressStreamRequests, true);
  assert.equal(compressedBidi.compressStreamResponses, true);
  const plain = compressed.CompressedEchoService.methods.plain;
  assert.equal(plain.compressRequests, true);
  assert.equal(plain.compressResponses, true);

  await protoc("unreliable.proto");
  const generated = await import(
    pathToFileURL(fileURLToPath(output) + "unreliable.trevrpc.js").href
  );
  const method = generated.UnreliableEchoService.methods.bidi;
  assert.equal(method.kind, "bidirectionalStreaming");
  assert.equal(method.unreliableRequests, true);
  assert.equal(method.unreliableResponses, true);

  // A misplaced flag fails protoc with the generator's message.
  await writeFile(
    new URL("misuse.proto", output),
    `syntax = "proto3";
package misuse.v1;
import "trevrpc/options.proto";
message M {}
service S {
  rpc Unary(M) returns (M) { option (trevrpc.method) = {unreliable_responses: true}; }
}
`,
  );
  await assert.rejects(protoc("misuse.proto"), (error) =>
    error.stderr.includes(
      "misuse.v1.S.Unary: unreliable_responses requires a server-streaming or bidirectional method",
    ),
  );
});

test("runGenerator reports errors in the plugin response", () => {
  assert.throws(() => runGenerator(new Uint8Array([0xff])));
});

/** The JSON namespace a generated file passes to createRoot. */
function rootJson(js) {
  const start = js.indexOf("createRoot(") + "createRoot(".length;
  return JSON.parse(js.slice(start, js.indexOf(", { int64: ", start)));
}

function fileContent(response, name) {
  assert.equal(response.error, undefined);
  const file = response.file.find((candidate) => candidate.name === name);
  assert.ok(file, `missing generated file ${name}`);
  return file.content;
}

const method = (name, extra = {}) => ({
  name,
  inputType: ".hello.v1.HelloRequest",
  outputType: ".hello.v1.HelloReply",
  ...extra,
});

function field(name, number, type, extra = {}) {
  return { name, number, label: 1, type, jsonName: name, ...extra };
}

function greeterRequest(parameter = "") {
  return {
    parameter,
    fileToGenerate: ["hello/v1/greeter.proto"],
    protoFile: [
      {
        name: "hello/v1/greeter.proto",
        package: "hello.v1",
        messageType: [
          { name: "HelloRequest", field: [field("name", 1, 9)] },
          { name: "HelloReply", field: [field("message", 1, 9)] },
        ],
        service: [
          {
            name: "Greeter",
            method: [
              method("SayHello"),
              method("LotsOfReplies", { serverStreaming: true }),
              method("LotsOfGreetings", { clientStreaming: true }),
              method("BidiHello", { clientStreaming: true, serverStreaming: true }),
            ],
          },
        ],
      },
    ],
  };
}

function longAndMapRequest(parameter = "") {
  return {
    parameter,
    fileToGenerate: ["types/v1/values.proto"],
    protoFile: [
      {
        name: "types/v1/values.proto",
        package: "types.v1",
        messageType: [
          {
            name: "Values",
            field: [
              field("count", 1, 3),
              field("labels", 2, 11, { label: 3, typeName: ".types.v1.Values.LabelsEntry" }),
            ],
            nestedType: [
              {
                name: "LabelsEntry",
                options: { mapEntry: true },
                field: [field("key", 1, 5), field("value", 2, 9)],
              },
            ],
          },
        ],
        service: [
          {
            name: "ValuesService",
            method: [
              { name: "GetValues", inputType: ".types.v1.Values", outputType: ".types.v1.Values" },
            ],
          },
        ],
      },
    ],
  };
}

function importedTypeRequest() {
  return {
    fileToGenerate: ["hello/v1/greeter.proto"],
    protoFile: [
      {
        name: "shared/v1/request.proto",
        package: "shared.v1",
        messageType: [{ name: "SharedRequest", field: [field("name", 1, 9)] }],
      },
      {
        name: "hello/v1/greeter.proto",
        package: "hello.v1",
        dependency: ["shared/v1/request.proto"],
        messageType: [{ name: "HelloReply", field: [field("message", 1, 9)] }],
        service: [
          {
            name: "Greeter",
            method: [
              {
                name: "SayHello",
                inputType: ".shared.v1.SharedRequest",
                outputType: ".hello.v1.HelloReply",
              },
            ],
          },
        ],
      },
    ],
  };
}
