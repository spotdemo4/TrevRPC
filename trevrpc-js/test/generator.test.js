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

  assert.match(types, /export interface HelloRequest \{\s*name\?: string;\s*\}/);
  assert.match(types, /export interface HelloReply/);
  assert.match(
    types,
    /sayHello\(request: HelloRequest, options\?: CallOptions\): Promise<HelloReply>;/,
  );
  assert.doesNotMatch(types, /WithResponse|UnaryResponse/);
  assert.match(types, /lotsOfReplies\([\s\S]*Promise<ResponseStream<HelloReply>>;/);
  assert.match(
    types,
    /lotsOfGreetings\(options\?: CallOptions\): Promise<ClientStreamingCall<HelloRequest, HelloReply>>;/,
  );
  assert.match(
    types,
    /bidiHello\(options\?: CallOptions\): Promise<BidirectionalStreamingCall<HelloRequest, HelloReply>>;/,
  );
  assert.match(
    types,
    /import type \{ BidirectionalStreamingCall, CallOptions, ClientStreamingCall, ResponseStream, RpcServiceDescriptor, Transport \} from "@trevrpc\/trevrpc-js";/,
  );
  assert.match(types, /import type \{ Root \} from "protobufjs";/);
});

test("declarations import only the types they use", () => {
  const types = fileContent(generate(longAndMapRequest()), "types/v1/values.trevrpc.d.ts");

  assert.match(types, /import type \{ Long, Root \} from "protobufjs";/);
  assert.match(types, /count\?: number \| string \| Long;/);
  assert.match(types, /labels\?: Record<string, string>;/);
  assert.match(types, /import type \{ CallOptions, RpcServiceDescriptor, Transport \} from/);
  assert.match(types, /getValues\(request: Values, options\?: CallOptions\): Promise<Values>;/);
});

test("declarations include message types from imported files", () => {
  const types = fileContent(generate(importedTypeRequest()), "hello/v1/greeter.trevrpc.d.ts");

  assert.match(types, /export interface SharedRequest/);
  assert.match(types, /sayHello\(request: SharedRequest/);
});

test("the generated protobuf root describes nested messages, enums, maps and oneofs", () => {
  const js = fileContent(generate(longAndMapRequest()), "types/v1/values.trevrpc.js");
  const json = JSON.parse(js.slice(js.indexOf("createRoot(") + 11, js.indexOf(");\n")));
  const root = protobuf.Root.fromJSON(json);
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

test("compress_stream applies to the streamed directions whose types have no redacted field", () => {
  const request = greeterRequest();
  const file = request.protoFile[0];
  // Secret is redacted; Wrapper holds one two levels down; Loop refers to itself.
  file.messageType.push(
    { name: "Secret", field: [field("token", 1, 9, { options: { debugRedact: true } })] },
    { name: "Middle", field: [field("secret", 1, 11, { typeName: ".hello.v1.Secret" })] },
    { name: "Wrapper", field: [field("middle", 1, 11, { typeName: ".hello.v1.Middle" })] },
    { name: "Loop", field: [field("next", 1, 11, { typeName: ".hello.v1.Loop" })] },
  );
  const compress = { trevrpcMethod: { compressStream: true } };
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
    "hello.v1.Greeter.SayHello: compress_stream requires a streaming method",
  );
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

function longAndMapRequest() {
  return {
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
