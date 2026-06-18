import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import {
  Code,
  RpcKind,
  RpcRequest,
  RpcResponse,
  WireVersion,
  createRoot,
  decodeFrame,
  encodeFrame,
  normalizeMetadata,
  unary
} from "../src/index.js";
import { generate as generateBindings } from "../src/generator.js";

test("frames round-trip TrevRPC requests", () => {
  const frame = encodeFrame(RpcRequest, {
    service: "hello.v1.Greeter",
    method: "SayHello",
    body: new Uint8Array([1, 2, 3]),
    metadata: normalizeMetadata({ Authorization: "Bearer token" }),
    kind: RpcKind.Unary,
    version: WireVersion,
    deadlineUnixNanos: "0"
  });

  const decoded = decodeFrame(RpcRequest, frame.subarray(4));

  assert.equal(decoded.service, "hello.v1.Greeter");
  assert.equal(decoded.method, "SayHello");
  assert.deepEqual(decoded.body, new Uint8Array([1, 2, 3]));
  assert.equal(decoded.kind, RpcKind.Unary);
  assert.equal(decoded.version, WireVersion);
  assert.deepEqual(decoded.metadata.authorization, new Uint8Array([66, 101, 97, 114, 101, 114, 32, 116, 111, 107, 101, 110]));
});

test("unary decodes protobuf.js response bodies", async () => {
  const root = createRoot({
    nested: {
      hello: {
        nested: {
          v1: {
            nested: {
              Hello: {
                fields: {
                  value: { type: "string", id: 1 }
                }
              }
            }
          }
        }
      }
    }
  });
  const Hello = root.lookupType("hello.v1.Hello");

  const transport = {
    async call(request) {
      const decoded = Hello.decode(request.body);
      return RpcResponse.create({
        status: Code.Ok,
        body: Hello.encode({ value: `hello ${decoded.value}` }).finish(),
        metadata: {}
      });
    }
  };

  const response = await unary(transport, "hello.v1.Greeter", "SayHello", Hello, Hello, { value: "Trev" });

  assert.equal(response.value, "hello Trev");
});

test("generator emits JavaScript service clients", () => {
  const response = generateBindings(greeterRequest());

  assert.equal(response.error, undefined);
  assert.equal(response.file.length, 1);
  assert.equal(response.file[0].name, "hello/v1/greeter.trevrpc.js");
  assert.match(response.file[0].content, /export class GreeterClient/);
  assert.match(response.file[0].content, /sayHello\(request, options = \{\}\)/);
  assert.match(response.file[0].content, /lotsOfReplies\(request, options = \{\}\)/);
  assert.match(response.file[0].content, /lotsOfGreetings\(request, options = \{\}\)/);
  assert.match(response.file[0].content, /bidiHello\(request, options = \{\}\)/);
});

test("generated JavaScript clients call the runtime", async () => {
  const runtimeImport = pathToFileURL(join(process.cwd(), "src", "index.js")).href;
  const response = generateBindings(greeterRequest(`runtime_import=${runtimeImport}`));
  assert.equal(response.error, undefined);

  const directory = await mkdtemp(join(tmpdir(), "trevrpc-js-generated-"));
  const generatedPath = join(directory, "greeter.trevrpc.js");
  await writeFile(generatedPath, response.file[0].content);

  const generated = await import(pathToFileURL(generatedPath).href);
  const HelloRequest = generated.root.lookupType("hello.v1.HelloRequest");
  const HelloReply = generated.root.lookupType("hello.v1.HelloReply");
  const transport = {
    async call(request) {
      const decoded = HelloRequest.decode(request.body);
      assert.equal(request.service, "hello.v1.Greeter");
      assert.equal(request.method, "SayHello");
      return RpcResponse.create({
        status: Code.Ok,
        body: HelloReply.encode({ message: `hello ${decoded.name}` }).finish(),
        metadata: {}
      });
    }
  };

  const client = new generated.GreeterClient(transport);
  const reply = await client.sayHello({ name: "Trev" });

  assert.equal(reply.message, "hello Trev");
});

function greeterRequest(parameter = "") {
  return {
    parameter,
    fileToGenerate: ["hello/v1/greeter.proto"],
    protoFile: [
      {
        name: "hello/v1/greeter.proto",
        package: "hello.v1",
        messageType: [
          {
            name: "HelloRequest",
            field: [{ name: "name", number: 1, label: 1, type: 9, jsonName: "name" }]
          },
          {
            name: "HelloReply",
            field: [{ name: "message", number: 1, label: 1, type: 9, jsonName: "message" }]
          }
        ],
        service: [
          {
            name: "Greeter",
            method: [
              {
                name: "SayHello",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply"
              },
              {
                name: "LotsOfReplies",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
                serverStreaming: true
              },
              {
                name: "LotsOfGreetings",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
                clientStreaming: true
              },
              {
                name: "BidiHello",
                inputType: ".hello.v1.HelloRequest",
                outputType: ".hello.v1.HelloReply",
                clientStreaming: true,
                serverStreaming: true
              }
            ]
          }
        ]
      }
    ]
  };
}
