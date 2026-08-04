import assert from "node:assert/strict";

import { Code, createStreamingResponse, createUnaryResponse } from "trevrpc-js";
import { Channel, NodeServer } from "trevrpc-js/node";

import { registerAllShapesServer } from "./generated/all-shapes.node.trevrpc.js";
import { AllShapesClient } from "./generated/all-shapes.trevrpc.js";

const server = await NodeServer.listen({
  host: "127.0.0.1",
  port: 0,
  certFile: process.env.TREVRPC_TEST_CERT,
  keyFile: process.env.TREVRPC_TEST_KEY,
});

registerAllShapesServer(server, {
  unary(request) {
    // Regression: this is a direct protobuf object, never a response envelope.
    return { message: `unary:${request.value}` };
  },
  async clientStream(requests) {
    const values = [];
    for await (const request of requests) values.push(request.value);
    return createUnaryResponse(
      { message: `client:${values.join(",")}` },
      { "terminal-shape": "client" },
    );
  },
  serverStream(request) {
    return createStreamingResponse(
      [{ message: `server:${request.value}:1` }, { message: `server:${request.value}:2` }],
      { code: Code.Ok, message: "server-complete", metadata: { "terminal-shape": "server" } },
    );
  },
  async *bidi(requests) {
    for await (const request of requests) yield { message: `bidi:${request.value}` };
  },
});

const serving = server.serve();
const channel = await Channel.connect({
  host: "127.0.0.1",
  port: server.port,
  skipCertificateValidation: true,
  timeoutMs: 10_000,
});

try {
  const client = new AllShapesClient(channel, { timeoutMs: 10_000 });
  assert.equal((await client.unary({ value: "one" })).message, "unary:one");

  const clientStream = await client.clientStream();
  await clientStream.sendMany([{ value: "one" }, { value: "two" }]);
  const clientResult = await clientStream.closeAndRecvWithResponse();
  assert.equal(clientResult.message.message, "client:one,two");
  assert.equal(new TextDecoder().decode(clientResult.metadata["terminal-shape"]), "client");

  const serverStream = await client.serverStream({ value: "one" });
  const serverMessages = [];
  for await (const message of serverStream) serverMessages.push(message.message);
  assert.deepEqual(serverMessages, ["server:one:1", "server:one:2"]);
  const serverStatus = await serverStream.status;
  assert.equal(serverStatus.message, "server-complete");
  assert.equal(new TextDecoder().decode(serverStatus.metadata["terminal-shape"]), "server");

  const bidi = await client.bidi();
  await bidi.sendMany([{ value: "one" }, { value: "two" }]);
  await bidi.closeSend();
  const bidiMessages = [];
  for await (const message of bidi) bidiMessages.push(message.message);
  assert.deepEqual(bidiMessages, ["bidi:one", "bidi:two"]);
  assert.equal((await bidi.status).code, Code.Ok);
} finally {
  channel.close();
  server.close();
  await serving;
}
