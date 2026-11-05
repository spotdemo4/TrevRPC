// What createClient takes from protobuf-es descriptors: method names and kinds, the trevrpc.method
// option of each method, and messages of every kind of field, end to end through a call.
import assert from "node:assert/strict";
import test from "node:test";

import { create, equals } from "@bufbuild/protobuf";
import { timestampDate, timestampFromMs } from "@bufbuild/protobuf/wkt";

import * as methods from "../build/test/gen/methods_pb.js";
import { SampleSchema, Sample_Kind, Samples, TimeInterval } from "../build/test/gen/types_pb.js";
import { createClient } from "../src/client.js";
import { methodSettings } from "../src/method-options.js";
import { Code } from "../src/status.js";
import { fakeTransport } from "./support.js";

/** The settings of every method of `service`, by local name, without its kind. */
function settingsOf(service) {
  return Object.fromEntries(
    service.methods.map((method) => {
      const { kind: _, ...flags } = methodSettings(method);
      return [method.localName, flags];
    }),
  );
}

test("clients have a function per method, named by its local name, that calls it by kind", async () => {
  const kinds = [];
  const transport = fakeTransport(async (stream) => {
    kinds.push((await stream.readRequest()).kind);
    await stream.writeStatus(Code.Unimplemented);
    await stream.finish();
  });
  const client = createClient(methods.Unreliable, transport);
  assert.deepEqual(Object.keys(client), ["unary", "server", "client", "bidi"]);
  const unimplemented = (error) => error.code === Code.Unimplemented;
  await assert.rejects(client.unary({}), unimplemented);
  const server = await client.server({});
  await assert.rejects(server[Symbol.asyncIterator]().next(), unimplemented);
  await assert.rejects((await client.client()).closeAndRecv(), unimplemented);
  const bidi = await client.bidi();
  await bidi.closeSend();
  await assert.rejects(bidi.recv(), unimplemented);
  assert.deepEqual(kinds, [0, 2, 1, 3]);
});

test("the trevrpc.method option marks directions unreliable", () => {
  assert.deepEqual(settingsOf(methods.Unreliable), {
    unary: {},
    server: { unreliableResponses: true },
    client: { unreliableRequests: true },
    bidi: { unreliableRequests: true, unreliableResponses: true },
  });
});

test("COMPRESSION_STREAM stream-compresses the streamed directions whose types have no redacted field", () => {
  const all = { compressRequests: true, compressResponses: true };
  assert.deepEqual(settingsOf(methods.StreamCompressed), {
    bidi: { ...all, compressStreamRequests: true, compressStreamResponses: true },
    server: { ...all, compressStreamResponses: true },
    client: { ...all, compressStreamRequests: true },
    // Wrapper holds a redacted field two levels down; Loop refers to itself.
    redacted: { compressResponses: true, compressStreamResponses: true },
    // The client leaves the unreliable direction out of stream compression when it calls.
    unreliableBidi: {
      ...all,
      unreliableResponses: true,
      compressStreamRequests: true,
      compressStreamResponses: true,
    },
    unmarked: {},
  });
});

test("COMPRESSION_MESSAGE compresses without a dictionary, except types with redacted fields", () => {
  assert.deepEqual(settingsOf(methods.MessageCompressed), {
    unary: { compressRequests: true, compressResponses: true },
    redacted: { compressResponses: true },
    // Redacted messages as the elements of a list and the values of a map.
    redactedElements: {},
    unmarked: {},
  });
});

test("createClient rejects an option the method's kind cannot use", () => {
  const requests = "unreliable_requests requires a client-streaming or bidirectional method";
  const responses = "unreliable_responses requires a server-streaming or bidirectional method";
  for (const [service, message] of [
    [methods.UnaryUnreliableRequests, `methods.v1.UnaryUnreliableRequests.Unary: ${requests}`],
    [methods.UnaryUnreliableResponses, `methods.v1.UnaryUnreliableResponses.Unary: ${responses}`],
    [methods.ServerUnreliableRequests, `methods.v1.ServerUnreliableRequests.Server: ${requests}`],
    [
      methods.ClientUnreliableResponses,
      `methods.v1.ClientUnreliableResponses.Client: ${responses}`,
    ],
    [
      methods.UnaryStreamCompression,
      "methods.v1.UnaryStreamCompression.Unary: COMPRESSION_STREAM requires a streaming method",
    ],
    [methods.UnknownCompression, "methods.v1.UnknownCompression.Client: unknown compression 7"],
  ]) {
    assert.throws(
      () =>
        createClient(
          service,
          fakeTransport(async () => {}),
        ),
      (error) => error.code === Code.InvalidArgument && error.statusMessage === message,
      service.typeName,
    );
  }
});

test("messages of every kind of field travel through a call", async () => {
  const sample = create(SampleSchema, {
    name: "cpu",
    count: -5n,
    total: 2n ** 64n - 1n,
    exact: "9223372036854775807",
    small: 42n,
    history: [1n, -2n],
    labels: { "-7": "low", "9007199254740993": "high" },
    sums: { a: 3n },
    maybe: 0n,
    interval: TimeInterval.DAY,
    kind: Sample_Kind.GAUGE,
    at: timestampFromMs(1_700_000_000_500),
    attributes: { host: "a", up: true, tags: ["x", null] },
    parent: { count: 8n },
    children: [{ total: 1n }],
    value: { case: "counter", value: 9n },
  });
  const received = [];
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    received.push(await stream.readMessage(SampleSchema));
    await stream.writeMessage(SampleSchema, received[0]);
    await stream.writeStatus();
    await stream.finish();
  });
  const client = createClient(Samples, transport);

  const stream = await client.list(sample);
  const responses = [];
  for await (const response of stream) {
    responses.push(response);
  }
  assert.ok(equals(SampleSchema, received[0], sample));
  assert.equal(responses.length, 1);
  const [response] = responses;
  assert.ok(equals(SampleSchema, response, sample));
  assert.equal(response.total, 2n ** 64n - 1n);
  assert.equal(response.maybe, 0n, "an explicitly set optional field keeps its presence");
  assert.equal(response.value.case, "counter");
  assert.equal(timestampDate(response.at).getTime(), 1_700_000_000_500);
  assert.deepEqual(response.attributes, { host: "a", up: true, tags: ["x", null] });
});
