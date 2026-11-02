import assert from "node:assert/strict";
import test from "node:test";

import * as numbers from "../build/test/gen-number/types.trevrpc.js";
import { Kind, Mixed, SamplesClient, TimeInterval, root } from "../build/test/gen/types.trevrpc.js";
import { createRoot } from "../src/wire.js";
import { structToJson, timestampMs } from "../src/wkt.js";
import { fakeTransport } from "./support.js";

const Sample = root.lookupType("types.v1.Sample");
const Empty = root.lookupType("google.protobuf.Empty");

/** A Samples client whose Get call answers `sample`, encoded with `root`'s Sample type. */
function samplesAnswering(client, sampleRoot, sample) {
  const type = sampleRoot.lookupType("types.v1.Sample");
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await stream.writeMessage(type, sample);
    await stream.writeStatus();
    await stream.finish();
  });
  return new client(transport);
}

const sample = {
  name: "cpu",
  count: -5n,
  total: 2n ** 64n - 1n,
  exact: 2n ** 63n - 1n,
  small: 42n,
  history: [1n, -2n],
  labels: { "-7": "low", "9007199254740993": "high" },
  sums: { a: 3n },
  interval: TimeInterval.DAY,
  kind: Kind.GAUGE,
  at: { seconds: 1_700_000_000n, nanos: 500_000_000 },
  attributes: { fields: { host: { stringValue: "a" }, up: { boolValue: true } } },
  parent: { count: 8n },
  children: [{ total: 1n }],
  counter: 9n,
};

test("enums are exported as frozen objects, without the enum's prefix when every value has it", () => {
  assert.deepEqual({ ...TimeInterval }, { UNSPECIFIED: 0, HOUR: 1, DAY: 2 });
  assert.deepEqual({ ...Kind }, { UNSPECIFIED: 0, GAUGE: 1 });
  assert.deepEqual({ ...Mixed }, { MIXED_A: 0, OTHER_B: 1 });
  assert.ok(Object.isFrozen(TimeInterval));
});

test("decoded responses hold 64-bit integers as bigints by default", async () => {
  const response = await samplesAnswering(SamplesClient, root, sample).get({});
  assert.equal(response.count, -5n);
  assert.equal(response.total, 2n ** 64n - 1n);
  // The jstype option wins over the generator's representation.
  assert.equal(response.exact, "9223372036854775807");
  assert.equal(response.small, 42);
  assert.deepEqual(response.history, [1n, -2n]);
  assert.deepEqual({ ...response.labels }, { "-7": "low", "9007199254740993": "high" });
  assert.deepEqual({ ...response.sums }, { a: 3n });
  assert.equal(response.parent.count, 8n);
  assert.equal(response.parent.total, 0n, "an unset field reads a converted default");
  assert.equal(response.children[0].total, 1n);
  assert.equal(response.counter, 9n);
  assert.equal(response.value, "counter", "the oneof still reports the member that is set");
  assert.equal(response.maybe, null, "an unset optional field reads null");
  assert.equal(response.interval, TimeInterval.DAY);
  assert.equal(response.at.seconds, 1_700_000_000n);
  assert.equal(timestampMs(response.at), 1_700_000_000_500);
  assert.deepEqual(structToJson(response.attributes), { host: "a", up: true });
});

test("a message whose fields are all unset reads converted defaults", async () => {
  const response = await samplesAnswering(SamplesClient, root, {}).get({});
  assert.equal(response.count, 0n);
  assert.equal(response.exact, "0");
  assert.equal(response.small, 0);
  assert.equal(response.counter, 0n);
  assert.equal(response.value, undefined);
  assert.deepEqual(response.history, []);
  assert.equal(response.at, null);
});

test("int64=number decodes 64-bit integers as numbers", async () => {
  const response = await samplesAnswering(numbers.SamplesClient, numbers.root, sample).get({});
  assert.equal(response.count, -5);
  assert.equal(response.small, 42);
  assert.equal(response.exact, "9223372036854775807");
  assert.deepEqual(response.history, [1, -2]);
  assert.equal(response.at.seconds, 1_700_000_000);
  assert.deepEqual(Object.keys(response.labels).sort(), ["-7", "9007199254740993"]);
});

test("requests take 64-bit integers as bigints, numbers or strings", async () => {
  let received;
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    received = Sample.toObject(await stream.readMessage(Sample), { longs: String });
    await stream.writeMessage(Empty, {});
    await stream.writeStatus();
    await stream.finish();
  });
  await new SamplesClient(transport).put({
    count: 2n ** 62n,
    total: "18446744073709551615",
    small: 7,
    history: [1n, "2", 3],
    labels: { "-7": "x" },
  });
  await transport.settled();
  assert.equal(received.count, String(2n ** 62n));
  assert.equal(received.total, "18446744073709551615");
  assert.equal(received.small, "7");
  assert.deepEqual(received.history, ["1", "2", "3"]);
  assert.deepEqual(Object.keys(received.labels).length, 1);
});

test("createRoot rejects an unknown int64 representation", () => {
  assert.throws(() => createRoot({ nested: {} }, { int64: "double" }), /int64 must be one of/u);
});
