import assert from "node:assert/strict";
import test from "node:test";

import { TrevRpcError } from "../src/status.js";
import {
  NullValue,
  durationFromMs,
  durationMs,
  structFromJson,
  structToJson,
  timestampDate,
  timestampFromDate,
  timestampFromMs,
  timestampMs,
  valueFromJson,
  valueToJson,
} from "../src/wkt.js";

test("timestamps convert to and from dates and milliseconds", () => {
  assert.deepEqual(timestampFromMs(1_700_000_000_123), {
    seconds: 1_700_000_000n,
    nanos: 123_000_000,
  });
  // Before the epoch, seconds round down so that nanos stay non-negative.
  assert.deepEqual(timestampFromMs(-1500), { seconds: -2n, nanos: 500_000_000 });
  assert.deepEqual(timestampFromDate(new Date(0)), { seconds: 0n, nanos: 0 });

  assert.equal(timestampMs({ seconds: -2n, nanos: 500_000_000 }), -1500);
  assert.equal(timestampMs({ seconds: "10", nanos: 999_999 }), 10_000);
  assert.equal(timestampMs({ seconds: 10 }), 10_000);
  // A Long, as protobuf.js decodes with int64=long.
  assert.equal(timestampMs({ seconds: { low: 5, high: 0, unsigned: false }, nanos: null }), 5000);
  assert.equal(timestampMs({}), 0);
  assert.equal(timestampDate({ seconds: 1n, nanos: 2_000_000 }).getTime(), 1002);
  assert.throws(() => timestampFromMs(Number.NaN), TrevRpcError);
});

test("durations convert to and from milliseconds, keeping seconds and nanos the same sign", () => {
  assert.deepEqual(durationFromMs(1500), { seconds: 1n, nanos: 500_000_000 });
  assert.deepEqual(durationFromMs(-1500), { seconds: -1n, nanos: -500_000_000 });
  assert.deepEqual(durationFromMs(-0.5), { seconds: 0n, nanos: 0 });
  assert.equal(durationMs({ seconds: -1n, nanos: -500_000_000 }), -1500);
  assert.equal(durationMs({ seconds: 2n, nanos: 1_999_999 }), 2001);
  assert.throws(() => durationFromMs(Number.POSITIVE_INFINITY), TrevRpcError);
});

test("structs and values convert to and from JSON", () => {
  const json = { a: 1, b: "two", c: true, d: null, e: [1, { f: "g" }], h: {} };
  const struct = structFromJson(json);
  assert.deepEqual(struct.fields.a, { numberValue: 1 });
  assert.deepEqual(struct.fields.d, { nullValue: NullValue.NULL_VALUE });
  assert.deepEqual(structToJson(struct), json);
  assert.deepEqual(valueToJson(valueFromJson([null, false])), [null, false]);
  assert.equal(valueToJson({}), null);
  assert.deepEqual(structToJson({}), {});
  assert.throws(() => structFromJson([]), TrevRpcError);
  assert.throws(() => valueFromJson(undefined), TrevRpcError);
});
