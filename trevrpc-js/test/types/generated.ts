import {
  NullValue,
  durationFromMs,
  durationMs,
  structFromJson,
  structToJson,
  timestampDate,
  timestampFromDate,
  timestampMs,
  valueToJson,
  type JsonObject,
  type Struct,
  type Timestamp,
  type TimestampInit,
} from "@trevrpc/trevrpc-js";

import * as numbers from "../../build/test/gen-number/types.trevrpc.js";
import {
  Kind,
  Mixed,
  SamplesClient,
  TimeInterval,
  type Sample,
  type SampleInit,
} from "../../build/test/gen/types.trevrpc.js";

declare const client: SamplesClient;

// Enums are values and types.
TimeInterval.HOUR satisfies 1;
const interval: TimeInterval = TimeInterval.DAY;
Kind.GAUGE satisfies Kind;
Mixed.OTHER_B satisfies Mixed;
// @ts-expect-error A number that is not one of the enum's values.
export const _badInterval: TimeInterval = 7;

// Requests take the input shape: every field optional, 64-bit integers in any representation.
const request: SampleInit = {
  count: 1n,
  total: "18446744073709551615",
  small: 3,
  history: [1n, 2, "3"],
  interval,
  at: timestampFromDate(new Date()),
  window: durationFromMs(1500),
  attributes: structFromJson({ host: "a", tags: ["x"] }),
  parent: { name: "p" },
};
await client.put(request);
await client.get({});

// Responses are decoded messages: scalars, arrays and maps are always there.
const sample: Sample = await client.get({});
sample.name satisfies string;
sample.count satisfies bigint;
sample.exact satisfies string;
sample.small satisfies number;
sample.history satisfies bigint[];
sample.sums satisfies Record<string, bigint>;
sample.maybe satisfies bigint | null;
sample.kind satisfies Kind;
sample.children satisfies Sample[];
sample.parent satisfies Sample | null;
sample.at satisfies Timestamp | null;
// A decoded message can be sent again.
await client.put(sample);
// @ts-expect-error Message fields can be null.
export const _parentName = sample.parent.name;

if (sample.at !== null) {
  timestampMs(sample.at) satisfies number;
  timestampDate(sample.at) satisfies Date;
}
if (sample.window !== null) {
  durationMs(sample.window) satisfies number;
}
if (sample.attributes !== null) {
  structToJson(sample.attributes satisfies Struct) satisfies JsonObject;
  valueToJson(sample.attributes.fields["x"]!);
}
NullValue.NULL_VALUE satisfies NullValue;

for await (const listed of await client.list({})) {
  listed.total satisfies bigint;
}

// int64=number declares its own Timestamp, with number seconds, that the helpers still take.
const numbered: numbers.Sample = await new numbers.SamplesClient({} as never).get({});
numbered.count satisfies number;
if (numbered.at !== null) {
  numbered.at.seconds satisfies number;
  timestampMs(numbered.at);
}
({ seconds: 1n }) satisfies TimestampInit satisfies numbers.TimestampInit;
