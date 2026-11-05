import type { JsonObject } from "@bufbuild/protobuf";
import type { MessageInitShape } from "@bufbuild/protobuf";
import { timestampDate, timestampFromDate, type Timestamp } from "@bufbuild/protobuf/wkt";
import { createClient } from "@trevrpc/trevrpc-js";

import {
  Sample_Kind,
  Samples,
  TimeInterval,
  type Sample,
  type SampleSchema,
} from "../../build/test/gen/types_pb.js";

const client = createClient(Samples, {} as never);

// Requests take protobuf-es init objects: every field optional, oneofs as { case, value }.
const request: MessageInitShape<typeof SampleSchema> = {
  count: 1n,
  total: 2n,
  exact: "3",
  history: [1n, 2n],
  interval: TimeInterval.DAY,
  at: timestampFromDate(new Date()),
  attributes: { host: "a", tags: ["x"] },
  parent: { name: "p" },
  value: { case: "counter", value: 9n },
};
await client.put(request);
await client.get({});

// Responses are messages: scalars, arrays and maps are always there, message fields may be absent.
const sample: Sample = await client.get({});
sample.name satisfies string;
sample.count satisfies bigint;
sample.exact satisfies string;
sample.history satisfies bigint[];
sample.sums satisfies Record<string, bigint>;
sample.maybe satisfies bigint | undefined;
sample.kind satisfies Sample_Kind;
sample.children satisfies Sample[];
sample.parent satisfies Sample | undefined;
sample.at satisfies Timestamp | undefined;
sample.attributes satisfies JsonObject | undefined;
if (sample.value.case === "counter") {
  sample.value.value satisfies bigint;
}
if (sample.at !== undefined) {
  timestampDate(sample.at) satisfies Date;
}
// A decoded message can be sent again.
await client.put(sample);
// @ts-expect-error Message fields can be absent.
export const _parentName = sample.parent.name;
// @ts-expect-error A oneof member is set through its case.
await client.put({ counter: 9n });

for await (const listed of await client.list({})) {
  listed.total satisfies bigint;
}
