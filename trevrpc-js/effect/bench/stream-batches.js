// Measures what a server stream costs per message in @trevrpc/effect, over the in-memory server
// of trevrpc-js's tests, which writes the response in transport-sized chunks that carry many
// frames each, as WebTransport reads do. Not part of the published package.
//
//   npm run bench -- [--messages 10000] [--size 32] [--chunk 65536] [--runs 7]
//
// Each lane receives the same server stream: `for await` over the plain client, `batches()` of the
// plain client, and the Effect stream consumed with runForEach (one effect per message) and with
// runForEachArray (one per Effect chunk). Besides the median and best time per message, it reports
// how many chunks (or batches) the consumer saw.
import { parseArgs } from "node:util";

import { createClient } from "@trevrpc/trevrpc-js";
import { Effect, Stream } from "effect";

import { encodeMessageFrame, encodeStatusFrame, marshalMessage } from "../../src/framing.js";
import { fakeTransport } from "../../test/support.js";
import { Echo, EchoResponseSchema } from "../build/test/gen/echo_pb.js";
import { createEffectClient } from "../src/index.js";

const { values } = parseArgs({
  options: {
    messages: { type: "string", default: "10000" },
    size: { type: "string", default: "32" },
    chunk: { type: "string", default: "65536" },
    runs: { type: "string", default: "7" },
  },
});
const messages = Number(values.messages);
const size = Number(values.size);
const chunkSize = Number(values.chunk);
const runs = Number(values.runs);

const response = encodeResponse();
const transport = fakeTransport(async (stream) => {
  await stream.readRequest();
  for (let offset = 0; offset < response.byteLength; offset += chunkSize) {
    await stream.write(response.subarray(offset, offset + chunkSize));
  }
  await stream.finish();
  await stream.stopSending();
});
const plain = createClient(Echo, transport);
const effect = createEffectClient(Echo, transport);

/** The whole response: `messages` MESSAGE frames of `size` bytes, then an OK STATUS. */
function encodeResponse() {
  const body = marshalMessage(EchoResponseSchema, { message: "x".repeat(size - 2) });
  const frames = [];
  for (let index = 0; index < messages; index += 1) {
    frames.push(encodeMessageFrame(body));
  }
  frames.push(encodeStatusFrame({ code: 0, message: "", metadata: {} }));
  const out = new Uint8Array(frames.reduce((sum, frame) => sum + frame.byteLength, 0));
  let offset = 0;
  for (const frame of frames) {
    out.set(frame, offset);
    offset += frame.byteLength;
  }
  return out;
}

const lanes = {
  "for await": async () => {
    let received = 0;
    for await (const _ of await plain.serverStream({})) {
      received += 1;
    }
    return { received, chunks: received };
  },
  batches: async () => {
    const stream = await plain.serverStream({});
    if (typeof stream.batches !== "function") {
      return undefined;
    }
    let received = 0;
    let chunks = 0;
    for await (const batch of stream.batches()) {
      received += batch.length;
      chunks += 1;
    }
    return { received, chunks };
  },
  "effect runForEach": async () => {
    let received = 0;
    await Effect.runPromise(
      Stream.runForEach(effect.serverStream({}), () =>
        Effect.sync(() => {
          received += 1;
        }),
      ),
    );
    return { received, chunks: received };
  },
  "effect runForEachArray": async () => {
    let received = 0;
    let chunks = 0;
    await Effect.runPromise(
      Stream.runForEachArray(effect.serverStream({}), (chunk) =>
        Effect.sync(() => {
          received += chunk.length;
          chunks += 1;
        }),
      ),
    );
    return { received, chunks };
  },
};

console.log(
  `${messages} messages of ${size} bytes, ${(response.byteLength / 1024).toFixed(0)} KiB in ` +
    `${chunkSize}-byte transport chunks, ${runs} runs (node ${process.version})`,
);
for (const [name, lane] of Object.entries(lanes)) {
  const times = [];
  let last;
  // The first run warms the JIT up and is not reported.
  for (let run = 0; run <= runs; run += 1) {
    const began = process.hrtime.bigint();
    last = await lane();
    const elapsed = Number(process.hrtime.bigint() - began);
    if (last === undefined) {
      break;
    }
    if (last.received !== messages) {
      throw new Error(`${name}: received ${last.received} of ${messages} messages`);
    }
    if (run > 0) {
      times.push(elapsed / messages);
    }
  }
  if (last === undefined) {
    console.log(`${name.padEnd(24)} not available`);
    continue;
  }
  times.sort((a, b) => a - b);
  const median = times[Math.floor(times.length / 2)];
  console.log(
    `${name.padEnd(24)} median ${median.toFixed(0).padStart(6)} ns/msg   ` +
      `best ${times[0].toFixed(0).padStart(6)} ns/msg   ${String(last.chunks).padStart(6)} chunks`,
  );
}
await transport.settled();
