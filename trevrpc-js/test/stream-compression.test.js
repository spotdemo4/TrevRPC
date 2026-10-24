import assert from "node:assert/strict";
import test from "node:test";

import { CompressedEchoClient, root } from "../build/interop/gen/compressed.trevrpc.js";
import { createServiceClient } from "../src/client.js";
import {
  DefaultMaxFrameSize,
  FrameType,
  encodeHeadersFrame,
  encodeOutgoingFrame,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import { Code, TrevRpcError } from "../src/status.js";
import { CallStreams, StreamBudget, StreamBudgetKey } from "../src/stream-compression.js";
import { varintLength, writeVarint } from "../src/varint.js";
import { RpcHeaders, RpcRequest } from "../src/wire.js";
import { createCompression } from "../src/zstd.js";
import { ServerStreams, fakeTransport, hex, readGoldenVectors } from "./support.js";

const EchoRequest = root.lookupType("trevrpc.interop.v1.EchoRequest");
const EchoResponse = root.lookupType("trevrpc.interop.v1.EchoResponse");

const vectors = await readGoldenVectors("zstd-golden-vectors.txt");
const fromHex = (value) => Uint8Array.from(Buffer.from(value, "hex"));
const withDictionary = await createCompression({
  dictionaries: [{ data: fromHex(vectors.get("dict.d1.dictionary")) }],
});
const dictionary = withDictionary.dictionary(1);
const plain = await createCompression();

const isStatus = (code) => (error) => error instanceof TrevRpcError && error.code === code;

/** The i-th message of the interop `compressed-stream` scenario. */
const compressible = (i) =>
  `{"service":"search","region":"us-east-1","status":404,"latency_ms":${100 + i}}`;

/** A client's response side that offered stream compression and accepts dict.d1. */
function responseReceiver() {
  const streams = new CallStreams(
    withDictionary,
    new StreamBudget(),
    { requests: false, responses: true },
    { session: { requestDictionary: () => undefined }, requestType: "", accept: dictionary },
  );
  streams.offer({});
  return streams;
}

/** The names of golden vector groups with this prefix, such as `stream.plain`. */
function groups(prefix) {
  return [
    ...new Set(
      [...vectors.keys()]
        .filter((key) => key.startsWith(prefix))
        .map((key) => key.split(".").slice(0, 2).join(".")),
    ),
  ];
}

/** The payloads and messages of a stream vector group, in order. */
function sequence(group) {
  const items = [];
  for (let i = 0; vectors.has(`${group}.${i}.payload`); i += 1) {
    items.push({
      payload: fromHex(vectors.get(`${group}.${i}.payload`)),
      message: vectors.get(`${group}.${i}.message`),
    });
  }
  return items;
}

test("the stream golden vectors decode in order, and invalid chunks are rejected", () => {
  assert.ok(groups("stream.").length >= 4);
  for (const group of groups("stream.")) {
    const receiver = responseReceiver();
    for (const [index, { payload, message }] of sequence(group).entries()) {
      assert.equal(hex(receiver.receive(payload, 4096)), message, `${group}.${index}`);
    }
  }
  assert.ok(groups("stream_bad.").length >= 9);
  for (const group of groups("stream_bad.")) {
    assert.throws(
      () => responseReceiver().receive(fromHex(vectors.get(`${group}.payload`)), 4096),
      isStatus(Code.Internal),
      group,
    );
  }
  assert.throws(
    () =>
      responseReceiver().receive(
        fromHex(vectors.get("stream_too_large.zeros.payload")),
        DefaultMaxFrameSize,
      ),
    isStatus(Code.ResourceExhausted),
  );
});

test("the stream encoder writes the golden payloads", () => {
  for (const [group, start] of [
    ["stream.plain", undefined],
    ["stream.seeded", dictionary],
    ["stream.empty", undefined],
  ]) {
    const encoder = withDictionary.streamEncoder(start, 16);
    for (const [index, { payload, message }] of sequence(group).entries()) {
      const body = fromHex(message);
      const chunk = encoder.encode(body);
      const written = new Uint8Array(varintLength(body.byteLength) + chunk.byteLength);
      written.set(chunk, writeVarint(written, 0, body.byteLength));
      assert.equal(hex(written), hex(payload), `${group}.${index}`);
    }
    encoder.close();
  }
});

test("the stream negotiation fields match the golden vectors", () => {
  assert.equal(
    hex(marshalMessage(RpcRequest, { method: "/svc/m", streamCompression: { maxWindowLog: 16 } })),
    vectors.get("request.stream_compression.payload"),
  );
  const headers = unmarshalMessage(
    RpcHeaders,
    fromHex(vectors.get("headers.stream_compression.payload")),
  );
  assert.equal(headers.streamCompression.maxWindowLog, 16);
});

/**
 * CompressedEcho.Bidi with stream compression, recording each call as
 * `{ request, requestFrames, responseFrames }`. `options` go to ServerStreams.
 */
function streamEcho(calls, options) {
  return async (stream) => {
    const request = await stream.readRequest();
    const streams = new ServerStreams(plain, request, options);
    const call = { request, requestFrames: [], responseFrames: [] };
    calls.push(call);
    try {
      await stream.write(encodeHeadersFrame({ metadata: {}, ...streams.headers }));
      for (let index = 0, frame; (frame = await stream.readFrame()) !== null; index += 1) {
        call.requestFrames.push(frame.type);
        const { message } = EchoRequest.decode(streams.receive(frame));
        const response = streams.encode(marshalMessage(EchoResponse, { message, index }));
        call.responseFrames.push(response.type);
        await stream.write(encodeOutgoingFrame(response));
      }
      await stream.writeStatus();
      await stream.finish();
    } finally {
      streams.close();
    }
  };
}

function compressedTransport(handler, budget) {
  const transport = fakeTransport(handler);
  transport.compression = plain;
  if (budget !== undefined) {
    transport[StreamBudgetKey] = budget;
  }
  return transport;
}

/** Echoes count messages one at a time on CompressedEcho.Bidi. */
async function pingPong(client, count) {
  const call = await client.bidi();
  for (let i = 0; i < count; i += 1) {
    await call.send({ message: compressible(i) });
    const { message, index } = await call.recv();
    assert.deepEqual([message, index], [compressible(i), i]);
  }
  await call.closeSend();
  assert.equal(await call.recv(), undefined);
  assert.equal((await call.status).code, Code.Ok);
}

const { Message, ZstdStreamMessage } = FrameType;

test("a call offers stream compression and compresses from the third message once answered", async () => {
  const calls = [];
  const transport = compressedTransport(streamEcho(calls));
  await pingPong(new CompressedEchoClient(transport), 5);
  await transport.settled();
  assert.equal(calls[0].request.streamCompression.maxWindowLog, 16);
  const compressed = [Message, Message, ZstdStreamMessage, ZstdStreamMessage, ZstdStreamMessage];
  assert.deepEqual(calls[0].requestFrames, compressed);
  assert.deepEqual(calls[0].responseFrames, compressed);
});

test("without the server's answer, requests stay per message", async () => {
  const calls = [];
  const transport = compressedTransport(streamEcho(calls, { answer: false }));
  await pingPong(new CompressedEchoClient(transport), 4);
  await transport.settled();
  assert.deepEqual(calls[0].requestFrames, [Message, Message, Message, Message]);
  assert.deepEqual(calls[0].responseFrames, [
    Message,
    Message,
    ZstdStreamMessage,
    ZstdStreamMessage,
  ]);
});

test("a budget too small for the decoding context makes the call offer nothing", async () => {
  const calls = [];
  const transport = compressedTransport(streamEcho(calls), new StreamBudget(1000));
  await pingPong(new CompressedEchoClient(transport), 4);
  await transport.settled();
  assert.equal(calls[0].request.streamCompression, null);
  assert.deepEqual(calls[0].responseFrames, [Message, Message, Message, Message]);
});

test("requests stay per message when the compressing context does not fit the budget", async () => {
  const calls = [];
  const budget = new StreamBudget(plain.streamDecoderCost(16));
  const transport = compressedTransport(streamEcho(calls), budget);
  await pingPong(new CompressedEchoClient(transport), 4);
  await transport.settled();
  assert.deepEqual(calls[0].requestFrames, [Message, Message, Message, Message]);
  assert.deepEqual(calls[0].responseFrames, [
    Message,
    Message,
    ZstdStreamMessage,
    ZstdStreamMessage,
  ]);
});

test("a call's contexts return their memory to the budget when it ends", async () => {
  const total = plain.streamDecoderCost(16) + plain.streamEncoderCost(16);
  const budget = new StreamBudget(total);
  const transport = compressedTransport(streamEcho([]), budget);
  const client = new CompressedEchoClient(transport);
  await pingPong(client, 4);
  // A cancelled call returns its memory too.
  const call = await client.bidi();
  await call.close();
  await transport.settled().catch(() => {});
  const release = budget.reserve(total);
  assert.equal(typeof release, "function");
  release();
});

test("a stream-compressed response the call did not enable fails with Internal", async () => {
  const encoder = plain.streamEncoder(undefined, 16);
  const body = marshalMessage(EchoResponse, { message: "a" });
  const chunk = encoder.encode(body);
  const payload = new Uint8Array(1 + chunk.byteLength);
  payload.set(chunk, writeVarint(payload, 0, body.byteLength));
  // No compression on the transport, so the call offers nothing.
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await stream.write(encodeOutgoingFrame({ type: ZstdStreamMessage, payload, size: 2 }));
    await stream.writeStatus();
    await stream.finish();
  });
  const call = await new CompressedEchoClient(transport).bidi();
  await assert.rejects(call.recv(), isStatus(Code.Internal));
});

test("directions marked unreliable are not stream-compressed", async () => {
  const calls = [];
  const transport = compressedTransport(streamEcho(calls));
  const service = {
    name: "CompressedEcho",
    fullName: "trevrpc.interop.v1.CompressedEcho",
    methods: {
      bidi: {
        name: "Bidi",
        kind: "bidirectionalStreaming",
        inputType: "trevrpc.interop.v1.EchoRequest",
        outputType: "trevrpc.interop.v1.EchoResponse",
        unreliableRequests: true,
        unreliableResponses: true,
        compressStreamRequests: true,
        compressStreamResponses: true,
      },
    },
  };
  await pingPong(createServiceClient(transport, service, root), 3);
  await transport.settled();
  assert.equal(calls[0].request.streamCompression, null);
});
