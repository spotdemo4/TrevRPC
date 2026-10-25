import assert from "node:assert/strict";
import test from "node:test";

import { CompressedEchoClient, root } from "../build/interop/gen/compressed.trevrpc.js";
import { createServiceClient } from "../src/client.js";
import { parseStreamChunk } from "../src/compression.js";
import {
  DefaultMaxFrameSize,
  FrameType,
  encodeHeadersFrame,
  encodeOutgoingFrame,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import { Code, TrevRpcError } from "../src/status.js";
import {
  CallStreams,
  DefaultStreamCompressionMemory,
  MaxReplayBytes,
  StreamBudget,
  StreamBudgetKey,
} from "../src/stream-compression.js";
import { varintLength, writeVarint } from "../src/varint.js";
import { RpcHeaders, RpcRequest, RpcStatus } from "../src/wire.js";
import { createCompression } from "../src/zstd.js";
import {
  ServerStreams,
  deferred,
  fakeDatagrams,
  fakeTransport,
  hex,
  isPending,
  readGoldenVectors,
  turn,
} from "./support.js";

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

test("the stream encoder ends a frame every 60 KiB, starting the next from its dictionary", () => {
  const limit = 60 << 10;
  for (const start of [undefined, dictionary]) {
    const encoder = withDictionary.streamEncoder(start, 16);
    const decoder = withDictionary.streamDecoder(16);
    let atFrameStart = true;
    let frames = 0;
    let inFrame = 0;
    for (let i = 0; i < 200; i++) {
      const body = new TextEncoder().encode(
        `record ${i} with some text;`.repeat(50).slice(0, 1000),
      );
      const chunk = encoder.encode(body);
      const info = parseStreamChunk(chunk, atFrameStart, 16);
      if (info.startsFrame) {
        frames++;
        assert.equal(info.dictionaryId, start === undefined ? 0 : 1, `frame ${frames}`);
      }
      inFrame += body.byteLength;
      assert.equal(info.endsFrame, inFrame >= limit, `message ${i}`);
      if (info.endsFrame) {
        inFrame = 0;
      }
      atFrameStart = info.endsFrame;
      const frameDictionary = info.dictionaryId === 0 ? undefined : dictionary;
      assert.deepEqual(
        decoder.decode(chunk, body.byteLength, frameDictionary),
        body,
        `message ${i}`,
      );
    }
    assert.equal(frames, Math.floor((200 * 1000) / limit) + 1);
    encoder.close();
    decoder.close();
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
  assert.equal(
    hex(
      marshalMessage(RpcRequest, {
        method: "/svc/m",
        streamCompression: { maxWindowLog: 16, optimistic: true },
      }),
    ),
    vectors.get("request.optimistic_stream_compression.payload"),
  );
  const refusal = { code: Code.ResourceExhausted, refusedStreamCompression: true };
  assert.equal(
    hex(marshalMessage(RpcStatus, refusal)),
    vectors.get("status.refused_stream_compression.payload"),
  );
  assert.equal(
    unmarshalMessage(RpcStatus, fromHex(vectors.get("status.refused_stream_compression.payload")))
      .refusedStreamCompression,
    true,
  );
});

/**
 * CompressedEcho.Bidi with stream compression, recording each call as
 * `{ request, requestFrames, responseFrames, refused }`. `options` go to ServerStreams; a function
 * gets the index of the call and returns them.
 */
function streamEcho(calls, options) {
  return async (stream) => {
    const request = await stream.readRequest();
    const streams = new ServerStreams(
      plain,
      request,
      typeof options === "function" ? options(calls.length) : options,
    );
    const call = { request, requestFrames: [], responseFrames: [], refused: streams.refuses };
    calls.push(call);
    if (streams.refuses) {
      await stream.refuse();
      return;
    }
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

test("a call offers stream compression optimistically and compresses from the third message", async () => {
  const calls = [];
  const transport = compressedTransport(streamEcho(calls));
  await pingPong(new CompressedEchoClient(transport), 5);
  await transport.settled();
  assert.equal(calls[0].request.streamCompression.maxWindowLog, 16);
  assert.equal(calls[0].request.streamCompression.optimistic, true);
  const compressed = [Message, Message, ZstdStreamMessage, ZstdStreamMessage, ZstdStreamMessage];
  assert.deepEqual(calls[0].requestFrames, compressed);
  assert.deepEqual(calls[0].responseFrames, compressed);
});

test("a refused call is replayed without stream compression, and requests stay per message without an answer", async () => {
  const calls = [];
  const transport = compressedTransport(streamEcho(calls, { answer: false }));
  const client = new CompressedEchoClient(transport);
  await pingPong(client, 4);
  // The session stopped being optimistic: the next call offers, and waits for an answer.
  await pingPong(client, 4);
  await transport.settled();
  assert.deepEqual(
    calls.map((call) => [call.refused, call.request.streamCompression.optimistic]),
    [
      [true, true],
      [false, false],
      [false, false],
    ],
  );
  assert.deepEqual(calls[1].requestFrames, [Message, Message, Message, Message]);
  assert.deepEqual(calls[1].responseFrames, [
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

/**
 * CompressedEcho.ClientStream, recording each stream as `{ request, frames, refused }`.
 * `behavior(i)` says how to serve the i-th stream: "answer" to accept it and answer its offer,
 * "quiet" to serve it without answering, as a server out of budget, and "now" or "after" to refuse
 * an optimistic call right after its REQUEST or once its request stream ended, as a slow server
 * might; an unanswered call that is not optimistic is served "quiet". `headers`, if given, holds
 * back the HEADERS until it settles.
 */
function uploadEcho(calls, { behavior = () => "answer", headers } = {}) {
  return async (stream) => {
    const request = await stream.readRequest();
    const call = { request, frames: [], refused: false };
    let mode = behavior(calls.push(call) - 1);
    if (!request.streamCompression?.optimistic && mode !== "answer") {
      mode = "quiet";
    }
    if (mode === "now") {
      call.refused = true;
      await stream.refuse();
      return;
    }
    const streams = new ServerStreams(plain, request, { answer: mode === "answer" });
    try {
      if (mode !== "after") {
        await headers;
        await stream.write(encodeHeadersFrame({ metadata: {}, ...streams.headers }));
      }
      const messages = [];
      for (let frame; (frame = await stream.readFrame()) !== null;) {
        call.frames.push(frame.type);
        if (mode !== "after") {
          messages.push(EchoRequest.decode(streams.receive(frame)).message);
        }
      }
      if (mode === "after") {
        call.refused = true;
        await stream.refuse();
        return;
      }
      const response = marshalMessage(EchoResponse, {
        message: messages.join(""),
        index: messages.length,
      });
      await stream.write(encodeOutgoingFrame(streams.encode(response)));
      await stream.writeStatus();
      await stream.finish();
    } finally {
      streams.close();
    }
  };
}

const uploadMessages = (count) => Array.from({ length: count }, (_, i) => compressible(i));

/** Uploads `count` messages on CompressedEcho.ClientStream without waiting, then checks the echo. */
async function upload(client, count, options) {
  const call = await client.clientStream(options);
  for (const message of uploadMessages(count)) {
    await call.send({ message });
  }
  const { message, index } = await call.closeAndRecv();
  assert.deepEqual([message, index], [uploadMessages(count).join(""), count]);
}

test("an optimistic upload compresses from the third request before the server answers", async () => {
  const calls = [];
  const gate = deferred();
  const transport = compressedTransport(uploadEcho(calls, { headers: gate.promise }));
  const client = new CompressedEchoClient(transport);
  const call = await client.clientStream();
  for (const message of uploadMessages(5)) {
    await call.send({ message });
  }
  // Every request went out before the HEADERS.
  const echo = call.closeAndRecv();
  await turn();
  gate.resolve();
  assert.deepEqual(Object.values(await echo), [uploadMessages(5).join(""), 5]);
  await transport.settled();
  assert.deepEqual(calls[0].frames, [
    Message,
    Message,
    ZstdStreamMessage,
    ZstdStreamMessage,
    ZstdStreamMessage,
  ]);
});

test("a refused upload is replayed, whether the refusal comes before or after closeSend", async () => {
  for (const when of ["now", "after"]) {
    const calls = [];
    const budget = new StreamBudget();
    const transport = compressedTransport(
      uploadEcho(calls, { behavior: (i) => (i === 0 ? when : "quiet") }),
      budget,
    );
    await upload(new CompressedEchoClient(transport), 6);
    await transport.settled();
    assert.equal(transport.opened, 2, when);
    assert.deepEqual(
      calls.map((call) => [call.refused, call.request.streamCompression.optimistic]),
      [
        [true, true],
        [false, false],
      ],
      when,
    );
    // Every request reached the replay exactly once, per message since nothing was answered.
    assert.deepEqual(calls[1].frames, Array(6).fill(Message), when);
    const release = budget.reserve(DefaultStreamCompressionMemory);
    assert.equal(typeof release, "function", `${when}: the call returned its memory`);
    release();
  }
});

test("a replayed call keeps its metadata and the time left before its deadline", async () => {
  const calls = [];
  const transport = compressedTransport(
    uploadEcho(calls, { behavior: (i) => (i === 0 ? "after" : "answer") }),
  );
  const client = new CompressedEchoClient(transport);
  const call = await client.clientStream({ timeoutMs: 5_000, metadata: { tag: "kept" } });
  await call.send({ message: compressible(0) });
  await new Promise((resolve) => setTimeout(resolve, 20));
  const { message } = await call.closeAndRecv();
  assert.equal(message, compressible(0));
  await transport.settled();
  const [first, replay] = calls.map((recorded) => recorded.request);
  assert.equal(new TextDecoder().decode(replay.metadata.tag), "kept");
  assert.equal(replay.method, first.method);
  assert.ok(BigInt(replay.timeoutNanos) > 0n);
  assert.ok(BigInt(replay.timeoutNanos) < BigInt(first.timeoutNanos));
});

test("a bidirectional call refused at dispatch is replayed and echoes each request once", async () => {
  const calls = [];
  const transport = compressedTransport(
    streamEcho(calls, (index) => (index === 0 ? { answer: false } : {})),
  );
  await pingPong(new CompressedEchoClient(transport), 5);
  await transport.settled();
  assert.equal(calls.length, 2);
  assert.equal(calls[0].refused, true);
  assert.equal(calls[1].requestFrames.length, 5);
});

test("cancelling a call during its replay ends it and returns its memory", async () => {
  const budget = new StreamBudget();
  const stalled = deferred();
  let streams = 0;
  const transport = compressedTransport(async (stream) => {
    const request = await stream.readRequest();
    streams += 1;
    if (request.streamCompression?.optimistic) {
      await stream.refuse();
      return;
    }
    stalled.resolve();
    // The replay is never answered.
    await stream.readFrame().catch(() => {});
  }, budget);
  const call = await new CompressedEchoClient(transport).clientStream();
  await call.send({ message: compressible(0) });
  const echo = call.closeAndRecv();
  await stalled.promise;
  await call.close();
  await assert.rejects(echo, isStatus(Code.Cancelled));
  await transport.settled().catch(() => {});
  assert.equal(streams, 2);
  const release = budget.reserve(DefaultStreamCompressionMemory);
  assert.equal(typeof release, "function");
  release();
});

test("requests that do not fit the copy budget or cap wait for the server's decision", async () => {
  const message = compressible(0);
  const messageSize = marshalMessage(EchoRequest, { message }).byteLength;
  const encoderCost = plain.streamEncoderCost(16);
  for (const [budget, sizes] of [
    // Room for the compressing context, so the call is optimistic, but once it opened, for one
    // copy only: a client-streaming call reserves no decoding context.
    [new StreamBudget(encoderCost), [message, message]],
    // A message above the 1 MiB cap.
    [new StreamBudget(), ["x".repeat(MaxReplayBytes + 1)]],
  ]) {
    const gate = deferred();
    const calls = [];
    const transport = compressedTransport(uploadEcho(calls, { headers: gate.promise }), budget);
    const call = await new CompressedEchoClient(transport).clientStream();
    const taken = sizes.length > 1 ? budget.reserve(encoderCost - messageSize) : undefined;
    const sends = [];
    for (const text of sizes) {
      sends.push(call.send({ message: text }));
    }
    assert.equal(await isPending(sends.at(-1)), true);
    gate.resolve();
    await Promise.all(sends);
    const { index } = await call.closeAndRecv();
    assert.equal(index, sizes.length);
    await transport.settled();
    assert.equal(calls[0].request.streamCompression.optimistic, true);
    taken?.();
  }
});

test("after a refusal the session waits for an answer, then is optimistic again", async () => {
  const calls = [];
  // Refuse the first call and leave its replay unanswered; later calls are answered.
  const transport = compressedTransport(
    uploadEcho(calls, { behavior: (i) => ["now", "quiet"][i] ?? "answer" }),
  );
  const client = new CompressedEchoClient(transport);
  await upload(client, 3);
  await upload(client, 3);
  await upload(client, 3);
  await transport.settled();
  assert.deepEqual(
    calls.map((call) => Boolean(call.request.streamCompression.optimistic)),
    [true, false, false, true],
  );
});

test("a refusal flag on a call that was not optimistic is a plain status", async () => {
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await stream.refuse();
  });
  const call = await new CompressedEchoClient(transport).clientStream();
  await assert.rejects(call.closeAndRecv(), isStatus(Code.ResourceExhausted));
  assert.equal(transport.opened, 1);
});

test("a channel whose budget has no room for a compressing context is not optimistic", async () => {
  const calls = [];
  const transport = compressedTransport(uploadEcho(calls), new StreamBudget(0));
  await upload(new CompressedEchoClient(transport), 3);
  await transport.settled();
  assert.equal(Boolean(calls[0].request.streamCompression.optimistic), false);
  assert.deepEqual(calls[0].frames, [Message, Message, Message]);
});

test("a call with a datagram flow is not optimistic and keeps no copies", async () => {
  const service = {
    name: "CompressedEcho",
    fullName: "trevrpc.interop.v1.CompressedEcho",
    methods: {
      bidi: {
        name: "Bidi",
        kind: "bidirectionalStreaming",
        inputType: "trevrpc.interop.v1.EchoRequest",
        outputType: "trevrpc.interop.v1.EchoResponse",
        unreliableResponses: true,
        compressStreamRequests: true,
      },
    },
  };
  for (const withDatagrams of [true, false]) {
    const calls = [];
    // A budget without room for copies: a call that kept them would wait for every HEADERS.
    const budget = new StreamBudget(plain.streamEncoderCost(16));
    const transport = compressedTransport(streamEcho(calls, { compressResponses: false }), budget);
    if (withDatagrams) {
      transport.datagrams = fakeDatagrams();
    }
    await pingPong(createServiceClient(transport, service, root), 3);
    await transport.settled();
    const { datagramFlow, streamCompression } = calls[0].request;
    assert.equal(Number(datagramFlow) !== 0, withDatagrams);
    assert.equal(Boolean(streamCompression.optimistic), !withDatagrams);
  }
});
