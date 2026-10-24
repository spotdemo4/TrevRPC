import assert from "node:assert/strict";
import test from "node:test";

import { EchoClient, root } from "../build/interop/gen/interop.trevrpc.js";
import { UnreliableEchoClient } from "../build/interop/gen/unreliable.trevrpc.js";
import { encodeMessage, inflateMessage } from "../src/compression.js";
import { encodeDatagram } from "../src/datagram.js";
import {
  DefaultMaxFrameSize,
  FrameType,
  encodeHeadersFrame,
  encodeOutgoingFrame,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import { Code, TrevRpcError } from "../src/status.js";
import { RpcHeaders, RpcRequest } from "../src/wire.js";
import { createCompression } from "../src/zstd.js";
import { fakeDatagrams, fakeTransport, hex, readGoldenVectors, turn } from "./support.js";

const EchoRequest = root.lookupType("trevrpc.interop.v1.EchoRequest");
const EchoResponse = root.lookupType("trevrpc.interop.v1.EchoResponse");
const EchoTypes = ["trevrpc.interop.v1.EchoRequest", "trevrpc.interop.v1.EchoResponse"];

const vectors = await readGoldenVectors("zstd-golden-vectors.txt");
const fromHex = (value) => Uint8Array.from(Buffer.from(value, "hex"));
const dictionaryData = fromHex(vectors.get("dict.d1.dictionary"));
const fingerprint = fromHex(vectors.get("dict.d1.fingerprint"));
const compression = await createCompression({
  dictionaries: [{ data: dictionaryData, messages: EchoTypes }],
});
const dictionary = compression.dictionary(1);

/** The i-th message of the interop `compressed` scenario, which the dictionary compresses. */
const compressible = (i) =>
  `{"service":"search","region":"us-east-1","status":404,"latency_ms":${100 + i}}`;

const isStatus = (code) => (error) => error instanceof TrevRpcError && error.code === code;

test("a dictionary is identified by its ID and fingerprint", () => {
  assert.equal(dictionary.id, Number(vectors.get("dict.d1.id")));
  assert.equal(hex(dictionary.fingerprint), hex(fingerprint));
  assert.deepEqual(
    compression.query.map(({ id, fingerprint: print }) => [id, hex(print)]),
    [[1, hex(fingerprint)]],
  );
  assert.equal(compression.dictionaryFor(EchoTypes[0]), dictionary);
  assert.equal(compression.dictionaryFor("trevrpc.interop.v1.Other"), undefined);
});

test("golden payloads decompress, and invalid ones are rejected", () => {
  const names = (prefix) =>
    [...vectors.keys()]
      .filter((key) => key.startsWith(prefix) && key.endsWith(".payload"))
      .map((key) => key.slice(0, -".payload".length));
  assert.ok(names("ok.").length >= 5);
  for (const name of names("ok.")) {
    const message = inflateMessage(dictionary, fromHex(vectors.get(`${name}.payload`)), 4096);
    assert.equal(hex(message), vectors.get(`${name}.message`), name);
  }
  assert.ok(names("bad.").length >= 9);
  for (const name of names("bad.")) {
    assert.throws(
      () => inflateMessage(dictionary, fromHex(vectors.get(`${name}.payload`)), 4096),
      isStatus(Code.Internal),
      name,
    );
  }
  const huge = fromHex(vectors.get("too_large.zeros.payload"));
  assert.throws(
    () => inflateMessage(dictionary, huge, DefaultMaxFrameSize),
    isStatus(Code.ResourceExhausted),
  );
  // The limit is inclusive.
  const small = fromHex(vectors.get("ok.small.payload"));
  const size = fromHex(vectors.get("ok.small.message")).byteLength;
  assert.equal(inflateMessage(dictionary, small, size).byteLength, size);
  assert.throws(
    () => inflateMessage(dictionary, small, size - 1),
    isStatus(Code.ResourceExhausted),
  );
  // Nothing is accepted without a dictionary.
  assert.throws(() => inflateMessage(undefined, small, 4096), isStatus(Code.Internal));
});

test("compressing writes the golden frame, and only when it is shorter", () => {
  const message = fromHex(vectors.get("ok.small.message"));
  assert.equal(
    hex(encodeOutgoingFrame(encodeMessage(message, dictionary))),
    vectors.get("zstd_message.small.frame"),
  );
  const random = Uint8Array.from({ length: 64 }, (_, index) => (index * 151 + 7) % 256);
  assert.deepEqual(encodeMessage(random, dictionary), {
    type: FrameType.Message,
    payload: random,
    size: 64,
  });
  assert.deepEqual(encodeMessage(new Uint8Array(0), dictionary).type, FrameType.Message);
});

test("the negotiation fields match the golden vectors", () => {
  const request = {
    method: "/svc/m",
    acceptDictionaries: [1, 300],
    dictionaryQuery: [{ id: 1, fingerprint }],
  };
  assert.equal(
    hex(marshalMessage(RpcRequest, request)),
    vectors.get("request.dictionaries.payload"),
  );
  const decoded = unmarshalMessage(
    RpcRequest,
    fromHex(vectors.get("request.dictionaries.payload")),
  );
  assert.deepEqual(decoded.acceptDictionaries, [1, 300]);
  assert.equal(hex(decoded.dictionaryQuery[0].fingerprint), hex(fingerprint));

  const headers = unmarshalMessage(
    RpcHeaders,
    fromHex(vectors.get("headers.dictionaries.payload")),
  );
  assert.equal(headers.dictionaries[0].id, 1);
  assert.equal(hex(headers.dictionaries[0].fingerprint), hex(fingerprint));
  assert.equal(
    hex(encodeHeadersFrame({ dictionaries: [{ id: 1, fingerprint }] })),
    vectors.get("headers.dictionaries.frame"),
  );
});

test("createCompression rejects invalid settings", async () => {
  const withId = (id) => {
    const data = dictionaryData.slice();
    new DataView(data.buffer).setUint32(4, id, true);
    return data;
  };
  const rejects = (options, pattern) => assert.rejects(createCompression(options), pattern);
  await rejects({ dictionaries: [{ data: new Uint8Array(16) }] }, /not a zstd-format dictionary/);
  await rejects({ dictionaries: [{ data: withId(0) }] }, /dictionary ID 0/);
  await rejects(
    { dictionaries: [{ data: dictionaryData }, { data: dictionaryData }] },
    /share ID 1/,
  );
  await rejects(
    {
      dictionaries: [
        { data: dictionaryData, messages: ["a.B"] },
        { data: withId(2), messages: ["a.B"] },
      ],
    },
    /a\.B is in two compression dictionaries/,
  );
  await rejects({ dictionaries: [{ data: dictionaryData, messages: [".a.B"] }] }, /invalid/);
  await rejects({ dictionaries: [], level: 1.5 }, /level must be an integer/);
  // No dictionaries compress nothing, and need no codec.
  assert.deepEqual((await createCompression()).query, []);
});

/**
 * An Echo server holding the dictionary: it answers queries in HEADERS right after the REQUEST,
 * echoes each request message as it arrives, compresses responses with a dictionary the call
 * accepts, and decompresses requests. `confirm: false` makes it answer that it holds none.
 * Every call is recorded as `{ request, requestFrames, responseFrames }`.
 */
function compressingEcho(calls, { confirm = true } = {}) {
  return async (stream) => {
    const request = await stream.readRequest();
    const call = { request, requestFrames: [], responseFrames: [] };
    calls.push(call);
    const query = request.dictionaryQuery ?? [];
    const confirmed =
      confirm &&
      query.some(({ id, fingerprint: print }) => id === 1 && hex(print) === hex(fingerprint));
    const accepted =
      (request.acceptDictionaries ?? []).includes(1) && (query.length === 0 || confirmed);
    if (query.length > 0) {
      const dictionaries = confirmed ? [{ id: 1, fingerprint }] : [];
      await stream.write(encodeHeadersFrame({ metadata: {}, dictionaries }));
    }
    let index = 0;
    for (let frame; (frame = await stream.readFrame()) !== null; index += 1) {
      call.requestFrames.push(frame.type);
      const payload =
        frame.type === FrameType.ZstdMessage
          ? inflateMessage(dictionary, frame.payload, DefaultMaxFrameSize)
          : frame.payload;
      const { message } = EchoRequest.decode(payload);
      const response = encodeMessage(
        marshalMessage(EchoResponse, { message, index }),
        accepted ? dictionary : undefined,
      );
      call.responseFrames.push(response.type);
      await stream.write(encodeOutgoingFrame(response));
    }
    await stream.writeStatus();
    await stream.finish();
  };
}

function compressedTransport(handler) {
  const transport = fakeTransport(handler);
  transport.compression = compression;
  return transport;
}

test("a session queries once, then compresses requests and accepts compressed responses", async () => {
  const calls = [];
  const echo = new EchoClient(compressedTransport(compressingEcho(calls)));

  assert.equal((await echo.unary({ message: compressible(0) })).message, compressible(0));
  assert.equal(calls[0].request.dictionaryQuery.length, 1);
  assert.equal(hex(calls[0].request.dictionaryQuery[0].fingerprint), hex(fingerprint));
  assert.deepEqual(calls[0].request.acceptDictionaries, [1]);
  // The first request goes out before the answer, so it is not compressed.
  assert.deepEqual(calls[0].requestFrames, [FrameType.Message]);
  assert.deepEqual(calls[0].responseFrames, [FrameType.ZstdMessage]);

  assert.equal((await echo.unary({ message: compressible(1) })).message, compressible(1));
  assert.deepEqual(calls[1].request.dictionaryQuery, []);
  assert.deepEqual(calls[1].request.acceptDictionaries, [1]);
  assert.deepEqual(calls[1].requestFrames, [FrameType.ZstdMessage]);
  assert.deepEqual(calls[1].responseFrames, [FrameType.ZstdMessage]);

  // Messages that do not compress travel as MESSAGE frames.
  assert.equal((await echo.unary({ message: "x" })).message, "x");
  assert.deepEqual(calls[2].requestFrames, [FrameType.Message]);
  assert.deepEqual(calls[2].responseFrames, [FrameType.Message]);
});

test("a request stream starts compressing as soon as its HEADERS answer", async () => {
  const calls = [];
  const echo = new EchoClient(compressedTransport(compressingEcho(calls)));
  const call = await echo.bidi();
  await call.send({ message: compressible(0) });
  assert.equal((await call.recv()).message, compressible(0));
  await call.send({ message: compressible(1) });
  assert.equal((await call.recv()).message, compressible(1));
  await call.closeSend();
  assert.equal(await call.recv(), undefined);
  assert.deepEqual(calls[0].requestFrames, [FrameType.Message, FrameType.ZstdMessage]);
  assert.deepEqual(calls[0].responseFrames, [FrameType.ZstdMessage, FrameType.ZstdMessage]);
});

test("an answer without the dictionary turns it off for the session", async () => {
  const calls = [];
  const echo = new EchoClient(compressedTransport(compressingEcho(calls, { confirm: false })));
  await echo.unary({ message: compressible(0) });
  await echo.unary({ message: compressible(1) });
  assert.deepEqual(calls[1].request.dictionaryQuery, []);
  assert.deepEqual(calls[1].request.acceptDictionaries, []);
  assert.deepEqual(
    calls.flatMap((call) => [...call.requestFrames, ...call.responseFrames]),
    [FrameType.Message, FrameType.Message, FrameType.Message, FrameType.Message],
  );
});

test("each session negotiates on its own", async () => {
  const calls = [];
  for (const transport of [
    compressedTransport(compressingEcho(calls)),
    compressedTransport(compressingEcho(calls)),
  ]) {
    await new EchoClient(transport).unary({ message: compressible(0) });
  }
  assert.deepEqual(
    calls.map((call) => call.request.dictionaryQuery.length),
    [1, 1],
  );
});

test("a lone STATUS leaves the query unanswered, so the next call asks again", async () => {
  const calls = [];
  const answer = compressingEcho(calls);
  let first = true;
  const echo = new EchoClient(
    compressedTransport(async (stream) => {
      if (first) {
        first = false;
        calls.push({ request: await stream.readRequest() });
        await stream.writeStatus(Code.Unavailable, "busy");
        await stream.finish();
        return;
      }
      await answer(stream);
    }),
  );
  await assert.rejects(echo.unary({ message: "a" }), isStatus(Code.Unavailable));
  await echo.unary({ message: "b" });
  assert.deepEqual(
    calls.map((call) => call.request.dictionaryQuery.length),
    [1, 1],
  );
});

/** A server that answers a unary call with HEADERS and then the given response frame. */
function respondingWith(frame) {
  return async (stream) => {
    await stream.readRequest();
    await stream.write(
      encodeHeadersFrame({ metadata: {}, dictionaries: [{ id: 1, fingerprint }] }),
    );
    await stream.write(frame);
    await stream.writeStatus();
    await stream.finish();
  };
}

const compressedResponse = encodeOutgoingFrame(
  encodeMessage(marshalMessage(EchoResponse, { message: compressible(0) }), dictionary),
);

test("a compressed response the call did not accept fails with Internal", async () => {
  // No compression on the client, so it accepts nothing.
  const echo = new EchoClient(fakeTransport(respondingWith(compressedResponse)));
  await assert.rejects(echo.unary({ message: "a" }), isStatus(Code.Internal));
});

test("a compressed response above the frame size limit fails with ResourceExhausted", async () => {
  const echo = new EchoClient(compressedTransport(respondingWith(compressedResponse)), {
    maxFrameSize: 64,
  });
  assert.ok(compressedResponse.byteLength < 64);
  await assert.rejects(echo.unary({ message: "a" }), isStatus(Code.ResourceExhausted));
});

test("an invalid compressed response fails with Internal", async () => {
  const frame = encodeOutgoingFrame({
    type: FrameType.ZstdMessage,
    payload: fromHex(vectors.get("bad.trailing_bytes.payload")),
    size: 0,
  });
  const echo = new EchoClient(compressedTransport(respondingWith(frame)));
  await assert.rejects(echo.unary({ message: "a" }), isStatus(Code.Internal));
});

test("compressed response datagrams are decompressed, and an invalid one drops its datagram", async () => {
  const datagrams = fakeDatagrams();
  const response = (message) =>
    encodeMessage(marshalMessage(EchoResponse, { message, index: 0 }), dictionary);
  const transport = compressedTransport(async (stream) => {
    const request = await stream.readRequest();
    const flow = Number(request.datagramFlow);
    assert.deepEqual(request.acceptDictionaries, [1]);
    await stream.write(
      encodeHeadersFrame({ metadata: {}, dictionaries: [{ id: 1, fingerprint }] }),
    );
    datagrams.push(encodeDatagram(flow, [response(compressible(0))]));
    const invalid = {
      type: FrameType.ZstdMessage,
      payload: fromHex(vectors.get("bad.checksum.payload")),
    };
    datagrams.push(encodeDatagram(flow, [response(compressible(1)), invalid]));
    await turn();
    await stream.writeMessage(EchoResponse, { message: "end", index: 0 });
    await stream.writeStatus();
    await stream.finish();
  });
  transport.datagrams = datagrams;
  const call = await new UnreliableEchoClient(transport).bidi();
  await call.closeSend();
  const received = [];
  for await (const { message } of call) {
    received.push(message);
  }
  assert.deepEqual(received, [compressible(0), "end"]);
});
