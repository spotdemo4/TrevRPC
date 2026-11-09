import assert from "node:assert/strict";
import test from "node:test";

import { EchoRequestSchema } from "../build/interop/gen/interop_pb.js";
import { SampleSchema } from "../build/test/gen/types_pb.js";
import { encodeMessage } from "../src/compression.js";
import { decodeDatagram, encodeDatagram } from "../src/datagram.js";
import {
  FrameReader,
  FrameType,
  encodeAnswerFrame,
  encodeFrame,
  encodeHeadersFrame,
  encodeMessageFrame,
  encodeOutgoingFrame,
  encodeRequestFrame,
  encodeStatusFrame,
  encodeWireMessage,
  decodeWireMessage,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import {
  MaxMetadataEntries,
  MaxMetadataKeyLen,
  MaxMetadataTotalSize,
  MaxMetadataValueLen,
  normalizeMetadata,
  validateMetadata,
} from "../src/metadata.js";
import { Code, TrevRpcError } from "../src/status.js";
import { readVarint, varintLength, writeVarint } from "../src/varint.js";
import { RpcAnswer, RpcHeaders, RpcKind, RpcRequest, RpcStatus } from "../src/wire.js";
import { chunked, concatBytes, hex, readGoldenVectors, readerOf } from "./support.js";

const bytes = (text) => new TextEncoder().encode(text);
const fromHex = (value) => Uint8Array.from(Buffer.from(value, "hex"));

const request = { method: "/svc/m" };

/** The documented logical value of every golden vector: [frame type, payload codec, fields]. */
const goldenCases = {
  "request.unary": [FrameType.Request, RpcRequest, request],
  "request.bidi_timeout": [
    FrameType.Request,
    RpcRequest,
    { ...request, kind: RpcKind.BidirectionalStreaming, timeoutNanos: 123456 },
  ],
  "request.metadata": [
    FrameType.Request,
    RpcRequest,
    { ...request, metadata: { authorization: bytes("ok") } },
  ],
  "request.datagram_flow": [
    FrameType.Request,
    RpcRequest,
    { ...request, kind: RpcKind.BidirectionalStreaming, datagramFlow: 1 },
  ],
  "request.connection_metadata_offer": [
    FrameType.Request,
    RpcRequest,
    { ...request, connectionMetadata: { id: 1, metadata: { authorization: bytes("ok") } } },
  ],
  "request.connection_metadata_reference": [
    FrameType.Request,
    RpcRequest,
    { ...request, metadata: { "x-a": bytes("1") }, connectionMetadata: { id: 1 } },
  ],
  "message.hi": [FrameType.Message, undefined, bytes("hi")],
  "message.empty": [FrameType.Message, undefined, new Uint8Array(0)],
  "headers.metadata": [FrameType.Headers, RpcHeaders, { metadata: { "x-a": bytes("1") } }],
  "headers.empty": [FrameType.Headers, RpcHeaders, {}],
  "answer.connection_metadata": [FrameType.Answer, RpcAnswer, { connectionMetadata: 1 }],
  "answer.empty": [FrameType.Answer, RpcAnswer, {}],
  "status.ok": [FrameType.Status, RpcStatus, { code: Code.Ok }],
  "status.unavailable": [FrameType.Status, RpcStatus, { code: Code.Unavailable, message: "down" }],
  "status.metadata": [
    FrameType.Status,
    RpcStatus,
    { code: Code.FailedPrecondition, message: "stop", metadata: { "x-t": bytes("2") } },
  ],
  "status.details": [
    FrameType.Status,
    RpcStatus,
    {
      code: Code.InvalidArgument,
      message: "bad",
      details: [{ type: "x.Y", value: Uint8Array.of(0x08, 0x01) }],
    },
  ],
  "grease.two_byte_type": [0x40, undefined, new Uint8Array([0, 1, 2])],
};

test("every golden vector has a documented logical value", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  const names = new Set();
  const streams = new Set();
  const datagrams = new Set();
  for (const key of vectors.keys()) {
    const [name, kind] = [key.slice(0, key.lastIndexOf(".")), key.slice(key.lastIndexOf(".") + 1)];
    if (name.startsWith("datagram.")) {
      datagrams.add(name);
      assert.ok(
        ["flow", "messages", "datagram", "received"].includes(kind),
        `unknown entry ${key}`,
      );
      continue;
    }
    (name.startsWith("stream.") ? streams : names).add(name);
    assert.ok(["payload", "frame", "stream", "frames"].includes(kind), `unknown entry ${key}`);
  }
  assert.deepEqual(names, new Set(Object.keys(goldenCases)));
  assert.ok(datagrams.size > 0);
  for (const name of datagrams) {
    assert.ok(
      ["flow", "messages"].every((kind) => vectors.has(`${name}.${kind}`)),
      name,
    );
    assert.ok(vectors.has(`${name}.datagram`) !== vectors.has(`${name}.received`), name);
  }
  for (const name of streams) {
    assert.ok(vectors.has(`${name}.stream`) && vectors.has(`${name}.frames`), name);
  }
  assert.ok(streams.size > 0);
});

test("wire encodings match the shared golden vectors and decode back", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  for (const [name, [type, codec, fields]] of Object.entries(goldenCases)) {
    const payload = codec === undefined ? fields : encodeWireMessage(codec, fields);
    assert.equal(hex(payload), vectors.get(`${name}.payload`), `${name} payload`);
    const frame = encodeFrame(type, payload);
    assert.equal(hex(frame), vectors.get(`${name}.frame`), `${name} frame`);

    // The frame reader yields the frame (grease is skipped) and its payload decodes to the value.
    const read = await new FrameReader(readerOf([frame])).readFrame();
    if (type === 0x40) {
      assert.equal(read, null, `${name} is skipped`);
      continue;
    }
    assert.equal(read.type, type, name);
    assert.equal(hex(read.payload), vectors.get(`${name}.payload`), name);
    if (codec !== undefined) {
      const decoded = decodeWireMessage(codec, read.payload);
      assert.equal(hex(encodeWireMessage(codec, decoded)), hex(payload), `${name} round trip`);
    }
  }
});

test("golden payloads decode to their logical fields", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  const decode = (name, codec) => decodeWireMessage(codec, fromHex(vectors.get(`${name}.payload`)));

  const timeout = decode("request.bidi_timeout", RpcRequest);
  assert.equal(timeout.kind, RpcKind.BidirectionalStreaming);
  assert.equal(timeout.timeoutNanos, 123456n);

  const flow = decode("request.datagram_flow", RpcRequest);
  assert.equal(flow.kind, RpcKind.BidirectionalStreaming);
  assert.equal(flow.datagramFlow, 1n);
  assert.equal(decode("request.unary", RpcRequest).datagramFlow, 0n);

  assert.equal(
    Buffer.from(decode("request.metadata", RpcRequest).metadata.authorization) + "",
    "ok",
  );
  const offer = decode("request.connection_metadata_offer", RpcRequest).connectionMetadata;
  assert.equal(offer.id, 1);
  assert.equal(Buffer.from(offer.metadata.authorization) + "", "ok");
  const reference = decode("request.connection_metadata_reference", RpcRequest);
  assert.equal(reference.connectionMetadata.id, 1);
  assert.deepEqual(Object.keys(reference.connectionMetadata.metadata), []);
  assert.deepEqual(Object.keys(reference.metadata), ["x-a"]);
  assert.equal(decode("request.unary", RpcRequest).connectionMetadata, undefined);
  assert.equal(decode("answer.connection_metadata", RpcAnswer).connectionMetadata, 1);
  assert.equal(decode("answer.empty", RpcAnswer).connectionMetadata, 0);
  assert.equal(decode("answer.empty", RpcAnswer).streamCompression, false);
  assert.deepEqual(decode("answer.empty", RpcAnswer).dictionaries, []);
  assert.equal(Buffer.from(decode("headers.metadata", RpcHeaders).metadata["x-a"]) + "", "1");

  const status = decode("status.metadata", RpcStatus);
  assert.equal(status.code, Code.FailedPrecondition);
  assert.equal(status.message, "stop");
  assert.equal(Buffer.from(status.metadata["x-t"]) + "", "2");
  assert.equal(decode("status.ok", RpcStatus).code, Code.Ok);
  assert.deepEqual(decode("status.ok", RpcStatus).details, []);
  const details = decode("status.details", RpcStatus).details;
  assert.equal(details.length, 1);
  assert.equal(details[0].type, "x.Y");
  assert.deepEqual([...details[0].value], [0x08, 0x01]);
});

test("datagram payloads match the golden vectors and decode back", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  const names = [
    ...new Set(
      [...vectors.keys()]
        .filter((key) => key.startsWith("datagram."))
        .map((key) => key.slice(0, key.lastIndexOf("."))),
    ),
  ];
  assert.ok(names.length >= 3);
  for (const name of names) {
    const flow = parseInt(vectors.get(`${name}.flow`), 16);
    const value = vectors.get(`${name}.messages`);
    const messages = value === "" ? [] : value.split(",");
    const decode = (datagram) => {
      const decoded = decodeDatagram(fromHex(datagram));
      assert.equal(decoded.flow, flow, name);
      assert.ok(
        decoded.messages.every(({ type }) => type === FrameType.Message),
        name,
      );
      return decoded.messages.map(({ payload }) => hex(payload));
    };

    if (vectors.has(`${name}.datagram`)) {
      // What a sender writes: encoding the messages gives these bytes, which decode back.
      const datagram = hex(
        encodeDatagram(
          flow,
          messages.map((message) => encodeMessage(fromHex(message))),
        ),
      );
      assert.equal(datagram, vectors.get(`${name}.datagram`), name);
      assert.deepEqual(decode(datagram), messages, name);
    } else {
      // What a receiver accepts: decoding only.
      assert.deepEqual(decode(vectors.get(`${name}.received`)), messages, name);
    }
  }
});

test("a datagram without a complete flow ID does not decode", () => {
  assert.equal(decodeDatagram(new Uint8Array(0)), null);
  assert.equal(decodeDatagram(new Uint8Array([0x40])), null);
  assert.equal(decodeDatagram(new Uint8Array([0x80, 0, 0])), null);
  assert.deepEqual(decodeDatagram(new Uint8Array([0x40, 0x01])), { flow: 1, messages: [] });
});

test("a datagram whose frames do not exactly fill it decodes to no messages", () => {
  const messagesOf = (...bytes) =>
    decodeDatagram(Uint8Array.from(bytes)).messages.map(({ payload }) => hex(payload));
  assert.deepEqual(messagesOf(1, 0, 1, 0x61), ["61"]);
  assert.deepEqual(messagesOf(1, 0, 1, 0x61, 0, 2, 0x62), []); // length past the end
  assert.deepEqual(messagesOf(1, 0, 1, 0x61, 0), []); // frame without a length
  assert.deepEqual(messagesOf(1, 0, 1, 0x61, 0x40), []); // truncated type varint
  assert.deepEqual(messagesOf(1, 0, 0xc0, 0, 0, 0, 0, 0, 0, 0x10), []); // length beyond 2^53
  assert.deepEqual(messagesOf(1, 6, 1, 0xff, 0, 1, 0x61), ["61"]); // other types are skipped
  // ZSTD_STREAM_MESSAGE needs a stream's ordered delivery, so it drops the whole datagram.
  assert.deepEqual(messagesOf(1, 0, 1, 0x61, 5, 1, 0x00), []);
});

test("the frame reader yields the golden known frames from every chunking", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  const streams = [...vectors.keys()].filter((key) => key.endsWith(".stream"));
  assert.ok(streams.length > 0);
  for (const key of streams) {
    const name = key.slice(0, -".stream".length);
    const wire = fromHex(vectors.get(key));
    for (const size of [1, 2, 3, 5, wire.byteLength]) {
      const reader = new FrameReader(readerOf(chunked(wire, size)));
      const frames = [];
      for (let frame; (frame = await reader.readFrame()) !== null;) {
        frames.push(`${frame.type}:${hex(frame.payload)}`);
      }
      assert.equal(frames.join(","), vectors.get(`${name}.frames`), `${name}, chunks of ${size}`);
    }
  }
});

test("request frames carry the request message in the same buffer", () => {
  const body = bytes("hi");
  const both = encodeRequestFrame(request, encodeMessage(body));
  const requestOnly = encodeRequestFrame(request);
  assert.deepEqual(both, concatBytes([requestOnly, encodeMessageFrame(body)]));
  assert.equal(hex(requestOnly), "01080a062f7376632f6d");
  assert.equal(
    hex(encodeRequestFrame(request, encodeMessage(new Uint8Array(0)))),
    `${hex(requestOnly)}0000`,
  );
});

test("unknown request fields are ignored when decoding", () => {
  // Fields 10 (varint) and 16 (bytes), which a later peer may send.
  const payload = fromHex("0a062f7376632f6d" + "5001" + "820101" + "6d");
  const decoded = decodeWireMessage(RpcRequest, payload);
  assert.equal(decoded.method, "/svc/m");
  assert.equal(hex(encodeWireMessage(RpcRequest, decoded)), "0a062f7376632f6d");
});

test("status, headers and answer encoders match the golden frames", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  assert.equal(
    hex(encodeStatusFrame({ code: Code.Unavailable, message: "down" })),
    vectors.get("status.unavailable.frame"),
  );
  assert.equal(
    hex(encodeHeadersFrame({ metadata: { "x-a": bytes("1") } })),
    vectors.get("headers.metadata.frame"),
  );
  assert.equal(
    hex(encodeAnswerFrame({ connectionMetadata: 1 })),
    vectors.get("answer.connection_metadata.frame"),
  );
});

test("encoders enforce the frame size limit inclusively", () => {
  assert.equal(encodeMessageFrame(new Uint8Array(16), 16).byteLength, 18);
  const exceeds = (error) => error instanceof TrevRpcError && error.code === Code.ResourceExhausted;
  assert.throws(() => encodeMessageFrame(new Uint8Array(17), 16), exceeds);
  assert.throws(() => encodeFrame(0x21, new Uint8Array(17), 16), exceeds);
  assert.throws(() => encodeRequestFrame(request, encodeMessage(new Uint8Array(17)), 16), exceeds);
  // A compressed message counts at its message size, not its payload size.
  const compressed = { type: FrameType.ZstdMessage, payload: new Uint8Array(8), size: 17 };
  assert.throws(() => encodeOutgoingFrame(compressed, 16), exceeds);
  assert.throws(() => encodeRequestFrame(request, compressed, 16), exceeds);
  assert.equal(encodeOutgoingFrame({ ...compressed, size: 16 }, 16).byteLength, 10);
  assert.throws(() => encodeRequestFrame(request, undefined, 4), exceeds);
});

test("varints round trip, including values beyond 2^53", () => {
  for (const value of [0, 63, 64, 16383, 16384, 0x3fffffff, 0x40000000, 2 ** 52]) {
    const encoded = new Uint8Array(8);
    const end = writeVarint(encoded, 0, value);
    assert.equal(end, varintLength(value));
    assert.deepEqual(readVarint(encoded, 0), { value, next: end }, String(value));
  }
  const max = (1n << 62n) - 1n;
  const encoded = new Uint8Array(8);
  writeVarint(encoded, 0, max);
  assert.equal(hex(encoded), "ffffffffffffffff");
  assert.equal(readVarint(encoded, 0).value, max);
});

/** `levels` unknown groups (field 19), each inside the last. */
function nestedGroups(levels) {
  return [...Array(levels).fill([0x9b, 0x01]).flat(), ...Array(levels).fill([0x9c, 0x01]).flat()];
}

const isInvalidArgument = (error) => error.code === Code.InvalidArgument;

test("malformed wire payloads map to invalid argument", () => {
  // RpcStatus is { uint32 code = 1; string message = 2; map<string, bytes> metadata = 3; ... }.
  const status = [
    [0xff], // truncated tag
    [0x12], // truncated length
    [0x12, 0x05, 0x61], // truncated string
    [0x12, 0x02, 0xc3, 0x28], // invalid UTF-8 in a string
    [0x1a, 0x04, 0x0a, 0x02, 0xc3, 0x28], // invalid UTF-8 in a map key
    [0x0a, 0x01, 0x00], // wrong wire type for a uint32
    [0x18, 0x00], // wrong wire type for a map
    [0x1a, 0x02, 0x08, 0x00], // wrong wire type for a map key
    [0x1a, 0x02, 0x0a, 0x05, 0x61, 0x61, 0x61, 0x61, 0x61], // a field past its entry's end
    [0x08, 0xff, 0xff, 0xff, 0xff, 0x1f], // varint above uint32
    [0x08, ...Array(10).fill(0xff), 0x01], // varint longer than 10 bytes
    [0x98, 0x01, ...Array(9).fill(0xff), 0x02], // unknown 10-byte varint above 64 bits
    [0x98, 0x01, 0xff], // truncated unknown varint
    [0x00, 0x01], // field number zero
    [0x80, 0x80, 0x80, 0x80, 0x10, 0x00], // tag above 32 bits
    [0xfe, 0x01], // wire type 6
    [0xfc, 0x01], // a stray end-group tag
    [0xfb, 0x01], // unterminated group
    [0xfb, 0x01, 0x84, 0x02], // group end tag for another field
    [0xfb, 0x01, 0x00, 0x00, 0xfc, 0x01], // field number zero inside a group
    nestedGroups(65),
  ];
  // RpcRequest adds packed uint32s (6), DictionaryInfo (7) and ConnectionMetadata (9) messages.
  const request = [
    [0x32, 0x05, 0xff, 0xff, 0xff, 0xff, 0x1f], // packed varint above uint32
    [0x32, 0x02, 0x01], // packed run past its field's end
    [0x22, 0x00], // wrong wire type for a uint64
    [0x38, 0x00], // wrong wire type for a message
    [0x3a, 0x03, 0x0a, 0x01, 0x00], // wrong wire type inside a nested message
    [0x4a, 0x02, 0x10, 0x00], // wrong wire type for a map inside a nested message
  ];
  for (const [codec, payloads] of [
    [RpcStatus, status],
    [RpcRequest, request],
  ]) {
    for (const payload of payloads) {
      assert.throws(
        () => decodeWireMessage(codec, new Uint8Array(payload)),
        isInvalidArgument,
        `${codec.name} ${hex(payload)}`,
      );
    }
  }
});

test("well-formed wire payloads decode, including unknown fields of every wire type", () => {
  const status = (payload) => decodeWireMessage(RpcStatus, new Uint8Array(payload));
  assert.equal(status([0x08, 0xff, 0xff, 0xff, 0xff, 0x0f]).code, 0xffffffff);
  assert.equal(status([0x08, 0x81, 0x80, 0x80, 0x80, 0x00]).code, 1); // padded but within uint32
  assert.equal(status(nestedGroups(64)).code, 0);

  const unknown = [
    [0x98, 0x01, 0x96, 0x01], // varint
    [0x99, 0x01, 1, 2, 3, 4, 5, 6, 7, 8], // fixed64
    [0x9a, 0x01, 0x02, 0xc3, 0x28], // length-delimited, not checked as UTF-8
    [0x9b, 0x01, 0x08, 0x01, 0xa3, 0x01, 0xa4, 0x01, 0x9c, 0x01], // group holding a group
    [0x9d, 0x01, 1, 2, 3, 4], // fixed32
  ].flat();
  assert.equal(status([...unknown, 0x12, 0x01, 0x62]).message, "b");

  // Packed and unpacked uint32s mix; a later map entry for a key replaces the earlier one.
  const request = decodeWireMessage(
    RpcRequest,
    new Uint8Array(
      [
        [0x32, 0x02, 0x01, 0x02, 0x30, 0x03],
        [0x12, 0x06, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x31],
        [0x12, 0x06, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x32],
        [0x20, ...Array(9).fill(0xff), 0x01],
      ].flat(),
    ),
  );
  assert.deepEqual(request.acceptDictionaries, [1, 2, 3]);
  assert.deepEqual(Object.keys(request.metadata), ["k"]);
  assert.equal(hex(request.metadata.k), "32");
  assert.equal(request.timeoutNanos, 2n ** 64n - 1n);
});

test("a metadata key named like an object property stays an entry", () => {
  // A computed key is an own property, unlike `__proto__: value`.
  const metadata = { ["__proto__"]: bytes("x"), constructor: bytes("y") };
  const decoded = decodeWireMessage(
    RpcHeaders,
    encodeWireMessage(RpcHeaders, { metadata }),
  ).metadata;
  assert.deepEqual(Object.keys(decoded).sort(), ["__proto__", "constructor"]);
  assert.equal(Object.getPrototypeOf(decoded), Object.prototype);
  assert.equal(hex(decoded.constructor), hex(bytes("y")));
});

test("arbitrary bytes either decode or fail with invalid argument", () => {
  let seed = 1;
  const random = () => (seed = (Math.imul(seed, 1103515245) + 12345) >>> 0) / 2 ** 32;
  const cases = [
    [
      (input) => decodeWireMessage(RpcRequest, input),
      encodeWireMessage(RpcRequest, {
        method: "/svc/m",
        metadata: { a: bytes("1") },
        kind: RpcKind.BidirectionalStreaming,
        timeoutNanos: 300n,
        acceptDictionaries: [1, 300],
        dictionaryQuery: [{ id: 1, fingerprint: bytes("fp") }],
        connectionMetadata: { id: 2, metadata: { b: bytes("2") } },
      }),
    ],
    [
      (input) => unmarshalMessage(SampleSchema, input),
      marshalMessage(SampleSchema, {
        name: "hi",
        count: 300n,
        labels: { 5: "x" },
        parent: { history: [1n, -2n], children: [{ gauge: 2 }] },
        attributes: { a: [1, "b"] },
      }),
    ],
  ];
  for (const [decode, valid] of cases) {
    for (let iteration = 0; iteration < 10_000; iteration += 1) {
      const input = valid.slice();
      for (let mutation = Math.floor(random() * 4); mutation > 0; mutation -= 1) {
        input[Math.floor(random() * input.length)] = Math.floor(random() * 256);
      }
      const cut = random() < 0.2 ? input.subarray(0, Math.floor(random() * input.length)) : input;
      try {
        decode(cut);
      } catch (error) {
        assert.equal(error.code, Code.InvalidArgument, hex(cut));
      }
    }
  }
});

test("malformed messages map to invalid argument", () => {
  // types.v1.Sample: string name = 1; ... Sample parent = 15.
  for (const payload of [
    [0x0a, 0x05, 0x61], // truncated string
    [0x0a, 0x02, 0xc3, 0x28], // invalid UTF-8
    [0x7a, 0x05, 0x0a], // a nested message past the end
    [0x00, 0x01], // field number zero
  ]) {
    assert.throws(
      () => unmarshalMessage(SampleSchema, new Uint8Array(payload)),
      isInvalidArgument,
      hex(payload),
    );
  }
});

test("marshalling rejects values that do not fit the schema", () => {
  assert.equal(hex(marshalMessage(SampleSchema, { parent: { name: "a" } })), "7a030a0161");
  assert.throws(() => marshalMessage(SampleSchema, { small: "x" }), isInvalidArgument);
  assert.throws(() => marshalMessage(EchoRequestSchema, { count: -1 }), isInvalidArgument);
});

test("frame reader reassembles frames from arbitrary chunking", async () => {
  const bodies = [new Uint8Array([1]), new Uint8Array(0), new Uint8Array(300).fill(2)];
  const wire = concatBytes([
    ...bodies.map((body) => encodeMessageFrame(body)),
    encodeFrame(0x3fff, new Uint8Array(16384).fill(3)), // unknown type, 4-byte length
    encodeStatusFrame({ code: Code.Ok }),
  ]);

  for (const size of [1, 2, 3, 7, 4096, wire.byteLength]) {
    const reader = new FrameReader(readerOf(chunked(wire, size)));
    for (const body of bodies) {
      const frame = await reader.readFrame();
      assert.equal(frame.type, FrameType.Message, `chunk size ${size}`);
      assert.deepEqual(frame.payload, body, `chunk size ${size}`);
    }
    assert.equal((await reader.readFrame()).type, FrameType.Status);
    assert.equal(await reader.readFrame(), null);
  }
});

test("a frame inside one chunk is returned as a view, not a copy", async () => {
  const wire = concatBytes([encodeMessageFrame(new Uint8Array([1, 2, 3]))]);
  const frame = await new FrameReader(readerOf([wire])).readFrame();
  assert.equal(frame.payload.buffer, wire.buffer);
});

test("unknown frames are skipped wherever they appear", async () => {
  const unknown = (type, length = 3) => encodeFrame(type, new Uint8Array(length).fill(9));
  const wire = concatBytes([
    unknown(0x21), // grease before everything
    encodeMessageFrame(bytes("a")),
    unknown(0x40, 0),
    unknown(7),
    unknown(2 ** 40), // 8-byte type that still fits a number
    encodeFrame(2n ** 62n - 1n, new Uint8Array(2)), // largest type, a bigint
    encodeMessageFrame(bytes("b")),
    encodeStatusFrame({ code: Code.Ok }),
    unknown(0x5f), // grease after STATUS
    encodeFrame(2n ** 53n, new Uint8Array(1)),
  ]);

  for (const size of [1, 4, wire.byteLength]) {
    const reader = new FrameReader(readerOf(chunked(wire, size)));
    const seen = [];
    for (let frame; (frame = await reader.readFrame()) !== null;) {
      seen.push(frame.type);
    }
    assert.deepEqual(seen, [FrameType.Message, FrameType.Message, FrameType.Status], `${size}`);
  }
});

test("unknown frames count toward the frame size limit", async () => {
  const wire = encodeFrame(0x21, new Uint8Array(17));
  await assert.rejects(
    new FrameReader(readerOf([wire])).readFrame(16),
    (error) => error.code === Code.ResourceExhausted,
  );
  assert.equal(await new FrameReader(readerOf([wire])).readFrame(17), null);
});

test("frame reader rejects oversized lengths before buffering the payload", async () => {
  const exceeds = (error) => error instanceof TrevRpcError && error.code === Code.ResourceExhausted;
  // Only the header arrives; a reader that tried to buffer the payload would hit end of stream.
  const header = (type, length) => {
    const out = new Uint8Array(16);
    return out.subarray(0, writeVarint(out, writeVarint(out, 0, type), length));
  };
  for (const type of [FrameType.Message, FrameType.Status, 0x21, 2n ** 60n]) {
    for (const length of [1025, 1 << 30, 2 ** 40, 2n ** 62n - 1n]) {
      const reader = new FrameReader(readerOf([header(type, length)]));
      await assert.rejects(reader.readFrame(1024), exceeds, `type ${type}, length ${length}`);
    }
  }
  // The limit is inclusive.
  const full = concatBytes([header(FrameType.Message, 1024), new Uint8Array(1024)]);
  assert.equal((await new FrameReader(readerOf([full])).readFrame(1024)).payload.byteLength, 1024);
});

test("frame reader fails on streams that end mid-frame", async () => {
  const wire = concatBytes([
    encodeFrame(0x4000, new Uint8Array(20)), // 4-byte type, 1-byte length
    encodeFrame(FrameType.Message, new Uint8Array(300)), // 2-byte length
  ]);
  const second = encodeFrame(0x4000, new Uint8Array(20)).byteLength;
  assert.equal(second, 4 + 1 + 20);

  // Cut inside the type varint, between type and length, inside the length varint, and inside
  // the payload, of both frames.
  for (const length of [
    1,
    2,
    3,
    4,
    5,
    6,
    24,
    second + 1,
    second + 2,
    second + 3,
    wire.byteLength - 1,
  ]) {
    for (const size of [1, wire.byteLength]) {
      const reader = new FrameReader(readerOf(chunked(wire.subarray(0, length), size)));
      await assert.rejects(
        (async () => {
          for (;;) {
            if ((await reader.readFrame()) === null) {
              return;
            }
          }
        })(),
        (error) => error.code === Code.Unavailable,
        `cut at ${length}, chunks of ${size}`,
      );
    }
  }
  assert.equal(await new FrameReader(readerOf([])).readFrame(), null);
});

test("frame reader expects only unknown frames and a clean end after the final frame", async () => {
  const unavailable = (error) => error.code === Code.Unavailable;
  const internal = (error) => error.code === Code.Internal;
  await new FrameReader(readerOf([])).expectEnd();
  await new FrameReader(
    readerOf([concatBytes([encodeFrame(0x21, bytes("x")), encodeFrame(2n ** 61n, bytes("y"))])]),
  ).expectEnd();

  for (const known of [FrameType.Message, FrameType.Request, FrameType.Headers, FrameType.Status]) {
    await assert.rejects(
      new FrameReader(readerOf([encodeFrame(known, new Uint8Array(0))])).expectEnd(),
      internal,
    );
  }
  await assert.rejects(
    new FrameReader(readerOf([new Uint8Array([0x21])])).expectEnd(),
    unavailable,
  );
  await assert.rejects(
    new FrameReader(readerOf([encodeFrame(0x21, new Uint8Array(17))])).expectEnd(16),
    (error) => error.code === Code.ResourceExhausted,
  );

  const wire = concatBytes([encodeStatusFrame({ code: Code.Ok }), encodeMessageFrame(bytes("x"))]);
  const reader = new FrameReader(readerOf([wire]));
  assert.equal((await reader.readFrame()).type, FrameType.Status);
  await assert.rejects(reader.expectEnd(), internal);
});

test("metadata is normalized to lowercase keys and byte values", () => {
  const metadata = normalizeMetadata(
    new Map([
      ["Authorization", "Bearer x"],
      ["Trace", new Uint8Array([1])],
      ["list", [1, 2]],
    ]),
  );
  assert.deepEqual(Object.keys(metadata).sort(), ["authorization", "list", "trace"]);
  assert.equal(Buffer.from(metadata.authorization).toString(), "Bearer x");
  assert.deepEqual([...metadata.list], [1, 2]);
  assert.throws(
    () => normalizeMetadata({ key: 1 }),
    (error) => error.code === Code.InvalidArgument,
  );
});

test("metadata validation limits are inclusive", () => {
  const invalid = (metadata) => {
    assert.throws(
      () => validateMetadata(metadata),
      (error) => error.code === Code.InvalidArgument,
    );
  };

  const maxEntries = Object.fromEntries(
    Array.from({ length: MaxMetadataEntries }, (_, index) => [`key-${index}`, new Uint8Array()]),
  );
  validateMetadata(maxEntries);
  invalid({ ...maxEntries, overflow: new Uint8Array() });

  validateMetadata({ ["a".repeat(MaxMetadataKeyLen)]: [] });
  invalid({ ["a".repeat(MaxMetadataKeyLen + 1)]: [] });

  validateMetadata({ key: new Uint8Array(MaxMetadataValueLen) });
  invalid({ key: new Uint8Array(MaxMetadataValueLen + 1) });

  const exactTotal = Object.fromEntries(
    "abcdefgh".split("").map((key) => [key, new Uint8Array(MaxMetadataTotalSize / 8 - 1)]),
  );
  validateMetadata(exactTotal);
  invalid({ ...exactTotal, i: new Uint8Array() });

  invalid({ "": [] });
  invalid({ "Upper-Case": [] });
  invalid({ "bad key": [] });
  invalid({ "trevrpc-reserved": [] });
});
