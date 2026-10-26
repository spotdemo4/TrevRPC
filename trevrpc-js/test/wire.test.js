import assert from "node:assert/strict";
import test from "node:test";

import { encodeMessage } from "../src/compression.js";
import { decodeDatagram, encodeDatagram } from "../src/datagram.js";
import {
  FrameReader,
  FrameType,
  encodeFrame,
  encodeHeadersFrame,
  encodeMessageFrame,
  encodeOutgoingFrame,
  encodeRequestFrame,
  encodeStatusFrame,
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
import { RpcHeaders, RpcKind, RpcRequest, RpcStatus, createRoot } from "../src/wire.js";
import { chunked, concatBytes, hex, readGoldenVectors, readerOf } from "./support.js";

const bytes = (text) => new TextEncoder().encode(text);
const fromHex = (value) => Uint8Array.from(Buffer.from(value, "hex"));

const request = { method: "/svc/m" };

/** The documented logical value of every golden vector: [frame type, message type, fields]. */
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
  "request.extensions": [FrameType.Request, RpcRequest, { ...request, extensions: [1, 300] }],
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
  "headers.connection_metadata": [FrameType.Headers, RpcHeaders, { connectionMetadata: 1 }],
  "headers.empty": [FrameType.Headers, RpcHeaders, {}],
  "headers.extensions": [FrameType.Headers, RpcHeaders, { extensions: [1] }],
  "status.ok": [FrameType.Status, RpcStatus, { code: Code.Ok }],
  "status.unavailable": [FrameType.Status, RpcStatus, { code: Code.Unavailable, message: "down" }],
  "status.metadata": [
    FrameType.Status,
    RpcStatus,
    { code: Code.FailedPrecondition, message: "stop", metadata: { "x-t": bytes("2") } },
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
  for (const [name, [type, messageType, fields]] of Object.entries(goldenCases)) {
    const payload = messageType === undefined ? fields : marshalMessage(messageType, fields);
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
    if (messageType !== undefined) {
      const decoded = unmarshalMessage(messageType, read.payload);
      assert.equal(hex(marshalMessage(messageType, decoded)), hex(payload), `${name} round trip`);
    }
  }
});

test("golden payloads decode to their logical fields", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  const decode = (name, type) => unmarshalMessage(type, fromHex(vectors.get(`${name}.payload`)));

  const withExtensions = decode("request.extensions", RpcRequest);
  assert.deepEqual(withExtensions.extensions.map(Number), [1, 300]);
  assert.equal(withExtensions.method, "/svc/m");

  const timeout = decode("request.bidi_timeout", RpcRequest);
  assert.equal(timeout.kind, RpcKind.BidirectionalStreaming);
  assert.equal(Number(timeout.timeoutNanos), 123456);

  const flow = decode("request.datagram_flow", RpcRequest);
  assert.equal(flow.kind, RpcKind.BidirectionalStreaming);
  assert.equal(Number(flow.datagramFlow), 1);
  assert.equal(Number(decode("request.unary", RpcRequest).datagramFlow), 0);

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
  assert.equal(decode("request.unary", RpcRequest).connectionMetadata, null);
  assert.equal(decode("headers.connection_metadata", RpcHeaders).connectionMetadata, 1);
  assert.equal(decode("headers.empty", RpcHeaders).connectionMetadata, 0);
  assert.deepEqual(decode("headers.extensions", RpcHeaders).extensions.map(Number), [1]);
  assert.equal(Buffer.from(decode("headers.metadata", RpcHeaders).metadata["x-a"]) + "", "1");

  const status = decode("status.metadata", RpcStatus);
  assert.equal(status.code, Code.FailedPrecondition);
  assert.equal(status.message, "stop");
  assert.equal(Buffer.from(status.metadata["x-t"]) + "", "2");
  assert.equal(decode("status.ok", RpcStatus).code, Code.Ok);
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

test("the reserved request fields are ignored when decoding", () => {
  // Field 2 (old method name), field 3 and field 6 (old version) from an older peer.
  const payload = fromHex("0a062f7376632f6d" + "12016d" + "1a0100" + "3001");
  const decoded = unmarshalMessage(RpcRequest, payload);
  assert.equal(decoded.method, "/svc/m");
  assert.equal(decoded.version, undefined);
  assert.equal(hex(marshalMessage(RpcRequest, decoded)), "0a062f7376632f6d");
});

test("status and headers encoders match the golden frames", async () => {
  const vectors = await readGoldenVectors("wire-golden-vectors.txt");
  assert.equal(
    hex(encodeStatusFrame({ code: Code.Unavailable, message: "down" })),
    vectors.get("status.unavailable.frame"),
  );
  assert.equal(
    hex(encodeHeadersFrame({ metadata: { "x-a": bytes("1") } })),
    vectors.get("headers.metadata.frame"),
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

test("malformed protobuf maps to invalid argument", () => {
  const Message = createRoot({
    nested: {
      t: {
        nested: {
          M: { fields: { text: { type: "string", id: 1 }, n: { type: "uint32", id: 2 } } },
        },
      },
    },
  }).lookupType("t.M");
  const malformed = [
    [0xff],
    [0x0a],
    [0x0a, 0x05, 0x61],
    [0x0a, 0x02, 0xc3, 0x28], // invalid UTF-8 in a string
    [0x12, 0x01, 0x00], // wrong wire type for a uint32
    [0x10, 0xff, 0xff, 0xff, 0xff, 0x1f], // varint above uint32
    [0x00, 0x01], // field number zero
  ];

  for (const bytes of malformed) {
    assert.throws(
      () => unmarshalMessage(Message, new Uint8Array(bytes)),
      (error) => error.code === Code.InvalidArgument,
      hex(bytes),
    );
  }
  assert.equal(unmarshalMessage(Message, new Uint8Array([0x0a, 0x01, 0x61])).text, "a");
});

test("marshalling rejects values that do not fit the schema", () => {
  const Outer = createRoot({
    nested: {
      t: {
        nested: {
          Inner: { fields: { id: { type: "uint32", id: 1 } } },
          Outer: { fields: { inner: { type: "Inner", id: 1 } } },
        },
      },
    },
  }).lookupType("t.Outer");

  assert.deepEqual([...marshalMessage(Outer, { inner: { id: 7 } })], [0x0a, 0x02, 0x08, 0x07]);
  assert.throws(
    () => marshalMessage(Outer, { inner: 5 }),
    (error) => error.code === Code.InvalidArgument && /object expected/.test(error.statusMessage),
  );
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
    unknown(6),
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
