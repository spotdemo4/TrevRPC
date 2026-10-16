import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  FrameReader,
  decodeStreamFrame,
  encodeFrame,
  encodeMessageFrame,
  frameBodyLength,
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
import {
  RpcKind,
  RpcRequest,
  RpcResponse,
  RpcStreamFrame,
  RpcStreamFrameKind,
  WireVersion,
  createRoot,
} from "../src/wire.js";
import { chunked, concatBytes, hex, readerOf } from "./support.js";

const goldenPath = new URL("../../testdata/wire-golden-vectors.txt", import.meta.url);

test("wire encodings match the shared golden vectors", async () => {
  const vectors = await readGoldenVectors();
  const request = {
    service: "svc",
    method: "m",
    body: new Uint8Array([104, 105]),
    metadata: {},
    version: WireVersion,
  };
  const cases = {
    "rpc_request.unary": [RpcRequest, request],
    "rpc_request.timeout": [RpcRequest, { ...request, timeoutNanos: 123456 }],
    "rpc_request.metadata": [
      RpcRequest,
      { ...request, metadata: { authorization: new Uint8Array([111, 107]) } },
    ],
    "rpc_stream_frame.message": [RpcStreamFrame, { body: new Uint8Array([104, 105]) }],
    "rpc_stream_frame.status": [
      RpcStreamFrame,
      { kind: RpcStreamFrameKind.Status, status: Code.Unavailable, message: "down" },
    ],
    "rpc_response.ok_body": [RpcResponse, { body: new Uint8Array([104, 105]) }],
    "rpc_response.unavailable": [RpcResponse, { status: Code.Unavailable, message: "down" }],
  };

  assert.deepEqual(
    new Set(Object.keys(cases)),
    new Set([...vectors.keys()].map((name) => name.replace(/\.(body|frame)$/, ""))),
    "every golden vector needs a case",
  );
  for (const [name, [type, message]] of Object.entries(cases)) {
    assert.equal(hex(marshalMessage(type, message)), vectors.get(`${name}.body`), name);
    assert.equal(hex(encodeFrame(type, message)), vectors.get(`${name}.frame`), name);
  }
});

test("golden frames decode back to their messages", async () => {
  const vectors = await readGoldenVectors();
  const status = unmarshalMessage(
    RpcStreamFrame,
    Buffer.from(vectors.get("rpc_stream_frame.status.body"), "hex"),
  );
  assert.equal(status.kind, RpcStreamFrameKind.Status);
  assert.equal(status.status, Code.Unavailable);
  assert.equal(status.message, "down");

  const request = unmarshalMessage(
    RpcRequest,
    Buffer.from(vectors.get("rpc_request.metadata.body"), "hex"),
  );
  assert.equal(request.service, "svc");
  assert.equal(request.kind, RpcKind.Unary);
  assert.equal(Buffer.from(request.metadata.authorization).toString(), "ok");
});

test("frame length limits are inclusive", () => {
  const header = (length) => {
    const bytes = new Uint8Array(4);
    new DataView(bytes.buffer).setUint32(0, length, false);
    return bytes;
  };

  for (const length of [0, 1, 15, 16]) {
    assert.equal(frameBodyLength(header(length), 16), length);
  }
  for (const length of [17, 0xffffffff]) {
    assert.throws(
      () => frameBodyLength(header(length), 16),
      (error) => error instanceof TrevRpcError && error.code === Code.ResourceExhausted,
    );
  }
  assert.throws(
    () => encodeFrame(RpcResponse, { body: new Uint8Array(32) }, 16),
    (error) => error.code === Code.ResourceExhausted,
  );
});

test("message stream frames match generic protobuf encoding", () => {
  for (const size of [0, 1, 127, 128, 16383, 16384, 70000]) {
    const body = new Uint8Array(size).fill(7);
    const generic = encodeFrame(RpcStreamFrame, { body });
    assert.deepEqual(encodeMessageFrame(body), generic, `${size} byte body`);

    const decoded = decodeStreamFrame(generic.subarray(4));
    assert.equal(decoded.kind, RpcStreamFrameKind.Message);
    assert.deepEqual(decoded.body, body);
  }
  assert.throws(
    () => encodeMessageFrame(new Uint8Array(32), 16),
    (error) => error.code === Code.ResourceExhausted,
  );
});

test("a message frame body is returned as a view, not a copy", () => {
  const frame = encodeMessageFrame(new Uint8Array([1, 2, 3]));
  const decoded = decodeStreamFrame(frame.subarray(4));
  assert.equal(decoded.body.buffer, frame.buffer);
});

test("stream frames with extra fields take the generic decoding path", () => {
  const frame = encodeFrame(RpcStreamFrame, {
    kind: RpcStreamFrameKind.Message,
    body: new Uint8Array([9]),
    metadata: { key: new Uint8Array([1]) },
  });
  const decoded = decodeStreamFrame(frame.subarray(4));
  assert.deepEqual([...decoded.body], [9]);

  const status = decodeStreamFrame(
    encodeFrame(RpcStreamFrame, { kind: RpcStreamFrameKind.Status, status: 5 }).subarray(4),
  );
  assert.equal(status.kind, RpcStreamFrameKind.Status);
  assert.equal(status.status, Code.NotFound);
});

test("unknown stream frame kinds are invalid arguments", () => {
  const body = new Uint8Array([0x08, 0x07]);
  assert.throws(
    () => decodeStreamFrame(body),
    (error) => error.code === Code.InvalidArgument && /kind 7/.test(error.statusMessage),
  );
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
  const wire = concatBytes(bodies.map((body) => encodeMessageFrame(body)));

  for (const size of [1, 2, 3, 7, 4096]) {
    const reader = new FrameReader(readerOf(chunked(wire, size)));
    for (const body of bodies) {
      assert.deepEqual((await reader.readStreamFrame()).body, body, `chunk size ${size}`);
    }
    assert.equal(await reader.readStreamFrame(), null);
  }
});

test("frame reader fails on streams that end mid-frame", async () => {
  const wire = encodeMessageFrame(new Uint8Array(20));
  for (const length of [1, 3, 5, wire.byteLength - 1]) {
    const reader = new FrameReader(readerOf([wire.subarray(0, length)]));
    await assert.rejects(
      reader.readStreamFrame(),
      (error) => error.code === Code.Unavailable,
      `${length} bytes`,
    );
  }
  await assert.rejects(new FrameReader(readerOf([])).readFrame(RpcResponse), (error) => {
    return error.code === Code.Unavailable;
  });
});

test("frame reader enforces the frame size limit before reading the body", async () => {
  const header = new Uint8Array(4);
  new DataView(header.buffer).setUint32(0, 1 << 30, false);
  const reader = new FrameReader(readerOf([header]));
  await assert.rejects(reader.readFrame(RpcResponse, 1024), (error) => {
    return error.code === Code.ResourceExhausted;
  });
});

test("frame reader expects a clean end of stream", async () => {
  await new FrameReader(readerOf([])).expectEnd();
  await assert.rejects(
    new FrameReader(readerOf([new Uint8Array([0])])).expectEnd(),
    (error) => error.code === Code.Unavailable,
  );

  const wire = concatBytes([encodeMessageFrame(new Uint8Array([1])), new Uint8Array([9])]);
  const reader = new FrameReader(readerOf([wire]));
  await reader.readStreamFrame();
  await assert.rejects(reader.expectEnd(), (error) => error.code === Code.Unavailable);
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
    [..."abcdefgh"].map((key) => [key, new Uint8Array(MaxMetadataTotalSize / 8 - 1)]),
  );
  validateMetadata(exactTotal);
  invalid({ ...exactTotal, i: new Uint8Array() });

  invalid({ "": [] });
  invalid({ "Upper-Case": [] });
  invalid({ "bad key": [] });
  invalid({ "trevrpc-reserved": [] });
});

async function readGoldenVectors() {
  const vectors = new Map();
  for (const [index, rawLine] of (await readFile(goldenPath, "utf8")).split(/\r?\n/u).entries()) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) {
      continue;
    }
    const [name, value] = line.split("=").map((part) => part.trim());
    assert.match(value ?? "", /^(?:[0-9a-f]{2})*$/u, `invalid golden vector on line ${index + 1}`);
    vectors.set(name, value);
  }
  return vectors;
}
