// The codecs of protoc-gen-trevrpc-js against protobuf-es: for random messages of every type in
// test/proto/codec.proto, a codec must encode to the bytes toBinary() produces and decode to what
// fromBinary() returns, key order and `$unknown` included, and reject exactly the inputs and
// values protobuf-es rejects. Most tests run a codec's read and write bare, without the
// protobuf-es fallback of its encode() and decode(), so that a codec bug can't hide behind it.
import assert from "node:assert/strict";
import test from "node:test";

import {
  ScalarType,
  create,
  fromBinary,
  mergeFromBinary,
  protoInt64,
  toBinary,
} from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";

import * as editionsPb from "../build/test/gen/codec_editions_pb.js";
import * as pb from "../build/test/gen/codec_pb.js";
import * as gen from "../build/test/gen/codec_trevrpc.js";
import * as interopPb from "../build/test/gen/interop_pb.js";
import * as interopGen from "../build/test/gen/interop_trevrpc.js";
import { createClient } from "../src/client.js";
import { Reader, Writer, defineCodec } from "../src/codec.js";
import { encodeMessageFrame } from "../src/framing.js";
import { Code } from "../src/status.js";
import { fakeTransport } from "./support.js";

// ---------------------------------------------------------------------------- random values

let seed = 12345;
function rand() {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const int = (n) => Math.floor(rand() * n);
const pick = (values) => values[int(values.length)];
const chance = (p) => rand() < p;

const Strings = [
  "",
  "a",
  "hello",
  "São Paulo",
  "Zürich",
  "日本語テキスト",
  "emoji 😀🎉",
  "\u0000nul",
  "\ud800 lone high",
  "lone low \udc00",
  "\ufeffleading byte order mark",
  "\ud800\ue000 lone high before U+E000",
  "\udbff\udfff",
  "日".repeat(42),
  "日".repeat(43),
  // Around the thresholds of the one-byte length prefix and the JS string paths.
  "x".repeat(32),
  "x".repeat(33),
  "x".repeat(42),
  "x".repeat(43),
  "x".repeat(64),
  "x".repeat(65),
  "é".repeat(63),
  "é".repeat(64),
  "é".repeat(65),
  `${"y".repeat(126)}é`,
  "z".repeat(200),
  "ü".repeat(100) + "😀".repeat(30),
  "w".repeat(20000),
  "a".repeat(256),
  "a".repeat(257),
  "é".repeat(256),
  "é".repeat(257),
  `${"a".repeat(255)}😀`,
  `${"😀".repeat(21)}x`,
];
const Int32s = [0, 1, -1, 127, 128, 255, 16383, 16384, 2147483647, -2147483648, -123456];
const Uint32s = [0, 1, 127, 128, 16384, 2097151, 2097152, 268435455, 268435456, 4294967295];
const Doubles = [
  0,
  -0,
  1,
  -1.5,
  1e-300,
  1e300,
  Infinity,
  -Infinity,
  NaN,
  Math.PI,
  Number.MIN_VALUE,
];
const Int64s = [
  0n,
  1n,
  -1n,
  127n,
  128n,
  2n ** 31n,
  -(2n ** 31n),
  2n ** 32n,
  2n ** 63n - 1n,
  -(2n ** 63n),
];
const Uint64s = [0n, 1n, 127n, 128n, 2n ** 32n - 1n, 2n ** 32n, 2n ** 53n + 1n, 2n ** 64n - 1n];

function randomInt64() {
  return chance(0.5)
    ? pick(Int64s)
    : BigInt.asIntN(64, (BigInt(int(2 ** 32)) << 32n) | BigInt(int(2 ** 32)));
}

function randomUint64() {
  return chance(0.5)
    ? pick(Uint64s)
    : BigInt.asUintN(64, (BigInt(int(2 ** 32)) << 32n) | BigInt(int(2 ** 32)));
}

function scalar(type, longAsString = false) {
  switch (type) {
    case ScalarType.INT32:
    case ScalarType.SINT32:
    case ScalarType.SFIXED32:
      return chance(0.5) ? pick(Int32s) : int(2 ** 32) | 0;
    case ScalarType.UINT32:
    case ScalarType.FIXED32:
      return chance(0.5) ? pick(Uint32s) : int(2 ** 32);
    case ScalarType.INT64:
    case ScalarType.SINT64:
    case ScalarType.SFIXED64: {
      const v = randomInt64();
      return longAsString ? String(v) : v;
    }
    case ScalarType.UINT64:
    case ScalarType.FIXED64: {
      const v = randomUint64();
      return longAsString ? String(v) : v;
    }
    case ScalarType.BOOL:
      return chance(0.5);
    case ScalarType.DOUBLE:
      return chance(0.5) ? pick(Doubles) : (rand() - 0.5) * 10 ** int(20);
    case ScalarType.FLOAT:
      return Math.fround(chance(0.5) ? pick(Doubles) : (rand() - 0.5) * 10 ** int(20));
    case ScalarType.STRING:
      if (chance(0.4)) {
        return pick(Strings);
      }
      // ASCII of every short length, which decodes on its own paths, or any UTF-16 units.
      return String.fromCharCode(
        ...Array.from({ length: int(80) }, () => (chance(0.5) ? int(0x80) : int(0x10000))),
      );
    case ScalarType.BYTES:
      return Uint8Array.from({ length: chance(0.1) ? 300 : int(10) }, () => int(256));
  }
  throw new Error(`no generator for scalar type ${type}`);
}

function enumValue(field) {
  // Open enums keep values they don't know.
  return chance(0.8) ? pick(field.enum.values).number : int(1000) - 500;
}

function mapKey(type) {
  return type === ScalarType.BOOL ? String(chance(0.5)) : String(scalar(type));
}

/** A random init object for `desc`, or a message ready to encode when `desc` has a create(). */
function randomInit(desc, depth) {
  const init = {};
  for (const member of desc.members) {
    if (member.kind === "oneof") {
      if (chance(0.25)) {
        continue;
      }
      const field = pick(member.fields);
      if (field.fieldKind === "message" && depth > 3) {
        continue;
      }
      init[member.localName] = { case: field.localName, value: singular(field, depth) };
      continue;
    }
    const field = member;
    // Required fields of the editions file are always set; toBinary() rejects them otherwise.
    if (field.presence !== 3 && chance(0.25)) {
      continue;
    }
    switch (field.fieldKind) {
      case "list":
        init[field.localName] = Array.from({ length: int(depth > 2 ? 2 : 6) }, () =>
          field.listKind === "message"
            ? randomMessage(field.message, depth + 1)
            : field.listKind === "enum"
              ? enumValue(field)
              : scalar(field.scalar, field.longAsString),
        );
        break;
      case "map": {
        const record = {};
        for (let i = 0, n = int(depth > 2 ? 2 : 5); i < n; i++) {
          record[mapKey(field.mapKey)] =
            field.mapKind === "message"
              ? randomMessage(field.message, depth + 1)
              : field.mapKind === "enum"
                ? enumValue(field)
                : scalar(field.scalar);
        }
        init[field.localName] = record;
        break;
      }
      default:
        if (field.fieldKind === "message" && depth > 3) {
          break;
        }
        init[field.localName] = singular(field, depth);
    }
  }
  return init;
}

function singular(field, depth) {
  switch (field.fieldKind) {
    case "message":
      return randomMessage(field.message, depth + 1);
    case "enum":
      return enumValue(field);
    case "scalar":
      return scalar(field.scalar, field.longAsString);
  }
  throw new Error("not a singular field");
}

/** A random value for a message field, which protobuf-es may represent as something else. */
function randomMessage(desc, depth) {
  switch (desc.typeName) {
    case "google.protobuf.Timestamp":
      return timestampFromMs(int(2 ** 40));
    case "google.protobuf.Value":
      return create(desc, { kind: { case: "stringValue", value: pick(Strings) } });
    case "google.protobuf.ListValue":
      return create(desc, {
        values: [{ kind: { case: "numberValue", value: int(100) } }],
      });
    case "google.protobuf.Struct":
      return { name: pick(Strings), count: int(100), nested: { ok: chance(0.5) } };
    case "google.protobuf.StringValue":
      return pick(Strings);
    case "google.protobuf.Int64Value":
      return randomInt64();
    case "google.protobuf.BytesValue":
      return scalar(ScalarType.BYTES);
    case "google.protobuf.Int32Value":
      return create(desc, { value: int(1000) });
  }
  return create(desc, randomInit(desc, depth));
}

// ------------------------------------------------------------------------------ comparison

/** Describes the first difference between a and b, key order and prototypes included. */
function difference(a, b, path) {
  if (Object.is(a, b)) {
    return undefined;
  }
  if (a instanceof Uint8Array || b instanceof Uint8Array) {
    if (
      !(a instanceof Uint8Array && b instanceof Uint8Array) ||
      a.length !== b.length ||
      a.some((x, i) => x !== b[i])
    ) {
      return `${path}: bytes differ`;
    }
    return undefined;
  }
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) {
    return `${path}: ${String(a)} !== ${String(b)}`;
  }
  if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) {
    return `${path}: prototypes differ`;
  }
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.join() !== keysB.join()) {
    return `${path}: keys [${keysA.join()}] !== [${keysB.join()}]`;
  }
  for (const key of keysA) {
    const found = difference(a[key], b[key], `${path}.${key}`);
    if (found !== undefined) {
      return found;
    }
  }
  return undefined;
}

function assertSame(actual, expected, what) {
  const found = difference(actual, expected, "message");
  assert.equal(found, undefined, `${what}: ${found}`);
}

function attempt(fn) {
  try {
    return { ok: true, value: fn() };
  } catch (error) {
    return { ok: false, error };
  }
}

/** Encodes with the bare codec, so that a failure is the codec's own. */
function encodeBare(codec, message) {
  const writer = new Writer(16);
  codec.write(writer, message);
  return writer.finish();
}

function decodeBare(codec, bytes) {
  return codec.read(new Reader(bytes), bytes.length, 0, undefined);
}

function concat(a, b) {
  const both = new Uint8Array(a.length + b.length);
  both.set(a);
  both.set(b, a.length);
  return both;
}

const Codecs = [
  gen.ScalarsCodec,
  gen.ListsCodec,
  gen.MapsCodec,
  gen.OneofsCodec,
  gen.NestedCodec,
  gen.Nested_InnerCodec,
  gen.Nested_Inner_DeepestCodec,
  gen.RecursiveCodec,
  gen.PackedOnlyCodec,
  gen.UnpackedOnlyCodec,
  gen.SubsetCodec,
  gen.ForeignCodec,
  gen.WrappedCodec,
  gen.StructuredCodec,
  gen.HolderCodec,
];
const Iterations = 300;

// --------------------------------------------------------------------------------- tests

test("codecs encode random messages to the bytes of toBinary and decode them as fromBinary", () => {
  for (const codec of Codecs) {
    const { schema } = codec;
    for (let i = 0; i < Iterations; i++) {
      const message = randomMessage(schema, 0);
      const expected = toBinary(schema, message);
      assert.deepEqual(encodeBare(codec, message), expected, `${schema.typeName} #${i}: encode`);
      assertSame(
        decodeBare(codec, expected),
        fromBinary(schema, expected),
        `${schema.typeName} #${i}: decode`,
      );
    }
  }
});

test("codecs merge concatenated messages as fromBinary does", () => {
  for (const codec of Codecs) {
    const { schema } = codec;
    for (let i = 0; i < Iterations / 4; i++) {
      const both = concat(
        toBinary(schema, randomMessage(schema, 0)),
        toBinary(schema, randomMessage(schema, 0)),
      );
      assertSame(decodeBare(codec, both), fromBinary(schema, both), `${schema.typeName} #${i}`);
    }
  }
});

test("codecs decode a message merged into an existing message", () => {
  for (let i = 0; i < Iterations / 4; i++) {
    const into = randomMessage(pb.NestedSchema, 0);
    const bytes = toBinary(pb.NestedSchema, randomMessage(pb.NestedSchema, 0));
    const expected = mergeFromBinary(pb.NestedSchema, structuredClone(into), bytes);
    const merged = gen.NestedCodec.read(new Reader(bytes), bytes.length, 0, into);
    assert.equal(merged, into);
    assertSame(merged, expected, `#${i}`);
  }
});

test("packed and unpacked repeated fields decode each other's encoding", () => {
  for (let i = 0; i < Iterations; i++) {
    const init = {
      v: Array.from({ length: int(8) }, () => scalar(ScalarType.INT32)),
      d: Array.from({ length: int(8) }, () => scalar(ScalarType.DOUBLE)),
      s: Array.from({ length: int(8) }, () => scalar(ScalarType.SINT64)),
      b: Array.from({ length: int(8) }, () => chance(0.5)),
    };
    const packed = toBinary(pb.PackedOnlySchema, create(pb.PackedOnlySchema, init));
    const unpacked = toBinary(pb.UnpackedOnlySchema, create(pb.UnpackedOnlySchema, init));
    assertSame(
      decodeBare(gen.PackedOnlyCodec, unpacked),
      fromBinary(pb.PackedOnlySchema, unpacked),
      `packed from unpacked #${i}`,
    );
    assertSame(
      decodeBare(gen.UnpackedOnlyCodec, packed),
      fromBinary(pb.UnpackedOnlySchema, packed),
      `unpacked from packed #${i}`,
    );
  }
});

test("unknown fields are kept and written again", () => {
  for (let i = 0; i < Iterations; i++) {
    const bytes = toBinary(pb.ScalarsSchema, randomMessage(pb.ScalarsSchema, 0));
    const ours = decodeBare(gen.SubsetCodec, bytes);
    const theirs = fromBinary(pb.SubsetSchema, bytes);
    assertSame(ours, theirs, `decode #${i}`);
    assert.deepEqual(encodeBare(gen.SubsetCodec, ours), toBinary(pb.SubsetSchema, theirs));
  }
  // Unknown groups, which proto2 senders write, are skipped and kept whole.
  const group = Uint8Array.of(0x2b, 0x10, 0x01, 0x33, 0x18, 0x02, 0x34, 0x2c, 0x08, 0x05);
  assertSame(decodeBare(gen.SubsetCodec, group), fromBinary(pb.SubsetSchema, group), "groups");
});

test("the recursion limit is protobuf-es's", () => {
  for (const levels of [99, 100, 101]) {
    let init = { depth: levels };
    for (let i = 1; i < levels; i++) {
      init = { child: init, depth: levels - i };
    }
    const bytes = toBinary(pb.RecursiveSchema, create(pb.RecursiveSchema, init));
    const ours = attempt(() => decodeBare(gen.RecursiveCodec, bytes));
    const theirs = attempt(() => fromBinary(pb.RecursiveSchema, bytes));
    assert.equal(ours.ok, theirs.ok, `${levels} levels`);
  }
});

test("codecs reject the truncated inputs that fromBinary rejects", () => {
  for (const codec of Codecs) {
    const { schema } = codec;
    for (let i = 0; i < 20; i++) {
      const bytes = toBinary(schema, randomMessage(schema, 0));
      // Every prefix of short messages; of long ones, a sample and the last few.
      const step = Math.max(1, Math.floor(bytes.length / 300));
      for (let n = 0; n < bytes.length; n += n >= bytes.length - 16 ? 1 : step) {
        const prefix = bytes.slice(0, n);
        const ours = attempt(() => decodeBare(codec, prefix));
        const theirs = attempt(() => fromBinary(schema, prefix));
        const what = `${schema.typeName}: ${n} of ${bytes.length} bytes`;
        if (!theirs.ok) {
          assert.equal(ours.ok, false, `${what}: accepted what protobuf-es rejects`);
        } else {
          assert.equal(
            ours.ok,
            true,
            `${what}: rejected what protobuf-es accepts: ${String(ours.error)}`,
          );
          assertSame(ours.value, theirs.value, what);
        }
      }
    }
  }
});

test("invalid UTF-8 in a string is an error, as in protobuf-es", () => {
  const strings = [
    [0x80],
    [0x00, 0x80],
    [0xc3],
    [0xc3, 0x28],
    [0xe2, 0x82],
    [0xf0, 0x9f, 0x98],
    [0xed, 0xa0, 0x80],
    [0xc0, 0x80],
    [0xff],
    [...new TextEncoder().encode("x".repeat(40)), 0x80],
    [...new TextEncoder().encode("é".repeat(40)), 0xc3],
  ];
  for (const data of strings) {
    const bytes = Uint8Array.of(0x4a, data.length, ...data);
    assert.throws(() => fromBinary(pb.ScalarsSchema, bytes));
    assert.throws(() => decodeBare(gen.ScalarsCodec, bytes), TypeError);
  }
  // A leading U+FEFF is text, not a byte order mark.
  const bom = toBinary(pb.ScalarsSchema, create(pb.ScalarsSchema, { s: "\ufeffx" }));
  assert.equal(decodeBare(gen.ScalarsCodec, bom).s, "\ufeffx");
});

test("strings around every threshold encode and decode as in protobuf-es", () => {
  for (const s of Strings) {
    const message = create(pb.ListsSchema, { s: [s, s.slice(1), `${s}${s}`] });
    const expected = toBinary(pb.ListsSchema, message);
    assert.deepEqual(encodeBare(gen.ListsCodec, message), expected, JSON.stringify(s.slice(0, 20)));
    assertSame(decodeBare(gen.ListsCodec, expected), fromBinary(pb.ListsSchema, expected), s);
  }
});

test("ASCII strings of every length decode", () => {
  for (let n = 0; n <= 80; n++) {
    const s = Array.from({ length: n }, (_, i) =>
      String.fromCharCode(32 + ((i * 7 + n) % 95)),
    ).join("");
    const bytes = toBinary(pb.ScalarsSchema, create(pb.ScalarsSchema, { s }));
    assert.equal(decodeBare(gen.ScalarsCodec, bytes).s, s);
  }
});

test("codecs decode the non-canonical encodings that fromBinary accepts as it does", () => {
  const inputs = [
    // A bool of 2^32 and one of 2, which are true; a bool padded to 10 bytes, which is false.
    [0x20, 0x80, 0x80, 0x80, 0x80, 0x10],
    [0x20, 0x02],
    [0x20, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00],
    // An int32 written as a 64-bit varint above 2^32, which keeps its low 32 bits.
    [0x08, 0xff, 0xff, 0xff, 0xff, 0x7f],
    // A padded tag, and a uint32 padded to 10 bytes.
    [0x88, 0x00, 0x05],
    [0x10, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00],
  ];
  for (const input of inputs) {
    const bytes = Uint8Array.from(input);
    assertSame(
      decodeBare(gen.ScalarsCodec, bytes),
      fromBinary(pb.ScalarsSchema, bytes),
      input.join(" "),
    );
  }
  // A map entry with two values: a second message merges into the first, a second scalar
  // replaces it.
  const entries = [
    [0x22, 0x0b, 0x0a, 0x01, 0x6b, 0x12, 0x02, 0x08, 0x01, 0x12, 0x02, 0x10, 0x02],
    [0x0a, 0x08, 0x0a, 0x01, 0x6b, 0x12, 0x01, 0x61, 0x12, 0x00],
  ];
  for (const entry of entries) {
    const bytes = Uint8Array.from(entry);
    assertSame(decodeBare(gen.MapsCodec, bytes), fromBinary(pb.MapsSchema, bytes), entry.join(" "));
  }
});

test("map entries reject the tags that fromBinary rejects", () => {
  for (const input of [
    // A field-0 tag in an entry, and an invalid wire type.
    [0x0a, 0x02, 0x00, 0x00],
    [0x0a, 0x02, 0x1e, 0x00],
  ]) {
    const bytes = Uint8Array.from(input);
    assert.throws(() => fromBinary(pb.MapsSchema, bytes), /illegal tag/);
    assert.throws(() => decodeBare(gen.MapsCodec, bytes), /illegal tag/, input.join(" "));
  }
});

test("a known field with another wire type is kept as an unknown field", () => {
  // i32 (field 1, a varint) as length-delimited data, and s (field 9, a string) as a varint:
  // fromBinary() reads each as its declared type anyway, and fails on what follows.
  for (const [input, unknown] of [
    [[0x0a, 0x01, 0x05], { no: 1, wireType: 2, data: [0x01, 0x05] }],
    [[0x48, 0x07], { no: 9, wireType: 0, data: [0x07] }],
  ]) {
    const bytes = Uint8Array.from(input);
    const decoded = gen.ScalarsCodec.decode(bytes);
    assert.equal(decoded.i32, 0);
    assert.equal(decoded.s, "");
    assert.deepEqual(
      decoded.$unknown.map(({ no, wireType, data }) => ({ no, wireType, data: [...data] })),
      [unknown],
    );
    // Written again as it came.
    assert.deepEqual(gen.ScalarsCodec.encode(decoded), bytes);
  }
});

test("a nested message that runs past its length decodes as fromBinary decodes it", () => {
  // inner's name ("ab") runs one byte past inner's length of 3, which fromBinary() accepts.
  const bytes = Uint8Array.of(0x0a, 0x03, 0x0a, 0x02, 0x61, 0x62);
  assert.throws(() => decodeBare(gen.NestedCodec, bytes), RangeError);
  const expected = fromBinary(pb.NestedSchema, bytes);
  assert.equal(expected.inner.name, "ab");
  assertSame(gen.NestedCodec.decode(bytes), expected, "fallback");
});

test("packed fields whose length doesn't hold whole elements are errors", () => {
  for (const input of [
    // Nine bytes of doubles, the last of which would read as the tag of the next field.
    [0x12, 0x09, 0, 0, 0, 0, 0, 0, 0, 0, 0x08, 0x01],
    [0x0a, 0x02, 0x80, 0x80],
  ]) {
    const bytes = Uint8Array.from(input);
    assert.throws(() => decodeBare(gen.PackedOnlyCodec, bytes), Error, input.join(" "));
  }
});

test("unknown groups nest as deeply as protobuf-es lets them", () => {
  const varint = (n) => (n < 0x80 ? [n] : [(n & 0x7f) | 0x80, n >>> 7]);
  for (const levels of [97, 98, 99, 100]) {
    // Groups of field 5 nested `levels` deep, in the message and in a child of it.
    const groups = [
      ...Array.from({ length: levels }, () => 0x2b),
      ...Array.from({ length: levels }, () => 0x2c),
    ];
    const child = [0x12, ...varint(groups.length), ...groups];
    for (const input of [groups, child]) {
      const bytes = Uint8Array.from(input);
      const ours = attempt(() => decodeBare(gen.Nested_InnerCodec, bytes));
      const theirs = attempt(() => fromBinary(pb.Nested_InnerSchema, bytes));
      const what = `${levels} levels${input === child ? " in a child" : ""}`;
      assert.equal(ours.ok, theirs.ok, `${what}: ${String(ours.error ?? theirs.error)}`);
    }
  }
});

test("codecs reject the values toBinary rejects, and accept the ones it accepts", () => {
  const cases = [
    { i32: 1.5 },
    { i32: 2 ** 31 },
    { i32: -(2 ** 31) - 1 },
    { i32: "12" },
    { i32: "x" },
    { i32: NaN },
    { u32: -1 },
    { u32: 2 ** 32 },
    { u32: "4294967295" },
    { s32: 2 ** 40 },
    { fx32: -1 },
    { sfx32: 0.5 },
    { f: 1e39 },
    { f: "1.5" },
    { f: "NaN" },
    { f: "x" },
    { f: Infinity },
    { d: "2.5" },
    { i64: "123" },
    { i64: "x" },
    { i64: 2n ** 64n },
    { u64: -1n },
    { u64: 12 },
    { i64String: "-9223372036854775808" },
    { s: 42 },
    { color: 1.5 },
    { optI32: "7" },
  ];
  for (const init of cases) {
    const message = { ...create(pb.ScalarsSchema), ...init };
    const theirs = attempt(() => toBinary(pb.ScalarsSchema, message));
    const ours = attempt(() => encodeBare(gen.ScalarsCodec, message));
    const what = JSON.stringify(init, (_, v) => (typeof v === "bigint" ? `${v}n` : v));
    assert.equal(ours.ok, theirs.ok, `${what}: ${String(ours.error ?? theirs.error)}`);
    if (theirs.ok) {
      assert.deepEqual(ours.value, theirs.value, what);
    }
  }
  const lists = [{ i32: [1, 2.5] }, { u32: [-1] }, { f: [1e39] }, { sfx32: ["3", 4] }];
  for (const init of lists) {
    const message = { ...create(pb.ListsSchema), ...init };
    const theirs = attempt(() => toBinary(pb.ListsSchema, message));
    const ours = attempt(() => encodeBare(gen.ListsCodec, message));
    assert.equal(ours.ok, theirs.ok, JSON.stringify(init));
    if (theirs.ok) {
      assert.deepEqual(ours.value, theirs.value, JSON.stringify(init));
    }
  }
});

test("codecs fail with protobuf-es's errors", () => {
  const foreign = create(pb.SubsetSchema, { i32: 1 });
  const theirs = attempt(() => toBinary(pb.ScalarsSchema, foreign));
  assert.equal(theirs.ok, false);
  assert.throws(() => gen.ScalarsCodec.encode(foreign), { message: theirs.error.message });

  const invalid = { ...create(pb.ScalarsSchema), i32: 1.5 };
  const message = attempt(() => toBinary(pb.ScalarsSchema, invalid)).error.message;
  assert.match(message, /codec\.v1\.Scalars\.i32/);
  assert.throws(() => gen.ScalarsCodec.encode(invalid), { message });

  const truncated = Uint8Array.of(0x4a, 0x05, 0x61);
  const decodeError = attempt(() => fromBinary(pb.ScalarsSchema, truncated)).error;
  assert.throws(() => gen.ScalarsCodec.decode(truncated), { message: decodeError.message });
});

test("encode and decode run the codec, and protobuf-es only on failure", () => {
  for (let i = 0; i < 50; i++) {
    const message = randomMessage(pb.HolderSchema, 0);
    const bytes = gen.HolderCodec.encode(message);
    assert.deepEqual(bytes, toBinary(pb.HolderSchema, message));
    assertSame(gen.HolderCodec.decode(bytes), fromBinary(pb.HolderSchema, bytes), `#${i}`);
  }
});

test("encode keeps working when a value's String() encodes too", () => {
  const inner = create(pb.ScalarsSchema, { s: "inner" });
  const sneaky = {
    toString() {
      return `x${gen.ScalarsCodec.encode(inner).length}`;
    },
  };
  const message = { ...create(pb.ScalarsSchema), s: sneaky, i32: 3 };
  assert.deepEqual(gen.ScalarsCodec.encode(message), toBinary(pb.ScalarsSchema, message));
});

test("encode handles messages larger than the writer keeps", () => {
  const large = create(pb.ScalarsSchema, { s: "x".repeat(3 * 1024 * 1024) });
  assert.deepEqual(gen.ScalarsCodec.encode(large), toBinary(pb.ScalarsSchema, large));
  const small = create(pb.ScalarsSchema, { s: "small", i32: -1 });
  assert.deepEqual(gen.ScalarsCodec.encode(small), toBinary(pb.ScalarsSchema, small));
  assert.deepEqual(gen.ScalarsCodec.encode(large), toBinary(pb.ScalarsSchema, large));
});

test("decoded messages have the layout of create()", () => {
  const empty = decodeBare(gen.ScalarsCodec, new Uint8Array(0));
  assert.deepEqual(Object.keys(empty), Object.keys(create(pb.ScalarsSchema)));
  assert.equal(Object.getPrototypeOf(empty), Object.prototype);
  assert.equal(protoInt64.zero, empty.i64);
  assert.equal(empty.i64String, "0");
});

test("services have the codecs of their generated request and response types", () => {
  assert.deepEqual(gen.CodecsCodecs, [
    gen.ScalarsCodec,
    gen.WrappedCodec,
    gen.HolderCodec,
    interopGen.EchoResponseCodec,
  ]);
  assert.equal(interopGen.EchoResponseCodec.schema, interopPb.EchoResponseSchema);
  assert.equal(gen.ForeignCodec.schema, pb.ForeignSchema);
  assert.ok(editionsPb.LegacySchema);
});

/** A codec of `schema` that counts its calls and otherwise is `codec`. */
function counting(codec, schema = codec.schema) {
  const counts = { read: 0, write: 0 };
  return {
    counts,
    codec: defineCodec(
      schema,
      (...args) => (counts.read++, codec.read(...args)),
      (...args) => (counts.write++, codec.write(...args)),
    ),
  };
}

test("clients encode and decode with the codecs they are given", async () => {
  const request = counting(gen.ScalarsCodec);
  const response = counting(interopGen.EchoResponseCodec);
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    const scalars = await stream.readMessage(pb.ScalarsSchema);
    await stream.writeMessage(interopPb.EchoResponseSchema, { message: scalars.s, index: 7 });
    await stream.writeStatus();
    await stream.finish();
  });
  const client = createClient(pb.Codecs, transport, { codecs: [request.codec, response.codec] });
  const reply = await client.shared({ s: "héllo", i64: -3n });
  assert.equal(reply.message, "héllo");
  assert.equal(reply.index, 7);
  assert.deepEqual(request.counts, { read: 0, write: 1 });
  assert.deepEqual(response.counts, { read: 1, write: 0 });
});

test("clients use protobuf-es for types without a codec and for codecs of other schemas", async () => {
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    const wrapped = await stream.readMessage(pb.WrappedSchema);
    await stream.writeMessage(pb.HolderSchema, { wrapped, scalars: { i32: -1 } });
    await stream.writeStatus();
    await stream.finish();
  });
  // A codec of a copy of the schema, as a second copy of the generated code would have, is
  // left alone.
  const copy = counting(gen.HolderCodec, { ...pb.HolderSchema });
  const client = createClient(pb.Codecs, transport, { codecs: [copy.codec] });
  const reply = await client.fallback({ name: "n", count: 5n });
  assert.equal(reply.wrapped.name, "n");
  assert.equal(reply.wrapped.count, 5n);
  assert.equal(reply.scalars.i32, -1);
  assert.deepEqual(copy.counts, { read: 0, write: 0 });
});

test("clients fail requests that their codecs cannot encode, and responses they cannot decode", async () => {
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await stream.readMessage(pb.ScalarsSchema);
    // An invalid string, which protobuf-es also rejects.
    await stream.write(encodeMessageFrame(Uint8Array.of(0x4a, 0x01, 0xff)));
    await stream.writeStatus();
    await stream.finish();
  });
  const client = createClient(pb.Codecs, transport, { codecs: gen.CodecsCodecs });
  await assert.rejects(
    client.echo({ i32: 1.5 }),
    (error) =>
      error.code === Code.InvalidArgument && /codec\.v1\.Scalars\.i32/.test(error.statusMessage),
  );
  await assert.rejects(client.echo({ i32: 1 }), (error) => error.code === Code.InvalidArgument);
});

test("createClient rejects codecs that aren't message codecs", () => {
  const transport = fakeTransport(async () => {});
  const cases = {
    "a codec": gen.ScalarsCodec,
    "a string": "codecs",
    "a number": 5,
    "an object": [{}],
    "a schema": [pb.ScalarsSchema],
    "a bare read and write": [{ schema: pb.ScalarsSchema, read() {}, write() {} }],
    null: [null],
  };
  for (const [what, codecs] of Object.entries(cases)) {
    assert.throws(
      () => createClient(pb.Codecs, transport, { codecs }),
      (error) => error.code === Code.InvalidArgument,
      what,
    );
  }
});
