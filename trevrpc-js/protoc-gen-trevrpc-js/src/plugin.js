// protoc-gen-trevrpc-js: generates a codec for each message, `<file>_trevrpc.js`, next to the
// `<file>_pb.js` that protoc-gen-es generates. A codec encodes and decodes the same protobuf-es
// messages as toBinary() and fromBinary(), to the same bytes, with code written for its type; see
// src/codec.js of @trevrpc/trevrpc-js, which runs it.
//
// Each file gets:
//
//   export const GreetRequestCodec = defineCodec(GreetRequestSchema, read, write);  (per message)
//   export const GreeterCodecs = [GreetRequestCodec, GreetResponseCodec];          (per service)
//
// The generated code covers proto3 messages. Messages of other editions, and those with fields
// that protobuf-es represents specially (wrapper types, which it unboxes, and Struct, which is a
// JSON object), get a codec that runs protobuf-es, as do fields of message types that weren't
// generated in the same run, such as the well-known types.
import { ScalarType } from "@bufbuild/protobuf";
import { isWrapperDesc } from "@bufbuild/protobuf/wkt";
import { createEcmaScriptPlugin } from "@bufbuild/protoplugin";

import packageJson from "../package.json" with { type: "json" };

/** Where generated code imports the codec runtime from. */
const Runtime = "@trevrpc/trevrpc-js/codec";
const EditionProto3 = 999;
/** FeatureSet.FieldPresence.IMPLICIT, the presence of proto3 fields without `optional`. */
const Implicit = 2;

const Varint = 0;
const Fixed64 = 1;
const LengthDelimited = 2;
const Fixed32 = 5;

/**
 * How each scalar type is read and written: the Reader and Writer method, the wire type, the
 * packed method (when it has a dedicated one), and the test that a value of an implicit-presence
 * field is set, which matches the one protobuf-es makes.
 */
const Scalars = new Map([
  [ScalarType.DOUBLE, scalar("double", Fixed64, "packedDouble", float)],
  [ScalarType.FLOAT, scalar("float", Fixed32, "packedFloat", float)],
  [ScalarType.INT64, scalar("int64", Varint, undefined, loose)],
  [ScalarType.UINT64, scalar("uint64", Varint, undefined, loose)],
  [ScalarType.INT32, scalar("int32", Varint, "packedInt32", loose)],
  [ScalarType.FIXED64, scalar("fixed64", Fixed64, undefined, loose)],
  [ScalarType.FIXED32, scalar("fixed32", Fixed32, "packedFixed32", loose)],
  [ScalarType.BOOL, scalar("bool", Varint, "packedBool", (v) => `${v} !== false`)],
  [ScalarType.STRING, scalar("string", LengthDelimited, undefined, (v) => `${v} !== ""`)],
  [
    ScalarType.BYTES,
    scalar(
      "bytes",
      LengthDelimited,
      undefined,
      (v) => `(!(${v} instanceof Uint8Array) || ${v}.byteLength > 0)`,
    ),
  ],
  [ScalarType.UINT32, scalar("uint32", Varint, "packedUint32", loose)],
  [ScalarType.SFIXED32, scalar("sfixed32", Fixed32, "packedSfixed32", loose)],
  [ScalarType.SFIXED64, scalar("sfixed64", Fixed64, undefined, loose)],
  [ScalarType.SINT32, scalar("sint32", Varint, "packedSint32", loose)],
  [ScalarType.SINT64, scalar("sint64", Varint, undefined, loose)],
]);

function scalar(method, wire, packed, isSet) {
  const long = method.endsWith("64");
  return { method, wire, packed, isSet, long };
}

/** Numbers, bigints and numeric strings: loose inequality matches 0, 0n and "0", as protobuf-es. */
function loose(v) {
  return `${v} != 0`;
}

/** Floats: Object.is tells -0, which is written, from 0, which is not. */
function float(v) {
  return `!Object.is(${v}, 0)`;
}

export const protocGenTrevrpcJs = createEcmaScriptPlugin({
  name: "protoc-gen-trevrpc-js",
  version: `v${packageJson.version}`,
  generateTs: (schema) => generate(schema, "ts"),
  generateJs: (schema) => generate(schema, "js"),
  generateDts: (schema) => generate(schema, "dts"),
});

function generate(schema, target) {
  const generated = new Set(schema.files);
  for (const file of schema.files) {
    const messages = fileMessages(file);
    if (messages.length === 0 && file.services.length === 0) {
      continue;
    }
    const extension = target === "dts" ? "d.ts" : target;
    const f = schema.generateFile(`${file.name}_trevrpc.${extension}`);
    f.preamble(file);
    new FileGenerator(f, file, target, generated).generate(messages);
  }
}

/** The messages of a file, nested ones included, in declaration order. */
function fileMessages(file) {
  const messages = [];
  const add = (message) => {
    messages.push(message);
    message.nestedMessages.forEach(add);
  };
  file.messages.forEach(add);
  return messages;
}

/**
 * Whether generated code reads and writes `message`. Only proto3 messages qualify: other editions
 * have closed enums, groups, required fields and defaults on the prototype chain. Wrapper fields,
 * which protobuf-es unboxes, and Struct fields, which it represents as JSON objects, are left to
 * protobuf-es too.
 */
function isNative(message) {
  if (message.file.edition !== EditionProto3) {
    return false;
  }
  return message.fields.every((field) => {
    const type = field.message;
    if (type === undefined) {
      return true;
    }
    if (field.fieldKind === "message" && field.oneof === undefined && isWrapperDesc(type)) {
      return false;
    }
    return (
      type.typeName !== "google.protobuf.Struct" || message.typeName === "google.protobuf.Value"
    );
  });
}

function tagValue(fieldNo, wireType) {
  return ((fieldNo << 3) | wireType) >>> 0;
}

/** The statement that writes a tag. */
function writeTag(fieldNo, wireType) {
  const tag = tagValue(fieldNo, wireType);
  return tag < 0x80 ? `w.byte(${tag});` : `w.varint(${tag});`;
}

class FileGenerator {
  constructor(f, file, target, generated) {
    this.f = f;
    this.file = file;
    this.target = target;
    this.generated = generated;
    this.ts = target === "ts";
    // The runtime's exports, each imported on first use: this.rt.Reader and the like.
    this.rt = new Proxy(
      {},
      {
        get: (cache, name) => (cache[name] ??= f.import(name, Runtime)),
      },
    );
  }

  /** A TypeScript annotation, printed only for the ts target. */
  type(...annotation) {
    return this.ts ? annotation : "";
  }

  generate(messages) {
    for (const message of messages) {
      if (this.target === "dts") {
        this.declareCodec(message);
      } else if (isNative(message)) {
        this.generateConstructor(message);
        this.generateReader(message);
        this.generateWriter(message);
        this.generateCodec(message);
      } else {
        this.generateFallback(message);
        this.generateCodec(message);
      }
    }
    for (const service of this.file.services) {
      this.generateServiceCodecs(service);
    }
  }

  /** The base name of a message's generated symbols: protoc-gen-es's name for its type. */
  name(message) {
    return this.f.importShape(message).name;
  }

  codecName(message) {
    return `${this.name(message)}Codec`;
  }

  /**
   * How generated code reads (`read`) or writes (`write`) a message of `type`: its generated
   * function in this file, the codec of another generated file, or protobuf-es. Returns the
   * printables of a call, given the printables of the arguments.
   */
  call(kind, type, args) {
    if (type.file === this.file) {
      return [`$${kind}${this.name(type)}(`, ...args, ")"];
    }
    if (this.generated.has(type.file)) {
      const codec = this.f.import(this.codecName(type), `./${type.file.name}_trevrpc.js`);
      return [codec, `.${kind}(`, ...args, ")"];
    }
    const fallback = kind === "read" ? this.rt.readMessage : this.rt.writeMessage;
    return [fallback, "(", this.f.importSchema(type), ", ", ...args, ")"];
  }

  /** The zero value of a scalar field, as protobuf-es creates it. */
  scalarZero(type, longAsString) {
    switch (type) {
      case ScalarType.STRING:
        return ['""'];
      case ScalarType.BOOL:
        return ["false"];
      case ScalarType.BYTES:
        return [this.rt.EmptyBytes];
      default:
        if (Scalars.get(type).long) {
          return longAsString ? ['"0"'] : [this.rt.Int64Zero];
        }
        return ["0"];
    }
  }

  /** The expression that reads a scalar value. */
  readScalar(type, longAsString) {
    const { method, long } = Scalars.get(type);
    return long && longAsString ? `String(r.${method}())` : `r.${method}()`;
  }

  // ------------------------------------------------------------------------------------- decode

  /**
   * A constructor that lays out properties exactly as create() does: members in declaration
   * order, zero values for implicit scalars, lists and maps, and no property for message fields
   * and scalars with explicit presence until they are set. Every decoded message of a type then
   * shares one object shape.
   *
   * A constructor, not an object literal, and `new Array()`, not []: V8 pretenures literals whose
   * objects tend to survive (decoded messages do), allocating them straight into the old
   * generation while the strings and arrays they point to stay young, and every scavenge then has
   * to process those old-to-young pointers. On a 2.6 MB message that spent ~40% of decode time in
   * GC.
   */
  generateConstructor(message) {
    const { f } = this;
    const assignments = [[`this.$typeName = ${JSON.stringify(message.typeName)};`]];
    for (const member of message.members) {
      const name = member.localName;
      if (member.kind === "oneof") {
        assignments.push([`this.${name} = { case: undefined };`]);
        continue;
      }
      switch (member.fieldKind) {
        case "list":
          assignments.push([`this.${name} = new Array();`]);
          break;
        case "map":
          assignments.push([`this.${name} = {};`]);
          break;
        case "enum":
          if (member.presence === Implicit) {
            assignments.push([`this.${name} = ${member.enum.values[0].number};`]);
          }
          break;
        case "scalar":
          if (member.presence === Implicit) {
            assignments.push([
              `this.${name} = `,
              ...this.scalarZero(member.scalar, member.longAsString),
              ";",
            ]);
          }
          break;
        // Singular message fields are absent from a new message.
      }
    }
    const ctor = `$${this.name(message)}`;
    f.print("function ", ctor, "(", this.type("this: any"), ") {");
    for (const assignment of assignments) {
      f.print("  ", ...assignment);
    }
    f.print("}");
    f.print(ctor, ".prototype = Object.prototype; // plain objects, as create() makes");
    f.print();
  }

  /** Opens the function that reads a message: `$read<name>(r, end, depth, into)`. */
  printReadHead(name) {
    const { rt } = this;
    this.f.print(
      "function $read",
      name,
      "(r",
      this.type(": ", rt.Reader),
      ", end",
      this.type(": number"),
      ", depth",
      this.type(": number"),
      ", into",
      this.type("?: any"),
      ")",
      this.type(": any"),
      " {",
    );
  }

  /** Opens the function that writes a message: `$write<name>(w, m)`. */
  printWriteHead(name) {
    const { rt } = this;
    this.f.print(
      "function $write",
      name,
      "(w",
      this.type(": ", rt.Writer),
      ", m",
      this.type(": any"),
      ")",
      this.type(": void"),
      " {",
    );
  }

  generateReader(message) {
    const { f, rt } = this;
    const name = this.name(message);
    this.printReadHead(name);
    f.print(
      "  if (depth >= ",
      rt.RecursionLimit,
      ") ",
      rt.depthError,
      "(",
      JSON.stringify(message.typeName),
      ");",
    );
    f.print(
      "  const m",
      this.type(": any"),
      " = into ?? new ",
      this.ts ? `($${name} as any)` : `$${name}`,
      "();",
    );
    f.print("  while (r.pos < end) {");
    f.print("    const tag = r.tag();");
    f.print("    switch (tag) {");
    for (const field of message.fields) {
      for (const line of this.decodeCases(field)) {
        f.print("      ", ...line);
      }
    }
    f.print("      default:");
    f.print("        r.unknown(m, tag, depth);");
    f.print("    }");
    f.print("  }");
    f.print("  r.checkEnd(end);");
    f.print("  return m;");
    f.print("}");
    f.print();
  }

  /** The `case` clauses that read one field, as lines of printables. */
  decodeCases(field) {
    const no = field.number;
    const target = `m.${field.localName}`;
    const assign = (value) =>
      field.oneof
        ? [`m.${field.oneof.localName} = { case: "${field.localName}", value: `, ...value, " };"]
        : [`${target} = `, ...value, ";"];

    switch (field.fieldKind) {
      case "enum":
        return [[`case ${tagValue(no, Varint)}: `, ...assign(["r.int32()"]), " break;"]];
      case "scalar": {
        const { wire } = Scalars.get(field.scalar);
        const value = this.readScalar(field.scalar, field.longAsString);
        return [[`case ${tagValue(no, wire)}: `, ...assign([value]), " break;"]];
      }
      case "message": {
        // Another occurrence of a singular message field merges into it.
        const into = field.oneof
          ? `m.${field.oneof.localName}.case === "${field.localName}" ? m.${field.oneof.localName}.value : undefined`
          : target;
        const read = this.call("read", field.message, [`r, r.lenEnd(), depth + 1, ${into}`]);
        return [[`case ${tagValue(no, LengthDelimited)}: `, ...assign(read), " break;"]];
      }
      case "list":
        return this.decodeListCases(field, target);
      case "map":
        return this.decodeMapCase(field, target);
    }
  }

  decodeListCases(field, target) {
    const no = field.number;
    if (field.listKind === "message") {
      const read = this.call("read", field.message, ["r, r.lenEnd(), depth + 1, undefined"]);
      return [[`case ${tagValue(no, LengthDelimited)}: ${target}.push(`, ...read, "); break;"]];
    }
    const type = field.listKind === "enum" ? ScalarType.INT32 : field.scalar;
    const longAsString = field.listKind === "scalar" && field.longAsString;
    const { wire, packed, long } = Scalars.get(type);
    const one = [
      `case ${tagValue(no, wire)}: ${target}.push(${this.readScalar(type, longAsString)}); break;`,
    ];
    if (wire === LengthDelimited) {
      return [one];
    }
    // Parsers accept both encodings of a repeated scalar, packed or not.
    const packedRead = long
      ? `r.packedLong(${target}, r.${Scalars.get(type).method}, ${longAsString});`
      : `r.${packed}(${target});`;
    return [one, [`case ${tagValue(no, LengthDelimited)}: ${packedRead} break;`]];
  }

  decodeMapCase(field, target) {
    const { rt } = this;
    const no = field.number;
    const key = Scalars.get(field.mapKey);
    // Map keys and values are read as protobuf-es reads them: 64-bit integers never as strings.
    let readValue;
    let zeroValue;
    let valueTag;
    switch (field.mapKind) {
      case "message":
        // Another value in one entry merges into the first, as for any message field.
        readValue = this.call("read", field.message, ["r, r.lenEnd(), depth + 1, v"]);
        zeroValue = [rt.emptyMessage, "(", this.f.importSchema(field.message), ")"];
        valueTag = tagValue(2, LengthDelimited);
        break;
      case "enum":
        readValue = ["r.int32()"];
        zeroValue = [`${field.enum.values[0].number}`];
        valueTag = tagValue(2, Varint);
        break;
      case "scalar":
        readValue = [this.readScalar(field.scalar, false)];
        // Bytes values get an instance each, as in protobuf-es.
        zeroValue =
          field.scalar === ScalarType.BYTES
            ? ["new Uint8Array(0)"]
            : this.scalarZero(field.scalar, false);
        valueTag = tagValue(2, Scalars.get(field.scalar).wire);
        break;
    }
    const set =
      field.mapKey === ScalarType.STRING
        ? [rt.setMapEntry, `(${target}, k, v);`]
        : [`${target}[k`, this.type(" as any"), "] = v;"];
    return [
      [`case ${tagValue(no, LengthDelimited)}: {`],
      ["  const e = r.lenEnd();"],
      ["  let k", this.type(": any"), " = ", ...this.scalarZero(field.mapKey, false), ";"],
      ["  let v", this.type(": any"), " = undefined;"],
      ["  while (r.pos < e) {"],
      ["    const t = r.tag();"],
      [`    if (t === ${tagValue(1, key.wire)}) k = ${this.readScalar(field.mapKey, false)};`],
      [`    else if (t === ${valueTag}) v = `, ...readValue, ";"],
      ["    else r.skipTag(t, depth);"],
      ["  }"],
      ["  r.checkEnd(e);"],
      ["  if (v === undefined) v = ", ...zeroValue, ";"],
      ["  ", ...set],
      ["  break;"],
      ["}"],
    ];
  }

  // ------------------------------------------------------------------------------------- encode

  generateWriter(message) {
    const { f, rt } = this;
    const name = this.name(message);
    this.printWriteHead(name);
    // As protobuf-es, which rejects a message of another type unless the type has no fields.
    if (message.fields.length > 0) {
      const typeName = JSON.stringify(message.typeName);
      f.print(`  if (m.$typeName !== ${typeName}) `, rt.foreignError, `(${typeName}, m);`);
    }
    // In field number order, as protobuf-es writes them.
    const fields = message.fields.toSorted((a, b) => a.number - b.number);
    for (const field of fields) {
      for (const line of this.encodeField(field)) {
        f.print("  ", ...line);
      }
    }
    f.print("  if (m.$unknown !== undefined) w.unknown(m.$unknown);");
    f.print("}");
    f.print();
  }

  /** The statements that write one field, as lines of printables. */
  encodeField(field) {
    const no = field.number;
    // A oneof member is written when its case is selected, and its value is `.value`.
    const value = field.oneof ? `m.${field.oneof.localName}.value` : `m.${field.localName}`;
    const guard = (condition) =>
      field.oneof
        ? `if (m.${field.oneof.localName}.case === "${field.localName}")`
        : `if (${condition})`;

    switch (field.fieldKind) {
      case "enum": {
        const condition =
          field.presence === Implicit
            ? `${value} !== ${field.enum.values[0].number}`
            : `${value} !== undefined`;
        return [[`${guard(condition)} { ${writeTag(no, Varint)} w.int32(${value}); }`]];
      }
      case "scalar": {
        const { method, wire, isSet } = Scalars.get(field.scalar);
        const condition = field.presence === Implicit ? isSet(value) : `${value} !== undefined`;
        return [[`${guard(condition)} { ${writeTag(no, wire)} w.${method}(${value}); }`]];
      }
      case "message": {
        const write = this.call("write", field.message, [`w, ${value}`]);
        return [
          [
            `${guard(`${value} !== undefined`)} { ${writeTag(no, LengthDelimited)} const s = w.fork(); `,
            ...write,
            "; w.join(s); }",
          ],
        ];
      }
      case "list":
        return this.encodeList(field, `m.${field.localName}`);
      case "map":
        return this.encodeMap(field, `m.${field.localName}`);
    }
  }

  encodeList(field, list) {
    const no = field.number;
    if (field.listKind === "message") {
      const write = this.call("write", field.message, ["w, a[i]"]);
      return [
        [
          `for (let i = 0, a = ${list}; i < a.length; i++) { ${writeTag(no, LengthDelimited)} const s = w.fork(); `,
          ...write,
          "; w.join(s); }",
        ],
      ];
    }
    const type = field.listKind === "enum" ? ScalarType.INT32 : field.scalar;
    const { method, wire, packed, long } = Scalars.get(type);
    if (!field.packed) {
      return [
        [
          `for (let i = 0, a = ${list}; i < a.length; i++) { ${writeTag(no, wire)} w.${method}(a[i]); }`,
        ],
      ];
    }
    const write = long ? `w.packedLong(${list}, w.${method});` : `w.${packed}(${list});`;
    return [[`if (${list}.length > 0) { ${writeTag(no, LengthDelimited)} ${write} }`]];
  }

  encodeMap(field, record) {
    const { rt } = this;
    const no = field.number;
    const key = Scalars.get(field.mapKey);
    // Keys are strings in the record; converted back as protobuf-es converts them.
    let keyValue;
    switch (field.mapKey) {
      case ScalarType.STRING:
        keyValue = ["k"];
        break;
      case ScalarType.BOOL:
        keyValue = [rt.boolKey, "(k)"];
        break;
      case ScalarType.UINT64:
      case ScalarType.FIXED64:
        keyValue = [rt.uint64Key, "(k)"];
        break;
      case ScalarType.INT64:
      case ScalarType.SFIXED64:
      case ScalarType.SINT64:
        keyValue = [rt.int64Key, "(k)"];
        break;
      default:
        keyValue = [rt.intKey, "(k)"];
    }
    let writeValue;
    if (field.mapKind === "message") {
      writeValue = [
        `${writeTag(2, LengthDelimited)} const s2 = w.fork(); `,
        ...this.call("write", field.message, ["w, o[k]"]),
        "; w.join(s2);",
      ];
    } else {
      const type = field.mapKind === "enum" ? ScalarType.INT32 : field.scalar;
      const { method, wire } = Scalars.get(type);
      writeValue = [`${writeTag(2, wire)} w.${method}(o[k]);`];
    }
    return [
      ["{"],
      [`  const o`, this.type(": any"), ` = ${record}, ks = Object.keys(o);`],
      ["  for (let i = 0; i < ks.length; i++) {"],
      ["    const k = ks[i];"],
      [
        `    ${writeTag(no, LengthDelimited)} const s = w.fork(); ${writeTag(1, key.wire)} w.${key.method}(`,
        ...keyValue,
        "); ",
        ...writeValue,
        " w.join(s);",
      ],
      ["  }"],
      ["}"],
    ];
  }

  // -------------------------------------------------------------------------------------- codecs

  /** Reads and writes a message the generator leaves to protobuf-es. */
  generateFallback(message) {
    const { f, rt } = this;
    const name = this.name(message);
    const schema = f.importSchema(message);
    this.printReadHead(name);
    f.print("  return ", rt.readMessage, "(", schema, ", r, end, depth, into);");
    f.print("}");
    f.print();
    this.printWriteHead(name);
    f.print("  ", rt.writeMessage, "(", schema, ", w, m);");
    f.print("}");
    f.print();
  }

  generateCodec(message) {
    const { f } = this;
    const name = this.name(message);
    f.print(f.jsDoc(`The codec of ${message.typeName}.`));
    f.print(
      f.export("const", this.codecName(message)),
      " = /* @__PURE__ */ ",
      this.rt.defineCodec,
      "(",
      f.importSchema(message),
      `, $read${name}, $write${name});`,
    );
    f.print();
  }

  declareCodec(message) {
    const { f } = this;
    f.print(f.jsDoc(`The codec of ${message.typeName}.`));
    f.print(
      f.export("declare const", this.codecName(message)),
      ": ",
      f.import("MessageCodec", Runtime, true),
      "<typeof ",
      f.importSchema(message, true),
      ">;",
    );
    f.print();
  }

  /**
   * The codecs of the request and response types of a service, which createClient() takes as its
   * `codecs` option. Types without a generated codec, such as the well-known types, are left out,
   * and protobuf-es encodes and decodes them.
   */
  generateServiceCodecs(service) {
    const { f } = this;
    const types = [];
    for (const method of service.methods) {
      for (const type of [method.input, method.output]) {
        if (this.generated.has(type.file) && !types.includes(type)) {
          types.push(type);
        }
      }
    }
    const name = `${f.importSchema(service).name}Codecs`;
    f.print(f.jsDoc(`The codecs of the requests and responses of ${service.typeName}.`));
    const codecType = f.import("MessageCodec", Runtime, true);
    if (this.target === "dts") {
      f.print(f.export("declare const", name), ": readonly ", codecType, "[];");
      f.print();
      return;
    }
    const codecs = [];
    for (const type of types) {
      if (codecs.length > 0) {
        codecs.push(", ");
      }
      codecs.push(
        type.file === this.file
          ? this.codecName(type)
          : f.import(this.codecName(type), `./${type.file.name}_trevrpc.js`),
      );
    }
    f.print(
      f.export("const", name),
      this.type(": readonly ", codecType, "[]"),
      " = [",
      ...codecs,
      "];",
    );
    f.print();
  }
}
