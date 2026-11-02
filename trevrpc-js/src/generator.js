import protobuf from "protobufjs";

import { Int64Representations } from "./int64.js";

const DESCRIPTOR_PROTO = String.raw`
syntax = "proto2";

package google.protobuf;

message FileDescriptorProto {
  optional string name = 1;
  optional string package = 2;
  repeated string dependency = 3;
  repeated DescriptorProto message_type = 4;
  repeated EnumDescriptorProto enum_type = 5;
  repeated ServiceDescriptorProto service = 6;
  optional FileOptions options = 8;
  optional string syntax = 12;
}

message DescriptorProto {
  optional string name = 1;
  repeated FieldDescriptorProto field = 2;
  repeated DescriptorProto nested_type = 3;
  repeated EnumDescriptorProto enum_type = 4;
  optional MessageOptions options = 7;
  repeated OneofDescriptorProto oneof_decl = 8;
}

message FieldDescriptorProto {
  enum Type {
    TYPE_DOUBLE = 1;
    TYPE_FLOAT = 2;
    TYPE_INT64 = 3;
    TYPE_UINT64 = 4;
    TYPE_INT32 = 5;
    TYPE_FIXED64 = 6;
    TYPE_FIXED32 = 7;
    TYPE_BOOL = 8;
    TYPE_STRING = 9;
    TYPE_GROUP = 10;
    TYPE_MESSAGE = 11;
    TYPE_BYTES = 12;
    TYPE_UINT32 = 13;
    TYPE_ENUM = 14;
    TYPE_SFIXED32 = 15;
    TYPE_SFIXED64 = 16;
    TYPE_SINT32 = 17;
    TYPE_SINT64 = 18;
  }

  enum Label {
    LABEL_OPTIONAL = 1;
    LABEL_REQUIRED = 2;
    LABEL_REPEATED = 3;
  }

  optional string name = 1;
  optional int32 number = 3;
  optional Label label = 4;
  optional Type type = 5;
  optional string type_name = 6;
  optional string default_value = 7;
  optional int32 oneof_index = 9;
  optional string json_name = 10;
  optional bool proto3_optional = 17;
  optional FieldOptions options = 8;
}

message OneofDescriptorProto {
  optional string name = 1;
}

message EnumDescriptorProto {
  optional string name = 1;
  repeated EnumValueDescriptorProto value = 2;
}

message EnumValueDescriptorProto {
  optional string name = 1;
  optional int32 number = 2;
}

message ServiceDescriptorProto {
  optional string name = 1;
  repeated MethodDescriptorProto method = 2;
}

message MethodDescriptorProto {
  optional string name = 1;
  optional string input_type = 2;
  optional string output_type = 3;
  optional bool client_streaming = 5 [default = false];
  optional bool server_streaming = 6 [default = false];
  optional MethodOptions options = 4;
}

// Only the trevrpc.method extension is modelled; protobuf.js keeps every other extension out of
// the decoded message. The numbers are those of proto/trevrpc/options.proto.
message MethodOptions {
  optional TrevrpcMethodOptions trevrpc_method = 58247;
}

message TrevrpcMethodOptions {
  optional bool unreliable_requests = 1;
  optional bool unreliable_responses = 2;
  // trevrpc.Compression, read as its number so that unknown values reach the check.
  optional int32 compression = 5;
}

message FieldOptions {
  enum JSType {
    JS_NORMAL = 0;
    JS_STRING = 1;
    JS_NUMBER = 2;
  }
  optional JSType jstype = 6;
  optional bool debug_redact = 16;
}

message FileOptions {
  optional string java_package = 1;
  optional string java_outer_classname = 8;
  optional bool java_multiple_files = 10;
  optional string go_package = 11;
  optional string objc_class_prefix = 36;
  optional string csharp_namespace = 37;
  optional string php_namespace = 41;
  optional string ruby_package = 45;
}

message MessageOptions {
  optional bool map_entry = 7;
}
`;

const PLUGIN_PROTO = String.raw`
syntax = "proto2";

package google.protobuf.compiler;

message CodeGeneratorRequest {
  repeated string file_to_generate = 1;
  optional string parameter = 2;
  repeated .google.protobuf.FileDescriptorProto proto_file = 15;
}

message CodeGeneratorResponse {
  optional string error = 1;
  optional uint64 supported_features = 2;
  repeated File file = 15;

  message File {
    optional string name = 1;
    optional string insertion_point = 2;
    optional string content = 15;
  }
}
`;

const pluginRoot = new protobuf.Root();
protobuf.parse(DESCRIPTOR_PROTO, pluginRoot);
protobuf.parse(PLUGIN_PROTO, pluginRoot);
const CodeGeneratorRequest = pluginRoot.lookupType("google.protobuf.compiler.CodeGeneratorRequest");
const CodeGeneratorResponse = pluginRoot.lookupType(
  "google.protobuf.compiler.CodeGeneratorResponse",
);

const FieldType = Object.freeze({
  Double: 1,
  Float: 2,
  Int64: 3,
  Uint64: 4,
  Int32: 5,
  Fixed64: 6,
  Fixed32: 7,
  Bool: 8,
  String: 9,
  Group: 10,
  Message: 11,
  Bytes: 12,
  Uint32: 13,
  Enum: 14,
  Sfixed32: 15,
  Sfixed64: 16,
  Sint32: 17,
  Sint64: 18,
});

const FieldLabel = Object.freeze({
  Optional: 1,
  Required: 2,
  Repeated: 3,
});

/** The CodeGeneratorResponse features this generator supports. */
const FEATURE_PROTO3_OPTIONAL = 1;

/** Generic runtime types that generated declarations import only when they use them. */
const GeneratedTypeImports = [
  "BidirectionalStreamingCall",
  "ClientStreamingCall",
  "ResponseStream",
];

/**
 * Well-known types whose declarations the runtime exports, so that generated files share them
 * instead of declaring their own: the full name and whether it holds 64-bit integers, which the
 * runtime declares as bigint, so it is shared only when that is the representation.
 */
const SharedWellKnownTypes = new Map([
  ["google.protobuf.Timestamp", { int64: true }],
  ["google.protobuf.Duration", { int64: true }],
  ["google.protobuf.Empty", { int64: false }],
  ["google.protobuf.Struct", { int64: false }],
  ["google.protobuf.Value", { int64: false }],
  ["google.protobuf.ListValue", { int64: false }],
  ["google.protobuf.NullValue", { int64: false }],
]);

/** FieldOptions.JSType values. */
const JSType = Object.freeze({ normal: 0, string: 1, number: 2 });

const RESERVED_WORDS = new Set([
  "break",
  "case",
  "catch",
  "class",
  "const",
  "continue",
  "debugger",
  "default",
  "delete",
  "do",
  "else",
  "export",
  "extends",
  "finally",
  "for",
  "function",
  "if",
  "import",
  "in",
  "instanceof",
  "new",
  "return",
  "super",
  "switch",
  "this",
  "throw",
  "try",
  "typeof",
  "var",
  "void",
  "while",
  "with",
  "yield",
]);

/** Runs the TrevRPC JavaScript code generator for protoc plugin input. */
export function runGenerator(input) {
  const request = CodeGeneratorRequest.decode(input);
  const response = generate(request);
  return CodeGeneratorResponse.encode(response).finish();
}

/** Generates a protoc code generator response. */
export function generate(request) {
  try {
    const options = parseOptions(request.parameter ?? "");
    const filesToGenerate = new Set(request.fileToGenerate ?? []);
    const protoFiles = request.protoFile ?? [];
    const rootJson = buildProtobufJson(protoFiles);
    const redacted = redactedTypes(protoFiles);
    const files = [];

    for (const file of protoFiles) {
      if (!filesToGenerate.has(file.name) || (file.service ?? []).length === 0) {
        continue;
      }

      const fileName = outputFileName(file.name, options.fileSuffix);
      const model = declarationModel(protoFiles, file, options);
      files.push({
        name: fileName,
        content: generateFile(file, rootJson, model, redacted, options),
      });
      files.push({
        name: declarationFileName(fileName),
        content: generateDeclarationFile(file, model, redacted, options),
      });
    }

    return { file: files, supportedFeatures: FEATURE_PROTO3_OPTIONAL };
  } catch (error) {
    return { error: error.message, supportedFeatures: FEATURE_PROTO3_OPTIONAL };
  }
}

/** Parses protoc plugin options for the JavaScript generator. */
export function parseOptions(parameter) {
  const options = {
    runtimeImport: "@trevrpc/trevrpc-js",
    runtimeTypeImport: null,
    fileSuffix: ".trevrpc.js",
    int64: "bigint",
  };

  for (const option of parameter.split(",")) {
    if (option === "") {
      continue;
    }

    const separator = option.indexOf("=");
    if (separator === -1) {
      throw new Error(`invalid trevrpc-js option ${JSON.stringify(option)}; expected key=value`);
    }

    const key = option.slice(0, separator);
    const value = option.slice(separator + 1);
    switch (key) {
      case "runtime_import":
        options.runtimeImport = value;
        break;
      case "runtime_type_import":
        options.runtimeTypeImport = value;
        break;
      case "file_suffix":
        options.fileSuffix = value;
        break;
      case "int64":
        if (!Int64Representations.includes(value)) {
          throw new Error(
            `invalid trevrpc-js int64 ${JSON.stringify(value)}; expected one of ${Int64Representations.join(", ")}`,
          );
        }
        options.int64 = value;
        break;
      default:
        throw new Error(`unknown trevrpc-js option ${JSON.stringify(key)}`);
    }
  }

  if (options.runtimeTypeImport == null) {
    if (/^[A-Za-z][A-Za-z0-9+.-]*:/u.test(options.runtimeImport)) {
      throw new Error(
        `runtime_import ${JSON.stringify(options.runtimeImport)} requires an explicit runtime_type_import`,
      );
    }
    options.runtimeTypeImport = options.runtimeImport;
  }
  return options;
}

/** Builds a protobuf.js JSON root from protobuf file descriptors. */
export function buildProtobufJson(files) {
  const root = { nested: {} };

  for (const file of files) {
    const scope = ensurePackage(root, file.package ?? "");
    for (const enumType of file.enumType ?? []) {
      scope.nested[enumType.name] = enumToJson(enumType);
    }
    for (const messageType of file.messageType ?? []) {
      scope.nested[messageType.name] = messageToJson(messageType);
    }
  }

  return root;
}

function generateFile(file, rootJson, model, redacted, options) {
  const services = (file.service ?? []).map((service) =>
    serviceDescriptor(file, service, redacted),
  );
  const serviceExports = services.map((service) => generateService(service)).join("\n");
  const enumExports = model.enums
    .filter(({ shared }) => !shared)
    .map(generateEnum)
    .join("\n\n");

  return `// Code generated by protoc-gen-trevrpc-js. DO NOT EDIT.\n\nimport { createRoot, createServiceClient } from ${JSON.stringify(
    options.runtimeImport,
  )};\n\nexport const root = createRoot(${JSON.stringify(rootJson, null, 2)}, { int64: ${JSON.stringify(options.int64)} });\n\n${enumExports === "" ? "" : `${enumExports}\n\n`}${serviceExports}`;
}

/** The runtime value of an enum: a frozen object of its values by name. */
function generateEnum({ fullName, name, values }) {
  const entries = values.map(({ key, number }) => `  ${typescriptPropertyName(key)}: ${number},`);
  return `/** Values of the ${fullName} enum. */\nexport const ${name} = Object.freeze({\n${entries.join("\n")}\n});`;
}

function generateDeclarationFile(file, model, redacted, options) {
  const context = { model, runtimeTypes: new Set(), usesLong: false };
  const enums = model.enums
    .filter(({ shared }) => !shared)
    .map(generateEnumDeclaration)
    .join("\n\n");
  const messages = model.messages
    .filter(({ shared }) => !shared)
    .map((message) => generateMessageDeclarations(message, context))
    .join("\n\n");
  const services = (file.service ?? [])
    .map((service) => serviceDescriptor(file, service, redacted))
    .map((service) => generateServiceDeclaration(service, context))
    .join("\n\n");
  const sections = [enums, messages, services].filter(Boolean).join("\n\n");
  const protobufImports = context.usesLong ? "Long, Root" : "Root";
  const runtimeImports = [
    "CallOptions",
    "ClientOptions",
    "RpcServiceDescriptor",
    "Transport",
    ...context.runtimeTypes,
    ...GeneratedTypeImports.filter((name) => sections.includes(`${name}<`)),
  ].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));

  return `// Code generated by protoc-gen-trevrpc-js. DO NOT EDIT.\n\nimport type { ${protobufImports} } from "protobufjs";\n\nimport type { ${runtimeImports.join(", ")} } from ${JSON.stringify(
    options.runtimeTypeImport,
  )};\n\nexport declare const root: Root;\n\n${sections}\n`;
}

/** The declaration of an enum's runtime value and of the type of its values. */
function generateEnumDeclaration({ fullName, name, values }) {
  const entries = values.map(({ key, number }) => `${typescriptPropertyName(key)}: ${number}`);
  return `/** Values of the ${fullName} enum. */\nexport declare const ${name}: Readonly<{ ${entries.join("; ")} }>;\n/** A value of the ${fullName} enum. */\nexport type ${name} = (typeof ${name})[keyof typeof ${name}];`;
}

function generateService(service) {
  const className = `${pascalIdentifier(service.name)}Client`;
  const factoryName = `create${className}`;
  const methods = Object.entries(service.methods)
    .map(([jsName, method]) => generateServiceMethod(jsName, method))
    .join("\n\n");
  return `/** Service descriptor for the ${service.name} service. */\nexport const ${service.exportName} = Object.freeze(${JSON.stringify(service, null, 2)});\n\n/** Client for the ${service.name} service. */\nexport class ${className} {\n  /** Creates a client for the ${service.name} service. */\n  constructor(transport, options = {}) {\n    this._client = createServiceClient(transport, ${service.exportName}, root, options);\n  }\n\n${methods}\n}\n\n/** Creates a client for the ${service.name} service. */\nexport function ${factoryName}(transport, options = {}) {\n  return new ${className}(transport, options);\n}\n`;
}

function generateServiceMethod(jsName, method) {
  if (method.kind === "clientStreaming" || method.kind === "bidirectionalStreaming") {
    return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(options = {}) {\n    return this._client.${jsName}(options);\n  }`;
  }

  return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(request, options = {}) {\n    return this._client.${jsName}(request, options);\n  }`;
}

function generateServiceDeclaration(service, context) {
  const className = `${pascalIdentifier(service.name)}Client`;
  const factoryName = `create${className}`;
  const methods = Object.entries(service.methods)
    .map(([jsName, method]) => generateMethodDeclaration(jsName, method, context))
    .join("\n");

  return `/** Service descriptor for the ${service.name} service. */\nexport declare const ${service.exportName}: RpcServiceDescriptor;\n\n/** Client for the ${service.name} service. */\nexport declare class ${className} {\n  /** Creates a client for the ${service.name} service. */\n  constructor(transport: Transport, options?: ClientOptions);\n\n${methods}\n}\n\n/** Creates a client for the ${service.name} service. */\nexport declare function ${factoryName}(transport: Transport, options?: ClientOptions): ${className};`;
}

function generateMethodDeclaration(jsName, method, context) {
  // Requests take the input shape; responses are decoded messages.
  const inputType = messageTypeReference(method.inputType, "input", context);
  const outputType = messageTypeReference(method.outputType, "output", context);

  switch (method.kind) {
    case "unary":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(request: ${inputType}, options?: CallOptions): Promise<${outputType}>;`;
    case "serverStreaming":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(request: ${inputType}, options?: CallOptions): Promise<ResponseStream<${outputType}>>;`;
    case "clientStreaming":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(options?: CallOptions): Promise<ClientStreamingCall<${inputType}, ${outputType}>>;`;
    case "bidirectionalStreaming":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(options?: CallOptions): Promise<BidirectionalStreamingCall<${inputType}, ${outputType}>>;`;
    default:
      throw new Error(`unsupported generated RPC kind ${JSON.stringify(method.kind)}`);
  }
}

function serviceDescriptor(file, service, redacted) {
  const fullName =
    file.package === "" || file.package == null ? service.name : `${file.package}.${service.name}`;
  const methods = {};
  const usedNames = new Set();

  for (const method of service.method ?? []) {
    const jsName = uniqueName(lowerCamelIdentifier(method.name), usedNames);
    const kind = methodKind(method);
    const inputType = stripLeadingDot(method.inputType);
    const outputType = stripLeadingDot(method.outputType);
    const methodName = `${fullName}.${method.name}`;
    const options = method.options?.trevrpcMethod;
    methods[jsName] = {
      name: method.name,
      kind,
      inputType,
      outputType,
      ...unreliableDirections(methodName, kind, options),
      ...compressionDirections(methodName, kind, options, { inputType, outputType }, redacted),
    };
  }

  return {
    name: service.name,
    fullName,
    exportName: `${pascalIdentifier(service.name)}Service`,
    methods,
  };
}

/**
 * The `(trevrpc.method)` flags of a method as descriptor fields, only for the ones that are set.
 * A flag on a direction that does not stream is an error.
 */
function unreliableDirections(methodName, kind, options) {
  const directions = {};
  if (options?.unreliableRequests) {
    if (kind !== "clientStreaming" && kind !== "bidirectionalStreaming") {
      throw new Error(
        `${methodName}: unreliable_requests requires a client-streaming or bidirectional method`,
      );
    }
    directions.unreliableRequests = true;
  }
  if (options?.unreliableResponses) {
    if (kind !== "serverStreaming" && kind !== "bidirectionalStreaming") {
      throw new Error(
        `${methodName}: unreliable_responses requires a server-streaming or bidirectional method`,
      );
    }
    directions.unreliableResponses = true;
  }
  return directions;
}

/** `trevrpc.Compression` values, from proto/trevrpc/options.proto. */
const Compression = { unspecified: 0, message: 1, stream: 2 };

/**
 * The directions a method's `compression` lets compress, as descriptor fields, leaving out message
 * types with a field marked `debug_redact`. `COMPRESSION_MESSAGE` compresses messages without a
 * dictionary in both directions. `COMPRESSION_STREAM` does too, for the messages outside the
 * stream, and stream-compresses the directions that stream; on a unary method it is an error, and
 * so is a value this generator does not know.
 */
function compressionDirections(methodName, kind, options, { inputType, outputType }, redacted) {
  const compression = options?.compression ?? Compression.unspecified;
  if (compression === Compression.unspecified) {
    return {};
  }
  if (compression !== Compression.message && compression !== Compression.stream) {
    throw new Error(`${methodName}: unknown compression ${compression}`);
  }
  const directions = {};
  if (!redacted.has(inputType)) {
    directions.compressRequests = true;
  }
  if (!redacted.has(outputType)) {
    directions.compressResponses = true;
  }
  if (compression === Compression.message) {
    return directions;
  }
  if (kind === "unary") {
    throw new Error(`${methodName}: COMPRESSION_STREAM requires a streaming method`);
  }
  if (
    (kind === "clientStreaming" || kind === "bidirectionalStreaming") &&
    !redacted.has(inputType)
  ) {
    directions.compressStreamRequests = true;
  }
  if (
    (kind === "serverStreaming" || kind === "bidirectionalStreaming") &&
    !redacted.has(outputType)
  ) {
    directions.compressStreamResponses = true;
  }
  return directions;
}

/**
 * The full names of the message types that contain a field marked `debug_redact`, directly or in
 * a message-typed field, however deeply nested.
 */
function redactedTypes(files) {
  const messages = new Map(
    collectMessages(files).map(({ fullName, message }) => [fullName, message]),
  );
  const redacted = new Set();
  const visiting = new Set();
  const visit = (name) => {
    if (redacted.has(name)) {
      return true;
    }
    const message = messages.get(name);
    if (message === undefined || visiting.has(name)) {
      return false;
    }
    visiting.add(name);
    const found = (message.field ?? []).some(
      (field) =>
        field.options?.debugRedact === true ||
        (field.typeName != null && visit(stripLeadingDot(field.typeName))),
    );
    visiting.delete(name);
    if (found) {
      redacted.add(name);
    }
    return found;
  };
  for (const name of messages.keys()) {
    visit(name);
  }
  return redacted;
}

/**
 * The declarations of a generated file: its own messages and enums and every type they and its
 * services use, directly or through other messages. `messages` holds each message's `name` (the
 * decoded shape) and `initName` (the input shape), `enums` each enum's `name` and `values`, and
 * `names` maps full names to them. Well-known types the runtime declares are `shared`: their names
 * are reserved first and refer to the runtime's declarations.
 */
function declarationModel(files, file, options) {
  const usedNames = new Set();
  const names = new Map();
  const isShared = (fullName) => {
    const shared = SharedWellKnownTypes.get(fullName);
    return shared !== undefined && (!shared.int64 || options.int64 === "bigint");
  };
  const used = usedTypes(files, file);
  const messages = collectMessages(files)
    .filter(({ fullName, message }) => used.has(fullName) && !message.options?.mapEntry)
    .map((entry) => ({ ...entry, shared: isShared(entry.fullName) }));
  const enums = collectEnums(files)
    .filter(({ fullName }) => used.has(fullName))
    .map((entry) => ({ ...entry, shared: isShared(entry.fullName) }));
  const declared = [...messages, ...enums];

  for (const entry of declared.filter(({ shared }) => shared)) {
    entry.name = uniqueName(lastTypeSegment(entry.fullName), usedNames);
  }
  for (const entry of declared.filter(({ shared }) => !shared)) {
    entry.name = uniqueName(pascalIdentifier(lastTypeSegment(entry.fullName)), usedNames);
  }
  for (const message of messages) {
    message.initName = message.shared
      ? `${message.name}Init`
      : uniqueName(`${message.name}Init`, usedNames);
  }
  for (const entry of declared) {
    names.set(entry.fullName, entry);
  }
  for (const enumEntry of enums) {
    enumEntry.values = enumValues(enumEntry);
  }
  return { messages, enums, names, int64: options.int64 };
}

/**
 * The full names of the types a generated file declares: the messages and enums `file` defines,
 * the request and response types of its services, and every type their fields use, transitively.
 */
function usedTypes(files, file) {
  const messages = new Map(
    collectMessages(files).map(({ fullName, message }) => [fullName, message]),
  );
  const used = new Set();
  const pending = [
    ...collectMessages([file]).map(({ fullName }) => fullName),
    ...collectEnums([file]).map(({ fullName }) => fullName),
    ...(file.service ?? []).flatMap((service) =>
      (service.method ?? []).flatMap((method) =>
        [method.inputType, method.outputType].map(stripLeadingDot),
      ),
    ),
  ];
  while (pending.length > 0) {
    const fullName = pending.pop();
    if (used.has(fullName)) {
      continue;
    }
    used.add(fullName);
    for (const field of messages.get(fullName)?.field ?? []) {
      if (field.typeName != null) {
        pending.push(stripLeadingDot(field.typeName));
      }
    }
  }
  return used;
}

/**
 * The values of an enum, each with the `key` it has in the exported object: its name without the
 * enum's prefix (`TIME_INTERVAL_HOUR` of `TimeInterval` is `HOUR`) when every value has that prefix
 * and every key that leaves is a distinct identifier, as protobuf-es does; its full name otherwise.
 */
function enumValues({ fullName, enumType }) {
  const values = (enumType.value ?? []).map((value) => ({
    name: value.name ?? "",
    number: value.number ?? 0,
  }));
  const prefix = `${upperSnakeCase(lastTypeSegment(fullName))}_`;
  const stripped = values.map(({ name }) => name.slice(prefix.length));
  const strip =
    values.length > 0 &&
    values.every(({ name }) => name.startsWith(prefix)) &&
    stripped.every((key) => /^[A-Za-z_][A-Za-z0-9_]*$/u.test(key)) &&
    new Set(stripped).size === stripped.length;
  return values.map((value, index) => ({ ...value, key: strip ? stripped[index] : value.name }));
}

/** Converts a PascalCase or camelCase name to UPPER_SNAKE_CASE. */
function upperSnakeCase(name) {
  return name.replace(/([a-z0-9])([A-Z])/gu, "$1_$2").toUpperCase();
}

/** Every enum of `files`, top-level and nested in messages, with its full name. */
function collectEnums(files) {
  const enums = [];
  for (const file of files) {
    const scope = file.package ?? "";
    for (const enumType of file.enumType ?? []) {
      enums.push({ fullName: joinTypeName(scope, enumType.name), enumType });
    }
  }
  for (const { fullName, message } of collectMessages(files)) {
    for (const enumType of message.enumType ?? []) {
      enums.push({ fullName: joinTypeName(fullName, enumType.name), enumType });
    }
  }
  return enums;
}

function collectMessages(files) {
  const messages = [];
  for (const file of files) {
    messages.push(...collectFileMessages(file));
  }

  return messages;
}

function collectFileMessages(file) {
  const messages = [];
  for (const message of file.messageType ?? []) {
    collectMessage(messages, joinTypeName(file.package ?? "", message.name), message);
  }

  return messages;
}

function collectMessage(messages, fullName, message) {
  messages.push({ fullName, message });

  for (const nested of message.nestedType ?? []) {
    collectMessage(messages, joinTypeName(fullName, nested.name), nested);
  }
}

/**
 * The two declarations of a message: the decoded shape that calls return and streams yield, in
 * which every field reads a value (scalars and enums their default, repeated fields and maps an
 * empty array and object, message and `optional` fields null when unset), and the input shape that
 * requests take, in which every field may be left out.
 */
function generateMessageDeclarations({ fullName, message, name, initName }, context) {
  const mapEntries = new Map(
    (message.nestedType ?? [])
      .filter((nested) => nested.options?.mapEntry)
      .map((nested) => [nested.name, nested]),
  );
  const fields = (shape) =>
    (message.field ?? [])
      .map((field) => generateFieldDeclaration(field, mapEntries, shape, context))
      .join("\n");
  const body = (shape) => {
    const declared = fields(shape);
    return declared === "" ? "{}" : `{\n${declared}\n}`;
  };

  return `/** A decoded ${fullName} message. */\nexport interface ${name} ${body("output")}\n\n/** A ${fullName} message to send; every field may be left out. */\nexport interface ${initName} ${body("input")}`;
}

function generateFieldDeclaration(field, mapEntries, shape, context) {
  const optional = shape === "input" && field.label !== FieldLabel.Required ? "?" : "";
  const nullable = decodesAbsentAsNull(field) ? " | null" : "";
  const type = fieldTypeScriptType(field, mapEntries, shape, context);
  return `  ${typescriptPropertyName(fieldJsonName(field))}${optional}: ${type}${nullable};`;
}

/** Reports whether protobuf.js decodes the field as null when it is absent. */
function decodesAbsentAsNull(field) {
  if (field.label === FieldLabel.Repeated) {
    return false;
  }
  return (
    field.proto3Optional === true ||
    field.type === FieldType.Message ||
    field.type === FieldType.Group
  );
}

function fieldTypeScriptType(field, mapEntries, shape, context) {
  const mapEntry = mapEntryForField(field, mapEntries);
  if (mapEntry != null) {
    const [, valueField] = mapEntry.field ?? [];
    return `Record<string, ${scalarTypeScriptType(valueField, shape, context)}>`;
  }

  const scalarType = scalarTypeScriptType(field, shape, context);
  return field.label === FieldLabel.Repeated ? `${arrayElementType(scalarType)}[]` : scalarType;
}

/** Parenthesizes a union so that it can be an array's element type. */
function arrayElementType(type) {
  return type.includes(" | ") ? `(${type})` : type;
}

function scalarTypeScriptType(field, shape, context) {
  switch (field?.type) {
    case FieldType.Double:
    case FieldType.Float:
    case FieldType.Int32:
    case FieldType.Uint32:
    case FieldType.Sint32:
    case FieldType.Fixed32:
    case FieldType.Sfixed32:
      return "number";
    case FieldType.Enum:
      return declaredName(field.typeName, "output", context) ?? "number";
    case FieldType.Int64:
    case FieldType.Uint64:
    case FieldType.Sint64:
    case FieldType.Fixed64:
    case FieldType.Sfixed64:
      return int64TypeScriptType(field, shape, context);
    case FieldType.Bool:
      return "boolean";
    case FieldType.String:
      return "string";
    case FieldType.Bytes:
      return "Uint8Array";
    case FieldType.Group:
    case FieldType.Message:
      return messageTypeReference(field.typeName, shape, context);
    default:
      throw new Error(`unsupported protobuf field type ${field?.type}`);
  }
}

/**
 * The type of a 64-bit field: the representation the decoded shape has (the field's `jstype`, or
 * the generator's `int64` option), and every representation the encoder accepts in the input.
 */
function int64TypeScriptType(field, shape, context) {
  const representation = int64Representation(field, context.model.int64);
  if (representation === "long") {
    context.usesLong = true;
  }
  if (shape === "input") {
    return representation === "long"
      ? "bigint | number | string | Long"
      : "bigint | number | string";
  }
  return representation === "long" ? "Long" : representation;
}

/** How decoded messages represent a 64-bit field: by its `jstype`, else by the `int64` option. */
function int64Representation(field, representation) {
  switch (field.options?.jstype) {
    case JSType.string:
      return "string";
    case JSType.number:
      return "number";
    default:
      return representation;
  }
}

/**
 * The declared name of a message type in `shape`, `"output"` or `"input"`, noting the runtime
 * declarations it uses.
 */
function messageTypeReference(typeName, shape, context) {
  return declaredName(typeName, shape, context) ?? "Record<string, unknown>";
}

/**
 * The declared name of a message or enum type in `shape` (enums have one for both), or undefined
 * for a type without a declaration. A name the runtime declares is noted for import.
 */
function declaredName(typeName, shape, context) {
  const entry = context.model.names.get(stripLeadingDot(typeName));
  if (entry === undefined) {
    return undefined;
  }
  const name = shape === "input" && entry.initName !== undefined ? entry.initName : entry.name;
  if (entry.shared) {
    context.runtimeTypes.add(name);
  }
  return name;
}

function joinTypeName(prefix, name) {
  const segment = name ?? "Message";
  return prefix === "" || prefix == null ? segment : `${prefix}.${segment}`;
}

function typescriptPropertyName(name) {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name) ? name : JSON.stringify(name);
}

function methodKind(method) {
  if (method.clientStreaming && method.serverStreaming) {
    return "bidirectionalStreaming";
  }
  if (method.clientStreaming) {
    return "clientStreaming";
  }
  if (method.serverStreaming) {
    return "serverStreaming";
  }
  return "unary";
}

function ensurePackage(root, packageName) {
  let scope = root;
  for (const part of packageName.split(".")) {
    if (part === "") {
      continue;
    }

    scope.nested ??= {};
    scope.nested[part] ??= { nested: {} };
    scope = scope.nested[part];
  }

  scope.nested ??= {};
  return scope;
}

function messageToJson(message) {
  const json = { fields: {} };
  const nested = {};
  const mapEntries = new Map();

  for (const nestedMessage of message.nestedType ?? []) {
    if (nestedMessage.options?.mapEntry) {
      mapEntries.set(nestedMessage.name, nestedMessage);
      continue;
    }

    nested[nestedMessage.name] = messageToJson(nestedMessage);
  }

  for (const enumType of message.enumType ?? []) {
    nested[enumType.name] = enumToJson(enumType);
  }

  for (const field of message.field ?? []) {
    json.fields[fieldJsonName(field)] = fieldToJson(field, mapEntries);
  }

  const oneofs = oneofsToJson(message);
  if (Object.keys(oneofs).length > 0) {
    json.oneofs = oneofs;
  }

  if (Object.keys(nested).length > 0) {
    json.nested = nested;
  }

  return json;
}

function enumToJson(enumType) {
  const values = {};
  for (const value of enumType.value ?? []) {
    values[value.name] = value.number ?? 0;
  }

  return { values };
}

function fieldToJson(field, mapEntries) {
  const json = {
    type: fieldTypeName(field),
    id: field.number ?? 0,
  };
  const mapEntry = mapEntryForField(field, mapEntries);

  if (mapEntry != null) {
    const [keyField, valueField] = mapEntry.field ?? [];
    json.keyType = fieldTypeName(keyField);
    json.type = fieldTypeName(valueField);
    return json;
  }

  if (field.label === FieldLabel.Repeated) {
    json.rule = "repeated";
  } else if (field.label === FieldLabel.Required) {
    json.rule = "required";
  }

  if (field.defaultValue != null && field.defaultValue !== "") {
    json.options = { default: field.defaultValue };
  }
  if (field.proto3Optional) {
    // protobuf.js gives the field presence, decoding it as null when absent,
    // only when this option marks its synthetic oneof.
    json.options = { ...json.options, proto3_optional: true };
  }
  // The runtime reads it to decode the field as a string or number instead of the root's int64
  // representation.
  if (field.options?.jstype === JSType.string) {
    json.options = { ...json.options, jstype: "JS_STRING" };
  } else if (field.options?.jstype === JSType.number) {
    json.options = { ...json.options, jstype: "JS_NUMBER" };
  }

  return json;
}

function mapEntryForField(field, mapEntries) {
  if (
    field.label !== FieldLabel.Repeated ||
    field.type !== FieldType.Message ||
    field.typeName == null
  ) {
    return null;
  }

  return mapEntries.get(lastTypeSegment(field.typeName)) ?? null;
}

function oneofsToJson(message) {
  const oneofs = {};
  for (const [index, oneof] of (message.oneofDecl ?? []).entries()) {
    const fields = (message.field ?? [])
      // A decoded descriptor reads an unset oneof_index as 0 from its
      // prototype, so only an own property places the field in a oneof.
      .filter((field) => Object.hasOwn(field, "oneofIndex") && field.oneofIndex === index)
      .map(fieldJsonName);
    if (fields.length > 0) {
      oneofs[oneof.name] = { oneof: fields };
    }
  }

  return oneofs;
}

function fieldJsonName(field) {
  return field.jsonName || lowerCamelIdentifier(field.name ?? "field");
}

function fieldTypeName(field) {
  switch (field?.type) {
    case FieldType.Double:
      return "double";
    case FieldType.Float:
      return "float";
    case FieldType.Int64:
      return "int64";
    case FieldType.Uint64:
      return "uint64";
    case FieldType.Int32:
      return "int32";
    case FieldType.Fixed64:
      return "fixed64";
    case FieldType.Fixed32:
      return "fixed32";
    case FieldType.Bool:
      return "bool";
    case FieldType.String:
      return "string";
    case FieldType.Group:
    case FieldType.Message:
    case FieldType.Enum:
      return stripLeadingDot(field.typeName);
    case FieldType.Bytes:
      return "bytes";
    case FieldType.Uint32:
      return "uint32";
    case FieldType.Sfixed32:
      return "sfixed32";
    case FieldType.Sfixed64:
      return "sfixed64";
    case FieldType.Sint32:
      return "sint32";
    case FieldType.Sint64:
      return "sint64";
    default:
      throw new Error(`unsupported protobuf field type ${field?.type}`);
  }
}

function outputFileName(inputName, suffix) {
  const extensionStart = inputName.lastIndexOf(".");
  return `${extensionStart === -1 ? inputName : inputName.slice(0, extensionStart)}${suffix}`;
}

function declarationFileName(outputName) {
  if (outputName.endsWith(".js")) {
    return `${outputName.slice(0, -3)}.d.ts`;
  }
  if (outputName.endsWith(".mjs")) {
    return `${outputName.slice(0, -4)}.d.mts`;
  }
  if (outputName.endsWith(".cjs")) {
    return `${outputName.slice(0, -4)}.d.cts`;
  }

  return `${outputName}.d.ts`;
}

function stripLeadingDot(name) {
  return name?.startsWith(".") ? name.slice(1) : name;
}

function lastTypeSegment(name) {
  const stripped = stripLeadingDot(name);
  const parts = stripped.split(".");
  return parts[parts.length - 1];
}

function lowerCamelIdentifier(name) {
  const pascal = pascalIdentifier(name);
  const candidate = `${pascal[0].toLowerCase()}${pascal.slice(1)}`;
  return RESERVED_WORDS.has(candidate) ? `${candidate}_` : candidate;
}

function pascalIdentifier(name) {
  const words = String(name)
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean);
  const candidate = words.map((word) => `${word[0].toUpperCase()}${word.slice(1)}`).join("");
  const valid = candidate === "" ? "X" : candidate;
  const prefixed = /^[0-9]/.test(valid) ? `_${valid}` : valid;
  return RESERVED_WORDS.has(prefixed) ? `${prefixed}_` : prefixed;
}

function uniqueName(name, usedNames) {
  if (!usedNames.has(name)) {
    usedNames.add(name);
    return name;
  }

  for (let suffix = 2; ; suffix += 1) {
    const candidate = `${name}${suffix}`;
    if (!usedNames.has(candidate)) {
      usedNames.add(candidate);
      return candidate;
    }
  }
}
