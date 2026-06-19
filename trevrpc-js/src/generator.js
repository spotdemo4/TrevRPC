import protobuf from "protobufjs";

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
    const typeNames = declarationTypeNames(protoFiles);
    const files = [];

    for (const file of protoFiles) {
      if (!filesToGenerate.has(file.name) || (file.service ?? []).length === 0) {
        continue;
      }

      const fileName = outputFileName(file.name, options.fileSuffix);
      files.push({
        name: fileName,
        content: generateFile(file, rootJson, options),
      });
      files.push({
        name: declarationFileName(fileName),
        content: generateDeclarationFile(protoFiles, file, typeNames, options),
      });
    }

    return { file: files };
  } catch (error) {
    return { error: error.message };
  }
}

/** Parses protoc plugin options for the JavaScript generator. */
export function parseOptions(parameter) {
  const options = {
    runtimeImport: "trevrpc-js",
    fileSuffix: ".trevrpc.js",
  };

  if (parameter === "") {
    return options;
  }

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
      case "file_suffix":
        options.fileSuffix = value;
        break;
      default:
        throw new Error(`unknown trevrpc-js option ${JSON.stringify(key)}`);
    }
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

function generateFile(file, rootJson, options) {
  const services = (file.service ?? []).map((service) => serviceDescriptor(file, service));
  const serviceExports = services.map((service) => generateService(file, service)).join("\n");

  return `// Code generated by protoc-gen-trevrpc-js. DO NOT EDIT.\n\nimport { createRoot, createServiceClient } from ${JSON.stringify(
    options.runtimeImport,
  )};\n\nexport const root = createRoot(${JSON.stringify(rootJson, null, 2)});\n\n${serviceExports}`;
}

function generateDeclarationFile(protoFiles, file, typeNames, options) {
  const messages = collectMessages(protoFiles)
    .filter(({ message }) => !message.options?.mapEntry)
    .map((message) => generateMessageDeclaration(message, typeNames))
    .join("\n\n");
  const services = (file.service ?? [])
    .map((service) => serviceDescriptor(file, service))
    .map((service) => generateServiceDeclaration(service, typeNames))
    .join("\n\n");
  const sections = [messages, services].filter(Boolean).join("\n\n");

  return `// Code generated by protoc-gen-trevrpc-js. DO NOT EDIT.\n\nimport type { Root } from "protobufjs";\n\nimport type { BidirectionalStreamingCall, CallOptions, ClientStreamingCall, RpcServiceDescriptor, Transport } from ${JSON.stringify(
    options.runtimeImport,
  )};\n\nexport declare const root: Root;\n\n${sections}\n`;
}

function generateService(_file, service) {
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

function generateServiceDeclaration(service, typeNames) {
  const className = `${pascalIdentifier(service.name)}Client`;
  const factoryName = `create${className}`;
  const methods = Object.entries(service.methods)
    .map(([jsName, method]) => generateMethodDeclaration(jsName, method, typeNames))
    .join("\n");

  return `/** Service descriptor for the ${service.name} service. */\nexport declare const ${service.exportName}: RpcServiceDescriptor;\n\n/** Client for the ${service.name} service. */\nexport declare class ${className} {\n  /** Creates a client for the ${service.name} service. */\n  constructor(transport: Transport, options?: CallOptions);\n\n${methods}\n}\n\n/** Creates a client for the ${service.name} service. */\nexport declare function ${factoryName}(transport: Transport, options?: CallOptions): ${className};`;
}

function generateMethodDeclaration(jsName, method, typeNames) {
  const inputType = declarationTypeReference(method.inputType, typeNames);
  const outputType = declarationTypeReference(method.outputType, typeNames);

  switch (method.kind) {
    case "unary":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(request: ${inputType}, options?: CallOptions): Promise<${outputType}>;`;
    case "serverStreaming":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(request: ${inputType}, options?: CallOptions): Promise<AsyncIterable<${outputType}>>;`;
    case "clientStreaming":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(options?: CallOptions): Promise<ClientStreamingCall<${inputType}, ${outputType}>>;`;
    case "bidirectionalStreaming":
      return `  /** Calls the ${method.name} RPC. */\n  ${jsName}(options?: CallOptions): Promise<BidirectionalStreamingCall<${inputType}, ${outputType}>>;`;
    default:
      throw new Error(`unsupported generated RPC kind ${JSON.stringify(method.kind)}`);
  }
}

function serviceDescriptor(file, service) {
  const fullName =
    file.package === "" || file.package == null ? service.name : `${file.package}.${service.name}`;
  const methods = {};
  const usedNames = new Set();

  for (const method of service.method ?? []) {
    const jsName = uniqueName(lowerCamelIdentifier(method.name), usedNames);
    methods[jsName] = {
      name: method.name,
      kind: methodKind(method),
      inputType: stripLeadingDot(method.inputType),
      outputType: stripLeadingDot(method.outputType),
    };
  }

  return {
    name: service.name,
    fullName,
    exportName: `${pascalIdentifier(service.name)}Service`,
    methods,
  };
}

function declarationTypeNames(files) {
  const names = new Map();
  const usedNames = new Set();

  for (const { fullName, message } of collectMessages(files)) {
    if (message.options?.mapEntry) {
      continue;
    }

    names.set(fullName, uniqueName(pascalIdentifier(lastTypeSegment(fullName)), usedNames));
  }

  return names;
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

function generateMessageDeclaration({ fullName, message }, typeNames) {
  const name = typeNames.get(fullName);
  const mapEntries = new Map(
    (message.nestedType ?? [])
      .filter((nested) => nested.options?.mapEntry)
      .map((nested) => [nested.name, nested]),
  );
  const fields = (message.field ?? [])
    .map((field) => generateFieldDeclaration(field, mapEntries, typeNames, fullName))
    .join("\n");

  return `export interface ${name} {\n${fields}\n}`;
}

function generateFieldDeclaration(field, mapEntries, typeNames, currentFullName) {
  const optional = field.label === FieldLabel.Required ? "" : "?";
  return `  ${typescriptPropertyName(fieldJsonName(field))}${optional}: ${fieldTypeScriptType(
    field,
    mapEntries,
    typeNames,
    currentFullName,
  )};`;
}

function fieldTypeScriptType(field, mapEntries, typeNames, currentFullName) {
  const mapEntry = mapEntryForField(field, mapEntries);
  if (mapEntry != null) {
    const [keyField, valueField] = mapEntry.field ?? [];
    const keyType = mapKeyTypeScriptType(keyField);
    const valueType = scalarTypeScriptType(valueField, typeNames, currentFullName);
    return `Record<${keyType}, ${valueType}>`;
  }

  const scalarType = scalarTypeScriptType(field, typeNames, currentFullName);
  return field.label === FieldLabel.Repeated ? `${scalarType}[]` : scalarType;
}

function scalarTypeScriptType(field, typeNames, currentFullName) {
  switch (field?.type) {
    case FieldType.Double:
    case FieldType.Float:
    case FieldType.Int32:
    case FieldType.Uint32:
    case FieldType.Sint32:
    case FieldType.Fixed32:
    case FieldType.Sfixed32:
    case FieldType.Enum:
      return "number";
    case FieldType.Int64:
    case FieldType.Uint64:
    case FieldType.Sint64:
    case FieldType.Fixed64:
    case FieldType.Sfixed64:
      return "number | string";
    case FieldType.Bool:
      return "boolean";
    case FieldType.String:
      return "string";
    case FieldType.Bytes:
      return "Uint8Array";
    case FieldType.Group:
    case FieldType.Message:
      return declarationTypeReference(
        resolveTypeName(field.typeName, typeNames, currentFullName),
        typeNames,
      );
    default:
      throw new Error(`unsupported protobuf field type ${field?.type}`);
  }
}

function mapKeyTypeScriptType(field) {
  switch (field?.type) {
    case FieldType.Bool:
      return "string";
    case FieldType.Int32:
    case FieldType.Uint32:
    case FieldType.Sint32:
    case FieldType.Fixed32:
    case FieldType.Sfixed32:
    case FieldType.Int64:
    case FieldType.Uint64:
    case FieldType.Sint64:
    case FieldType.Fixed64:
    case FieldType.Sfixed64:
      return "number";
    case FieldType.String:
      return "string";
    default:
      return "string";
  }
}

function declarationTypeReference(typeName, typeNames) {
  return typeNames.get(stripLeadingDot(typeName)) ?? "Record<string, unknown>";
}

function resolveTypeName(typeName, typeNames, currentFullName) {
  const stripped = stripLeadingDot(typeName);
  if (typeNames.has(stripped)) {
    return stripped;
  }

  const scope = currentFullName.split(".");
  scope.pop();
  while (scope.length > 0) {
    const candidate = `${scope.join(".")}.${stripped}`;
    if (typeNames.has(candidate)) {
      return candidate;
    }
    scope.pop();
  }

  return stripped;
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
      .filter((field) => field.oneofIndex === index)
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
