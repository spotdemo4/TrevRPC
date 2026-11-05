// How a client calls each method of a protobuf-es service descriptor: the method's kind, and what
// its `(trevrpc.method)` option (proto/trevrpc/options.proto) lets it send as datagrams and
// compress. protoc-gen-es keeps the option in the descriptors it embeds, where protobuf-es leaves
// it as an unknown field of the method's options, so it is read from there without the generated
// extension.
import { BinaryReader, WireType } from "@bufbuild/protobuf/wire";

import { invalidArgument } from "./status.js";

/** The field number of the `trevrpc.method` extension of google.protobuf.MethodOptions. */
const MethodOptionField = 58247;

/** `trevrpc.Compression` values. */
const Compression = Object.freeze({ Unspecified: 0, Message: 1, Stream: 2 });

/** The kind of each protobuf-es `methodKind`, as calls and interceptors name it. */
const Kinds = Object.freeze({
  __proto__: null,
  unary: "unary",
  server_streaming: "serverStreaming",
  client_streaming: "clientStreaming",
  bidi_streaming: "bidirectionalStreaming",
});

/**
 * The call settings of `method`, a protobuf-es DescMethod: `{ kind, unreliableRequests,
 * unreliableResponses, compressRequests, compressResponses, compressStreamRequests,
 * compressStreamResponses }`, with each flag true only where the option sets it for a direction
 * that allows it. An option the method's kind cannot use throws InvalidArgument.
 */
export function methodSettings(method) {
  const kind = Kinds[method.methodKind];
  const name = method.parent?.typeName ? `${method.parent.typeName}.${method.name}` : method.name;
  if (kind === undefined) {
    throw invalidArgument(`${name}: unsupported RPC kind ${JSON.stringify(method.methodKind)}`);
  }
  const options = trevrpcMethodOptions(method, name);
  return {
    kind,
    ...unreliableDirections(name, kind, options),
    ...compressionDirections(name, kind, options, method),
  };
}

/** The method's `(trevrpc.method)` option, with unset fields false or zero. */
function trevrpcMethodOptions(method, name) {
  const options = { unreliableRequests: false, unreliableResponses: false, compression: 0 };
  for (const field of method.proto?.options?.$unknown ?? []) {
    if (field.no !== MethodOptionField) {
      continue;
    }
    try {
      if (field.wireType !== WireType.LengthDelimited) {
        throw new Error(`wire type ${field.wireType}`);
      }
      // A message-typed option may be split over several fields, which merge in order.
      const reader = new BinaryReader(new BinaryReader(field.data).bytes());
      while (reader.pos < reader.len) {
        const [number, wireType] = reader.tag();
        if (number === 1 || number === 2 || number === 5) {
          if (wireType !== WireType.Varint) {
            throw new Error(`field ${number} has wire type ${wireType}`);
          }
          if (number === 5) {
            options.compression = reader.int32();
          } else {
            options[number === 1 ? "unreliableRequests" : "unreliableResponses"] = reader.bool();
          }
        } else {
          reader.skip(wireType, number);
        }
      }
    } catch (error) {
      throw invalidArgument(`${name}: malformed trevrpc.method option: ${error.message}`, error);
    }
  }
  return options;
}

/** The `unreliable_*` flags that are set; one on a direction that does not stream throws. */
function unreliableDirections(name, kind, options) {
  const directions = {};
  if (options.unreliableRequests) {
    if (kind !== "clientStreaming" && kind !== "bidirectionalStreaming") {
      throw invalidArgument(
        `${name}: unreliable_requests requires a client-streaming or bidirectional method`,
      );
    }
    directions.unreliableRequests = true;
  }
  if (options.unreliableResponses) {
    if (kind !== "serverStreaming" && kind !== "bidirectionalStreaming") {
      throw invalidArgument(
        `${name}: unreliable_responses requires a server-streaming or bidirectional method`,
      );
    }
    directions.unreliableResponses = true;
  }
  return directions;
}

/**
 * The directions `compression` lets compress, leaving out message types with a field marked
 * `debug_redact`. `COMPRESSION_MESSAGE` compresses messages without a dictionary in both
 * directions. `COMPRESSION_STREAM` does too, for the messages outside the stream, and
 * stream-compresses the directions that stream; on a unary method it throws, as does a value this
 * runtime does not know.
 */
function compressionDirections(name, kind, { compression }, method) {
  if (compression === Compression.Unspecified) {
    return {};
  }
  if (compression !== Compression.Message && compression !== Compression.Stream) {
    throw invalidArgument(`${name}: unknown compression ${compression}`);
  }
  if (compression === Compression.Stream && kind === "unary") {
    throw invalidArgument(`${name}: COMPRESSION_STREAM requires a streaming method`);
  }
  const requests = !hasRedactedField(method.input);
  const responses = !hasRedactedField(method.output);
  const directions = {};
  if (requests) {
    directions.compressRequests = true;
  }
  if (responses) {
    directions.compressResponses = true;
  }
  if (compression === Compression.Stream) {
    if (requests && (kind === "clientStreaming" || kind === "bidirectionalStreaming")) {
      directions.compressStreamRequests = true;
    }
    if (responses && (kind === "serverStreaming" || kind === "bidirectionalStreaming")) {
      directions.compressStreamResponses = true;
    }
  }
  return directions;
}

/** Whether each message type checked so far has a redacted field. */
const redacted = new WeakMap();

/**
 * Whether `message`, a DescMessage, has a field marked `debug_redact`, directly or in a
 * message-typed field (singular, repeated or a map value), however deeply nested.
 */
export function hasRedactedField(message) {
  let found = redacted.get(message);
  if (found !== undefined) {
    return found;
  }
  // Every message type reachable from this one; a cycle is visited once.
  const reachable = new Set([message]);
  found = false;
  for (const type of reachable) {
    for (const field of type.fields) {
      if (field.proto?.options?.debugRedact === true) {
        found = true;
        break;
      }
      if (field.message !== undefined) {
        reachable.add(field.message);
      }
    }
    if (found) {
      break;
    }
  }
  redacted.set(message, found);
  return found;
}
