// How a client calls each method of a protobuf-es service descriptor: the method's kind, and what
// its `(trevrpc.method)` option (proto/trevrpc/options.proto) lets it send as datagrams and
// compress. protoc-gen-es keeps the option in the descriptors it embeds, where protobuf-es leaves
// it as an unknown field of the method's options, so it is read from there without the generated
// extension.
import { BinaryReader, WireType } from "@bufbuild/protobuf/wire";

import { invalidArgument } from "./status.js";
import { RpcKind } from "./wire.js";

/** The field number of the `trevrpc.method` extension of google.protobuf.MethodOptions. */
const MethodOptionField = 58247;

/** `trevrpc.Compression` values. */
const Compression = Object.freeze({ Unspecified: 0, Message: 1, Stream: 2 });

/**
 * Each protobuf-es `methodKind`: its kind as calls and interceptors name it, its `RpcKind` on the
 * wire, and which of its directions stream.
 */
const Kinds = Object.freeze({
  __proto__: null,
  unary: kind("unary", RpcKind.Unary, false, false),
  server_streaming: kind("serverStreaming", RpcKind.ServerStreaming, false, true),
  client_streaming: kind("clientStreaming", RpcKind.ClientStreaming, true, false),
  bidi_streaming: kind("bidirectionalStreaming", RpcKind.BidirectionalStreaming, true, true),
});

function kind(name, rpcKind, streamsRequest, streamsResponse) {
  return Object.freeze({ name, rpcKind, streamsRequest, streamsResponse });
}

/**
 * The call settings of `method`, a protobuf-es DescMethod, as calls read them:
 *
 * - `kind`, as calls and interceptors name it, `rpcKind`, its value on the wire, and
 *   `streamsRequest`, whether the request side streams.
 * - `datagrams`, `{ send, receive }` for a method that marks a direction unreliable, else
 *   undefined: `send` for request messages, which needs `unreliable_requests`, and `receive` for
 *   response messages, which every such method with a streaming response accepts.
 * - `compressStream`, `{ requests, responses }`, the directions it may stream-compress, else
 *   undefined.
 * - `compress`, `{ requests, responses }`, the directions whose messages it may compress without a
 *   dictionary, else undefined.
 *
 * An option the method's kind cannot use throws InvalidArgument.
 */
export function methodSettings(method) {
  const shape = Kinds[method.methodKind];
  const name = method.parent?.typeName ? `${method.parent.typeName}.${method.name}` : method.name;
  if (shape === undefined) {
    throw invalidArgument(`${name}: unsupported RPC kind ${JSON.stringify(method.methodKind)}`);
  }
  const options = trevrpcMethodOptions(method, name);
  checkUnreliable(name, shape, options);
  const { unreliableRequests, unreliableResponses } = options;
  const compression = compressionDirections(name, shape, options, method);
  return {
    kind: shape.name,
    rpcKind: shape.rpcKind,
    streamsRequest: shape.streamsRequest,
    datagrams:
      unreliableRequests || unreliableResponses
        ? { send: unreliableRequests, receive: shape.streamsResponse }
        : undefined,
    // Directions marked unreliable mostly travel as datagrams, so they are not stream-compressed.
    compressStream: directions(
      compression.streamRequests && !unreliableRequests,
      compression.streamResponses && !unreliableResponses,
    ),
    compress: directions(compression.requests, compression.responses),
  };
}

/** `{ requests, responses }`, or undefined if neither is set. */
function directions(requests, responses) {
  return requests || responses ? { requests, responses } : undefined;
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

/** Throws if an `unreliable_*` flag marks a direction that does not stream. */
function checkUnreliable(name, shape, options) {
  if (options.unreliableRequests && !shape.streamsRequest) {
    throw invalidArgument(
      `${name}: unreliable_requests requires a client-streaming or bidirectional method`,
    );
  }
  if (options.unreliableResponses && !shape.streamsResponse) {
    throw invalidArgument(
      `${name}: unreliable_responses requires a server-streaming or bidirectional method`,
    );
  }
}

/**
 * The directions `compression` lets compress, `{ requests, responses, streamRequests,
 * streamResponses }`, leaving out message types with a field marked `debug_redact`.
 * `COMPRESSION_MESSAGE` compresses messages without a dictionary in both directions.
 * `COMPRESSION_STREAM` does too, for the messages outside the stream, and stream-compresses the
 * directions that stream; on a unary method it throws, as does a value this runtime does not know.
 */
function compressionDirections(name, shape, { compression }, method) {
  const none = { requests: false, responses: false, streamRequests: false, streamResponses: false };
  if (compression === Compression.Unspecified) {
    return none;
  }
  if (compression !== Compression.Message && compression !== Compression.Stream) {
    throw invalidArgument(`${name}: unknown compression ${compression}`);
  }
  const stream = compression === Compression.Stream;
  if (stream && !shape.streamsRequest && !shape.streamsResponse) {
    throw invalidArgument(`${name}: COMPRESSION_STREAM requires a streaming method`);
  }
  const requests = !hasRedactedField(method.input);
  const responses = !hasRedactedField(method.output);
  return {
    requests,
    responses,
    streamRequests: stream && requests && shape.streamsRequest,
    streamResponses: stream && responses && shape.streamsResponse,
  };
}

/** Whether each message type checked so far has a redacted field. */
const redacted = new WeakMap();

/**
 * Whether `message`, a DescMessage, has a field marked `debug_redact`, directly or in a
 * message-typed field (singular, repeated or a map value), however deeply nested.
 */
function hasRedactedField(message) {
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
