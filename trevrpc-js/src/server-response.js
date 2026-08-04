import { normalizeMetadata } from "./metadata.js";
import { Code, codeFromNumber } from "./status.js";

const UnaryResponseBrand = Symbol("trevrpc.unaryServerResponse");
const StreamingResponseBrand = Symbol("trevrpc.streamingServerResponse");

/** Creates an opaque unary generated-server response envelope. */
export function createUnaryResponse(message, metadata = {}) {
  return Object.freeze({
    [UnaryResponseBrand]: true,
    message,
    metadata: Object.freeze(normalizeMetadata(metadata)),
  });
}

/** Creates an opaque streaming generated-server response envelope. */
export function createStreamingResponse(messages, status = {}) {
  if (!isIterable(messages)) {
    throw new TypeError("streaming response messages must be an iterable or async iterable");
  }

  return Object.freeze({
    [StreamingResponseBrand]: true,
    messages,
    status: Object.freeze({
      code: codeFromNumber(status.code ?? Code.Ok),
      message: status.message ?? "",
      metadata: Object.freeze(normalizeMetadata(status.metadata ?? {})),
    }),
  });
}

/** Internal generated-dispatcher predicate. */
export function isUnaryServerResponse(value) {
  return value?.[UnaryResponseBrand] === true;
}

/** Internal generated-dispatcher predicate. */
export function isStreamingServerResponse(value) {
  return value?.[StreamingResponseBrand] === true;
}

function isIterable(value) {
  return (
    value != null &&
    (typeof value[Symbol.iterator] === "function" ||
      typeof value[Symbol.asyncIterator] === "function")
  );
}
