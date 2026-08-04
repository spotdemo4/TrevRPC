export const Code = Object.freeze({
  Ok: 0,
  Cancelled: 1,
  Unknown: 2,
  InvalidArgument: 3,
  DeadlineExceeded: 4,
  NotFound: 5,
  AlreadyExists: 6,
  PermissionDenied: 7,
  ResourceExhausted: 8,
  FailedPrecondition: 9,
  Aborted: 10,
  OutOfRange: 11,
  Unimplemented: 12,
  Internal: 13,
  Unavailable: 14,
  DataLoss: 15,
  Unauthenticated: 16,
});

const CODE_NAMES = Object.freeze([
  "Ok",
  "Cancelled",
  "Unknown",
  "InvalidArgument",
  "DeadlineExceeded",
  "NotFound",
  "AlreadyExists",
  "PermissionDenied",
  "ResourceExhausted",
  "FailedPrecondition",
  "Aborted",
  "OutOfRange",
  "Unimplemented",
  "Internal",
  "Unavailable",
  "DataLoss",
  "Unauthenticated",
]);

/** Error carrying a TrevRPC status code, message, and metadata. */
export class TrevRpcError extends Error {
  /** Creates a TrevRPC status error. */
  constructor(code, message = "", metadata = {}, options = {}) {
    const normalizedCode = codeFromNumber(code);
    super(message === "" ? codeName(normalizedCode) : `${codeName(normalizedCode)}: ${message}`, {
      cause: options.cause,
    });
    this.name = "TrevRpcError";
    this.code = normalizedCode;
    this.statusMessage = message;
    this.metadata = metadata;
    if (Number.isInteger(options.nativeCode)) {
      this.nativeCode = options.nativeCode;
    }
  }
}

/** Resource-exhausted status reported when a frame exceeds the configured size limit. */
export class FrameTooLargeError extends TrevRpcError {
  /** Creates a frame size error. */
  constructor(length, max) {
    super(Code.ResourceExhausted, `frame length ${length} exceeds maximum ${max}`);
    this.name = "FrameTooLargeError";
    this.length = length;
    this.max = max;
  }
}

/** Converts a number into a known status code, defaulting unknown values to Unknown. */
export function codeFromNumber(code) {
  return Number.isInteger(code) && code >= 0 && code < CODE_NAMES.length ? code : Code.Unknown;
}

/** Returns the canonical status code name. */
export function codeName(code) {
  return CODE_NAMES[codeFromNumber(code)];
}

/** Creates a TrevRPC status error. */
export function statusError(code, message = "", metadata = {}) {
  return new TrevRpcError(code, message, metadata);
}

/** Builds a status error from an RPC response. */
export function statusFromResponse(response) {
  if (response == null) {
    return statusError(Code.Internal, "missing RPC response");
  }

  return statusError(response.status ?? Code.Ok, response.message ?? "", response.metadata ?? {});
}

/** Reports whether a status-like value is OK. */
export function isOkStatus(status) {
  return status != null && codeFromNumber(status.code) === Code.Ok;
}

/** Creates an internal error status. */
export function internal(message) {
  return statusError(Code.Internal, message);
}

/** Creates an invalid-argument status. */
export function invalidArgument(message) {
  return statusError(Code.InvalidArgument, message);
}

/** Creates a deadline-exceeded status. */
export function deadlineExceeded(message) {
  return statusError(Code.DeadlineExceeded, message);
}

/** Creates an unavailable status. */
export function unavailable(message) {
  return statusError(Code.Unavailable, message);
}

/** Creates a resource-exhausted status. */
export function resourceExhausted(message) {
  return statusError(Code.ResourceExhausted, message);
}

/** Creates a cancelled status. */
export function cancelled(message) {
  return statusError(Code.Cancelled, message);
}

/** Converts a transport error into a TrevRPC status error. */
export function statusFromTransportError(error) {
  if (error instanceof TrevRpcError) {
    return error;
  }

  if (error?.name === "AbortError") {
    return new TrevRpcError(Code.Cancelled, "transport closed locally", {}, { cause: error });
  }

  return new TrevRpcError(
    Code.Unavailable,
    `transport unavailable: ${error?.message ?? String(error)}`,
    {},
    { cause: error },
  );
}
