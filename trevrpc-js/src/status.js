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

const CodeNames = Object.freeze(Object.keys(Code));

/** Error carrying a TrevRPC status code, message, and metadata. */
export class TrevRpcError extends Error {
  /** Creates a TrevRPC status error. */
  constructor(code, message = "", metadata = {}, options = {}) {
    const normalizedCode = codeFromNumber(code);
    const name = codeName(normalizedCode);
    super(
      message === "" ? name : `${name}: ${message}`,
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.name = "TrevRpcError";
    this.code = normalizedCode;
    this.statusMessage = message;
    this.metadata = metadata;
  }
}

/** Converts a number into a known status code, defaulting unknown values to Unknown. */
export function codeFromNumber(code) {
  return Number.isInteger(code) && code >= 0 && code < CodeNames.length ? code : Code.Unknown;
}

/** Returns the canonical status code name. */
export function codeName(code) {
  return CodeNames[codeFromNumber(code)];
}

/** Creates a TrevRPC status error. */
export function statusError(code, message = "", metadata = {}, cause) {
  return new TrevRpcError(code, message, metadata, { cause });
}

/** Creates an internal error status. */
export function internal(message) {
  return statusError(Code.Internal, message);
}

/** Creates an invalid-argument status. */
export function invalidArgument(message, cause) {
  return statusError(Code.InvalidArgument, message, {}, cause);
}

/** Creates a failed-precondition status. */
export function failedPrecondition(message) {
  return statusError(Code.FailedPrecondition, message);
}

/** Creates a deadline-exceeded status. */
export function deadlineExceeded(message) {
  return statusError(Code.DeadlineExceeded, message);
}

/** Creates an unavailable status. */
export function unavailable(message, cause) {
  return statusError(Code.Unavailable, message, {}, cause);
}

/** Creates a resource-exhausted status. */
export function resourceExhausted(message) {
  return statusError(Code.ResourceExhausted, message);
}

/** Creates a cancelled status. */
export function cancelled(message, cause) {
  return statusError(Code.Cancelled, message, {}, cause);
}

/** Converts a transport failure into a status error; status errors pass through. */
export function statusFromTransportError(error) {
  if (error instanceof TrevRpcError) {
    return error;
  }
  if (error?.name === "AbortError") {
    return cancelled("transport closed locally", error);
  }
  return unavailable(`transport unavailable: ${error?.message ?? String(error)}`, error);
}
