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

export class TrevRpcError extends Error {
  constructor(code, message = "", metadata = {}) {
    const normalizedCode = codeFromNumber(code);
    super(message === "" ? codeName(normalizedCode) : `${codeName(normalizedCode)}: ${message}`);
    this.name = "TrevRpcError";
    this.code = normalizedCode;
    this.statusMessage = message;
    this.metadata = metadata;
  }
}

export class FrameTooLargeError extends Error {
  constructor(length, max) {
    super(`frame length ${length} exceeds maximum ${max}`);
    this.name = "FrameTooLargeError";
    this.length = length;
    this.max = max;
  }
}

export function codeFromNumber(code) {
  return Number.isInteger(code) && code >= 0 && code < CODE_NAMES.length ? code : Code.Unknown;
}

export function codeName(code) {
  return CODE_NAMES[codeFromNumber(code)];
}

export function statusError(code, message = "", metadata = {}) {
  return new TrevRpcError(code, message, metadata);
}

export function statusFromResponse(response) {
  if (response == null) {
    return statusError(Code.Internal, "missing RPC response");
  }

  return statusError(response.status ?? Code.Ok, response.message ?? "", response.metadata ?? {});
}

export function isOkStatus(status) {
  return status != null && codeFromNumber(status.code) === Code.Ok;
}

export function internal(message) {
  return statusError(Code.Internal, message);
}

export function invalidArgument(message) {
  return statusError(Code.InvalidArgument, message);
}

export function deadlineExceeded(message) {
  return statusError(Code.DeadlineExceeded, message);
}

export function unavailable(message) {
  return statusError(Code.Unavailable, message);
}

export function resourceExhausted(message) {
  return statusError(Code.ResourceExhausted, message);
}

export function cancelled(message) {
  return statusError(Code.Cancelled, message);
}

export function statusFromTransportError(error) {
  if (error instanceof TrevRpcError) {
    return error;
  }

  if (error instanceof FrameTooLargeError) {
    return unavailable(`transport unavailable: ${error.message}`);
  }

  if (error?.name === "AbortError") {
    return cancelled("transport closed locally");
  }

  return unavailable(`transport unavailable: ${error?.message ?? String(error)}`);
}
