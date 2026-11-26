//// How a call ends: a code, whose values are gRPC's, a message, trailing
//// metadata and details.

import trevrpc/metadata.{type Metadata}

/// A status code. `Ok` is a call that succeeded, which a client sees as
/// the end of the responses rather than as an error, except where a send
/// finds the call already over.
pub type Code {
  Ok
  Cancelled
  Unknown
  InvalidArgument
  DeadlineExceeded
  NotFound
  AlreadyExists
  PermissionDenied
  ResourceExhausted
  FailedPrecondition
  Aborted
  OutOfRange
  Unimplemented
  Internal
  Unavailable
  DataLoss
  Unauthenticated
}

/// A detail of a status: a protobuf `Any`, its type URL and its message.
pub type Detail {
  Detail(type_url: String, value: BitArray)
}

pub type Status {
  Status(code: Code, message: String, metadata: Metadata, details: List(Detail))
}

/// A status with a code and a message.
pub fn new(code: Code, message: String) -> Status {
  Status(code:, message:, metadata: metadata.new(), details: [])
}

/// Tells the client to wait `milliseconds` before it retries, with a
/// `google.rpc.RetryInfo` detail, the standard detail a rate limit answers
/// with. The detail replaces any `RetryInfo` the status had and leaves its
/// other details alone. A negative delay is zero. Clients read the delay
/// with `retry_delay`, or the equivalent in their language.
@external(erlang, "trevrpc_gleam_ffi", "with_retry_delay")
pub fn with_retry_delay(status: Status, milliseconds: Int) -> Status

/// How many milliseconds the status tells the client to wait before it
/// retries, from its first well-formed `google.rpc.RetryInfo` detail, such
/// as that of a call a server's rate limit rejected, rounded down to a
/// millisecond. A detail that does not decode, or whose delay is negative
/// or not a valid `google.protobuf.Duration`, is ignored.
@external(erlang, "trevrpc_gleam_ffi", "retry_delay")
pub fn retry_delay(status: Status) -> Result(Int, Nil)
