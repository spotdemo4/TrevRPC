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
