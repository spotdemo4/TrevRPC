//// What a method is: its path, how its messages are encoded, and what its
//// schema options allow. Code that protoc-gen-trevrpc-gleam generates
//// describes every method this way, for its client functions and routes.

/// How the messages of one type are encoded and decoded. A message that
/// cannot be decoded is `Error(Nil)`.
pub type Codec(a) {
  Codec(encode: fn(a) -> BitArray, decode: fn(BitArray) -> Result(a, Nil))
}

/// How a direction's messages may be compressed without a dictionary: not
/// at all, each on its own, or with a context per call, for a direction
/// that streams.
pub type Compression {
  NoCompression
  MessageCompression
  StreamCompression
}

pub type Method(request, response) {
  Method(
    /// `/package.Service/Method`.
    path: String,
    request: Codec(request),
    response: Codec(response),
    /// The protobuf full names of the messages, which pick their
    /// compression dictionaries; empty for none.
    request_type: String,
    response_type: String,
    request_compression: Compression,
    response_compression: Compression,
    /// Whether the requests and responses of a streaming direction may
    /// travel as datagrams.
    unreliable_requests: Bool,
    unreliable_responses: Bool,
  )
}

/// A method whose messages are bytes, with no schema options.
pub fn raw(path: String) -> Method(BitArray, BitArray) {
  let codec = Codec(encode: fn(bytes) { bytes }, decode: Ok)
  Method(
    path:,
    request: codec,
    response: codec,
    request_type: "",
    response_type: "",
    request_compression: NoCompression,
    response_compression: NoCompression,
    unreliable_requests: False,
    unreliable_responses: False,
  )
}
