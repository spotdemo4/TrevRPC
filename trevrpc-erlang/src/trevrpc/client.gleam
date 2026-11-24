//// Calls on a channel. A call belongs to the process that started it,
//// which alone reads and writes it; if that process exits, the channel
//// cancels the call. A call is finished once it ends (`End` or an error)
//// or is cancelled; reading or writing it then, or from another process,
//// is an error with code `FailedPrecondition`.

import gleam/option.{type Option, None}
import trevrpc/channel.{type Channel}
import trevrpc/metadata.{type Metadata}
import trevrpc/schema.{type Method}
import trevrpc/status.{type Status}

/// A streaming call.
pub type Call(request, response)

pub type Options {
  Options(
    /// Milliseconds the call may take, sent to the server and enforced
    /// locally.
    timeout: Option(Int),
    /// The request metadata.
    metadata: Metadata,
  )
}

/// No timeout and no metadata.
pub fn options() -> Options {
  Options(timeout: None, metadata: metadata.new())
}

/// The response of a call with a single one, and its metadata.
pub type Reply(response) {
  Reply(message: response, headers: Metadata, trailers: Metadata)
}

/// What a streaming call reads: a response, or the end of the responses,
/// with the trailing metadata of the call's OK status.
pub type Received(response) {
  Message(response)
  End(trailers: Metadata)
}

@external(erlang, "trevrpc_gleam_ffi", "unary")
pub fn unary(
  channel: Channel,
  method: Method(request, response),
  request: request,
  options: Options,
) -> Result(Reply(response), Status)

/// Starts a server-streaming call; read its responses with `recv`.
@external(erlang, "trevrpc_gleam_ffi", "server_stream")
pub fn server_stream(
  channel: Channel,
  method: Method(request, response),
  request: request,
  options: Options,
) -> Result(Call(request, response), Status)

/// Starts a client-streaming call: `send` its requests, then
/// `close_and_recv` its response.
@external(erlang, "trevrpc_gleam_ffi", "client_stream")
pub fn client_stream(
  channel: Channel,
  method: Method(request, response),
  options: Options,
) -> Result(Call(request, response), Status)

@external(erlang, "trevrpc_gleam_ffi", "bidi")
pub fn bidi(
  channel: Channel,
  method: Method(request, response),
  options: Options,
) -> Result(Call(request, response), Status)

/// Why `send` did not send a request.
pub type SendError {
  /// The call ended, with this status (OK when the server finished it
  /// successfully); its responses can still be read.
  Ended(Status)
  /// The channel's validator rejected the request, with this status,
  /// normally `InvalidArgument` with the violations `violations.from_status`
  /// reads: it was not sent, and the call goes on. See `trevrpc/validator`.
  Invalid(Status)
}

/// Sends a request.
@external(erlang, "trevrpc_gleam_ffi", "send")
pub fn send(
  call: Call(request, response),
  request: request,
) -> Result(Nil, SendError)

/// Ends the requests.
@external(erlang, "trevrpc_gleam_ffi", "close_send")
pub fn close_send(call: Call(request, response)) -> Result(Nil, Status)

/// Reads the next response, or the end of the call.
@external(erlang, "trevrpc_gleam_ffi", "recv")
pub fn recv(call: Call(request, response)) -> Result(Received(response), Status)

/// Reads the next response, or the end of the call, as `recv` does, but
/// waits for it at most `within` milliseconds: `Ok(None)` means none arrived
/// in time, and the call goes on. The call's timeout still ends it.
@external(erlang, "trevrpc_gleam_ffi", "recv_within")
pub fn recv_within(
  call: Call(request, response),
  within milliseconds: Int,
) -> Result(Option(Received(response)), Status)

/// Ends the requests of a client-streaming call and reads its response.
@external(erlang, "trevrpc_gleam_ffi", "close_and_recv")
pub fn close_and_recv(call: Call(request, response)) -> Result(response, Status)

/// The response headers, reading as far as needed.
@external(erlang, "trevrpc_gleam_ffi", "headers")
pub fn headers(call: Call(request, response)) -> Result(Metadata, Status)

/// Cancels the call.
@external(erlang, "trevrpc_gleam_ffi", "cancel")
pub fn cancel(call: Call(request, response)) -> Nil
