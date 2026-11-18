//// A server call, as its handler sees it. A handler runs in a process of
//// its own, which alone can read and write the call; the server ends it
//// when the call's deadline passes, when the client cancels it, and on
//// shutdown.

import gleam/option.{type Option}
import trevrpc/metadata.{type Metadata}
import trevrpc/status.{type Status}

pub type Call(request, response)

/// Reads the next request of a client-streaming or bidirectional call:
/// `None` once the client sent them all.
@external(erlang, "trevrpc_gleam_ffi", "handler_recv")
pub fn recv(call: Call(request, response)) -> Result(Option(request), Status)

/// Sends a response of a server-streaming or bidirectional call, after the
/// headers if they are waiting.
@external(erlang, "trevrpc_gleam_ffi", "handler_send")
pub fn send(
  call: Call(request, response),
  response: response,
) -> Result(Nil, Status)

/// Sends the response headers at once, before any response.
@external(erlang, "trevrpc_gleam_ffi", "send_headers")
pub fn send_headers(
  call: Call(request, response),
  headers: Metadata,
) -> Result(Nil, Status)

/// Sets the trailing metadata the call's status carries.
@external(erlang, "trevrpc_gleam_ffi", "set_trailers")
pub fn set_trailers(call: Call(request, response), trailers: Metadata) -> Nil

/// The request metadata.
@external(erlang, "trevrpc_call", "metadata")
pub fn metadata(call: Call(request, response)) -> Metadata

/// The method's path, `/package.Service/Method`.
@external(erlang, "trevrpc_call", "method")
pub fn method(call: Call(request, response)) -> String

/// How many of the requests read so far arrived as datagrams.
@external(erlang, "trevrpc_call", "datagrams_received")
pub fn datagrams_received(call: Call(request, response)) -> Int
