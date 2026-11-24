//// A request of the HTTP fallback: an HTTP request on the server's sockets
//// that TrevRPC does not claim, such as one for the static content of a web
//// application served from the same port. `server.Config`'s
//// `http_fallback` handles them, over HTTP/3, HTTP/2 and HTTP/1.1, each in a
//// process of its own:
////
//// ```gleam
//// fn fallback(request: Request) -> Nil {
////   let _ = case http_request.path(request) {
////     "/" ->
////       http_request.reply(
////         request,
////         200,
////         [#("content-type", "text/html")],
////         bytes_tree.from_string(html),
////       )
////     _ -> http_request.reply(request, 404, [], bytes_tree.new())
////   }
////   Nil
//// }
//// ```
////
//// The handler answers with `reply`, or with `stream_reply` and then
//// `stream_body`. A handler that returns without answering gets `204`; one
//// that panics before it answered, `500`; and one that panics while it
//// streams a response body has the stream reset, or over HTTP/1.1 the
//// connection closed. One that returns before it ended the body has it
//// ended for it. A client that resets the stream stops the handler's
//// process.
////
//// A request is only for the process that runs its handler: its functions
//// panic in any other, as do a second response and `stream_body` without
//// `stream_reply`.

import gleam/bytes_tree.{type BytesTree}

/// An HTTP request of the fallback.
pub type Request

/// Header fields, with names in lower case.
pub type Headers =
  List(#(String, String))

/// The request's method, such as `GET`.
@external(erlang, "trevrpc_http_request", "method")
pub fn method(request: Request) -> String

/// The path of the request target, without its query.
@external(erlang, "trevrpc_http_request", "path")
pub fn path(request: Request) -> String

/// The query of the request target, without its `?`: empty when there is
/// none.
@external(erlang, "trevrpc_http_request", "qs")
pub fn qs(request: Request) -> String

/// The request's header fields, pseudo-headers left out.
@external(erlang, "trevrpc_http_request", "headers")
pub fn headers(request: Request) -> Headers

/// The first value of header `name`, which is in lower case.
@external(erlang, "trevrpc_gleam_ffi", "http_header")
pub fn header(request: Request, name: String) -> Result(String, Nil)

/// The request's `:authority`, or its `Host` header.
@external(erlang, "trevrpc_http_request", "authority")
pub fn authority(request: Request) -> String

/// `HTTP/3.0`, `HTTP/2.0` or `HTTP/1.1`.
@external(erlang, "trevrpc_http_request", "protocol")
pub fn protocol(request: Request) -> String

/// The client's address, such as `127.0.0.1` or `::1`, and port.
pub type Peer {
  Peer(address: String, port: Int)
}

/// The client's address, when known.
@external(erlang, "trevrpc_gleam_ffi", "http_peer")
pub fn peer(request: Request) -> Result(Peer, Nil)

/// How `read_body` reads.
pub type ReadOptions {
  ReadOptions(
    /// The bytes to collect before returning `More`, at least the first part
    /// that arrives.
    length: Int,
    /// The milliseconds to wait for them.
    timeout: Int,
  )
}

/// 8 MB within 15 s.
pub fn read_options() -> ReadOptions {
  ReadOptions(length: 8_000_000, timeout: 15_000)
}

/// Part of the request body, and the request to read the rest from.
pub type Body {
  /// At least `length` bytes, or what arrived within `timeout`.
  More(data: BitArray, request: Request)
  /// The rest of the body: reading again gives an empty `Last`.
  Last(data: BitArray, request: Request)
}

/// Reads the request body as it arrives; `Error(Nil)` if nothing did within
/// `timeout`. The client may send a window past what the handler read, so a
/// handler that reads slowly holds it back.
@external(erlang, "trevrpc_gleam_ffi", "http_read_body")
pub fn read_body(request: Request, options: ReadOptions) -> Result(Body, Nil)

/// Answers with the whole response. The response to a `HEAD` request leaves
/// `body` out, and keeps the headers that describe it, such as
/// `content-length`. HTTP/1.1 and HTTP/2 responses advertise HTTP/3 through
/// `alt-svc` unless `headers` set it. `Error(Nil)` once the client is gone.
@external(erlang, "trevrpc_gleam_ffi", "http_reply")
pub fn reply(
  request: Request,
  status: Int,
  headers: Headers,
  body: BytesTree,
) -> Result(Nil, Nil)

/// Answers with the response head, whose body follows with `stream_body`.
/// Over HTTP/1.1, a response without `content-length` is sent chunked.
@external(erlang, "trevrpc_gleam_ffi", "http_stream_reply")
pub fn stream_reply(
  request: Request,
  status: Int,
  headers: Headers,
) -> Result(Nil, Nil)

/// Sends part of the response body, the last part with `fin: True`. It waits
/// for a client that reads slowly; `Error(Nil)` once the client is gone.
@external(erlang, "trevrpc_gleam_ffi", "http_stream_body")
pub fn stream_body(
  request: Request,
  data: BytesTree,
  fin fin: Bool,
) -> Result(Nil, Nil)
