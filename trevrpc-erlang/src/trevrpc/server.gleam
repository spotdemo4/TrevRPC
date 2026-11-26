//// A TrevRPC server: native QUIC, and QMux over TLS on TCP for clients
//// whose networks block UDP, and optionally the HTTP that TrevRPC does not
//// claim, such as a web application's pages, from the same port.

import gleam/option.{type Option, None}
import trevrpc/handler.{type Call}
import trevrpc/http_request.{type Request}
import trevrpc/metadata.{type Metadata}
import trevrpc/schema.{type Method}
import trevrpc/status.{type Status}
import trevrpc/validator.{type Validator}

pub type Server

/// A method's handler.
pub type Route

pub fn unary(
  method: Method(request, response),
  handler: fn(request, Call(request, response)) -> Result(response, Status),
) -> Route {
  route(Unary, method, handler)
}

pub fn server_streaming(
  method: Method(request, response),
  handler: fn(request, Call(request, response)) -> Result(Nil, Status),
) -> Route {
  route(ServerStreaming, method, handler)
}

pub fn client_streaming(
  method: Method(request, response),
  handler: fn(Call(request, response)) -> Result(response, Status),
) -> Route {
  route(ClientStreaming, method, handler)
}

pub fn bidi(
  method: Method(request, response),
  handler: fn(Call(request, response)) -> Result(Nil, Status),
) -> Route {
  route(BidirectionalStreaming, method, handler)
}

type Kind {
  Unary
  ServerStreaming
  ClientStreaming
  BidirectionalStreaming
}

@external(erlang, "trevrpc_gleam_ffi", "route")
fn route(
  kind: Kind,
  method: Method(request, response),
  handler: handler,
) -> Route

/// What an authorizer and the interceptors know of a call: its method's
/// path (`/package.Service/Method`), split into its service and method
/// names, and its request metadata, the connection metadata merged in.
pub type CallInfo {
  CallInfo(
    method: String,
    service: String,
    method_name: String,
    metadata: Metadata,
  )
}

/// How a call ended: its method, as in `CallInfo`, the status it ended
/// with, and the microseconds since its stream opened. An `Internal`
/// status keeps the message the client does not see.
pub type Finished {
  Finished(
    method: String,
    service: String,
    method_name: String,
    status: Status,
    elapsed: Int,
  )
}

/// Authorizes a call before the server looks up its method, so that a
/// caller it rejects learns nothing about which methods exist: `Ok(Nil)`
/// lets the call go on, `Error(status)` ends it with that status, usually
/// `Unauthenticated` or `PermissionDenied`. A call the server refuses for
/// optimistic stream compression is replayed by the client and authorized
/// again, so put side effects, such as counting calls against a rate limit,
/// in an interceptor. See `trevrpc_server` for the details.
pub type Authorizer =
  fn(CallInfo) -> Result(Nil, Status)

/// Runs before the handler of every call that was authorized and whose
/// method the server has, once per handler invocation: `Ok(Some(finish))`
/// registers a function the server calls once with how the call ended,
/// however it ended, in a process of its own, `Ok(None)` none, and
/// `Error(status)` ends the call instead, without running the handler or
/// later interceptors, whose earlier ones still finish.
pub type Interceptor =
  fn(CallInfo) -> Result(Option(fn(Finished) -> Nil), Status)

pub type Config {
  Config(
    /// The UDP port, 0 for any free one.
    port: Int,
    /// PEM files of the certificate, with its chain, and its key.
    cert_file: String,
    key_file: String,
    routes: List(Route),
    /// Whether to listen on TCP for QMux over TLS, by default on the UDP
    /// port's number.
    tcp: Bool,
    tcp_port: Option(Int),
    /// Whether to serve the HTTP mapping, over HTTP/3 and HTTP/2, which
    /// the Android client uses.
    http: Bool,
    /// Whether to serve browsers: WebTransport over HTTP/3, and QMux over
    /// WebSocket on the TCP listener.
    webtransport: Bool,
    /// The origins of the pages, besides the server's own, that may open a
    /// browser session. Browsers always send an `Origin`; clients that send
    /// none are admitted too.
    webtransport_origins: List(String),
    /// The handler of every HTTP request TrevRPC does not claim, over
    /// HTTP/3 on the UDP port, and HTTP/2 and HTTP/1.1 on the TCP listener,
    /// even with `http` and `webtransport` off: see `trevrpc/http_request`.
    /// TrevRPC claims the HTTP path while either of them is on.
    http_fallback: Option(fn(Request) -> Nil),
    /// Whether an HTTP request whose host is another name than the TLS
    /// server name (SNI) its client sent for the connection is answered
    /// `421 Misdirected Request`, as browsers that reuse a connection for
    /// another name the certificate covers need to retry on a connection of
    /// their own. Turn it off behind a proxy that terminates TLS and sends a
    /// server name of its own. See `trevrpc_server` for the details.
    misdirected_request_check: Bool,
    max_frame_size: Option(Int),
    max_streams_per_connection: Option(Int),
    max_concurrent_requests: Option(Int),
    /// Milliseconds the handlers get to finish on shutdown.
    graceful_shutdown_timeout: Option(Int),
    /// Checks request messages before handlers see them: see
    /// `trevrpc/validator`.
    validator: Option(Validator),
    /// Authorizes every call before its method is looked up.
    authorizer: Option(Authorizer),
    /// Run in order before the handler of every call, the first registering
    /// the finish function that runs last.
    interceptors: List(Interceptor),
    /// Whether every call emits the `telemetry` events of
    /// `trevrpc_telemetry`, which cost a little of every call, so they are
    /// off by default.
    telemetry: Bool,
  )
}

/// A server on `port` with these credentials and routes, every other option
/// at its default.
pub fn config(
  port: Int,
  cert_file: String,
  key_file: String,
  routes: List(Route),
) -> Config {
  Config(
    port:,
    cert_file:,
    key_file:,
    routes:,
    tcp: True,
    tcp_port: None,
    http: False,
    webtransport: False,
    webtransport_origins: [],
    http_fallback: None,
    misdirected_request_check: True,
    max_frame_size: None,
    max_streams_per_connection: None,
    max_concurrent_requests: None,
    graceful_shutdown_timeout: None,
    validator: None,
    authorizer: None,
    interceptors: [],
    telemetry: False,
  )
}

/// Starts a server, linked to the calling process: it stops when that
/// process exits. An error says what failed, such as credentials that cannot
/// be read, a port in use, or two routes for one path.
@external(erlang, "trevrpc_gleam_ffi", "start")
pub fn start(config: Config) -> Result(Server, String)

/// The UDP port the server listens on, unless it has stopped.
@external(erlang, "trevrpc_gleam_ffi", "port")
pub fn port(server: Server) -> Result(Int, Nil)

/// The TCP port, unless TCP is off or the server has stopped.
@external(erlang, "trevrpc_gleam_ffi", "tcp_port")
pub fn tcp_port(server: Server) -> Result(Int, Nil)

/// Shuts the server down gracefully, and returns once it is down. Stopping a
/// server that is already down does nothing.
@external(erlang, "trevrpc_gleam_ffi", "stop")
pub fn stop(server: Server) -> Nil
