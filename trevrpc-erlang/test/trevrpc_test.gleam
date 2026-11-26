//// A Gleam server and client on loopback, through the code
//// protoc-gen-trevrpc-gleam generates from test/proto: every kind of call,
//// over native QUIC and QMux over TLS, statuses, metadata, datagrams,
//// deadlines, cancellation, the failures the binding reports, and the HTTP
//// fallback.

import gleam/bit_array
import gleam/bytes_tree
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import gleeunit/should
import greet/v1/greeter.{HelloReply, HelloRequest}
import greet/v1/greeter_trevrpc
import trevrpc/bearer
import trevrpc/channel
import trevrpc/client
import trevrpc/handler
import trevrpc/http_request
import trevrpc/metadata
import trevrpc/schema
import trevrpc/server
import trevrpc/status
import trevrpc/validator
import trevrpc/violations

/// Runs this module's tests. gleeunit.main() would run every module in test/,
/// the runtime's eunit tests too, which rebar3 runs.
pub fn main() -> Nil {
  run_tests()
}

@external(erlang, "trevrpc_test_ffi", "run_tests")
fn run_tests() -> Nil

// The service

fn handlers() -> greeter_trevrpc.Handlers {
  greeter_trevrpc.Handlers(
    say_hello: say_hello,
    countdown: countdown,
    collect: collect,
    chat: chat,
    telemetry: fn(call) { telemetry(call, 0) },
    channel: say_hello,
    client: say_hello,
    handler: say_hello,
    schema: say_hello,
    server: say_hello,
    status: say_hello,
  )
}

type Call =
  handler.Call(greeter.HelloRequest, greeter.HelloReply)

/// Echoes x-echo as a header and as a trailer.
fn echo_metadata(call: Call) -> Nil {
  case metadata.get(handler.metadata(call), "x-echo") {
    Ok(value) -> {
      let assert Ok(Nil) =
        handler.send_headers(call, metadata.from_list([#("x-echo", value)]))
      handler.set_trailers(call, metadata.from_list([#("x-trailer", value)]))
    }
    Error(Nil) -> Nil
  }
}

fn say_hello(
  request: greeter.HelloRequest,
  call: Call,
) -> Result(greeter.HelloReply, status.Status) {
  sleep(request.delay)
  case request.fail {
    Some(message) -> Error(status.new(status.InvalidArgument, message))
    None -> {
      echo_metadata(call)
      Ok(HelloReply(message: "hello " <> request.name, index: 0))
    }
  }
}

fn countdown(
  request: greeter.HelloRequest,
  call: Call,
) -> Result(Nil, status.Status) {
  echo_metadata(call)
  range(1, request.count)
  |> list.try_each(fn(index) {
    case handler.send(call, HelloReply(message: request.name, index:)) {
      Ok(Nil) ->
        case request.fail {
          Some(message) -> Error(status.new(status.InvalidArgument, message))
          None -> Ok(Nil)
        }
      Error(status) -> Error(status)
    }
  })
}

fn collect(call: Call) -> Result(greeter.HelloReply, status.Status) {
  collect_names(call, [])
}

fn collect_names(
  call: Call,
  names: List(String),
) -> Result(greeter.HelloReply, status.Status) {
  case handler.recv(call) {
    Ok(Some(request)) -> collect_names(call, [request.name, ..names])
    Ok(None) ->
      Ok(HelloReply(
        message: string.join(list.reverse(names), ","),
        index: list.length(names),
      ))
    Error(status) -> Error(status)
  }
}

/// A request named "watch" registers the handler's process under that name.
fn chat(call: Call) -> Result(Nil, status.Status) {
  case handler.recv(call) {
    Ok(Some(request)) -> {
      case request.name {
        "watch" -> register_self(watched)
        _ -> Nil
      }
      case handler.send(call, HelloReply(message: request.name, index: 0)) {
        Ok(Nil) -> chat(call)
        Error(status) -> Error(status)
      }
    }
    Ok(None) -> Ok(Nil)
    Error(status) -> Error(status)
  }
}

fn telemetry(call: Call, received: Int) -> Result(Nil, status.Status) {
  case handler.recv(call) {
    Ok(Some(request)) -> {
      case handler.send(call, HelloReply(message: request.name, index: 0)) {
        Ok(Nil) -> telemetry(call, received + 1)
        Error(status) -> Error(status)
      }
    }
    Ok(None) -> {
      handler.set_trailers(
        call,
        metadata.from_list([
          #("x-received", int.to_string(received)),
          #("x-datagrams", int.to_string(handler.datagrams_received(call))),
        ]),
      )
      Ok(Nil)
    }
    Error(status) -> Error(status)
  }
}

const watched = "trevrpc_test_watched_handler"

// Erlang

@external(erlang, "trevrpc_test_ffi", "credentials")
fn credentials() -> #(String, String, String)

@external(erlang, "trevrpc_test_ffi", "remove")
fn remove(dir: String) -> Nil

@external(erlang, "trevrpc_test_ffi", "sleep")
fn sleep(milliseconds: Int) -> Nil

@external(erlang, "trevrpc_test_ffi", "quietly")
fn quietly(run: fn() -> a) -> a

@external(erlang, "trevrpc_test_ffi", "register_self")
fn register_self(name: String) -> Nil

@external(erlang, "trevrpc_test_ffi", "is_alive")
fn is_alive(name: String) -> Bool

type Inbox

@external(erlang, "trevrpc_test_ffi", "inbox")
fn inbox() -> Inbox

@external(erlang, "trevrpc_test_ffi", "send")
fn send(inbox: Inbox, message: a) -> Nil

@external(erlang, "trevrpc_test_ffi", "receive_within")
fn receive_within(milliseconds: Int) -> Result(a, Nil)

type Monitor

@external(erlang, "trevrpc_test_ffi", "monitor")
fn monitor(name: String) -> Monitor

@external(erlang, "trevrpc_test_ffi", "wait_down")
fn wait_down(monitor: Monitor, timeout: Int) -> Bool

// Servers and channels

/// Runs `run` with a channel over `transport` to a new server with the
/// Greeter's routes, and `extra` ones.
fn with_channel(
  transport: channel.Transport,
  extra: List(server.Route),
  run: fn(channel.Channel) -> a,
) -> a {
  with_configured_channel(transport, extra, fn(config) { config }, run)
}

/// As `with_channel`, with the channel's configuration changed by
/// `configure`.
fn with_configured_channel(
  transport: channel.Transport,
  extra: List(server.Route),
  configure: fn(channel.Config) -> channel.Config,
  run: fn(channel.Channel) -> a,
) -> a {
  let #(cert, key, dir) = credentials()
  let routes = list.append(greeter_trevrpc.routes(handlers()), extra)
  let assert Ok(srv) = server.start(server.config(0, cert, key, routes))
  let assert Ok(port) = server.port(srv)
  let assert Ok(tcp_port) = server.tcp_port(srv)
  let config =
    channel.Config(
      ..channel.config("127.0.0.1", port),
      tcp_port: Some(tcp_port),
      transport:,
      server_name: Some("localhost"),
      ca_file: Some(cert),
    )
  let assert Ok(ch) = channel.connect(configure(config), 10_000)
  let result = run(ch)
  channel.close(ch)
  server.stop(srv)
  remove(dir)
  result
}

fn both(run: fn(channel.Transport, channel.Channel) -> a) -> Nil {
  [channel.Quic, channel.Tcp]
  |> list.each(fn(transport) {
    with_channel(transport, [], fn(ch) { run(transport, ch) })
  })
}

/// `from` up to `to`.
fn range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..range(from + 1, to)]
  }
}

fn hello(name: String) -> greeter.HelloRequest {
  HelloRequest(..greeter.new_hello_request(), name:)
}

/// The messages until the end of the call, and its trailers.
fn drain(
  call: client.Call(greeter.HelloRequest, greeter.HelloReply),
  replies: List(greeter.HelloReply),
) -> #(List(greeter.HelloReply), metadata.Metadata) {
  case client.recv(call) {
    Ok(client.Message(reply)) -> drain(call, [reply, ..replies])
    Ok(client.End(trailers)) -> #(list.reverse(replies), trailers)
    Error(error) -> panic as string.inspect(error)
  }
}

fn int_trailer(trailers: metadata.Metadata, name: String) -> Int {
  let assert Ok(text) = metadata.get(trailers, name)
  let assert Ok(value) = int.parse(text)
  value
}

// Calls

pub fn unary_test() {
  use _, ch <- both()
  let assert Ok(reply) =
    greeter_trevrpc.say_hello(ch, hello("alice"), client.options())
  reply.message |> should.equal(HelloReply(message: "hello alice", index: 0))
}

/// Methods named as the modules the generated code imports from trevrpc:
/// the generated code still reaches the modules.
pub fn module_names_test() {
  use ch <- with_channel(channel.Quic, [])
  [
    greeter_trevrpc.channel,
    greeter_trevrpc.client,
    greeter_trevrpc.handler,
    greeter_trevrpc.schema,
    greeter_trevrpc.server,
    greeter_trevrpc.status,
  ]
  |> list.each(fn(call) {
    let assert Ok(reply) = call(ch, hello("m"), client.options())
    reply.message.message |> should.equal("hello m")
  })
}

pub fn metadata_test() {
  use _, ch <- both()
  let options =
    client.Options(
      timeout: Some(5000),
      metadata: metadata.from_list([#("x-echo", "v")]),
    )
  let assert Ok(reply) = greeter_trevrpc.say_hello(ch, hello("bob"), options)
  metadata.get(reply.headers, "x-echo") |> should.equal(Ok("v"))
  metadata.get(reply.trailers, "x-trailer") |> should.equal(Ok("v"))
}

/// Connection metadata reaches every call, the call's own entries winning,
/// until it is replaced or removed.
pub fn connection_metadata_test() {
  use transport <- list.each([channel.Quic, channel.Tcp])
  use ch <- with_configured_channel(transport, [], fn(config) {
    channel.Config(
      ..config,
      connection_metadata: metadata.from_list([#("x-echo", "c")]),
    )
  })
  let echoed = fn(options) {
    let assert Ok(reply) = greeter_trevrpc.say_hello(ch, hello("c"), options)
    metadata.get(reply.headers, "x-echo")
  }
  echoed(client.options()) |> should.equal(Ok("c"))
  echoed(client.options()) |> should.equal(Ok("c"))
  echoed(
    client.Options(
      ..client.options(),
      metadata: metadata.from_list([#("x-echo", "own")]),
    ),
  )
  |> should.equal(Ok("own"))
  let assert Ok(Nil) =
    channel.set_connection_metadata(ch, metadata.from_list([#("x-echo", "d")]))
  echoed(client.options()) |> should.equal(Ok("d"))
  let assert Ok(Nil) = channel.set_connection_metadata(ch, metadata.new())
  echoed(client.options()) |> should.equal(Error(Nil))
  let assert Error(error) =
    channel.set_connection_metadata(ch, metadata.from_list([#("Bad", "x")]))
  error.code |> should.equal(status.InvalidArgument)
}

pub fn headers_test() {
  use _, ch <- both()
  let options =
    client.Options(
      ..client.options(),
      metadata: metadata.from_list([#("x-echo", "h")]),
    )
  let assert Ok(call) =
    greeter_trevrpc.countdown(ch, HelloRequest(..hello("n"), count: 1), options)
  let assert Ok(headers) = client.headers(call)
  metadata.get(headers, "x-echo") |> should.equal(Ok("h"))
  let #(replies, trailers) = drain(call, [])
  list.length(replies) |> should.equal(1)
  metadata.get(trailers, "x-trailer") |> should.equal(Ok("h"))
}

pub fn error_test() {
  use _, ch <- both()
  let request = HelloRequest(..hello("x"), fail: Some("no thanks"))
  let assert Error(error) =
    greeter_trevrpc.say_hello(ch, request, client.options())
  error.code |> should.equal(status.InvalidArgument)
  error.message |> should.equal("no thanks")
}

pub fn server_stream_test() {
  use _, ch <- both()
  let assert Ok(call) =
    greeter_trevrpc.countdown(
      ch,
      HelloRequest(..hello("n"), count: 3),
      client.options(),
    )
  let #(replies, trailers) = drain(call, [])
  list.map(replies, fn(reply) { reply.index }) |> should.equal([1, 2, 3])
  trailers |> should.equal(metadata.new())
}

/// A handler that fails once it has sent a response.
pub fn stream_error_test() {
  use _, ch <- both()
  let request = HelloRequest(..hello("n"), count: 3, fail: Some("enough"))
  let assert Ok(call) = greeter_trevrpc.countdown(ch, request, client.options())
  let assert Ok(client.Message(reply)) = client.recv(call)
  reply.index |> should.equal(1)
  let assert Error(error) = client.recv(call)
  error.code |> should.equal(status.InvalidArgument)
  error.message |> should.equal("enough")
}

pub fn client_stream_test() {
  use _, ch <- both()
  let assert Ok(call) = greeter_trevrpc.collect(ch, client.options())
  ["a", "b", "c"]
  |> list.each(fn(name) {
    let assert Ok(Nil) = client.send(call, hello(name))
  })
  client.close_and_recv(call)
  |> should.equal(Ok(HelloReply(message: "a,b,c", index: 3)))
}

pub fn bidi_test() {
  use _, ch <- both()
  let assert Ok(call) = greeter_trevrpc.chat(ch, client.options())
  range(1, 5)
  |> list.each(fn(n) {
    let name = int.to_string(n)
    let assert Ok(Nil) = client.send(call, hello(name))
    let assert Ok(client.Message(reply)) = client.recv(call)
    reply.message |> should.equal(name)
  })
  let assert Ok(Nil) = client.close_send(call)
  let assert Ok(client.End(_)) = client.recv(call)
}

/// `recv_within` stops waiting without ending the call.
pub fn recv_within_test() {
  use _, ch <- both()
  let assert Ok(call) = greeter_trevrpc.chat(ch, client.options())
  client.recv_within(call, within: 0) |> should.equal(Ok(None))
  client.recv_within(call, within: 50) |> should.equal(Ok(None))
  let assert Ok(Nil) = client.send(call, hello("1"))
  let assert Ok(Some(client.Message(reply))) =
    client.recv_within(call, within: 5000)
  reply.message |> should.equal("1")
  let assert Ok(Nil) = client.close_send(call)
  let assert Ok(Some(client.End(_))) = client.recv_within(call, within: 5000)
}

/// The call's timeout ends a call that takes longer.
pub fn timeout_test() {
  use _, ch <- both()
  let options = client.Options(..client.options(), timeout: Some(200))
  let assert Error(error) =
    greeter_trevrpc.say_hello(
      ch,
      HelloRequest(..hello("slow"), delay: 2000),
      options,
    )
  error.code |> should.equal(status.DeadlineExceeded)
}

/// Cancelling a call ends it on both sides: the server stops its handler.
pub fn cancel_test() {
  use _, ch <- both()
  let assert Ok(call) = greeter_trevrpc.chat(ch, client.options())
  let assert Ok(Nil) = client.send(call, hello("watch"))
  let assert Ok(client.Message(_)) = client.recv(call)
  is_alive(watched) |> should.be_true
  let handler_process = monitor(watched)
  client.cancel(call)
  wait_down(handler_process, 5000) |> should.be_true
  // The call is finished.
  let assert Error(error) = client.recv(call)
  error.code |> should.equal(status.FailedPrecondition)
  let assert Error(client.Ended(error)) = client.send(call, hello("late"))
  error.code |> should.equal(status.FailedPrecondition)
}

/// An unreliable method's messages travel as datagrams over native QUIC,
/// where some may be lost but never duplicated, and on the stream over QMux.
pub fn unreliable_test() {
  use transport, ch <- both()
  let assert Ok(call) = greeter_trevrpc.telemetry(ch, client.options())
  let names = list.map(range(1, 20), int.to_string)
  list.each(names, fn(name) {
    let assert Ok(Nil) = client.send(call, hello(name))
  })
  let assert Ok(Nil) = client.close_send(call)
  let #(replies, trailers) = drain(call, [])
  let echoed = list.map(replies, fn(reply) { reply.message })
  let received = int_trailer(trailers, "x-received")
  let datagrams = int_trailer(trailers, "x-datagrams")
  case transport {
    channel.Quic -> {
      list.unique(echoed) |> should.equal(echoed)
      list.filter(echoed, fn(name) { !list.contains(names, name) })
      |> should.equal([])
      { received >= 1 && received <= 20 } |> should.be_true
      { datagrams >= 1 } |> should.be_true
    }
    _ -> {
      echoed |> should.equal(names)
      #(received, datagrams) |> should.equal(#(20, 0))
    }
  }
}

pub fn unknown_method_test() {
  use _, ch <- both()
  let assert Error(error) =
    client.unary(
      ch,
      schema.raw("/greet.v1.Greeter/Nope"),
      <<>>,
      client.options(),
    )
  error.code |> should.equal(status.Unimplemented)
}

/// A request the server cannot decode, and a response the client cannot.
pub fn decode_failure_test() {
  let garbage =
    server.unary(schema.raw("/raw.Garbage/Get"), fn(_request, _call) {
      Ok(<<255, 255, 255>>)
    })
  use ch <- with_channel(channel.Quic, [garbage])
  let assert Error(error) =
    client.unary(
      ch,
      schema.raw("/greet.v1.Greeter/SayHello"),
      <<255, 255, 255>>,
      client.options(),
    )
  error.code |> should.equal(status.InvalidArgument)
  let raw = schema.raw("/raw.Garbage/Get")
  let typed =
    schema.Method(
      path: raw.path,
      request: raw.request,
      response: greeter_trevrpc.say_hello_method().response,
      request_type: "",
      response_type: "",
      request_compression: schema.NoCompression,
      response_compression: schema.NoCompression,
      unreliable_requests: False,
      unreliable_responses: False,
    )
  let assert Error(error) = client.unary(ch, typed, <<>>, client.options())
  error.code |> should.equal(status.Internal)
}

/// A route written by hand, over bytes.
pub fn raw_route_test() {
  let echo_ =
    server.unary(schema.raw("/raw.Echo/Echo"), fn(request, _call) {
      Ok(<<request:bits, "!":utf8>>)
    })
  use ch <- with_channel(channel.Quic, [echo_])
  let assert Ok(reply) =
    client.unary(
      ch,
      schema.raw("/raw.Echo/Echo"),
      <<"hi":utf8>>,
      client.options(),
    )
  reply.message |> should.equal(<<"hi!":utf8>>)
}

/// The schema options the generator read from the method's trevrpc.method.
pub fn schema_options_test() {
  let method = greeter_trevrpc.telemetry_method()
  method.unreliable_requests |> should.be_true
  method.unreliable_responses |> should.be_true
  method.request_compression |> should.equal(schema.MessageCompression)
  greeter_trevrpc.say_hello_method().request_compression
  |> should.equal(schema.NoCompression)
}

/// A server that cannot start says why, and a stopped one has no port.
pub fn server_lifecycle_test() {
  let #(cert, key, dir) = credentials()
  let assert Error(_) =
    server.start(server.config(0, dir <> "/missing.pem", key, []))
  let route = server.unary(schema.raw("/raw.Echo/Echo"), fn(r, _) { Ok(r) })
  server.start(server.config(0, cert, key, [route, route]))
  |> should.equal(Error("two routes for /raw.Echo/Echo"))
  server.start(
    server.Config(
      ..server.config(0, cert, key, [route]),
      webtransport: True,
      webtransport_origins: ["app.example.com"],
    ),
  )
  |> should.equal(Error("not an origin: app.example.com"))
  let assert Ok(srv) =
    server.start(
      server.Config(
        ..server.config(0, cert, key, [route]),
        webtransport: True,
        webtransport_origins: ["https://app.example.com"],
      ),
    )
  let assert Ok(_) = server.port(srv)
  server.stop(srv)
  server.port(srv) |> should.equal(Error(Nil))
  server.tcp_port(srv) |> should.equal(Error(Nil))
  server.stop(srv)
  remove(dir)
}

// The HTTP fallback

fn fallback(request: http_request.Request) -> Nil {
  let assert Ok(Nil) = case http_request.path(request) {
    "/upload" -> upload(request, bytes_tree.new())
    "/upload-in-two" -> upload_in_two(request)
    "/download" -> download(request)
    "/timeout" -> read_timeout(request)
    "/panic" -> panic as "the test's panic"
    _ -> describe(request)
  }
  Nil
}

/// The request, as the accessors see it, and its body.
fn describe(request: http_request.Request) -> Result(Nil, Nil) {
  let assert Ok(http_request.Last(body, request)) =
    http_request.read_body(request, http_request.read_options())
  let assert Ok(peer) = http_request.peer(request)
  let test_header = result.unwrap(http_request.header(request, "x-test"), "-")
  let listed =
    list.contains(http_request.headers(request), #("x-test", test_header))
  let description =
    string.join(
      [
        http_request.protocol(request),
        http_request.method(request),
        http_request.path(request),
        http_request.qs(request),
        http_request.authority(request),
        test_header,
        string.inspect(listed),
        peer.address,
        string.inspect(peer.port > 0),
      ],
      " ",
    )
  http_request.reply(
    request,
    200,
    [#("content-type", "text/plain")],
    bytes_tree.from_string(description <> "\n")
      |> bytes_tree.append(body),
  )
}

/// Echoes the body, read in parts of at least 16 KiB.
fn upload(
  request: http_request.Request,
  body: bytes_tree.BytesTree,
) -> Result(Nil, Nil) {
  case read_rest(request, 16_384, body) {
    Ok(#(body, request)) -> http_request.reply(request, 200, [], body)
    Error(Nil) -> http_request.reply(request, 408, [], bytes_tree.new())
  }
}

/// `body` and the rest of the request body, read in parts of at least
/// `length` bytes.
fn read_rest(
  request: http_request.Request,
  length: Int,
  body: bytes_tree.BytesTree,
) -> Result(#(bytes_tree.BytesTree, http_request.Request), Nil) {
  let options = http_request.ReadOptions(..http_request.read_options(), length:)
  case http_request.read_body(request, options) {
    Ok(http_request.More(data, request)) ->
      read_rest(request, length, bytes_tree.append(body, data))
    Ok(http_request.Last(data, request)) ->
      Ok(#(bytes_tree.append(body, data), request))
    Error(Nil) -> Error(Nil)
  }
}

/// Reads the first part of the body, which is `More`, as the client sends
/// the rest only once the response head arrived; then echoes the body.
fn upload_in_two(request: http_request.Request) -> Result(Nil, Nil) {
  let options =
    http_request.ReadOptions(..http_request.read_options(), length: 1)
  let assert Ok(http_request.More(first, request)) =
    http_request.read_body(request, options)
  use Nil <- result.try(http_request.stream_reply(request, 200, []))
  let assert Ok(#(body, request)) =
    read_rest(request, 16_384, bytes_tree.from_bit_array(first))
  http_request.stream_body(request, body, fin: True)
}

/// Waits 50 ms for a body that does not come.
fn read_timeout(request: http_request.Request) -> Result(Nil, Nil) {
  let options =
    http_request.ReadOptions(..http_request.read_options(), timeout: 50)
  case http_request.read_body(request, options) {
    Error(Nil) -> http_request.reply(request, 408, [], bytes_tree.new())
    Ok(_) -> http_request.reply(request, 200, [], bytes_tree.new())
  }
}

/// Streams `qs` parts of 64 KiB, the first of `a`s, the next of `b`s, and
/// so on.
fn download(request: http_request.Request) -> Result(Nil, Nil) {
  let assert Ok(count) = int.parse(http_request.qs(request))
  use Nil <- result.try(
    http_request.stream_reply(request, 200, [
      #("content-type", "application/octet-stream"),
    ]),
  )
  use Nil <- result.try(
    list.try_each(range(0, count - 1), fn(n) {
      http_request.stream_body(request, download_part(n), fin: False)
    }),
  )
  http_request.stream_body(request, bytes_tree.new(), fin: True)
}

fn download_part(n: Int) -> bytes_tree.BytesTree {
  let assert Ok(letter) = bit_array.to_string(<<{ 97 + n % 26 }>>)
  bytes_tree.from_string(string.repeat(letter, 65_536))
}

type HttpVersion {
  H3
  H2
  H1
}

@external(erlang, "trevrpc_test_ffi", "http_unfinished")
fn http_unfinished(version: HttpVersion, port: Int, path: String) -> Int

@external(erlang, "trevrpc_test_ffi", "http_upload_in_two")
fn http_upload_in_two(
  version: HttpVersion,
  port: Int,
  path: String,
  first: BitArray,
  rest: BitArray,
) -> #(Int, BitArray)

@external(erlang, "trevrpc_test_ffi", "http_status_for")
fn http_status_for(
  version: HttpVersion,
  port: Int,
  server_name: String,
  authority: String,
) -> Int

@external(erlang, "trevrpc_test_ffi", "http_exchange")
fn http_exchange(
  version: HttpVersion,
  port: Int,
  method: String,
  path: String,
  headers: List(#(String, String)),
  body: BitArray,
) -> #(Int, List(#(String, String)), BitArray)

/// Runs `run` with the UDP and TCP ports of a server with only the
/// fallback.
fn with_fallback(run: fn(Int, Int) -> a) -> a {
  let #(cert, key, dir) = credentials()
  let assert Ok(srv) =
    server.start(
      server.Config(
        ..server.config(0, cert, key, []),
        http_fallback: Some(fallback),
      ),
    )
  let assert Ok(port) = server.port(srv)
  let assert Ok(tcp_port) = server.tcp_port(srv)
  let result = run(port, tcp_port)
  server.stop(srv)
  remove(dir)
  result
}

/// Runs `run` over each of `versions`, against a server with only the
/// fallback, with the port and the protocol `http_request.protocol` gives.
fn each_http(
  versions: List(HttpVersion),
  run: fn(HttpVersion, Int, String) -> a,
) -> Nil {
  use port, tcp_port <- with_fallback()
  versions
  |> list.each(fn(version) {
    case version {
      H3 -> run(H3, port, "HTTP/3.0")
      H2 -> run(H2, tcp_port, "HTTP/2.0")
      H1 -> run(H1, tcp_port, "HTTP/1.1")
    }
  })
}

const all_http = [H3, H2, H1]

/// A request for another name than the connection's TLS server name gets
/// 421, unless `misdirected_request_check` is off.
pub fn http_fallback_misdirected_request_test() {
  [#(True, 421), #(False, 200)]
  |> list.each(fn(case_) {
    let #(check, want) = case_
    let #(cert, key, dir) = credentials()
    let assert Ok(srv) =
      server.start(
        server.Config(
          ..server.config(0, cert, key, []),
          http_fallback: Some(fallback),
          misdirected_request_check: check,
        ),
      )
    let assert Ok(port) = server.port(srv)
    let assert Ok(tcp_port) = server.tcp_port(srv)
    [#(H3, port), #(H2, tcp_port), #(H1, tcp_port)]
    |> list.each(fn(client) {
      let #(version, port) = client
      http_status_for(version, port, "localhost", "other.example")
      |> should.equal(want)
      http_status_for(version, port, "localhost", "localhost")
      |> should.equal(200)
    })
    server.stop(srv)
    remove(dir)
  })
}

pub fn http_fallback_request_test() {
  use version, port, protocol <- each_http(all_http)
  let #(status, headers, body) =
    http_exchange(version, port, "POST", "/some/path?n=1", [#("x-test", "t")], <<
      "hello":utf8,
    >>)
  status |> should.equal(200)
  list.key_find(headers, "content-type") |> should.equal(Ok("text/plain"))
  body
  |> should.equal(<<
    protocol:utf8,
    " POST /some/path n=1 localhost t True 127.0.0.1 True\nhello":utf8,
  >>)
  // A request without a body, query or header.
  let #(_, _, body) = http_exchange(version, port, "GET", "/", [], <<>>)
  body
  |> should.equal(<<
    protocol:utf8,
    " GET /  localhost - False 127.0.0.1 True\n":utf8,
  >>)
}

pub fn http_fallback_upload_test() {
  use version, port, _ <- each_http(all_http)
  let upload = bit_array.from_string(string.repeat("0123456789abcdef", 16_384))
  let #(status, _, body) =
    http_exchange(version, port, "POST", "/upload", [], upload)
  status |> should.equal(200)
  body |> should.equal(upload)
}

/// A body read before it all arrived comes as `More`, then `Last`. HTTP/1.1
/// is left out: its client sends the body whole.
pub fn http_fallback_read_parts_test() {
  use version, port, _ <- each_http([H3, H2])
  let first = <<"first part":utf8>>
  let rest = bit_array.from_string(string.repeat("rest", 16_384))
  http_upload_in_two(version, port, "/upload-in-two", first, rest)
  |> should.equal(#(200, bit_array.append(first, rest)))
}

/// `read_body` gives up on a body that does not come. HTTP/1.1 is left out:
/// its client cannot hold a body back.
pub fn http_fallback_read_timeout_test() {
  use version, port, _ <- each_http([H3, H2])
  http_unfinished(version, port, "/timeout") |> should.equal(408)
}

/// A handler that panics before it answered gets 500.
pub fn http_fallback_panic_test() {
  use <- quietly()
  use version, port, _ <- each_http(all_http)
  let #(status, _, _) = http_exchange(version, port, "GET", "/panic", [], <<>>)
  status |> should.equal(500)
}

pub fn http_fallback_download_test() {
  use version, port, _ <- each_http(all_http)
  let #(status, headers, body) =
    http_exchange(version, port, "GET", "/download?8", [], <<>>)
  status |> should.equal(200)
  list.key_find(headers, "content-type")
  |> should.equal(Ok("application/octet-stream"))
  body
  |> should.equal(
    range(0, 7)
    |> list.map(download_part)
    |> bytes_tree.concat
    |> bytes_tree.to_bit_array,
  )
}

// Validation

/// A name that is empty breaks `string.min_len` on the field `name`.
fn empty_name() -> violations.Violation {
  violations.Violation(
    field: [
      violations.FieldPathElement(
        field_number: 1,
        field_name: "name",
        field_type: Some(violations.TypeString),
        key_type: None,
        value_type: None,
        subscript: None,
      ),
    ],
    rule: [],
    rule_id: "string.min_len",
    message: "value length must be at least 1 characters",
    for_key: False,
  )
}

/// Checks HelloRequests, which it reads the name of from the decoded
/// message: an empty name breaks a rule.
fn hello_validator() -> validator.Validator {
  fn(type_name) {
    case type_name {
      "greet.v1.HelloRequest" -> Some(check_hello)
      _ -> None
    }
  }
}

fn check_hello(
  request: Dynamic,
  _encoded: BitArray,
) -> Result(Nil, status.Status) {
  case decode.run(request, decode.at([1], decode.string)) {
    Ok("") -> Error(violations.status([empty_name()]))
    Ok(_) -> Ok(Nil)
    Error(_) -> Error(status.new(status.Internal, "not a HelloRequest"))
  }
}

/// A RetryInfo detail round-trips, replacing an earlier one and keeping
/// other details, and its bytes are the ones every runtime's tests use.
pub fn retry_delay_test() {
  let other = status.Detail(type_url: "other", value: <<1, 2>>)
  let busy =
    status.Status(..status.new(status.ResourceExhausted, "busy"), details: [
      other,
    ])
    |> status.with_retry_delay(30_000)
    |> status.with_retry_delay(1500)
  busy.details
  |> should.equal([
    other,
    status.Detail(type_url: "google.rpc.RetryInfo", value: <<
      0x0a, 0x08, 0x08, 0x01, 0x10, 0x80, 0xca, 0xb5, 0xee, 0x01,
    >>),
  ])
  status.retry_delay(busy) |> should.equal(Ok(1500))
  status.retry_delay(status.new(status.NotFound, "missing"))
  |> should.equal(Error(Nil))
}

/// Bearer metadata carries the token a handler reads back, and malformed or
/// missing credentials are told apart.
pub fn bearer_test() {
  let assert Ok(authorization) = bearer.metadata("abc.DEF-123_~+/==")
  metadata.get(authorization, "authorization")
  |> should.equal(Ok("Bearer abc.DEF-123_~+/=="))
  bearer.token(authorization) |> should.equal(Ok("abc.DEF-123_~+/=="))
  let assert Error(invalid) = bearer.metadata("to ken")
  invalid.code |> should.equal(status.InvalidArgument)
  let assert Error(missing) = bearer.token(metadata.new())
  missing.code |> should.equal(status.Unauthenticated)
  missing.message |> should.equal("missing bearer token")
  let assert Error(malformed) =
    bearer.token(metadata.from_list([#("authorization", "Basic dXNlcjpwYXNz")]))
  malformed.message |> should.equal("malformed bearer token")
}

pub fn violations_test() {
  let keyed =
    violations.Violation(
      field: [
        violations.FieldPathElement(
          field_number: 3,
          field_name: "labels",
          field_type: Some(violations.TypeMessage),
          key_type: Some(violations.TypeString),
          value_type: Some(violations.UnknownType(99)),
          subscript: Some(violations.StringKey("a")),
        ),
        violations.FieldPathElement(
          field_number: 0,
          field_name: "items",
          field_type: None,
          key_type: None,
          value_type: None,
          subscript: Some(violations.Index(2)),
        ),
      ],
      rule: [],
      rule_id: "",
      message: "",
      for_key: True,
    )
  let failed = violations.status([empty_name(), keyed])
  failed.code |> should.equal(status.InvalidArgument)
  failed.message
  |> should.equal(
    "validation error: name: value length must be at least 1 characters (and 1 more)",
  )
  violations.from_status(failed) |> should.equal(Ok([empty_name(), keyed]))
  violations.to_string(keyed)
  |> should.equal("labels[\"a\"].items[2]: [unknown]")
  violations.from_status(status.new(status.InvalidArgument, "no detail"))
  |> should.equal(Error(Nil))
}

/// A server's authorizer rejects calls without the bearer token, methods
/// the server has or not alike, and its interceptors see the calls it lets
/// through and report how they ended.
pub fn authorizer_interceptors_test() {
  let #(cert, key, dir) = credentials()
  let test_inbox = inbox()
  let authorizer = fn(info: server.CallInfo) {
    case bearer.token(info.metadata) {
      Ok("s3cret") -> Ok(Nil)
      Ok(_) -> Error(status.new(status.PermissionDenied, "wrong token"))
      Error(failed) -> Error(failed)
    }
  }
  let interceptor = fn(info: server.CallInfo) {
    send(test_inbox, #("intercepted", info.method_name))
    Ok(Some(fn(finished: server.Finished) { send(test_inbox, finished) }))
  }
  let config =
    server.Config(
      ..server.config(0, cert, key, greeter_trevrpc.routes(handlers())),
      authorizer: Some(authorizer),
      interceptors: [interceptor],
    )
  let assert Ok(srv) = server.start(config)
  let assert Ok(port) = server.port(srv)
  let assert Ok(ch) =
    channel.connect(
      channel.Config(
        ..channel.config("127.0.0.1", port),
        transport: channel.Quic,
        server_name: Some("localhost"),
        ca_file: Some(cert),
      ),
      10_000,
    )
  let assert Error(missing) =
    greeter_trevrpc.say_hello(ch, hello("ada"), client.options())
  missing.code |> should.equal(status.Unauthenticated)
  let assert Ok(authorization) = bearer.metadata("s3cret")
  let options = client.Options(..client.options(), metadata: authorization)
  let assert Ok(reply) = greeter_trevrpc.say_hello(ch, hello("ada"), options)
  reply.message.message |> should.equal("hello ada")
  let assert Ok(#("intercepted", "SayHello")) = receive_within(5000)
  let assert Ok(finished) = receive_within(5000)
  let server.Finished(method:, service:, method_name:, status: ended, elapsed:) =
    finished
  method |> should.equal("/greet.v1.Greeter/SayHello")
  service |> should.equal("greet.v1.Greeter")
  method_name |> should.equal("SayHello")
  ended.code |> should.equal(status.Ok)
  { elapsed >= 0 } |> should.be_true
  // A failing handler reports its status.
  let request = HelloRequest(..hello("x"), fail: Some("no thanks"))
  let assert Error(_) = greeter_trevrpc.say_hello(ch, request, options)
  let assert Ok(#("intercepted", "SayHello")) = receive_within(5000)
  let assert Ok(server.Finished(status: failed, ..)) = receive_within(5000)
  failed.code |> should.equal(status.InvalidArgument)
  failed.message |> should.equal("no thanks")
  channel.close(ch)
  server.stop(srv)
  remove(dir)
}

/// A server's validator fails a request that breaks its rules before the
/// handler sees it.
pub fn server_validator_test() {
  let #(cert, key, dir) = credentials()
  let routes = greeter_trevrpc.routes(handlers())
  let config =
    server.Config(
      ..server.config(0, cert, key, routes),
      validator: Some(hello_validator()),
    )
  let assert Ok(srv) = server.start(config)
  let assert Ok(port) = server.port(srv)
  let assert Ok(ch) =
    channel.connect(
      channel.Config(
        ..channel.config("127.0.0.1", port),
        transport: channel.Quic,
        server_name: Some("localhost"),
        ca_file: Some(cert),
      ),
      10_000,
    )
  let assert Ok(reply) =
    greeter_trevrpc.say_hello(ch, hello("ada"), client.options())
  reply.message.message |> should.equal("hello ada")
  let assert Error(failed) =
    greeter_trevrpc.say_hello(ch, hello(""), client.options())
  failed.code |> should.equal(status.InvalidArgument)
  violations.from_status(failed) |> should.equal(Ok([empty_name()]))
  // The handler's recv returns the status, which it fails the call with.
  let assert Ok(call) = greeter_trevrpc.collect(ch, client.options())
  let assert Ok(Nil) = client.send(call, hello("a"))
  let _ = client.send(call, hello(""))
  let assert Error(failed) = client.close_and_recv(call)
  violations.from_status(failed) |> should.equal(Ok([empty_name()]))
  channel.close(ch)
  server.stop(srv)
  remove(dir)
}

/// A channel's validator fails a request that breaks its rules without
/// sending it; the server, which checks nothing, would have answered it.
pub fn client_validator_test() {
  let configure = fn(config) {
    channel.Config(..config, validator: Some(hello_validator()))
  }
  use ch <- with_configured_channel(channel.Quic, [], configure)
  let assert Error(failed) =
    greeter_trevrpc.say_hello(ch, hello(""), client.options())
  failed.code |> should.equal(status.InvalidArgument)
  violations.from_status(failed) |> should.equal(Ok([empty_name()]))
  // A rejected request is not sent, and the call goes on.
  let assert Ok(call) = greeter_trevrpc.collect(ch, client.options())
  let assert Ok(Nil) = client.send(call, hello("a"))
  let assert Error(client.Invalid(failed)) = client.send(call, hello(""))
  violations.from_status(failed) |> should.equal(Ok([empty_name()]))
  let assert Ok(Nil) = client.send(call, hello("b"))
  client.close_and_recv(call)
  |> should.equal(Ok(HelloReply(message: "a,b", index: 2)))
}
