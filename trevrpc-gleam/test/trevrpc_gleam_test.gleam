//// A Gleam server and client on loopback, through the code
//// protoc-gen-trevrpc-gleam generates from test/proto: every kind of call,
//// over native QUIC and QMux over TLS, statuses, metadata, datagrams,
//// deadlines, cancellation, and the failures the binding reports.

import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should
import greet/v1/greeter.{HelloReply, HelloRequest}
import greet/v1/greeter_trevrpc
import trevrpc/channel
import trevrpc/client
import trevrpc/handler
import trevrpc/metadata
import trevrpc/schema
import trevrpc/server
import trevrpc/status

pub fn main() {
  gleeunit.main()
}

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

const watched = "trevrpc_gleam_watched_handler"

// Erlang

@external(erlang, "trevrpc_test_ffi", "credentials")
fn credentials() -> #(String, String, String)

@external(erlang, "trevrpc_test_ffi", "remove")
fn remove(dir: String) -> Nil

@external(erlang, "trevrpc_test_ffi", "sleep")
fn sleep(milliseconds: Int) -> Nil

@external(erlang, "trevrpc_test_ffi", "register_self")
fn register_self(name: String) -> Nil

@external(erlang, "trevrpc_test_ffi", "is_alive")
fn is_alive(name: String) -> Bool

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
  let assert Ok(ch) = channel.connect(config, 10_000)
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
  let assert Error(error) = client.send(call, hello("late"))
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
  let assert Ok(srv) = server.start(server.config(0, cert, key, [route]))
  let assert Ok(_) = server.port(srv)
  server.stop(srv)
  server.port(srv) |> should.equal(Error(Nil))
  server.tcp_port(srv) |> should.equal(Error(Nil))
  remove(dir)
}
