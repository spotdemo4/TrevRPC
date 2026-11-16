# trevrpc-gleam

A typed Gleam binding for [`trevrpc-erlang`](../trevrpc-erlang), the Erlang runtime of TrevRPC: Gleam servers and clients over native QUIC, QMux over TLS where UDP is blocked, and the HTTP mapping for Android clients. It holds no protocol logic of its own; the runtime does the work, and [`protoc-gen-trevrpc-gleam`](#code-generation) generates the messages and service code.

Not published yet ([#477](https://trev.zip/llc/TrevRPC/issues/477)). In this repository it depends on the runtime by path, which `trevrpc-erlang/gleam.toml` lets Gleam build from its sources; published, it will depend on the runtime's Hex package.

## Code generation

`protoc-gen-trevrpc-gleam` ([`protoc-gen-trevrpc-gleam/`](protoc-gen-trevrpc-gleam)) is a protoc plugin. It generates Gleam types for a proto file's messages, which gpb encodes and decodes, so Gleam needs no protobuf runtime of its own, and a module per service over this binding. The `trevrpc-gleam` Nix package provides it, or build it with `rebar3 escriptize` in its directory (the escript is `_build/default/bin/protoc-gen-trevrpc-gleam`). It builds on [protoc-gen-trevrpc-erlang](../trevrpc-erlang#code-generation), whose modules it carries and takes from `_checkouts`, a link to it; the escript carries gpb, which is LGPL-2.1-or-later. Gleam's custom types compile to tagged tuples (`Point(x: 1, y: 2)` is `{point, 1, 2}`), which are gpb's records once they take the Gleam constructors' names. Generate into a Gleam project's `src/`, with every proto file the ones you generate import, `google/protobuf/*.proto` included:

```sh
protoc --plugin=protoc-gen-trevrpc-gleam=path/to/protoc-gen-trevrpc-gleam \
    -I proto -I path/to/TrevRPC/proto --trevrpc-gleam_out=src example/shapes/v1/shapes.proto
```

For `example/shapes/v1/shapes.proto` it writes the module `example/shapes/v1/shapes.gleam`, and beside it gpb's module `example@shapes@v1@shapes_pb.erl` (with its `.hrl`) and `example@shapes@v1@shapes_shim.erl`, which converts the fields whose representations differ:

```gleam
import example/shapes/v1/shapes.{Corner, Shape}
import gleam/option.{Some}

let shape = Shape(..shapes.new_shape(), name: "square", kind: Some(Corner(shapes.Point(1, 2))))
let bytes = shapes.encode_shape(shape)
let assert Ok(decoded) = shapes.decode_shape(bytes)
```

- A message is a type with one constructor, its fields labelled with their names in snake case (a Gleam keyword gets an underscore after it, `type_`). Nested types take their parents' names, `Shape.Style` is `ShapeStyle`.
- proto3 `optional` fields, message fields and oneofs are `Option`s, so a field set to its default stays apart from an unset one. A repeated field is a `List`, a map a `Dict`, bytes a `BitArray`, and every integer an `Int`.
- An enum is a type with a constructor per value (`COLOR_RED` is `ColorRed`, prefixed with the enum's name where the value is not), and `ColorUnrecognized(Int)` for a value the schema does not know.
- A oneof is a type with a constructor per field, named after the field (`Corner(Point)`), or after the oneof and the field (`ShapeKindCorner`) where the short name is taken.
- `new_*` gives a message with every field at its default, as Gleam constructors list every field. `encode_*` and `decode_*` use the wire format; `decode_*` returns `Error(Nil)` for malformed data, for a string that is not UTF-8, and for a float that is infinite or NaN, which a Gleam `Float` cannot hold. `encode_*` panics on a `BitArray` field that does not hold whole bytes.
- Unknown fields are dropped. Groups and editions files are refused, as are names Gleam cannot take or tell apart: module paths that are not lower-case, types or constructors that would collide or hide Gleam's own (`Result`, `Ok`), and two messages one file uses that would share a constructor name in gpb's module (`a.v1.Point` and `b.v1.Point`).
- A message whose fields all match gpb's representation goes to gpb untouched; the shim converts the others, and checks their strings, in a pass over the message on each encode or decode.
- Each service gets a module beside its file's, `greet/v1/greeter_trevrpc.gleam` for a service `Greeter`: a `Handlers` record with a function per method, `routes(handlers)`, a client function per method, and `say_hello_method()` and the like, each method's `schema.Method`, with the compression and datagram options its `trevrpc.method` option allows.

`protoc-gen-trevrpc-gleam/test/gleam` is a Gleam project that runs the generated code: `test/gleam/test.sh` generates it (`generate.sh`), runs its tests, and checks with protoc that its bytes decode as expected (`compare.sh`).

## Server

```gleam
import greet/v1/greeter.{HelloReply}
import greet/v1/greeter_trevrpc
import trevrpc/server
import trevrpc/status

pub fn main() {
  let routes =
    greeter_trevrpc.routes(greeter_trevrpc.Handlers(
      say_hello: fn(request, _call) {
        Ok(HelloReply(..greeter.new_hello_reply(), message: "hello " <> request.name))
      },
      chat: chat,
    ))
  let assert Ok(srv) = server.start(server.config(4433, "cert.pem", "key.pem", routes))
}
```

A unary or client-streaming handler returns `Ok(response)`, a streaming one `Ok(Nil)`, and any of them `Error(status.new(status.NotFound, "missing"))` to end the call with that status. `trevrpc/handler` reads requests (`handler.recv`, `None` once the client sent them all), sends responses, headers and trailers, and gives the request metadata. Each call runs in a process of its own, which the server stops when the call's deadline passes, when the client cancels it, and on shutdown. A request that does not decode fails the call with `InvalidArgument` before the handler runs. `server.start` links the server to the calling process, so it stops when that process exits, and returns an error for credentials it cannot read, a port in use, or two routes for one path; `server.port` and `server.tcp_port` are errors once it has stopped. `server.Config` takes the runtime's main options: the TCP listener for QMux, `http` for the Android client's HTTP/3 and HTTP/2, and the limits.

## Client

```gleam
import gleam/option.{Some}
import greet/v1/greeter.{HelloRequest}
import greet/v1/greeter_trevrpc
import trevrpc/channel
import trevrpc/client

pub fn main() {
  let config = channel.Config(..channel.config("localhost", 4433), ca_file: Some("ca.pem"))
  let assert Ok(ch) = channel.connect(config, 5000)
  let assert Ok(reply) =
    greeter_trevrpc.say_hello(ch, HelloRequest(..greeter.new_hello_request(), name: "gleam"), client.options())

  let assert Ok(call) = greeter_trevrpc.chat(ch, client.options())
  let assert Ok(Nil) = client.send(call, greeter.new_hello_request())
  let assert Ok(client.Message(response)) = client.recv(call)
  let assert Ok(Nil) = client.close_send(call)
  let assert Ok(client.End(trailers)) = client.recv(call)
}
```

A channel reconnects on its own; its calls never move between connections. `channel.connect` links the channel to the calling process, which closes it by exiting. A call belongs to the process that started it, and is finished once it ends (`End` or an error) or is cancelled: reading or writing it then, or from another process, is an error with code `FailedPrecondition`. A response that does not decode ends the call with `Internal`, and a call past its timeout with `DeadlineExceeded`. `client.Options` sets the call's timeout and request metadata. Methods that their schema marks unreliable send their messages as datagrams over native QUIC, and on the stream elsewhere.

## Modules

| Module             | Purpose                                                                           |
| ------------------ | --------------------------------------------------------------------------------- |
| `trevrpc/channel`  | Channels: connecting, transports, reconnects                                      |
| `trevrpc/client`   | Calls on a channel                                                                |
| `trevrpc/server`   | Servers and routes                                                                |
| `trevrpc/handler`  | A server call, as its handler sees it                                             |
| `trevrpc/schema`   | Methods: their paths, codecs and schema options, as generated code describes them |
| `trevrpc/status`   | Status codes and statuses                                                         |
| `trevrpc/metadata` | Request and response metadata                                                     |

## Development

```sh
./test.sh   # generate.sh, which generates the test service with protoc-gen-trevrpc-gleam, then gleam test
```

It needs `gleam`, `rebar3`, Erlang/OTP 29 and `protoc` (`nix shell nixpkgs#gleam`, in the repository's dev shell for the rest), and builds the plugin in `protoc-gen-trevrpc-gleam/` unless `PROTOC_GEN_TREVRPC_GLEAM` names it. In that directory, `rebar3 eunit`, `rebar3 xref` and `rebar3 dialyzer` check the plugin itself, with `protoc` on the PATH. `nix build .#trevrpc-gleam` builds the library with trevpkgs' `mkGleamPackage`, on Erlang/OTP 29, and runs the same tests offline; the `trevrpc-gleam` check is that package, and `trevrpc-gleam-protoc-gen` runs the plugin's Gleam fixture.
