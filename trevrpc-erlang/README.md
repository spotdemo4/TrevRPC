# trevrpc-erlang

The Erlang runtime for TrevRPC. It is the core that a Gleam package (`trevrpc-gleam`) will wrap with a typed API and generated stubs, and Elixir can use it directly.

> **Status:** early work in progress, not published. Progress is tracked in [#477](https://trev.zip/llc/TrevRPC/issues/477).

## Transports

The runtime is pure Erlang, built on [erlang_quic](https://github.com/benoitc/erlang_quic) (and its HTTP/3, `quic_h3`), [erlang_h2](https://github.com/benoitc/erlang_h2), [erlang_h1](https://github.com/benoitc/erlang_h1), [erlang_ws](https://github.com/benoitc/erlang_ws) and OTP's `ssl` and `zstd`, so nothing native is built. It needs Erlang/OTP 29 or later, whose `zstd:flush/1` stream compression needs.

| Role   | Today                                                                                                                                                                                 | Planned      |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| Server | Native QUIC (ALPN `trevrpc/1`), QMux over TLS (ALPN `qmux-02.trevrpc-1`), the HTTP mapping over HTTP/3 and HTTP/2 for Cronet (opt-in) and, for browsers, QMux over WebSocket (opt-in) | WebTransport |
| Client | Native QUIC and QMux over TLS, racing QUIC against TCP when the network may block UDP                                                                                                 | -            |

QMux ([draft-ietf-quic-qmux-02](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/)) carries the same streams over TLS on TCP, with the same windows and limits as QUIC, and over a WebSocket ([draft-lcurley-qmux-websocket-00](https://datatracker.ietf.org/doc/draft-lcurley-qmux-websocket/)) one record per binary message. Its implementation is in-tree (`trevrpc_qmux`) and checked against the shared golden vectors in `testdata/qmux-golden-vectors.txt`.

Known issue: when a client on erlang_quic has finished an upload (`close_send`) while most of it is still queued, and the server answers early, erlang_quic can answer the server's STOP_SENDING with a RESET_STREAM whose final size contradicts the FIN it already sent. An erlang_quic server, such as this runtime's, then closes the connection with FINAL_SIZE_ERROR; quic-go does not check it. The Go and Rust clients are not affected. See [benoitc/erlang_quic#551](https://github.com/benoitc/erlang_quic/issues/551).

Readers accept DATAGRAM_COUNT (frame type 7) only where the protocol allows it, right before the end of a request stream or right before STATUS on a response stream, with a payload of exactly one varint. Without datagram messages the runtime sends no count and waits for none.

Not implemented yet: datagram messages, connection metadata sets (an offer is merged into its own call and never confirmed, so clients keep offering), and stream idle timeouts.

## Server

```erlang
Routes = #{
    <<"/example.greeter.Greeter/SayHello">> => #{
        kind => unary,
        handler => fun(Request, _Call) -> {ok, Request} end
    },
    <<"/example.greeter.Greeter/Chat">> => #{
        kind => bidirectional_streaming,
        handler => fun chat/1
    }
},
{ok, Server} = trevrpc_server:start_link(#{
    port => 4433, certfile => "cert.pem", keyfile => "key.pem", routes => Routes
}).

chat(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, Message} ->
            ok = trevrpc_call:send(Call, Message),
            chat(Call);
        eof ->
            ok;
        {error, Status} ->
            {error, Status}
    end.
```

The runtime depends on no protobuf library: messages are binaries, unless the route has a `codec` (`#{encode := fun((Message) -> iodata()), decode := fun((binary()) -> Message)}`, see `trevrpc_codec`), with which handlers get and give messages as terms. The code [protoc-gen-trevrpc-erlang](#code-generation) generates sets one on every route and call; Elixir users can pass protox's encoders and decoders. A request the codec cannot decode ends the call with `invalid_argument` (`trevrpc_call:recv/1` returns that status to a streaming handler), and a response it cannot encode ends it with `internal`, which the client sees as an internal server error (`trevrpc_call:send/2` returns that status to the handler). A codec that cannot encode is logged as an error, as it is a bug of this side, and one that cannot decode at debug level, as it is more likely a bad peer; the log has the exception's class, its reason's tag and where it was raised, never the message.

Unary and server-streaming handlers take `(Request, Call)`, client-streaming and bidirectional ones `(Call)`. They return `{ok, Response}` (single responses), `ok` (streamed responses) or `{error, Status}`, where a status is a map such as `trevrpc_status:new(not_found, <<"missing">>)`, optionally with `metadata` and `details`. `trevrpc_call` reads requests, sends responses, headers and trailers, and gives the call's metadata and deadline.

The server also listens on TCP for QMux over TLS, by default on the same port number as QUIC, or on any free port if that one is taken: `tcp_port => P` picks it, `tcp => false` turns TCP off, and `trevrpc_server:tcp_port/1` returns it. Connections over either transport share the server's limits. A TLS client has `tls_handshake_timeout` (10 s) for its TLS handshake and again for its QMux parameters, at most `max_tls_handshakes` (256) handshakes run at once, and a QMux client that stops reading is dropped after the connection idle timeout.

### Android (HTTP/3 and HTTP/2)

With `http => true`, the server also serves the [HTTP mapping](https://trev.zip/llc/TrevRPC/wiki/HTTP3) that the Kotlin Cronet client uses: each call is a `POST` to the HTTP path (`http_path`, `/trevrpc` by default) with media type `application/trevrpc`, whose request and response bodies carry the usual frames. The UDP socket then also offers ALPN `h3` and the TCP listener `h2`, as the Go and Rust servers do, and HTTP/2 responses advertise HTTP/3 with `Alt-Svc`.

```erlang
{ok, Server} = trevrpc_server:start_link(#{
    ...,
    http => true,
    http_admission => fun(#{authority := Authority}) -> Authority =:= <<"rpc.example.com">> end
}).
```

Before a call is accepted, a request off the HTTP path gets `404`, another method `405`, another media type `415`, one that `http_admission` refuses `403` (and `500` if it fails), and one past `max_streams_per_connection`, or on a server shutting down, `503`. The admission is optional (every valid request is admitted without one), gets the same request map as the browser admission below, with `protocol` `<<"HTTP/3.0">>` or `<<"HTTP/2.0">>`, and runs in the connection's process, so keep it quick. After the `200`, a call has the deadlines and the server-wide limits it has over native QUIC, and its request stream is credited as its handler reads. Over HTTP/2 a stream receives one frame ahead of its handler. Over HTTP/3, quic_h3 delivers a request's headers before the stream can be put in manual flow control, and erlang_quic credits that first delivery with a whole connection window, so one stream can receive up to the connection window (16 MiB by default) ahead, a weaker bound than native QUIC's one frame per stream; what a connection holds unread stays within that window. Upstream: [benoitc/erlang_quic#554](https://github.com/benoitc/erlang_quic/issues/554). A client cancels a call by resetting its stream, and the server cancels with `H3_REQUEST_CANCELLED` or `CANCEL`. On shutdown, HTTP/3 clients get a `GOAWAY` at once; HTTP/2 clients get `503` for new requests and the `GOAWAY` as the connection closes, because erlang_h2 closes a connection 100 ms after its `GOAWAY` whatever still runs ([benoitc/erlang_h2#41](https://github.com/benoitc/erlang_h2/issues/41)). An HTTP/2 connection without a call for the connection idle timeout is closed. There is no HTTP fallback for other paths yet.

### Browsers

With `webtransport => true`, the TCP listener also offers `http/1.1` and serves the browser client's fallback, QMux over WebSocket, at the HTTP path (`http_path`, `/trevrpc` by default), as the Go and Rust servers do. WebTransport itself is not served yet, so browsers reach an Erlang server over WebSocket only (`transports: ["websocket"]`, or `auto`, which falls back to it).

```erlang
{ok, Server} = trevrpc_server:start_link(#{
    ...,
    webtransport => true,
    webtransport_admission => trevrpc_admission:same_origin([<<"https://app.example.com">>])
}).
```

A session needs a `GET` upgrade that offers subprotocol `qmux-02.trevrpc-1` (else `400`) and that `webtransport_admission` accepts (else `403`). The admission is a fun that gets the request, a map with `method`, `protocol`, `path`, `authority`, `origin` when present, `secure`, `headers` and `peer`. The default, `trevrpc_admission:no_origin/1`, refuses every request with an `Origin`, which browsers always send, so configure one before serving browsers: the `Origin` check is the defence against cross-site WebSocket hijacking. `trevrpc_admission:same_origin/1` admits like Go's `SameOriginAdmission`: requests without an `Origin`, the server's own origin, and the listed origins. Other requests get `404` off the HTTP path and `505` at it, and a full or stopping server answers `503`. WebSocket sessions count as connections and share the server's limits. An HTTP/1.1 connection counts as a TLS handshake until it upgrades, carries one request (the server closes it after any other response), and gets `tls_handshake_timeout` to send it.

Each call runs in a process of its own, which the server stops when the call's deadline passes, when the client cancels it, and on shutdown, whether or not the handler cooperates. A handler that crashes, or returns an `internal` status, ends its call with `internal server error`; the cause goes to the logger. The server's limits and their defaults match the Go and Rust servers: 4 MiB frames, 256 connections, 64 concurrent streams per connection, 1024 concurrent requests, a 10 s initial request timeout, a 30 s connection idle timeout and a 30 s graceful shutdown.

## Client

```erlang
{ok, Channel} = trevrpc_channel:connect(#{host => "localhost", port => 4433, cacertfile => "ca.pem"}, 5000),
{ok, Reply, #{headers := Headers, trailers := Trailers}} =
    trevrpc_client:unary(Channel, <<"/example.greeter.Greeter/SayHello">>, Request, #{timeout => 1000}),

{ok, Call} = trevrpc_client:bidi(Channel, <<"/example.greeter.Greeter/Chat">>, #{}),
ok = trevrpc_client:send(Call, Request),
{ok, Response} = trevrpc_client:recv(Call),
ok = trevrpc_client:close_send(Call),
{eof, Trailers} = trevrpc_client:recv(Call).
```

Messages are binaries here too, unless the call's options have a `codec`, which encodes requests and decodes responses. A request it cannot encode and a response it cannot decode end the call with an `internal` status, the first before anything is sent when it is the call's only request.

`transport` picks how a channel connects: `quic`, `tcp` (QMux over TLS) or `auto`, the default, which starts QUIC and, if it has not connected after `fallback_delay` (300 ms) or fails before, races QMux over TLS against it; the first to connect wins. Once QMux won, reconnects go straight to it for `reprobe_interval` (5 minutes) before trying QUIC again. `tcp_host` and `tcp_port` address the server's TCP listener, by default the QUIC host and port. `trevrpc_channel:transport/1` says which transport the current connection uses. A certificate in `cacerts` or `cacertfile` may be the server's own self-signed one.

A channel reconnects on its own and follows the [connection lifecycle](https://trev.zip/llc/TrevRPC/wiki/Connection-Lifecycle) of the other runtimes: each connection is a generation, calls never move between generations, calls made while it reconnects fail at once with `unavailable`, and reconnects back off from 100 ms to 30 s with jitter. A call belongs to the process that started it; if that process exits, the channel cancels the call. `trevrpc_channel:subscribe/1` sends the calling process `{trevrpc_channel, Channel, State}` messages with the channel's state, `{ready, Generation}`, `connecting` or, last, `closed`, now and after every change, until `unsubscribe/1` or the process exits.

## Code generation

`protoc-gen-trevrpc-erlang` ([`protoc-gen-trevrpc-erlang/`](protoc-gen-trevrpc-erlang)) is a protoc plugin. For every proto file it generates a module that encodes and decodes the file's messages, made by [gpb](https://github.com/tomas-abrahamsson/gpb), and for every service a module with its behaviour, its routes and a client function per method. The `trevrpc-erlang` Nix package provides it, as the other runtimes' packages provide their plugins, or build it with `rebar3 escriptize` in its directory (the escript is `_build/default/bin/protoc-gen-trevrpc-erlang`). Run it with protoc or buf, with the repository's `proto/` on the include path for `trevrpc/options.proto`:

```sh
protoc --plugin=protoc-gen-trevrpc-erlang=path/to/protoc-gen-trevrpc-erlang \
    -I proto -I path/to/TrevRPC/proto --trevrpc-erlang_out=src/generated example/greeter/v1/greeter.proto
```

For service `example.greeter.v1.Greeter` in `greeter.proto`, it writes `greeter_pb.erl` and `example_greeter_v1_greeter.erl`:

```erlang
-module(greeter).
-behaviour(example_greeter_v1_greeter).
-export([say_hello/2, chat/1]).

say_hello(#{name := Name}, _Call) ->
    {ok, #{message => <<"hello ", Name/binary>>}}.

chat(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, #{name := Name}} ->
            ok = trevrpc_call:send(Call, #{message => <<"hello ", Name/binary>>}),
            chat(Call);
        eof ->
            ok;
        {error, Status} ->
            {error, Status}
    end.
```

```erlang
{ok, Server} = trevrpc_server:start_link(#{..., routes => example_greeter_v1_greeter:routes(greeter)}),

{ok, #{message := Message}, _} = example_greeter_v1_greeter:say_hello(Channel, #{name => <<"you">>}),
{ok, Call} = example_greeter_v1_greeter:chat(Channel, #{timeout => 5000}),
ok = trevrpc_client:send(Call, #{name => <<"you">>}),
{ok, #{message := Reply}} = trevrpc_client:recv(Call).
```

- A method is a callback and a client function named in snake case (`SayHello` is `say_hello`). Client functions take the channel, then the request unless the client streams, then, optionally, `trevrpc_client` call options, which win over those of the schema.
- `routes/1` takes a module that implements the behaviour, or a map of handler funs by method name; a method left out of the map is not routed, and a name that is no method fails with `{unknown_method, Name, Methods}`. Several services' routes merge with `maps:merge/2`.
- Routes and calls carry each method's message types, which pick its compression dictionaries, how its `trevrpc.method` option lets its messages be compressed (never a side whose message type has a field marked `debug_redact`), and a codec: handlers and callers get and give messages as gpb terms.
- Messages are gpb's maps, or records with `msg_format=records` (defined in the message module's `.hrl`), with strings as binaries and message names that carry their package (`'example.greeter.v1.HelloRequest'`); a oneof is `{Field, Value}` and an unset optional field is left out. A message module holds its file's messages and those they use from other files, but not the definitions behind custom options (descriptor.proto).
- Parameters (`--trevrpc-erlang_opt=name=value,...`): `msg_format` (`maps` or `records`), `module_prefix`, `pb_suffix` (`_pb`), and `service_suffix`.
- Editions files are refused, and `unreliable_requests` and `unreliable_responses` are checked but do nothing until the runtime has datagram messages.
- gpb's modules export functions without specs, and carry `-compile([nowarn_missing_spec])` for that; xref's `locals_not_used` check needs them in `xref_ignores`, as gpb leaves functions they do not use.

The generated modules need nothing but the runtime. The escript carries gpb, which is LGPL-2.1-or-later; the code gpb generates is not covered by that license.

## Compression

Messages can travel compressed with zstd and a dictionary trained on your own messages, negotiated per connection as in the other runtimes (see the wiki's [Compression](https://trev.zip/llc/TrevRPC/wiki/Compression) guide). Servers and channels take the same settings:

```erlang
{ok, Dictionary} = file:read_file("metrics.dict"),
Compression = #{
    dictionaries => [#{data => Dictionary, messages => [<<"example.v1.MetricSample">>]}],
    level => 1
},
{ok, Server} = trevrpc_server:start_link(#{..., compression => Compression}),
{ok, Channel} = trevrpc_channel:connect(#{..., compression => Compression}, 5000).
```

Messages are opaque binaries, so the runtime learns their protobuf types from the route (`request_type`, `response_type`) and from the call options of the same names, and picks each direction's dictionary from them. `compression => message` (the schema's `COMPRESSION_MESSAGE`) on a route or in a call's options also lets messages of 256 bytes or more travel compressed without a dictionary where none applies; `#{requests => message, responses => none}` sets the directions apart, for a type with a field marked `debug_redact`. Without settings, servers and channels still compress without a dictionary where methods allow it.

```erlang
Routes = #{
    <<"/example.v1.Metrics/Get">> => #{
        kind => unary,
        handler => fun get/2,
        request_type => <<"example.v1.GetRequest">>,
        response_type => <<"example.v1.MetricSample">>,
        compression => message
    }
},
trevrpc_client:unary(Channel, <<"/example.v1.Metrics/Get">>, Request, #{
    request_type => <<"example.v1.GetRequest">>,
    response_type => <<"example.v1.MetricSample">>,
    compression => message
}).
```

A message travels compressed only when that makes it smaller. A channel's first call on a connection asks which dictionaries the server shares, so its request messages go uncompressed until the answer arrives; a request stream starts compressing as soon as it has. Decompressed sizes count towards the frame size and body limits. `trevrpc_call:compressed_received/1` tells a handler how many of its requests arrived compressed.

`compression => stream` (the schema's `COMPRESSION_STREAM`) also lets each direction a method streams compress with one zstd context for the whole call, from its third message, so a stream of similar messages compresses against its earlier ones; see the wiki's [Stream Compression](https://trev.zip/llc/TrevRPC/wiki/Compression#stream-compression). It needs no settings. Channels compress requests optimistically, before the server answers: a call keeps a copy of up to 1 MiB of its requests until then, and a server that does not decode them refuses the call before its handler runs, which the channel replays without stream compression, transparently. After a refusal, the connection's calls wait for the answer until one is answered again. Contexts take about 0.6 MiB to compress and 0.35 MiB to decompress for as long as the call lasts, within a budget (`stream_compression_memory`: 256 MiB per server, 64 MiB per channel, 0 turns stream compression off); calls beyond it compress per message.

The codecs, `trevrpc_zstd` and `trevrpc_zstd_stream`, are checked against the shared golden vectors in `testdata/zstd-golden-vectors.txt`, and write the bytes they hold.

## Modules

| Module                         | Purpose                                                                                       |
| ------------------------------ | --------------------------------------------------------------------------------------------- |
| `trevrpc_server`               | The server: listener, connection limit, graceful shutdown                                     |
| `trevrpc_admission`            | Admission of browser sessions and HTTP requests                                               |
| `trevrpc_call`                 | The handler API                                                                               |
| `trevrpc_channel`              | The client channel: reconnects, generations, stream credit                                    |
| `trevrpc_client`               | Client calls                                                                                  |
| `trevrpc_codec`                | The codecs of routes and calls                                                                |
| `trevrpc_status`               | Status codes and statuses                                                                     |
| `trevrpc_metadata`             | Metadata key rules and size limits                                                            |
| `trevrpc`                      | Protocol identifiers and the default frame size limit                                         |
| `trevrpc_frame`                | Frame encoding, and an incremental reader with the frame size and cumulative byte limits      |
| `trevrpc_wire`                 | The control messages `RpcRequest`, `RpcHeaders`, `RpcAnswer` and `RpcStatus`, encoded by hand |
| `trevrpc_datagram`             | Datagram payloads                                                                             |
| `trevrpc_zstd`                 | Compression settings, their negotiation, and ZSTD_MESSAGE payloads                            |
| `trevrpc_zstd_stream`          | Stream compression: ZSTD_STREAM_MESSAGE payloads and the memory budget                        |
| `trevrpc_varint`, `trevrpc_pb` | QUIC varints and the protobuf wire primitives                                                 |

The rest (`trevrpc_server_conn`, `trevrpc_server_call`, `trevrpc_stream`, `trevrpc_quic`, `trevrpc_transport`, `trevrpc_qmux`, `trevrpc_qmux_frame`, `trevrpc_qmux_record`, `trevrpc_tcp_listener`, `trevrpc_http`, `trevrpc_http1`) is internal.

## Development

rebar3 and Erlang/OTP 29 are in the repository's dev shell.

```sh
rebar3 eunit      # unit tests, the shared golden vectors in ../testdata, client/server over QUIC and QMux, QMux over WebSocket, and the HTTP mapping
rebar3 xref
rebar3 dialyzer
```

`nix build .#trevrpc-erlang` runs all three. `nix fmt` formats the sources with erlfmt. The same commands work in `protoc-gen-trevrpc-erlang/`, whose tests also need `protoc`, and `nix build .#trevrpc-erlang` runs them too, as it builds the plugin it provides. The interop executable's bindings, in `interop/src/generated`, are regenerated with `interop/generate.sh`, and a check fails when they differ from what the plugin generates.

The Hex dependencies are declared in `rebar.config` and pinned in `rebar.lock`, which is the one place their versions and hashes live: after changing a dependency, update the lock with `rebar3 upgrade`, and the Nix package reads versions and tarball hashes from it.
