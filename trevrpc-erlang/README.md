# trevrpc-erlang

The Erlang runtime for TrevRPC, with its typed [Gleam binding](#gleam): one Hex package, `trevrpc`, for Erlang, Gleam and Elixir projects. `gleam publish` builds it, shipping the runtime's Erlang modules, the binding's Gleam sources and the Erlang Gleam compiles them to, so rebar3 and mix build it without Gleam. Erlang projects get the binding's modules (`trevrpc@*`, `trevrpc_gleam_ffi`) and its one dependency, `gleam_stdlib`, too, which the runtime's own modules do not use.

> **Status:** early work in progress, not published. Progress is tracked in [#477](https://trev.zip/llc/TrevRPC/issues/477).

## Transports

The runtime is pure Erlang, built on [erlang_quic](https://github.com/benoitc/erlang_quic) (and its HTTP/3, `quic_h3`), [erlang-webtransport](https://github.com/benoitc/erlang-webtransport), [erlang_h2](https://github.com/benoitc/erlang_h2), [erlang_h1](https://github.com/benoitc/erlang_h1), [erlang_ws](https://github.com/benoitc/erlang_ws) and OTP's `ssl` and `zstd`. Nothing native is needed, though erlang_quic can build an optional crypto NIF that makes QUIC faster (see [Client](#client)). It needs Erlang/OTP 29 or later, whose `zstd:flush/1` stream compression needs.

| Role   | Transports                                                                                                                                                                                             |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Server | Native QUIC (ALPN `trevrpc/1`), QMux over TLS (ALPN `qmux-02.trevrpc-1`), the HTTP mapping over HTTP/3 and HTTP/2 for Cronet (opt-in) and, for browsers, WebTransport and QMux over WebSocket (opt-in) |
| Client | Native QUIC and QMux over TLS, racing QUIC against TCP when the network may block UDP                                                                                                                  |

QMux ([draft-ietf-quic-qmux-02](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/)) carries the same streams over TLS on TCP, with the same windows and limits as QUIC, and over a WebSocket ([draft-lcurley-qmux-websocket-00](https://datatracker.ietf.org/doc/draft-lcurley-qmux-websocket/)) one record per binary message. Its implementation is in-tree (`trevrpc_qmux`) and checked against the shared golden vectors in `testdata/qmux-golden-vectors.txt`.

A method whose schema marks a streaming direction unreliable (`unreliable_requests` or `unreliable_responses` in `trevrpc.method`) can send that direction's messages as [datagram messages](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#datagram-messages). Generated routes and client calls carry `unreliable => #{requests => true, responses => true}`, which hand-written ones can set too. A client gives such a call a datagram flow when its connection is native QUIC and the server takes datagrams; a browser does over WebTransport, whose session datagrams the server reads and writes like QUIC's. The call hands them to a process of its own, which packs the messages that queued while its last datagram went into as few datagrams as fit, without waiting for more, so a call that keeps up still sends each message at once; a call with 128 messages or 64 KiB queued waits for them to go, and drops the message if its deadline comes first. A message goes on the stream instead when it does not fit the largest datagram the connection takes now, or when its datagram's send fails, as when the congestion window is full: erlang_quic refuses such a datagram at once rather than queue it, so the message arrives late but arrives. A direction that sent datagrams ends with DATAGRAM_COUNT, for whose missing datagrams the receiver waits 10 ms. Datagram messages can be lost or reordered, but are never duplicated, and they count towards the same limits as the stream's. Over QMux and HTTP, every message stays on the stream. A server holds the datagrams that overtake their REQUEST (64 datagrams, 64 KiB, 500 ms per connection), and a call queues at most 128 received datagram messages, or 64 KiB, dropping the rest. `trevrpc_call:datagrams_received/1` says how many request messages arrived as datagrams, and `trevrpc_channel:datagram_stats/1` gives the connection's datagram counters.

Not implemented yet: connection metadata sets (an offer is merged into its own call and never confirmed, so clients keep offering), and stream idle timeouts.

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

Before a call is accepted, a request off the HTTP path gets `404`, another method `405`, another media type `415`, one that `http_admission` refuses `403` (and `500` if it fails), and one past `max_streams_per_connection`, or on a server shutting down, `503`. The admission is optional (every valid request is admitted without one), gets the same request map as the browser admission below, with `protocol` `<<"HTTP/3.0">>` or `<<"HTTP/2.0">>`, and runs in the connection's process, so keep it quick. After the `200`, a call has the deadlines and the server-wide limits it has over native QUIC, and its request stream is credited as its handler reads. Over HTTP/3 and HTTP/2, as over native QUIC, a stream receives one frame ahead of its handler. A client cancels a call by resetting its stream, and the server cancels with `H3_REQUEST_CANCELLED` or `CANCEL`. On shutdown, clients get a `GOAWAY` at once, and a request already on its way gets `503`. An HTTP/2 connection without a call for the connection idle timeout is closed. There is no HTTP fallback for other paths yet.

### Browsers

With `webtransport => true`, the server also serves the browser client at the HTTP path (`http_path`, `/trevrpc` by default), as the Go and Rust servers do: [WebTransport](https://trev.zip/llc/TrevRPC/wiki/WebTransport) over HTTP/3 on the UDP socket, which then also offers ALPN `h3`, through [erlang-webtransport](https://github.com/benoitc/erlang-webtransport), and its fallback, QMux over WebSocket, on the TCP listener, which then also offers `http/1.1`.

```erlang
{ok, Server} = trevrpc_server:start_link(#{
    ...,
    webtransport => true,
    webtransport_admission => trevrpc_admission:same_origin([<<"https://app.example.com">>])
}).
```

A WebSocket session needs a `GET` upgrade that offers subprotocol `qmux-02.trevrpc-1` (else `400`), and a WebTransport session a `CONNECT` that offers the WebTransport protocol `trevrpc-1`, if it offers any (else `400`); both need `webtransport_admission` to accept them (else `403`). The admission is a fun that gets the request, a map with `method`, `protocol`, `path`, `authority`, `origin` when present, `secure`, `headers` and `peer`. The default, `trevrpc_admission:no_origin/1`, refuses every request with an `Origin`, which browsers always send, so configure one before serving browsers: the `Origin` check is the defence against cross-site WebSocket hijacking. `trevrpc_admission:same_origin/1` admits like Go's `SameOriginAdmission`: requests without an `Origin`, the server's own origin, and the listed origins. Other requests get `404` off the HTTP path and `505` at it, and a full or stopping server answers `503`. WebSocket sessions count as connections and share the server's limits. An HTTP/1.1 connection counts as a TLS handshake until it upgrades, carries one request (the server closes it after any other response), and gets `tls_handshake_timeout` to send it.

An HTTP/3 connection carries one WebTransport session (a second `CONNECT` gets `503`), whose calls count against `max_streams_per_connection` as a connection's do, and Chrome's and Firefox's draft as well as Safari's and the latest drafts are accepted. Unreliable methods send their messages as session datagrams. On shutdown a session is drained (`WT_DRAIN_SESSION`), its calls get the graceful shutdown timeout, and it is closed. erlang-webtransport 0.5.13 hands every chunk of a session's streams to one process, and returns a chunk's credit to the client once that process is done with it ([#609](https://trev.zip/llc/TrevRPC/issues/609)). To hold each stream to about a window ahead of its call, as over the other transports, a session waits while one of its calls is that far behind on reading, which holds up its other calls until that one catches up, its deadline passes, or a call sends: a session is slower than a native QUIC connection when a call reads slowly. A client reset or cancel is not seen while the session waits. Each send lets the waiting stream take one more chunk, so a stream that gets four windows ahead of its call anyway (one more than a window ahead, while the session is four connection windows ahead) fails its call with `resource_exhausted`, and datagrams sent while the session waits are dropped. That differs from the other transports, and from the Go and Rust servers' WebTransport, which hold such a client back however long its call takes to read.

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

A channel reconnects on its own and follows the [connection lifecycle](https://trev.zip/llc/TrevRPC/wiki/Connection-Lifecycle) of the other runtimes: each connection is a generation, calls never move between generations, calls made while it reconnects fail at once with `unavailable`, and reconnects back off from 100 ms to 30 s with jitter. A call belongs to the process that started it; if that process exits, the channel cancels the call. A process opens its call's stream on the channel's connection itself, then registers it with the channel; one killed between the two leaves the stream unused, which over native QUIC keeps one of the connection's stream credits until it closes (a QMux server ends such a stream, and the channel cancels it). Since only that process reads and writes it, `trevrpc_client:recv/2` waits for the next response at most so many milliseconds and returns `timeout` if none arrived, leaving the call open, so that the process can also send on a schedule; the call's own deadline still ends it. `trevrpc_channel:subscribe/1` sends the calling process `{trevrpc_channel, Channel, State}` messages with the channel's state, `{ready, Generation}`, `connecting` or, last, `closed`, now and after every change, until `unsubscribe/1` or the process exits.

Channels and servers run their QUIC connections and listeners on OTP's `socket` module wherever erlang_quic finds it (Linux), where erlang_quic sends packets in GSO batches and receives them with GRO; other platforms keep erlang_quic's default, `gen_udp`, which sends every packet on its own. erlang_quic's `socket` backend sends every connection's batches of a listener through one process and does not take a server's preferred address, which TrevRPC does not use.

erlang_quic can seal packets with a crypto NIF of its own, which keeps an OpenSSL cipher context per key, instead of OTP's `crypto`, which sets the key up again for every packet. It builds the NIF only when `QUIC_BUILD_NIF=1` is set while the `quic` dependency compiles, which needs CMake, a C compiler and the OpenSSL headers. Set it for that build (`QUIC_BUILD_NIF=1 rebar3 compile` or `QUIC_BUILD_NIF=1 gleam build`, after removing an already compiled `quic`: `_build/default/lib/quic` or `build/dev/erlang/quic`), and `quic_crypto_nif:is_loaded()` says whether it loaded. An Erlang server streaming 4 KiB messages over one connection ran 15-36% faster with it. Without it, erlang_quic runs on OTP's `crypto`. The Nix packages build it.

Over native QUIC, a channel spreads its calls over a pool of connections to the server, as the Rust channel does. A QUIC connection's packets all go through one process of erlang_quic, so one connection's calls share one core, and the process slows down as many streams queue data on it; busy channels go faster over several. The pool starts with `min_quic_connections` (default 1), and grows under load up to `max_quic_connections` (default the schedulers online, from one to four): while the channel starts calls faster than `quic_connection_call_rate` (5,000 a second per connection) or moves bytes faster than `quic_connection_byte_rate` (20 MB/s per connection), each call goes to the connection with the fewest in flight, and the channel dials another connection when every one carries that rate and two calls. Streaming calls stay on the connection they start on, so streaming calls that could each make a round trip faster than the call rate allows crowd their connection too: a streaming call that finds its connection crowded waits up to `quic_crowded_dial_wait` (100 ms) for the connection the channel dials, and a burst of streams waits for as many as it needs. On a slow link many streams fit in a round trip without crowding it, so such a channel keeps one connection. Below both rates every call goes to the generation's own connection, and a connection the pool added closes once it went unused for `quic_connection_idle_timeout` (30 s). When a generation's own connection is up, the channel dials the connections of its minimum in parallel and measures the round trip, and `wait_until_ready/2` and `connect/2` return once none is being dialled any more: each joined, or failed and is dialled again later. A call with a datagram flow goes to the generation's own connection, which owns its datagram flows. A connection that is lost fails only its own calls; one of the minimum is dialled again, backing off as the channel does to reconnect, and one the pool added is not. The generation still lives as long as its own connection: its other connections then take no new calls and close after their last, which can still finish normally. Each connection counts against the server's connection limit and has its own congestion controller, which takes a larger share of a slow or lossy link than one connection would. QMux over TLS always uses one connection.

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
- Routes and calls carry each method's message types, which pick its compression dictionaries, how its `trevrpc.method` option lets its messages be compressed (never a side whose message type has a field marked `debug_redact`) and travel as datagrams, and a codec: handlers and callers get and give messages as gpb terms.
- Messages are gpb's maps, or records with `msg_format=records` (defined in the message module's `.hrl`), with strings as binaries and message names that carry their package (`'example.greeter.v1.HelloRequest'`); a oneof is `{Field, Value}` and an unset optional field is left out. A message module holds its file's messages and those they use from other files, but not the definitions behind custom options (descriptor.proto).
- Parameters (`--trevrpc-erlang_opt=name=value,...`): `msg_format` (`maps` or `records`), `module_prefix`, `pb_suffix` (`_pb`), and `service_suffix`.
- Editions files are refused.
- gpb's modules export functions without specs, and carry `-compile([nowarn_missing_spec])` for that; xref's `locals_not_used` check needs them in `xref_ignores`, as gpb leaves functions they do not use.

The generated modules need nothing but the runtime. The escript carries gpb, which is LGPL-2.1-or-later; the code gpb generates is not covered by that license.

[`protoc-gen-trevrpc-gleam`](#gleam-code-generation) builds on this plugin to generate Gleam code.

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
| `trevrpc_datagram`             | Datagram payloads, the `unreliable` option, and sending and receiving datagram messages       |
| `trevrpc_zstd`                 | Compression settings, their negotiation, and ZSTD_MESSAGE payloads                            |
| `trevrpc_zstd_stream`          | Stream compression: ZSTD_STREAM_MESSAGE payloads and the memory budget                        |
| `trevrpc_varint`, `trevrpc_pb` | QUIC varints and the protobuf wire primitives                                                 |

The rest (`trevrpc_server_conn`, `trevrpc_server_call`, `trevrpc_stream`, `trevrpc_datagram_flows`, `trevrpc_quic`, `trevrpc_transport`, `trevrpc_qmux`, `trevrpc_qmux_frame`, `trevrpc_qmux_record`, `trevrpc_tcp_listener`, `trevrpc_http`, `trevrpc_http1`, `trevrpc_webtransport`) is internal, as is `trevrpc_gleam_ffi`, the Gleam binding's calls into the runtime.

## Gleam

The Gleam binding, `src/trevrpc/*.gleam`, gives Gleam servers and clients typed calls over the runtime: native QUIC, QMux over TLS where UDP is blocked, and the HTTP mapping for Android clients. It holds no protocol logic of its own, and [`protoc-gen-trevrpc-gleam`](#gleam-code-generation) generates the messages and service code. It is part of the same `trevrpc` package, so a Gleam project adds `trevrpc` and imports `trevrpc/server`, `trevrpc/channel` and the rest.

### Gleam code generation

`protoc-gen-trevrpc-gleam` ([`protoc-gen-trevrpc-gleam/`](protoc-gen-trevrpc-gleam)) is a protoc plugin. It generates Gleam types for a proto file's messages, which gpb encodes and decodes, so Gleam needs no protobuf runtime of its own, and a module per service over the binding. The `trevrpc-erlang` Nix package provides it, or build it with `rebar3 escriptize` in its directory (the escript is `_build/default/bin/protoc-gen-trevrpc-gleam`). It builds on [protoc-gen-trevrpc-erlang](#code-generation): it carries that plugin's modules, which make gpb's modules, check methods and read their options, and the plugin protocol (`trevrpc_gen_plugin`), and takes them from `_checkouts`, a link to it, so changes there reach it too; its tests share `test/support` and `test/proto`. The escript carries gpb, which is LGPL-2.1-or-later. Gleam's custom types compile to tagged tuples (`Point(x: 1, y: 2)` is `{point, 1, 2}`), which are gpb's records once they take the Gleam constructors' names. Generate into a Gleam project's `src/`, with every proto file the ones you generate import, `google/protobuf/*.proto` included:

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

### Gleam server

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

A unary or client-streaming handler returns `Ok(response)`, a streaming one `Ok(Nil)`, and any of them `Error(status.new(status.NotFound, "missing"))` to end the call with that status. `trevrpc/handler` reads requests (`handler.recv`, `None` once the client sent them all), sends responses, headers and trailers, and gives the request metadata. Each call runs in a process of its own, which the server stops when the call's deadline passes, when the client cancels it, and on shutdown. A request that does not decode fails the call with `InvalidArgument` before the handler runs. `server.start` links the server to the calling process, so it stops when that process exits, and returns an error for credentials it cannot read, a port in use, or two routes for one path; `server.port` and `server.tcp_port` are errors once it has stopped, and `server.stop` does nothing then. `server.Config` takes the runtime's main options: the TCP listener for QMux, `http` for the Android client's HTTP/3 and HTTP/2, `webtransport` for browsers, with `webtransport_origins` the pages besides the server's own that may call it (see [Browsers](#browsers)), and the limits.

### Gleam client

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

A channel reconnects on its own; its calls never move between connections. `channel.connect` links the channel to the calling process, which closes it by exiting. A call belongs to the process that started it, and is finished once it ends (`End` or an error) or is cancelled: reading or writing it then, or from another process, is an error with code `FailedPrecondition`. A response that does not decode ends the call with `Internal`, and a call past its timeout with `DeadlineExceeded`. `client.recv_within(call, within: milliseconds)` reads as `client.recv` does but returns `Ok(None)` if nothing arrived in time, leaving the call open. `client.Options` sets the call's timeout and request metadata. Methods that their schema marks unreliable send their messages as datagrams over native QUIC, and on the stream elsewhere.

### Gleam modules

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

rebar3 and Erlang/OTP 29 are in the repository's dev shell; the Gleam binding also needs `gleam` (`nix shell nixpkgs#gleam`) and `protoc`.

```sh
rebar3 eunit      # unit tests, the shared golden vectors in ../testdata, client/server over QUIC and QMux, datagram messages, QMux over WebSocket, and the HTTP mapping
rebar3 xref
rebar3 dialyzer
```

`nix build .#trevrpc-erlang` runs all three. `nix fmt` formats the sources with erlfmt, and the Gleam sources with `gleam format`. The same commands work in `protoc-gen-trevrpc-erlang/` and `protoc-gen-trevrpc-gleam/`, whose tests also need `protoc`, and `nix build .#trevrpc-erlang` runs them too, as it builds the plugins it provides. The interop executable's bindings, in `interop/src/generated`, are regenerated with `interop/generate.sh`, and a check fails when they differ from what the plugin generates.

```sh
./gleam-test.sh   # gleam-generate.sh, which generates the Gleam tests' service with protoc-gen-trevrpc-gleam, then gleam test
```

The Gleam tests (`test/trevrpc_test.gleam`) run a Gleam server and client on loopback. `gleam test` builds the plugin in `protoc-gen-trevrpc-gleam/` unless `PROTOC_GEN_TREVRPC_GLEAM` names it, and runs only that module, as rebar3 runs the Erlang tests. The `trevrpc-erlang-gleam` check (`nix build .#trevrpc-erlang.gleam`, from `gleam.nix`) builds the package as `gleam publish` does and runs them offline, and `trevrpc-erlang-protoc-gen-gleam` runs the Gleam plugin's fixture.

rebar3 builds the application from `rebar.config` and `src/trevrpc.app.src`, Gleam from `gleam.toml`, which `gleam publish` turns into the Hex package's metadata and `.app` file. The Hex dependencies are declared in `gleam.toml` and locked in `manifest.toml`, which Renovate's Gleam manager updates; no Renovate manager reads rebar3's files. `nix run .#sync-erlang-deps` (`erlang_deps.py`) copies them into `rebar.config` (the ranges, verbatim, as rebar3 reads Hex's syntax), `rebar.lock` (the locked versions and their checksums, the inner ones from the packages' tarballs), the interop executable's `rebar.lock`, and `protoc-gen-trevrpc-gleam`'s `gpb`, which follows `protoc-gen-trevrpc-erlang`'s. Renovate runs it after updating the Gleam files; after changing a dependency by hand, change `gleam.toml`, run `gleam update` (or `gleam deps download`), then the sync. The Nix package reads versions and tarball hashes from `rebar.lock`, and the `trevrpc-erlang-deps` check fails when rebar3's files drift from the Gleam ones. `python3 -I -m unittest discover -s trevrpc-erlang`, from the repository's root, runs the sync's tests, as the `python-test` check does. `gleam_stdlib` is the one dependency only `gleam.toml` has, and adding or removing one takes both `gleam.toml` and `rebar.config`.
