# trevrpc-erlang

The Erlang runtime for TrevRPC. It is the core that a Gleam package (`trevrpc-gleam`) will wrap with a typed API and generated stubs, and Elixir can use it directly.

> **Status:** early work in progress, not published. Progress is tracked in [#477](https://trev.zip/llc/TrevRPC/issues/477).

## Transports

The runtime is pure Erlang, built on [erlang_quic](https://github.com/benoitc/erlang_quic) and OTP's `ssl` and `zstd`, so nothing native is built. It needs Erlang/OTP 29 or later, whose `zstd:flush/1` stream compression needs.

| Role   | Today                                                                                 | Planned                                                 |
| ------ | ------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| Server | Native QUIC (ALPN `trevrpc/1`) and QMux over TLS (ALPN `qmux-02.trevrpc-1`)           | HTTP/3 and HTTP/2, WebTransport and QMux over WebSocket |
| Client | Native QUIC and QMux over TLS, racing QUIC against TCP when the network may block UDP | -                                                       |

QMux ([draft-ietf-quic-qmux-02](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/)) carries the same streams over TLS on TCP, with the same windows and limits as QUIC. Its implementation is in-tree (`trevrpc_qmux`) and checked against the shared golden vectors in `testdata/qmux-golden-vectors.txt`.

Known issue: when a client on erlang_quic has finished an upload (`close_send`) while most of it is still queued, and the server answers early, erlang_quic can answer the server's STOP_SENDING with a RESET_STREAM whose final size contradicts the FIN it already sent. An erlang_quic server, such as this runtime's, then closes the connection with FINAL_SIZE_ERROR; quic-go does not check it. The Go and Rust clients are not affected. See [benoitc/erlang_quic#551](https://github.com/benoitc/erlang_quic/issues/551).

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

Messages are opaque binaries: handlers decode requests and encode responses, so the runtime depends on no protobuf library (Erlang users can pick gpb, Elixir users protox, and the Gleam package its own generator).

Unary and server-streaming handlers take `(Request, Call)`, client-streaming and bidirectional ones `(Call)`. They return `{ok, Response}` (single responses), `ok` (streamed responses) or `{error, Status}`, where a status is a map such as `trevrpc_status:new(not_found, <<"missing">>)`, optionally with `metadata` and `details`. `trevrpc_call` reads requests, sends responses, headers and trailers, and gives the call's metadata and deadline.

The server also listens on TCP for QMux over TLS, by default on the same port number as QUIC, or on any free port if that one is taken: `tcp_port => P` picks it, `tcp => false` turns TCP off, and `trevrpc_server:tcp_port/1` returns it. Connections over either transport share the server's limits. A TLS client has `tls_handshake_timeout` (10 s) for its TLS handshake and again for its QMux parameters, at most `max_tls_handshakes` (256) handshakes run at once, and a QMux client that stops reading is dropped after the connection idle timeout.

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

`transport` picks how a channel connects: `quic`, `tcp` (QMux over TLS) or `auto`, the default, which starts QUIC and, if it has not connected after `fallback_delay` (300 ms) or fails before, races QMux over TLS against it; the first to connect wins. Once QMux won, reconnects go straight to it for `reprobe_interval` (5 minutes) before trying QUIC again. `tcp_host` and `tcp_port` address the server's TCP listener, by default the QUIC host and port. `trevrpc_channel:transport/1` says which transport the current connection uses. A certificate in `cacerts` or `cacertfile` may be the server's own self-signed one.

A channel reconnects on its own and follows the [connection lifecycle](https://trev.zip/llc/TrevRPC/wiki/Connection-Lifecycle) of the other runtimes: each connection is a generation, calls never move between generations, calls made while it reconnects fail at once with `unavailable`, and reconnects back off from 100 ms to 30 s with jitter. A call belongs to the process that started it; if that process exits, the channel cancels the call. `trevrpc_channel:subscribe/1` sends the calling process `{trevrpc_channel, Channel, State}` messages with the channel's state, `{ready, Generation}`, `connecting` or, last, `closed`, now and after every change, until `unsubscribe/1` or the process exits.

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
| `trevrpc_call`                 | The handler API                                                                               |
| `trevrpc_channel`              | The client channel: reconnects, generations, stream credit                                    |
| `trevrpc_client`               | Client calls                                                                                  |
| `trevrpc_status`               | Status codes and statuses                                                                     |
| `trevrpc_metadata`             | Metadata key rules and size limits                                                            |
| `trevrpc`                      | Protocol identifiers and the default frame size limit                                         |
| `trevrpc_frame`                | Frame encoding, and an incremental reader with the frame size and cumulative byte limits      |
| `trevrpc_wire`                 | The control messages `RpcRequest`, `RpcHeaders`, `RpcAnswer` and `RpcStatus`, encoded by hand |
| `trevrpc_datagram`             | Datagram payloads                                                                             |
| `trevrpc_zstd`                 | Compression settings, their negotiation, and ZSTD_MESSAGE payloads                            |
| `trevrpc_zstd_stream`          | Stream compression: ZSTD_STREAM_MESSAGE payloads and the memory budget                        |
| `trevrpc_varint`, `trevrpc_pb` | QUIC varints and the protobuf wire primitives                                                 |

The rest (`trevrpc_server_conn`, `trevrpc_server_call`, `trevrpc_stream`, `trevrpc_quic`, `trevrpc_transport`, `trevrpc_qmux`, `trevrpc_qmux_frame`, `trevrpc_tcp_listener`) is internal.

## Development

rebar3 and Erlang/OTP 29 are in the repository's dev shell.

```sh
rebar3 eunit      # unit tests, the shared golden vectors in ../testdata, and client/server over QUIC and QMux
rebar3 xref
rebar3 dialyzer
```

`nix build .#trevrpc-erlang` runs all three. `nix fmt` formats the sources with erlfmt.

The Hex dependencies are declared in `rebar.config` and pinned in `rebar.lock`, which is the one place their versions and hashes live: after changing a dependency, update the lock with `rebar3 upgrade`, and the Nix package reads versions and tarball hashes from it.
