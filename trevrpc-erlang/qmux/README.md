# qmux

An Erlang implementation of [QMux](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/) (draft-ietf-quic-qmux-02): QUIC's streams, flow control, stream limits and datagrams carried over a reliable byte stream such as TLS over TCP, or over WebSocket messages ([draft-lcurley-qmux-websocket-00](https://datatracker.ietf.org/doc/draft-lcurley-qmux-websocket/)). Applications built on QUIC streams can use it as a fallback for networks that block UDP. Its API and events mirror [erlang_quic](https://github.com/benoitc/erlang_quic)'s, so an application can drive native QUIC and QMux the same way.

It needs only OTP (Erlang/OTP 29 or later). [TrevRPC](https://trev.zip/llc/TrevRPC) uses it for its TCP fallback. TrevRPC's interop suite runs its bidirectional streams against the [`qmux`](https://crates.io/crates/qmux) crate and TrevRPC's Go and JavaScript implementations, and all of them are checked against the same golden vectors. Unidirectional streams and datagrams are covered by this package's own tests only.

```erlang
%% rebar.config
{deps, [{qmux, "~> 1.12"}]}.
```

`gleam add qmux` and `{:qmux, "~> 1.12"}` in mix.exs work too.

> **Versioning:** the package shares the TrevRPC version and is released with it. QMux is still an Internet-Draft,
> so a new draft revision may change the API and the wire format.

## Use

QMux has no ALPN identifier of its own. The application defines one per draft version, such as `myapp-qmux-02`, and negotiates it with TLS ALPN, or as the WebSocket subprotocol, before it starts the session.

```erlang
{ok, Socket} = ssl:connect("example.com", 443, [
    binary, {active, false}, {alpn_advertised_protocols, [<<"myapp-qmux-02">>]}
]),
{ok, Session, Queue} = qmux:start({qmux_ssl, Socket}, client, self(), #{
    parameters => #{
        max_idle_timeout => 30000,
        initial_max_data => 1 bsl 20,
        initial_max_stream_data_bidi_local => 256 bsl 10,
        initial_max_stream_data_uni => 256 bsl 10,
        initial_max_streams_uni => 16,
        max_datagram_frame_size => 1200,
        max_record_size => 64 bsl 10
    },
    keep_alive => 10000
}),
receive
    {qmux, Session, {connected, _}} -> ok
end,
{ok, Stream} = qmux:open_stream(Session),
ok = qmux:send_data(Session, Queue, Stream, <<"hello">>, true).
```

A server does the same with `server` on each accepted connection, and receives `{qmux, Session, {stream_opened, Id}}`, `{stream_data, Id, Data, Fin}` and `{datagram, Data}` events.

- **Sessions.** `qmux:start/4` takes the socket over (the caller must be its controlling process) and sends the owner `{qmux, Session, Event}` messages. `close/2` sends an application CONNECTION_CLOSE, and the owner's exit closes the session too.
- **Streams.** `open_stream/1` opens a bidirectional stream, `open_unidirectional_stream/1` a send-only one; both fail with `stream_limit` until the peer allows more, and `{streams_available, bidi | uni}` follows when it does. The peer's streams arrive as `stream_opened` events; those it opens with `Id band 2 =/= 0` are receive-only. As over erlang_quic, `send_data/5` writes and finishes a stream, `reset_stream/3` and `stop_sending/3` send RESET_STREAM and STOP_SENDING.
- **Backpressure.** Writers count what they write in the session's send queue themselves, without waiting for the session: `send_data/5` fails with `send_queue_full` past `send_queue_limit`, and `notify_send_ready/1` asks for a `send_ready` event once it drains. `send_data_async/5` always queues. The session writes from a process of its own, so it reads on while the peer is slow to read.
- **Flow control.** Streams grant credit `auto`matically as data is delivered, while the owner's mailbox holds no more than `max_owner_queue` messages, or `manual`ly for what `consume/3` returns, so an owner that stops reading stops the peer after one window. `stream_flow_control` sets it for new streams and `set_stream_flow_control/3` for one.
- **Datagrams** (RFC 9221) go each way only to an endpoint that set `max_datagram_frame_size`: a session sends them once the peer advertised it, and accepts them if it advertised it itself, so both set it for datagrams both ways. `send_datagram/2` fails with `datagrams_not_supported` or `datagram_too_large` (see `datagram_max_size/1`). As over QUIC, a datagram may be dropped: when too many wait to be sent, or when the owner is behind as it arrives. Unlike over QUIC, one that is sent is delivered reliably and in order.
- **Transports.** `qmux_ssl` runs a session over TLS and `qmux_tcp` over plain TCP. Other byte streams implement the `qmux_transport` behaviour: send, active mode, controlling process, shutdown, close and peername.
- **Framing.** Records are Size-prefixed by default (`qmux_stream_framing`). The `framing` option takes another `qmux_framing` module, such as one that carries each record in a binary WebSocket message: its `decode/2` parses the messages that arrive into records and returns Pongs for the peer's Pings, and its `close/2` appends a Close frame to the last record. TrevRPC's QMux over WebSocket ([`trevrpc_qmux_websocket.erl`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/src/trevrpc_qmux_websocket.erl)) works that way.
- **Idle timeout.** Every record received restarts it, and so does the first record sent after each (RFC 9000 §10.1), so keep-alives alone can't keep a session to a vanished peer open. While stream data or datagrams a session sent wait for the peer to be heard from, it sends a QX_PING halfway through the timeout: a live peer answers it, even when it has nothing else to send. Replies such as flow control updates don't count, so a session idle both ways sends no probe.
- **Errors.** A session ends with `{closed, Reason}`: `{peer_closed, application, Code, Reason}` or `{peer_closed, transport, ...}` for the peer's CONNECTION_CLOSE, `{transport_error, Code, Reason}` for a QMux protocol error, `idle_timeout`, `handshake_timeout`, or `connection_lost`.

Not supported: 0-RTT, and the RESET_STREAM_AT extension (draft-02 §9.2). Neither is advertised, so peers do not use them.

## Development

```sh
rebar3 eunit      # the shared golden vectors in ../../testdata, and sessions against a hand-written peer over TLS and TCP
rebar3 xref
rebar3 dialyzer
```

`nix build .#trevrpc-erlang-qmux` runs all three, and the `trevrpc-erlang-qmux-lint` check runs erlfmt. `trevrpc` depends on this package through `_checkouts/qmux` (rebar3) and a path dependency (Gleam); flake-release publishes this package to Hex first with rebar3, and replaces the path dependency with its version when it publishes `trevrpc`.
