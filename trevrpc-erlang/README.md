# trevrpc-erlang

The Erlang runtime for TrevRPC. It is the core that a Gleam package (`trevrpc-gleam`) will wrap with a typed API and generated stubs, and Elixir can use it directly.

> **Status:** early work in progress, not published. Progress is tracked in [#477](https://trev.zip/llc/TrevRPC/issues/477).

## What's here

The wire protocol, with no transport yet:

| Module             | Purpose                                                                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `trevrpc`          | Protocol identifiers (ALPN, QMux and WebTransport protocol, media type) and the default frame size limit                                     |
| `trevrpc_varint`   | QUIC variable-length integers                                                                                                                |
| `trevrpc_frame`    | Frame encoding, and an incremental reader that enforces the frame size limit and an optional cumulative byte limit, and skips unknown frames |
| `trevrpc_wire`     | The control messages `RpcRequest`, `RpcHeaders`, `RpcAnswer` and `RpcStatus`, encoded by hand                                                |
| `trevrpc_pb`       | The protobuf wire primitives `trevrpc_wire` uses                                                                                             |
| `trevrpc_datagram` | Datagram payloads: a flow ID and frames                                                                                                      |
| `trevrpc_metadata` | Metadata key rules and size limits                                                                                                           |
| `trevrpc_status`   | Status codes                                                                                                                                 |

The runtime treats application messages as opaque bytes, so it depends on no protobuf library: Erlang users can pick gpb, Elixir users protox, and the Gleam package its own generator.

## Planned transports

All of them use pure-Erlang libraries, so nothing native is built:

- Native QUIC (`trevrpc/1`) on [erlang_quic](https://github.com/benoitc/erlang_quic)
- HTTP/3 and WebTransport on erlang_quic's `quic_h3` and [erlang-webtransport](https://github.com/benoitc/erlang-webtransport)
- HTTP/2 on [erlang_h2](https://github.com/benoitc/erlang_h2)
- QMux over TLS and over WebSocket, implemented in-tree

## Development

It needs Erlang/OTP 28 or later (for the `zstd` module that compression will use) and rebar3, both in the repository's dev shell.

```sh
rebar3 eunit      # unit tests and the shared golden vectors in ../testdata
rebar3 xref
rebar3 dialyzer
```

`nix build .#trevrpc-erlang` runs all three. `nix fmt` formats the sources with erlfmt.
