# trevrpc-erlang

The BEAM runtime for [TrevRPC](https://trev.zip/llc/TrevRPC), an RPC framework like gRPC built on QUIC instead of HTTP/2: one Hex package, `trevrpc`, for Erlang, Gleam and Elixir projects, with servers that serve native QUIC and QMux over TLS (and, when enabled, the HTTP mapping over HTTP/3 and HTTP/2 and browser sessions over WebTransport and QMux over WebSocket), and native QUIC clients that fall back to QMux over TLS when UDP is blocked. It is pure Erlang, built on [erlang_quic](https://github.com/benoitc/erlang_quic), [erlang-webtransport](https://github.com/benoitc/erlang-webtransport), [erlang_h2](https://github.com/benoitc/erlang_h2), [erlang_h1](https://github.com/benoitc/erlang_h1), [erlang_ws](https://github.com/benoitc/erlang_ws) and OTP's `ssl` and `zstd`, with its QMux implementation in a Hex package of its own, [`qmux`](qmux), and needs Erlang/OTP 29 or later. A typed Gleam binding is part of the package, and `protoc-gen-trevrpc-elixir` generates Elixir code over the runtime.

> **Status:** early work in progress. Progress is tracked in [#477](https://trev.zip/llc/TrevRPC/issues/477).

> **Compatibility:** the TrevRPC runtimes and code generators share one version, and runtimes of the same version
> are compatible with each other: `trevrpc-rust` 1.12.0 works with `trevrpc-go` 1.12.0, `trevrpc-js` 1.12.0,
> `trevrpc-kotlin` 1.12.0, and the Erlang `trevrpc` 1.12.0. Compatibility across different versions isn't guaranteed
> yet; that's a milestone for 2.0. The QMux packages and the integrations have versions of their own.

## Packages

| Package                                                                                                              | Install                                                                                                                                                                         | Purpose                                                                                                                                          |
| -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`trevrpc`](https://hex.pm/packages/trevrpc)                                                                         | rebar.config: `{trevrpc, "~> 1.12"}`; `gleam add trevrpc`; mix.exs: `{:trevrpc, "~> 1.12"}`                                                                                     | Client and server runtime, with the [Gleam binding](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#gleam)                                        |
| [`qmux`](https://hex.pm/packages/qmux)                                                                               | rebar.config: `{qmux, "~> 1.12"}`; `gleam add qmux`; mix.exs: `{:qmux, "~> 1.12"}`                                                                                              | Standalone [QMux](qmux) (draft-ietf-quic-qmux-02): QUIC streams and datagrams over TLS or WebSocket                                              |
| [`protoc-gen-trevrpc-erlang`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/protoc-gen-trevrpc-erlang) | escript: the Nix package below, or `rebar3 escriptize` in [`protoc-gen-trevrpc-erlang/`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/protoc-gen-trevrpc-erlang) | `protoc` plugin: [gpb message modules and service behaviours, routes and clients](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#generated-code) |
| [`protoc-gen-trevrpc-gleam`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/protoc-gen-trevrpc-gleam)   | escript: the Nix package below, or `rebar3 escriptize` in [`protoc-gen-trevrpc-gleam/`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/protoc-gen-trevrpc-gleam)   | `protoc` plugin: [Gleam types and a module per service](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#generated-gleam-code)                     |
| [`protoc-gen-trevrpc-elixir`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/protoc-gen-trevrpc-elixir) | escript: the Nix package below, or `rebar3 escriptize` in [`protoc-gen-trevrpc-elixir/`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/protoc-gen-trevrpc-elixir) | `protoc` plugin: [an Elixir module per service](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#elixir)                                           |

The `trevrpc-erlang` Nix package provides all three plugins: `nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-erlang`.

## Example

Generate code for a service with `protoc-gen-trevrpc-erlang`:

```sh
protoc --plugin=protoc-gen-trevrpc-erlang=path/to/protoc-gen-trevrpc-erlang \
    -I proto -I path/to/TrevRPC/proto --trevrpc-erlang_out=src/generated example/greeter/v1/greeter.proto
```

Serve it:

```erlang
say_hello(#{name := Name}, _Call) ->
    {ok, #{message => <<"hello ", Name/binary>>}}.

{ok, Server} = trevrpc_server:start_link(#{
    port => 4433,
    certfile => "cert.pem",
    keyfile => "key.pem",
    routes => example_greeter_v1_greeter:routes(#{say_hello => fun say_hello/2})
}).
```

And call it:

```erlang
{ok, Channel} = trevrpc_channel:connect(#{host => "localhost", port => 4433, cacertfile => "ca.pem"}, 5000),
{ok, #{message := Message}, _} = example_greeter_v1_greeter:say_hello(Channel, #{name => <<"you">>}).
```

The guide has the same service in [Gleam](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#gleam) and [Elixir](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#elixir).

## Documentation

The [Erlang guide](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide) covers generated code, servers, clients, compression, statuses, the Gleam binding and its code generation, and Elixir code generation. The rest of the [wiki](https://trev.zip/llc/TrevRPC/wiki) covers the transports, the protocol, compression, and operations.

## Development

rebar3 and Erlang/OTP 29 are in the repository's dev shell; the Gleam binding also needs `gleam` (`nix shell nixpkgs#gleam`) and `protoc`.

```sh
rebar3 eunit      # unit tests, the shared golden vectors in ../testdata, client/server over QUIC and QMux, datagram messages, QMux over WebSocket, and the HTTP mapping
rebar3 xref
rebar3 dialyzer
```

`nix build .#trevrpc-erlang` runs all three. `nix fmt` formats the sources with erlfmt, and the Gleam sources with `gleam format`; the `trevrpc-erlang-lint` check runs erlfmt. The same commands work in `protoc-gen-trevrpc-erlang/`, `protoc-gen-trevrpc-gleam/` and `protoc-gen-trevrpc-elixir/`, whose tests also need `protoc`, and `nix build .#trevrpc-erlang` runs them too, as it builds the plugins it provides.

```sh
./gleam-test.sh   # gleam-generate.sh, which generates the Gleam tests' service with protoc-gen-trevrpc-gleam, then gleam test
```

The Gleam tests (`test/trevrpc_test.gleam`) run a Gleam server and client on loopback. `gleam test` builds the plugin in `protoc-gen-trevrpc-gleam/` unless `PROTOC_GEN_TREVRPC_GLEAM` names it, and runs only that module, as rebar3 runs the Erlang tests. The `trevrpc-erlang-gleam` check (`nix build .#trevrpc-erlang.gleam`, from `gleam.nix`) builds the package as `gleam publish` does and runs them offline.

`protoc-gen-trevrpc-gleam` and `protoc-gen-trevrpc-elixir` build on `protoc-gen-trevrpc-erlang`: each carries that plugin's modules, which make gpb's modules, check methods and read their options, and takes them from `_checkouts`, a link to it, so changes there reach them too. The Gleam plugin also carries the plugin protocol (`trevrpc_gen_plugin`), and its tests share `test/support` and `test/proto`.

- `protoc-gen-trevrpc-gleam/test/gleam` is a Gleam project that runs the generated code: `test/gleam/test.sh` generates it (`generate.sh`), runs its tests, and checks with protoc that its bytes decode as expected (`compare.sh`). The `trevrpc-erlang-protoc-gen-gleam` check runs it.
- `protoc-gen-trevrpc-elixir/test/elixir` is a Mix project that runs the generated code: `test/elixir/test.sh` generates it into `gen/` and runs an Elixir server and client through it with `mix test`, taking `trevrpc` from `ERL_LIBS` rather than Hex, with Elixir on the PATH. The `trevrpc-erlang-protoc-gen-elixir` check runs it.

`qmux/` is the `qmux` package, with its own tests (see [its README](qmux/README.md)). rebar3 takes it from `_checkouts/qmux`, a link to it, both here and in `interop/`, and Gleam from the path dependency in `gleam.toml`, which flake-release replaces with the version it publishes to Hex just before `trevrpc`.

The interop executable, `trevrpc-interop-erlang` (the `trevrpc-erlang-interop` package), is in the [interop suite](https://trev.zip/llc/TrevRPC/wiki/Local-Development#interop-tests) and has the load benchmark's client. Its bindings, in `interop/src/generated`, are regenerated with `interop/generate.sh`, and a check fails when they differ from what the plugin generates.

erlang_quic builds its optional crypto NIF only when `QUIC_BUILD_NIF=1` is set while the `quic` dependency compiles, which needs CMake, a C compiler and the OpenSSL headers; the Nix packages set it. Neither rebar3 nor Gleam rebuilds a dependency it already compiled, so remove it first (`rm -rf _build/default/lib/quic && QUIC_BUILD_NIF=1 rebar3 compile`, or `rm -rf build/dev/erlang/quic && QUIC_BUILD_NIF=1 gleam build`). `quic_crypto_nif:is_loaded()` says whether it loaded; see the guide's [crypto NIF](https://trev.zip/llc/TrevRPC/wiki/Erlang-Guide#crypto-nif) section.

rebar3 builds the application from `rebar.config` and `src/trevrpc.app.src`, Gleam from `gleam.toml`, which `gleam publish` turns into the Hex package's metadata and `.app` file. The Hex dependencies are declared in `gleam.toml` and locked in `manifest.toml`, which Renovate's Gleam manager updates; no Renovate manager reads rebar3's files. `nix run .#sync-erlang-deps` (`erlang_deps.py`) copies them into `rebar.config` (the ranges, verbatim, as rebar3 reads Hex's syntax), `rebar.lock` (the locked versions and their checksums, the inner ones from the packages' tarballs), the interop executable's `rebar.lock`, and `protoc-gen-trevrpc-gleam`'s and `protoc-gen-trevrpc-elixir`'s `gpb`, which follows `protoc-gen-trevrpc-erlang`'s. Renovate runs it after updating the Gleam files; after changing a dependency by hand, change `gleam.toml`, run `gleam update` (or `gleam deps download`), then the sync. The Nix package reads versions and tarball hashes from `rebar.lock`, and the `trevrpc-erlang-deps` check fails when rebar3's files drift from the Gleam ones. `python3 -I -m unittest discover -s trevrpc-erlang`, from the repository's root, runs the sync's tests, as the `python-test` check does. `gleam_stdlib` is the one dependency only `gleam.toml` has, `qmux`, a path dependency there, is required by name in `rebar.config`, and adding or removing one takes both `gleam.toml` and `rebar.config`.

See [Local Development](https://trev.zip/llc/TrevRPC/wiki/Local-Development) for the repository-wide checks.
