# contributing

## requirements

- [nix](https://nixos.org/)

## getting started

```sh
nix develop
```

with [direnv](https://direnv.net/):

```sh
ln -s .envrc.project .envrc
direnv allow
```

| directory                                   | contents                                                                                        |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `trevrpc-go/`                               | Go runtime, `protoc-gen-trevrpc-go`, examples                                                   |
| `trevrpc-rust/`                             | Rust crate, `protoc-gen-trevrpc-rust`, examples                                                 |
| `trevrpc-js/`                               | browser client, browser interop runner                                                          |
| `trevrpc-js/effect/`                        | Effect adapter for the browser client                                                           |
| `trevrpc-js/protovalidate/`                 | protovalidate request validation for the browser client                                         |
| `trevrpc-js/protoc-gen-trevrpc-js/`         | `protoc-gen-trevrpc-js`, message codecs for the browser client                                  |
| `trevrpc-kotlin/`                           | Android client, Cronet transport, `protoc-gen-trevrpc-kotlin`                                   |
| `trevrpc-erlang/`                           | Erlang runtime (in progress: native QUIC, QMux over TLS and WebSocket, HTTP/3 and HTTP/2, zstd) |
| `trevrpc-erlang/protoc-gen-trevrpc-erlang/` | `protoc-gen-trevrpc-erlang`, gpb message modules and service stubs                              |
| `trevrpc-gleam/`                            | Gleam binding to the Erlang runtime                                                             |
| `trevrpc-gleam/protoc-gen-trevrpc-gleam/`   | `protoc-gen-trevrpc-gleam`, Gleam messages and service modules, on `protoc-gen-trevrpc-erlang`  |
| `interop/`                                  | cross-runtime test suite                                                                        |
| `bench/`                                    | load benchmarks and the grpc-go and connect-go baselines                                        |
| `testdata/`                                 | wire, QMux, and zstd golden vectors shared by the runtimes                                      |

### run

with [go](https://go.dev/):

```sh
cd trevrpc-go
go run ./examples/greeter_server
go run ./examples/greeter_client TrevRPC
```

with [cargo](https://doc.rust-lang.org/cargo/):

```sh
cd trevrpc-rust
cargo run --features dev-certificate --example greeter_server
cargo run --example greeter_client -- TrevRPC
```

### format

```sh
nix fmt
```

### check

```sh
nix flake check
```

with [go](https://go.dev/) and [golangci-lint](https://golangci-lint.run/), in `trevrpc-go/`:

```sh
go test ./...
golangci-lint run ./...
```

with [cargo](https://doc.rust-lang.org/cargo/), in `trevrpc-rust/`:

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

with [npm](https://docs.npmjs.com/) and [oxlint](https://oxc.rs/), in `trevrpc-js/`, then in `trevrpc-js/effect/`, `trevrpc-js/protovalidate/` and
`trevrpc-js/protoc-gen-trevrpc-js/` (whose tests are those of `trevrpc-js/`, which generate codecs with it):

```sh
npm ci
npm run check
```

with [gradle](https://gradle.org/) and [ktlint](https://pinterest.github.io/ktlint/), in `trevrpc-kotlin/`:

```sh
./gradlew check
ktlint "**/src/**/*.kt" "**/*.gradle.kts"
```

with [rebar3](https://rebar3.org/) and [erlfmt](https://github.com/WhatsApp/erlfmt), in `trevrpc-erlang/`:

```sh
rebar3 eunit
rebar3 xref
rebar3 dialyzer
erlfmt --check rebar.config src/*.erl src/*.app.src test/*.erl
```

the same in `trevrpc-erlang/protoc-gen-trevrpc-erlang/` and `trevrpc-gleam/protoc-gen-trevrpc-gleam/`, whose
tests also need `protoc`. The Gleam plugin's fixture, `test/gleam/test.sh`, generates Gleam code from its protos
and runs it, and needs `gleam` too (`nix shell nixpkgs#gleam`); the `trevrpc-gleam-protoc-gen` check runs it.
`trevrpc-gleam/test.sh`
generates the binding's test service and runs its tests, a Gleam server and client on loopback; the
`nix build .#trevrpc-gleam` runs it, and `nix fmt` formats Gleam with `gleam format`. The Erlang interop
executable runs on bindings generated from `interop/proto`, kept in `trevrpc-erlang/interop/src/generated`;
after changing the plugin or those protos, regenerate them with `protoc` on the PATH:

```sh
trevrpc-erlang/interop/generate.sh
```

with the cross-runtime [interop](interop) suite:

```sh
nix run .#trevrpc-interop
```

### build

```sh
nix build .#trevrpc-go
nix build .#trevrpc-rust
nix build .#trevrpc-js
nix build .#trevrpc-js-effect
nix build .#trevrpc-js-protovalidate
nix build .#trevrpc-kotlin
nix build .#trevrpc-erlang
nix build .#trevrpc-erlang-interop
nix build .#trevrpc-gleam
```

### release

with [bumper](https://trev.zip/llc/bumper):

```sh
bumper
```

the Gleam packages repeat the runtime's version in `trevrpc-erlang/gleam.toml`, `trevrpc-gleam/gleam.toml`,
`trevrpc-gleam/manifest.toml` and `protoc-gen-trevrpc-gleam`'s `app.src` and `default.nix`; the
`trevrpc-gleam-versions` check fails when a bump or a dependency update misses one of them, or leaves the
dependency ranges and locked versions apart from `rebar.config` and `rebar.lock`, or the two protoc plugins' `gpb`
apart

releases are automatically created per runtime for [significant](https://www.conventionalcommits.org/en/v1.0.0/#summary) changes
