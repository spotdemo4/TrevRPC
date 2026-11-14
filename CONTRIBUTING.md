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

| directory                           | contents                                                                     |
| ----------------------------------- | ---------------------------------------------------------------------------- |
| `trevrpc-go/`                       | Go runtime, `protoc-gen-trevrpc-go`, examples                                |
| `trevrpc-rust/`                     | Rust crate, `protoc-gen-trevrpc-rust`, examples                              |
| `trevrpc-js/`                       | browser client, browser interop runner                                       |
| `trevrpc-js/effect/`                | Effect adapter for the browser client                                        |
| `trevrpc-js/protovalidate/`         | protovalidate request validation for the browser client                      |
| `trevrpc-js/protoc-gen-trevrpc-js/` | `protoc-gen-trevrpc-js`, message codecs for the browser client               |
| `trevrpc-kotlin/`                   | Android client, Cronet transport, `protoc-gen-trevrpc-kotlin`                |
| `trevrpc-erlang/`                   | Erlang runtime (in progress: native QUIC, QMux over TLS and WebSocket, zstd) |
| `interop/`                          | cross-runtime test suite                                                     |
| `bench/`                            | load benchmarks and the grpc-go and connect-go baselines                     |
| `testdata/`                         | wire, QMux, and zstd golden vectors shared by the runtimes                   |

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
```

### release

with [bumper](https://trev.zip/llc/bumper):

```sh
bumper
```

releases are automatically created per runtime for [significant](https://www.conventionalcommits.org/en/v1.0.0/#summary) changes
