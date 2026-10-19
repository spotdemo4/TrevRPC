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

| directory         | contents                                                        |
| ----------------- | --------------------------------------------------------------- |
| `trevrpc-go/`     | Go runtime, `protoc-gen-trevrpc-go`, examples                   |
| `trevrpc-rust/`   | Rust crate, `protoc-gen-trevrpc-rust`, examples                 |
| `trevrpc-js/`     | browser client, `protoc-gen-trevrpc-js`, browser interop runner |
| `trevrpc-kotlin/` | Android client, Cronet transport, `protoc-gen-trevrpc-kotlin`   |
| `interop/`        | cross-runtime test suite                                        |
| `bench/`          | load benchmarks and the grpc-go baseline                        |
| `testdata/`       | wire and QMux golden vectors shared by every runtime's tests    |

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
cargo run --example greeter_server
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

with [npm](https://docs.npmjs.com/) and [oxlint](https://oxc.rs/), in `trevrpc-js/`:

```sh
npm ci
npm run check
```

with [gradle](https://gradle.org/) and [ktlint](https://pinterest.github.io/ktlint/), in `trevrpc-kotlin/`:

```sh
./gradlew check
ktlint "**/src/**/*.kt" "**/*.gradle.kts"
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
nix build .#trevrpc-kotlin
```

### release

with [bumper](https://trev.zip/llc/bumper):

```sh
bumper
```

releases are automatically created per runtime for [significant](https://www.conventionalcommits.org/en/v1.0.0/#summary) changes
