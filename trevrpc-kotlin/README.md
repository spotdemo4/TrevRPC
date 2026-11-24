# trevrpc-kotlin

The Kotlin client for [TrevRPC](https://trev.zip/llc/TrevRPC), an RPC framework like gRPC built on QUIC instead of HTTP/2. It is client-only and targets Android: calls are `POST /trevrpc` over HTTP/3 through [Cronet](https://developer.android.com/develop/connectivity/cronet), and when UDP is blocked Cronet falls back to HTTP/2 over TCP on its own, so there is nothing to configure. Servers are written in Go, Rust, or Erlang.

> **Compatibility:** the TrevRPC runtimes and code generators share one version, and runtimes of the same version
> are compatible with each other: `trevrpc-rust` 1.13.0 works with `trevrpc-go` 1.13.0, `trevrpc-js` 1.13.0,
> `trevrpc-kotlin` 1.13.0, and the Erlang `trevrpc` 1.13.0. Compatibility across different versions isn't guaranteed
> yet; that's a milestone for 2.0. The QMux packages and the integrations have versions of their own.

## Packages

Published to Maven Central under the group `zip.trev.trevrpc`.

| Package                                                                                                         | Install                                                         | Purpose                                                      |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------ |
| [`zip.trev.trevrpc:core`](https://central.sonatype.com/artifact/zip.trev.trevrpc/core)                          | `implementation("zip.trev.trevrpc:core:<version>")`             | Client runtime: calls, framing, metadata, status, wire codec |
| [`zip.trev.trevrpc:transport-cronet`](https://central.sonatype.com/artifact/zip.trev.trevrpc/transport-cronet)  | `implementation("zip.trev.trevrpc:transport-cronet:<version>")` | `CronetRpcChannel`, the Cronet HTTP/3 transport              |
| [`protoc-gen-trevrpc-kotlin`](https://central.sonatype.com/artifact/zip.trev.trevrpc/protoc-gen-trevrpc-kotlin) | `zip.trev.trevrpc:protoc-gen-trevrpc-kotlin:<version>:jdk21`    | `protoc` plugin: one client class per service                |

The plugin is an executable JAR with its dependencies that runs with `java -jar` on JDK 21+; `protoc` needs a small wrapper script that calls it. The `trevrpc-kotlin` Nix package also provides the plugin: `nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-kotlin`. From a checkout, `./gradlew :protoc-gen-trevrpc-kotlin:installDist` builds it under `protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin/bin/`.

`core` compiles against the API that protobuf-javalite and protobuf-java share and depends on neither, since the two hold the same classes and clash on one classpath. The application adds the runtime its generated messages use: `com.google.protobuf:protobuf-javalite` on Android, or `com.google.protobuf:protobuf-java`, at the version TrevRPC is tested with in [`libs.versions.toml`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-kotlin/gradle/libs.versions.toml) or newer.

Compression calls zstd-jni, which `core` doesn't depend on either; an application that configures compression adds it. `core`'s JAR carries R8 and ProGuard rules (`META-INF/proguard/trevrpc-core.pro`), which both apply on their own, so an application can shrink with R8 with or without zstd-jni. With it, the rules keep the names and members of the zstd-jni classes the application uses, which zstd-jni's native code looks up by name.

## Example

Generate Java messages for protobuf-javalite with `protoc` (drop `lite:` for protobuf-java) and the client with `protoc-gen-trevrpc-kotlin`:

```sh
protoc \
  --java_out=lite:build/generated \
  --plugin=protoc-gen-trevrpc-kotlin=path/to/protoc-gen-trevrpc-kotlin \
  --trevrpc-kotlin_out=build/generated \
  greeter.proto
```

Open a Cronet channel and call the generated client:

```kotlin
val engine = CronetEngine.Builder(context).enableQuic(true).build()
val executor = Executors.newSingleThreadExecutor()
val channel = CronetRpcChannel.create(engine, "https://rpc.example.com", executor)
val client = GreeterClient(channel, CallOptions(timeout = 10.seconds))

val reply = client.sayHello(HelloRequest.newBuilder().setName("TrevRPC").build())

// A successful close proves every call has settled; only then release the engine and executor.
channel.close()
engine.shutdown()
executor.shutdown()
```

## Documentation

The [Kotlin guide](https://trev.zip/llc/TrevRPC/wiki/Kotlin-Guide) covers generating a client, the Cronet channel, calls and streams, headers and metadata, ending a call early, call options, compression, and Cronet notes. The rest of the [wiki](https://trev.zip/llc/TrevRPC/wiki) covers the transports, the protocol, compression, and operations.

## Development

```shell
cd trevrpc-kotlin
./gradlew check                    # core, transport, and generator tests
./gradlew verifyStagedMavenRepository   # stage and verify the published artifacts
ktlint "**/src/**/*.kt" "**/*.gradle.kts"  # lint and format check
python3 -I -m unittest discover -s interop-android
```

### Interop tests

`interop-android` is an Android app and instrumented test that generates its client from the shared [`interop/proto/interop.proto`](https://trev.zip/llc/TrevRPC/src/branch/main/interop/proto/interop.proto) and runs every scenario in [`interop/README.md`](https://trev.zip/llc/TrevRPC/src/branch/main/interop/README.md) from an emulator against a Go, Rust, or Erlang server on the host. It is not published. It needs KVM, so it is built only with `-PtrevrpcAndroidInterop=true` and exposed as `trevrpc-kotlin.interopAndroid` in the flake:

```shell
nix shell .#trevrpc-kotlin.interopAndroid .#trevrpc-go-interop --command \
  trevrpc-interop-android \
  --server-command trevrpc-interop-go \
  --output out/interop --work-dir /var/tmp/interop
```

`--transport http3` (the default) runs the scenarios over HTTP/3. `--transport http2` runs them with Cronet's QUIC disabled, as on a network that blocks UDP, to exercise the HTTP/2 mapping on the server's TCP listener. The `android` workflow runs both for each server.

The scenarios run on the debug build. The app's two minified builds check `core`'s shrinker rules, and the Nix package builds both: `release` leaves out zstd-jni, as an application without compression does, so R8 fails on `core`'s references to it without the rules; `zstd` adds it, and `:interop-android:verifyZstdShrinking` checks in R8's output that every zstd-jni class R8 keeps keeps its name and all of its fields.

The runner generates a certificate for `10.0.2.2` (the emulator's alias for the host), trusts it in the emulator's system store, turns the emulator's Wi-Fi off and waits until mobile data is its default network and reaches the host, so no network change interrupts the scenarios, and runs the server command with `server --cert <pem> --key <pem> --host 0.0.0.0 --dictionary <file>`. It prints `ok <name>` or `fail <name>: <reason>` per scenario and exits 0 only if every scenario passed and the server exited 0 once its stdin closed.

Several runs can share a host. Each run starts its own adb server on a free port, which does not scan for emulators and so knows only the emulator that registers with it, and the emulator picks a free console port itself.

See [Local Development](https://trev.zip/llc/TrevRPC/wiki/Local-Development) for the repository-wide checks.
