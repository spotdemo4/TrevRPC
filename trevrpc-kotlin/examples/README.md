# TrevRPC Kotlin examples

`trevrpc-xruntime-kotlin` is the installed JVM executable. It serves the generated Greeter API over native QUIC, HTTP/3, and WebTransport on one UDP listener, or exercises another runtime as a client.

```shell
./gradlew :examples:installDist
examples/build/install/trevrpc-xruntime-kotlin/bin/trevrpc-xruntime-kotlin \
  -mode server -addr 127.0.0.1:0 -cert /tmp/trevrpc-kotlin.pem -token cross-runtime-token
```

Client modes are `client` and `lifecycle-client`. Select `-transport native` (the default) or `-transport http3`; `-iterations` repeats the complete operation. `browser-server` adds the browser lifecycle routes and emits `READY https://<address>/trevrpc`. Browser admission can be set with `-browser-origin` and `-browser-authority`, or `TREVRPC_EXAMPLE_ORIGIN` and `TREVRPC_EXAMPLE_AUTHORITIES` (comma separated). `-max-streams` or `TREVRPC_EXAMPLE_MAX_STREAMS` configures the per-connection limit used by lifecycle tests. Without overrides, only `http://localhost:8080`, `http://127.0.0.1:8080`, and loopback authorities on the bound port are admitted.

The public Netty client transport does not expose raw QUIC streams, so the executable does not send the Go harness's malformed initial frame. `GreeterServiceTest` separately verifies malformed request decoding. Adding that executable probe requires a safe public raw-frame test API in `transport-netty`, which is outside this module's edit boundary.

## Protobuf generation

`generateProto` uses `protoc` 4.35.1 for Java messages and this repository's `protoc-gen-trevrpc-kotlin` executable for bindings. Normal compilation consumes both generated outputs. The TrevRPC source is checked in at `generated/trevrpc/greeter.trevrpc.kt`.

```shell
./gradlew :examples:syncGeneratedTrevrpc
./gradlew :examples:verifyGeneratedTrevrpc
```

## Android Cronet injection

Keep Android runtime/provider selection in the Android application and inject the resulting engine. This JVM examples module intentionally does not depend on Android classes.

```kotlin
interface CronetEngineProvider {
    fun create(context: Context): CronetEngine
}

// Use with org.chromium.net:cronet-embedded when APK size and deterministic
// provider availability matter.
class EmbeddedCronetProvider : CronetEngineProvider {
    override fun create(context: Context): CronetEngine =
        CronetEngine.Builder(context)
            .enableQuic(true)
            .enableHttp2(true)
            .build()
}

// After CronetProviderInstaller.installProvider(context) succeeds, the same
// builder can use the Google Play services implementation instead.
class PlayServicesCronetProvider : CronetEngineProvider {
    override fun create(context: Context): CronetEngine =
        CronetEngine.Builder(context)
            .enableQuic(true)
            .enableHttp2(true)
            .build()
}

val engine = provider.create(applicationContext)
val executor = Executors.newSingleThreadExecutor()
val transport = CronetRpcTransport(engine, "https://localhost:7443", executor)
val client = GreeterClient(transport, authenticatedOptions("cross-runtime-token"))
```

The application owns and shuts down both `CronetEngine` and the callback executor. Chromium's embedded provider and Google Play services Cronet have different update/availability tradeoffs; select explicitly in dependency injection rather than letting RPC code construct an engine.
