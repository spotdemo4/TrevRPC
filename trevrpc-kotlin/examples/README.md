# TrevRPC Kotlin examples

This module is a compile-tested consumer of the Kotlin generator and runtime. `GreeterExample.kt`
implements and exercises unary, client-streaming, server-streaming, and bidirectional-streaming
methods. `GreeterServiceTest` verifies those methods, authorization, and malformed request decoding.

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
val channel = CronetRpcChannel.create(engine, "https://localhost:7443", executor)
val client = GreeterClient(channel, authenticatedOptions("example-token"))
```

`channel.close()` prevents future TrevRPC calls but does not shut down the injected engine or
executor. Cronet's provider owns connection pooling and reconnection; TrevRPC does not present those
pooled connections as channel generations. The channel is ready to submit calls immediately, so
its readiness does not describe the provider's network pool. The application owns and shuts down
both `CronetEngine` and the callback executor. Chromium's embedded provider and Google Play services
Cronet have different update/availability tradeoffs; select explicitly in dependency injection
rather than letting RPC code construct an engine.
