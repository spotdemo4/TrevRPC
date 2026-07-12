# trevrpc-cpp

`trevrpc-cpp` is the C++20 TrevRPC runtime and code generator. Its synchronous public API reports failures through explicit `Result` values.

Generated bindings use protobuf C++ full or lite message classes. They serialize and parse those messages around the byte-oriented `trevrpc-c` transport, so C++ applications reuse its QUIC, framing, cancellation, limits, server, and channel lifecycle without generating or linking protobuf-c code.

## API

- `{Service}Service` synchronous server interfaces.
- `{Service}Client` synchronous typed clients.
- `Register{Service}` registration functions for `Server`.
- `Channel`, `Server`, and `Result` runtime types.
- Unary, server-streaming, client-streaming, and bidirectional-streaming RPCs.
- Protobuf C++ full and lite generated messages.
- Move-only RAII server and stream owners, plus shared long-lived channels.
- `CallOptions`, `Metadata`, `Cancellation`, explicit terminal stream events, and status-preserving errors.

## Build And Generation Model

Build the runtime, tests, examples, and `protoc-gen-trevrpc-cpp` from the repository root:

```sh
cmake -S trevrpc-cpp -B build/trevrpc-cpp
cmake --build build/trevrpc-cpp
ctest --test-dir build/trevrpc-cpp --output-on-failure
```

Generate protobuf messages and TrevRPC service bindings from the same schema:

```sh
protoc -I proto \
  --cpp_out=gen/cpp \
  --trevrpc-cpp_out=gen/cpp \
  proto/example/greeter.proto
```

The plugin emits `.trevrpc.hpp` and `.trevrpc.cpp` beside protobuf's `.pb.h` and `.pb.cc`. Compile both generated source sets as C++20 and link `trevrpc::cpp` plus `protobuf::libprotobuf` or `protobuf::libprotobuf-lite`. Do not run the protobuf-c generator for this runtime.

Installed CMake packages expose `trevrpc::cpp`, and `trevrpc_cpp_generate` can create a generated binding target. The plugin accepts `runtime_include`, `header_suffix`, and `source_suffix` options.

## Channel Lifecycle

Create one long-lived `Channel` per destination and share it across generated clients. Every RPC remains pinned to the ready connection generation on which it starts. Connection loss fails that RPC through `Result`; TrevRPC never retries, replays, resumes, or moves it. Reconnection only enables future calls, and new work fails fast while reconnecting unless the application explicitly waits for readiness.

During shutdown, stop starting calls, finish or cancel active streams, explicitly close the channel, and then destroy clients and channel state. The C++ wrapper owns its underlying C transport handles.

See the [C++ Guide](../wiki/Cpp-Guide.md) and [Protobuf and Code Generation](../wiki/Protobuf-and-Code-Generation.md) for the complete API and integration workflow.
