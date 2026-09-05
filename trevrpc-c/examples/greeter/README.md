# C Generated Greeter Example

This example uses the asynchronous RPC ABI 1 generated helpers with the MsQuic
provider (`trevrpc_rpc_msquic`) over native QUIC, HTTP/3 POST, or WebTransport.
The client and server each own a single poll-based event loop. They borrow the
runtime wake fd, drain events until `-EAGAIN`, and do not create per-call threads.

The example covers all four protobuf RPC shapes:

- unary `SayHello`;
- server-streaming `LotsOfReplies`;
- client-streaming `LotsOfGreetings`; and
- bidirectional-streaming `BidiHello`.

Server-side `CALL_INCOMING` handling explicitly takes and accepts transferred
call, stream, and initial-receive ownership. Both peers send copied messages,
finish request or response sides as appropriate, decode owned receives, release
receives and events, and close and release typed calls and streams. Runtime
shutdown closes the endpoint, waits for terminal events, drains the event queue,
and releases the runtime.

The optional final argument selects `native`, `http3`, or `webtransport`. Both
processes must select the same transport.

The repository CMake build generates the bindings and builds both example
executables:

```sh
cmake -S trevrpc-c -B build \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DTREVRPC_BUILD_ENGINE_MSQUIC=ON \
  -DTREVRPC_BUILD_RPC=ON \
  -DTREVRPC_BUILD_RPC_MSQUIC=ON \
  -DTREVRPC_BUILD_MSQUIC=OFF \
  -DTREVRPC_BUILD_WEBTRANSPORT=OFF \
  -DTREVRPC_BUILD_RUNTIME=OFF \
  -DTREVRPC_BUILD_EXAMPLES=ON
cmake --build build --target trevrpc_greeter_server trevrpc_greeter_client
```

Generate the protobuf-c and TrevRPC C files:

```sh
protoc \
  --proto_path=. \
  --c_out=. \
  --trevrpc-c_out=. \
  greeter.proto
```

That produces:

```text
greeter.pb-c.c
greeter.pb-c.h
greeter.trevrpc.c
greeter.trevrpc.h
```

Build the examples against installed TrevRPC RPC ABI 1 libraries,
protobuf-c, and MsQuic through pkg-config. Set `PKG_CONFIG_PATH` when the
packages are outside the default search path.

```sh
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
cc -I. -o greeter_server server.c greeter.pb-c.c greeter.trevrpc.c \
  $(pkg-config --cflags --libs --static trevrpc_rpc_msquic libprotobuf-c)

cc -I. -o greeter_client client.c greeter.pb-c.c greeter.trevrpc.c \
  $(pkg-config --cflags --libs --static trevrpc_rpc_msquic libprotobuf-c)
```

Every transport needs a certificate and private key. The server arguments are
`host`, `port`, `certificate`, `key`, and optional transport; defaults are
`127.0.0.1`, `50051`, `server.crt`, `server.key`, and `native`. The client takes
`host`, `port`, a greeting name, and the same optional transport.

```sh
./greeter_server 127.0.0.1 50051 server.crt server.key webtransport
./greeter_client 127.0.0.1 50051 TrevRPC webtransport
```

Press Ctrl-C to request orderly server shutdown.
