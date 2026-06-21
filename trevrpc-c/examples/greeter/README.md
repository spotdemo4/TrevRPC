# C Generated Greeter Example

This example shows the shape of a C client and server built with generated protobuf-c TrevRPC bindings.

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

Build the examples against installed TrevRPC C libraries, protobuf-c, and MsQuic. Exact linker flags depend on the installation prefix and platform.

```sh
cc -I/usr/local/include -o greeter_server server.c greeter.pb-c.c greeter.trevrpc.c \
  -L/usr/local/lib -ltrevrpc -ltrevrpc_core -ltrevrpc_msquic -lprotobuf-c -lmsquic

cc -I/usr/local/include -o greeter_client client.c greeter.pb-c.c greeter.trevrpc.c \
  -L/usr/local/lib -ltrevrpc -ltrevrpc_core -ltrevrpc_msquic -lprotobuf-c -lmsquic
```

Run the server, then the client:

```sh
./greeter_server
./greeter_client 127.0.0.1 50051 TrevRPC
```
