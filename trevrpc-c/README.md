# trevrpc-c

TrevRPC is an RPC framework like gRPC, but uses QUIC, HTTP/3, and WebTransport instead of HTTP/2. Define services in protobuf, generate typed bindings, and drive calls through an asynchronous event loop.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Engine ABI

[Engine ABI 1](engine-abi-v1.md) is the provider-neutral transport ABI. It defines generation-safe handles, nonblocking commands, copied framed sends, owned receives, a bounded event queue, a borrowed level-triggered wake source, diagnostics, and deterministic close/drain/release behavior.

[MsQuic Engine factory ABI 1](engine-msquic-abi-v1.md) is the separately versioned MsQuic provider companion. It creates a `trevrpc_engine`; all operations after construction use the generic `trevrpc_engine_*` API.

## RPC ABI

`trevrpc_rpc.h` defines asynchronous RPC ABI 1. `trevrpc_rpc_msquic.h` provides the MsQuic runtime factory and endpoint configuration. They are independently packaged as `trevrpc_rpc` and `trevrpc_rpc_msquic`.

The RPC contract is event-driven:

- commands with operation IDs either fail synchronously with no completion or are admitted with exactly one completion;
- generated calls use typed endpoint, call, and stream handles;
- incoming-call events explicitly transfer their call, stream, and initial receive;
- sent protobuf messages are copied before an admitted command returns;
- received messages, metadata, and statuses are exposed through immutable owned receives; pointers in receive info remain borrowed until the receive is explicitly released;
- applications drain events until `-EAGAIN` before waiting on the runtime's borrowed wake source;
- calls, streams, endpoints, and runtimes have explicit close and release operations.

The RPC MsQuic provider supports explicit native QUIC, HTTP/3 POST, and WebTransport endpoint modes through the same asynchronous RPC event model. `AUTO` remains a distinct selection mode; unsupported or invalid configurations fail rather than silently changing transports.

## Protobuf and generated C

```proto
syntax = "proto3";

package hello.v1;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
  rpc LotsOfReplies(HelloRequest) returns (stream HelloReply);
  rpc LotsOfGreetings(stream HelloRequest) returns (HelloReply);
  rpc BidiHello(stream HelloRequest) returns (stream HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Generate protobuf-c messages and RPC ABI 1 helpers:

```sh
protoc-c -I . --c_out=. greeter.proto
protoc -I . --trevrpc-c_out=. greeter.proto
```

Generated headers include `trevrpc_rpc.h` and require `TREVRPC_RPC_ABI_VERSION == 1`. The generator emits:

- `*_call_config_init` and `*_open` for every RPC shape;
- typed request and response send helpers;
- client input half-close and server terminal-status helpers;
- direction-specific `*_decode_request_receive` and `*_decode_response_receive` functions;
- `*_matches_incoming`, `*_take_incoming`, and `*_accept` server helpers.

## Client flow

Create one runtime, obtain its wake source, and start a client endpoint:

```c
trevrpc_rpc_runtime_config_v1 runtime_config;
trevrpc_rpc_msquic_config_v1 provider_config;
trevrpc_rpc_msquic_endpoint_config_v1 endpoint_config;
trevrpc_rpc_runtime *runtime = NULL;
trevrpc_rpc_endpoint_v1 endpoint = {0};

trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config));
trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config));
trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime);

trevrpc_rpc_msquic_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config));
endpoint_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
endpoint_config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
endpoint_config.host = "127.0.0.1";
endpoint_config.host_len = 9;
endpoint_config.port = 50051;
endpoint_config.flags &= ~TREVRPC_RPC_MSQUIC_VERIFY_PEER;
trevrpc_rpc_msquic_endpoint_start_v1(runtime, &endpoint_config, 1, &endpoint);
```

After the matching `TREVRPC_RPC_EVENT_ENDPOINT_READY`, open a generated call:

```c
Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
trevrpc_rpc_call_v1 call = {0};
trevrpc_rpc_stream_v1 stream = {0};
request.name = "TrevRPC";

hello_v1_greeter_say_hello_open(runtime, endpoint, &request, 2, &call, &stream);
```

Drain runtime events. For a readable stream, repeatedly call `trevrpc_rpc_stream_receive()` until `-EAGAIN`, inspect `trevrpc_rpc_receive_info_v1.kind`, decode only message receives with the generated response decoder, and handle metadata and status receives separately. Release every decoded protobuf message and every `trevrpc_rpc_receive`. Command-completion events carry the admitted nonzero operation ID; passive readiness and terminal events identify their call or stream but may carry operation ID zero.

## Server flow

Start a listener endpoint with certificate and key configuration, then process `TREVRPC_RPC_EVENT_CALL_INCOMING` events. Match the generated method before atomically transferring ownership. Check the result of `*_take_incoming` before using the returned call, stream, or initial receive; after a successful transfer, application state owns all three.

Decode and release the initial receive, then admit `*_accept` and the generated response operations with fresh nonzero operation IDs. If decoding, acceptance, or response admission fails after transfer, issue abortive call and stream close operations instead of dropping the handles. Retain the call and stream until their terminal events have been drained, then release both typed handles. Streaming handlers follow the same ownership pattern while draining request receives, sending responses, and explicitly finishing the response or input side.

The complete checked implementation is in `examples/greeter/server.c`; it tracks transferred handles immediately, closes them on every post-transfer failure path, and releases them only after both terminal events.

See [`examples/greeter`](examples/greeter) and `tests/generated_service_test.c` for complete event-loop and four-shape examples.

## Linking

> **Scope:** this section describes installed C ABI consumers using the CMake and
> pkg-config distribution. Go consumers should instead use the nested
> [`provider/msquic`](provider/msquic) module, which compiles the provider-neutral
> sources and links its pinned static MsQuic archive without host MsQuic discovery.

Generated code is transport-neutral and links against `trevrpc_rpc` plus protobuf-c. An executable that constructs the MsQuic provider also links `trevrpc_rpc_msquic`:

```sh
pkg-config --cflags --libs --static trevrpc_rpc_msquic libprotobuf-c
```
