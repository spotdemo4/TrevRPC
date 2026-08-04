# trevrpc-c

`trevrpc-c` 0.2.0 provides TrevRPC C ABI 6. The transport protocol remains
`TREVRPC_ALPN == "trevrpc/1"` and `TREVRPC_WIRE_VERSION == 1u`; ABI 6 changes only the
native C API and ownership contract.

ABI 6 has no ABI-5 compatibility declarations, targets, or shims. See
[`MIGRATING-ABI5-TO-ABI6.md`](MIGRATING-ABI5-TO-ABI6.md) when updating an existing
consumer.

## Generate bindings

Install the package and run both protobuf-c and TrevRPC generation for a `.proto` file:

```sh
protoc-c -I proto --c_out=generated proto/greeter.proto
protoc -I proto \
  --plugin=protoc-gen-trevrpc-c="$(command -v protoc-gen-trevrpc-c)" \
  --trevrpc-c_out=generated proto/greeter.proto
```

Generated headers reject any C runtime header whose `TREVRPC_C_ABI_VERSION` is not `6u`.
Generated client APIs use typed unary result objects, retained opaque response envelopes, and
stateful stream receivers. Generated server responders are synchronous and borrow protobuf values
only until the responder returns.

## Client configuration and channels

All extensible public configuration structures are versioned and initialized with the caller's
visible structure size:

```c
#include <trevrpc.h>

trevrpc_client_config_v1 config;
if (trevrpc_client_config_v1_init(&config, sizeof(config)) != 0) {
    return 1;
}
config.skip_certificate_validation = 1;

trevrpc_channel* channel = NULL;
int rc = trevrpc_channel_connect_v1(
    "127.0.0.1", 50051, &config, NULL, 5000000000ull, NULL, &channel);
```

A successful channel connect must be paired with close and release:

```c
trevrpc_channel_close(channel);
trevrpc_channel_release(channel);
```

The runtime deep-copies pointer-backed client configuration before connect returns. A zero connect
timeout waits indefinitely. Connect and readiness waits accept an optional retained
`trevrpc_cancellation`.

## Generated unary calls

A generated unary helper returns transport/decode outcomes through a typed discriminated result.
The result retains the opaque inbound envelope so status text, metadata, and body storage remain
valid until reset.

```c
Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
request.name = "TrevRPC";

hello_v1_greeter_say_hello_result result =
    HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
rc = hello_v1_greeter_say_hello(channel, &request, &result);
if (rc == 0 && result.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_SUCCESS) {
    printf("%s\n", result.response->message);
} else if (rc == 0 && result.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_RPC_STATUS) {
    uint32_t status = TREVRPC_STATUS_UNKNOWN;
    trevrpc_inbound_response_get_status(result.envelope, &status);
    fprintf(stderr, "RPC status: %u\n", status);
}
hello_v1_greeter_say_hello_result_reset(&result);
```

Reset is null-safe and idempotent for a zeroed or previously reset result.

## Generated response streams

Generated receivers are stateful. Each event owns its decoded protobuf message and retained opaque
frame until the event is reset. A terminal status is emitted only after the receiver has observed a
clean transport FIN. FIN without status and any frame after status are distinct terminal errors.

```c
trevrpc_stream* stream = NULL;
rc = hello_v1_greeter_lots_of_replies(channel, &request, &stream);

hello_v1_greeter_hello_reply_receiver receiver =
    HELLO_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
hello_v1_greeter_hello_reply_event event =
    HELLO_V1_GREETER_HELLO_REPLY_EVENT_INIT;
if (rc == 0) {
    rc = hello_v1_greeter_hello_reply_receiver_init(&receiver, stream);
}
while (rc == 0) {
    rc = hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event);
    if (rc != 0) {
        break;
    }
    if (event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_MESSAGE) {
        printf("%s\n", event.message->message);
        hello_v1_greeter_hello_reply_event_reset(&event);
        continue;
    }
    if (event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_TERMINAL_STATUS) {
        uint32_t status = TREVRPC_STATUS_UNKNOWN;
        trevrpc_inbound_stream_frame_get_status(event.frame, &status);
        rc = status == TREVRPC_STATUS_OK ? 0 : (int)status;
    } else {
        rc = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
    }
    break;
}
hello_v1_greeter_hello_reply_event_reset(&event);
trevrpc_stream_close(stream);
```

Client-streaming and bidirectional calls use generated `_start` helpers and generated request send
helpers. Call `trevrpc_stream_finish_send` when no more requests will be sent. Send and receive may
run on independent threads for a bidirectional stream.

## Opaque inbound ownership

Raw and binding-oriented receive APIs return `trevrpc_inbound_response` or
`trevrpc_inbound_stream_frame`. Access message, body, metadata, status, and kind only through their
getters. Release each shell exactly once.

A body can be transferred without copying:

```c
trevrpc_body_owner* owner = NULL;
if (trevrpc_inbound_response_take_body(response, &owner) == 0 && owner != NULL) {
    trevrpc_bytes_view body;
    trevrpc_body_owner_get_view(owner, &body);
    consume(body.data, body.len);
}
trevrpc_inbound_response_release(response);
trevrpc_body_owner_release(owner);
```

After a successful take, another take and body access on the shell return `-EALREADY`. The body owner
preserves the original allocation pointer, visible interior slice, release callback, and release
context. Release functions accept `NULL`.

## Advanced raw client

The raw API uses the same versioned configuration and opaque ownership contract:

```c
#include <trevrpc_raw.h>

trevrpc_raw_client* client = NULL;
rc = trevrpc_raw_client_connect_v1("127.0.0.1", 50051, &config, NULL, &client);

trevrpc_request rpc_request = {
    .service = "hello.v1.Greeter",
    .service_len = sizeof("hello.v1.Greeter") - 1,
    .method = "SayHello",
    .method_len = sizeof("SayHello") - 1,
    .body = packed,
    .body_len = packed_len,
    .kind = TREVRPC_RPC_KIND_UNARY,
    .version = TREVRPC_WIRE_VERSION,
};
trevrpc_inbound_response* response = NULL;
rc = trevrpc_raw_client_call_request_inbound_v1(client, &rpc_request, NULL, &response);
trevrpc_inbound_response_release(response);
trevrpc_raw_client_close(client);
```

Raw operations never reconnect, retry, replay, or resume an RPC.

## Generated server responders

Unary and client-streaming generated handlers receive a synchronous responder. The responder packs
the borrowed protobuf value before returning, may be invoked at most once, and returns `-EALREADY`
on a repeated invocation.

```c
static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    hello_v1_greeter_say_hello_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    (void)context;
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = request->name != NULL ? request->name : "world";
    hello_v1_greeter_say_hello_response_view response = {
        .message = &reply,
        .status = TREVRPC_STATUS_OK,
    };
    return respond(respond_context, &response);
}
```

Server-streaming and bidirectional handlers send borrowed messages with the generated send helper.
The generated callback submits the final OK status after the handler returns successfully.

## Server lifecycle

The ABI-6 lifecycle is explicit: listen, configure/register, freeze, serve, stop or cancel, wait, and
release.

```c
trevrpc_server_config_v1 config;
trevrpc_server_config_v1_init(&config, sizeof(config));
config.host = "127.0.0.1";
config.port = 50051;

trevrpc_server* server = NULL;
if (trevrpc_server_listen_v1(&config, &server) != 0) {
    return 1;
}
hello_v1_greeter_server implementation = {.say_hello = say_hello /* ... */};
if (hello_v1_greeter_register(server, &implementation) != 0 ||
    trevrpc_server_freeze(server) != 0) {
    trevrpc_server_cancel(server);
    trevrpc_server_release(server);
    return 1;
}

int rc = trevrpc_server_serve(server);
trevrpc_server_stop(server);
trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
trevrpc_server_release(server);
```

`trevrpc_server_stop` requests graceful shutdown. `trevrpc_server_cancel` aborts outstanding work.
Configuration and registration are rejected after freeze.

See [`examples/greeter`](examples/greeter) for complete programs.
