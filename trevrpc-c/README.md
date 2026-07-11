# trevrpc-c

The examples use the generated protobuf-c and TrevRPC bindings for
[`examples/greeter/greeter.proto`](examples/greeter/greeter.proto). The generated service has
unary, server-streaming, client-streaming, and bidirectional-streaming methods.

## Client

For native QUIC, create a managed client, wait for its asynchronous connection to become ready, and
reuse it across calls. Generated managed helpers are the default client path:

```c
#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdint.h>
#include <stdio.h>

trevrpc_config config = trevrpc_default_config();
trevrpc_managed_client* client = NULL;
int rc = trevrpc_managed_client_create("127.0.0.1", 50051, &config, NULL, &client);
if (rc != 0) {
    return 1;
}

rc = trevrpc_managed_client_wait_ready(client, 5000000000ull, NULL, NULL);

Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
request.name = "TrevRPC";
Hello__V1__HelloReply* reply = NULL;
if (rc == 0) {
    rc = hello_v1_greeter_say_hello_managed(client, &request, &reply);
}
if (reply != NULL) {
    printf("%s\n", reply->message);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
}

trevrpc_managed_client_close(client);
trevrpc_managed_client_release(client);
return rc == 0 ? 0 : 1;
```

Creation is asynchronous. The managed client deep-copies the host and all pointer-backed fields in
`trevrpc_config`; the caller may release or change them after creation. A zero readiness timeout
waits indefinitely, and readiness waits may also receive a `trevrpc_cancellation`.

`trevrpc_managed_client_wait_ready` gates initial calls, but it does not add retry semantics. Each new
operation selects one ready connection generation; the runtime never queues, retries, replays, or
resumes an RPC. A stream stays attached to the generation on which it started and fails naturally if
that connection is lost. Reconnection only makes a later operation possible. New operations return
`TREV_MSQUIC_ERR_CLOSED` immediately while connecting, reconnecting, or closed.

Generated bindings expose managed helpers without changing the existing `trevrpc_client` helpers.
Unary and server-streaming methods add `_managed` and `_managed_with_options` variants. Client- and
bidirectional-streaming methods add `_managed_start` and `_managed_start_with_options` variants.

`trevrpc_managed_client_get_state` reports `CONNECTING`, `READY`, `RECONNECTING`, or `CLOSED` and the
latest successfully installed generation. `trevrpc_managed_client_options_set_lifecycle_callback`
observes state changes, connect failures, actual MsQuic connection shutdown, local and peer path
address changes, resumption-ticket receipt or retention failure, and successful TLS session
resumption. Lifecycle callbacks run serially on a dedicated dispatcher thread and may reenter the
managed API, including `trevrpc_managed_client_close`. The dispatcher queue is bounded at 64 complete
event records. If full, a new event replaces the newest queued event of the same kind; an unmatched
event is dropped. Callbacks therefore receive source-time state and generation snapshots, but slow
callbacks may observe coalescing or dropped event kinds under sustained event load.

Resumption tickets are copied into managed-client memory and applied to the next MsQuic connection.
`RESUMPTION_TICKET_RECEIVED` is emitted only after a nonempty ticket is successfully retained;
`RESUMPTION_TICKET_RETAIN_FAILED` reports `-ENOMEM` (or `-EINVAL` for an invalid transport record).
Servers use resume-only tickets, and TrevRPC never passes `QUIC_SEND_FLAG_ALLOW_0_RTT`, so RPC data is
not sent as 0-RTT. Tickets are not persisted to disk or shared between managed-client instances; if
a ticket is absent, expired, or rejected, MsQuic performs a full handshake. Address-change events are
currently observational and do not expose the platform-specific `QUIC_ADDR` value.

`trevrpc_managed_client_close` is idempotent and initiates shutdown without releasing the handle. It
is safe against concurrently entered managed operations and from lifecycle callbacks. After all
threads that can begin managed API calls have stopped, call `trevrpc_managed_client_release`; release
joins the reconnect worker and lifecycle dispatcher, drains entered operations, and frees the handle.
Release must not run from a lifecycle callback and no new managed API call may begin concurrently
with it. Already-started calls and streams safely retain their generation until they return or are
closed. Every successful create must follow this close-then-release lifecycle, including error paths.

The managed API is additive and uses opaque types, so `TREVRPC_C_ABI_VERSION` remains 4.

### Unary

The generated unary helper returns one decoded response:

```c
Hello__V1__HelloReply* reply = NULL;
int rc = hello_v1_greeter_say_hello_managed(client, &request, &reply);
if (rc == 0) {
    printf("%s\n", reply->message);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
} else {
    fprintf(stderr, "SayHello failed: %s\n", trevrpc_error(rc));
}
```

### Server streaming

Receive messages until the generated receive helper returns `NULL`. The accompanying status is
the terminal RPC status:

```c
trevrpc_stream* stream = NULL;
int rc = hello_v1_greeter_lots_of_replies_managed(client, &request, &stream);
while (rc == 0) {
    uint32_t status = TREVRPC_STATUS_OK;
    Hello__V1__HelloReply* reply = NULL;
    rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
    if (rc != 0 || reply == NULL) {
        if (rc == 0 && status != TREVRPC_STATUS_OK) {
            fprintf(stderr, "LotsOfReplies failed with status %u\n", status);
            rc = (int)status;
        }
        break;
    }

    printf("%s\n", reply->message);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
}
trevrpc_stream_close(stream);
```

### Client streaming

Start the call, send each request, finish the request side, and receive the response followed by
the terminal status:

```c
trevrpc_stream* stream = NULL;
int rc = hello_v1_greeter_lots_of_greetings_managed_start(client, &stream);

Hello__V1__HelloRequest first = HELLO__V1__HELLO_REQUEST__INIT;
first.name = "Alice";
if (rc == 0) {
    rc = hello_v1_greeter_send_hello_v1_hello_request(stream, &first);
}

Hello__V1__HelloRequest second = HELLO__V1__HELLO_REQUEST__INIT;
second.name = "Bob";
if (rc == 0) {
    rc = hello_v1_greeter_send_hello_v1_hello_request(stream, &second);
}
if (rc == 0) {
    rc = trevrpc_stream_finish_send(stream);
}

uint32_t status = TREVRPC_STATUS_OK;
Hello__V1__HelloReply* reply = NULL;
if (rc == 0) {
    rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
}
if (rc == 0 && reply != NULL) {
    printf("%s\n", reply->message);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    reply = NULL;
    rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
}
if (rc == 0 && (reply != NULL || status != TREVRPC_STATUS_OK)) {
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    fprintf(stderr, "LotsOfGreetings ended with status %u\n", status);
    rc = status == TREVRPC_STATUS_OK ? -1 : (int)status;
}
trevrpc_stream_close(stream);
```

### Bidirectional streaming

Requests and responses may be interleaved. This example sends one request and receives its reply
before sending the next request:

```c
trevrpc_stream* stream = NULL;
int rc = hello_v1_greeter_bidi_hello_managed_start(client, &stream);

const char* names[] = {"Alice", "Bob"};
for (size_t i = 0; rc == 0 && i < sizeof(names) / sizeof(names[0]); i++) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = (char*)names[i];
    rc = hello_v1_greeter_send_hello_v1_hello_request(stream, &request);

    uint32_t status = TREVRPC_STATUS_OK;
    Hello__V1__HelloReply* reply = NULL;
    if (rc == 0) {
        rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
    }
    if (rc == 0 && reply != NULL) {
        printf("%s\n", reply->message);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    } else if (rc == 0) {
        rc = -1;
    }
}

if (rc == 0) {
    rc = trevrpc_stream_finish_send(stream);
}
if (rc == 0) {
    uint32_t status = TREVRPC_STATUS_OK;
    Hello__V1__HelloReply* reply = NULL;
    rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
    if (rc == 0 && (reply != NULL || status != TREVRPC_STATUS_OK)) {
        if (reply != NULL) {
            hello__v1__hello_reply__free_unpacked(reply, NULL);
        }
        fprintf(stderr, "BidiHello ended with status %u\n", status);
        rc = status == TREVRPC_STATUS_OK ? -1 : (int)status;
    }
}
trevrpc_stream_close(stream);
```

Use independent send and receive threads when a protocol must continuously read and write large
bidi streams.

### Advanced: established-connection client

Use `trevrpc_client` when the application explicitly wants a low-level, already-established
connection without managed reconnection. The original generated helpers remain available without
the `_managed` or `_managed_start` suffixes:

```c
trevrpc_config config = trevrpc_default_config();
trevrpc_client* client = NULL;
int rc = trevrpc_client_connect("127.0.0.1", 50051, &config, &client);
if (rc != 0) {
    return 1;
}

Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
request.name = "TrevRPC";
Hello__V1__HelloReply* reply = NULL;
rc = hello_v1_greeter_say_hello(client, &request, &reply);
if (reply != NULL) {
    printf("%s\n", reply->message);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
}

trevrpc_client_close(client);
return rc == 0 ? 0 : 1;
```

## Server

Generated server bindings expose one callback for each RPC shape. The examples below use this
helper to allocate responses; protobuf-c owns string fields separately from the message struct.

```c
#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static Hello__V1__HelloReply* new_reply(const char* prefix, const char* value) {
    size_t len = strlen(prefix) + strlen(value) + 1;
    char* message = malloc(len);
    Hello__V1__HelloReply* reply = malloc(sizeof(*reply));
    if (message == NULL || reply == NULL) {
        free(message);
        free(reply);
        return NULL;
    }

    snprintf(message, len, "%s%s", prefix, value);
    hello__v1__hello_reply__init(reply);
    reply->message = message;
    return reply;
}
```

### Unary

```c
static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    Hello__V1__HelloReply** response) {
    (void)user_data;
    if (trevrpc_call_context_cancelled(context)) {
        return -1;
    }

    const char* name = request->name != NULL ? request->name : "world";
    *response = new_reply("Hello, ", name);
    return *response == NULL ? -1 : 0;
}
```

### Server streaming

Send each response and then an explicit terminal status:

```c
static int lots_of_replies(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    trevrpc_stream* stream) {
    (void)user_data;
    const char* name = request->name != NULL ? request->name : "world";

    for (int i = 0; i < 3; i++) {
        if (trevrpc_call_context_cancelled(context)) {
            return -1;
        }
        Hello__V1__HelloReply* reply = new_reply("Hello again, ", name);
        if (reply == NULL) {
            return -1;
        }
        int rc = hello_v1_greeter_send_hello_v1_hello_reply(stream, reply);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        if (rc != 0) {
            return rc;
        }
    }

    return trevrpc_stream_send_status(stream, TREVRPC_STATUS_OK, NULL, 0);
}
```

### Client streaming

Read requests until the receive helper returns `NULL`, then return one response:

```c
static int lots_of_greetings(void* user_data,
    const trevrpc_call_context* context,
    trevrpc_stream* stream,
    Hello__V1__HelloReply** response) {
    (void)user_data;
    size_t count = 0;

    for (;;) {
        if (trevrpc_call_context_cancelled(context)) {
            return -1;
        }
        uint32_t status = TREVRPC_STATUS_OK;
        Hello__V1__HelloRequest* request = NULL;
        int rc = hello_v1_greeter_recv_hello_v1_hello_request(stream, &request, &status);
        if (rc != 0) {
            return rc;
        }
        if (request == NULL) {
            break;
        }
        count++;
        hello__v1__hello_request__free_unpacked(request, NULL);
    }

    char count_text[32];
    snprintf(count_text, sizeof(count_text), "%zu", count);
    *response = new_reply("Received greetings: ", count_text);
    return *response == NULL ? -1 : 0;
}
```

### Bidirectional streaming

Read and respond until the client finishes its request side, then send the terminal status:

```c
static int bidi_hello(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;

    for (;;) {
        if (trevrpc_call_context_cancelled(context)) {
            return -1;
        }
        uint32_t status = TREVRPC_STATUS_OK;
        Hello__V1__HelloRequest* request = NULL;
        int rc = hello_v1_greeter_recv_hello_v1_hello_request(stream, &request, &status);
        if (rc != 0) {
            return rc;
        }
        if (request == NULL) {
            break;
        }

        const char* name = request->name != NULL ? request->name : "world";
        Hello__V1__HelloReply* reply = new_reply("Hello from bidi, ", name);
        hello__v1__hello_request__free_unpacked(request, NULL);
        if (reply == NULL) {
            return -1;
        }
        rc = hello_v1_greeter_send_hello_v1_hello_reply(stream, reply);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        if (rc != 0) {
            return rc;
        }
    }

    return trevrpc_stream_send_status(stream, TREVRPC_STATUS_OK, NULL, 0);
}
```

Register all four handlers before serving:

```c
trevrpc_server_config config = trevrpc_default_server_config();
config.host = "127.0.0.1";
config.port = 50051;
config.enable_http3 = 1;

trevrpc_server* server = NULL;
if (trevrpc_server_listen(&config, &server) != 0) {
    return 1;
}

hello_v1_greeter_server greeter = {
    .say_hello = say_hello,
    .lots_of_replies = lots_of_replies,
    .lots_of_greetings = lots_of_greetings,
    .bidi_hello = bidi_hello,
};
if (hello_v1_greeter_register(server, &greeter) != 0) {
    trevrpc_server_close(server);
    return 1;
}

int rc = trevrpc_server_serve(server);
trevrpc_server_close(server);
```

See [`examples/greeter`](examples/greeter) for complete client and server programs.
