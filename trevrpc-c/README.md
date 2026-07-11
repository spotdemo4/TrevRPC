# trevrpc-c

Minimal unary Greeter flow using generated protobuf-c and TrevRPC C bindings.

## Client

Create a client with `trevrpc_client_connect`, build a protobuf request, and call the generated unary helper:

```c
#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdio.h>

int main(void) {
    trevrpc_config config = trevrpc_default_config();
    trevrpc_client* client = NULL;
    if (trevrpc_client_connect("127.0.0.1", 50051, &config, &client) != 0) {
        return 1;
    }

    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "TrevRPC";

    Hello__V1__HelloReply* reply = NULL;
    int rc = hello_v1_greeter_say_hello(client, &request, &reply);
    if (rc == 0) {
        printf("%s\n", reply->message);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }

    trevrpc_client_close(client);
    return rc == 0 ? 0 : 1;
}
```

## Server

Create a server, register a handler, and read the request passed to the handler:

```c
#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static Hello__V1__HelloReply* new_reply(const char* name) {
    const char* prefix = "Hello, ";
    size_t len = strlen(prefix) + strlen(name) + 1;
    Hello__V1__HelloReply* reply = malloc(sizeof(*reply));
    char* message = malloc(len);
    if (reply == NULL || message == NULL) {
        free(reply);
        free(message);
        return NULL;
    }
    snprintf(message, len, "%s%s", prefix, name);
    hello__v1__hello_reply__init(reply);
    reply->message = message;
    return reply;
}

static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    Hello__V1__HelloReply** response) {
    (void)user_data;
    if (trevrpc_call_context_cancelled(context)) {
        return -1;
    }

    const char* name = request->name != NULL ? request->name : "world";
    *response = new_reply(name);
    return *response == NULL ? -1 : 0;
}

int main(void) {
    trevrpc_server_config config = trevrpc_default_server_config();
    config.host = "127.0.0.1";
    config.port = 50051;
    config.enable_http3 = 1;

    trevrpc_server* server = NULL;
    if (trevrpc_server_listen(&config, &server) != 0) {
        return 1;
    }

    hello_v1_greeter_server greeter = {0};
    greeter.say_hello = say_hello;
    hello_v1_greeter_register(server, &greeter);

    int rc = trevrpc_server_serve(server);
    trevrpc_server_close(server);
    return rc == 0 ? 0 : 1;
}
```
