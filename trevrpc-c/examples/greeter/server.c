#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    Hello__V1__HelloReply** response) {
    (void)user_data;
    if (trevrpc_call_context_cancelled(context)) {
        return -1;
    }

    const char* name = request->name != NULL ? request->name : "world";
    size_t len = strlen("Hello, ") + strlen(name) + 1;
    char* message = malloc(len);
    Hello__V1__HelloReply* reply = malloc(sizeof(*reply));
    if (message == NULL || reply == NULL) {
        free(message);
        free(reply);
        return -1;
    }

    snprintf(message, len, "Hello, %s", name);
    hello__v1__hello_reply__init(reply);
    reply->message = message;
    *response = reply;
    return 0;
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? (uint16_t)atoi(argv[2]) : 50051;

    trevrpc_config config = trevrpc_default_config();
    trevrpc_server* server = NULL;
    int rc = trevrpc_server_listen(host, port, &config, &server);
    if (rc != 0) {
        fprintf(stderr, "listen failed: %s\n", trevrpc_error(rc));
        return 1;
    }

    hello_v1_greeter_server greeter = {0};
    greeter.say_hello = say_hello;
    rc = hello_v1_greeter_register(server, &greeter);
    if (rc != 0) {
        fprintf(stderr, "register failed: %s\n", trevrpc_error(rc));
        trevrpc_server_close(server);
        return 1;
    }

    printf("serving on %s:%u\n", host, port);
    rc = trevrpc_server_serve(server);
    trevrpc_server_close(server);
    return rc == 0 ? 0 : 1;
}
