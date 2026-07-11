#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static Hello__V1__HelloReply* new_reply(const char* prefix, const char* name) {
    size_t len = strlen(prefix) + strlen(name) + 1;
    char* message = malloc(len);
    Hello__V1__HelloReply* reply = malloc(sizeof(*reply));
    if (message == NULL || reply == NULL) {
        free(message);
        free(reply);
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
    *response = new_reply("Hello, ", name);
    return *response == NULL ? -1 : 0;
}

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

static int lots_of_greetings(
    void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream, Hello__V1__HelloReply** response) {
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

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? (uint16_t)atoi(argv[2]) : 50051;

    trevrpc_server_config config = trevrpc_default_server_config();
    config.host = host;
    config.port = port;
    config.enable_http3 = 1;
    trevrpc_server* server = NULL;
    int rc = trevrpc_server_listen(&config, &server);
    if (rc != 0) {
        fprintf(stderr, "listen failed: %s\n", trevrpc_error(rc));
        return 1;
    }

    hello_v1_greeter_server greeter = {0};
    greeter.say_hello = say_hello;
    greeter.lots_of_replies = lots_of_replies;
    greeter.lots_of_greetings = lots_of_greetings;
    greeter.bidi_hello = bidi_hello;
    rc = hello_v1_greeter_register(server, &greeter);
    if (rc != 0) {
        fprintf(stderr, "register failed: %s\n", trevrpc_error(rc));
        trevrpc_server_close(server);
        return 1;
    }

    printf("serving native QUIC, HTTP/3, and WebTransport on %s:%u\n", host, port);
    rc = trevrpc_server_serve(server);
    trevrpc_server_close(server);
    return rc == 0 ? 0 : 1;
}
