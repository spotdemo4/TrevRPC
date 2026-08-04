#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <errno.h> // IWYU pragma: keep
#include <stdint.h>
#include <stdio.h>
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
    hello_v1_greeter_say_hello_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    if (trevrpc_call_context_cancelled(context)) {
        return -ECANCELED;
    }

    const char* name = request->name != NULL ? request->name : "world";
    Hello__V1__HelloReply* reply = new_reply("Hello, ", name);
    if (reply == NULL) {
        return -ENOMEM;
    }
    hello_v1_greeter_say_hello_response_view response = {
        .message = reply,
        .status = TREVRPC_STATUS_OK,
    };
    int rc = respond(respond_context, &response);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    return rc;
}

static int lots_of_replies(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    trevrpc_stream* stream) {
    (void)user_data;
    const char* name = request->name != NULL ? request->name : "world";

    for (int i = 0; i < 3; i++) {
        if (trevrpc_call_context_cancelled(context)) {
            return -ECANCELED;
        }
        Hello__V1__HelloReply* reply = new_reply("Hello again, ", name);
        if (reply == NULL) {
            return -ENOMEM;
        }
        int rc = hello_v1_greeter_send_hello_v1_hello_reply(stream, reply);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

static int lots_of_greetings(void* user_data,
    const trevrpc_call_context* context,
    trevrpc_stream* stream,
    hello_v1_greeter_lots_of_greetings_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    size_t count = 0;
    hello_v1_greeter_hello_request_request_receiver receiver = HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_RECEIVER_INIT;
    int rc = hello_v1_greeter_hello_request_request_receiver_init(&receiver, stream);
    if (rc != 0) {
        return rc;
    }

    for (;;) {
        if (trevrpc_call_context_cancelled(context)) {
            return -ECANCELED;
        }
        hello_v1_greeter_hello_request_request_event event = HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_INIT;
        rc = hello_v1_greeter_recv_hello_v1_hello_request_request(&receiver, &event);
        if (rc != 0) {
            return rc;
        }
        if (event.kind == HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_END) {
            hello_v1_greeter_hello_request_request_event_reset(&event);
            break;
        }
        if (event.kind != HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_MESSAGE) {
            rc = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
            hello_v1_greeter_hello_request_request_event_reset(&event);
            return rc;
        }
        count++;
        hello_v1_greeter_hello_request_request_event_reset(&event);
    }
    hello_v1_greeter_hello_request_request_receiver_reset(&receiver);

    char count_text[32];
    snprintf(count_text, sizeof(count_text), "%zu", count);
    Hello__V1__HelloReply* reply = new_reply("Received greetings: ", count_text);
    if (reply == NULL) {
        return -ENOMEM;
    }
    hello_v1_greeter_lots_of_greetings_response_view response = {
        .message = reply,
        .status = TREVRPC_STATUS_OK,
    };
    rc = respond(respond_context, &response);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    return rc;
}

static int bidi_hello(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;
    hello_v1_greeter_hello_request_request_receiver receiver = HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_RECEIVER_INIT;
    int rc = hello_v1_greeter_hello_request_request_receiver_init(&receiver, stream);
    if (rc != 0) {
        return rc;
    }

    for (;;) {
        if (trevrpc_call_context_cancelled(context)) {
            return -ECANCELED;
        }
        hello_v1_greeter_hello_request_request_event event = HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_INIT;
        rc = hello_v1_greeter_recv_hello_v1_hello_request_request(&receiver, &event);
        if (rc != 0) {
            return rc;
        }
        if (event.kind == HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_END) {
            hello_v1_greeter_hello_request_request_event_reset(&event);
            break;
        }
        if (event.kind != HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_MESSAGE) {
            rc = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
            hello_v1_greeter_hello_request_request_event_reset(&event);
            return rc;
        }

        const char* name = event.message->name != NULL ? event.message->name : "world";
        Hello__V1__HelloReply* reply = new_reply("Hello from bidi, ", name);
        hello_v1_greeter_hello_request_request_event_reset(&event);
        if (reply == NULL) {
            return -ENOMEM;
        }
        rc = hello_v1_greeter_send_hello_v1_hello_reply(stream, reply);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        if (rc != 0) {
            return rc;
        }
    }
    hello_v1_greeter_hello_request_request_receiver_reset(&receiver);
    return 0;
}

static void release_server(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }
    (void)trevrpc_server_stop(server);
    (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
    (void)trevrpc_server_release(server);
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? (uint16_t)atoi(argv[2]) : 50051;

    trevrpc_server_config_v1 config;
    int rc = trevrpc_server_config_v1_init(&config, sizeof(config));
    if (rc != 0) {
        fprintf(stderr, "config initialization failed: %s\n", trevrpc_error(rc));
        return 1;
    }
    config.host = host;
    config.port = port;
    config.enable_http3 = 1;
    trevrpc_server* server = NULL;
    rc = trevrpc_server_listen_v1(&config, &server);
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
    if (rc == 0) {
        rc = trevrpc_server_freeze(server);
    }
    if (rc != 0) {
        fprintf(stderr, "register failed: %s\n", trevrpc_error(rc));
        release_server(server);
        return 1;
    }

    printf("serving native QUIC, HTTP/3, and WebTransport on %s:%u\n", host, port);
    rc = trevrpc_server_serve(server);
    release_server(server);
    return rc == 0 ? 0 : 1;
}
