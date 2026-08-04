#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int print_replies(trevrpc_stream* stream) {
    hello_v1_greeter_hello_reply_receiver receiver = HELLO_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
    int rc = hello_v1_greeter_hello_reply_receiver_init(&receiver, stream);
    if (rc != 0) {
        return rc;
    }

    for (;;) {
        hello_v1_greeter_hello_reply_event event = HELLO_V1_GREETER_HELLO_REPLY_EVENT_INIT;
        rc = hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event);
        if (rc != 0) {
            return rc;
        }
        if (event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_MESSAGE) {
            printf("%s\n", event.message->message != NULL ? event.message->message : "");
            hello_v1_greeter_hello_reply_event_reset(&event);
            continue;
        }
        if (event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_TERMINAL_STATUS) {
            uint32_t status = TREVRPC_STATUS_UNKNOWN;
            rc = trevrpc_inbound_stream_frame_get_status(event.frame, &status);
            hello_v1_greeter_hello_reply_event_reset(&event);
            hello_v1_greeter_hello_reply_receiver_reset(&receiver);
            return rc == 0 && status == TREVRPC_STATUS_OK ? 0 : (rc != 0 ? rc : -1);
        }
        rc = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        hello_v1_greeter_hello_reply_event_reset(&event);
        hello_v1_greeter_hello_reply_receiver_reset(&receiver);
        return rc;
    }
}

static int print_single_reply(trevrpc_stream* stream, const char* label) {
    hello_v1_greeter_hello_reply_receiver receiver = HELLO_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
    int rc = hello_v1_greeter_hello_reply_receiver_init(&receiver, stream);
    if (rc != 0) {
        return rc;
    }

    hello_v1_greeter_hello_reply_event event = HELLO_V1_GREETER_HELLO_REPLY_EVENT_INIT;
    rc = hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event);
    if (rc == 0 && event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_MESSAGE) {
        printf("%s: %s\n", label, event.message->message != NULL ? event.message->message : "");
    } else if (rc == 0) {
        rc = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
    }
    hello_v1_greeter_hello_reply_event_reset(&event);

    if (rc == 0) {
        rc = hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event);
        if (rc == 0 && event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_TERMINAL_STATUS) {
            uint32_t status = TREVRPC_STATUS_UNKNOWN;
            rc = trevrpc_inbound_stream_frame_get_status(event.frame, &status);
            if (rc == 0 && status != TREVRPC_STATUS_OK) {
                rc = -1;
            }
        } else if (rc == 0) {
            rc = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        hello_v1_greeter_hello_reply_event_reset(&event);
    }
    hello_v1_greeter_hello_reply_receiver_reset(&receiver);
    return rc;
}

static int send_name(trevrpc_stream* stream, const char* name) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = (char*)name;
    return hello_v1_greeter_send_hello_v1_hello_request(stream, &request);
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? (uint16_t)atoi(argv[2]) : 50051;
    const char* name = argc > 3 ? argv[3] : "TrevRPC";

    int exit_code = 1;
    trevrpc_client_config_v1 config;
    trevrpc_channel* channel = NULL;
    trevrpc_stream* stream = NULL;
    hello_v1_greeter_say_hello_result unary = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
    int rc = trevrpc_client_config_v1_init(&config, sizeof(config));
    if (rc == 0) {
        rc = trevrpc_channel_connect_v1(host, port, &config, NULL, 5000000000ull, NULL, &channel);
    }
    if (rc != 0) {
        fprintf(stderr, "channel connection failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }
    /* Reconnects serve later calls; in-flight RPCs are never retried or replayed. */

    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = (char*)name;

    rc = hello_v1_greeter_say_hello(channel, &request, &unary);
    if (rc == 0 && unary.kind != HELLO_V1_GREETER_SAY_HELLO_RESULT_SUCCESS) {
        rc = unary.error != 0 ? unary.error : -1;
    }
    if (rc != 0) {
        fprintf(stderr, "SayHello failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    printf("unary: %s\n", unary.response->message != NULL ? unary.response->message : "");
    hello_v1_greeter_say_hello_result_reset(&unary);

    rc = hello_v1_greeter_lots_of_replies(channel, &request, &stream);
    if (rc == 0) {
        printf("server streaming:\n");
        rc = print_replies(stream);
        trevrpc_stream_close(stream);
        stream = NULL;
    }
    if (rc != 0) {
        fprintf(stderr, "LotsOfReplies failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    rc = hello_v1_greeter_lots_of_greetings_start(channel, &stream);
    if (rc == 0) {
        rc = send_name(stream, "Alice");
    }
    if (rc == 0) {
        rc = send_name(stream, "Bob");
    }
    if (rc == 0) {
        rc = trevrpc_stream_finish_send(stream);
    }
    if (rc == 0) {
        rc = print_single_reply(stream, "client streaming");
    }
    if (stream != NULL) {
        trevrpc_stream_close(stream);
        stream = NULL;
    }
    if (rc != 0) {
        fprintf(stderr, "LotsOfGreetings failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    rc = hello_v1_greeter_bidi_hello_start(channel, &stream);
    if (rc == 0) {
        rc = send_name(stream, "Carol");
    }
    if (rc == 0) {
        rc = send_name(stream, "Dave");
    }
    if (rc == 0) {
        rc = trevrpc_stream_finish_send(stream);
    }
    if (rc == 0) {
        printf("bidi streaming:\n");
        rc = print_replies(stream);
    }
    if (stream != NULL) {
        trevrpc_stream_close(stream);
        stream = NULL;
    }
    if (rc != 0) {
        fprintf(stderr, "BidiHello failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    exit_code = 0;

cleanup:
    hello_v1_greeter_say_hello_result_reset(&unary);
    if (stream != NULL) {
        trevrpc_stream_close(stream);
    }
    if (channel != NULL) {
        trevrpc_channel_close(channel);
        trevrpc_channel_release(channel);
    }
    return exit_code;
}
