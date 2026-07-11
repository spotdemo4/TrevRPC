#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int print_replies(trevrpc_stream* stream) {
    for (;;) {
        uint32_t status = TREVRPC_STATUS_OK;
        Hello__V1__HelloReply* reply = NULL;
        int rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
        if (rc != 0) {
            return rc;
        }
        if (reply == NULL) {
            return status == TREVRPC_STATUS_OK ? 0 : -1;
        }

        printf("%s\n", reply->message != NULL ? reply->message : "");
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
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
    trevrpc_config config = trevrpc_default_config();
    trevrpc_managed_client* client = NULL;
    trevrpc_stream* stream = NULL;
    Hello__V1__HelloReply* response = NULL;
    int rc = trevrpc_managed_client_create(host, port, &config, NULL, &client);
    if (rc != 0) {
        fprintf(stderr, "client creation failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    rc = trevrpc_managed_client_wait_ready(client, 5000000000ull, NULL, NULL);
    if (rc != 0) {
        fprintf(stderr, "client did not become ready: %s\n", trevrpc_error(rc));
        goto cleanup;
    }
    /* Reconnects serve later calls; in-flight RPCs are never retried or replayed. */

    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = (char*)name;

    rc = hello_v1_greeter_say_hello_managed(client, &request, &response);
    if (rc != 0) {
        fprintf(stderr, "SayHello failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    printf("unary: %s\n", response->message != NULL ? response->message : "");
    hello__v1__hello_reply__free_unpacked(response, NULL);
    response = NULL;

    rc = hello_v1_greeter_lots_of_replies_managed(client, &request, &stream);
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

    stream = NULL;
    rc = hello_v1_greeter_lots_of_greetings_managed_start(client, &stream);
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
        uint32_t status = TREVRPC_STATUS_OK;
        rc = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &response, &status);
        if (rc == 0 && response != NULL) {
            printf("client streaming: %s\n", response->message != NULL ? response->message : "");
            hello__v1__hello_reply__free_unpacked(response, NULL);
            response = NULL;
        }
    }
    if (stream != NULL) {
        trevrpc_stream_close(stream);
        stream = NULL;
    }
    if (rc != 0) {
        fprintf(stderr, "LotsOfGreetings failed: %s\n", trevrpc_error(rc));
        goto cleanup;
    }

    stream = NULL;
    rc = hello_v1_greeter_bidi_hello_managed_start(client, &stream);
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
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    if (stream != NULL) {
        trevrpc_stream_close(stream);
    }
    if (client != NULL) {
        trevrpc_managed_client_close(client);
        trevrpc_managed_client_release(client);
    }
    return exit_code;
}
