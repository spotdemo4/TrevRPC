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

    trevrpc_config config = trevrpc_default_config();
    trevrpc_client* client = NULL;
    int rc = trevrpc_client_connect(host, port, &config, &client);
    if (rc != 0) {
        fprintf(stderr, "connect failed: %s\n", trevrpc_error(rc));
        return 1;
    }

    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = (char*)name;

    Hello__V1__HelloReply* response = NULL;
    rc = hello_v1_greeter_say_hello(client, &request, &response);
    if (rc != 0) {
        fprintf(stderr, "SayHello failed: %s\n", trevrpc_error(rc));
        trevrpc_client_close(client);
        return 1;
    }

    printf("unary: %s\n", response->message != NULL ? response->message : "");
    hello__v1__hello_reply__free_unpacked(response, NULL);

    trevrpc_stream* stream = NULL;
    rc = hello_v1_greeter_lots_of_replies(client, &request, &stream);
    if (rc == 0) {
        printf("server streaming:\n");
        rc = print_replies(stream);
        trevrpc_stream_close(stream);
    }
    if (rc != 0) {
        fprintf(stderr, "LotsOfReplies failed: %s\n", trevrpc_error(rc));
        trevrpc_client_close(client);
        return 1;
    }

    stream = NULL;
    rc = hello_v1_greeter_lots_of_greetings_start(client, &stream);
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
        }
    }
    if (stream != NULL) {
        trevrpc_stream_close(stream);
    }
    if (rc != 0) {
        fprintf(stderr, "LotsOfGreetings failed: %s\n", trevrpc_error(rc));
        trevrpc_client_close(client);
        return 1;
    }

    stream = NULL;
    rc = hello_v1_greeter_bidi_hello_start(client, &stream);
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
    }
    if (rc != 0) {
        fprintf(stderr, "BidiHello failed: %s\n", trevrpc_error(rc));
        trevrpc_client_close(client);
        return 1;
    }

    trevrpc_client_close(client);
    return 0;
}
