#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"

#include <protobuf-c/protobuf-c.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

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

    printf("%s\n", response->message != NULL ? response->message : "");
    hello__v1__hello_reply__free_unpacked(response, NULL);
    trevrpc_client_close(client);
    return 0;
}
