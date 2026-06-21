#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"
#include "trevrpc_wire_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

static uint64_t monotonic_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void print_rate(const char* name, size_t iterations, uint64_t elapsed_nanos) {
    double seconds = (double)elapsed_nanos / 1000000000.0;
    double ops_per_second = seconds > 0.0 ? (double)iterations / seconds : 0.0;
    printf("%s: %.0f ops/s (%zu iterations in %.3fs)\n", name, ops_per_second, iterations, seconds);
}

int main(int argc, char** argv) {
    size_t iterations = argc > 1 ? (size_t)strtoull(argv[1], NULL, 10) : 100000;
    uint8_t body[256];
    for (size_t i = 0; i < sizeof(body); i++) {
        body[i] = (uint8_t)i;
    }

    uint8_t* request_frame = NULL;
    size_t request_frame_len = 0;
    uint64_t start = monotonic_nanos();
    for (size_t i = 0; i < iterations; i++) {
        free(request_frame);
        request_frame = NULL;
        int err = trevrpc_wire_encode_request("bench.Service",
            "Unary",
            TREVRPC_RPC_KIND_UNARY,
            body,
            sizeof(body),
            NULL,
            0,
            TREVRPC_DEFAULT_MAX_FRAME_SIZE,
            &request_frame,
            &request_frame_len);
        CHECK(err == 0);
    }
    print_rate("wire encode request", iterations, monotonic_nanos() - start);

    start = monotonic_nanos();
    for (size_t i = 0; i < iterations; i++) {
        trevrpc_request request = {0};
        int err = trevrpc_wire_decode_request(request_frame + 4, request_frame_len - 4, &request);
        CHECK(err == 0);
        trevrpc_request_reset(&request);
    }
    print_rate("wire decode request", iterations, monotonic_nanos() - start);

    trevrpc_response response = {0};
    CHECK(trevrpc_response_set_body(&response, body, sizeof(body)) == 0);
    uint8_t* response_frame = NULL;
    size_t response_frame_len = 0;
    start = monotonic_nanos();
    for (size_t i = 0; i < iterations; i++) {
        free(response_frame);
        response_frame = NULL;
        int err = trevrpc_wire_encode_response(
            &response, TREVRPC_DEFAULT_MAX_FRAME_SIZE, &response_frame, &response_frame_len);
        CHECK(err == 0);
    }
    print_rate("wire encode response", iterations, monotonic_nanos() - start);

    start = monotonic_nanos();
    for (size_t i = 0; i < iterations; i++) {
        trevrpc_response* decoded = NULL;
        int err = trevrpc_wire_decode_response(response_frame + 4, response_frame_len - 4, &decoded);
        CHECK(err == 0);
        trevrpc_response_free(decoded);
    }
    print_rate("wire decode response", iterations, monotonic_nanos() - start);

    free(request_frame);
    free(response_frame);
    trevrpc_response_reset(&response);
    return 0;
}
