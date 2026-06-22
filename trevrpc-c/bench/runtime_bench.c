#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"
#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;

int trevrpc_test_server_new(const trevrpc_config* config, trevrpc_server** out_server);
void trevrpc_test_server_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream);

typedef struct trevrpc_msquic_chunk {
    struct trevrpc_msquic_chunk* next;
    size_t len;
    size_t offset;
    uint8_t data[];
} trevrpc_msquic_chunk;

typedef struct trevrpc_msquic_send trevrpc_msquic_send;

struct trevrpc_msquic_send {
    trevrpc_msquic_send* next;
};

struct trevrpc_msquic_stream {
    void* handle;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_chunk* recv_head;
    trevrpc_msquic_chunk* recv_tail;
    bool recv_fin;
    bool send_closed;
    bool shutdown_complete;
    bool closed;
    int err;
    trevrpc_msquic_send* send_pool;
    size_t send_pool_count;
};

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

static void reset_raw_stream(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_chunk* chunk = stream->recv_head;
    while (chunk != NULL) {
        trevrpc_msquic_chunk* next = chunk->next;
        free(chunk);
        chunk = next;
    }
    trevrpc_msquic_send* send = stream->send_pool;
    while (send != NULL) {
        trevrpc_msquic_send* next = send->next;
        free(send);
        send = next;
    }
    pthread_cond_destroy(&stream->cond);
    pthread_mutex_destroy(&stream->mutex);
}

static int init_raw_stream(trevrpc_msquic_stream* stream, const uint8_t* frame, size_t frame_len) {
    memset(stream, 0, sizeof(*stream));
    int err = pthread_mutex_init(&stream->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&stream->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&stream->mutex);
        return -err;
    }

    trevrpc_msquic_chunk* chunk = malloc(sizeof(*chunk) + frame_len);
    if (chunk == NULL) {
        reset_raw_stream(stream);
        return -ENOMEM;
    }
    chunk->next = NULL;
    chunk->len = frame_len;
    chunk->offset = 0;
    memcpy(chunk->data, frame, frame_len);
    stream->recv_head = chunk;
    stream->recv_tail = chunk;
    stream->recv_fin = true;
    return 0;
}

static int unary_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    return trevrpc_response_set_body(response, request->body, request->body_len);
}

static int streaming_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    int err = trevrpc_stream_send_message(stream, request->body, request->body_len);
    if (err != 0) {
        return err;
    }
    return trevrpc_stream_send_status(stream, TREVRPC_STATUS_OK, NULL, 0);
}

static int run_bench_case(
    trevrpc_server* server, const char* name, const uint8_t* frame, size_t frame_len, size_t iterations) {
    uint64_t start = monotonic_nanos();
    for (size_t i = 0; i < iterations; i++) {
        trevrpc_msquic_stream stream;
        int err = init_raw_stream(&stream, frame, frame_len);
        CHECK(err == 0);
        trevrpc_test_server_handle_stream(server, &stream);
        reset_raw_stream(&stream);
    }
    print_rate(name, iterations, monotonic_nanos() - start);
    return 0;
}

int main(int argc, char** argv) {
    size_t iterations = argc > 1 ? (size_t)strtoull(argv[1], NULL, 10) : 10000;
    uint8_t body[256];
    for (size_t i = 0; i < sizeof(body); i++) {
        body[i] = (uint8_t)i;
    }

    trevrpc_server* server = NULL;
    CHECK(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK(trevrpc_server_register_unary(server, "bench.Service", "Unary", unary_handler, NULL) == 0);
    CHECK(trevrpc_server_register_streaming(
              server, "bench.Service", "Streaming", TREVRPC_RPC_KIND_SERVER_STREAMING, streaming_handler, NULL) == 0);

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    CHECK(trevrpc_wire_encode_request("bench.Service",
              "Unary",
              TREVRPC_RPC_KIND_UNARY,
              body,
              sizeof(body),
              NULL,
              0,
              TREVRPC_DEFAULT_MAX_FRAME_SIZE,
              &frame,
              &frame_len) == 0);
    CHECK(run_bench_case(server, "runtime unary dispatch", frame, frame_len, iterations) == 0);
    free(frame);

    CHECK(trevrpc_wire_encode_request("bench.Service",
              "Streaming",
              TREVRPC_RPC_KIND_SERVER_STREAMING,
              body,
              sizeof(body),
              NULL,
              0,
              TREVRPC_DEFAULT_MAX_FRAME_SIZE,
              &frame,
              &frame_len) == 0);
    CHECK(run_bench_case(server, "runtime server-stream dispatch", frame, frame_len, iterations) == 0);

    free(frame);
    trevrpc_server_close(server);
    return 0;
}
