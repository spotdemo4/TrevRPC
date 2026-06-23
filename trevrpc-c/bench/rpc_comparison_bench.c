#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "trevrpc_webtransport.h"

#include <errno.h> // IWYU pragma: keep
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#define TREVRPC_MSQUIC_TEST_CERT ""
#endif

#ifndef TREVRPC_MSQUIC_TEST_KEY
#define TREVRPC_MSQUIC_TEST_KEY ""
#endif

#define BENCHMARK_REQUEST_NAME "TrevRPC benchmark"
#define BENCHMARK_STREAM_MESSAGE_COUNT 16u
#define BENCHMARK_IDLE_TIMEOUT_MS 600000u
#define BENCHMARK_KEEP_ALIVE_MS 5000u
#define BENCHMARK_GRACEFUL_SHUTDOWN_NANOS 1000000000ull
#define BENCHMARK_SPLIT_SERVICE "example.greeter.Greeter"
#define BENCHMARK_METHOD_SAY_HELLO "SayHello"
#define BENCHMARK_METHOD_LOTS_OF_REPLIES "LotsOfReplies"
#define BENCHMARK_METHOD_LOTS_OF_GREETINGS "LotsOfGreetings"
#define BENCHMARK_METHOD_BIDI_HELLO "BidiHello"

int trevrpc_test_server_webtransport_port(trevrpc_server* server, uint16_t* port);

typedef struct serve_args {
    trevrpc_server* server;
    int result;
} serve_args;

typedef struct benchmark_fixture {
    trevrpc_server* server;
    trevrpc_client* native_client;
    pthread_t thread;
    bool thread_started;
    serve_args args;
} benchmark_fixture;

typedef int (*benchmark_case)(trevrpc_client* client);

typedef struct split_benchmark_case {
    const char* name;
    benchmark_case fn;
} split_benchmark_case;

static volatile size_t benchmark_count_sink;
static volatile size_t benchmark_bytes_sink;
static volatile sig_atomic_t split_server_stop_requested;

static int start_benchmark_server(benchmark_fixture* fixture);
static Hello__V1__HelloRequest benchmark_request(void);

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

static Hello__V1__HelloReply* make_reply(const char* message) {
    Hello__V1__HelloReply* reply = malloc(sizeof(*reply));
    if (reply == NULL) {
        return NULL;
    }
    hello__v1__hello_reply__init(reply);
    reply->message = strdup(message);
    if (reply->message == NULL) {
        free(reply);
        return NULL;
    }
    return reply;
}

static Hello__V1__HelloReply* make_count_reply(size_t count) {
    char message[64];
    snprintf(message, sizeof(message), "streamed %zu greetings", count);
    return make_reply(message);
}

static void consume_reply(Hello__V1__HelloReply* reply) {
    if (reply != NULL && reply->message != NULL) {
        benchmark_bytes_sink += strlen(reply->message);
    }
}

static int pack_message(const ProtobufCMessage* message, uint8_t** out_body, size_t* out_body_len) {
    if (message == NULL || out_body == NULL || out_body_len == NULL) {
        return -EINVAL;
    }
    *out_body = NULL;
    *out_body_len = protobuf_c_message_get_packed_size(message);
    if (*out_body_len == 0) {
        return 0;
    }

    *out_body = malloc(*out_body_len);
    if (*out_body == NULL) {
        return -ENOMEM;
    }
    protobuf_c_message_pack(message, *out_body);
    return 0;
}

static int benchmark_request_body(uint8_t** out_body, size_t* out_body_len) {
    Hello__V1__HelloRequest request = benchmark_request();
    return pack_message((const ProtobufCMessage*)&request, out_body, out_body_len);
}

static int split_response_set_reply(trevrpc_response* response, const char* message) {
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = (char*)message;

    uint8_t* body = NULL;
    size_t body_len = 0;
    int err = pack_message((const ProtobufCMessage*)&reply, &body, &body_len);
    if (err == 0) {
        err = trevrpc_response_set_body(response, body, body_len);
    }
    free(body);
    return err;
}

static int split_stream_send_reply(trevrpc_stream* stream, const char* message) {
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = (char*)message;

    uint8_t* body = NULL;
    size_t body_len = 0;
    int err = pack_message((const ProtobufCMessage*)&reply, &body, &body_len);
    if (err == 0) {
        err = trevrpc_stream_send_message(stream, body, body_len);
    }
    free(body);
    return err;
}

static int split_decode_request(const uint8_t* body, size_t body_len, Hello__V1__HelloRequest** out_request) {
    if (out_request == NULL) {
        return -EINVAL;
    }
    *out_request = hello__v1__hello_request__unpack(NULL, body_len, body);
    return *out_request == NULL ? -EINVAL : 0;
}

static int split_recv_request_frame(trevrpc_stream* stream, bool* done) {
    trevrpc_stream_frame* frame = NULL;
    int err = trevrpc_stream_recv(stream, &frame);
    if (err != 0) {
        return err;
    }
    if (done == NULL) {
        trevrpc_stream_frame_free(frame);
        return -EINVAL;
    }
    if (frame == NULL) {
        *done = true;
        return 0;
    }

    if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
        *done = true;
        err = frame->status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
        trevrpc_stream_frame_free(frame);
        return err;
    }
    if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
        trevrpc_stream_frame_free(frame);
        return -EINVAL;
    }

    Hello__V1__HelloRequest* request = NULL;
    err = split_decode_request(frame->body, frame->body_len, &request);
    if (request != NULL) {
        hello__v1__hello_request__free_unpacked(request, NULL);
    }
    trevrpc_stream_frame_free(frame);
    *done = false;
    return err;
}

static int split_say_hello(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    if (request == NULL || response == NULL) {
        return -EINVAL;
    }

    Hello__V1__HelloRequest* decoded = NULL;
    int err = split_decode_request(request->body, request->body_len, &decoded);
    if (err == 0) {
        err = split_response_set_reply(response, decoded->name == NULL ? "" : decoded->name);
    }
    if (decoded != NULL) {
        hello__v1__hello_request__free_unpacked(decoded, NULL);
    }
    return err;
}

static int split_lots_of_replies(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    if (request == NULL) {
        return -EINVAL;
    }

    Hello__V1__HelloRequest* decoded = NULL;
    int err = split_decode_request(request->body, request->body_len, &decoded);
    if (decoded != NULL) {
        hello__v1__hello_request__free_unpacked(decoded, NULL);
    }
    if (err != 0) {
        return err;
    }

    for (size_t i = 0; i < BENCHMARK_STREAM_MESSAGE_COUNT; i++) {
        err = split_stream_send_reply(stream, "server stream");
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int split_lots_of_greetings(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;

    size_t count = 0;
    for (;;) {
        bool done = false;
        int err = split_recv_request_frame(stream, &done);
        if (err != 0) {
            return err;
        }
        if (done) {
            break;
        }
        count++;
    }

    Hello__V1__HelloReply* reply = make_count_reply(count);
    if (reply == NULL) {
        return -ENOMEM;
    }
    int err = split_stream_send_reply(stream, reply->message);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    return err;
}

static int split_bidi_hello(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;

    for (;;) {
        trevrpc_stream_frame* frame = NULL;
        int err = trevrpc_stream_recv(stream, &frame);
        if (err != 0) {
            return err;
        }
        if (frame == NULL) {
            return 0;
        }
        if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
            err = frame->status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
            trevrpc_stream_frame_free(frame);
            return err;
        }
        if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
            trevrpc_stream_frame_free(frame);
            return -EINVAL;
        }

        Hello__V1__HelloRequest* decoded = NULL;
        err = split_decode_request(frame->body, frame->body_len, &decoded);
        if (err == 0) {
            err = split_stream_send_reply(stream, decoded->name == NULL ? "" : decoded->name);
        }
        if (decoded != NULL) {
            hello__v1__hello_request__free_unpacked(decoded, NULL);
        }
        trevrpc_stream_frame_free(frame);
        if (err != 0) {
            return err;
        }
    }
}

static int bench_say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    Hello__V1__HelloReply** response) {
    (void)user_data;
    (void)context;
    if (request == NULL || response == NULL) {
        return -EINVAL;
    }
    *response = make_reply(request->name == NULL ? "" : request->name);
    return *response == NULL ? -ENOMEM : 0;
}

static int bench_lots_of_replies(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;

    for (size_t i = 0; i < BENCHMARK_STREAM_MESSAGE_COUNT; i++) {
        Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
        reply.message = "server stream";
        int err = hello_v1_greeter_send_hello_v1_hello_reply(stream, &reply);
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int bench_lots_of_greetings(
    void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream, Hello__V1__HelloReply** response) {
    (void)user_data;
    (void)context;
    if (response == NULL) {
        return -EINVAL;
    }

    size_t count = 0;
    for (;;) {
        Hello__V1__HelloRequest* request = NULL;
        uint32_t status = TREVRPC_STATUS_OK;
        int err = hello_v1_greeter_recv_hello_v1_hello_request(stream, &request, &status);
        if (err != 0) {
            return err;
        }
        if (request == NULL) {
            if (status != TREVRPC_STATUS_OK) {
                return -EINVAL;
            }
            break;
        }
        hello__v1__hello_request__free_unpacked(request, NULL);
        count++;
    }

    *response = make_count_reply(count);
    return *response == NULL ? -ENOMEM : 0;
}

static int bench_bidi_hello(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;

    for (;;) {
        Hello__V1__HelloRequest* request = NULL;
        uint32_t status = TREVRPC_STATUS_OK;
        int err = hello_v1_greeter_recv_hello_v1_hello_request(stream, &request, &status);
        if (err != 0) {
            return err;
        }
        if (request == NULL) {
            return status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
        }

        Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
        reply.message = request->name == NULL ? "" : request->name;
        err = hello_v1_greeter_send_hello_v1_hello_reply(stream, &reply);
        hello__v1__hello_request__free_unpacked(request, NULL);
        if (err != 0) {
            return err;
        }
    }
}

static const hello_v1_greeter_server BenchmarkGreeterImplementation = {
    .user_data = NULL,
    .say_hello = bench_say_hello,
    .lots_of_replies = bench_lots_of_replies,
    .lots_of_greetings = bench_lots_of_greetings,
    .bidi_hello = bench_bidi_hello,
};

static int register_split_benchmark_service(trevrpc_server* server) {
    int err = trevrpc_server_register_unary(
        server, BENCHMARK_SPLIT_SERVICE, BENCHMARK_METHOD_SAY_HELLO, split_say_hello, NULL);
    if (err != 0) {
        return err;
    }
    err = trevrpc_server_register_streaming(server,
        BENCHMARK_SPLIT_SERVICE,
        BENCHMARK_METHOD_LOTS_OF_REPLIES,
        TREVRPC_RPC_KIND_SERVER_STREAMING,
        split_lots_of_replies,
        NULL);
    if (err != 0) {
        return err;
    }
    err = trevrpc_server_register_streaming(server,
        BENCHMARK_SPLIT_SERVICE,
        BENCHMARK_METHOD_LOTS_OF_GREETINGS,
        TREVRPC_RPC_KIND_CLIENT_STREAMING,
        split_lots_of_greetings,
        NULL);
    if (err != 0) {
        return err;
    }
    return trevrpc_server_register_streaming(server,
        BENCHMARK_SPLIT_SERVICE,
        BENCHMARK_METHOD_BIDI_HELLO,
        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
        split_bidi_hello,
        NULL);
}

static void* serve_thread(void* arg) {
    serve_args* args = arg;
    args->result = trevrpc_server_serve(args->server);
    return NULL;
}

static Hello__V1__HelloRequest benchmark_request(void) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = BENCHMARK_REQUEST_NAME;
    return request;
}

static int benchmark_unary_round_trip(trevrpc_client* client) {
    Hello__V1__HelloRequest request = benchmark_request();
    Hello__V1__HelloReply* response = NULL;
    int err = hello_v1_greeter_say_hello(client, &request, &response);
    if (err == 0 && (response == NULL || response->message == NULL || strcmp(response->message, request.name) != 0)) {
        err = -EINVAL;
    }
    consume_reply(response);
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    return err;
}

static int benchmark_server_streaming(trevrpc_client* client) {
    Hello__V1__HelloRequest request = benchmark_request();
    trevrpc_stream* stream = NULL;
    int err = hello_v1_greeter_lots_of_replies(client, &request, &stream);
    if (err != 0) {
        return err;
    }

    size_t count = 0;
    for (;;) {
        Hello__V1__HelloReply* reply = NULL;
        uint32_t status = TREVRPC_STATUS_OK;
        err = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
        if (err != 0) {
            break;
        }
        if (reply == NULL) {
            err = status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
            break;
        }
        consume_reply(reply);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        count++;
    }

    if (err == 0 && count != BENCHMARK_STREAM_MESSAGE_COUNT) {
        err = -EINVAL;
    }
    benchmark_count_sink += count;
    trevrpc_stream_close(stream);
    return err;
}

static int send_benchmark_requests(trevrpc_stream* stream) {
    Hello__V1__HelloRequest request = benchmark_request();
    for (size_t i = 0; i < BENCHMARK_STREAM_MESSAGE_COUNT; i++) {
        int err = hello_v1_greeter_send_hello_v1_hello_request(stream, &request);
        if (err != 0) {
            return err;
        }
    }
    return trevrpc_stream_finish_send(stream);
}

static int recv_terminal_status(trevrpc_stream* stream) {
    Hello__V1__HelloReply* reply = NULL;
    uint32_t status = TREVRPC_STATUS_OK;
    int err = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        return -EINVAL;
    }
    if (err != 0) {
        return err;
    }
    return status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
}

static int benchmark_client_streaming(trevrpc_client* client) {
    trevrpc_stream* stream = NULL;
    int err = hello_v1_greeter_lots_of_greetings_start(client, &stream);
    if (err == 0) {
        err = send_benchmark_requests(stream);
    }

    Hello__V1__HelloReply* response = NULL;
    uint32_t status = TREVRPC_STATUS_OK;
    if (err == 0) {
        err = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &response, &status);
    }
    if (err == 0 && (response == NULL || status != TREVRPC_STATUS_OK)) {
        err = -EINVAL;
    }
    consume_reply(response);
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    if (err == 0) {
        err = recv_terminal_status(stream);
    }
    trevrpc_stream_close(stream);
    return err;
}

static int benchmark_bidi_streaming(trevrpc_client* client) {
    trevrpc_stream* stream = NULL;
    int err = hello_v1_greeter_bidi_hello_start(client, &stream);
    if (err == 0) {
        err = send_benchmark_requests(stream);
    }

    size_t count = 0;
    while (err == 0) {
        Hello__V1__HelloReply* reply = NULL;
        uint32_t status = TREVRPC_STATUS_OK;
        err = hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status);
        if (err != 0) {
            break;
        }
        if (reply == NULL) {
            err = status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
            break;
        }
        consume_reply(reply);
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        count++;
    }

    if (err == 0 && count != BENCHMARK_STREAM_MESSAGE_COUNT) {
        err = -EINVAL;
    }
    benchmark_count_sink += count;
    trevrpc_stream_close(stream);
    return err;
}

static int split_decode_reply(const uint8_t* body, size_t body_len, Hello__V1__HelloReply** out_reply) {
    if (out_reply == NULL) {
        return -EINVAL;
    }
    *out_reply = hello__v1__hello_reply__unpack(NULL, body_len, body);
    return *out_reply == NULL ? -EINVAL : 0;
}

static int split_recv_reply_frame(trevrpc_stream* stream, bool* done) {
    trevrpc_stream_frame* frame = NULL;
    int err = trevrpc_stream_recv(stream, &frame);
    if (err != 0) {
        return err;
    }
    if (done == NULL) {
        trevrpc_stream_frame_free(frame);
        return -EINVAL;
    }
    if (frame == NULL) {
        *done = true;
        return 0;
    }

    if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
        *done = true;
        err = frame->status == TREVRPC_STATUS_OK ? 0 : -EINVAL;
        trevrpc_stream_frame_free(frame);
        return err;
    }
    if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
        trevrpc_stream_frame_free(frame);
        return -EINVAL;
    }

    Hello__V1__HelloReply* reply = NULL;
    err = split_decode_reply(frame->body, frame->body_len, &reply);
    consume_reply(reply);
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    trevrpc_stream_frame_free(frame);
    *done = false;
    return err;
}

static int split_send_benchmark_requests(trevrpc_stream* stream) {
    for (size_t i = 0; i < BENCHMARK_STREAM_MESSAGE_COUNT; i++) {
        uint8_t* body = NULL;
        size_t body_len = 0;
        int err = benchmark_request_body(&body, &body_len);
        if (err == 0) {
            err = trevrpc_stream_send_message(stream, body, body_len);
        }
        free(body);
        if (err != 0) {
            return err;
        }
    }
    return trevrpc_stream_finish_send(stream);
}

static int split_benchmark_unary_round_trip(trevrpc_client* client) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    trevrpc_response* response = NULL;
    int err = benchmark_request_body(&body, &body_len);
    if (err == 0) {
        err = trevrpc_client_call_unary(
            client, BENCHMARK_SPLIT_SERVICE, BENCHMARK_METHOD_SAY_HELLO, body, body_len, &response);
    }
    free(body);
    if (err == 0 && (response == NULL || response->status != TREVRPC_STATUS_OK)) {
        err = -EINVAL;
    }
    Hello__V1__HelloReply* reply = NULL;
    if (err == 0) {
        err = split_decode_reply(response->body, response->body_len, &reply);
    }
    if (err == 0 && (reply == NULL || reply->message == NULL || strcmp(reply->message, BENCHMARK_REQUEST_NAME) != 0)) {
        err = -EINVAL;
    }
    consume_reply(reply);
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    trevrpc_response_free(response);
    return err;
}

static int split_benchmark_server_streaming(trevrpc_client* client) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    trevrpc_stream* stream = NULL;
    int err = benchmark_request_body(&body, &body_len);
    if (err == 0) {
        err = trevrpc_client_start_stream(client,
            BENCHMARK_SPLIT_SERVICE,
            BENCHMARK_METHOD_LOTS_OF_REPLIES,
            TREVRPC_RPC_KIND_SERVER_STREAMING,
            body,
            body_len,
            &stream);
    }
    free(body);
    if (err != 0) {
        return err;
    }

    size_t count = 0;
    for (;;) {
        bool done = false;
        err = split_recv_reply_frame(stream, &done);
        if (err != 0 || done) {
            break;
        }
        count++;
    }
    if (err == 0 && count != BENCHMARK_STREAM_MESSAGE_COUNT) {
        err = -EINVAL;
    }
    benchmark_count_sink += count;
    trevrpc_stream_close(stream);
    return err;
}

static int split_benchmark_client_streaming(trevrpc_client* client) {
    trevrpc_stream* stream = NULL;
    int err = trevrpc_client_start_stream(client,
        BENCHMARK_SPLIT_SERVICE,
        BENCHMARK_METHOD_LOTS_OF_GREETINGS,
        TREVRPC_RPC_KIND_CLIENT_STREAMING,
        NULL,
        0,
        &stream);
    if (err == 0) {
        err = split_send_benchmark_requests(stream);
    }

    size_t count = 0;
    while (err == 0) {
        bool done = false;
        err = split_recv_reply_frame(stream, &done);
        if (err != 0 || done) {
            break;
        }
        count++;
    }
    if (err == 0 && count != 1) {
        err = -EINVAL;
    }
    trevrpc_stream_close(stream);
    return err;
}

static int split_benchmark_bidi_streaming(trevrpc_client* client) {
    trevrpc_stream* stream = NULL;
    int err = trevrpc_client_start_stream(client,
        BENCHMARK_SPLIT_SERVICE,
        BENCHMARK_METHOD_BIDI_HELLO,
        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
        NULL,
        0,
        &stream);
    if (err == 0) {
        err = split_send_benchmark_requests(stream);
    }

    size_t count = 0;
    while (err == 0) {
        bool done = false;
        err = split_recv_reply_frame(stream, &done);
        if (err != 0 || done) {
            break;
        }
        count++;
    }
    if (err == 0 && count != BENCHMARK_STREAM_MESSAGE_COUNT) {
        err = -EINVAL;
    }
    benchmark_count_sink += count;
    trevrpc_stream_close(stream);
    return err;
}

static int run_benchmark_case(const char* name, trevrpc_client* client, benchmark_case fn, size_t iterations) {
    uint64_t start = monotonic_nanos();
    for (size_t i = 0; i < iterations; i++) {
        int err = fn(client);
        if (err != 0) {
            fprintf(stderr, "%s failed: %s (%d)\n", name, trevrpc_error(err), err);
            return err;
        }
    }
    print_rate(name, iterations, monotonic_nanos() - start);
    return 0;
}

static int warm_client(const char* name, trevrpc_client* client) {
    int err = benchmark_unary_round_trip(client);
    if (err == 0) {
        err = benchmark_server_streaming(client);
    }
    if (err == 0) {
        err = benchmark_client_streaming(client);
    }
    if (err == 0) {
        err = benchmark_bidi_streaming(client);
    }
    if (err != 0) {
        fprintf(stderr, "warm %s failed: %s (%d)\n", name, trevrpc_error(err), err);
    }
    return err;
}

static const split_benchmark_case split_benchmark_cases[] = {
    {"unary_round_trip", split_benchmark_unary_round_trip},
    {"server_stream_16_messages", split_benchmark_server_streaming},
    {"client_stream_16_messages", split_benchmark_client_streaming},
    {"bidi_stream_16_messages", split_benchmark_bidi_streaming},
};

static const split_benchmark_case* find_split_benchmark_case(const char* shape) {
    if (shape == NULL || strcmp(shape, "all") == 0) {
        return NULL;
    }

    for (size_t i = 0; i < sizeof(split_benchmark_cases) / sizeof(split_benchmark_cases[0]); i++) {
        if (strcmp(shape, split_benchmark_cases[i].name) == 0) {
            return &split_benchmark_cases[i];
        }
    }

    return NULL;
}

static int warm_split_client(const char* name, trevrpc_client* client, const split_benchmark_case* only_case) {
    if (only_case != NULL) {
        int err = only_case->fn(client);
        if (err != 0) {
            fprintf(stderr, "warm %s %s failed: %s (%d)\n", name, only_case->name, trevrpc_error(err), err);
        }
        return err;
    }

    for (size_t i = 0; i < sizeof(split_benchmark_cases) / sizeof(split_benchmark_cases[0]); i++) {
        int err = split_benchmark_cases[i].fn(client);
        if (err != 0) {
            fprintf(
                stderr, "warm %s %s failed: %s (%d)\n", name, split_benchmark_cases[i].name, trevrpc_error(err), err);
            return err;
        }
    }

    return 0;
}

static int connect_split_client(const char* transport, const char* host, uint16_t port, trevrpc_client** out_client) {
    trevrpc_config client_config = trevrpc_default_config();
    client_config.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
    client_config.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
    client_config.peer_bidi_stream_count = 128;
    if (strcmp(transport, "msquic") == 0) {
        return trevrpc_client_connect(host, port, &client_config, out_client);
    }
    if (strcmp(transport, "webtransport") == 0) {
        trevrpc_wt_config wt_client_config = {
            .host = host,
            .port = port,
            .path = "/trevrpc",
            .skip_certificate_validation = 1,
            .max_streams_per_session = 128,
            .idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS,
        };
        return trevrpc_client_connect_webtransport(&wt_client_config, &client_config, out_client);
    }
    return -EINVAL;
}

static int run_split_client_mode(
    const char* transport, const char* host, uint16_t port, size_t iterations, const char* shape) {
    const split_benchmark_case* only_case = find_split_benchmark_case(shape);
    if (shape != NULL && strcmp(shape, "all") != 0 && only_case == NULL) {
        fprintf(stderr, "unknown split benchmark shape: %s\n", shape);
        return 2;
    }

    trevrpc_client* client = NULL;
    int err = connect_split_client(transport, host, port, &client);
    if (err != 0) {
        fprintf(stderr, "connect split %s client failed: %s (%d)\n", transport, trevrpc_error(err), err);
        return 1;
    }

    err = warm_split_client(transport, client, only_case);
    if (err == 0 && only_case != NULL) {
        err = run_benchmark_case(only_case->name, client, only_case->fn, iterations);
    }
    for (size_t i = 0;
        err == 0 && only_case == NULL && i < sizeof(split_benchmark_cases) / sizeof(split_benchmark_cases[0]);
        i++) {
        err = run_benchmark_case(split_benchmark_cases[i].name, client, split_benchmark_cases[i].fn, iterations);
    }

    trevrpc_client_close(client);
    return err == 0 ? 0 : 1;
}

static int start_fixture(benchmark_fixture* fixture) {
    memset(fixture, 0, sizeof(*fixture));

    int err = start_benchmark_server(fixture);
    if (err != 0) {
        return err;
    }

    uint16_t port = 0;
    err = trevrpc_test_server_webtransport_port(fixture->server, &port);
    if (err != 0) {
        return err;
    }

    trevrpc_config client_config = trevrpc_default_config();
    client_config.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
    client_config.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
    client_config.peer_bidi_stream_count = 128;
    err = trevrpc_client_connect("127.0.0.1", port, &client_config, &fixture->native_client);
    if (err != 0) {
        return err;
    }

    return warm_client("trevrpc_msquic", fixture->native_client);
}

static int start_benchmark_server(benchmark_fixture* fixture) {
    if (fixture == NULL) {
        return -EINVAL;
    }

    trevrpc_server_config server_config = trevrpc_default_server_config();
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
    server_config.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
    server_config.peer_bidi_stream_count = 128;
    server_config.max_stateless_operations = 1024;
    server_config.max_binding_stateless_operations = 256;
    server_config.webtransport_path = "/trevrpc";
    server_config.max_sessions_per_connection = 16;
    server_config.max_streams_per_session = 65535;

    int err = trevrpc_server_listen(&server_config, &fixture->server);
    if (err != 0) {
        return err;
    }

    trevrpc_server_options options = trevrpc_default_server_options();
    options.graceful_shutdown_timeout_nanos = BENCHMARK_GRACEFUL_SHUTDOWN_NANOS;
    options.stream_idle_timeout_nanos = 0;
    err = trevrpc_server_set_options(fixture->server, &options);
    if (err == 0) {
        err = hello_v1_greeter_register(fixture->server, &BenchmarkGreeterImplementation);
    }
    if (err == 0) {
        err = register_split_benchmark_service(fixture->server);
    }
    if (err != 0) {
        return err;
    }

    fixture->args.server = fixture->server;
    err = pthread_create(&fixture->thread, NULL, serve_thread, &fixture->args);
    if (err != 0) {
        return -err;
    }
    fixture->thread_started = true;
    return 0;
}

static int stop_fixture(benchmark_fixture* fixture) {
    trevrpc_client_close(fixture->native_client);
    fixture->native_client = NULL;
    trevrpc_server_shutdown(fixture->server);
    if (fixture->thread_started) {
        int err = pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
        if (err != 0) {
            return -err;
        }
    }
    return fixture->args.result;
}

static void close_fixture(benchmark_fixture* fixture) {
    trevrpc_client_close(fixture->native_client);
    fixture->native_client = NULL;
    trevrpc_server_shutdown(fixture->server);
    if (fixture->thread_started) {
        (void)pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
    }
    trevrpc_server_close(fixture->server);
    fixture->server = NULL;
}

static void split_server_signal(int signal_number) {
    (void)signal_number;
    split_server_stop_requested = 1;
}

static int run_server_mode(bool split_mode) {
    benchmark_fixture fixture = {0};
    int err = start_benchmark_server(&fixture);
    if (err != 0) {
        fprintf(stderr, "start benchmark server failed: %s (%d)\n", trevrpc_error(err), err);
        close_fixture(&fixture);
        return 1;
    }

    uint16_t port = 0;
    err = trevrpc_test_server_webtransport_port(fixture.server, &port);
    if (err != 0) {
        fprintf(stderr, "read benchmark server port failed: %s (%d)\n", trevrpc_error(err), err);
        close_fixture(&fixture);
        return 1;
    }

    printf("PORT %u\n", port);
    printf("CERT %s\n", TREVRPC_MSQUIC_TEST_CERT);
    fflush(stdout);
    if (split_mode) {
        signal(SIGTERM, split_server_signal);
        signal(SIGINT, split_server_signal);
        while (!split_server_stop_requested) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 100000000L};
            nanosleep(&delay, NULL);
        }
    } else {
        while (getchar() != EOF) {
        }
    }

    int stop_err = stop_fixture(&fixture);
    close_fixture(&fixture);
    if (stop_err != 0) {
        fprintf(stderr, "stop benchmark server failed: %s (%d)\n", trevrpc_error(stop_err), stop_err);
        return 1;
    }
    return 0;
}

int main(int argc, char** argv) {
    if (argc > 1 && strcmp(argv[1], "--serve") == 0) {
        return run_server_mode(false);
    }
    if (argc > 1 && strcmp(argv[1], "--split-serve") == 0) {
        return run_server_mode(true);
    }
    if (argc > 1 && strcmp(argv[1], "--split-client") == 0) {
        if (argc != 6 && argc != 7) {
            fprintf(stderr,
                "usage: %s --split-client msquic|webtransport <host> <port> <iterations> [shape|all]\n",
                argv[0]);
            return 2;
        }
        unsigned long port = strtoul(argv[4], NULL, 10);
        if (port == 0 || port > UINT16_MAX) {
            fprintf(stderr, "invalid split client port: %s\n", argv[4]);
            return 2;
        }
        size_t iterations = (size_t)strtoull(argv[5], NULL, 10);
        CHECK(iterations > 0);
        return run_split_client_mode(argv[2], argv[3], (uint16_t)port, iterations, argc == 7 ? argv[6] : NULL);
    }

    size_t iterations = argc > 1 ? (size_t)strtoull(argv[1], NULL, 10) : 1000;
    CHECK(iterations > 0);

    benchmark_fixture fixture = {0};
    int err = start_fixture(&fixture);
    if (err != 0) {
        fprintf(stderr, "start benchmark fixture failed: %s (%d)\n", trevrpc_error(err), err);
        close_fixture(&fixture);
        return 1;
    }

    err = run_benchmark_case(
        "unary_round_trip/trevrpc_msquic", fixture.native_client, benchmark_unary_round_trip, iterations);
    if (err == 0) {
        err = run_benchmark_case(
            "server_stream_16_messages/trevrpc_msquic", fixture.native_client, benchmark_server_streaming, iterations);
    }
    if (err == 0) {
        err = run_benchmark_case(
            "client_stream_16_messages/trevrpc_msquic", fixture.native_client, benchmark_client_streaming, iterations);
    }
    if (err == 0) {
        err = run_benchmark_case(
            "bidi_stream_16_messages/trevrpc_msquic", fixture.native_client, benchmark_bidi_streaming, iterations);
    }

    int stop_err = stop_fixture(&fixture);
    if (err == 0 && stop_err != 0) {
        fprintf(stderr, "stop benchmark fixture failed: %s (%d)\n", trevrpc_error(stop_err), stop_err);
        err = stop_err;
    }
    close_fixture(&fixture);
    return err == 0 ? 0 : 1;
}
