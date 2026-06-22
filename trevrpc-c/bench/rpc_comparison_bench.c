#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "trevrpc_webtransport.h"

#include <errno.h>
#include <pthread.h>
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

int trevrpc_test_server_webtransport_port(trevrpc_server* server, uint16_t* port);

typedef struct serve_args {
    trevrpc_server* server;
    int result;
} serve_args;

typedef struct benchmark_fixture {
    trevrpc_server* server;
    trevrpc_client* native_client;
    trevrpc_client* webtransport_client;
    pthread_t thread;
    bool thread_started;
    serve_args args;
} benchmark_fixture;

typedef int (*benchmark_case)(trevrpc_client* client);

static volatile size_t benchmark_count_sink;
static volatile size_t benchmark_bytes_sink;

static int start_benchmark_server(benchmark_fixture* fixture);

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

    trevrpc_wt_config wt_client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 128,
        .idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS,
    };
    err = trevrpc_client_connect_webtransport(&wt_client_config, &client_config, &fixture->webtransport_client);
    if (err != 0) {
        return err;
    }

    err = warm_client("trevrpc_msquic_native", fixture->native_client);
    if (err == 0) {
        err = warm_client("trevrpc_webtransport", fixture->webtransport_client);
    }
    return err;
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
    server_config.max_streams_per_session = 128;

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
    trevrpc_client_close(fixture->webtransport_client);
    fixture->webtransport_client = NULL;
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
    trevrpc_client_close(fixture->webtransport_client);
    fixture->webtransport_client = NULL;
    trevrpc_server_shutdown(fixture->server);
    if (fixture->thread_started) {
        (void)pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
    }
    trevrpc_server_close(fixture->server);
    fixture->server = NULL;
}

static int run_server_mode(void) {
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
    fflush(stdout);
    while (getchar() != EOF) {
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
        return run_server_mode();
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
        "unary_round_trip/trevrpc_msquic_native", fixture.native_client, benchmark_unary_round_trip, iterations);
    if (err == 0) {
        err = run_benchmark_case("unary_round_trip/trevrpc_webtransport",
            fixture.webtransport_client,
            benchmark_unary_round_trip,
            iterations);
    }
    if (err == 0) {
        err = run_benchmark_case("server_stream_16_messages/trevrpc_msquic_native",
            fixture.native_client,
            benchmark_server_streaming,
            iterations);
    }
    if (err == 0) {
        err = run_benchmark_case("server_stream_16_messages/trevrpc_webtransport",
            fixture.webtransport_client,
            benchmark_server_streaming,
            iterations);
    }
    if (err == 0) {
        err = run_benchmark_case("client_stream_16_messages/trevrpc_msquic_native",
            fixture.native_client,
            benchmark_client_streaming,
            iterations);
    }
    if (err == 0) {
        err = run_benchmark_case("client_stream_16_messages/trevrpc_webtransport",
            fixture.webtransport_client,
            benchmark_client_streaming,
            iterations);
    }
    if (err == 0) {
        err = run_benchmark_case("bidi_stream_16_messages/trevrpc_msquic_native",
            fixture.native_client,
            benchmark_bidi_streaming,
            iterations);
    }
    if (err == 0) {
        err = run_benchmark_case("bidi_stream_16_messages/trevrpc_webtransport",
            fixture.webtransport_client,
            benchmark_bidi_streaming,
            iterations);
    }

    int stop_err = stop_fixture(&fixture);
    if (err == 0 && stop_err != 0) {
        fprintf(stderr, "stop benchmark fixture failed: %s (%d)\n", trevrpc_error(stop_err), stop_err);
        err = stop_err;
    }
    close_fixture(&fixture);
    return err == 0 ? 0 : 1;
}
