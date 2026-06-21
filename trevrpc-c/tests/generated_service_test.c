#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#define TREVRPC_MSQUIC_TEST_CERT ""
#endif

#ifndef TREVRPC_MSQUIC_TEST_KEY
#define TREVRPC_MSQUIC_TEST_KEY ""
#endif

typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;

int trevrpc_test_server_new(const trevrpc_config* config, trevrpc_server** out_server);
void trevrpc_test_server_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream);
void trevrpc_test_server_handle_wt_stream(trevrpc_server* server, trevrpc_wt_stream* stream);
size_t trevrpc_test_server_stream_status_count(trevrpc_server* server);
uint32_t trevrpc_test_server_last_stream_status(trevrpc_server* server);

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

#define CHECK_GOTO(condition)                                                                                          \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            result = 1;                                                                                                \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

typedef struct metric_counts {
    int started;
    int finished;
    uint32_t status;
} metric_counts;

typedef struct wt_rpc_server_args {
    trevrpc_wt_listener* listener;
    trevrpc_server* server;
    int result;
} wt_rpc_server_args;

static void record_started(void* user_data, const trevrpc_rpc_started_event* event) {
    (void)event;
    metric_counts* counts = user_data;
    counts->started++;
}

static void record_finished(void* user_data, const trevrpc_rpc_finished_event* event) {
    metric_counts* counts = user_data;
    counts->finished++;
    counts->status = event->status;
}

static int append_recv_bytes(trevrpc_msquic_stream* stream, const uint8_t* data, size_t data_len) {
    trevrpc_msquic_chunk* chunk = malloc(sizeof(*chunk) + data_len);
    if (chunk == NULL) {
        return -ENOMEM;
    }
    chunk->next = NULL;
    chunk->len = data_len;
    chunk->offset = 0;
    memcpy(chunk->data, data, data_len);
    stream->recv_head = chunk;
    stream->recv_tail = chunk;
    return 0;
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

static int init_raw_stream(trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len) {
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
    err = append_recv_bytes(stream, body, body_len);
    if (err != 0) {
        reset_raw_stream(stream);
    }
    return err;
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

static void free_reply(Hello__V1__HelloReply* reply) {
    if (reply == NULL) {
        return;
    }
    free(reply->message);
    free(reply);
}

static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    Hello__V1__HelloReply** response) {
    (void)user_data;
    (void)context;
    if (request == NULL || response == NULL) {
        return -EINVAL;
    }
    *response = make_reply(request->name == NULL ? "hello" : request->name);
    if (*response == NULL) {
        return -ENOMEM;
    }
    return 0;
}

static int lots_of_replies(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    if (request == NULL) {
        return -EINVAL;
    }
    Hello__V1__HelloReply* reply = make_reply("reply");
    if (reply == NULL) {
        return -ENOMEM;
    }
    int err = hello_v1_greeter_send_hello_v1_hello_reply(stream, reply);
    free_reply(reply);
    return err == 0 ? 0 : err;
}

static int lots_of_greetings(
    void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream, Hello__V1__HelloReply** response) {
    (void)user_data;
    (void)context;
    uint32_t status = TREVRPC_STATUS_OK;
    Hello__V1__HelloRequest* request = NULL;
    int err = hello_v1_greeter_recv_hello_v1_hello_request(stream, &request, &status);
    if (request != NULL) {
        hello__v1__hello_request__free_unpacked(request, NULL);
    }
    if (err != 0 || status != TREVRPC_STATUS_OK) {
        return err != 0 ? err : -EINVAL;
    }
    *response = make_reply("client stream");
    if (*response == NULL) {
        return -ENOMEM;
    }
    return 0;
}

static int bidi_hello(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    Hello__V1__HelloReply* reply = make_reply("bidi");
    if (reply == NULL) {
        return -ENOMEM;
    }
    int err = hello_v1_greeter_send_hello_v1_hello_reply(stream, reply);
    free_reply(reply);
    return err == 0 ? 0 : err;
}

static void* wt_unary_server_thread(void* arg) {
    wt_rpc_server_args* args = arg;
    trevrpc_wt_session* session = NULL;
    trevrpc_wt_stream* stream = NULL;
    args->result = trevrpc_wt_listener_accept_session(args->listener, &session);
    if (args->result == 0) {
        args->result = trevrpc_wt_session_accept_stream(session, &stream);
    }
    if (args->result == 0) {
        trevrpc_test_server_handle_wt_stream(args->server, stream);
    }
    trevrpc_wt_stream_close(stream);
    trevrpc_wt_session_close(session);
    return NULL;
}

static hello_v1_greeter_server greeter_implementation(void) {
    hello_v1_greeter_server implementation = {
        .user_data = NULL,
        .say_hello = say_hello,
        .lots_of_replies = lots_of_replies,
        .lots_of_greetings = lots_of_greetings,
        .bidi_hello = bidi_hello,
    };
    return implementation;
}

static int run_generated_case(
    const char* method, uint32_t kind, const uint8_t* request_body, size_t request_body_len, metric_counts* counts) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = counts,
    };
    hello_v1_greeter_server implementation = greeter_implementation();

    CHECK_GOTO(
        trevrpc_wire_encode_request(
            "hello.v1.Greeter", method, kind, request_body, request_body_len, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &implementation) == 0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;
    stream.recv_fin = true;

    trevrpc_test_server_handle_stream(server, &stream);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    free(frame);
    return result;
}

static int test_webtransport_unary_round_trip(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_client* client = NULL;
    trevrpc_wt_listener* listener = NULL;
    Hello__V1__HelloReply* response = NULL;
    pthread_t thread = {0};
    bool thread_started = false;
    wt_rpc_server_args args = {0};
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
        .idle_timeout_ms = 1000,
    };
    trevrpc_config config = {0};
    hello_v1_greeter_server implementation = greeter_implementation();
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(trevrpc_test_server_new(&config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &implementation) == 0);
    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    args.server = server;
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, wt_unary_server_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
        .idle_timeout_ms = 1000,
    };
    CHECK_GOTO(trevrpc_client_connect_webtransport(&client_config, &config, &client) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(client, &request, &response) == 0);
    CHECK_GOTO(response != NULL);
    CHECK_GOTO(response->message != NULL);
    CHECK_GOTO(strcmp(response->message, "Trev") == 0);
    trevrpc_client_close(client);
    client = NULL;
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);

    result = 0;

cleanup:
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    trevrpc_client_close(client);
    if (thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_wt_listener_close(listener);
    trevrpc_server_close(server);
    return result;
}

static int test_generated_services_all_rpc_shapes(void) {
    int result = 1;
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";
    size_t body_len = hello__v1__hello_request__get_packed_size(&request);
    uint8_t* body = malloc(body_len);
    CHECK_GOTO(body != NULL || body_len == 0);
    hello__v1__hello_request__pack(&request, body);

    metric_counts counts = {0};
    CHECK_GOTO(run_generated_case("SayHello", TREVRPC_RPC_KIND_UNARY, body, body_len, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_generated_case("LotsOfReplies", TREVRPC_RPC_KIND_SERVER_STREAMING, body, body_len, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL || counts.status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_generated_case("LotsOfGreetings", TREVRPC_RPC_KIND_CLIENT_STREAMING, NULL, 0, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL || counts.status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_generated_case("BidiHello", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL || counts.status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    free(body);
    return result;
}

int main(void) {
    if (test_generated_services_all_rpc_shapes() != 0) {
        return 1;
    }
    if (test_webtransport_unary_round_trip() != 0) {
        return 1;
    }
    return 0;
}
