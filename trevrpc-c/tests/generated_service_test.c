#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "trevrpc_msquic.h"
#include "trevrpc_raw.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
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

#ifndef TREVRPC_GENERATED_HEADER
#define TREVRPC_GENERATED_HEADER "build/generated-service-test/greeter.trevrpc.h"
#endif

#ifndef TREVRPC_GENERATED_SOURCE
#define TREVRPC_GENERATED_SOURCE "build/generated-service-test/greeter.trevrpc.c"
#endif

typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;

int trevrpc_test_server_new(const trevrpc_config* config, trevrpc_server** out_server);
void trevrpc_test_server_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream);
void trevrpc_test_server_handle_wt_stream(trevrpc_server* server, trevrpc_wt_stream* stream);
trevrpc_wt_session* trevrpc_test_client_webtransport_session(trevrpc_raw_client* client);
int trevrpc_test_server_webtransport_port(trevrpc_server* server, uint16_t* port);
size_t trevrpc_test_server_stream_status_count(trevrpc_server* server);
uint32_t trevrpc_test_server_last_stream_status(trevrpc_server* server);

typedef struct trevrpc_msquic_chunk {
    struct trevrpc_msquic_chunk* next;
    size_t len;
    size_t offset;
    uint8_t data[];
} trevrpc_msquic_chunk;

typedef struct trevrpc_msquic_send trevrpc_msquic_send;
typedef struct trevrpc_msquic_frame trevrpc_msquic_frame;

typedef enum trevrpc_msquic_recv_mode {
    TREV_MSQUIC_RECV_BYTES = 0,
    TREV_MSQUIC_RECV_FRAMES = 1,
} trevrpc_msquic_recv_mode;

struct trevrpc_msquic_send {
    trevrpc_msquic_send* next;
};

struct trevrpc_msquic_frame {
    trevrpc_msquic_frame* next;
    uint8_t* body;
    size_t len;
    intptr_t err;
};

struct trevrpc_msquic_stream {
    void* handle;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_recv_mode recv_mode;
    trevrpc_msquic_chunk* recv_head;
    trevrpc_msquic_chunk* recv_tail;
    size_t recv_buffered;
    trevrpc_msquic_frame* frame_head;
    trevrpc_msquic_frame* frame_tail;
    size_t frame_max_len;
    uint8_t frame_header[4];
    size_t frame_header_len;
    size_t frame_body_len;
    size_t frame_body_offset;
    uint8_t* frame_body;
    size_t frame_skip_remaining;
    bool recv_fin;
    bool send_closed;
    bool send_aborted;
    bool api_closing;
    bool shutdown_complete;
    bool close_pending;
    bool closed;
    bool api_ref_acquired;
    size_t active_send_ops;
    size_t active_handle_ops;
    size_t active_send_completions;
    size_t send_capacity_waiters;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    size_t pending_send_bytes;
    size_t pending_send_count;
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

typedef struct serve_args {
    trevrpc_server* server;
    int result;
} serve_args;

typedef struct wt_serve_fixture {
    trevrpc_server* server;
    trevrpc_raw_client* client;
    pthread_t thread;
    bool thread_started;
    serve_args args;
} wt_serve_fixture;

typedef struct channel_serve_fixture {
    trevrpc_server* server;
    trevrpc_channel* channel;
    pthread_t thread;
    bool thread_started;
    serve_args args;
} channel_serve_fixture;

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
    if (stream->recv_tail != NULL) {
        stream->recv_tail->next = chunk;
    } else {
        stream->recv_head = chunk;
    }
    stream->recv_tail = chunk;
    stream->recv_buffered += data_len;
    return 0;
}

static void reset_raw_stream(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_chunk* chunk = stream->recv_head;
    while (chunk != NULL) {
        trevrpc_msquic_chunk* next = chunk->next;
        free(chunk);
        chunk = next;
    }
    trevrpc_msquic_frame* frame = stream->frame_head;
    while (frame != NULL) {
        trevrpc_msquic_frame* next = frame->next;
        free(frame->body);
        free(frame);
        frame = next;
    }
    free(stream->frame_body);
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

static void* serve_thread(void* arg) {
    serve_args* args = arg;
    args->result = trevrpc_server_serve(args->server);
    return NULL;
}

static const hello_v1_greeter_server GreeterImplementation = {
    .user_data = NULL,
    .say_hello = say_hello,
    .lots_of_replies = lots_of_replies,
    .lots_of_greetings = lots_of_greetings,
    .bidi_hello = bidi_hello,
};

static bool text_file_contains(const char* path, const char* needle) {
    FILE* file = fopen(path, "rb");
    if (file == NULL) {
        return false;
    }
    char line[4096];
    bool found = false;
    while (fgets(line, sizeof(line), file) != NULL) {
        if (strstr(line, needle) != NULL) {
            found = true;
            break;
        }
    }
    fclose(file);
    return found;
}

static int test_generator_channel_output(void) {
    int result = 1;
    const char* header_names[] = {
        "hello_v1_greeter_say_hello(",
        "hello_v1_greeter_say_hello_with_options(",
        "hello_v1_greeter_lots_of_replies(",
        "hello_v1_greeter_lots_of_replies_with_options(",
        "hello_v1_greeter_lots_of_greetings_start(",
        "hello_v1_greeter_lots_of_greetings_start_with_options(",
        "hello_v1_greeter_bidi_hello_start(",
        "hello_v1_greeter_bidi_hello_start_with_options(",
    };
    for (size_t i = 0; i < sizeof(header_names) / sizeof(header_names[0]); i++) {
        CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_HEADER, header_names[i]));
    }
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_channel_call_unary_with_options("));
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_channel_start_stream_with_options("));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_HEADER, "_managed"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_SOURCE, "_managed"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_HEADER, "trevrpc_client"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_client"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_HEADER, "trevrpc_raw_client"));
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "hello_v1_greeter_say_hello_impl("));
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "hello_v1_greeter_lots_of_replies_impl("));

    result = 0;

cleanup:
    return result;
}

static int test_generated_helper_signatures(void) {
    int (*unary)(trevrpc_channel*, const Hello__V1__HelloRequest*, Hello__V1__HelloReply**) =
        hello_v1_greeter_say_hello;
    int (*unary_options)(
        trevrpc_channel*, const Hello__V1__HelloRequest*, const trevrpc_call_options*, Hello__V1__HelloReply**) =
        hello_v1_greeter_say_hello_with_options;
    int (*server_stream)(trevrpc_channel*, const Hello__V1__HelloRequest*, trevrpc_stream**) =
        hello_v1_greeter_lots_of_replies;
    int (*server_stream_options)(
        trevrpc_channel*, const Hello__V1__HelloRequest*, const trevrpc_call_options*, trevrpc_stream**) =
        hello_v1_greeter_lots_of_replies_with_options;
    int (*client_stream)(trevrpc_channel*, trevrpc_stream**) = hello_v1_greeter_lots_of_greetings_start;
    int (*client_stream_options)(trevrpc_channel*, const trevrpc_call_options*, trevrpc_stream**) =
        hello_v1_greeter_lots_of_greetings_start_with_options;
    int (*bidi_stream)(trevrpc_channel*, trevrpc_stream**) = hello_v1_greeter_bidi_hello_start;
    int (*bidi_stream_options)(trevrpc_channel*, const trevrpc_call_options*, trevrpc_stream**) =
        hello_v1_greeter_bidi_hello_start_with_options;

    return unary == NULL || unary_options == NULL || server_stream == NULL || server_stream_options == NULL ||
           client_stream == NULL || client_stream_options == NULL || bidi_stream == NULL ||
           bidi_stream_options == NULL || TREVRPC_C_ABI_VERSION != 5u;
}

static int start_wt_serve_fixture(wt_serve_fixture* fixture, const trevrpc_config* config) {
    memset(fixture, 0, sizeof(*fixture));
    trevrpc_server_config server_config = trevrpc_default_server_config();
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.webtransport_path = "/trevrpc";
    server_config.max_streams_per_session = 8;
    server_config.max_idle_timeout_ms = 1000;
    int err = trevrpc_server_listen(&server_config, &fixture->server);
    if (err != 0) {
        return err;
    }
    err = hello_v1_greeter_register(fixture->server, &GreeterImplementation);
    if (err != 0) {
        return err;
    }
    fixture->args.server = fixture->server;
    err = pthread_create(&fixture->thread, NULL, serve_thread, &fixture->args);
    if (err != 0) {
        return -err;
    }
    fixture->thread_started = true;

    uint16_t port = 0;
    err = trevrpc_test_server_webtransport_port(fixture->server, &port);
    if (err != 0) {
        return err;
    }
    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
        .idle_timeout_ms = 1000,
    };
    return trevrpc_raw_client_connect_webtransport(&client_config, config, &fixture->client);
}

static int stop_wt_serve_fixture(wt_serve_fixture* fixture) {
    trevrpc_raw_client_close(fixture->client);
    fixture->client = NULL;
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

static void close_wt_serve_fixture(wt_serve_fixture* fixture) {
    trevrpc_raw_client_close(fixture->client);
    fixture->client = NULL;
    trevrpc_server_shutdown(fixture->server);
    if (fixture->thread_started) {
        (void)pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
    }
    trevrpc_server_close(fixture->server);
    fixture->server = NULL;
}

static int start_channel_serve_fixture(channel_serve_fixture* fixture) {
    memset(fixture, 0, sizeof(*fixture));
    trevrpc_server_config server_config = trevrpc_default_server_config();
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    int err = trevrpc_server_listen(&server_config, &fixture->server);
    if (err != 0) {
        return err;
    }
    err = hello_v1_greeter_register(fixture->server, &GreeterImplementation);
    if (err != 0) {
        return err;
    }
    fixture->args.server = fixture->server;
    err = pthread_create(&fixture->thread, NULL, serve_thread, &fixture->args);
    if (err != 0) {
        return -err;
    }
    fixture->thread_started = true;

    uint16_t port = 0;
    err = trevrpc_server_port(fixture->server, &port);
    if (err != 0) {
        return err;
    }
    trevrpc_config client_config = trevrpc_default_config();
    client_config.skip_certificate_validation = 1;
    return trevrpc_channel_connect("127.0.0.1", port, &client_config, NULL, 5000000000ull, NULL, &fixture->channel);
}

static int stop_channel_serve_fixture(channel_serve_fixture* fixture) {
    trevrpc_channel_close(fixture->channel);
    trevrpc_channel_release(fixture->channel);
    fixture->channel = NULL;
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

static void close_channel_serve_fixture(channel_serve_fixture* fixture) {
    trevrpc_channel_close(fixture->channel);
    trevrpc_channel_release(fixture->channel);
    fixture->channel = NULL;
    trevrpc_server_shutdown(fixture->server);
    if (fixture->thread_started) {
        (void)pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
    }
    trevrpc_server_close(fixture->server);
    fixture->server = NULL;
}

static int expect_single_stream_reply(trevrpc_stream* stream, const char* expected) {
    int result = 1;
    Hello__V1__HelloReply* reply = NULL;
    uint32_t status = TREVRPC_STATUS_OK;

    if (hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) != 0 || status != TREVRPC_STATUS_OK ||
        reply == NULL || reply->message == NULL || strcmp(reply->message, expected) != 0) {
        goto cleanup;
    }
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    reply = NULL;
    if (hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) != 0 || reply != NULL ||
        status != TREVRPC_STATUS_OK) {
        goto cleanup;
    }
    result = 0;

cleanup:
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    trevrpc_stream_close(stream);
    return result;
}

static int finish_channel_client_stream(trevrpc_stream* stream, const Hello__V1__HelloRequest* request) {
    if (hello_v1_greeter_send_hello_v1_hello_request(stream, request) != 0 || trevrpc_stream_finish_send(stream) != 0) {
        trevrpc_stream_close(stream);
        return 1;
    }
    return expect_single_stream_reply(stream, "client stream");
}

static int finish_channel_bidi_stream(trevrpc_stream* stream) {
    if (trevrpc_stream_finish_send(stream) != 0) {
        trevrpc_stream_close(stream);
        return 1;
    }
    return expect_single_stream_reply(stream, "bidi");
}

static int test_generated_channel_helpers_all_rpc_shapes(void) {
    int result = 1;
    channel_serve_fixture fixture = {0};
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloReply* response = NULL;
    trevrpc_stream* stream = NULL;
    trevrpc_call_options options = trevrpc_default_call_options();
    options.timeout_nanos = 5000000000ull;
    request.name = "channel";

    CHECK_GOTO(start_channel_serve_fixture(&fixture) == 0);

    CHECK_GOTO(hello_v1_greeter_say_hello(fixture.channel, &request, &response) == 0);
    CHECK_GOTO(response != NULL && response->message != NULL && strcmp(response->message, request.name) == 0);
    hello__v1__hello_reply__free_unpacked(response, NULL);
    response = NULL;
    CHECK_GOTO(hello_v1_greeter_say_hello_with_options(fixture.channel, &request, &options, &response) == 0);
    CHECK_GOTO(response != NULL && response->message != NULL && strcmp(response->message, request.name) == 0);
    hello__v1__hello_reply__free_unpacked(response, NULL);
    response = NULL;

    CHECK_GOTO(hello_v1_greeter_lots_of_replies(fixture.channel, &request, &stream) == 0);
    int stream_result = expect_single_stream_reply(stream, "reply");
    stream = NULL;
    CHECK_GOTO(stream_result == 0);
    CHECK_GOTO(hello_v1_greeter_lots_of_replies_with_options(fixture.channel, &request, &options, &stream) == 0);
    stream_result = expect_single_stream_reply(stream, "reply");
    stream = NULL;
    CHECK_GOTO(stream_result == 0);

    CHECK_GOTO(hello_v1_greeter_lots_of_greetings_start(fixture.channel, &stream) == 0);
    stream_result = finish_channel_client_stream(stream, &request);
    stream = NULL;
    CHECK_GOTO(stream_result == 0);
    CHECK_GOTO(hello_v1_greeter_lots_of_greetings_start_with_options(fixture.channel, &options, &stream) == 0);
    stream_result = finish_channel_client_stream(stream, &request);
    stream = NULL;
    CHECK_GOTO(stream_result == 0);

    CHECK_GOTO(hello_v1_greeter_bidi_hello_start(fixture.channel, &stream) == 0);
    stream_result = finish_channel_bidi_stream(stream);
    stream = NULL;
    CHECK_GOTO(stream_result == 0);
    CHECK_GOTO(hello_v1_greeter_bidi_hello_start_with_options(fixture.channel, &options, &stream) == 0);
    stream_result = finish_channel_bidi_stream(stream);
    stream = NULL;
    CHECK_GOTO(stream_result == 0);

    CHECK_GOTO(stop_channel_serve_fixture(&fixture) == 0);
    result = 0;

cleanup:
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    trevrpc_stream_close(stream);
    close_channel_serve_fixture(&fixture);
    return result;
}

static int raw_say_hello(
    trevrpc_raw_client* client, const Hello__V1__HelloRequest* request, Hello__V1__HelloReply** response) {
    *response = NULL;
    size_t body_len = hello__v1__hello_request__get_packed_size(request);
    uint8_t* body = body_len == 0 ? NULL : malloc(body_len);
    if (body_len > 0 && body == NULL) {
        return -ENOMEM;
    }
    hello__v1__hello_request__pack(request, body);
    trevrpc_response* raw_response = NULL;
    int err = trevrpc_raw_client_call_unary(client, "hello.v1.Greeter", "SayHello", body, body_len, &raw_response);
    free(body);
    if (err == 0 && raw_response->status != TREVRPC_STATUS_OK) {
        err = (int)raw_response->status;
    }
    if (err == 0) {
        *response = hello__v1__hello_reply__unpack(NULL, raw_response->body_len, raw_response->body);
        if (*response == NULL) {
            err = TREVRPC_ERR_INVALID_FRAME;
        }
    }
    trevrpc_response_free(raw_response);
    return err;
}

static int raw_lots_of_replies(
    trevrpc_raw_client* client, const Hello__V1__HelloRequest* request, trevrpc_stream** stream) {
    size_t body_len = hello__v1__hello_request__get_packed_size(request);
    uint8_t* body = body_len == 0 ? NULL : malloc(body_len);
    if (body_len > 0 && body == NULL) {
        return -ENOMEM;
    }
    hello__v1__hello_request__pack(request, body);
    int err = trevrpc_raw_client_start_stream(
        client, "hello.v1.Greeter", "LotsOfReplies", TREVRPC_RPC_KIND_SERVER_STREAMING, body, body_len, stream);
    free(body);
    return err;
}

static int raw_lots_of_greetings_start(trevrpc_raw_client* client, trevrpc_stream** stream) {
    return trevrpc_raw_client_start_stream(
        client, "hello.v1.Greeter", "LotsOfGreetings", TREVRPC_RPC_KIND_CLIENT_STREAMING, NULL, 0, stream);
}

static int raw_bidi_hello_start(trevrpc_raw_client* client, trevrpc_stream** stream) {
    return trevrpc_raw_client_start_stream(
        client, "hello.v1.Greeter", "BidiHello", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, stream);
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

    CHECK_GOTO(
        trevrpc_wire_encode_request(
            "hello.v1.Greeter", method, kind, request_body, request_body_len, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
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
    trevrpc_raw_client* client = NULL;
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
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(trevrpc_test_server_new(&config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
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
    CHECK_GOTO(trevrpc_raw_client_connect_webtransport(&client_config, &config, &client) == 0);
    CHECK_GOTO(raw_say_hello(client, &request, &response) == 0);
    CHECK_GOTO(response != NULL);
    CHECK_GOTO(response->message != NULL);
    CHECK_GOTO(strcmp(response->message, "Trev") == 0);
    trevrpc_raw_client_close(client);
    client = NULL;
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);

    result = 0;

cleanup:
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    trevrpc_raw_client_close(client);
    if (thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_wt_listener_close(listener);
    trevrpc_server_close(server);
    return result;
}

static int test_webtransport_serve_loop_unary_shutdown(void) {
    int result = 1;
    Hello__V1__HelloReply* response = NULL;
    wt_serve_fixture fixture = {0};
    trevrpc_config config = {0};
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_say_hello(fixture.client, &request, &response) == 0);
    CHECK_GOTO(response != NULL && response->message != NULL && strcmp(response->message, "Trev") == 0);
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    close_wt_serve_fixture(&fixture);
    return result;
}

static int test_webtransport_serve_loop_server_streaming(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_config config = {0};
    trevrpc_stream* stream = NULL;
    Hello__V1__HelloReply* reply = NULL;
    uint32_t status = TREVRPC_STATUS_OK;
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_lots_of_replies(fixture.client, &request, &stream) == 0);
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) == 0);
    CHECK_GOTO(status == TREVRPC_STATUS_OK);
    CHECK_GOTO(reply != NULL && reply->message != NULL && strcmp(reply->message, "reply") == 0);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    reply = NULL;
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) == 0);
    CHECK_GOTO(reply == NULL);
    CHECK_GOTO(status == TREVRPC_STATUS_OK);
    trevrpc_stream_close(stream);
    stream = NULL;
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    trevrpc_stream_close(stream);
    close_wt_serve_fixture(&fixture);
    return result;
}

static int test_webtransport_serve_loop_client_streaming(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_config config = {0};
    trevrpc_stream* stream = NULL;
    Hello__V1__HelloReply* reply = NULL;
    uint32_t status = TREVRPC_STATUS_OK;
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_lots_of_greetings_start(fixture.client, &stream) == 0);
    CHECK_GOTO(hello_v1_greeter_send_hello_v1_hello_request(stream, &request) == 0);
    CHECK_GOTO(trevrpc_stream_finish_send(stream) == 0);
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) == 0);
    CHECK_GOTO(status == TREVRPC_STATUS_OK);
    CHECK_GOTO(reply != NULL && reply->message != NULL && strcmp(reply->message, "client stream") == 0);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    reply = NULL;
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) == 0);
    CHECK_GOTO(reply == NULL);
    CHECK_GOTO(status == TREVRPC_STATUS_OK);
    trevrpc_stream_close(stream);
    stream = NULL;
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    trevrpc_stream_close(stream);
    close_wt_serve_fixture(&fixture);
    return result;
}

static int test_webtransport_serve_loop_bidi_streaming(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_config config = {0};
    trevrpc_stream* stream = NULL;
    Hello__V1__HelloReply* reply = NULL;
    uint32_t status = TREVRPC_STATUS_OK;

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_bidi_hello_start(fixture.client, &stream) == 0);
    CHECK_GOTO(trevrpc_stream_finish_send(stream) == 0);
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) == 0);
    CHECK_GOTO(status == TREVRPC_STATUS_OK);
    CHECK_GOTO(reply != NULL && reply->message != NULL && strcmp(reply->message, "bidi") == 0);
    hello__v1__hello_reply__free_unpacked(reply, NULL);
    reply = NULL;
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(stream, &reply, &status) == 0);
    CHECK_GOTO(reply == NULL);
    CHECK_GOTO(status == TREVRPC_STATUS_OK);
    trevrpc_stream_close(stream);
    stream = NULL;
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    if (reply != NULL) {
        hello__v1__hello_reply__free_unpacked(reply, NULL);
    }
    trevrpc_stream_close(stream);
    close_wt_serve_fixture(&fixture);
    return result;
}

static int test_webtransport_serve_loop_partial_request_close(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_config config = {0};
    trevrpc_wt_session* session = NULL;
    trevrpc_wt_stream* stream = NULL;
    const uint8_t partial_frame[] = {0x40, 0x10};

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    session = trevrpc_test_client_webtransport_session(fixture.client);
    CHECK_GOTO(session != NULL);
    CHECK_GOTO(trevrpc_wt_session_open_stream(session, &stream) == 0);
    CHECK_GOTO(
        trevrpc_wt_stream_write(stream, partial_frame, sizeof(partial_frame)) == (intptr_t)sizeof(partial_frame));
    trevrpc_wt_stream_close(stream);
    stream = NULL;
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    trevrpc_wt_stream_close(stream);
    close_wt_serve_fixture(&fixture);
    return result;
}

static int test_shared_listener_native_and_webtransport_unary(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_raw_client* native_client = NULL;
    trevrpc_raw_client* wt_client = NULL;
    Hello__V1__HelloReply* native_response = NULL;
    Hello__V1__HelloReply* wt_response = NULL;
    serve_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    trevrpc_server_config server_config = trevrpc_default_server_config();
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    server_config.webtransport_path = "/trevrpc";
    server_config.max_streams_per_session = 8;
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "shared";

    CHECK_GOTO(trevrpc_server_listen(&server_config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    args.server = server;
    CHECK_GOTO(pthread_create(&thread, NULL, serve_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_test_server_webtransport_port(server, &port) == 0);
    trevrpc_config client_config = {
        .skip_certificate_validation = 1,
    };
    CHECK_GOTO(trevrpc_raw_client_connect("127.0.0.1", port, &client_config, &native_client) == 0);
    CHECK_GOTO(raw_say_hello(native_client, &request, &native_response) == 0);
    CHECK_GOTO(
        native_response != NULL && native_response->message != NULL && strcmp(native_response->message, "shared") == 0);

    trevrpc_wt_config wt_client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
        .idle_timeout_ms = 1000,
    };
    CHECK_GOTO(trevrpc_raw_client_connect_webtransport(&wt_client_config, &client_config, &wt_client) == 0);
    CHECK_GOTO(raw_say_hello(wt_client, &request, &wt_response) == 0);
    CHECK_GOTO(wt_response != NULL && wt_response->message != NULL && strcmp(wt_response->message, "shared") == 0);

    trevrpc_raw_client_close(native_client);
    native_client = NULL;
    trevrpc_raw_client_close(wt_client);
    wt_client = NULL;
    trevrpc_server_shutdown(server);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);

    result = 0;

cleanup:
    if (native_response != NULL) {
        hello__v1__hello_reply__free_unpacked(native_response, NULL);
    }
    if (wt_response != NULL) {
        hello__v1__hello_reply__free_unpacked(wt_response, NULL);
    }
    trevrpc_raw_client_close(native_client);
    trevrpc_raw_client_close(wt_client);
    if (thread_started) {
        trevrpc_server_shutdown(server);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_server_close(server);
    return result;
}

static int test_generated_native_unary_stack_and_heap_protobuf_buffers(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_channel* channel = NULL;
    Hello__V1__HelloReply* stack_response = NULL;
    Hello__V1__HelloReply* heap_response = NULL;
    serve_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    trevrpc_server_config server_config = trevrpc_default_server_config();
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    trevrpc_config client_config = {
        .skip_certificate_validation = 1,
    };
    Hello__V1__HelloRequest stack_request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloRequest heap_request = HELLO__V1__HELLO_REQUEST__INIT;
    char heap_name[700];

    memset(heap_name, 'h', sizeof(heap_name) - 1);
    heap_name[sizeof(heap_name) - 1] = '\0';
    stack_request.name = "stack-buffer";
    heap_request.name = heap_name;

    CHECK_GOTO(trevrpc_server_listen(&server_config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    args.server = server;
    CHECK_GOTO(pthread_create(&thread, NULL, serve_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_server_port(server, &port) == 0);
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", port, &client_config, NULL, 5000000000ull, NULL, &channel) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(channel, &stack_request, &stack_response) == 0);
    CHECK_GOTO(stack_response != NULL && stack_response->message != NULL);
    CHECK_GOTO(strcmp(stack_response->message, stack_request.name) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(channel, &heap_request, &heap_response) == 0);
    CHECK_GOTO(heap_response != NULL && heap_response->message != NULL);
    CHECK_GOTO(strcmp(heap_response->message, heap_request.name) == 0);

    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    channel = NULL;
    trevrpc_server_shutdown(server);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);

    result = 0;

cleanup:
    if (stack_response != NULL) {
        hello__v1__hello_reply__free_unpacked(stack_response, NULL);
    }
    if (heap_response != NULL) {
        hello__v1__hello_reply__free_unpacked(heap_response, NULL);
    }
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    if (thread_started) {
        trevrpc_server_shutdown(server);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_server_close(server);
    return result;
}

static int test_generated_native_helper_pending_send_resource_exhausted(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_channel* channel = NULL;
    serve_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    trevrpc_server_config server_config = trevrpc_default_server_config();
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    trevrpc_config client_config = trevrpc_default_config();
    client_config.skip_certificate_validation = 1;
    client_config.max_pending_send_bytes = 4;
    client_config.max_pending_send_count = 1;
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloReply* response = NULL;
    request.name = "tiny-budget";

    CHECK_GOTO(trevrpc_server_listen(&server_config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    args.server = server;
    CHECK_GOTO(pthread_create(&thread, NULL, serve_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_server_port(server, &port) == 0);
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", port, &client_config, NULL, 5000000000ull, NULL, &channel) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(channel, &request, &response) == TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(response == NULL);

    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    channel = NULL;
    trevrpc_server_shutdown(server);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;

    result = 0;

cleanup:
    if (response != NULL) {
        hello__v1__hello_reply__free_unpacked(response, NULL);
    }
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    if (thread_started) {
        trevrpc_server_shutdown(server);
        (void)pthread_join(thread, NULL);
    }
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
    CHECK_GOTO(counts.status == TREVRPC_STATUS_CANCELLED || counts.status == TREVRPC_STATUS_INTERNAL ||
               counts.status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_generated_case("LotsOfGreetings", TREVRPC_RPC_KIND_CLIENT_STREAMING, NULL, 0, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_CANCELLED || counts.status == TREVRPC_STATUS_INTERNAL ||
               counts.status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_generated_case("BidiHello", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_CANCELLED || counts.status == TREVRPC_STATUS_INTERNAL ||
               counts.status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    free(body);
    return result;
}

int main(void) {
    int result = 1;

    if (test_generator_channel_output() != 0) {
        goto cleanup;
    }
    if (test_generated_helper_signatures() != 0) {
        goto cleanup;
    }
    if (test_generated_channel_helpers_all_rpc_shapes() != 0) {
        goto cleanup;
    }
    if (test_generated_services_all_rpc_shapes() != 0) {
        goto cleanup;
    }
    if (test_webtransport_serve_loop_unary_shutdown() != 0) {
        goto cleanup;
    }
    if (test_webtransport_unary_round_trip() != 0) {
        goto cleanup;
    }
    if (test_webtransport_serve_loop_server_streaming() != 0) {
        goto cleanup;
    }
    if (test_webtransport_serve_loop_client_streaming() != 0) {
        goto cleanup;
    }
    if (test_webtransport_serve_loop_bidi_streaming() != 0) {
        goto cleanup;
    }
    if (test_webtransport_serve_loop_partial_request_close() != 0) {
        goto cleanup;
    }
    if (test_shared_listener_native_and_webtransport_unary() != 0) {
        goto cleanup;
    }
    if (test_generated_native_unary_stack_and_heap_protobuf_buffers() != 0) {
        goto cleanup;
    }
    if (test_generated_native_helper_pending_send_resource_exhausted() != 0) {
        goto cleanup;
    }
    result = 0;

cleanup:
    return result;
}
