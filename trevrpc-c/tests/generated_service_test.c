#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "trevrpc_msquic.h"
#include "trevrpc_msquic_internal.h"
#include "trevrpc_runtime_internal.h"
#include "trevrpc_raw.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

#include "trevrpc_frame_internal.h"

#include <errno.h> // IWYU pragma: keep
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

#ifndef TREVRPC_GENERATED_HEADER
#define TREVRPC_GENERATED_HEADER "build/generated-service-test/greeter.trevrpc.h"
#endif

#ifndef TREVRPC_GENERATED_SOURCE
#define TREVRPC_GENERATED_SOURCE "build/generated-service-test/greeter.trevrpc.c"
#endif

typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;

int trevrpc_test_server_new(const trevrpc_client_config_internal* config, trevrpc_server** out_server);
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
    trevrpc_frame_parser frame_parser;
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

typedef int (*wt_fixture_register_fn)(trevrpc_server* server, void* context);

static int stop_and_release_server(trevrpc_server** server_slot) {
    if (server_slot == NULL || *server_slot == NULL) {
        return 0;
    }

    trevrpc_server* server = *server_slot;
    int result = trevrpc_server_stop(server);
    int err = trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
    if (result == 0) {
        result = err;
    }
    if (err == 0) {
        err = trevrpc_server_release(server);
        if (result == 0) {
            result = err;
        }
        if (err == 0) {
            *server_slot = NULL;
        }
    }
    return result;
}

typedef struct shutdown_action_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t counts[TREV_MSQUIC_TEST_STREAM_EVENT_COUNT];
} shutdown_action_state;

typedef struct binding_unary_state {
    shutdown_action_state* shutdown_actions;
    const uint8_t* expected_request;
    size_t expected_request_len;
    const uint8_t* response_body;
    size_t response_body_len;
    uint32_t response_status;
    bool fail_terminal_send;
    bool request_matched;
    bool full_abort_before_return;
    int called;
    int respond_result;
} binding_unary_state;

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
    trevrpc_frame_parser_reset(&stream->frame_parser);
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
    trevrpc_frame_parser_init(&stream->frame_parser, 0);
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

static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Hello__V1__HelloRequest* request,
    hello_v1_greeter_say_hello_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    (void)context;
    if (request == NULL || respond == NULL) {
        return -EINVAL;
    }
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = request->name == NULL ? "hello" : request->name;
    hello_v1_greeter_say_hello_response_view response = {
        .message = &reply,
        .status = TREVRPC_STATUS_OK,
    };
    return respond(respond_context, &response);
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
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = "reply";
    return hello_v1_greeter_send_hello_v1_hello_reply(stream, &reply);
}

static int lots_of_greetings(void* user_data,
    const trevrpc_call_context* context,
    trevrpc_stream* stream,
    hello_v1_greeter_lots_of_greetings_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    (void)context;
    hello_v1_greeter_hello_request_request_receiver receiver = HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_RECEIVER_INIT;
    hello_v1_greeter_hello_request_request_event event = HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_INIT;
    int err = hello_v1_greeter_hello_request_request_receiver_init(&receiver, stream);
    while (err == 0) {
        err = hello_v1_greeter_recv_hello_v1_hello_request_request(&receiver, &event);
        if (err != 0) {
            break;
        }
        if (event.kind == HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_MESSAGE) {
            hello_v1_greeter_hello_request_request_event_reset(&event);
            continue;
        }
        if (event.kind == HELLO_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_END) {
            hello_v1_greeter_hello_request_request_event_reset(&event);
            break;
        }
        err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        hello_v1_greeter_hello_request_request_event_reset(&event);
        break;
    }
    if (err != 0) {
        return err;
    }
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = "client stream";
    hello_v1_greeter_lots_of_greetings_response_view response = {
        .message = &reply,
        .status = TREVRPC_STATUS_OK,
    };
    return respond(respond_context, &response);
}

static int bidi_hello(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = "bidi";
    return hello_v1_greeter_send_hello_v1_hello_reply(stream, &reply);
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
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_channel_call_request_inbound_v1("));
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_channel_start_stream_request_v1("));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_HEADER, "_managed"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_SOURCE, "_managed"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_HEADER, "trevrpc_client"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_client"));
    CHECK_GOTO(!text_file_contains(TREVRPC_GENERATED_HEADER, "trevrpc_raw_client"));
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "hello_v1_greeter_say_hello_respond("));
    CHECK_GOTO(text_file_contains(TREVRPC_GENERATED_SOURCE, "hello_v1_greeter_lots_of_replies_callback("));

    result = 0;

cleanup:
    return result;
}

static int test_generated_helper_signatures(void) {
    int (*unary)(trevrpc_channel*, const Hello__V1__HelloRequest*, hello_v1_greeter_say_hello_result*) =
        hello_v1_greeter_say_hello;
    int (*unary_options)(trevrpc_channel*,
        const Hello__V1__HelloRequest*,
        const trevrpc_call_options_v1*,
        hello_v1_greeter_say_hello_result*) = hello_v1_greeter_say_hello_with_options;
    int (*server_stream)(trevrpc_channel*, const Hello__V1__HelloRequest*, trevrpc_stream**) =
        hello_v1_greeter_lots_of_replies;
    int (*server_stream_options)(
        trevrpc_channel*, const Hello__V1__HelloRequest*, const trevrpc_call_options_v1*, trevrpc_stream**) =
        hello_v1_greeter_lots_of_replies_with_options;
    int (*client_stream)(trevrpc_channel*, trevrpc_stream**) = hello_v1_greeter_lots_of_greetings_start;
    int (*client_stream_options)(trevrpc_channel*, const trevrpc_call_options_v1*, trevrpc_stream**) =
        hello_v1_greeter_lots_of_greetings_start_with_options;
    int (*bidi_stream)(trevrpc_channel*, trevrpc_stream**) = hello_v1_greeter_bidi_hello_start;
    int (*bidi_stream_options)(trevrpc_channel*, const trevrpc_call_options_v1*, trevrpc_stream**) =
        hello_v1_greeter_bidi_hello_start_with_options;

    return unary == NULL || unary_options == NULL || server_stream == NULL || server_stream_options == NULL ||
           client_stream == NULL || client_stream_options == NULL || bidi_stream == NULL ||
           bidi_stream_options == NULL || TREVRPC_C_ABI_VERSION != 6u;
}

static void record_shutdown_action(trevrpc_msquic_test_stream_event event, void* context) {
    shutdown_action_state* state = context;
    if (event >= TREV_MSQUIC_TEST_STREAM_EVENT_COUNT) {
        return;
    }
    pthread_mutex_lock(&state->mutex);
    state->counts[event]++;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static size_t shutdown_action_count(shutdown_action_state* state, trevrpc_msquic_test_stream_event event) {
    pthread_mutex_lock(&state->mutex);
    size_t count = state->counts[event];
    pthread_mutex_unlock(&state->mutex);
    return count;
}

static bool wait_for_shutdown_action(
    shutdown_action_state* state, trevrpc_msquic_test_stream_event event, size_t minimum_count) {
    struct timespec deadline = {0};
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return false;
    }
    deadline.tv_sec += 5;

    pthread_mutex_lock(&state->mutex);
    while (state->counts[event] < minimum_count) {
        int err = pthread_cond_timedwait(&state->cond, &state->mutex, &deadline);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static int binding_unary_handler(void* user_data, trevrpc_call* call) {
    binding_unary_state* state = user_data;
    const trevrpc_request* request = trevrpc_call_request(call);
    bool request_matched =
        request != NULL && request->body_len == state->expected_request_len &&
        (request->body_len == 0 || memcmp(request->body, state->expected_request, request->body_len) == 0);

    pthread_mutex_lock(&state->shutdown_actions->mutex);
    state->called++;
    state->request_matched = request_matched;
    pthread_mutex_unlock(&state->shutdown_actions->mutex);

    trevrpc_response_view_v1 response;
    int err = trevrpc_response_view_v1_init(&response, sizeof(response));
    if (err == 0) {
        response.status = state->response_status;
        response.body = state->response_body;
        response.body_len = state->response_body_len;
        trevrpc_msquic_test_set_stream_hook(record_shutdown_action, state->shutdown_actions);
        if (state->fail_terminal_send) {
            trevrpc_msquic_test_fail_next_stream_send();
        }
        err = trevrpc_call_respond_borrowed_v1(call, &response);
    }

    pthread_mutex_lock(&state->shutdown_actions->mutex);
    state->respond_result = err;
    state->full_abort_before_return = state->shutdown_actions->counts[TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT] > 0;
    pthread_mutex_unlock(&state->shutdown_actions->mutex);
    return err;
}

static int register_generated_wt_routes(trevrpc_server* server, void* context) {
    (void)context;
    return hello_v1_greeter_register(server, &GreeterImplementation);
}

static int register_binding_unary_route(trevrpc_server* server, void* context) {
    return trevrpc_server_register_call(
        server, "binding.v1.Lifecycle", "Unary", TREVRPC_RPC_KIND_UNARY, binding_unary_handler, context);
}

static int open_binding_unary_request(
    wt_serve_fixture* fixture, const uint8_t* request_body, size_t request_body_len, trevrpc_wt_stream** out_stream) {
    if (fixture == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;

    trevrpc_wt_session* session = trevrpc_test_client_webtransport_session(fixture->client);
    if (session == NULL) {
        return -EINVAL;
    }

    trevrpc_wt_stream* stream = NULL;
    int err = trevrpc_wt_session_open_stream(session, &stream);
    if (err != 0) {
        return err;
    }

    uint8_t* request_frame = NULL;
    size_t request_frame_len = 0;
    err = trevrpc_wire_encode_request("binding.v1.Lifecycle",
        "Unary",
        TREVRPC_RPC_KIND_UNARY,
        request_body,
        request_body_len,
        NULL,
        0,
        4096,
        &request_frame,
        &request_frame_len);
    if (err == 0) {
        intptr_t written = trevrpc_wt_stream_write(stream, request_frame, request_frame_len);
        if (written < 0) {
            err = (int)written;
        } else if ((size_t)written != request_frame_len) {
            err = TREV_WT_ERR_CLOSED;
        }
    }
    free(request_frame);
    if (err != 0) {
        trevrpc_wt_stream_close(stream);
        return err;
    }

    *out_stream = stream;
    return 0;
}

static int read_binding_unary_response(trevrpc_wt_stream* stream, trevrpc_inbound_response** out_response) {
    if (stream == NULL || out_response == NULL) {
        return -EINVAL;
    }
    *out_response = NULL;

    uint8_t* response_frame = NULL;
    size_t response_frame_len = 0;
    intptr_t read_result =
        trevrpc_wt_stream_read_frame_timeout(stream, &response_frame, &response_frame_len, 4096, 5ull * 1000000000ull);
    if (read_result != 1) {
        trevrpc_wt_free(response_frame);
        return read_result < 0 ? (int)read_result : TREV_WT_ERR_CLOSED;
    }

    trevrpc_wire_response_values* values = NULL;
    int err = trevrpc_wire_decode_response(response_frame, response_frame_len, &values);
    trevrpc_wt_free(response_frame);
    if (err == 0) {
        err = trevrpc_inbound_response_create(values, out_response);
    }
    trevrpc_internal_response_free(values);
    return err;
}

static int start_wt_serve_fixture_registered(wt_serve_fixture* fixture,
    const trevrpc_client_config_v1* config,
    wt_fixture_register_fn register_routes,
    void* register_context) {
    memset(fixture, 0, sizeof(*fixture));
    trevrpc_server_config_v1 server_config;
    int err = trevrpc_server_config_v1_init(&server_config, sizeof(server_config));
    if (err != 0) {
        return err;
    }
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.webtransport_path = "/trevrpc";
    server_config.max_streams_per_session = 8;
    server_config.max_idle_timeout_ms = 1000;
    err = trevrpc_server_listen_v1(&server_config, &fixture->server);
    if (err != 0) {
        return err;
    }
    err = register_routes(fixture->server, register_context);
    if (err != 0) {
        return err;
    }
    err = trevrpc_server_freeze(fixture->server);
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
    return trevrpc_raw_client_connect_webtransport_v1(&client_config, config, &fixture->client);
}

static int start_wt_serve_fixture(wt_serve_fixture* fixture, const trevrpc_client_config_v1* config) {
    return start_wt_serve_fixture_registered(fixture, config, register_generated_wt_routes, NULL);
}

static int stop_wt_serve_fixture(wt_serve_fixture* fixture) {
    trevrpc_raw_client_close(fixture->client);
    fixture->client = NULL;
    int result = trevrpc_server_stop(fixture->server);
    if (fixture->thread_started) {
        int err = pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
        if (result == 0 && err != 0) {
            result = -err;
        }
    }
    int err = trevrpc_server_wait_until(fixture->server, TREVRPC_DEADLINE_INFINITE);
    if (result == 0) {
        result = err;
    }
    return result == 0 ? fixture->args.result : result;
}

static int close_wt_serve_fixture(wt_serve_fixture* fixture) {
    trevrpc_raw_client_close(fixture->client);
    fixture->client = NULL;
    int result = trevrpc_server_stop(fixture->server);
    if (fixture->thread_started) {
        int err = pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
        if (result == 0 && err != 0) {
            result = -err;
        }
    }
    int err = stop_and_release_server(&fixture->server);
    return result == 0 ? err : result;
}

static int start_channel_serve_fixture(channel_serve_fixture* fixture) {
    memset(fixture, 0, sizeof(*fixture));
    trevrpc_server_config_v1 server_config;
    int err = trevrpc_server_config_v1_init(&server_config, sizeof(server_config));
    if (err != 0) {
        return err;
    }
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    err = trevrpc_server_listen_v1(&server_config, &fixture->server);
    if (err != 0) {
        return err;
    }
    err = hello_v1_greeter_register(fixture->server, &GreeterImplementation);
    if (err != 0) {
        return err;
    }
    err = trevrpc_server_freeze(fixture->server);
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
    trevrpc_client_config_v1 client_config;
    err = trevrpc_client_config_v1_init(&client_config, sizeof(client_config));
    if (err != 0) {
        return err;
    }
    client_config.skip_certificate_validation = 1;
    return trevrpc_channel_connect_v1("127.0.0.1", port, &client_config, NULL, 5000000000ull, NULL, &fixture->channel);
}

static int stop_channel_serve_fixture(channel_serve_fixture* fixture) {
    trevrpc_channel_close(fixture->channel);
    trevrpc_channel_release(fixture->channel);
    fixture->channel = NULL;
    int result = trevrpc_server_stop(fixture->server);
    if (fixture->thread_started) {
        int err = pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
        if (result == 0 && err != 0) {
            result = -err;
        }
    }
    int err = trevrpc_server_wait_until(fixture->server, TREVRPC_DEADLINE_INFINITE);
    if (result == 0) {
        result = err;
    }
    return result == 0 ? fixture->args.result : result;
}

static int close_channel_serve_fixture(channel_serve_fixture* fixture) {
    trevrpc_channel_close(fixture->channel);
    trevrpc_channel_release(fixture->channel);
    fixture->channel = NULL;
    int result = trevrpc_server_stop(fixture->server);
    if (fixture->thread_started) {
        int err = pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
        if (result == 0 && err != 0) {
            result = -err;
        }
    }
    int err = stop_and_release_server(&fixture->server);
    return result == 0 ? err : result;
}

static int expect_single_stream_reply(trevrpc_stream* stream, const char* expected) {
    int result = 1;
    hello_v1_greeter_hello_reply_receiver receiver = HELLO_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
    hello_v1_greeter_hello_reply_event event = HELLO_V1_GREETER_HELLO_REPLY_EVENT_INIT;
    if (hello_v1_greeter_hello_reply_receiver_init(&receiver, stream) != 0) {
        goto cleanup;
    }
    if (hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event) != 0 ||
        event.kind != HELLO_V1_GREETER_HELLO_REPLY_EVENT_MESSAGE || event.message == NULL ||
        event.message->message == NULL || strcmp(event.message->message, expected) != 0) {
        goto cleanup;
    }
    hello_v1_greeter_hello_reply_event_reset(&event);
    if (hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event) != 0 ||
        event.kind != HELLO_V1_GREETER_HELLO_REPLY_EVENT_TERMINAL_STATUS) {
        goto cleanup;
    }
    uint32_t status = TREVRPC_STATUS_UNKNOWN;
    if (trevrpc_inbound_stream_frame_get_status(event.frame, &status) != 0 || status != TREVRPC_STATUS_OK) {
        goto cleanup;
    }
    result = 0;

cleanup:
    hello_v1_greeter_hello_reply_event_reset(&event);
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
    hello_v1_greeter_say_hello_result response = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
    trevrpc_stream* stream = NULL;
    trevrpc_call_options_v1 options;
    CHECK_GOTO(trevrpc_call_options_v1_init(&options, sizeof(options)) == 0);
    options.timeout_nanos = 5000000000ull;
    request.name = "channel";

    CHECK_GOTO(start_channel_serve_fixture(&fixture) == 0);

    CHECK_GOTO(hello_v1_greeter_say_hello(fixture.channel, &request, &response) == 0);
    CHECK_GOTO(response.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_SUCCESS && response.response != NULL &&
               response.response->message != NULL && strcmp(response.response->message, request.name) == 0);
    hello_v1_greeter_say_hello_result_reset(&response);
    CHECK_GOTO(hello_v1_greeter_say_hello_with_options(fixture.channel, &request, &options, &response) == 0);
    CHECK_GOTO(response.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_SUCCESS && response.response != NULL &&
               response.response->message != NULL && strcmp(response.response->message, request.name) == 0);
    hello_v1_greeter_say_hello_result_reset(&response);

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
    hello_v1_greeter_say_hello_result_reset(&response);
    trevrpc_stream_close(stream);
    if (close_channel_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    return result;
}

static int test_generated_allocator_failures(void) {
    int result = 1;
    channel_serve_fixture fixture = {0};
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    hello_v1_greeter_say_hello_result unary = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
    trevrpc_stream* stream = NULL;
    hello_v1_greeter_hello_reply_receiver receiver = HELLO_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
    hello_v1_greeter_hello_reply_event event = HELLO_V1_GREETER_HELLO_REPLY_EVENT_INIT;
    request.name = "allocation failure";

    CHECK_GOTO(start_channel_serve_fixture(&fixture) == 0);
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(0);
    CHECK_GOTO(hello_v1_greeter_say_hello(fixture.channel, &request, &unary) == 0);
    CHECK_GOTO(unary.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_RUNTIME_ERROR);
    CHECK_GOTO(unary.error == -ENOMEM);
    hello_v1_greeter_say_hello_result_reset(&unary);
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(SIZE_MAX);

    CHECK_GOTO(hello_v1_greeter_lots_of_replies(fixture.channel, &request, &stream) == 0);
    CHECK_GOTO(hello_v1_greeter_hello_reply_receiver_init(&receiver, stream) == 0);
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(0);
    CHECK_GOTO(hello_v1_greeter_recv_hello_v1_hello_reply(&receiver, &event) == 0);
    CHECK_GOTO(event.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_RUNTIME_ERROR);
    CHECK_GOTO(event.error == -ENOMEM);
    hello_v1_greeter_hello_reply_event_reset(&event);
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(SIZE_MAX);
    trevrpc_stream_close(stream);
    stream = NULL;

    CHECK_GOTO(stop_channel_serve_fixture(&fixture) == 0);
    result = 0;

cleanup:
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(SIZE_MAX);
    hello_v1_greeter_say_hello_result_reset(&unary);
    hello_v1_greeter_hello_reply_event_reset(&event);
    trevrpc_stream_close(stream);
    if (close_channel_serve_fixture(&fixture) != 0) {
        result = 1;
    }
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
    trevrpc_request rpc_request = {
        .service = "hello.v1.Greeter",
        .service_len = sizeof("hello.v1.Greeter") - 1,
        .method = "SayHello",
        .method_len = sizeof("SayHello") - 1,
        .body = body,
        .body_len = body_len,
        .kind = TREVRPC_RPC_KIND_UNARY,
        .version = TREVRPC_WIRE_VERSION,
    };
    trevrpc_inbound_response* raw_response = NULL;
    int err = trevrpc_raw_client_call_request_inbound_v1(client, &rpc_request, NULL, &raw_response);
    free(body);
    uint32_t status = TREVRPC_STATUS_UNKNOWN;
    if (err == 0) {
        err = trevrpc_inbound_response_get_status(raw_response, &status);
    }
    if (err == 0 && status != TREVRPC_STATUS_OK) {
        err = (int)status;
    }
    trevrpc_bytes_view body_view = {0};
    if (err == 0) {
        err = trevrpc_inbound_response_get_body(raw_response, &body_view);
    }
    if (err == 0) {
        *response = hello__v1__hello_reply__unpack(NULL, body_view.len, body_view.data);
        if (*response == NULL) {
            err = TREVRPC_ERR_INVALID_FRAME;
        }
    }
    trevrpc_inbound_response_release(raw_response);
    return err;
}

static int raw_start_stream(trevrpc_raw_client* client,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** stream) {
    trevrpc_request request = {
        .service = "hello.v1.Greeter",
        .service_len = sizeof("hello.v1.Greeter") - 1,
        .method = method,
        .method_len = strlen(method),
        .body = body,
        .body_len = body_len,
        .kind = kind,
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_start_stream_request_v1(client, &request, NULL, stream);
}

static int raw_lots_of_replies(
    trevrpc_raw_client* client, const Hello__V1__HelloRequest* request, trevrpc_stream** stream) {
    size_t body_len = hello__v1__hello_request__get_packed_size(request);
    uint8_t* body = body_len == 0 ? NULL : malloc(body_len);
    if (body_len > 0 && body == NULL) {
        return -ENOMEM;
    }
    hello__v1__hello_request__pack(request, body);
    int err = raw_start_stream(client, "LotsOfReplies", TREVRPC_RPC_KIND_SERVER_STREAMING, body, body_len, stream);
    free(body);
    if (err == 0) {
        err = trevrpc_stream_finish_send(*stream);
    }
    return err;
}

static int raw_lots_of_greetings_start(trevrpc_raw_client* client, trevrpc_stream** stream) {
    return raw_start_stream(client, "LotsOfGreetings", TREVRPC_RPC_KIND_CLIENT_STREAMING, NULL, 0, stream);
}

static int raw_bidi_hello_start(trevrpc_raw_client* client, trevrpc_stream** stream) {
    return raw_start_stream(client, "BidiHello", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, stream);
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
    if (stop_and_release_server(&server) != 0) {
        result = 1;
    }
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
    trevrpc_client_config_v1 config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    CHECK_GOTO(trevrpc_server_freeze(server) == 0);
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
    CHECK_GOTO(trevrpc_raw_client_connect_webtransport_v1(&client_config, &config, &client) == 0);
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
    if (stop_and_release_server(&server) != 0) {
        result = 1;
    }
    return result;
}

static int test_webtransport_serve_loop_unary_shutdown(void) {
    int result = 1;
    Hello__V1__HelloReply* response = NULL;
    wt_serve_fixture fixture = {0};
    trevrpc_client_config_v1 config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
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
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    return result;
}

static int test_binding_webtransport_unary_graceful_cleanup(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_client_config_v1 config;
    trevrpc_wt_stream* client_stream = NULL;
    trevrpc_inbound_response* response = NULL;
    shutdown_action_state actions = {0};
    bool actions_initialized = false;
    const uint8_t request_body[] = {'b', 'i', 'n', 'd', 'i', 'n', 'g'};
    const uint8_t response_body[] = {'r', 'e', 's', 'p', 'o', 'n', 's', 'e'};
    binding_unary_state state = {
        .shutdown_actions = &actions,
        .expected_request = request_body,
        .expected_request_len = sizeof(request_body),
        .response_body = response_body,
        .response_body_len = sizeof(response_body),
        .response_status = TREVRPC_STATUS_OK,
    };

    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
    CHECK_GOTO(pthread_mutex_init(&actions.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&actions.cond, NULL) == 0);
    actions_initialized = true;
    CHECK_GOTO(start_wt_serve_fixture_registered(&fixture, &config, register_binding_unary_route, &state) == 0);
    CHECK_GOTO(open_binding_unary_request(&fixture, request_body, sizeof(request_body), &client_stream) == 0);
    CHECK_GOTO(read_binding_unary_response(client_stream, &response) == 0);
    uint32_t response_status = TREVRPC_STATUS_UNKNOWN;
    trevrpc_bytes_view response_view = {0};
    CHECK_GOTO(response != NULL && trevrpc_inbound_response_get_status(response, &response_status) == 0 &&
               response_status == TREVRPC_STATUS_OK);
    CHECK_GOTO(trevrpc_inbound_response_get_body(response, &response_view) == 0);
    CHECK_GOTO(response_view.len == sizeof(response_body));
    CHECK_GOTO(memcmp(response_view.data, response_body, sizeof(response_body)) == 0);
    trevrpc_inbound_response_release(response);
    response = NULL;
    CHECK_GOTO(wait_for_shutdown_action(&actions, TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED, 1));

    pthread_mutex_lock(&actions.mutex);
    bool handler_ok = state.called == 1 && state.request_matched && state.respond_result == 0;
    pthread_mutex_unlock(&actions.mutex);
    CHECK_GOTO(handler_ok);
    CHECK_GOTO(shutdown_action_count(&actions, TREV_MSQUIC_TEST_STREAM_SHUTDOWN_GRACEFUL) > 0);
    CHECK_GOTO(shutdown_action_count(&actions, TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE) > 0);
    CHECK_GOTO(shutdown_action_count(&actions, TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT) == 0);
    CHECK_GOTO(shutdown_action_count(&actions, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED) > 0);

    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_wt_stream_close(client_stream);
    client_stream = NULL;
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);
    result = 0;

cleanup:
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_inbound_response_release(response);
    trevrpc_wt_stream_close(client_stream);
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    if (actions_initialized) {
        pthread_cond_destroy(&actions.cond);
        pthread_mutex_destroy(&actions.mutex);
    }
    return result;
}

static int test_binding_webtransport_unary_failed_terminal_submission_aborts(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_client_config_v1 config;
    trevrpc_wt_stream* client_stream = NULL;
    trevrpc_inbound_response* response = NULL;
    shutdown_action_state actions = {0};
    bool actions_initialized = false;
    const uint8_t request_body[] = {'f', 'a', 'i', 'l'};
    const uint8_t response_body[] = {'n', 'o', 't', '-', 's', 'e', 'n', 't'};
    binding_unary_state state = {
        .shutdown_actions = &actions,
        .expected_request = request_body,
        .expected_request_len = sizeof(request_body),
        .response_body = response_body,
        .response_body_len = sizeof(response_body),
        .response_status = TREVRPC_STATUS_OK,
        .fail_terminal_send = true,
    };

    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
    CHECK_GOTO(pthread_mutex_init(&actions.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&actions.cond, NULL) == 0);
    actions_initialized = true;
    CHECK_GOTO(start_wt_serve_fixture_registered(&fixture, &config, register_binding_unary_route, &state) == 0);
    CHECK_GOTO(open_binding_unary_request(&fixture, request_body, sizeof(request_body), &client_stream) == 0);
    int call_err = read_binding_unary_response(client_stream, &response);
    CHECK_GOTO(call_err != 0);
    CHECK_GOTO(wait_for_shutdown_action(&actions, TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT, 1));
    CHECK_GOTO(wait_for_shutdown_action(&actions, TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED, 1));

    pthread_mutex_lock(&actions.mutex);
    bool handler_failed =
        state.called == 1 && state.request_matched && state.respond_result != 0 && state.full_abort_before_return;
    pthread_mutex_unlock(&actions.mutex);
    CHECK_GOTO(handler_failed);
    CHECK_GOTO(shutdown_action_count(&actions, TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT) > 0);
    CHECK_GOTO(shutdown_action_count(&actions, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED) > 0);

    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_wt_stream_close(client_stream);
    client_stream = NULL;
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);
    result = 0;

cleanup:
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_inbound_response_release(response);
    trevrpc_wt_stream_close(client_stream);
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    if (actions_initialized) {
        pthread_cond_destroy(&actions.cond);
        pthread_mutex_destroy(&actions.mutex);
    }
    return result;
}

static int test_webtransport_serve_loop_server_streaming(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_stream* stream = NULL;
    trevrpc_client_config_v1 config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_lots_of_replies(fixture.client, &request, &stream) == 0);
    int stream_result = expect_single_stream_reply(stream, "reply");
    stream = NULL;
    CHECK_GOTO(stream_result == 0);
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    trevrpc_stream_close(stream);
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    return result;
}

static int test_webtransport_serve_loop_client_streaming(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_stream* stream = NULL;
    trevrpc_client_config_v1 config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = "Trev";

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_lots_of_greetings_start(fixture.client, &stream) == 0);
    CHECK_GOTO(hello_v1_greeter_send_hello_v1_hello_request(stream, &request) == 0);
    CHECK_GOTO(trevrpc_stream_finish_send(stream) == 0);
    int stream_result = expect_single_stream_reply(stream, "client stream");
    stream = NULL;
    CHECK_GOTO(stream_result == 0);
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    trevrpc_stream_close(stream);
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    return result;
}

static int test_webtransport_serve_loop_bidi_streaming(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_stream* stream = NULL;
    trevrpc_client_config_v1 config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);

    CHECK_GOTO(start_wt_serve_fixture(&fixture, &config) == 0);
    CHECK_GOTO(raw_bidi_hello_start(fixture.client, &stream) == 0);
    CHECK_GOTO(trevrpc_stream_finish_send(stream) == 0);
    int stream_result = expect_single_stream_reply(stream, "bidi");
    stream = NULL;
    CHECK_GOTO(stream_result == 0);
    CHECK_GOTO(stop_wt_serve_fixture(&fixture) == 0);

    result = 0;

cleanup:
    trevrpc_stream_close(stream);
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
    return result;
}

static int test_webtransport_serve_loop_partial_request_close(void) {
    int result = 1;
    wt_serve_fixture fixture = {0};
    trevrpc_wt_session* session = NULL;
    trevrpc_wt_stream* stream = NULL;
    trevrpc_client_config_v1 config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&config, sizeof(config)) == 0);
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
    if (close_wt_serve_fixture(&fixture) != 0) {
        result = 1;
    }
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
    trevrpc_server_config_v1 server_config;
    int err = trevrpc_server_config_v1_init(&server_config, sizeof(server_config));
    if (err != 0) {
        return err;
    }
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

    CHECK_GOTO(trevrpc_server_listen_v1(&server_config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    CHECK_GOTO(trevrpc_server_freeze(server) == 0);
    args.server = server;
    CHECK_GOTO(pthread_create(&thread, NULL, serve_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_test_server_webtransport_port(server, &port) == 0);
    trevrpc_client_config_v1 client_config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&client_config, sizeof(client_config)) == 0);
    client_config.skip_certificate_validation = 1;
    CHECK_GOTO(trevrpc_raw_client_connect_v1("127.0.0.1", port, &client_config, NULL, &native_client) == 0);
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
    CHECK_GOTO(trevrpc_raw_client_connect_webtransport_v1(&wt_client_config, &client_config, &wt_client) == 0);
    CHECK_GOTO(raw_say_hello(wt_client, &request, &wt_response) == 0);
    CHECK_GOTO(wt_response != NULL && wt_response->message != NULL && strcmp(wt_response->message, "shared") == 0);

    trevrpc_raw_client_close(native_client);
    native_client = NULL;
    trevrpc_raw_client_close(wt_client);
    wt_client = NULL;
    trevrpc_server_stop(server);
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
        trevrpc_server_stop(server);
        (void)pthread_join(thread, NULL);
    }
    if (stop_and_release_server(&server) != 0) {
        result = 1;
    }
    return result;
}

static int test_generated_native_unary_stack_and_heap_protobuf_buffers(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_channel* channel = NULL;
    hello_v1_greeter_say_hello_result stack_response = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
    hello_v1_greeter_say_hello_result heap_response = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
    serve_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    trevrpc_server_config_v1 server_config;
    int err = trevrpc_server_config_v1_init(&server_config, sizeof(server_config));
    if (err != 0) {
        return err;
    }
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    trevrpc_client_config_v1 client_config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&client_config, sizeof(client_config)) == 0);
    client_config.skip_certificate_validation = 1;
    Hello__V1__HelloRequest stack_request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloRequest heap_request = HELLO__V1__HELLO_REQUEST__INIT;
    char heap_name[700];

    memset(heap_name, 'h', sizeof(heap_name) - 1);
    heap_name[sizeof(heap_name) - 1] = '\0';
    stack_request.name = "stack-buffer";
    heap_request.name = heap_name;

    CHECK_GOTO(trevrpc_server_listen_v1(&server_config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    CHECK_GOTO(trevrpc_server_freeze(server) == 0);
    args.server = server;
    CHECK_GOTO(pthread_create(&thread, NULL, serve_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_server_port(server, &port) == 0);
    CHECK_GOTO(trevrpc_channel_connect_v1("127.0.0.1", port, &client_config, NULL, 5000000000ull, NULL, &channel) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(channel, &stack_request, &stack_response) == 0);
    CHECK_GOTO(stack_response.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_SUCCESS && stack_response.response != NULL &&
               stack_response.response->message != NULL);
    CHECK_GOTO(strcmp(stack_response.response->message, stack_request.name) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(channel, &heap_request, &heap_response) == 0);
    CHECK_GOTO(heap_response.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_SUCCESS && heap_response.response != NULL &&
               heap_response.response->message != NULL);
    CHECK_GOTO(strcmp(heap_response.response->message, heap_request.name) == 0);

    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    channel = NULL;
    trevrpc_server_stop(server);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);

    result = 0;

cleanup:
    hello_v1_greeter_say_hello_result_reset(&stack_response);
    hello_v1_greeter_say_hello_result_reset(&heap_response);
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    if (thread_started) {
        trevrpc_server_stop(server);
        (void)pthread_join(thread, NULL);
    }
    if (stop_and_release_server(&server) != 0) {
        result = 1;
    }
    return result;
}

static int test_generated_native_helper_pending_send_resource_exhausted(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_channel* channel = NULL;
    serve_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    trevrpc_server_config_v1 server_config;
    int err = trevrpc_server_config_v1_init(&server_config, sizeof(server_config));
    if (err != 0) {
        return err;
    }
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = 1000;
    server_config.peer_bidi_stream_count = 8;
    trevrpc_client_config_v1 client_config;
    err = trevrpc_client_config_v1_init(&client_config, sizeof(client_config));
    if (err != 0) {
        return err;
    }
    client_config.skip_certificate_validation = 1;
    client_config.max_pending_send_bytes = 4;
    client_config.max_pending_send_count = 1;
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    hello_v1_greeter_say_hello_result response = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
    request.name = "tiny-budget";

    CHECK_GOTO(trevrpc_server_listen_v1(&server_config, &server) == 0);
    CHECK_GOTO(hello_v1_greeter_register(server, &GreeterImplementation) == 0);
    CHECK_GOTO(trevrpc_server_freeze(server) == 0);
    args.server = server;
    CHECK_GOTO(pthread_create(&thread, NULL, serve_thread, &args) == 0);
    thread_started = true;

    uint16_t port = 0;
    CHECK_GOTO(trevrpc_server_port(server, &port) == 0);
    CHECK_GOTO(trevrpc_channel_connect_v1("127.0.0.1", port, &client_config, NULL, 5000000000ull, NULL, &channel) == 0);
    CHECK_GOTO(hello_v1_greeter_say_hello(channel, &request, &response) == 0);
    CHECK_GOTO(response.kind == HELLO_V1_GREETER_SAY_HELLO_RESULT_RUNTIME_ERROR);
    CHECK_GOTO(response.error == TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(response.response == NULL);

    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    channel = NULL;
    trevrpc_server_stop(server);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;

    result = 0;

cleanup:
    hello_v1_greeter_say_hello_result_reset(&response);
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    if (thread_started) {
        trevrpc_server_stop(server);
        (void)pthread_join(thread, NULL);
    }
    if (stop_and_release_server(&server) != 0) {
        result = 1;
    }
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
    if (test_generated_allocator_failures() != 0) {
        goto cleanup;
    }
    if (test_generated_services_all_rpc_shapes() != 0) {
        goto cleanup;
    }
    if (test_webtransport_serve_loop_unary_shutdown() != 0) {
        goto cleanup;
    }
    if (test_binding_webtransport_unary_graceful_cleanup() != 0) {
        goto cleanup;
    }
    if (test_binding_webtransport_unary_failed_terminal_submission_aborts() != 0) {
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
