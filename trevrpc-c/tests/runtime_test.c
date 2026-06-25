#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"
#include "trevrpc_msquic.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define NANOS_PER_SEC 1000000000ull

struct trevrpc_call_context {
    trevrpc_server* server;
    bool has_deadline;
    struct timespec deadline;
};

typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;

int trevrpc_test_server_new(const trevrpc_config* config, trevrpc_server** out_server);
void trevrpc_test_server_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream);
uint32_t trevrpc_test_status_from_error(int err, const char** message);
uint32_t trevrpc_test_transport_status_from_error(int err, const char** message);
size_t trevrpc_test_server_stream_status_count(trevrpc_server* server);
uint32_t trevrpc_test_server_last_stream_status(trevrpc_server* server);
bool trevrpc_test_server_request_try_start(trevrpc_server* server);
void trevrpc_test_server_request_finish(trevrpc_server* server);
bool trevrpc_test_server_connection_try_start(trevrpc_server* server);
void trevrpc_test_server_connection_finish(trevrpc_server* server);
int trevrpc_test_conn_stream_limiter_init(void* limiter);
bool trevrpc_test_conn_stream_try_start(void* limiter, int64_t limit);
void trevrpc_test_conn_stream_finish(void* limiter);
void trevrpc_test_conn_stream_limiter_destroy(void* limiter);

struct trevrpc_stream {
    uint32_t transport;
    trevrpc_msquic_stream* msquic_stream;
    void* wt_stream;
    const trevrpc_call_context* context;
    size_t max_frame_size;
    bool owns_stream;
    bool sent_status;
    bool status_queued;
    uint32_t terminal_status;
    int64_t max_stream_messages;
    int64_t max_stream_body_size;
    uint64_t stream_idle_timeout_nanos;
    int64_t request_message_count;
    int64_t response_message_count;
    uint64_t request_body_size;
    uint64_t response_body_size;
    bool response_idle_started;
    struct timespec response_last_activity;
    uint32_t failure_status;
    const char* failure_message;
};

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
    bool shutdown_complete;
    bool close_pending;
    bool closed;
    bool api_ref_acquired;
    size_t active_handle_ops;
    int err;
    trevrpc_msquic_send* send_pool;
    size_t send_pool_count;
};

typedef struct trevrpc_conn_stream_limiter_for_test {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t active_streams;
} trevrpc_conn_stream_limiter_for_test;

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
    size_t request_body_len;
    size_t response_body_len;
    char service[32];
    char method[32];
} metric_counts;

static void record_started(void* user_data, const trevrpc_rpc_started_event* event) {
    metric_counts* counts = user_data;
    counts->started++;
    counts->request_body_len = event->request_body_len;
    size_t service_len =
        event->service_len < sizeof(counts->service) - 1 ? event->service_len : sizeof(counts->service) - 1;
    size_t method_len = event->method_len < sizeof(counts->method) - 1 ? event->method_len : sizeof(counts->method) - 1;
    if (event->service != NULL) {
        memcpy(counts->service, event->service, service_len);
    }
    if (event->method != NULL) {
        memcpy(counts->method, event->method, method_len);
    }
    counts->service[service_len] = '\0';
    counts->method[method_len] = '\0';
}

static void record_finished(void* user_data, const trevrpc_rpc_finished_event* event) {
    metric_counts* counts = user_data;
    counts->finished++;
    counts->status = event->status;
    counts->request_body_len = event->request_body_len;
    counts->response_body_len = event->response_body_len;
}

static int success_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    (void)request;
    static const uint8_t body[] = {'o', 'k'};
    return trevrpc_response_set_body(response, body, sizeof(body));
}

static int empty_response_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)response;
    return 0;
}

static int failing_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)response;
    return -EIO;
}

static int invalid_response_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    (void)request;
    response->body = NULL;
    response->body_len = 1;
    return 0;
}

static int stream_error_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)stream;
    return -EIO;
}

static int stream_message_then_error_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;
    const uint8_t body[] = {1};
    (void)trevrpc_stream_send_message(stream, body, sizeof(body));
    return -EIO;
}

static int stream_omit_terminal_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)stream;
    return 0;
}

static int stream_explicit_status_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;
    return trevrpc_stream_send_status(stream, TREVRPC_STATUS_PERMISSION_DENIED, NULL, 0);
}

typedef struct raw_call_state {
    trevrpc_call* call;
    int called;
    size_t request_body_len;
    bool saw_context;
    bool saw_stream;
} raw_call_state;

static int raw_unary_immediate_handler(void* user_data, trevrpc_call* call) {
    raw_call_state* state = user_data;
    const trevrpc_request* request = trevrpc_call_request(call);
    state->called++;
    state->request_body_len = request == NULL ? 0 : request->body_len;
    state->saw_context = trevrpc_call_get_context(call) != NULL;

    const uint8_t body[] = {'r', 'a', 'w'};
    trevrpc_response response = {0};
    int err = trevrpc_response_set_body(&response, body, sizeof(body));
    if (err == 0) {
        err = trevrpc_call_respond(call, &response);
    }
    trevrpc_response_reset(&response);
    return err;
}

static int raw_unary_deferred_handler(void* user_data, trevrpc_call* call) {
    raw_call_state* state = user_data;
    const trevrpc_request* request = trevrpc_call_request(call);
    state->called++;
    state->call = call;
    state->request_body_len = request == NULL ? 0 : request->body_len;
    state->saw_context = trevrpc_call_get_context(call) != NULL;
    return TREVRPC_CALL_DEFERRED;
}

static int raw_stream_deferred_handler(void* user_data, trevrpc_call* call) {
    raw_call_state* state = user_data;
    state->called++;
    state->call = call;
    state->saw_context = trevrpc_call_get_context(call) != NULL;
    state->saw_stream = trevrpc_call_stream(call) != NULL;
    return trevrpc_call_defer(call);
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

static int init_empty_raw_stream(trevrpc_msquic_stream* stream) {
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
    return 0;
}

static int now(struct timespec* out) {
    if (clock_gettime(CLOCK_MONOTONIC, out) != 0) {
        return -errno;
    }
    return 0;
}

static int test_null_context(void) {
    int result = 1;
    uint64_t remaining = 123;

    CHECK_GOTO(trevrpc_call_context_has_deadline(NULL) == 0);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(NULL) == 0);
    CHECK_GOTO(trevrpc_call_context_cancelled(NULL) == 1);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(NULL, NULL) == -EINVAL);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(NULL, &remaining) == 0);
    CHECK_GOTO(remaining == 0);

    result = 0;

cleanup:
    return result;
}

static int test_context_without_deadline(void) {
    int result = 1;
    uint64_t remaining = 123;
    trevrpc_call_context context = {0};

    CHECK_GOTO(trevrpc_call_context_has_deadline(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_cancelled(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(&context, &remaining) == 0);
    CHECK_GOTO(remaining == 0);

    result = 0;

cleanup:
    return result;
}

static int test_future_deadline(void) {
    int result = 1;
    uint64_t remaining = 0;
    trevrpc_call_context context = {0};

    CHECK_GOTO(now(&context.deadline) == 0);
    context.has_deadline = true;
    context.deadline.tv_sec++;

    CHECK_GOTO(trevrpc_call_context_has_deadline(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_cancelled(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(&context, &remaining) == 1);
    CHECK_GOTO(remaining > 0);
    CHECK_GOTO(remaining <= 1000000000ull);

    result = 0;

cleanup:
    return result;
}

static int test_expired_deadline(void) {
    int result = 1;
    uint64_t remaining = 123;
    trevrpc_call_context context = {0};

    CHECK_GOTO(now(&context.deadline) == 0);
    context.has_deadline = true;
    context.deadline.tv_sec--;

    CHECK_GOTO(trevrpc_call_context_has_deadline(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_cancelled(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(&context, &remaining) == 1);
    CHECK_GOTO(remaining == 0);

    result = 0;

cleanup:
    return result;
}

static int test_default_server_options(void) {
    int result = 1;
    trevrpc_server_options options = trevrpc_default_server_options();

    CHECK_GOTO(options.max_concurrent_connections == 256);
    CHECK_GOTO(options.max_concurrent_streams_per_connection == 64);
    CHECK_GOTO(options.max_concurrent_requests == 1024);
    CHECK_GOTO(options.graceful_shutdown_timeout_nanos == 30ull * NANOS_PER_SEC);
    CHECK_GOTO(options.initial_request_timeout_nanos == 10ull * NANOS_PER_SEC);
    CHECK_GOTO(options.max_stream_messages == 4096);
    CHECK_GOTO(options.max_stream_body_size == 16 * 1024 * 1024);
    CHECK_GOTO(options.stream_idle_timeout_nanos == 30ull * NANOS_PER_SEC);

    result = 0;

cleanup:
    return result;
}

static int test_request_stream_message_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = 0,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    trevrpc_stream_frame* frame = (trevrpc_stream_frame*)1;

    CHECK_GOTO(trevrpc_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_message_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = 0,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t body[] = {1};

    CHECK_GOTO(trevrpc_stream_send_message(&stream, body, sizeof(body)) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(stream.response_message_count == 0);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_request_stream_body_size_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = -1,
        .max_stream_body_size = 0,
        .request_body_size = 1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    trevrpc_stream_frame* frame = (trevrpc_stream_frame*)1;

    CHECK_GOTO(trevrpc_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_body_size_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = -1,
        .max_stream_body_size = 0,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t body[] = {1};

    CHECK_GOTO(trevrpc_stream_send_message(&stream, body, sizeof(body)) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(stream.response_body_size == sizeof(body));
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_empty_message_batch_is_noop(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };

    CHECK_GOTO(trevrpc_stream_send_messages(&stream, NULL, NULL, 0) == 0);
    CHECK_GOTO(stream.response_message_count == 0);
    CHECK_GOTO(stream.response_body_size == 0);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_message_batch_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = 1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t bodies[] = {1, 2};
    const size_t body_lens[] = {1, 1};

    CHECK_GOTO(trevrpc_stream_send_messages(&stream, bodies, body_lens, 2) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(stream.response_message_count == 1);
    CHECK_GOTO(stream.response_body_size == 1);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_message_batch_body_size_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = -1,
        .max_stream_body_size = 1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t bodies[] = {1, 2};
    const size_t body_lens[] = {1, 1};

    CHECK_GOTO(trevrpc_stream_send_messages(&stream, bodies, body_lens, 2) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(stream.response_message_count == 2);
    CHECK_GOTO(stream.response_body_size == 2);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_message_batch_rejects_missing_body_bytes(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const size_t body_lens[] = {1};

    CHECK_GOTO(trevrpc_stream_send_messages(&stream, NULL, body_lens, 1) == -EINVAL);
    CHECK_GOTO(stream.response_message_count == 0);
    CHECK_GOTO(stream.response_body_size == 0);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    return result;
}

static int test_request_stream_idle_timeout(void) {
    int result = 1;
    bool mutex_initialized = false;
    bool cond_initialized = false;
    trevrpc_msquic_stream raw_stream = {0};
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = &raw_stream,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .stream_idle_timeout_nanos = 1000000ull,
        .failure_status = TREVRPC_STATUS_OK,
    };
    trevrpc_stream_frame* frame = (trevrpc_stream_frame*)1;

    CHECK_GOTO(pthread_mutex_init(&raw_stream.mutex, NULL) == 0);
    mutex_initialized = true;
    CHECK_GOTO(pthread_cond_init(&raw_stream.cond, NULL) == 0);
    cond_initialized = true;

    CHECK_GOTO(trevrpc_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_IDLE_TIMEOUT);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_UNAVAILABLE);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    if (cond_initialized) {
        pthread_cond_destroy(&raw_stream.cond);
    }
    if (mutex_initialized) {
        pthread_mutex_destroy(&raw_stream.mutex);
    }
    return result;
}

static int test_response_stream_idle_timeout(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .stream_idle_timeout_nanos = 1,
        .response_idle_started = true,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t body[] = {1};

    CHECK_GOTO(now(&stream.response_last_activity) == 0);
    stream.response_last_activity.tv_sec--;

    CHECK_GOTO(trevrpc_stream_send_message(&stream, body, sizeof(body)) == TREVRPC_ERR_STREAM_IDLE_TIMEOUT);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_UNAVAILABLE);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_stream_rejects_writes_after_terminal_status(void) {
    int result = 1;
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = (trevrpc_msquic_stream*)1,
        .sent_status = true,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t body[] = {1};

    CHECK_GOTO(trevrpc_stream_send_message(&stream, body, sizeof(body)) == -EPIPE);
    CHECK_GOTO(trevrpc_stream_send_status(&stream, TREVRPC_STATUS_OK, NULL, 0) == -EPIPE);

    result = 0;

cleanup:
    return result;
}

static int test_metadata_value_authorizer(void) {
    int result = 1;
    const uint8_t value[] = {'s', 'e', 'c', 'r', 'e', 't'};
    const uint8_t wrong_value[] = {'w', 'r', 'o', 'n', 'g'};
    trevrpc_request request = {0};
    trevrpc_metadata_value_authorizer authorizer = {
        .key = "authorization",
        .key_len = strlen("authorization"),
        .value = value,
        .value_len = sizeof(value),
    };
    trevrpc_status status = trevrpc_status_ok();

    CHECK_GOTO(
        trevrpc_metadata_set(&request.metadata, "authorization", strlen("authorization"), value, sizeof(value)) == 0);
    CHECK_GOTO(trevrpc_authorize_metadata_value(&authorizer, NULL, &request, &status) == 0);
    CHECK_GOTO(status.code == TREVRPC_STATUS_OK);

    authorizer.value = wrong_value;
    authorizer.value_len = sizeof(wrong_value);
    CHECK_GOTO(trevrpc_authorize_metadata_value(&authorizer, NULL, &request, &status) == 0);
    CHECK_GOTO(status.code == TREVRPC_STATUS_UNAUTHENTICATED);

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    return result;
}

static int test_bearer_authorizer(void) {
    int result = 1;
    const uint8_t value[] = {'B', 'e', 'a', 'r', 'e', 'r', ' ', 's', 'e', 'c', 'r', 'e', 't'};
    trevrpc_request request = {0};
    trevrpc_bearer_authorizer authorizer = {
        .token = "secret",
        .token_len = strlen("secret"),
    };
    trevrpc_status status = trevrpc_status_ok();

    CHECK_GOTO(
        trevrpc_metadata_set(&request.metadata, "authorization", strlen("authorization"), value, sizeof(value)) == 0);
    CHECK_GOTO(trevrpc_authorize_bearer_token(&authorizer, NULL, &request, &status) == 0);
    CHECK_GOTO(status.code == TREVRPC_STATUS_OK);

    authorizer.token = "wrong";
    authorizer.token_len = strlen("wrong");
    CHECK_GOTO(trevrpc_authorize_bearer_token(&authorizer, NULL, &request, &status) == 0);
    CHECK_GOTO(status.code == TREVRPC_STATUS_UNAUTHENTICATED);

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    return result;
}

static int test_authorizer_failure_variants(void) {
    int result = 1;
    trevrpc_request request = {0};
    trevrpc_status status = trevrpc_status_ok();

    CHECK_GOTO(trevrpc_authorize_metadata_value(NULL, NULL, &request, &status) == -EINVAL);

    status = trevrpc_status_permission_denied("permission denied", strlen("permission denied"));
    CHECK_GOTO(status.code == TREVRPC_STATUS_PERMISSION_DENIED);
    CHECK_GOTO(status.message_len == strlen("permission denied"));

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    return result;
}

static int run_metrics_case_with_config(const trevrpc_config* config,
    const uint8_t* request_body,
    size_t request_body_len,
    trevrpc_unary_handler handler,
    metric_counts* counts);

static int run_metrics_case(
    const uint8_t* request_body, size_t request_body_len, trevrpc_unary_handler handler, metric_counts* counts) {
    return run_metrics_case_with_config(NULL, request_body, request_body_len, handler, counts);
}

static int run_metrics_case_with_config(const trevrpc_config* config,
    const uint8_t* request_body,
    size_t request_body_len,
    trevrpc_unary_handler handler,
    metric_counts* counts) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = counts,
    };

    CHECK_GOTO(trevrpc_test_server_new(config, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    if (handler != NULL) {
        CHECK_GOTO(trevrpc_server_register_unary(server, "svc", "method", handler, NULL) == 0);
    }
    CHECK_GOTO(init_raw_stream(&stream, request_body, request_body_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    return result;
}

static int run_stream_case_kind(
    uint32_t kind, trevrpc_stream_handler handler, metric_counts* counts, size_t* status_count, uint32_t* last_status) {
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

    CHECK_GOTO(trevrpc_wire_encode_request("svc", "method", kind, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_server_register_streaming(server, "svc", "method", kind, handler, NULL) == 0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);
    *status_count = trevrpc_test_server_stream_status_count(server);
    *last_status = trevrpc_test_server_last_stream_status(server);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    free(frame);
    return result;
}

static int run_stream_case(
    trevrpc_stream_handler handler, metric_counts* counts, size_t* status_count, uint32_t* last_status) {
    return run_stream_case_kind(TREVRPC_RPC_KIND_SERVER_STREAMING, handler, counts, status_count, last_status);
}

static int test_metrics_exactly_once_success(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};
    const uint8_t body[] = {'h', 'i'};

    CHECK_GOTO(
        trevrpc_wire_encode_request(
            "svc", "method", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(counts.request_body_len == sizeof(body));
    CHECK_GOTO(counts.response_body_len == 2);
    CHECK_GOTO(strcmp(counts.service, "svc") == 0);
    CHECK_GOTO(strcmp(counts.method, "method") == 0);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_metrics_exactly_once_handler_failure(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, failing_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_metrics_exactly_once_cancellation(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 1, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_DEADLINE_EXCEEDED);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_metrics_exactly_once_decode_error(void) {
    int result = 1;
    metric_counts counts = {0};
    const uint8_t invalid_frame[] = {0x00, 0x00, 0x00, 0x03, 0xff, 0xff, 0xff};

    CHECK_GOTO(run_metrics_case(invalid_frame, sizeof(invalid_frame), success_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INVALID_ARGUMENT);
    CHECK_GOTO(counts.request_body_len == 0);
    CHECK_GOTO(counts.response_body_len == 0);
    CHECK_GOTO(counts.service[0] == '\0');
    CHECK_GOTO(counts.method[0] == '\0');

    result = 0;

cleanup:
    return result;
}

static int test_status_from_error_matches_go_policy(void) {
    int result = 1;
    const char* message = NULL;

    CHECK_GOTO(
        trevrpc_test_status_from_error(TREVRPC_ERR_FRAME_TOO_LARGE, &message) == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_status_from_error(TREVRPC_ERR_INVALID_FRAME, &message) == TREVRPC_STATUS_INVALID_ARGUMENT);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(
        trevrpc_test_status_from_error(TREVRPC_ERR_UNSUPPORTED_RPC_KIND, &message) == TREVRPC_STATUS_INVALID_ARGUMENT);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_status_from_error(TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION, &message) ==
               TREVRPC_STATUS_FAILED_PRECONDITION);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_status_from_error(-ETIMEDOUT, &message) == TREVRPC_STATUS_DEADLINE_EXCEEDED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_status_from_error(-ECANCELED, &message) == TREVRPC_STATUS_CANCELLED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_status_from_error(-EIO, &message) == TREVRPC_STATUS_INTERNAL);
    CHECK_GOTO(message != NULL);

    result = 0;

cleanup:
    return result;
}

static int run_transport_error_case(int err, metric_counts* counts) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = counts,
    };

    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(init_empty_raw_stream(&stream) == 0);
    stream_initialized = true;
    stream.err = err;

    trevrpc_test_server_handle_stream(server, &stream);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    return result;
}

static int test_transport_status_from_error_is_predictable(void) {
    int result = 1;
    const char* message = NULL;
    metric_counts counts = {0};

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_MSQUIC_ERR_FRAME_TOO_LARGE, &message) ==
               TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_MSQUIC_ERR_CLOSED, &message) == TREVRPC_STATUS_CANCELLED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_MSQUIC_ERR_TIMEOUT, &message) ==
               TREVRPC_STATUS_DEADLINE_EXCEEDED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_WT_ERR_CLOSED, &message) == TREVRPC_STATUS_CANCELLED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_WT_ERR_FRAME_TOO_LARGE, &message) ==
               TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_transport_status_from_error(-EIO, &message) == TREVRPC_STATUS_UNAVAILABLE);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(run_transport_error_case(TREV_MSQUIC_ERR_CLOSED, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_CANCELLED);

    result = 0;

cleanup:
    return result;
}

static int test_error_status_runtime_paths(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};
    const uint8_t invalid_frame[] = {0x00, 0x00, 0x00, 0x03, 0xff, 0xff, 0xff};
    trevrpc_config small_frame_config = trevrpc_default_config();

    CHECK_GOTO(run_metrics_case(invalid_frame, sizeof(invalid_frame), success_handler, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INVALID_ARGUMENT);

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(trevrpc_wire_encode_request("svc", "method", 99, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INVALID_ARGUMENT);
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    bool version_field_found = false;
    for (size_t i = 4; i + 1 < frame_len; i++) {
        if (frame[i] == 0x30) {
            frame[i + 1] = 2;
            version_field_found = true;
            break;
        }
    }
    CHECK_GOTO(version_field_found);
    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_metrics_case(frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_FAILED_PRECONDITION);
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    small_frame_config.max_frame_size = 8;
    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_metrics_case_with_config(&small_frame_config, frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_metrics_case(frame, frame_len, failing_handler, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL);
    free(frame);
    frame = NULL;

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_transport_error_case(TREV_MSQUIC_ERR_CLOSED, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_CANCELLED);

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 1, 4096, &frame, &frame_len) == 0);
    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_metrics_case(frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_DEADLINE_EXCEEDED);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_invalid_unary_handler_response_is_internal(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, invalid_response_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_unary_handler_may_omit_response_body(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, empty_response_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(counts.response_body_len == 0);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_raw_unary_call_handler_responds(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    metric_counts counts = {0};
    raw_call_state state = {0};
    const uint8_t body[] = {'h', 'i'};
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };

    CHECK_GOTO(
        trevrpc_wire_encode_request(
            "svc", "method", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_server_register_call(
                   server, "svc", "method", TREVRPC_RPC_KIND_UNARY, raw_unary_immediate_handler, &state) == 0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);
    CHECK_GOTO(state.called == 1);
    CHECK_GOTO(state.request_body_len == sizeof(body));
    CHECK_GOTO(state.saw_context);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(counts.response_body_len == 3);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    free(frame);
    return result;
}

static int test_raw_unary_call_handler_defers(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    metric_counts counts = {0};
    raw_call_state state = {0};
    const uint8_t request_body[] = {'h', 'i'};
    const uint8_t response_body[] = {'l', 'a', 't', 'e', 'r'};
    trevrpc_response response = {0};
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };

    CHECK_GOTO(trevrpc_wire_encode_request("svc",
                   "method",
                   TREVRPC_RPC_KIND_UNARY,
                   request_body,
                   sizeof(request_body),
                   NULL,
                   0,
                   4096,
                   &frame,
                   &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_server_register_call(
                   server, "svc", "method", TREVRPC_RPC_KIND_UNARY, raw_unary_deferred_handler, &state) == 0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);
    CHECK_GOTO(state.called == 1);
    CHECK_GOTO(state.call != NULL);
    CHECK_GOTO(state.request_body_len == sizeof(request_body));
    CHECK_GOTO(state.saw_context);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 0);

    CHECK_GOTO(trevrpc_response_set_body(&response, response_body, sizeof(response_body)) == 0);
    CHECK_GOTO(trevrpc_call_respond(state.call, &response) == 0);
    state.call = NULL;
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(counts.response_body_len == sizeof(response_body));

    result = 0;

cleanup:
    trevrpc_response_reset(&response);
    if (state.call != NULL) {
        trevrpc_call_close(state.call);
    }
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    free(frame);
    return result;
}

static int test_raw_stream_call_handler_defers(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    metric_counts counts = {0};
    raw_call_state state = {0};
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };

    CHECK_GOTO(
        trevrpc_wire_encode_request(
            "svc", "method", TREVRPC_RPC_KIND_SERVER_STREAMING, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(
        trevrpc_server_register_call(
            server, "svc", "method", TREVRPC_RPC_KIND_SERVER_STREAMING, raw_stream_deferred_handler, &state) == 0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);
    CHECK_GOTO(state.called == 1);
    CHECK_GOTO(state.call != NULL);
    CHECK_GOTO(state.saw_context);
    CHECK_GOTO(state.saw_stream);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 0);

    CHECK_GOTO(trevrpc_call_finish_stream(state.call, TREVRPC_STATUS_PERMISSION_DENIED, NULL, 0) == 0);
    state.call = NULL;
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_PERMISSION_DENIED);

    result = 0;

cleanup:
    if (state.call != NULL) {
        trevrpc_call_close(state.call);
    }
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    free(frame);
    return result;
}

static int test_streaming_handlers_send_exactly_one_terminal_status(void) {
    int result = 1;
    metric_counts counts = {0};
    size_t status_count = 0;
    uint32_t last_status = TREVRPC_STATUS_UNKNOWN;

    CHECK_GOTO(run_stream_case(stream_error_handler, &counts, &status_count, &last_status) == 0);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_INTERNAL);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL);

    memset(&counts, 0, sizeof(counts));
    status_count = 0;
    last_status = TREVRPC_STATUS_UNKNOWN;
    CHECK_GOTO(run_stream_case(stream_message_then_error_handler, &counts, &status_count, &last_status) == 0);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_INTERNAL);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL);

    memset(&counts, 0, sizeof(counts));
    status_count = 0;
    last_status = TREVRPC_STATUS_UNKNOWN;
    CHECK_GOTO(run_stream_case(stream_omit_terminal_handler, &counts, &status_count, &last_status) == 0);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_OK);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    status_count = 0;
    last_status = TREVRPC_STATUS_UNKNOWN;
    CHECK_GOTO(run_stream_case(stream_explicit_status_handler, &counts, &status_count, &last_status) == 0);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_PERMISSION_DENIED);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_PERMISSION_DENIED);

    result = 0;

cleanup:
    return result;
}

static int test_inprocess_msquic_runtime_all_rpc_shapes(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    metric_counts counts = {0};
    size_t status_count = 0;
    uint32_t last_status = TREVRPC_STATUS_UNKNOWN;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(run_metrics_case(frame, frame_len, success_handler, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    free(frame);
    frame = NULL;

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(
        run_stream_case_kind(
            TREVRPC_RPC_KIND_SERVER_STREAMING, stream_omit_terminal_handler, &counts, &status_count, &last_status) ==
        0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    status_count = 0;
    last_status = TREVRPC_STATUS_UNKNOWN;
    CHECK_GOTO(
        run_stream_case_kind(
            TREVRPC_RPC_KIND_CLIENT_STREAMING, stream_omit_terminal_handler, &counts, &status_count, &last_status) ==
        0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_OK);

    memset(&counts, 0, sizeof(counts));
    status_count = 0;
    last_status = TREVRPC_STATUS_UNKNOWN;
    CHECK_GOTO(run_stream_case_kind(TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
                   stream_omit_terminal_handler,
                   &counts,
                   &status_count,
                   &last_status) == 0);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_server_shutdown_rejects_new_work(void) {
    int result = 1;
    trevrpc_server* server = NULL;

    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_test_server_request_try_start(server));
    trevrpc_server_shutdown(server);
    CHECK_GOTO(!trevrpc_test_server_request_try_start(server));
    trevrpc_test_server_request_finish(server);

    result = 0;

cleanup:
    trevrpc_server_close(server);
    return result;
}

static int test_partial_stream_failure_reports_terminal_status(void) {
    int result = 1;
    metric_counts counts = {0};
    size_t status_count = 0;
    uint32_t last_status = TREVRPC_STATUS_UNKNOWN;

    CHECK_GOTO(run_stream_case(stream_message_then_error_handler, &counts, &status_count, &last_status) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_INTERNAL);
    CHECK_GOTO(status_count == 1);
    CHECK_GOTO(last_status == TREVRPC_STATUS_INTERNAL);

    result = 0;

cleanup:
    return result;
}

static int test_concurrent_request_limit_rejects_overload(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    metric_counts counts = {0};
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };
    trevrpc_server_options options = trevrpc_default_server_options();
    options.max_concurrent_requests = 1;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_server_register_unary(server, "svc", "method", success_handler, NULL) == 0);
    CHECK_GOTO(trevrpc_test_server_request_try_start(server));
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);

    result = 0;

cleanup:
    if (server != NULL) {
        trevrpc_test_server_request_finish(server);
    }
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    trevrpc_server_close(server);
    free(frame);
    return result;
}

static int test_concurrent_connection_limit_rejects_overload(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_server_options options = trevrpc_default_server_options();
    options.max_concurrent_connections = 1;

    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_test_server_connection_try_start(server));
    CHECK_GOTO(!trevrpc_test_server_connection_try_start(server));
    trevrpc_test_server_connection_finish(server);
    CHECK_GOTO(trevrpc_test_server_connection_try_start(server));
    trevrpc_test_server_connection_finish(server);

    result = 0;

cleanup:
    trevrpc_server_close(server);
    return result;
}

static int test_concurrent_stream_limit_rejects_overload(void) {
    int result = 1;
    trevrpc_conn_stream_limiter_for_test limiter = {0};
    bool limiter_initialized = false;

    CHECK_GOTO(trevrpc_test_conn_stream_limiter_init(&limiter) == 0);
    limiter_initialized = true;
    CHECK_GOTO(trevrpc_test_conn_stream_try_start(&limiter, 1));
    CHECK_GOTO(!trevrpc_test_conn_stream_try_start(&limiter, 1));
    trevrpc_test_conn_stream_finish(&limiter);
    CHECK_GOTO(trevrpc_test_conn_stream_try_start(&limiter, 1));
    trevrpc_test_conn_stream_finish(&limiter);

    result = 0;

cleanup:
    if (limiter_initialized) {
        trevrpc_test_conn_stream_limiter_destroy(&limiter);
    }
    return result;
}

int main(void) {
    if (test_null_context() != 0) {
        return 1;
    }
    if (test_context_without_deadline() != 0) {
        return 1;
    }
    if (test_future_deadline() != 0) {
        return 1;
    }
    if (test_expired_deadline() != 0) {
        return 1;
    }
    if (test_default_server_options() != 0) {
        return 1;
    }
    if (test_request_stream_message_limit() != 0) {
        return 1;
    }
    if (test_response_stream_message_limit() != 0) {
        return 1;
    }
    if (test_request_stream_body_size_limit() != 0) {
        return 1;
    }
    if (test_response_stream_body_size_limit() != 0) {
        return 1;
    }
    if (test_response_stream_empty_message_batch_is_noop() != 0) {
        return 1;
    }
    if (test_response_stream_message_batch_limit() != 0) {
        return 1;
    }
    if (test_response_stream_message_batch_body_size_limit() != 0) {
        return 1;
    }
    if (test_response_stream_message_batch_rejects_missing_body_bytes() != 0) {
        return 1;
    }
    if (test_request_stream_idle_timeout() != 0) {
        return 1;
    }
    if (test_response_stream_idle_timeout() != 0) {
        return 1;
    }
    if (test_stream_rejects_writes_after_terminal_status() != 0) {
        return 1;
    }
    if (test_metadata_value_authorizer() != 0) {
        return 1;
    }
    if (test_bearer_authorizer() != 0) {
        return 1;
    }
    if (test_authorizer_failure_variants() != 0) {
        return 1;
    }
    if (test_metrics_exactly_once_success() != 0) {
        return 1;
    }
    if (test_metrics_exactly_once_handler_failure() != 0) {
        return 1;
    }
    if (test_metrics_exactly_once_cancellation() != 0) {
        return 1;
    }
    if (test_metrics_exactly_once_decode_error() != 0) {
        return 1;
    }
    if (test_status_from_error_matches_go_policy() != 0) {
        return 1;
    }
    if (test_transport_status_from_error_is_predictable() != 0) {
        return 1;
    }
    if (test_error_status_runtime_paths() != 0) {
        return 1;
    }
    if (test_invalid_unary_handler_response_is_internal() != 0) {
        return 1;
    }
    if (test_unary_handler_may_omit_response_body() != 0) {
        return 1;
    }
    if (test_raw_unary_call_handler_responds() != 0) {
        return 1;
    }
    if (test_raw_unary_call_handler_defers() != 0) {
        return 1;
    }
    if (test_raw_stream_call_handler_defers() != 0) {
        return 1;
    }
    if (test_streaming_handlers_send_exactly_one_terminal_status() != 0) {
        return 1;
    }
    if (test_inprocess_msquic_runtime_all_rpc_shapes() != 0) {
        return 1;
    }
    if (test_server_shutdown_rejects_new_work() != 0) {
        return 1;
    }
    if (test_partial_stream_failure_reports_terminal_status() != 0) {
        return 1;
    }
    if (test_concurrent_request_limit_rejects_overload() != 0) {
        return 1;
    }
    if (test_concurrent_connection_limit_rejects_overload() != 0) {
        return 1;
    }
    if (test_concurrent_stream_limit_rejects_overload() != 0) {
        return 1;
    }
    return 0;
}
