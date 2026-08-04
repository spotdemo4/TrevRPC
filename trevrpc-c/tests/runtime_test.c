#define _POSIX_C_SOURCE 200809L

#include "trevrpc_runtime_internal.h"
#include "trevrpc_msquic.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

#include "trevrpc_frame_internal.h"

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

int trevrpc_test_server_new(const trevrpc_client_config_internal* config, trevrpc_server** out_server);
void trevrpc_test_server_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream);
void trevrpc_test_server_freeze_routes(trevrpc_server* server);
int trevrpc_test_server_start_worker_pool(trevrpc_server* server);
int trevrpc_test_server_queue_msquic_stream(trevrpc_server* server, trevrpc_msquic_stream* stream, void* limiter);
int trevrpc_test_server_queue_msquic_stream_record_envelope(
    trevrpc_server* server, trevrpc_msquic_stream* stream, void* limiter);
bool trevrpc_test_server_wait_for_tasks(trevrpc_server* server, uint64_t timeout_nanos);
uint32_t trevrpc_test_status_from_error(int err, const char** message);
uint32_t trevrpc_test_transport_status_from_error(int err, const char** message);
size_t trevrpc_test_server_unary_status_count(trevrpc_server* server);
uint32_t trevrpc_test_server_last_unary_status(trevrpc_server* server);
size_t trevrpc_test_server_stream_status_count(trevrpc_server* server);
uint32_t trevrpc_test_server_last_stream_status(trevrpc_server* server);
const uint8_t* trevrpc_test_server_last_envelope(trevrpc_server* server, size_t* frame_len, bool* finished_send);
void trevrpc_test_fail_next_call_context_init(int err);
bool trevrpc_test_server_request_try_start(trevrpc_server* server);
void trevrpc_test_server_request_finish(trevrpc_server* server);
bool trevrpc_test_server_connection_try_start(trevrpc_server* server);
void trevrpc_test_server_connection_finish(trevrpc_server* server);
void* trevrpc_test_conn_stream_limiter_new(void);
void trevrpc_test_conn_stream_limiter_release(void* limiter);
int trevrpc_test_conn_stream_limiter_init(void* limiter);
bool trevrpc_test_conn_stream_try_start(void* limiter, int64_t limit);
void trevrpc_test_conn_stream_finish(void* limiter);
void trevrpc_test_conn_stream_limiter_destroy(void* limiter);
#define TREVRPC_TEST_STREAM_SEND_LOCK_WAIT 1u
#define TREVRPC_TEST_STREAM_STATUS_CLAIMED 2u
typedef void (*trevrpc_test_stream_send_hook)(uint32_t event, void* context);
void trevrpc_test_set_stream_send_hook(trevrpc_test_stream_send_hook hook, void* context);
typedef void (*trevrpc_test_call_insert_hook)(void* context);
void trevrpc_test_set_call_insert_hook(trevrpc_test_call_insert_hook hook, void* context);

struct trevrpc_stream {
    trevrpc_msquic_stream* msquic_stream;
    void* wt_stream;
    void* h3_stream;
    const trevrpc_call_context* context;
    size_t max_frame_size;
    int64_t max_stream_messages;
    int64_t max_stream_body_size;
    int64_t max_recv_stream_messages;
    int64_t max_recv_stream_body_size;
    uint64_t stream_idle_timeout_nanos;
    int64_t request_message_count;
    int64_t response_message_count;
    uint64_t request_body_size;
    uint64_t response_body_size;
    const char* failure_message;
    void (*release)(void* context);
    void* release_context;
    trevrpc_cancellation* cancellation;
    void* scripted_source;
    void* state;
    struct timespec response_last_activity;
    struct timespec request_poll_started_at;
    uint32_t transport;
    uint32_t terminal_status;
    uint32_t failure_status;
    trevrpc_wire_diagnostic_reason last_wire_diagnostic;
    bool owns_stream;
    bool sent_status;
    bool status_queued;
    bool has_recv_limits;
    bool response_idle_started;
    bool request_poll_idle_started;
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

typedef struct trevrpc_conn_stream_limiter_for_test {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t active_streams;
    size_t refs;
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
    int status_counts[TREVRPC_STATUS_UNAUTHENTICATED + 1];
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
    if (event->status <= TREVRPC_STATUS_UNAUTHENTICATED) {
        counts->status_counts[event->status]++;
    }
    counts->request_body_len = event->request_body_len;
    counts->response_body_len = event->response_body_len;
}

static int counting_unary_handler(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response) {
    (void)context;
    (void)request;
    (void)response;
    int* calls = user_data;
    (*calls)++;
    return 0;
}

static int counting_stream_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)context;
    (void)request;
    (void)stream;
    int* calls = user_data;
    (*calls)++;
    return 0;
}

static int success_handler(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response) {
    (void)user_data;
    (void)context;
    (void)request;
    static const uint8_t body[] = {'o', 'k'};
    return trevrpc_internal_response_set_body(response, body, sizeof(body));
}

static int empty_response_handler(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)response;
    return 0;
}

static int failing_handler(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)response;
    return -EIO;
}

static int invalid_response_handler(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response) {
    (void)user_data;
    (void)context;
    (void)request;
    response->body.data = NULL;
    response->body.len = 1;
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

typedef struct call_finish_race_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_call* call;
    trevrpc_stream* stream;
    int status_result;
    int finish_result;
    bool status_claimed;
    bool finish_waiting;
    bool release_status;
} call_finish_race_state;

static void call_finish_race_hook(uint32_t event, void* context) {
    call_finish_race_state* state = context;
    pthread_mutex_lock(&state->mutex);
    if (event == TREVRPC_TEST_STREAM_STATUS_CLAIMED) {
        state->status_claimed = true;
        pthread_cond_broadcast(&state->cond);
        while (!state->release_status) {
            pthread_cond_wait(&state->cond, &state->mutex);
        }
    } else if (event == TREVRPC_TEST_STREAM_SEND_LOCK_WAIT && state->status_claimed && !state->release_status) {
        state->finish_waiting = true;
        pthread_cond_broadcast(&state->cond);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void* direct_status_main(void* context) {
    call_finish_race_state* state = context;
    state->status_result = trevrpc_stream_send_status(state->stream, TREVRPC_STATUS_PERMISSION_DENIED, NULL, 0);
    return NULL;
}

static void* call_finish_main(void* context) {
    call_finish_race_state* state = context;
    state->finish_result = trevrpc_call_finish_stream(state->call, TREVRPC_STATUS_OK, NULL, 0);
    return NULL;
}

typedef struct call_insert_race_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_server* server;
    trevrpc_msquic_stream* stream;
    bool insert_waiting;
    bool release_insert;
} call_insert_race_state;

static void call_insert_race_hook(void* context) {
    call_insert_race_state* state = context;
    pthread_mutex_lock(&state->mutex);
    state->insert_waiting = true;
    pthread_cond_broadcast(&state->cond);
    while (!state->release_insert) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void* handle_stream_main(void* context) {
    call_insert_race_state* state = context;
    trevrpc_test_server_handle_stream(state->server, state->stream);
    return NULL;
}

static int raw_unary_immediate_handler(void* user_data, trevrpc_call* call) {
    raw_call_state* state = user_data;
    const trevrpc_request* request = trevrpc_call_request(call);
    state->called++;
    state->request_body_len = request == NULL ? 0 : request->body_len;
    state->saw_context = trevrpc_call_get_context(call) != NULL;

    const uint8_t body[] = {'r', 'a', 'w'};
    trevrpc_wire_response_values response = {0};
    int err = trevrpc_internal_response_set_body(&response, body, sizeof(body));
    if (err == 0) {
        err = trevrpc_internal_call_respond(call, &response);
    }
    trevrpc_internal_response_reset(&response);
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

typedef struct multiple_raw_calls_state {
    size_t called;
} multiple_raw_calls_state;

static int multiple_raw_calls_deferred_handler(void* user_data, trevrpc_call* call) {
    multiple_raw_calls_state* state = user_data;
    state->called++;
    return trevrpc_call_defer(call);
}

typedef struct blocking_handler_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int entered;
    bool release;
} blocking_handler_state;

typedef struct deferred_signal_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_call* call;
    int called;
} deferred_signal_state;

static int realtime_after_ms(uint64_t timeout_ms, struct timespec* out_deadline) {
    if (clock_gettime(CLOCK_REALTIME, out_deadline) != 0) {
        return -errno;
    }
    uint64_t seconds = timeout_ms / 1000;
    uint64_t nanos = (timeout_ms % 1000) * 1000000ull;
    out_deadline->tv_sec += (time_t)seconds;
    out_deadline->tv_nsec += (long)nanos;
    if (out_deadline->tv_nsec >= (long)NANOS_PER_SEC) {
        out_deadline->tv_sec++;
        out_deadline->tv_nsec -= (long)NANOS_PER_SEC;
    }
    return 0;
}

static int blocking_handler_state_init(blocking_handler_state* state) {
    memset(state, 0, sizeof(*state));
    int err = pthread_mutex_init(&state->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&state->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&state->mutex);
        return -err;
    }
    return 0;
}

static void blocking_handler_state_release(blocking_handler_state* state) {
    pthread_mutex_lock(&state->mutex);
    state->release = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static bool blocking_handler_wait_entered(blocking_handler_state* state, int count, uint64_t timeout_ms) {
    struct timespec deadline = {0};
    if (realtime_after_ms(timeout_ms, &deadline) != 0) {
        return false;
    }

    pthread_mutex_lock(&state->mutex);
    while (state->entered < count) {
        int err = pthread_cond_timedwait(&state->cond, &state->mutex, &deadline);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static int blocking_handler_entered(blocking_handler_state* state) {
    pthread_mutex_lock(&state->mutex);
    int entered = state->entered;
    pthread_mutex_unlock(&state->mutex);
    return entered;
}

static void blocking_handler_state_destroy(blocking_handler_state* state) {
    pthread_cond_destroy(&state->cond);
    pthread_mutex_destroy(&state->mutex);
}

static int blocking_unary_handler(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response) {
    (void)context;
    (void)request;
    blocking_handler_state* state = user_data;
    pthread_mutex_lock(&state->mutex);
    state->entered++;
    pthread_cond_broadcast(&state->cond);
    while (!state->release) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);

    static const uint8_t body[] = {'o', 'k'};
    return trevrpc_internal_response_set_body(response, body, sizeof(body));
}

static int deferred_signal_state_init(deferred_signal_state* state) {
    memset(state, 0, sizeof(*state));
    int err = pthread_mutex_init(&state->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&state->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&state->mutex);
        return -err;
    }
    return 0;
}

static int raw_unary_deferred_signal_handler(void* user_data, trevrpc_call* call) {
    deferred_signal_state* state = user_data;
    pthread_mutex_lock(&state->mutex);
    state->called++;
    state->call = call;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
    return TREVRPC_CALL_DEFERRED;
}

static bool deferred_signal_wait_called(deferred_signal_state* state, uint64_t timeout_ms) {
    struct timespec deadline = {0};
    if (realtime_after_ms(timeout_ms, &deadline) != 0) {
        return false;
    }

    pthread_mutex_lock(&state->mutex);
    while (state->called == 0 || state->call == NULL) {
        int err = pthread_cond_timedwait(&state->cond, &state->mutex, &deadline);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static trevrpc_call* deferred_signal_take_call(deferred_signal_state* state) {
    pthread_mutex_lock(&state->mutex);
    trevrpc_call* call = state->call;
    state->call = NULL;
    pthread_mutex_unlock(&state->mutex);
    return call;
}

static void deferred_signal_state_destroy(deferred_signal_state* state) {
    trevrpc_call* call = deferred_signal_take_call(state);
    if (call != NULL) {
        trevrpc_call_close(call);
    }
    pthread_cond_destroy(&state->cond);
    pthread_mutex_destroy(&state->mutex);
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

static int init_empty_raw_stream(trevrpc_msquic_stream* stream) {
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
    return 0;
}

static trevrpc_msquic_stream* new_raw_stream(const uint8_t* body, size_t body_len) {
    trevrpc_msquic_stream* stream = malloc(sizeof(*stream));
    if (stream == NULL) {
        return NULL;
    }
    if (init_raw_stream(stream, body, body_len) != 0) {
        free(stream);
        return NULL;
    }
    return stream;
}

static trevrpc_msquic_stream* new_empty_raw_stream(void) {
    trevrpc_msquic_stream* stream = malloc(sizeof(*stream));
    if (stream == NULL) {
        return NULL;
    }
    if (init_empty_raw_stream(stream) != 0) {
        free(stream);
        return NULL;
    }
    return stream;
}

static void free_unowned_raw_stream(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return;
    }
    reset_raw_stream(stream);
    free(stream);
}

static int now(struct timespec* out) {
    if (clock_gettime(CLOCK_MONOTONIC, out) != 0) {
        return -errno;
    }
    return 0;
}

static void test_server_destroy(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }
    (void)trevrpc_server_stop(server);
    (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
    (void)trevrpc_server_release(server);
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

static int test_cancellation_token_lifecycle(void) {
    int result = 1;
    trevrpc_cancellation* cancellation = trevrpc_cancellation_new();

    CHECK_GOTO(cancellation != NULL);
    CHECK_GOTO(trevrpc_cancellation_cancelled(cancellation) == 0);

    trevrpc_cancellation_cancel(cancellation);
    CHECK_GOTO(trevrpc_cancellation_cancelled(cancellation) == 1);

    trevrpc_cancellation_cancel(NULL);
    CHECK_GOTO(trevrpc_cancellation_cancelled(NULL) == 0);
    trevrpc_cancellation_release(NULL);

    result = 0;

cleanup:
    trevrpc_cancellation_release(cancellation);
    return result;
}

static int test_default_server_options(void) {
    int result = 1;
    trevrpc_server_options_v1 options;
    CHECK_GOTO(trevrpc_server_options_v1_init(&options, sizeof(options)) == 0);

    CHECK_GOTO(options.max_concurrent_connections == 256);
    CHECK_GOTO(options.max_concurrent_streams_per_connection == 64);
    CHECK_GOTO(options.max_concurrent_requests == 1024);
    CHECK_GOTO(options.worker_count == 16);
    CHECK_GOTO(options.worker_queue_capacity == 1024);
    CHECK_GOTO(options.graceful_shutdown_timeout_nanos == 30ull * NANOS_PER_SEC);
    CHECK_GOTO(options.initial_request_timeout_nanos == 10ull * NANOS_PER_SEC);
    CHECK_GOTO(options.max_stream_messages == 4096);
    CHECK_GOTO(options.max_stream_body_size == 16 * 1024 * 1024);
    CHECK_GOTO(options.stream_idle_timeout_nanos == 30ull * NANOS_PER_SEC);

    result = 0;

cleanup:
    return result;
}

static int test_default_native_pending_send_limits(void) {
    int result = 1;
    trevrpc_client_config_v1 client_config;
    trevrpc_server_config_v1 server_config;
    CHECK_GOTO(trevrpc_client_config_v1_init(&client_config, sizeof(client_config)) == 0);
    CHECK_GOTO(trevrpc_server_config_v1_init(&server_config, sizeof(server_config)) == 0);

    CHECK_GOTO(client_config.max_pending_send_bytes == TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES);
    CHECK_GOTO(client_config.max_pending_send_count == TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT);
    CHECK_GOTO(client_config.stream_recv_window == 0);
    CHECK_GOTO(client_config.conn_flow_control_window == 0);
    CHECK_GOTO(client_config.msquic_execution_profile == TREV_MSQUIC_EXECUTION_PROFILE_LOW_LATENCY);
    CHECK_GOTO(client_config.msquic_send_buffering_enabled == 0);
    CHECK_GOTO(server_config.max_pending_send_bytes == TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES);
    CHECK_GOTO(server_config.max_pending_send_count == TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT);
    CHECK_GOTO(server_config.stream_recv_window == 0);
    CHECK_GOTO(server_config.conn_flow_control_window == 0);
    CHECK_GOTO(server_config.msquic_execution_profile == TREV_MSQUIC_EXECUTION_PROFILE_LOW_LATENCY);
    CHECK_GOTO(server_config.msquic_send_buffering_enabled == 0);

    result = 0;

cleanup:
    return result;
}

static int test_default_call_options(void) {
    int result = 1;
    trevrpc_call_options_v1 options;
    CHECK_GOTO(trevrpc_call_options_v1_init(&options, sizeof(options)) == 0);

    CHECK_GOTO(options.metadata == NULL);
    CHECK_GOTO(options.timeout_nanos == 0);
    CHECK_GOTO(options.cancellation == NULL);
    CHECK_GOTO(options.max_response_body_size == TREVRPC_DEFAULT_MAX_FRAME_SIZE);
    CHECK_GOTO(options.max_response_messages == 4096);
    CHECK_GOTO(options.max_response_stream_body_size == 16 * 1024 * 1024);
    CHECK_GOTO(options.response_idle_timeout_nanos == 0);

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
    trevrpc_wire_stream_frame_values* frame = (trevrpc_wire_stream_frame_values*)1;

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_request_stream_message_limit_boundary(void) {
    int result = 1;
    uint8_t* first_frame = NULL;
    uint8_t* second_frame = NULL;
    size_t first_frame_len = 0;
    size_t second_frame_len = 0;
    trevrpc_msquic_stream raw_stream = {0};
    bool stream_initialized = false;
    const uint8_t body[] = {1};
    trevrpc_wire_stream_frame_values* frame = NULL;

    CHECK_GOTO(trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
                   TREVRPC_STATUS_OK,
                   NULL,
                   0,
                   body,
                   sizeof(body),
                   NULL,
                   4096,
                   &first_frame,
                   &first_frame_len) == 0);
    CHECK_GOTO(trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
                   TREVRPC_STATUS_OK,
                   NULL,
                   0,
                   body,
                   sizeof(body),
                   NULL,
                   4096,
                   &second_frame,
                   &second_frame_len) == 0);
    CHECK_GOTO(init_raw_stream(&raw_stream, first_frame, first_frame_len) == 0);
    stream_initialized = true;
    CHECK_GOTO(append_recv_bytes(&raw_stream, second_frame, second_frame_len) == 0);

    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = &raw_stream,
        .max_frame_size = 4096,
        .max_stream_messages = 1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == 0);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
    CHECK_GOTO(stream.request_message_count == 1);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_OK);
    trevrpc_internal_stream_frame_free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.request_message_count == 1);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(frame);
    if (stream_initialized) {
        reset_raw_stream(&raw_stream);
    }
    free(first_frame);
    free(second_frame);
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
    trevrpc_wire_stream_frame_values* frame = (trevrpc_wire_stream_frame_values*)1;

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_request_stream_body_size_limit_boundary(void) {
    int result = 1;
    uint8_t* first_frame = NULL;
    uint8_t* second_frame = NULL;
    size_t first_frame_len = 0;
    size_t second_frame_len = 0;
    trevrpc_msquic_stream raw_stream = {0};
    bool stream_initialized = false;
    const uint8_t body[] = {1};
    trevrpc_wire_stream_frame_values* frame = NULL;

    CHECK_GOTO(trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
                   TREVRPC_STATUS_OK,
                   NULL,
                   0,
                   body,
                   sizeof(body),
                   NULL,
                   4096,
                   &first_frame,
                   &first_frame_len) == 0);
    CHECK_GOTO(trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
                   TREVRPC_STATUS_OK,
                   NULL,
                   0,
                   body,
                   sizeof(body),
                   NULL,
                   4096,
                   &second_frame,
                   &second_frame_len) == 0);
    CHECK_GOTO(init_raw_stream(&raw_stream, first_frame, first_frame_len) == 0);
    stream_initialized = true;
    CHECK_GOTO(append_recv_bytes(&raw_stream, second_frame, second_frame_len) == 0);

    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = &raw_stream,
        .max_frame_size = 4096,
        .max_stream_messages = -1,
        .max_stream_body_size = 1,
        .failure_status = TREVRPC_STATUS_OK,
    };

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == 0);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
    CHECK_GOTO(stream.request_body_size == sizeof(body));
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_OK);
    trevrpc_internal_stream_frame_free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.request_body_size == 2 * sizeof(body));
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(frame);
    if (stream_initialized) {
        reset_raw_stream(&raw_stream);
    }
    free(first_frame);
    free(second_frame);
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
    CHECK_GOTO(stream.response_body_size == 0);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_limit_boundaries(void) {
    int result = 1;
    trevrpc_msquic_stream raw_stream = {0};
    bool stream_initialized = false;
    const uint8_t body[] = {1};

    CHECK_GOTO(init_empty_raw_stream(&raw_stream) == 0);
    stream_initialized = true;

    trevrpc_stream message_limited = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = &raw_stream,
        .max_frame_size = 4096,
        .max_stream_messages = 1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    CHECK_GOTO(trevrpc_stream_send_message(&message_limited, body, sizeof(body)) == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(message_limited.response_message_count == 0);
    CHECK_GOTO(message_limited.response_body_size == 0);
    CHECK_GOTO(message_limited.failure_status == TREVRPC_STATUS_OK);
    CHECK_GOTO(trevrpc_stream_send_message(&message_limited, body, sizeof(body)) == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(message_limited.response_message_count == 0);
    CHECK_GOTO(message_limited.failure_status == TREVRPC_STATUS_OK);

    trevrpc_stream body_limited = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = &raw_stream,
        .max_frame_size = 4096,
        .max_stream_messages = -1,
        .max_stream_body_size = 1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    CHECK_GOTO(trevrpc_stream_send_message(&body_limited, body, sizeof(body)) == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(body_limited.response_message_count == 0);
    CHECK_GOTO(body_limited.response_body_size == 0);
    CHECK_GOTO(body_limited.failure_status == TREVRPC_STATUS_OK);
    CHECK_GOTO(trevrpc_stream_send_message(&body_limited, body, sizeof(body)) == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(body_limited.response_message_count == 0);
    CHECK_GOTO(body_limited.response_body_size == 0);
    CHECK_GOTO(body_limited.failure_status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&raw_stream);
    }
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
    CHECK_GOTO(stream.response_message_count == 0);
    CHECK_GOTO(stream.response_body_size == 0);
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
    CHECK_GOTO(stream.response_message_count == 0);
    CHECK_GOTO(stream.response_body_size == 0);
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
    trevrpc_wire_stream_frame_values* frame = (trevrpc_wire_stream_frame_values*)1;

    CHECK_GOTO(pthread_mutex_init(&raw_stream.mutex, NULL) == 0);
    mutex_initialized = true;
    CHECK_GOTO(pthread_cond_init(&raw_stream.cond, NULL) == 0);
    cond_initialized = true;

    CHECK_GOTO(trevrpc_internal_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_IDLE_TIMEOUT);
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

static int run_failure_envelope_case(const uint8_t* request_frame,
    size_t request_frame_len,
    uint32_t route_kind,
    int context_init_error,
    const uint8_t* expected_frame,
    size_t expected_frame_len,
    bool expected_streaming,
    uint32_t expected_status) {
    int result = 1;
    int handler_calls = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;

    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    if (route_kind == TREVRPC_RPC_KIND_UNARY) {
        CHECK_GOTO(trevrpc_internal_server_register_unary(
                       server, "svc", "method", counting_unary_handler, &handler_calls) == 0);
    } else if (route_kind >= TREVRPC_RPC_KIND_CLIENT_STREAMING &&
               route_kind <= TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        CHECK_GOTO(trevrpc_server_register_streaming(
                       server, "svc", "method", route_kind, counting_stream_handler, &handler_calls) == 0);
    }
    CHECK_GOTO(init_raw_stream(&stream, request_frame, request_frame_len) == 0);
    stream_initialized = true;
    if (context_init_error != 0) {
        trevrpc_test_fail_next_call_context_init(context_init_error);
    }

    trevrpc_test_server_handle_stream(server, &stream);

    CHECK_GOTO(handler_calls == 0);
    CHECK_GOTO(trevrpc_test_server_unary_status_count(server) == (expected_streaming ? 0 : 1));
    CHECK_GOTO(trevrpc_test_server_stream_status_count(server) == (expected_streaming ? 1 : 0));
    CHECK_GOTO(expected_streaming ? trevrpc_test_server_last_stream_status(server) == expected_status
                                  : trevrpc_test_server_last_unary_status(server) == expected_status);
    size_t frame_len = 0;
    bool finished_send = false;
    const uint8_t* frame = trevrpc_test_server_last_envelope(server, &frame_len, &finished_send);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(finished_send);
    CHECK_GOTO(frame_len == expected_frame_len);
    CHECK_GOTO(memcmp(frame, expected_frame, expected_frame_len) == 0);

    result = 0;

cleanup:
    trevrpc_test_fail_next_call_context_init(0);
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    test_server_destroy(server);
    return result;
}

static int run_encoded_failure_envelope_case(uint32_t kind,
    uint64_t timeout_nanos,
    int context_init_error,
    const uint8_t* expected_frame,
    size_t expected_frame_len,
    uint32_t expected_status) {
    int result = 1;
    uint8_t* request_frame = NULL;
    size_t request_frame_len = 0;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", kind, NULL, 0, NULL, timeout_nanos, 4096, &request_frame, &request_frame_len) == 0);
    CHECK_GOTO(run_failure_envelope_case(request_frame,
                   request_frame_len,
                   kind,
                   context_init_error,
                   expected_frame,
                   expected_frame_len,
                   kind != TREVRPC_RPC_KIND_UNARY,
                   expected_status) == 0);

    result = 0;

cleanup:
    free(request_frame);
    return result;
}

static int test_kind_aware_pre_handler_failure_envelopes(void) {
    int result = 1;
    static const uint8_t unsupported_version_unary_request[] = "\x00\x00\x00\x0a\x0a\x03"
                                                               "svc"
                                                               "\x12\x01"
                                                               "m"
                                                               "\x30\x02";
    static const uint8_t unsupported_version_client_stream_request[] = "\x00\x00\x00\x0c\x0a\x03"
                                                                       "svc"
                                                                       "\x12\x01"
                                                                       "m"
                                                                       "\x28\x01\x30\x02";
    static const uint8_t unsupported_version_server_stream_request[] = "\x00\x00\x00\x0c\x0a\x03"
                                                                       "svc"
                                                                       "\x12\x01"
                                                                       "m"
                                                                       "\x28\x02\x30\x02";
    static const uint8_t unsupported_version_bidi_stream_request[] = "\x00\x00\x00\x0c\x0a\x03"
                                                                     "svc"
                                                                     "\x12\x01"
                                                                     "m"
                                                                     "\x28\x03\x30\x02";
    static const uint8_t invalid_metadata_unary_request[] = "\x00\x00\x00\x0d\x0a\x03"
                                                            "svc"
                                                            "\x12\x01"
                                                            "m"
                                                            "\x22\x01\x00\x30\x01";
    static const uint8_t invalid_metadata_stream_request[] = "\x00\x00\x00\x0f\x0a\x03"
                                                             "svc"
                                                             "\x12\x01"
                                                             "m"
                                                             "\x22\x01\x00\x28\x03\x30\x01";
    static const uint8_t missing_service_unary_request[] = "\x00\x00\x00\x05\x12\x01"
                                                           "m"
                                                           "\x30\x01";
    static const uint8_t missing_service_stream_request[] = "\x00\x00\x00\x07\x12\x01"
                                                            "m"
                                                            "\x28\x02\x30\x01";
    static const uint8_t unsupported_kind_request[] = "\x00\x00\x00\x0c\x0a\x03"
                                                      "svc"
                                                      "\x12\x01"
                                                      "m"
                                                      "\x28\x63\x30\x01";
    static const uint8_t malformed_request[] = "\x00\x00\x00\x01\x80";
    static const uint8_t overflowed_kind_request[] = {0x00,
        0x00,
        0x00,
        0x15,
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x83,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x02,
        0x30,
        0x01};
    static const uint8_t overflowed_version_request[] = {0x00,
        0x00,
        0x00,
        0x15,
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x03,
        0x30,
        0x81,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x02};
    static const uint8_t unsupported_version_unary[] = "\x00\x00\x00\x24\x08\x09\x12\x20"
                                                       "unsupported TrevRPC wire version";
    static const uint8_t unsupported_version_stream[] = "\x00\x00\x00\x26\x08\x01\x10\x09\x1a\x20"
                                                        "unsupported TrevRPC wire version";
    static const uint8_t invalid_frame_unary[] = "\x00\x00\x00\x19\x08\x03\x12\x15"
                                                 "invalid request frame";
    static const uint8_t invalid_frame_stream[] = "\x00\x00\x00\x1b\x08\x01\x10\x03\x1a\x15"
                                                  "invalid request frame";
    static const uint8_t invalid_request_unary[] = "\x00\x00\x00\x13\x08\x03\x12\x0f"
                                                   "invalid request";

    CHECK_GOTO(run_failure_envelope_case(unsupported_version_unary_request,
                   sizeof(unsupported_version_unary_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   unsupported_version_unary,
                   sizeof(unsupported_version_unary) - 1,
                   false,
                   TREVRPC_STATUS_FAILED_PRECONDITION) == 0);
    CHECK_GOTO(run_failure_envelope_case(unsupported_version_client_stream_request,
                   sizeof(unsupported_version_client_stream_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   unsupported_version_stream,
                   sizeof(unsupported_version_stream) - 1,
                   true,
                   TREVRPC_STATUS_FAILED_PRECONDITION) == 0);
    CHECK_GOTO(run_failure_envelope_case(unsupported_version_server_stream_request,
                   sizeof(unsupported_version_server_stream_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   unsupported_version_stream,
                   sizeof(unsupported_version_stream) - 1,
                   true,
                   TREVRPC_STATUS_FAILED_PRECONDITION) == 0);
    CHECK_GOTO(run_failure_envelope_case(unsupported_version_bidi_stream_request,
                   sizeof(unsupported_version_bidi_stream_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   unsupported_version_stream,
                   sizeof(unsupported_version_stream) - 1,
                   true,
                   TREVRPC_STATUS_FAILED_PRECONDITION) == 0);
    CHECK_GOTO(run_failure_envelope_case(invalid_metadata_unary_request,
                   sizeof(invalid_metadata_unary_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   invalid_frame_unary,
                   sizeof(invalid_frame_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(invalid_metadata_stream_request,
                   sizeof(invalid_metadata_stream_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   invalid_frame_stream,
                   sizeof(invalid_frame_stream) - 1,
                   true,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(missing_service_unary_request,
                   sizeof(missing_service_unary_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   invalid_frame_unary,
                   sizeof(invalid_frame_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(missing_service_stream_request,
                   sizeof(missing_service_stream_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   invalid_frame_stream,
                   sizeof(invalid_frame_stream) - 1,
                   true,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(unsupported_kind_request,
                   sizeof(unsupported_kind_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   invalid_request_unary,
                   sizeof(invalid_request_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(malformed_request,
                   sizeof(malformed_request) - 1,
                   TREVRPC_WIRE_REQUEST_KIND_UNKNOWN,
                   0,
                   invalid_frame_unary,
                   sizeof(invalid_frame_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(overflowed_kind_request,
                   sizeof(overflowed_kind_request),
                   TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
                   0,
                   invalid_frame_unary,
                   sizeof(invalid_frame_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_failure_envelope_case(overflowed_version_request,
                   sizeof(overflowed_version_request),
                   TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
                   0,
                   invalid_frame_unary,
                   sizeof(invalid_frame_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);

    result = 0;

cleanup:
    return result;
}

static int test_kind_aware_request_preparation_failures(void) {
    int result = 1;
    static const uint8_t timeout_too_large_unary[] = "\x00\x00\x00\x1c\x08\x03\x12\x18"
                                                     "RPC timeout is too large";
    static const uint8_t timeout_too_large_stream[] = "\x00\x00\x00\x1e\x08\x01\x10\x03\x1a\x18"
                                                      "RPC timeout is too large";
    static const uint8_t timeout_overflow_unary[] = "\x00\x00\x00\x1a\x08\x03\x12\x16"
                                                    "RPC timeout overflowed";
    static const uint8_t timeout_overflow_stream[] = "\x00\x00\x00\x1c\x08\x01\x10\x03\x1a\x16"
                                                     "RPC timeout overflowed";
    static const uint8_t preparation_failure_unary[] = "\x00\x00\x00\x1d\x08\x0d\x12\x19"
                                                       "failed to prepare request";
    static const uint8_t preparation_failure_stream[] = "\x00\x00\x00\x1f\x08\x01\x10\x0d\x1a\x19"
                                                        "failed to prepare request";
    static const uint8_t deadline_unary[] = "\x00\x00\x00\x19\x08\x04\x12\x15"
                                            "RPC deadline exceeded";
    static const uint8_t deadline_stream[] = "\x00\x00\x00\x1b\x08\x01\x10\x04\x1a\x15"
                                             "RPC deadline exceeded";
    const uint32_t kinds[] = {
        TREVRPC_RPC_KIND_SERVER_STREAMING,
        TREVRPC_RPC_KIND_CLIENT_STREAMING,
        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
    };

    CHECK_GOTO(run_encoded_failure_envelope_case(TREVRPC_RPC_KIND_UNARY,
                   (uint64_t)INT64_MAX + 1,
                   0,
                   timeout_too_large_unary,
                   sizeof(timeout_too_large_unary) - 1,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_encoded_failure_envelope_case(TREVRPC_RPC_KIND_UNARY,
                   1,
                   -EOVERFLOW,
                   timeout_overflow_unary,
                   sizeof(timeout_overflow_unary) - 1,
                   TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
    CHECK_GOTO(run_encoded_failure_envelope_case(TREVRPC_RPC_KIND_UNARY,
                   1,
                   -EIO,
                   preparation_failure_unary,
                   sizeof(preparation_failure_unary) - 1,
                   TREVRPC_STATUS_INTERNAL) == 0);
    CHECK_GOTO(run_encoded_failure_envelope_case(TREVRPC_RPC_KIND_UNARY,
                   1,
                   0,
                   deadline_unary,
                   sizeof(deadline_unary) - 1,
                   TREVRPC_STATUS_DEADLINE_EXCEEDED) == 0);

    for (size_t i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        CHECK_GOTO(run_encoded_failure_envelope_case(kinds[i],
                       (uint64_t)INT64_MAX + 1,
                       0,
                       timeout_too_large_stream,
                       sizeof(timeout_too_large_stream) - 1,
                       TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
        CHECK_GOTO(run_encoded_failure_envelope_case(kinds[i],
                       1,
                       -EOVERFLOW,
                       timeout_overflow_stream,
                       sizeof(timeout_overflow_stream) - 1,
                       TREVRPC_STATUS_INVALID_ARGUMENT) == 0);
        CHECK_GOTO(run_encoded_failure_envelope_case(kinds[i],
                       1,
                       -EIO,
                       preparation_failure_stream,
                       sizeof(preparation_failure_stream) - 1,
                       TREVRPC_STATUS_INTERNAL) == 0);
        CHECK_GOTO(
            run_encoded_failure_envelope_case(
                kinds[i], 1, 0, deadline_stream, sizeof(deadline_stream) - 1, TREVRPC_STATUS_DEADLINE_EXCEEDED) == 0);
    }

    result = 0;

cleanup:
    return result;
}

static int run_metrics_case_with_config(const trevrpc_client_config_internal* config,
    const uint8_t* request_body,
    size_t request_body_len,
    trevrpc_unary_handler_internal handler,
    metric_counts* counts);

static int run_metrics_case(const uint8_t* request_body,
    size_t request_body_len,
    trevrpc_unary_handler_internal handler,
    metric_counts* counts) {
    return run_metrics_case_with_config(NULL, request_body, request_body_len, handler, counts);
}

static int run_metrics_case_with_config(const trevrpc_client_config_internal* config,
    const uint8_t* request_body,
    size_t request_body_len,
    trevrpc_unary_handler_internal handler,
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
        CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "method", handler, NULL) == 0);
    }
    CHECK_GOTO(init_raw_stream(&stream, request_body, request_body_len) == 0);
    stream_initialized = true;

    trevrpc_test_server_handle_stream(server, &stream);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    test_server_destroy(server);
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
    test_server_destroy(server);
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

    CHECK_GOTO(trevrpc_test_status_from_error(TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED, &message) ==
               TREVRPC_STATUS_RESOURCE_EXHAUSTED);
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
    test_server_destroy(server);
    return result;
}

static int test_transport_status_from_error_is_predictable(void) {
    int result = 1;
    const char* message = NULL;
    metric_counts counts = {0};

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_MSQUIC_ERR_FRAME_TOO_LARGE, &message) ==
               TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(message != NULL);

    CHECK_GOTO(trevrpc_test_transport_status_from_error(TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED, &message) ==
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

    memset(&counts, 0, sizeof(counts));
    CHECK_GOTO(run_transport_error_case(TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED, &counts) == 0);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);

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
    trevrpc_client_config_internal small_frame_config = trevrpc_internal_default_config();

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
    test_server_destroy(server);
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
    trevrpc_wire_response_values response = {0};
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

    CHECK_GOTO(trevrpc_internal_response_set_body(&response, response_body, sizeof(response_body)) == 0);
    CHECK_GOTO(trevrpc_call_retain(state.call) == 0);
    CHECK_GOTO(trevrpc_internal_call_respond(state.call, &response) == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(counts.finished == 0);
    CHECK_GOTO(trevrpc_call_request(state.call) != NULL);
    trevrpc_call_release(state.call);
    state.call = NULL;
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(counts.response_body_len == sizeof(response_body));

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    if (state.call != NULL) {
        trevrpc_call_close(state.call);
    }
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    test_server_destroy(server);
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

    CHECK_GOTO(
        trevrpc_call_finish_stream(state.call, TREVRPC_STATUS_PERMISSION_DENIED, NULL, 0) == TREV_MSQUIC_ERR_CLOSED);
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
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_call_finish_serializes_direct_terminal_status(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    bool hook_mutex_initialized = false;
    bool hook_initialized = false;
    bool status_started = false;
    bool finish_started = false;
    metric_counts counts = {0};
    raw_call_state call_state = {0};
    call_finish_race_state race = {0};
    pthread_t status_thread;
    pthread_t finish_thread;
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
            server, "svc", "method", TREVRPC_RPC_KIND_SERVER_STREAMING, raw_stream_deferred_handler, &call_state) == 0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;
    trevrpc_test_server_handle_stream(server, &stream);
    CHECK_GOTO(call_state.call != NULL);

    CHECK_GOTO(pthread_mutex_init(&race.mutex, NULL) == 0);
    hook_mutex_initialized = true;
    CHECK_GOTO(pthread_cond_init(&race.cond, NULL) == 0);
    hook_initialized = true;
    race.call = call_state.call;
    race.stream = trevrpc_call_stream(call_state.call);
    CHECK_GOTO(race.stream != NULL);
    trevrpc_test_set_stream_send_hook(call_finish_race_hook, &race);

    CHECK_GOTO(pthread_create(&status_thread, NULL, direct_status_main, &race) == 0);
    status_started = true;
    pthread_mutex_lock(&race.mutex);
    while (!race.status_claimed) {
        pthread_cond_wait(&race.cond, &race.mutex);
    }
    pthread_mutex_unlock(&race.mutex);

    CHECK_GOTO(pthread_create(&finish_thread, NULL, call_finish_main, &race) == 0);
    finish_started = true;
    pthread_mutex_lock(&race.mutex);
    while (!race.finish_waiting) {
        pthread_cond_wait(&race.cond, &race.mutex);
    }
    race.release_status = true;
    pthread_cond_broadcast(&race.cond);
    pthread_mutex_unlock(&race.mutex);

    CHECK_GOTO(pthread_join(status_thread, NULL) == 0);
    status_started = false;
    CHECK_GOTO(pthread_join(finish_thread, NULL) == 0);
    finish_started = false;
    call_state.call = NULL;
    trevrpc_test_set_stream_send_hook(NULL, NULL);
    CHECK_GOTO(race.status_result == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(race.finish_result == 0);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_PERMISSION_DENIED);
    result = 0;

cleanup:
    trevrpc_test_set_stream_send_hook(NULL, NULL);
    if (hook_initialized) {
        pthread_mutex_lock(&race.mutex);
        race.release_status = true;
        pthread_cond_broadcast(&race.cond);
        pthread_mutex_unlock(&race.mutex);
    }
    if (status_started) {
        (void)pthread_join(status_thread, NULL);
    }
    if (finish_started) {
        (void)pthread_join(finish_thread, NULL);
        call_state.call = NULL;
    }
    if (call_state.call != NULL) {
        trevrpc_call_close(call_state.call);
    }
    if (hook_initialized) {
        pthread_cond_destroy(&race.cond);
    }
    if (hook_mutex_initialized) {
        pthread_mutex_destroy(&race.mutex);
    }
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_server_cancel_closes_all_deferred_calls_without_snapshot_allocation(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream streams[2] = {0};
    size_t initialized_streams = 0;
    metric_counts counts = {0};
    multiple_raw_calls_state state = {0};
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
            server, "svc", "method", TREVRPC_RPC_KIND_SERVER_STREAMING, multiple_raw_calls_deferred_handler, &state) ==
        0);
    for (size_t i = 0; i < 2; i++) {
        CHECK_GOTO(init_raw_stream(&streams[i], frame, frame_len) == 0);
        initialized_streams++;
        trevrpc_test_server_handle_stream(server, &streams[i]);
    }
    CHECK_GOTO(state.called == 2);
    CHECK_GOTO(counts.started == 2 && counts.finished == 0);
    CHECK_GOTO(trevrpc_server_cancel(server) == 0);
    CHECK_GOTO(counts.finished == 2);
    CHECK_GOTO(counts.status_counts[TREVRPC_STATUS_CANCELLED] == 2);
    CHECK_GOTO(trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE) == 0);
    CHECK_GOTO(trevrpc_server_release(server) == 0);
    server = NULL;
    result = 0;

cleanup:
    for (size_t i = 0; i < initialized_streams; i++) {
        reset_raw_stream(&streams[i]);
    }
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_server_cancel_wins_call_insertion_race(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    trevrpc_msquic_stream stream = {0};
    bool stream_initialized = false;
    bool hook_mutex_initialized = false;
    bool hook_initialized = false;
    bool handle_started = false;
    metric_counts counts = {0};
    multiple_raw_calls_state calls = {0};
    call_insert_race_state race = {0};
    pthread_t handle_thread;
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
            server, "svc", "method", TREVRPC_RPC_KIND_SERVER_STREAMING, multiple_raw_calls_deferred_handler, &calls) ==
        0);
    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;
    CHECK_GOTO(pthread_mutex_init(&race.mutex, NULL) == 0);
    hook_mutex_initialized = true;
    CHECK_GOTO(pthread_cond_init(&race.cond, NULL) == 0);
    hook_initialized = true;
    race.server = server;
    race.stream = &stream;
    trevrpc_test_set_call_insert_hook(call_insert_race_hook, &race);

    CHECK_GOTO(pthread_create(&handle_thread, NULL, handle_stream_main, &race) == 0);
    handle_started = true;
    pthread_mutex_lock(&race.mutex);
    while (!race.insert_waiting) {
        pthread_cond_wait(&race.cond, &race.mutex);
    }
    pthread_mutex_unlock(&race.mutex);

    CHECK_GOTO(trevrpc_server_cancel(server) == 0);
    pthread_mutex_lock(&race.mutex);
    race.release_insert = true;
    pthread_cond_broadcast(&race.cond);
    pthread_mutex_unlock(&race.mutex);
    CHECK_GOTO(pthread_join(handle_thread, NULL) == 0);
    handle_started = false;
    trevrpc_test_set_call_insert_hook(NULL, NULL);

    CHECK_GOTO(calls.called == 0);
    CHECK_GOTO(counts.started == 1 && counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_CANCELLED);
    CHECK_GOTO(trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE) == 0);
    CHECK_GOTO(trevrpc_server_release(server) == 0);
    server = NULL;
    result = 0;

cleanup:
    trevrpc_test_set_call_insert_hook(NULL, NULL);
    if (hook_initialized) {
        pthread_mutex_lock(&race.mutex);
        race.release_insert = true;
        pthread_cond_broadcast(&race.cond);
        pthread_mutex_unlock(&race.mutex);
    }
    if (handle_started) {
        (void)pthread_join(handle_thread, NULL);
    }
    if (hook_initialized) {
        pthread_cond_destroy(&race.cond);
    }
    if (hook_mutex_initialized) {
        pthread_mutex_destroy(&race.mutex);
    }
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    test_server_destroy(server);
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
    (void)trevrpc_server_stop(server);
    CHECK_GOTO(!trevrpc_test_server_request_try_start(server));
    trevrpc_test_server_request_finish(server);

    result = 0;

cleanup:
    test_server_destroy(server);
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
    trevrpc_server_options_internal options = trevrpc_internal_default_server_options();
    options.max_concurrent_requests = 1;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_internal_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "method", success_handler, NULL) == 0);
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
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_concurrent_connection_limit_rejects_overload(void) {
    int result = 1;
    trevrpc_server* server = NULL;
    trevrpc_server_options_internal options = trevrpc_internal_default_server_options();
    options.max_concurrent_connections = 1;

    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_internal_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_test_server_connection_try_start(server));
    CHECK_GOTO(!trevrpc_test_server_connection_try_start(server));
    trevrpc_test_server_connection_finish(server);
    CHECK_GOTO(trevrpc_test_server_connection_try_start(server));
    trevrpc_test_server_connection_finish(server);

    result = 0;

cleanup:
    test_server_destroy(server);
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

static int test_route_registration_freezes_before_serving(void) {
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

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "method", success_handler, NULL) == 0);
    trevrpc_test_server_freeze_routes(server);

    CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "late", success_handler, NULL) == -EALREADY);
    CHECK_GOTO(
        trevrpc_server_register_streaming(
            server, "svc", "late-stream", TREVRPC_RPC_KIND_SERVER_STREAMING, stream_error_handler, NULL) == -EALREADY);
    CHECK_GOTO(trevrpc_server_register_call(
                   server, "svc", "late-call", TREVRPC_RPC_KIND_UNARY, raw_unary_immediate_handler, NULL) == -EALREADY);

    CHECK_GOTO(init_raw_stream(&stream, frame, frame_len) == 0);
    stream_initialized = true;
    trevrpc_test_server_handle_stream(server, &stream);
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    if (stream_initialized) {
        reset_raw_stream(&stream);
    }
    test_server_destroy(server);
    free(frame);
    return result;
}

static int run_queue_full_failure_case(trevrpc_server* server,
    void* limiter,
    const uint8_t* request_frame,
    size_t request_frame_len,
    const uint8_t* expected_frame,
    size_t expected_frame_len,
    bool expected_streaming,
    uint32_t expected_status,
    size_t expected_unary_count,
    size_t expected_stream_count) {
    int result = 1;
    trevrpc_msquic_stream* stream = new_raw_stream(request_frame, request_frame_len);
    CHECK_GOTO(stream != NULL);

    int err = trevrpc_test_server_queue_msquic_stream_record_envelope(server, stream, limiter);
    stream = NULL;
    CHECK_GOTO(err == 1);
    CHECK_GOTO(trevrpc_test_server_unary_status_count(server) == expected_unary_count);
    CHECK_GOTO(trevrpc_test_server_stream_status_count(server) == expected_stream_count);
    CHECK_GOTO(expected_streaming ? trevrpc_test_server_last_stream_status(server) == expected_status
                                  : trevrpc_test_server_last_unary_status(server) == expected_status);
    size_t frame_len = 0;
    bool finished_send = false;
    const uint8_t* frame = trevrpc_test_server_last_envelope(server, &frame_len, &finished_send);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(finished_send);
    CHECK_GOTO(frame_len == expected_frame_len);
    CHECK_GOTO(memcmp(frame, expected_frame, expected_frame_len) == 0);

    result = 0;

cleanup:
    free_unowned_raw_stream(stream);
    return result;
}

static int test_worker_queue_saturation_rejects_overload(void) {
    int result = 1;
    static const uint8_t valid_stream_request[] = "\x00\x00\x00\x0c\x0a\x03"
                                                  "svc"
                                                  "\x12\x01"
                                                  "m"
                                                  "\x28\x03\x30\x01";
    static const uint8_t unsupported_version_unary_request[] = "\x00\x00\x00\x0a\x0a\x03"
                                                               "svc"
                                                               "\x12\x01"
                                                               "m"
                                                               "\x30\x02";
    static const uint8_t unsupported_version_stream_request[] = "\x00\x00\x00\x0c\x0a\x03"
                                                                "svc"
                                                                "\x12\x01"
                                                                "m"
                                                                "\x28\x02\x30\x02";
    static const uint8_t invalid_metadata_unary_request[] = "\x00\x00\x00\x0d\x0a\x03"
                                                            "svc"
                                                            "\x12\x01"
                                                            "m"
                                                            "\x22\x01\x00\x30\x01";
    static const uint8_t invalid_metadata_stream_request[] = "\x00\x00\x00\x0f\x0a\x03"
                                                             "svc"
                                                             "\x12\x01"
                                                             "m"
                                                             "\x22\x01\x00\x28\x03\x30\x01";
    static const uint8_t queue_full_unary[] = "\x00\x00\x00\x1c\x08\x08\x12\x18"
                                              "RPC worker queue is full";
    static const uint8_t queue_full_stream[] = "\x00\x00\x00\x1e\x08\x01\x10\x08\x1a\x18"
                                               "RPC worker queue is full";
    static const uint8_t unsupported_version_unary[] = "\x00\x00\x00\x24\x08\x09\x12\x20"
                                                       "unsupported TrevRPC wire version";
    static const uint8_t unsupported_version_stream[] = "\x00\x00\x00\x26\x08\x01\x10\x09\x1a\x20"
                                                        "unsupported TrevRPC wire version";
    static const uint8_t invalid_frame_unary[] = "\x00\x00\x00\x19\x08\x03\x12\x15"
                                                 "invalid request frame";
    static const uint8_t invalid_frame_stream[] = "\x00\x00\x00\x1b\x08\x01\x10\x03\x1a\x15"
                                                  "invalid request frame";
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    void* limiter = NULL;
    trevrpc_msquic_stream* active_stream = NULL;
    trevrpc_msquic_stream* queued_stream = NULL;
    metric_counts counts = {0};
    blocking_handler_state blocker = {0};
    bool blocker_initialized = false;
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };
    trevrpc_server_options_internal options = trevrpc_internal_default_server_options();
    options.worker_count = 1;
    options.worker_queue_capacity = 1;

    CHECK_GOTO(blocking_handler_state_init(&blocker) == 0);
    blocker_initialized = true;
    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_internal_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "method", blocking_unary_handler, &blocker) == 0);
    trevrpc_test_server_freeze_routes(server);
    CHECK_GOTO(trevrpc_test_server_start_worker_pool(server) == 0);
    limiter = trevrpc_test_conn_stream_limiter_new();
    CHECK_GOTO(limiter != NULL);

    active_stream = new_raw_stream(frame, frame_len);
    CHECK_GOTO(active_stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, active_stream, limiter) == 0);
    active_stream = NULL;
    CHECK_GOTO(blocking_handler_wait_entered(&blocker, 1, 1000));

    queued_stream = new_raw_stream(frame, frame_len);
    CHECK_GOTO(queued_stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, queued_stream, limiter) == 0);
    queued_stream = NULL;

    CHECK_GOTO(run_queue_full_failure_case(server,
                   limiter,
                   frame,
                   frame_len,
                   queue_full_unary,
                   sizeof(queue_full_unary) - 1,
                   false,
                   TREVRPC_STATUS_RESOURCE_EXHAUSTED,
                   1,
                   0) == 0);
    CHECK_GOTO(run_queue_full_failure_case(server,
                   limiter,
                   valid_stream_request,
                   sizeof(valid_stream_request) - 1,
                   queue_full_stream,
                   sizeof(queue_full_stream) - 1,
                   true,
                   TREVRPC_STATUS_RESOURCE_EXHAUSTED,
                   1,
                   1) == 0);
    CHECK_GOTO(run_queue_full_failure_case(server,
                   limiter,
                   unsupported_version_unary_request,
                   sizeof(unsupported_version_unary_request) - 1,
                   unsupported_version_unary,
                   sizeof(unsupported_version_unary) - 1,
                   false,
                   TREVRPC_STATUS_FAILED_PRECONDITION,
                   2,
                   1) == 0);
    CHECK_GOTO(run_queue_full_failure_case(server,
                   limiter,
                   unsupported_version_stream_request,
                   sizeof(unsupported_version_stream_request) - 1,
                   unsupported_version_stream,
                   sizeof(unsupported_version_stream) - 1,
                   true,
                   TREVRPC_STATUS_FAILED_PRECONDITION,
                   2,
                   2) == 0);
    CHECK_GOTO(run_queue_full_failure_case(server,
                   limiter,
                   invalid_metadata_unary_request,
                   sizeof(invalid_metadata_unary_request) - 1,
                   invalid_frame_unary,
                   sizeof(invalid_frame_unary) - 1,
                   false,
                   TREVRPC_STATUS_INVALID_ARGUMENT,
                   3,
                   2) == 0);
    CHECK_GOTO(run_queue_full_failure_case(server,
                   limiter,
                   invalid_metadata_stream_request,
                   sizeof(invalid_metadata_stream_request) - 1,
                   invalid_frame_stream,
                   sizeof(invalid_frame_stream) - 1,
                   true,
                   TREVRPC_STATUS_INVALID_ARGUMENT,
                   3,
                   3) == 0);
    CHECK_GOTO(blocking_handler_entered(&blocker) == 1);
    CHECK_GOTO(counts.status_counts[TREVRPC_STATUS_RESOURCE_EXHAUSTED] == 2);
    CHECK_GOTO(counts.status_counts[TREVRPC_STATUS_FAILED_PRECONDITION] == 2);
    CHECK_GOTO(counts.status_counts[TREVRPC_STATUS_INVALID_ARGUMENT] == 2);

    blocking_handler_state_release(&blocker);
    (void)trevrpc_server_stop(server);
    CHECK_GOTO(trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC));

    result = 0;

cleanup:
    if (blocker_initialized) {
        blocking_handler_state_release(&blocker);
    }
    if (server != NULL) {
        (void)trevrpc_server_stop(server);
        (void)trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC);
    }
    if (limiter != NULL) {
        trevrpc_test_conn_stream_limiter_release(limiter);
    }
    free_unowned_raw_stream(active_stream);
    free_unowned_raw_stream(queued_stream);
    if (blocker_initialized) {
        blocking_handler_state_destroy(&blocker);
    }
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_worker_queue_shutdown_drains_queued_and_active(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    void* limiter = NULL;
    trevrpc_msquic_stream* active_stream = NULL;
    trevrpc_msquic_stream* queued_stream = NULL;
    metric_counts counts = {0};
    blocking_handler_state blocker = {0};
    bool blocker_initialized = false;
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };
    trevrpc_server_options_internal options = trevrpc_internal_default_server_options();
    options.worker_count = 1;
    options.worker_queue_capacity = 2;

    CHECK_GOTO(blocking_handler_state_init(&blocker) == 0);
    blocker_initialized = true;
    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_internal_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "method", blocking_unary_handler, &blocker) == 0);
    trevrpc_test_server_freeze_routes(server);
    CHECK_GOTO(trevrpc_test_server_start_worker_pool(server) == 0);
    limiter = trevrpc_test_conn_stream_limiter_new();
    CHECK_GOTO(limiter != NULL);

    active_stream = new_raw_stream(frame, frame_len);
    CHECK_GOTO(active_stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, active_stream, limiter) == 0);
    active_stream = NULL;
    CHECK_GOTO(blocking_handler_wait_entered(&blocker, 1, 1000));

    queued_stream = new_raw_stream(frame, frame_len);
    CHECK_GOTO(queued_stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, queued_stream, limiter) == 0);
    queued_stream = NULL;

    (void)trevrpc_server_stop(server);
    CHECK_GOTO(!trevrpc_test_server_wait_for_tasks(server, 1000000ull));
    blocking_handler_state_release(&blocker);
    CHECK_GOTO(trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC));
    CHECK_GOTO(blocking_handler_entered(&blocker) == 1);
    CHECK_GOTO(counts.status_counts[TREVRPC_STATUS_RESOURCE_EXHAUSTED] == 1);

    result = 0;

cleanup:
    if (blocker_initialized) {
        blocking_handler_state_release(&blocker);
    }
    if (server != NULL) {
        (void)trevrpc_server_stop(server);
        (void)trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC);
    }
    if (limiter != NULL) {
        trevrpc_test_conn_stream_limiter_release(limiter);
    }
    free_unowned_raw_stream(active_stream);
    free_unowned_raw_stream(queued_stream);
    if (blocker_initialized) {
        blocking_handler_state_destroy(&blocker);
    }
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_worker_queue_reset_while_queued(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    void* limiter = NULL;
    trevrpc_msquic_stream* active_stream = NULL;
    trevrpc_msquic_stream* reset_stream = NULL;
    metric_counts counts = {0};
    blocking_handler_state blocker = {0};
    bool blocker_initialized = false;
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };
    trevrpc_server_options_internal options = trevrpc_internal_default_server_options();
    options.worker_count = 1;
    options.worker_queue_capacity = 2;

    CHECK_GOTO(blocking_handler_state_init(&blocker) == 0);
    blocker_initialized = true;
    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_internal_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_internal_server_register_unary(server, "svc", "method", blocking_unary_handler, &blocker) == 0);
    trevrpc_test_server_freeze_routes(server);
    CHECK_GOTO(trevrpc_test_server_start_worker_pool(server) == 0);
    limiter = trevrpc_test_conn_stream_limiter_new();
    CHECK_GOTO(limiter != NULL);

    active_stream = new_raw_stream(frame, frame_len);
    CHECK_GOTO(active_stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, active_stream, limiter) == 0);
    active_stream = NULL;
    CHECK_GOTO(blocking_handler_wait_entered(&blocker, 1, 1000));

    reset_stream = new_empty_raw_stream();
    CHECK_GOTO(reset_stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, reset_stream, limiter) == 0);
    pthread_mutex_lock(&reset_stream->mutex);
    reset_stream->err = TREV_MSQUIC_ERR_CLOSED;
    pthread_cond_broadcast(&reset_stream->cond);
    pthread_mutex_unlock(&reset_stream->mutex);
    reset_stream = NULL;

    (void)trevrpc_server_stop(server);
    blocking_handler_state_release(&blocker);
    CHECK_GOTO(trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC));
    CHECK_GOTO(blocking_handler_entered(&blocker) == 1);
    CHECK_GOTO(counts.status_counts[TREVRPC_STATUS_CANCELLED] == 1);

    result = 0;

cleanup:
    if (blocker_initialized) {
        blocking_handler_state_release(&blocker);
    }
    if (server != NULL) {
        (void)trevrpc_server_stop(server);
        (void)trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC);
    }
    if (limiter != NULL) {
        trevrpc_test_conn_stream_limiter_release(limiter);
    }
    free_unowned_raw_stream(active_stream);
    free_unowned_raw_stream(reset_stream);
    if (blocker_initialized) {
        blocking_handler_state_destroy(&blocker);
    }
    test_server_destroy(server);
    free(frame);
    return result;
}

static int test_worker_queue_deferred_raw_call_cleanup(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_server* server = NULL;
    void* limiter = NULL;
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_call* call = NULL;
    metric_counts counts = {0};
    deferred_signal_state deferred = {0};
    bool deferred_initialized = false;
    trevrpc_wire_response_values response = {0};
    const uint8_t response_body[] = {'o', 'k'};
    trevrpc_metrics metrics = {
        .rpc_started = record_started,
        .rpc_finished = record_finished,
        .user_data = &counts,
    };
    trevrpc_server_options_internal options = trevrpc_internal_default_server_options();
    options.worker_count = 1;
    options.worker_queue_capacity = 1;

    CHECK_GOTO(deferred_signal_state_init(&deferred) == 0);
    deferred_initialized = true;
    CHECK_GOTO(trevrpc_wire_encode_request(
                   "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 4096, &frame, &frame_len) == 0);
    CHECK_GOTO(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK_GOTO(trevrpc_internal_server_set_options(server, &options) == 0);
    CHECK_GOTO(trevrpc_server_set_metrics(server, &metrics) == 0);
    CHECK_GOTO(trevrpc_server_register_call(
                   server, "svc", "method", TREVRPC_RPC_KIND_UNARY, raw_unary_deferred_signal_handler, &deferred) == 0);
    trevrpc_test_server_freeze_routes(server);
    CHECK_GOTO(trevrpc_test_server_start_worker_pool(server) == 0);
    limiter = trevrpc_test_conn_stream_limiter_new();
    CHECK_GOTO(limiter != NULL);

    stream = new_raw_stream(frame, frame_len);
    CHECK_GOTO(stream != NULL);
    CHECK_GOTO(trevrpc_test_server_queue_msquic_stream(server, stream, limiter) == 0);
    stream = NULL;
    CHECK_GOTO(deferred_signal_wait_called(&deferred, 1000));
    CHECK_GOTO(counts.started == 1);
    CHECK_GOTO(counts.finished == 0);

    (void)trevrpc_server_stop(server);
    CHECK_GOTO(!trevrpc_test_server_wait_for_tasks(server, 1000000ull));

    CHECK_GOTO(trevrpc_internal_response_set_body(&response, response_body, sizeof(response_body)) == 0);
    call = deferred_signal_take_call(&deferred);
    CHECK_GOTO(call != NULL);
    CHECK_GOTO(trevrpc_internal_call_respond(call, &response) == TREV_MSQUIC_ERR_CLOSED);
    call = NULL;
    CHECK_GOTO(trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC));
    CHECK_GOTO(counts.finished == 1);
    CHECK_GOTO(counts.status == TREVRPC_STATUS_OK);

    result = 0;

cleanup:
    if (call != NULL) {
        trevrpc_call_close(call);
    }
    trevrpc_internal_response_reset(&response);
    if (server != NULL) {
        (void)trevrpc_server_stop(server);
        (void)trevrpc_test_server_wait_for_tasks(server, 2ull * NANOS_PER_SEC);
    }
    if (limiter != NULL) {
        trevrpc_test_conn_stream_limiter_release(limiter);
    }
    free_unowned_raw_stream(stream);
    if (deferred_initialized) {
        deferred_signal_state_destroy(&deferred);
    }
    test_server_destroy(server);
    free(frame);
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
    if (test_cancellation_token_lifecycle() != 0) {
        return 1;
    }
    if (test_default_server_options() != 0) {
        return 1;
    }
    if (test_default_native_pending_send_limits() != 0) {
        return 1;
    }
    if (test_default_call_options() != 0) {
        return 1;
    }
    if (test_request_stream_message_limit() != 0) {
        return 1;
    }
    if (test_request_stream_message_limit_boundary() != 0) {
        return 1;
    }
    if (test_response_stream_message_limit() != 0) {
        return 1;
    }
    if (test_request_stream_body_size_limit() != 0) {
        return 1;
    }
    if (test_request_stream_body_size_limit_boundary() != 0) {
        return 1;
    }
    if (test_response_stream_body_size_limit() != 0) {
        return 1;
    }
    if (test_response_stream_limit_boundaries() != 0) {
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
    if (test_kind_aware_pre_handler_failure_envelopes() != 0) {
        return 1;
    }
    if (test_kind_aware_request_preparation_failures() != 0) {
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
    if (test_call_finish_serializes_direct_terminal_status() != 0) {
        return 1;
    }
    if (test_server_cancel_closes_all_deferred_calls_without_snapshot_allocation() != 0) {
        return 1;
    }
    if (test_server_cancel_wins_call_insertion_race() != 0) {
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
    if (test_route_registration_freezes_before_serving() != 0) {
        return 1;
    }
    if (test_worker_queue_saturation_rejects_overload() != 0) {
        return 1;
    }
    if (test_worker_queue_shutdown_drains_queued_and_active() != 0) {
        return 1;
    }
    if (test_worker_queue_reset_while_queued() != 0) {
        return 1;
    }
    if (test_worker_queue_deferred_raw_call_cleanup() != 0) {
        return 1;
    }
    return 0;
}
