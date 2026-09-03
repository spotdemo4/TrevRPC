#define _POSIX_C_SOURCE 200809L

#include "trevrpc_msquic.h"
#include "trevrpc.h"
#include "trevrpc_http3_frame_internal.h"
#include "trevrpc_msquic_internal.h"
#include "trevrpc_runtime_internal.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

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

#define CHECK_GOTO(condition)                                                                                          \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            result = 1;                                                                                                \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

#define CHECK_EQ_GOTO(actual, expected)                                                                                \
    do {                                                                                                               \
        int actual_value = (actual);                                                                                   \
        int expected_value = (expected);                                                                               \
        if (actual_value != expected_value) {                                                                          \
            fprintf(stderr,                                                                                            \
                "%s:%d: check failed: %s == %s (actual=%d expected=%d)\n",                                             \
                __FILE__,                                                                                              \
                __LINE__,                                                                                              \
                #actual,                                                                                               \
                #expected,                                                                                             \
                actual_value,                                                                                          \
                expected_value);                                                                                       \
            result = 1;                                                                                                \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

typedef struct accept_args {
    trevrpc_msquic_listener* listener;
    trevrpc_msquic_conn* conn;
    int result;
} accept_args;

typedef struct listener_shutdown_args {
    trevrpc_msquic_listener* listener;
    pthread_mutex_t* mutex;
    pthread_cond_t* cond;
    bool* start;
} listener_shutdown_args;

typedef struct stream_args {
    trevrpc_msquic_conn* conn;
    trevrpc_msquic_stream* stream;
    int result;
} stream_args;

typedef enum test_stream_write_kind {
    TEST_STREAM_WRITE_BYTES = 0,
    TEST_STREAM_WRITE_FRAME_PARTS = 1,
    TEST_STREAM_WRITE_MESSAGE = 2,
    TEST_STREAM_WRITE_MESSAGE_WAIT = 3,
    TEST_STREAM_WRITE_MESSAGES = 4,
    TEST_STREAM_WRITE_MESSAGES_BORROWED = 5,
    TEST_STREAM_WRITE_FIN_BYTES = 6,
    TEST_STREAM_WRITE_FIN_EMPTY = 7,
    TEST_STREAM_SHUTDOWN_SEND = 8,
} test_stream_write_kind;

typedef struct stream_write_args {
    trevrpc_msquic_stream* stream;
    test_stream_write_kind kind;
    intptr_t result;
} stream_write_args;

typedef struct tracked_send_args {
    trevrpc_msquic_stream* stream;
    trevrpc_msquic_send_completion* completion;
    intptr_t result;
} tracked_send_args;

typedef struct stream_close_args {
    trevrpc_msquic_stream* stream;
} stream_close_args;

typedef struct stream_read_args {
    trevrpc_msquic_stream* stream;
    uint8_t* data;
    size_t len;
    intptr_t result;
} stream_read_args;

typedef struct stream_hook_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_test_stream_event block_event;
    bool seen[TREV_MSQUIC_TEST_STREAM_EVENT_COUNT];
    size_t calls[TREV_MSQUIC_TEST_STREAM_EVENT_COUNT];
    bool release;
} stream_hook_state;

typedef struct stream_observer_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_stream* stream;
    size_t calls;
    uint32_t flags;
    bool block;
    bool release;
    bool self_clear;
    bool self_close;
    bool close_returned;
} stream_observer_state;

typedef struct raw_shutdown_close_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_raw_client* client;
    size_t calls;
    bool close_returned;
} raw_shutdown_close_state;

typedef struct conn_observer_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_conn* conn;
    size_t terminal_calls;
    bool block;
    bool release;
    bool self_close;
    bool close_returned;
} conn_observer_state;

typedef struct conn_close_operation {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_conn* conn;
    bool started;
    bool completed;
} conn_close_operation;

typedef struct stream_observer_operation {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_stream* stream;
    bool started;
    bool completed;
} stream_observer_operation;

typedef struct wt_accept_args {
    trevrpc_wt_listener* listener;
    trevrpc_wt_session* session;
    int result;
} wt_accept_args;

typedef struct wt_dial_args {
    trevrpc_wt_config config;
    trevrpc_wt_session* session;
    int result;
} wt_dial_args;

typedef struct wt_stream_args {
    trevrpc_wt_session* session;
    trevrpc_wt_stream* stream;
    int result;
} wt_stream_args;

typedef struct h3_accept_args {
    trevrpc_msquic_conn* conn;
    trevrpc_wt_config config;
    trevrpc_h3_conn* h3_conn;
    trevrpc_http3_admission admission;
    void* admission_user_data;
    size_t max_h3_frame_payload;
    int result;
} h3_accept_args;

typedef struct h3_stream_args {
    trevrpc_h3_conn* conn;
    trevrpc_wt_stream* wt_stream;
    trevrpc_h3_stream* h3_stream;
    int result;
} h3_stream_args;

typedef struct h3_resolve_args {
    trevrpc_h3_conn* conn;
    trevrpc_h3_stream* stream;
    trevrpc_wt_stream* wt_stream;
    uint64_t timeout_nanos;
    int resolution;
    int result;
} h3_resolve_args;

int trevrpc_h3_test_wait_unidi_progress(
    trevrpc_h3_conn* conn, size_t minimum_started_count, size_t minimum_retired_count, uint64_t timeout_nanos);

static struct timespec test_timeout_deadline(void);

typedef struct malformed_wt_peer_case {
    const uint8_t* control;
    size_t control_len;
    const uint8_t* headers;
    size_t headers_len;
    bool control_bidirectional;
} malformed_wt_peer_case;

typedef struct wt_setting_pair {
    uint64_t id;
    uint64_t value;
} wt_setting_pair;

static const trevrpc_msquic_config test_config = {
    .alpn = "trevrpc",
    .alpn_len = 7,
    .cert_file = TREVRPC_MSQUIC_TEST_CERT,
    .key_file = TREVRPC_MSQUIC_TEST_KEY,
    .skip_certificate_validation = 1,
    .peer_bidi_stream_count = 8,
};

static const trevrpc_msquic_config test_h3_config = {
    .alpn = "h3",
    .alpn_len = 2,
    .cert_file = TREVRPC_MSQUIC_TEST_CERT,
    .key_file = TREVRPC_MSQUIC_TEST_KEY,
    .skip_certificate_validation = 1,
    .peer_bidi_stream_count = 8,
    .peer_unidi_stream_count = 8,
};

static const trevrpc_msquic_config test_native_config = {
    .alpn = TREVRPC_ALPN,
    .alpn_len = sizeof(TREVRPC_ALPN) - 1,
    .cert_file = TREVRPC_MSQUIC_TEST_CERT,
    .key_file = TREVRPC_MSQUIC_TEST_KEY,
    .skip_certificate_validation = 1,
    .peer_bidi_stream_count = 8,
    .max_frame_size = 64,
};

struct trevrpc_stream {
    trevrpc_msquic_stream* msquic_stream;
    trevrpc_wt_stream* wt_stream;
    trevrpc_h3_stream* h3_stream;
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

#define TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL 0x08
#define TEST_WT_SETTINGS_GREASE 0x21
#define TEST_WT_SETTINGS_H3_DATAGRAM 0x33
#define TEST_WT_SETTINGS_H3_DRAFT04_DATAGRAM 0xffd277
#define TEST_WT_SETTINGS_WEBTRANSPORT_DRAFT02 0x2b603742
#define TEST_WT_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07 0xc671706a
#define TEST_WT_SETTINGS_WT_ENABLED_DRAFT15 0x2c7cf000
#define TEST_WT_SETTINGS_WT_MAX_SESSIONS 0x14e9cd29
#define TEST_WT_SETTINGS_WT_INITIAL_MAX_DATA 0x2b61
#define TEST_WT_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI 0x2b64
#define TEST_WT_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI 0x2b65
#define TEST_WT_CAPSULE_MAX_DATA 0x190b4d3d
#define TEST_WT_CAPSULE_MAX_STREAMS_BIDI 0x190b4d3f
#define TEST_WT_CAPSULE_MAX_STREAMS_UNI 0x190b4d40

static void* accept_conn_thread(void* arg) {
    accept_args* args = arg;
    args->result = trevrpc_msquic_listener_accept(args->listener, &args->conn);
    return NULL;
}

static void* listener_shutdown_thread(void* arg) {
    listener_shutdown_args* args = arg;
    pthread_mutex_lock(args->mutex);
    while (!*args->start) {
        pthread_cond_wait(args->cond, args->mutex);
    }
    pthread_mutex_unlock(args->mutex);
    trevrpc_msquic_listener_shutdown(args->listener);
    return NULL;
}

static void* accept_stream_thread(void* arg) {
    stream_args* args = arg;
    args->result = trevrpc_msquic_conn_accept_stream(args->conn, &args->stream);
    return NULL;
}

static void test_stream_hook(trevrpc_msquic_test_stream_event event, void* context) {
    stream_hook_state* state = context;
    pthread_mutex_lock(&state->mutex);
    state->seen[event] = true;
    state->calls[event]++;
    pthread_cond_broadcast(&state->cond);
    while (event == state->block_event && !state->release) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void test_stream_hook_wait(stream_hook_state* state, trevrpc_msquic_test_stream_event event) {
    pthread_mutex_lock(&state->mutex);
    while (!state->seen[event]) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void test_stream_hook_release(stream_hook_state* state) {
    pthread_mutex_lock(&state->mutex);
    state->release = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static size_t test_stream_hook_calls(stream_hook_state* state, trevrpc_msquic_test_stream_event event) {
    pthread_mutex_lock(&state->mutex);
    size_t calls = state->calls[event];
    pthread_mutex_unlock(&state->mutex);
    return calls;
}

static void test_stream_observer(void* context, uint32_t flags) {
    stream_observer_state* state = context;
    pthread_mutex_lock(&state->mutex);
    state->calls++;
    state->flags |= flags;
    bool self_clear = state->self_clear;
    bool self_close = state->self_close;
    pthread_cond_broadcast(&state->cond);
    while (state->block && !state->release) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
    if (self_clear) {
        trevrpc_msquic_stream_clear_observer(state->stream);
    }
    if (self_close) {
        trevrpc_msquic_stream_close(state->stream);
        pthread_mutex_lock(&state->mutex);
        state->close_returned = true;
        pthread_cond_broadcast(&state->cond);
        pthread_mutex_unlock(&state->mutex);
    }
}

static void test_raw_shutdown_close(void* context, int error_code) {
    (void)error_code;
    raw_shutdown_close_state* state = context;
    pthread_mutex_lock(&state->mutex);
    trevrpc_raw_client* client = state->client;
    state->client = NULL;
    state->calls++;
    pthread_mutex_unlock(&state->mutex);

    trevrpc_raw_client_close(client);

    pthread_mutex_lock(&state->mutex);
    state->close_returned = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static bool test_raw_shutdown_wait_close_returned(raw_shutdown_close_state* state) {
    struct timespec deadline = {0};
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return false;
    }
    deadline.tv_sec += 5;
    pthread_mutex_lock(&state->mutex);
    while (!state->close_returned) {
        if (pthread_cond_timedwait(&state->cond, &state->mutex, &deadline) == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static void test_conn_observer(void* context, const trevrpc_msquic_conn_event* event) {
    if (event == NULL || event->kind != TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE) {
        return;
    }
    conn_observer_state* state = context;
    pthread_mutex_lock(&state->mutex);
    state->terminal_calls++;
    bool self_close = state->self_close;
    pthread_cond_broadcast(&state->cond);
    while (state->block && !state->release) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    trevrpc_msquic_conn* conn = state->conn;
    pthread_mutex_unlock(&state->mutex);

    if (self_close) {
        trevrpc_msquic_conn_close(conn);
        pthread_mutex_lock(&state->mutex);
        state->close_returned = true;
        pthread_cond_broadcast(&state->cond);
        pthread_mutex_unlock(&state->mutex);
    }
}

static bool test_conn_observer_wait(conn_observer_state* state, bool wait_for_close) {
    struct timespec deadline = test_timeout_deadline();
    pthread_mutex_lock(&state->mutex);
    while (state->terminal_calls == 0 || (wait_for_close && !state->close_returned)) {
        if (pthread_cond_timedwait(&state->cond, &state->mutex, &deadline) == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static void test_conn_observer_release(conn_observer_state* state) {
    pthread_mutex_lock(&state->mutex);
    state->release = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static void* test_conn_close_thread(void* context) {
    conn_close_operation* operation = context;
    pthread_mutex_lock(&operation->mutex);
    operation->started = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);
    trevrpc_msquic_conn_close(operation->conn);
    pthread_mutex_lock(&operation->mutex);
    operation->completed = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);
    return NULL;
}

static bool test_conn_close_wait(conn_close_operation* operation, bool wait_for_completion) {
    struct timespec deadline = test_timeout_deadline();
    pthread_mutex_lock(&operation->mutex);
    while (!operation->started || (wait_for_completion && !operation->completed)) {
        if (pthread_cond_timedwait(&operation->cond, &operation->mutex, &deadline) == ETIMEDOUT) {
            pthread_mutex_unlock(&operation->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&operation->mutex);
    return true;
}

static void test_stream_observer_wait_calls(stream_observer_state* state, size_t calls) {
    pthread_mutex_lock(&state->mutex);
    while (state->calls < calls) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void test_stream_observer_release(stream_observer_state* state) {
    pthread_mutex_lock(&state->mutex);
    state->release = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static void test_stream_observer_snapshot(stream_observer_state* state, size_t* out_calls, uint32_t* out_flags) {
    pthread_mutex_lock(&state->mutex);
    *out_calls = state->calls;
    *out_flags = state->flags;
    pthread_mutex_unlock(&state->mutex);
}

static struct timespec test_timeout_deadline(void) {
    struct timespec deadline = {0};
    if (clock_gettime(CLOCK_REALTIME, &deadline) == 0) {
        deadline.tv_sec += 5;
    }
    return deadline;
}

#if defined(__linux__)
static bool test_process_thread_count(size_t* out_count) {
    FILE* status = fopen("/proc/self/status", "r");
    if (status == NULL) {
        return false;
    }

    char line[256];
    size_t count = 0;
    bool found = false;
    while (fgets(line, sizeof(line), status) != NULL) {
        if (sscanf(line, "Threads: %zu", &count) == 1) {
            found = true;
            break;
        }
    }
    (void)fclose(status);
    if (found) {
        *out_count = count;
    }
    return found;
}
#endif

static bool test_stream_observer_wait_close_returned(stream_observer_state* state) {
    struct timespec deadline = test_timeout_deadline();
    pthread_mutex_lock(&state->mutex);
    while (!state->close_returned) {
        if (pthread_cond_timedwait(&state->cond, &state->mutex, &deadline) == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static bool test_stream_hook_wait_timeout(stream_hook_state* state, trevrpc_msquic_test_stream_event event) {
    struct timespec deadline = test_timeout_deadline();
    pthread_mutex_lock(&state->mutex);
    while (!state->seen[event]) {
        if (pthread_cond_timedwait(&state->cond, &state->mutex, &deadline) == ETIMEDOUT) {
            pthread_mutex_unlock(&state->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return true;
}

static void* stream_observer_drain_thread(void* context) {
    stream_observer_operation* operation = context;
    pthread_mutex_lock(&operation->mutex);
    operation->started = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);

    trevrpc_msquic_stream_drain_observer(operation->stream);

    pthread_mutex_lock(&operation->mutex);
    operation->completed = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);
    return NULL;
}

static void* stream_pending_sends_wait_thread(void* context) {
    stream_observer_operation* operation = context;
    pthread_mutex_lock(&operation->mutex);
    operation->started = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);

    (void)trevrpc_msquic_stream_wait_pending_sends(operation->stream);

    pthread_mutex_lock(&operation->mutex);
    operation->completed = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);
    return NULL;
}

static void* stream_observer_close_thread(void* context) {
    stream_observer_operation* operation = context;
    pthread_mutex_lock(&operation->mutex);
    operation->started = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);

    trevrpc_msquic_stream_close(operation->stream);

    pthread_mutex_lock(&operation->mutex);
    operation->completed = true;
    pthread_cond_broadcast(&operation->cond);
    pthread_mutex_unlock(&operation->mutex);
    return NULL;
}

static void test_stream_observer_operation_wait_started(stream_observer_operation* operation) {
    pthread_mutex_lock(&operation->mutex);
    while (!operation->started) {
        pthread_cond_wait(&operation->cond, &operation->mutex);
    }
    pthread_mutex_unlock(&operation->mutex);
}

static bool test_stream_observer_operation_completed(stream_observer_operation* operation) {
    pthread_mutex_lock(&operation->mutex);
    bool completed = operation->completed;
    pthread_mutex_unlock(&operation->mutex);
    return completed;
}

static int test_stream_observer_state_init(stream_observer_state* state, trevrpc_msquic_stream* stream) {
    memset(state, 0, sizeof(*state));
    state->stream = stream;
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

static void test_stream_observer_state_destroy(stream_observer_state* state) {
    pthread_cond_destroy(&state->cond);
    pthread_mutex_destroy(&state->mutex);
}

static int test_stream_observer_operation_init(stream_observer_operation* operation, trevrpc_msquic_stream* stream) {
    memset(operation, 0, sizeof(*operation));
    operation->stream = stream;
    int err = pthread_mutex_init(&operation->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&operation->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&operation->mutex);
        return -err;
    }
    return 0;
}

static void test_stream_observer_operation_destroy(stream_observer_operation* operation) {
    pthread_cond_destroy(&operation->cond);
    pthread_mutex_destroy(&operation->mutex);
}

static void* stream_write_thread(void* arg) {
    stream_write_args* args = arg;
    static const uint8_t data[] = {1, 2, 3};
    static const size_t body_lens[] = {1, 1, 1};
    static const uint8_t* const bodies[] = {data, data + 1, data + 2};
    trevrpc_msquic_send_completion* completion = NULL;
    const trevrpc_msquic_frame_part parts[] = {{.data = data, .len = sizeof(data)}};

    switch (args->kind) {
    case TEST_STREAM_WRITE_BYTES:
        args->result = trevrpc_msquic_stream_write(args->stream, data, sizeof(data));
        break;
    case TEST_STREAM_WRITE_FRAME_PARTS:
        args->result = trevrpc_msquic_stream_write_frame_parts(args->stream, parts, 1, 64);
        break;
    case TEST_STREAM_WRITE_MESSAGE:
        args->result = trevrpc_msquic_stream_write_message_frame(args->stream, data, sizeof(data), 64);
        break;
    case TEST_STREAM_WRITE_MESSAGE_WAIT:
        args->result = trevrpc_msquic_stream_write_message_frame_wait_capacity(args->stream, data, sizeof(data), 64);
        break;
    case TEST_STREAM_WRITE_MESSAGES:
        args->result = trevrpc_msquic_stream_write_message_frames(args->stream, data, body_lens, 3, 64);
        break;
    case TEST_STREAM_WRITE_MESSAGES_BORROWED:
        args->result =
            trevrpc_msquic_stream_write_message_frames_borrowed(args->stream, bodies, body_lens, 3, 64, &completion);
        if (completion != NULL) {
            (void)trevrpc_msquic_send_completion_wait(completion);
            trevrpc_msquic_send_completion_free(completion);
        }
        break;
    case TEST_STREAM_WRITE_FIN_BYTES:
        args->result = trevrpc_msquic_stream_write_fin(args->stream, data, sizeof(data));
        break;
    case TEST_STREAM_WRITE_FIN_EMPTY:
        args->result = trevrpc_msquic_stream_write_fin(args->stream, NULL, 0);
        break;
    case TEST_STREAM_SHUTDOWN_SEND:
        args->result = trevrpc_msquic_stream_shutdown_send(args->stream);
        break;
    }
    return NULL;
}

static void* tracked_send_thread(void* arg) {
    tracked_send_args* args = arg;
    static const uint8_t data[] = {1, 2, 3};
    const trevrpc_msquic_frame_part parts[] = {{.data = data, .len = sizeof(data)}};
    args->result =
        trevrpc_msquic_stream_write_frame_parts_with_completion(args->stream, parts, 1, 64, &args->completion);
    return NULL;
}

static void* stream_close_thread(void* arg) {
    stream_close_args* args = arg;
    trevrpc_msquic_stream_close(args->stream);
    return NULL;
}

static void* stream_read_thread(void* arg) {
    stream_read_args* args = arg;
    args->result = trevrpc_msquic_stream_read_ready(args->stream, args->data, args->len);
    return NULL;
}

static void* accept_wt_session_thread(void* arg) {
    wt_accept_args* args = arg;
    args->result = trevrpc_wt_listener_accept_session(args->listener, &args->session);
    return NULL;
}

static void* dial_wt_session_thread(void* arg) {
    wt_dial_args* args = arg;
    args->result = trevrpc_wt_dial(&args->config, &args->session);
    return NULL;
}

static void* accept_wt_stream_thread(void* arg) {
    wt_stream_args* args = arg;
    args->result = trevrpc_wt_session_accept_stream(args->session, &args->stream);
    return NULL;
}

static void* accept_h3_conn_thread(void* arg) {
    h3_accept_args* args = arg;
    size_t max_h3_frame_payload = args->max_h3_frame_payload == 0 ? 1024 : args->max_h3_frame_payload;
    args->result = trevrpc_h3_accept_from_msquic(args->conn,
        &args->config,
        1,
        "/rpc",
        args->admission,
        args->admission_user_data,
        max_h3_frame_payload,
        &args->h3_conn);
    return NULL;
}

static int test_http3_admission(void* user_data, const trevrpc_http3_admission_request* request) {
    int* calls = user_data;
    (*calls)++;
    return request != NULL && request->secure && request->path_len == 4 && memcmp(request->path, "/rpc", 4) == 0 ? 0
                                                                                                                 : -1;
}

static void* accept_h3_stream_thread(void* arg) {
    h3_stream_args* args = arg;
    for (;;) {
        trevrpc_h3_stream* pending = NULL;
        args->result = trevrpc_h3_conn_accept_stream(args->conn, &pending);
        if (args->result != 0) {
            return NULL;
        }
        int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
        args->result = trevrpc_h3_stream_resolve(args->conn, pending, 5000000000ull, &args->wt_stream, &resolution);
        if (args->result == 0 && resolution == TREV_H3_STREAM_RESOLVED_HTTP3) {
            args->h3_stream = pending;
            return NULL;
        }
        trevrpc_h3_stream_close(pending);
        if (args->result != 0 || resolution == TREV_H3_STREAM_RESOLVED_WEBTRANSPORT) {
            return NULL;
        }
    }
}

static void* resolve_h3_stream_thread(void* arg) {
    h3_resolve_args* args = arg;
    args->resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    args->result =
        trevrpc_h3_stream_resolve(args->conn, args->stream, args->timeout_nanos, &args->wt_stream, &args->resolution);
    return NULL;
}

static int test_msquic_dial(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_conn** conn) {
    if (config != &test_h3_config) {
        return trevrpc_msquic_dial(host, port, config, conn);
    }
    trevrpc_msquic_feature_request features = trevrpc_msquic_default_h3_features();
    return trevrpc_msquic_dial_observed_features(host, port, config, &features, NULL, NULL, NULL, 0, NULL, NULL, conn);
}

static int connect_pair_with_config_and_features(const trevrpc_msquic_config* config,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_listener** out_listener,
    trevrpc_msquic_conn** out_client,
    trevrpc_msquic_conn** out_server) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    pthread_t thread = {0};
    bool thread_started = false;
    accept_args args = {0};
    uint16_t port = 0;

    CHECK_GOTO(trevrpc_msquic_listen_alpns_features("127.0.0.1", 0, config, NULL, 0, features, &listener) == 0);
    CHECK_GOTO(trevrpc_msquic_listener_port(listener, &port) == 0);
    CHECK_GOTO(port != 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_conn_thread, &args) == 0);
    thread_started = true;
    CHECK_GOTO(trevrpc_msquic_dial_observed_features(
                   "127.0.0.1", port, config, features, NULL, NULL, NULL, 0, NULL, NULL, &client) == 0);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);
    server = args.conn;

    *out_listener = listener;
    *out_client = client;
    *out_server = server;
    listener = NULL;
    client = NULL;
    server = NULL;
    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_msquic_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int connect_pair_with_config(const trevrpc_msquic_config* config,
    trevrpc_msquic_listener** out_listener,
    trevrpc_msquic_conn** out_client,
    trevrpc_msquic_conn** out_server) {
    trevrpc_msquic_feature_request features =
        config != NULL && config->alpn_len == 2 && config->alpn != NULL && memcmp(config->alpn, "h3", 2) == 0
            ? trevrpc_msquic_default_h3_features()
            : trevrpc_msquic_generic_feature_request();
    return connect_pair_with_config_and_features(config, &features, out_listener, out_client, out_server);
}

static int connect_pair(
    trevrpc_msquic_listener** out_listener, trevrpc_msquic_conn** out_client, trevrpc_msquic_conn** out_server) {
    return connect_pair_with_config(&test_config, out_listener, out_client, out_server);
}

static int connect_pair_with_client_observer(conn_observer_state* observer,
    trevrpc_msquic_listener** out_listener,
    trevrpc_msquic_conn** out_client,
    trevrpc_msquic_conn** out_server) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    pthread_t thread = {0};
    bool thread_started = false;
    accept_args args = {0};
    uint16_t port = 0;
    trevrpc_msquic_feature_request features = trevrpc_msquic_generic_feature_request();

    CHECK_GOTO(trevrpc_msquic_listen_alpns_features("127.0.0.1", 0, &test_config, NULL, 0, &features, &listener) == 0);
    CHECK_GOTO(trevrpc_msquic_listener_port(listener, &port) == 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_conn_thread, &args) == 0);
    thread_started = true;
    CHECK_GOTO(
        trevrpc_msquic_dial_observed_features(
            "127.0.0.1", port, &test_config, &features, NULL, NULL, NULL, 0, test_conn_observer, observer, &client) ==
        0);
    pthread_mutex_lock(&observer->mutex);
    observer->conn = client;
    pthread_mutex_unlock(&observer->mutex);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);
    server = args.conn;

    *out_listener = listener;
    *out_client = client;
    *out_server = server;
    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_msquic_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    if (result != 0) {
        trevrpc_msquic_conn_close(args.conn);
        trevrpc_msquic_conn_close(client);
        trevrpc_msquic_listener_close(listener);
    }
    return result;
}

static int test_webtransport_transport_requirements_are_negotiated(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client, &server) == 0);
    trevrpc_msquic_provider_features provider = trevrpc_msquic_provider_features_current();
    CHECK_GOTO(provider.datagrams);
    const uint32_t attested_reset_dialects =
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT | TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    CHECK_GOTO(provider.supported_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE ||
               provider.supported_reset_dialects == attested_reset_dialects);
    bool reliable_reset = provider.supported_reset_dialects == attested_reset_dialects;
    CHECK_GOTO(trevrpc_msquic_test_reliable_reset_negotiated(client) == reliable_reset);
    CHECK_GOTO(trevrpc_msquic_test_reliable_reset_negotiated(server) == reliable_reset);
    trevrpc_msquic_feature_snapshot client_snapshot = {0};
    trevrpc_msquic_feature_snapshot server_snapshot = {0};
    CHECK_GOTO(trevrpc_msquic_conn_feature_snapshot(client, &client_snapshot) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_feature_snapshot(server, &server_snapshot) == 0);
    CHECK_GOTO(trevrpc_msquic_feature_snapshot_usable_datagrams(&client_snapshot));
    CHECK_GOTO(trevrpc_msquic_feature_snapshot_usable_datagrams(&server_snapshot));
    uint32_t expected_reset_dialects =
        reliable_reset ? TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT : TREV_MSQUIC_RESET_DIALECTS_NONE;
    CHECK_GOTO(client_snapshot.configured_reset_dialects == expected_reset_dialects);
    CHECK_GOTO(client_snapshot.negotiated_reset_dialects == expected_reset_dialects);
    CHECK_GOTO(server_snapshot.configured_reset_dialects == expected_reset_dialects);
    CHECK_GOTO(server_snapshot.negotiated_reset_dialects == expected_reset_dialects);
    result = 0;

cleanup:
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_listener_shutdown_is_concurrent_and_idempotent(void) {
    enum { shutdown_thread_count = 16 };
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    pthread_t threads[shutdown_thread_count] = {0};
    size_t threads_started = 0;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    bool start = false;
    listener_shutdown_args args = {
        .mutex = &mutex,
        .cond = &cond,
        .start = &start,
    };

    CHECK_GOTO(trevrpc_msquic_listen("127.0.0.1", 0, &test_config, &listener) == 0);
    args.listener = listener;
    for (; threads_started < shutdown_thread_count; threads_started++) {
        CHECK_GOTO(pthread_create(&threads[threads_started], NULL, listener_shutdown_thread, &args) == 0);
    }

    pthread_mutex_lock(&mutex);
    start = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);
    for (size_t i = 0; i < threads_started; i++) {
        CHECK_GOTO(pthread_join(threads[i], NULL) == 0);
    }
    threads_started = 0;
    result = 0;

cleanup:
    pthread_mutex_lock(&mutex);
    start = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);
    for (size_t i = 0; i < threads_started; i++) {
        (void)pthread_join(threads[i], NULL);
    }
    trevrpc_msquic_listener_close(listener);
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&mutex);
    return result;
}

static int open_stream_pair(trevrpc_msquic_conn* client,
    trevrpc_msquic_conn* server,
    trevrpc_msquic_stream** out_client_stream,
    trevrpc_msquic_stream** out_server_stream) {
    int result = 1;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    pthread_t thread = {0};
    bool thread_started = false;
    stream_args args = {.conn = server};

    CHECK_GOTO(pthread_create(&thread, NULL, accept_stream_thread, &args) == 0);
    thread_started = true;
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client, &client_stream) == 0);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);
    server_stream = args.stream;

    *out_client_stream = client_stream;
    *out_server_stream = server_stream;
    client_stream = NULL;
    server_stream = NULL;
    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_msquic_conn_shutdown(server);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    return result;
}

static int test_stream_observer_readable_reinstall_self_clear_and_terminal(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    stream_observer_state readable = {0};
    stream_observer_state self_clearing = {0};
    stream_observer_state terminal = {0};
    bool readable_initialized = false;
    bool self_clearing_initialized = false;
    bool terminal_initialized = false;
    const uint8_t first = 0x11;
    const uint8_t second = 0x22;
    uint8_t received = 0;
    size_t calls = 0;
    uint32_t flags = 0;

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(test_stream_observer_state_init(&readable, server_stream) == 0);
    readable_initialized = true;
    CHECK_GOTO(trevrpc_msquic_stream_set_observer(server_stream, test_stream_observer, &readable) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, &first, sizeof(first)), (int)sizeof(first));
    test_stream_observer_wait_calls(&readable, 1);
    test_stream_observer_snapshot(&readable, &calls, &flags);
    CHECK_GOTO(calls == 1);
    CHECK_GOTO((flags & TREV_MSQUIC_STREAM_OBSERVER_READABLE) != 0);
    trevrpc_msquic_stream_clear_observer(server_stream);
    trevrpc_msquic_stream_drain_observer(server_stream);

    CHECK_GOTO(test_stream_observer_state_init(&self_clearing, server_stream) == 0);
    self_clearing_initialized = true;
    self_clearing.self_clear = true;
    CHECK_GOTO(trevrpc_msquic_stream_set_observer(server_stream, test_stream_observer, &self_clearing) == 0);
    test_stream_observer_snapshot(&self_clearing, &calls, &flags);
    CHECK_GOTO(calls == 1);
    CHECK_GOTO((flags & TREV_MSQUIC_STREAM_OBSERVER_READABLE) != 0);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, &second, sizeof(second)), (int)sizeof(second));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_protocol(server_stream, &received, sizeof(received)), 1);
    CHECK_GOTO(received == first);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_protocol(server_stream, &received, sizeof(received)), 1);
    CHECK_GOTO(received == second);
    test_stream_observer_snapshot(&self_clearing, &calls, &flags);
    CHECK_GOTO(calls == 1);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_shutdown_send(client_stream), 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_protocol(server_stream, &received, sizeof(received)), 0);
    CHECK_GOTO(test_stream_observer_state_init(&terminal, server_stream) == 0);
    terminal_initialized = true;
    CHECK_GOTO(trevrpc_msquic_stream_set_observer(server_stream, test_stream_observer, &terminal) == 0);
    test_stream_observer_snapshot(&terminal, &calls, &flags);
    CHECK_GOTO(calls == 1);
    CHECK_GOTO((flags & TREV_MSQUIC_STREAM_OBSERVER_TERMINAL) != 0);
    trevrpc_msquic_stream_clear_observer(server_stream);
    trevrpc_msquic_stream_drain_observer(server_stream);

    result = 0;

cleanup:
    if (server_stream != NULL) {
        trevrpc_msquic_stream_clear_observer(server_stream);
        trevrpc_msquic_stream_drain_observer(server_stream);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    if (terminal_initialized) {
        test_stream_observer_state_destroy(&terminal);
    }
    if (self_clearing_initialized) {
        test_stream_observer_state_destroy(&self_clearing);
    }
    if (readable_initialized) {
        test_stream_observer_state_destroy(&readable);
    }
    return result;
}

static int test_stream_observer_clear_and_drain_blocked_callback(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    stream_observer_state observer = {0};
    stream_observer_operation drain = {0};
    bool observer_initialized = false;
    bool drain_initialized = false;
    pthread_t drain_thread = {0};
    bool drain_thread_started = false;
    const uint8_t byte = 0x33;

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(test_stream_observer_state_init(&observer, server_stream) == 0);
    observer_initialized = true;
    observer.block = true;
    CHECK_GOTO(trevrpc_msquic_stream_set_observer(server_stream, test_stream_observer, &observer) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, &byte, sizeof(byte)), (int)sizeof(byte));
    test_stream_observer_wait_calls(&observer, 1);

    trevrpc_msquic_stream_clear_observer(server_stream);
    CHECK_GOTO(test_stream_observer_operation_init(&drain, server_stream) == 0);
    drain_initialized = true;
    CHECK_GOTO(pthread_create(&drain_thread, NULL, stream_observer_drain_thread, &drain) == 0);
    drain_thread_started = true;
    test_stream_observer_operation_wait_started(&drain);
    CHECK_GOTO(!test_stream_observer_operation_completed(&drain));

    test_stream_observer_release(&observer);
    CHECK_GOTO(pthread_join(drain_thread, NULL) == 0);
    drain_thread_started = false;
    CHECK_GOTO(test_stream_observer_operation_completed(&drain));
    result = 0;

cleanup:
    if (observer_initialized) {
        test_stream_observer_release(&observer);
    }
    if (drain_thread_started) {
        (void)pthread_join(drain_thread, NULL);
    }
    if (server_stream != NULL) {
        trevrpc_msquic_stream_clear_observer(server_stream);
        trevrpc_msquic_stream_drain_observer(server_stream);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    if (drain_initialized) {
        test_stream_observer_operation_destroy(&drain);
    }
    if (observer_initialized) {
        test_stream_observer_state_destroy(&observer);
    }
    return result;
}

static int run_stream_wait_close_lifetime_case(
    trevrpc_msquic_test_stream_event pinned_event, void* (*wait_thread_fn)(void*)) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* stream = NULL;
    stream_hook_state hook = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .block_event = pinned_event,
    };
    stream_observer_operation wait = {0};
    stream_observer_operation close = {0};
    bool wait_initialized = false;
    bool close_initialized = false;
    pthread_t wait_thread = {0};
    pthread_t close_thread = {0};
    bool wait_thread_started = false;
    bool close_thread_started = false;

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(NULL, 1, 1, &fixture) == 0);
    stream = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    CHECK_GOTO(stream != NULL);
    CHECK_GOTO(test_stream_observer_operation_init(&wait, stream) == 0);
    wait_initialized = true;
    CHECK_GOTO(test_stream_observer_operation_init(&close, stream) == 0);
    close_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);

    CHECK_GOTO(pthread_create(&wait_thread, NULL, wait_thread_fn, &wait) == 0);
    wait_thread_started = true;
    test_stream_hook_wait(&hook, pinned_event);
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_observer_close_thread, &close) == 0);
    close_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    CHECK_GOTO(!test_stream_observer_operation_completed(&close));

    test_stream_hook_release(&hook);
    CHECK_GOTO(pthread_join(wait_thread, NULL) == 0);
    wait_thread_started = false;
    CHECK_GOTO(test_stream_observer_operation_completed(&wait));
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    stream = NULL;
    CHECK_GOTO(test_stream_observer_operation_completed(&close));
    result = 0;

cleanup:
    test_stream_hook_release(&hook);
    if (wait_thread_started) {
        (void)pthread_join(wait_thread, NULL);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        stream = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_stream_close(stream);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    if (close_initialized) {
        test_stream_observer_operation_destroy(&close);
    }
    if (wait_initialized) {
        test_stream_observer_operation_destroy(&wait);
    }
    pthread_cond_destroy(&hook.cond);
    pthread_mutex_destroy(&hook.mutex);
    return result;
}

static int test_stream_wait_calls_pin_close_lifetime(void) {
    if (run_stream_wait_close_lifetime_case(
            TREV_MSQUIC_TEST_STREAM_DRAIN_OBSERVER_PINNED, stream_observer_drain_thread) != 0) {
        return 1;
    }
    return run_stream_wait_close_lifetime_case(
        TREV_MSQUIC_TEST_STREAM_WAIT_SENDS_PINNED, stream_pending_sends_wait_thread);
}

static int test_stream_observer_close_waits_for_blocked_callback(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    trevrpc_msquic_stream* probe_client_stream = NULL;
    trevrpc_msquic_stream* probe_server_stream = NULL;
    stream_observer_state observer = {0};
    stream_observer_operation close = {0};
    stream_hook_state close_hook = {.block_event = TREV_MSQUIC_TEST_STREAM_EVENT_COUNT};
    bool observer_initialized = false;
    bool close_initialized = false;
    bool close_hook_initialized = false;
    pthread_t close_thread = {0};
    bool close_thread_started = false;
    const uint8_t byte = 0x44;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &probe_client_stream, &probe_server_stream) == 0);
    CHECK_GOTO(test_stream_observer_state_init(&observer, server_stream) == 0);
    observer_initialized = true;
    observer.block = true;
    observer.self_close = true;
    CHECK_GOTO(trevrpc_msquic_stream_set_observer(server_stream, test_stream_observer, &observer) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, &byte, sizeof(byte)), (int)sizeof(byte));
    test_stream_observer_wait_calls(&observer, 1);

    CHECK_GOTO(test_stream_observer_operation_init(&close, server_stream) == 0);
    close_initialized = true;
    CHECK_GOTO(pthread_mutex_init(&close_hook.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&close_hook.cond, NULL) == 0);
    close_hook_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &close_hook);
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_observer_close_thread, &close) == 0);
    close_thread_started = true;
    test_stream_observer_operation_wait_started(&close);
    test_stream_hook_wait(&close_hook, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    CHECK_GOTO(!test_stream_observer_operation_completed(&close));

    test_stream_observer_release(&observer);
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    server_stream = NULL;
    CHECK_GOTO(test_stream_observer_operation_completed(&close));
    CHECK_GOTO(test_stream_observer_wait_close_returned(&observer));
    CHECK_GOTO(test_stream_hook_calls(&close_hook, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED) == 1);
    CHECK_GOTO(test_stream_hook_calls(&close_hook, TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED) == 1);
    trevrpc_msquic_test_receive_snapshot_get(probe_server_stream, &snapshot);
    CHECK_GOTO(snapshot.stream_owned_bytes == 0 && snapshot.stream_owned_count == 0);
    CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);
    CHECK_GOTO(snapshot.buffered_bytes == 0 && snapshot.queued_frames == 0);
    CHECK_GOTO(snapshot.active_resume_pins == 0 && !snapshot.paused && !snapshot.pending_frame);
    result = 0;

cleanup:
    if (observer_initialized) {
        test_stream_observer_release(&observer);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        server_stream = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_stream_close(probe_server_stream);
    trevrpc_msquic_stream_close(probe_client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    if (close_hook_initialized) {
        pthread_cond_destroy(&close_hook.cond);
        pthread_mutex_destroy(&close_hook.mutex);
    }
    if (close_initialized) {
        test_stream_observer_operation_destroy(&close);
    }
    if (observer_initialized) {
        test_stream_observer_state_destroy(&observer);
    }
    return result;
}

static int test_stream_observer_self_close(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    stream_observer_state observer = {0};
    stream_hook_state close_hook = {.block_event = TREV_MSQUIC_TEST_STREAM_EVENT_COUNT};
    bool observer_initialized = false;
    bool close_hook_initialized = false;
    bool self_close_returned = false;

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(test_stream_observer_state_init(&observer, server_stream) == 0);
    observer_initialized = true;
    observer.self_close = true;
    CHECK_GOTO(pthread_mutex_init(&close_hook.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&close_hook.cond, NULL) == 0);
    close_hook_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &close_hook);
    CHECK_GOTO(trevrpc_msquic_stream_set_observer(server_stream, test_stream_observer, &observer) == 0);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_shutdown_send(client_stream), 0);
    CHECK_GOTO(test_stream_observer_wait_close_returned(&observer));
    self_close_returned = true;
    CHECK_GOTO(test_stream_hook_wait_timeout(&close_hook, TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED));
    server_stream = NULL;
    result = 0;

cleanup:
    if (self_close_returned) {
        server_stream = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    if (close_hook_initialized) {
        pthread_cond_destroy(&close_hook.cond);
        pthread_mutex_destroy(&close_hook.mutex);
    }
    if (observer_initialized) {
        test_stream_observer_state_destroy(&observer);
    }
    return result;
}

static bool test_wait_api_leases(size_t expected) {
    struct timespec pause = {.tv_nsec = 1000000};
    for (size_t attempt = 0; attempt < 5000; attempt++) {
        if (trevrpc_msquic_test_api_leases() == expected) {
            return true;
        }
        (void)nanosleep(&pause, NULL);
    }
    return false;
}

static int test_raw_client_terminal_observer_self_close(void) {
    int result = 1;
    size_t baseline_leases = trevrpc_msquic_test_api_leases();
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_raw_client* client = NULL;
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    accept_args accept = {0};
    raw_shutdown_close_state observer = {0};
    bool observer_initialized = false;
    uint16_t port = 0;

    CHECK_GOTO(pthread_mutex_init(&observer.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&observer.cond, NULL) == 0);
    observer_initialized = true;
    CHECK_GOTO(trevrpc_msquic_listen("127.0.0.1", 0, &test_native_config, &listener) == 0);
    CHECK_GOTO(trevrpc_msquic_listener_port(listener, &port) == 0);
    accept.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_conn_thread, &accept) == 0);
    accept_thread_started = true;

    trevrpc_client_config_internal client_config = trevrpc_internal_default_config();
    client_config.skip_certificate_validation = 1;
    CHECK_GOTO(trevrpc_internal_raw_client_connect_cancellable_with_shutdown_callback(
                   "127.0.0.1", port, &client_config, NULL, test_raw_shutdown_close, &observer, &client) == 0);
    pthread_mutex_lock(&observer.mutex);
    observer.client = client;
    pthread_mutex_unlock(&observer.mutex);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_GOTO(accept.result == 0);
    server = accept.conn;

    trevrpc_msquic_conn_shutdown(server);
    CHECK_GOTO(test_raw_shutdown_wait_close_returned(&observer));
    client = NULL;
    pthread_mutex_lock(&observer.mutex);
    size_t observer_calls = observer.calls;
    pthread_mutex_unlock(&observer.mutex);
    CHECK_GOTO(observer_calls == 1);

    trevrpc_msquic_conn_close(server);
    server = NULL;
    trevrpc_msquic_listener_close(listener);
    listener = NULL;
    CHECK_GOTO(test_wait_api_leases(baseline_leases));
    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
        server = accept.conn;
    }
    if (client != NULL) {
        trevrpc_raw_client_close(client);
    }
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_listener_close(listener);
    if (observer_initialized) {
        pthread_cond_destroy(&observer.cond);
        pthread_mutex_destroy(&observer.mutex);
    }
    return result;
}

static int test_conn_observer_self_close_with_external_close(void) {
    int result = 1;
    size_t baseline_leases = trevrpc_msquic_test_api_leases();
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    conn_observer_state observer = {0};
    conn_close_operation close = {0};
    pthread_t close_thread = {0};
    bool observer_initialized = false;
    bool close_initialized = false;
    bool close_thread_started = false;

    CHECK_GOTO(pthread_mutex_init(&observer.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&observer.cond, NULL) == 0);
    observer_initialized = true;
    observer.block = true;
    observer.self_close = true;
    CHECK_GOTO(connect_pair_with_client_observer(&observer, &listener, &client, &server) == 0);

    trevrpc_msquic_conn_shutdown(server);
    CHECK_GOTO(test_conn_observer_wait(&observer, false));

    CHECK_GOTO(pthread_mutex_init(&close.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&close.cond, NULL) == 0);
    close_initialized = true;
    close.conn = client;
    CHECK_GOTO(pthread_create(&close_thread, NULL, test_conn_close_thread, &close) == 0);
    close_thread_started = true;
    CHECK_GOTO(test_conn_close_wait(&close, false));
    pthread_mutex_lock(&close.mutex);
    bool completed_while_observer_blocked = close.completed;
    pthread_mutex_unlock(&close.mutex);
    CHECK_GOTO(!completed_while_observer_blocked);

    test_conn_observer_release(&observer);
    CHECK_GOTO(test_conn_observer_wait(&observer, true));
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    client = NULL;
    CHECK_GOTO(test_conn_close_wait(&close, true));
    pthread_mutex_lock(&observer.mutex);
    size_t terminal_calls = observer.terminal_calls;
    pthread_mutex_unlock(&observer.mutex);
    CHECK_GOTO(terminal_calls == 1);

    trevrpc_msquic_conn_close(server);
    server = NULL;
    trevrpc_msquic_listener_close(listener);
    listener = NULL;
    CHECK_GOTO(test_wait_api_leases(baseline_leases));
    result = 0;

cleanup:
    if (observer_initialized) {
        test_conn_observer_release(&observer);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        client = NULL;
    }
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_listener_close(listener);
    if (close_initialized) {
        pthread_cond_destroy(&close.cond);
        pthread_mutex_destroy(&close.mutex);
    }
    if (observer_initialized) {
        pthread_cond_destroy(&observer.cond);
        pthread_mutex_destroy(&observer.mutex);
    }
    return result;
}

static int test_varint_write(uint8_t* out, size_t out_len, size_t* offset, uint64_t value) {
    size_t len = 0;
    if (value <= 0x3f) {
        len = 1;
    } else if (value <= 0x3fff) {
        len = 2;
    } else if (value <= 0x3fffffff) {
        len = 4;
    } else {
        len = 8;
    }
    if (out_len - *offset < len) {
        return -1;
    }

    switch (len) {
    case 1:
        out[(*offset)++] = (uint8_t)value;
        break;
    case 2:
        out[(*offset)++] = (uint8_t)(0x40 | (value >> 8));
        out[(*offset)++] = (uint8_t)value;
        break;
    case 4:
        out[(*offset)++] = (uint8_t)(0x80 | (value >> 24));
        out[(*offset)++] = (uint8_t)(value >> 16);
        out[(*offset)++] = (uint8_t)(value >> 8);
        out[(*offset)++] = (uint8_t)value;
        break;
    case 8:
        out[(*offset)++] = (uint8_t)(0xc0 | (value >> 56));
        out[(*offset)++] = (uint8_t)(value >> 48);
        out[(*offset)++] = (uint8_t)(value >> 40);
        out[(*offset)++] = (uint8_t)(value >> 32);
        out[(*offset)++] = (uint8_t)(value >> 24);
        out[(*offset)++] = (uint8_t)(value >> 16);
        out[(*offset)++] = (uint8_t)(value >> 8);
        out[(*offset)++] = (uint8_t)value;
        break;
    default:
        return -1;
    }
    return 0;
}

static int test_varint_write_width(uint8_t* out, size_t out_len, size_t* offset, uint64_t value, size_t width) {
    if ((width != 1 && width != 2 && width != 4 && width != 8) || value > ((UINT64_C(1) << (width * 8 - 2)) - 1) ||
        out_len - *offset < width) {
        return -1;
    }
    size_t start = *offset;
    for (size_t i = 0; i < width; i++) {
        size_t shift = (width - i - 1) * 8;
        out[start + i] = (uint8_t)(value >> shift);
    }
    out[start] |= width == 1 ? 0x00 : width == 2 ? 0x40 : width == 4 ? 0x80 : 0xc0;
    *offset += width;
    return 0;
}

static int test_varint_read(const uint8_t* data, size_t len, size_t* offset, uint64_t* value) {
    if (*offset >= len) {
        return -1;
    }
    size_t varint_len = (size_t)1 << (data[*offset] >> 6);
    if (len - *offset < varint_len) {
        return -1;
    }
    uint64_t result = data[*offset] & 0x3f;
    for (size_t i = 1; i < varint_len; i++) {
        result = (result << 8) | data[*offset + i];
    }
    *offset += varint_len;
    *value = result;
    return 0;
}

static int test_qpack_varint_write(
    uint8_t* out, size_t out_len, size_t* offset, uint8_t prefix_bits, uint8_t flags, uint64_t value) {
    if (prefix_bits == 0 || prefix_bits > 8 || *offset >= out_len) {
        return -1;
    }

    uint64_t prefix_max = ((uint64_t)1 << prefix_bits) - 1;
    if (value < prefix_max) {
        out[(*offset)++] = (uint8_t)(flags | value);
        return 0;
    }

    out[(*offset)++] = (uint8_t)(flags | prefix_max);
    value -= prefix_max;
    while (value >= 128) {
        if (*offset >= out_len) {
            return -1;
        }
        out[(*offset)++] = (uint8_t)(0x80 | (value & 0x7f));
        value >>= 7;
    }
    if (*offset >= out_len) {
        return -1;
    }
    out[(*offset)++] = (uint8_t)value;
    return 0;
}

static int test_build_control_settings(
    uint8_t* out, size_t out_len, size_t* out_written, const wt_setting_pair* settings, size_t settings_len) {
    uint8_t payload[256];
    size_t payload_offset = 0;
    size_t offset = 0;

    for (size_t i = 0; i < settings_len; i++) {
        if (test_varint_write(payload, sizeof(payload), &payload_offset, settings[i].id) != 0 ||
            test_varint_write(payload, sizeof(payload), &payload_offset, settings[i].value) != 0) {
            return -1;
        }
    }

    if (test_varint_write(out, out_len, &offset, 0x00) != 0 || test_varint_write(out, out_len, &offset, 0x04) != 0 ||
        test_varint_write(out, out_len, &offset, payload_offset) != 0 || out_len - offset < payload_offset) {
        return -1;
    }
    memcpy(out + offset, payload, payload_offset);
    offset += payload_offset;
    *out_written = offset;
    return 0;
}

static int test_build_exact_control_settings(uint8_t* out, size_t out_len, size_t* out_written, size_t payload_len) {
    if (payload_len == 0 || payload_len % 16 != 0) {
        return -1;
    }
    size_t offset = 0;
    if (test_varint_write(out, out_len, &offset, 0x00) != 0 ||
        test_varint_write(out, out_len, &offset, TREV_H3_FRAME_SETTINGS) != 0 ||
        test_varint_write(out, out_len, &offset, payload_len) != 0 || out_len - offset < payload_len) {
        return -1;
    }
    size_t payload_start = offset;
    for (size_t i = 0; i < payload_len / 16; i++) {
        if (test_varint_write_width(out, out_len, &offset, UINT64_C(0x100) + i, 8) != 0 ||
            test_varint_write_width(out, out_len, &offset, 0, 8) != 0) {
            return -1;
        }
    }
    if (offset - payload_start != payload_len) {
        return -1;
    }
    *out_written = offset;
    return 0;
}

static int test_build_draft02_control_settings(uint8_t* out, size_t out_len, size_t* out_written) {
    const wt_setting_pair settings[] = {
        {TEST_WT_SETTINGS_WEBTRANSPORT_DRAFT02, 1},
        {TEST_WT_SETTINGS_H3_DRAFT04_DATAGRAM, 1},
    };
    return test_build_control_settings(out, out_len, out_written, settings, sizeof(settings) / sizeof(settings[0]));
}

static int test_build_draft07_control_settings(uint8_t* out, size_t out_len, size_t* out_written) {
    const wt_setting_pair settings[] = {
        {TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1},
        {TEST_WT_SETTINGS_H3_DATAGRAM, 1},
        {TEST_WT_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07, 1},
    };
    return test_build_control_settings(out, out_len, out_written, settings, sizeof(settings) / sizeof(settings[0]));
}

static int test_build_draft15_control_settings(uint8_t* out, size_t out_len, size_t* out_written) {
    const wt_setting_pair settings[] = {
        {TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1},
        {TEST_WT_SETTINGS_H3_DATAGRAM, 1},
        {TEST_WT_SETTINGS_WT_ENABLED_DRAFT15, 1},
        {TEST_WT_SETTINGS_WT_MAX_SESSIONS, 1},
    };
    return test_build_control_settings(out, out_len, out_written, settings, sizeof(settings) / sizeof(settings[0]));
}

static int test_build_draft14_control_settings(uint8_t* out, size_t out_len, size_t* out_written) {
    const wt_setting_pair settings[] = {
        {TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1},
        {TEST_WT_SETTINGS_H3_DATAGRAM, 1},
        {TEST_WT_SETTINGS_WT_MAX_SESSIONS, 1},
    };
    return test_build_control_settings(out, out_len, out_written, settings, sizeof(settings) / sizeof(settings[0]));
}

static int test_build_webkit_control_settings(
    uint8_t* out, size_t out_len, size_t* out_written, uint64_t initial_max_streams) {
    const wt_setting_pair settings[] = {
        {TEST_WT_SETTINGS_GREASE, 42},
        {TEST_WT_SETTINGS_H3_DATAGRAM, 1},
        {TEST_WT_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07, 1},
        {TEST_WT_SETTINGS_WT_MAX_SESSIONS, 1},
        {TEST_WT_SETTINGS_WT_INITIAL_MAX_DATA, 8 * 1024 * 1024},
        {TEST_WT_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, initial_max_streams},
        {TEST_WT_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, initial_max_streams},
    };
    return test_build_control_settings(out, out_len, out_written, settings, sizeof(settings) / sizeof(settings[0]));
}

static int test_header_block_put_literal(
    uint8_t* out, size_t out_len, size_t* offset, const char* name, const char* value) {
    size_t name_len = strlen(name);
    size_t value_len = strlen(value);
    if (test_qpack_varint_write(out, out_len, offset, 3, 0x20, name_len) != 0 || out_len - *offset < name_len) {
        return -1;
    }
    memcpy(out + *offset, name, name_len);
    *offset += name_len;
    if (test_qpack_varint_write(out, out_len, offset, 7, 0, value_len) != 0 || out_len - *offset < value_len) {
        return -1;
    }
    memcpy(out + *offset, value, value_len);
    *offset += value_len;
    return 0;
}

static int test_build_connect_headers(uint8_t* out,
    size_t out_len,
    size_t* out_written,
    const char* method,
    const char* protocol,
    const char* scheme,
    const char* path,
    const char* authority,
    bool draft02_request) {
    size_t block_offset = 0;
    size_t offset = 0;
    uint8_t block[512];
    block[block_offset++] = 0;
    block[block_offset++] = 0;
    if (method != NULL && test_header_block_put_literal(block, sizeof(block), &block_offset, ":method", method) != 0) {
        return -1;
    }
    if (protocol != NULL &&
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":protocol", protocol) != 0) {
        return -1;
    }
    if (scheme != NULL && test_header_block_put_literal(block, sizeof(block), &block_offset, ":scheme", scheme) != 0) {
        return -1;
    }
    if (authority != NULL &&
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":authority", authority) != 0) {
        return -1;
    }
    if (path != NULL && test_header_block_put_literal(block, sizeof(block), &block_offset, ":path", path) != 0) {
        return -1;
    }
    if (draft02_request && test_header_block_put_literal(
                               block, sizeof(block), &block_offset, "sec-webtransport-http3-draft02", "1") != 0) {
        return -1;
    }

    if (test_varint_write(out, out_len, &offset, 0x01) != 0 ||
        test_varint_write(out, out_len, &offset, block_offset) != 0 || out_len - offset < block_offset) {
        return -1;
    }
    memcpy(out + offset, block, block_offset);
    offset += block_offset;
    *out_written = offset;
    return 0;
}

typedef enum test_invalid_connect_response_kind {
    TEST_CONNECT_RESPONSE_REQUEST_PSEUDO = 0,
    TEST_CONNECT_RESPONSE_INVALID_CONTENT_LENGTH,
} test_invalid_connect_response_kind;

static int test_build_invalid_connect_response(
    uint8_t* out, size_t out_len, size_t* out_written, test_invalid_connect_response_kind kind) {
    size_t block_offset = 0;
    size_t offset = 0;
    uint8_t block[128];
    block[block_offset++] = 0;
    block[block_offset++] = 0;
    int err = test_header_block_put_literal(block, sizeof(block), &block_offset, ":status", "200");
    if (err == 0 && kind == TEST_CONNECT_RESPONSE_REQUEST_PSEUDO) {
        err = test_header_block_put_literal(block, sizeof(block), &block_offset, ":method", "GET");
    }
    if (err == 0) {
        err = test_header_block_put_literal(
            block, sizeof(block), &block_offset, "sec-webtransport-http3-draft", "draft02");
    }
    if (err == 0 && kind == TEST_CONNECT_RESPONSE_INVALID_CONTENT_LENGTH) {
        err = test_header_block_put_literal(block, sizeof(block), &block_offset, "content-length", "invalid");
    }
    if (err != 0 || test_varint_write(out, out_len, &offset, TREV_H3_FRAME_HEADERS) != 0 ||
        test_varint_write(out, out_len, &offset, block_offset) != 0 || out_len - offset < block_offset) {
        return -1;
    }
    memcpy(out + offset, block, block_offset);
    offset += block_offset;
    *out_written = offset;
    return 0;
}

static int test_build_post_headers(
    uint8_t* out, size_t out_len, size_t* out_written, const char* method, const char* path, const char* content_type) {
    size_t block_offset = 0;
    size_t offset = 0;
    uint8_t block[512];
    block[block_offset++] = 0;
    block[block_offset++] = 0;
    if (test_header_block_put_literal(block, sizeof(block), &block_offset, ":method", method) != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":scheme", "https") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":authority", "localhost") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":path", path) != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, "content-type", content_type) != 0 ||
        test_varint_write(out, out_len, &offset, 0x01) != 0 ||
        test_varint_write(out, out_len, &offset, block_offset) != 0 || out_len - offset < block_offset) {
        return -1;
    }
    memcpy(out + offset, block, block_offset);
    offset += block_offset;
    *out_written = offset;
    return 0;
}

static int test_build_post_headers_with_extra(
    uint8_t* out, size_t out_len, size_t* out_written, size_t extra_value_len) {
    uint8_t block[8192];
    char extra_value[4096];
    if (extra_value_len >= sizeof(extra_value)) {
        return -1;
    }
    memset(extra_value, 'a', extra_value_len);
    extra_value[extra_value_len] = 0;
    size_t block_offset = 0;
    size_t offset = 0;
    block[block_offset++] = 0;
    block[block_offset++] = 0;
    if (test_header_block_put_literal(block, sizeof(block), &block_offset, ":method", "POST") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":scheme", "https") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":authority", "localhost") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":path", "/rpc") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, "content-type", "application/trevrpc") !=
            0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, "x", extra_value) != 0 ||
        test_varint_write(out, out_len, &offset, 0x01) != 0 ||
        test_varint_write(out, out_len, &offset, block_offset) != 0 || out_len - offset < block_offset) {
        return -1;
    }
    memcpy(out + offset, block, block_offset);
    offset += block_offset;
    *out_written = offset;
    return 0;
}

static int test_build_post_headers_duplicate_content(uint8_t* out, size_t out_len, size_t* out_written) {
    uint8_t block[512];
    size_t block_offset = 0;
    size_t offset = 0;
    block[block_offset++] = 0;
    block[block_offset++] = 0;
    if (test_header_block_put_literal(block, sizeof(block), &block_offset, ":method", "POST") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":scheme", "https") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":authority", "localhost") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, ":path", "/rpc") != 0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, "content-type", "application/trevrpc") !=
            0 ||
        test_header_block_put_literal(block, sizeof(block), &block_offset, "content-type", "application/trevrpc") !=
            0 ||
        test_varint_write(out, out_len, &offset, 0x01) != 0 ||
        test_varint_write(out, out_len, &offset, block_offset) != 0 || out_len - offset < block_offset) {
        return -1;
    }
    memcpy(out + offset, block, block_offset);
    *out_written = offset + block_offset;
    return 0;
}

static int test_read_exact(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        intptr_t n = trevrpc_msquic_stream_read(stream, data + offset, len - offset);
        if (n <= 0) {
            return -1;
        }
        offset += (size_t)n;
    }
    return 0;
}

static int test_read_varint(trevrpc_msquic_stream* stream, uint64_t* value) {
    uint8_t bytes[8];
    if (test_read_exact(stream, bytes, 1) != 0) {
        return -1;
    }
    size_t len = (size_t)1 << (bytes[0] >> 6);
    if (test_read_exact(stream, bytes + 1, len - 1) != 0) {
        return -1;
    }
    uint64_t decoded = bytes[0] & 0x3f;
    for (size_t i = 1; i < len; i++) {
        decoded = (decoded << 8) | bytes[i];
    }
    *value = decoded;
    return 0;
}

static int test_read_exact_timeout(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        intptr_t n = trevrpc_msquic_stream_read_timeout(stream, data + offset, len - offset, 5000000000ull);
        if (n <= 0) {
            return -1;
        }
        offset += (size_t)n;
    }
    return 0;
}

static int test_read_varint_timeout(trevrpc_msquic_stream* stream, uint64_t* value) {
    uint8_t bytes[8];
    if (test_read_exact_timeout(stream, bytes, 1) != 0) {
        return -1;
    }
    size_t len = (size_t)1 << (bytes[0] >> 6);
    if (test_read_exact_timeout(stream, bytes + 1, len - 1) != 0) {
        return -1;
    }
    uint64_t decoded = bytes[0] & 0x3f;
    for (size_t i = 1; i < len; i++) {
        decoded = (decoded << 8) | bytes[i];
    }
    *value = decoded;
    return 0;
}

static int test_wait_peer_close_error_millis(trevrpc_msquic_conn* conn, uint64_t expected, size_t timeout_millis) {
    const struct timespec pause = {.tv_nsec = 1000000};
    for (size_t i = 0; i < timeout_millis; i++) {
        uint64_t error_code = 0;
        if (trevrpc_msquic_conn_peer_close_error(conn, &error_code) == 0) {
            if (error_code != expected) {
                fprintf(stderr,
                    "%s:%d: peer close error mismatch (actual=0x%llx expected=0x%llx)\n",
                    __FILE__,
                    __LINE__,
                    (unsigned long long)error_code,
                    (unsigned long long)expected);
                return -1;
            }
            return 0;
        }
        nanosleep(&pause, NULL);
    }
    return -1;
}

static int test_wait_peer_close_error(trevrpc_msquic_conn* conn, uint64_t expected) {
    return test_wait_peer_close_error_millis(conn, expected, 1000);
}

static int test_stream_receive_codec_selection_is_immutable(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;
    uint8_t byte = 0;
    const uint8_t frame[] = {0, 0, 0, 1, 0x5a};

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, frame, sizeof(frame)), (int)sizeof(frame));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_timeout(server_stream, &byte, 1, 1000000000ull), 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_ready(server_stream, &body, &body_len, 64), TREV_MSQUIC_ERR_CLOSED);

    trevrpc_msquic_stream_close(server_stream);
    server_stream = NULL;
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, frame, sizeof(frame)), (int)sizeof(frame));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read(server_stream, NULL, 0), 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 1);
    CHECK_GOTO(body_len == 1 && body[0] == 0x5a);
    trevrpc_msquic_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(server_stream, &byte, 1), TREV_MSQUIC_ERR_CLOSED);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_peer_send_abort_preserves_local_send_direction(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t byte = 0;
    uint8_t received[3] = {0};
    const uint8_t reply[] = {1, 2, 3};

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_test_inject_peer_send_aborted(server_stream, 17), 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(server_stream, &byte, sizeof(byte)), TREV_MSQUIC_ERR_CLOSED);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(server_stream, reply, sizeof(reply)), (int)sizeof(reply));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_timeout(client_stream, received, sizeof(received), 1000000000ull),
        (int)sizeof(received));
    CHECK_GOTO(memcmp(received, reply, sizeof(reply)) == 0);

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_reset_unblocks_peer_read(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;
    CHECK_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4096, 1000000000ull) ==
               TREV_MSQUIC_ERR_CLOSED);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_write_fin_close_preserves_peer_eof(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t frame[] = {0, 0, 0, 7, 0x22, 5, 'h', 'e', 'l', 'l', 'o'};

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, frame, sizeof(frame)), (int)sizeof(frame));
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4096, 1000000000ull), 1);
    CHECK_GOTO(body_len == 7);
    CHECK_GOTO(body[0] == 0x22);
    CHECK_GOTO(body[1] == 5);
    CHECK_GOTO(memcmp(body + 2, "hello", 5) == 0);
    trevrpc_msquic_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4096, 1000000000ull), 0);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int run_empty_stream_finalization_case(test_stream_write_kind kind) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    stream_write_args args = {.kind = kind};
    uint8_t byte = 0;

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    args.stream = client_stream;
    (void)stream_write_thread(&args);
    CHECK_EQ_GOTO(args.result, 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_shutdown_send(server_stream), 0);
    trevrpc_msquic_test_wait_stream_shutdown_complete(client_stream);
    (void)stream_write_thread(&args);
    CHECK_EQ_GOTO(args.result, 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read(server_stream, &byte, sizeof(byte)), 0);

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_empty_stream_finalization_is_graceful_and_idempotent(void) {
    return run_empty_stream_finalization_case(TEST_STREAM_WRITE_FIN_EMPTY) != 0 ||
           run_empty_stream_finalization_case(TEST_STREAM_SHUTDOWN_SEND) != 0;
}

static int open_native_stream_pair(trevrpc_msquic_listener** out_listener,
    trevrpc_msquic_conn** out_client,
    trevrpc_msquic_conn** out_server,
    trevrpc_msquic_stream** out_client_stream,
    trevrpc_msquic_stream** out_server_stream) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;

    CHECK_GOTO(connect_pair_with_config(&test_native_config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);

    *out_listener = listener;
    *out_client = client;
    *out_server = server;
    *out_client_stream = client_stream;
    *out_server_stream = server_stream;
    listener = NULL;
    client = NULL;
    server = NULL;
    client_stream = NULL;
    server_stream = NULL;
    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_native_accepted_stream_starts_in_frame_mode(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    pthread_t thread = {0};
    bool thread_started = false;
    stream_args args = {0};
    const uint8_t empty_frame[] = {0, 0, 0, 0};
    uint8_t byte = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;

    CHECK_GOTO(connect_pair_with_config(&test_native_config, &listener, &client, &server) == 0);
    args.conn = server;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_stream_thread, &args) == 0);
    thread_started = true;
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client, &client_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write_fin(client_stream, empty_frame, sizeof(empty_frame)), (int)sizeof(empty_frame));
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_EQ_GOTO(args.result, 0);
    server_stream = args.stream;

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read(server_stream, &byte, sizeof(byte)), TREV_MSQUIC_ERR_CLOSED);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 1);
    CHECK_GOTO(body_len == 0);
    trevrpc_msquic_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 0);

    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_msquic_conn_shutdown(server);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int run_native_partial_frame_case(const uint8_t* frame, size_t frame_len) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, frame, frame_len), (int)frame_len);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull),
        TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(body == NULL);
    CHECK_GOTO(body_len == 0);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_native_partial_header_remains_closed_error(void) {
    const uint8_t partial_header[] = {0, 0};
    return run_native_partial_frame_case(partial_header, sizeof(partial_header));
}

static int test_native_partial_body_remains_closed_error(void) {
    const uint8_t partial_body[] = {0, 0, 0, 3, 0x22};
    return run_native_partial_frame_case(partial_body, sizeof(partial_body));
}

static int test_native_oversized_frame_remains_frame_too_large(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t oversized_frame[69];
    const uint8_t empty_frame[] = {0, 0, 0, 0};
    uint8_t* body = NULL;
    size_t body_len = 0;

    memset(oversized_frame, 0xa5, sizeof(oversized_frame));
    oversized_frame[0] = 0;
    oversized_frame[1] = 0;
    oversized_frame[2] = 0;
    oversized_frame[3] = 65;

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, oversized_frame, sizeof(oversized_frame)),
        (int)sizeof(oversized_frame));
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write_fin(client_stream, empty_frame, sizeof(empty_frame)), (int)sizeof(empty_frame));

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4, 1000000000ull),
        TREV_MSQUIC_ERR_FRAME_TOO_LARGE);
    CHECK_GOTO(body == NULL);
    CHECK_GOTO(body_len == 65);
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4, 1000000000ull), 1);
    CHECK_GOTO(body_len == 0);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_native_malformed_stream_frame_remains_invalid_frame(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    const uint8_t malformed_frame[] = {0, 0, 0, 3, 0xff, 0xff, 0xff};
    uint8_t* body = NULL;
    size_t body_len = 0;
    trevrpc_wire_stream_frame_values* decoded = NULL;

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, malformed_frame, sizeof(malformed_frame)),
        (int)sizeof(malformed_frame));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 1);
    CHECK_EQ_GOTO(trevrpc_wire_decode_stream_frame(body, body_len, &decoded), TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(decoded == NULL);

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(decoded);
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_pending_send_final_failure_rolls_back_send_closed(void) {
    int result = 1;
    trevrpc_msquic_config config = test_config;
    config.max_pending_send_bytes = 4;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t too_large_final_frame[] = {0, 0, 0, 1, 0};
    const uint8_t empty_final_frame[] = {0, 0, 0, 0};

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, too_large_final_frame, sizeof(too_large_final_frame)),
        TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, empty_final_frame, sizeof(empty_final_frame)),
        (int)sizeof(empty_final_frame));

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4096, 1000000000ull), 1);
    CHECK_GOTO(body_len == 0);
    trevrpc_msquic_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 4096, 1000000000ull), 0);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_tracked_borrowed_send_failure_returns_no_completion(void) {
    int result = 1;
    trevrpc_msquic_config config = test_config;
    config.max_pending_send_bytes = 4;
    config.max_pending_send_count = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    const uint8_t borrowed[] = {1};
    const trevrpc_msquic_frame_part parts[] = {{.data = borrowed, .len = sizeof(borrowed)}};
    trevrpc_msquic_send_completion* completion = NULL;

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_frame_parts_with_completion(
                      client_stream, parts, sizeof(parts) / sizeof(parts[0]), 64, &completion),
        TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(completion == NULL);

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_pending_send_slow_reader_is_bounded_and_close_drains(void) {
    int result = 1;
    trevrpc_msquic_config config = test_config;
    config.max_pending_send_bytes = 8 * 1024;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t payload[4096];
    bool exhausted = false;

    memset(payload, 0xa5, sizeof(payload));
    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);

    for (size_t i = 0; i < 4096; i++) {
        intptr_t written = trevrpc_msquic_stream_write(client_stream, payload, sizeof(payload));
        if (written == TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED) {
            exhausted = true;
            break;
        }
        CHECK_EQ_GOTO(written, (int)sizeof(payload));
    }
    CHECK_GOTO(exhausted);

    trevrpc_msquic_stream_abort(client_stream);
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int run_stream_send_prepare_close_case(test_stream_write_kind kind) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    pthread_t write_thread = {0};
    pthread_t close_thread = {0};
    bool write_thread_started = false;
    bool close_thread_started = false;
    bool hook_initialized = false;
    stream_write_args write_args = {.kind = kind};
    stream_close_args close_args = {0};
    stream_hook_state hook = {.block_event = TREV_MSQUIC_TEST_STREAM_SEND_PREPARE};

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(pthread_mutex_init(&hook.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&hook.cond, NULL) == 0);
    hook_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);

    write_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&write_thread, NULL, stream_write_thread, &write_args) == 0);
    write_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_SEND_PREPARE);

    close_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_close_thread, &close_args) == 0);
    close_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    test_stream_hook_release(&hook);

    CHECK_GOTO(pthread_join(write_thread, NULL) == 0);
    write_thread_started = false;
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    client_stream = NULL;
    CHECK_EQ_GOTO(write_args.result, -ECANCELED);
    result = 0;

cleanup:
    if (hook_initialized) {
        test_stream_hook_release(&hook);
    }
    if (write_thread_started) {
        (void)pthread_join(write_thread, NULL);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        client_stream = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    if (hook_initialized) {
        pthread_cond_destroy(&hook.cond);
        pthread_mutex_destroy(&hook.mutex);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_send_preparation_close_lifetime(void) {
    for (test_stream_write_kind kind = TEST_STREAM_WRITE_BYTES; kind <= TEST_STREAM_SHUTDOWN_SEND; kind++) {
        if (run_stream_send_prepare_close_case(kind) != 0) {
            return 1;
        }
    }
    return 0;
}

static int run_failed_stream_finalization_close_case(test_stream_write_kind kind) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    pthread_t write_thread = {0};
    pthread_t close_thread = {0};
    bool write_thread_started = false;
    bool close_thread_started = false;
    bool hook_initialized = false;
    stream_write_args write_args = {.kind = kind};
    stream_close_args close_args = {0};
    stream_hook_state hook = {.block_event = TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL};

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(pthread_mutex_init(&hook.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&hook.cond, NULL) == 0);
    hook_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);
    if (kind == TEST_STREAM_WRITE_FIN_BYTES) {
        trevrpc_msquic_test_fail_next_stream_send();
    } else {
        trevrpc_msquic_test_fail_next_graceful_shutdown();
    }

    write_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&write_thread, NULL, stream_write_thread, &write_args) == 0);
    write_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL);

    close_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_close_thread, &close_args) == 0);
    close_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    test_stream_hook_release(&hook);

    CHECK_GOTO(pthread_join(write_thread, NULL) == 0);
    write_thread_started = false;
    CHECK_EQ_GOTO(write_args.result, kind == TEST_STREAM_WRITE_FIN_BYTES ? -EIO : EIO);
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    client_stream = NULL;
    result = 0;

cleanup:
    if (hook_initialized) {
        test_stream_hook_release(&hook);
    }
    if (write_thread_started) {
        (void)pthread_join(write_thread, NULL);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        client_stream = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    if (hook_initialized) {
        pthread_cond_destroy(&hook.cond);
        pthread_mutex_destroy(&hook.mutex);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_failed_stream_finalization_close_aborts(void) {
    if (run_failed_stream_finalization_close_case(TEST_STREAM_WRITE_FIN_BYTES) != 0) {
        return 1;
    }
    return run_failed_stream_finalization_close_case(TEST_STREAM_SHUTDOWN_SEND);
}

static int run_blocked_capacity_waiter_close_case(bool abort_first) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_pending_send_bytes = 64;
    config.max_pending_send_count = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    pthread_t reserved_thread = {0};
    pthread_t waiter_thread = {0};
    pthread_t close_thread = {0};
    bool reserved_thread_started = false;
    bool waiter_thread_started = false;
    bool close_thread_started = false;
    bool hook_initialized = false;
    stream_write_args reserved_args = {.kind = TEST_STREAM_WRITE_BYTES};
    stream_write_args waiter_args = {.kind = TEST_STREAM_WRITE_MESSAGE_WAIT};
    stream_close_args close_args = {0};
    stream_hook_state hook = {.block_event = TREV_MSQUIC_TEST_STREAM_SEND_RESERVED};

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(pthread_mutex_init(&hook.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&hook.cond, NULL) == 0);
    hook_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);

    reserved_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&reserved_thread, NULL, stream_write_thread, &reserved_args) == 0);
    reserved_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_SEND_RESERVED);

    waiter_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&waiter_thread, NULL, stream_write_thread, &waiter_args) == 0);
    waiter_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_SEND_CAPACITY_WAIT);

    if (abort_first) {
        CHECK_EQ_GOTO(trevrpc_msquic_stream_abort(client_stream), 0);
    }
    close_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_close_thread, &close_args) == 0);
    close_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    test_stream_hook_release(&hook);

    CHECK_GOTO(pthread_join(waiter_thread, NULL) == 0);
    waiter_thread_started = false;
    CHECK_GOTO(pthread_join(reserved_thread, NULL) == 0);
    reserved_thread_started = false;
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    client_stream = NULL;
    CHECK_EQ_GOTO(waiter_args.result, -ECANCELED);
    result = 0;

cleanup:
    if (hook_initialized) {
        test_stream_hook_release(&hook);
    }
    if (waiter_thread_started) {
        (void)pthread_join(waiter_thread, NULL);
    }
    if (reserved_thread_started) {
        (void)pthread_join(reserved_thread, NULL);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        client_stream = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    if (hook_initialized) {
        pthread_cond_destroy(&hook.cond);
        pthread_mutex_destroy(&hook.mutex);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_blocked_capacity_waiter_close_and_abort(void) {
    return run_blocked_capacity_waiter_close_case(false) != 0 || run_blocked_capacity_waiter_close_case(true) != 0;
}

static int test_stream_send_guard_error_states(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* first_client_stream = NULL;
    trevrpc_msquic_stream* first_server_stream = NULL;
    trevrpc_msquic_stream* second_client_stream = NULL;
    trevrpc_msquic_stream* second_server_stream = NULL;
    const uint8_t data[] = {1, 2, 3};
    const trevrpc_msquic_frame_part part = {.data = data, .len = sizeof(data)};

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &first_client_stream, &first_server_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write_frame_parts(first_client_stream, &part, 1, 2), TREV_MSQUIC_ERR_FRAME_TOO_LARGE);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(first_client_stream, data, sizeof(data)), (int)sizeof(data));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_shutdown_send(first_client_stream), 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(first_client_stream, data, sizeof(data)), TREV_MSQUIC_ERR_CLOSED);

    CHECK_GOTO(open_stream_pair(client, server, &second_client_stream, &second_server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_abort(second_client_stream), 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_message_frame_wait_capacity(second_client_stream, data, sizeof(data), 64),
        -ECANCELED);
    result = 0;

cleanup:
    trevrpc_msquic_stream_close(second_server_stream);
    trevrpc_msquic_stream_close(second_client_stream);
    trevrpc_msquic_stream_close(first_server_stream);
    trevrpc_msquic_stream_close(first_client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_buffering_profile_settings_connect_and_transfer(void) {
    const trevrpc_msquic_config configs[] = {
        {
            .alpn = "trevrpc",
            .alpn_len = 7,
            .cert_file = TREVRPC_MSQUIC_TEST_CERT,
            .key_file = TREVRPC_MSQUIC_TEST_KEY,
            .skip_certificate_validation = 1,
            .max_idle_timeout_ms = 30000,
            .peer_bidi_stream_count = 8,
            .stream_recv_window = 1024 * 1024,
            .conn_flow_control_window = 64 * 1024 * 1024,
            .execution_profile = TREV_MSQUIC_EXECUTION_PROFILE_LOW_LATENCY,
            .send_buffering_enabled = 0,
        },
        {
            .alpn = "trevrpc",
            .alpn_len = 7,
            .cert_file = TREVRPC_MSQUIC_TEST_CERT,
            .key_file = TREVRPC_MSQUIC_TEST_KEY,
            .skip_certificate_validation = 1,
            .max_idle_timeout_ms = 30000,
            .peer_bidi_stream_count = 8,
            .stream_recv_window = 1024 * 1024,
            .conn_flow_control_window = 64 * 1024 * 1024,
            .execution_profile = TREV_MSQUIC_EXECUTION_PROFILE_MAX_THROUGHPUT,
            .send_buffering_enabled = 1,
        },
    };
    const uint8_t payload[] = {'t', 'e', 's', 't'};

    for (size_t i = 0; i < sizeof(configs) / sizeof(configs[0]); i++) {
        int result = 1;
        trevrpc_msquic_listener* listener = NULL;
        trevrpc_msquic_conn* client = NULL;
        trevrpc_msquic_conn* server = NULL;
        trevrpc_msquic_stream* client_stream = NULL;
        trevrpc_msquic_stream* server_stream = NULL;
        uint8_t received[sizeof(payload)] = {0};

        CHECK_GOTO(connect_pair_with_config(&configs[i], &listener, &client, &server) == 0);
        CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
        CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, payload, sizeof(payload)), (int)sizeof(payload));
        CHECK_EQ_GOTO(trevrpc_msquic_stream_read(server_stream, received, sizeof(received)), (int)sizeof(received));
        CHECK_GOTO(memcmp(received, payload, sizeof(payload)) == 0);
        if (i == 0) {
            trevrpc_msquic_stream_close(client_stream);
        } else {
            CHECK_EQ_GOTO(trevrpc_msquic_stream_abort(client_stream), 0);
            trevrpc_msquic_stream_close(client_stream);
        }
        client_stream = NULL;
        result = 0;

    cleanup:
        trevrpc_msquic_stream_close(server_stream);
        trevrpc_msquic_stream_close(client_stream);
        trevrpc_msquic_conn_close(server);
        trevrpc_msquic_conn_close(client);
        trevrpc_msquic_listener_close(listener);
        if (result != 0) {
            return result;
        }
    }
    return 0;
}

static int test_invalid_execution_profile_is_rejected(void) {
    trevrpc_msquic_config config = test_config;
    config.execution_profile = (trevrpc_msquic_execution_profile)99;
    trevrpc_msquic_listener* listener = NULL;
    int err = trevrpc_msquic_listen("127.0.0.1", 0, &config, &listener);
    trevrpc_msquic_listener_close(listener);
    return err == EINVAL ? 0 : 1;
}

static int test_frame_parts_borrowed_body_close_drains_send_complete(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t prefix[] = {'p', 'r', 'e'};
    uint8_t borrowed[] = {'b', 'o', 'd', 'y'};
    const uint8_t suffix[] = {'p', 'o', 's', 't'};
    const trevrpc_msquic_frame_part parts[] = {
        {.data = prefix, .len = sizeof(prefix)},
        {.data = borrowed, .len = sizeof(borrowed)},
        {.data = suffix, .len = sizeof(suffix)},
    };

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write_frame_parts_fin(client_stream, parts, sizeof(parts) / sizeof(parts[0]), 64),
        (int)(4 + sizeof(prefix) + sizeof(borrowed) + sizeof(suffix)));
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;

    memset(borrowed, 0xa5, sizeof(borrowed));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 1);
    CHECK_GOTO(body_len == sizeof(prefix) + sizeof(borrowed) + sizeof(suffix));
    CHECK_GOTO(memcmp(body, "prebodypost", body_len) == 0);
    trevrpc_msquic_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 0);

    result = 0;

cleanup:
    trevrpc_msquic_free(body);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_successful_send_completion_is_not_canceled_by_later_abort(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    pthread_t send_thread = {0};
    bool send_thread_started = false;
    bool hook_initialized = false;
    tracked_send_args send_args = {0};
    stream_hook_state hook = {.block_event = TREV_MSQUIC_TEST_STREAM_SEND_COMPLETE_ENTERED};

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_GOTO(pthread_mutex_init(&hook.mutex, NULL) == 0);
    CHECK_GOTO(pthread_cond_init(&hook.cond, NULL) == 0);
    hook_initialized = true;
    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);

    send_args.stream = client_stream;
    CHECK_GOTO(pthread_create(&send_thread, NULL, tracked_send_thread, &send_args) == 0);
    send_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_SEND_COMPLETE_ENTERED);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_abort(client_stream), 0);
    test_stream_hook_release(&hook);
    CHECK_GOTO(pthread_join(send_thread, NULL) == 0);
    send_thread_started = false;
    CHECK_EQ_GOTO(send_args.result, 7);
    CHECK_GOTO(send_args.completion != NULL);
    CHECK_EQ_GOTO(trevrpc_msquic_send_completion_wait(send_args.completion), 0);
    trevrpc_msquic_send_completion_free(send_args.completion);
    send_args.completion = NULL;

    result = 0;

cleanup:
    if (hook_initialized) {
        test_stream_hook_release(&hook);
    }
    if (send_thread_started) {
        (void)pthread_join(send_thread, NULL);
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    if (send_args.completion != NULL) {
        (void)trevrpc_msquic_send_completion_wait(send_args.completion);
        trevrpc_msquic_send_completion_free(send_args.completion);
    }
    if (hook_initialized) {
        pthread_cond_destroy(&hook.cond);
        pthread_mutex_destroy(&hook.mutex);
    }
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_frame_parts_borrowed_body_reset_drains_send_complete(void) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_frame_size = 65536;
    config.max_pending_send_bytes = 65536;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* borrowed = NULL;
    const size_t borrowed_len = 32768;
    trevrpc_msquic_send_completion* completion = NULL;

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    borrowed = malloc(borrowed_len);
    CHECK_GOTO(borrowed != NULL);
    memset(borrowed, 0x5a, borrowed_len);
    const trevrpc_msquic_frame_part parts[] = {
        {.data = borrowed, .len = borrowed_len},
    };
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_frame_parts_with_completion(
                      client_stream, parts, sizeof(parts) / sizeof(parts[0]), config.max_frame_size, &completion),
        (int)(4 + borrowed_len));

    CHECK_EQ_GOTO(trevrpc_msquic_stream_abort(client_stream), 0);
    {
        int wait_result = trevrpc_msquic_send_completion_wait(completion);
        // Abort races with SEND_COMPLETE; MsQuic may report success if the send
        // completed before the abort was processed, or -ECANCELED if canceled.
        // Both are valid; the key is that the wait drains and borrowed memory
        // can be safely reclaimed without hanging.
        CHECK_GOTO(wait_result == 0 || wait_result == -ECANCELED);
    }
    trevrpc_msquic_send_completion_free(completion);
    completion = NULL;
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;
    free(borrowed);
    borrowed = NULL;

    result = 0;

cleanup:
    if (completion != NULL) {
        (void)trevrpc_msquic_send_completion_wait(completion);
        trevrpc_msquic_send_completion_free(completion);
    }
    free(borrowed);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_borrowed_message_wait_drains_send_complete(void) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_frame_size = 65536;
    config.max_pending_send_bytes = 65536;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t* borrowed = NULL;
    uint8_t* expected = NULL;
    uint8_t* frame_body = NULL;
    size_t frame_body_len = 0;
    trevrpc_wire_stream_frame_values* frame = NULL;
    bool took_frame_body = false;
    const size_t body_len = 32768;

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);

    borrowed = malloc(body_len);
    expected = malloc(body_len);
    CHECK_GOTO(borrowed != NULL);
    CHECK_GOTO(expected != NULL);
    for (size_t i = 0; i < body_len; i++) {
        expected[i] = (uint8_t)(i & 0xffu);
    }
    memcpy(borrowed, expected, body_len);

    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = client_stream,
        .max_frame_size = config.max_frame_size,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };

    CHECK_EQ_GOTO(trevrpc_stream_send_message_borrowed_wait(&stream, borrowed, body_len), 0);
    memset(borrowed, 0xa5, body_len);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(
                      server_stream, &frame_body, &frame_body_len, config.max_frame_size, 1000000000ull),
        1);
    CHECK_EQ_GOTO(trevrpc_wire_decode_stream_frame_take(frame_body, frame_body_len, &frame, &took_frame_body), 0);
    if (took_frame_body) {
        frame_body = NULL;
    }
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
    CHECK_GOTO(frame->body.len == body_len);
    CHECK_GOTO(memcmp(frame->body.data, expected, body_len) == 0);

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(frame);
    trevrpc_msquic_free(frame_body);
    free(expected);
    free(borrowed);
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_copy_wait_and_terminal_status_with_one_pending_send(void) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_frame_size = 4096;
    config.max_pending_send_bytes = 4096;
    config.max_pending_send_count = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    const uint8_t message[] = {1, 2, 3};
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .max_frame_size = config.max_frame_size,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    stream.msquic_stream = client_stream;

    CHECK_EQ_GOTO(trevrpc_stream_send_message_copy_wait(&stream, message, sizeof(message)), 0);
    CHECK_EQ_GOTO(trevrpc_stream_send_status(&stream, TREVRPC_STATUS_OK, NULL, 0), 0);
    CHECK_EQ_GOTO(trevrpc_stream_finish_send(&stream), 0);

    for (size_t i = 0; i < 2; i++) {
        uint8_t* frame_body = NULL;
        size_t frame_body_len = 0;
        trevrpc_wire_stream_frame_values* frame = NULL;
        bool took_frame_body = false;
        CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(
                          server_stream, &frame_body, &frame_body_len, config.max_frame_size, 1000000000ull),
            1);
        CHECK_EQ_GOTO(trevrpc_wire_decode_stream_frame_take(frame_body, frame_body_len, &frame, &took_frame_body), 0);
        if (took_frame_body) {
            frame_body = NULL;
        }
        CHECK_GOTO(frame != NULL);
        if (i == 0) {
            CHECK_GOTO(frame->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
            CHECK_GOTO(frame->body.len == sizeof(message));
            CHECK_GOTO(memcmp(frame->body.data, message, sizeof(message)) == 0);
        } else {
            CHECK_GOTO(frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
            CHECK_GOTO(frame->status == TREVRPC_STATUS_OK);
        }
        trevrpc_internal_stream_frame_free(frame);
        trevrpc_msquic_free(frame_body);
    }

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_borrowed_message_batch_wait_drains_send_complete(void) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_frame_size = 65536;
    config.max_pending_send_bytes = 65536;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t first[] = {1, 2, 3};
    uint8_t second_storage[] = {0xa5, 4, 5, 6, 0xa5};
    uint8_t third[] = {7, 8};
    const uint8_t* bodies[] = {first, second_storage + 1, third};
    const size_t body_lens[] = {sizeof(first), 3, sizeof(third)};
    const uint8_t expected[][3] = {{1, 2, 3}, {4, 5, 6}, {7, 8, 0}};

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = client_stream,
        .max_frame_size = config.max_frame_size,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    CHECK_EQ_GOTO(trevrpc_stream_send_messages_borrowed_wait(
                      &stream, bodies, body_lens, sizeof(body_lens) / sizeof(body_lens[0])),
        0);
    memset(first, 0xa5, sizeof(first));
    memset(second_storage, 0xa5, sizeof(second_storage));
    memset(third, 0xa5, sizeof(third));

    for (size_t i = 0; i < sizeof(body_lens) / sizeof(body_lens[0]); i++) {
        uint8_t* frame_body = NULL;
        size_t frame_body_len = 0;
        trevrpc_wire_stream_frame_values* frame = NULL;
        bool took_frame_body = false;
        CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(
                          server_stream, &frame_body, &frame_body_len, config.max_frame_size, 1000000000ull),
            1);
        CHECK_EQ_GOTO(trevrpc_wire_decode_stream_frame_take(frame_body, frame_body_len, &frame, &took_frame_body), 0);
        if (took_frame_body) {
            frame_body = NULL;
        }
        CHECK_GOTO(frame != NULL);
        CHECK_GOTO(frame->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
        CHECK_GOTO(frame->body.len == body_lens[i]);
        CHECK_GOTO(memcmp(frame->body.data, expected[i], body_lens[i]) == 0);
        trevrpc_internal_stream_frame_free(frame);
        trevrpc_msquic_free(frame_body);
    }

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_message_batch_submission_failure_rolls_back_accounting(void) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_frame_size = 4096;
    config.max_pending_send_bytes = 64;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_msquic_stream* server_stream = NULL;
    uint8_t body[256] = {0};
    const uint8_t* borrowed[] = {body, body + 128};
    const size_t body_lens[] = {128, 128};
    trevrpc_stream stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .max_frame_size = config.max_frame_size,
        .max_stream_messages = -1,
        .max_stream_body_size = -1,
        .response_message_count = 7,
        .response_body_size = 11,
        .failure_status = TREVRPC_STATUS_OK,
    };

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    stream.msquic_stream = client_stream;

    CHECK_EQ_GOTO(trevrpc_stream_send_messages(&stream, body, body_lens, 2), TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.response_message_count == 7);
    CHECK_GOTO(stream.response_body_size == 11);
    CHECK_EQ_GOTO(trevrpc_stream_send_messages_borrowed_wait(&stream, borrowed, body_lens, 2),
        TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.response_message_count == 7);
    CHECK_GOTO(stream.response_body_size == 11);

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_stream_single_message_submission_failure_allows_exact_retry(void) {
    int result = 1;
    trevrpc_msquic_config config = test_native_config;
    config.max_frame_size = 4096;
    config.max_pending_send_bytes = 64;
    config.max_pending_send_count = 8;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* copy_client_stream = NULL;
    trevrpc_msquic_stream* copy_server_stream = NULL;
    trevrpc_msquic_stream* borrowed_client_stream = NULL;
    trevrpc_msquic_stream* borrowed_server_stream = NULL;
    uint8_t large_body[128] = {0};
    const uint8_t small_body[] = {1};
    trevrpc_stream copy_stream = {
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .max_frame_size = config.max_frame_size,
        .max_stream_messages = 1,
        .max_stream_body_size = -1,
        .failure_status = TREVRPC_STATUS_OK,
    };
    trevrpc_stream borrowed_stream = copy_stream;

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &copy_client_stream, &copy_server_stream) == 0);
    copy_stream.msquic_stream = copy_client_stream;
    CHECK_EQ_GOTO(
        trevrpc_stream_send_message(&copy_stream, large_body, sizeof(large_body)), TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(copy_stream.response_message_count == 0);
    CHECK_GOTO(copy_stream.response_body_size == 0);
    CHECK_EQ_GOTO(trevrpc_stream_send_message_copy_wait(&copy_stream, large_body, sizeof(large_body)),
        TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(copy_stream.response_message_count == 0);
    CHECK_GOTO(copy_stream.response_body_size == 0);
    CHECK_EQ_GOTO(trevrpc_stream_send_message(&copy_stream, small_body, sizeof(small_body)), 0);
    CHECK_GOTO(copy_stream.response_message_count == 1);
    CHECK_GOTO(copy_stream.response_body_size == sizeof(small_body));

    CHECK_GOTO(open_stream_pair(client, server, &borrowed_client_stream, &borrowed_server_stream) == 0);
    borrowed_stream.msquic_stream = borrowed_client_stream;
    CHECK_EQ_GOTO(trevrpc_stream_send_message_borrowed_wait(&borrowed_stream, large_body, sizeof(large_body)),
        TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED);
    CHECK_GOTO(borrowed_stream.response_message_count == 0);
    CHECK_GOTO(borrowed_stream.response_body_size == 0);
    CHECK_EQ_GOTO(trevrpc_stream_send_message_borrowed_wait(&borrowed_stream, small_body, sizeof(small_body)), 0);
    CHECK_GOTO(borrowed_stream.response_message_count == 1);
    CHECK_GOTO(borrowed_stream.response_body_size == sizeof(small_body));

    result = 0;

cleanup:
    trevrpc_msquic_stream_close(borrowed_server_stream);
    trevrpc_msquic_stream_close(borrowed_client_stream);
    trevrpc_msquic_stream_close(copy_server_stream);
    trevrpc_msquic_stream_close(copy_client_stream);
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_client_close_unblocks_server_accept_stream(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    trevrpc_msquic_stream* stream = NULL;

    CHECK_GOTO(connect_pair(&listener, &client, &server) == 0);
    trevrpc_msquic_conn_close(client);
    client = NULL;
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(server, &stream) == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(stream == NULL);

    result = 0;

cleanup:
    trevrpc_msquic_conn_close(server);
    trevrpc_msquic_conn_close(client);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_webtransport_connects_h3_quic_session(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_wt_session* client_session = NULL;
    wt_accept_args args = {0};
    wt_stream_args stream_args = {0};
    pthread_t thread = {0};
    pthread_t stream_thread = {0};
    bool thread_started = false;
    bool stream_thread_started = false;
    trevrpc_wt_stream* client_stream = NULL;
    uint8_t* body = NULL;
    size_t body_len = 0;
    uint16_t port = 0;
    const uint8_t request[] = {0, 0, 0, 7, 0x22, 5, 'h', 'e', 'l', 'l', 'o'};
    const uint8_t response[] = {0, 0, 0, 7, 0x22, 5, 'w', 'o', 'r', 'l', 'd'};
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .origin = "https://example.test",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    CHECK_GOTO(port != 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_wt_session_thread, &args) == 0);
    thread_started = true;

    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .origin = "https://example.test",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };
    CHECK_EQ_GOTO(trevrpc_wt_dial(&client_config, &client_session), 0);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == 0);
    CHECK_GOTO(args.session != NULL);

    stream_args.session = args.session;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_wt_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_EQ_GOTO(trevrpc_wt_session_open_stream(client_session, &client_stream), 0);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);
    CHECK_GOTO(stream_args.stream != NULL);

    CHECK_EQ_GOTO(trevrpc_wt_stream_write(client_stream, request, sizeof(request)), (int)sizeof(request));
    CHECK_EQ_GOTO(trevrpc_wt_stream_read_frame(stream_args.stream, &body, &body_len, 1024), 1);
    CHECK_GOTO(body_len == 7);
    CHECK_GOTO(body[0] == 0x22);
    CHECK_GOTO(body[1] == 5);
    CHECK_GOTO(memcmp(body + 2, "hello", 5) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;

    CHECK_EQ_GOTO(trevrpc_wt_stream_write(stream_args.stream, response, sizeof(response)), (int)sizeof(response));
    CHECK_EQ_GOTO(trevrpc_wt_stream_read_frame(client_stream, &body, &body_len, 1024), 1);
    CHECK_GOTO(body_len == 7);
    CHECK_GOTO(body[0] == 0x22);
    CHECK_GOTO(body[1] == 5);
    CHECK_GOTO(memcmp(body + 2, "world", 5) == 0);

    result = 0;

cleanup:
    trevrpc_wt_free(body);
    if (stream_thread_started) {
        trevrpc_wt_session_close(args.session);
        (void)pthread_join(stream_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.stream);
    trevrpc_wt_stream_close(client_stream);
    if (thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_wt_session_close(args.session);
    trevrpc_wt_session_close(client_session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_webtransport_session_churn_has_bounded_threads(void) {
    enum { session_count = 16, fixed_thread_allowance = 1 };
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_wt_session* client_session = NULL;
    wt_accept_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    uint16_t port = 0;
#if defined(__linux__)
    size_t baseline_threads = 0;
    size_t churned_threads = 0;
#endif
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .origin = "https://example.test",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    CHECK_GOTO(port != 0);
    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .origin = "https://example.test",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };

    for (size_t i = 0; i <= session_count; i++) {
        args = (wt_accept_args){.listener = listener};
        CHECK_GOTO(pthread_create(&thread, NULL, accept_wt_session_thread, &args) == 0);
        thread_started = true;
        CHECK_EQ_GOTO(trevrpc_wt_dial(&client_config, &client_session), 0);
        CHECK_GOTO(pthread_join(thread, NULL) == 0);
        thread_started = false;
        CHECK_GOTO(args.result == 0);
        CHECK_GOTO(args.session != NULL);
        trevrpc_wt_session_close(args.session);
        args.session = NULL;
        trevrpc_wt_session_close(client_session);
        client_session = NULL;
#if defined(__linux__)
        if (i == 0) {
            CHECK_GOTO(test_process_thread_count(&baseline_threads));
        }
#endif
    }
#if defined(__linux__)
    CHECK_GOTO(test_process_thread_count(&churned_threads));
    if (churned_threads > baseline_threads + fixed_thread_allowance) {
        fprintf(stderr,
            "thread count grew after session churn (before=%zu after=%zu allowance=%d)\n",
            baseline_threads,
            churned_threads,
            fixed_thread_allowance);
        goto cleanup;
    }
#endif
    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_wt_session_close(args.session);
    trevrpc_wt_session_close(client_session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int run_webtransport_rejects_invalid_response(test_invalid_connect_response_kind kind) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_dial_args dial_args = {0};
    pthread_t dial_thread = {0};
    bool dial_thread_started = false;
    uint8_t control[128];
    size_t control_len = 0;
    uint8_t response[128];
    size_t response_len = 0;
    uint16_t port = 0;
    trevrpc_msquic_feature_request features = trevrpc_msquic_default_h3_features();

    CHECK_GOTO(
        trevrpc_msquic_listen_alpns_features("127.0.0.1", 0, &test_h3_config, NULL, 0, &features, &listener) == 0);
    CHECK_GOTO(trevrpc_msquic_listener_port(listener, &port) == 0);
    CHECK_GOTO(port != 0);

    dial_args.config = (trevrpc_wt_config){
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };
    CHECK_GOTO(pthread_create(&dial_thread, NULL, dial_wt_session_thread, &dial_args) == 0);
    dial_thread_started = true;

    CHECK_GOTO(trevrpc_msquic_listener_accept(listener, &server_conn) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(server_conn, &client_control) == 0);
    CHECK_GOTO(test_build_draft02_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(server_conn, &server_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(server_control, control, control_len), (intptr_t)control_len);
    CHECK_EQ_GOTO(trevrpc_msquic_conn_accept_stream(server_conn, &connect_stream), 0);
    CHECK_GOTO(test_build_invalid_connect_response(response, sizeof(response), &response_len, kind) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(connect_stream, response, response_len), (intptr_t)response_len);

    CHECK_GOTO(pthread_join(dial_thread, NULL) == 0);
    dial_thread_started = false;
    CHECK_EQ_GOTO(dial_args.result, TREV_WT_ERR_REJECTED);
    CHECK_GOTO(dial_args.session == NULL);
    result = 0;

cleanup:
    if (dial_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        trevrpc_msquic_listener_shutdown(listener);
        (void)pthread_join(dial_thread, NULL);
    }
    trevrpc_wt_session_close(dial_args.session);
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_webtransport_rejects_invalid_response_headers(void) {
    if (run_webtransport_rejects_invalid_response(TEST_CONNECT_RESPONSE_REQUEST_PSEUDO) != 0) {
        return 1;
    }
    return run_webtransport_rejects_invalid_response(TEST_CONNECT_RESPONSE_INVALID_CONTENT_LENGTH);
}

static int test_webtransport_stream_prelude_remains_byte_oriented(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_wt_session* client_session = NULL;
    wt_accept_args accept_args = {0};
    wt_stream_args stream_args = {0};
    pthread_t accept_thread = {0};
    pthread_t stream_thread = {0};
    bool accept_thread_started = false;
    bool stream_thread_started = false;
    trevrpc_wt_stream* client_stream = NULL;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .origin = "https://example.test",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;

    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .origin = "https://example.test",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };
    CHECK_EQ_GOTO(trevrpc_wt_dial(&client_config, &client_session), 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.session != NULL);

    stream_args.session = accept_args.session;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_wt_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_EQ_GOTO(trevrpc_wt_session_open_stream(client_session, &client_stream), 0);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);
    CHECK_GOTO(stream_args.stream != NULL);

    result = 0;

cleanup:
    if (stream_thread_started) {
        trevrpc_wt_session_close(accept_args.session);
        (void)pthread_join(stream_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.stream);
    trevrpc_wt_stream_close(client_stream);
    if (accept_thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_session_close(client_session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_webtransport_rejects_path_mismatch(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_wt_session* client_session = NULL;
    wt_accept_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/expected",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_wt_session_thread, &args) == 0);
    thread_started = true;

    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/wrong",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };
    CHECK_EQ_GOTO(trevrpc_wt_dial(&client_config, &client_session), TREV_WT_ERR_CLOSED);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == TREV_WT_ERR_REJECTED);
    CHECK_GOTO(args.session == NULL);

    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_wt_session_close(args.session);
    trevrpc_wt_session_close(client_session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int run_malformed_wt_peer_case(const malformed_wt_peer_case* test_case) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_stream* local_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    int open_err = test_case->control_bidirectional ? trevrpc_msquic_conn_open_stream(client_conn, &local_control)
                                                    : trevrpc_msquic_conn_open_uni_stream(client_conn, &local_control);
    CHECK_GOTO(open_err == 0);
    CHECK_GOTO(trevrpc_msquic_stream_write(local_control, test_case->control, test_case->control_len) ==
               (intptr_t)test_case->control_len);

    if (test_case->headers != NULL) {
        CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
        CHECK_GOTO(trevrpc_msquic_stream_write(connect_stream, test_case->headers, test_case->headers_len) ==
                   (intptr_t)test_case->headers_len);
    }

    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, TREV_WT_ERR_REJECTED);
    CHECK_GOTO(accept_args.session == NULL);

    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(client_conn);
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(local_control);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int run_wt_accepts_raw_peer_case(
    const uint8_t* control, size_t control_len, const char* protocol, bool draft02_request, bool expect_accept) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_stream* local_control = NULL;
    trevrpc_msquic_stream* peer_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t server_control[128];
    uint8_t headers[512];
    size_t headers_len = 0;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &local_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_write(local_control, control, control_len) == (intptr_t)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &peer_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(peer_control, server_control, sizeof(server_control)) > 0);
    if (!expect_accept) {
        CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
        accept_thread_started = false;
        CHECK_EQ_GOTO(accept_args.result, TREV_WT_ERR_REJECTED);
        CHECK_GOTO(accept_args.session == NULL);
        result = 0;
        goto cleanup;
    }

    CHECK_GOTO(test_build_connect_headers(headers,
                   sizeof(headers),
                   &headers_len,
                   "CONNECT",
                   protocol,
                   "https",
                   "/trevrpc",
                   "127.0.0.1",
                   draft02_request) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_write(connect_stream, headers, headers_len) == (intptr_t)headers_len);

    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.session != NULL);

    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(peer_control);
    trevrpc_msquic_stream_close(local_control);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_webtransport_accepts_draft02_peer(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft02_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport", true, true);
}

static int test_webtransport_accepts_draft07_peer(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft07_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport", false, true);
}

static bool test_webtransport_modern_profiles_supported(void) {
    trevrpc_msquic_provider_features provider = trevrpc_msquic_provider_features_current();
    return (provider.supported_reset_dialects & TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT) != 0;
}

static int test_webtransport_accepts_draft14_peer(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft14_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(
        control, control_len, "webtransport", false, test_webtransport_modern_profiles_supported());
}

static int test_webtransport_accepts_draft15_peer(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft15_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(
        control, control_len, "webtransport-h3", false, test_webtransport_modern_profiles_supported());
}

static int test_webtransport_accepts_draft15_legacy_connect_token(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft15_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(
        control, control_len, "webtransport", false, test_webtransport_modern_profiles_supported());
}

static int test_expect_server_settings(trevrpc_msquic_stream* control) {
    uint8_t payload[256];
    uint64_t stream_type = 0;
    uint64_t frame_type = 0;
    uint64_t frame_len = 0;
    if (test_read_varint_timeout(control, &stream_type) != 0 || test_read_varint_timeout(control, &frame_type) != 0 ||
        test_read_varint_timeout(control, &frame_len) != 0 || stream_type != 0 || frame_type != 0x04 ||
        frame_len > sizeof(payload) || test_read_exact_timeout(control, payload, (size_t)frame_len) != 0) {
        return -1;
    }

    bool saw_connect_protocol = false;
    bool saw_h3_datagram = false;
    bool saw_draft04_datagram = false;
    bool saw_draft02 = false;
    bool saw_draft07 = false;
    bool saw_draft14 = false;
    bool saw_draft15 = false;
    bool saw_initial_max_data = false;
    bool saw_initial_max_streams_uni = false;
    bool saw_initial_max_streams_bidi = false;
    size_t offset = 0;
    while (offset < (size_t)frame_len) {
        uint64_t id = 0;
        uint64_t value = 0;
        if (test_varint_read(payload, (size_t)frame_len, &offset, &id) != 0 ||
            test_varint_read(payload, (size_t)frame_len, &offset, &value) != 0) {
            return -1;
        }
        switch (id) {
        case TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL:
            if (saw_connect_protocol || value != 1) {
                return -1;
            }
            saw_connect_protocol = true;
            break;
        case TEST_WT_SETTINGS_H3_DATAGRAM:
            if (saw_h3_datagram || value != 1) {
                return -1;
            }
            saw_h3_datagram = true;
            break;
        case TEST_WT_SETTINGS_H3_DRAFT04_DATAGRAM:
            if (saw_draft04_datagram || value != 1) {
                return -1;
            }
            saw_draft04_datagram = true;
            break;
        case TEST_WT_SETTINGS_WEBTRANSPORT_DRAFT02:
            if (saw_draft02 || value != 1) {
                return -1;
            }
            saw_draft02 = true;
            break;
        case TEST_WT_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07:
            if (saw_draft07 || value != 1) {
                return -1;
            }
            saw_draft07 = true;
            break;
        case TEST_WT_SETTINGS_WT_MAX_SESSIONS:
            if (saw_draft14 || value != 1) {
                return -1;
            }
            saw_draft14 = true;
            break;
        case TEST_WT_SETTINGS_WT_ENABLED_DRAFT15:
            if (saw_draft15 || value != 1) {
                return -1;
            }
            saw_draft15 = true;
            break;
        case TEST_WT_SETTINGS_WT_INITIAL_MAX_DATA:
            if (saw_initial_max_data || value != ((uint64_t)1 << 60)) {
                return -1;
            }
            saw_initial_max_data = true;
            break;
        case TEST_WT_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI:
            if (saw_initial_max_streams_uni || value != 0) {
                return -1;
            }
            saw_initial_max_streams_uni = true;
            break;
        case TEST_WT_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI:
            if (saw_initial_max_streams_bidi || value != ((uint64_t)1 << 60)) {
                return -1;
            }
            saw_initial_max_streams_bidi = true;
            break;
        default:
            break;
        }
    }
    bool common = saw_connect_protocol && saw_h3_datagram && saw_draft04_datagram && saw_draft02 && saw_draft07;
    bool legacy = !saw_draft14 && !saw_draft15 && !saw_initial_max_data && !saw_initial_max_streams_uni &&
                  !saw_initial_max_streams_bidi;
    bool current = saw_draft14 && saw_draft15 && saw_initial_max_data && saw_initial_max_streams_uni &&
                   saw_initial_max_streams_bidi;
    return common && (legacy || current) ? 0 : -1;
}

static int test_webtransport_server_settings_do_not_wait_for_peer(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[64];
    uint8_t headers[512];
    size_t control_len = 0;
    size_t headers_len = 0;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(test_expect_server_settings(server_control) == 0);

    CHECK_GOTO(test_build_draft07_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(test_build_connect_headers(headers,
                   sizeof(headers),
                   &headers_len,
                   "CONNECT",
                   "webtransport",
                   "https",
                   "/trevrpc",
                   "127.0.0.1",
                   false) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(connect_stream, headers, headers_len), (int)headers_len);

    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.session != NULL);
    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(client_conn);
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_expect_connect_response_headers(trevrpc_msquic_stream* connect_stream) {
    uint8_t payload[256];
    uint64_t frame_type = 0;
    uint64_t frame_len = 0;
    if (test_read_varint_timeout(connect_stream, &frame_type) != 0 ||
        test_read_varint_timeout(connect_stream, &frame_len) != 0 || frame_type != 0x01 ||
        frame_len > sizeof(payload) || test_read_exact_timeout(connect_stream, payload, (size_t)frame_len) != 0) {
        return -1;
    }
    return 0;
}

static int test_expect_initial_capsule_flow_control(trevrpc_msquic_stream* connect_stream) {
    uint8_t payload[256];
    uint64_t frame_type = 0;
    uint64_t frame_len = 0;
    if (test_expect_connect_response_headers(connect_stream) != 0) {
        return -1;
    }

    bool saw_max_data = false;
    bool saw_max_streams_bidi = false;
    bool saw_max_streams_uni = false;
    for (size_t i = 0; i < 3; i++) {
        if (test_read_varint_timeout(connect_stream, &frame_type) != 0 ||
            test_read_varint_timeout(connect_stream, &frame_len) != 0 || frame_type != 0x00 ||
            frame_len > sizeof(payload) || test_read_exact_timeout(connect_stream, payload, (size_t)frame_len) != 0) {
            return -1;
        }
        size_t offset = 0;
        uint64_t capsule_type = 0;
        uint64_t capsule_len = 0;
        uint64_t capsule_value = 0;
        if (test_varint_read(payload, (size_t)frame_len, &offset, &capsule_type) != 0 ||
            test_varint_read(payload, (size_t)frame_len, &offset, &capsule_len) != 0) {
            return -1;
        }
        size_t value_offset = offset;
        if (test_varint_read(payload, (size_t)frame_len, &offset, &capsule_value) != 0 ||
            capsule_len != offset - value_offset || offset != (size_t)frame_len) {
            return -1;
        }
        switch (capsule_type) {
        case TEST_WT_CAPSULE_MAX_DATA:
            if (saw_max_data || capsule_value != ((uint64_t)1 << 60)) {
                return -1;
            }
            saw_max_data = true;
            break;
        case TEST_WT_CAPSULE_MAX_STREAMS_BIDI:
            if (saw_max_streams_bidi || capsule_value != ((uint64_t)1 << 60)) {
                return -1;
            }
            saw_max_streams_bidi = true;
            break;
        case TEST_WT_CAPSULE_MAX_STREAMS_UNI:
            if (saw_max_streams_uni || capsule_value != 0) {
                return -1;
            }
            saw_max_streams_uni = true;
            break;
        default:
            return -1;
        }
    }
    return saw_max_data && saw_max_streams_bidi && saw_max_streams_uni ? 0 : -1;
}

static int test_webtransport_accepts_webkit_hybrid_peer(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[128];
    uint8_t headers[512];
    size_t control_len = 0;
    size_t headers_len = 0;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    CHECK_GOTO(test_build_webkit_control_settings(control, sizeof(control), &control_len, 100) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(test_expect_server_settings(server_control) == 0);

    CHECK_GOTO(test_build_connect_headers(headers,
                   sizeof(headers),
                   &headers_len,
                   "CONNECT",
                   "webtransport",
                   "https",
                   "/trevrpc",
                   "127.0.0.1",
                   false) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(connect_stream, headers, headers_len), (int)headers_len);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.session != NULL);
    CHECK_GOTO(test_expect_initial_capsule_flow_control(connect_stream) == 0);

    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(client_conn);
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_webtransport_does_not_downgrade_near_webkit_fingerprint(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[128];
    uint8_t headers[512];
    size_t control_len = 0;
    size_t headers_len = 0;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    CHECK_GOTO(test_build_webkit_control_settings(control, sizeof(control), &control_len, 101) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(test_expect_server_settings(server_control) == 0);

    CHECK_GOTO(test_build_connect_headers(headers,
                   sizeof(headers),
                   &headers_len,
                   "CONNECT",
                   "webtransport",
                   "https",
                   "/trevrpc",
                   "127.0.0.1",
                   false) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(connect_stream, headers, headers_len), (int)headers_len);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.session != NULL);
    CHECK_GOTO(test_expect_connect_response_headers(connect_stream) == 0);
    uint8_t unexpected = 0;
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_read_timeout(connect_stream, &unexpected, 1, 100000000ull), TREV_MSQUIC_ERR_TIMEOUT);

    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(client_conn);
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_combined_http3_accepts_webkit_hybrid_peer(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    trevrpc_h3_stream* pending_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    h3_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[128];
    uint8_t headers[512];
    size_t control_len = 0;
    size_t headers_len = 0;
    const trevrpc_wt_config server_config = {
        .path = "/trevrpc",
        .max_streams_per_session = 11,
    };

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client_conn, &server_conn) == 0);
    accept_args.conn = server_conn;
    accept_args.config = server_config;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_h3_conn_thread, &accept_args) == 0);
    accept_thread_started = true;

    CHECK_GOTO(test_build_webkit_control_settings(control, sizeof(control), &control_len, 100) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(test_expect_server_settings(server_control) == 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.h3_conn != NULL);

    CHECK_GOTO(test_build_connect_headers(headers,
                   sizeof(headers),
                   &headers_len,
                   "CONNECT",
                   "webtransport",
                   "https",
                   "/trevrpc",
                   "127.0.0.1",
                   false) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(connect_stream, headers, headers_len), (int)headers_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &pending_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HTTP3;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(accept_args.h3_conn, pending_stream, 5000000000ull, &wt_stream, &resolution), 0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HANDLED);
    CHECK_GOTO(wt_stream == NULL);
    CHECK_GOTO(test_expect_initial_capsule_flow_control(connect_stream) == 0);

    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        (void)pthread_join(accept_thread, NULL);
        server_conn = NULL;
    }
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(pending_stream);
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_webtransport_h3_control_and_connect_remain_byte_oriented(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft07_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport", false, true);
}

static int test_webtransport_rejects_malformed_control_stream_type(void) {
    const uint8_t control[] = {0x01, 0x04, 0x00};
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_bidirectional_control_stream(void) {
    const uint8_t control[] = {0x00, 0x04, 0x00};
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
        .control_bidirectional = true,
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_missing_webtransport_setting(void) {
    const uint8_t control[] = {0x00, 0x04, 0x00};
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_malformed_settings_payload(void) {
    const uint8_t control[] = {0x00, 0x04, 0x01, 0x40};
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_duplicate_settings(void) {
    const wt_setting_pair settings[] = {
        {TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1},
        {TEST_WT_SETTINGS_H3_DATAGRAM, 1},
        {TEST_WT_SETTINGS_WT_ENABLED_DRAFT15, 1},
        {TEST_WT_SETTINGS_WT_ENABLED_DRAFT15, 1},
    };
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_control_settings(
            control, sizeof(control), &control_len, settings, sizeof(settings) / sizeof(settings[0])) != 0) {
        return 1;
    }
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = control_len,
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_malformed_qpack_block(void) {
    uint8_t control[64];
    size_t control_len = 0;
    const uint8_t headers[] = {0x01, 0x03, 0x00, 0x00, 0x80};
    if (test_build_draft02_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = control_len,
        .headers = headers,
        .headers_len = sizeof(headers),
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_missing_connect_pseudo_header(void) {
    int result = 1;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t headers[512];
    size_t headers_len = 0;

    CHECK_GOTO(test_build_draft02_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(
        test_build_connect_headers(
            headers, sizeof(headers), &headers_len, "CONNECT", "webtransport", "https", NULL, "127.0.0.1", false) == 0);
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = control_len,
        .headers = headers,
        .headers_len = headers_len,
    };
    CHECK_GOTO(run_malformed_wt_peer_case(&test_case) == 0);
    result = 0;

cleanup:
    return result;
}

static int test_webtransport_rejects_invalid_connect_method(void) {
    int result = 1;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t headers[512];
    size_t headers_len = 0;

    CHECK_GOTO(test_build_draft02_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(
        test_build_connect_headers(
            headers, sizeof(headers), &headers_len, "GET", "webtransport", "https", "/trevrpc", "127.0.0.1", false) ==
        0);
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = control_len,
        .headers = headers,
        .headers_len = headers_len,
    };
    CHECK_GOTO(run_malformed_wt_peer_case(&test_case) == 0);
    result = 0;

cleanup:
    return result;
}

static int test_webtransport_listener_shutdown_unblocks_accept(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    wt_accept_args args = {0};
    pthread_t thread = {0};
    bool thread_started = false;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_wt_session_thread, &args) == 0);
    thread_started = true;
    trevrpc_wt_listener_shutdown(listener);
    CHECK_GOTO(pthread_join(thread, NULL) == 0);
    thread_started = false;
    CHECK_GOTO(args.result == TREV_WT_ERR_CLOSED);
    CHECK_GOTO(args.session == NULL);

    result = 0;

cleanup:
    if (thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(thread, NULL);
    }
    trevrpc_wt_session_close(args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_webtransport_session_shutdown_unblocks_accept_stream(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_wt_session* client_session = NULL;
    wt_accept_args accept_args = {0};
    wt_stream_args stream_args = {0};
    pthread_t accept_thread = {0};
    pthread_t stream_thread = {0};
    bool accept_thread_started = false;
    bool stream_thread_started = false;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;

    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };
    CHECK_EQ_GOTO(trevrpc_wt_dial(&client_config, &client_session), 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.session != NULL);

    stream_args.session = accept_args.session;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_wt_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    trevrpc_wt_session_shutdown(accept_args.session);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, TREV_WT_ERR_CLOSED);
    CHECK_GOTO(stream_args.stream == NULL);

    result = 0;

cleanup:
    if (stream_thread_started) {
        trevrpc_wt_session_shutdown(accept_args.session);
        (void)pthread_join(stream_thread, NULL);
    }
    if (accept_thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.stream);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_session_close(client_session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int test_webtransport_stream_close_unblocks_peer_read(void) {
    int result = 1;
    trevrpc_wt_listener* listener = NULL;
    trevrpc_wt_session* client_session = NULL;
    trevrpc_wt_stream* client_stream = NULL;
    wt_accept_args accept_args = {0};
    wt_stream_args stream_args = {0};
    pthread_t accept_thread = {0};
    pthread_t stream_thread = {0};
    bool accept_thread_started = false;
    bool stream_thread_started = false;
    uint8_t* body = NULL;
    size_t body_len = 0;
    uint16_t port = 0;
    trevrpc_wt_config server_config = {
        .host = "127.0.0.1",
        .path = "/trevrpc",
        .cert_file = TREVRPC_MSQUIC_TEST_CERT,
        .key_file = TREVRPC_MSQUIC_TEST_KEY,
        .max_streams_per_session = 8,
    };

    CHECK_GOTO(trevrpc_wt_listen(&server_config, &listener) == 0);
    CHECK_GOTO(trevrpc_wt_listener_port(listener, &port) == 0);
    accept_args.listener = listener;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_wt_session_thread, &accept_args) == 0);
    accept_thread_started = true;

    trevrpc_wt_config client_config = {
        .host = "127.0.0.1",
        .port = port,
        .path = "/trevrpc",
        .skip_certificate_validation = 1,
        .max_streams_per_session = 8,
    };
    CHECK_EQ_GOTO(trevrpc_wt_dial(&client_config, &client_session), 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    CHECK_EQ_GOTO(accept_args.result, 0);

    stream_args.session = accept_args.session;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_wt_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_EQ_GOTO(trevrpc_wt_session_open_stream(client_session, &client_stream), 0);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);

    trevrpc_wt_stream_close(client_stream);
    client_stream = NULL;
    CHECK_EQ_GOTO(trevrpc_wt_stream_read_frame(stream_args.stream, &body, &body_len, 1024), TREV_WT_ERR_CLOSED);

    result = 0;

cleanup:
    trevrpc_wt_free(body);
    if (stream_thread_started) {
        trevrpc_wt_session_shutdown(accept_args.session);
        (void)pthread_join(stream_thread, NULL);
    }
    if (accept_thread_started) {
        trevrpc_wt_listener_shutdown(listener);
        (void)pthread_join(accept_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.stream);
    trevrpc_wt_stream_close(client_stream);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_session_close(client_session);
    trevrpc_wt_listener_close(listener);
    return result;
}

typedef struct h3_bound_fixture {
    trevrpc_msquic_config transport_config;
    trevrpc_msquic_listener* listener;
    trevrpc_msquic_conn* client_conn;
    trevrpc_msquic_conn* server_conn;
    trevrpc_msquic_stream* server_control;
    trevrpc_msquic_stream* client_control;
    h3_accept_args accept_args;
    pthread_t accept_thread;
    bool accept_thread_started;
} h3_bound_fixture;

static void test_h3_bound_fixture_cleanup(h3_bound_fixture* fixture) {
    if (fixture->accept_thread_started) {
        trevrpc_msquic_conn_shutdown(fixture->server_conn);
        (void)pthread_join(fixture->accept_thread, NULL);
        fixture->accept_thread_started = false;
        fixture->server_conn = NULL;
    }
    trevrpc_msquic_stream_close(fixture->server_control);
    trevrpc_msquic_stream_close(fixture->client_control);
    trevrpc_h3_conn_close(fixture->accept_args.h3_conn);
    trevrpc_msquic_conn_close(fixture->server_conn);
    trevrpc_msquic_conn_close(fixture->client_conn);
    trevrpc_msquic_listener_close(fixture->listener);
    memset(fixture, 0, sizeof(*fixture));
}

static int test_h3_bound_fixture_open(
    h3_bound_fixture* fixture, const uint8_t* peer_control, size_t peer_control_len, size_t max_h3_frame_payload) {
    trevrpc_msquic_config configured_transport = fixture->transport_config;
    memset(fixture, 0, sizeof(*fixture));
    fixture->transport_config = configured_transport;
    const trevrpc_msquic_config* transport_config =
        fixture->transport_config.alpn != NULL ? &fixture->transport_config : &test_h3_config;
    if (connect_pair_with_config(transport_config, &fixture->listener, &fixture->client_conn, &fixture->server_conn) !=
        0) {
        goto fail;
    }
    fixture->accept_args.conn = fixture->server_conn;
    fixture->accept_args.config = (trevrpc_wt_config){
        .path = "/trevrpc",
        .max_streams_per_session = 8,
    };
    fixture->accept_args.max_h3_frame_payload = max_h3_frame_payload;
    if (pthread_create(&fixture->accept_thread, NULL, accept_h3_conn_thread, &fixture->accept_args) != 0) {
        goto fail;
    }
    fixture->accept_thread_started = true;
    if (trevrpc_msquic_conn_open_uni_stream(fixture->client_conn, &fixture->client_control) != 0 ||
        trevrpc_msquic_stream_write(fixture->client_control, peer_control, peer_control_len) !=
            (intptr_t)peer_control_len ||
        trevrpc_msquic_conn_accept_stream(fixture->client_conn, &fixture->server_control) != 0) {
        goto fail;
    }
    uint8_t server_settings[64];
    if (trevrpc_msquic_stream_read_timeout(
            fixture->server_control, server_settings, sizeof(server_settings), 5000000000ull) <= 0 ||
        pthread_join(fixture->accept_thread, NULL) != 0) {
        goto fail;
    }
    fixture->accept_thread_started = false;
    fixture->server_conn = NULL;
    if (fixture->accept_args.result != 0 || fixture->accept_args.h3_conn == NULL) {
        goto fail;
    }
    return 0;

fail:
    test_h3_bound_fixture_cleanup(fixture);
    return -1;
}

static int test_append_filled_h3_frame(
    uint8_t* out, size_t out_len, size_t* offset, uint64_t frame_type, size_t payload_len, uint8_t fill) {
    if (test_varint_write(out, out_len, offset, frame_type) != 0 ||
        test_varint_write(out, out_len, offset, payload_len) != 0 || out_len - *offset < payload_len) {
        return -1;
    }
    memset(out + *offset, fill, payload_len);
    *offset += payload_len;
    return 0;
}

static int test_http3_post_data_adapter_and_request_local_rejection(void) {
    int result = 1;
    int admission_calls = 0;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* client_qpack_encoder = NULL;
    trevrpc_msquic_stream* rejected_stream = NULL;
    trevrpc_msquic_stream* media_stream = NULL;
    trevrpc_msquic_stream* duplicate_media_stream = NULL;
    trevrpc_msquic_stream* request_stream = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    trevrpc_msquic_stream* wt_client_stream = NULL;
    trevrpc_msquic_stream* partial_client_stream = NULL;
    trevrpc_msquic_stream* shutdown_client_stream = NULL;
    trevrpc_msquic_stream* exact_client_stream = NULL;
    trevrpc_msquic_stream* oversized_client_stream = NULL;
    trevrpc_h3_stream* partial_server_stream = NULL;
    trevrpc_h3_stream* shutdown_server_stream = NULL;
    trevrpc_h3_stream* exact_server_stream = NULL;
    trevrpc_h3_stream* oversized_server_stream = NULL;
    h3_accept_args accept_args = {0};
    h3_stream_args stream_args = {0};
    h3_resolve_args partial_args = {0};
    h3_resolve_args shutdown_args = {0};
    pthread_t accept_thread = {0};
    pthread_t stream_thread = {0};
    pthread_t partial_thread = {0};
    pthread_t shutdown_thread = {0};
    bool accept_thread_started = false;
    bool stream_thread_started = false;
    bool partial_thread_started = false;
    bool shutdown_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t rejected_headers[512];
    size_t rejected_headers_len = 0;
    uint8_t request[1024];
    size_t request_len = 0;
    uint8_t connect_headers[512];
    size_t connect_headers_len = 0;
    uint8_t partial_headers[512];
    size_t partial_headers_len = 0;
    uint8_t field_headers[8192];
    size_t field_headers_len = 0;
    uint8_t response_payload[64];
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t rpc_request[] = {0, 0, 0, 7, 0x22, 5, 'h', 'e', 'l', 'l', 'o'};
    const uint8_t rpc_response[] = {0, 0, 0, 7, 0x22, 5, 'w', 'o', 'r', 'l', 'd'};
    const trevrpc_wt_config server_config = {.path = "/trevrpc", .max_streams_per_session = 8};

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client_conn, &server_conn) == 0);
    accept_args.conn = server_conn;
    accept_args.config = server_config;
    accept_args.admission = test_http3_admission;
    accept_args.admission_user_data = &admission_calls;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_h3_conn_thread, &accept_args) == 0);
    accept_thread_started = true;

    CHECK_GOTO(test_build_draft07_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.h3_conn != NULL);

    const uint8_t qpack_zero_capacity[] = {0x02, 0x20};
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_qpack_encoder) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_qpack_encoder, qpack_zero_capacity, sizeof(qpack_zero_capacity)),
        (int)sizeof(qpack_zero_capacity));

    CHECK_GOTO(test_build_post_headers_with_extra(field_headers, sizeof(field_headers), &field_headers_len, 3821) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &exact_client_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write_fin(exact_client_stream, field_headers, field_headers_len), (int)field_headers_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &exact_server_stream), 0);
    int field_resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    trevrpc_wt_stream* field_wt_stream = NULL;
    CHECK_EQ_GOTO(trevrpc_h3_stream_resolve(
                      accept_args.h3_conn, exact_server_stream, 5000000000ull, &field_wt_stream, &field_resolution),
        0);
    CHECK_EQ_GOTO(field_resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_GOTO(field_wt_stream == NULL);
    CHECK_GOTO(admission_calls == 1);

    field_headers_len = 0;
    CHECK_GOTO(test_build_post_headers_with_extra(field_headers, sizeof(field_headers), &field_headers_len, 3822) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &oversized_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(oversized_client_stream, field_headers, field_headers_len),
        (int)field_headers_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &oversized_server_stream), 0);
    field_resolution = TREV_H3_STREAM_RESOLVED_HTTP3;
    CHECK_EQ_GOTO(trevrpc_h3_stream_resolve(
                      accept_args.h3_conn, oversized_server_stream, 5000000000ull, &field_wt_stream, &field_resolution),
        0);
    CHECK_EQ_GOTO(field_resolution, TREV_H3_STREAM_RESOLVED_HANDLED);

    CHECK_GOTO(
        test_build_post_headers(
            partial_headers, sizeof(partial_headers), &partial_headers_len, "POST", "/rpc", "application/trevrpc") ==
        0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &partial_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(partial_client_stream, partial_headers, 1), 1);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &partial_server_stream), 0);
    partial_args.conn = accept_args.h3_conn;
    partial_args.stream = partial_server_stream;
    partial_args.timeout_nanos = 5000000000ull;
    CHECK_GOTO(pthread_create(&partial_thread, NULL, resolve_h3_stream_thread, &partial_args) == 0);
    partial_thread_started = true;

    stream_args.conn = accept_args.h3_conn;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_h3_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_GOTO(test_build_post_headers(rejected_headers,
                   sizeof(rejected_headers),
                   &rejected_headers_len,
                   "POST",
                   "/rpc",
                   "application/trevrpc; charset=binary") == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &media_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(media_stream, rejected_headers, rejected_headers_len),
        (int)rejected_headers_len);
    rejected_headers_len = 0;
    CHECK_GOTO(test_build_post_headers_duplicate_content(
                   rejected_headers, sizeof(rejected_headers), &rejected_headers_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &duplicate_media_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(duplicate_media_stream, rejected_headers, rejected_headers_len),
        (int)rejected_headers_len);
    rejected_headers_len = 0;
    CHECK_GOTO(test_build_post_headers(rejected_headers,
                   sizeof(rejected_headers),
                   &rejected_headers_len,
                   "POST",
                   "/wrong",
                   "application/trevrpc") == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &rejected_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(rejected_stream, rejected_headers, rejected_headers_len),
        (int)rejected_headers_len);

    CHECK_GOTO(
        test_build_post_headers(request, sizeof(request), &request_len, "POST", "/rpc", "Application/TrevRPC") == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x21) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 3) == 0);
    memcpy(request + request_len, "ext", 3);
    request_len += 3;
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x00) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 2) == 0);
    memcpy(request + request_len, rpc_request, 2);
    request_len += 2;
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x00) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, sizeof(rpc_request) - 2) == 0);
    memcpy(request + request_len, rpc_request + 2, sizeof(rpc_request) - 2);
    request_len += sizeof(rpc_request) - 2;
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x00) == 0);
    CHECK_GOTO(
        test_varint_write(request, sizeof(request), &request_len, sizeof(rpc_request) + sizeof(rpc_response)) == 0);
    memcpy(request + request_len, rpc_request, sizeof(rpc_request));
    request_len += sizeof(rpc_request);
    memcpy(request + request_len, rpc_response, sizeof(rpc_response));
    request_len += sizeof(rpc_response);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &request_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(request_stream, request, request_len), (int)request_len);

    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);
    CHECK_GOTO(stream_args.wt_stream == NULL);
    CHECK_GOTO(stream_args.h3_stream != NULL);
    CHECK_GOTO(admission_calls == 2);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(partial_client_stream, partial_headers + 1, partial_headers_len - 1),
        (int)(partial_headers_len - 1));
    CHECK_GOTO(pthread_join(partial_thread, NULL) == 0);
    partial_thread_started = false;
    CHECK_EQ_GOTO(partial_args.result, 0);
    CHECK_EQ_GOTO(partial_args.resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_GOTO(admission_calls == 3);
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(stream_args.h3_stream, &body, &body_len, 7), 1);
    CHECK_GOTO(body_len == 7);
    CHECK_GOTO(memcmp(body, rpc_request + 4, body_len) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(stream_args.h3_stream, &body, &body_len, 7), 1);
    CHECK_GOTO(body_len == 7 && memcmp(body, rpc_request + 4, body_len) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(stream_args.h3_stream, &body, &body_len, 7), 1);
    CHECK_GOTO(body_len == 7 && memcmp(body, rpc_response + 4, body_len) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_h3_stream_write_fin(stream_args.h3_stream, rpc_response, sizeof(rpc_response)),
        (int)sizeof(rpc_response));

    uint64_t frame_type = 0;
    uint64_t frame_len = 0;
    CHECK_GOTO(test_read_varint(request_stream, &frame_type) == 0);
    CHECK_GOTO(test_read_varint(request_stream, &frame_len) == 0);
    CHECK_GOTO(frame_type == 0x01 && frame_len <= sizeof(response_payload));
    CHECK_GOTO(test_read_exact(request_stream, response_payload, (size_t)frame_len) == 0);
    CHECK_GOTO(test_read_varint(request_stream, &frame_type) == 0);
    CHECK_GOTO(test_read_varint(request_stream, &frame_len) == 0);
    CHECK_GOTO(frame_type == 0x00 && frame_len == sizeof(rpc_response));
    CHECK_GOTO(test_read_exact(request_stream, response_payload, sizeof(rpc_response)) == 0);
    CHECK_GOTO(memcmp(response_payload, rpc_response, sizeof(rpc_response)) == 0);
    trevrpc_h3_stream_close(stream_args.h3_stream);
    stream_args.h3_stream = NULL;

    memset(&stream_args, 0, sizeof(stream_args));
    stream_args.conn = accept_args.h3_conn;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_h3_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_GOTO(test_build_connect_headers(connect_headers,
                   sizeof(connect_headers),
                   &connect_headers_len,
                   "CONNECT",
                   "webtransport",
                   "https",
                   "/trevrpc",
                   "localhost",
                   false) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &connect_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write(connect_stream, connect_headers, connect_headers_len), (int)connect_headers_len);
    uint64_t connect_stream_id = 0;
    CHECK_GOTO(trevrpc_msquic_stream_id(connect_stream, &connect_stream_id) == 0);
    request_len = 0;
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x41) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, connect_stream_id) == 0);
    memcpy(request + request_len, rpc_request, sizeof(rpc_request));
    request_len += sizeof(rpc_request);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &wt_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(wt_client_stream, request, request_len), (int)request_len);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);
    CHECK_GOTO(stream_args.wt_stream != NULL);
    CHECK_GOTO(stream_args.h3_stream == NULL);
    CHECK_EQ_GOTO(trevrpc_wt_stream_read_frame(stream_args.wt_stream, &body, &body_len, 1024), 1);
    CHECK_GOTO(body_len == 7 && memcmp(body, rpc_request + 4, body_len) == 0);

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &shutdown_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &shutdown_server_stream), 0);
    shutdown_args.conn = accept_args.h3_conn;
    shutdown_args.stream = shutdown_server_stream;
    shutdown_args.timeout_nanos = 5000000000ull;
    CHECK_GOTO(pthread_create(&shutdown_thread, NULL, resolve_h3_stream_thread, &shutdown_args) == 0);
    shutdown_thread_started = true;
    trevrpc_h3_conn_shutdown(accept_args.h3_conn);
    CHECK_GOTO(pthread_join(shutdown_thread, NULL) == 0);
    shutdown_thread_started = false;
    CHECK_GOTO(shutdown_args.result != 0 || shutdown_args.resolution == TREV_H3_STREAM_RESOLVED_HANDLED);

    result = 0;

cleanup:
    trevrpc_wt_free(body);
    if (stream_thread_started) {
        trevrpc_h3_conn_shutdown(accept_args.h3_conn);
        (void)pthread_join(stream_thread, NULL);
    }
    if (partial_thread_started || shutdown_thread_started) {
        trevrpc_h3_conn_shutdown(accept_args.h3_conn);
    }
    if (partial_thread_started) {
        (void)pthread_join(partial_thread, NULL);
    }
    if (shutdown_thread_started) {
        (void)pthread_join(shutdown_thread, NULL);
    }
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        (void)pthread_join(accept_thread, NULL);
        server_conn = NULL;
    }
    trevrpc_h3_stream_close(stream_args.h3_stream);
    trevrpc_h3_stream_close(partial_server_stream);
    trevrpc_h3_stream_close(shutdown_server_stream);
    trevrpc_h3_stream_close(exact_server_stream);
    trevrpc_h3_stream_close(oversized_server_stream);
    trevrpc_wt_stream_close(stream_args.wt_stream);
    trevrpc_msquic_stream_close(request_stream);
    trevrpc_msquic_stream_close(rejected_stream);
    trevrpc_msquic_stream_close(media_stream);
    trevrpc_msquic_stream_close(duplicate_media_stream);
    trevrpc_msquic_stream_close(wt_client_stream);
    trevrpc_msquic_stream_close(connect_stream);
    trevrpc_msquic_stream_close(partial_client_stream);
    trevrpc_msquic_stream_close(shutdown_client_stream);
    trevrpc_msquic_stream_close(exact_client_stream);
    trevrpc_msquic_stream_close(oversized_client_stream);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_stream_close(client_qpack_encoder);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_webtransport_unidirectional_stream_rejects_mismatched_session(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* connect_client_stream = NULL;
    trevrpc_msquic_stream* unidi_client_stream = NULL;
    trevrpc_msquic_stream* trigger_client_stream = NULL;
    trevrpc_h3_stream* connect_server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    h3_stream_args stream_args = {0};
    pthread_t stream_thread = {0};
    bool stream_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t connect_headers[512];
    size_t connect_headers_len = 0;
    uint8_t unidi_prefix[16];
    size_t unidi_prefix_len = 0;
    uint8_t trigger_headers[512];
    size_t trigger_headers_len = 0;

    CHECK_GOTO(test_build_draft07_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(test_build_connect_headers(connect_headers,
                   sizeof(connect_headers),
                   &connect_headers_len,
                   "CONNECT",
                   "webtransport",
                   "https",
                   "/trevrpc",
                   "localhost",
                   false) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &connect_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(connect_client_stream, connect_headers, connect_headers_len),
        (int)connect_headers_len);
    uint64_t connect_stream_id = 0;
    CHECK_GOTO(trevrpc_msquic_stream_id(connect_client_stream, &connect_stream_id) == 0);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &connect_server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HTTP3;
    CHECK_EQ_GOTO(trevrpc_h3_stream_resolve(
                      fixture.accept_args.h3_conn, connect_server_stream, 5000000000ull, &wt_stream, &resolution),
        0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HANDLED);
    CHECK_GOTO(wt_stream == NULL);

    stream_args.conn = fixture.accept_args.h3_conn;
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(fixture.client_conn, &unidi_client_stream) == 0);
    CHECK_GOTO(test_varint_write(unidi_prefix, sizeof(unidi_prefix), &unidi_prefix_len, 0x54) == 0);
    CHECK_GOTO(test_varint_write(unidi_prefix, sizeof(unidi_prefix), &unidi_prefix_len, connect_stream_id + 4) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write(unidi_client_stream, unidi_prefix, unidi_prefix_len), (int)unidi_prefix_len);
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_h3_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_GOTO(
        test_build_post_headers(
            trigger_headers, sizeof(trigger_headers), &trigger_headers_len, "POST", "/rpc", "application/trevrpc") ==
        0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &trigger_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(trigger_client_stream, trigger_headers, trigger_headers_len),
        (int)trigger_headers_len);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x108) == 0);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    result = 0;

cleanup:
    if (stream_thread_started) {
        trevrpc_h3_conn_shutdown(fixture.accept_args.h3_conn);
        (void)pthread_join(stream_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.wt_stream);
    trevrpc_h3_stream_close(stream_args.h3_stream);
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(connect_server_stream);
    trevrpc_msquic_stream_close(trigger_client_stream);
    trevrpc_msquic_stream_close(unidi_client_stream);
    trevrpc_msquic_stream_close(connect_client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_webtransport_unidirectional_stream_times_out_incomplete_session_id(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* unidi_client_stream = NULL;
    trevrpc_msquic_stream* trigger_client_stream = NULL;
    h3_stream_args stream_args = {0};
    pthread_t stream_thread = {0};
    bool stream_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t unidi_prefix[16];
    size_t unidi_prefix_len = 0;
    uint8_t trigger_headers[512];
    size_t trigger_headers_len = 0;

    CHECK_GOTO(test_build_draft07_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(fixture.client_conn, &unidi_client_stream) == 0);
    CHECK_GOTO(test_varint_write(unidi_prefix, sizeof(unidi_prefix), &unidi_prefix_len, 0x54) == 0);
    unidi_prefix[unidi_prefix_len++] = 0x40;
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write(unidi_client_stream, unidi_prefix, unidi_prefix_len), (int)unidi_prefix_len);

    stream_args.conn = fixture.accept_args.h3_conn;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_h3_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
    CHECK_GOTO(
        test_build_post_headers(
            trigger_headers, sizeof(trigger_headers), &trigger_headers_len, "POST", "/rpc", "application/trevrpc") ==
        0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &trigger_client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(trigger_client_stream, trigger_headers, trigger_headers_len),
        (int)trigger_headers_len);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);
    CHECK_GOTO(stream_args.h3_stream != NULL);
    CHECK_GOTO(test_wait_peer_close_error_millis(fixture.client_conn, 0x108, 7000) == 0);
    result = 0;

cleanup:
    if (stream_thread_started) {
        trevrpc_h3_conn_shutdown(fixture.accept_args.h3_conn);
        (void)pthread_join(stream_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.wt_stream);
    trevrpc_h3_stream_close(stream_args.h3_stream);
    trevrpc_msquic_stream_close(trigger_client_stream);
    trevrpc_msquic_stream_close(unidi_client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int send_webtransport_unknown_unidirectional_stream(
    h3_bound_fixture* fixture, size_t expected_progress_count, trevrpc_msquic_stream** out_unidi) {
    int result = 1;
    trevrpc_msquic_stream* unidi = NULL;
    uint8_t stream_type[8];
    size_t stream_type_len = 0;

    *out_unidi = NULL;
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(fixture->client_conn, &unidi) == 0);
    CHECK_GOTO(test_varint_write(stream_type, sizeof(stream_type), &stream_type_len, 0x21) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(unidi, stream_type, stream_type_len), (int)stream_type_len);
    CHECK_GOTO(trevrpc_h3_test_wait_unidi_progress(
                   fixture->accept_args.h3_conn, expected_progress_count, expected_progress_count, 5000000000ull) == 0);
    *out_unidi = unidi;
    unidi = NULL;
    result = 0;

cleanup:
    trevrpc_msquic_stream_close(unidi);
    return result;
}

static int test_webtransport_unidirectional_monitor_slots_reused(void) {
    enum { stream_count = 64, fixed_thread_allowance = 1 };
    int result = 1;
    h3_bound_fixture fixture = {0};
    h3_stream_args stream_args = {0};
    pthread_t stream_thread = {0};
    bool stream_thread_started = false;
    trevrpc_msquic_stream* trigger = NULL;
    trevrpc_msquic_stream* unknown_unidi = NULL;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t headers[512];
    size_t headers_len = 0;
#if defined(__linux__)
    size_t baseline_threads = 0;
    size_t churned_threads = 0;
#endif

    fixture.transport_config = test_h3_config;
    fixture.transport_config.peer_bidi_stream_count = 64;
    fixture.transport_config.peer_unidi_stream_count = 64;
    CHECK_GOTO(test_build_draft07_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(
        test_build_post_headers(headers, sizeof(headers), &headers_len, "POST", "/rpc", "application/trevrpc") == 0);

    stream_args.conn = fixture.accept_args.h3_conn;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_h3_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
#if defined(__linux__)
    CHECK_GOTO(test_process_thread_count(&baseline_threads));
#endif
    for (size_t i = 0; i < stream_count; i++) {
        CHECK_GOTO(send_webtransport_unknown_unidirectional_stream(&fixture, i + 1, &unknown_unidi) == 0);
        trevrpc_msquic_stream_close(unknown_unidi);
        unknown_unidi = NULL;
    }
#if defined(__linux__)
    CHECK_GOTO(test_process_thread_count(&churned_threads));
    if (churned_threads > baseline_threads + fixed_thread_allowance) {
        fprintf(stderr,
            "thread count grew after transport churn (before=%zu after=%zu allowance=%d)\n",
            baseline_threads,
            churned_threads,
            fixed_thread_allowance);
        goto cleanup;
    }
#endif
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &trigger) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(trigger, headers, headers_len), (int)headers_len);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_EQ_GOTO(stream_args.result, 0);
    CHECK_GOTO(stream_args.h3_stream != NULL);
    result = 0;

cleanup:
    if (stream_thread_started) {
        trevrpc_h3_conn_shutdown(fixture.accept_args.h3_conn);
        (void)pthread_join(stream_thread, NULL);
    }
    trevrpc_wt_stream_close(stream_args.wt_stream);
    trevrpc_h3_stream_close(stream_args.h3_stream);
    trevrpc_msquic_stream_close(trigger);
    trevrpc_msquic_stream_close(unknown_unidi);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_webtransport_unidirectional_monitor_shutdown(void) {
    enum { monitor_count = 16, fixed_thread_allowance = 1 };
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* unidi[monitor_count] = {0};
    h3_stream_args stream_args = {0};
    pthread_t stream_thread = {0};
    bool stream_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    const uint8_t partial_prefix[] = {0x54, 0x40};
#if defined(__linux__)
    size_t baseline_threads = 0;
    size_t active_threads = 0;
    size_t shutdown_threads = 0;
#endif

    fixture.transport_config = test_h3_config;
    fixture.transport_config.peer_unidi_stream_count = 32;
    CHECK_GOTO(test_build_draft07_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    stream_args.conn = fixture.accept_args.h3_conn;
    CHECK_GOTO(pthread_create(&stream_thread, NULL, accept_h3_stream_thread, &stream_args) == 0);
    stream_thread_started = true;
#if defined(__linux__)
    CHECK_GOTO(test_process_thread_count(&baseline_threads));
#endif
    for (size_t i = 0; i < monitor_count; i++) {
        CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(fixture.client_conn, &unidi[i]) == 0);
        CHECK_EQ_GOTO(
            trevrpc_msquic_stream_write(unidi[i], partial_prefix, sizeof(partial_prefix)), (int)sizeof(partial_prefix));
    }
    CHECK_GOTO(trevrpc_h3_test_wait_unidi_progress(fixture.accept_args.h3_conn, monitor_count, 0, 5000000000ull) == 0);
#if defined(__linux__)
    CHECK_GOTO(test_process_thread_count(&active_threads));
    CHECK_GOTO(active_threads <= baseline_threads + fixed_thread_allowance);
#endif
    trevrpc_h3_conn_shutdown(fixture.accept_args.h3_conn);
    CHECK_GOTO(pthread_join(stream_thread, NULL) == 0);
    stream_thread_started = false;
    CHECK_GOTO(stream_args.result != 0);
    for (size_t i = 0; i < monitor_count; i++) {
        trevrpc_msquic_stream_close(unidi[i]);
        unidi[i] = NULL;
    }
    test_h3_bound_fixture_cleanup(&fixture);
#if defined(__linux__)
    CHECK_GOTO(test_process_thread_count(&shutdown_threads));
    CHECK_GOTO(shutdown_threads <= baseline_threads + fixed_thread_allowance);
#endif
    result = 0;

cleanup:
    if (stream_thread_started) {
        trevrpc_h3_conn_shutdown(fixture.accept_args.h3_conn);
        (void)pthread_join(stream_thread, NULL);
    }
    for (size_t i = 0; i < monitor_count; i++) {
        trevrpc_msquic_stream_close(unidi[i]);
    }
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int run_http3_control_unknown_budget_case(bool overflow, uint64_t expected_error) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t frames[1100];
    size_t frames_len = 0;

    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(test_append_filled_h3_frame(frames, sizeof(frames), &frames_len, 0x21, 400, 0xa1) == 0);
    CHECK_GOTO(test_append_filled_h3_frame(frames, sizeof(frames), &frames_len, 0x22, 624, 0xb2) == 0);
    if (overflow) {
        CHECK_GOTO(test_varint_write(frames, sizeof(frames), &frames_len, 0x23) == 0);
        CHECK_GOTO(test_varint_write(frames, sizeof(frames), &frames_len, 1) == 0);
    } else {
        CHECK_GOTO(test_append_filled_h3_frame(frames, sizeof(frames), &frames_len, TREV_H3_FRAME_GOAWAY, 0, 0) == 0);
    }
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(fixture.client_control, frames, frames_len), (int)frames_len);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, expected_error) == 0);
    result = 0;

cleanup:
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_http3_control_unknown_discard_budget_bounds(void) {
    if (run_http3_control_unknown_budget_case(false, 0x106) != 0) {
        return 1;
    }
    return run_http3_control_unknown_budget_case(true, 0x107);
}

static int test_http3_exact_settings_headers_and_request_unknown_budget(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* exact_client_stream = NULL;
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_h3_stream* exact_server_stream = NULL;
    trevrpc_h3_stream* server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    uint8_t control[8192];
    size_t control_len = 0;
    uint8_t exact_headers[16];
    size_t exact_headers_len = 0;
    uint8_t request[4096];
    size_t request_len = 0;
    size_t headers_len = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t rpc_request[] = {0, 0, 0, 7, 0x22, 5, 'h', 'e', 'l', 'l', 'o'};

    CHECK_GOTO(test_build_exact_control_settings(control, sizeof(control), &control_len, 4096) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);

    /* An exact-limit prefix must wait for payload instead of failing as excessive load. */
    CHECK_GOTO(test_varint_write(exact_headers, sizeof(exact_headers), &exact_headers_len, TREV_H3_FRAME_HEADERS) == 0);
    CHECK_GOTO(test_varint_write(exact_headers, sizeof(exact_headers), &exact_headers_len, 16 * 1024) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &exact_client_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write(exact_client_stream, exact_headers, exact_headers_len), (int)exact_headers_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &exact_server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HTTP3;
    CHECK_EQ_GOTO(trevrpc_h3_stream_resolve(
                      fixture.accept_args.h3_conn, exact_server_stream, 100000000ull, &wt_stream, &resolution),
        0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HANDLED);
    CHECK_GOTO(wt_stream == NULL);
    trevrpc_h3_stream_close(exact_server_stream);
    exact_server_stream = NULL;
    trevrpc_msquic_stream_close(exact_client_stream);
    exact_client_stream = NULL;

    CHECK_GOTO(test_append_filled_h3_frame(request, sizeof(request), &request_len, 0x21, 400, 0xa1) == 0);
    CHECK_GOTO(test_append_filled_h3_frame(request, sizeof(request), &request_len, 0x41, 1, 0xa2) == 0);
    CHECK_GOTO(test_build_post_headers(request + request_len,
                   sizeof(request) - request_len,
                   &headers_len,
                   "POST",
                   "/rpc",
                   "application/trevrpc") == 0);
    request_len += headers_len;
    CHECK_GOTO(test_append_filled_h3_frame(request, sizeof(request), &request_len, 0x22, 623, 0xb2) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_DATA) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, sizeof(rpc_request)) == 0);
    CHECK_GOTO(sizeof(request) - request_len >= sizeof(rpc_request));
    memcpy(request + request_len, rpc_request, sizeof(rpc_request));
    request_len += sizeof(rpc_request);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x23) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 1) == 0);

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, request, request_len), (int)request_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &server_stream), 0);
    resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(fixture.accept_args.h3_conn, server_stream, 5000000000ull, &wt_stream, &resolution),
        0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_GOTO(wt_stream == NULL);
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(server_stream, &body, &body_len, 7), 1);
    CHECK_GOTO(body_len == 7 && memcmp(body, rpc_request + 4, body_len) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame_timeout(server_stream, &body, &body_len, 7, 5000000000ull), -3107);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x107) == 0);
    result = 0;

cleanup:
    trevrpc_wt_free(body);
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(server_stream);
    trevrpc_h3_stream_close(exact_server_stream);
    trevrpc_msquic_stream_close(client_stream);
    trevrpc_msquic_stream_close(exact_client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_http3_settings_over_limit_rejected_before_payload_read(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    h3_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[16];
    size_t control_len = 0;
    uint8_t server_settings[64];

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client_conn, &server_conn) == 0);
    accept_args.conn = server_conn;
    accept_args.config = (trevrpc_wt_config){
        .path = "/trevrpc",
        .max_streams_per_session = 8,
    };
    accept_args.max_h3_frame_payload = 1024;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_h3_conn_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_varint_write(control, sizeof(control), &control_len, 0x00) == 0);
    CHECK_GOTO(test_varint_write(control, sizeof(control), &control_len, TREV_H3_FRAME_SETTINGS) == 0);
    CHECK_GOTO(test_varint_write(control, sizeof(control), &control_len, 4097) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read_timeout(
                   server_control, server_settings, sizeof(server_settings), 5000000000ull) > 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, TREV_WT_ERR_REJECTED);
    CHECK_GOTO(accept_args.h3_conn == NULL);
    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        (void)pthread_join(accept_thread, NULL);
        server_conn = NULL;
    }
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_http3_headers_over_limit_rejected_before_payload_read(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_h3_stream* server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t request[16];
    size_t request_len = 0;

    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_HEADERS) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 16 * 1024 + 1) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, request, request_len), (int)request_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(fixture.accept_args.h3_conn, server_stream, 5000000000ull, &wt_stream, &resolution),
        -3107);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x107) == 0);
    result = 0;

cleanup:
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_http3_unknown_preamble_over_limit_rejected_before_payload_read(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_h3_stream* server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t request[16];
    size_t request_len = 0;

    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x21) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 1025) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, request, request_len), (int)request_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(fixture.accept_args.h3_conn, server_stream, 5000000000ull, &wt_stream, &resolution),
        -3107);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x107) == 0);
    result = 0;

cleanup:
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_http3_fragmented_data_length_over_limit_closes_connection(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_h3_stream* server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t request[1024];
    size_t request_len = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t rpc_request[] = {0, 0, 0, 7, 0x22, 5, 'h', 'e', 'l', 'l', 'o'};

    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 128) == 0);
    CHECK_GOTO(
        test_build_post_headers(request, sizeof(request), &request_len, "POST", "/rpc", "application/trevrpc") == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_DATA) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 128) == 0);
    CHECK_GOTO(sizeof(request) - request_len >= 128);
    memcpy(request + request_len, rpc_request, sizeof(rpc_request));
    request_len += sizeof(rpc_request);
    request[request_len++] = 0;
    request[request_len++] = 0;
    request[request_len++] = 0;
    request[request_len++] = 113;
    memset(request + request_len, 0x5a, 113);
    request_len += 113;
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_DATA) == 0);
    CHECK_GOTO(test_varint_write_width(request, sizeof(request), &request_len, 129, 2) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, request, request_len - 1), (int)(request_len - 1));
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(fixture.accept_args.h3_conn, server_stream, 5000000000ull, &wt_stream, &resolution),
        0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(server_stream, &body, &body_len, 128), 1);
    CHECK_GOTO(body_len == 7 && memcmp(body, rpc_request + 4, body_len) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(server_stream, &body, &body_len, 128), 1);
    CHECK_GOTO(body_len == 113);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame_ready(server_stream, &body, &body_len, 128), TREV_MSQUIC_ERR_TIMEOUT);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, request + request_len - 1, 1), 1);
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame_timeout(server_stream, &body, &body_len, 128, 5000000000ull), -3107);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x107) == 0);
    result = 0;

cleanup:
    trevrpc_wt_free(body);
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int run_http3_trailer_headers_bound_case(bool over_limit) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_h3_stream* server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t request[1024];
    size_t request_len = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const uint8_t rpc_request[] = {0, 0, 0, 7, 0x22, 5, 'h', 'e', 'l', 'l', 'o'};

    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(
        test_build_post_headers(request, sizeof(request), &request_len, "POST", "/rpc", "application/trevrpc") == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_DATA) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, sizeof(rpc_request)) == 0);
    CHECK_GOTO(sizeof(request) - request_len >= sizeof(rpc_request));
    memcpy(request + request_len, rpc_request, sizeof(rpc_request));
    request_len += sizeof(rpc_request);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_HEADERS) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 16 * 1024 + (over_limit ? 1 : 0)) == 0);
    if (!over_limit) {
        CHECK_GOTO(request_len < sizeof(request));
        request[request_len++] = 0;
    }

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, request, request_len), (int)request_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(fixture.accept_args.h3_conn, server_stream, 5000000000ull, &wt_stream, &resolution),
        0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame(server_stream, &body, &body_len, 7), 1);
    CHECK_GOTO(body_len == 7 && memcmp(body, rpc_request + 4, body_len) == 0);
    trevrpc_wt_free(body);
    body = NULL;
    body_len = 0;
    intptr_t trailer_result = trevrpc_h3_stream_read_frame_ready(server_stream, &body, &body_len, 7);
    if (over_limit) {
        CHECK_EQ_GOTO(trailer_result, -3107);
        CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x107) == 0);
    } else {
        CHECK_EQ_GOTO(trailer_result, TREV_MSQUIC_ERR_TIMEOUT);
    }
    result = 0;

cleanup:
    trevrpc_wt_free(body);
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_http3_oversized_prohibited_frame_reports_unexpected(void) {
    int result = 1;
    h3_bound_fixture fixture = {0};
    trevrpc_msquic_stream* client_stream = NULL;
    trevrpc_h3_stream* server_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t request[1024];
    size_t request_len = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;

    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(test_h3_bound_fixture_open(&fixture, control, control_len, 1024) == 0);
    CHECK_GOTO(
        test_build_post_headers(request, sizeof(request), &request_len, "POST", "/rpc", "application/trevrpc") == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, TREV_H3_FRAME_SETTINGS) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 16 * 1024 + 1) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_stream(fixture.client_conn, &client_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_stream, request, request_len), (int)request_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(fixture.accept_args.h3_conn, &server_stream), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(
        trevrpc_h3_stream_resolve(fixture.accept_args.h3_conn, server_stream, 5000000000ull, &wt_stream, &resolution),
        0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_EQ_GOTO(trevrpc_h3_stream_read_frame_timeout(server_stream, &body, &body_len, 1024, 100000000ull), -3102);
    CHECK_GOTO(test_wait_peer_close_error(fixture.client_conn, 0x105) == 0);
    result = 0;

cleanup:
    trevrpc_wt_free(body);
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(server_stream);
    trevrpc_msquic_stream_close(client_stream);
    test_h3_bound_fixture_cleanup(&fixture);
    return result;
}

static int test_http3_trailer_headers_payload_bounds(void) {
    if (run_http3_trailer_headers_bound_case(false) != 0) {
        return 1;
    }
    return run_http3_trailer_headers_bound_case(true);
}

static int run_http3_connection_error_case(
    const uint8_t* request_bytes, size_t request_bytes_len, uint64_t expected_error) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* request_stream = NULL;
    trevrpc_h3_stream* pending = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    h3_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    const trevrpc_wt_config server_config = {.path = "/trevrpc", .max_streams_per_session = 8};

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client_conn, &server_conn) == 0);
    accept_args.conn = server_conn;
    accept_args.config = server_config;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_h3_conn_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, 0);

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &request_stream) == 0);
    CHECK_EQ_GOTO(
        trevrpc_msquic_stream_write_fin(request_stream, request_bytes, request_bytes_len), (int)request_bytes_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &pending), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_GOTO(trevrpc_h3_stream_resolve(accept_args.h3_conn, pending, 5000000000ull, &wt_stream, &resolution) != 0);
    CHECK_GOTO(test_wait_peer_close_error(client_conn, expected_error) == 0);
    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        (void)pthread_join(accept_thread, NULL);
        server_conn = NULL;
    }
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(pending);
    trevrpc_msquic_stream_close(request_stream);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_http3_qpack_failure_closes_connection(void) {
    const uint8_t malformed_headers[] = {0x01, 0x03, 0x00, 0x00, 0xff};
    return run_http3_connection_error_case(malformed_headers, sizeof(malformed_headers), 0x200);
}

static int test_http3_forbidden_first_frame_closes_connection(void) {
    const uint8_t data_before_headers[] = {0x00, 0x00};
    return run_http3_connection_error_case(data_before_headers, sizeof(data_before_headers), 0x105);
}

static int test_http3_forbidden_trailer_closes_connection(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    trevrpc_msquic_stream* request_stream = NULL;
    trevrpc_h3_stream* pending = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    h3_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    uint8_t request[1024];
    size_t request_len = 0;
    uint8_t trailer_block[128];
    size_t trailer_len = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;
    const trevrpc_wt_config server_config = {.path = "/trevrpc", .max_streams_per_session = 8};

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client_conn, &server_conn) == 0);
    accept_args.conn = server_conn;
    accept_args.config = server_config;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_h3_conn_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, 0);

    CHECK_GOTO(
        test_build_post_headers(request, sizeof(request), &request_len, "POST", "/rpc", "application/trevrpc") == 0);
    trailer_block[trailer_len++] = 0;
    trailer_block[trailer_len++] = 0;
    CHECK_GOTO(
        test_header_block_put_literal(trailer_block, sizeof(trailer_block), &trailer_len, "content-length", "0") == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, 0x01) == 0);
    CHECK_GOTO(test_varint_write(request, sizeof(request), &request_len, trailer_len) == 0);
    CHECK_GOTO(sizeof(request) - request_len >= trailer_len);
    memcpy(request + request_len, trailer_block, trailer_len);
    request_len += trailer_len;

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &request_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(request_stream, request, request_len), (int)request_len);
    CHECK_EQ_GOTO(trevrpc_h3_conn_accept_stream(accept_args.h3_conn, &pending), 0);
    int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    CHECK_EQ_GOTO(trevrpc_h3_stream_resolve(accept_args.h3_conn, pending, 5000000000ull, &wt_stream, &resolution), 0);
    CHECK_EQ_GOTO(resolution, TREV_H3_STREAM_RESOLVED_HTTP3);
    CHECK_GOTO(wt_stream == NULL);
    CHECK_GOTO(trevrpc_h3_stream_read_frame(pending, &body, &body_len, 1024) < 0);
    CHECK_GOTO(test_wait_peer_close_error(client_conn, 0x10e) == 0);
    result = 0;

cleanup:
    trevrpc_wt_free(body);
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        (void)pthread_join(accept_thread, NULL);
        server_conn = NULL;
    }
    trevrpc_wt_stream_close(wt_stream);
    trevrpc_h3_stream_close(pending);
    trevrpc_msquic_stream_close(request_stream);
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

static int test_http3_closed_control_stream_closes_connection(void) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* client_control = NULL;
    h3_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t control[64];
    size_t control_len = 0;
    const trevrpc_wt_config server_config = {.path = "/trevrpc", .max_streams_per_session = 8};

    CHECK_GOTO(connect_pair_with_config(&test_h3_config, &listener, &client_conn, &server_conn) == 0);
    accept_args.conn = server_conn;
    accept_args.config = server_config;
    CHECK_GOTO(pthread_create(&accept_thread, NULL, accept_h3_conn_thread, &accept_args) == 0);
    accept_thread_started = true;
    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_shutdown_send(client_control), 0);
    CHECK_GOTO(test_wait_peer_close_error(client_conn, 0x104) == 0);
    result = 0;

cleanup:
    if (accept_thread_started) {
        trevrpc_msquic_conn_shutdown(server_conn);
        (void)pthread_join(accept_thread, NULL);
        server_conn = NULL;
    }
    trevrpc_msquic_stream_close(client_control);
    trevrpc_msquic_stream_close(server_control);
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
}

typedef struct owned_frame_allocator_state {
    void* allocated;
    void* released;
    void* expected_context;
    size_t release_count;
} owned_frame_allocator_state;

static void* owned_frame_alloc(size_t size, void* context) {
    owned_frame_allocator_state* state = context;
    state->allocated = malloc(size);
    return state->allocated;
}

static void owned_frame_free(void* owner, void* context) {
    owned_frame_allocator_state* state = context;
    state->released = owner;
    state->expected_context = context;
    state->release_count++;
    free(owner);
}

static int test_owned_frame_queue_preserves_allocator_provenance(void) {
    int result = 0;
    static const uint8_t payload[] = {0xa1, 0xb2, 0xc3};
    trevrpc_wire_response_values response = {
        .status = TREVRPC_STATUS_OK,
        .body =
            {
                .data = payload,
                .len = sizeof(payload),
            },
    };
    uint8_t* framed = NULL;
    size_t framed_len = 0;
    trevrpc_owned_bytes owned;
    trevrpc_owned_bytes_init(&owned);
    trevrpc_inbound_response* inbound = NULL;
    trevrpc_body_owner* owner = NULL;
    owned_frame_allocator_state allocator = {0};

    CHECK_GOTO(trevrpc_wire_encode_response(&response, 1024, &framed, &framed_len) == 0);
    CHECK_GOTO(trevrpc_msquic_test_parse_frame_owned(
                   framed, framed_len, 1024, owned_frame_alloc, owned_frame_free, &allocator, &owned) == 0);
    CHECK_GOTO(owned.owner == allocator.allocated);
    CHECK_GOTO(owned.release_context == &allocator);
    CHECK_GOTO(trevrpc_wire_decode_response_owned(&owned, &inbound) == 0);
    CHECK_GOTO(allocator.release_count == 0);

    trevrpc_bytes_view body = {0};
    CHECK_GOTO(trevrpc_inbound_response_get_body(inbound, &body) == 0);
    CHECK_GOTO(body.len == sizeof(payload));
    CHECK_GOTO(memcmp(body.data, payload, sizeof(payload)) == 0);
    CHECK_GOTO(trevrpc_inbound_response_take_body(inbound, &owner) == 0);
    CHECK_GOTO(owner != NULL);
    trevrpc_inbound_response_release(inbound);
    inbound = NULL;
    CHECK_GOTO(allocator.release_count == 0);
    trevrpc_body_owner_release(owner);
    owner = NULL;
    CHECK_GOTO(allocator.release_count == 1);
    CHECK_GOTO(allocator.released == allocator.allocated);
    CHECK_GOTO(allocator.expected_context == &allocator);

cleanup:
    trevrpc_body_owner_release(owner);
    trevrpc_inbound_response_release(inbound);
    trevrpc_owned_bytes_reset(&owned);
    free(framed);
    return result;
}

static trevrpc_msquic_receive_policy test_receive_policy(size_t max_frame_size, size_t connection_count) {
    size_t minimum = trevrpc_msquic_test_receive_minimum_bytes(max_frame_size);
    trevrpc_msquic_receive_policy policy = {
        .max_stream_owned_bytes = minimum,
        .max_stream_owned_count = 3,
        .max_connection_owned_bytes = minimum,
        .max_connection_owned_count = connection_count,
    };
    return policy;
}

static int test_receive_budget_raw_multibuffer_partial_fin_and_cross_stream_resume(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* first = NULL;
    trevrpc_msquic_stream* second = NULL;
    const uint8_t abc[] = {'a', 'b', 'c'};
    const uint8_t def[] = {'d', 'e', 'f'};
    const uint8_t* buffers[] = {abc, def};
    const size_t lengths[] = {sizeof(abc), sizeof(def)};
    uint8_t out[5] = {0};
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    CHECK_GOTO(first != NULL && second != NULL);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, buffers, lengths, 2, true, &accepted) == 0);
    CHECK_GOTO(accepted == 5);
    trevrpc_msquic_test_receive_snapshot_get(first, &snapshot);
    CHECK_GOTO(snapshot.buffered_bytes == 5);
    CHECK_GOTO(snapshot.stream_owned_count == 1);
    CHECK_GOTO(snapshot.paused && snapshot.receive_disabled && !snapshot.recv_fin);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(first, out, 1), 1);
    CHECK_GOTO(out[0] == 'a');

    const uint8_t z = 'z';
    const uint8_t* z_buffers[] = {&z};
    const size_t z_lengths[] = {1};
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, z_buffers, z_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == 0);
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(snapshot.paused && snapshot.receive_disabled);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(first, out + 1, 4), 4);
    CHECK_GOTO(memcmp(out, "abcde", 5) == 0);

    const uint8_t f = 'f';
    const uint8_t* f_buffers[] = {&f};
    const size_t f_lengths[] = {1};
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, f_buffers, f_lengths, 1, true, &accepted) == 0);
    CHECK_GOTO(accepted == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(first, out, 1), 1);
    CHECK_GOTO(out[0] == 'f');
    trevrpc_msquic_test_receive_snapshot_get(first, &snapshot);
    CHECK_GOTO(snapshot.recv_fin);

    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, z_buffers, z_lengths, 1, true, &accepted) == 0);
    CHECK_GOTO(accepted == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(second, out, 1), 1);
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(snapshot.recv_fin && snapshot.stream_owned_bytes == 0 && snapshot.stream_owned_count == 0);
    CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);
    result = 0;

cleanup:
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

static int test_receive_budget_frame_accounting_and_detached_lifetime(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_owned_bytes body;
    trevrpc_owned_bytes_init(&body);
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};
    const uint8_t header[] = {0, 0, 0, 1};
    const uint8_t payload[] = {0x5a};
    const uint8_t* header_buffers[] = {header};
    const size_t header_lengths[] = {sizeof(header)};
    const uint8_t* payload_buffers[] = {payload};
    const size_t payload_lengths[] = {sizeof(payload)};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 1, &fixture) == 0);
    stream = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), TREV_MSQUIC_ERR_TIMEOUT);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(stream, header_buffers, header_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(header));
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.stream_owned_bytes == 1 && snapshot.stream_owned_count == 1);
    CHECK_GOTO(snapshot.queued_frames == 0 && !snapshot.pending_frame);

    CHECK_GOTO(trevrpc_msquic_test_receive_inject(stream, payload_buffers, payload_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(payload));
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.queued_frames == 1 && snapshot.stream_owned_count == 2);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), 1);
    CHECK_GOTO(body.len == 1 && body.data[0] == 0x5a);
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.stream_owned_bytes == 0 && snapshot.stream_owned_count == 0);
    CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);

    trevrpc_msquic_test_receive_fixture_drop_connection(fixture);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    fixture = NULL;
    trevrpc_owned_bytes_reset(&body);
    result = 0;

cleanup:
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

static int test_receive_budget_conversion_zero_length_oversized_and_truncated(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_owned_bytes body;
    trevrpc_owned_bytes_init(&body);
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};
    const uint8_t frame[] = {0, 0, 0, 1, 0x44};
    const uint8_t* frame_buffers[] = {frame};
    const size_t frame_lengths[] = {sizeof(frame)};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 1, &fixture) == 0);
    stream = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(stream, frame_buffers, frame_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(frame));
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.buffered_bytes == sizeof(frame) && snapshot.stream_owned_count == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), 1);
    CHECK_GOTO(body.len == 1 && body.data[0] == 0x44);
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.buffered_bytes == 0 && snapshot.connection_owned_count == 0);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    fixture = NULL;

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 1, &fixture) == 0);
    stream = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), TREV_MSQUIC_ERR_TIMEOUT);
    const uint8_t malformed[] = {0, 0, 0, 2, 0xaa};
    const uint8_t* malformed_buffers[] = {malformed};
    const size_t malformed_lengths[] = {sizeof(malformed)};
    CHECK_GOTO(
        trevrpc_msquic_test_receive_inject(stream, malformed_buffers, malformed_lengths, 1, true, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(malformed));
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.recv_fin && snapshot.queued_frames == 1 && snapshot.stream_owned_count == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), TREV_MSQUIC_ERR_FRAME_TOO_LARGE);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), TREV_MSQUIC_ERR_CLOSED);
    trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
    CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);
    result = 0;

cleanup:
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

static int test_receive_budget_count_pressure_parser_retry_and_zero_frames(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    policy.max_stream_owned_bytes *= 4;
    policy.max_connection_owned_bytes = policy.max_stream_owned_bytes;
    trevrpc_msquic_stream* first = NULL;
    trevrpc_msquic_stream* second = NULL;
    trevrpc_owned_bytes body;
    trevrpc_owned_bytes_init(&body);
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};
    const uint8_t zero_frames[12] = {0};
    const uint8_t* zero_buffers[] = {zero_frames};
    const size_t zero_lengths[] = {sizeof(zero_frames)};
    const uint8_t header[] = {0, 0, 0, 1};
    const uint8_t payload[] = {0x7f};
    const uint8_t* header_buffers[] = {header};
    const size_t header_lengths[] = {sizeof(header)};
    const uint8_t* payload_buffers[] = {payload};
    const size_t payload_lengths[] = {sizeof(payload)};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(first, &body, 1), TREV_MSQUIC_ERR_TIMEOUT);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(second, &body, 1), TREV_MSQUIC_ERR_TIMEOUT);

    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, zero_buffers, zero_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(zero_frames));
    trevrpc_msquic_test_receive_snapshot_get(first, &snapshot);
    CHECK_GOTO(snapshot.queued_frames == 3 && snapshot.connection_owned_count == 3);

    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, header_buffers, header_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(header));
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(snapshot.paused && snapshot.stream_owned_count == 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, payload_buffers, payload_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == 0);
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(snapshot.paused && snapshot.receive_disabled);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(first, &body, 1), 1);
    CHECK_GOTO(body.len == 0);
    trevrpc_owned_bytes_reset(&body);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, payload_buffers, payload_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == sizeof(payload));
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(snapshot.paused && snapshot.pending_frame && snapshot.connection_owned_count == 3);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(first, &body, 1), 1);
    CHECK_GOTO(body.len == 0);
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(!snapshot.pending_frame && snapshot.queued_frames == 1 && snapshot.connection_owned_count == 3);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(second, &body, 1), 1);
    CHECK_GOTO(body.len == 1 && body.data[0] == payload[0]);
    trevrpc_owned_bytes_reset(&body);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(first, &body, 1), 1);
    CHECK_GOTO(body.len == 0);
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_snapshot_get(first, &snapshot);
    CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);
    result = 0;

cleanup:
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

static int test_receive_budget_fifo_fairness_and_close_under_pressure(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* first = NULL;
    trevrpc_msquic_stream* second = NULL;
    trevrpc_msquic_stream* third = NULL;
    const uint8_t initial[] = {'a', 'b', 'c', 'd', 'e'};
    const uint8_t second_byte = 's';
    const uint8_t third_byte = 't';
    const uint8_t* initial_buffers[] = {initial};
    const size_t initial_lengths[] = {sizeof(initial)};
    const uint8_t* second_buffers[] = {&second_byte};
    const size_t second_lengths[] = {1};
    const uint8_t* third_buffers[] = {&third_byte};
    const size_t third_lengths[] = {1};
    uint8_t out[8] = {0};
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 3, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    third = trevrpc_msquic_test_receive_fixture_stream(fixture, 2);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, initial_buffers, initial_lengths, 1, false, &accepted) == 0);
    CHECK_GOTO(accepted == 5);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, second_buffers, second_lengths, 1, false, &accepted) == 0 &&
               accepted == 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(third, third_buffers, third_lengths, 1, false, &accepted) == 0 &&
               accepted == 0);
    trevrpc_msquic_test_receive_snapshot_get(third, &snapshot);
    CHECK_GOTO(snapshot.resume_scan_count == 0);

    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(first, out, 5), 5);
    trevrpc_msquic_test_receive_snapshot_get(third, &snapshot);
    CHECK_GOTO(snapshot.resume_scan_count > 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(third, third_buffers, third_lengths, 1, false, &accepted) == 0);
    CHECK_EQ_GOTO((int)accepted, 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, second_buffers, second_lengths, 1, false, &accepted) == 0);
    CHECK_EQ_GOTO((int)accepted, 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(second, out, 1), 1);
    CHECK_GOTO(out[0] == second_byte);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(third, third_buffers, third_lengths, 1, false, &accepted) == 0 &&
               accepted == 1);
    trevrpc_msquic_test_receive_snapshot_get(third, &snapshot);
    CHECK_GOTO(snapshot.stream_owned_count == 1 && snapshot.connection_owned_count == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(third, out, 1), 1);
    CHECK_GOTO(out[0] == third_byte);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    fixture = NULL;

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, initial_buffers, initial_lengths, 1, false, &accepted) == 0 &&
               accepted == 5);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, second_buffers, second_lengths, 1, false, &accepted) == 0 &&
               accepted == 0);
    trevrpc_msquic_test_receive_fixture_drop_connection(fixture);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    fixture = NULL;
    result = 0;

cleanup:
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

static int test_receive_budget_terminal_races_pinned_resume(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* first = NULL;
    trevrpc_msquic_stream* closing = NULL;
    stream_hook_state hook = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .block_event = TREV_MSQUIC_TEST_STREAM_RECV_RESUME_PINNED,
    };
    stream_observer_operation close = {0};
    bool close_initialized = false;
    pthread_t read_thread = {0};
    pthread_t close_thread = {0};
    bool read_thread_started = false;
    bool close_thread_started = false;
    const uint8_t initial[] = {'a', 'b', 'c', 'd', 'e'};
    const uint8_t waiting_byte = 'z';
    const uint8_t* initial_buffers[] = {initial};
    const size_t initial_lengths[] = {sizeof(initial)};
    const uint8_t* waiting_buffers[] = {&waiting_byte};
    const size_t waiting_lengths[] = {1};
    uint8_t out[sizeof(initial)] = {0};
    stream_read_args read = {.data = out, .len = sizeof(out)};
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    closing = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    CHECK_GOTO(first != NULL && closing != NULL);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, initial_buffers, initial_lengths, 1, false, &accepted) == 0 &&
               accepted == sizeof(initial));
    CHECK_GOTO(
        trevrpc_msquic_test_receive_inject(closing, waiting_buffers, waiting_lengths, 1, false, &accepted) == 0 &&
        accepted == 0);

    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);
    read.stream = first;
    CHECK_GOTO(pthread_create(&read_thread, NULL, stream_read_thread, &read) == 0);
    read_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_RECV_RESUME_PINNED);

    CHECK_GOTO(test_stream_observer_operation_init(&close, closing) == 0);
    close_initialized = true;
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_observer_close_thread, &close) == 0);
    close_thread_started = true;
    test_stream_observer_operation_wait_started(&close);
    CHECK_GOTO(!test_stream_observer_operation_completed(&close));

    test_stream_hook_release(&hook);
    CHECK_GOTO(pthread_join(read_thread, NULL) == 0);
    read_thread_started = false;
    CHECK_GOTO(read.result == (intptr_t)sizeof(initial));
    CHECK_GOTO(memcmp(out, initial, sizeof(initial)) == 0);
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    closing = NULL;
    CHECK_GOTO(test_stream_observer_operation_completed(&close));

    trevrpc_msquic_test_receive_snapshot_get(first, &snapshot);
    CHECK_GOTO(snapshot.stream_owned_bytes == 0 && snapshot.stream_owned_count == 0);
    CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);
    CHECK_GOTO(snapshot.buffered_bytes == 0 && snapshot.queued_frames == 0);
    CHECK_GOTO(snapshot.active_resume_pins == 0 && !snapshot.paused && !snapshot.pending_frame);
    result = 0;

cleanup:
    test_stream_hook_release(&hook);
    if (read_thread_started) {
        (void)pthread_join(read_thread, NULL);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        closing = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_stream_close(closing);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    if (close_initialized) {
        test_stream_observer_operation_destroy(&close);
    }
    pthread_cond_destroy(&hook.cond);
    pthread_mutex_destroy(&hook.mutex);
    return result;
}

static int test_receive_read_close_waits_for_post_unlock_progress(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* reader = NULL;
    trevrpc_msquic_stream* waiting = NULL;
    stream_hook_state hook = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .block_event = TREV_MSQUIC_TEST_STREAM_RECV_RESUME_PINNED,
    };
    stream_observer_operation close = {0};
    bool close_initialized = false;
    pthread_t read_thread = {0};
    pthread_t close_thread = {0};
    bool read_thread_started = false;
    bool close_thread_started = false;
    const uint8_t initial[] = {'a', 'b', 'c', 'd', 'e'};
    const uint8_t waiting_byte = 'z';
    const uint8_t* initial_buffers[] = {initial};
    const size_t initial_lengths[] = {sizeof(initial)};
    const uint8_t* waiting_buffers[] = {&waiting_byte};
    const size_t waiting_lengths[] = {1};
    uint8_t out[sizeof(initial)] = {0};
    stream_read_args read = {.data = out, .len = sizeof(out)};
    size_t accepted = 0;

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    reader = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    waiting = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    CHECK_GOTO(reader != NULL && waiting != NULL);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(reader, initial_buffers, initial_lengths, 1, false, &accepted) == 0 &&
               accepted == sizeof(initial));
    CHECK_GOTO(
        trevrpc_msquic_test_receive_inject(waiting, waiting_buffers, waiting_lengths, 1, false, &accepted) == 0 &&
        accepted == 0);

    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);
    read.stream = reader;
    CHECK_GOTO(pthread_create(&read_thread, NULL, stream_read_thread, &read) == 0);
    read_thread_started = true;
    test_stream_hook_wait(&hook, TREV_MSQUIC_TEST_STREAM_RECV_RESUME_PINNED);

    CHECK_GOTO(test_stream_observer_operation_init(&close, reader) == 0);
    close_initialized = true;
    CHECK_GOTO(pthread_create(&close_thread, NULL, stream_observer_close_thread, &close) == 0);
    close_thread_started = true;
    test_stream_observer_operation_wait_started(&close);
    CHECK_GOTO(!test_stream_observer_operation_completed(&close));

    test_stream_hook_release(&hook);
    CHECK_GOTO(pthread_join(read_thread, NULL) == 0);
    read_thread_started = false;
    CHECK_GOTO(read.result == (intptr_t)sizeof(initial));
    CHECK_GOTO(memcmp(out, initial, sizeof(initial)) == 0);
    CHECK_GOTO(pthread_join(close_thread, NULL) == 0);
    close_thread_started = false;
    reader = NULL;
    CHECK_GOTO(test_stream_observer_operation_completed(&close));
    result = 0;

cleanup:
    test_stream_hook_release(&hook);
    if (read_thread_started) {
        (void)pthread_join(read_thread, NULL);
    }
    if (close_thread_started) {
        (void)pthread_join(close_thread, NULL);
        reader = NULL;
    }
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_stream_close(reader);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    if (close_initialized) {
        test_stream_observer_operation_destroy(&close);
    }
    pthread_cond_destroy(&hook.cond);
    pthread_mutex_destroy(&hook.mutex);
    return result;
}

static int test_receive_mode_switch_and_error_credit_wakeup(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* first = NULL;
    trevrpc_msquic_stream* second = NULL;
    trevrpc_owned_bytes body;
    trevrpc_owned_bytes_init(&body);
    const uint8_t raw[] = {'a', 'b', 'c', 'd', 'e'};
    const uint8_t z = 'z';
    const uint8_t frame[] = {0, 0, 0, 1, 0x44};
    const uint8_t parser_header[] = {0, 0, 0, 1};
    const uint8_t* raw_buffers[] = {raw};
    const size_t raw_lengths[] = {sizeof(raw)};
    const uint8_t* z_buffers[] = {&z};
    const size_t z_lengths[] = {1};
    const uint8_t* frame_buffers[] = {frame};
    const size_t frame_lengths[] = {sizeof(frame)};
    const uint8_t* header_buffers[] = {parser_header};
    const size_t header_lengths[] = {sizeof(parser_header)};
    uint8_t out[sizeof(raw)] = {0};
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, raw_buffers, raw_lengths, 1, false, &accepted) == 0 &&
               accepted == sizeof(raw));
    CHECK_GOTO(
        trevrpc_msquic_test_receive_inject(second, z_buffers, z_lengths, 1, false, &accepted) == 0 && accepted == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(first, out, sizeof(out)), (intptr_t)sizeof(out));
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(!snapshot.paused && snapshot.stream_owned_count == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(second, &body, 1), TREV_MSQUIC_ERR_TIMEOUT);
    trevrpc_msquic_test_receive_snapshot_get(second, &snapshot);
    CHECK_GOTO(!snapshot.paused && snapshot.stream_owned_bytes == 0 && snapshot.connection_owned_bytes == 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(second, frame_buffers, frame_lengths, 1, false, &accepted) == 0 &&
               accepted == sizeof(frame));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(second, &body, 1), 1);
    CHECK_GOTO(body.len == 1 && body.data[0] == 0x44);
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    fixture = NULL;

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, header_buffers, header_lengths, 1, false, &accepted) == 0 &&
               accepted == sizeof(parser_header));
    CHECK_GOTO(
        trevrpc_msquic_test_receive_inject(second, z_buffers, z_lengths, 1, false, &accepted) == 0 && accepted == 0);
    trevrpc_msquic_test_fail_next_receive_parser_allocation();
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(first, &body, 1), -ENOMEM);
    CHECK_GOTO(
        trevrpc_msquic_test_receive_inject(second, z_buffers, z_lengths, 1, false, &accepted) == 0 && accepted == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(second, out, 1), 1);
    CHECK_GOTO(out[0] == z);
    result = 0;

cleanup:
    trevrpc_owned_bytes_reset(&body);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

static int test_receive_handle_loss_releases_grant(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_msquic_stream* first = NULL;
    trevrpc_msquic_stream* lost = NULL;
    trevrpc_msquic_stream* next = NULL;
    const uint8_t raw[] = {'a', 'b', 'c', 'd', 'e'};
    const uint8_t lost_byte = 'l';
    const uint8_t next_byte = 'n';
    const uint8_t* raw_buffers[] = {raw};
    const size_t raw_lengths[] = {sizeof(raw)};
    const uint8_t* lost_buffers[] = {&lost_byte};
    const size_t lost_lengths[] = {1};
    const uint8_t* next_buffers[] = {&next_byte};
    const size_t next_lengths[] = {1};
    uint8_t out[sizeof(raw)] = {0};
    size_t accepted = 0;
    trevrpc_msquic_test_receive_snapshot snapshot = {0};

    CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 3, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
    lost = trevrpc_msquic_test_receive_fixture_stream(fixture, 1);
    next = trevrpc_msquic_test_receive_fixture_stream(fixture, 2);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(first, raw_buffers, raw_lengths, 1, false, &accepted) == 0 &&
               accepted == sizeof(raw));
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(lost, lost_buffers, lost_lengths, 1, false, &accepted) == 0 &&
               accepted == 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(next, next_buffers, next_lengths, 1, false, &accepted) == 0 &&
               accepted == 0);
    trevrpc_msquic_test_receive_simulate_handle_loss(lost);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(first, out, sizeof(out)), (intptr_t)sizeof(out));
    trevrpc_msquic_test_receive_snapshot_get(lost, &snapshot);
    CHECK_GOTO(snapshot.stream_owned_bytes == 0 && snapshot.stream_owned_count == 0);
    CHECK_GOTO(trevrpc_msquic_test_receive_inject(next, next_buffers, next_lengths, 1, false, &accepted) == 0 &&
               accepted == 1);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_ready(next, out, 1), 1);
    CHECK_GOTO(out[0] == next_byte);
    result = 0;

cleanup:
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    return result;
}

typedef void (*receive_allocation_fail_fn)(void);

static int test_receive_fatal_allocation_shutdown(void) {
    int result = 1;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy policy = test_receive_policy(1, 3);
    trevrpc_owned_bytes body;
    trevrpc_owned_bytes_init(&body);
    stream_hook_state hook = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .block_event = TREV_MSQUIC_TEST_STREAM_EVENT_COUNT,
    };
    const uint8_t raw[] = {0x7a};
    const uint8_t parser_header[] = {0, 0, 0, 1};
    const uint8_t empty_frame[] = {0, 0, 0, 0};
    const uint8_t* const inputs[] = {raw, parser_header, empty_frame};
    const size_t lengths[] = {sizeof(raw), sizeof(parser_header), sizeof(empty_frame)};
    const size_t expected_accepted[] = {0, sizeof(parser_header), sizeof(empty_frame)};
    const bool frame_mode[] = {false, true, true};
    receive_allocation_fail_fn fail[] = {
        trevrpc_msquic_test_fail_next_receive_raw_allocation,
        trevrpc_msquic_test_fail_next_receive_parser_allocation,
        trevrpc_msquic_test_fail_next_receive_frame_allocation,
    };

    trevrpc_msquic_test_set_stream_hook(test_stream_hook, &hook);
    for (size_t index = 0; index < sizeof(fail) / sizeof(fail[0]); index++) {
        trevrpc_msquic_stream* stream = NULL;
        size_t accepted = 0;
        trevrpc_msquic_test_receive_snapshot snapshot = {0};
        pthread_mutex_lock(&hook.mutex);
        hook.seen[TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE] = false;
        pthread_mutex_unlock(&hook.mutex);

        CHECK_GOTO(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 1, &fixture) == 0);
        stream = trevrpc_msquic_test_receive_fixture_stream(fixture, 0);
        CHECK_GOTO(stream != NULL);
        if (frame_mode[index]) {
            CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_owned_ready(stream, &body, 1), TREV_MSQUIC_ERR_TIMEOUT);
        }
        fail[index]();
        const uint8_t* buffers[] = {inputs[index]};
        const size_t buffer_lengths[] = {lengths[index]};
        CHECK_GOTO(trevrpc_msquic_test_receive_inject(stream, buffers, buffer_lengths, 1, false, &accepted) == 0);
        CHECK_GOTO(accepted == expected_accepted[index]);
        trevrpc_msquic_test_receive_snapshot_get(stream, &snapshot);
        CHECK_GOTO(snapshot.err == ENOMEM);
        CHECK_GOTO(snapshot.stream_owned_bytes == 0 && snapshot.stream_owned_count == 0);
        CHECK_GOTO(snapshot.connection_owned_bytes == 0 && snapshot.connection_owned_count == 0);
        pthread_mutex_lock(&hook.mutex);
        bool aborted = hook.seen[TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE];
        pthread_mutex_unlock(&hook.mutex);
        CHECK_GOTO(aborted);
        trevrpc_msquic_test_receive_fixture_destroy(fixture);
        fixture = NULL;
    }
    result = 0;

cleanup:
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_owned_bytes_reset(&body);
    pthread_cond_destroy(&hook.cond);
    pthread_mutex_destroy(&hook.mutex);
    return result;
}

static int test_receive_read_argument_validation(void) {
    uint8_t* body = NULL;
    size_t len = 0;
    trevrpc_owned_bytes owned;
    trevrpc_owned_bytes_init(&owned);
    return trevrpc_msquic_stream_read_frame(NULL, &body, &len, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame_ready(NULL, &body, &len, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame_timeout(NULL, &body, &len, 1, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame(NULL, NULL, &len, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame(NULL, &body, NULL, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame_owned(NULL, &owned, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame_owned_ready(NULL, &owned, 1) == -EINVAL &&
                   trevrpc_msquic_stream_read_frame_owned_timeout(NULL, &owned, 1, 1) == -EINVAL
               ? 0
               : 1;
}

static int test_receive_policy_validation(void) {
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_receive_policy too_small = {
        .max_stream_owned_bytes = 1,
        .max_stream_owned_count = 3,
        .max_connection_owned_bytes = 1,
        .max_connection_owned_count = 3,
    };
    if (trevrpc_msquic_test_receive_fixture_create(&too_small, 1, 1, &fixture) != -EINVAL || fixture != NULL) {
        trevrpc_msquic_test_receive_fixture_destroy(fixture);
        return 1;
    }
    return trevrpc_msquic_test_receive_fixture_create(NULL, SIZE_MAX, 1, &fixture) == -EOVERFLOW && fixture == NULL ? 0
                                                                                                                    : 1;
}

int main(void) {
    int result = 1;
    if (test_webtransport_unidirectional_monitor_slots_reused() != 0) {
        goto cleanup;
    }
    if (test_webtransport_unidirectional_monitor_shutdown() != 0) {
        goto cleanup;
    }
    if (test_receive_policy_validation() != 0) {
        goto cleanup;
    }
    if (test_receive_read_argument_validation() != 0) {
        goto cleanup;
    }
    if (test_receive_mode_switch_and_error_credit_wakeup() != 0) {
        goto cleanup;
    }
    if (test_receive_handle_loss_releases_grant() != 0) {
        goto cleanup;
    }
    if (test_receive_fatal_allocation_shutdown() != 0) {
        goto cleanup;
    }
    if (test_receive_budget_raw_multibuffer_partial_fin_and_cross_stream_resume() != 0) {
        goto cleanup;
    }
    if (test_receive_budget_frame_accounting_and_detached_lifetime() != 0) {
        goto cleanup;
    }
    if (test_receive_budget_conversion_zero_length_oversized_and_truncated() != 0) {
        goto cleanup;
    }
    if (test_receive_budget_count_pressure_parser_retry_and_zero_frames() != 0) {
        goto cleanup;
    }
    if (test_receive_budget_fifo_fairness_and_close_under_pressure() != 0) {
        goto cleanup;
    }
    if (test_receive_budget_terminal_races_pinned_resume() != 0) {
        goto cleanup;
    }
    if (test_receive_read_close_waits_for_post_unlock_progress() != 0) {
        goto cleanup;
    }
    if (test_owned_frame_queue_preserves_allocator_provenance() != 0) {
        goto cleanup;
    }
    if (test_listener_shutdown_is_concurrent_and_idempotent() != 0) {
        goto cleanup;
    }
    if (test_stream_receive_codec_selection_is_immutable() != 0) {
        goto cleanup;
    }
    if (test_stream_observer_readable_reinstall_self_clear_and_terminal() != 0) {
        goto cleanup;
    }
    if (test_stream_observer_clear_and_drain_blocked_callback() != 0) {
        goto cleanup;
    }
    if (test_stream_wait_calls_pin_close_lifetime() != 0) {
        goto cleanup;
    }
    if (test_stream_observer_close_waits_for_blocked_callback() != 0) {
        goto cleanup;
    }
    if (test_stream_observer_self_close() != 0) {
        goto cleanup;
    }
    if (test_raw_client_terminal_observer_self_close() != 0) {
        goto cleanup;
    }
    if (test_conn_observer_self_close_with_external_close() != 0) {
        goto cleanup;
    }
    if (test_peer_send_abort_preserves_local_send_direction() != 0) {
        goto cleanup;
    }
    if (test_stream_reset_unblocks_peer_read() != 0) {
        goto cleanup;
    }
    if (test_stream_write_fin_close_preserves_peer_eof() != 0) {
        goto cleanup;
    }
    if (test_empty_stream_finalization_is_graceful_and_idempotent() != 0) {
        goto cleanup;
    }
    if (test_native_accepted_stream_starts_in_frame_mode() != 0) {
        goto cleanup;
    }
    if (test_native_partial_header_remains_closed_error() != 0) {
        goto cleanup;
    }
    if (test_native_partial_body_remains_closed_error() != 0) {
        goto cleanup;
    }
    if (test_native_oversized_frame_remains_frame_too_large() != 0) {
        goto cleanup;
    }
    if (test_native_malformed_stream_frame_remains_invalid_frame() != 0) {
        goto cleanup;
    }
    if (test_pending_send_final_failure_rolls_back_send_closed() != 0) {
        goto cleanup;
    }
    if (test_tracked_borrowed_send_failure_returns_no_completion() != 0) {
        goto cleanup;
    }
    if (test_pending_send_slow_reader_is_bounded_and_close_drains() != 0) {
        goto cleanup;
    }
    if (test_stream_send_preparation_close_lifetime() != 0) {
        goto cleanup;
    }
    if (test_failed_stream_finalization_close_aborts() != 0) {
        goto cleanup;
    }
    if (test_blocked_capacity_waiter_close_and_abort() != 0) {
        goto cleanup;
    }
    if (test_stream_send_guard_error_states() != 0) {
        goto cleanup;
    }
    if (test_buffering_profile_settings_connect_and_transfer() != 0) {
        goto cleanup;
    }
    if (test_invalid_execution_profile_is_rejected() != 0) {
        goto cleanup;
    }
    if (test_frame_parts_borrowed_body_close_drains_send_complete() != 0) {
        goto cleanup;
    }
    if (test_successful_send_completion_is_not_canceled_by_later_abort() != 0) {
        goto cleanup;
    }
    if (test_frame_parts_borrowed_body_reset_drains_send_complete() != 0) {
        goto cleanup;
    }
    if (test_stream_borrowed_message_wait_drains_send_complete() != 0) {
        goto cleanup;
    }
    if (test_copy_wait_and_terminal_status_with_one_pending_send() != 0) {
        goto cleanup;
    }
    if (test_stream_borrowed_message_batch_wait_drains_send_complete() != 0) {
        goto cleanup;
    }
    if (test_stream_message_batch_submission_failure_rolls_back_accounting() != 0) {
        goto cleanup;
    }
    if (test_stream_single_message_submission_failure_allows_exact_retry() != 0) {
        goto cleanup;
    }
    if (test_client_close_unblocks_server_accept_stream() != 0) {
        goto cleanup;
    }
    if (test_webtransport_transport_requirements_are_negotiated() != 0) {
        goto cleanup;
    }
    if (test_webtransport_connects_h3_quic_session() != 0) {
        goto cleanup;
    }
    if (test_webtransport_session_churn_has_bounded_threads() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_invalid_response_headers() != 0) {
        goto cleanup;
    }
    if (test_webtransport_server_settings_do_not_wait_for_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_stream_prelude_remains_byte_oriented() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_path_mismatch() != 0) {
        goto cleanup;
    }
    if (test_webtransport_accepts_draft02_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_accepts_draft07_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_accepts_draft14_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_accepts_draft15_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_accepts_draft15_legacy_connect_token() != 0) {
        goto cleanup;
    }
    if (test_webtransport_accepts_webkit_hybrid_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_does_not_downgrade_near_webkit_fingerprint() != 0) {
        goto cleanup;
    }
    if (test_combined_http3_accepts_webkit_hybrid_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_h3_control_and_connect_remain_byte_oriented() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_malformed_control_stream_type() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_bidirectional_control_stream() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_missing_webtransport_setting() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_malformed_settings_payload() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_duplicate_settings() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_malformed_qpack_block() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_missing_connect_pseudo_header() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_invalid_connect_method() != 0) {
        goto cleanup;
    }
    if (test_webtransport_listener_shutdown_unblocks_accept() != 0) {
        goto cleanup;
    }
    if (test_webtransport_session_shutdown_unblocks_accept_stream() != 0) {
        goto cleanup;
    }
    if (test_webtransport_stream_close_unblocks_peer_read() != 0) {
        goto cleanup;
    }
    if (test_http3_post_data_adapter_and_request_local_rejection() != 0) {
        goto cleanup;
    }
    if (test_webtransport_unidirectional_stream_rejects_mismatched_session() != 0) {
        goto cleanup;
    }
    if (test_webtransport_unidirectional_stream_times_out_incomplete_session_id() != 0) {
        goto cleanup;
    }
    if (test_http3_control_unknown_discard_budget_bounds() != 0) {
        goto cleanup;
    }
    if (test_http3_exact_settings_headers_and_request_unknown_budget() != 0) {
        goto cleanup;
    }
    if (test_http3_settings_over_limit_rejected_before_payload_read() != 0) {
        goto cleanup;
    }
    if (test_http3_headers_over_limit_rejected_before_payload_read() != 0) {
        goto cleanup;
    }
    if (test_http3_unknown_preamble_over_limit_rejected_before_payload_read() != 0) {
        goto cleanup;
    }
    if (test_http3_fragmented_data_length_over_limit_closes_connection() != 0) {
        goto cleanup;
    }
    if (test_http3_oversized_prohibited_frame_reports_unexpected() != 0) {
        goto cleanup;
    }
    if (test_http3_trailer_headers_payload_bounds() != 0) {
        goto cleanup;
    }
    if (test_http3_qpack_failure_closes_connection() != 0) {
        goto cleanup;
    }
    if (test_http3_forbidden_first_frame_closes_connection() != 0) {
        goto cleanup;
    }
    if (test_http3_forbidden_trailer_closes_connection() != 0) {
        goto cleanup;
    }
    if (test_http3_closed_control_stream_closes_connection() != 0) {
        goto cleanup;
    }
    result = 0;

cleanup:
    return result;
}
