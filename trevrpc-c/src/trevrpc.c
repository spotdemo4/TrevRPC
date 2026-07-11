#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"

#include "trevrpc_msquic.h"
#include "trevrpc_raw.h"
#include "trevrpc_runtime_internal.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREVRPC_NANOS_PER_SEC 1000000000ull

#define TREVRPC_STREAM_LIMIT_DISABLED (-1)
#define TREVRPC_H3_ALPN "h3"
#define TREVRPC_H3_DEFAULT_UNIDI_STREAMS 16
#define TREVRPC_DEFAULT_WORKER_COUNT 16
#define TREVRPC_DEFAULT_WORKER_QUEUE_CAPACITY 1024
#define TREVRPC_MAX_WORKER_COUNT 1024
#define TREVRPC_MAX_WORKER_QUEUE_CAPACITY 65536

typedef struct trevrpc_method trevrpc_method;
typedef struct trevrpc_server_conn_ref trevrpc_server_conn_ref;
typedef struct trevrpc_conn_stream_limiter trevrpc_conn_stream_limiter;
typedef struct trevrpc_stream_task trevrpc_stream_task;

struct trevrpc_raw_client {
    uint32_t transport;
    trevrpc_msquic_conn* msquic_conn;
    trevrpc_wt_session* wt_session;
    size_t max_frame_size;
};

struct trevrpc_call_context {
    trevrpc_server* server;
    bool has_deadline;
    struct timespec deadline;
};

struct trevrpc_cancellation {
    pthread_mutex_t mutex;
    uint32_t transport;
    trevrpc_msquic_stream* msquic_stream;
    trevrpc_wt_stream* wt_stream;
    bool cancelled;
};

struct trevrpc_method {
    trevrpc_method* next;
    char* service;
    size_t service_len;
    char* method;
    size_t method_len;
    uint32_t kind;
    trevrpc_unary_handler handler;
    trevrpc_stream_handler stream_handler;
    trevrpc_call_handler call_handler;
    void* user_data;
};

struct trevrpc_stream {
    uint32_t transport;
    trevrpc_msquic_stream* msquic_stream;
    trevrpc_wt_stream* wt_stream;
    trevrpc_h3_stream* h3_stream;
    const trevrpc_call_context* context;
    size_t max_frame_size;
    bool owns_stream;
    bool sent_status;
    bool status_queued;
    uint32_t terminal_status;
    int64_t max_stream_messages;
    int64_t max_stream_body_size;
    bool has_recv_limits;
    int64_t max_recv_stream_messages;
    int64_t max_recv_stream_body_size;
    uint64_t stream_idle_timeout_nanos;
    int64_t request_message_count;
    int64_t response_message_count;
    uint64_t request_body_size;
    uint64_t response_body_size;
    bool response_idle_started;
    struct timespec response_last_activity;
    bool request_poll_idle_started;
    struct timespec request_poll_started_at;
    uint32_t failure_status;
    const char* failure_message;
    void (*release)(void* context);
    void* release_context;
};

struct trevrpc_call {
    trevrpc_server* server;
    trevrpc_stream stream;
    trevrpc_request request;
    trevrpc_call_context context;
    struct timespec started_at;
    uint8_t* request_frame_body;
    trevrpc_conn_stream_limiter* stream_limiter;
    pthread_mutex_t mutex;
    size_t refs;
    size_t response_body_len;
    uint32_t final_status;
    bool deferred;
    bool completed;
    bool server_task_active;
    bool stream_limiter_active;
    bool close_stream_active;
};

struct trevrpc_server_conn_ref {
    trevrpc_server_conn_ref* next;
    trevrpc_msquic_conn* conn;
    trevrpc_wt_session* wt_session;
    trevrpc_h3_conn* h3_conn;
};

struct trevrpc_server {
    trevrpc_msquic_listener* listener;
    trevrpc_wt_listener* wt_listener;
    trevrpc_msquic_listener* shared_listener;
    trevrpc_wt_config shared_wt_config;
    char* shared_wt_path;
    char* shared_wt_origin;
    char* shared_h3_path;
    int enable_http3;
    trevrpc_http3_admission http3_admission;
    void* http3_admission_user_data;
    size_t max_frame_size;
    trevrpc_server_options options;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_method* methods;
    trevrpc_server_conn_ref* conns;
    trevrpc_authorizer authorizer;
    void* authorizer_user_data;
    trevrpc_metrics metrics;
    trevrpc_transport_observer transport_observer;
    trevrpc_logger logger;
    pthread_t* worker_threads;
    size_t worker_count;
    size_t worker_queue_capacity;
    trevrpc_stream_task* worker_queue_head;
    trevrpc_stream_task* worker_queue_tail;
    size_t worker_queue_len;
#ifdef TREVRPC_TESTING
    uint32_t test_last_stream_status;
    size_t test_stream_status_count;
#endif
    size_t active_tasks;
    size_t active_connections;
    size_t active_requests;
    atomic_bool routes_frozen;
    bool shutting_down;
    bool worker_pool_started;
    bool worker_pool_stopping;
};

struct trevrpc_conn_stream_limiter {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t active_streams;
    size_t refs;
};

typedef enum trevrpc_worker_queue_result {
    TREV_WORKER_QUEUE_ENQUEUED = 0,
    TREV_WORKER_QUEUE_FULL = 1,
    TREV_WORKER_QUEUE_CLOSED = 2,
} trevrpc_worker_queue_result;

typedef struct trevrpc_conn_task {
    trevrpc_server* server;
    trevrpc_msquic_conn* conn;
    trevrpc_wt_session* wt_session;
    trevrpc_h3_conn* h3_conn;
} trevrpc_conn_task;

typedef struct trevrpc_accept_task {
    trevrpc_server* server;
    uint32_t transport;
    int result;
} trevrpc_accept_task;

typedef struct trevrpc_stream_task {
    struct trevrpc_stream_task* next;
    trevrpc_server* server;
    trevrpc_msquic_stream* stream;
    trevrpc_wt_stream* wt_stream;
    trevrpc_h3_stream* h3_stream;
    trevrpc_h3_conn* h3_conn;
    trevrpc_conn_stream_limiter* stream_limiter;
    struct timespec accepted_at;
} trevrpc_stream_task;

static bool trevrpc_server_is_shutting_down(trevrpc_server* server);
static void trevrpc_server_freeze_routes(trevrpc_server* server);
static bool trevrpc_handle_stream(trevrpc_server* server,
    trevrpc_stream* stream,
    trevrpc_conn_stream_limiter* stream_limiter,
    bool server_task_active,
    const struct timespec* accepted_at);
static uint32_t trevrpc_status_from_error(int err, const char** message);
static uint32_t trevrpc_transport_status_from_error(int err, const char** message);
static uint32_t trevrpc_transport_event_transport(trevrpc_server* server);
static void trevrpc_transport_record_event_for_transport(
    trevrpc_server* server, uint32_t kind, uint32_t transport, int error_code, const char* message);
static void trevrpc_conn_stream_limiter_release(trevrpc_conn_stream_limiter* limiter);

static size_t trevrpc_effective_max_frame_size(size_t max_frame_size) {
    if (max_frame_size > 0) {
        return max_frame_size;
    }

    return TREVRPC_DEFAULT_MAX_FRAME_SIZE;
}

static size_t trevrpc_config_max_frame_size(const trevrpc_config* config) {
    return trevrpc_effective_max_frame_size(config == NULL ? 0 : config->max_frame_size);
}

static trevrpc_msquic_config trevrpc_make_msquic_config(const trevrpc_config* config) {
    trevrpc_config defaults = trevrpc_default_config();
    if (config == NULL) {
        config = &defaults;
    }

    trevrpc_msquic_config msquic_config = {0};
    msquic_config.alpn = TREVRPC_ALPN;
    msquic_config.alpn_len = (uint32_t)(sizeof(TREVRPC_ALPN) - 1);
    msquic_config.cert_file = config->cert_file;
    msquic_config.key_file = config->key_file;
    msquic_config.ca_cert_file = config->ca_cert_file;
    msquic_config.skip_certificate_validation = config->skip_certificate_validation;
    msquic_config.max_idle_timeout_ms = config->max_idle_timeout_ms;
    msquic_config.keep_alive_ms = config->keep_alive_ms;
    msquic_config.peer_bidi_stream_count = config->peer_bidi_stream_count;
    msquic_config.max_stateless_operations = config->max_stateless_operations;
    msquic_config.max_binding_stateless_operations = config->max_binding_stateless_operations;
    msquic_config.stream_recv_window = config->stream_recv_window;
    msquic_config.conn_flow_control_window = config->conn_flow_control_window;
    msquic_config.execution_profile = config->msquic_execution_profile;
    msquic_config.send_buffering_enabled = config->msquic_send_buffering_enabled;
    msquic_config.max_pending_send_bytes = config->max_pending_send_bytes;
    msquic_config.max_pending_send_count = config->max_pending_send_count;
    msquic_config.max_frame_size = config->max_frame_size;
    return msquic_config;
}

static trevrpc_server_config trevrpc_effective_server_config(const trevrpc_server_config* config) {
    trevrpc_server_config effective = trevrpc_default_server_config();
    if (config == NULL) {
        return effective;
    }

    if (config->host != NULL) {
        effective.host = config->host;
    }
    if (config->port != 0) {
        effective.port = config->port;
    }
    if (config->cert_file != NULL) {
        effective.cert_file = config->cert_file;
    }
    if (config->key_file != NULL) {
        effective.key_file = config->key_file;
    }
    if (config->webtransport_path != NULL) {
        effective.webtransport_path = config->webtransport_path;
    }
    if (config->webtransport_origin != NULL) {
        effective.webtransport_origin = config->webtransport_origin;
    }
    if (config->webtransport_admission != NULL) {
        effective.webtransport_admission = config->webtransport_admission;
        effective.webtransport_admission_user_data = config->webtransport_admission_user_data;
    }
    if (config->enable_http3 != 0) {
        effective.enable_http3 = 1;
    }
    if (config->http3_path != NULL) {
        effective.http3_path = config->http3_path;
    }
    if (config->http3_admission != NULL) {
        effective.http3_admission = config->http3_admission;
        effective.http3_admission_user_data = config->http3_admission_user_data;
    }
    if (config->max_idle_timeout_ms != 0) {
        effective.max_idle_timeout_ms = config->max_idle_timeout_ms;
    }
    if (config->keep_alive_ms != 0) {
        effective.keep_alive_ms = config->keep_alive_ms;
    }
    if (config->peer_bidi_stream_count != 0) {
        effective.peer_bidi_stream_count = config->peer_bidi_stream_count;
    }
    if (config->max_stateless_operations != 0) {
        effective.max_stateless_operations = config->max_stateless_operations;
    }
    if (config->max_binding_stateless_operations != 0) {
        effective.max_binding_stateless_operations = config->max_binding_stateless_operations;
    }
    if (config->max_pending_send_bytes != 0) {
        effective.max_pending_send_bytes = config->max_pending_send_bytes;
    }
    if (config->max_pending_send_count != 0) {
        effective.max_pending_send_count = config->max_pending_send_count;
    }
    if (config->max_sessions_per_connection != 0) {
        effective.max_sessions_per_connection = config->max_sessions_per_connection;
    }
    if (config->max_streams_per_session != 0) {
        effective.max_streams_per_session = config->max_streams_per_session;
    }
    if (config->stream_recv_window != 0) {
        effective.stream_recv_window = config->stream_recv_window;
    }
    if (config->conn_flow_control_window != 0) {
        effective.conn_flow_control_window = config->conn_flow_control_window;
    }
    if (config->msquic_execution_profile != 0) {
        effective.msquic_execution_profile = config->msquic_execution_profile;
    }
    if (config->msquic_send_buffering_enabled != 0) {
        effective.msquic_send_buffering_enabled = config->msquic_send_buffering_enabled;
    }
    if (config->max_frame_size != 0) {
        effective.max_frame_size = config->max_frame_size;
    }
    return effective;
}

static trevrpc_msquic_config trevrpc_make_server_msquic_config(const trevrpc_server_config* config) {
    trevrpc_msquic_config msquic_config = {
        .cert_file = config->cert_file,
        .key_file = config->key_file,
        .max_idle_timeout_ms = config->max_idle_timeout_ms,
        .keep_alive_ms = config->keep_alive_ms,
        .peer_bidi_stream_count = config->peer_bidi_stream_count,
        .max_stateless_operations = config->max_stateless_operations,
        .max_binding_stateless_operations = config->max_binding_stateless_operations,
        .stream_recv_window = config->stream_recv_window,
        .conn_flow_control_window = config->conn_flow_control_window,
        .execution_profile = config->msquic_execution_profile,
        .send_buffering_enabled = config->msquic_send_buffering_enabled,
        .max_pending_send_bytes = config->max_pending_send_bytes,
        .max_pending_send_count = config->max_pending_send_count,
        .max_frame_size = config->max_frame_size,
    };
    uint32_t wt_stream_count = config->max_streams_per_session;
    if (wt_stream_count > UINT16_MAX) {
        wt_stream_count = UINT16_MAX;
    }
    if (wt_stream_count > msquic_config.peer_bidi_stream_count) {
        msquic_config.peer_bidi_stream_count = (uint16_t)wt_stream_count;
    }
    if (config->webtransport_path != NULL || config->enable_http3) {
        msquic_config.peer_unidi_stream_count = TREVRPC_H3_DEFAULT_UNIDI_STREAMS;
    }
    return msquic_config;
}

static trevrpc_wt_config trevrpc_make_server_wt_config(const trevrpc_server_config* config) {
    return (trevrpc_wt_config){
        .path = config->webtransport_path,
        .origin = config->webtransport_origin,
        .admission = config->webtransport_admission,
        .admission_user_data = config->webtransport_admission_user_data,
        .max_sessions_per_connection = config->max_sessions_per_connection,
        .max_streams_per_session = config->max_streams_per_session,
        .idle_timeout_ms = (uint32_t)config->max_idle_timeout_ms,
    };
}

static char* trevrpc_copy_cstring(const char* value) {
    if (value == NULL) {
        return NULL;
    }
    size_t len = strlen(value);
    char* copy = malloc(len + 1);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, value, len + 1);
    return copy;
}

static bool trevrpc_alpn_equals(const uint8_t* alpn, size_t alpn_len, const char* expected) {
    size_t expected_len = strlen(expected);
    return alpn_len == expected_len && memcmp(alpn, expected, expected_len) == 0;
}

trevrpc_config trevrpc_default_config(void) {
    trevrpc_config config = {0};
    config.max_idle_timeout_ms = 30000;
    config.keep_alive_ms = 15000;
    config.peer_bidi_stream_count = 100;
    config.max_pending_send_bytes = TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES;
    config.max_pending_send_count = TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT;
    config.max_frame_size = TREVRPC_DEFAULT_MAX_FRAME_SIZE;
    return config;
}

uint32_t trevrpc_c_abi_version(void) {
    return TREVRPC_C_ABI_VERSION;
}

trevrpc_server_config trevrpc_default_server_config(void) {
    trevrpc_server_config config = {0};
    config.host = "127.0.0.1";
    config.webtransport_path = "/trevrpc";
    config.http3_path = "/trevrpc";
    config.max_idle_timeout_ms = 30000;
    config.keep_alive_ms = 15000;
    config.peer_bidi_stream_count = 128;
    config.max_stateless_operations = 1024;
    config.max_binding_stateless_operations = 256;
    config.max_pending_send_bytes = TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES;
    config.max_pending_send_count = TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT;
    config.max_sessions_per_connection = 16;
    config.max_streams_per_session = 128;
    config.max_frame_size = TREVRPC_DEFAULT_MAX_FRAME_SIZE;
    return config;
}

trevrpc_server_options trevrpc_default_server_options(void) {
    trevrpc_server_options options = {0};
    options.max_concurrent_connections = 256;
    options.max_concurrent_streams_per_connection = 64;
    options.max_concurrent_requests = 1024;
    options.worker_count = TREVRPC_DEFAULT_WORKER_COUNT;
    options.worker_queue_capacity = TREVRPC_DEFAULT_WORKER_QUEUE_CAPACITY;
    options.graceful_shutdown_timeout_nanos = 30ull * TREVRPC_NANOS_PER_SEC;
    options.initial_request_timeout_nanos = 10ull * TREVRPC_NANOS_PER_SEC;
    options.max_stream_messages = 4096;
    options.max_stream_body_size = 16 * 1024 * 1024;
    options.stream_idle_timeout_nanos = 30ull * TREVRPC_NANOS_PER_SEC;
    return options;
}

static size_t trevrpc_effective_worker_count(int64_t configured) {
    if (configured <= 0) {
        return TREVRPC_DEFAULT_WORKER_COUNT;
    }
    if ((uint64_t)configured > TREVRPC_MAX_WORKER_COUNT) {
        return TREVRPC_MAX_WORKER_COUNT;
    }
    return (size_t)configured;
}

static size_t trevrpc_effective_worker_queue_capacity(int64_t configured) {
    if (configured <= 0) {
        return TREVRPC_DEFAULT_WORKER_QUEUE_CAPACITY;
    }
    if ((uint64_t)configured > TREVRPC_MAX_WORKER_QUEUE_CAPACITY) {
        return TREVRPC_MAX_WORKER_QUEUE_CAPACITY;
    }
    return (size_t)configured;
}

static trevrpc_server_options trevrpc_normalize_server_options(trevrpc_server_options options) {
    options.worker_count = (int64_t)trevrpc_effective_worker_count(options.worker_count);
    options.worker_queue_capacity = (int64_t)trevrpc_effective_worker_queue_capacity(options.worker_queue_capacity);
    return options;
}

trevrpc_call_options trevrpc_default_call_options(void) {
    trevrpc_call_options options = {0};
    options.max_response_body_size = TREVRPC_DEFAULT_MAX_FRAME_SIZE;
    options.max_response_messages = 4096;
    options.max_response_stream_body_size = 16 * 1024 * 1024;
    return options;
}

static int trevrpc_clock_now(struct timespec* out_now) {
    if (out_now == NULL) {
        return -EINVAL;
    }
    if (clock_gettime(CLOCK_MONOTONIC, out_now) != 0) {
        return -errno;
    }
    return 0;
}

static int trevrpc_realtime_deadline(uint64_t timeout_nanos, struct timespec* out_deadline) {
    if (clock_gettime(CLOCK_REALTIME, out_deadline) != 0) {
        return -errno;
    }

    uint64_t seconds = timeout_nanos / TREVRPC_NANOS_PER_SEC;
    uint64_t nanos = timeout_nanos % TREVRPC_NANOS_PER_SEC;
    if (seconds > (uint64_t)(INT64_MAX - out_deadline->tv_sec)) {
        return -EOVERFLOW;
    }

    out_deadline->tv_sec += (time_t)seconds;
    out_deadline->tv_nsec += (long)nanos;
    if (out_deadline->tv_nsec >= (long)TREVRPC_NANOS_PER_SEC) {
        out_deadline->tv_sec++;
        out_deadline->tv_nsec -= (long)TREVRPC_NANOS_PER_SEC;
    }
    return 0;
}

static int trevrpc_timespec_compare(const struct timespec* lhs, const struct timespec* rhs) {
    if (lhs->tv_sec < rhs->tv_sec) {
        return -1;
    }
    if (lhs->tv_sec > rhs->tv_sec) {
        return 1;
    }
    if (lhs->tv_nsec < rhs->tv_nsec) {
        return -1;
    }
    if (lhs->tv_nsec > rhs->tv_nsec) {
        return 1;
    }
    return 0;
}

static uint64_t trevrpc_timespec_diff_nanos(const struct timespec* end, const struct timespec* start) {
    if (trevrpc_timespec_compare(end, start) <= 0) {
        return 0;
    }

    uint64_t seconds = (uint64_t)(end->tv_sec - start->tv_sec);
    uint64_t nanos = 0;
    if (end->tv_nsec >= start->tv_nsec) {
        nanos = (uint64_t)(end->tv_nsec - start->tv_nsec);
    } else {
        seconds--;
        nanos = TREVRPC_NANOS_PER_SEC + (uint64_t)end->tv_nsec - (uint64_t)start->tv_nsec;
    }
    if (seconds > (UINT64_MAX - nanos) / TREVRPC_NANOS_PER_SEC) {
        return UINT64_MAX;
    }
    return seconds * TREVRPC_NANOS_PER_SEC + nanos;
}

static uint64_t trevrpc_initial_timeout_remaining(
    uint64_t initial_timeout, const struct timespec* accepted_at, bool* expired) {
    *expired = false;
    if (initial_timeout == 0 || accepted_at == NULL) {
        return initial_timeout;
    }

    struct timespec now = {0};
    if (trevrpc_clock_now(&now) != 0) {
        return initial_timeout;
    }

    uint64_t elapsed = trevrpc_timespec_diff_nanos(&now, accepted_at);
    if (elapsed >= initial_timeout) {
        *expired = true;
        return 0;
    }
    return initial_timeout - elapsed;
}

static int trevrpc_call_context_init(
    trevrpc_call_context* context, trevrpc_server* server, const trevrpc_request* request) {
    if (context == NULL || request == NULL) {
        return -EINVAL;
    }

    context->server = server;
    context->has_deadline = false;
    context->deadline = (struct timespec){0};
    if (request->timeout_nanos == 0) {
        return 0;
    }
    if (request->timeout_nanos > INT64_MAX) {
        return -ERANGE;
    }

    struct timespec now = {0};
    int err = trevrpc_clock_now(&now);
    if (err != 0) {
        return err;
    }

    uint64_t seconds = request->timeout_nanos / TREVRPC_NANOS_PER_SEC;
    uint64_t nanos = request->timeout_nanos % TREVRPC_NANOS_PER_SEC;
    if (seconds > (uint64_t)(INT64_MAX - now.tv_sec)) {
        return -EOVERFLOW;
    }

    context->deadline.tv_sec = now.tv_sec + (time_t)seconds;
    context->deadline.tv_nsec = now.tv_nsec + (long)nanos;
    if (context->deadline.tv_nsec >= (long)TREVRPC_NANOS_PER_SEC) {
        context->deadline.tv_sec++;
        context->deadline.tv_nsec -= (long)TREVRPC_NANOS_PER_SEC;
    }
    if (context->deadline.tv_sec < now.tv_sec) {
        return -EOVERFLOW;
    }

    context->has_deadline = true;
    return 0;
}

int trevrpc_call_context_has_deadline(const trevrpc_call_context* context) {
    return context != NULL && context->has_deadline;
}

int trevrpc_call_context_deadline_expired(const trevrpc_call_context* context) {
    if (context == NULL || !context->has_deadline) {
        return 0;
    }

    struct timespec now = {0};
    if (trevrpc_clock_now(&now) != 0) {
        return 0;
    }
    return trevrpc_timespec_compare(&now, &context->deadline) >= 0;
}

int trevrpc_call_context_cancelled(const trevrpc_call_context* context) {
    if (context == NULL) {
        return 1;
    }
    if (trevrpc_call_context_deadline_expired(context)) {
        return 1;
    }
    return context->server != NULL && trevrpc_server_is_shutting_down(context->server);
}

int trevrpc_call_context_time_remaining_nanos(const trevrpc_call_context* context, uint64_t* remaining_nanos) {
    if (remaining_nanos == NULL) {
        return -EINVAL;
    }
    *remaining_nanos = 0;
    if (context == NULL || !context->has_deadline) {
        return 0;
    }

    struct timespec now = {0};
    int err = trevrpc_clock_now(&now);
    if (err != 0) {
        return err;
    }
    *remaining_nanos = trevrpc_timespec_diff_nanos(&context->deadline, &now);
    return 1;
}

static int trevrpc_stream_write_frame(trevrpc_stream* stream, const uint8_t* frame, size_t frame_len) {
    intptr_t written = 0;
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        written = trevrpc_msquic_stream_write(stream->msquic_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_MSQUIC_ERR_CLOSED;
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        written = trevrpc_wt_stream_write(stream->wt_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_WT_ERR_CLOSED;
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        written = trevrpc_h3_stream_write(stream->h3_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_WT_ERR_CLOSED;
    }
    return -EINVAL;
}

static int trevrpc_stream_write_final_frame(trevrpc_stream* stream, const uint8_t* frame, size_t frame_len) {
    intptr_t written = 0;
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        written = trevrpc_msquic_stream_write_fin(stream->msquic_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_MSQUIC_ERR_CLOSED;
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        int err = trevrpc_stream_write_frame(stream, frame, frame_len);
        if (err != 0) {
            return err;
        }
        return trevrpc_wt_stream_shutdown_send(stream->wt_stream);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        written = trevrpc_h3_stream_write_fin(stream->h3_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_WT_ERR_CLOSED;
    }
    return -EINVAL;
}

static int trevrpc_write_msquic_frame_parts(trevrpc_msquic_stream* stream,
    size_t max_frame_size,
    const trevrpc_wire_frame_parts* parts,
    bool finish_send,
    bool* submitted) {
    if (submitted != NULL) {
        *submitted = false;
    }
    trevrpc_msquic_frame_part msquic_parts[3] = {
        {.data = parts->prefix, .len = parts->prefix_len},
        {.data = parts->body, .len = parts->body_len},
        {.data = parts->suffix, .len = parts->suffix_len},
    };
    trevrpc_msquic_send_completion* completion = NULL;
    intptr_t written = finish_send ? trevrpc_msquic_stream_write_frame_parts_fin_with_completion(
                                         stream, msquic_parts, 3, max_frame_size, &completion)
                                   : trevrpc_msquic_stream_write_frame_parts_with_completion(
                                         stream, msquic_parts, 3, max_frame_size, &completion);
    if (written < 0) {
        return (int)written;
    }
    if ((size_t)written != 4 + parts->frame_body_len) {
        trevrpc_msquic_send_completion_wait(completion);
        trevrpc_msquic_send_completion_free(completion);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (submitted != NULL) {
        *submitted = true;
    }

    int err = trevrpc_msquic_send_completion_wait(completion);
    trevrpc_msquic_send_completion_free(completion);
    return err;
}

static int trevrpc_stream_write_msquic_frame_parts(
    trevrpc_stream* stream, const trevrpc_wire_frame_parts* parts, bool finish_send, bool* submitted) {
    return trevrpc_write_msquic_frame_parts(
        stream->msquic_stream, stream->max_frame_size, parts, finish_send, submitted);
}

static intptr_t trevrpc_stream_write_message_frame(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_msquic_stream_write_message_frame(stream->msquic_stream, body, body_len, stream->max_frame_size);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return trevrpc_wt_stream_write_message_frame(stream->wt_stream, body, body_len, stream->max_frame_size);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        uint8_t* frame = NULL;
        size_t frame_len = 0;
        int err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
            TREVRPC_STATUS_OK,
            NULL,
            0,
            body,
            body_len,
            NULL,
            stream->max_frame_size,
            &frame,
            &frame_len);
        if (err != 0) {
            return err;
        }
        intptr_t written = trevrpc_h3_stream_write(stream->h3_stream, frame, frame_len);
        free(frame);
        return written < 0 ? written : (intptr_t)body_len;
    }
    return -EINVAL;
}

static intptr_t trevrpc_stream_read_frame_body(trevrpc_stream* stream, uint8_t** body, size_t* body_len) {
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return stream->stream_idle_timeout_nanos == 0
                   ? trevrpc_msquic_stream_read_frame(stream->msquic_stream, body, body_len, stream->max_frame_size)
                   : trevrpc_msquic_stream_read_frame_timeout(stream->msquic_stream,
                         body,
                         body_len,
                         stream->max_frame_size,
                         stream->stream_idle_timeout_nanos);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return stream->stream_idle_timeout_nanos == 0
                   ? trevrpc_wt_stream_read_frame(stream->wt_stream, body, body_len, stream->max_frame_size)
                   : trevrpc_wt_stream_read_frame_timeout(
                         stream->wt_stream, body, body_len, stream->max_frame_size, stream->stream_idle_timeout_nanos);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        return stream->stream_idle_timeout_nanos == 0
                   ? trevrpc_h3_stream_read_frame(stream->h3_stream, body, body_len, stream->max_frame_size)
                   : trevrpc_h3_stream_read_frame_timeout(
                         stream->h3_stream, body, body_len, stream->max_frame_size, stream->stream_idle_timeout_nanos);
    }
    return -EINVAL;
}

static intptr_t trevrpc_stream_read_frame_body_ready(trevrpc_stream* stream, uint8_t** body, size_t* body_len) {
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_msquic_stream_read_frame_ready(stream->msquic_stream, body, body_len, stream->max_frame_size);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return trevrpc_wt_stream_read_frame_ready(stream->wt_stream, body, body_len, stream->max_frame_size);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        return trevrpc_h3_stream_read_frame_ready(stream->h3_stream, body, body_len, stream->max_frame_size);
    }
    return TREV_MSQUIC_ERR_TIMEOUT;
}

static void trevrpc_stream_free_body(trevrpc_stream* stream, uint8_t* body) {
    if (stream != NULL && stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        trevrpc_wt_free(body);
        return;
    }
    if (stream != NULL && stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        trevrpc_wt_free(body);
        return;
    }
    trevrpc_msquic_free(body);
}

static trevrpc_stream trevrpc_stream_ref_msquic(trevrpc_msquic_stream* stream, size_t max_frame_size) {
    return (trevrpc_stream){
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
        .msquic_stream = stream,
        .max_frame_size = max_frame_size,
        .max_stream_messages = TREVRPC_STREAM_LIMIT_DISABLED,
        .max_stream_body_size = TREVRPC_STREAM_LIMIT_DISABLED,
        .failure_status = TREVRPC_STATUS_OK,
    };
}

static trevrpc_stream trevrpc_stream_ref_webtransport(trevrpc_wt_stream* stream, size_t max_frame_size) {
    return (trevrpc_stream){
        .transport = TREVRPC_TRANSPORT_KIND_WEBTRANSPORT,
        .wt_stream = stream,
        .max_frame_size = max_frame_size,
        .max_stream_messages = TREVRPC_STREAM_LIMIT_DISABLED,
        .max_stream_body_size = TREVRPC_STREAM_LIMIT_DISABLED,
        .failure_status = TREVRPC_STATUS_OK,
    };
}

static trevrpc_stream trevrpc_stream_ref_http3(trevrpc_h3_stream* stream, size_t max_frame_size) {
    return (trevrpc_stream){
        .transport = TREVRPC_TRANSPORT_KIND_HTTP3,
        .h3_stream = stream,
        .max_frame_size = max_frame_size,
        .max_stream_messages = TREVRPC_STREAM_LIMIT_DISABLED,
        .max_stream_body_size = TREVRPC_STREAM_LIMIT_DISABLED,
        .failure_status = TREVRPC_STATUS_OK,
    };
}

static trevrpc_stream trevrpc_stream_ref_server(
    trevrpc_msquic_stream* stream, trevrpc_wt_stream* wt_stream, trevrpc_h3_stream* h3_stream, size_t max_frame_size) {
    if (stream != NULL) {
        return trevrpc_stream_ref_msquic(stream, max_frame_size);
    }
    if (wt_stream != NULL) {
        return trevrpc_stream_ref_webtransport(wt_stream, max_frame_size);
    }
    return trevrpc_stream_ref_http3(h3_stream, max_frame_size);
}

static int trevrpc_stream_shutdown_send_raw(trevrpc_stream* stream) {
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_msquic_stream_shutdown_send(stream->msquic_stream);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return trevrpc_wt_stream_shutdown_send(stream->wt_stream);
    }
    if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        return trevrpc_h3_stream_shutdown_send(stream->h3_stream);
    }
    return -EINVAL;
}

static void trevrpc_stream_close_raw(trevrpc_stream* stream) {
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_msquic_stream_close(stream->msquic_stream);
    } else if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        trevrpc_wt_stream_close(stream->wt_stream);
    } else if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        trevrpc_h3_stream_close(stream->h3_stream);
    }
}

#ifdef TREVRPC_TESTING
static trevrpc_server* trevrpc_test_current_server;

static void trevrpc_test_record_stream_status(uint32_t status) {
    if (trevrpc_test_current_server == NULL) {
        return;
    }
    trevrpc_test_current_server->test_last_stream_status = trevrpc_status_code_from_uint32(status);
    trevrpc_test_current_server->test_stream_status_count++;
}
#endif

static trevrpc_stream* trevrpc_stream_alloc_msquic(
    trevrpc_msquic_stream* stream, size_t max_frame_size, bool owns_stream) {
    trevrpc_stream* rpc_stream = calloc(1, sizeof(*rpc_stream));
    if (rpc_stream == NULL) {
        return NULL;
    }

    rpc_stream->transport = TREVRPC_TRANSPORT_KIND_MSQUIC;
    rpc_stream->msquic_stream = stream;
    rpc_stream->max_frame_size = max_frame_size;
    rpc_stream->owns_stream = owns_stream;
    rpc_stream->max_stream_messages = TREVRPC_STREAM_LIMIT_DISABLED;
    rpc_stream->max_stream_body_size = TREVRPC_STREAM_LIMIT_DISABLED;
    rpc_stream->failure_status = TREVRPC_STATUS_OK;
    return rpc_stream;
}

static trevrpc_stream* trevrpc_stream_alloc_webtransport(
    trevrpc_wt_stream* stream, size_t max_frame_size, bool owns_stream) {
    trevrpc_stream* rpc_stream = calloc(1, sizeof(*rpc_stream));
    if (rpc_stream == NULL) {
        return NULL;
    }

    rpc_stream->transport = TREVRPC_TRANSPORT_KIND_WEBTRANSPORT;
    rpc_stream->wt_stream = stream;
    rpc_stream->max_frame_size = max_frame_size;
    rpc_stream->owns_stream = owns_stream;
    rpc_stream->max_stream_messages = TREVRPC_STREAM_LIMIT_DISABLED;
    rpc_stream->max_stream_body_size = TREVRPC_STREAM_LIMIT_DISABLED;
    rpc_stream->failure_status = TREVRPC_STATUS_OK;
    return rpc_stream;
}

static void trevrpc_stream_record_failure(trevrpc_stream* stream, uint32_t status, const char* message) {
    if (stream == NULL || stream->sent_status || stream->failure_status != TREVRPC_STATUS_OK) {
        return;
    }

    stream->failure_status = status;
    stream->failure_message = message;
}

static bool trevrpc_stream_direction_is_recv(const char* direction) {
    return strcmp(direction, "request") == 0;
}

static int64_t trevrpc_stream_message_limit(const trevrpc_stream* stream, const char* direction) {
    if (stream->has_recv_limits && trevrpc_stream_direction_is_recv(direction)) {
        return stream->max_recv_stream_messages;
    }
    return stream->max_stream_messages;
}

static int64_t trevrpc_stream_body_size_limit(const trevrpc_stream* stream, const char* direction) {
    if (stream->has_recv_limits && trevrpc_stream_direction_is_recv(direction)) {
        return stream->max_recv_stream_body_size;
    }
    return stream->max_stream_body_size;
}

static int trevrpc_stream_check_message_limit(trevrpc_stream* stream, int64_t* count, const char* direction) {
    int64_t limit = trevrpc_stream_message_limit(stream, direction);
    if (limit >= 0 && *count >= limit) {
        const char* message = strcmp(direction, "request") == 0 ? "request stream exceeded maximum message count"
                                                                : "response stream exceeded maximum message count";
        trevrpc_stream_record_failure(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, message);
        return TREVRPC_ERR_STREAM_LIMIT_EXCEEDED;
    }

    *count += 1;
    return 0;
}

static uint64_t trevrpc_saturating_add_size(uint64_t total, size_t size) {
    if ((uint64_t)size > UINT64_MAX - total) {
        return UINT64_MAX;
    }

    return total + (uint64_t)size;
}

static int trevrpc_stream_check_body_size_limit(
    trevrpc_stream* stream, uint64_t* total_body_size, size_t body_len, const char* direction) {
    *total_body_size = trevrpc_saturating_add_size(*total_body_size, body_len);
    int64_t limit = trevrpc_stream_body_size_limit(stream, direction);
    if (limit < 0 || *total_body_size <= (uint64_t)limit) {
        return 0;
    }

    const char* message = strcmp(direction, "request") == 0 ? "request stream exceeded maximum body size"
                                                            : "response stream exceeded maximum body size";
    trevrpc_stream_record_failure(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, message);
    return TREVRPC_ERR_STREAM_LIMIT_EXCEEDED;
}

static int trevrpc_stream_check_exhausted_message_limit(
    trevrpc_stream* stream, const int64_t* count, const char* direction) {
    int64_t limit = trevrpc_stream_message_limit(stream, direction);
    if (limit < 0 || *count < limit) {
        return 0;
    }

    const char* message = strcmp(direction, "request") == 0 ? "request stream exceeded maximum message count"
                                                            : "response stream exceeded maximum message count";
    trevrpc_stream_record_failure(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, message);
    return TREVRPC_ERR_STREAM_LIMIT_EXCEEDED;
}

static int trevrpc_stream_check_exhausted_body_size_limit(
    trevrpc_stream* stream, const uint64_t* total_body_size, const char* direction) {
    int64_t limit = trevrpc_stream_body_size_limit(stream, direction);
    if (limit < 0 || *total_body_size <= (uint64_t)limit) {
        return 0;
    }

    const char* message = strcmp(direction, "request") == 0 ? "request stream exceeded maximum body size"
                                                            : "response stream exceeded maximum body size";
    trevrpc_stream_record_failure(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, message);
    return TREVRPC_ERR_STREAM_LIMIT_EXCEEDED;
}

static int trevrpc_stream_start_response_idle_timer(trevrpc_stream* stream) {
    if (stream == NULL || stream->stream_idle_timeout_nanos == 0) {
        return 0;
    }

    int err = trevrpc_clock_now(&stream->response_last_activity);
    if (err != 0) {
        return err;
    }
    stream->response_idle_started = true;
    return 0;
}

static int trevrpc_stream_check_response_idle_timeout(trevrpc_stream* stream) {
    if (stream->stream_idle_timeout_nanos == 0) {
        return 0;
    }
    if (!stream->response_idle_started) {
        return trevrpc_stream_start_response_idle_timer(stream);
    }

    struct timespec now = {0};
    int err = trevrpc_clock_now(&now);
    if (err != 0) {
        return err;
    }
    if (trevrpc_timespec_diff_nanos(&now, &stream->response_last_activity) > stream->stream_idle_timeout_nanos) {
        trevrpc_stream_record_failure(stream, TREVRPC_STATUS_UNAVAILABLE, "response stream idle timeout");
        return TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
    }

    stream->response_last_activity = now;
    return 0;
}

static int trevrpc_stream_context_error(const trevrpc_stream* stream) {
    if (stream == NULL || stream->context == NULL) {
        return 0;
    }
    if (trevrpc_call_context_deadline_expired(stream->context)) {
        return -ETIMEDOUT;
    }
    if (trevrpc_call_context_cancelled(stream->context)) {
        return -ECANCELED;
    }
    return 0;
}

static int trevrpc_stream_check_message_batch_limits(
    trevrpc_stream* stream, const size_t* body_lens, size_t count, int64_t* next_count, uint64_t* next_body_size) {
    *next_count = stream->response_message_count;
    *next_body_size = stream->response_body_size;
    for (size_t i = 0; i < count; i++) {
        int err = trevrpc_stream_check_message_limit(stream, next_count, "response");
        if (err == 0) {
            err = trevrpc_stream_check_body_size_limit(stream, next_body_size, body_lens[i], "response");
        }
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int trevrpc_stream_prepare_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    if (stream == NULL || (stream->msquic_stream == NULL && stream->wt_stream == NULL && stream->h3_stream == NULL) ||
        (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    if (stream->sent_status) {
        return -EPIPE;
    }
    int err = trevrpc_stream_context_error(stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_check_response_idle_timeout(stream);
    if (err != 0) {
        return err;
    }
    int64_t next_count = stream->response_message_count;
    uint64_t next_body_size = stream->response_body_size;
    err = trevrpc_stream_check_message_limit(stream, &next_count, "response");
    if (err == 0) {
        err = trevrpc_stream_check_body_size_limit(stream, &next_body_size, body_len, "response");
    }
    if (err != 0) {
        return err;
    }
    stream->response_message_count = next_count;
    stream->response_body_size = next_body_size;
    return 0;
}

int trevrpc_stream_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    int64_t previous_count = stream == NULL ? 0 : stream->response_message_count;
    uint64_t previous_body_size = stream == NULL ? 0 : stream->response_body_size;
    int err = trevrpc_stream_prepare_send_message(stream, body, body_len);
    if (err != 0) {
        return err;
    }
    intptr_t written = trevrpc_stream_write_message_frame(stream, body, body_len);
    if (written < 0) {
        stream->response_message_count = previous_count;
        stream->response_body_size = previous_body_size;
        return (int)written;
    }
    return 0;
}

int trevrpc_stream_send_message_borrowed_wait(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    if (stream == NULL || stream->transport != TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_stream_send_message(stream, body, body_len);
    }

    int64_t previous_count = stream->response_message_count;
    uint64_t previous_body_size = stream->response_body_size;
    int err = trevrpc_stream_prepare_send_message(stream, body, body_len);
    if (err != 0) {
        return err;
    }
    trevrpc_wire_frame_parts parts = {0};
    bool submitted = false;
    err = trevrpc_wire_encode_stream_message_parts(body, body_len, stream->max_frame_size, &parts);
    if (err == 0) {
        err = trevrpc_stream_write_msquic_frame_parts(stream, &parts, false, &submitted);
    }
    trevrpc_wire_frame_parts_reset(&parts);
    if (err != 0 && !submitted) {
        stream->response_message_count = previous_count;
        stream->response_body_size = previous_body_size;
    }
    return err;
}

int trevrpc_stream_send_messages(trevrpc_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count) {
    if (stream == NULL || (stream->msquic_stream == NULL && stream->wt_stream == NULL && stream->h3_stream == NULL) ||
        (body_lens == NULL && count > 0)) {
        return -EINVAL;
    }

    size_t total_body_len = 0;
    for (size_t i = 0; i < count; i++) {
        if (body_lens[i] > SIZE_MAX - total_body_len) {
            return -EOVERFLOW;
        }
        total_body_len += body_lens[i];
    }
    if (bodies == NULL && total_body_len > 0) {
        return -EINVAL;
    }
    if (count == 0) {
        return 0;
    }

    if (stream->transport != TREVRPC_TRANSPORT_KIND_MSQUIC &&
        stream->transport != TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        size_t body_offset = 0;
        for (size_t i = 0; i < count; i++) {
            int err =
                trevrpc_stream_send_message(stream, body_lens[i] == 0 ? NULL : bodies + body_offset, body_lens[i]);
            if (err != 0) {
                return err;
            }
            body_offset += body_lens[i];
        }
        return 0;
    }

    if (stream->sent_status) {
        return -EPIPE;
    }
    int err = trevrpc_stream_context_error(stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_check_response_idle_timeout(stream);
    if (err != 0) {
        return err;
    }

    int64_t previous_count = stream->response_message_count;
    uint64_t previous_body_size = stream->response_body_size;
    int64_t next_count = 0;
    uint64_t next_body_size = 0;
    err = trevrpc_stream_check_message_batch_limits(stream, body_lens, count, &next_count, &next_body_size);
    if (err != 0) {
        return err;
    }
    stream->response_message_count = next_count;
    stream->response_body_size = next_body_size;

    intptr_t written = stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC
                           ? trevrpc_msquic_stream_write_message_frames(
                                 stream->msquic_stream, bodies, body_lens, count, stream->max_frame_size)
                           : trevrpc_wt_stream_write_message_frames(
                                 stream->wt_stream, bodies, body_lens, count, stream->max_frame_size);
    if (written < 0) {
        stream->response_message_count = previous_count;
        stream->response_body_size = previous_body_size;
        return (int)written;
    }
    return 0;
}

int trevrpc_stream_send_messages_borrowed_wait(
    trevrpc_stream* stream, const uint8_t* const* bodies, const size_t* body_lens, size_t count) {
    if (stream == NULL || (body_lens == NULL && count > 0) || (bodies == NULL && count > 0)) {
        return -EINVAL;
    }
    if (count == 0) {
        return 0;
    }
    if (stream->transport != TREVRPC_TRANSPORT_KIND_MSQUIC) {
        size_t total_body_len = 0;
        for (size_t i = 0; i < count; i++) {
            if ((bodies[i] == NULL && body_lens[i] > 0) || body_lens[i] > SIZE_MAX - total_body_len) {
                return -EINVAL;
            }
            total_body_len += body_lens[i];
        }
        uint8_t* copied = malloc(total_body_len == 0 ? 1 : total_body_len);
        if (copied == NULL) {
            return -ENOMEM;
        }
        size_t offset = 0;
        for (size_t i = 0; i < count; i++) {
            if (body_lens[i] > 0) {
                memcpy(copied + offset, bodies[i], body_lens[i]);
                offset += body_lens[i];
            }
        }
        int err = trevrpc_stream_send_messages(stream, copied, body_lens, count);
        free(copied);
        return err;
    }
    if (stream->msquic_stream == NULL) {
        return -EINVAL;
    }
    for (size_t i = 0; i < count; i++) {
        if (bodies[i] == NULL && body_lens[i] > 0) {
            return -EINVAL;
        }
    }
    if (stream->sent_status) {
        return -EPIPE;
    }
    int err = trevrpc_stream_context_error(stream);
    if (err == 0) {
        err = trevrpc_stream_check_response_idle_timeout(stream);
    }
    if (err != 0) {
        return err;
    }

    int64_t previous_count = stream->response_message_count;
    uint64_t previous_body_size = stream->response_body_size;
    int64_t next_count = 0;
    uint64_t next_body_size = 0;
    err = trevrpc_stream_check_message_batch_limits(stream, body_lens, count, &next_count, &next_body_size);
    if (err != 0) {
        return err;
    }
    stream->response_message_count = next_count;
    stream->response_body_size = next_body_size;

    trevrpc_msquic_send_completion* completion = NULL;
    intptr_t written = trevrpc_msquic_stream_write_message_frames_borrowed(
        stream->msquic_stream, bodies, body_lens, count, stream->max_frame_size, &completion);
    if (written < 0) {
        stream->response_message_count = previous_count;
        stream->response_body_size = previous_body_size;
        return (int)written;
    }
    err = trevrpc_msquic_send_completion_wait(completion);
    trevrpc_msquic_send_completion_free(completion);
    return err;
}

static int trevrpc_stream_send_status_with_metadata_internal(trevrpc_stream* stream,
    uint32_t status,
    const char* message,
    size_t message_len,
    const trevrpc_metadata* metadata,
    bool finish_send) {
    if (stream == NULL || (stream->msquic_stream == NULL && stream->wt_stream == NULL && stream->h3_stream == NULL) ||
        (message == NULL && message_len > 0)) {
        return -EINVAL;
    }
    if (stream->sent_status) {
        return -EPIPE;
    }

    int err = 0;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_wire_frame_parts parts = {0};
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        err = trevrpc_wire_encode_stream_status_parts(
            status, message, message_len, metadata, stream->max_frame_size, &parts);
    } else {
        err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
            status,
            message,
            message_len,
            NULL,
            0,
            metadata,
            stream->max_frame_size,
            &frame,
            &frame_len);
    }
    if (err == 0) {
        stream->sent_status = true;
        stream->terminal_status = trevrpc_status_code_from_uint32(status);
#ifdef TREVRPC_TESTING
        trevrpc_test_record_stream_status(status);
#endif
        if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
            err = trevrpc_stream_write_msquic_frame_parts(stream, &parts, finish_send, NULL);
        } else {
            err = finish_send ? trevrpc_stream_write_final_frame(stream, frame, frame_len)
                              : trevrpc_stream_write_frame(stream, frame, frame_len);
        }
        stream->status_queued = err == 0;
    }
    trevrpc_wire_frame_parts_reset(&parts);
    free(frame);
    return err;
}

int trevrpc_stream_send_status_with_metadata(trevrpc_stream* stream,
    uint32_t status,
    const char* message,
    size_t message_len,
    const trevrpc_metadata* metadata) {
    return trevrpc_stream_send_status_with_metadata_internal(stream, status, message, message_len, metadata, false);
}

static int trevrpc_stream_send_final_status_with_metadata(trevrpc_stream* stream,
    uint32_t status,
    const char* message,
    size_t message_len,
    const trevrpc_metadata* metadata) {
    return trevrpc_stream_send_status_with_metadata_internal(stream, status, message, message_len, metadata, true);
}

int trevrpc_stream_send_status(trevrpc_stream* stream, uint32_t status, const char* message, size_t message_len) {
    return trevrpc_stream_send_status_with_metadata(stream, status, message, message_len, NULL);
}

int trevrpc_stream_recv(trevrpc_stream* stream, trevrpc_stream_frame** out_frame) {
    if (stream == NULL || (stream->msquic_stream == NULL && stream->wt_stream == NULL && stream->h3_stream == NULL) ||
        out_frame == NULL) {
        return -EINVAL;
    }
    *out_frame = NULL;
    int err = trevrpc_stream_context_error(stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_check_exhausted_message_limit(stream, &stream->request_message_count, "request");
    if (err == 0) {
        err = trevrpc_stream_check_exhausted_body_size_limit(stream, &stream->request_body_size, "request");
    }
    if (err != 0) {
        return err;
    }

    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_stream_read_frame_body(stream, &body, &body_len);
    if (read < 0) {
        if (read == TREV_MSQUIC_ERR_TIMEOUT) {
            trevrpc_stream_record_failure(stream, TREVRPC_STATUS_UNAVAILABLE, "request stream idle timeout");
            return TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
        }
        if (read == TREV_MSQUIC_ERR_CLOSED || read == TREV_WT_ERR_CLOSED || read == -ECANCELED) {
            trevrpc_stream_record_failure(stream, TREVRPC_STATUS_CANCELLED, "transport closed");
        }
        return (int)read;
    }
    stream->request_poll_idle_started = false;
    if (read == 0) {
        return 0;
    }

    bool took_body = false;
    err = trevrpc_wire_decode_stream_frame_take(body, body_len, out_frame, &took_body);
    if (!took_body) {
        trevrpc_stream_free_body(stream, body);
    }
    if (err == 0 && *out_frame != NULL && (*out_frame)->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
        err = trevrpc_stream_check_message_limit(stream, &stream->request_message_count, "request");
        if (err == 0) {
            err = trevrpc_stream_check_body_size_limit(
                stream, &stream->request_body_size, (*out_frame)->body_len, "request");
        }
        if (err != 0) {
            trevrpc_stream_frame_free(*out_frame);
            *out_frame = NULL;
        }
    }
    return err;
}

static int trevrpc_stream_recv_ready_internal(
    trevrpc_stream* stream, trevrpc_stream_frame** out_frame, int* ready, const struct timespec* wait_started_at) {
    if (stream == NULL || (stream->msquic_stream == NULL && stream->wt_stream == NULL && stream->h3_stream == NULL) ||
        out_frame == NULL || ready == NULL) {
        return -EINVAL;
    }
    *out_frame = NULL;
    *ready = 0;
    int err = trevrpc_stream_context_error(stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_check_exhausted_message_limit(stream, &stream->request_message_count, "request");
    if (err == 0) {
        err = trevrpc_stream_check_exhausted_body_size_limit(stream, &stream->request_body_size, "request");
    }
    if (err != 0) {
        return err;
    }

    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_stream_read_frame_body_ready(stream, &body, &body_len);
    if (read == TREV_MSQUIC_ERR_TIMEOUT) {
        if (stream->stream_idle_timeout_nanos == 0) {
            return 0;
        }
        struct timespec now = {0};
        err = trevrpc_clock_now(&now);
        if (err != 0) {
            return err;
        }
        if (!stream->request_poll_idle_started) {
            stream->request_poll_idle_started = true;
            stream->request_poll_started_at = wait_started_at == NULL ? now : *wait_started_at;
            return 0;
        }
        if (trevrpc_timespec_diff_nanos(&now, &stream->request_poll_started_at) <= stream->stream_idle_timeout_nanos) {
            return 0;
        }
        trevrpc_stream_record_failure(stream, TREVRPC_STATUS_UNAVAILABLE, "request stream idle timeout");
        return TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
    }
    stream->request_poll_idle_started = false;
    *ready = 1;
    if (read < 0) {
        if (read == TREV_MSQUIC_ERR_CLOSED || read == TREV_WT_ERR_CLOSED || read == -ECANCELED) {
            trevrpc_stream_record_failure(stream, TREVRPC_STATUS_CANCELLED, "transport closed");
        }
        return (int)read;
    }
    if (read == 0) {
        return 0;
    }

    bool took_body = false;
    err = trevrpc_wire_decode_stream_frame_take(body, body_len, out_frame, &took_body);
    if (!took_body) {
        trevrpc_stream_free_body(stream, body);
    }
    if (err == 0 && *out_frame != NULL && (*out_frame)->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
        err = trevrpc_stream_check_message_limit(stream, &stream->request_message_count, "request");
        if (err == 0) {
            err = trevrpc_stream_check_body_size_limit(
                stream, &stream->request_body_size, (*out_frame)->body_len, "request");
        }
        if (err != 0) {
            trevrpc_stream_frame_free(*out_frame);
            *out_frame = NULL;
        }
    }
    return err;
}

int trevrpc_stream_recv_ready(trevrpc_stream* stream, trevrpc_stream_frame** out_frame, int* ready) {
    return trevrpc_stream_recv_ready_internal(stream, out_frame, ready, NULL);
}

int trevrpc_stream_recv_ready_since(
    trevrpc_stream* stream, trevrpc_stream_frame** out_frame, int* ready, uint64_t wait_started_nanos) {
    if (wait_started_nanos == 0) {
        return trevrpc_stream_recv_ready_internal(stream, out_frame, ready, NULL);
    }
    struct timespec wait_started_at = {
        .tv_sec = (time_t)(wait_started_nanos / TREVRPC_NANOS_PER_SEC),
        .tv_nsec = (long)(wait_started_nanos % TREVRPC_NANOS_PER_SEC),
    };
    return trevrpc_stream_recv_ready_internal(stream, out_frame, ready, &wait_started_at);
}

int trevrpc_stream_wait_timeout_elapsed(const trevrpc_stream* stream, uint64_t wait_started_nanos) {
    if (stream == NULL || stream->stream_idle_timeout_nanos == 0 || wait_started_nanos == 0) {
        return 0;
    }
    struct timespec now = {0};
    if (trevrpc_clock_now(&now) != 0) {
        return 0;
    }
    uint64_t now_nanos = (uint64_t)now.tv_sec * TREVRPC_NANOS_PER_SEC + (uint64_t)now.tv_nsec;
    return now_nanos >= wait_started_nanos && now_nanos - wait_started_nanos > stream->stream_idle_timeout_nanos;
}

int trevrpc_stream_recv_batch(
    trevrpc_stream* stream, trevrpc_stream_frame** frames, size_t capacity, size_t* count, int* eof) {
    if (stream == NULL || frames == NULL || capacity == 0 || count == NULL || eof == NULL) {
        return -EINVAL;
    }

    *count = 0;
    *eof = 0;
    for (size_t i = 0; i < capacity; i++) {
        frames[i] = NULL;
    }

    trevrpc_stream_frame* frame = NULL;
    int err = trevrpc_stream_recv(stream, &frame);
    if (err != 0) {
        return err;
    }
    if (frame == NULL) {
        *eof = 1;
        return 0;
    }

    frames[(*count)++] = frame;
    if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
        return 0;
    }

    while (*count < capacity) {
        int ready = 0;
        frame = NULL;
        err = trevrpc_stream_recv_ready(stream, &frame, &ready);
        if (err != 0 || !ready) {
            return err;
        }
        if (frame == NULL) {
            *eof = 1;
            return 0;
        }
        frames[(*count)++] = frame;
        if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
            return 0;
        }
    }

    return 0;
}

int trevrpc_stream_finish_send(trevrpc_stream* stream) {
    if (stream == NULL || (stream->msquic_stream == NULL && stream->wt_stream == NULL && stream->h3_stream == NULL)) {
        return -EINVAL;
    }

    return trevrpc_stream_shutdown_send_raw(stream);
}

void trevrpc_stream_cancel(trevrpc_stream* stream) {
    if (stream == NULL) {
        return;
    }

    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        (void)trevrpc_msquic_stream_abort(stream->msquic_stream);
    } else if (stream->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        (void)trevrpc_wt_stream_abort(stream->wt_stream, 0);
    } else if (stream->transport == TREVRPC_TRANSPORT_KIND_HTTP3) {
        (void)trevrpc_h3_stream_abort(stream->h3_stream);
    }
}

void trevrpc_stream_close(trevrpc_stream* stream) {
    if (stream == NULL) {
        return;
    }

    if (stream->owns_stream) {
        trevrpc_stream_close_raw(stream);
    }
    if (stream->release != NULL) {
        stream->release(stream->release_context);
    }
    free(stream);
}

void trevrpc_stream_set_release(trevrpc_stream* stream, void (*release)(void* context), void* context) {
    if (stream == NULL) {
        return;
    }
    stream->release = release;
    stream->release_context = context;
}

int trevrpc_raw_client_connect(
    const char* host, uint16_t port, const trevrpc_config* config, trevrpc_raw_client** out_client) {
    return trevrpc_raw_client_connect_cancellable(host, port, config, NULL, out_client);
}

static int trevrpc_connect_cancelled(void* context) {
    return trevrpc_cancellation_cancelled(context);
}

int trevrpc_raw_client_connect_cancellable(const char* host,
    uint16_t port,
    const trevrpc_config* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client** out_client) {
    return trevrpc_raw_client_connect_observed(host,
        port,
        config,
        cancellation == NULL ? NULL : trevrpc_connect_cancelled,
        cancellation,
        NULL,
        0,
        NULL,
        NULL,
        out_client);
}

int trevrpc_raw_client_connect_observed(const char* host,
    uint16_t port,
    const trevrpc_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_raw_client** out_client) {
    if (host == NULL || out_client == NULL) {
        return -EINVAL;
    }
    *out_client = NULL;

    trevrpc_raw_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return -ENOMEM;
    }
    client->max_frame_size = trevrpc_config_max_frame_size(config);
    client->transport = TREVRPC_TRANSPORT_KIND_MSQUIC;

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_dial_observed(host,
        port,
        &msquic_config,
        cancelled,
        cancellation_context,
        resumption_ticket,
        resumption_ticket_len,
        observer,
        observer_context,
        &client->msquic_conn);
    if (err != 0) {
        trevrpc_raw_client_close(client);
        return err;
    }

    *out_client = client;
    return 0;
}

void trevrpc_raw_client_clear_observer(trevrpc_raw_client* client) {
    if (client != NULL && client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_msquic_conn_clear_observer(client->msquic_conn);
    }
}

int trevrpc_raw_client_connect_webtransport(
    const trevrpc_wt_config* wt_config, const trevrpc_config* config, trevrpc_raw_client** out_client) {
    if (wt_config == NULL || out_client == NULL) {
        return -EINVAL;
    }
    *out_client = NULL;

    trevrpc_raw_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return -ENOMEM;
    }
    client->transport = TREVRPC_TRANSPORT_KIND_WEBTRANSPORT;
    client->max_frame_size = trevrpc_config_max_frame_size(config);

    int err = trevrpc_wt_dial(wt_config, &client->wt_session);
    if (err != 0) {
        trevrpc_raw_client_close(client);
        return err;
    }

    *out_client = client;
    return 0;
}

static int trevrpc_raw_client_open_stream(
    trevrpc_raw_client* client, trevrpc_msquic_stream** msquic_stream, trevrpc_wt_stream** wt_stream) {
    *msquic_stream = NULL;
    *wt_stream = NULL;
    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_msquic_conn_open_stream(client->msquic_conn, msquic_stream);
    }
    if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return trevrpc_wt_session_open_stream(client->wt_session, wt_stream);
    }
    return -EINVAL;
}

static int trevrpc_raw_client_write_frame(trevrpc_raw_client* client,
    trevrpc_msquic_stream* msquic_stream,
    trevrpc_wt_stream* wt_stream,
    const uint8_t* frame,
    size_t frame_len) {
    intptr_t written = 0;
    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        written = trevrpc_msquic_stream_write(msquic_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_MSQUIC_ERR_CLOSED;
    }
    if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        written = trevrpc_wt_stream_write(wt_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_WT_ERR_CLOSED;
    }
    return -EINVAL;
}

static int trevrpc_raw_client_shutdown_send(
    trevrpc_raw_client* client, trevrpc_msquic_stream* msquic_stream, trevrpc_wt_stream* wt_stream);

static int trevrpc_raw_client_write_final_frame(trevrpc_raw_client* client,
    trevrpc_msquic_stream* msquic_stream,
    trevrpc_wt_stream* wt_stream,
    const uint8_t* frame,
    size_t frame_len) {
    intptr_t written = 0;
    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        written = trevrpc_msquic_stream_write_fin(msquic_stream, frame, frame_len);
        if (written < 0) {
            return (int)written;
        }
        return (size_t)written == frame_len ? 0 : TREV_MSQUIC_ERR_CLOSED;
    }
    int err = trevrpc_raw_client_write_frame(client, msquic_stream, wt_stream, frame, frame_len);
    if (err != 0) {
        return err;
    }
    return trevrpc_raw_client_shutdown_send(client, msquic_stream, wt_stream);
}

static int trevrpc_raw_client_write_request(trevrpc_raw_client* client,
    trevrpc_msquic_stream* msquic_stream,
    trevrpc_wt_stream* wt_stream,
    const trevrpc_request* request,
    bool finish_send,
    bool borrow_body) {
    if (borrow_body && client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_wire_frame_parts parts = {0};
        int err = trevrpc_wire_encode_request_parts(request, client->max_frame_size, &parts);
        if (err == 0) {
            err = trevrpc_write_msquic_frame_parts(msquic_stream, client->max_frame_size, &parts, finish_send, NULL);
        }
        trevrpc_wire_frame_parts_reset(&parts);
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_request_view(request->service,
        request->service_len,
        request->method,
        request->method_len,
        request->kind,
        request->version,
        request->body,
        request->body_len,
        &request->metadata,
        request->timeout_nanos,
        client->max_frame_size,
        &frame,
        &frame_len);
    if (err == 0) {
        err = finish_send ? trevrpc_raw_client_write_final_frame(client, msquic_stream, wt_stream, frame, frame_len)
                          : trevrpc_raw_client_write_frame(client, msquic_stream, wt_stream, frame, frame_len);
    }
    free(frame);
    return err;
}

static int trevrpc_raw_client_shutdown_send(
    trevrpc_raw_client* client, trevrpc_msquic_stream* msquic_stream, trevrpc_wt_stream* wt_stream) {
    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_msquic_stream_shutdown_send(msquic_stream);
    }
    if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return trevrpc_wt_stream_shutdown_send(wt_stream);
    }
    return -EINVAL;
}

static intptr_t trevrpc_raw_client_read_frame(trevrpc_raw_client* client,
    trevrpc_msquic_stream* msquic_stream,
    trevrpc_wt_stream* wt_stream,
    uint8_t** body,
    size_t* body_len) {
    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        return trevrpc_msquic_stream_read_frame(msquic_stream, body, body_len, client->max_frame_size);
    }
    if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return trevrpc_wt_stream_read_frame(wt_stream, body, body_len, client->max_frame_size);
    }
    return -EINVAL;
}

static void trevrpc_raw_client_free_body(trevrpc_raw_client* client, uint8_t* body) {
    if (client != NULL && client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        trevrpc_wt_free(body);
        return;
    }
    trevrpc_msquic_free(body);
}

static void trevrpc_raw_client_close_raw_stream(
    trevrpc_raw_client* client, trevrpc_msquic_stream* msquic_stream, trevrpc_wt_stream* wt_stream) {
    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_msquic_stream_close(msquic_stream);
    } else if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        trevrpc_wt_stream_close(wt_stream);
    }
}

static void trevrpc_raw_client_abort_stream(
    uint32_t transport, trevrpc_msquic_stream* msquic_stream, trevrpc_wt_stream* wt_stream) {
    if (transport == TREVRPC_TRANSPORT_KIND_MSQUIC && msquic_stream != NULL) {
        (void)trevrpc_msquic_stream_abort(msquic_stream);
    } else if (transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT && wt_stream != NULL) {
        (void)trevrpc_wt_stream_abort(wt_stream, 0);
    }
}

trevrpc_cancellation* trevrpc_cancellation_new(void) {
    trevrpc_cancellation* cancellation = calloc(1, sizeof(*cancellation));
    if (cancellation == NULL) {
        return NULL;
    }
    pthread_mutex_init(&cancellation->mutex, NULL);
    return cancellation;
}

void trevrpc_cancellation_cancel(trevrpc_cancellation* cancellation) {
    if (cancellation == NULL) {
        return;
    }

    uint32_t transport = 0;
    trevrpc_msquic_stream* msquic_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    pthread_mutex_lock(&cancellation->mutex);
    cancellation->cancelled = true;
    transport = cancellation->transport;
    msquic_stream = cancellation->msquic_stream;
    wt_stream = cancellation->wt_stream;
    pthread_mutex_unlock(&cancellation->mutex);

    trevrpc_raw_client_abort_stream(transport, msquic_stream, wt_stream);
}

int trevrpc_cancellation_cancelled(trevrpc_cancellation* cancellation) {
    if (cancellation == NULL) {
        return 0;
    }

    pthread_mutex_lock(&cancellation->mutex);
    bool cancelled = cancellation->cancelled;
    pthread_mutex_unlock(&cancellation->mutex);
    return cancelled ? 1 : 0;
}

void trevrpc_cancellation_free(trevrpc_cancellation* cancellation) {
    if (cancellation == NULL) {
        return;
    }

    pthread_mutex_destroy(&cancellation->mutex);
    free(cancellation);
}

static int trevrpc_cancellation_bind_raw_stream(trevrpc_cancellation* cancellation,
    uint32_t transport,
    trevrpc_msquic_stream* msquic_stream,
    trevrpc_wt_stream* wt_stream) {
    if (cancellation == NULL) {
        return 0;
    }

    pthread_mutex_lock(&cancellation->mutex);
    bool cancelled = cancellation->cancelled;
    if (!cancelled) {
        cancellation->transport = transport;
        cancellation->msquic_stream = msquic_stream;
        cancellation->wt_stream = wt_stream;
    }
    pthread_mutex_unlock(&cancellation->mutex);

    if (cancelled) {
        trevrpc_raw_client_abort_stream(transport, msquic_stream, wt_stream);
        return -ECANCELED;
    }
    return 0;
}

static void trevrpc_cancellation_unbind_raw_stream(
    trevrpc_cancellation* cancellation, trevrpc_msquic_stream* msquic_stream, trevrpc_wt_stream* wt_stream) {
    if (cancellation == NULL) {
        return;
    }

    pthread_mutex_lock(&cancellation->mutex);
    if (cancellation->msquic_stream == msquic_stream && cancellation->wt_stream == wt_stream) {
        cancellation->transport = 0;
        cancellation->msquic_stream = NULL;
        cancellation->wt_stream = NULL;
    }
    pthread_mutex_unlock(&cancellation->mutex);
}

static trevrpc_call_options trevrpc_resolve_call_options(const trevrpc_call_options* options) {
    trevrpc_call_options resolved = trevrpc_default_call_options();
    if (options == NULL) {
        return resolved;
    }

    resolved = *options;
    trevrpc_call_options defaults = trevrpc_default_call_options();
    if (resolved.max_response_body_size == 0) {
        resolved.max_response_body_size = defaults.max_response_body_size;
    }
    if (resolved.max_response_messages == 0) {
        resolved.max_response_messages = defaults.max_response_messages;
    }
    if (resolved.max_response_stream_body_size == 0) {
        resolved.max_response_stream_body_size = defaults.max_response_stream_body_size;
    }
    return resolved;
}

static const trevrpc_request* trevrpc_apply_call_options_to_request(
    const trevrpc_request* request, const trevrpc_call_options* options, trevrpc_request* scratch) {
    if (options == NULL || request == NULL || (options->metadata == NULL && options->timeout_nanos == 0)) {
        return request;
    }

    *scratch = *request;
    if (options->metadata != NULL) {
        scratch->metadata = *options->metadata;
    }
    if (options->timeout_nanos != 0) {
        scratch->timeout_nanos = options->timeout_nanos;
    }
    return scratch;
}

static int trevrpc_check_unary_response_limits(const trevrpc_response* response, const trevrpc_call_options* options) {
    if (response == NULL || options == NULL || options->max_response_body_size < 0) {
        return 0;
    }
    return response->body_len <= (uint64_t)options->max_response_body_size ? 0 : TREVRPC_ERR_FRAME_TOO_LARGE;
}

static void trevrpc_stream_apply_recv_call_options(trevrpc_stream* stream, const trevrpc_call_options* options) {
    if (stream == NULL || options == NULL) {
        return;
    }

    stream->has_recv_limits = true;
    stream->max_recv_stream_messages = options->max_response_messages;
    stream->max_recv_stream_body_size = options->max_response_stream_body_size;
    stream->stream_idle_timeout_nanos = options->response_idle_timeout_nanos;
}

int trevrpc_raw_client_call_unary(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_response** out_response) {
    trevrpc_request request = {
        .service = service,
        .service_len = service != NULL ? strlen(service) : 0,
        .method = method,
        .method_len = method != NULL ? strlen(method) : 0,
        .body = body,
        .body_len = body_len,
        .kind = TREVRPC_RPC_KIND_UNARY,
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_call_request(client, &request, out_response);
}

int trevrpc_raw_client_call_unary_with_options(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_call_options* options,
    trevrpc_response** out_response) {
    trevrpc_request request = {
        .service = service,
        .service_len = service != NULL ? strlen(service) : 0,
        .method = method,
        .method_len = method != NULL ? strlen(method) : 0,
        .body = body,
        .body_len = body_len,
        .kind = TREVRPC_RPC_KIND_UNARY,
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_call_request_with_options(client, &request, options, out_response);
}

int trevrpc_raw_client_call_request(
    trevrpc_raw_client* client, const trevrpc_request* request, trevrpc_response** out_response) {
    return trevrpc_raw_client_call_request_cancellable(client, request, NULL, out_response);
}

static int trevrpc_raw_client_call_request_internal(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    const trevrpc_call_options* options,
    bool apply_options,
    bool borrow_request_body,
    trevrpc_response** out_response) {
    if (client == NULL || out_response == NULL) {
        return -EINVAL;
    }
    *out_response = NULL;
    if (request == NULL) {
        return -EINVAL;
    }
    if (request->kind != TREVRPC_RPC_KIND_UNARY) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }
    if (request->version != TREVRPC_WIRE_VERSION) {
        return TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION;
    }

    trevrpc_call_options resolved_options = {0};
    trevrpc_request request_with_options = {0};
    if (apply_options) {
        resolved_options = trevrpc_resolve_call_options(options);
        cancellation = resolved_options.cancellation;
        request = trevrpc_apply_call_options_to_request(request, &resolved_options, &request_with_options);
    }
    if (trevrpc_cancellation_cancelled(cancellation)) {
        return -ECANCELED;
    }

    trevrpc_msquic_stream* msquic_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    int err = trevrpc_raw_client_open_stream(client, &msquic_stream, &wt_stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_cancellation_bind_raw_stream(cancellation, client->transport, msquic_stream, wt_stream);
    if (err != 0) {
        trevrpc_raw_client_close_raw_stream(client, msquic_stream, wt_stream);
        return err;
    }

    err = trevrpc_raw_client_write_request(client, msquic_stream, wt_stream, request, true, borrow_request_body);

    uint8_t* response_body = NULL;
    size_t response_body_len = 0;
    bool response_body_taken = false;
    if (err == 0) {
        intptr_t read =
            trevrpc_raw_client_read_frame(client, msquic_stream, wt_stream, &response_body, &response_body_len);
        if (read < 0) {
            err = (int)read;
        } else if (read == 0) {
            err = TREV_MSQUIC_ERR_CLOSED;
        }
    }
    if (err == 0) {
        err = trevrpc_wire_decode_response_take(response_body, response_body_len, out_response, &response_body_taken);
    }
    if (err == 0 && apply_options) {
        err = trevrpc_check_unary_response_limits(*out_response, &resolved_options);
        if (err != 0) {
            trevrpc_response_free(*out_response);
            *out_response = NULL;
        }
    }

    trevrpc_cancellation_unbind_raw_stream(cancellation, msquic_stream, wt_stream);
    if (!response_body_taken) {
        trevrpc_raw_client_free_body(client, response_body);
    }
    trevrpc_raw_client_close_raw_stream(client, msquic_stream, wt_stream);
    if (err == 0 && trevrpc_cancellation_cancelled(cancellation)) {
        trevrpc_response_free(*out_response);
        *out_response = NULL;
        return -ECANCELED;
    }
    return err;
}

int trevrpc_raw_client_call_request_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_response** out_response) {
    return trevrpc_raw_client_call_request_internal(client, request, cancellation, NULL, false, false, out_response);
}

int trevrpc_raw_client_call_request_borrowed_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_response** out_response) {
    return trevrpc_raw_client_call_request_internal(client, request, cancellation, NULL, false, true, out_response);
}

int trevrpc_raw_client_call_request_with_options(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options* options,
    trevrpc_response** out_response) {
    return trevrpc_raw_client_call_request_internal(client, request, NULL, options, true, false, out_response);
}

int trevrpc_raw_client_start_stream(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** out_stream) {
    trevrpc_request request = {
        .service = service,
        .service_len = service != NULL ? strlen(service) : 0,
        .method = method,
        .method_len = method != NULL ? strlen(method) : 0,
        .body = body,
        .body_len = body_len,
        .kind = kind,
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_start_stream_request(client, &request, out_stream);
}

int trevrpc_raw_client_start_stream_with_options(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_call_options* options,
    trevrpc_stream** out_stream) {
    trevrpc_request request = {
        .service = service,
        .service_len = service != NULL ? strlen(service) : 0,
        .method = method,
        .method_len = method != NULL ? strlen(method) : 0,
        .body = body,
        .body_len = body_len,
        .kind = kind,
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_start_stream_request_with_options(client, &request, options, out_stream);
}

int trevrpc_raw_client_start_stream_request(
    trevrpc_raw_client* client, const trevrpc_request* request, trevrpc_stream** out_stream) {
    return trevrpc_raw_client_start_stream_request_cancellable(client, request, NULL, out_stream);
}

static int trevrpc_raw_client_start_stream_request_internal(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    const trevrpc_call_options* options,
    bool apply_options,
    bool borrow_request_body,
    trevrpc_stream** out_stream) {
    if (client == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;
    if (request == NULL) {
        return -EINVAL;
    }
    if (request->kind == TREVRPC_RPC_KIND_UNARY || request->kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }
    if (request->version != TREVRPC_WIRE_VERSION) {
        return TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION;
    }

    trevrpc_call_options resolved_options = {0};
    trevrpc_request request_with_options = {0};
    if (apply_options) {
        resolved_options = trevrpc_resolve_call_options(options);
        cancellation = resolved_options.cancellation;
        request = trevrpc_apply_call_options_to_request(request, &resolved_options, &request_with_options);
    }
    if (trevrpc_cancellation_cancelled(cancellation)) {
        return -ECANCELED;
    }

    trevrpc_msquic_stream* msquic_stream = NULL;
    trevrpc_wt_stream* wt_stream = NULL;
    int err = trevrpc_raw_client_open_stream(client, &msquic_stream, &wt_stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_cancellation_bind_raw_stream(cancellation, client->transport, msquic_stream, wt_stream);
    if (err != 0) {
        trevrpc_raw_client_close_raw_stream(client, msquic_stream, wt_stream);
        return err;
    }

    err = trevrpc_raw_client_write_request(client, msquic_stream, wt_stream, request, false, borrow_request_body);
    if (err != 0) {
        trevrpc_cancellation_unbind_raw_stream(cancellation, msquic_stream, wt_stream);
        trevrpc_raw_client_close_raw_stream(client, msquic_stream, wt_stream);
        return err;
    }
    if (request->kind == TREVRPC_RPC_KIND_SERVER_STREAMING) {
        err = trevrpc_raw_client_shutdown_send(client, msquic_stream, wt_stream);
        if (err != 0) {
            trevrpc_cancellation_unbind_raw_stream(cancellation, msquic_stream, wt_stream);
            trevrpc_raw_client_close_raw_stream(client, msquic_stream, wt_stream);
            return err;
        }
    }

    trevrpc_stream* stream = client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC
                                 ? trevrpc_stream_alloc_msquic(msquic_stream, client->max_frame_size, true)
                                 : trevrpc_stream_alloc_webtransport(wt_stream, client->max_frame_size, true);
    if (stream == NULL) {
        trevrpc_cancellation_unbind_raw_stream(cancellation, msquic_stream, wt_stream);
        trevrpc_raw_client_close_raw_stream(client, msquic_stream, wt_stream);
        return -ENOMEM;
    }
    if (apply_options) {
        trevrpc_stream_apply_recv_call_options(stream, &resolved_options);
    }

    trevrpc_cancellation_unbind_raw_stream(cancellation, msquic_stream, wt_stream);
    if (trevrpc_cancellation_cancelled(cancellation)) {
        trevrpc_stream_close(stream);
        return -ECANCELED;
    }
    *out_stream = stream;
    return 0;
}

int trevrpc_raw_client_start_stream_request_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_stream** out_stream) {
    return trevrpc_raw_client_start_stream_request_internal(
        client, request, cancellation, NULL, false, false, out_stream);
}

int trevrpc_raw_client_start_stream_request_borrowed_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_stream** out_stream) {
    return trevrpc_raw_client_start_stream_request_internal(
        client, request, cancellation, NULL, false, true, out_stream);
}

int trevrpc_raw_client_start_stream_request_with_options(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options* options,
    trevrpc_stream** out_stream) {
    return trevrpc_raw_client_start_stream_request_internal(client, request, NULL, options, true, false, out_stream);
}

void trevrpc_raw_client_shutdown(trevrpc_raw_client* client) {
    if (client == NULL) {
        return;
    }

    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_msquic_conn_shutdown(client->msquic_conn);
    } else if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        trevrpc_wt_session_shutdown(client->wt_session);
    }
}

void trevrpc_raw_client_close(trevrpc_raw_client* client) {
    if (client == NULL) {
        return;
    }

    if (client->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_msquic_conn_close(client->msquic_conn);
    } else if (client->transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        trevrpc_wt_session_close(client->wt_session);
    }
    free(client);
}

static bool trevrpc_method_matches(
    const trevrpc_method* method, const char* service, size_t service_len, const char* name, size_t name_len) {
    return method->service_len == service_len && method->method_len == name_len &&
           memcmp(method->service, service, service_len) == 0 && memcmp(method->method, name, name_len) == 0;
}

static int trevrpc_server_new(size_t max_frame_size, trevrpc_server** out_server) {
    if (out_server == NULL) {
        return -EINVAL;
    }
    *out_server = NULL;

    trevrpc_server* server = calloc(1, sizeof(*server));
    if (server == NULL) {
        return -ENOMEM;
    }
    server->max_frame_size = trevrpc_effective_max_frame_size(max_frame_size);
    server->options = trevrpc_normalize_server_options(trevrpc_default_server_options());
    pthread_mutex_init(&server->mutex, NULL);
    pthread_cond_init(&server->cond, NULL);
    *out_server = server;
    return 0;
}

int trevrpc_server_listen(const trevrpc_server_config* config, trevrpc_server** out_server) {
    if (config == NULL || out_server == NULL) {
        return -EINVAL;
    }
    *out_server = NULL;

    trevrpc_server_config effective = trevrpc_effective_server_config(config);
    if (effective.host == NULL || effective.cert_file == NULL || effective.key_file == NULL ||
        effective.max_idle_timeout_ms > UINT32_MAX ||
        (effective.enable_http3 && (effective.http3_path == NULL || effective.http3_path[0] != '/'))) {
        return -EINVAL;
    }

    trevrpc_server* server = NULL;
    int err = trevrpc_server_new(effective.max_frame_size, &server);
    if (err != 0) {
        return err;
    }

    trevrpc_wt_config wt_config = trevrpc_make_server_wt_config(&effective);
    server->shared_wt_path = trevrpc_copy_cstring(effective.webtransport_path);
    server->shared_wt_origin = trevrpc_copy_cstring(effective.webtransport_origin);
    server->shared_h3_path = trevrpc_copy_cstring(effective.http3_path);
    if ((effective.webtransport_path != NULL && server->shared_wt_path == NULL) ||
        (effective.webtransport_origin != NULL && server->shared_wt_origin == NULL) ||
        (effective.http3_path != NULL && server->shared_h3_path == NULL)) {
        trevrpc_server_close(server);
        return -ENOMEM;
    }
    wt_config.path = server->shared_wt_path;
    wt_config.origin = server->shared_wt_origin;
    server->shared_wt_config = wt_config;
    server->enable_http3 = effective.enable_http3;
    server->http3_admission = effective.http3_admission;
    server->http3_admission_user_data = effective.http3_admission_user_data;

    trevrpc_msquic_config msquic_config = trevrpc_make_server_msquic_config(&effective);
    if (msquic_config.cert_file == NULL || msquic_config.key_file == NULL) {
        trevrpc_server_close(server);
        return -EINVAL;
    }
    const trevrpc_msquic_alpn alpns[] = {
        {.alpn = TREVRPC_ALPN, .alpn_len = (uint32_t)(sizeof(TREVRPC_ALPN) - 1)},
        {.alpn = TREVRPC_H3_ALPN, .alpn_len = (uint32_t)(sizeof(TREVRPC_H3_ALPN) - 1)},
    };
    err = trevrpc_msquic_listen_alpns(effective.host,
        effective.port,
        &msquic_config,
        alpns,
        sizeof(alpns) / sizeof(alpns[0]),
        &server->shared_listener);
    if (err != 0) {
        trevrpc_server_close(server);
        return err;
    }

    *out_server = server;
    return 0;
}

int trevrpc_server_port(trevrpc_server* server, uint16_t* port) {
    if (server == NULL || port == NULL) {
        return -EINVAL;
    }
    if (server->shared_listener != NULL) {
        return trevrpc_msquic_listener_port(server->shared_listener, port);
    }
    if (server->listener != NULL) {
        return trevrpc_msquic_listener_port(server->listener, port);
    }
    if (server->wt_listener != NULL) {
        return trevrpc_wt_listener_port(server->wt_listener, port);
    }
    return -EINVAL;
}

#ifdef TREVRPC_TESTING
int trevrpc_test_make_client_msquic_config(const trevrpc_config* config, trevrpc_msquic_config* out_config) {
    if (out_config == NULL) {
        return -EINVAL;
    }
    *out_config = trevrpc_make_msquic_config(config);
    return 0;
}

int trevrpc_test_make_server_msquic_config(const trevrpc_server_config* config,
    trevrpc_server_config* out_effective,
    trevrpc_msquic_config* out_msquic_config,
    trevrpc_wt_config* out_wt_config) {
    if (out_effective == NULL || out_msquic_config == NULL || out_wt_config == NULL) {
        return -EINVAL;
    }
    *out_effective = trevrpc_effective_server_config(config);
    *out_msquic_config = trevrpc_make_server_msquic_config(out_effective);
    *out_wt_config = trevrpc_make_server_wt_config(out_effective);
    return 0;
}

int trevrpc_test_server_new(const trevrpc_config* config, trevrpc_server** out_server) {
    if (out_server == NULL) {
        return -EINVAL;
    }
    *out_server = NULL;

    return trevrpc_server_new(config == NULL ? 0 : config->max_frame_size, out_server);
}

void trevrpc_test_server_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream) {
    trevrpc_test_current_server = server;
    trevrpc_stream rpc_stream = trevrpc_stream_ref_msquic(stream, server->max_frame_size);
    (void)trevrpc_handle_stream(server, &rpc_stream, NULL, false, NULL);
    trevrpc_test_current_server = NULL;
}

void trevrpc_test_server_freeze_routes(trevrpc_server* server) {
    trevrpc_server_freeze_routes(server);
}

void trevrpc_test_server_handle_wt_stream(trevrpc_server* server, trevrpc_wt_stream* stream) {
    trevrpc_test_current_server = server;
    trevrpc_stream rpc_stream = trevrpc_stream_ref_webtransport(stream, server->max_frame_size);
    (void)trevrpc_handle_stream(server, &rpc_stream, NULL, false, NULL);
    trevrpc_test_current_server = NULL;
}

trevrpc_wt_session* trevrpc_test_client_webtransport_session(trevrpc_raw_client* client) {
    if (client == NULL || client->transport != TREVRPC_TRANSPORT_KIND_WEBTRANSPORT) {
        return NULL;
    }
    return client->wt_session;
}

int trevrpc_test_server_webtransport_port(trevrpc_server* server, uint16_t* port) {
    if (server == NULL || port == NULL || (server->wt_listener == NULL && server->shared_listener == NULL)) {
        return -EINVAL;
    }
    if (server->shared_listener != NULL) {
        return trevrpc_msquic_listener_port(server->shared_listener, port);
    }
    return trevrpc_wt_listener_port(server->wt_listener, port);
}

uint32_t trevrpc_test_status_from_error(int err, const char** message) {
    return trevrpc_status_from_error(err, message);
}

uint32_t trevrpc_test_transport_status_from_error(int err, const char** message) {
    return trevrpc_transport_status_from_error(err, message);
}

size_t trevrpc_test_server_stream_status_count(trevrpc_server* server) {
    return server == NULL ? 0 : server->test_stream_status_count;
}

uint32_t trevrpc_test_server_last_stream_status(trevrpc_server* server) {
    return server == NULL ? TREVRPC_STATUS_UNKNOWN : server->test_last_stream_status;
}

#endif

int trevrpc_server_set_options(trevrpc_server* server, const trevrpc_server_options* options) {
    if (server == NULL || options == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    server->options = trevrpc_normalize_server_options(*options);
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

int trevrpc_server_get_options(trevrpc_server* server, trevrpc_server_options* options) {
    if (server == NULL || options == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    *options = server->options;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

int trevrpc_server_set_authorizer(trevrpc_server* server, trevrpc_authorizer authorizer, void* user_data) {
    if (server == NULL || authorizer == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    server->authorizer = authorizer;
    server->authorizer_user_data = user_data;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

void trevrpc_server_clear_authorizer(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->authorizer = NULL;
    server->authorizer_user_data = NULL;
    pthread_mutex_unlock(&server->mutex);
}

int trevrpc_server_set_metrics(trevrpc_server* server, const trevrpc_metrics* metrics) {
    if (server == NULL || metrics == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    server->metrics = *metrics;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

void trevrpc_server_clear_metrics(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->metrics = (trevrpc_metrics){0};
    pthread_mutex_unlock(&server->mutex);
}

int trevrpc_server_set_transport_observer(trevrpc_server* server, const trevrpc_transport_observer* observer) {
    if (server == NULL || observer == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    server->transport_observer = *observer;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

void trevrpc_server_clear_transport_observer(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->transport_observer = (trevrpc_transport_observer){0};
    pthread_mutex_unlock(&server->mutex);
}

int trevrpc_server_set_logger(trevrpc_server* server, const trevrpc_logger* logger) {
    if (server == NULL || logger == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    server->logger = *logger;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

void trevrpc_server_clear_logger(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->logger = (trevrpc_logger){0};
    pthread_mutex_unlock(&server->mutex);
}

int trevrpc_server_register_unary(
    trevrpc_server* server, const char* service, const char* method, trevrpc_unary_handler handler, void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = TREVRPC_RPC_KIND_UNARY;
    registered->handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (atomic_load_explicit(&server->routes_frozen, memory_order_acquire)) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return -EALREADY;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

int trevrpc_server_register_streaming(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_stream_handler handler,
    void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }
    if (kind == TREVRPC_RPC_KIND_UNARY || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = kind;
    registered->stream_handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (atomic_load_explicit(&server->routes_frozen, memory_order_acquire)) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return -EALREADY;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

int trevrpc_server_register_call(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_call_handler handler,
    void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }
    if (kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = kind;
    registered->call_handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (atomic_load_explicit(&server->routes_frozen, memory_order_acquire)) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return -EALREADY;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

static bool trevrpc_server_task_start(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool start = !server->shutting_down;
    if (start) {
        server->active_tasks++;
    }
    pthread_mutex_unlock(&server->mutex);
    return start;
}

static void trevrpc_server_task_finish(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    if (server->active_tasks > 0) {
        server->active_tasks--;
    }
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static bool trevrpc_limit_try_acquire(int64_t limit, size_t* active) {
    if (limit > 0 && *active >= (size_t)limit) {
        return false;
    }
    if (*active == SIZE_MAX) {
        return false;
    }

    *active += 1;
    return true;
}

static void trevrpc_limit_release(size_t* active) {
    if (*active > 0) {
        *active -= 1;
    }
}

static bool trevrpc_server_connection_try_start(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool start = !server->shutting_down &&
                 trevrpc_limit_try_acquire(server->options.max_concurrent_connections, &server->active_connections);
    pthread_mutex_unlock(&server->mutex);
    return start;
}

static void trevrpc_server_connection_finish(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_limit_release(&server->active_connections);
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static bool trevrpc_server_request_try_start(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool start = !server->shutting_down &&
                 trevrpc_limit_try_acquire(server->options.max_concurrent_requests, &server->active_requests);
    pthread_mutex_unlock(&server->mutex);
    return start;
}

static void trevrpc_server_request_finish(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_limit_release(&server->active_requests);
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

#ifdef TREVRPC_TESTING
bool trevrpc_test_server_request_try_start(trevrpc_server* server) {
    return trevrpc_server_request_try_start(server);
}

void trevrpc_test_server_request_finish(trevrpc_server* server) {
    trevrpc_server_request_finish(server);
}

bool trevrpc_test_server_connection_try_start(trevrpc_server* server) {
    return trevrpc_server_connection_try_start(server);
}

void trevrpc_test_server_connection_finish(trevrpc_server* server) {
    trevrpc_server_connection_finish(server);
}
#endif

static int trevrpc_conn_stream_limiter_init(trevrpc_conn_stream_limiter* limiter) {
    int err = pthread_mutex_init(&limiter->mutex, NULL);
    if (err != 0) {
        return -err;
    }

    err = pthread_cond_init(&limiter->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&limiter->mutex);
        return -err;
    }

    limiter->active_streams = 0;
    limiter->refs = 1;
    return 0;
}

static trevrpc_conn_stream_limiter* trevrpc_conn_stream_limiter_new(void) {
    trevrpc_conn_stream_limiter* limiter = calloc(1, sizeof(*limiter));
    if (limiter == NULL) {
        return NULL;
    }
    if (trevrpc_conn_stream_limiter_init(limiter) != 0) {
        free(limiter);
        return NULL;
    }
    return limiter;
}

static bool trevrpc_conn_stream_limiter_retain(trevrpc_conn_stream_limiter* limiter) {
    pthread_mutex_lock(&limiter->mutex);
    bool retained = limiter->refs < SIZE_MAX;
    if (retained) {
        limiter->refs++;
    }
    pthread_mutex_unlock(&limiter->mutex);
    return retained;
}

static bool trevrpc_conn_stream_try_start(trevrpc_conn_stream_limiter* limiter, int64_t limit) {
    pthread_mutex_lock(&limiter->mutex);
    bool start = trevrpc_limit_try_acquire(limit, &limiter->active_streams);
    pthread_mutex_unlock(&limiter->mutex);
    return start;
}

static void trevrpc_conn_stream_finish(trevrpc_conn_stream_limiter* limiter) {
    pthread_mutex_lock(&limiter->mutex);
    trevrpc_limit_release(&limiter->active_streams);
    pthread_cond_broadcast(&limiter->cond);
    pthread_mutex_unlock(&limiter->mutex);
}

static void trevrpc_conn_stream_limiter_wait(trevrpc_conn_stream_limiter* limiter) {
    pthread_mutex_lock(&limiter->mutex);
    while (limiter->active_streams > 0) {
        pthread_cond_wait(&limiter->cond, &limiter->mutex);
    }
    pthread_mutex_unlock(&limiter->mutex);
}

static void trevrpc_conn_stream_limiter_destroy(trevrpc_conn_stream_limiter* limiter) {
    pthread_cond_destroy(&limiter->cond);
    pthread_mutex_destroy(&limiter->mutex);
}

static void trevrpc_conn_stream_limiter_release(trevrpc_conn_stream_limiter* limiter) {
    if (limiter == NULL) {
        return;
    }

    bool destroy = false;
    pthread_mutex_lock(&limiter->mutex);
    if (limiter->refs > 0) {
        limiter->refs--;
    }
    destroy = limiter->refs == 0;
    pthread_mutex_unlock(&limiter->mutex);

    if (destroy) {
        trevrpc_conn_stream_limiter_destroy(limiter);
        free(limiter);
    }
}

#ifdef TREVRPC_TESTING
trevrpc_conn_stream_limiter* trevrpc_test_conn_stream_limiter_new(void) {
    return trevrpc_conn_stream_limiter_new();
}

void trevrpc_test_conn_stream_limiter_release(trevrpc_conn_stream_limiter* limiter) {
    trevrpc_conn_stream_limiter_release(limiter);
}

int trevrpc_test_conn_stream_limiter_init(trevrpc_conn_stream_limiter* limiter) {
    return trevrpc_conn_stream_limiter_init(limiter);
}

bool trevrpc_test_conn_stream_try_start(trevrpc_conn_stream_limiter* limiter, int64_t limit) {
    return trevrpc_conn_stream_try_start(limiter, limit);
}

void trevrpc_test_conn_stream_finish(trevrpc_conn_stream_limiter* limiter) {
    trevrpc_conn_stream_finish(limiter);
}

void trevrpc_test_conn_stream_limiter_destroy(trevrpc_conn_stream_limiter* limiter) {
    trevrpc_conn_stream_limiter_destroy(limiter);
}
#endif

static bool trevrpc_server_is_shutting_down(trevrpc_server* server) {
    if (server == NULL) {
        return false;
    }

    pthread_mutex_lock(&server->mutex);
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);
    return shutting_down;
}

static trevrpc_server_options trevrpc_server_options_snapshot(trevrpc_server* server) {
    if (server == NULL) {
        return trevrpc_default_server_options();
    }

    pthread_mutex_lock(&server->mutex);
    trevrpc_server_options options = server->options;
    pthread_mutex_unlock(&server->mutex);
    return options;
}

static trevrpc_authorizer trevrpc_server_authorizer_snapshot(trevrpc_server* server, void** out_user_data) {
    if (server == NULL) {
        *out_user_data = NULL;
        return NULL;
    }

    pthread_mutex_lock(&server->mutex);
    trevrpc_authorizer authorizer = server->authorizer;
    *out_user_data = server->authorizer_user_data;
    pthread_mutex_unlock(&server->mutex);
    return authorizer;
}

static trevrpc_metrics trevrpc_server_metrics_snapshot(trevrpc_server* server) {
    if (server == NULL) {
        return (trevrpc_metrics){0};
    }

    pthread_mutex_lock(&server->mutex);
    trevrpc_metrics metrics = server->metrics;
    pthread_mutex_unlock(&server->mutex);
    return metrics;
}

static trevrpc_transport_observer trevrpc_server_transport_observer_snapshot(trevrpc_server* server) {
    if (server == NULL) {
        return (trevrpc_transport_observer){0};
    }

    pthread_mutex_lock(&server->mutex);
    trevrpc_transport_observer observer = server->transport_observer;
    pthread_mutex_unlock(&server->mutex);
    return observer;
}

static trevrpc_logger trevrpc_server_logger_snapshot(trevrpc_server* server) {
    if (server == NULL) {
        return (trevrpc_logger){0};
    }

    pthread_mutex_lock(&server->mutex);
    trevrpc_logger logger = server->logger;
    pthread_mutex_unlock(&server->mutex);
    return logger;
}

static void trevrpc_log(trevrpc_server* server,
    uint32_t level,
    const char* event_name,
    const char* message,
    const trevrpc_request* request,
    int error_code) {
    trevrpc_logger logger = trevrpc_server_logger_snapshot(server);
    if (logger.log == NULL) {
        return;
    }

    trevrpc_log_event event = {
        .level = level,
        .event = event_name,
        .event_len = event_name == NULL ? 0 : strlen(event_name),
        .message = message,
        .message_len = message == NULL ? 0 : strlen(message),
        .service = request == NULL ? NULL : request->service,
        .service_len = request == NULL ? 0 : request->service_len,
        .method = request == NULL ? NULL : request->method,
        .method_len = request == NULL ? 0 : request->method_len,
        .error_code = error_code,
    };
    logger.log(logger.user_data, &event);
}

static void trevrpc_transport_record_event(trevrpc_server* server, uint32_t kind, int error_code, const char* message) {
    trevrpc_transport_record_event_for_transport(
        server, kind, trevrpc_transport_event_transport(server), error_code, message);
}

static void trevrpc_transport_record_event_for_transport(
    trevrpc_server* server, uint32_t kind, uint32_t transport, int error_code, const char* message) {
    if (kind == TREVRPC_TRANSPORT_EVENT_LISTENER_ERROR || kind == TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR ||
        kind == TREVRPC_TRANSPORT_EVENT_STREAM_ERROR) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_ERROR, "transport.error", message, NULL, error_code);
    }

    trevrpc_transport_observer observer = trevrpc_server_transport_observer_snapshot(server);
    if (observer.transport_event == NULL) {
        return;
    }

    trevrpc_transport_event event = {
        .kind = kind,
        .transport = transport,
        .error_code = error_code,
        .message = message,
        .message_len = message == NULL ? 0 : strlen(message),
    };
    observer.transport_event(observer.user_data, &event);
}

static void trevrpc_metrics_record_started(trevrpc_server* server, const trevrpc_request* request) {
    trevrpc_metrics metrics = trevrpc_server_metrics_snapshot(server);
    if (metrics.rpc_started == NULL) {
        return;
    }

    trevrpc_rpc_started_event event = {
        .service = request->service,
        .service_len = request->service_len,
        .method = request->method,
        .method_len = request->method_len,
        .request_body_len = request->body_len,
    };
    metrics.rpc_started(metrics.user_data, &event);
}

static void trevrpc_metrics_record_finished(trevrpc_server* server,
    const trevrpc_request* request,
    size_t response_body_len,
    uint32_t status,
    const struct timespec* started_at) {
    trevrpc_metrics metrics = trevrpc_server_metrics_snapshot(server);
    if (metrics.rpc_finished == NULL) {
        return;
    }

    struct timespec finished_at = {0};
    uint64_t elapsed_nanos = 0;
    if (started_at != NULL && trevrpc_clock_now(&finished_at) == 0) {
        elapsed_nanos = trevrpc_timespec_diff_nanos(&finished_at, started_at);
    }

    trevrpc_rpc_finished_event event = {
        .service = request->service,
        .service_len = request->service_len,
        .method = request->method,
        .method_len = request->method_len,
        .request_body_len = request->body_len,
        .response_body_len = response_body_len,
        .status = trevrpc_status_code_from_uint32(status),
        .elapsed_nanos = elapsed_nanos,
    };
    metrics.rpc_finished(metrics.user_data, &event);
}

static void trevrpc_metrics_record_pre_handler(trevrpc_server* server, uint32_t status) {
    trevrpc_request request = {0};
    struct timespec started_at = {0};
    (void)trevrpc_clock_now(&started_at);
    trevrpc_metrics_record_started(server, &request);
    trevrpc_metrics_record_finished(server, &request, 0, status, &started_at);
}

static void trevrpc_server_shutdown_connections(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    for (trevrpc_server_conn_ref* ref = server->conns; ref != NULL; ref = ref->next) {
        if (ref->conn != NULL) {
            trevrpc_msquic_conn_shutdown(ref->conn);
        } else if (ref->wt_session != NULL) {
            trevrpc_wt_session_shutdown(ref->wt_session);
        } else if (ref->h3_conn != NULL) {
            trevrpc_h3_conn_shutdown(ref->h3_conn);
        }
    }
    pthread_mutex_unlock(&server->mutex);
}

static bool trevrpc_server_wait_for_tasks(trevrpc_server* server, uint64_t timeout_nanos) {
    if (timeout_nanos == 0) {
        pthread_mutex_lock(&server->mutex);
        while (server->active_tasks > 0) {
            pthread_cond_wait(&server->cond, &server->mutex);
        }
        pthread_mutex_unlock(&server->mutex);
        return true;
    }

    struct timespec deadline = {0};
    if (trevrpc_realtime_deadline(timeout_nanos, &deadline) != 0) {
        return false;
    }

    pthread_mutex_lock(&server->mutex);
    while (server->active_tasks > 0) {
        int err = pthread_cond_timedwait(&server->cond, &server->mutex, &deadline);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&server->mutex);
            return false;
        }
    }
    pthread_mutex_unlock(&server->mutex);
    return true;
}

static int trevrpc_server_conn_add(trevrpc_server* server,
    trevrpc_msquic_conn* conn,
    trevrpc_wt_session* wt_session,
    trevrpc_h3_conn* h3_conn,
    trevrpc_server_conn_ref** out_ref) {
    *out_ref = NULL;
    trevrpc_server_conn_ref* ref = malloc(sizeof(*ref));
    if (ref == NULL) {
        return -ENOMEM;
    }
    ref->conn = conn;
    ref->wt_session = wt_session;
    ref->h3_conn = h3_conn;

    pthread_mutex_lock(&server->mutex);
    ref->next = server->conns;
    server->conns = ref;
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);

    if (shutting_down) {
        if (conn != NULL) {
            trevrpc_msquic_conn_shutdown(conn);
        } else if (wt_session != NULL) {
            trevrpc_wt_session_shutdown(wt_session);
        } else if (h3_conn != NULL) {
            trevrpc_h3_conn_shutdown(h3_conn);
        }
    }

    *out_ref = ref;
    return 0;
}

static void trevrpc_server_conn_remove(trevrpc_server* server, trevrpc_server_conn_ref* ref) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_server_conn_ref** link = &server->conns;
    while (*link != NULL) {
        if (*link == ref) {
            *link = ref->next;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&server->mutex);
    free(ref);
}

static trevrpc_method* trevrpc_server_find_method(trevrpc_server* server, const trevrpc_request* request) {
    if (atomic_load_explicit(&server->routes_frozen, memory_order_acquire)) {
        for (trevrpc_method* method = server->methods; method != NULL; method = method->next) {
            if (trevrpc_method_matches(
                    method, request->service, request->service_len, request->method, request->method_len)) {
                return method;
            }
        }
        return NULL;
    }

    pthread_mutex_lock(&server->mutex);
    for (trevrpc_method* method = server->methods; method != NULL; method = method->next) {
        if (trevrpc_method_matches(
                method, request->service, request->service_len, request->method, request->method_len)) {
            pthread_mutex_unlock(&server->mutex);
            return method;
        }
    }
    pthread_mutex_unlock(&server->mutex);
    return NULL;
}

static void trevrpc_server_freeze_routes(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    atomic_store_explicit(&server->routes_frozen, true, memory_order_release);
    pthread_mutex_unlock(&server->mutex);
}

static void trevrpc_set_status(trevrpc_response* response, uint32_t status, const char* message) {
    response->status = status;
    if (message != NULL) {
        (void)trevrpc_response_set_message(response, message, strlen(message));
    }
}

static bool trevrpc_response_fields_valid(const trevrpc_response* response) {
    return response != NULL && (response->message != NULL || response->message_len == 0) &&
           (response->body != NULL || response->body_len == 0) && trevrpc_metadata_validate(&response->metadata) == 0;
}

static int trevrpc_server_write_response(trevrpc_stream* stream, trevrpc_response* response) {
    if (stream->transport == TREVRPC_TRANSPORT_KIND_MSQUIC) {
        trevrpc_wire_frame_parts parts = {0};
        int encode_err = trevrpc_wire_encode_response_parts(response, stream->max_frame_size, &parts);
        if (encode_err != 0 && response->status == TREVRPC_STATUS_OK) {
            trevrpc_response_reset(response);
            trevrpc_set_status(response, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "response frame exceeded maximum size");
            encode_err = trevrpc_wire_encode_response_parts(response, stream->max_frame_size, &parts);
        }
        int write_err =
            encode_err == 0 ? trevrpc_stream_write_msquic_frame_parts(stream, &parts, true, NULL) : encode_err;
        trevrpc_wire_frame_parts_reset(&parts);
        return write_err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int encode_err = trevrpc_wire_encode_response(response, stream->max_frame_size, &frame, &frame_len);
    if (encode_err != 0 && response->status == TREVRPC_STATUS_OK) {
        trevrpc_response_reset(response);
        trevrpc_set_status(response, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "response frame exceeded maximum size");
        encode_err = trevrpc_wire_encode_response(response, stream->max_frame_size, &frame, &frame_len);
    }
    int write_err = encode_err == 0 ? trevrpc_stream_write_final_frame(stream, frame, frame_len) : encode_err;
    free(frame);
    return write_err;
}

static void trevrpc_server_write_status(trevrpc_stream* stream, uint32_t status, const char* message) {
    trevrpc_response response = {0};
    trevrpc_set_status(&response, status, message);
    (void)trevrpc_server_write_response(stream, &response);
    trevrpc_response_reset(&response);
}

static void trevrpc_server_write_stream_status(trevrpc_stream* stream, uint32_t status, const char* message) {
    (void)trevrpc_stream_send_final_status_with_metadata(
        stream, status, message, message == NULL ? 0 : strlen(message), NULL);
}

static uint32_t trevrpc_status_from_error(int err, const char** message) {
    switch (err) {
    case TREVRPC_ERR_STREAM_LIMIT_EXCEEDED:
        *message = "stream limit exceeded";
        return TREVRPC_STATUS_RESOURCE_EXHAUSTED;
    case TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED:
        *message = "pending send budget exhausted";
        return TREVRPC_STATUS_RESOURCE_EXHAUSTED;
    case TREVRPC_ERR_STREAM_IDLE_TIMEOUT:
    case TREV_MSQUIC_ERR_TIMEOUT:
        *message = "stream idle timeout";
        return TREVRPC_STATUS_UNAVAILABLE;
    case TREVRPC_ERR_FRAME_TOO_LARGE:
    case TREV_MSQUIC_ERR_FRAME_TOO_LARGE:
        *message = "frame exceeded maximum size";
        return TREVRPC_STATUS_RESOURCE_EXHAUSTED;
    case TREVRPC_ERR_INVALID_FRAME:
    case TREVRPC_ERR_UNSUPPORTED_RPC_KIND:
        *message = "invalid request";
        return TREVRPC_STATUS_INVALID_ARGUMENT;
    case TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION:
        *message = "unsupported TrevRPC wire version";
        return TREVRPC_STATUS_FAILED_PRECONDITION;
    case -ETIMEDOUT:
        *message = "RPC deadline exceeded";
        return TREVRPC_STATUS_DEADLINE_EXCEEDED;
    case -ECANCELED:
        *message = "RPC cancelled";
        return TREVRPC_STATUS_CANCELLED;
    default:
        *message = "handler failed";
        return TREVRPC_STATUS_INTERNAL;
    }
}

static uint32_t trevrpc_transport_status_from_error(int err, const char** message) {
    switch (err) {
    case TREVRPC_ERR_INVALID_FRAME:
    case TREVRPC_ERR_UNSUPPORTED_RPC_KIND:
        *message = "invalid request";
        return TREVRPC_STATUS_INVALID_ARGUMENT;
    case TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION:
        *message = "unsupported TrevRPC wire version";
        return TREVRPC_STATUS_FAILED_PRECONDITION;
    case TREVRPC_ERR_FRAME_TOO_LARGE:
    case TREV_MSQUIC_ERR_FRAME_TOO_LARGE:
    case TREV_WT_ERR_FRAME_TOO_LARGE:
        *message = "request frame exceeded maximum size";
        return TREVRPC_STATUS_RESOURCE_EXHAUSTED;
    case TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED:
        *message = "pending send budget exhausted";
        return TREVRPC_STATUS_RESOURCE_EXHAUSTED;
    case TREV_MSQUIC_ERR_CLOSED:
    case TREV_WT_ERR_CLOSED:
    case TREV_WT_ERR_REJECTED:
    case -ECANCELED:
        *message = "transport closed";
        return TREVRPC_STATUS_CANCELLED;
    case TREV_MSQUIC_ERR_TIMEOUT:
    case -ETIMEDOUT:
        *message = "transport deadline exceeded";
        return TREVRPC_STATUS_DEADLINE_EXCEEDED;
    default:
        *message = "transport unavailable";
        return TREVRPC_STATUS_UNAVAILABLE;
    }
}

static bool trevrpc_error_is_transport_failure(int err) {
    switch (err) {
    case TREV_MSQUIC_ERR_CLOSED:
    case TREV_WT_ERR_CLOSED:
    case TREV_WT_ERR_REJECTED:
    case TREV_MSQUIC_ERR_TIMEOUT:
    case -ECANCELED:
    case -ETIMEDOUT:
        return true;
    default:
        return false;
    }
}

static trevrpc_call* trevrpc_call_new(trevrpc_server* server,
    const trevrpc_stream* stream,
    const trevrpc_request* request,
    const trevrpc_call_context* context,
    const struct timespec* started_at,
    uint8_t* request_frame_body,
    trevrpc_conn_stream_limiter* stream_limiter,
    bool server_task_active) {
    trevrpc_call* call = calloc(1, sizeof(*call));
    if (call == NULL) {
        return NULL;
    }
    int err = pthread_mutex_init(&call->mutex, NULL);
    if (err != 0) {
        free(call);
        return NULL;
    }

    call->server = server;
    call->stream = *stream;
    call->request = *request;
    call->context = *context;
    call->stream.context = &call->context;
    call->started_at = *started_at;
    call->request_frame_body = request_frame_body;
    call->stream_limiter = stream_limiter;
    call->refs = 1;
    call->final_status = TREVRPC_STATUS_OK;
    call->server_task_active = server_task_active;
    call->stream_limiter_active = stream_limiter != NULL;
    call->close_stream_active = server_task_active || stream_limiter != NULL;

    if (call->request.kind != TREVRPC_RPC_KIND_UNARY) {
        trevrpc_server_options options = trevrpc_server_options_snapshot(server);
        call->stream.max_stream_messages = options.max_stream_messages;
        call->stream.max_stream_body_size = options.max_stream_body_size;
        call->stream.stream_idle_timeout_nanos = options.stream_idle_timeout_nanos;
        call->stream.failure_status = TREVRPC_STATUS_OK;
        (void)trevrpc_stream_start_response_idle_timer(&call->stream);
    }

    return call;
}

static bool trevrpc_call_mark_completed(trevrpc_call* call) {
    bool complete = false;
    pthread_mutex_lock(&call->mutex);
    if (!call->completed) {
        call->completed = true;
        complete = true;
    }
    pthread_mutex_unlock(&call->mutex);
    return complete;
}

static bool trevrpc_call_completed(trevrpc_call* call) {
    pthread_mutex_lock(&call->mutex);
    bool completed = call->completed;
    pthread_mutex_unlock(&call->mutex);
    return completed;
}

static bool trevrpc_call_deferred(trevrpc_call* call) {
    pthread_mutex_lock(&call->mutex);
    bool deferred = call->deferred;
    pthread_mutex_unlock(&call->mutex);
    return deferred;
}

static void trevrpc_call_cleanup(trevrpc_call* call) {
    trevrpc_metrics_record_finished(
        call->server, &call->request, call->response_body_len, call->final_status, &call->started_at);
    trevrpc_server_request_finish(call->server);
    trevrpc_request_reset(&call->request);
    trevrpc_stream_free_body(&call->stream, call->request_frame_body);
    if (call->close_stream_active) {
        trevrpc_transport_record_event_for_transport(
            call->server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, call->stream.transport, 0, NULL);
        trevrpc_stream_close_raw(&call->stream);
    }
    if (call->stream_limiter_active) {
        trevrpc_conn_stream_finish(call->stream_limiter);
        trevrpc_conn_stream_limiter_release(call->stream_limiter);
    }
    if (call->server_task_active) {
        trevrpc_server_task_finish(call->server);
    }
    pthread_mutex_destroy(&call->mutex);
    free(call);
}

void trevrpc_call_release(trevrpc_call* call) {
    if (call == NULL) {
        return;
    }

    bool destroy = false;
    pthread_mutex_lock(&call->mutex);
    if (call->refs > 0) {
        call->refs--;
    }
    destroy = call->refs == 0 && call->completed;
    pthread_mutex_unlock(&call->mutex);

    if (destroy) {
        trevrpc_call_cleanup(call);
    }
}

int trevrpc_call_retain(trevrpc_call* call) {
    if (call == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&call->mutex);
    if (call->refs == 0 || call->refs == SIZE_MAX) {
        int err = call->refs == 0 ? -EALREADY : -EOVERFLOW;
        pthread_mutex_unlock(&call->mutex);
        return err;
    }
    call->refs++;
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

static bool trevrpc_call_should_release_after_complete(trevrpc_call* call) {
    bool release = false;
    pthread_mutex_lock(&call->mutex);
    release = call->deferred;
    pthread_mutex_unlock(&call->mutex);
    return release;
}

const trevrpc_request* trevrpc_call_request(const trevrpc_call* call) {
    return call == NULL ? NULL : &call->request;
}

const trevrpc_call_context* trevrpc_call_get_context(const trevrpc_call* call) {
    return call == NULL ? NULL : &call->context;
}

trevrpc_stream* trevrpc_call_stream(trevrpc_call* call) {
    if (call == NULL || call->request.kind == TREVRPC_RPC_KIND_UNARY) {
        return NULL;
    }
    return &call->stream;
}

void trevrpc_call_cancel(trevrpc_call* call) {
    if (call != NULL) {
        trevrpc_stream_cancel(&call->stream);
    }
}

int trevrpc_call_defer(trevrpc_call* call) {
    if (call == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&call->mutex);
    if (call->completed) {
        pthread_mutex_unlock(&call->mutex);
        return -EALREADY;
    }
    if (!call->deferred) {
        call->deferred = true;
        call->refs++;
    }
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

int trevrpc_call_respond(trevrpc_call* call, trevrpc_response* response) {
    if (call == NULL || response == NULL) {
        return -EINVAL;
    }
    if (call->request.kind != TREVRPC_RPC_KIND_UNARY) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }
    if (!trevrpc_call_mark_completed(call)) {
        return -EALREADY;
    }

    trevrpc_response fallback = {0};
    trevrpc_response* out = response;
    if (trevrpc_call_context_deadline_expired(&call->context)) {
        trevrpc_set_status(&fallback, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
        out = &fallback;
    } else if (!trevrpc_response_fields_valid(response)) {
        trevrpc_log(call->server,
            TREVRPC_LOG_LEVEL_ERROR,
            "rpc.handler_invalid_response",
            "handler produced invalid response",
            &call->request,
            -EINVAL);
        trevrpc_set_status(&fallback, TREVRPC_STATUS_INTERNAL, "handler produced invalid response");
        out = &fallback;
    }

    int write_err = trevrpc_server_write_response(&call->stream, out);
    if (write_err != 0) {
        trevrpc_stream_cancel(&call->stream);
    }
    call->response_body_len = out->body_len;
    call->final_status = out->status;
    trevrpc_response_reset(&fallback);
    if (trevrpc_call_should_release_after_complete(call)) {
        trevrpc_call_release(call);
    }
    return write_err;
}

static int trevrpc_call_write_stream_finish(
    trevrpc_call* call, uint32_t status, const char* message, size_t message_len, const trevrpc_metadata* metadata) {
    bool status_was_sent = call->stream.sent_status;
    int write_err = 0;
    if (trevrpc_call_context_deadline_expired(&call->context) && !call->stream.sent_status) {
        call->stream.context = NULL;
        call->final_status = TREVRPC_STATUS_DEADLINE_EXCEEDED;
        write_err = trevrpc_stream_send_final_status_with_metadata(&call->stream,
            TREVRPC_STATUS_DEADLINE_EXCEEDED,
            "RPC deadline exceeded",
            strlen("RPC deadline exceeded"),
            NULL);
    } else if (call->stream.failure_status != TREVRPC_STATUS_OK && !call->stream.sent_status) {
        call->stream.context = NULL;
        call->final_status = call->stream.failure_status;
        write_err = trevrpc_stream_send_final_status_with_metadata(&call->stream,
            call->stream.failure_status,
            call->stream.failure_message,
            call->stream.failure_message == NULL ? 0 : strlen(call->stream.failure_message),
            NULL);
    } else if (!call->stream.sent_status) {
        call->stream.context = NULL;
        call->final_status = trevrpc_status_code_from_uint32(status);
        write_err = trevrpc_stream_send_final_status_with_metadata(
            &call->stream, call->final_status, message, message_len, metadata);
    } else {
        call->final_status = call->stream.terminal_status;
    }
    call->response_body_len = (size_t)call->stream.response_body_size;
    if (status_was_sent && call->stream.status_queued) {
        write_err = trevrpc_stream_finish_send(&call->stream);
    }
    return write_err;
}

int trevrpc_call_finish_stream(trevrpc_call* call, uint32_t status, const char* message, size_t message_len) {
    return trevrpc_call_finish_stream_with_metadata(call, status, message, message_len, NULL);
}

int trevrpc_call_finish_stream_with_metadata(
    trevrpc_call* call, uint32_t status, const char* message, size_t message_len, const trevrpc_metadata* metadata) {
    if (call == NULL || (message == NULL && message_len > 0)) {
        return -EINVAL;
    }
    if (call->request.kind == TREVRPC_RPC_KIND_UNARY) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }
    if (!trevrpc_call_mark_completed(call)) {
        return -EALREADY;
    }

    int write_err = trevrpc_call_write_stream_finish(call, status, message, message_len, metadata);
    if (write_err != 0) {
        trevrpc_stream_cancel(&call->stream);
    }
    if (trevrpc_call_should_release_after_complete(call)) {
        trevrpc_call_release(call);
    }
    return write_err;
}

void trevrpc_call_close(trevrpc_call* call) {
    if (call == NULL || !trevrpc_call_mark_completed(call)) {
        return;
    }

    if (call->request.kind == TREVRPC_RPC_KIND_UNARY) {
        trevrpc_server_write_status(&call->stream, TREVRPC_STATUS_CANCELLED, "RPC cancelled");
    } else if (!call->stream.sent_status) {
        call->stream.context = NULL;
        (void)trevrpc_stream_send_status(
            &call->stream, TREVRPC_STATUS_CANCELLED, "RPC cancelled", strlen("RPC cancelled"));
        if (call->stream.status_queued) {
            (void)trevrpc_stream_finish_send(&call->stream);
        }
    }
    call->final_status = TREVRPC_STATUS_CANCELLED;
    call->response_body_len = (size_t)call->stream.response_body_size;
    trevrpc_stream_cancel(&call->stream);
    if (trevrpc_call_should_release_after_complete(call)) {
        trevrpc_call_release(call);
    }
}

static void trevrpc_call_complete_handler_error(trevrpc_call* call, int err) {
    if (call == NULL || trevrpc_call_completed(call)) {
        return;
    }

    const char* message = NULL;
    uint32_t status = TREVRPC_STATUS_INTERNAL;
    if (err == 0) {
        message = "handler did not complete call";
    } else if (call->request.kind == TREVRPC_RPC_KIND_UNARY) {
        message = "handler failed";
    } else {
        status = trevrpc_error_is_transport_failure(err) ? trevrpc_transport_status_from_error(err, &message)
                                                         : trevrpc_status_from_error(err, &message);
    }
    if (message == NULL) {
        message = "handler failed";
    }

    trevrpc_log(call->server, TREVRPC_LOG_LEVEL_ERROR, "rpc.handler_failed", message, &call->request, err);
    if (!trevrpc_call_mark_completed(call)) {
        return;
    }
    if (call->request.kind == TREVRPC_RPC_KIND_UNARY) {
        trevrpc_response response = {0};
        if (trevrpc_call_context_deadline_expired(&call->context)) {
            trevrpc_set_status(&response, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
        } else {
            trevrpc_set_status(&response, status, message);
        }
        (void)trevrpc_server_write_response(&call->stream, &response);
        call->response_body_len = response.body_len;
        call->final_status = response.status;
        trevrpc_response_reset(&response);
    } else {
        trevrpc_call_write_stream_finish(call, status, message, strlen(message), NULL);
    }
}

static uint32_t trevrpc_transport_event_transport(trevrpc_server* server) {
    if (server == NULL || server->wt_listener == NULL) {
        return TREVRPC_TRANSPORT_KIND_MSQUIC;
    }
    if (server->listener == NULL) {
        return TREVRPC_TRANSPORT_KIND_WEBTRANSPORT;
    }
    return TREVRPC_TRANSPORT_KIND_MSQUIC;
}

static bool trevrpc_handle_stream(trevrpc_server* server,
    trevrpc_stream* stream,
    trevrpc_conn_stream_limiter* stream_limiter,
    bool server_task_active,
    const struct timespec* accepted_at) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    trevrpc_server_options options = trevrpc_server_options_snapshot(server);
    bool initial_timeout_expired = false;
    uint64_t initial_timeout =
        trevrpc_initial_timeout_remaining(options.initial_request_timeout_nanos, accepted_at, &initial_timeout_expired);
    intptr_t read = TREV_MSQUIC_ERR_TIMEOUT;
    if (!initial_timeout_expired) {
        stream->stream_idle_timeout_nanos = initial_timeout;
        read = trevrpc_stream_read_frame_body(stream, &body, &body_len);
        stream->stream_idle_timeout_nanos = 0;
    }
    if (read < 0) {
        const char* message = NULL;
        uint32_t status = trevrpc_transport_status_from_error((int)read, &message);
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, stream->transport, (int)read, message);
        trevrpc_server_write_status(stream, status, message);
        trevrpc_metrics_record_pre_handler(server, status);
        return false;
    }
    if (read == 0) {
        return false;
    }

    trevrpc_request request;
    int err = trevrpc_wire_decode_request(body, body_len, &request);
    if (err == TREVRPC_ERR_INVALID_FRAME) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_WARN, "rpc.decode_failed", "invalid request frame", NULL, err);
        trevrpc_server_write_status(stream, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request frame");
        trevrpc_metrics_record_pre_handler(server, TREVRPC_STATUS_INVALID_ARGUMENT);
        trevrpc_stream_free_body(stream, body);
        return false;
    }
    if (err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_WARN, "rpc.decode_failed", "unsupported TrevRPC wire version", NULL, err);
        trevrpc_server_write_status(stream, TREVRPC_STATUS_FAILED_PRECONDITION, "unsupported TrevRPC wire version");
        trevrpc_metrics_record_pre_handler(server, TREVRPC_STATUS_FAILED_PRECONDITION);
        trevrpc_stream_free_body(stream, body);
        return false;
    }
    if (err != 0) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_WARN, "rpc.decode_failed", "invalid request", NULL, err);
        trevrpc_server_write_status(stream, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request");
        trevrpc_metrics_record_pre_handler(server, TREVRPC_STATUS_INVALID_ARGUMENT);
        trevrpc_stream_free_body(stream, body);
        return false;
    }

    struct timespec rpc_started_at = {0};
    (void)trevrpc_clock_now(&rpc_started_at);
    trevrpc_metrics_record_started(server, &request);

    trevrpc_call_context context;
    err = trevrpc_call_context_init(&context, server, &request);
    if (err == -ERANGE) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(stream, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout is too large");
        } else {
            trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout is too large");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_INVALID_ARGUMENT, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }
    if (err == -EOVERFLOW) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(stream, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout overflowed");
        } else {
            trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout overflowed");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_INVALID_ARGUMENT, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }
    if (err != 0) {
        trevrpc_server_write_status(stream, TREVRPC_STATUS_INTERNAL, "failed to prepare request");
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_INTERNAL, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }
    if (trevrpc_call_context_deadline_expired(&context)) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(stream, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
        } else {
            trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_DEADLINE_EXCEEDED, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }

    void* authorizer_user_data = NULL;
    trevrpc_authorizer authorizer = trevrpc_server_authorizer_snapshot(server, &authorizer_user_data);
    if (authorizer != NULL) {
        trevrpc_status status = trevrpc_status_ok();
        err = authorizer(authorizer_user_data, &context, &request, &status);
        if (err != 0) {
            trevrpc_log(server, TREVRPC_LOG_LEVEL_ERROR, "rpc.authorizer_failed", "authorizer failed", &request, err);
            status = trevrpc_status_internal("authorizer failed", strlen("authorizer failed"));
        }
        if (status.code != TREVRPC_STATUS_OK) {
            trevrpc_log(server,
                status.code == TREVRPC_STATUS_INTERNAL ? TREVRPC_LOG_LEVEL_ERROR : TREVRPC_LOG_LEVEL_WARN,
                "rpc.authorization_denied",
                status.message,
                &request,
                (int)status.code);
            if (request.kind == TREVRPC_RPC_KIND_UNARY) {
                trevrpc_response response = {0};
                (void)trevrpc_response_set_status(&response, status);
                trevrpc_server_write_response(stream, &response);
                trevrpc_response_reset(&response);
            } else {
                trevrpc_server_write_stream_status(stream, status.code, status.message);
            }
            trevrpc_metrics_record_finished(server, &request, 0, status.code, &rpc_started_at);
            trevrpc_request_reset(&request);
            trevrpc_stream_free_body(stream, body);
            return false;
        }
    }

    if (!trevrpc_server_request_try_start(server)) {
        trevrpc_log(server,
            TREVRPC_LOG_LEVEL_WARN,
            "rpc.overloaded",
            "too many concurrent RPCs",
            &request,
            TREVRPC_STATUS_RESOURCE_EXHAUSTED);
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "too many concurrent RPCs");
        } else {
            trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "too many concurrent RPCs");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_RESOURCE_EXHAUSTED, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }

    trevrpc_method* method = trevrpc_server_find_method(server, &request);
    if (method == NULL) {
        trevrpc_log(server,
            TREVRPC_LOG_LEVEL_WARN,
            "rpc.route_not_found",
            "method is not implemented",
            &request,
            TREVRPC_STATUS_UNIMPLEMENTED);
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(stream, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        } else {
            trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_UNIMPLEMENTED, &rpc_started_at);
        trevrpc_server_request_finish(server);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }
    if (method->kind != request.kind) {
        trevrpc_log(server,
            TREVRPC_LOG_LEVEL_WARN,
            "rpc.kind_mismatch",
            "method RPC kind mismatch",
            &request,
            TREVRPC_STATUS_UNIMPLEMENTED);
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(stream, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        } else {
            trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_UNIMPLEMENTED, &rpc_started_at);
        trevrpc_server_request_finish(server);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }

    if (method->call_handler != NULL) {
        trevrpc_call* call = trevrpc_call_new(
            server, stream, &request, &context, &rpc_started_at, body, stream_limiter, server_task_active);
        if (call == NULL) {
            trevrpc_transport_record_event_for_transport(
                server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, stream->transport, -ENOMEM, "failed to allocate call");
            if (request.kind == TREVRPC_RPC_KIND_UNARY) {
                trevrpc_server_write_status(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "failed to allocate call");
            } else {
                trevrpc_server_write_stream_status(
                    stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "failed to allocate call");
            }
            trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_RESOURCE_EXHAUSTED, &rpc_started_at);
            trevrpc_server_request_finish(server);
            trevrpc_request_reset(&request);
            trevrpc_stream_free_body(stream, body);
            return false;
        }

        err = method->call_handler(method->user_data, call);
        if (err == TREVRPC_CALL_DEFERRED) {
            int defer_err = trevrpc_call_defer(call);
            if (defer_err != 0 && defer_err != -EALREADY) {
                err = defer_err;
            }
        }
        if (!trevrpc_call_deferred(call) && !trevrpc_call_completed(call)) {
            trevrpc_call_complete_handler_error(call, err == TREVRPC_CALL_DEFERRED ? 0 : err);
        }
        trevrpc_call_release(call);
        return true;
    }

    if (request.kind != TREVRPC_RPC_KIND_UNARY) {
        trevrpc_server_options options = trevrpc_server_options_snapshot(server);
        trevrpc_stream rpc_stream = *stream;
        rpc_stream.context = &context;
        rpc_stream.max_stream_messages = options.max_stream_messages;
        rpc_stream.max_stream_body_size = options.max_stream_body_size;
        rpc_stream.stream_idle_timeout_nanos = options.stream_idle_timeout_nanos;
        rpc_stream.failure_status = TREVRPC_STATUS_OK;
        (void)trevrpc_stream_start_response_idle_timer(&rpc_stream);
        err = method->stream_handler(method->user_data, &context, &request, &rpc_stream);
        uint32_t final_status = TREVRPC_STATUS_OK;
        if (trevrpc_call_context_deadline_expired(&context) && !rpc_stream.sent_status) {
            rpc_stream.context = NULL;
            final_status = TREVRPC_STATUS_DEADLINE_EXCEEDED;
            (void)trevrpc_stream_send_status(&rpc_stream,
                TREVRPC_STATUS_DEADLINE_EXCEEDED,
                "RPC deadline exceeded",
                strlen("RPC deadline exceeded"));
        } else if (rpc_stream.failure_status != TREVRPC_STATUS_OK && !rpc_stream.sent_status) {
            rpc_stream.context = NULL;
            final_status = rpc_stream.failure_status;
            (void)trevrpc_stream_send_status(&rpc_stream,
                rpc_stream.failure_status,
                rpc_stream.failure_message,
                rpc_stream.failure_message == NULL ? 0 : strlen(rpc_stream.failure_message));
        } else if (err != 0 && !rpc_stream.sent_status) {
            rpc_stream.context = NULL;
            const char* message = NULL;
            final_status = trevrpc_error_is_transport_failure(err) ? trevrpc_transport_status_from_error(err, &message)
                                                                   : trevrpc_status_from_error(err, &message);
            trevrpc_log(server, TREVRPC_LOG_LEVEL_ERROR, "rpc.handler_failed", message, &request, err);
            (void)trevrpc_stream_send_status(&rpc_stream, final_status, message, message == NULL ? 0 : strlen(message));
        } else if (!rpc_stream.sent_status) {
            rpc_stream.context = NULL;
            (void)trevrpc_stream_send_status(&rpc_stream, TREVRPC_STATUS_OK, NULL, 0);
        } else {
            final_status = rpc_stream.terminal_status;
        }
        if (rpc_stream.status_queued) {
            (void)trevrpc_stream_finish_send(&rpc_stream);
        }
        trevrpc_metrics_record_finished(
            server, &request, (size_t)rpc_stream.response_body_size, final_status, &rpc_started_at);
        trevrpc_server_request_finish(server);
        trevrpc_request_reset(&request);
        trevrpc_stream_free_body(stream, body);
        return false;
    }

    trevrpc_response response = {0};
    err = method->handler(method->user_data, &context, &request, &response);
    if (trevrpc_call_context_deadline_expired(&context)) {
        trevrpc_response_reset(&response);
        trevrpc_set_status(&response, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
    } else if (err != 0) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_ERROR, "rpc.handler_failed", "handler failed", &request, err);
        trevrpc_response_reset(&response);
        trevrpc_set_status(&response, TREVRPC_STATUS_INTERNAL, "handler failed");
    } else if (!trevrpc_response_fields_valid(&response)) {
        trevrpc_log(server,
            TREVRPC_LOG_LEVEL_ERROR,
            "rpc.handler_invalid_response",
            "handler produced invalid response",
            &request,
            -EINVAL);
        trevrpc_response_reset(&response);
        trevrpc_set_status(&response, TREVRPC_STATUS_INTERNAL, "handler produced invalid response");
    }

    trevrpc_server_write_response(stream, &response);
    trevrpc_metrics_record_finished(server, &request, response.body_len, response.status, &rpc_started_at);
    trevrpc_response_reset(&response);
    trevrpc_server_request_finish(server);
    trevrpc_request_reset(&request);
    trevrpc_stream_free_body(stream, body);
    return false;
}

static bool trevrpc_reject_stream_queue_full(trevrpc_server* server, trevrpc_stream* stream) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_stream_read_frame_body_ready(stream, &body, &body_len);
    if (read <= 0) {
        return false;
    }

    trevrpc_request request = {0};
    int err = trevrpc_wire_decode_request(body, body_len, &request);
    if (err != 0) {
        trevrpc_stream_free_body(stream, body);
        return false;
    }

    struct timespec rejected_at = {0};
    (void)trevrpc_clock_now(&rejected_at);
    trevrpc_metrics_record_started(server, &request);
    trevrpc_log(server,
        TREVRPC_LOG_LEVEL_WARN,
        "rpc.overloaded",
        "RPC worker queue is full",
        &request,
        TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    if (request.kind == TREVRPC_RPC_KIND_UNARY) {
        trevrpc_server_write_status(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "RPC worker queue is full");
    } else {
        trevrpc_server_write_stream_status(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "RPC worker queue is full");
    }
    trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_RESOURCE_EXHAUSTED, &rejected_at);
    trevrpc_request_reset(&request);
    trevrpc_stream_free_body(stream, body);
    return true;
}

static void trevrpc_stream_task_cleanup(trevrpc_stream_task* task, trevrpc_stream* stream) {
    trevrpc_transport_record_event_for_transport(
        task->server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, stream->transport, 0, NULL);
    trevrpc_stream_close_raw(stream);
    if (task->stream_limiter != NULL) {
        trevrpc_conn_stream_finish(task->stream_limiter);
        trevrpc_conn_stream_limiter_release(task->stream_limiter);
    }
    trevrpc_server_task_finish(task->server);
}

static void trevrpc_stream_task_run(trevrpc_stream_task* task) {
    if (task->h3_conn != NULL) {
        trevrpc_server_options options = trevrpc_server_options_snapshot(task->server);
        bool expired = false;
        uint64_t timeout =
            trevrpc_initial_timeout_remaining(options.initial_request_timeout_nanos, &task->accepted_at, &expired);
        trevrpc_wt_stream* wt_stream = NULL;
        int resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
        int err =
            trevrpc_h3_stream_resolve(task->h3_conn, task->h3_stream, expired ? 0 : timeout, &wt_stream, &resolution);
        if (err != 0 || resolution == TREV_H3_STREAM_RESOLVED_HANDLED) {
            trevrpc_transport_record_event_for_transport(
                task->server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, TREVRPC_TRANSPORT_KIND_HTTP3, 0, NULL);
            if (err != 0) {
                trevrpc_transport_record_event_for_transport(task->server,
                    TREVRPC_TRANSPORT_EVENT_STREAM_ERROR,
                    TREVRPC_TRANSPORT_KIND_HTTP3,
                    err,
                    "failed to resolve HTTP/3 request stream");
            }
            trevrpc_transport_record_event_for_transport(
                task->server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, TREVRPC_TRANSPORT_KIND_HTTP3, 0, NULL);
            trevrpc_h3_stream_close(task->h3_stream);
            trevrpc_conn_stream_finish(task->stream_limiter);
            trevrpc_conn_stream_limiter_release(task->stream_limiter);
            trevrpc_server_task_finish(task->server);
            free(task);
            return;
        }
        if (resolution == TREV_H3_STREAM_RESOLVED_WEBTRANSPORT) {
            trevrpc_h3_stream_close(task->h3_stream);
            task->h3_stream = NULL;
            task->wt_stream = wt_stream;
        }
        task->h3_conn = NULL;
    }
    trevrpc_stream stream =
        trevrpc_stream_ref_server(task->stream, task->wt_stream, task->h3_stream, task->server->max_frame_size);
    trevrpc_transport_record_event_for_transport(
        task->server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, stream.transport, 0, NULL);
    bool cleanup_transferred =
        trevrpc_handle_stream(task->server, &stream, task->stream_limiter, true, &task->accepted_at);
    if (!cleanup_transferred) {
        trevrpc_stream_task_cleanup(task, &stream);
    }
    free(task);
}

static void* trevrpc_worker_thread(void* arg) {
    trevrpc_server* server = arg;

    for (;;) {
        pthread_mutex_lock(&server->mutex);
        while (server->worker_queue_head == NULL && !server->worker_pool_stopping) {
            pthread_cond_wait(&server->cond, &server->mutex);
        }
        if (server->worker_queue_head == NULL && server->worker_pool_stopping) {
            pthread_mutex_unlock(&server->mutex);
            break;
        }

        trevrpc_stream_task* task = server->worker_queue_head;
        server->worker_queue_head = task->next;
        if (server->worker_queue_head == NULL) {
            server->worker_queue_tail = NULL;
        }
        if (server->worker_queue_len > 0) {
            server->worker_queue_len--;
        }
        task->next = NULL;
        pthread_mutex_unlock(&server->mutex);

        trevrpc_stream_task_run(task);
    }

    trevrpc_server_task_finish(server);
    return NULL;
}

static trevrpc_worker_queue_result trevrpc_server_worker_queue_push(trevrpc_server* server, trevrpc_stream_task* task) {
    pthread_mutex_lock(&server->mutex);
    if (!server->worker_pool_started || server->worker_pool_stopping || server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        return TREV_WORKER_QUEUE_CLOSED;
    }
    if (server->worker_queue_len >= server->worker_queue_capacity) {
        pthread_mutex_unlock(&server->mutex);
        return TREV_WORKER_QUEUE_FULL;
    }
    if (task->stream_limiter != NULL && !trevrpc_conn_stream_limiter_retain(task->stream_limiter)) {
        pthread_mutex_unlock(&server->mutex);
        return TREV_WORKER_QUEUE_CLOSED;
    }

    task->next = NULL;
    if (server->worker_queue_tail != NULL) {
        server->worker_queue_tail->next = task;
    } else {
        server->worker_queue_head = task;
    }
    server->worker_queue_tail = task;
    server->worker_queue_len++;
    pthread_cond_signal(&server->cond);
    pthread_mutex_unlock(&server->mutex);
    return TREV_WORKER_QUEUE_ENQUEUED;
}

static void trevrpc_server_worker_pool_request_stop(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    server->worker_pool_stopping = true;
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static void trevrpc_server_worker_pool_join(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    pthread_t* threads = server->worker_threads;
    size_t count = server->worker_count;
    server->worker_threads = NULL;
    server->worker_count = 0;
    server->worker_pool_started = false;
    server->worker_queue_capacity = 0;
    pthread_mutex_unlock(&server->mutex);

    for (size_t i = 0; i < count; i++) {
        (void)pthread_join(threads[i], NULL);
    }
    free(threads);
}

static int trevrpc_server_worker_pool_start(trevrpc_server* server) {
    trevrpc_server_options options = trevrpc_server_options_snapshot(server);
    size_t worker_count = trevrpc_effective_worker_count(options.worker_count);
    size_t queue_capacity = trevrpc_effective_worker_queue_capacity(options.worker_queue_capacity);
    pthread_t* threads = calloc(worker_count, sizeof(*threads));
    if (threads == NULL) {
        return -ENOMEM;
    }

    pthread_mutex_lock(&server->mutex);
    if (server->worker_pool_started) {
        pthread_mutex_unlock(&server->mutex);
        free(threads);
        return 0;
    }
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(threads);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    server->worker_threads = threads;
    server->worker_count = 0;
    server->worker_queue_capacity = queue_capacity;
    server->worker_queue_head = NULL;
    server->worker_queue_tail = NULL;
    server->worker_queue_len = 0;
    server->worker_pool_started = true;
    server->worker_pool_stopping = false;
    pthread_mutex_unlock(&server->mutex);

    for (size_t i = 0; i < worker_count; i++) {
        if (!trevrpc_server_task_start(server)) {
            trevrpc_server_worker_pool_request_stop(server);
            trevrpc_server_worker_pool_join(server);
            return TREV_MSQUIC_ERR_CLOSED;
        }

        int err = pthread_create(&threads[i], NULL, trevrpc_worker_thread, server);
        if (err != 0) {
            trevrpc_server_task_finish(server);
            trevrpc_server_worker_pool_request_stop(server);
            trevrpc_server_worker_pool_join(server);
            return -err;
        }

        pthread_mutex_lock(&server->mutex);
        server->worker_count++;
        pthread_mutex_unlock(&server->mutex);
    }
    return 0;
}

static int trevrpc_server_submit_stream_task(trevrpc_server* server,
    trevrpc_msquic_stream* stream,
    trevrpc_wt_stream* wt_stream,
    trevrpc_h3_stream* h3_stream,
    trevrpc_h3_conn* h3_conn,
    trevrpc_conn_stream_limiter* stream_limiter,
    const struct timespec* accepted_at) {
    uint32_t transport = stream != NULL      ? TREVRPC_TRANSPORT_KIND_MSQUIC
                         : wt_stream != NULL ? TREVRPC_TRANSPORT_KIND_WEBTRANSPORT
                                             : TREVRPC_TRANSPORT_KIND_HTTP3;
    trevrpc_server_options options = trevrpc_server_options_snapshot(server);
    if (!trevrpc_conn_stream_try_start(stream_limiter, options.max_concurrent_streams_per_connection)) {
        trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, transport, 0, NULL);
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, transport, 0, "too many concurrent streams on connection");
        trevrpc_stream rpc_stream = trevrpc_stream_ref_server(stream, wt_stream, h3_stream, server->max_frame_size);
        if (h3_conn == NULL) {
            trevrpc_server_write_status(
                &rpc_stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "too many concurrent streams on connection");
        }
        trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, transport, 0, NULL);
        trevrpc_stream_close_raw(&rpc_stream);
        return 1;
    }

    if (!trevrpc_server_task_start(server)) {
        trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, transport, 0, NULL);
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, transport, 0, "server is shutting down");
        trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, transport, 0, NULL);
        trevrpc_conn_stream_finish(stream_limiter);
        if (stream != NULL) {
            trevrpc_msquic_stream_close(stream);
        } else if (wt_stream != NULL) {
            trevrpc_wt_stream_close(wt_stream);
        } else {
            trevrpc_h3_stream_close(h3_stream);
        }
        return TREV_MSQUIC_ERR_CLOSED;
    }

    trevrpc_stream_task* stream_task = malloc(sizeof(*stream_task));
    if (stream_task == NULL) {
        trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, transport, 0, NULL);
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, transport, -ENOMEM, "failed to allocate stream task");
        trevrpc_stream rpc_stream = trevrpc_stream_ref_server(stream, wt_stream, h3_stream, server->max_frame_size);
        if (h3_conn == NULL) {
            trevrpc_server_write_status(
                &rpc_stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "failed to allocate stream task");
        }
        trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, transport, 0, NULL);
        trevrpc_conn_stream_finish(stream_limiter);
        trevrpc_stream_close_raw(&rpc_stream);
        trevrpc_server_task_finish(server);
        return 1;
    }

    stream_task->server = server;
    stream_task->stream = stream;
    stream_task->wt_stream = wt_stream;
    stream_task->h3_stream = h3_stream;
    stream_task->h3_conn = h3_conn;
    stream_task->stream_limiter = stream_limiter;
    stream_task->accepted_at = accepted_at == NULL ? (struct timespec){0} : *accepted_at;

    trevrpc_worker_queue_result queued = trevrpc_server_worker_queue_push(server, stream_task);
    if (queued == TREV_WORKER_QUEUE_ENQUEUED) {
        return 0;
    }

    trevrpc_stream rpc_stream = trevrpc_stream_ref_server(stream, wt_stream, h3_stream, server->max_frame_size);
    trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, transport, 0, NULL);
    if (queued == TREV_WORKER_QUEUE_FULL) {
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, transport, 0, "RPC worker queue is full");
        if (h3_conn == NULL) {
            (void)trevrpc_reject_stream_queue_full(server, &rpc_stream);
        }
    } else {
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, transport, 0, "server is shutting down");
    }
    trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, transport, 0, NULL);
    trevrpc_stream_close_raw(&rpc_stream);
    trevrpc_conn_stream_finish(stream_limiter);
    trevrpc_server_task_finish(server);
    free(stream_task);
    return queued == TREV_WORKER_QUEUE_FULL ? 1 : TREV_MSQUIC_ERR_CLOSED;
}

#ifdef TREVRPC_TESTING
int trevrpc_test_server_start_worker_pool(trevrpc_server* server) {
    return trevrpc_server_worker_pool_start(server);
}

int trevrpc_test_server_queue_msquic_stream(
    trevrpc_server* server, trevrpc_msquic_stream* stream, trevrpc_conn_stream_limiter* stream_limiter) {
    struct timespec accepted_at = {0};
    (void)trevrpc_clock_now(&accepted_at);
    return trevrpc_server_submit_stream_task(server, stream, NULL, NULL, NULL, stream_limiter, &accepted_at);
}

bool trevrpc_test_server_wait_for_tasks(trevrpc_server* server, uint64_t timeout_nanos) {
    return trevrpc_server_wait_for_tasks(server, timeout_nanos);
}
#endif

static void* trevrpc_conn_thread(void* arg) {
    trevrpc_conn_task* task = arg;
    trevrpc_server* server = task->server;
    trevrpc_msquic_conn* conn = task->conn;
    trevrpc_wt_session* wt_session = task->wt_session;
    trevrpc_h3_conn* h3_conn = task->h3_conn;
    uint32_t transport = conn != NULL           ? TREVRPC_TRANSPORT_KIND_MSQUIC
                         : wt_session != NULL   ? TREVRPC_TRANSPORT_KIND_WEBTRANSPORT
                         : server->enable_http3 ? TREVRPC_TRANSPORT_KIND_HTTP3
                                                : TREVRPC_TRANSPORT_KIND_WEBTRANSPORT;
    free(task);

    trevrpc_server_conn_ref* conn_ref = NULL;
    if (trevrpc_server_conn_add(server, conn, wt_session, h3_conn, &conn_ref) != 0) {
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, transport, -ENOMEM, "failed to track connection");
        if (conn != NULL) {
            trevrpc_msquic_conn_close(conn);
        } else {
            if (wt_session != NULL) {
                trevrpc_wt_session_close(wt_session);
            } else {
                trevrpc_h3_conn_close(h3_conn);
            }
        }
        trevrpc_server_connection_finish(server);
        trevrpc_server_task_finish(server);
        return NULL;
    }

    trevrpc_conn_stream_limiter* stream_limiter = trevrpc_conn_stream_limiter_new();
    if (stream_limiter == NULL) {
        trevrpc_transport_record_event_for_transport(server,
            TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR,
            transport,
            -ENOMEM,
            "failed to initialize stream limiter");
        trevrpc_server_conn_remove(server, conn_ref);
        if (conn != NULL) {
            trevrpc_msquic_conn_close(conn);
        } else {
            if (wt_session != NULL) {
                trevrpc_wt_session_close(wt_session);
            } else {
                trevrpc_h3_conn_close(h3_conn);
            }
        }
        trevrpc_server_connection_finish(server);
        trevrpc_server_task_finish(server);
        return NULL;
    }

    while (!trevrpc_server_is_shutting_down(server)) {
        trevrpc_msquic_stream* stream = NULL;
        trevrpc_wt_stream* wt_stream = NULL;
        trevrpc_h3_stream* h3_stream = NULL;
        struct timespec accepted_at = {0};
        int err = conn != NULL         ? trevrpc_msquic_conn_accept_stream(conn, &stream)
                  : wt_session != NULL ? trevrpc_wt_session_accept_stream(wt_session, &wt_stream)
                                       : trevrpc_h3_conn_accept_stream(h3_conn, &h3_stream);
        if (err != 0) {
            if (!trevrpc_server_is_shutting_down(server)) {
                trevrpc_transport_record_event_for_transport(
                    server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, transport, err, "failed to accept stream");
            }
            break;
        }

        (void)trevrpc_clock_now(&accepted_at);
        err = trevrpc_server_submit_stream_task(
            server, stream, wt_stream, h3_stream, h3_conn, stream_limiter, &accepted_at);
        if (err == TREV_MSQUIC_ERR_CLOSED) {
            break;
        }
    }

    trevrpc_conn_stream_limiter_wait(stream_limiter);
    trevrpc_conn_stream_limiter_release(stream_limiter);
    trevrpc_server_conn_remove(server, conn_ref);
    trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE, transport, 0, NULL);
    if (conn != NULL) {
        trevrpc_msquic_conn_close(conn);
    } else {
        if (wt_session != NULL) {
            trevrpc_wt_session_close(wt_session);
        } else {
            trevrpc_h3_conn_close(h3_conn);
        }
    }
    trevrpc_server_connection_finish(server);
    trevrpc_server_task_finish(server);
    return NULL;
}

static int trevrpc_server_start_connection_task(
    trevrpc_server* server, trevrpc_msquic_conn* conn, trevrpc_wt_session* wt_session, trevrpc_h3_conn* h3_conn) {
    uint32_t transport = conn != NULL           ? TREVRPC_TRANSPORT_KIND_MSQUIC
                         : wt_session != NULL   ? TREVRPC_TRANSPORT_KIND_WEBTRANSPORT
                         : server->enable_http3 ? TREVRPC_TRANSPORT_KIND_HTTP3
                                                : TREVRPC_TRANSPORT_KIND_WEBTRANSPORT;
    if (!trevrpc_server_connection_try_start(server)) {
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, transport, 0, "too many concurrent connections");
        if (conn != NULL) {
            trevrpc_msquic_conn_close(conn);
        } else {
            if (wt_session != NULL) {
                trevrpc_wt_session_close(wt_session);
            } else {
                trevrpc_h3_conn_close(h3_conn);
            }
        }
        return 0;
    }

    if (!trevrpc_server_task_start(server)) {
        trevrpc_server_connection_finish(server);
        if (conn != NULL) {
            trevrpc_msquic_conn_close(conn);
        } else {
            if (wt_session != NULL) {
                trevrpc_wt_session_close(wt_session);
            } else {
                trevrpc_h3_conn_close(h3_conn);
            }
        }
        return 0;
    }

    trevrpc_transport_record_event_for_transport(server, TREVRPC_TRANSPORT_EVENT_CONNECTION_OPEN, transport, 0, NULL);

    trevrpc_conn_task* task = malloc(sizeof(*task));
    if (task == NULL) {
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, transport, -ENOMEM, "failed to allocate connection task");
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE, transport, 0, NULL);
        if (conn != NULL) {
            trevrpc_msquic_conn_close(conn);
        } else {
            if (wt_session != NULL) {
                trevrpc_wt_session_close(wt_session);
            } else {
                trevrpc_h3_conn_close(h3_conn);
            }
        }
        trevrpc_server_connection_finish(server);
        trevrpc_server_task_finish(server);
        return -ENOMEM;
    }
    task->server = server;
    task->conn = conn;
    task->wt_session = wt_session;
    task->h3_conn = h3_conn;

    pthread_t thread;
    int err = pthread_create(&thread, NULL, trevrpc_conn_thread, task);
    if (err != 0) {
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, transport, -err, "failed to start connection thread");
        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE, transport, 0, NULL);
        free(task);
        if (conn != NULL) {
            trevrpc_msquic_conn_close(conn);
        } else {
            if (wt_session != NULL) {
                trevrpc_wt_session_close(wt_session);
            } else {
                trevrpc_h3_conn_close(h3_conn);
            }
        }
        trevrpc_server_connection_finish(server);
        trevrpc_server_task_finish(server);
        return -err;
    }
    pthread_detach(thread);
    return 0;
}

static int trevrpc_server_serve_transport(trevrpc_server* server, uint32_t transport) {
    int result = 0;
    for (;;) {
        trevrpc_msquic_conn* conn = NULL;
        trevrpc_wt_session* wt_session = NULL;
        int err = transport == TREVRPC_TRANSPORT_KIND_WEBTRANSPORT
                      ? trevrpc_wt_listener_accept_session(server->wt_listener, &wt_session)
                      : trevrpc_msquic_listener_accept(server->listener, &conn);
        if (err != 0) {
            if (!trevrpc_server_is_shutting_down(server)) {
                trevrpc_transport_record_event_for_transport(server,
                    TREVRPC_TRANSPORT_EVENT_LISTENER_ERROR,
                    transport,
                    err,
                    "failed to accept connection/session");
            }
            result = trevrpc_server_is_shutting_down(server) ? 0 : err;
            break;
        }
        err = trevrpc_server_start_connection_task(server, conn, wt_session, NULL);
        if (err != 0) {
            result = err;
            break;
        }
    }

    return result;
}

static int trevrpc_server_serve_shared_transport(trevrpc_server* server) {
    int result = 0;
    for (;;) {
        trevrpc_msquic_conn* conn = NULL;
        int err = trevrpc_msquic_listener_accept(server->shared_listener, &conn);
        if (err != 0) {
            if (!trevrpc_server_is_shutting_down(server)) {
                trevrpc_transport_record_event(
                    server, TREVRPC_TRANSPORT_EVENT_LISTENER_ERROR, err, "failed to accept connection");
            }
            result = trevrpc_server_is_shutting_down(server) ? 0 : err;
            break;
        }

        const uint8_t* alpn = NULL;
        size_t alpn_len = 0;
        err = trevrpc_msquic_conn_negotiated_alpn(conn, &alpn, &alpn_len);
        if (err != 0) {
            trevrpc_transport_record_event_for_transport(server,
                TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR,
                TREVRPC_TRANSPORT_KIND_MSQUIC,
                err,
                "missing negotiated ALPN");
            trevrpc_msquic_conn_close(conn);
            continue;
        }

        if (trevrpc_alpn_equals(alpn, alpn_len, TREVRPC_ALPN)) {
            err = trevrpc_server_start_connection_task(server, conn, NULL, NULL);
            if (err != 0) {
                result = err;
                break;
            }
            continue;
        }

        if (trevrpc_alpn_equals(alpn, alpn_len, TREVRPC_H3_ALPN)) {
            trevrpc_h3_conn* h3_conn = NULL;
            err = trevrpc_h3_accept_from_msquic(conn,
                &server->shared_wt_config,
                server->enable_http3,
                server->shared_h3_path,
                server->http3_admission,
                server->http3_admission_user_data,
                server->max_frame_size,
                &h3_conn);
            conn = NULL;
            if (err != 0) {
                if (!trevrpc_server_is_shutting_down(server)) {
                    trevrpc_transport_record_event_for_transport(server,
                        TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR,
                        TREVRPC_TRANSPORT_KIND_HTTP3,
                        err,
                        "failed to initialize HTTP/3 connection");
                }
                continue;
            }
            err = trevrpc_server_start_connection_task(server, NULL, NULL, h3_conn);
            if (err != 0) {
                result = err;
                break;
            }
            continue;
        }

        trevrpc_transport_record_event_for_transport(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, TREVRPC_TRANSPORT_KIND_MSQUIC, 0, "unsupported ALPN");
        trevrpc_msquic_conn_close(conn);
    }

    return result;
}

static void* trevrpc_accept_thread(void* arg) {
    trevrpc_accept_task* task = arg;
    task->result = trevrpc_server_serve_transport(task->server, task->transport);
    return NULL;
}

int trevrpc_server_serve(trevrpc_server* server) {
    if (server == NULL) {
        return -EINVAL;
    }
    if (server->listener == NULL && server->wt_listener == NULL && server->shared_listener == NULL) {
        return -EINVAL;
    }
    if (!trevrpc_server_task_start(server)) {
        return TREV_MSQUIC_ERR_CLOSED;
    }
    trevrpc_server_freeze_routes(server);

    int err = trevrpc_server_worker_pool_start(server);
    if (err != 0) {
        trevrpc_server_task_finish(server);
        return err;
    }

    trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_LISTENER_OPEN, 0, NULL);

    int result = 0;
    if (server->shared_listener != NULL) {
        result = trevrpc_server_serve_shared_transport(server);
    } else if (server->listener != NULL && server->wt_listener != NULL) {
        trevrpc_accept_task wt_task = {
            .server = server,
            .transport = TREVRPC_TRANSPORT_KIND_WEBTRANSPORT,
        };
        pthread_t wt_thread;
        int err = pthread_create(&wt_thread, NULL, trevrpc_accept_thread, &wt_task);
        if (err != 0) {
            result = -err;
        } else {
            result = trevrpc_server_serve_transport(server, TREVRPC_TRANSPORT_KIND_MSQUIC);
            trevrpc_wt_listener_shutdown(server->wt_listener);
            (void)pthread_join(wt_thread, NULL);
            if (result == 0 && wt_task.result != 0) {
                result = wt_task.result;
            }
        }
    } else if (server->wt_listener != NULL) {
        result = trevrpc_server_serve_transport(server, TREVRPC_TRANSPORT_KIND_WEBTRANSPORT);
    } else {
        result = trevrpc_server_serve_transport(server, TREVRPC_TRANSPORT_KIND_MSQUIC);
    }

    trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_LISTENER_CLOSE, result, NULL);
    trevrpc_server_task_finish(server);
    return result;
}

void trevrpc_server_shutdown(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->shutting_down = true;
    server->worker_pool_stopping = true;
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);

    trevrpc_server_shutdown_connections(server);
    if (server->listener != NULL) {
        trevrpc_msquic_listener_shutdown(server->listener);
    }
    if (server->wt_listener != NULL) {
        trevrpc_wt_listener_shutdown(server->wt_listener);
    }
    if (server->shared_listener != NULL) {
        trevrpc_msquic_listener_shutdown(server->shared_listener);
    }
}

void trevrpc_server_close(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    trevrpc_server_shutdown(server);
    trevrpc_server_options options = trevrpc_server_options_snapshot(server);
    if (!trevrpc_server_wait_for_tasks(server, options.graceful_shutdown_timeout_nanos)) {
        trevrpc_server_shutdown_connections(server);
        (void)trevrpc_server_wait_for_tasks(server, 0);
    }
    trevrpc_server_worker_pool_join(server);

    if (server->listener != NULL) {
        trevrpc_msquic_listener_close(server->listener);
    }
    if (server->wt_listener != NULL) {
        trevrpc_wt_listener_close(server->wt_listener);
    }
    if (server->shared_listener != NULL) {
        trevrpc_msquic_listener_close(server->shared_listener);
    }
    free(server->shared_wt_path);
    free(server->shared_wt_origin);
    free(server->shared_h3_path);
    trevrpc_method* method = server->methods;
    while (method != NULL) {
        trevrpc_method* next = method->next;
        free(method->service);
        free(method->method);
        free(method);
        method = next;
    }

    pthread_cond_destroy(&server->cond);
    pthread_mutex_destroy(&server->mutex);
    free(server);
}

const char* trevrpc_error(int code) {
    switch (code) {
    case TREVRPC_ERR_INVALID_FRAME:
        return "invalid RPC frame";
    case TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION:
        return "unsupported TrevRPC wire version";
    case TREVRPC_ERR_UNSUPPORTED_RPC_KIND:
        return "unsupported TrevRPC RPC kind";
    case TREVRPC_ERR_HANDLER_FAILED:
        return "RPC handler failed";
    case TREVRPC_ERR_FRAME_TOO_LARGE:
        return "frame too large";
    case TREVRPC_ERR_STREAM_LIMIT_EXCEEDED:
        return "stream limit exceeded";
    case TREVRPC_ERR_STREAM_IDLE_TIMEOUT:
        return "stream idle timeout";
    case -ENOMEM:
    case ENOMEM:
        return "out of memory";
    case -EINVAL:
    case EINVAL:
        return "invalid argument";
    case -EEXIST:
    case EEXIST:
        return "method already registered";
    default:
        return trevrpc_msquic_error(code);
    }
}
