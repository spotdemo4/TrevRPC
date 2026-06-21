#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"

#include "trevrpc_msquic.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREVRPC_NANOS_PER_SEC 1000000000ull

#define TREVRPC_STREAM_LIMIT_DISABLED (-1)

typedef struct trevrpc_method trevrpc_method;
typedef struct trevrpc_server_conn_ref trevrpc_server_conn_ref;
typedef struct trevrpc_conn_stream_limiter trevrpc_conn_stream_limiter;

struct trevrpc_client {
    trevrpc_msquic_conn* conn;
    size_t max_frame_size;
};

struct trevrpc_call_context {
    trevrpc_server* server;
    bool has_deadline;
    struct timespec deadline;
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
    void* user_data;
};

struct trevrpc_stream {
    trevrpc_msquic_stream* stream;
    const trevrpc_call_context* context;
    size_t max_frame_size;
    bool owns_stream;
    bool sent_status;
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

struct trevrpc_server_conn_ref {
    trevrpc_server_conn_ref* next;
    trevrpc_msquic_conn* conn;
};

struct trevrpc_server {
    trevrpc_msquic_listener* listener;
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
    size_t active_tasks;
    size_t active_connections;
    size_t active_requests;
    bool shutting_down;
};

struct trevrpc_conn_stream_limiter {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t active_streams;
};

typedef struct trevrpc_conn_task {
    trevrpc_server* server;
    trevrpc_msquic_conn* conn;
} trevrpc_conn_task;

typedef struct trevrpc_stream_task {
    trevrpc_server* server;
    trevrpc_msquic_stream* stream;
    trevrpc_conn_stream_limiter* stream_limiter;
} trevrpc_stream_task;

static bool trevrpc_server_is_shutting_down(trevrpc_server* server);

static size_t trevrpc_effective_max_frame_size(const trevrpc_config* config) {
    if (config != NULL && config->max_frame_size > 0) {
        return config->max_frame_size;
    }

    return TREVRPC_DEFAULT_MAX_FRAME_SIZE;
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
    msquic_config.max_idle_timeout_ms = config->max_idle_timeout_ms;
    msquic_config.keep_alive_ms = config->keep_alive_ms;
    msquic_config.peer_bidi_stream_count = config->peer_bidi_stream_count;
    msquic_config.max_stateless_operations = config->max_stateless_operations;
    msquic_config.max_binding_stateless_operations = config->max_binding_stateless_operations;
    return msquic_config;
}

trevrpc_config trevrpc_default_config(void) {
    trevrpc_config config = {0};
    config.max_idle_timeout_ms = 30000;
    config.keep_alive_ms = 15000;
    config.peer_bidi_stream_count = 100;
    config.max_frame_size = TREVRPC_DEFAULT_MAX_FRAME_SIZE;
    return config;
}

trevrpc_server_options trevrpc_default_server_options(void) {
    trevrpc_server_options options = {0};
    options.max_concurrent_connections = 256;
    options.max_concurrent_streams_per_connection = 64;
    options.max_concurrent_requests = 1024;
    options.graceful_shutdown_timeout_nanos = 30ull * TREVRPC_NANOS_PER_SEC;
    options.initial_request_timeout_nanos = 10ull * TREVRPC_NANOS_PER_SEC;
    options.max_stream_messages = 4096;
    options.max_stream_body_size = 16 * 1024 * 1024;
    options.stream_idle_timeout_nanos = 30ull * TREVRPC_NANOS_PER_SEC;
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

static int trevrpc_write_frame(trevrpc_msquic_stream* stream, const uint8_t* frame, size_t frame_len) {
    intptr_t written = trevrpc_msquic_stream_write(stream, frame, frame_len);
    if (written < 0) {
        return (int)written;
    }
    if ((size_t)written != frame_len) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    return 0;
}

static trevrpc_stream* trevrpc_stream_alloc(trevrpc_msquic_stream* stream, size_t max_frame_size, bool owns_stream) {
    trevrpc_stream* rpc_stream = calloc(1, sizeof(*rpc_stream));
    if (rpc_stream == NULL) {
        return NULL;
    }

    rpc_stream->stream = stream;
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

static int trevrpc_stream_check_message_limit(trevrpc_stream* stream, int64_t* count, const char* direction) {
    if (stream->max_stream_messages >= 0 && *count >= stream->max_stream_messages) {
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
    if (stream->max_stream_body_size < 0 || *total_body_size <= (uint64_t)stream->max_stream_body_size) {
        return 0;
    }

    const char* message = strcmp(direction, "request") == 0 ? "request stream exceeded maximum body size"
                                                            : "response stream exceeded maximum body size";
    trevrpc_stream_record_failure(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, message);
    return TREVRPC_ERR_STREAM_LIMIT_EXCEEDED;
}

static int trevrpc_stream_check_exhausted_message_limit(
    trevrpc_stream* stream, const int64_t* count, const char* direction) {
    if (stream->max_stream_messages < 0 || *count < stream->max_stream_messages) {
        return 0;
    }

    const char* message = strcmp(direction, "request") == 0 ? "request stream exceeded maximum message count"
                                                            : "response stream exceeded maximum message count";
    trevrpc_stream_record_failure(stream, TREVRPC_STATUS_RESOURCE_EXHAUSTED, message);
    return TREVRPC_ERR_STREAM_LIMIT_EXCEEDED;
}

static int trevrpc_stream_check_exhausted_body_size_limit(
    trevrpc_stream* stream, const uint64_t* total_body_size, const char* direction) {
    if (stream->max_stream_body_size < 0 || *total_body_size <= (uint64_t)stream->max_stream_body_size) {
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

int trevrpc_stream_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    if (stream == NULL || stream->stream == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    int err = trevrpc_stream_context_error(stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_check_response_idle_timeout(stream);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_check_message_limit(stream, &stream->response_message_count, "response");
    if (err == 0) {
        err = trevrpc_stream_check_body_size_limit(stream, &stream->response_body_size, body_len, "response");
    }
    if (err != 0) {
        return err;
    }

    intptr_t written =
        trevrpc_msquic_stream_write_message_frame(stream->stream, body, body_len, stream->max_frame_size);
    return written < 0 ? (int)written : 0;
}

int trevrpc_stream_send_status(trevrpc_stream* stream, uint32_t status, const char* message, size_t message_len) {
    if (stream == NULL || stream->stream == NULL || (message == NULL && message_len > 0)) {
        return -EINVAL;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        status,
        message,
        message_len,
        NULL,
        0,
        NULL,
        stream->max_frame_size,
        &frame,
        &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream->stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        stream->sent_status = true;
        stream->terminal_status = trevrpc_status_code_from_uint32(status);
    }
    return err;
}

int trevrpc_stream_recv(trevrpc_stream* stream, trevrpc_stream_frame** out_frame) {
    if (stream == NULL || stream->stream == NULL || out_frame == NULL) {
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
    intptr_t read =
        stream->stream_idle_timeout_nanos == 0
            ? trevrpc_msquic_stream_read_frame(stream->stream, &body, &body_len, stream->max_frame_size)
            : trevrpc_msquic_stream_read_frame_timeout(
                  stream->stream, &body, &body_len, stream->max_frame_size, stream->stream_idle_timeout_nanos);
    if (read < 0) {
        if (read == TREV_MSQUIC_ERR_TIMEOUT) {
            trevrpc_stream_record_failure(stream, TREVRPC_STATUS_UNAVAILABLE, "request stream idle timeout");
            return TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
        }
        return (int)read;
    }
    if (read == 0) {
        return 0;
    }

    err = trevrpc_wire_decode_stream_frame(body, body_len, out_frame);
    trevrpc_msquic_free(body);
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

int trevrpc_stream_finish_send(trevrpc_stream* stream) {
    if (stream == NULL || stream->stream == NULL) {
        return -EINVAL;
    }

    return trevrpc_msquic_stream_shutdown_send(stream->stream);
}

void trevrpc_stream_close(trevrpc_stream* stream) {
    if (stream == NULL) {
        return;
    }

    if (stream->owns_stream) {
        trevrpc_msquic_stream_close(stream->stream);
    }
    free(stream);
}

int trevrpc_client_connect(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_client** out_client) {
    if (host == NULL || out_client == NULL) {
        return -EINVAL;
    }
    *out_client = NULL;

    trevrpc_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return -ENOMEM;
    }
    client->max_frame_size = trevrpc_effective_max_frame_size(config);

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_dial(host, port, &msquic_config, &client->conn);
    if (err != 0) {
        trevrpc_client_close(client);
        return err;
    }

    *out_client = client;
    return 0;
}

int trevrpc_client_call_unary(trevrpc_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_response** out_response) {
    if (client == NULL || out_response == NULL) {
        return -EINVAL;
    }
    *out_response = NULL;

    trevrpc_msquic_stream* stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err = trevrpc_wire_encode_request(
        service, method, TREVRPC_RPC_KIND_UNARY, body, body_len, NULL, 0, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        err = trevrpc_msquic_stream_shutdown_send(stream);
    }

    uint8_t* response_body = NULL;
    size_t response_body_len = 0;
    if (err == 0) {
        intptr_t read =
            trevrpc_msquic_stream_read_frame(stream, &response_body, &response_body_len, client->max_frame_size);
        if (read < 0) {
            err = (int)read;
        } else if (read == 0) {
            err = TREV_MSQUIC_ERR_CLOSED;
        }
    }
    if (err == 0) {
        err = trevrpc_wire_decode_response(response_body, response_body_len, out_response);
    }

    trevrpc_msquic_free(response_body);
    trevrpc_msquic_stream_close(stream);
    return err;
}

int trevrpc_client_start_stream(trevrpc_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** out_stream) {
    if (client == NULL || out_stream == NULL || service == NULL || method == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    *out_stream = NULL;
    if (kind == TREVRPC_RPC_KIND_UNARY || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    trevrpc_msquic_stream* raw_stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &raw_stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err = trevrpc_wire_encode_request(
        service, method, kind, body, body_len, NULL, 0, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(raw_stream, frame, frame_len);
    }
    free(frame);
    if (err != 0) {
        trevrpc_msquic_stream_close(raw_stream);
        return err;
    }

    trevrpc_stream* stream = trevrpc_stream_alloc(raw_stream, client->max_frame_size, true);
    if (stream == NULL) {
        trevrpc_msquic_stream_close(raw_stream);
        return -ENOMEM;
    }

    *out_stream = stream;
    return 0;
}

void trevrpc_client_close(trevrpc_client* client) {
    if (client == NULL) {
        return;
    }

    trevrpc_msquic_conn_close(client->conn);
    free(client);
}

static bool trevrpc_method_matches(
    const trevrpc_method* method, const char* service, size_t service_len, const char* name, size_t name_len) {
    return method->service_len == service_len && method->method_len == name_len &&
           memcmp(method->service, service, service_len) == 0 && memcmp(method->method, name, name_len) == 0;
}

int trevrpc_server_listen(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_server** out_server) {
    if (host == NULL || out_server == NULL) {
        return -EINVAL;
    }
    *out_server = NULL;

    trevrpc_server* server = calloc(1, sizeof(*server));
    if (server == NULL) {
        return -ENOMEM;
    }
    server->max_frame_size = trevrpc_effective_max_frame_size(config);
    server->options = trevrpc_default_server_options();
    pthread_mutex_init(&server->mutex, NULL);
    pthread_cond_init(&server->cond, NULL);

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_listen(host, port, &msquic_config, &server->listener);
    if (err != 0) {
        trevrpc_server_close(server);
        return err;
    }

    *out_server = server;
    return 0;
}

int trevrpc_server_set_options(trevrpc_server* server, const trevrpc_server_options* options) {
    if (server == NULL || options == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&server->mutex);
    server->options = *options;
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
    return 0;
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
    pthread_mutex_lock(&server->mutex);
    trevrpc_server_options options = server->options;
    pthread_mutex_unlock(&server->mutex);
    return options;
}

static trevrpc_authorizer trevrpc_server_authorizer_snapshot(trevrpc_server* server, void** out_user_data) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_authorizer authorizer = server->authorizer;
    *out_user_data = server->authorizer_user_data;
    pthread_mutex_unlock(&server->mutex);
    return authorizer;
}

static trevrpc_metrics trevrpc_server_metrics_snapshot(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_metrics metrics = server->metrics;
    pthread_mutex_unlock(&server->mutex);
    return metrics;
}

static trevrpc_transport_observer trevrpc_server_transport_observer_snapshot(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_transport_observer observer = server->transport_observer;
    pthread_mutex_unlock(&server->mutex);
    return observer;
}

static trevrpc_logger trevrpc_server_logger_snapshot(trevrpc_server* server) {
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
        .transport = TREVRPC_TRANSPORT_KIND_MSQUIC,
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

static void trevrpc_server_shutdown_connections(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    for (trevrpc_server_conn_ref* ref = server->conns; ref != NULL; ref = ref->next) {
        trevrpc_msquic_conn_shutdown(ref->conn);
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

static int trevrpc_server_conn_add(
    trevrpc_server* server, trevrpc_msquic_conn* conn, trevrpc_server_conn_ref** out_ref) {
    *out_ref = NULL;
    trevrpc_server_conn_ref* ref = malloc(sizeof(*ref));
    if (ref == NULL) {
        return -ENOMEM;
    }
    ref->conn = conn;

    pthread_mutex_lock(&server->mutex);
    ref->next = server->conns;
    server->conns = ref;
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);

    if (shutting_down) {
        trevrpc_msquic_conn_shutdown(conn);
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

static void trevrpc_set_status(trevrpc_response* response, uint32_t status, const char* message) {
    response->status = status;
    if (message != NULL) {
        (void)trevrpc_response_set_message(response, message, strlen(message));
    }
}

static void trevrpc_server_write_response(
    trevrpc_msquic_stream* stream, size_t max_frame_size, trevrpc_response* response) {
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_response(response, max_frame_size, &frame, &frame_len);
    if (err != 0 && response->status == TREVRPC_STATUS_OK) {
        trevrpc_response_reset(response);
        trevrpc_set_status(response, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "response frame exceeded maximum size");
        err = trevrpc_wire_encode_response(response, max_frame_size, &frame, &frame_len);
    }
    if (err == 0) {
        (void)trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    (void)trevrpc_msquic_stream_shutdown_send(stream);
}

static void trevrpc_server_write_status(
    trevrpc_msquic_stream* stream, size_t max_frame_size, uint32_t status, const char* message) {
    trevrpc_response response = {0};
    trevrpc_set_status(&response, status, message);
    trevrpc_server_write_response(stream, max_frame_size, &response);
    trevrpc_response_reset(&response);
}

static void trevrpc_server_write_stream_status(
    trevrpc_msquic_stream* stream, size_t max_frame_size, uint32_t status, const char* message) {
    trevrpc_stream rpc_stream = {
        .stream = stream,
        .max_frame_size = max_frame_size,
        .max_stream_messages = TREVRPC_STREAM_LIMIT_DISABLED,
        .max_stream_body_size = TREVRPC_STREAM_LIMIT_DISABLED,
        .failure_status = TREVRPC_STATUS_OK,
    };
    (void)trevrpc_stream_send_status(&rpc_stream, status, message, message == NULL ? 0 : strlen(message));
    (void)trevrpc_msquic_stream_shutdown_send(stream);
}

static uint32_t trevrpc_status_from_error(int err, const char** message) {
    switch (err) {
    case TREVRPC_ERR_STREAM_LIMIT_EXCEEDED:
        *message = "stream limit exceeded";
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

static void trevrpc_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_msquic_stream_read_frame(stream, &body, &body_len, server->max_frame_size);
    if (read < 0) {
        uint32_t status =
            read == TREV_MSQUIC_ERR_FRAME_TOO_LARGE ? TREVRPC_STATUS_RESOURCE_EXHAUSTED : TREVRPC_STATUS_UNAVAILABLE;
        const char* message = read == TREV_MSQUIC_ERR_FRAME_TOO_LARGE ? "request frame exceeded maximum size"
                                                                      : "failed to read request frame";
        trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, (int)read, message);
        trevrpc_server_write_status(stream, server->max_frame_size, status, message);
        return;
    }
    if (read == 0) {
        return;
    }

    trevrpc_request request;
    int err = trevrpc_wire_decode_request(body, body_len, &request);
    if (err == TREVRPC_ERR_INVALID_FRAME) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_WARN, "rpc.decode_failed", "invalid request frame", NULL, err);
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request frame");
        trevrpc_msquic_free(body);
        return;
    }
    if (err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_WARN, "rpc.decode_failed", "unsupported TrevRPC wire version", NULL, err);
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_FAILED_PRECONDITION, "unsupported TrevRPC wire version");
        trevrpc_msquic_free(body);
        return;
    }
    if (err != 0) {
        trevrpc_log(server, TREVRPC_LOG_LEVEL_WARN, "rpc.decode_failed", "invalid request", NULL, err);
        trevrpc_server_write_status(stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request");
        trevrpc_msquic_free(body);
        return;
    }

    struct timespec rpc_started_at = {0};
    (void)trevrpc_clock_now(&rpc_started_at);
    trevrpc_metrics_record_started(server, &request);

    trevrpc_call_context context;
    err = trevrpc_call_context_init(&context, server, &request);
    if (err == -ERANGE) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout is too large");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout is too large");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_INVALID_ARGUMENT, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }
    if (err == -EOVERFLOW) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout overflowed");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "RPC timeout overflowed");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_INVALID_ARGUMENT, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }
    if (err != 0) {
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_INTERNAL, "failed to prepare request");
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_INTERNAL, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }
    if (trevrpc_call_context_deadline_expired(&context)) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_DEADLINE_EXCEEDED, "RPC deadline exceeded");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_DEADLINE_EXCEEDED, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
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
                trevrpc_server_write_response(stream, server->max_frame_size, &response);
                trevrpc_response_reset(&response);
            } else {
                trevrpc_server_write_stream_status(stream, server->max_frame_size, status.code, status.message);
            }
            trevrpc_metrics_record_finished(server, &request, 0, status.code, &rpc_started_at);
            trevrpc_request_reset(&request);
            trevrpc_msquic_free(body);
            return;
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
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "too many concurrent RPCs");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "too many concurrent RPCs");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_RESOURCE_EXHAUSTED, &rpc_started_at);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
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
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_UNIMPLEMENTED, &rpc_started_at);
        trevrpc_server_request_finish(server);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }
    if (method->kind != request.kind) {
        trevrpc_log(server,
            TREVRPC_LOG_LEVEL_WARN,
            "rpc.kind_mismatch",
            "method RPC kind mismatch",
            &request,
            TREVRPC_STATUS_UNIMPLEMENTED);
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        }
        trevrpc_metrics_record_finished(server, &request, 0, TREVRPC_STATUS_UNIMPLEMENTED, &rpc_started_at);
        trevrpc_server_request_finish(server);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }

    if (request.kind != TREVRPC_RPC_KIND_UNARY) {
        trevrpc_server_options options = trevrpc_server_options_snapshot(server);
        trevrpc_stream rpc_stream = {
            .stream = stream,
            .context = &context,
            .max_frame_size = server->max_frame_size,
            .max_stream_messages = options.max_stream_messages,
            .max_stream_body_size = options.max_stream_body_size,
            .stream_idle_timeout_nanos = options.stream_idle_timeout_nanos,
            .failure_status = TREVRPC_STATUS_OK,
        };
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
            final_status = trevrpc_status_from_error(err, &message);
            trevrpc_log(server, TREVRPC_LOG_LEVEL_ERROR, "rpc.handler_failed", message, &request, err);
            (void)trevrpc_stream_send_status(&rpc_stream, final_status, message, message == NULL ? 0 : strlen(message));
        } else if (!rpc_stream.sent_status) {
            rpc_stream.context = NULL;
            (void)trevrpc_stream_send_status(&rpc_stream, TREVRPC_STATUS_OK, NULL, 0);
        } else {
            final_status = rpc_stream.terminal_status;
        }
        (void)trevrpc_stream_finish_send(&rpc_stream);
        trevrpc_metrics_record_finished(
            server, &request, (size_t)rpc_stream.response_body_size, final_status, &rpc_started_at);
        trevrpc_server_request_finish(server);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
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
    }

    trevrpc_server_write_response(stream, server->max_frame_size, &response);
    trevrpc_metrics_record_finished(server, &request, response.body_len, response.status, &rpc_started_at);
    trevrpc_response_reset(&response);
    trevrpc_server_request_finish(server);
    trevrpc_request_reset(&request);
    trevrpc_msquic_free(body);
}

static void* trevrpc_stream_thread(void* arg) {
    trevrpc_stream_task* task = arg;
    trevrpc_transport_record_event(task->server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, 0, NULL);
    trevrpc_handle_stream(task->server, task->stream);
    trevrpc_transport_record_event(task->server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, 0, NULL);
    trevrpc_msquic_stream_close(task->stream);
    trevrpc_conn_stream_finish(task->stream_limiter);
    trevrpc_server_task_finish(task->server);
    free(task);
    return NULL;
}

static void* trevrpc_conn_thread(void* arg) {
    trevrpc_conn_task* task = arg;
    trevrpc_server* server = task->server;
    trevrpc_msquic_conn* conn = task->conn;
    free(task);

    trevrpc_server_conn_ref* conn_ref = NULL;
    if (trevrpc_server_conn_add(server, conn, &conn_ref) != 0) {
        trevrpc_transport_record_event(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, -ENOMEM, "failed to track connection");
        trevrpc_msquic_conn_close(conn);
        trevrpc_server_connection_finish(server);
        trevrpc_server_task_finish(server);
        return NULL;
    }

    trevrpc_conn_stream_limiter stream_limiter = {0};
    if (trevrpc_conn_stream_limiter_init(&stream_limiter) != 0) {
        trevrpc_transport_record_event(
            server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, -ENOMEM, "failed to initialize stream limiter");
        trevrpc_server_conn_remove(server, conn_ref);
        trevrpc_msquic_conn_close(conn);
        trevrpc_server_connection_finish(server);
        trevrpc_server_task_finish(server);
        return NULL;
    }

    while (!trevrpc_server_is_shutting_down(server)) {
        trevrpc_msquic_stream* stream = NULL;
        int err = trevrpc_msquic_conn_accept_stream(conn, &stream);
        if (err != 0) {
            if (!trevrpc_server_is_shutting_down(server)) {
                trevrpc_transport_record_event(
                    server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, err, "failed to accept stream");
            }
            break;
        }

        trevrpc_server_options options = trevrpc_server_options_snapshot(server);
        if (!trevrpc_conn_stream_try_start(&stream_limiter, options.max_concurrent_streams_per_connection)) {
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, 0, NULL);
            trevrpc_transport_record_event(
                server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, 0, "too many concurrent streams on connection");
            trevrpc_server_write_status(stream,
                server->max_frame_size,
                TREVRPC_STATUS_RESOURCE_EXHAUSTED,
                "too many concurrent streams on connection");
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, 0, NULL);
            trevrpc_msquic_stream_close(stream);
            continue;
        }

        if (!trevrpc_server_task_start(server)) {
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, 0, NULL);
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, 0, "server is shutting down");
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, 0, NULL);
            trevrpc_conn_stream_finish(&stream_limiter);
            trevrpc_msquic_stream_close(stream);
            break;
        }

        trevrpc_stream_task* stream_task = malloc(sizeof(*stream_task));
        if (stream_task == NULL) {
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, 0, NULL);
            trevrpc_transport_record_event(
                server, TREVRPC_TRANSPORT_EVENT_STREAM_ERROR, -ENOMEM, "failed to allocate stream task");
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "failed to allocate stream task");
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, 0, NULL);
            trevrpc_conn_stream_finish(&stream_limiter);
            trevrpc_msquic_stream_close(stream);
            trevrpc_server_task_finish(server);
            continue;
        }
        stream_task->server = server;
        stream_task->stream = stream;
        stream_task->stream_limiter = &stream_limiter;

        pthread_t thread;
        err = pthread_create(&thread, NULL, trevrpc_stream_thread, stream_task);
        if (err != 0) {
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_OPEN, 0, "handling stream inline");
            trevrpc_handle_stream(server, stream);
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE, 0, NULL);
            trevrpc_msquic_stream_close(stream);
            trevrpc_conn_stream_finish(&stream_limiter);
            trevrpc_server_task_finish(server);
            free(stream_task);
            continue;
        }
        pthread_detach(thread);
    }

    trevrpc_conn_stream_limiter_wait(&stream_limiter);
    trevrpc_conn_stream_limiter_destroy(&stream_limiter);
    trevrpc_server_conn_remove(server, conn_ref);
    trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE, 0, NULL);
    trevrpc_msquic_conn_close(conn);
    trevrpc_server_connection_finish(server);
    trevrpc_server_task_finish(server);
    return NULL;
}

int trevrpc_server_serve(trevrpc_server* server) {
    if (server == NULL) {
        return -EINVAL;
    }
    if (!trevrpc_server_task_start(server)) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_LISTENER_OPEN, 0, NULL);

    int result = 0;
    for (;;) {
        trevrpc_msquic_conn* conn = NULL;
        int err = trevrpc_msquic_listener_accept(server->listener, &conn);
        if (err != 0) {
            if (!trevrpc_server_is_shutting_down(server)) {
                trevrpc_transport_record_event(
                    server, TREVRPC_TRANSPORT_EVENT_LISTENER_ERROR, err, "failed to accept connection");
            }
            result = trevrpc_server_is_shutting_down(server) ? 0 : err;
            break;
        }

        if (!trevrpc_server_connection_try_start(server)) {
            trevrpc_transport_record_event(
                server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, 0, "too many concurrent connections");
            trevrpc_msquic_conn_close(conn);
            continue;
        }

        if (!trevrpc_server_task_start(server)) {
            trevrpc_server_connection_finish(server);
            trevrpc_msquic_conn_close(conn);
            continue;
        }

        trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_CONNECTION_OPEN, 0, NULL);

        trevrpc_conn_task* task = malloc(sizeof(*task));
        if (task == NULL) {
            trevrpc_transport_record_event(
                server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, -ENOMEM, "failed to allocate connection task");
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE, 0, NULL);
            trevrpc_msquic_conn_close(conn);
            trevrpc_server_connection_finish(server);
            trevrpc_server_task_finish(server);
            result = -ENOMEM;
            break;
        }
        task->server = server;
        task->conn = conn;

        pthread_t thread;
        err = pthread_create(&thread, NULL, trevrpc_conn_thread, task);
        if (err != 0) {
            trevrpc_transport_record_event(
                server, TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR, -err, "failed to start connection thread");
            trevrpc_transport_record_event(server, TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE, 0, NULL);
            free(task);
            trevrpc_msquic_conn_close(conn);
            trevrpc_server_connection_finish(server);
            trevrpc_server_task_finish(server);
            result = -err;
            break;
        }
        pthread_detach(thread);
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
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);

    trevrpc_server_shutdown_connections(server);
    trevrpc_msquic_listener_shutdown(server->listener);
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

    trevrpc_msquic_listener_close(server->listener);
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
