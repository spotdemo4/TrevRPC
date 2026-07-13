#define _POSIX_C_SOURCE 200809L

#include "trevrpc_msquic.h"
#include "trevrpc.h"
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

typedef struct wt_accept_args {
    trevrpc_wt_listener* listener;
    trevrpc_wt_session* session;
    int result;
} wt_accept_args;

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

typedef struct malformed_wt_peer_case {
    const uint8_t* control;
    size_t control_len;
    const uint8_t* headers;
    size_t headers_len;
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
};

#define TEST_WT_SETTINGS_ENABLE_CONNECT_PROTOCOL 0x08
#define TEST_WT_SETTINGS_H3_DATAGRAM 0x33
#define TEST_WT_SETTINGS_H3_DRAFT04_DATAGRAM 0xffd277
#define TEST_WT_SETTINGS_WEBTRANSPORT_DRAFT02 0x2b603742
#define TEST_WT_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07 0xc671706a
#define TEST_WT_SETTINGS_WT_ENABLED_DRAFT15 0x2c7cf000
#define TEST_WT_SETTINGS_WT_MAX_SESSIONS 0x14e9cd29

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

static void* accept_wt_session_thread(void* arg) {
    wt_accept_args* args = arg;
    args->result = trevrpc_wt_listener_accept_session(args->listener, &args->session);
    return NULL;
}

static void* accept_wt_stream_thread(void* arg) {
    wt_stream_args* args = arg;
    args->result = trevrpc_wt_session_accept_stream(args->session, &args->stream);
    return NULL;
}

static void* accept_h3_conn_thread(void* arg) {
    h3_accept_args* args = arg;
    args->result = trevrpc_h3_accept_from_msquic(
        args->conn, &args->config, 1, "/rpc", args->admission, args->admission_user_data, 1024, &args->h3_conn);
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

static int connect_pair_with_config(const trevrpc_msquic_config* config,
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

    CHECK_GOTO(trevrpc_msquic_listen("127.0.0.1", 0, config, &listener) == 0);
    CHECK_GOTO(trevrpc_msquic_listener_port(listener, &port) == 0);
    CHECK_GOTO(port != 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_conn_thread, &args) == 0);
    thread_started = true;
    CHECK_GOTO(trevrpc_msquic_dial("127.0.0.1", port, config, &client) == 0);
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

static int connect_pair(
    trevrpc_msquic_listener** out_listener, trevrpc_msquic_conn** out_client, trevrpc_msquic_conn** out_server) {
    return connect_pair_with_config(&test_config, out_listener, out_client, out_server);
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

static int test_wait_peer_close_error(trevrpc_msquic_conn* conn, uint64_t expected) {
    const struct timespec pause = {.tv_nsec = 1000000};
    for (size_t i = 0; i < 1000; i++) {
        uint64_t error_code = 0;
        if (trevrpc_msquic_conn_peer_close_error(conn, &error_code) == 0) {
            return error_code == expected ? 0 : -1;
        }
        nanosleep(&pause, NULL);
    }
    return -1;
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
    trevrpc_stream_frame* decoded = NULL;

    CHECK_GOTO(open_native_stream_pair(&listener, &client, &server, &client_stream, &server_stream) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_fin(client_stream, malformed_frame, sizeof(malformed_frame)),
        (int)sizeof(malformed_frame));
    CHECK_EQ_GOTO(trevrpc_msquic_stream_read_frame_timeout(server_stream, &body, &body_len, 64, 1000000000ull), 1);
    CHECK_EQ_GOTO(trevrpc_wire_decode_stream_frame(body, body_len, &decoded), TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(decoded == NULL);

    result = 0;

cleanup:
    trevrpc_stream_frame_free(decoded);
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
    CHECK_EQ_GOTO(trevrpc_msquic_send_completion_wait(completion), -ECANCELED);
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
    trevrpc_stream_frame* frame = NULL;
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
    CHECK_GOTO(frame->body_len == body_len);
    CHECK_GOTO(memcmp(frame->body, expected, body_len) == 0);

    result = 0;

cleanup:
    trevrpc_stream_frame_free(frame);
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
        trevrpc_stream_frame* frame = NULL;
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
        CHECK_GOTO(frame->body_len == body_lens[i]);
        CHECK_GOTO(memcmp(frame->body, expected[i], body_lens[i]) == 0);
        trevrpc_stream_frame_free(frame);
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
    trevrpc_msquic_stream* peer_control = NULL;
    trevrpc_msquic_stream* connect_stream = NULL;
    wt_accept_args accept_args = {0};
    pthread_t accept_thread = {0};
    bool accept_thread_started = false;
    uint8_t server_control[32];
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
    CHECK_GOTO(trevrpc_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &peer_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(peer_control, server_control, sizeof(server_control)) > 0);

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &local_control) == 0);
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
    trevrpc_msquic_stream_close(peer_control);
    trevrpc_msquic_stream_close(local_control);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_wt_session_close(accept_args.session);
    trevrpc_wt_listener_close(listener);
    return result;
}

static int run_wt_accepts_raw_peer_case(
    const uint8_t* control, size_t control_len, const char* protocol, bool draft02_request) {
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
    CHECK_GOTO(trevrpc_msquic_dial("127.0.0.1", port, &test_h3_config, &client_conn) == 0);

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &local_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_write(local_control, control, control_len) == (intptr_t)control_len);
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &peer_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(peer_control, server_control, sizeof(server_control)) > 0);

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
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport", true);
}

static int test_webtransport_accepts_draft07_peer(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft07_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport", false);
}

static int test_webtransport_accepts_draft15_peer(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft15_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport-h3", false);
}

static int test_webtransport_h3_control_and_connect_remain_byte_oriented(void) {
    uint8_t control[64];
    size_t control_len = 0;
    if (test_build_draft15_control_settings(control, sizeof(control), &control_len) != 0) {
        return 1;
    }
    return run_wt_accepts_raw_peer_case(control, control_len, "webtransport-h3", false);
}

static int test_webtransport_rejects_malformed_control_stream_type(void) {
    const uint8_t control[] = {0x01, 0x04, 0x00};
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
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

static int test_http3_post_data_adapter_and_request_local_rejection(void) {
    int result = 1;
    int admission_calls = 0;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client_conn = NULL;
    trevrpc_msquic_conn* server_conn = NULL;
    trevrpc_msquic_stream* server_control = NULL;
    trevrpc_msquic_stream* client_control = NULL;
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

    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
    CHECK_GOTO(pthread_join(accept_thread, NULL) == 0);
    accept_thread_started = false;
    server_conn = NULL;
    CHECK_EQ_GOTO(accept_args.result, 0);
    CHECK_GOTO(accept_args.h3_conn != NULL);

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
                   "webtransport-h3",
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
    trevrpc_msquic_stream_close(server_control);
    trevrpc_h3_conn_close(accept_args.h3_conn);
    trevrpc_msquic_conn_close(server_conn);
    trevrpc_msquic_conn_close(client_conn);
    trevrpc_msquic_listener_close(listener);
    return result;
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
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
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
    CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &server_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_read(server_control, control, sizeof(control)) > 0);
    CHECK_GOTO(test_build_draft15_control_settings(control, sizeof(control), &control_len) == 0);
    CHECK_GOTO(trevrpc_msquic_conn_open_uni_stream(client_conn, &client_control) == 0);
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write(client_control, control, control_len), (int)control_len);
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

int main(void) {
    int result = 1;

    if (test_listener_shutdown_is_concurrent_and_idempotent() != 0) {
        goto cleanup;
    }
    if (test_stream_reset_unblocks_peer_read() != 0) {
        goto cleanup;
    }
    if (test_stream_write_fin_close_preserves_peer_eof() != 0) {
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
    if (test_buffering_profile_settings_connect_and_transfer() != 0) {
        goto cleanup;
    }
    if (test_invalid_execution_profile_is_rejected() != 0) {
        goto cleanup;
    }
    if (test_frame_parts_borrowed_body_close_drains_send_complete() != 0) {
        goto cleanup;
    }
    if (test_frame_parts_borrowed_body_reset_drains_send_complete() != 0) {
        goto cleanup;
    }
    if (test_stream_borrowed_message_wait_drains_send_complete() != 0) {
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
    if (test_webtransport_connects_h3_quic_session() != 0) {
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
    if (test_webtransport_accepts_draft15_peer() != 0) {
        goto cleanup;
    }
    if (test_webtransport_h3_control_and_connect_remain_byte_oriented() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_malformed_control_stream_type() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_missing_webtransport_setting() != 0) {
        goto cleanup;
    }
    if (test_webtransport_rejects_malformed_settings_payload() != 0) {
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
    if (test_http3_qpack_failure_closes_connection() != 0) {
        goto cleanup;
    }
    if (test_http3_forbidden_first_frame_closes_connection() != 0) {
        goto cleanup;
    }
    if (test_http3_closed_control_stream_closes_connection() != 0) {
        goto cleanup;
    }
    result = 0;

cleanup:
    return result;
}
