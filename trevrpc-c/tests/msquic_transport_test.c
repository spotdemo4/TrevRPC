#define _POSIX_C_SOURCE 200809L

#include "trevrpc_msquic.h"
#include "trevrpc.h"
#include "trevrpc_webtransport.h"
#include "trevrpc_wire_internal.h"

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

    CHECK_GOTO(connect_pair_with_config(&config, &listener, &client, &server) == 0);
    CHECK_GOTO(open_stream_pair(client, server, &client_stream, &server_stream) == 0);
    borrowed = malloc(borrowed_len);
    CHECK_GOTO(borrowed != NULL);
    memset(borrowed, 0x5a, borrowed_len);
    const trevrpc_msquic_frame_part parts[] = {
        {.data = borrowed, .len = borrowed_len},
    };
    CHECK_EQ_GOTO(trevrpc_msquic_stream_write_frame_parts(
                      client_stream, parts, sizeof(parts) / sizeof(parts[0]), config.max_frame_size),
        (int)(4 + borrowed_len));

    CHECK_EQ_GOTO(trevrpc_msquic_stream_abort(client_stream), 0);
    trevrpc_msquic_stream_close(client_stream);
    client_stream = NULL;
    free(borrowed);
    borrowed = NULL;

    result = 0;

cleanup:
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

int main(void) {
    int result = 1;

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
    if (test_pending_send_slow_reader_is_bounded_and_close_drains() != 0) {
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
    result = 0;

cleanup:
    return result;
}
