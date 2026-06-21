#define _POSIX_C_SOURCE 200809L

#include "trevrpc_msquic.h"
#include "trevrpc_webtransport.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

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

static const trevrpc_msquic_config test_config = {
    .alpn = "trevrpc",
    .alpn_len = 7,
    .cert_file = TREVRPC_MSQUIC_TEST_CERT,
    .key_file = TREVRPC_MSQUIC_TEST_KEY,
    .peer_bidi_stream_count = 8,
};

static const trevrpc_msquic_config test_h3_config = {
    .alpn = "h3",
    .alpn_len = 2,
    .cert_file = TREVRPC_MSQUIC_TEST_CERT,
    .key_file = TREVRPC_MSQUIC_TEST_KEY,
    .peer_bidi_stream_count = 8,
};

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

static int connect_pair(
    trevrpc_msquic_listener** out_listener, trevrpc_msquic_conn** out_client, trevrpc_msquic_conn** out_server) {
    int result = 1;
    trevrpc_msquic_listener* listener = NULL;
    trevrpc_msquic_conn* client = NULL;
    trevrpc_msquic_conn* server = NULL;
    pthread_t thread = {0};
    bool thread_started = false;
    accept_args args = {0};
    uint16_t port = 0;

    CHECK_GOTO(trevrpc_msquic_listen("127.0.0.1", 0, &test_config, &listener) == 0);
    CHECK_GOTO(trevrpc_msquic_listener_port(listener, &port) == 0);
    CHECK_GOTO(port != 0);
    args.listener = listener;
    CHECK_GOTO(pthread_create(&thread, NULL, accept_conn_thread, &args) == 0);
    thread_started = true;
    CHECK_GOTO(trevrpc_msquic_dial("127.0.0.1", port, &test_config, &client) == 0);
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

static int test_header_block_put_literal(
    uint8_t* out, size_t out_len, size_t* offset, const char* name, const char* value) {
    size_t name_len = strlen(name);
    size_t value_len = strlen(value);
    if (out_len - *offset < 1) {
        return -1;
    }
    out[(*offset)++] = 0x20;
    if (test_varint_write(out, out_len, offset, name_len) != 0 || out_len - *offset < name_len) {
        return -1;
    }
    memcpy(out + *offset, name, name_len);
    *offset += name_len;
    if (test_varint_write(out, out_len, offset, value_len) != 0 || out_len - *offset < value_len) {
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
    const char* authority) {
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

    CHECK_GOTO(trevrpc_msquic_conn_open_stream(client_conn, &local_control) == 0);
    CHECK_GOTO(trevrpc_msquic_stream_write(local_control, test_case->control, test_case->control_len) ==
               (intptr_t)test_case->control_len);

    if (test_case->headers != NULL) {
        CHECK_GOTO(trevrpc_msquic_conn_accept_stream(client_conn, &peer_control) == 0);
        CHECK_GOTO(trevrpc_msquic_stream_read(peer_control, server_control, sizeof(server_control)) > 0);
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
    const uint8_t control[] = {0x00, 0x04, 0x05, 0xab, 0x60, 0x37, 0x42, 0x01};
    const uint8_t headers[] = {0x01, 0x03, 0x00, 0x00, 0x80};
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
        .headers = headers,
        .headers_len = sizeof(headers),
    };
    return run_malformed_wt_peer_case(&test_case);
}

static int test_webtransport_rejects_missing_connect_pseudo_header(void) {
    int result = 1;
    uint8_t headers[512];
    size_t headers_len = 0;
    const uint8_t control[] = {0x00, 0x04, 0x05, 0xab, 0x60, 0x37, 0x42, 0x01};

    CHECK_GOTO(test_build_connect_headers(
                   headers, sizeof(headers), &headers_len, "CONNECT", "webtransport", "https", NULL, "127.0.0.1") == 0);
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
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
    uint8_t headers[512];
    size_t headers_len = 0;
    const uint8_t control[] = {0x00, 0x04, 0x05, 0xab, 0x60, 0x37, 0x42, 0x01};

    CHECK_GOTO(
        test_build_connect_headers(
            headers, sizeof(headers), &headers_len, "GET", "webtransport", "https", "/trevrpc", "127.0.0.1") == 0);
    const malformed_wt_peer_case test_case = {
        .control = control,
        .control_len = sizeof(control),
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
    if (test_stream_reset_unblocks_peer_read() != 0) {
        return 1;
    }
    if (test_client_close_unblocks_server_accept_stream() != 0) {
        return 1;
    }
    if (test_webtransport_connects_h3_quic_session() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_path_mismatch() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_malformed_control_stream_type() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_missing_webtransport_setting() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_malformed_settings_payload() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_malformed_qpack_block() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_missing_connect_pseudo_header() != 0) {
        return 1;
    }
    if (test_webtransport_rejects_invalid_connect_method() != 0) {
        return 1;
    }
    if (test_webtransport_listener_shutdown_unblocks_accept() != 0) {
        return 1;
    }
    if (test_webtransport_session_shutdown_unblocks_accept_stream() != 0) {
        return 1;
    }
    if (test_webtransport_stream_close_unblocks_peer_read() != 0) {
        return 1;
    }
    return 0;
}
