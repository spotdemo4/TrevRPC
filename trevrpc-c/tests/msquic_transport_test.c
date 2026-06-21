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

static const trevrpc_msquic_config test_config = {
    .alpn = "trevrpc",
    .alpn_len = 7,
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
