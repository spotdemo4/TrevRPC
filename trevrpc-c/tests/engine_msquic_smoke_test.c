#define _POSIX_C_SOURCE 200809L

#include "trevrpc_engine_msquic.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#define TREVRPC_MSQUIC_TEST_CERT ""
#endif
#ifndef TREVRPC_MSQUIC_TEST_KEY
#define TREVRPC_MSQUIC_TEST_KEY ""
#endif

#define SEEN_CLIENT_CONNECTION 0x0001u
#define SEEN_SERVER_CONNECTION 0x0002u
#define SEEN_CLIENT_STREAM 0x0004u
#define SEEN_SERVER_STREAM 0x0008u
#define SEEN_CLIENT_SEND 0x0010u
#define SEEN_SERVER_READABLE 0x0020u
#define SEEN_SERVER_SEND 0x0040u
#define SEEN_CLIENT_READABLE 0x0080u
#define SEEN_REPLACEMENT_STREAM 0x0100u
#define SEEN_REPLACEMENT_PEER_STREAM 0x0200u
#define SEEN_CLIENT_SEND_SECOND 0x0400u
#define SEEN_REPLACEMENT_READABLE 0x0800u

typedef struct observations {
    trevrpc_engine_handle_v1 client_connection;
    trevrpc_engine_handle_v1 server_connection;
    trevrpc_engine_handle_v1 client_stream;
    trevrpc_engine_handle_v1 server_stream;
    trevrpc_engine_handle_v1 replacement_stream;
    trevrpc_engine_handle_v1 replacement_peer_stream;
    uint32_t seen;
    uint32_t client_send_completions;
} observations;

typedef struct admission_observations {
    trevrpc_engine_handle_v1 extra_connection;
    trevrpc_engine_handle_v1 local_streams[2];
    trevrpc_engine_handle_v1 peer_streams[2];
    trevrpc_engine_handle_v1 terminal_subjects[8];
    uint32_t ready_connections;
    uint32_t failed_connections;
    uint32_t terminal_count;
    uint64_t local_operation_ids[2];
    uint32_t peer_stream_count;
    uint32_t local_stream_count;
    uint32_t saw_connection_rejection;
} admission_observations;

static uint64_t monotonic_millis(void) {
    struct timespec now;
    assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
    return (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u;
}

static void pump_until(
    trevrpc_engine* adapter, const trevrpc_engine_wake_source_v1* wake, observations* observed, uint32_t wanted) {
    uint64_t deadline = monotonic_millis() + 10000;
    while ((observed->seen & wanted) != wanted && monotonic_millis() < deadline) {
        struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN};
        assert(poll(&descriptor, 1, 1000) >= 0);
        for (;;) {
            trevrpc_engine_event* event = NULL;
            int result = trevrpc_engine_next_event(adapter, &event);
            if (result == -EAGAIN) {
                break;
            }
            assert(result == 0);
            trevrpc_engine_event_info_v1 info;
            assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
            assert(info.status == 0);
            if (info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_READY) {
                if (info.operation_id == 1) {
                    observed->client_connection = info.subject;
                    observed->seen |= SEEN_CLIENT_CONNECTION;
                } else {
                    observed->server_connection = info.subject;
                    observed->seen |= SEEN_SERVER_CONNECTION;
                }
            } else if (info.kind == TREVRPC_ENGINE_EVENT_STREAM_READY) {
                if (info.operation_id == 2) {
                    observed->client_stream = info.subject;
                    observed->seen |= SEEN_CLIENT_STREAM;
                } else if (info.operation_id == 5) {
                    observed->replacement_stream = info.subject;
                    observed->seen |= SEEN_REPLACEMENT_STREAM;
                } else if ((observed->seen & SEEN_SERVER_STREAM) == 0) {
                    observed->server_stream = info.subject;
                    observed->seen |= SEEN_SERVER_STREAM;
                } else {
                    observed->replacement_peer_stream = info.subject;
                    observed->seen |= SEEN_REPLACEMENT_PEER_STREAM;
                }
            } else if (info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE) {
                if (info.operation_id == 3) {
                    observed->client_send_completions++;
                    observed->seen |=
                        observed->client_send_completions == 1 ? SEEN_CLIENT_SEND : SEEN_CLIENT_SEND_SECOND;
                } else if (info.operation_id == 4) {
                    observed->seen |= SEEN_SERVER_SEND;
                }
            } else if (info.kind == TREVRPC_ENGINE_EVENT_STREAM_READABLE) {
                if (info.subject.owner == observed->server_stream.owner &&
                    info.subject.slot == observed->server_stream.slot &&
                    info.subject.generation == observed->server_stream.generation) {
                    observed->seen |= SEEN_SERVER_READABLE;
                } else if (info.subject.owner == observed->replacement_peer_stream.owner &&
                           info.subject.slot == observed->replacement_peer_stream.slot &&
                           info.subject.generation == observed->replacement_peer_stream.generation) {
                    observed->seen |= SEEN_REPLACEMENT_READABLE;
                } else {
                    observed->seen |= SEEN_CLIENT_READABLE;
                }
            }
            trevrpc_engine_event_release(event);
        }
    }
    if ((observed->seen & wanted) != wanted) {
        fprintf(stderr, "pump timeout (seen=0x%08x wanted=0x%08x)\n", observed->seen, wanted);
    }
    assert((observed->seen & wanted) == wanted);
}

static trevrpc_engine_receive* check_receive(
    trevrpc_engine* adapter, trevrpc_engine_handle_v1 stream, const uint8_t* expected, size_t expected_len) {
    trevrpc_engine_receive* receive = NULL;
    trevrpc_engine_receive_info_v1 info;
    assert(trevrpc_engine_stream_receive_frame(adapter, stream, &receive) == 0);
    assert(trevrpc_engine_receive_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_engine_receive_get_info_v1(receive, &info) == 0);
    assert(info.data_len == expected_len);
    assert(memcmp(info.data, expected, expected_len) == 0);
    return receive;
}

static void wait_for_pending_sends(trevrpc_engine* adapter) {
    uint64_t deadline = monotonic_millis() + 10000;
    for (;;) {
        trevrpc_engine_diagnostics_v1 diagnostics;
        assert(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
        assert(trevrpc_engine_get_diagnostics_v1(adapter, &diagnostics) == 0);
        if (diagnostics.pending_send_count == 0) {
            return;
        }
        assert(monotonic_millis() < deadline);
    }
}

#define ADMISSION_READY_CONNECTIONS 0x01u
#define ADMISSION_FAILED_CONNECTION 0x02u
#define ADMISSION_LOCAL_STREAMS 0x04u
#define ADMISSION_PEER_STREAM 0x08u

static void check_admission_diagnostics(trevrpc_engine* adapter, uint64_t* max_connections, uint64_t* max_streams) {
    trevrpc_engine_diagnostics_v1 diagnostics;
    assert(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    assert(trevrpc_engine_get_diagnostics_v1(adapter, &diagnostics) == 0);
    assert(diagnostics.live_connections <= 3);
    assert(diagnostics.live_streams <= 3);
    assert(diagnostics.terminal_status == 0);
    assert(diagnostics.provider_error_code == 0);
    if (diagnostics.live_connections > *max_connections) {
        *max_connections = diagnostics.live_connections;
    }
    if (diagnostics.live_streams > *max_streams) {
        *max_streams = diagnostics.live_streams;
    }
}

static void admission_record_terminal(admission_observations* observed, trevrpc_engine_handle_v1 subject) {
    for (uint32_t index = 0; index < observed->terminal_count; index++) {
        trevrpc_engine_handle_v1 prior = observed->terminal_subjects[index];
        assert(prior.owner != subject.owner || prior.slot != subject.slot || prior.generation != subject.generation);
    }
    assert(observed->terminal_count < sizeof(observed->terminal_subjects) / sizeof(observed->terminal_subjects[0]));
    observed->terminal_subjects[observed->terminal_count++] = subject;
}

static bool admission_terminal_seen(const admission_observations* observed, trevrpc_engine_handle_v1 subject) {
    for (uint32_t index = 0; index < observed->terminal_count; index++) {
        trevrpc_engine_handle_v1 prior = observed->terminal_subjects[index];
        if (prior.owner == subject.owner && prior.slot == subject.slot && prior.generation == subject.generation) {
            return true;
        }
    }
    return false;
}

static void admission_pump(trevrpc_engine* adapter,
    const trevrpc_engine_wake_source_v1* wake,
    admission_observations* observed,
    uint32_t wanted,
    uint64_t* max_connections,
    uint64_t* max_streams) {
    uint64_t deadline = monotonic_millis() + 10000;
    while (monotonic_millis() < deadline) {
        check_admission_diagnostics(adapter, max_connections, max_streams);
        if ((observed->ready_connections >= 2 || (wanted & ADMISSION_READY_CONNECTIONS) == 0) &&
            (observed->failed_connections >= 1 || (wanted & ADMISSION_FAILED_CONNECTION) == 0) &&
            (observed->local_stream_count >= 2 || (wanted & ADMISSION_LOCAL_STREAMS) == 0) &&
            (observed->peer_stream_count >= 1 || (wanted & ADMISSION_PEER_STREAM) == 0)) {
            return;
        }
        struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN};
        assert(poll(&descriptor, 1, 1000) >= 0);
        for (;;) {
            trevrpc_engine_event* event = NULL;
            int result = trevrpc_engine_next_event(adapter, &event);
            if (result == -EAGAIN || result == -EPIPE) {
                break;
            }
            assert(result == 0);
            trevrpc_engine_event_info_v1 info;
            assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
            bool terminal = info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_FAILED ||
                            info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_CLOSED ||
                            info.kind == TREVRPC_ENGINE_EVENT_STREAM_FAILED ||
                            info.kind == TREVRPC_ENGINE_EVENT_STREAM_CLOSED;
            assert(info.status == 0 || (info.flags & TREVRPC_ENGINE_EVENT_FLAG_TERMINAL) != 0 ||
                   info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE);
            if (terminal) {
                admission_record_terminal(observed, info.subject);
                if (info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_FAILED) {
                    observed->failed_connections++;
                }
                if (info.subject.owner == observed->extra_connection.owner &&
                    info.subject.slot == observed->extra_connection.slot &&
                    info.subject.generation == observed->extra_connection.generation &&
                    info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_FAILED) {
                    observed->saw_connection_rejection = 1;
                }
            } else if (info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_READY) {
                observed->ready_connections++;
            } else if (info.kind == TREVRPC_ENGINE_EVENT_STREAM_READY) {
                if ((info.flags & TREVRPC_ENGINE_EVENT_FLAG_PEER) != 0) {
                    assert(observed->peer_stream_count < 2);
                    observed->peer_streams[observed->peer_stream_count++] = info.subject;
                } else if ((info.flags & TREVRPC_ENGINE_EVENT_FLAG_LOCAL) != 0) {
                    assert(observed->local_stream_count < 2);
                    observed->local_operation_ids[observed->local_stream_count] = info.operation_id;
                    observed->local_streams[observed->local_stream_count++] = info.subject;
                }
            }
            trevrpc_engine_event_release(event);
        }
    }
    assert(false);
}

static bool admission_wait_stream_ready(trevrpc_engine* adapter,
    const trevrpc_engine_wake_source_v1* wake,
    admission_observations* observed,
    trevrpc_engine_handle_v1 stream) {
    if (admission_terminal_seen(observed, stream)) {
        return false;
    }
    uint64_t deadline = monotonic_millis() + 10000;
    while (monotonic_millis() < deadline) {
        struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN};
        assert(poll(&descriptor, 1, 1000) >= 0);
        for (;;) {
            trevrpc_engine_event* event = NULL;
            int result = trevrpc_engine_next_event(adapter, &event);
            if (result == -EAGAIN || result == -EPIPE) {
                break;
            }
            assert(result == 0);
            trevrpc_engine_event_info_v1 info;
            assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
            bool terminal = info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_FAILED ||
                            info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_CLOSED ||
                            info.kind == TREVRPC_ENGINE_EVENT_STREAM_FAILED ||
                            info.kind == TREVRPC_ENGINE_EVENT_STREAM_CLOSED;
            assert(info.status == 0 || (info.flags & TREVRPC_ENGINE_EVENT_FLAG_TERMINAL) != 0 ||
                   info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE);
            if (terminal) {
                admission_record_terminal(observed, info.subject);
            }
            bool subject_match = info.subject.owner == stream.owner && info.subject.slot == stream.slot &&
                                 info.subject.generation == stream.generation;
            bool ready = subject_match && info.kind == TREVRPC_ENGINE_EVENT_STREAM_READY;
            bool finished = ready || (subject_match && terminal);
            trevrpc_engine_event_release(event);
            if (finished) {
                return ready;
            }
        }
    }
    assert(false);
    return false;
}

static void test_real_bounded_admission(void) {
    trevrpc_engine_config_v1 engine_config;
    trevrpc_engine_msquic_config_v1 provider_config;
    trevrpc_engine* adapter = NULL;
    trevrpc_engine_handle_v1 listener = {0};
    trevrpc_engine_wake_source_v1 wake;
    trevrpc_engine_endpoint_config_v1 endpoint;
    admission_observations observed = {0};
    uint64_t max_connections = 0;
    uint64_t max_streams = 0;
    uint16_t port = 0;

    assert(trevrpc_engine_config_v1_init(&engine_config, sizeof(engine_config)) == 0);
    engine_config.event_capacity = 2;
    engine_config.listener_capacity = 1;
    engine_config.connection_capacity = 3;
    engine_config.stream_capacity = 3;
    engine_config.max_receive_owned_count = 1;
    assert(trevrpc_engine_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_engine_msquic_create_v1(&engine_config, &provider_config, &adapter) == 0);
    assert(trevrpc_engine_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_engine_get_wake_source_v1(adapter, &wake) == 0);

    assert(trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.alpn = (const uint8_t*)"trevrpc/1";
    endpoint.alpn_len = 9;
    endpoint.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    endpoint.cert_file_len = (uint32_t)strlen(endpoint.cert_file);
    endpoint.key_file = TREVRPC_MSQUIC_TEST_KEY;
    endpoint.key_file_len = (uint32_t)strlen(endpoint.key_file);
    assert(trevrpc_engine_listen_v1(adapter, &endpoint, &listener) == 0);
    assert(trevrpc_engine_listener_get_port_v1(adapter, listener, &port) == 0);
    assert(port != 0);

    assert(trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.alpn = (const uint8_t*)"trevrpc/1";
    endpoint.alpn_len = 9;
    endpoint.port = port;
    endpoint.flags = TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION;
    trevrpc_engine_handle_v1 first_connection = {0};
    assert(trevrpc_engine_dial_v1(adapter, &endpoint, 1, &first_connection) == 0);
    admission_pump(adapter, &wake, &observed, ADMISSION_READY_CONNECTIONS, &max_connections, &max_streams);
    assert(observed.ready_connections == 2);
    assert(max_connections <= engine_config.connection_capacity);

    assert(trevrpc_engine_dial_v1(adapter, &endpoint, 2, &observed.extra_connection) == 0);
    admission_pump(adapter, &wake, &observed, ADMISSION_FAILED_CONNECTION, &max_connections, &max_streams);
    assert(observed.saw_connection_rejection != 0);

    assert(
        trevrpc_engine_connection_open_bidi_stream_v1(adapter, first_connection, 10, &observed.local_streams[0]) == 0);
    assert(
        trevrpc_engine_connection_open_bidi_stream_v1(adapter, first_connection, 11, &observed.local_streams[1]) == 0);
    admission_pump(
        adapter, &wake, &observed, ADMISSION_LOCAL_STREAMS | ADMISSION_PEER_STREAM, &max_connections, &max_streams);
    assert(observed.local_stream_count == 2);
    assert(observed.local_operation_ids[0] == 10);
    assert(observed.local_operation_ids[1] == 11);
    assert(observed.peer_stream_count == 1);
    trevrpc_engine_handle_v1 additional_stream = {0};
    int additional_result =
        trevrpc_engine_connection_open_bidi_stream_v1(adapter, first_connection, 12, &additional_stream);
    assert(additional_result == 0 || additional_result == -ENOSPC);
    if (additional_result == 0 && admission_wait_stream_ready(adapter, &wake, &observed, additional_stream)) {
        assert(trevrpc_engine_stream_abort(adapter, additional_stream, 0) == 0);
    }

    if (!admission_terminal_seen(&observed, observed.local_streams[0])) {
        assert(trevrpc_engine_stream_abort(adapter, observed.local_streams[0], 0) == 0);
    }
    if (!admission_terminal_seen(&observed, observed.local_streams[1])) {
        assert(trevrpc_engine_stream_abort(adapter, observed.local_streams[1], 0) == 0);
    }
    if (!admission_terminal_seen(&observed, observed.peer_streams[0])) {
        assert(trevrpc_engine_stream_abort(adapter, observed.peer_streams[0], 0) == 0);
    }
    trevrpc_engine_handle_v1 pending_connection = {0};
    assert(trevrpc_engine_dial_v1(adapter, &endpoint, 4, &pending_connection) == 0);
    assert(trevrpc_engine_listener_close(adapter, listener) == 0);
    assert(trevrpc_engine_connection_close(adapter, first_connection, 0) == 0);
    int extra_close_result = trevrpc_engine_connection_close(adapter, observed.extra_connection, 0);
    assert(extra_close_result == 0 || extra_close_result == -EPIPE || extra_close_result == -ESTALE);
    assert(trevrpc_engine_close(adapter) == 0);

    uint64_t deadline = monotonic_millis() + 10000;
    for (;;) {
        trevrpc_engine_diagnostics_v1 diagnostics;
        assert(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
        assert(trevrpc_engine_get_diagnostics_v1(adapter, &diagnostics) == 0);
        assert(diagnostics.live_connections <= engine_config.connection_capacity);
        assert(diagnostics.live_streams <= engine_config.stream_capacity);
        assert(diagnostics.terminal_status == 0);
        assert(diagnostics.provider_error_code == 0);
        if (diagnostics.live_listeners == 0 && diagnostics.live_connections == 0 && diagnostics.live_streams == 0) {
            break;
        }
        struct pollfd descriptor = {.fd = (int)wake.native_handle, .events = POLLIN};
        assert(poll(&descriptor, 1, 1000) >= 0);
        for (;;) {
            trevrpc_engine_event* event = NULL;
            int result = trevrpc_engine_next_event(adapter, &event);
            if (result == -EAGAIN || result == -EPIPE) {
                break;
            }
            assert(result == 0);
            trevrpc_engine_event_info_v1 info;
            assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
            bool terminal = info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_FAILED ||
                            info.kind == TREVRPC_ENGINE_EVENT_CONNECTION_CLOSED ||
                            info.kind == TREVRPC_ENGINE_EVENT_STREAM_FAILED ||
                            info.kind == TREVRPC_ENGINE_EVENT_STREAM_CLOSED;
            assert(info.status == 0 || (info.flags & TREVRPC_ENGINE_EVENT_FLAG_TERMINAL) != 0 ||
                   info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE);
            if (terminal) {
                admission_record_terminal(&observed, info.subject);
            }
            trevrpc_engine_event_release(event);
        }
        assert(monotonic_millis() < deadline);
    }
    assert(trevrpc_engine_drain(adapter) == 0);
    assert(observed.terminal_count <= 8);
    assert(trevrpc_engine_release(adapter) == 0);
}

int main(void) {
    test_real_bounded_admission();

    trevrpc_engine_config_v1 engine_config;
    trevrpc_engine_msquic_config_v1 provider_config;
    trevrpc_engine* adapter = NULL;
    trevrpc_engine_handle_v1 listener = {0};
    trevrpc_engine_wake_source_v1 wake;
    trevrpc_engine_endpoint_config_v1 endpoint;
    observations observed = {0};
    uint16_t port = 0;

    assert(trevrpc_engine_config_v1_init(&engine_config, sizeof(engine_config)) == 0);
    engine_config.event_capacity = 1;
    engine_config.listener_capacity = 1;
    engine_config.connection_capacity = 2;
    engine_config.stream_capacity = 2;
    engine_config.max_receive_owned_count = 1;
    assert(trevrpc_engine_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_engine_msquic_create_v1(&engine_config, &provider_config, &adapter) == 0);
    assert(trevrpc_engine_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_engine_get_wake_source_v1(adapter, &wake) == 0);

    assert(trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.alpn = (const uint8_t*)"trevrpc/1";
    endpoint.alpn_len = 9;
    endpoint.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    endpoint.cert_file_len = (uint32_t)strlen(endpoint.cert_file);
    endpoint.key_file = TREVRPC_MSQUIC_TEST_KEY;
    endpoint.key_file_len = (uint32_t)strlen(endpoint.key_file);
    assert(trevrpc_engine_listen_v1(adapter, &endpoint, &listener) == 0);
    assert(trevrpc_engine_listener_get_port_v1(adapter, listener, &port) == 0);
    assert(port != 0);

    assert(trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.alpn = (const uint8_t*)"trevrpc/1";
    endpoint.alpn_len = 9;
    endpoint.port = port;
    endpoint.flags = TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION;
    assert(trevrpc_engine_dial_v1(adapter, &endpoint, 1, &observed.client_connection) == 0);
    pump_until(adapter, &wake, &observed, SEEN_CLIENT_CONNECTION | SEEN_SERVER_CONNECTION);

    assert(trevrpc_engine_connection_open_bidi_stream_v1(
               adapter, observed.client_connection, 2, &observed.client_stream) == 0);
    pump_until(adapter, &wake, &observed, SEEN_CLIENT_STREAM);

    static const uint8_t request[] = "request";
    static const uint8_t second_request = 0;
    assert(trevrpc_engine_stream_send_frame_v1(adapter, observed.client_stream, 3, request, sizeof(request) - 1) == 0);

    pump_until(adapter, &wake, &observed, SEEN_CLIENT_SEND);
    assert(trevrpc_engine_stream_send_frame_v1(
               adapter, observed.client_stream, 3, &second_request, sizeof(second_request) - 1) == 0);

    pump_until(adapter,
        &wake,
        &observed,
        SEEN_SERVER_STREAM | SEEN_CLIENT_SEND | SEEN_SERVER_READABLE | SEEN_CLIENT_SEND_SECOND);

    observed.seen &= ~SEEN_SERVER_READABLE;
    trevrpc_engine_receive* request_receive =
        check_receive(adapter, observed.server_stream, request, sizeof(request) - 1);
    pump_until(adapter, &wake, &observed, SEEN_SERVER_READABLE);
    trevrpc_engine_receive* second_request_receive =
        check_receive(adapter, observed.server_stream, &second_request, sizeof(second_request) - 1);
    trevrpc_engine_receive_release(second_request_receive);
    trevrpc_engine_receive_release(request_receive);

    static const uint8_t response[] = "response";
    assert(
        trevrpc_engine_stream_send_frame_v1(adapter, observed.server_stream, 4, response, sizeof(response) - 1) == 0);
    pump_until(adapter, &wake, &observed, SEEN_SERVER_SEND | SEEN_CLIENT_READABLE);
    trevrpc_engine_receive* detached_receive = NULL;

    assert(trevrpc_engine_stream_finish_send(adapter, observed.client_stream) == 0);
    assert(trevrpc_engine_stream_finish_send(adapter, observed.server_stream) == 0);

    uint64_t reclaim_deadline = monotonic_millis() + 10000;
    for (;;) {
        trevrpc_engine_diagnostics_v1 diagnostics;
        assert(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
        assert(trevrpc_engine_get_diagnostics_v1(adapter, &diagnostics) == 0);
        if (diagnostics.live_streams == 0) {
            break;
        }
        assert(monotonic_millis() < reclaim_deadline);
    }

    assert(trevrpc_engine_connection_open_bidi_stream_v1(
               adapter, observed.client_connection, 5, &observed.replacement_stream) == -ENOSPC);
    for (;;) {
        trevrpc_engine_event* event = NULL;
        int next = trevrpc_engine_next_event(adapter, &event);
        if (next == -EAGAIN) {
            break;
        }
        assert(next == 0);
        trevrpc_engine_event_release(event);
    }
    assert(trevrpc_engine_connection_open_bidi_stream_v1(
               adapter, observed.client_connection, 5, &observed.replacement_stream) == 0);
    assert(observed.replacement_stream.slot == observed.client_stream.slot ||
           observed.replacement_stream.slot == observed.server_stream.slot);
    if (observed.replacement_stream.slot == observed.client_stream.slot) {
        assert(observed.replacement_stream.generation != observed.client_stream.generation);
    } else {
        assert(observed.replacement_stream.generation != observed.server_stream.generation);
    }
    pump_until(adapter, &wake, &observed, SEEN_REPLACEMENT_STREAM | SEEN_REPLACEMENT_PEER_STREAM);
    detached_receive = check_receive(adapter, observed.client_stream, response, sizeof(response) - 1);
    trevrpc_engine_receive* exhausted_receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(adapter, observed.client_stream, &exhausted_receive) == -ESTALE);
    assert(exhausted_receive == NULL);

    static const uint8_t burst_payload[] = "burst";
    assert(trevrpc_engine_stream_send_frame_v1(
               adapter, observed.replacement_stream, 6, burst_payload, sizeof(burst_payload) - 1) == 0);
    pump_until(adapter, &wake, &observed, SEEN_REPLACEMENT_READABLE);
    for (uint64_t operation_id = 7; operation_id < 23; operation_id++) {
        assert(trevrpc_engine_stream_send_frame_v1(
                   adapter, observed.replacement_stream, operation_id, burst_payload, sizeof(burst_payload) - 1) == 0);
    }
    wait_for_pending_sends(adapter);
    trevrpc_engine_diagnostics_v1 backpressure_diagnostics;
    assert(trevrpc_engine_diagnostics_v1_init(&backpressure_diagnostics, sizeof(backpressure_diagnostics)) == 0);
    assert(trevrpc_engine_get_diagnostics_v1(adapter, &backpressure_diagnostics) == 0);
    assert(backpressure_diagnostics.terminal_status == 0);
    assert(backpressure_diagnostics.receive_owned_count == 1);
    assert(backpressure_diagnostics.peak_receive_owned_count == 1);

    assert(trevrpc_engine_connection_close(adapter, observed.client_connection, 0) == 0);
    assert(trevrpc_engine_dial_cancel(adapter, observed.client_connection) == -EALREADY);
    assert(trevrpc_engine_connection_close(adapter, observed.server_connection, 0) == 0);
    assert(trevrpc_engine_listener_close(adapter, listener) == 0);
    assert(trevrpc_engine_close(adapter) == 0);
    assert(trevrpc_engine_drain(adapter) == 0);
    assert(trevrpc_engine_release(adapter) == 0);

    trevrpc_engine_receive_info_v1 detached_info;
    assert(trevrpc_engine_receive_info_v1_init(&detached_info, sizeof(detached_info)) == 0);
    assert(trevrpc_engine_receive_get_info_v1(detached_receive, &detached_info) == 0);
    assert(detached_info.data_len == sizeof(response) - 1);
    assert(memcmp(detached_info.data, response, sizeof(response) - 1) == 0);
    trevrpc_engine_receive_release(detached_receive);
    trevrpc_engine_receive_release(NULL);
    return 0;
}
