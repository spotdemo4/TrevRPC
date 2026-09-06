#define _POSIX_C_SOURCE 200809L

#include "trevrpc_transport_msquic.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h> // NOLINT(misc-include-cleaner)
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
#define SEEN_SEND_COMPLETE 0x0010u
#define SEEN_SERVER_READABLE 0x0020u
#define SEEN_SERVER_FIN 0x0040u
#define SEEN_CLIENT_STREAM_TERMINAL 0x0080u
#define SEEN_SERVER_STREAM_TERMINAL 0x0100u
#define SEEN_CLIENT_CONNECTION_TERMINAL 0x0200u
#define SEEN_SERVER_CONNECTION_TERMINAL 0x0400u
#define SEEN_LISTENER_TERMINAL 0x0800u
#define SEEN_STOPPED 0x1000u

typedef struct observations {
    trevrpc_transport_handle_v1 listener;
    trevrpc_transport_handle_v1 client_connection;
    trevrpc_transport_handle_v1 server_connection;
    trevrpc_transport_handle_v1 client_stream;
    trevrpc_transport_handle_v1 server_stream;
    uint64_t last_sequence;
    uint32_t seen;
} observations;

static uint64_t monotonic_millis(void) {
    struct timespec now;
    assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
    return (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u;
}

static bool same_handle(trevrpc_transport_handle_v1 left, trevrpc_transport_handle_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static bool object_terminal(uint32_t kind) {
    return kind == TREVRPC_TRANSPORT_EVENT_LISTENER_STOPPED || kind == TREVRPC_TRANSPORT_EVENT_CONNECTION_FAILED ||
           kind == TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSED || kind == TREVRPC_TRANSPORT_EVENT_STREAM_FAILED ||
           kind == TREVRPC_TRANSPORT_EVENT_STREAM_CLOSED;
}

static void observe_event(observations* observed, const trevrpc_transport_event_info_v1* info) {
    assert(info->sequence > observed->last_sequence);
    observed->last_sequence = info->sequence;
    if (info->kind == TREVRPC_TRANSPORT_EVENT_CONNECTION_READY) {
        if (same_handle(info->subject, observed->client_connection)) {
            assert(info->operation_id == 1);
            observed->seen |= SEEN_CLIENT_CONNECTION;
        } else {
            observed->server_connection = info->subject;
            observed->seen |= SEEN_SERVER_CONNECTION;
        }
    } else if (info->kind == TREVRPC_TRANSPORT_EVENT_STREAM_READY) {
        if (same_handle(info->subject, observed->client_stream)) {
            assert(info->operation_id == 2);
            observed->seen |= SEEN_CLIENT_STREAM;
        } else {
            observed->server_stream = info->subject;
            observed->seen |= SEEN_SERVER_STREAM;
        }
    } else if (info->kind == TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE && info->operation_id == 3) {
        assert(info->status == 0);
        observed->seen |= SEEN_SEND_COMPLETE;
    } else if (info->kind == TREVRPC_TRANSPORT_EVENT_STREAM_READABLE &&
               same_handle(info->subject, observed->server_stream)) {
        observed->seen |= SEEN_SERVER_READABLE;
    } else if (info->kind == TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN &&
               same_handle(info->subject, observed->server_stream)) {
        observed->seen |= SEEN_SERVER_FIN;
    } else if (object_terminal(info->kind)) {
        if (same_handle(info->subject, observed->client_stream)) {
            observed->seen |= SEEN_CLIENT_STREAM_TERMINAL;
        } else if (same_handle(info->subject, observed->server_stream)) {
            observed->seen |= SEEN_SERVER_STREAM_TERMINAL;
        } else if (same_handle(info->subject, observed->client_connection)) {
            observed->seen |= SEEN_CLIENT_CONNECTION_TERMINAL;
        } else if (same_handle(info->subject, observed->server_connection)) {
            observed->seen |= SEEN_SERVER_CONNECTION_TERMINAL;
        } else if (same_handle(info->subject, observed->listener)) {
            observed->seen |= SEEN_LISTENER_TERMINAL;
        }
    } else if (info->kind == TREVRPC_TRANSPORT_EVENT_STOPPED) {
        observed->seen |= SEEN_STOPPED;
    }
}

static void pump_until(trevrpc_transport* transport,
    const trevrpc_transport_wake_source_v1* wakes,
    size_t wake_count,
    observations* observed,
    uint32_t wanted) {
    uint64_t deadline = monotonic_millis() + 10000;
    while ((observed->seen & wanted) != wanted && monotonic_millis() < deadline) {
        struct pollfd descriptors[TREVRPC_TRANSPORT_MAX_WAKE_SOURCES];
        int timeout = trevrpc_transport_poll_timeout_ms(transport);
        size_t index;
        if (timeout < 0 || timeout > 1000) {
            timeout = 1000;
        }
        for (index = 0; index < wake_count; ++index) {
            descriptors[index].fd = (int)wakes[index].native_handle;
            descriptors[index].events = POLLIN;
            descriptors[index].revents = 0;
        }
        assert(poll(descriptors, wake_count, timeout) >= 0);
        for (;;) {
            trevrpc_transport_event_v1* event = NULL;
            trevrpc_transport_event_info_v1 info;
            trevrpc_transport_event_protocol_info_v1 protocol;
            int result = trevrpc_transport_next_event(transport, &event);
            if (result == -EAGAIN || result == -EPIPE) {
                break;
            }
            assert(result == 0);
            assert(trevrpc_transport_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_transport_event_get_info_v1(transport, event, &info) == 0);
            assert(trevrpc_transport_event_protocol_info_v1_init(&protocol, sizeof(protocol)) == 0);
            if (info.subject_kind == TREVRPC_TRANSPORT_OBJECT_NONE) {
                assert(trevrpc_transport_event_get_protocol_info_v1(transport, event, &protocol) == -ENOTSUP);
            } else {
                assert(trevrpc_transport_event_get_protocol_info_v1(transport, event, &protocol) == 0);
                assert(protocol.protocol == TREVRPC_TRANSPORT_PROTOCOL_NATIVE);
            }
            observe_event(observed, &info);
            trevrpc_transport_event_release(transport, event);
        }
    }
    if ((observed->seen & wanted) != wanted) {
        fprintf(stderr, "pump timeout (seen=0x%08x wanted=0x%08x)\n", observed->seen, wanted);
    }
    assert((observed->seen & wanted) == wanted);
}

int main(void) {
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_msquic_config_v1 provider_config;
    trevrpc_transport_endpoint_config_v1 endpoint;
    trevrpc_transport_wake_source_v1 wakes[TREVRPC_TRANSPORT_MAX_WAKE_SOURCES];
    trevrpc_transport* transport = NULL;
    observations observed = {0};
    size_t wake_count = 0;
    uint16_t port = 0;
    size_t index;

    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    assert(trevrpc_transport_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == 0);
    for (index = 0; index < TREVRPC_TRANSPORT_MAX_WAKE_SOURCES; ++index) {
        assert(trevrpc_transport_wake_source_v1_init(&wakes[index], sizeof(wakes[index])) == 0);
    }
    assert(
        trevrpc_transport_get_wake_sources_v1(transport, wakes, TREVRPC_TRANSPORT_MAX_WAKE_SOURCES, &wake_count) == 0);
    assert(wake_count > 0);

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.protocol = TREVRPC_TRANSPORT_PROTOCOL_NATIVE;
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    endpoint.cert_file_len = (uint32_t)strlen(endpoint.cert_file);
    endpoint.key_file = TREVRPC_MSQUIC_TEST_KEY;
    endpoint.key_file_len = (uint32_t)strlen(endpoint.key_file);
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == 0);
    assert(trevrpc_transport_listener_get_port_v1(transport, observed.listener, &port) == 0);
    assert(port != 0);

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.protocol = TREVRPC_TRANSPORT_PROTOCOL_NATIVE;
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.port = port;
    endpoint.flags = TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION;
    assert(trevrpc_transport_dial_v1(transport, &endpoint, 1, &observed.client_connection) == 0);
    pump_until(transport, wakes, wake_count, &observed, SEEN_CLIENT_CONNECTION | SEEN_SERVER_CONNECTION);

    assert(trevrpc_transport_connection_open_bidi_stream_v1(
               transport, observed.client_connection, 2, &observed.client_stream) == 0);
    pump_until(transport, wakes, wake_count, &observed, SEEN_CLIENT_STREAM);

    static const uint8_t request[] = "request";
    assert(trevrpc_transport_stream_send_v1(transport, observed.client_stream, 3, request, sizeof(request) - 1) == 0);
    pump_until(transport, wakes, wake_count, &observed, SEEN_SERVER_STREAM | SEEN_SEND_COMPLETE | SEEN_SERVER_READABLE);

    trevrpc_transport_receive_v1* receive = NULL;
    trevrpc_transport_receive_info_v1 receive_info;
    assert(trevrpc_transport_stream_receive(transport, observed.server_stream, &receive) == 0);
    assert(trevrpc_transport_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_transport_receive_get_info_v1(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == sizeof(request) - 1);
    assert(memcmp(receive_info.data, request, sizeof(request) - 1) == 0);
    trevrpc_transport_receive_release(transport, receive);

    assert(trevrpc_transport_stream_finish_send(transport, observed.client_stream) == 0);
    pump_until(transport, wakes, wake_count, &observed, SEEN_SERVER_FIN);

    assert(trevrpc_transport_stream_close(transport, observed.client_stream) == 0);
    assert(trevrpc_transport_stream_close(transport, observed.server_stream) == 0);
    assert(trevrpc_transport_connection_close(transport, observed.client_connection, 0) == 0);
    assert(trevrpc_transport_connection_close(transport, observed.server_connection, 0) == 0);
    assert(trevrpc_transport_listener_close(transport, observed.listener) == 0);
    assert(trevrpc_transport_close(transport) == 0);
    pump_until(transport,
        wakes,
        wake_count,
        &observed,
        SEEN_CLIENT_STREAM_TERMINAL | SEEN_SERVER_STREAM_TERMINAL | SEEN_CLIENT_CONNECTION_TERMINAL |
            SEEN_SERVER_CONNECTION_TERMINAL | SEEN_LISTENER_TERMINAL | SEEN_STOPPED);

    assert(trevrpc_transport_release_handle(transport, observed.client_stream, TREVRPC_TRANSPORT_OBJECT_STREAM) == 0);
    assert(trevrpc_transport_release_handle(transport, observed.server_stream, TREVRPC_TRANSPORT_OBJECT_STREAM) == 0);
    assert(trevrpc_transport_release_handle(
               transport, observed.client_connection, TREVRPC_TRANSPORT_OBJECT_CONNECTION) == 0);
    assert(trevrpc_transport_release_handle(
               transport, observed.server_connection, TREVRPC_TRANSPORT_OBJECT_CONNECTION) == 0);
    assert(trevrpc_transport_release_handle(transport, observed.listener, TREVRPC_TRANSPORT_OBJECT_LISTENER) == 0);
    assert(trevrpc_transport_drain(transport) == 0);
    assert(trevrpc_transport_release(transport) == 0);
    return 0;
}
