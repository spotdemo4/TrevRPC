#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L

#include "../src/trevrpc_credential_internal.h"
#include "trevrpc_transport_msquic.h"

#include <dirent.h>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h> // NOLINT(misc-include-cleaner)
#include <poll.h>
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
#ifndef TREVRPC_TRANSPORT_TEST_PROTOCOL
#define TREVRPC_TRANSPORT_TEST_PROTOCOL TREVRPC_TRANSPORT_PROTOCOL_NATIVE
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
#define EVENT_TRACE_CAPACITY 64u

static int credential_cleanup_failures;
static int credential_cleanup_obstructions;
static char credential_cleanup_obstruction[PATH_MAX];

int trevrpc_credential_test_fail_cleanup(void) {
    if (credential_cleanup_failures == 0)
        return 0;
    --credential_cleanup_failures;
    return 1;
}

void trevrpc_credential_test_before_cleanup(trevrpc_credential_files* files) {
    if (credential_cleanup_obstructions == 0)
        return;
    --credential_cleanup_obstructions;
    assert(files != NULL && files->key_created);
    assert(unlink(files->key_file) == 0);
    assert(mkdir(files->key_file, S_IRWXU) == 0);
    assert(strlen(files->key_file) < sizeof(credential_cleanup_obstruction));
    memcpy(credential_cleanup_obstruction, files->key_file, strlen(files->key_file) + 1u);
}

typedef struct observations {
    trevrpc_transport_handle_v1 listener;
    trevrpc_transport_handle_v1 client_connection;
    trevrpc_transport_handle_v1 server_connection;
    trevrpc_transport_handle_v1 client_stream;
    trevrpc_transport_handle_v1 server_stream;
    trevrpc_transport_event_info_v1 trace[EVENT_TRACE_CAPACITY];
    uint64_t last_sequence;
    size_t trace_count;
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
    observed->trace[observed->trace_count % EVENT_TRACE_CAPACITY] = *info;
    ++observed->trace_count;
    assert(info->sequence > observed->last_sequence);
    observed->last_sequence = info->sequence;
    if (info->kind == TREVRPC_TRANSPORT_EVENT_CONNECTION_READY) {
        assert(info->subject_kind == TREVRPC_TRANSPORT_OBJECT_CONNECTION);
        if (same_handle(info->subject, observed->client_connection)) {
            assert(info->operation_id == 2);
            observed->seen |= SEEN_CLIENT_CONNECTION;
        } else {
            assert(info->operation_id == 0);
        }
    } else if (info->kind == TREVRPC_TRANSPORT_EVENT_STREAM_READY) {
        if (same_handle(info->subject, observed->client_stream)) {
            assert(info->operation_id == 2);
            observed->seen |= SEEN_CLIENT_STREAM;
        } else {
            observed->server_connection = info->parent;
            observed->server_stream = info->subject;
            observed->seen |= SEEN_SERVER_CONNECTION | SEEN_SERVER_STREAM;
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

static void dump_event_trace(const observations* observed) {
    size_t first = observed->trace_count > EVENT_TRACE_CAPACITY ? observed->trace_count - EVENT_TRACE_CAPACITY : 0;
    size_t index;
    for (index = first; index < observed->trace_count; ++index) {
        const trevrpc_transport_event_info_v1* info = &observed->trace[index % EVENT_TRACE_CAPACITY];
        fprintf(stderr,
            "  event sequence=%llu kind=%u operation=%llu status=%d flags=0x%08x subject_kind=%u "
            "application=%llu provider=%llu subject=%llu/%u/%u parent=%llu/%u/%u\n",
            (unsigned long long)info->sequence,
            info->kind,
            (unsigned long long)info->operation_id,
            info->status,
            info->flags,
            info->subject_kind,
            (unsigned long long)info->application_error_code,
            (unsigned long long)info->provider_error_code,
            (unsigned long long)info->subject.owner,
            info->subject.slot,
            info->subject.generation,
            (unsigned long long)info->parent.owner,
            info->parent.slot,
            info->parent.generation);
    }
}

static void dump_transport_diagnostics(trevrpc_transport* transport) {
    trevrpc_transport_diagnostics_v1 diagnostics;
    if (trevrpc_transport_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) != 0 ||
        trevrpc_transport_get_diagnostics_v1(transport, &diagnostics) != 0)
        return;
    fprintf(stderr,
        "  transport state=%u terminal=%d capacity=%u queue=%u ordinary=%u enqueued=%llu dequeued=%llu "
        "rejected=%llu receives=%llu/%llu receive_bytes=%llu/%llu sends=%llu/%llu listeners=%llu "
        "connections=%llu streams=%llu callbacks=%llu api_calls=%llu wake=%llu eagain=%llu failures=%llu "
        "provider=%llu reservations=%llu\n",
        diagnostics.state,
        diagnostics.terminal_status,
        diagnostics.event_capacity,
        diagnostics.queue_depth,
        diagnostics.ordinary_queue_depth,
        (unsigned long long)diagnostics.events_enqueued,
        (unsigned long long)diagnostics.events_dequeued,
        (unsigned long long)diagnostics.events_rejected,
        (unsigned long long)diagnostics.receive_owned_count,
        (unsigned long long)diagnostics.peak_receive_owned_count,
        (unsigned long long)diagnostics.receive_owned_bytes,
        (unsigned long long)diagnostics.peak_receive_owned_bytes,
        (unsigned long long)diagnostics.pending_send_count,
        (unsigned long long)diagnostics.pending_send_bytes,
        (unsigned long long)diagnostics.live_listeners,
        (unsigned long long)diagnostics.live_connections,
        (unsigned long long)diagnostics.live_streams,
        (unsigned long long)diagnostics.active_callbacks,
        (unsigned long long)diagnostics.active_api_calls,
        (unsigned long long)diagnostics.wake_signals,
        (unsigned long long)diagnostics.wake_write_eagain,
        (unsigned long long)diagnostics.wake_failures,
        (unsigned long long)diagnostics.provider_error_code,
        (unsigned long long)diagnostics.mandatory_reservations);
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
            {
                int protocol_result = trevrpc_transport_event_get_protocol_info_v1(transport, event, &protocol);
                if (info.subject_kind == TREVRPC_TRANSPORT_OBJECT_NONE) {
                    assert(protocol_result == -ENOTSUP);
                } else {
                    /* Every object event must retain its negotiated protocol. */
                    assert(protocol_result == 0);
                    assert(protocol.protocol == TREVRPC_TRANSPORT_TEST_PROTOCOL);
                }
            }
            observe_event(observed, &info);
            trevrpc_transport_event_release(transport, event);
        }
    }
    if ((observed->seen & wanted) != wanted) {
        fprintf(stderr, "pump timeout (seen=0x%08x wanted=0x%08x)\n", observed->seen, wanted);
        dump_event_trace(observed);
        dump_transport_diagnostics(transport);
    }
    assert((observed->seen & wanted) == wanted);
}

static uint8_t* read_file(const char* path, size_t* out_len) {
    FILE* file = fopen(path, "rb");
    long length;
    uint8_t* data;
    if (file == NULL)
        return NULL;
    assert(fseek(file, 0, SEEK_END) == 0);
    length = ftell(file);
    assert(length > 0);
    assert(fseek(file, 0, SEEK_SET) == 0);
    data = malloc((size_t)length);
    assert(data != NULL);
    assert(fread(data, 1, (size_t)length, file) == (size_t)length);
    assert(fclose(file) == 0);
    *out_len = (size_t)length;
    return data;
}

static size_t credential_bundle_count(void) {
    DIR* directory = opendir("/tmp");
    struct dirent* entry;
    size_t count = 0;
    if (directory == NULL)
        return 0;
    while ((entry = readdir(directory)) != NULL) {
        if (strncmp(entry->d_name, "trevrpc-credentials-", sizeof("trevrpc-credentials-") - 1u) == 0)
            ++count;
    }
    assert(closedir(directory) == 0);
    return count;
}

static void test_credential_cleanup_failure(void) {
    trevrpc_credential_cleanup_owner owner;
    trevrpc_credential_cleanup_lease* lease = NULL;
    trevrpc_credential_files files = {0};
    char directory[] = "/tmp/trevrpc-cleanup-test-XXXXXX";
    char obstruction[PATH_MAX];
    int result;
    assert(mkdtemp(directory) != NULL);
    assert(snprintf(obstruction, sizeof(obstruction), "%s/private-key.pem", directory) > 0);
    assert(mkdir(obstruction, S_IRWXU) == 0);
    memcpy(files.directory, directory, strlen(directory) + 1u);
    memcpy(files.key_file, obstruction, strlen(obstruction) + 1u);
    files.directory_created = 1;
    files.key_created = 1;
    assert(trevrpc_credential_cleanup_owner_init(&owner) == 0);
    assert(trevrpc_credential_cleanup_owner_prepare_lease(&owner, 1, &lease) == 0);
    trevrpc_credential_cleanup_owner_finish(&owner, &files, &lease);
    assert(owner.head != NULL);
    result = trevrpc_credential_cleanup_owner_prepare_release(&owner);
    assert(result < 0);
    assert(owner.head != NULL);
    assert(rmdir(obstruction) == 0);
    trevrpc_credential_cleanup_owner_progress(&owner);
    assert(owner.head == NULL);
    assert(access(directory, F_OK) != 0 && errno == ENOENT);
    trevrpc_credential_cleanup_owner_destroy(&owner);
}

static void test_credential_cleanup_reservations(void) {
    trevrpc_credential_cleanup_owner owner;
    trevrpc_credential_cleanup_lease* first = NULL;
    trevrpc_credential_cleanup_lease* second = NULL;
    trevrpc_credential_files files = {0};
    assert(trevrpc_credential_cleanup_owner_init(&owner) == 0);
    assert(trevrpc_credential_cleanup_owner_prepare_lease(&owner, 1, &first) == 0);
    assert(trevrpc_credential_cleanup_owner_prepare_lease(&owner, 1, &second) == 0);
    assert(trevrpc_credential_cleanup_owner_prepare_release(&owner) == -EBUSY);
    trevrpc_credential_cleanup_owner_finish(&owner, &files, &first);
    assert(trevrpc_credential_cleanup_owner_prepare_release(&owner) == -EBUSY);
    trevrpc_credential_cleanup_owner_finish(&owner, &files, &second);
    assert(owner.in_flight == 0);
    assert(trevrpc_credential_cleanup_owner_prepare_release(&owner) == 0);
    trevrpc_credential_cleanup_owner_destroy(&owner);
}

int main(void) {
    test_credential_cleanup_failure();
    test_credential_cleanup_reservations();
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_msquic_config_v1 provider_config;
    trevrpc_transport_endpoint_config_v1 endpoint;
    trevrpc_transport_wake_source_v1 wakes[TREVRPC_TRANSPORT_MAX_WAKE_SOURCES];
    trevrpc_transport* transport = NULL;
    observations observed = {0};
    size_t wake_count = 0;
    uint16_t port = 0;
    uint8_t* server_cert;
    uint8_t* server_key;
    uint8_t* client_cert;
    uint8_t* client_key;
    uint8_t* client_ca;
    size_t server_cert_len;
    size_t server_key_len;
    size_t client_cert_len;
    size_t client_key_len;
    size_t client_ca_len;
    size_t client_bundles_before;
    size_t index;

    server_cert = read_file(TREVRPC_MSQUIC_TEST_CERT, &server_cert_len);
    server_key = read_file(TREVRPC_MSQUIC_TEST_KEY, &server_key_len);
    client_cert = read_file(TREVRPC_MSQUIC_TEST_CERT, &client_cert_len);
    client_key = read_file(TREVRPC_MSQUIC_TEST_KEY, &client_key_len);
    client_ca = read_file(TREVRPC_MSQUIC_TEST_CERT, &client_ca_len);
    assert(server_cert != NULL && server_key != NULL && client_cert != NULL && client_key != NULL && client_ca != NULL);

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
    endpoint.protocol = TREVRPC_TRANSPORT_TEST_PROTOCOL;
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.cert_data_len = 1;
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == -EINVAL);
    endpoint.cert_data = server_cert;
    endpoint.cert_data_len = server_cert_len;
    endpoint.key_data_len = 1;
    endpoint.key_data = NULL;
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == -EINVAL);
    endpoint.key_data = server_key;
    endpoint.key_data_len = server_key_len;
    endpoint.cert_data = NULL;
    endpoint.cert_data_len = 0;
    endpoint.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    endpoint.cert_file_len = 0;
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == -EINVAL);
    endpoint.cert_data = server_cert;
    endpoint.cert_data_len = server_cert_len;
    endpoint.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    endpoint.cert_file_len = (uint32_t)strlen(endpoint.cert_file);
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == -EINVAL);
    endpoint.cert_file = NULL;
    endpoint.cert_file_len = 0;
    endpoint.ca_cert_data = server_cert;
    endpoint.ca_cert_data_len = server_cert_len;
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == -ENOTSUP);

#if TREVRPC_TRANSPORT_TEST_PROTOCOL == TREVRPC_TRANSPORT_PROTOCOL_NATIVE
    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.protocol = TREVRPC_TRANSPORT_TEST_PROTOCOL;
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.server_name = "127.0.0.1";
    endpoint.server_name_len = 9;
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == -ENOTSUP);
#endif

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.protocol = TREVRPC_TRANSPORT_TEST_PROTOCOL;
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.cert_data = server_cert;
    endpoint.cert_data_len = server_cert_len;
    endpoint.key_data = server_key;
    endpoint.key_data_len = server_key_len;
    {
        size_t bundles_before = credential_bundle_count();
        credential_cleanup_failures = 1;
        assert(trevrpc_transport_listen_v1(transport, &endpoint, &observed.listener) == 0);
        assert(credential_bundle_count() == bundles_before);
    }
    memset(server_cert, 0, server_cert_len);
    memset(server_key, 0, server_key_len);
    free(server_cert);
    free(server_key);
    server_cert = NULL;
    server_key = NULL;
    assert(trevrpc_transport_listener_get_port_v1(transport, observed.listener, &port) == 0);
    assert(port != 0);

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    endpoint.protocol = TREVRPC_TRANSPORT_TEST_PROTOCOL;
    endpoint.host = "127.0.0.1";
    endpoint.host_len = 9;
    endpoint.server_name = "127.0.0.1";
    endpoint.server_name_len = 9;
    endpoint.port = port;
    endpoint.cert_data = client_cert;
    endpoint.cert_data_len = client_cert_len;
    endpoint.key_data = client_key;
    endpoint.key_data_len = client_key_len;
    endpoint.ca_cert_data = client_ca;
    endpoint.ca_cert_data_len = client_ca_len;
#if TREVRPC_TRANSPORT_TEST_PROTOCOL == TREVRPC_TRANSPORT_PROTOCOL_NATIVE
    endpoint.server_name = "localhost";
    endpoint.server_name_len = 9;
    assert(trevrpc_transport_dial_v1(transport, &endpoint, 1, &observed.client_connection) == -ENOTSUP);
    endpoint.server_name = "127.0.0.1";
    endpoint.server_name_len = 9;
#endif
    client_bundles_before = credential_bundle_count();
    credential_cleanup_obstructions = 1;
    assert(trevrpc_transport_dial_v1(transport, &endpoint, 2, &observed.client_connection) == 0);
    assert(credential_cleanup_obstruction[0] != '\0');
    assert(credential_bundle_count() == client_bundles_before + 1u);
    memset(client_cert, 0, client_cert_len);
    memset(client_key, 0, client_key_len);
    memset(client_ca, 0, client_ca_len);
    free(client_cert);
    free(client_key);
    free(client_ca);
    client_cert = NULL;
    client_key = NULL;
    client_ca = NULL;
    pump_until(transport, wakes, wake_count, &observed, SEEN_CLIENT_CONNECTION);

    assert(trevrpc_transport_connection_open_bidi_stream_v1(
               transport, observed.client_connection, 2, &observed.client_stream) == 0);
    pump_until(transport, wakes, wake_count, &observed, SEEN_CLIENT_STREAM);

    static const uint8_t request[] = "request";
    assert(trevrpc_transport_stream_send_v1(transport, observed.client_stream, 3, request, sizeof(request) - 1) == 0);
    pump_until(transport,
        wakes,
        wake_count,
        &observed,
        SEEN_SERVER_CONNECTION | SEEN_SERVER_STREAM | SEEN_SEND_COMPLETE | SEEN_SERVER_READABLE);

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
    assert(trevrpc_transport_release(transport) < 0);
    assert(trevrpc_transport_drain(transport) == 0);
    assert(credential_bundle_count() == client_bundles_before + 1u);
    assert(rmdir(credential_cleanup_obstruction) == 0);
    credential_cleanup_obstruction[0] = '\0';
    assert(trevrpc_transport_release(transport) == 0);
    assert(credential_bundle_count() == client_bundles_before);
    return 0;
}
