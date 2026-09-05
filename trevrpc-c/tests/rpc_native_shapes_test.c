#include "trevrpc_rpc_msquic.h"

#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#error "TREVRPC_MSQUIC_TEST_CERT must be defined"
#endif
#ifndef TREVRPC_MSQUIC_TEST_KEY
#error "TREVRPC_MSQUIC_TEST_KEY must be defined"
#endif

#define MAX_PENDING_EVENTS 256u
#define EVENT_WAIT_ATTEMPTS 20
#define TEST_CANCEL_APPLICATION_ERROR UINT64_C(42)

#ifndef TREVRPC_RPC_TEST_TRANSPORT
#define TREVRPC_RPC_TEST_TRANSPORT TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE
#endif

static uint32_t test_webtransport_profiles;

typedef struct pending_event {
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
} pending_event;

typedef struct harness {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 listener;
    trevrpc_rpc_endpoint_v1 client_endpoint;
    pending_event pending[MAX_PENDING_EVENTS];
    size_t pending_count;
} harness;

typedef struct call_pair {
    trevrpc_rpc_call_v1 client_call;
    trevrpc_rpc_stream_v1 client_stream;
    trevrpc_rpc_call_v1 server_call;
    trevrpc_rpc_stream_v1 server_stream;
} call_pair;

static bool handle_equal(uint64_t left_owner,
    uint32_t left_slot,
    uint32_t left_generation,
    uint64_t right_owner,
    uint32_t right_slot,
    uint32_t right_generation) {
    return left_owner == right_owner && left_slot == right_slot && left_generation == right_generation;
}

static void collect_events(harness* state) {
    for (;;) {
        trevrpc_rpc_event* event = NULL;
        pending_event* pending;
        int result = trevrpc_rpc_runtime_next_event(state->runtime, &event);
        if (result == -EAGAIN) {
            return;
        }
        assert(result == 0);
        assert(state->pending_count < MAX_PENDING_EVENTS);
        pending = &state->pending[state->pending_count++];
        pending->event = event;
        assert(trevrpc_rpc_event_info_v1_init(&pending->info, sizeof(pending->info)) == 0);
        assert(trevrpc_rpc_event_get_info_v1(event, &pending->info) == 0);
    }
}

static void wait_for_events(harness* state) {
    struct pollfd descriptor = {.fd = (int)state->wake.native_handle, .events = POLLIN, .revents = 0};
    int result = poll(&descriptor, 1, 1000);
    assert(result >= 0);
    if (result != 0) {
        collect_events(state);
    }
}

static trevrpc_rpc_event* take_pending(harness* state, size_t index, trevrpc_rpc_event_info_v1* out_info) {
    trevrpc_rpc_event* event = state->pending[index].event;
    *out_info = state->pending[index].info;
    --state->pending_count;
    if (index != state->pending_count) {
        state->pending[index] = state->pending[state->pending_count];
    }
    return event;
}

static void dump_pending_events(harness* state) {
    trevrpc_rpc_diagnostics_v1 diagnostics;
    if (trevrpc_rpc_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0 &&
        trevrpc_rpc_runtime_get_diagnostics_v1(state->runtime, &diagnostics) == 0) {
        fprintf(stderr,
            "  runtime state=%u terminal=%d queue=%u ordinary=%u enqueued=%llu dequeued=%llu rejected=%llu "
            "endpoints=%llu calls=%llu streams=%llu callbacks=%llu reservations=%llu provider=%llu "
            "wake_signals=%llu wake_eagain=%llu wake_failures=%llu\n",
            diagnostics.state,
            diagnostics.terminal_status,
            diagnostics.queue_depth,
            diagnostics.ordinary_queue_depth,
            (unsigned long long)diagnostics.events_enqueued,
            (unsigned long long)diagnostics.events_dequeued,
            (unsigned long long)diagnostics.events_rejected,
            (unsigned long long)diagnostics.live_endpoints,
            (unsigned long long)diagnostics.live_calls,
            (unsigned long long)diagnostics.live_streams,
            (unsigned long long)diagnostics.active_callbacks,
            (unsigned long long)diagnostics.mandatory_reservations,
            (unsigned long long)diagnostics.provider_error_code,
            (unsigned long long)diagnostics.wake_signals,
            (unsigned long long)diagnostics.wake_write_eagain,
            (unsigned long long)diagnostics.wake_failures);
    }
    for (size_t index = 0; index < state->pending_count; ++index) {
        const trevrpc_rpc_event_info_v1* info = &state->pending[index].info;
        fprintf(stderr,
            "  pending kind=%u operation=%llu status=%d flags=%u application=%llu provider=%llu "
            "endpoint=%llu/%u/%u call=%llu/%u/%u stream=%llu/%u/%u\n",
            info->kind,
            (unsigned long long)info->operation_id,
            info->status,
            info->flags,
            (unsigned long long)info->application_error_code,
            (unsigned long long)info->provider_error_code,
            (unsigned long long)info->endpoint.owner,
            info->endpoint.slot,
            info->endpoint.generation,
            (unsigned long long)info->call.owner,
            info->call.slot,
            info->call.generation,
            (unsigned long long)info->stream.owner,
            info->stream.slot,
            info->stream.generation);
    }
}

static trevrpc_rpc_event* wait_kind_operation(
    harness* state, uint32_t kind, uint64_t operation_id, trevrpc_rpc_event_info_v1* out_info) {
    int attempt;
    for (attempt = 0; attempt < EVENT_WAIT_ATTEMPTS; ++attempt) {
        size_t index;
        collect_events(state);
        for (index = 0; index < state->pending_count; ++index) {
            if (state->pending[index].info.kind == kind && state->pending[index].info.operation_id == operation_id) {
                return take_pending(state, index, out_info);
            }
        }
        wait_for_events(state);
    }
    fprintf(stderr,
        "timed out waiting for event kind=%u operation=%llu; pending=%zu\n",
        kind,
        (unsigned long long)operation_id,
        state->pending_count);
    dump_pending_events(state);
    assert(false);
    return NULL;
}

static trevrpc_rpc_event* wait_kind_stream(harness* state,
    uint32_t kind,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    trevrpc_rpc_event_info_v1* out_info) {
    int attempt;
    for (attempt = 0; attempt < EVENT_WAIT_ATTEMPTS; ++attempt) {
        size_t index;
        collect_events(state);
        for (index = 0; index < state->pending_count; ++index) {
            const trevrpc_rpc_event_info_v1* info = &state->pending[index].info;
            if (info->kind == kind && info->operation_id == operation_id &&
                handle_equal(info->stream.owner,
                    info->stream.slot,
                    info->stream.generation,
                    stream.owner,
                    stream.slot,
                    stream.generation)) {
                return take_pending(state, index, out_info);
            }
        }
        wait_for_events(state);
    }
    fprintf(stderr,
        "timed out waiting for stream event kind=%u operation=%llu stream=%llu/%u/%u; pending=%zu\n",
        kind,
        (unsigned long long)operation_id,
        (unsigned long long)stream.owner,
        stream.slot,
        stream.generation,
        state->pending_count);
    dump_pending_events(state);
    assert(false);
    return NULL;
}

static trevrpc_rpc_event* wait_kind_call(harness* state,
    uint32_t kind,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    trevrpc_rpc_event_info_v1* out_info) {
    int attempt;
    for (attempt = 0; attempt < EVENT_WAIT_ATTEMPTS; ++attempt) {
        size_t index;
        collect_events(state);
        for (index = 0; index < state->pending_count; ++index) {
            const trevrpc_rpc_event_info_v1* info = &state->pending[index].info;
            if (info->kind == kind && info->operation_id == operation_id &&
                handle_equal(
                    info->call.owner, info->call.slot, info->call.generation, call.owner, call.slot, call.generation)) {
                return take_pending(state, index, out_info);
            }
        }
        wait_for_events(state);
    }
    fprintf(stderr,
        "timed out waiting for call event kind=%u operation=%llu call=%llu/%u/%u; pending=%zu\n",
        kind,
        (unsigned long long)operation_id,
        (unsigned long long)call.owner,
        call.slot,
        call.generation,
        state->pending_count);
    dump_pending_events(state);
    assert(false);
    return NULL;
}

static void release_waited(trevrpc_rpc_event* event) {
    assert(event != NULL);
    trevrpc_rpc_event_release(event);
}

static void setup_harness(harness* state) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_msquic_endpoint_config_v1 listener_config;
    trevrpc_rpc_msquic_endpoint_config_v1 client_config;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    uint16_t port = 0;

    memset(state, 0, sizeof(*state));
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &state->runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&state->wake, sizeof(state->wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(state->runtime, &state->wake) == 0);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&listener_config, sizeof(listener_config)) == 0);
    listener_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
    listener_config.transport = TREVRPC_RPC_TEST_TRANSPORT;
    if (test_webtransport_profiles != 0)
        listener_config.webtransport_profiles = test_webtransport_profiles;
    listener_config.host = "127.0.0.1";
    listener_config.host_len = (uint32_t)strlen(listener_config.host);
    listener_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    listener_config.cert_file_len = (uint32_t)strlen(listener_config.cert_file);
    listener_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    listener_config.key_file_len = (uint32_t)strlen(listener_config.key_file);
    int start_result = trevrpc_rpc_msquic_endpoint_start_v1(state->runtime, &listener_config, 1, &state->listener);
    if (start_result != 0)
        fprintf(stderr,
            "listener endpoint start failed: %d frame=%llu receive_bytes=%llu receive_count=%u\n",
            start_result,
            (unsigned long long)listener_config.max_frame_size,
            (unsigned long long)listener_config.max_pending_receive_bytes,
            listener_config.max_pending_receive_count);
    assert(start_result == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_ENDPOINT_READY, 1, &info);
    release_waited(event);
    assert(trevrpc_rpc_endpoint_get_port_v1(state->runtime, state->listener, &port) == 0);
    assert(port != 0);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&client_config, sizeof(client_config)) == 0);
    client_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
    client_config.transport = TREVRPC_RPC_TEST_TRANSPORT;
    if (test_webtransport_profiles != 0)
        client_config.webtransport_profiles = test_webtransport_profiles;
    client_config.host = "127.0.0.1";
    client_config.host_len = (uint32_t)strlen(client_config.host);
    client_config.port = port;
    client_config.flags &= ~TREVRPC_RPC_MSQUIC_VERIFY_PEER;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(state->runtime, &client_config, 2, &state->client_endpoint) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_ENDPOINT_READY, 2, &info);
    release_waited(event);
}

static call_pair open_call(harness* state, uint32_t kind, const char* method, uint64_t operation_id) {
    trevrpc_rpc_call_config_v1 config;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    call_pair pair = {0};

    assert(trevrpc_rpc_call_config_v1_init(&config, sizeof(config)) == 0);
    config.kind = kind;
    config.service = "test.Shapes";
    config.service_len = (uint32_t)strlen(config.service);
    config.method = method;
    config.method_len = (uint32_t)strlen(method);
    config.initial_message = (const uint8_t*)"initial";
    config.initial_message_len = strlen("initial");
    assert(trevrpc_rpc_call_open_v1(
               state->runtime, state->client_endpoint, &config, operation_id, &pair.client_call, &pair.client_stream) ==
           0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_READY, operation_id, &info);
    assert(handle_equal(info.call.owner,
        info.call.slot,
        info.call.generation,
        pair.client_call.owner,
        pair.client_call.slot,
        pair.client_call.generation));
    release_waited(event);

    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_INCOMING, 0, &info);
    assert(info.rpc_kind == kind);
    assert(info.method_len == strlen(method));
    assert(memcmp(info.method, method, info.method_len) == 0);
    assert(trevrpc_rpc_event_take_incoming_call(event, &pair.server_call, &pair.server_stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
    assert(receive_info.data_len == strlen("initial"));
    assert(memcmp(receive_info.data, "initial", receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(receive);
    release_waited(event);

    assert(trevrpc_rpc_call_accept(state->runtime, pair.server_call, operation_id + 1) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_ACCEPTED, operation_id + 1, &info);
    release_waited(event);
    if (kind == TREVRPC_RPC_KIND_UNARY || kind == TREVRPC_RPC_KIND_SERVER_STREAMING) {
        event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN, pair.server_stream, 0, &info);
        release_waited(event);
    } else {
        trevrpc_rpc_receive* extra = NULL;
        event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_READABLE, pair.server_stream, 0, &info);
        release_waited(event);
        assert(trevrpc_rpc_stream_receive(state->runtime, pair.server_stream, &extra) == -EAGAIN);
        assert(extra == NULL);
    }
    return pair;
}

static void wait_readable(harness* state, trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_READABLE, stream, 0, &info);
    release_waited(event);
}

static int receive_message(harness* state, trevrpc_rpc_stream_v1 stream, const char* expected, uint32_t expected_kind) {
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_receive* receive = NULL;
    int result = trevrpc_rpc_stream_receive(state->runtime, stream, &receive);
    if (result != 0) {
        assert(receive == NULL);
        return result;
    }
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == expected_kind);
    if (expected != NULL) {
        assert(receive_info.data_len == strlen(expected));
        assert(memcmp(receive_info.data, expected, receive_info.data_len) == 0);
    }
    trevrpc_rpc_receive_release(receive);
    return 0;
}

static void finish_readable(harness* state, trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_receive* receive = NULL;
    assert(trevrpc_rpc_stream_receive(state->runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
}

static void wait_receive_message(
    harness* state, trevrpc_rpc_stream_v1 stream, const char* expected, uint32_t expected_kind) {
    for (;;) {
        int result;
        wait_readable(state, stream);
        result = receive_message(state, stream, expected, expected_kind);
        if (result == 0) {
            break;
        }
        assert(result == -EAGAIN);
    }
    finish_readable(state, stream);
}

static void send_message(harness* state,
    trevrpc_rpc_stream_v1 source,
    trevrpc_rpc_stream_v1 destination,
    const char* message,
    uint64_t operation_id) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_stream_send_copy_v1(state->runtime,
               source,
               operation_id,
               (const uint8_t*)message,
               strlen(message),
               TREVRPC_RPC_SEND_FLAG_NONE) == 0);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_COMPLETE, source, operation_id, &info);
    release_waited(event);
    wait_receive_message(state, destination, message, TREVRPC_RPC_RECEIVE_MESSAGE);
}

static void wait_receive_fin(harness* state, trevrpc_rpc_stream_v1 stream) {
    for (int attempt = 0; attempt < EVENT_WAIT_ATTEMPTS; ++attempt) {
        trevrpc_rpc_event_info_v1 info;
        trevrpc_rpc_event* event = NULL;
        collect_events(state);
        for (size_t index = 0; index < state->pending_count; ++index) {
            const trevrpc_rpc_event_info_v1* pending = &state->pending[index].info;
            if (pending->kind == TREVRPC_RPC_EVENT_STREAM_READABLE && handle_equal(pending->stream.owner,
                                                                          pending->stream.slot,
                                                                          pending->stream.generation,
                                                                          stream.owner,
                                                                          stream.slot,
                                                                          stream.generation)) {
                event = take_pending(state, index, &info);
                release_waited(event);
                finish_readable(state, stream);
                break;
            }
        }
        if (event != NULL) {
            continue;
        }
        for (size_t index = 0; index < state->pending_count; ++index) {
            const trevrpc_rpc_event_info_v1* pending = &state->pending[index].info;
            if (pending->kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN && handle_equal(pending->stream.owner,
                                                                             pending->stream.slot,
                                                                             pending->stream.generation,
                                                                             stream.owner,
                                                                             stream.slot,
                                                                             stream.generation)) {
                event = take_pending(state, index, &info);
                release_waited(event);
                return;
            }
        }
        wait_for_events(state);
    }
    fprintf(stderr,
        "timed out waiting for receive FIN stream=%llu/%u/%u; pending=%zu\n",
        (unsigned long long)stream.owner,
        stream.slot,
        stream.generation,
        state->pending_count);
    dump_pending_events(state);
    assert(false);
}

static void finish_send(
    harness* state, trevrpc_rpc_stream_v1 source, trevrpc_rpc_stream_v1 destination, uint64_t operation_id) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_stream_finish_send(state->runtime, source, operation_id) == 0);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_FINISHED, source, operation_id, &info);
    release_waited(event);
    wait_receive_fin(state, destination);
}

static void close_call(harness* state, call_pair pair, uint64_t operation_id) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    int result = trevrpc_rpc_call_close(state->runtime, pair.client_call, operation_id, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
    uint64_t completion_id = operation_id;
    if (result == -EALREADY || result == -ESTALE) {
        completion_id = 0;
    } else {
        assert(result == 0);
    }
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_CLOSED, pair.client_stream, 0, &info);
    release_waited(event);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_CLOSED, pair.client_call, completion_id, &info);
    release_waited(event);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_CLOSED, pair.server_stream, 0, &info);
    release_waited(event);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_CLOSED, pair.server_call, 0, &info);
    release_waited(event);
    assert(trevrpc_rpc_stream_release(state->runtime, pair.client_stream) == 0);
    assert(trevrpc_rpc_call_release(state->runtime, pair.client_call) == 0);
    assert(trevrpc_rpc_stream_release(state->runtime, pair.server_stream) == 0);
    assert(trevrpc_rpc_call_release(state->runtime, pair.server_call) == 0);
}

static void run_unary(harness* state) {
    call_pair pair = open_call(state, TREVRPC_RPC_KIND_UNARY, "Unary", 100);
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(trevrpc_rpc_call_respond_copy_v1(state->runtime,
               pair.server_call,
               102,
               &status,
               (const uint8_t*)"unary-response",
               strlen("unary-response")) == 0);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_COMPLETE, pair.server_stream, 102, &info);
    release_waited(event);
    wait_receive_message(state, pair.client_stream, "unary-response", TREVRPC_RPC_RECEIVE_MESSAGE);
    wait_receive_fin(state, pair.client_stream);
    close_call(state, pair, 103);
}

static void wait_response_and_status(harness* state, trevrpc_rpc_stream_v1 stream, const char* expected_message) {
    bool received_message = false;
    bool received_status = false;
    while (!received_status) {
        wait_readable(state, stream);
        for (;;) {
            trevrpc_rpc_receive_info_v1 info;
            trevrpc_rpc_receive* receive = NULL;
            int result = trevrpc_rpc_stream_receive(state->runtime, stream, &receive);
            if (result == -EAGAIN) {
                break;
            }
            assert(result == 0);
            assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_rpc_receive_get_info_v1(receive, &info) == 0);
            if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE) {
                assert(!received_message);
                assert(info.data_len == strlen(expected_message));
                assert(memcmp(info.data, expected_message, info.data_len) == 0);
                received_message = true;
            } else {
                assert(info.kind == TREVRPC_RPC_RECEIVE_STATUS);
                assert(info.rpc_status == TREVRPC_RPC_STATUS_OK);
                received_status = true;
            }
            trevrpc_rpc_receive_release(receive);
        }
    }
    assert(received_message);
}

static void run_client_streaming(harness* state) {
    call_pair pair = open_call(state, TREVRPC_RPC_KIND_CLIENT_STREAMING, "ClientStreaming", 200);
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    send_message(state, pair.client_stream, pair.server_stream, "client-message", 202);
    finish_send(state, pair.client_stream, pair.server_stream, 203);
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(trevrpc_rpc_call_respond_copy_v1(state->runtime,
               pair.server_call,
               204,
               &status,
               (const uint8_t*)"client-summary",
               strlen("client-summary")) == 0);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_COMPLETE, pair.server_stream, 204, &info);
    release_waited(event);
    wait_response_and_status(state, pair.client_stream, "client-summary");
    wait_receive_fin(state, pair.client_stream);
    close_call(state, pair, 205);
}

static void finish_server_call(harness* state, call_pair pair, uint64_t operation_id) {
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(trevrpc_rpc_call_finish_v1(state->runtime, pair.server_call, operation_id, &status) == 0);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_FINISHED, pair.server_call, operation_id, &info);
    release_waited(event);
    wait_receive_message(state, pair.client_stream, NULL, TREVRPC_RPC_RECEIVE_STATUS);
}

static void run_server_streaming(harness* state) {
    call_pair pair = open_call(state, TREVRPC_RPC_KIND_SERVER_STREAMING, "ServerStreaming", 300);
    send_message(state, pair.server_stream, pair.client_stream, "server-one", 302);
    send_message(state, pair.server_stream, pair.client_stream, "server-two", 303);
    finish_server_call(state, pair, 304);
    close_call(state, pair, 305);
}

static void run_bidirectional(harness* state) {
    call_pair pair = open_call(state, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, "Bidirectional", 400);
    send_message(state, pair.client_stream, pair.server_stream, "client-bidi", 402);
    send_message(state, pair.server_stream, pair.client_stream, "server-bidi", 403);
    finish_send(state, pair.client_stream, pair.server_stream, 404);
    finish_server_call(state, pair, 405);
    close_call(state, pair, 406);
}

static void run_active_cancel(harness* state) {
    call_pair pair = open_call(state, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, "ActiveCancel", 500);
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;

    send_message(state, pair.client_stream, pair.server_stream, "before-cancel-client", 502);
    send_message(state, pair.server_stream, pair.client_stream, "before-cancel-server", 503);

    assert(trevrpc_rpc_call_cancel(state->runtime, pair.client_call, 504, TEST_CANCEL_APPLICATION_ERROR) == 0);
    assert(trevrpc_rpc_call_cancel(state->runtime, pair.client_call, 505, TEST_CANCEL_APPLICATION_ERROR) == -EALREADY);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CANCELLED, pair.client_call, 504, &info);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_CALL);
    assert(info.status == 0);
    assert(handle_equal(info.stream.owner,
        info.stream.slot,
        info.stream.generation,
        pair.client_stream.owner,
        pair.client_stream.slot,
        pair.client_stream.generation));
    assert(info.application_error_code == TEST_CANCEL_APPLICATION_ERROR);
    release_waited(event);

    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_CLOSED, pair.client_stream, 0, &info);
    release_waited(event);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_CLOSED, pair.client_call, 0, &info);
    release_waited(event);
#if TREVRPC_RPC_TEST_TRANSPORT == TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE
    /* Native MsQuic publishes the reset on RECEIVE_FIN; its later terminal
     * event intentionally carries only the generic stream-close state. */
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN, pair.server_stream, 0, &info);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_PEER_RESET) != 0);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_PEER_CANCELLED) != 0);
    assert(info.status == -ECANCELED);
    assert(info.application_error_code == TEST_CANCEL_APPLICATION_ERROR);
    release_waited(event);
#endif
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_CLOSED, pair.server_stream, 0, &info);
#if TREVRPC_RPC_TEST_TRANSPORT != TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_PEER_RESET) != 0);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_PEER_CANCELLED) != 0);
    assert(info.status == -ECANCELED);
#if TREVRPC_RPC_TEST_TRANSPORT == TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3
    assert(info.application_error_code == TREVRPC_RPC_STATUS_CANCELLED);
#else
    assert(info.application_error_code == TEST_CANCEL_APPLICATION_ERROR);
#endif
#endif
    release_waited(event);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_CLOSED, pair.server_call, 0, &info);
    release_waited(event);

    assert(trevrpc_rpc_stream_release(state->runtime, pair.client_stream) == 0);
    assert(trevrpc_rpc_call_release(state->runtime, pair.client_call) == 0);
    assert(trevrpc_rpc_stream_release(state->runtime, pair.server_stream) == 0);
    assert(trevrpc_rpc_call_release(state->runtime, pair.server_call) == 0);

    pair = open_call(state, TREVRPC_RPC_KIND_UNARY, "AfterCancel", 510);
    close_call(state, pair, 512);
}

static void teardown_harness(harness* state) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
    size_t index;
    assert(trevrpc_rpc_endpoint_close(state->runtime, state->client_endpoint, 900) == 0);
    assert(trevrpc_rpc_endpoint_close(state->runtime, state->listener, 901) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_ENDPOINT_CLOSED, 900, &info);
    release_waited(event);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_ENDPOINT_CLOSED, 901, &info);
    release_waited(event);
    assert(trevrpc_rpc_endpoint_release(state->runtime, state->client_endpoint) == 0);
    assert(trevrpc_rpc_endpoint_release(state->runtime, state->listener) == 0);
    assert(trevrpc_rpc_runtime_close(state->runtime, 902) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_STOPPED, 902, &info);
    release_waited(event);
    collect_events(state);
    for (index = 0; index < state->pending_count; ++index) {
        trevrpc_rpc_event_release(state->pending[index].event);
    }
    state->pending_count = 0;
    assert(trevrpc_rpc_runtime_drain(state->runtime) == 0);
    assert(trevrpc_rpc_runtime_release(state->runtime) == 0);
}

int main(int argc, char** argv) {
    harness state;
    if (argc == 2) {
        char* end = NULL;
        unsigned long value = strtoul(argv[1], &end, 0);
        assert(end != argv[1] && *end == '\0' && value <= UINT32_MAX);
        test_webtransport_profiles = (uint32_t)value;
    } else {
        assert(argc == 1);
    }
    setup_harness(&state);
    run_unary(&state);
    run_client_streaming(&state);
    run_server_streaming(&state);
    run_bidirectional(&state);
    run_active_cancel(&state);
    teardown_harness(&state);
    return 0;
}
