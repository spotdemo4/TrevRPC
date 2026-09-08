#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "trevrpc_rpc_msquic.h"

#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#error "TREVRPC_MSQUIC_TEST_CERT must be defined"
#endif
#ifndef TREVRPC_MSQUIC_TEST_KEY
#error "TREVRPC_MSQUIC_TEST_KEY must be defined"
#endif
#ifndef TREVRPC_GENERATED_HEADER
#error "TREVRPC_GENERATED_HEADER must be defined"
#endif
#ifndef TREVRPC_GENERATED_SOURCE
#error "TREVRPC_GENERATED_SOURCE must be defined"
#endif

#define MAX_PENDING_EVENTS 256u

static uint32_t test_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;

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
    struct pollfd descriptor = {
        .fd = (int)state->wake.native_handle,
        .events = POLLIN,
        .revents = 0,
    };
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

static trevrpc_rpc_event* wait_kind_operation(
    harness* state, uint32_t kind, uint64_t operation_id, trevrpc_rpc_event_info_v1* out_info) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        collect_events(state);
        for (size_t index = 0; index < state->pending_count; ++index) {
            if (state->pending[index].info.kind == kind && state->pending[index].info.operation_id == operation_id) {
                return take_pending(state, index, out_info);
            }
        }
        wait_for_events(state);
    }
    assert(false);
    return NULL;
}

static trevrpc_rpc_event* wait_kind_stream(harness* state,
    uint32_t kind,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    trevrpc_rpc_event_info_v1* out_info) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        collect_events(state);
        for (size_t index = 0; index < state->pending_count; ++index) {
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
    assert(false);
    return NULL;
}

static trevrpc_rpc_event* wait_kind_call(harness* state,
    uint32_t kind,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    trevrpc_rpc_event_info_v1* out_info) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        collect_events(state);
        for (size_t index = 0; index < state->pending_count; ++index) {
            const trevrpc_rpc_event_info_v1* info = &state->pending[index].info;
            if (info->kind == kind && info->operation_id == operation_id &&
                handle_equal(
                    info->call.owner, info->call.slot, info->call.generation, call.owner, call.slot, call.generation)) {
                return take_pending(state, index, out_info);
            }
        }
        wait_for_events(state);
    }
    assert(false);
    return NULL;
}

static void release_waited(trevrpc_rpc_event* event) {
    assert(event != NULL);
    trevrpc_rpc_event_release(event);
}

static bool text_file_contains(const char* path, const char* needle) {
    FILE* file = fopen(path, "rb");
    char line[4096];
    bool found = false;
    if (file == NULL) {
        return false;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        if (strstr(line, needle) != NULL) {
            found = true;
            break;
        }
    }
    fclose(file);
    return found;
}

static void test_generated_surface(void) {
    assert(text_file_contains(TREVRPC_GENERATED_HEADER, "#include \"trevrpc_rpc.h\""));
    assert(text_file_contains(TREVRPC_GENERATED_HEADER, "TREVRPC_RPC_ABI_VERSION != 1u"));
    assert(text_file_contains(TREVRPC_GENERATED_HEADER, "_decode_request_receive("));
    assert(text_file_contains(TREVRPC_GENERATED_HEADER, "_decode_response_receive("));
    assert(text_file_contains(TREVRPC_GENERATED_HEADER, "_take_incoming("));
    assert(text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_rpc_call_open_v1("));
    assert(text_file_contains(TREVRPC_GENERATED_SOURCE, "trevrpc_rpc_event_take_incoming_call("));
}

static void test_unresolved_stream_budget_validation(void) {
    trevrpc_rpc_msquic_endpoint_config_v1 config;
    trevrpc_rpc_endpoint_v1 endpoint = {0};
    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&config, sizeof(config)) == 0);
    assert(config.webtransport_profiles == TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED);
    assert(TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED == 0x0000000fu);
    config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
    config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT;
    config.unresolved_stream_bytes = 1;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(NULL, &config, 1, &endpoint) == -EINVAL);
    config.unresolved_stream_bytes = 0;
    config.webtransport_profiles = 0x00000010u;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(NULL, &config, 1, &endpoint) == -EINVAL);
}

static void test_generated_signatures(void) {
    int (*unary_open)(trevrpc_rpc_runtime*,
        trevrpc_rpc_endpoint_v1,
        const Hello__V1__HelloRequest*,
        uint64_t,
        trevrpc_rpc_call_v1*,
        trevrpc_rpc_stream_v1*) = hello_v1_greeter_say_hello_open;
    int (*client_send)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, const Hello__V1__HelloRequest*, uint64_t) =
        hello_v1_greeter_lots_of_greetings_send;
    int (*server_finish)(trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, uint64_t, const trevrpc_rpc_status_v1*) =
        hello_v1_greeter_lots_of_replies_finish;
    int (*take_incoming)(trevrpc_rpc_event*, trevrpc_rpc_call_v1*, trevrpc_rpc_stream_v1*, trevrpc_rpc_receive**) =
        hello_v1_greeter_bidi_hello_take_incoming;
    assert(unary_open != NULL);
    assert(client_send != NULL);
    assert(server_finish != NULL);
    assert(take_incoming != NULL);
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
    listener_config.transport = test_transport;
    listener_config.host = "127.0.0.1";
    listener_config.host_len = (uint32_t)strlen(listener_config.host);
    listener_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    listener_config.cert_file_len = (uint32_t)strlen(listener_config.cert_file);
    listener_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    listener_config.key_file_len = (uint32_t)strlen(listener_config.key_file);
    if (test_transport == TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT)
        listener_config.unresolved_stream_bytes = 128;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(state->runtime, &listener_config, 1, &state->listener) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_ENDPOINT_READY, 1, &info);
    release_waited(event);
    assert(trevrpc_rpc_endpoint_get_port_v1(state->runtime, state->listener, &port) == 0);
    assert(port != 0);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&client_config, sizeof(client_config)) == 0);
    client_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
    client_config.transport = test_transport;
    client_config.host = "127.0.0.1";
    client_config.host_len = (uint32_t)strlen(client_config.host);
    client_config.port = port;
    client_config.flags &= ~TREVRPC_RPC_MSQUIC_VERIFY_PEER;
    if (test_transport == TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT)
        client_config.unresolved_stream_bytes = 128;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(state->runtime, &client_config, 2, &state->client_endpoint) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_ENDPOINT_READY, 2, &info);
    release_waited(event);
}

static trevrpc_rpc_event* wait_incoming(harness* state, trevrpc_rpc_event_info_v1* out_info) {
    return wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_INCOMING, 0, out_info);
}

static void verify_request_receive(trevrpc_rpc_receive* receive, const char* expected);

static void verify_initial_request(trevrpc_rpc_receive* receive, const char* expected_name) {
    Hello__V1__HelloRequest* decoded = (Hello__V1__HelloRequest*)(uintptr_t)1;
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(0);
    assert(hello_v1_greeter_say_hello_decode_request_receive(receive, &decoded) == -ENOMEM);
    assert(decoded == (Hello__V1__HelloRequest*)(uintptr_t)1);
    trevrpc_hello_v1_greeter_proto_test_fail_allocation_after(SIZE_MAX);
    decoded = NULL;
    assert(hello_v1_greeter_say_hello_decode_request_receive(receive, &decoded) == 0);
    assert(decoded != NULL);
    assert(decoded->name != NULL);
    assert(strcmp(decoded->name, expected_name) == 0);
    hello__v1__hello_request__free_unpacked(decoded, NULL);
}

static call_pair open_unary(harness* state, uint64_t operation_id, const char* name) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_receive* initial = NULL;
    trevrpc_rpc_event* event;
    call_pair pair = {0};
    request.name = (char*)name;

    assert(
        hello_v1_greeter_say_hello_open(
            state->runtime, state->client_endpoint, &request, operation_id, &pair.client_call, &pair.client_stream) ==
        0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_READY, operation_id, &info);
    release_waited(event);

    event = wait_incoming(state, &info);
    assert(hello_v1_greeter_say_hello_matches_incoming(&info) == 1);
    trevrpc_rpc_call_v1 unchanged_call = {11, 12, 13};
    trevrpc_rpc_stream_v1 unchanged_stream = {21, 22, 23};
    trevrpc_rpc_receive* unchanged_receive = (trevrpc_rpc_receive*)(uintptr_t)1;
    assert(hello_v1_greeter_lots_of_replies_take_incoming(
               event, &unchanged_call, &unchanged_stream, &unchanged_receive) == -EPROTO);
    assert(unchanged_call.owner == 11 && unchanged_call.slot == 12 && unchanged_call.generation == 13);
    assert(unchanged_stream.owner == 21 && unchanged_stream.slot == 22 && unchanged_stream.generation == 23);
    assert(unchanged_receive == (trevrpc_rpc_receive*)(uintptr_t)1);
    assert(hello_v1_greeter_say_hello_take_incoming(event, &pair.server_call, &pair.server_stream, &initial) == 0);
    verify_initial_request(initial, name);
    trevrpc_rpc_receive_release(initial);
    release_waited(event);
    assert(hello_v1_greeter_say_hello_accept(state->runtime, pair.server_call, operation_id + 1) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_ACCEPTED, operation_id + 1, &info);
    release_waited(event);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN, pair.server_stream, 0, &info);
    release_waited(event);
    return pair;
}

static call_pair open_server_streaming(harness* state, uint64_t operation_id, const char* name) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloRequest* decoded = NULL;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_receive* initial = NULL;
    trevrpc_rpc_event* event;
    call_pair pair = {0};
    request.name = (char*)name;

    assert(
        hello_v1_greeter_lots_of_replies_open(
            state->runtime, state->client_endpoint, &request, operation_id, &pair.client_call, &pair.client_stream) ==
        0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_READY, operation_id, &info);
    release_waited(event);
    event = wait_incoming(state, &info);
    assert(hello_v1_greeter_lots_of_replies_matches_incoming(&info) == 1);
    assert(
        hello_v1_greeter_lots_of_replies_take_incoming(event, &pair.server_call, &pair.server_stream, &initial) == 0);
    assert(hello_v1_greeter_lots_of_replies_decode_request_receive(initial, &decoded) == 0);
    assert(decoded != NULL && decoded->name != NULL && strcmp(decoded->name, name) == 0);
    hello__v1__hello_request__free_unpacked(decoded, NULL);
    trevrpc_rpc_receive_release(initial);
    release_waited(event);
    assert(hello_v1_greeter_lots_of_replies_accept(state->runtime, pair.server_call, operation_id + 1) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_ACCEPTED, operation_id + 1, &info);
    release_waited(event);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN, pair.server_stream, 0, &info);
    release_waited(event);
    return pair;
}

static call_pair open_client_streaming(harness* state, uint64_t operation_id) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_receive* initial = NULL;
    trevrpc_rpc_event* event;
    call_pair pair = {0};
    request.name = "client-initial";

    assert(
        hello_v1_greeter_lots_of_greetings_open(
            state->runtime, state->client_endpoint, &request, operation_id, &pair.client_call, &pair.client_stream) ==
        0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_READY, operation_id, &info);
    release_waited(event);
    event = wait_incoming(state, &info);
    assert(
        hello_v1_greeter_lots_of_greetings_take_incoming(event, &pair.server_call, &pair.server_stream, &initial) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(initial, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
    assert(receive_info.data_len > 0);
    verify_request_receive(initial, request.name);
    trevrpc_rpc_receive_release(initial);
    release_waited(event);
    assert(hello_v1_greeter_lots_of_greetings_accept(state->runtime, pair.server_call, operation_id + 1) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_ACCEPTED, operation_id + 1, &info);
    release_waited(event);
    return pair;
}

static call_pair open_bidi(harness* state, uint64_t operation_id) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_receive* initial = NULL;
    trevrpc_rpc_event* event;
    call_pair pair = {0};
    request.name = "bidi-initial";

    assert(
        hello_v1_greeter_bidi_hello_open(
            state->runtime, state->client_endpoint, &request, operation_id, &pair.client_call, &pair.client_stream) ==
        0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_READY, operation_id, &info);
    release_waited(event);
    event = wait_incoming(state, &info);
    assert(hello_v1_greeter_bidi_hello_take_incoming(event, &pair.server_call, &pair.server_stream, &initial) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(initial, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
    assert(receive_info.data_len > 0);
    verify_request_receive(initial, request.name);
    trevrpc_rpc_receive_release(initial);
    release_waited(event);
    assert(hello_v1_greeter_bidi_hello_accept(state->runtime, pair.server_call, operation_id + 1) == 0);
    event = wait_kind_operation(state, TREVRPC_RPC_EVENT_CALL_ACCEPTED, operation_id + 1, &info);
    release_waited(event);
    return pair;
}

#define MAX_READABLE_RECEIVES 4u

static size_t receive_generation(harness* state, trevrpc_rpc_stream_v1 stream, trevrpc_rpc_receive** receives) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event = wait_kind_stream(state, TREVRPC_RPC_EVENT_STREAM_READABLE, stream, 0, &info);
    size_t count = 0;
    release_waited(event);
    for (;;) {
        int result;
        assert(count < MAX_READABLE_RECEIVES);
        result = trevrpc_rpc_stream_receive(state->runtime, stream, &receives[count]);
        if (result == -EAGAIN) {
            return count;
        }
        assert(result == 0);
        ++count;
    }
}

static trevrpc_rpc_receive* receive_next(harness* state, trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_receive* receives[MAX_READABLE_RECEIVES];
    size_t count;
    do {
        count = receive_generation(state, stream, receives);
    } while (count == 0);
    assert(count == 1);
    return receives[0];
}

static void wait_send(harness* state, trevrpc_rpc_stream_v1 stream, uint64_t operation_id) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_COMPLETE, stream, operation_id, &info);
    release_waited(event);
}

static void wait_fin(harness* state, trevrpc_rpc_stream_v1 stream) {
    for (int attempt = 0; attempt < 100; ++attempt) {
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
                trevrpc_rpc_receive* receive = NULL;
                event = take_pending(state, index, &info);
                release_waited(event);
                assert(trevrpc_rpc_stream_receive(state->runtime, stream, &receive) == -EAGAIN);
                assert(receive == NULL);
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
    assert(false);
}

static void verify_request_receive(trevrpc_rpc_receive* receive, const char* expected) {
    Hello__V1__HelloRequest* decoded = NULL;
    assert(hello_v1_greeter_lots_of_greetings_decode_request_receive(receive, &decoded) == 0);
    assert(decoded != NULL && decoded->name != NULL && strcmp(decoded->name, expected) == 0);
    hello__v1__hello_request__free_unpacked(decoded, NULL);
}

static void verify_response_receive(trevrpc_rpc_receive* receive, const char* expected) {
    Hello__V1__HelloReply* decoded = NULL;
    assert(hello_v1_greeter_bidi_hello_decode_response_receive(receive, &decoded) == 0);
    assert(decoded != NULL && decoded->message != NULL && strcmp(decoded->message, expected) == 0);
    hello__v1__hello_reply__free_unpacked(decoded, NULL);
}

static void verify_status_receive(trevrpc_rpc_receive* receive, uint32_t expected_status) {
    trevrpc_rpc_receive_info_v1 info;
    assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &info) == 0);
    assert(info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(info.rpc_status == expected_status);
}

static void receive_response_and_status(harness* state, trevrpc_rpc_stream_v1 stream, const char* expected) {
    bool received_message = false;
    bool received_status = false;
    while (!received_status) {
        trevrpc_rpc_receive* receives[MAX_READABLE_RECEIVES];
        size_t count = receive_generation(state, stream, receives);
        for (size_t index = 0; index < count; ++index) {
            trevrpc_rpc_receive_info_v1 info;
            assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_rpc_receive_get_info_v1(receives[index], &info) == 0);
            if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE) {
                verify_response_receive(receives[index], expected);
                received_message = true;
            } else {
                verify_status_receive(receives[index], TREVRPC_RPC_STATUS_OK);
                received_status = true;
            }
            trevrpc_rpc_receive_release(receives[index]);
        }
    }
    assert(received_message);
}

static void close_pair(harness* state, call_pair pair, uint64_t operation_id) {
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

static void test_unary(harness* state) {
    call_pair pair = open_unary(state, 100, "unary-request");
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    Hello__V1__HelloReply* decoded = NULL;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_receive* receive;
    reply.message = "unary-response";
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(hello_v1_greeter_say_hello_respond(state->runtime, pair.server_call, &reply, &status, 102) == 0);
    wait_send(state, pair.server_stream, 102);
    receive = receive_next(state, pair.client_stream);
    assert(hello_v1_greeter_say_hello_decode_response_receive(receive, &decoded) == 0);
    assert(decoded != NULL && decoded->message != NULL && strcmp(decoded->message, reply.message) == 0);
    hello__v1__hello_reply__free_unpacked(decoded, NULL);
    trevrpc_rpc_receive_release(receive);
    wait_fin(state, pair.client_stream);
    close_pair(state, pair, 103);
}

static void test_unary_empty_message(harness* state) {
    call_pair pair = open_unary(state, 150, "empty-response-request");
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    Hello__V1__HelloReply* decoded = NULL;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_receive* receive;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(hello_v1_greeter_say_hello_respond(state->runtime, pair.server_call, &reply, &status, 152) == 0);
    wait_send(state, pair.server_stream, 152);
    receive = receive_next(state, pair.client_stream);
    assert(hello_v1_greeter_say_hello_decode_response_receive(receive, &decoded) == 0);
    assert(decoded != NULL);
    assert(decoded->message == NULL || decoded->message[0] == '\0');
    hello__v1__hello_reply__free_unpacked(decoded, NULL);
    trevrpc_rpc_receive_release(receive);
    wait_fin(state, pair.client_stream);
    close_pair(state, pair, 153);
}

static void test_server_streaming(harness* state) {
    call_pair pair = open_server_streaming(state, 200, "server-stream-request");
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_receive* receive;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);

    reply.message = "server-one";
    assert(hello_v1_greeter_lots_of_replies_send(state->runtime, pair.server_stream, &reply, 202) == 0);
    wait_send(state, pair.server_stream, 202);
    receive = receive_next(state, pair.client_stream);
    verify_response_receive(receive, reply.message);
    trevrpc_rpc_receive_release(receive);

    reply.message = "server-two";
    assert(hello_v1_greeter_lots_of_replies_send(state->runtime, pair.server_stream, &reply, 203) == 0);
    wait_send(state, pair.server_stream, 203);
    receive = receive_next(state, pair.client_stream);
    verify_response_receive(receive, reply.message);
    trevrpc_rpc_receive_release(receive);

    assert(hello_v1_greeter_lots_of_replies_finish(state->runtime, pair.server_call, 204, &status) == 0);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_FINISHED, pair.server_call, 204, &info);
    release_waited(event);
    receive = receive_next(state, pair.client_stream);
    verify_status_receive(receive, TREVRPC_RPC_STATUS_OK);
    trevrpc_rpc_receive_release(receive);
    close_pair(state, pair, 205);
}

static void test_client_streaming(harness* state) {
    call_pair pair = open_client_streaming(state, 300);
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_receive* receive;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);

    request.name = "client-one";
    assert(hello_v1_greeter_lots_of_greetings_send(state->runtime, pair.client_stream, &request, 302) == 0);
    wait_send(state, pair.client_stream, 302);
    receive = receive_next(state, pair.server_stream);
    verify_request_receive(receive, request.name);
    trevrpc_rpc_receive_release(receive);

    request.name = "client-two";
    assert(hello_v1_greeter_lots_of_greetings_send(state->runtime, pair.client_stream, &request, 303) == 0);
    wait_send(state, pair.client_stream, 303);
    receive = receive_next(state, pair.server_stream);
    verify_request_receive(receive, request.name);
    trevrpc_rpc_receive_release(receive);

    assert(hello_v1_greeter_lots_of_greetings_finish_send(state->runtime, pair.client_stream, 304) == 0);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_FINISHED, pair.client_stream, 304, &info);
    release_waited(event);
    wait_fin(state, pair.server_stream);

    reply.message = "client-summary";
    assert(hello_v1_greeter_lots_of_greetings_respond(state->runtime, pair.server_call, &reply, &status, 305) == 0);
    wait_send(state, pair.server_stream, 305);
    receive_response_and_status(state, pair.client_stream, reply.message);
    wait_fin(state, pair.client_stream);
    close_pair(state, pair, 306);
}

static void test_bidirectional(harness* state) {
    call_pair pair = open_bidi(state, 400);
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_receive* receive;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);

    request.name = "bidi-request";
    assert(hello_v1_greeter_bidi_hello_send_request(state->runtime, pair.client_stream, &request, 402) == 0);
    wait_send(state, pair.client_stream, 402);
    receive = receive_next(state, pair.server_stream);
    verify_request_receive(receive, request.name);
    trevrpc_rpc_receive_release(receive);

    reply.message = "bidi-response";
    assert(hello_v1_greeter_bidi_hello_send_response(state->runtime, pair.server_stream, &reply, 403) == 0);
    wait_send(state, pair.server_stream, 403);
    receive = receive_next(state, pair.client_stream);
    verify_response_receive(receive, reply.message);
    trevrpc_rpc_receive_release(receive);

    assert(hello_v1_greeter_bidi_hello_finish_send(state->runtime, pair.client_stream, 404) == 0);
    event = wait_kind_stream(state, TREVRPC_RPC_EVENT_SEND_FINISHED, pair.client_stream, 404, &info);
    release_waited(event);
    wait_fin(state, pair.server_stream);

    assert(hello_v1_greeter_bidi_hello_finish(state->runtime, pair.server_call, 405, &status) == 0);
    event = wait_kind_call(state, TREVRPC_RPC_EVENT_CALL_FINISHED, pair.server_call, 405, &info);
    release_waited(event);
    receive = receive_next(state, pair.client_stream);
    verify_status_receive(receive, TREVRPC_RPC_STATUS_OK);
    trevrpc_rpc_receive_release(receive);
    close_pair(state, pair, 406);
}

static void teardown_harness(harness* state) {
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_event* event;
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
    for (size_t index = 0; index < state->pending_count; ++index) {
        trevrpc_rpc_event_release(state->pending[index].event);
    }
    state->pending_count = 0;
    assert(trevrpc_rpc_runtime_drain(state->runtime) == 0);
    assert(trevrpc_rpc_runtime_release(state->runtime) == 0);
}

int main(int argc, char** argv) {
    harness state;
    if (argc == 2) {
        if (strcmp(argv[1], "native") == 0)
            test_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
        else if (strcmp(argv[1], "http3") == 0)
            test_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3;
        else if (strcmp(argv[1], "webtransport") == 0)
            test_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT;
        else
            assert(false);
    } else {
        assert(argc == 1);
    }
    test_generated_surface();
    test_unresolved_stream_budget_validation();
    test_generated_signatures();
    setup_harness(&state);
    test_unary(&state);
    test_unary_empty_message(&state);
    test_server_streaming(&state);
    test_client_streaming(&state);
    test_bidirectional(&state);
    teardown_harness(&state);
    return 0;
}
