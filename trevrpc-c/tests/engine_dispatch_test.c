#include "trevrpc_engine.h"
#include "trevrpc_engine_internal.h"
#include "trevrpc_engine_testing_internal.h"
#include "trevrpc_rpc_transport_engine_internal.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expression)                                                                                              \
    do {                                                                                                               \
        if (!(expression)) {                                                                                           \
            fprintf(stderr, "check failed at %s:%d: %s\n", __FILE__, __LINE__, #expression);                           \
            abort();                                                                                                   \
        }                                                                                                              \
    } while (0)

#define TEST_OWNER UINT64_C(0x6469737061746368)

struct test_provider {
    trevrpc_engine* engine;
    unsigned sends;
    bool invalid_completion;
    bool invalid_listener;
    int close_result;
    bool* destroyed;
};

int trevrpc_engine_msquic_adopt_accepted_connection_v1(trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_engine_handle_v1* out_connection) {
    (void)engine;
    (void)config;
    (void)accepted;
    (void)out_connection;
    return -ENOTSUP;
}

static int attach_provider(void* context, trevrpc_engine* engine) {
    struct test_provider* provider = context;
    provider->engine = engine;
    return 0;
}

static int listen_provider(void* context,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_engine_reservation* terminal,
    trevrpc_engine_handle_v1* out_listener) {
    struct test_provider* provider = context;
    trevrpc_engine_event_spec event;
    (void)config;
    if (!provider->invalid_listener) {
        out_listener->owner = TEST_OWNER;
        out_listener->slot = 1;
        out_listener->generation = 1;
    }
    memset(&event, 0, sizeof(event));
    event.kind = TREVRPC_ENGINE_EVENT_LISTENER_STOPPED;
    event.flags = TREVRPC_ENGINE_EVENT_FLAG_TERMINAL;
    event.subject_kind = TREVRPC_ENGINE_OBJECT_LISTENER;
    event.subject = *out_listener;
    return trevrpc_engine_provider_publish_reserved(provider->engine, terminal, &event);
}

static int dial_provider(void* context,
    const trevrpc_engine_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_engine_reservation* completion,
    trevrpc_engine_reservation* terminal,
    trevrpc_engine_handle_v1* out_connection) {
    (void)context;
    (void)config;
    (void)operation_id;
    (void)completion;
    (void)terminal;
    (void)out_connection;
    return -EALREADY;
}

static int send_provider(void* context,
    trevrpc_engine_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len,
    trevrpc_engine_reservation* completion) {
    struct test_provider* provider = context;
    trevrpc_engine_event_spec event;
    (void)body;
    (void)body_len;
    ++provider->sends;
    memset(&event, 0, sizeof(event));
    event.kind = TREVRPC_ENGINE_EVENT_SEND_COMPLETE;
    event.subject_kind = TREVRPC_ENGINE_OBJECT_STREAM;
    event.subject = stream;
    event.operation_id = operation_id;
    if (provider->invalid_completion) {
        event.data = body;
        event.data_len = body_len;
    }
    return trevrpc_engine_provider_publish_reserved(provider->engine, completion, &event);
}

static int null_receive_provider(void* context, trevrpc_engine_handle_v1 stream, trevrpc_engine_receive** out_receive) {
    (void)context;
    (void)stream;
    (void)out_receive;
    return 0;
}

static int close_provider(void* context) {
    struct test_provider* provider = context;
    trevrpc_engine_provider_stopped(provider->engine, 0, 0);
    return provider->close_result;
}

static void diagnostics_provider(void* context, trevrpc_engine_provider_diagnostics* diagnostics) {
    struct test_provider* provider = context;
    diagnostics->pending_send_count = provider->sends;
    diagnostics->live_streams = 1;
}

static void destroy_provider(void* context) {
    struct test_provider* provider = context;
    if (provider->destroyed != NULL) {
        *provider->destroyed = true;
    }
    free(provider);
}

int main(void) {
    static const trevrpc_engine_provider_ops operations = {
        .attach = attach_provider,
        .listen = listen_provider,
        .dial = dial_provider,
        .stream_send_frame = send_provider,
        .stream_receive_frame = null_receive_provider,
        .close = close_provider,
        .get_diagnostics = diagnostics_provider,
        .destroy = destroy_provider,
    };
    struct test_provider* provider = calloc(1, sizeof(*provider));
    trevrpc_engine_config_v1 config;
    trevrpc_engine_endpoint_config_v1 endpoint;
    trevrpc_engine_diagnostics_v1 diagnostics;
    trevrpc_engine_handle_v1 listener = {0};
    trevrpc_engine_handle_v1 connection = {TEST_OWNER, 77, 88};
    trevrpc_engine_handle_v1 stream = {TEST_OWNER, 3, 9};
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_receive* receive = NULL;
    trevrpc_engine_event_info_v1 info;
    trevrpc_engine* engine = NULL;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_event* transport_event = NULL;
    trevrpc_rpc_transport_event_info transport_info;
    bool destroyed = false;
    uint8_t byte = 1;
    CHECK(provider != NULL);
    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    CHECK(trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    CHECK(trevrpc_engine_provider_create_v1(&config, &operations, provider, TEST_OWNER, &engine) == 0);
    CHECK(trevrpc_engine_listen_v1(engine, &endpoint, &listener) == 0);
    CHECK(listener.owner == TEST_OWNER && listener.slot == 1 && listener.generation == 1);
    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_LISTENER_STOPPED);
    CHECK(memcmp(&info.subject, &listener, sizeof(listener)) == 0);
    trevrpc_engine_event_release(event);
    event = NULL;

    CHECK(trevrpc_engine_dial_v1(engine, &endpoint, 41, &connection) == -EALREADY);
    CHECK(connection.slot == 77 && connection.generation == 88);
    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.mandatory_reservations == 0);
    CHECK(diagnostics.live_streams == 1);

    CHECK(trevrpc_engine_stream_send_frame_v1(engine, stream, 42, &byte, sizeof(byte)) == 0);
    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE && info.operation_id == 42);
    trevrpc_engine_event_release(event);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.mandatory_reservations == 0 && diagnostics.pending_send_count == 1);
    provider->invalid_completion = true;
    CHECK(trevrpc_engine_stream_send_frame_v1(engine, stream, 43, &byte, sizeof(byte)) == -EMSGSIZE);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.mandatory_reservations == 0 && diagnostics.pending_send_count == 2);
    CHECK(trevrpc_engine_stream_finish_send(engine, stream) == -ENOTSUP);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);

    provider = calloc(1, sizeof(*provider));
    CHECK(provider != NULL);
    provider->close_result = -EIO;
    provider->destroyed = &destroyed;
    engine = NULL;
    CHECK(trevrpc_engine_provider_create_v1(&config, &operations, provider, TEST_OWNER, &engine) == 0);
    CHECK(trevrpc_rpc_transport_engine_adopt(engine, &transport) == 0);
    CHECK(trevrpc_rpc_transport_close(transport) == -EIO);
    CHECK(trevrpc_rpc_transport_next_event(transport, &transport_event) == 0);
    CHECK(trevrpc_rpc_transport_event_get_info(transport, transport_event, &transport_info) == 0);
    CHECK(transport_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC);
    CHECK(transport_info.flags == TREVRPC_RPC_TRANSPORT_EVENT_FLAG_FATAL);
    CHECK(transport_info.status == -EIO);
    trevrpc_rpc_transport_event_release(transport, transport_event);
    transport_event = NULL;
    CHECK(trevrpc_rpc_transport_next_event(transport, &transport_event) == 0);
    CHECK(trevrpc_rpc_transport_event_get_info(transport, transport_event, &transport_info) == 0);
    CHECK(transport_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED);
    CHECK(transport_info.flags == (TREVRPC_RPC_TRANSPORT_EVENT_FLAG_FATAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL));
    CHECK(transport_info.status == -EIO);
    trevrpc_rpc_transport_event_release(transport, transport_event);
    transport_event = NULL;
    CHECK(trevrpc_rpc_transport_next_event(transport, &transport_event) == -EAGAIN);
    CHECK(transport_event == NULL);
    CHECK(trevrpc_rpc_transport_drain(transport) == 0);
    trevrpc_rpc_transport_destroy(transport);
    transport = NULL;
    engine = NULL;
    CHECK(destroyed);

    provider = calloc(1, sizeof(*provider));
    CHECK(provider != NULL);
    provider->invalid_listener = true;
    engine = NULL;
    listener = (trevrpc_engine_handle_v1){TEST_OWNER, 77, 88};
    CHECK(trevrpc_engine_provider_create_v1(&config, &operations, provider, TEST_OWNER, &engine) == 0);
    CHECK(trevrpc_engine_listen_v1(engine, &endpoint, &listener) == -EIO);
    CHECK(listener.owner == TEST_OWNER && listener.slot == 77 && listener.generation == 88);
    CHECK(trevrpc_engine_release(engine) == 0);

    provider = calloc(1, sizeof(*provider));
    CHECK(provider != NULL);
    engine = NULL;
    receive = (trevrpc_engine_receive*)(uintptr_t)1;
    CHECK(trevrpc_engine_provider_create_v1(&config, &operations, provider, TEST_OWNER, &engine) == 0);
    CHECK(trevrpc_engine_stream_receive_frame(engine, stream, &receive) == -EIO);
    CHECK(receive == (trevrpc_engine_receive*)(uintptr_t)1);
    CHECK(trevrpc_engine_release(engine) == 0);

    provider = calloc(1, sizeof(*provider));
    CHECK(provider != NULL);
    engine = NULL;
    CHECK(trevrpc_engine_provider_create_v1(&config, &operations, provider, TEST_OWNER, &engine) == 0);
    CHECK(trevrpc_engine_provider_callback_enter(engine) == 0);
    CHECK(trevrpc_engine_drain(engine) == -EDEADLK);
    trevrpc_engine_provider_callback_leave(engine);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_STOPPED && info.status == 0);
    trevrpc_engine_event_release(event);
    event = NULL;
    trevrpc_engine_provider_fail(engine, -EIO, 1);
    trevrpc_engine_provider_stopped(engine, -EIO, 1);
    CHECK(trevrpc_engine_next_event(engine, &event) == -EAGAIN);
    CHECK(event == NULL);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    CHECK(diagnostics.terminal_status == 0 && diagnostics.provider_error_code == 0);
    CHECK(trevrpc_engine_release(engine) == 0);

    provider = calloc(1, sizeof(*provider));
    CHECK(provider != NULL);
    engine = NULL;
    listener = (trevrpc_engine_handle_v1){0};
    CHECK(trevrpc_engine_provider_create_v1(&config, &operations, provider, TEST_OWNER, &engine) == 0);
    CHECK(trevrpc_engine_internal_test_force_wake_failure(engine, TREVRPC_ENGINE_TEST_WAKE_FAILURE_WRITE, 0) == 0);
    CHECK(trevrpc_engine_listen_v1(engine, &endpoint, &listener) == 0);
    CHECK(listener.owner == TEST_OWNER && listener.slot == 1 && listener.generation == 1);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    CHECK(diagnostics.terminal_status == -EBADF);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}
