#define _POSIX_C_SOURCE 200809L
#define TREVRPC_ENGINE_MSQUIC_TESTING

#include "../src/trevrpc_engine_msquic.c"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <time.h>

#define TEST_STREAM_HANDLE ((HQUIC)(uintptr_t)1u)
#define TEST_LISTENER_HANDLE ((HQUIC)(uintptr_t)2u)
#define TEST_CONNECTION_HANDLE_BASE ((uintptr_t)16u)
#define TEST_PEER_STREAM_HANDLE_BASE ((uintptr_t)64u)

typedef struct fake_msquic_state {
    adapter_stream* stream;
    uint32_t enable_calls;
    uint32_t shutdown_calls;
    uint32_t close_calls;
    uint32_t connection_shutdown_calls;
    uint32_t connection_close_calls;
    uint32_t connection_configure_calls;
    uint32_t callback_handler_calls;
    uint32_t send_calls;
    adapter_send* send_contexts[32];
    adapter_stream* send_streams[32];
    HQUIC configured_handles[8];
    uint32_t configured_handle_count;
    uint32_t listener_stop_calls;
    uint32_t listener_close_calls;
    QUIC_STATUS enable_status;
    QUIC_STATUS configure_status;
    QUIC_STATUS send_status;
    QUIC_STATUS shutdown_status;
    bool close_during_enable;
    bool peer_abort_during_enable;
    bool deliver_shutdown_complete;
    bool shutdown_complete_delivered;
    bool lock_held_across_shutdown;
    msquic_provider* adapter;
    bool close_during_configuration;
    bool close_triggered;
} fake_msquic_state;

typedef struct receive_fixture {
    msquic_provider* adapter;
    adapter_stream* stream;
    adapter_listener* listener;
    trevrpc_engine* engine;
    trevrpc_engine_handle_v1 stream_handle;
} receive_fixture;

static fake_msquic_state FakeMsQuic;

static void QUIC_API fake_stream_close(HQUIC stream_handle) {
    (void)stream_handle;
    FakeMsQuic.close_calls++;
}

static QUIC_STATUS QUIC_API fake_stream_send(
    HQUIC stream_handle, const QUIC_BUFFER* buffers, uint32_t buffer_count, uint32_t flags, void* client_context) {
    (void)buffers;
    (void)buffer_count;
    (void)flags;
    if (FakeMsQuic.send_calls < 32) {
        FakeMsQuic.send_contexts[FakeMsQuic.send_calls] = client_context;
        FakeMsQuic.send_streams[FakeMsQuic.send_calls] = ((adapter_send*)client_context)->stream;
    }
    FakeMsQuic.send_calls++;
    (void)stream_handle;
    return FakeMsQuic.send_status;
}

static QUIC_STATUS QUIC_API fake_stream_shutdown(
    HQUIC stream_handle, QUIC_STREAM_SHUTDOWN_FLAGS flags, QUIC_UINT62 error_code) {
    (void)error_code;
    assert((flags & QUIC_STREAM_SHUTDOWN_FLAG_ABORT) != 0);
    FakeMsQuic.shutdown_calls++;
    if (pthread_mutex_trylock(&FakeMsQuic.stream->mutex) == 0) {
        pthread_mutex_unlock(&FakeMsQuic.stream->mutex);
    } else {
        FakeMsQuic.lock_held_across_shutdown = true;
    }
    if (QUIC_FAILED(FakeMsQuic.shutdown_status)) {
        return FakeMsQuic.shutdown_status;
    }
    if (stream_handle == TEST_STREAM_HANDLE && FakeMsQuic.deliver_shutdown_complete &&
        !FakeMsQuic.shutdown_complete_delivered) {
        FakeMsQuic.shutdown_complete_delivered = true;
        QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
        assert(adapter_stream_callback(stream_handle, FakeMsQuic.stream, &event) == QUIC_STATUS_SUCCESS);
    }
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API fake_stream_receive_set_enabled(HQUIC stream_handle, BOOLEAN enabled) {
    assert(stream_handle == TEST_STREAM_HANDLE);
    assert(enabled == TRUE);
    FakeMsQuic.enable_calls++;
    if (FakeMsQuic.close_during_enable) {
        atomic_store_explicit(&FakeMsQuic.stream->base.closing, true, memory_order_release);
    }
    if (FakeMsQuic.peer_abort_during_enable) {
        QUIC_STREAM_EVENT aborted = {.Type = QUIC_STREAM_EVENT_PEER_SEND_ABORTED};
        aborted.PEER_SEND_ABORTED.ErrorCode = 19;
        assert(adapter_stream_callback(stream_handle, FakeMsQuic.stream, &aborted) == QUIC_STATUS_SUCCESS);
    }
    return FakeMsQuic.enable_status;
}

static void QUIC_API fake_listener_stop(HQUIC handle) {
    (void)handle;
    FakeMsQuic.listener_stop_calls++;
}

static void QUIC_API fake_listener_close(HQUIC handle) {
    (void)handle;
    FakeMsQuic.listener_close_calls++;
}

static void QUIC_API fake_connection_close(HQUIC handle) {
    (void)handle;
    FakeMsQuic.connection_close_calls++;
}

static void QUIC_API fake_connection_shutdown(
    HQUIC handle, QUIC_CONNECTION_SHUTDOWN_FLAGS flags, QUIC_UINT62 error_code) {
    (void)handle;
    (void)flags;
    (void)error_code;
    FakeMsQuic.connection_shutdown_calls++;
}

static QUIC_STATUS QUIC_API fake_connection_set_configuration(HQUIC handle, HQUIC configuration) {
    (void)configuration;
    FakeMsQuic.connection_configure_calls++;
    if (FakeMsQuic.configured_handle_count < 8) {
        FakeMsQuic.configured_handles[FakeMsQuic.configured_handle_count++] = handle;
    }
    if (FakeMsQuic.close_during_configuration && !FakeMsQuic.close_triggered) {
        FakeMsQuic.close_triggered = true;
        assert(FakeMsQuic.adapter != NULL);
        assert(provider_close(FakeMsQuic.adapter) == 0);
    }
    return FakeMsQuic.configure_status;
}

static void QUIC_API fake_set_callback_handler(HQUIC handle, void* handler, void* context) {
    (void)handle;
    (void)handler;
    (void)context;
    FakeMsQuic.callback_handler_calls++;
}

static const QUIC_API_TABLE FakeApi = {
    .SetCallbackHandler = fake_set_callback_handler,
    .ListenerStop = fake_listener_stop,
    .ListenerClose = fake_listener_close,
    .ConnectionClose = fake_connection_close,
    .ConnectionShutdown = fake_connection_shutdown,
    .ConnectionSetConfiguration = fake_connection_set_configuration,
    .StreamClose = fake_stream_close,
    .StreamSend = fake_stream_send,
    .StreamShutdown = fake_stream_shutdown,
    .StreamReceiveSetEnabled = fake_stream_receive_set_enabled,
};

int trevrpc_msquic_api_owner_acquire(const QUIC_API_TABLE** out_api) {
    (void)out_api;
    return -ENOTSUP;
}

void trevrpc_msquic_api_owner_release(const QUIC_API_TABLE* api) {
    (void)api;
}

static receive_fixture fixture_create_with_capacities(
    uint64_t max_frame_size, uint32_t connection_capacity, uint32_t stream_capacity) {
    receive_fixture fixture = {0};
    trevrpc_engine_config_v1 config;
    assert(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 32;
    config.listener_capacity = 1;
    config.connection_capacity = connection_capacity;
    config.stream_capacity = stream_capacity;
    config.max_receive_owned_count = 1;
    config.max_receive_owned_bytes = 1024;

    fixture.adapter = calloc(1, sizeof(*fixture.adapter));
    assert(fixture.adapter != NULL);
    atomic_init(&fixture.adapter->readable_retry_count, 0);
    uint32_t slot_count = 1u + config.listener_capacity + config.connection_capacity + config.stream_capacity + 1u;
    fixture.adapter->slots = calloc(slot_count, sizeof(*fixture.adapter->slots));
    assert(fixture.adapter->slots != NULL);
    assert(pthread_mutex_init(&fixture.adapter->mutex, NULL) == 0);
    assert(pthread_mutex_init(&fixture.adapter->budget_mutex, NULL) == 0);
    fixture.adapter->api = &FakeApi;
    fixture.adapter->state = TREVRPC_ENGINE_STATE_RUNNING;
    fixture.adapter->owner = 1;
    fixture.adapter->slot_count = slot_count;
    fixture.adapter->listener_begin = 1;
    fixture.adapter->connection_begin = 1u + config.listener_capacity;
    fixture.adapter->stream_begin = fixture.adapter->connection_begin + config.connection_capacity;
    fixture.adapter->connection_capacity = connection_capacity;
    fixture.adapter->stream_capacity = stream_capacity;
    fixture.adapter->accept_admission_open = true;
    fixture.adapter->max_receive_owned_count = 1;
    fixture.adapter->max_receive_owned_bytes = 1024;
    assert(trevrpc_engine_provider_create_v1(
               &config, &MsQuicProviderOps, fixture.adapter, fixture.adapter->owner, &fixture.engine) == 0);

    adapter_endpoint* endpoint = calloc(1, sizeof(*endpoint));
    assert(endpoint != NULL);
    atomic_init(&endpoint->refs, 1);
    endpoint->api = &FakeApi;
    endpoint->max_frame_size = max_frame_size;
    endpoint->max_pending_send_count = 2;
    endpoint->max_pending_send_bytes = 1024;

    fixture.stream = calloc(1, sizeof(*fixture.stream));
    assert(fixture.stream != NULL);
    assert(pthread_mutex_init(&fixture.stream->mutex, NULL) == 0);
    assert(pthread_mutex_init(&fixture.stream->send_gate, NULL) == 0);
    fixture.stream->pending_operation_ids =
        calloc(endpoint->max_pending_send_count, sizeof(*fixture.stream->pending_operation_ids));
    assert(fixture.stream->pending_operation_ids != NULL);
    for (uint32_t index = 0; index < endpoint->max_pending_send_count; index++) {
        atomic_init(&fixture.stream->pending_operation_ids[index], 0);
    }
    atomic_init(&fixture.stream->base.closing, false);
    fixture.stream->base.endpoint = endpoint;
    fixture.stream->base.handle = TEST_STREAM_HANDLE;
    fixture.stream->base.ready = true;
    fixture.stream->ready_event_committed = true;
    trevrpc_frame_parser_init_with_allocator(
        &fixture.stream->parser, max_frame_size, adapter_receive_alloc, adapter_receive_free, fixture.stream);
    trevrpc_frame_parser_set_retain_on_allocation_failure(&fixture.stream->parser, true);
    assert(trevrpc_engine_provider_reserve_mandatory(fixture.engine, &fixture.stream->base.terminal_reservation) == 0);
    assert(trevrpc_engine_provider_reserve_mandatory(fixture.engine, &fixture.stream->receive_fin_reservation) == 0);
    assert(trevrpc_engine_provider_reserve_mandatory(fixture.engine, &fixture.stream->send_stopped_reservation) == 0);
    assert(registry_add(fixture.adapter, &fixture.stream->base, TREVRPC_ENGINE_OBJECT_STREAM, &fixture.stream_handle) ==
           0);

    memset(&FakeMsQuic, 0, sizeof(FakeMsQuic));
    FakeMsQuic.stream = fixture.stream;
    FakeMsQuic.enable_status = QUIC_STATUS_SUCCESS;
    FakeMsQuic.configure_status = QUIC_STATUS_SUCCESS;
    FakeMsQuic.send_status = QUIC_STATUS_SUCCESS;
    FakeMsQuic.shutdown_status = QUIC_STATUS_SUCCESS;
    return fixture;
}

static receive_fixture fixture_create(uint64_t max_frame_size) {
    return fixture_create_with_capacities(max_frame_size, 1, 1);
}

static bool stream_has_pending_operation_id(adapter_stream* stream, uint64_t operation_id) {
    bool found = false;
    pthread_mutex_lock(&stream->mutex);
    if (stream->pending_operation_ids != NULL) {
        for (uint32_t index = 0; index < stream->base.endpoint->max_pending_send_count; index++) {
            if (atomic_load_explicit(&stream->pending_operation_ids[index], memory_order_acquire) == operation_id) {
                found = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return found;
}

static void fixture_discard_events(receive_fixture* fixture) {
    for (;;) {
        trevrpc_engine_event* event = NULL;
        int result = trevrpc_engine_next_event(fixture->engine, &event);
        if (result == -EAGAIN || result == -EPIPE) {
            return;
        }
        assert(result == 0);
        trevrpc_engine_event_release(event);
    }
}

static void fixture_destroy_stopped(receive_fixture* fixture) {
    fixture_discard_events(fixture);
    assert(trevrpc_engine_close(fixture->engine) == 0);
    assert(trevrpc_engine_drain(fixture->engine) == 0);
    assert(trevrpc_engine_release(fixture->engine) == 0);
}

static void fixture_destroy(receive_fixture* fixture) {
    if (fixture->stream != NULL) {
        trevrpc_engine_reservation* terminal_reservation = fixture->stream->base.terminal_reservation;
        fixture->stream->base.terminal_reservation = NULL;
        trevrpc_engine_reservation* receive_fin_reservation = fixture->stream->receive_fin_reservation;
        fixture->stream->receive_fin_reservation = NULL;
        trevrpc_engine_reservation* send_stopped_reservation = fixture->stream->send_stopped_reservation;
        fixture->stream->send_stopped_reservation = NULL;
        trevrpc_engine_provider_cancel_reservation(fixture->engine, terminal_reservation);
        trevrpc_engine_provider_cancel_reservation(fixture->engine, receive_fin_reservation);
        trevrpc_engine_provider_cancel_reservation(fixture->engine, send_stopped_reservation);
    }
    if (fixture->listener != NULL) {
        QUIC_LISTENER_EVENT event = {.Type = QUIC_LISTENER_EVENT_STOP_COMPLETE};
        assert(adapter_listener_callback(TEST_LISTENER_HANDLE, fixture->listener, &event) == QUIC_STATUS_SUCCESS);
        fixture->listener = NULL;
    }
    fixture_discard_events(fixture);
    trevrpc_engine_diagnostics_v1 diagnostics;
    assert(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    assert(trevrpc_engine_get_diagnostics_v1(fixture->engine, &diagnostics) == 0);
    assert(diagnostics.mandatory_reservations == 0);
    assert(fixture->adapter->live_listeners == 0);
    assert(fixture->adapter->live_connections == 0);
    assert(fixture->adapter->live_streams == 1);
    assert(fixture->adapter->pending_connection_count == 0);
    assert(fixture->adapter->pending_stream_count == 0);
    pthread_mutex_lock(&fixture->adapter->mutex);
    fixture->adapter->state = TREVRPC_ENGINE_STATE_STOPPED;
    pthread_mutex_unlock(&fixture->adapter->mutex);
    assert(trevrpc_engine_close(fixture->engine) == 0);
    trevrpc_engine_provider_stopped(fixture->engine, fixture->adapter->terminal_status, 0);
    assert(trevrpc_engine_drain(fixture->engine) == 0);
    assert(trevrpc_engine_release(fixture->engine) == 0);
}

static adapter_listener* fixture_add_listener(receive_fixture* fixture) {
    adapter_listener* listener = calloc(1, sizeof(*listener));
    assert(listener != NULL);
    atomic_init(&listener->base.closing, false);
    listener->base.endpoint = fixture->stream->base.endpoint;
    endpoint_retain(listener->base.endpoint);
    listener->base.ready = true;
    listener->base.event_flags = TREVRPC_ENGINE_EVENT_FLAG_SERVER;
    assert(trevrpc_engine_provider_reserve_mandatory(fixture->engine, &listener->base.terminal_reservation) == 0);
    assert(registry_add(fixture->adapter, &listener->base, TREVRPC_ENGINE_OBJECT_LISTENER, &listener->base.token) == 0);
    assert(!object_publish_handle(&listener->base, TEST_LISTENER_HANDLE));
    fixture->listener = listener;
    return listener;
}

static adapter_connection* fixture_queued_connection(receive_fixture* fixture, uint32_t index) {
    adapter_pending_connection* node = fixture->adapter->pending_connection_head;
    while (node != NULL && index != 0) {
        node = node->next;
        index--;
    }
    assert(node != NULL);
    assert(node->connection != NULL);
    return node->connection;
}

static QUIC_STATUS fixture_new_connection(receive_fixture* fixture, adapter_listener* listener, uintptr_t handle) {
    (void)fixture;
    QUIC_LISTENER_EVENT event = {.Type = QUIC_LISTENER_EVENT_NEW_CONNECTION};
    event.NEW_CONNECTION.Connection = (HQUIC)handle;
    return adapter_listener_callback(TEST_LISTENER_HANDLE, listener, &event);
}

static QUIC_STATUS fixture_connection_event(adapter_connection* connection, QUIC_CONNECTION_EVENT_TYPE type) {
    QUIC_CONNECTION_EVENT event = {.Type = type};
    if (type == QUIC_CONNECTION_EVENT_CONNECTED) {
        static const uint8_t alpn[] = "trevrpc/1";
        event.CONNECTED.NegotiatedAlpn = alpn;
        event.CONNECTED.NegotiatedAlpnLength = sizeof(alpn) - 1;
    }
    return adapter_connection_callback(connection->base.handle, connection, &event);
}

static QUIC_STATUS fixture_peer_stream(adapter_connection* connection, uintptr_t handle) {
    QUIC_CONNECTION_EVENT event = {.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED};
    event.PEER_STREAM_STARTED.Stream = (HQUIC)handle;
    return adapter_connection_callback(connection->base.handle, connection, &event);
}

static QUIC_STATUS indicate_stream_receive(
    adapter_stream* stream, uint8_t* bytes, uint32_t len, QUIC_RECEIVE_FLAGS flags, uint64_t* accepted) {
    QUIC_BUFFER buffer = {.Length = len, .Buffer = bytes};
    QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_RECEIVE};
    event.RECEIVE.TotalBufferLength = len;
    event.RECEIVE.BufferCount = 1;
    event.RECEIVE.Buffers = &buffer;
    event.RECEIVE.Flags = flags;
    QUIC_STATUS status = adapter_stream_callback(stream->base.handle, stream, &event);
    *accepted = event.RECEIVE.TotalBufferLength;
    return status;
}

static QUIC_STATUS indicate_receive_flags(
    receive_fixture* fixture, uint8_t* bytes, uint32_t len, QUIC_RECEIVE_FLAGS flags, uint64_t* accepted) {
    return indicate_stream_receive(fixture->stream, bytes, len, flags, accepted);
}

static QUIC_STATUS indicate_receive(receive_fixture* fixture, uint8_t* bytes, uint32_t len, uint64_t* accepted) {
    return indicate_receive_flags(fixture, bytes, len, QUIC_RECEIVE_FLAG_NONE, accepted);
}

static trevrpc_engine_receive* pop_receive(receive_fixture* fixture) {
    trevrpc_engine_receive* receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(fixture->engine, fixture->stream_handle, &receive) == 0);
    assert(receive != NULL);
    return receive;
}

typedef struct stream_event_counts {
    uint32_t terminal;
    uint32_t receive_fin;
    uint32_t receive_fin_flags;
    int receive_fin_status;
    uint64_t receive_fin_application_error;
    uint32_t send_stopped;
    uint32_t send_stopped_flags;
    int send_stopped_status;
    uint64_t send_stopped_application_error;
    uint32_t order[8];
    uint32_t order_count;
} stream_event_counts;

static stream_event_counts drain_stream_events(receive_fixture* fixture) {
    stream_event_counts counts = {0};
    for (;;) {
        trevrpc_engine_event* event = NULL;
        int result = trevrpc_engine_next_event(fixture->engine, &event);
        if (result == -EAGAIN || result == -EPIPE) {
            return counts;
        }
        assert(result == 0);
        trevrpc_engine_event_info_v1 info;
        assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
        bool same_stream = info.subject.owner == fixture->stream_handle.owner &&
                           info.subject.slot == fixture->stream_handle.slot &&
                           info.subject.generation == fixture->stream_handle.generation;
        if (same_stream && counts.order_count < 8) {
            counts.order[counts.order_count++] = info.kind;
        }
        if (same_stream &&
            (info.kind == TREVRPC_ENGINE_EVENT_STREAM_CLOSED || info.kind == TREVRPC_ENGINE_EVENT_STREAM_FAILED)) {
            counts.terminal++;
        } else if (same_stream && info.kind == TREVRPC_ENGINE_EVENT_RECEIVE_FIN) {
            counts.receive_fin++;
            counts.receive_fin_flags = info.flags;
            counts.receive_fin_status = info.status;
            counts.receive_fin_application_error = info.application_error_code;
        } else if (same_stream && info.kind == TREVRPC_ENGINE_EVENT_SEND_STOPPED) {
            counts.send_stopped++;
            counts.send_stopped_flags = info.flags;
            counts.send_stopped_status = info.status;
            counts.send_stopped_application_error = info.application_error_code;
        }
        trevrpc_engine_event_release(event);
    }
}

typedef struct accept_event_counts {
    uint32_t connection_ready;
    uint32_t connection_failed;
    uint32_t connection_closed;
    uint32_t stream_ready;
    uint32_t stream_readable;
    uint32_t stream_failed;
    uint32_t stream_closed;
    uint32_t receive_fin;
    uint32_t order[16];
    trevrpc_engine_handle_v1 order_subjects[16];
    uint32_t order_count;
} accept_event_counts;

static accept_event_counts drain_accept_events(receive_fixture* fixture) {
    accept_event_counts counts = {0};
    for (;;) {
        trevrpc_engine_event* event = NULL;
        int result = trevrpc_engine_next_event(fixture->engine, &event);
        if (result == -EAGAIN || result == -EPIPE) {
            return counts;
        }
        assert(result == 0);
        trevrpc_engine_event_info_v1 info;
        assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
        if (counts.order_count < 16) {
            counts.order[counts.order_count] = info.kind;
            counts.order_subjects[counts.order_count] = info.subject;
            counts.order_count++;
        }
        switch (info.kind) {
        case TREVRPC_ENGINE_EVENT_CONNECTION_READY:
            counts.connection_ready++;
            break;
        case TREVRPC_ENGINE_EVENT_CONNECTION_FAILED:
            counts.connection_failed++;
            break;
        case TREVRPC_ENGINE_EVENT_CONNECTION_CLOSED:
            counts.connection_closed++;
            break;
        case TREVRPC_ENGINE_EVENT_STREAM_READY:
            counts.stream_ready++;
            break;
        case TREVRPC_ENGINE_EVENT_STREAM_READABLE:
            counts.stream_readable++;
            break;
        case TREVRPC_ENGINE_EVENT_STREAM_FAILED:
            counts.stream_failed++;
            break;
        case TREVRPC_ENGINE_EVENT_STREAM_CLOSED:
            counts.stream_closed++;
            break;
        case TREVRPC_ENGINE_EVENT_RECEIVE_FIN:
            counts.receive_fin++;
            break;
        default:
            break;
        }
        trevrpc_engine_event_release(event);
    }
}

static adapter_stream* fixture_extra_stream(receive_fixture* fixture, uintptr_t handle) {
    adapter_connection parent = {0};
    parent.base.adapter = fixture->adapter;
    parent.base.endpoint = fixture->stream->base.endpoint;
    int result = 0;
    trevrpc_engine_handle_v1 token = {0};
    adapter_stream* stream = stream_alloc(&parent, (HQUIC)handle, 0, 0, false, NULL, NULL, &token, &result);
    assert(stream != NULL);
    assert(result == 0);
    stream->base.ready = true;
    stream->ready_event_committed = true;
    return stream;
}

static void test_pending_send_round_robin_three_streams(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 3);
    fixture.adapter->max_pending_send_count = 6;
    fixture.adapter->max_pending_send_bytes = 6144;
    adapter_stream* streams[3] = {
        fixture.stream, fixture_extra_stream(&fixture, 10u), fixture_extra_stream(&fixture, 11u)};
    uint8_t body = 7;
    for (uint32_t round = 0; round < 2; round++) {
        for (uint32_t index = 0; index < 3; index++) {
            assert(trevrpc_engine_stream_send_frame_v1(
                       fixture.engine, streams[index]->base.token, round * 3u + index + 1u, &body, 1) == 0);
        }
    }
    for (uint32_t attempt = 0; attempt < 100 && FakeMsQuic.send_calls < 6; attempt++) {
        adapter_scheduler_drain(fixture.adapter);
        struct timespec delay = {.tv_nsec = 1000000};
        nanosleep(&delay, NULL);
    }
    assert(FakeMsQuic.send_calls == 6);
    for (uint32_t index = 0; index < 6; index++) {
        assert(FakeMsQuic.send_streams[index] == streams[index % 3]);
    }
    for (uint32_t index = 0; index < 6; index++) {
        QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SEND_COMPLETE};
        event.SEND_COMPLETE.ClientContext = FakeMsQuic.send_contexts[index];
        assert(adapter_stream_callback(FakeMsQuic.send_streams[index]->base.handle,
                   FakeMsQuic.send_streams[index],
                   &event) == QUIC_STATUS_SUCCESS);
    }
    assert(fixture.adapter->pending_send_count == 0);
    assert(fixture.adapter->pending_send_bytes == 0);
    for (uint32_t index = 1; index < 3; index++) {
        QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
        assert(adapter_stream_callback(streams[index]->base.handle, streams[index], &event) == QUIC_STATUS_SUCCESS);
    }
    fixture_discard_events(&fixture);
    fixture_destroy(&fixture);
}

static void test_synchronous_send_failure_releases_budget(void) {
    receive_fixture fixture = fixture_create(1024);
    FakeMsQuic.send_status = QUIC_STATUS_ABORTED;
    uint8_t body = 7;
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, fixture.stream_handle, 1, &body, 1) == 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.send_calls == 1);
    assert(fixture.adapter->pending_send_count == 0);
    assert(fixture.adapter->pending_send_bytes == 0);
    assert(fixture.stream->pending_send_count == 0);
    assert(fixture.stream->pending_send_bytes == 0);

    trevrpc_engine_event* event = NULL;
    assert(trevrpc_engine_next_event(fixture.engine, &event) == 0);
    trevrpc_engine_event_info_v1 info;
    assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE);
    assert(info.operation_id == 1);
    assert(info.status == -EIO);
    trevrpc_engine_event_release(event);
    assert(trevrpc_engine_next_event(fixture.engine, &event) == -EAGAIN);
    fixture_destroy(&fixture);
}

static void test_send_operation_id_reuse_waits_for_completion_delivery(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t body = 7;
    const uint64_t operation_id = 23;
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, fixture.stream_handle, operation_id, &body, 1) == 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.send_calls == 1);

    assert(stream_has_pending_operation_id(fixture.stream, operation_id));
    QUIC_STREAM_EVENT completion = {.Type = QUIC_STREAM_EVENT_SEND_COMPLETE};
    completion.SEND_COMPLETE.ClientContext = FakeMsQuic.send_contexts[0];
    assert(adapter_stream_callback(fixture.stream->base.handle, fixture.stream, &completion) == QUIC_STATUS_SUCCESS);
    assert(!stream_has_pending_operation_id(fixture.stream, operation_id));

    trevrpc_engine_event* event = NULL;
    assert(trevrpc_engine_next_event(fixture.engine, &event) == 0);
    trevrpc_engine_event_info_v1 info;
    assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_ENGINE_EVENT_SEND_COMPLETE);
    assert(info.operation_id == operation_id);
    assert(!stream_has_pending_operation_id(fixture.stream, operation_id));
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, fixture.stream_handle, operation_id, &body, 1) == 0);
    trevrpc_engine_event_release(event);

    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.send_calls == 2);
    completion.SEND_COMPLETE.ClientContext = FakeMsQuic.send_contexts[1];
    assert(adapter_stream_callback(fixture.stream->base.handle, fixture.stream, &completion) == QUIC_STATUS_SUCCESS);
    assert(trevrpc_engine_next_event(fixture.engine, &event) == 0);
    trevrpc_engine_event_release(event);
    assert(!stream_has_pending_operation_id(fixture.stream, operation_id));
    fixture_destroy(&fixture);
}

static void test_send_completion_wake_failure_does_not_hold_send_gate(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t body = 7;
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, fixture.stream_handle, 17, &body, 1) == 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.send_calls == 1);
    assert(trevrpc_engine_internal_test_force_wake_failure(fixture.engine, 2u, 0) == 0);

    QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SEND_COMPLETE};
    event.SEND_COMPLETE.ClientContext = FakeMsQuic.send_contexts[0];
    assert(adapter_stream_callback(fixture.stream->base.handle, fixture.stream, &event) == QUIC_STATUS_SUCCESS);
    assert(FakeMsQuic.shutdown_calls == 1);
    assert(fixture.adapter->pending_send_count == 0);
    assert(fixture.adapter->pending_send_bytes == 0);
    assert(fixture.stream->pending_send_count == 0);
    assert(fixture.stream->pending_send_bytes == 0);
    assert(!stream_has_pending_operation_id(fixture.stream, 17));

    QUIC_STREAM_EVENT shutdown = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
    assert(adapter_stream_callback(fixture.stream->base.handle, fixture.stream, &shutdown) == QUIC_STATUS_SUCCESS);
    fixture.stream = NULL;
    fixture_destroy_stopped(&fixture);
}

static void test_cancel_unsent_unlinks_scheduler_stream(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_stream* stream = fixture_extra_stream(&fixture, 12u);
    uint8_t body = 7;
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, stream->base.token, 1, &body, 1) == 0);
    assert(fixture.adapter->send_ready_head == stream);
    assert(stream->send_scheduler_queued);

    QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
    assert(adapter_stream_callback(stream->base.handle, stream, &event) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->send_ready_head == NULL);
    assert(fixture.adapter->send_ready_tail == NULL);
    assert(!stream->send_scheduler_queued);
    assert(fixture.adapter->pending_send_count == 0);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_streams == 1);
    adapter_scheduler_drain(fixture.adapter);
    fixture_destroy(&fixture);
}

static void test_cancel_unsent_restores_pending_tail(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_stream* stream = fixture_extra_stream(&fixture, 13u);
    uint8_t body = 7;
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, stream->base.token, 1, &body, 1) == 0);
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, stream->base.token, 2, &body, 1) == 0);
    adapter_send* first = stream->pending_sends;
    assert(first != NULL && first->next != NULL);
    first->submitted = true;

    cancel_unsent_sends(stream, -ECANCELED);
    assert(stream->pending_sends == first);
    assert(stream->pending_send_tail == first);
    assert(first->next == NULL);
    assert(stream->pending_send_count == 1);
    first->submitted = false;
    cancel_unsent_sends(stream, -ECANCELED);
    assert(stream->pending_sends == NULL);
    assert(stream->pending_send_tail == NULL);
    assert(stream->pending_send_count == 0);

    QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
    assert(adapter_stream_callback(stream->base.handle, stream, &event) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_send_reservation_release_finishes_shutdown(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_stream* stream = fixture_extra_stream(&fixture, 14u);
    assert(reserve_send_admission(stream, 1, 4) == 0);
    atomic_store_explicit(&stream->base.closing, true, memory_order_release);
    pthread_mutex_lock(&fixture.adapter->mutex);
    stream->base.shutdown_complete = true;
    pthread_mutex_unlock(&fixture.adapter->mutex);
    release_send_admission(stream, 1, 4);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_shutdown_after_send_admission_rejects_and_reclaims(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_stream* stream = fixture_extra_stream(&fixture, 15u);
    uint8_t body = 7;
    AdapterTestShutdownAfterSendAdmission = true;
    assert(trevrpc_engine_stream_send_frame_v1(fixture.engine, stream->base.token, 1, &body, 1) == -EPIPE);
    assert(stream->base.shutdown_complete);
    assert(stream->base.terminal_published);
    assert(stream->pending_send_count == 0);
    assert(stream->pending_send_bytes == 0);
    assert(fixture.adapter->pending_send_count == 0);
    assert(fixture.adapter->pending_send_bytes == 0);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_clean_shutdown_waits_for_receive_fin(void) {
    receive_fixture fixture = fixture_create(1024);
    fixture.stream->send_finished = true;
    trevrpc_engine_reservation* receive_fin_reservation = fixture.stream->receive_fin_reservation;
    assert(receive_fin_reservation != NULL);

    QUIC_STREAM_EVENT shutdown = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
    assert(adapter_stream_callback(TEST_STREAM_HANDLE, fixture.stream, &shutdown) == QUIC_STATUS_SUCCESS);
    assert(fixture.stream->base.shutdown_complete);
    assert(!fixture.stream->base.terminal_published);
    assert(!fixture.stream->receive_fin_published);
    assert(fixture.stream->receive_fin_reservation == receive_fin_reservation);
    trevrpc_engine_event* event = NULL;
    assert(trevrpc_engine_next_event(fixture.engine, &event) == -EAGAIN);

    QUIC_STREAM_EVENT peer_fin = {.Type = QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN};
    assert(adapter_stream_callback(TEST_STREAM_HANDLE, fixture.stream, &peer_fin) == QUIC_STATUS_SUCCESS);
    assert(fixture.stream->receive_fin_published);
    assert(fixture.stream->receive_fin_reservation == NULL);
    assert(fixture.stream->base.terminal_published);

    stream_event_counts counts = drain_stream_events(&fixture);
    assert(counts.receive_fin == 1);
    assert(counts.terminal == 1);
    assert(counts.order_count == 2);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_RECEIVE_FIN);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_STREAM_CLOSED);

    trevrpc_engine_diagnostics_v1 diagnostics;
    assert(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    assert(trevrpc_engine_get_diagnostics_v1(fixture.engine, &diagnostics) == 0);
    assert(diagnostics.mandatory_reservations == 0);
    fixture.stream = NULL;
    fixture_destroy_stopped(&fixture);
}

static void test_partial_zero_and_resume(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frames[8] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frames, sizeof(frames), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    assert(fixture.stream->receive_paused);
    assert(FakeMsQuic.shutdown_calls == 0);

    assert(indicate_receive(&fixture, frames + 4, 4, &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 0);
    assert(FakeMsQuic.shutdown_calls == 0);

    trevrpc_engine_receive* first = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 1);
    assert(!fixture.stream->receive_paused);
    assert(indicate_receive(&fixture, frames + 4, 4, &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    trevrpc_engine_receive* second = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 1);
    trevrpc_engine_receive_release(second);
    trevrpc_engine_receive_release(first);
    fixture_destroy(&fixture);
}

static void test_receive_create_failure_preserves_queue(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frame[] = {0, 0, 0, 1, 42};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frame, sizeof(frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == sizeof(frame));

    trevrpc_engine_event* readable = NULL;
    assert(trevrpc_engine_next_event(fixture.engine, &readable) == 0);
    trevrpc_engine_event_info_v1 event_info;
    assert(trevrpc_engine_event_info_v1_init(&event_info, sizeof(event_info)) == 0);
    assert(trevrpc_engine_event_get_info_v1(readable, &event_info) == 0);
    assert(event_info.kind == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    trevrpc_engine_event_release(readable);
    assert(!fixture.stream->readable_pending);

    AdapterTestFailNextReceiveCreate = true;
    trevrpc_engine_receive* receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(fixture.engine, fixture.stream_handle, &receive) == -ENOMEM);
    assert(receive == NULL);
    assert(fixture.stream->receive_head != NULL);
    assert(fixture.adapter->receive_owned_count == 1);
    assert(fixture.adapter->receive_owned_bytes == 1);
    assert(fixture.stream->readable_pending);

    readable = NULL;
    assert(trevrpc_engine_next_event(fixture.engine, &readable) == 0);
    assert(trevrpc_engine_event_info_v1_init(&event_info, sizeof(event_info)) == 0);
    assert(trevrpc_engine_event_get_info_v1(readable, &event_info) == 0);
    assert(event_info.kind == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    trevrpc_engine_event_release(readable);

    receive = pop_receive(&fixture);
    trevrpc_engine_receive_info_v1 receive_info;
    assert(trevrpc_engine_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_engine_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.data_len == 1);
    assert(receive_info.data[0] == 42);
    assert(fixture.adapter->receive_owned_count == 0);
    assert(fixture.adapter->receive_owned_bytes == 0);
    trevrpc_engine_receive_release(receive);
    fixture_destroy(&fixture);
}

static void test_readable_publication_failure_retries(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frame[] = {0, 0, 0, 1, 42};
    uint64_t accepted = UINT64_MAX;
    AdapterTestFailNextReadablePublication = true;
    assert(indicate_receive(&fixture, frame, sizeof(frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == sizeof(frame));
    assert(fixture.stream->receive_head != NULL);
    assert(fixture.adapter->receive_owned_count == 1);
    assert(fixture.adapter->receive_owned_bytes == 1);
    assert(fixture.stream->readable_pending);
    assert(fixture.stream->readable_retry_pending);
    assert(fixture.stream->readable_published_epoch == 0);
    assert(atomic_load_explicit(&fixture.adapter->readable_retry_count, memory_order_relaxed) == 1);

    trevrpc_engine_event* readable = NULL;
    assert(trevrpc_engine_next_event(fixture.engine, &readable) == -EAGAIN);
    adapter_scheduler_drain(fixture.adapter);
    assert(!fixture.stream->readable_retry_pending);
    assert(fixture.stream->readable_published_epoch == fixture.stream->readable_epoch);
    assert(atomic_load_explicit(&fixture.adapter->readable_retry_count, memory_order_relaxed) == 0);

    assert(trevrpc_engine_next_event(fixture.engine, &readable) == 0);
    trevrpc_engine_event_info_v1 event_info;
    assert(trevrpc_engine_event_info_v1_init(&event_info, sizeof(event_info)) == 0);
    assert(trevrpc_engine_event_get_info_v1(readable, &event_info) == 0);
    assert(event_info.kind == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    trevrpc_engine_event_release(readable);

    trevrpc_engine_receive* receive = pop_receive(&fixture);
    trevrpc_engine_receive_info_v1 receive_info;
    assert(trevrpc_engine_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_engine_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.data_len == 1);
    assert(receive_info.data[0] == 42);
    assert(fixture.adapter->receive_owned_count == 0);
    assert(fixture.adapter->receive_owned_bytes == 0);
    trevrpc_engine_receive_release(receive);
    assert(fixture.adapter->terminal_status == 0);
    fixture_destroy(&fixture);
}

static void test_exact_header_boundary_is_backpressure(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t zero_frame[4] = {0};
    uint8_t body_header[4] = {0, 0, 0, 1};
    uint8_t body = 42;
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, zero_frame, sizeof(zero_frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    assert(indicate_receive(&fixture, body_header, sizeof(body_header), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    assert(!fixture.stream->receive_paused);
    assert(fixture.adapter->terminal_status == 0);
    assert(FakeMsQuic.shutdown_calls == 0);

    trevrpc_engine_receive* first = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 0);
    assert(indicate_receive(&fixture, &body, sizeof(body), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 1);
    trevrpc_engine_receive* second = pop_receive(&fixture);
    trevrpc_engine_receive_info_v1 info;
    assert(trevrpc_engine_receive_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_engine_receive_get_info_v1(second, &info) == 0);
    assert(info.data_len == 1);
    assert(info.data[0] == body);
    trevrpc_engine_receive_release(second);
    trevrpc_engine_receive_release(first);
    fixture_destroy(&fixture);
}

static void test_exact_header_boundary_fin_is_truncation(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t zero_frame[4] = {0};
    uint8_t body_header[4] = {0, 0, 0, 1};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, zero_frame, sizeof(zero_frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    FakeMsQuic.deliver_shutdown_complete = true;
    assert(indicate_receive_flags(&fixture, body_header, sizeof(body_header), QUIC_RECEIVE_FLAG_FIN, &accepted) ==
           QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    assert(fixture.adapter->terminal_status == 0);
    assert(fixture.stream->receive_fin_published);
    assert(!fixture.stream->receive_paused);
    assert(FakeMsQuic.shutdown_calls >= 1);
    assert(!FakeMsQuic.lock_held_across_shutdown);
    assert(FakeMsQuic.shutdown_complete_delivered);
    assert(fixture.stream->base.terminal_published);
    assert(FakeMsQuic.close_calls == 1);

    trevrpc_engine_receive* receive = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 0);
    trevrpc_engine_receive_release(receive);
    stream_event_counts counts = drain_stream_events(&fixture);
    assert(counts.receive_fin == 1);
    assert(counts.receive_fin_status == -EPROTO);
    assert(counts.terminal == 1);
    fixture_destroy_stopped(&fixture);
}

static void test_peer_send_abort_retires_pause(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frames[8] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frames, sizeof(frames), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    assert(fixture.stream->receive_paused);

    QUIC_STREAM_EVENT aborted = {.Type = QUIC_STREAM_EVENT_PEER_SEND_ABORTED};
    aborted.PEER_SEND_ABORTED.ErrorCode = 17;
    assert(adapter_stream_callback(TEST_STREAM_HANDLE, fixture.stream, &aborted) == QUIC_STATUS_SUCCESS);
    assert(fixture.stream->receive_fin_published);
    assert(!fixture.stream->receive_paused);
    FakeMsQuic.enable_status = QUIC_STATUS_INVALID_STATE;
    trevrpc_engine_receive* receive = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 0);
    assert(fixture.adapter->terminal_status == 0);
    trevrpc_engine_receive_release(receive);
    stream_event_counts counts = drain_stream_events(&fixture);
    assert(counts.receive_fin == 1);
    assert(counts.receive_fin_status == -ECANCELED);
    assert((counts.receive_fin_flags & TREVRPC_ENGINE_EVENT_FLAG_PEER_RESET) != 0);
    assert((counts.receive_fin_flags & TREVRPC_ENGINE_EVENT_FLAG_CLEAN_FIN) == 0);
    assert(counts.receive_fin_application_error == 17);
    fixture_destroy(&fixture);
}

static void test_peer_receive_abort_publishes_send_stopped_once(void) {
    receive_fixture fixture = fixture_create(1024);
    QUIC_STREAM_EVENT aborted = {.Type = QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED};
    aborted.PEER_RECEIVE_ABORTED.ErrorCode = 23;

    assert(adapter_stream_callback(TEST_STREAM_HANDLE, fixture.stream, &aborted) == QUIC_STATUS_SUCCESS);
    assert(adapter_stream_callback(TEST_STREAM_HANDLE, fixture.stream, &aborted) == QUIC_STATUS_SUCCESS);
    assert(fixture.stream->send_aborted);
    assert(fixture.stream->send_stopped_published);

    stream_event_counts counts = drain_stream_events(&fixture);
    assert(counts.send_stopped == 1);
    assert(counts.send_stopped_status == -ECANCELED);
    assert((counts.send_stopped_flags & TREVRPC_ENGINE_EVENT_FLAG_TERMINAL) != 0);
    assert((counts.send_stopped_flags & TREVRPC_ENGINE_EVENT_FLAG_PEER) != 0);
    assert((counts.send_stopped_flags & TREVRPC_ENGINE_EVENT_FLAG_LOCAL) == 0);
    assert((counts.send_stopped_flags & TREVRPC_ENGINE_EVENT_FLAG_PEER_RESET) != 0);
    assert(counts.send_stopped_application_error == 23);
    fixture_destroy(&fixture);
}

static void test_receive_abort_failure_preserves_retryable_state(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frame[4] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frame, sizeof(frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == sizeof(frame));

    FakeMsQuic.shutdown_status = QUIC_STATUS_INVALID_STATE;
    assert(trevrpc_engine_stream_abort_receive(fixture.engine, fixture.stream_handle, 7) == -EIO);
    assert(!fixture.stream->receive_aborted);
    trevrpc_engine_receive* receive = pop_receive(&fixture);
    trevrpc_engine_receive_release(receive);

    FakeMsQuic.shutdown_status = QUIC_STATUS_SUCCESS;
    assert(trevrpc_engine_stream_abort_receive(fixture.engine, fixture.stream_handle, 7) == 0);
    assert(fixture.stream->receive_aborted);
    assert(FakeMsQuic.shutdown_calls == 2);
    fixture_destroy(&fixture);
}

static void test_local_receive_abort_suppresses_late_peer_fin(void) {
    receive_fixture fixture = fixture_create(1024);
    assert(trevrpc_engine_stream_abort_receive(fixture.engine, fixture.stream_handle, 9) == 0);
    assert(fixture.stream->receive_aborted);

    QUIC_STREAM_EVENT aborted = {.Type = QUIC_STREAM_EVENT_PEER_SEND_ABORTED};
    aborted.PEER_SEND_ABORTED.ErrorCode = 17;
    assert(adapter_stream_callback(TEST_STREAM_HANDLE, fixture.stream, &aborted) == QUIC_STATUS_SUCCESS);
    assert(!fixture.stream->receive_fin_published);
    stream_event_counts counts = drain_stream_events(&fixture);
    assert(counts.receive_fin == 0);
    fixture_destroy(&fixture);
}

static void test_peer_abort_resume_race_is_nonfatal(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frames[8] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frames, sizeof(frames), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    assert(fixture.stream->receive_paused);

    FakeMsQuic.peer_abort_during_enable = true;
    FakeMsQuic.enable_status = QUIC_STATUS_INVALID_STATE;
    trevrpc_engine_receive* receive = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 1);
    assert(fixture.stream->receive_fin_published);
    assert(!fixture.stream->receive_paused);
    assert(fixture.adapter->terminal_status == 0);
    trevrpc_engine_receive_release(receive);
    fixture_destroy(&fixture);
}

static void test_callback_entry_failure_aborts_explicitly(void) {
    receive_fixture fixture = fixture_create(1024);
    trevrpc_engine_provider_cancel_reservation(fixture.engine, fixture.stream->base.terminal_reservation);
    fixture.stream->base.terminal_reservation = NULL;
    trevrpc_engine_provider_cancel_reservation(fixture.engine, fixture.stream->receive_fin_reservation);
    fixture.stream->receive_fin_reservation = NULL;
    trevrpc_engine_provider_cancel_reservation(fixture.engine, fixture.stream->send_stopped_reservation);
    fixture.stream->send_stopped_reservation = NULL;
    trevrpc_engine_provider_stopped(fixture.engine, 0, 0);
    uint8_t frame[4] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frame, sizeof(frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == sizeof(frame));
    assert(FakeMsQuic.shutdown_calls == 1);
    assert(!FakeMsQuic.lock_held_across_shutdown);
    assert(atomic_load_explicit(&fixture.stream->base.closing, memory_order_acquire));
    fixture_destroy(&fixture);
}

static void test_closing_stream_is_not_resumed(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frames[8] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frames, sizeof(frames), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    atomic_store_explicit(&fixture.stream->base.closing, true, memory_order_release);
    trevrpc_engine_receive* receive = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 0);
    trevrpc_engine_receive_release(receive);
    fixture_destroy(&fixture);
}

static void test_resume_close_race_is_nonfatal(void) {
    receive_fixture fixture = fixture_create(1024);
    uint8_t frames[8] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_receive(&fixture, frames, sizeof(frames), &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == 4);
    FakeMsQuic.close_during_enable = true;
    FakeMsQuic.enable_status = QUIC_STATUS_INVALID_STATE;
    trevrpc_engine_receive* receive = pop_receive(&fixture);
    assert(FakeMsQuic.enable_calls == 1);
    assert(fixture.adapter->terminal_status == 0);
    trevrpc_engine_receive_release(receive);
    fixture_destroy(&fixture);
}

static void test_connection_accept_queue_is_bounded(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 1);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE) == QUIC_STATUS_SUCCESS);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 1u) == QUIC_STATUS_ABORTED);
    assert(fixture.adapter->pending_connection_count == 1);
    assert(fixture.adapter->live_connections == 1);
    assert(fixture.adapter->terminal_status == 0);

    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.connection_failed == 1);
    assert(counts.connection_ready == 0);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->pending_connection_count == 0);
    assert(fixture.adapter->terminal_status == 0);
    fixture_destroy(&fixture);
}

static void test_connection_setup_failure_is_terminal_without_engine_failure(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 1);
    adapter_listener* listener = fixture_add_listener(&fixture);
    FakeMsQuic.configure_status = QUIC_STATUS_INTERNAL_ERROR;
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 2u) == QUIC_STATUS_ABORTED);
    adapter_connection* connection =
        (adapter_connection*)fixture.adapter->slots[fixture.adapter->connection_begin].object;
    assert(connection != NULL);
    assert(FakeMsQuic.connection_configure_calls == 1);
    assert(FakeMsQuic.connection_shutdown_calls == 1);
    assert(fixture.adapter->pending_connection_count == 0);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.connection_failed == 1);
    assert(counts.connection_closed == 0);
    assert(fixture.adapter->terminal_status == 0);
    assert(fixture.adapter->live_connections == 0);
    fixture_destroy(&fixture);
}

static void test_connection_is_configured_before_callback_returns(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 1);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 6u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    assert(FakeMsQuic.connection_configure_calls == 1);
    assert(FakeMsQuic.configured_handles[0] == connection->base.handle);
    assert(connection->configuration_set);
    assert(fixture.adapter->connection_handshakes_in_flight == 1);
    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.connection_configure_calls == 1);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->connection_handshakes_in_flight == 0);
    assert(fixture.adapter->pending_connection_count == 0);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->live_connections == 0);
    fixture_destroy(&fixture);
}

static void test_close_during_configuration_defers_native_shutdown(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 1);
    adapter_listener* listener = fixture_add_listener(&fixture);
    FakeMsQuic.adapter = fixture.adapter;
    FakeMsQuic.close_during_configuration = true;
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 7u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection =
        (adapter_connection*)fixture.adapter->slots[fixture.adapter->connection_begin].object;
    assert(connection != NULL);
    assert(FakeMsQuic.close_triggered);
    assert(FakeMsQuic.connection_configure_calls == 1);
    assert(connection->configuration_set);
    assert(FakeMsQuic.connection_shutdown_calls == 1);
    assert(fixture.adapter->state == TREVRPC_ENGINE_STATE_STOPPING);
    assert(fixture.adapter->connection_handshakes_in_flight == 0);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->connection_handshakes_in_flight == 0);
    assert(fixture.adapter->pending_connection_count == 0);
    assert(adapter_listener_callback(TEST_LISTENER_HANDLE,
               listener,
               &(QUIC_LISTENER_EVENT){.Type = QUIC_LISTENER_EVENT_STOP_COMPLETE}) == QUIC_STATUS_SUCCESS);
    fixture.listener = NULL;
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.connection_ready == 0);
    assert(counts.connection_failed + counts.connection_closed == 1);
    assert(fixture.adapter->live_connections == 0);
    fixture_destroy(&fixture);
}

static void test_connection_promotion_is_fifo(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 2, 1);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 3u) == QUIC_STATUS_SUCCESS);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 4u) == QUIC_STATUS_SUCCESS);
    adapter_connection* first = fixture_queued_connection(&fixture, 0);
    adapter_connection* second = fixture_queued_connection(&fixture, 1);
    assert(FakeMsQuic.connection_configure_calls == 2);
    assert(FakeMsQuic.configured_handles[0] == first->base.handle);
    assert(FakeMsQuic.configured_handles[1] == second->base.handle);
    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.connection_configure_calls == 2);
    assert(fixture.adapter->pending_connection_count == 2);
    assert(fixture_connection_event(first, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    adapter_scheduler_drain(fixture.adapter);
    assert(FakeMsQuic.connection_configure_calls == 2);
    assert(fixture.adapter->pending_connection_count == 1);
    assert(fixture_connection_event(second, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.connection_ready == 2);
    assert(counts.connection_failed == 0);
    assert(counts.order_count == 2);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_CONNECTION_READY);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_CONNECTION_READY);
    assert(counts.order_subjects[0].slot == first->base.token.slot);
    assert(counts.order_subjects[1].slot == second->base.token.slot);
    assert(fixture.adapter->terminal_status == 0);
    assert(fixture_connection_event(first, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(second, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->live_connections == 0);
    fixture_destroy(&fixture);
}

static void test_fatal_receive_error_aborts_after_unlock(void) {
    receive_fixture fixture = fixture_create(1);
    uint8_t oversized_frame[] = {0, 0, 0, 2, 1, 2};
    uint64_t accepted = UINT64_MAX;
    FakeMsQuic.deliver_shutdown_complete = true;
    assert(indicate_receive(&fixture, oversized_frame, sizeof(oversized_frame), &accepted) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->terminal_status == -EMSGSIZE);
    assert(FakeMsQuic.shutdown_calls >= 1);
    assert(!FakeMsQuic.lock_held_across_shutdown);
    assert(FakeMsQuic.shutdown_complete_delivered);
    assert(fixture.stream->base.terminal_published);
    assert(FakeMsQuic.close_calls == 1);

    stream_event_counts counts = drain_stream_events(&fixture);
    assert(counts.terminal == 1);
    fixture_destroy_stopped(&fixture);
}

static void test_peer_readable_waits_for_ready_commit(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 5u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);

    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE) == QUIC_STATUS_SUCCESS);
    adapter_stream* stream = connection->pending_peer_stream_head->stream;
    AdapterTestReceiveDuringReadyPublication = true;
    adapter_scheduler_drain(fixture.adapter);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.stream_ready == 1);
    assert(counts.stream_readable == 1);
    assert(counts.order_count == 2);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_STREAM_READY);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_STREAM_READABLE);

    trevrpc_engine_receive* receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(fixture.engine, stream->base.token, &receive) == 0);
    trevrpc_engine_receive_release(receive);
    assert(adapter_stream_callback(stream->base.handle,
               stream,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_peer_receive_fin_waits_for_ready_commit(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 6u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);

    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 1u) == QUIC_STATUS_SUCCESS);
    adapter_stream* stream = connection->pending_peer_stream_head->stream;
    AdapterTestReceiveDuringReadyPublication = true;
    AdapterTestReceiveFINDuringReadyPublication = true;
    adapter_scheduler_drain(fixture.adapter);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.stream_ready == 1);
    assert(counts.stream_readable == 1);
    assert(counts.order_count == 3);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_STREAM_READY);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    assert(counts.order[2] == TREVRPC_ENGINE_EVENT_RECEIVE_FIN);

    trevrpc_engine_receive* receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(fixture.engine, stream->base.token, &receive) == 0);
    trevrpc_engine_receive_release(receive);
    assert(adapter_stream_callback(stream->base.handle,
               stream,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_peer_readiness_flush_failure_preserves_fin(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 8u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);

    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 6u) == QUIC_STATUS_SUCCESS);
    adapter_stream* stream = connection->pending_peer_stream_head->stream;
    AdapterTestReceiveDuringReadyPublication = true;
    AdapterTestReceiveFINAfterReadyCommit = true;
    AdapterTestFailNextReadablePublication = true;
    AdapterTestSkipNextReadableRetry = true;
    adapter_scheduler_drain(fixture.adapter);

    accept_event_counts counts = drain_accept_events(&fixture);
    assert(!AdapterTestFailNextReadablePublication);
    assert(counts.stream_ready == 1);
    assert(counts.stream_readable == 0);
    assert(counts.receive_fin == 0);
    assert(counts.order_count == 1);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_STREAM_READY);
    assert(stream->readable_pending);
    assert(stream->receive_fin_pending);
    assert(stream->readiness_flush_in_progress);

    adapter_scheduler_drain(fixture.adapter);
    counts = drain_accept_events(&fixture);
    assert(counts.stream_readable == 1);
    assert(counts.receive_fin == 1);
    assert(counts.order_count == 2);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_RECEIVE_FIN);
    assert(stream->receive_fin_published);
    assert(!stream->readiness_flush_in_progress);

    trevrpc_engine_receive* receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(fixture.engine, stream->base.token, &receive) == 0);
    trevrpc_engine_receive_release(receive);
    assert(adapter_stream_callback(stream->base.handle,
               stream,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_peer_stream_waits_for_native_handle_publication(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 7u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);

    AdapterTestPeerStreamStayedQueued = false;
    AdapterTestPromoteBeforePeerHandlePublication = true;
    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 5u) == QUIC_STATUS_SUCCESS);
    assert(AdapterTestPeerStreamStayedQueued);
    assert(connection->pending_peer_stream_count == 1);
    adapter_stream* stream = connection->pending_peer_stream_head->stream;
    assert(stream->base.handle == (HQUIC)(TEST_PEER_STREAM_HANDLE_BASE + 5u));
    assert(!stream->base.ready);
    assert(!stream->base.terminal_published);

    uint8_t frame[4] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(indicate_stream_receive(stream, frame, sizeof(frame), QUIC_RECEIVE_FLAG_NONE, &accepted) ==
           QUIC_STATUS_SUCCESS);
    assert(accepted == sizeof(frame));
    accept_event_counts before_promotion = drain_accept_events(&fixture);
    assert(before_promotion.order_count == 0);

    adapter_scheduler_drain(fixture.adapter);
    assert(connection->pending_peer_stream_count == 0);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.stream_ready == 1);
    assert(counts.stream_readable == 1);
    assert(counts.stream_failed == 0);
    assert(counts.stream_closed == 0);
    assert(counts.order_count == 2);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_STREAM_READY);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    assert(counts.order_subjects[0].slot == stream->base.token.slot);
    assert(counts.order_subjects[1].slot == stream->base.token.slot);

    trevrpc_engine_receive* receive = NULL;
    assert(trevrpc_engine_stream_receive_frame(fixture.engine, stream->base.token, &receive) == 0);
    trevrpc_engine_receive_release(receive);
    assert(adapter_stream_callback(stream->base.handle,
               stream,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_peer_stream_queue_is_bounded_and_fifo(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 5u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);

    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE) == QUIC_STATUS_SUCCESS);
    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 1u) == QUIC_STATUS_SUCCESS);
    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 2u) == QUIC_STATUS_ABORTED);
    assert(connection->pending_peer_stream_count == 2);
    adapter_stream* first = connection->pending_peer_stream_head->stream;
    adapter_stream* second = connection->pending_peer_stream_head->next->stream;
    uint8_t frame[4] = {0};
    uint64_t accepted = UINT64_MAX;
    assert(
        indicate_stream_receive(first, frame, sizeof(frame), QUIC_RECEIVE_FLAG_NONE, &accepted) == QUIC_STATUS_SUCCESS);
    assert(accepted == sizeof(frame));
    adapter_scheduler_drain(fixture.adapter);
    assert(first->base.ready);
    assert(second->base.ready);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.stream_ready == 2);
    assert(counts.stream_readable == 1);
    assert(counts.order_count == 3);
    assert(counts.order[0] == TREVRPC_ENGINE_EVENT_STREAM_READY);
    assert(counts.order[1] == TREVRPC_ENGINE_EVENT_STREAM_READABLE);
    assert(counts.order[2] == TREVRPC_ENGINE_EVENT_STREAM_READY);
    assert(counts.order_subjects[0].slot == first->base.token.slot);
    assert(counts.order_subjects[1].slot == first->base.token.slot);
    assert(counts.order_subjects[2].slot == second->base.token.slot);
    assert(fixture.adapter->terminal_status == 0);
    assert(adapter_stream_callback(first->base.handle,
               first,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(adapter_stream_callback(second->base.handle,
               second,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

static void test_peer_stream_close_while_queued_publishes_once(void) {
    receive_fixture fixture = fixture_create_with_capacities(1024, 1, 2);
    adapter_listener* listener = fixture_add_listener(&fixture);
    assert(fixture_new_connection(&fixture, listener, TEST_CONNECTION_HANDLE_BASE + 6u) == QUIC_STATUS_SUCCESS);
    adapter_connection* connection = fixture_queued_connection(&fixture, 0);
    adapter_scheduler_drain(fixture.adapter);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_CONNECTED) == QUIC_STATUS_SUCCESS);
    fixture_discard_events(&fixture);
    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 3u) == QUIC_STATUS_SUCCESS);
    adapter_stream* stream = connection->pending_peer_stream_head->stream;
    assert(fixture_peer_stream(connection, TEST_PEER_STREAM_HANDLE_BASE + 4u) == QUIC_STATUS_SUCCESS);
    adapter_stream* second = connection->pending_peer_stream_head->next->stream;
    assert(connection->pending_peer_stream_count == 2);
    assert(adapter_stream_callback(stream->base.handle,
               stream,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(adapter_stream_callback(stream->base.handle,
               stream,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    accept_event_counts counts = drain_accept_events(&fixture);
    assert(counts.stream_failed + counts.stream_closed == 1);
    assert(connection->pending_peer_stream_count == 1);
    assert(fixture.adapter->terminal_status == 0);
    assert(adapter_stream_callback(second->base.handle,
               second,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(adapter_stream_callback(second->base.handle,
               second,
               &(QUIC_STREAM_EVENT){.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE}) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    assert(fixture_connection_event(connection, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) == QUIC_STATUS_SUCCESS);
    accept_event_counts tail_counts = drain_accept_events(&fixture);
    assert(tail_counts.stream_failed + tail_counts.stream_closed == 1);
    assert(tail_counts.connection_closed + tail_counts.connection_failed == 1);
    assert(fixture.adapter->pending_stream_count == 0);
    assert(fixture.adapter->live_connections == 0);
    assert(fixture.adapter->live_streams == 1);
    fixture_destroy(&fixture);
}

int main(void) {
    test_clean_shutdown_waits_for_receive_fin();
    test_partial_zero_and_resume();
    test_receive_create_failure_preserves_queue();
    test_readable_publication_failure_retries();
    test_exact_header_boundary_is_backpressure();
    test_exact_header_boundary_fin_is_truncation();
    test_peer_send_abort_retires_pause();
    test_peer_receive_abort_publishes_send_stopped_once();
    test_receive_abort_failure_preserves_retryable_state();
    test_local_receive_abort_suppresses_late_peer_fin();
    test_peer_abort_resume_race_is_nonfatal();
    test_callback_entry_failure_aborts_explicitly();
    test_closing_stream_is_not_resumed();
    test_resume_close_race_is_nonfatal();
    test_connection_accept_queue_is_bounded();
    test_connection_is_configured_before_callback_returns();
    test_connection_setup_failure_is_terminal_without_engine_failure();
    test_close_during_configuration_defers_native_shutdown();
    test_connection_promotion_is_fifo();
    test_pending_send_round_robin_three_streams();
    test_synchronous_send_failure_releases_budget();
    test_send_operation_id_reuse_waits_for_completion_delivery();
    test_send_completion_wake_failure_does_not_hold_send_gate();
    test_cancel_unsent_unlinks_scheduler_stream();
    test_cancel_unsent_restores_pending_tail();
    test_send_reservation_release_finishes_shutdown();
    test_shutdown_after_send_admission_rejects_and_reclaims();
    test_fatal_receive_error_aborts_after_unlock();
    test_peer_readable_waits_for_ready_commit();
    test_peer_receive_fin_waits_for_ready_commit();
    test_peer_readiness_flush_failure_preserves_fin();
    test_peer_stream_waits_for_native_handle_publication();
    test_peer_stream_queue_is_bounded_and_fifo();
    test_peer_stream_close_while_queued_publishes_once();
    return 0;
}
