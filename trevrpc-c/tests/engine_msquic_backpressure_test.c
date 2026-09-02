#define _POSIX_C_SOURCE 200809L
#define TREVRPC_ENGINE_MSQUIC_TESTING

#include "../src/trevrpc_engine_msquic.c"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h>
#include <stdint.h>

#define TEST_STREAM_HANDLE ((HQUIC)(uintptr_t)1u)

typedef struct fake_msquic_state {
    adapter_stream* stream;
    uint32_t enable_calls;
    uint32_t shutdown_calls;
    uint32_t close_calls;
    QUIC_STATUS enable_status;
    bool close_during_enable;
    bool peer_abort_during_enable;
    bool deliver_shutdown_complete;
    bool shutdown_complete_delivered;
    bool lock_held_across_shutdown;
} fake_msquic_state;

typedef struct receive_fixture {
    msquic_provider* adapter;
    adapter_stream* stream;
    trevrpc_engine* engine;
    trevrpc_engine_handle_v1 stream_handle;
} receive_fixture;

static fake_msquic_state FakeMsQuic;

static void QUIC_API fake_stream_close(HQUIC stream_handle) {
    assert(stream_handle == TEST_STREAM_HANDLE);
    FakeMsQuic.close_calls++;
}

static QUIC_STATUS QUIC_API fake_stream_shutdown(
    HQUIC stream_handle, QUIC_STREAM_SHUTDOWN_FLAGS flags, QUIC_UINT62 error_code) {
    (void)error_code;
    assert(stream_handle == TEST_STREAM_HANDLE);
    assert((flags & QUIC_STREAM_SHUTDOWN_FLAG_ABORT) != 0);
    FakeMsQuic.shutdown_calls++;
    if (pthread_mutex_trylock(&FakeMsQuic.stream->mutex) == 0) {
        pthread_mutex_unlock(&FakeMsQuic.stream->mutex);
    } else {
        FakeMsQuic.lock_held_across_shutdown = true;
    }
    if (FakeMsQuic.deliver_shutdown_complete && !FakeMsQuic.shutdown_complete_delivered) {
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

static const QUIC_API_TABLE FakeApi = {
    .StreamClose = fake_stream_close,
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

static receive_fixture fixture_create(uint64_t max_frame_size) {
    receive_fixture fixture = {0};
    trevrpc_engine_config_v1 config;
    assert(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 8;
    config.listener_capacity = 1;
    config.connection_capacity = 1;
    config.stream_capacity = 1;
    config.max_receive_owned_count = 1;
    config.max_receive_owned_bytes = 1024;

    fixture.adapter = calloc(1, sizeof(*fixture.adapter));
    assert(fixture.adapter != NULL);
    fixture.adapter->slots = calloc(4, sizeof(*fixture.adapter->slots));
    assert(fixture.adapter->slots != NULL);
    assert(pthread_mutex_init(&fixture.adapter->mutex, NULL) == 0);
    assert(pthread_mutex_init(&fixture.adapter->budget_mutex, NULL) == 0);
    fixture.adapter->api = &FakeApi;
    fixture.adapter->state = TREVRPC_ENGINE_STATE_RUNNING;
    fixture.adapter->owner = 1;
    fixture.adapter->slot_count = 4;
    fixture.adapter->listener_begin = 1;
    fixture.adapter->connection_begin = 2;
    fixture.adapter->stream_begin = 3;
    fixture.adapter->max_receive_owned_count = 1;
    fixture.adapter->max_receive_owned_bytes = 1024;
    assert(trevrpc_engine_provider_create_v1(
               &config, &MsQuicProviderOps, fixture.adapter, fixture.adapter->owner, &fixture.engine) == 0);

    adapter_endpoint* endpoint = calloc(1, sizeof(*endpoint));
    assert(endpoint != NULL);
    atomic_init(&endpoint->refs, 1);
    endpoint->api = &FakeApi;
    endpoint->max_frame_size = max_frame_size;

    fixture.stream = calloc(1, sizeof(*fixture.stream));
    assert(fixture.stream != NULL);
    assert(pthread_mutex_init(&fixture.stream->mutex, NULL) == 0);
    assert(pthread_mutex_init(&fixture.stream->send_gate, NULL) == 0);
    atomic_init(&fixture.stream->base.closing, false);
    fixture.stream->base.endpoint = endpoint;
    fixture.stream->base.handle = TEST_STREAM_HANDLE;
    fixture.stream->base.ready = true;
    trevrpc_frame_parser_init_with_allocator(
        &fixture.stream->parser, max_frame_size, adapter_receive_alloc, adapter_receive_free, fixture.stream);
    trevrpc_frame_parser_set_retain_on_allocation_failure(&fixture.stream->parser, true);
    assert(trevrpc_engine_provider_reserve_mandatory(fixture.engine, &fixture.stream->base.terminal_reservation) == 0);
    assert(trevrpc_engine_provider_reserve_mandatory(fixture.engine, &fixture.stream->receive_fin_reservation) == 0);
    assert(registry_add(fixture.adapter, &fixture.stream->base, TREVRPC_ENGINE_OBJECT_STREAM, &fixture.stream_handle) ==
           0);

    memset(&FakeMsQuic, 0, sizeof(FakeMsQuic));
    FakeMsQuic.stream = fixture.stream;
    FakeMsQuic.enable_status = QUIC_STATUS_SUCCESS;
    return fixture;
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
        trevrpc_engine_provider_cancel_reservation(fixture->engine, terminal_reservation);
        trevrpc_engine_provider_cancel_reservation(fixture->engine, receive_fin_reservation);
    }
    fixture_discard_events(fixture);
    pthread_mutex_lock(&fixture->adapter->mutex);
    fixture->adapter->state = TREVRPC_ENGINE_STATE_STOPPED;
    pthread_mutex_unlock(&fixture->adapter->mutex);
    assert(trevrpc_engine_close(fixture->engine) == 0);
    trevrpc_engine_provider_stopped(fixture->engine, fixture->adapter->terminal_status, 0);
    assert(trevrpc_engine_drain(fixture->engine) == 0);
    assert(trevrpc_engine_release(fixture->engine) == 0);
}

static QUIC_STATUS indicate_receive_flags(
    receive_fixture* fixture, uint8_t* bytes, uint32_t len, QUIC_RECEIVE_FLAGS flags, uint64_t* accepted) {
    QUIC_BUFFER buffer = {.Length = len, .Buffer = bytes};
    QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_RECEIVE};
    event.RECEIVE.TotalBufferLength = len;
    event.RECEIVE.BufferCount = 1;
    event.RECEIVE.Buffers = &buffer;
    event.RECEIVE.Flags = flags;
    QUIC_STATUS status = adapter_stream_callback(TEST_STREAM_HANDLE, fixture->stream, &event);
    *accepted = event.RECEIVE.TotalBufferLength;
    return status;
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
        if (same_stream &&
            (info.kind == TREVRPC_ENGINE_EVENT_STREAM_CLOSED || info.kind == TREVRPC_ENGINE_EVENT_STREAM_FAILED)) {
            counts.terminal++;
        } else if (same_stream && info.kind == TREVRPC_ENGINE_EVENT_RECEIVE_FIN) {
            counts.receive_fin++;
            counts.receive_fin_flags = info.flags;
            counts.receive_fin_status = info.status;
            counts.receive_fin_application_error = info.application_error_code;
        }
        trevrpc_engine_event_release(event);
    }
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

int main(void) {
    test_partial_zero_and_resume();
    test_receive_create_failure_preserves_queue();
    test_exact_header_boundary_is_backpressure();
    test_exact_header_boundary_fin_is_truncation();
    test_peer_send_abort_retires_pause();
    test_peer_abort_resume_race_is_nonfatal();
    test_callback_entry_failure_aborts_explicitly();
    test_closing_stream_is_not_resumed();
    test_resume_close_race_is_nonfatal();
    test_fatal_receive_error_aborts_after_unlock();
    return 0;
}
