#include "trevrpc_h3_demux_internal.h"
#include "trevrpc_msquic_internal.h"
#include "trevrpc_msquic_objects_internal.h" // IWYU pragma: keep
#include "trevrpc_rpc_transport_h3_internal.h"
#include "trevrpc_rpc_transport_internal.h"
#include "trevrpc_rpc_transport_msquic_internal.h"
#include "trevrpc_qpack_static_internal.h"
#include "trevrpc_quic_varint_internal.h"
#include "trevrpc_webtransport_profile_internal.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static bool handle_equal(trevrpc_rpc_transport_handle left, trevrpc_rpc_transport_handle right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static trevrpc_rpc_transport_config test_config(void) {
    trevrpc_rpc_transport_config config = {0};
    config.event_capacity = 8;
    config.listener_capacity = 4;
    config.connection_capacity = 4;
    config.stream_capacity = 8;
    config.max_receive_owned_count = 16;
    config.max_receive_owned_bytes = 4096;
    return config;
}

static size_t build_request_headers_frame(uint8_t* output, size_t capacity, const char* path) {
    static const uint8_t authority[] = "host";
    static const uint8_t content_type[] = "application/trevrpc";
    uint8_t block[256];
    trevrpc_qpack_static_encoder encoder;
    size_t type_len = 0;
    size_t length_len = 0;
    size_t path_len = strlen(path);
    assert(trevrpc_qpack_static_encoder_init(&encoder, block, sizeof(block)) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 20) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 23) == 0);
    assert(
        trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 0, authority, sizeof(authority) - 1u) == 0);
    assert(trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 1, (const uint8_t*)path, path_len) == 0);
    assert(trevrpc_qpack_static_encoder_put_literal_name_reference(
               &encoder, 44, content_type, sizeof(content_type) - 1u) == 0);
    assert(trevrpc_quic_varint_write(output, capacity, 1, &type_len) == 0);
    assert(trevrpc_quic_varint_write(output + type_len, capacity - type_len, encoder.length, &length_len) == 0);
    assert(encoder.length <= capacity - type_len - length_len);
    memcpy(output + type_len + length_len, block, encoder.length);
    return type_len + length_len + encoder.length;
}

static size_t build_connect_headers_frame(uint8_t* output, size_t capacity) {
    static const uint8_t authority[] = "host";
    static const uint8_t protocol[] = "webtransport-h3";
    uint8_t block[256];
    trevrpc_qpack_static_encoder encoder;
    size_t type_len = 0;
    size_t length_len = 0;
    assert(trevrpc_qpack_static_encoder_init(&encoder, block, sizeof(block)) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 15) == 0);
    assert(trevrpc_qpack_static_encoder_put_literal(
               &encoder, (const uint8_t*)":protocol", sizeof(":protocol") - 1u, protocol, sizeof(protocol) - 1u) == 0);
    assert(
        trevrpc_qpack_static_encoder_put_literal(
            &encoder, (const uint8_t*)":authority", sizeof(":authority") - 1u, authority, sizeof(authority) - 1u) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 23) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 1) == 0);
    assert(trevrpc_quic_varint_write(output, capacity, 1, &type_len) == 0);
    assert(trevrpc_quic_varint_write(output + type_len, capacity - type_len, encoder.length, &length_len) == 0);
    assert(encoder.length <= capacity - type_len - length_len);
    memcpy(output + type_len + length_len, block, encoder.length);
    return type_len + length_len + encoder.length;
}

typedef struct h3_stream_close_hook_state {
    size_t immediate_closes;
} h3_stream_close_hook_state;

static void h3_stream_close_hook(trevrpc_msquic_test_stream_event event, void* context) {
    h3_stream_close_hook_state* state = context;
    if (event == TREV_MSQUIC_TEST_STREAM_CLOSE_IMMEDIATE)
        ++state->immediate_closes;
}

typedef struct h3_finalizer_progress_state {
    size_t immediate_closes;
    size_t completed_closes;
} h3_finalizer_progress_state;

static void h3_finalizer_progress_hook(trevrpc_msquic_test_stream_event event, void* context) {
    h3_finalizer_progress_state* state = context;
    if (event == TREV_MSQUIC_TEST_STREAM_CLOSE_IMMEDIATE) {
        ++state->immediate_closes;
    } else if (event == TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED) {
        ++state->completed_closes;
    }
}

typedef struct h3_finalizer_isolation_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool close_started;
    bool release_close;
} h3_finalizer_isolation_state;

static void h3_finalizer_isolation_hook(trevrpc_msquic_test_stream_event event, void* context) {
    h3_finalizer_isolation_state* state = context;
    if (event != TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED)
        return;
    pthread_mutex_lock(&state->mutex);
    state->close_started = true;
    pthread_cond_broadcast(&state->cond);
    while (!state->release_close)
        pthread_cond_wait(&state->cond, &state->mutex);
    pthread_mutex_unlock(&state->mutex);
}

typedef struct h3_destroy_pending_send_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_rpc_transport* transport;
    bool immediate_close_observed;
    bool destroy_finished;
} h3_destroy_pending_send_state;

static void h3_destroy_pending_send_hook(trevrpc_msquic_test_stream_event event, void* context) {
    h3_destroy_pending_send_state* state = context;
    if (event != TREV_MSQUIC_TEST_STREAM_CLOSE_IMMEDIATE)
        return;
    pthread_mutex_lock(&state->mutex);
    state->immediate_close_observed = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static void* h3_destroy_pending_send_thread(void* context) {
    h3_destroy_pending_send_state* state = context;
    trevrpc_rpc_transport_destroy(state->transport);
    pthread_mutex_lock(&state->mutex);
    state->destroy_finished = true;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
    return NULL;
}

typedef struct h3_stream_finish_hook_state {
    trevrpc_rpc_transport* transport;
    trevrpc_rpc_transport_handle stream;
    bool observed;
    bool send_fin;
} h3_stream_finish_hook_state;

static void h3_stream_finish_hook(trevrpc_msquic_test_stream_event event, void* context) {
    h3_stream_finish_hook_state* state = context;
    if (event != TREV_MSQUIC_TEST_STREAM_SHUTDOWN_GRACEFUL)
        return;
    state->observed = true;
    assert(trevrpc_rpc_transport_h3_test_stream_send_fin(state->transport, state->stream, &state->send_fin) == 0);
}

static void test_h3_stream_close_modes(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object = NULL;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    h3_stream_close_hook_state hook = {0};

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    trevrpc_msquic_test_set_stream_hook(h3_stream_close_hook, &hook);
    assert(trevrpc_rpc_transport_stream_close(transport, stream) == 0);
    assert(hook.immediate_closes == 0);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);

    fixture = NULL;
    object = NULL;
    transport = NULL;
    hook.immediate_closes = 0;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    trevrpc_msquic_test_set_stream_hook(h3_stream_close_hook, &hook);
    trevrpc_rpc_transport_destroy(transport);
    assert(hook.immediate_closes == 1);
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_msquic_finalizer_avoids_graceful_stream_convoy(void) {
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* blocked = NULL;
    trevrpc_msquic_stream* following = NULL;
    h3_finalizer_progress_state state = {0};

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 2, &fixture) == 0);
    blocked = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    following = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(blocked != NULL && following != NULL);

    trevrpc_msquic_test_receive_fixture_prepare_graceful_close(blocked);
    trevrpc_msquic_test_set_stream_hook(h3_finalizer_progress_hook, &state);
    trevrpc_msquic_test_fail_next_graceful_shutdown();
    trevrpc_msquic_stream_close_deferred_owned(blocked, false);
    trevrpc_msquic_stream_close_deferred_owned(following, true);
    trevrpc_msquic_finalizer_drain();

    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    assert(state.immediate_closes == 1);
    assert(state.completed_closes == 2);
}

static void test_h3_destroy_isolated_from_other_finalizer_scope(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_finalizer_scope blocked_scope = {0};
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_rpc_transport* independent = NULL;
    h3_finalizer_isolation_state state = {0};
    struct timespec deadline;
    bool close_started;

    assert(trevrpc_msquic_finalizer_scope_init(&blocked_scope) == 0);
    assert(pthread_mutex_init(&state.mutex, NULL) == 0);
    assert(pthread_cond_init(&state.cond, NULL) == 0);
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    stream = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(stream != NULL);
    trevrpc_msquic_test_receive_fixture_prepare_graceful_close(stream);
    trevrpc_msquic_stream_set_finalizer_scope(stream, &blocked_scope);
    trevrpc_msquic_test_set_stream_hook(h3_finalizer_isolation_hook, &state);
    trevrpc_msquic_test_fail_next_graceful_shutdown();
    trevrpc_msquic_stream_close_deferred_owned(stream, false);

    assert(timespec_get(&deadline, TIME_UTC) == TIME_UTC);
    deadline.tv_sec += 2;
    pthread_mutex_lock(&state.mutex);
    while (!state.close_started) {
        int wait_result = pthread_cond_timedwait(&state.cond, &state.mutex, &deadline);
        if (wait_result == ETIMEDOUT)
            break;
        assert(wait_result == 0);
    }
    close_started = state.close_started;
    pthread_mutex_unlock(&state.mutex);
    assert(close_started);

    assert(trevrpc_rpc_transport_h3_create(&config, &independent) == 0);
    trevrpc_rpc_transport_destroy(independent);
    independent = NULL;

    pthread_mutex_lock(&state.mutex);
    state.release_close = true;
    pthread_cond_broadcast(&state.cond);
    pthread_mutex_unlock(&state.mutex);
    trevrpc_msquic_finalizer_scope_drain(&blocked_scope);
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_finalizer_scope_destroy(&blocked_scope);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);
}

static void test_h3_destroy_waits_for_provider_owned_send_completion(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object = NULL;
    trevrpc_msquic_send_completion* completion = NULL;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    h3_destroy_pending_send_state state;
    pthread_t destroy_thread;
    struct timespec deadline;
    bool destroy_waited;

    assert(pthread_mutex_init(&state.mutex, NULL) == 0);
    assert(pthread_cond_init(&state.cond, NULL) == 0);
    state.transport = NULL;
    state.immediate_close_observed = false;
    state.destroy_finished = false;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_stage_pending_send(transport, stream, 101, 1, &completion) == 0);
    assert(completion != NULL);

    state.transport = transport;
    trevrpc_msquic_test_set_stream_hook(h3_destroy_pending_send_hook, &state);
    assert(pthread_create(&destroy_thread, NULL, h3_destroy_pending_send_thread, &state) == 0);
    assert(timespec_get(&deadline, TIME_UTC) == TIME_UTC);
    deadline.tv_sec += 2;
    pthread_mutex_lock(&state.mutex);
    while (!state.immediate_close_observed && !state.destroy_finished) {
        int wait_result = pthread_cond_timedwait(&state.cond, &state.mutex, &deadline);
        if (wait_result == ETIMEDOUT)
            break;
        assert(wait_result == 0);
    }
    destroy_waited = state.immediate_close_observed && !state.destroy_finished;
    pthread_mutex_unlock(&state.mutex);

    trevrpc_rpc_transport_h3_test_signal_send_completion(completion, 0);
    assert(pthread_join(destroy_thread, NULL) == 0);
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);
    assert(destroy_waited);
}

typedef struct h3_send_capture_state {
    uint8_t data[20000];
    size_t len;
} h3_send_capture_state;

static h3_send_capture_state* h3_send_capture;

static void h3_capture_send(const uint8_t* data, size_t len) {
    assert(h3_send_capture != NULL);
    assert(len <= sizeof(h3_send_capture->data));
    memcpy(h3_send_capture->data, data, len);
    h3_send_capture->len = len;
}

static size_t h3_expected_data_frame(uint8_t* output, size_t capacity, const uint8_t* body, size_t body_len) {
    size_t type_len = 0;
    size_t length_len = 0;
    assert(trevrpc_quic_varint_size(TREV_H3_FRAME_DATA, &type_len) == 0);
    assert(trevrpc_quic_varint_size(body_len + 4, &length_len) == 0);
    assert(trevrpc_quic_varint_write(output, capacity, TREV_H3_FRAME_DATA, &type_len) == 0);
    assert(trevrpc_quic_varint_write(output + type_len, capacity - type_len, body_len + 4, &length_len) == 0);
    output[type_len + length_len] = (uint8_t)(body_len >> 24);
    output[type_len + length_len + 1] = (uint8_t)(body_len >> 16);
    output[type_len + length_len + 2] = (uint8_t)(body_len >> 8);
    output[type_len + length_len + 3] = (uint8_t)body_len;
    if (body_len != 0)
        memcpy(output + type_len + length_len + 4, body, body_len);
    return type_len + length_len + 4 + body_len;
}

static void test_h3_send_encoding_boundaries_and_rejection_cleanup(void) {
    static const size_t body_lengths[] = {0, 60, 16380};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object = NULL;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    h3_send_capture_state capture = {0};
    uint8_t body[16380];
    uint8_t expected[20000];

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_attach_stream_object(transport, stream, object) == 0);
    trevrpc_msquic_test_receive_fixture_set_handle_present(object, true);
    h3_send_capture = &capture;
    trevrpc_rpc_transport_h3_test_set_send_capture(h3_capture_send);

    for (size_t index = 0; index < sizeof(body_lengths) / sizeof(body_lengths[0]); ++index) {
        size_t body_len = body_lengths[index];
        for (size_t offset = 0; offset < body_len; ++offset)
            body[offset] = (uint8_t)(offset ^ (index * 0x31u));
        capture.len = 0;
        trevrpc_msquic_test_fail_next_stream_send();
        assert(trevrpc_rpc_transport_stream_send(transport, stream, index + 1, body, body_len) == -EIO);
        size_t expected_len = h3_expected_data_frame(expected, sizeof(expected), body, body_len);
        memset(body, 0xa5, body_len);
        assert(capture.len == expected_len);
        assert(memcmp(capture.data, expected, expected_len) == 0);
        pthread_mutex_lock(&object->mutex);
        assert(object->pending_send_count == 0);
        assert(object->pending_send_bytes == 0);
        pthread_mutex_unlock(&object->mutex);
        assert(trevrpc_rpc_transport_h3_test_pending_events(transport) == 0);
    }

    trevrpc_rpc_transport_h3_test_set_send_capture(NULL);
    h3_send_capture = NULL;
    trevrpc_msquic_test_receive_fixture_set_handle_present(object, false);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_finish_publishes_fin_before_provider_shutdown(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object = NULL;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    h3_stream_finish_hook_state hook = {0};
    bool send_fin = true;

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    hook.transport = transport;
    hook.stream = stream;
    trevrpc_msquic_test_receive_fixture_set_handle_present(object, true);
    trevrpc_msquic_test_set_stream_hook(h3_stream_finish_hook, &hook);
    trevrpc_msquic_test_fail_next_graceful_shutdown();
    assert(trevrpc_rpc_transport_stream_finish_send(transport, stream) != 0);
    trevrpc_msquic_test_set_stream_hook(NULL, NULL);
    trevrpc_msquic_test_receive_fixture_set_handle_present(object, false);
    assert(hook.observed);
    assert(hook.send_fin);
    assert(trevrpc_rpc_transport_h3_test_stream_send_fin(transport, stream, &send_fin) == 0);
    assert(!send_fin);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_fragmented_h3_data_and_rpc_frame(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_event_info event_info;
    trevrpc_rpc_transport_receive_info receive_info;
    const uint8_t frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};

    trevrpc_rpc_transport_config config = test_config();
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, 5, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_pending_events(transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame + 5, sizeof(frame) - 5, true) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_readable_reemits_after_event_pressure(void) {
    static const uint8_t frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;

    config.event_capacity = 1;
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_emit(transport,
               TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
               0,
               0,
               connection,
               (trevrpc_rpc_transport_handle){0},
               0) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, sizeof(frame), false) == 0);

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_post_requires_configured_path(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    uint8_t headers[320];
    size_t headers_len = build_request_headers_frame(headers, sizeof(headers), "/wrong");

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/trevrpc", sizeof("/trevrpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, headers, headers_len, false) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_parser_commits_headers_before_receive_retry(void) {
    static const uint8_t blocker_frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'o', 'l', 'd'};
    static const uint8_t rpc_body[] = {0x00, 0x00, 0x00, 0x03, 'n', 'e', 'w'};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle blocker;
    trevrpc_rpc_transport_handle request;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    uint8_t wire[384];
    size_t wire_len = build_request_headers_frame(wire, sizeof(wire), "/trevrpc");
    size_t written = 0;

    config.max_receive_owned_count = 1;
    assert(trevrpc_quic_varint_write(wire + wire_len, sizeof(wire) - wire_len, 0, &written) == 0);
    wire_len += written;
    assert(trevrpc_quic_varint_write(wire + wire_len, sizeof(wire) - wire_len, sizeof(rpc_body), &written) == 0);
    wire_len += written;
    assert(sizeof(rpc_body) <= sizeof(wire) - wire_len);
    memcpy(wire + wire_len, rpc_body, sizeof(rpc_body));
    wire_len += sizeof(rpc_body);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &blocker) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, blocker, blocker_frame, sizeof(blocker_frame), false) ==
           0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/trevrpc", sizeof("/trevrpc") - 1u, &request) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, request, wire, wire_len, false) == -EAGAIN);

    assert(trevrpc_rpc_transport_stream_receive(transport, blocker, &receive) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    receive = NULL;
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, request, NULL, 0, false) == 0);
    assert(trevrpc_rpc_transport_stream_receive(transport, request, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "new", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_peer_wt_stream_replays_after_profile_and_session_resolution(bool unidirectional) {
    static const uint8_t bidi_wire[] = {
        0x40,
        0x41,
        0x04,
        0x00,
        0x00,
        0x00,
        0x03,
        'a',
        'b',
        'c',
    };
    static const uint8_t unidi_wire[] = {
        0x40,
        0x54,
        0x04,
        0x00,
        0x00,
        0x00,
        0x03,
        'a',
        'b',
        'c',
    };
    const uint8_t* wire = unidirectional ? unidi_wire : bidi_wire;
    const size_t wire_len = unidirectional ? sizeof(unidi_wire) : sizeof(bidi_wire);
    const uint8_t* buffers[] = {wire};
    const size_t lengths[] = {wire_len};
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info event_info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    size_t accepted = 0;
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(
        trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, object, unidirectional, &stream) == 0);
    assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, false, &accepted) == 0);
    assert(accepted == wire_len);

    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(handle_equal(event_info.subject, connection));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(handle_equal(event_info.subject, stream));
    assert(handle_equal(event_info.parent, connection));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    assert(handle_equal(event_info.subject, stream));
    assert(handle_equal(event_info.parent, connection));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);

    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_peer_wt_exact_prefix_replays_without_more_provider_data(void) {
    static const uint8_t wire[] = {0x40, 0x41, 0x04};
    const uint8_t* buffers[] = {wire};
    const size_t lengths[] = {sizeof(wire)};
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info event_info;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    size_t accepted = 0;
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, object, false, &stream) == 0);
    assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, false, &accepted) == 0);
    assert(accepted == sizeof(wire));

    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(handle_equal(event_info.subject, connection));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(handle_equal(event_info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_unresolved_wt_stream_count_rejects_before_admission(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* first;
    trevrpc_msquic_stream* second;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    uint64_t abort_error = 0;

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(first != NULL && second != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_unresolved_limits(transport, connection, 1, 0) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, first, false, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, second, true, &stream) == -EAGAIN);
    assert(trevrpc_msquic_test_stream_last_abort_error(second, &abort_error) == 0);
    assert(abort_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    trevrpc_msquic_test_stream_clear_abort_error(second);

    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_unresolved_wt_stream_timeout_is_driver_scheduled(void) {
    const uint64_t start = UINT64_C(1000000000);
    const uint64_t deadline = start + UINT64_C(5000000);
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* first;
    trevrpc_msquic_stream* second;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle first_stream;
    trevrpc_rpc_transport_handle second_stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    uint64_t abort_error = 0;

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 2, &fixture) == 0);
    first = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    second = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(first != NULL && second != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_set_now_nanos(transport, start);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_unresolved_limits(transport, connection, 1, 5) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, first, false, &first_stream) == 0);
    assert(trevrpc_rpc_transport_poll_timeout_ms(transport) == 5);
    trevrpc_rpc_transport_h3_test_set_now_nanos(transport, deadline - 1);
    assert(trevrpc_rpc_transport_poll_timeout_ms(transport) == 1);
    trevrpc_rpc_transport_h3_test_set_now_nanos(transport, deadline);
    assert(trevrpc_rpc_transport_poll_timeout_ms(transport) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
    assert(info.status == -ETIMEDOUT);
    assert(handle_equal(info.subject, first_stream));
    assert(trevrpc_msquic_test_stream_last_abort_error(first, &abort_error) == 0);
    assert(abort_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    trevrpc_msquic_test_stream_clear_abort_error(first);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, second, false, &second_stream) == 0);
    assert(!handle_equal(first_stream, second_stream));
    {
        static const uint8_t incomplete_session_id[] = {0x40, 0x41, 0x40};
        const uint8_t* buffers[] = {incomplete_session_id};
        const size_t lengths[] = {sizeof(incomplete_session_id)};
        size_t accepted = 0;
        assert(trevrpc_msquic_test_receive_inject(second, buffers, lengths, 1, false, &accepted) == 0);
        assert(accepted == sizeof(incomplete_session_id));
        assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
        assert(event == NULL);
    }
    trevrpc_rpc_transport_h3_test_set_now_nanos(transport, deadline + UINT64_C(5000000));
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
    assert(info.status == -ETIMEDOUT);
    assert(handle_equal(info.subject, second_stream));
    assert(trevrpc_msquic_test_stream_last_abort_error(second, &abort_error) == 0);
    assert(abort_error == TREV_H3_DEMUX_APP_ID_ERROR);
    trevrpc_msquic_test_stream_clear_abort_error(second);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static trevrpc_rpc_transport_handle next_subject(trevrpc_rpc_transport* transport, uint32_t expected_kind) {
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == expected_kind);
    trevrpc_rpc_transport_event_release(transport, event);
    return info.subject;
}

static void test_composite_translation_and_readable_tombstone(void) {
    trevrpc_rpc_transport* source = NULL;
    trevrpc_rpc_transport* other = NULL;
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle local_stream;
    trevrpc_rpc_transport_handle public_stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info event_info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    const uint8_t frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'x', 'y', 'z'};
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_rpc_transport_h3_create(&config, &source) == 0);
    assert(trevrpc_rpc_transport_h3_create(&config, &other) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(source, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(source, connection, &local_stream) == 0);
    assert(trevrpc_rpc_transport_msquic_adopt(source, other, &config, &composite) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(source, local_stream, frame, sizeof(frame), false) == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    public_stream = event_info.subject;
    trevrpc_rpc_transport_event_release(composite, event);
    event = NULL;
    assert(trevrpc_rpc_transport_h3_test_emit(source,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
               -EPIPE,
               local_stream,
               connection,
               0) == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
    trevrpc_rpc_transport_event_release(composite, event);
    assert(trevrpc_rpc_transport_stream_receive(composite, public_stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(composite, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "xyz", 3) == 0);
    trevrpc_rpc_transport_receive_release(composite, receive);
    assert(trevrpc_rpc_transport_stream_receive(composite, public_stream, &receive) == -EAGAIN);
    assert(trevrpc_rpc_transport_stream_send(composite, public_stream, 1, frame, sizeof(frame)) == -ESTALE);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_composite_generation_reuse(void) {
    trevrpc_rpc_transport* source = NULL;
    trevrpc_rpc_transport* other = NULL;
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle local_stream_a;
    trevrpc_rpc_transport_handle local_stream_b;
    trevrpc_rpc_transport_handle public_stream_a;
    trevrpc_rpc_transport_handle public_stream_b;
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_rpc_transport_h3_create(&config, &source) == 0);
    assert(trevrpc_rpc_transport_h3_create(&config, &other) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(source, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(source, connection, &local_stream_a) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(source, connection, &local_stream_b) == 0);
    assert(trevrpc_rpc_transport_msquic_adopt(source, other, &config, &composite) == 0);

    assert(trevrpc_rpc_transport_h3_test_emit(
               source, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY, 0, 0, local_stream_a, connection, 1) == 0);
    public_stream_a = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(trevrpc_rpc_transport_h3_test_emit(source,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
               0,
               local_stream_a,
               connection,
               0) == 0);
    assert(handle_equal(public_stream_a, next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED)));
    assert(trevrpc_rpc_transport_release_handle(composite, public_stream_a, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) == 0);

    assert(trevrpc_rpc_transport_h3_test_emit(
               source, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY, 0, 0, local_stream_b, connection, 2) == 0);
    public_stream_b = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(public_stream_b.slot == public_stream_a.slot);
    assert(public_stream_b.generation != public_stream_a.generation);
    assert(trevrpc_rpc_transport_stream_send(composite, public_stream_a, 3, NULL, 0) == -ESTALE);

    trevrpc_rpc_transport_destroy(composite);
}

static void test_h3_registry_bounded_churn_reuses_generation(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle previous = {0};
    trevrpc_rpc_transport_config config = test_config();
    size_t i;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    for (i = 0; i < 128; ++i) {
        trevrpc_rpc_transport_handle stream;
        trevrpc_rpc_transport_event* event = NULL;
        trevrpc_rpc_transport_event_info info;
        assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
        if (i != 0) {
            assert(stream.slot == previous.slot);
            assert(stream.generation != previous.generation);
            assert(trevrpc_rpc_transport_stream_close(transport, previous) == -ESTALE);
        }
        assert(trevrpc_rpc_transport_stream_close(transport, stream) == 0);
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        assert(handle_equal(info.subject, stream));
        trevrpc_rpc_transport_event_release(transport, event);
        previous = stream;
    }
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_retired_receive_drains_after_reuse(void) {
    static const uint8_t frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'r', 'e', 't'};
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle old_stream;
    trevrpc_rpc_transport_handle new_stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info event_info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &old_stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, old_stream, frame, sizeof(frame), false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_stream_close(transport, old_stream) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &new_stream) == 0);
    assert(new_stream.slot == old_stream.slot);
    assert(new_stream.generation != old_stream.generation);
    assert(trevrpc_rpc_transport_stream_receive(transport, old_stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "ret", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    assert(trevrpc_rpc_transport_stream_receive(transport, old_stream, &receive) == -ESTALE);
    assert(trevrpc_rpc_transport_stream_close(transport, new_stream) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_shutdown_orders_terminals_before_stopped(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_config config = test_config();
    bool stopped = false;
    size_t terminal_count = 0;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_close(transport) == 0);
    while (trevrpc_rpc_transport_next_event(transport, &event) == 0) {
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED) {
            assert(!stopped);
            stopped = true;
        } else {
            assert(!stopped);
            assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL) != 0);
            ++terminal_count;
        }
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(stopped);
    assert(terminal_count == 2);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_abort_wire_codes_and_peer_reset_decode(void) {
    static const trevrpc_wt_profile_id profiles[] = {
        TREV_WT_PROFILE_DRAFT_02,
        TREV_WT_PROFILE_DRAFT_07,
        TREV_WT_PROFILE_DRAFT_14,
        TREV_WT_PROFILE_DRAFT_15,
    };
    const uint64_t caller_code = 42;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    uint64_t wire_code = 0;
    size_t index;

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    assert(trevrpc_rpc_transport_stream_abort(transport, stream, caller_code) == 0);
    assert(trevrpc_msquic_test_stream_last_abort_error(object, &wire_code) == 0);
    assert(wire_code == UINT64_C(0x10c));
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(transport);
    transport = NULL;
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    fixture = NULL;

    /* Unexpected ordinary H3 reset codes remain available diagnostically. */
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    assert(trevrpc_msquic_test_inject_peer_send_aborted(object, UINT64_C(0x1ff)) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0);
    assert(info.status == -ECANCELED);
    assert(info.application_error_code == 0);
    assert(info.provider_error_code == UINT64_C(0x1ff));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    transport = NULL;
    fixture = NULL;

    for (index = 0; index < sizeof(profiles) / sizeof(profiles[0]); ++index) {
        trevrpc_wt_profile_id profile = profiles[index];
        uint64_t expected_wire = 0;
        assert(trevrpc_wt_profile_encode_application_error(profile, caller_code, &expected_wire) == 0);
        assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
        object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
        assert(object != NULL);
        assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
        assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
        assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(transport, connection, profile) == 0);
        assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
        trevrpc_msquic_test_stream_clear_abort_error(object);
        if (profile == TREV_WT_PROFILE_DRAFT_02) {
            assert(trevrpc_rpc_transport_stream_abort(transport, stream, UINT64_C(256)) == -ERANGE);
            assert(trevrpc_msquic_test_stream_last_abort_error(object, &wire_code) == -ENOENT);
        }
        assert(trevrpc_rpc_transport_stream_abort(transport, stream, caller_code) == 0);
        assert(trevrpc_msquic_test_stream_last_abort_error(object, &wire_code) == 0);
        assert(wire_code == expected_wire);
        trevrpc_rpc_transport_destroy(transport);
        transport = NULL;
        trevrpc_msquic_test_receive_fixture_destroy(fixture);
        fixture = NULL;
    }

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    assert(trevrpc_msquic_test_inject_peer_send_aborted(object, UINT64_C(0x10c)) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0);
    assert(info.status == -ECANCELED);
    assert(info.application_error_code == 1);
    assert(info.provider_error_code == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);

    for (index = 0; index < sizeof(profiles) / sizeof(profiles[0]); ++index) {
        static const uint8_t prefix[] = {0x40, 0x41, 0x04};
        const uint8_t* buffers[] = {prefix};
        const size_t lengths[] = {sizeof(prefix)};
        trevrpc_wt_profile_id profile = profiles[index];
        uint64_t expected_wire = 0;
        size_t accepted = 0;
        assert(trevrpc_wt_profile_encode_application_error(profile, caller_code, &expected_wire) == 0);
        assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
        object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
        assert(object != NULL);
        assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
        assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
        assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(transport, connection, profile) == 0);
        assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);
        assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
        assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, false, &accepted) == 0);
        assert(accepted == sizeof(prefix));
        while (trevrpc_rpc_transport_next_event(transport, &event) == 0)
            trevrpc_rpc_transport_event_release(transport, event);
        assert(trevrpc_msquic_test_inject_peer_send_aborted(object, expected_wire) == 0);
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
        assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0);
        assert(info.status == -ECANCELED);
        assert(info.application_error_code == caller_code);
        assert(info.provider_error_code == 0);
        trevrpc_rpc_transport_event_release(transport, event);
        trevrpc_rpc_transport_destroy(transport);
        trevrpc_msquic_test_receive_fixture_destroy(fixture);
        transport = NULL;
        fixture = NULL;
    }

    /* A H3 reset code is not a valid WebTransport application code. */
    {
        static const uint8_t prefix[] = {0x40, 0x41, 0x04};
        const uint8_t* buffers[] = {prefix};
        const size_t lengths[] = {sizeof(prefix)};
        size_t accepted = 0;
        assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
        object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
        assert(object != NULL);
        assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
        assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
        assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
                   transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
        assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);
        assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
        assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, false, &accepted) == 0);
        assert(accepted == sizeof(prefix));
        while (trevrpc_rpc_transport_next_event(transport, &event) == 0)
            trevrpc_rpc_transport_event_release(transport, event);
        assert(trevrpc_msquic_test_inject_peer_send_aborted(object, UINT64_C(0x10c)) == 0);
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
        assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0);
        assert(info.status == -ECANCELED);
        assert(info.application_error_code == 0);
        assert(info.provider_error_code == UINT64_C(0x10c));
        trevrpc_rpc_transport_event_release(transport, event);
        trevrpc_rpc_transport_destroy(transport);
        trevrpc_msquic_test_receive_fixture_destroy(fixture);
    }
}

static void test_h3_peer_receive_abort_after_request_fin_closes_stream(void) {
    static const uint8_t wire[] = {
        0x40,
        0x41,
        0x04,
        0x00,
        0x00,
        0x00,
        0x03,
        'a',
        'b',
        'c',
    };
    const uint8_t* buffers[] = {wire};
    const size_t lengths[] = {sizeof(wire)};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    size_t accepted = 0;
    bool readable = false;
    bool fin = false;

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);
    while (trevrpc_rpc_transport_next_event(transport, &event) == 0) {
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, true, &accepted) == 0);
    assert(accepted == sizeof(wire));

    while (!readable) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE)
            readable = true;
        else
            assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
        assert(handle_equal(info.subject, stream));
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    receive = NULL;

    while (!fin) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN)
            fin = true;
        else
            assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
        assert(handle_equal(info.subject, stream));
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);

    trevrpc_rpc_transport_h3_test_fail_event_alloc_after(transport, 0);
    assert(trevrpc_msquic_test_inject_peer_receive_aborted(object, 0) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED);
    assert(handle_equal(info.subject, stream));
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL) != 0);
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0);
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL) == 0);
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0);
    assert(info.status == -ECANCELED);
    assert(info.application_error_code == 0);
    assert(info.provider_error_code == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
    assert(handle_equal(info.subject, stream));
    assert((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0);
    assert(info.status == -ECANCELED);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);

    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_receive_budget_retries_ordinary_parser(void) {
    static const uint8_t frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    const uint8_t* buffers[] = {frame};
    const size_t lengths[] = {sizeof(frame)};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    size_t accepted = 0;

    config.max_receive_owned_count = 1;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);

    assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, false, &accepted) == 0);
    assert(accepted == sizeof(frame));
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, true, &accepted) == 0);
    assert(accepted == sizeof(frame));
    /* The second body remains in the H3 parser; budget exhaustion is not a
     * stream failure and does not consume the parser's source bytes. */
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    /* No self-rescheduling spin while the receive budget is still full. */
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    receive = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    trevrpc_rpc_transport_event_release(transport, event);
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    receive = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
    trevrpc_rpc_transport_event_release(transport, event);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_receive_budget_retries_wt_session_replay(void) {
    static const uint8_t wire[] = {
        0x40,
        0x41,
        0x04,
        0x00,
        0x00,
        0x00,
        0x03,
        'a',
        'b',
        'c',
    };
    const uint8_t* buffers[] = {wire};
    const size_t lengths[] = {sizeof(wire)};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle streams[2];
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* objects[2];
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    trevrpc_rpc_transport_handle first_readable = {0};
    size_t accepted = 0;
    size_t index;
    bool found = false;
    bool fin_seen = false;

    config.max_receive_owned_count = 1;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 2, &fixture) == 0);
    objects[0] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    objects[1] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(objects[0] != NULL && objects[1] != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, objects[0], &streams[0]) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, objects[1], &streams[1]) == 0);
    for (index = 0; index < 2; ++index) {
        assert(trevrpc_msquic_test_receive_inject(objects[index], buffers, lengths, 1, false, &accepted) == 0);
        assert(accepted == sizeof(wire));
    }

    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);

    while (!found) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE) {
            first_readable = info.subject;
            found = true;
        }
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_stream_receive(transport, first_readable, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    receive = NULL;

    found = false;
    while (!found) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE && !handle_equal(info.subject, first_readable))
            found = true;
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN && handle_equal(info.subject, streams[1]))
            fin_seen = true;
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_stream_receive(transport, info.subject, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    assert(trevrpc_msquic_test_receive_inject(objects[1], NULL, NULL, 0, true, &accepted) == 0);
    while (!fin_seen) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN && handle_equal(info.subject, streams[1]))
            fin_seen = true;
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_unresolved_wt_bytes_reject_too_small_provider_budget(void) {
    trevrpc_msquic_receive_policy policy = {0};
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    policy.max_undecided_owned_bytes = 1;
    assert(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 1, &fixture) == -EINVAL);
    assert(fixture == NULL);
}

static void test_unresolved_wt_bytes_share_provider_budget(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle streams[2];
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* objects[2];
    trevrpc_msquic_test_receive_snapshot first_snapshot;
    trevrpc_msquic_test_receive_snapshot second_snapshot;
    trevrpc_msquic_receive_policy policy = {0};
    trevrpc_rpc_transport_event* event = NULL;
    uint8_t one = 0;
    const uint8_t* one_buffer[] = {&one};
    const size_t one_length[] = {1};
    uint8_t* pressure;
    const uint8_t* pressure_buffer[1];
    size_t pressure_length[1];
    size_t minimum = trevrpc_msquic_test_receive_minimum_bytes(1);
    size_t accepted = 0;

    assert(minimum <= SIZE_MAX / 2u);
    policy.max_stream_owned_bytes = minimum * 2u;
    policy.max_stream_owned_count = 4;
    policy.max_connection_owned_bytes = minimum * 2u;
    policy.max_connection_owned_count = 6;
    policy.max_undecided_owned_bytes = minimum;
    policy.max_undecided_owned_count = 4;
    pressure = calloc(1, minimum);
    assert(pressure != NULL);
    pressure[0] = 0x40;
    pressure[1] = 0x41;
    pressure_buffer[0] = pressure;
    pressure_length[0] = minimum;

    assert(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 2, &fixture) == 0);
    objects[0] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    objects[1] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(objects[0] != NULL && objects[1] != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, objects[0], false, &streams[0]) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, objects[1], false, &streams[1]) == 0);

    assert(trevrpc_msquic_test_receive_inject(objects[0], pressure_buffer, pressure_length, 1, false, &accepted) == 0);
    assert(accepted != 0 && accepted < minimum);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_msquic_test_receive_inject(objects[1], one_buffer, one_length, 1, false, &accepted) == 0);
    assert(accepted == 0);
    trevrpc_msquic_test_receive_snapshot_get(objects[0], &first_snapshot);
    trevrpc_msquic_test_receive_snapshot_get(objects[1], &second_snapshot);
    assert(first_snapshot.connection_owned_bytes != 0);
    assert(first_snapshot.connection_owned_bytes <= minimum);
    assert(second_snapshot.connection_owned_bytes == first_snapshot.connection_owned_bytes);
    assert(second_snapshot.paused);

    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    free(pressure);
}

static void test_resolved_wt_stream_leaves_unresolved_provider_budget(void) {
    static const uint8_t prefix[] = {0x40, 0x41, 0x04};
    const uint8_t* prefix_buffers[] = {prefix};
    const size_t prefix_lengths[] = {sizeof(prefix)};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    trevrpc_msquic_receive_policy policy = {0};
    trevrpc_rpc_transport_event* event = NULL;
    size_t minimum = trevrpc_msquic_test_receive_minimum_bytes(1);
    size_t pressure_len;
    uint8_t* pressure;
    const uint8_t* pressure_buffers[1];
    size_t pressure_lengths[1];
    size_t accepted = 0;

    assert(minimum != 0 && minimum < SIZE_MAX / 2u);
    pressure_len = minimum + 1u;
    pressure = calloc(1, pressure_len);
    assert(pressure != NULL);
    pressure_buffers[0] = pressure;
    pressure_lengths[0] = pressure_len;
    policy.max_stream_owned_bytes = minimum * 2u;
    policy.max_stream_owned_count = 4;
    policy.max_connection_owned_bytes = minimum * 2u;
    policy.max_connection_owned_count = 4;
    policy.max_undecided_owned_bytes = minimum;
    policy.max_undecided_owned_count = 2;

    assert(trevrpc_msquic_test_receive_fixture_create(&policy, 1, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, object, false, &stream) == 0);
    assert(trevrpc_msquic_test_receive_inject(object, prefix_buffers, prefix_lengths, 1, false, &accepted) == 0);
    assert(accepted == sizeof(prefix));
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);
    while (trevrpc_rpc_transport_next_event(transport, &event) == 0) {
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(event == NULL);

    assert(trevrpc_msquic_test_receive_inject(object, pressure_buffers, pressure_lengths, 1, false, &accepted) == 0);
    assert(accepted == pressure_len);

    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
    free(pressure);
}

static void test_h3_receive_budget_retries_wt_post_session_data(void) {
    static const uint8_t wire[] = {
        0x40,
        0x41,
        0x04,
        0x00,
        0x00,
        0x00,
        0x03,
        'a',
        'b',
        'c',
    };
    const uint8_t* buffers[] = {wire};
    const size_t lengths[] = {sizeof(wire)};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle streams[2];
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* objects[2];
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    trevrpc_rpc_transport_handle first_readable = {0};
    size_t accepted = 0;
    size_t index;
    bool found = false;
    bool fin_seen = false;

    config.max_receive_owned_count = 1;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 2, &fixture) == 0);
    objects[0] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    objects[1] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(objects[0] != NULL && objects[1] != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_session(transport, connection, 4) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, objects[0], &streams[0]) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, objects[1], &streams[1]) == 0);
    for (index = 0; index < 2; ++index) {
        assert(trevrpc_msquic_test_receive_inject(objects[index], buffers, lengths, 1, false, &accepted) == 0);
        assert(accepted == sizeof(wire));
    }

    while (!found) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE) {
            first_readable = info.subject;
            found = true;
        }
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_stream_receive(transport, first_readable, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);

    found = false;
    while (!found) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE && !handle_equal(info.subject, first_readable))
            found = true;
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN && handle_equal(info.subject, streams[1]))
            fin_seen = true;
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_stream_receive(transport, info.subject, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);

    assert(trevrpc_msquic_test_receive_inject(objects[1], NULL, NULL, 0, true, &accepted) == 0);
    while (!fin_seen) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN && handle_equal(info.subject, streams[1]))
            fin_seen = true;
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
        assert(info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_pre_ready_connection_failure_and_explicit_close(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_fail_connection(transport, connection) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_close(transport) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_control_frame_unexpected_and_connect_capsules(void) {
    static const uint8_t settings[] = {0x04, 0x02, 0x01, 0x00};
    static const uint8_t data[] = {0x00, 0x00};
    static const uint8_t headers[] = {0x01, 0x00};
    static const uint8_t unknown_capsule[] = {
        0x00,
        0x0c,
        0xc2,
        0x82,
        0x9e,
        0xc9,
        0x6b,
        0x96,
        0x69,
        0xb9,
        0x03,
        'a',
        'b',
        'c',
    };
    static const uint8_t unknown_capsule_first[] = {
        0x00,
        0x05,
        0xc2,
        0x82,
        0x9e,
        0xc9,
        0x6b,
    };
    static const uint8_t unknown_capsule_second[] = {
        0x00,
        0x07,
        0x96,
        0x69,
        0xb9,
        0x03,
        'a',
        'b',
        'c',
    };
    static const uint8_t truncated_capsule[] = {
        0x00,
        0x03,
        0x21,
        0x03,
        'a',
    };
    static const uint8_t excessive_capsule[] = {
        0x00,
        0x05,
        0x21,
        0x80,
        0x40,
        0x00,
        0x01,
    };
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_control_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, settings, sizeof(settings), false) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, data, sizeof(data), false) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_control_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, settings, sizeof(settings), false) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, headers, sizeof(headers), false) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_02) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, unknown_capsule, sizeof(unknown_capsule), false) == 0);
    assert(trevrpc_rpc_transport_h3_test_pending_events(transport) == 0);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_02) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, unknown_capsule_first, sizeof(unknown_capsule_first), false) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, unknown_capsule_second, sizeof(unknown_capsule_second), false) == 0);
    assert(trevrpc_rpc_transport_h3_test_pending_events(transport) == 0);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_02) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, truncated_capsule, sizeof(truncated_capsule), true) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_02) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, excessive_capsule, sizeof(excessive_capsule), false) == -EMSGSIZE);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_duplicate_connect_failure_is_hidden(void) {
    uint8_t connect_wire[128];
    uint8_t connect_section[128];
    trevrpc_qpack_static_encoder encoder;
    size_t connect_wire_len;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* objects[2];
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle streams[2];
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    bool saw_failed = false;
    size_t i;

    assert(trevrpc_qpack_static_encoder_init(&encoder, connect_section, sizeof(connect_section)) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 15) == 0);
    assert(trevrpc_qpack_static_encoder_put_literal(&encoder,
               (const uint8_t*)":protocol",
               sizeof(":protocol") - 1,
               (const uint8_t*)"webtransport-h3",
               sizeof("webtransport-h3") - 1) == 0);
    assert(trevrpc_qpack_static_encoder_put_literal(&encoder,
               (const uint8_t*)":authority",
               sizeof(":authority") - 1,
               (const uint8_t*)"host",
               sizeof("host") - 1) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 23) == 0);
    assert(trevrpc_qpack_static_encoder_put_indexed(&encoder, 1) == 0);
    assert(encoder.length <= sizeof(connect_wire) - 2 && encoder.length <= UINT8_MAX);
    memcpy(connect_wire + 2, connect_section, encoder.length);
    connect_wire[0] = 0x01;
    connect_wire[1] = (uint8_t)encoder.length;
    connect_wire_len = encoder.length + 2;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 2, &fixture) == 0);
    objects[0] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    objects[1] = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 1);
    assert(objects[0] != NULL && objects[1] != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    for (i = 0; i < 2; ++i)
        assert(
            trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, objects[i], &streams[i]) == 0);

    assert(
        trevrpc_rpc_transport_h3_test_inject_data(transport, streams[0], connect_wire, connect_wire_len, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, streams[1], connect_wire, connect_wire_len, false) ==
           -EPROTO);
    assert(trevrpc_msquic_test_inject_peer_send_aborted(objects[1], 7) == 0);

    for (;;) {
        int result = trevrpc_rpc_transport_next_event(transport, &event);
        if (result == -EAGAIN)
            break;
        assert(result == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED)
            saw_failed = true;
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(!saw_failed);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void h3_drain_connection_ready(trevrpc_rpc_transport* transport) {
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    for (;;) {
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY)
            return;
    }
}

static void test_h3_accepted_connect_lifecycle(void) {
    static const uint8_t drain_data[] = {0x00, 0x05, 0x80, 0x00, 0x78, 0xae, 0x00};
    static const uint8_t close_data[] = {0x00, 0x07, 0x68, 0x43, 0x04, 0x00, 0x00, 0x00, 0x00};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle connect_stream;
    trevrpc_rpc_transport_handle new_stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &connect_stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_mark_connect_accepted(transport, connection, connect_stream) == 0);
    h3_drain_connection_ready(transport);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, connect_stream, drain_data, sizeof(drain_data), false) == 0);
    assert(trevrpc_rpc_transport_stream_open(transport, connection, 1, &new_stream) == -ESHUTDOWN);
    trevrpc_rpc_transport_destroy(transport);

    transport = NULL;
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &connect_stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_mark_connect_accepted(transport, connection, connect_stream) == 0);
    h3_drain_connection_ready(transport);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, connect_stream, close_data, sizeof(close_data), false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);

    transport = NULL;
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connect_stream(transport, connection, &connect_stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_mark_connect_accepted(transport, connection, connect_stream) == 0);
    h3_drain_connection_ready(transport);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, connect_stream, NULL, 0, true) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_qpack_encoder_accepts_zero_capacity_updates(void) {
    static const uint8_t capacity_zero[] = {0x20, 0x20};
    static const uint8_t invalid_capacity[] = {0x21};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_qpack_encoder_stream(transport, connection, &stream) == 0);
    assert(
        trevrpc_rpc_transport_h3_test_inject_data(transport, stream, capacity_zero, sizeof(capacity_zero), false) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, invalid_capacity, sizeof(invalid_capacity), false) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_qpack_encoder_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, capacity_zero, 1, true) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_pre_ready_stream_failure_is_failed_event(void) {
    static const uint8_t truncated[] = {0x00, 0x05, 0x00, 0x00, 0x00, 0x05, 'x'};
    const uint8_t* buffers[] = {truncated};
    const size_t lengths[] = {sizeof(truncated)};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    size_t accepted = 0;

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(transport, connection, object, &stream) == 0);
    assert(trevrpc_msquic_test_receive_inject(object, buffers, lengths, 1, true, &accepted) == 0);
    assert(accepted == sizeof(truncated));
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
    assert(info.status == -EPROTO);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_malformed_monolithic_data_is_rejected(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_config config = test_config();
    const uint8_t truncated[] = {0x00, 0x05, 0x00, 0x00, 0x00, 0x05, 'x'};
    const uint8_t oversized[] = {0x00, 0x05, 0xff, 0xff, 0xff, 0xff, 'x'};

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, truncated, sizeof(truncated), true) == -EPROTO);
    assert(trevrpc_rpc_transport_stream_close(transport, stream) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(
        trevrpc_rpc_transport_h3_test_inject_data(transport, stream, oversized, sizeof(oversized), false) == -EMSGSIZE);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_mandatory_events_survive_allocation_pressure(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_config config = test_config();
    const uint8_t frame[] = {0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 'x'};

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    /* Existing entries own their mandatory nodes; only ordinary event nodes
     * are denied.  The accepted receive and FIN must remain observable. */
    trevrpc_rpc_transport_h3_test_fail_event_alloc_after(transport, 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, sizeof(frame), true) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN);
    trevrpc_rpc_transport_event_release(transport, event);
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    while (trevrpc_rpc_transport_next_event(transport, &event) == 0)
        trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_reserved_send_and_terminal_order_under_pressure(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_config config = test_config();
    size_t position = 0, send_position = 0, terminal_position = 0;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_stage_pending_send(transport, stream, 91, 1, NULL) == 0);
    trevrpc_rpc_transport_h3_test_fail_event_alloc_after(transport, 0);
    assert(trevrpc_rpc_transport_stream_close(transport, stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_complete_pending_send(transport, stream, 0) == 0);
    while (trevrpc_rpc_transport_next_event(transport, &event) == 0) {
        ++position;
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE) {
            assert(info.operation_id == 91);
            send_position = position;
        } else if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED) {
            terminal_position = position;
        }
        trevrpc_rpc_transport_event_release(transport, event);
    }
    assert(send_position != 0 && terminal_position > send_position);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_terminal_dequeue_controls_slot_reuse(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle old_stream;
    trevrpc_rpc_transport_handle new_stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &old_stream) == 0);
    assert(trevrpc_rpc_transport_stream_close(transport, old_stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &new_stream) == 0);
    assert(new_stream.slot != old_stream.slot);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &new_stream) == 0);
    trevrpc_rpc_transport_stream_close(transport, new_stream);
    while (trevrpc_rpc_transport_next_event(transport, &event) == 0)
        trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

typedef struct rollback_native_transport {
    trevrpc_rpc_transport base;
    unsigned close_calls;
    unsigned release_calls;
} rollback_native_transport;

static int rollback_native_next_event(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** out_event) {
    (void)transport;
    *out_event = NULL;
    return -EAGAIN;
}

static int rollback_native_connection_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t code) {
    rollback_native_transport* native = (rollback_native_transport*)transport;
    (void)connection;
    (void)code;
    ++native->close_calls;
    return 0;
}

static int rollback_native_release_handle(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    rollback_native_transport* native = (rollback_native_transport*)transport;
    (void)handle;
    assert(kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    ++native->release_calls;
    return 0;
}

static void rollback_native_destroy(trevrpc_rpc_transport* transport) {
    (void)transport;
}

static const trevrpc_rpc_transport_ops rollback_native_ops = {
    .next_event = rollback_native_next_event,
    .connection_close = rollback_native_connection_close,
    .release_handle = rollback_native_release_handle,
    .destroy = rollback_native_destroy,
};

static void test_shared_listener_rollback_closes_and_releases_child(void) {
    rollback_native_transport native = {.base = {.ops = &rollback_native_ops}};
    trevrpc_rpc_transport* h3 = NULL;
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_handle local_listener;
    trevrpc_rpc_transport_handle ignored_connection;
    trevrpc_rpc_transport_handle local_child = {UINT64_C(0x7777), 1u, 1u};
    trevrpc_rpc_transport_handle public_listener;
    trevrpc_rpc_transport_handle public_child = {0};
    trevrpc_rpc_transport_config config = test_config();

    assert(trevrpc_rpc_transport_h3_create(&config, &h3) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(h3, &local_listener, &ignored_connection) == 0);
    assert(trevrpc_rpc_transport_msquic_adopt(&native.base, h3, &config, &composite) == 0);

    assert(
        trevrpc_rpc_transport_h3_test_emit(
            h3, TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC, 0, 0, local_listener, (trevrpc_rpc_transport_handle){0}, 0) ==
        0);
    public_listener = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC);
    assert(trevrpc_rpc_transport_msquic_test_rollback_shared_listener(
               composite, public_listener, local_child, &public_child) == 0);
    assert(public_child.owner != 0 && public_child.slot != 0 && public_child.generation != 0);
    assert(native.close_calls == 1);
    assert(native.release_calls == 1);
    assert(trevrpc_rpc_transport_release_handle(composite, public_child, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) == 0);
    assert(trevrpc_rpc_transport_endpoint_get_port(composite, public_listener, &(uint16_t){0}) == -ESTALE);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_composite_translates_admission_listener(void) {
    trevrpc_rpc_transport* source = NULL;
    trevrpc_rpc_transport* other = NULL;
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_handle local_listener;
    trevrpc_rpc_transport_handle local_connection;
    trevrpc_rpc_transport_handle local_stream;
    trevrpc_rpc_transport_handle public_listener;
    trevrpc_rpc_transport_handle public_connection;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    trevrpc_rpc_transport_config config = test_config();
    uint32_t event_refs = 0;
    uint8_t frame[512];
    size_t frame_len = build_request_headers_frame(frame, sizeof(frame), "/rpc");

    assert(trevrpc_rpc_transport_h3_create(&config, &source) == 0);
    assert(trevrpc_rpc_transport_h3_create(&config, &other) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(source, &local_listener, &local_connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(source, local_connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               source, local_connection, "/rpc", sizeof("/rpc") - 1u, &local_stream) == 0);
    assert(trevrpc_rpc_transport_msquic_adopt(source, other, &config, &composite) == 0);

    assert(trevrpc_rpc_transport_h3_test_emit(source,
               TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
               0,
               0,
               local_listener,
               (trevrpc_rpc_transport_handle){0},
               0) == 0);
    public_listener = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC);
    assert(trevrpc_rpc_transport_h3_test_emit(
               source, TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC, 0, 0, local_connection, local_listener, 0) == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    public_connection = info.subject;
    assert(handle_equal(info.parent, public_listener));
    trevrpc_rpc_transport_event_release(composite, event);
    event = NULL;

    assert(trevrpc_rpc_transport_h3_test_inject_data(source, local_stream, frame, frame_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
    assert(handle_equal(info.parent, public_connection));
    assert(trevrpc_rpc_transport_event_get_admission_info(composite, event, &admission) == 0);
    assert(handle_equal(admission.listener, public_listener));
    assert(trevrpc_rpc_transport_msquic_test_event_refs(composite, public_listener, &event_refs) == 0);
    assert(event_refs == 1);
    assert(trevrpc_rpc_transport_admission_respond(composite, event, 200) == 0);
    trevrpc_rpc_transport_event_release(composite, event);
    assert(trevrpc_rpc_transport_msquic_test_event_refs(composite, public_listener, &event_refs) == 0);
    assert(event_refs == 0);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_h3_deferred_admission_snapshot_and_accept(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    trevrpc_rpc_transport_event_protocol_info protocol;
    trevrpc_rpc_transport_config config = test_config();
    uint8_t frame[512];
    size_t frame_len = build_request_headers_frame(frame, sizeof(frame), "/rpc");

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, frame_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
    assert(info.subject_kind == TREVRPC_RPC_TRANSPORT_OBJECT_NONE);
    assert(handle_equal(info.subject, (trevrpc_rpc_transport_handle){0}));
    assert(handle_equal(info.parent, connection));
    assert(trevrpc_rpc_transport_event_get_admission_info(transport, event, &admission) == 0);
    assert(admission.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3);
    assert(handle_equal(admission.listener, listener));
    assert(admission.header_count == 5);
    assert(admission.method_len == 4 && memcmp(admission.method, "POST", 4) == 0);
    assert(admission.path_len == 4 && memcmp(admission.path, "/rpc", 4) == 0);
    assert(admission.authority_len == 4 && memcmp(admission.authority, "host", 4) == 0);
    assert(admission.origin == NULL && admission.origin_len == 0);
    assert(trevrpc_rpc_transport_event_get_protocol_info(transport, event, &protocol) == 0);
    assert(protocol.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 200) == 0);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 200) == -EALREADY);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void run_h3_deferred_admission_buffers_body(bool coalesced) {
    static const uint8_t data_frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event* extra_event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    uint8_t headers[512];
    uint8_t wire[sizeof(headers) + sizeof(data_frame)];
    size_t headers_len = build_request_headers_frame(headers, sizeof(headers), "/rpc");

    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_attach_stream_object(transport, stream, object) == 0);
    trevrpc_rpc_transport_h3_test_capture_acceptances(transport, true);

    if (coalesced) {
        memcpy(wire, headers, headers_len);
        memcpy(wire + headers_len, data_frame, sizeof(data_frame));
        assert(trevrpc_rpc_transport_h3_test_inject_data(
                   transport, stream, wire, headers_len + sizeof(data_frame), false) == 0);
    } else {
        assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, headers, headers_len, false) == 0);
    }

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
    if (!coalesced)
        assert(
            trevrpc_rpc_transport_h3_test_inject_data(transport, stream, data_frame, sizeof(data_frame), false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &extra_event) == -EAGAIN);
    assert(extra_event == NULL);

    assert(trevrpc_rpc_transport_admission_respond(transport, event, 200) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_deferred_admission_buffers_body(void) {
    run_h3_deferred_admission_buffers_body(false);
    run_h3_deferred_admission_buffers_body(true);
}

static void test_h3_parent_ready_precedes_stream_readability(void) {
    static const uint8_t data_frame[] = {0x00, 0x07, 0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_receive* receive = NULL;
    trevrpc_rpc_transport_receive_info receive_info;
    uint8_t headers[512];
    uint8_t wire[sizeof(headers) + sizeof(data_frame)];
    size_t headers_len = build_request_headers_frame(headers, sizeof(headers), "/rpc");

    memcpy(wire, headers, headers_len);
    memcpy(wire + headers_len, data_frame, sizeof(data_frame));
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(
               transport, stream, wire, headers_len + sizeof(data_frame), false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(handle_equal(info.subject, connection));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE);
    assert(handle_equal(info.subject, stream));
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_rpc_transport_receive_get_info(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 3 && memcmp(receive_info.data, "abc", 3) == 0);
    trevrpc_rpc_transport_receive_release(transport, receive);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_deferred_admission_retries_after_event_capacity_returns(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_msquic_test_receive_fixture* fixture = NULL;
    trevrpc_msquic_stream* object;
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    uint8_t frame[512];
    size_t frame_len = build_request_headers_frame(frame, sizeof(frame), "/rpc");

    config.event_capacity = 1;
    assert(trevrpc_msquic_test_receive_fixture_create(NULL, 1024, 1, &fixture) == 0);
    object = trevrpc_msquic_test_receive_fixture_take_stream(fixture, 0);
    assert(object != NULL);
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_attach_stream_object(transport, stream, object) == 0);
    trevrpc_rpc_transport_h3_test_capture_acceptances(transport, true);
    assert(
        trevrpc_rpc_transport_h3_test_emit(
            transport, TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC, 0, 0, listener, (trevrpc_rpc_transport_handle){0}, 0) ==
        0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, frame_len, false) == -EAGAIN);

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 200) == 0);
    assert(trevrpc_rpc_transport_h3_test_acceptance_count(transport) == 1);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
    trevrpc_msquic_test_receive_fixture_destroy(fixture);
}

static void test_h3_deferred_admission_reject_and_fail_closed(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    uint8_t frame[512];
    size_t frame_len = build_request_headers_frame(frame, sizeof(frame), "/rpc");
    uint16_t status = 0;
    size_t count = 0;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, frame_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == 0);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == -EALREADY);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_h3_test_last_rejection(transport, &status, &count) == 0);
    assert(status == 403 && count == 1);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_stream_close(transport, stream) == -ESTALE);
    trevrpc_rpc_transport_destroy(transport);

    transport = NULL;
    event = NULL;
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, frame_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_h3_test_last_rejection(transport, &status, &count) == 0);
    assert(status == 500 && count == 1);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_stream_close(transport, stream) == -ESTALE);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_deferred_webtransport_admission(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    trevrpc_rpc_transport_event_protocol_info protocol;
    uint8_t frame[512];
    size_t frame_len = build_connect_headers_frame(frame, sizeof(frame));
    uint16_t status = 0;
    size_t count = 0;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/", sizeof("/") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, frame, frame_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION);
    assert(handle_equal(info.parent, connection));
    assert(trevrpc_rpc_transport_event_get_admission_info(transport, event, &admission) == 0);
    assert(admission.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT);
    assert(handle_equal(admission.listener, listener));
    assert(admission.method_len == sizeof("CONNECT") - 1u &&
           memcmp(admission.method, "CONNECT", sizeof("CONNECT") - 1u) == 0);
    assert(admission.path_len == sizeof("/") - 1u && memcmp(admission.path, "/", sizeof("/") - 1u) == 0);
    assert(trevrpc_rpc_transport_event_get_protocol_info(transport, event, &protocol) == 0);
    assert(protocol.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    assert(trevrpc_rpc_transport_h3_test_last_rejection(transport, &status, &count) == 0);
    assert(status == 403 && count == 1);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_multiplexed_selects_http3_and_rejects_connect(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_event_protocol_info protocol;
    uint8_t post[512];
    uint8_t connect[512];
    size_t post_len = build_request_headers_frame(post, sizeof(post), "/rpc");
    size_t connect_len = build_connect_headers_frame(connect, sizeof(connect));
    size_t index;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    for (index = 0; index < 2; ++index) {
        assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
                   transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
        assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, post, post_len, false) == 0);
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (index == 0) {
            assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
            assert(handle_equal(info.subject, connection));
            assert(trevrpc_rpc_transport_event_get_protocol_info(transport, event, &protocol) == 0);
            assert(protocol.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3);
            trevrpc_rpc_transport_event_release(transport, event);
            event = NULL;
            assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
            assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        }
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
        assert(trevrpc_rpc_transport_admission_respond(transport, event, 200) == 0);
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
        assert(handle_equal(info.subject, stream));
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
        assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
        assert(handle_equal(info.subject, stream));
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
        assert(trevrpc_rpc_transport_release_handle(transport, stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) == 0);
    }
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/", sizeof("/") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, connect, connect_len, false) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_multiplexed_selects_webtransport_and_rejects_post(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle second_listener;
    trevrpc_rpc_transport_handle second_connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_event_protocol_info protocol;
    uint8_t connect[512];
    uint8_t post[512];
    size_t connect_len = build_connect_headers_frame(connect, sizeof(connect));
    size_t post_len = build_request_headers_frame(post, sizeof(post), "/rpc");

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/", sizeof("/") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, connect, connect_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;

    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, post, post_len, false) == -EPROTO);

    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &second_listener, &second_connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, second_connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, second_connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, second_connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, post, post_len, false) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(handle_equal(info.subject, second_connection));
    assert(trevrpc_rpc_transport_event_get_protocol_info(transport, event, &protocol) == 0);
    assert(protocol.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_multiplexed_post_waits_for_settings(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_event_protocol_info protocol;
    uint8_t post[512];
    size_t post_len = build_request_headers_frame(post, sizeof(post), "/rpc");

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/rpc", sizeof("/rpc") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, post, post_len, false) == -EAGAIN);
    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_retry_buffered_stream(transport, stream) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(handle_equal(info.subject, connection));
    assert(trevrpc_rpc_transport_event_get_protocol_info(transport, event, &protocol) == 0);
    assert(protocol.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3);
    trevrpc_rpc_transport_event_release(transport, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_multiplexed_connect_waits_for_settings(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    uint8_t connect[512];
    size_t connect_len = build_connect_headers_frame(connect, sizeof(connect));

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/", sizeof("/") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, connect, connect_len, false) == -EAGAIN);
    assert(trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
               transport, connection, TREV_WT_PROFILE_DRAFT_15) == 0);
    assert(trevrpc_rpc_transport_h3_test_retry_buffered_stream(transport, stream) == 0);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION);
    assert(trevrpc_rpc_transport_admission_respond(transport, event, 403) == 0);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);

    transport = NULL;
    event = NULL;
    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    trevrpc_rpc_transport_h3_test_capture_rejections(transport, true);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_deferred_admission(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/", sizeof("/") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, connect, connect_len, false) == -EAGAIN);
    assert(trevrpc_rpc_transport_h3_test_set_peer_settings_ready(transport, connection, true) == 0);
    assert(trevrpc_rpc_transport_h3_test_retry_buffered_stream(transport, stream) == -EPROTO);
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED);
    trevrpc_rpc_transport_event_release(transport, event);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_multiplexed_http3_only_rejects_connect(void) {
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    uint8_t connect[512];
    size_t connect_len = build_connect_headers_frame(connect, sizeof(connect));

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_server_connection(transport, &listener, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_connection_protocol(
               transport, connection, TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) == 0);
    assert(trevrpc_rpc_transport_h3_test_set_multiplexed_webtransport(transport, connection, false) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_peer_request_stream(
               transport, connection, "/", sizeof("/") - 1u, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_inject_data(transport, stream, connect, connect_len, false) == -EPROTO);
    trevrpc_rpc_transport_destroy(transport);
}

static void test_h3_shutdown_orders_pending_send_before_terminals_and_stopped(void) {
    trevrpc_rpc_transport* transport = NULL;
    trevrpc_rpc_transport_handle connection;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_config config = test_config();
    size_t position = 0;
    size_t send_position = 0;
    size_t stream_terminal_position = 0;
    size_t stopped_position = 0;

    assert(trevrpc_rpc_transport_h3_create(&config, &transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_connection(transport, &connection) == 0);
    assert(trevrpc_rpc_transport_h3_test_make_stream(transport, connection, &stream) == 0);
    assert(trevrpc_rpc_transport_h3_test_stage_pending_send(transport, stream, 77, 1, NULL) == 0);
    assert(trevrpc_rpc_transport_close(transport) == 0);
    assert(trevrpc_rpc_transport_h3_test_pending_events(transport) == 1);
    assert(trevrpc_rpc_transport_h3_test_complete_pending_send(transport, stream, 0) == 0);

    while (trevrpc_rpc_transport_next_event(transport, &event) == 0) {
        ++position;
        assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
        if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE) {
            assert(info.operation_id == 77);
            send_position = position;
        } else if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED) {
            stream_terminal_position = position;
        } else if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED) {
            stopped_position = position;
        }
        trevrpc_rpc_transport_event_release(transport, event);
        event = NULL;
    }
    assert(send_position != 0);
    assert(stream_terminal_position > send_position);
    assert(stopped_position > stream_terminal_position);
    trevrpc_rpc_transport_destroy(transport);
}

int main(void) {
    test_fragmented_h3_data_and_rpc_frame();
    test_h3_readable_reemits_after_event_pressure();
    test_h3_post_requires_configured_path();
    test_h3_parser_commits_headers_before_receive_retry();
    test_peer_wt_stream_replays_after_profile_and_session_resolution(false);
    test_peer_wt_stream_replays_after_profile_and_session_resolution(true);
    test_peer_wt_exact_prefix_replays_without_more_provider_data();
    test_unresolved_wt_stream_count_rejects_before_admission();
    test_unresolved_wt_stream_timeout_is_driver_scheduled();
    test_composite_translation_and_readable_tombstone();
    test_composite_generation_reuse();
    test_h3_registry_bounded_churn_reuses_generation();
    test_h3_retired_receive_drains_after_reuse();
    test_h3_stream_close_modes();
    test_msquic_finalizer_avoids_graceful_stream_convoy();
    test_h3_destroy_isolated_from_other_finalizer_scope();
    test_h3_destroy_waits_for_provider_owned_send_completion();
    test_h3_send_encoding_boundaries_and_rejection_cleanup();
    test_h3_finish_publishes_fin_before_provider_shutdown();
    test_h3_shutdown_orders_terminals_before_stopped();
    test_h3_abort_wire_codes_and_peer_reset_decode();
    test_h3_peer_receive_abort_after_request_fin_closes_stream();
    test_h3_receive_budget_retries_ordinary_parser();
    test_h3_receive_budget_retries_wt_session_replay();
    test_unresolved_wt_bytes_reject_too_small_provider_budget();
    test_unresolved_wt_bytes_share_provider_budget();
    test_resolved_wt_stream_leaves_unresolved_provider_budget();
    test_h3_receive_budget_retries_wt_post_session_data();
    test_h3_pre_ready_connection_failure_and_explicit_close();
    test_h3_control_frame_unexpected_and_connect_capsules();
    test_h3_duplicate_connect_failure_is_hidden();
    test_h3_accepted_connect_lifecycle();
    test_h3_qpack_encoder_accepts_zero_capacity_updates();
    test_h3_pre_ready_stream_failure_is_failed_event();
    test_h3_malformed_monolithic_data_is_rejected();
    test_h3_mandatory_events_survive_allocation_pressure();
    test_h3_reserved_send_and_terminal_order_under_pressure();
    test_h3_terminal_dequeue_controls_slot_reuse();
    test_h3_deferred_admission_snapshot_and_accept();
    test_h3_deferred_admission_buffers_body();
    test_h3_parent_ready_precedes_stream_readability();
    test_h3_deferred_admission_retries_after_event_capacity_returns();
    test_h3_deferred_admission_reject_and_fail_closed();
    test_h3_deferred_webtransport_admission();
    test_h3_multiplexed_selects_http3_and_rejects_connect();
    test_h3_multiplexed_selects_webtransport_and_rejects_post();
    test_h3_multiplexed_post_waits_for_settings();
    test_h3_multiplexed_connect_waits_for_settings();
    test_h3_multiplexed_http3_only_rejects_connect();
    test_shared_listener_rollback_closes_and_releases_child();
    test_composite_translates_admission_listener();
    test_h3_shutdown_orders_pending_send_before_terminals_and_stopped();
    puts("rpc_transport_h3_test: ok");
    return 0;
}
