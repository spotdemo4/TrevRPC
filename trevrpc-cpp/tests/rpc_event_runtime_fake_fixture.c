#include "rpc_event_runtime_fake_fixture.h"

#include "rpc_fake_transport_support.h"
#include "trevrpc_rpc_internal.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static fake_transport* fixture_transport(trevrpc_cpp_rpc_fake_fixture* fixture) {
    return (fake_transport*)fixture;
}

int trevrpc_cpp_rpc_fake_fixture_create(trevrpc_cpp_rpc_fake_fixture** out_fixture,
    trevrpc_rpc_runtime** out_runtime) {
    trevrpc_rpc_runtime_config_v1 config;
    fake_transport* fake;
    int result;
    if (out_fixture == NULL || out_runtime == NULL) {
        return -EINVAL;
    }
    fake = fake_create();
    if (fake == NULL) {
        return -ENOMEM;
    }
    result = trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config));
    if (result == 0) {
        result = trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, out_runtime);
    }
    if (result != 0) {
        fake_destroy(&fake->base);
        return result;
    }
    *out_fixture = (trevrpc_cpp_rpc_fake_fixture*)fake;
    return 0;
}

int trevrpc_cpp_rpc_fake_start_endpoint(trevrpc_cpp_rpc_fake_fixture* fixture,
    trevrpc_rpc_runtime* runtime,
    uint32_t mode,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint) {
    trevrpc_rpc_transport_endpoint_config config = {0};
    if (fixture == NULL || runtime == NULL) {
        return -EINVAL;
    }
    return trevrpc_rpc_runtime_start_transport_endpoint_v1(
        runtime, &config, mode, operation_id, out_endpoint);
}

void trevrpc_cpp_rpc_fake_set_close_result(trevrpc_cpp_rpc_fake_fixture* fixture, int result) {
    atomic_store_explicit(&fixture_transport(fixture)->close_result, result, memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_close_status(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
    atomic_store_explicit(&fixture_transport(fixture)->close_status, status, memory_order_release);
}

void trevrpc_cpp_rpc_fake_block_close(trevrpc_cpp_rpc_fake_fixture* fixture) {
    fake_transport* fake = fixture_transport(fixture);
    pthread_mutex_lock(&fake->mutex);
    fake->close_blocked = true;
    pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_wait_close_entered(trevrpc_cpp_rpc_fake_fixture* fixture) {
    fake_transport* fake = fixture_transport(fixture);
    pthread_mutex_lock(&fake->mutex);
    while (!fake->close_entered) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_release_close(trevrpc_cpp_rpc_fake_fixture* fixture) {
    fake_transport* fake = fixture_transport(fixture);
    pthread_mutex_lock(&fake->mutex);
    fake->close_release = true;
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
}

int trevrpc_cpp_rpc_fake_push_connection_ready(trevrpc_cpp_rpc_fake_fixture* fixture) {
    return fake_push_event(fixture_transport(fixture),
        TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
        fake_connection_handle,
        (trevrpc_rpc_transport_handle){0});
}

int trevrpc_cpp_rpc_fake_push_connection_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
    return fake_push_status_event(fixture_transport(fixture),
        TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
            TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
        fake_connection_handle,
        (trevrpc_rpc_transport_handle){0},
        status);
}

int trevrpc_cpp_rpc_fake_push_stream_ready(trevrpc_cpp_rpc_fake_fixture* fixture) {
    return fake_push_event(fixture_transport(fixture),
        TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
        fake_stream_handle,
        fake_connection_handle);
}

int trevrpc_cpp_rpc_fake_push_stream_readable(trevrpc_cpp_rpc_fake_fixture* fixture) {
    return fake_push_event(fixture_transport(fixture),
        TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
        fake_stream_handle,
        fake_connection_handle);
}

int trevrpc_cpp_rpc_fake_push_last_send_complete(trevrpc_cpp_rpc_fake_fixture* fixture) {
    fake_transport* fake = fixture_transport(fixture);
    uint64_t operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    if (operation_id == 0) {
        return -EAGAIN;
    }
    return fake_push_operation_event(fake,
        TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
        fake_stream_handle,
        fake_connection_handle,
        operation_id);
}

int trevrpc_cpp_rpc_fake_push_stream_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
    return fake_push_operation_status_event(fixture_transport(fixture),
        TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        fake_stream_handle,
        fake_connection_handle,
        0,
        status);
}

int trevrpc_cpp_rpc_fake_push_incoming(trevrpc_cpp_rpc_fake_fixture* fixture,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len) {
    fake_transport* fake = fixture_transport(fixture);
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    char metadata_key[] = "empty-incoming";
    trevrpc_metadata_entry metadata_entry = {
        .key = metadata_key,
        .key_len = sizeof(metadata_key) - 1u,
        .value = NULL,
        .value_len = 0,
    };
    trevrpc_metadata metadata = {
        .entries = &metadata_entry,
        .entries_len = 1,
    };
    int result;
    if (service == NULL || method == NULL || (body == NULL && body_len != 0)) {
        return -EINVAL;
    }
    result = trevrpc_wire_encode_request_view(service,
        strlen(service),
        method,
        strlen(method),
        kind,
        TREVRPC_RPC_ABI_VERSION,
        body,
        body_len,
        &metadata,
        TREVRPC_RPC_DEADLINE_INFINITE,
        1024u * 1024u,
        &frame,
        &frame_len);
    if (result != 0) {
        return result;
    }
    if (frame_len < 4) {
        free(frame);
        return -EPROTO;
    }
    result = fake_push_incoming_stream(fake, frame + 4, frame_len - 4);
    free(frame);
    return result;
}

void trevrpc_cpp_rpc_fake_wait_stream_open_calls(
    trevrpc_cpp_rpc_fake_fixture* fixture, unsigned int minimum_calls) {
    fake_transport* fake = fixture_transport(fixture);
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->stream_open_calls, memory_order_acquire) < minimum_calls) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_wait_stream_send_calls(
    trevrpc_cpp_rpc_fake_fixture* fixture, unsigned int minimum_calls) {
    fake_transport* fake = fixture_transport(fixture);
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) < minimum_calls) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}
