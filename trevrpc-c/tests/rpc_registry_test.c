#include "trevrpc_rpc_internal.h"

#include <assert.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>

#include "rpc_fake_transport_support.h"

static trevrpc_rpc_event* next_event(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_wake_source_v1* wake) {
    for (;;) {
        trevrpc_rpc_event* event = NULL;
        int result = trevrpc_rpc_runtime_next_event(runtime, &event);
        if (result == 0) {
            return event;
        }
        assert(result == -EAGAIN);
        {
            struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN};
            assert(poll(&descriptor, 1, 1000) == 1);
        }
    }
}

static void run_operation_scope_lookup(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&runtime_config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.service = "registry.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "Scope";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 10, &call, &stream) == 0);
    assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 10, (const uint8_t*)"later", 5, 0) == -EALREADY);
    assert(trevrpc_rpc_stream_close(runtime, stream, 10, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == -EALREADY);
    assert(trevrpc_rpc_call_close(runtime, call, 10, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == -EALREADY);
    assert(trevrpc_rpc_call_close(runtime, call, 11, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == 0);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    trevrpc_rpc_event_release(event);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 12) == 0);
    event = next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 13) == 0);
    event = next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_deadline_heap_expiry(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&runtime_config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    event = next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.service = "registry.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "Deadline";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.timeout_nanos = UINT64_C(20000000);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 20, &call, &stream) == 0);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    assert(info.status == -ETIMEDOUT);
    trevrpc_rpc_event_release(event);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.status == -ETIMEDOUT);
    trevrpc_rpc_event_release(event);
    event = next_event(runtime, &wake);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.status == -ETIMEDOUT);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 21) == 0);
    event = next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 22) == 0);
    event = next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_cancellation_registry_churn(void) {
    enum { CAPACITY = 4096, CHURN = 3500 };
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_cancellation_v1 handles[CHURN];
    size_t index;
    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.endpoint_capacity = CAPACITY;
    config.call_capacity = CAPACITY;
    config.stream_capacity = CAPACITY;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    for (index = 0; index < CHURN; ++index) {
        assert(trevrpc_rpc_cancellation_create(runtime, &handles[index]) == 0);
    }
    for (index = 0; index < CHURN; index += 2) {
        assert(trevrpc_rpc_cancellation_release(runtime, handles[index]) == 0);
        assert(trevrpc_rpc_cancellation_cancel(runtime, handles[index], 1) == -ESTALE);
    }
    for (index = 1; index < CHURN; index += 2) {
        assert(trevrpc_rpc_cancellation_release(runtime, handles[index]) == 0);
        assert(trevrpc_rpc_cancellation_release(runtime, handles[index]) == -ESTALE);
    }
    for (index = 0; index < CHURN; ++index) {
        assert(trevrpc_rpc_cancellation_create(runtime, &handles[index]) == 0);
    }
    for (index = 0; index < CHURN; ++index) {
        assert(trevrpc_rpc_cancellation_release(runtime, handles[index]) == 0);
        assert(trevrpc_rpc_cancellation_cancel(runtime, handles[index], 1) == -ESTALE);
    }
    assert(trevrpc_rpc_runtime_close(runtime, 900) == 0);
    {
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_event* event;
        trevrpc_rpc_event_info_v1 info;
        assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
        assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
        event = next_event(runtime, &wake);
        assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
        assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    }
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

int main(void) {
    run_operation_scope_lookup();
    run_deadline_heap_expiry();
    run_cancellation_registry_churn();
    return 0;
}
