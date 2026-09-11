#include "trevrpc_rpc_internal.h"
#include "trevrpc_wire_internal.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "rpc_fake_transport_support.h"

typedef struct rpc_receive_args {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive;
    int result;
} rpc_receive_args;

static void* rpc_receive_thread(void* context) {
    rpc_receive_args* args = context;
    args->result = trevrpc_rpc_stream_receive(args->runtime, args->stream, &args->receive);
    return NULL;
}

typedef struct endpoint_close_args {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_endpoint_v1 endpoint;
    uint64_t operation_id;
    int result;
} endpoint_close_args;

static void* endpoint_close_thread(void* context) {
    endpoint_close_args* args = context;
    args->result = trevrpc_rpc_endpoint_close(args->runtime, args->endpoint, args->operation_id);
    return NULL;
}

static trevrpc_rpc_event* wait_next_event(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_wake_source_v1* wake) {
    int attempt;
    for (attempt = 0; attempt < 1000; ++attempt) {
        trevrpc_rpc_event* event = NULL;
        int result = trevrpc_rpc_runtime_next_event(runtime, &event);
        if (result == 0) {
            return event;
        }
        assert(result == -EAGAIN);
        {
            struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN, .revents = 0};
            assert(poll(&descriptor, 1, 10) >= 0);
        }
    }
    assert(false);
    return NULL;
}

static trevrpc_rpc_event_info_v1 event_info(trevrpc_rpc_event* event) {
    trevrpc_rpc_event_info_v1 info;
    assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    return info;
}

static void consume_endpoint_ready(
    trevrpc_rpc_runtime* runtime, const trevrpc_rpc_wake_source_v1* wake, trevrpc_rpc_endpoint_v1 endpoint) {
    trevrpc_rpc_event* event = wait_next_event(runtime, wake);
    trevrpc_rpc_event_info_v1 info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_ENDPOINT);
    assert(info.operation_id == 1);
    assert(info.endpoint.owner == endpoint.owner);
    assert(info.endpoint.slot == endpoint.slot);
    assert(info.endpoint.generation == endpoint.generation);
    trevrpc_rpc_event_release(event);
}

static void make_request_kind(
    const char* method, uint32_t kind, uint64_t timeout_nanos, uint8_t** out_data, size_t* out_data_len) {
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    assert(trevrpc_wire_encode_request_view("fake.Service",
               strlen("fake.Service"),
               method,
               strlen(method),
               kind,
               TREVRPC_RPC_ABI_VERSION,
               (const uint8_t*)"request",
               strlen("request"),
               NULL,
               timeout_nanos,
               1024u * 1024u,
               &frame,
               &frame_len) == 0);
    assert(frame_len >= 4);
    *out_data = malloc(frame_len - 4);
    assert(*out_data != NULL);
    memcpy(*out_data, frame + 4, frame_len - 4);
    *out_data_len = frame_len - 4;
    free(frame);
}

static void make_request(const char* method, uint64_t timeout_nanos, uint8_t** out_data, size_t* out_data_len) {
    make_request_kind(method, TREVRPC_RPC_KIND_UNARY, timeout_nanos, out_data, out_data_len);
}

static void assert_initial_request_uses_infinite_wire_timeout(fake_transport* fake) {
    trevrpc_wire_request_values request = {0};
    trevrpc_wire_request_diagnostic diagnostic = {0};
    pthread_mutex_lock(&fake->mutex);
    assert(fake->last_send_body != NULL);
    assert(trevrpc_wire_decode_request_diagnostic(
               fake->last_send_body, fake->last_send_body_len, &request, &diagnostic) == 0);
    assert(request.timeout_nanos == 0);
    trevrpc_internal_request_reset(&request);
    pthread_mutex_unlock(&fake->mutex);
}

static void assert_last_send_is_ok_status(fake_transport* fake) {
    trevrpc_wire_stream_frame_values* frame = NULL;
    trevrpc_wire_diagnostic diagnostic = {0};
    pthread_mutex_lock(&fake->mutex);
    assert(fake->last_send_body != NULL);
    assert(trevrpc_wire_decode_stream_frame_diagnostic(
               fake->last_send_body, fake->last_send_body_len, &frame, &diagnostic) == 0);
    assert(frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
    assert(frame->status == TREVRPC_RPC_STATUS_OK);
    assert(frame->body.len == 0);
    trevrpc_internal_stream_frame_free(frame);
    pthread_mutex_unlock(&fake->mutex);
}

static void setup_client_call(fake_transport* fake,
    trevrpc_rpc_runtime** out_runtime,
    trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1* endpoint,
    trevrpc_rpc_call_v1* call,
    trevrpc_rpc_stream_v1* stream,
    uint32_t kind,
    uint64_t operation_id) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_call_config_v1 call_config;
    int attempt;
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, out_runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(wake, sizeof(*wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(*out_runtime, wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               *out_runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(*out_runtime, wake, *endpoint);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    assert(call_config.timeout_nanos == TREVRPC_RPC_DEADLINE_INFINITE);
    call_config.kind = kind;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "Test";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(*out_runtime, *endpoint, &call_config, operation_id, call, stream) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) != 0) {
            assert_initial_request_uses_infinite_wire_timeout(fake);
            return;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(false);
}

static void setup_ready_client_call_with_config(fake_transport* fake,
    const trevrpc_rpc_runtime_config_v1* runtime_config,
    trevrpc_rpc_call_config_v1* call_config,
    trevrpc_rpc_runtime** out_runtime,
    trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1* endpoint,
    trevrpc_rpc_call_v1* call,
    trevrpc_rpc_stream_v1* stream) {
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_event* event;
    int attempt;

    assert(trevrpc_rpc_runtime_adopt_transport_v1(runtime_config, &fake->base, out_runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(wake, sizeof(*wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(*out_runtime, wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               *out_runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(*out_runtime, wake, *endpoint);
    assert(trevrpc_rpc_call_open_v1(*out_runtime, *endpoint, call_config, 2, call, stream) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 1) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               2) == 0);
    event = wait_next_event(*out_runtime, wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);
}

static void setup_accepted_peer_call(fake_transport* fake,
    const trevrpc_rpc_runtime_config_v1* runtime_config,
    uint32_t kind,
    trevrpc_rpc_runtime** out_runtime,
    trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1* endpoint,
    trevrpc_rpc_call_v1* call,
    trevrpc_rpc_stream_v1* stream) {
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_event* event;
    trevrpc_rpc_receive* receive = NULL;
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(trevrpc_rpc_runtime_adopt_transport_v1(runtime_config, &fake->base, out_runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(wake, sizeof(*wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(*out_runtime, wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               *out_runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, endpoint) == 0);
    consume_endpoint_ready(*out_runtime, wake, *endpoint);
    make_request_kind("ServerLimit", kind, TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    event = wait_next_event(*out_runtime, wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(trevrpc_rpc_event_take_incoming_call(event, call, stream, &receive) == 0);
    trevrpc_rpc_receive_release(receive);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_accept(*out_runtime, *call, 2) == 0);
    event = wait_next_event(*out_runtime, wake);
    if (event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
        trevrpc_rpc_event_release(event);
        event = wait_next_event(*out_runtime, wake);
    }
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);
    trevrpc_rpc_event_release(event);
}

static void close_ready_client_call(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1 endpoint,
    trevrpc_rpc_call_v1 call,
    trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void wait_for_receive_terminals_processed(fake_transport* fake) {
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->receive_fin_release_calls, memory_order_acquire) == 0 ||
           atomic_load_explicit(&fake->stream_terminal_release_calls, memory_order_acquire) == 0) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

static void wait_for_stream_terminal_processed(fake_transport* fake) {
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->stream_terminal_release_calls, memory_order_acquire) == 0) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

static void push_stream_frame(
    fake_transport* fake, uint32_t kind, uint32_t status, const uint8_t* body, size_t body_len) {
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    assert(trevrpc_wire_encode_stream_frame(
               kind, status, NULL, 0, body, body_len, NULL, 1024u * 1024u, &frame, &frame_len) == 0);
    assert(frame_len >= 4);
    assert(fake_push_receive(fake, frame + 4, frame_len - 4) == 0);
    free(frame);
}

static void push_unary_response(fake_transport* fake, uint32_t status, const uint8_t* body, size_t body_len) {
    trevrpc_wire_response_values response = {0};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    response.status = status;
    response.body.data = (uint8_t*)body;
    response.body.len = body_len;
    assert(trevrpc_wire_encode_response(&response, 1024u * 1024u, &frame, &frame_len) == 0);
    assert(frame_len >= 4);
    assert(fake_push_receive(fake, frame + 4, frame_len - 4) == 0);
    free(frame);
}

static void finish_call_and_runtime_after_close(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1 endpoint,
    trevrpc_rpc_call_v1 call,
    trevrpc_rpc_stream_v1 stream,
    uint64_t close_application_error_code) {
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    event = wait_next_event(runtime, wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.stream.owner == stream.owner);
    assert(info.stream.slot == stream.slot);
    assert(info.stream.generation == stream.generation);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.call.owner == call.owner);
    assert(info.call.slot == call.slot);
    assert(info.call.generation == call.generation);
    assert(info.operation_id == 3);
    assert(info.application_error_code == close_application_error_code);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);

    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 4) == 0);
    event = wait_next_event(runtime, wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    assert(info.endpoint.owner == endpoint.owner);
    assert(info.endpoint.slot == endpoint.slot);
    assert(info.endpoint.generation == endpoint.generation);
    assert(info.operation_id == 4);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);

    assert(trevrpc_rpc_runtime_close(runtime, 5) == 0);
    event = wait_next_event(runtime, wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.operation_id == 5);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void finish_call_and_runtime(fake_transport* fake,
    trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1 endpoint,
    trevrpc_rpc_call_v1 call,
    trevrpc_rpc_stream_v1 stream,
    uint32_t close_flags,
    uint64_t close_application_error_code) {
    assert(trevrpc_rpc_call_close(runtime, call, 3, close_flags, close_application_error_code) == 0);
    if ((close_flags & TREVRPC_RPC_CLOSE_FLAG_ABORT) != 0) {
        assert(atomic_load_explicit(&fake->last_abort_error, memory_order_acquire) == close_application_error_code);
    }
    finish_call_and_runtime_after_close(runtime, wake, endpoint, call, stream, close_application_error_code);
}

static void run_finish_send_status_then_fin(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    int attempt;

    assert(fake != NULL);
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_CLIENT_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_finish_send(runtime, stream, 6) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    assert(atomic_load_explicit(&fake->stream_finish_send_calls, memory_order_acquire) == 0);
    assert_last_send_is_ok_status(fake);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_FINISHED);
    assert(info.operation_id == 6);
    assert(info.status == 0);
    trevrpc_rpc_event_release(event);
    assert(atomic_load_explicit(&fake->stream_finish_send_calls, memory_order_acquire) == 1);
    assert(trevrpc_rpc_stream_finish_send(runtime, stream, 7) == -EALREADY);

    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_lifecycle_operation_id_scope(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 41, &endpoint) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 41) == -EALREADY);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 42) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED);
    assert(info.operation_id == 41);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    assert(info.operation_id == 42);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 43) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);

    fake = fake_create();
    runtime = NULL;
    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "Scoped";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 51, &call, &stream) == 0);
    assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 51, (const uint8_t*)"later", 5, 0) == -EALREADY);
    assert(trevrpc_rpc_stream_close(runtime, stream, 51, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == -EALREADY);
    assert(trevrpc_rpc_call_close(runtime, call, 51, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == -EALREADY);
    assert(trevrpc_rpc_call_close(runtime, call, 52, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    assert(info.operation_id == 51);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.operation_id == 0);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.operation_id == 52);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 53) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 54) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_send_operation_id_collision_and_reuse(void) {
    static const uint8_t first[] = "first";
    static const uint8_t second[] = "second";
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t initial_transport_operation_id;
    uint64_t first_transport_operation_id;
    uint64_t second_transport_operation_id;
    int attempt;
    int result = -EALREADY;

    assert(fake != NULL);
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, 2);
    initial_transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(initial_transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               initial_transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    assert(info.status == 0);
    trevrpc_rpc_event_release(event);

    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 1);
    assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 16, first, sizeof(first) - 1, 0) == 0);
    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2);
    first_transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(first_transport_operation_id != 0);
    assert(first_transport_operation_id != initial_transport_operation_id);

    assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 16, second, sizeof(second) - 1, 0) == -EALREADY);
    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2);
    assert(atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire) == first_transport_operation_id);

    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               first_transport_operation_id) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        result = trevrpc_rpc_stream_send_copy_v1(runtime, stream, 16, second, sizeof(second) - 1, 0);
        if (result == 0)
            break;
        assert(result == -EALREADY);
        assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2);
        assert(
            atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire) == first_transport_operation_id);
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt < 1000);
    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 3);
    second_transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(second_transport_operation_id != 0);
    assert(second_transport_operation_id != first_transport_operation_id);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_STREAM);
    assert(info.operation_id == 16);
    assert(info.status == 0);
    assert(info.stream.owner == stream.owner);
    assert(info.stream.slot == stream.slot);
    assert(info.stream.generation == stream.generation);
    trevrpc_rpc_event_release(event);

    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               second_transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_STREAM);
    assert(info.operation_id == 16);
    assert(info.status == 0);
    assert(info.stream.owner == stream.owner);
    assert(info.stream.slot == stream.slot);
    assert(info.stream.generation == stream.generation);
    trevrpc_rpc_event_release(event);

    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_late_reused_operation_id(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_call_v1 reused_call;
    trevrpc_rpc_stream_v1 reused_stream;
    trevrpc_rpc_call_config_v1 config;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t old_transport_id;
    uint64_t new_transport_id;
    int attempt;
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_UNARY, 2);
    assert(trevrpc_rpc_runtime_drain(runtime) == -EBUSY);
    old_transport_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_call_config_v1_init(&config, sizeof(config)) == 0);
    config.kind = TREVRPC_RPC_KIND_UNARY;
    config.service = "fake.Service";
    config.service_len = (uint32_t)strlen(config.service);
    config.method = "Reuse";
    config.method_len = (uint32_t)strlen(config.method);
    config.initial_message = (const uint8_t*)"request";
    config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &config, 100, &reused_call, &reused_stream) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt < 1000);
    new_transport_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(new_transport_id != old_transport_id);
    assert(fake_push_operation_status_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               old_transport_id,
               -EIO) == 0);
    assert(fake_push_operation_status_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               new_transport_id,
               0) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 100);
    assert(info.status == 0);
    trevrpc_rpc_event_release(event);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, reused_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, reused_call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_failed_request_send_complete(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_UNARY, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_status_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id,
               -EIO) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    assert(info.operation_id == 2);
    assert(info.status == -EIO);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_send_stopped_preserves_receive_direction(void) {
    static const uint8_t outbound[] = "outbound";
    static const uint8_t inbound[] = "inbound";
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    int attempt;

    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 6, outbound, sizeof(outbound) - 1, 0) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt < 1000);
    assert(fake_push_operation_status_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET,
               fake_stream_handle,
               fake_connection_handle,
               0,
               -ECANCELED) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_STREAM);
    assert(info.operation_id == 6);
    assert(info.status == -ECANCELED);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_TERMINAL) != 0);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_PEER_RESET) != 0);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_PEER_CANCELLED) != 0);
    trevrpc_rpc_event_release(event);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 7, outbound, sizeof(outbound) - 1, 0) == -EPIPE);

    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, inbound, sizeof(inbound) - 1);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.data_len == sizeof(inbound) - 1);
    assert(memcmp(receive_info.data, inbound, sizeof(inbound) - 1) == 0);
    trevrpc_rpc_receive_release(receive);

    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_peer_stopped_local_finish_is_settled(bool synchronous_failure) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;

    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);

    if (synchronous_failure) {
        atomic_store_explicit(&fake->stream_send_result, -EPIPE, memory_order_release);
    }
    assert(trevrpc_rpc_stream_finish_send(runtime, stream, 6) == 0);
    if (!synchronous_failure) {
        transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
        assert(fake_push_operation_status_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                   fake_stream_handle,
                   fake_connection_handle,
                   transport_operation_id,
                   -EPIPE) == 0);
    }
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_FINISHED);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_STREAM);
    assert(info.operation_id == 6);
    assert(info.status == 0);
    assert(info.flags == (TREVRPC_RPC_EVENT_FLAG_LOCAL | TREVRPC_RPC_EVENT_FLAG_CLIENT));
    trevrpc_rpc_event_release(event);

    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_send_stopped_readies_initial_request(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;

    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, 2);
    assert(fake_push_operation_status_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET,
               fake_stream_handle,
               fake_connection_handle,
               0,
               -ECANCELED) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_CALL);
    assert(info.operation_id == 2);
    assert(info.status == 0);
    assert(info.flags == (TREVRPC_RPC_EVENT_FLAG_LOCAL | TREVRPC_RPC_EVENT_FLAG_CLIENT));
    trevrpc_rpc_event_release(event);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_coalesced_streaming_response_before_terminal(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive* detached_receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event = NULL;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    const uint8_t response[] = "summary";

    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_CLIENT_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    assert(info.stream.owner == stream.owner);
    assert(info.stream.slot == stream.slot);
    assert(info.stream.generation == stream.generation);
    trevrpc_rpc_event_release(event);
    wait_for_receive_terminals_processed(fake);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &detached_receive) == 0);
    assert(atomic_load_explicit(&fake->receive_release_calls, memory_order_acquire) != 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(detached_receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.data_len == sizeof(response) - 1);
    assert(memcmp(receive_info.data, response, receive_info.data_len) == 0);
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
    assert(receive_info.data_len == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(detached_receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.data_len == sizeof(response) - 1);
    assert(memcmp(receive_info.data, response, receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(detached_receive);
}

static void run_clean_peer_terminal_settles_local_finish(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event = NULL;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    const uint8_t response[] = "summary";
    int attempt;

    assert(fake != NULL);
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_CLIENT_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_finish_send(runtime, stream, 6) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    assert(atomic_load_explicit(&fake->stream_finish_send_calls, memory_order_acquire) == 0);

    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.data_len == sizeof(response) - 1);
    assert(memcmp(receive_info.data, response, receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_FINISHED);
    assert(info.operation_id == 6);
    assert(info.status == 0);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(atomic_load_explicit(&fake->stream_finish_send_calls, memory_order_acquire) == 0);

    close_ready_client_call(runtime, &wake, endpoint, call, stream);
}

static void run_clean_terminal_waits_for_late_receive_fin(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event = NULL;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    const uint8_t response[] = "summary";

    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_CLIENT_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);

    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    event = NULL;

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);
    wait_for_stream_terminal_processed(fake);
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);

    close_ready_client_call(runtime, &wake, endpoint, call, stream);
}

typedef enum response_terminal_resolution {
    RESPONSE_TERMINAL_LATE_FIN,
    RESPONSE_TERMINAL_OPTIONAL_FIN,
    RESPONSE_TERMINAL_GRACEFUL_CLOSE,
    RESPONSE_TERMINAL_ABORT_CLOSE,
} response_terminal_resolution;

static void run_terminal_before_drained_response(uint32_t kind, response_terminal_resolution resolution) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event = NULL;
    uint64_t transport_operation_id;
    const uint8_t response[] = "late-fin";
    bool close_locally = resolution == RESPONSE_TERMINAL_GRACEFUL_CLOSE || resolution == RESPONSE_TERMINAL_ABORT_CLOSE;
    uint32_t close_flags =
        resolution == RESPONSE_TERMINAL_ABORT_CLOSE ? TREVRPC_RPC_CLOSE_FLAG_ABORT : TREVRPC_RPC_CLOSE_FLAG_NONE;
    uint32_t terminal_flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                              TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT;

    assert(kind == TREVRPC_RPC_KIND_UNARY || kind == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
           kind == TREVRPC_RPC_KIND_SERVER_STREAMING);
    if (resolution != RESPONSE_TERMINAL_OPTIONAL_FIN) {
        terminal_flags |= TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
    }
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, kind, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);

    if (kind == TREVRPC_RPC_KIND_UNARY) {
        push_unary_response(fake, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
    } else {
        push_stream_frame(
            fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
        push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    }
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               terminal_flags,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    wait_for_stream_terminal_processed(fake);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
    assert(receive_info.data_len == sizeof(response) - 1);
    assert(memcmp(receive_info.data, response, receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    if (kind != TREVRPC_RPC_KIND_UNARY) {
        assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
        assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
        assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
        assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
        trevrpc_rpc_receive_release(receive);
        receive = NULL;
    }
    atomic_store_explicit(&fake->stream_receive_result, -EPIPE, memory_order_release);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
    if (resolution == RESPONSE_TERMINAL_OPTIONAL_FIN) {
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
        trevrpc_rpc_event_release(event);
        close_ready_client_call(runtime, &wake, endpoint, call, stream);
        return;
    }
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    if (!close_locally) {
        assert(fake_push_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                       TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
                   fake_stream_handle,
                   fake_connection_handle) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
        trevrpc_rpc_event_release(event);
        close_ready_client_call(runtime, &wake, endpoint, call, stream);
        return;
    }

    assert(trevrpc_rpc_call_close(runtime, call, 3, close_flags, 91) == 0);
    finish_call_and_runtime_after_close(runtime, &wake, endpoint, call, stream, 91);
}

static void run_trailing_response_after_status(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event;
    uint64_t transport_operation_id;
    const uint8_t trailing[] = "trailing";

    assert(fake != NULL);
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_SERVER_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);

    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, trailing, sizeof(trailing) - 1);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EPROTO);
    assert(receive == NULL);
    assert(trevrpc_rpc_stream_last_receive_diagnostic(runtime, stream) == TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 1);
    assert(atomic_load_explicit(&fake->last_abort_error, memory_order_acquire) == TREVRPC_RPC_STATUS_INTERNAL);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_receive_allocation_failure_rearms_readable(bool fail_before_transfer) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event = NULL;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    const uint8_t response[] = "retry";

    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_CLIENT_STREAMING, 2);
    transport_operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    wait_for_receive_terminals_processed(fake);

    if (fail_before_transfer) {
        atomic_store_explicit(&fake->stream_receive_result, -ENOMEM, memory_order_release);
    } else {
        trevrpc_rpc_internal_test_fail_next_receive_copy(runtime);
    }
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -ENOMEM);
    assert(receive == NULL);
    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    assert(info.stream.owner == stream.owner);
    assert(info.stream.slot == stream.slot);
    assert(info.stream.generation == stream.generation);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.data_len == sizeof(response) - 1);
    assert(memcmp(receive_info.data, response, receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_OK);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);

    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_coalesced_peer_streaming_request_before_terminal(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event = NULL;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    uint8_t* request = NULL;
    size_t request_len = 0;
    const uint8_t message[] = "next";

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request_kind(
        "Coalesced", TREVRPC_RPC_KIND_CLIENT_STREAMING, TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, message, sizeof(message) - 1);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(info.rpc_kind == TREVRPC_RPC_KIND_CLIENT_STREAMING);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_accept(runtime, call, 2) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);
    wait_for_receive_terminals_processed(fake);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
    assert(receive_info.data_len == sizeof(message) - 1);
    assert(memcmp(receive_info.data, message, receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_peer_request_status_rejection(bool trailing_frame) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    uint8_t* request = NULL;
    size_t request_len = 0;
    const uint8_t invalid[] = "invalid";

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request_kind(
        "StatusRejection", TREVRPC_RPC_KIND_CLIENT_STREAMING, TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    push_stream_frame(fake,
        TREVRPC_STREAM_FRAME_KIND_STATUS,
        TREVRPC_RPC_STATUS_OK,
        trailing_frame ? NULL : invalid,
        trailing_frame ? 0 : sizeof(invalid) - 1);
    if (trailing_frame) {
        push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, invalid, sizeof(invalid) - 1);
    }
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_accept(runtime, call, 2) == 0);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EPROTO);
    assert(receive == NULL);
    assert(trevrpc_rpc_stream_last_receive_diagnostic(runtime, stream) == TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 1);
    assert(atomic_load_explicit(&fake->last_abort_error, memory_order_acquire) == TREVRPC_RPC_STATUS_INTERNAL);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_peer_terminal_before_initial_readable(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    make_request("TerminalFirst", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(info.method_len == strlen("TerminalFirst"));
    assert(memcmp(info.method, "TerminalFirst", info.method_len) == 0);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
    assert(receive_info.data_len == strlen("request"));
    trevrpc_rpc_receive_release(receive);
    trevrpc_rpc_event_release(event);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_receive_fin_before_ready(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    uint8_t* request = NULL;
    size_t request_len = 0;

    fake->signal_alternate_wake = true;
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 4;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request("BeforeReady", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_HAS_INCOMING_CALL) != 0);
    assert(info.service_len == strlen("fake.Service"));
    assert(memcmp(info.service, "fake.Service", info.service_len) == 0);
    assert(info.method_len == strlen("BeforeReady"));
    assert(memcmp(info.method, "BeforeReady", info.method_len) == 0);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
    assert(receive_info.data_len == strlen("request"));
    assert(memcmp(receive_info.data, "request", receive_info.data_len) == 0);
    trevrpc_rpc_receive_release(receive);
    trevrpc_rpc_event_release(event);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
    assert(info.stream.owner == stream.owner);
    assert(info.stream.slot == stream.slot);
    assert(info.stream.generation == stream.generation);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_call_accept(runtime, call, 2) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(trevrpc_rpc_call_respond_copy_v1(runtime, call, 100, &status, NULL, 0) == -EINVAL);
    status.code = TREVRPC_RPC_STATUS_INTERNAL;
    assert(trevrpc_rpc_call_respond_copy_v1(runtime, call, 101, &status, (const uint8_t*)"", 0) == -EINVAL);

    finish_call_and_runtime(
        fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_ABORT, UINT64_C(0x12345678));
}

static void wait_for_readable_info_calls(fake_transport* fake, unsigned expected) {
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->readable_info_calls, memory_order_acquire) < expected) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

static void wait_for_readable_release_calls(fake_transport* fake, unsigned expected) {
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->readable_release_calls, memory_order_acquire) < expected) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

static void wait_for_stream_abort(fake_transport* fake) {
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
    assert(atomic_load_explicit(&fake->last_abort_error, memory_order_acquire) == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
}

static void assert_deadline_terminal(const trevrpc_rpc_event_info_v1* info) {
    assert((info->flags & TREVRPC_RPC_EVENT_FLAG_LOCAL) != 0);
    assert(info->status == -ETIMEDOUT);
    assert(info->rpc_status == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
    assert(info->application_error_code == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
}

static void run_readable_backpressure_retry(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    uint8_t* request = NULL;
    size_t request_len = 0;
    bool accepted = false;
    bool readable = false;

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 1;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request("BeforeReady", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    wait_for_readable_info_calls(fake, 2);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_accept(runtime, call, 2) == 0);

    while (!accepted || !readable) {
        event = wait_next_event(runtime, &wake);
        info = event_info(event);
        if (info.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED) {
            assert(info.operation_id == 2);
            accepted = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
            assert(info.stream.owner == stream.owner);
            assert(info.stream.slot == stream.slot);
            assert(info.stream.generation == stream.generation);
            readable = true;
        } else {
            assert(false);
        }
        trevrpc_rpc_event_release(event);
    }
    wait_for_readable_release_calls(fake, 2);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_unary_error_status_receive(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_wire_response_values response = {0};
    fake_transport* fake = fake_create();
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int attempt;

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 4;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "ErrorStatus";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 2, &call, &stream) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 1) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt < 1000);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               2) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    response.status = TREVRPC_RPC_STATUS_PERMISSION_DENIED;
    response.message = "denied";
    response.message_len = strlen(response.message);
    assert(trevrpc_wire_encode_response(&response, 1024u * 1024u, &frame, &frame_len) == 0);
    assert(frame_len >= 4);
    assert(fake_push_receive(fake, frame + 4, frame_len - 4) == 0);
    free(frame);
    frame = NULL;
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
    assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
    assert(receive_info.kind == TREVRPC_RPC_RECEIVE_STATUS);
    assert(receive_info.rpc_status == TREVRPC_RPC_STATUS_PERMISSION_DENIED);
    assert(receive_info.data_len == 0);
    assert(receive_info.message_len == strlen("denied"));
    assert(memcmp(receive_info.message, "denied", receive_info.message_len) == 0);
    trevrpc_rpc_receive_release(receive);
    receive = NULL;
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);

    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_outgoing_deadline_before_stream_ready(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_call_context_info_v1 context;
    fake_transport* fake = fake_create();

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 4;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "DeadlineBeforeReady";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.timeout_nanos = UINT64_C(20000000);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 2, &call, &stream) == 0);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE) != 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) == 0);
    assert(context.time_remaining_nanos != 0);
    context.reserved0 = 1;
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == -EINVAL);
    context.reserved0 = 0;

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & (TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE | TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED |
                                TREVRPC_RPC_CALL_CONTEXT_CANCELLED)) ==
           (TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE | TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED |
               TREVRPC_RPC_CALL_CONTEXT_CANCELLED));
    assert(context.time_remaining_nanos == 0);
    assert(info.operation_id == 2);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_LOCAL) != 0);
    assert(info.status == -ETIMEDOUT);
    assert(info.application_error_code == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
    trevrpc_rpc_event_release(event);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.stream.owner == stream.owner);
    assert_deadline_terminal(&info);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.call.owner == call.owner);
    assert_deadline_terminal(&info);
    trevrpc_rpc_event_release(event);
    wait_for_stream_abort(fake);
    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 0);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    assert(info.operation_id == 3);
    trevrpc_rpc_event_release(event);
    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 0);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);

    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.operation_id == 4);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_deadline_while_queue_full(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 1;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request("QueueDeadline", UINT64_C(500000000), &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(info.method_len == strlen("QueueDeadline"));
    assert(memcmp(info.method, "QueueDeadline", info.method_len) == 0);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    trevrpc_rpc_receive_release(receive);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_call_accept(runtime, call, 2) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    wait_for_readable_info_calls(fake, 3);
    wait_for_stream_abort(fake);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert_deadline_terminal(&info);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert_deadline_terminal(&info);
    trevrpc_rpc_event_release(event);
    wait_for_readable_release_calls(fake, 3);
    assert(trevrpc_rpc_call_cancel(runtime, call, 99, TREVRPC_RPC_STATUS_CANCELLED) == -EALREADY);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);

    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.operation_id == 3);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_endpoint_terminal_before_ready(uint32_t transport_event_kind) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 1;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_status_event(fake,
               transport_event_kind,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0},
               -ENOTSUP) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED);
    assert(info.subject_kind == TREVRPC_RPC_OBJECT_ENDPOINT);
    assert(info.operation_id == 1);
    assert(info.status == -ENOTSUP);
    assert(info.endpoint.owner == endpoint.owner);
    assert(info.endpoint.slot == endpoint.slot);
    assert(info.endpoint.generation == endpoint.generation);
    trevrpc_rpc_event_release(event);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    assert(info.operation_id == 0);
    assert(info.status == -ENOTSUP);
    assert(info.endpoint.owner == endpoint.owner);
    assert(info.endpoint.slot == endpoint.slot);
    assert(info.endpoint.generation == endpoint.generation);
    trevrpc_rpc_event_release(event);

    fake->call_release_handle_result = -EIO;
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == -EIO);
    fake->call_release_handle_result = 0;
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(fake_release_handle_count(fake) == 2);
    assert(trevrpc_rpc_runtime_close(runtime, 2) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

typedef struct diagnostics_thread_args {
    trevrpc_rpc_runtime* runtime;
    uint64_t active_api_calls;
    int result;
} diagnostics_thread_args;

typedef struct release_thread_args {
    trevrpc_rpc_runtime* runtime;
    fake_transport* fake;
    atomic_bool started;
    atomic_bool completed;
    int result;
} release_thread_args;

static void fake_block_diagnostics(fake_transport* fake) {
    pthread_mutex_lock(&fake->mutex);
    fake->diagnostics_blocked = true;
    fake->diagnostics_entered = false;
    fake->diagnostics_release = false;
    pthread_mutex_unlock(&fake->mutex);
}

static void fake_wait_diagnostics_entered(fake_transport* fake) {
    pthread_mutex_lock(&fake->mutex);
    while (!fake->diagnostics_entered) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
}

static void fake_unblock_diagnostics(fake_transport* fake) {
    pthread_mutex_lock(&fake->mutex);
    fake->diagnostics_release = true;
    fake->diagnostics_blocked = false;
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
}

static void* diagnostics_thread_main(void* context) {
    diagnostics_thread_args* args = context;
    trevrpc_rpc_diagnostics_v1 diagnostics;
    assert(trevrpc_rpc_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    args->result = trevrpc_rpc_runtime_get_diagnostics_v1(args->runtime, &diagnostics);
    if (args->result == 0) {
        args->active_api_calls = diagnostics.active_api_calls;
    }
    return NULL;
}

static void* release_thread_main(void* context) {
    release_thread_args* args = context;
    pthread_mutex_lock(&args->fake->mutex);
    atomic_store_explicit(&args->started, true, memory_order_release);
    pthread_cond_broadcast(&args->fake->condition);
    pthread_mutex_unlock(&args->fake->mutex);
    args->result = trevrpc_rpc_runtime_release(args->runtime);
    atomic_store_explicit(&args->completed, true, memory_order_release);
    return NULL;
}

static void run_orphan_stream_release_retry(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    fake->stream_abort_result = -EIO;
    fake->stream_release_handle_result = -EIO;
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->release_handle_calls, memory_order_acquire) < 1) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
    fake->stream_release_handle_result = 0;
    assert(trevrpc_rpc_runtime_close(runtime, 1) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(fake_release_handle_count(fake) >= 2);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_peer_capacity_rejection_does_not_stall(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.call_capacity = 1;
    config.stream_capacity = 1;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request("Capacity", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    trevrpc_rpc_receive_release(receive);
    trevrpc_rpc_event_release(event);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_second_stream_handle,
               fake_listener_handle) == 0);
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
    assert(atomic_load_explicit(&fake->last_abort_error, memory_order_acquire) == TREVRPC_RPC_STATUS_UNAVAILABLE);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(fake_release_handle_count(fake) >= 2);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_incoming_survives_ordinary_saturation(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 1;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    make_request("Saturated", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
               0,
               (trevrpc_rpc_transport_handle){0},
               (trevrpc_rpc_transport_handle){0}) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_DIAGNOSTIC);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(trevrpc_rpc_event_take_incoming_call(event, &call, &stream, &receive) == 0);
    trevrpc_rpc_receive_release(receive);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_call_accept(runtime, call, 2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED);
    trevrpc_rpc_event_release(event);
    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
}

static void run_receive_failure_aborts_stream(bool fail_receive_info) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_wire_response_values response = {0};
    const uint8_t malformed[] = {0x18, 0x00};
    const uint8_t* receive_data = malformed;
    size_t receive_len = sizeof(malformed);
    uint8_t* encoded = NULL;
    size_t encoded_len = 0;
    rpc_receive_args receive_args = {0};
    pthread_t receive_thread;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "Malformed";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 2, &call, &stream) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    while (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 0)
        assert(poll(NULL, 0, 1) >= 0);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_READY);
    trevrpc_rpc_event_release(event);

    if (fail_receive_info) {
        response.status = TREVRPC_RPC_STATUS_OK;
        response.body.data = (uint8_t*)"response";
        response.body.len = strlen("response");
        assert(trevrpc_wire_encode_response(&response, 1024u * 1024u, &encoded, &encoded_len) == 0);
        assert(encoded_len >= 4);
        receive_data = encoded + 4;
        receive_len = encoded_len - 4;
        atomic_store_explicit(&fake->receive_get_info_result, -EIO, memory_order_release);
    }
    assert(fake_push_receive(fake, receive_data, receive_len) == 0);
    free(encoded);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);

    pthread_mutex_lock(&fake->mutex);
    fake->receive_blocked = true;
    pthread_mutex_unlock(&fake->mutex);
    receive_args.runtime = runtime;
    receive_args.stream = stream;
    assert(pthread_create(&receive_thread, NULL, rpc_receive_thread, &receive_args) == 0);
    pthread_mutex_lock(&fake->mutex);
    while (!fake->receive_entered)
        pthread_cond_wait(&fake->condition, &fake->mutex);
    pthread_mutex_unlock(&fake->mutex);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    wait_for_readable_release_calls(fake, 2);
    pthread_mutex_lock(&fake->mutex);
    fake->receive_release = true;
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    assert(pthread_join(receive_thread, NULL) == 0);
    receive = receive_args.receive;
    assert(receive_args.result == (fail_receive_info ? -EIO : TREVRPC_ERR_INVALID_FRAME));
    assert(receive == NULL);
    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 1);
    assert(atomic_load_explicit(&fake->last_abort_error, memory_order_acquire) == TREVRPC_RPC_STATUS_INTERNAL);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_waiting_request_receive_failure(bool fail_receive_info) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    if (fail_receive_info) {
        make_request("ReceiveInfoFailure", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
        assert(fake_push_receive(fake, request, request_len) == 0);
        free(request);
        atomic_store_explicit(&fake->receive_get_info_result, -EIO, memory_order_release);
    } else {
        atomic_store_explicit(&fake->stream_receive_result, -EAGAIN, memory_order_release);
    }
    fake->stream_abort_result = -ENOTSUP;
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.status == (fail_receive_info ? -EIO : -EAGAIN));
    assert(info.application_error_code == TREVRPC_RPC_STATUS_INTERNAL);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.status == (fail_receive_info ? -EIO : -EAGAIN));
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_waiting_request_decode_rejection(const uint8_t* request,
    size_t request_len,
    uint64_t max_message_size,
    int expected_status,
    uint32_t expected_rpc_status) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.max_message_size = max_message_size;
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_receive(fake, request, request_len) == 0);
    fake->stream_abort_result = -ENOTSUP;
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);

    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.status == expected_status);
    assert(info.application_error_code == expected_rpc_status);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.status == expected_status);
    assert(info.application_error_code == expected_rpc_status);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_waiting_request_decode_rejections(void) {
    const uint8_t malformed[] = {0x0a, 0x02, 'x'};
    const uint8_t unsupported_kind[] = {0x28, 0x04, 0x30, TREVRPC_RPC_ABI_VERSION};
    const uint8_t unsupported_version[] = {0x30, TREVRPC_RPC_ABI_VERSION + 1};
    const uint8_t invalid_metadata[] = {0x22,
        0x13,
        0x0a,
        0x0d,
        'A',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
        0x30,
        TREVRPC_RPC_ABI_VERSION};
    uint8_t* oversized = NULL;
    size_t oversized_len = 0;

    run_waiting_request_decode_rejection(
        malformed, sizeof(malformed), 1024u * 1024u, TREVRPC_ERR_INVALID_FRAME, TREVRPC_RPC_STATUS_INVALID_ARGUMENT);
    run_waiting_request_decode_rejection(unsupported_kind,
        sizeof(unsupported_kind),
        1024u * 1024u,
        TREVRPC_ERR_UNSUPPORTED_RPC_KIND,
        TREVRPC_RPC_STATUS_INVALID_ARGUMENT);
    run_waiting_request_decode_rejection(unsupported_version,
        sizeof(unsupported_version),
        1024u * 1024u,
        TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION,
        TREVRPC_RPC_STATUS_FAILED_PRECONDITION);
    run_waiting_request_decode_rejection(invalid_metadata,
        sizeof(invalid_metadata),
        1024u * 1024u,
        TREVRPC_ERR_INVALID_FRAME,
        TREVRPC_RPC_STATUS_INVALID_ARGUMENT);
    make_request("Oversized", TREVRPC_RPC_DEADLINE_INFINITE, &oversized, &oversized_len);
    run_waiting_request_decode_rejection(oversized, oversized_len, 4, -EMSGSIZE, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED);
    free(oversized);
}

static void run_outgoing_ready_send_abort_failure(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
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
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "SendFailure";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 2, &call, &stream) == 0);
    atomic_store_explicit(&fake->stream_send_result, -EIO, memory_order_release);
    fake->stream_abort_result = -ENOTSUP;
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_FAILED);
    assert(info.operation_id == 2);
    assert(info.status == -EIO);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    assert(info.status == -EIO);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.status == -EIO);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_closed_listener_rejects_late_peer(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event = NULL;
    uint8_t* request = NULL;
    size_t request_len = 0;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);

    make_request("Late", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    pthread_mutex_lock(&fake->mutex);
    while (atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0 ||
           atomic_load_explicit(&fake->release_handle_calls, memory_order_acquire) == 0) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_dropped_incoming_abort_failure(void) {
    fake_transport* fake = fake_create();
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint8_t* request = NULL;
    size_t request_len = 0;
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);
    make_request("Dropped", TREVRPC_RPC_DEADLINE_INFINITE, &request, &request_len);
    assert(fake_push_receive(fake, request, request_len) == 0);
    free(request);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
    assert(trevrpc_rpc_call_accept(runtime, info.call, 2) == -EACCES);
    fake->stream_abort_result = -EIO;
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_stopping_driver_oom(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);

    atomic_store_explicit(&fake->next_event_result, -ENOMEM, memory_order_release);
    assert(trevrpc_rpc_runtime_close(runtime, 1) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.sequence == 1);
    assert(info.operation_id == 1);
    assert(info.status == -ENOMEM);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_FATAL) != 0);
    trevrpc_rpc_event_release(event);
    event = NULL;
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_driver_fatal_paths(void) {
    const int failures[] = {-ENOTSUP, -EIO, -EPROTO};
    size_t index;
    for (index = 0; index < sizeof(failures) / sizeof(failures[0]); ++index) {
        trevrpc_rpc_runtime_config_v1 config;
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_event* event;
        trevrpc_rpc_event_info_v1 info;
        fake_transport* fake = fake_create();
        assert(fake != NULL);
        if (index == 0) {
            fake->wake_sources_result = failures[index];
        } else if (index == 1) {
            fake->next_event_result = failures[index];
        } else {
            fake->event_get_info_result = failures[index];
        }
        assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
        assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
        assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
        assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
        if (index != 0) {
            assert(fake_push_event(fake,
                       TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
                       0,
                       (trevrpc_rpc_transport_handle){0},
                       (trevrpc_rpc_transport_handle){0}) == 0);
        }
        event = wait_next_event(runtime, &wake);
        info = event_info(event);
        assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
        assert(info.operation_id == 0);
        assert(info.status == failures[index]);
        assert((info.flags & TREVRPC_RPC_EVENT_FLAG_FATAL) != 0);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_runtime_drain(runtime) == 0);
        assert(trevrpc_rpc_runtime_release(runtime) == 0);
    }
    {
        trevrpc_rpc_runtime_config_v1 config;
        trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_event* event;
        trevrpc_rpc_event_info_v1 info;
        fake_transport* fake = fake_create();
        assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
        assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
        assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
        assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
        assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
                   runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 7, &endpoint) == 0);
        fake->next_event_result = -EIO;
        assert(fake_push_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
                   0,
                   (trevrpc_rpc_transport_handle){0},
                   (trevrpc_rpc_transport_handle){0}) == 0);
        event = wait_next_event(runtime, &wake);
        info = event_info(event);
        assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED);
        assert(info.operation_id == 7);
        assert(info.status == -EIO);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        info = event_info(event);
        assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
        assert(info.operation_id == 0);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_runtime_drain(runtime) == 0);
        assert(trevrpc_rpc_runtime_release(runtime) == 0);
    }
}

static void run_api_admission_barrier(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();
    diagnostics_thread_args diagnostics_args = {0};
    release_thread_args release_args = {0};
    pthread_t diagnostics_thread;
    pthread_t release_thread;
    unsigned attempt;

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == -EBUSY);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 1) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.operation_id == 1);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);

    fake_block_diagnostics(fake);
    diagnostics_args.runtime = runtime;
    assert(pthread_create(&diagnostics_thread, NULL, diagnostics_thread_main, &diagnostics_args) == 0);
    fake_wait_diagnostics_entered(fake);

    release_args.runtime = runtime;
    release_args.fake = fake;
    atomic_init(&release_args.started, false);
    atomic_init(&release_args.completed, false);
    assert(pthread_create(&release_thread, NULL, release_thread_main, &release_args) == 0);
    pthread_mutex_lock(&fake->mutex);
    while (!atomic_load_explicit(&release_args.started, memory_order_acquire)) {
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    pthread_mutex_unlock(&fake->mutex);
    for (attempt = 0; attempt < 5000; ++attempt) {
        trevrpc_rpc_event* probe = NULL;
        int result = trevrpc_rpc_runtime_next_event(runtime, &probe);
        if (result == -EPIPE || result == -ESHUTDOWN) {
            break;
        }
        assert(result == -EAGAIN);
        assert(probe == NULL);
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt < 5000);
    assert(!atomic_load_explicit(&release_args.completed, memory_order_acquire));

    fake_unblock_diagnostics(fake);
    assert(pthread_join(diagnostics_thread, NULL) == 0);
    assert(pthread_join(release_thread, NULL) == 0);
    assert(diagnostics_args.result == 0);
    assert(diagnostics_args.active_api_calls >= 1);
    assert(release_args.result == 0);
}

static void run_stopped_with_retained_endpoint(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    fake_transport* fake = fake_create();

    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);

    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    assert(info.operation_id == 2);
    assert(info.status == 0);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(info.operation_id == 3);
    assert(info.status == 0);
    assert((info.flags & TREVRPC_RPC_EVENT_FLAG_FATAL) == 0);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void close_listener_runtime(fake_transport* fake,
    trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_wake_source_v1* wake,
    trevrpc_rpc_endpoint_v1 endpoint) {
    trevrpc_rpc_event* event;
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 2) == 0);
    event = wait_next_event(runtime, wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    event = wait_next_event(runtime, wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
    (void)fake;
}

static void run_admission_events(void) {
    static const uint8_t path[] = "/fake.Service/Test";
    static const uint8_t authority[] = "localhost:4242";
    static const uint8_t origin[] = "https://localhost:4242";
    const uint32_t transport_kinds[] = {
        TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION, TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION};
    const uint32_t rpc_kinds[] = {TREVRPC_RPC_EVENT_HTTP3_ADMISSION, TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION};
    const uint32_t protocols[] = {TREVRPC_RPC_ADMISSION_PROTOCOL_HTTP3, TREVRPC_RPC_ADMISSION_PROTOCOL_WEBTRANSPORT};
    size_t index;

    for (index = 0; index < sizeof(transport_kinds) / sizeof(transport_kinds[0]); ++index) {
        fake_transport* fake = fake_create();
        trevrpc_rpc_runtime_config_v1 config;
        trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_event* event;
        trevrpc_rpc_event_info_v1 general;
        trevrpc_rpc_admission_info_v1 admission;

        assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
        assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
        assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
        assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
        assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
                   runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
        consume_endpoint_ready(runtime, &wake, endpoint);
        assert(fake_push_admission_event(fake,
                   transport_kinds[index],
                   path,
                   sizeof(path) - 1,
                   authority,
                   sizeof(authority) - 1,
                   origin,
                   sizeof(origin) - 1) == 0);
        event = wait_next_event(runtime, &wake);
        general = event_info(event);
        assert(general.kind == rpc_kinds[index]);
        assert(general.subject_kind == TREVRPC_RPC_OBJECT_ENDPOINT);
        assert(general.endpoint.owner == endpoint.owner);
        assert(trevrpc_rpc_admission_info_v1_init(&admission, sizeof(admission)) == 0);
        assert(trevrpc_rpc_event_get_admission_info_v1(event, &admission) == 0);
        assert(admission.protocol == protocols[index]);
        assert(admission.flags == TREVRPC_RPC_ADMISSION_FLAG_SECURE);
        assert(admission.listener.owner == endpoint.owner);
        assert(admission.listener.slot == endpoint.slot);
        assert(admission.listener.generation == endpoint.generation);
        assert(admission.path_len == sizeof(path) - 1);
        assert(memcmp(admission.path, path, sizeof(path) - 1) == 0);
        assert(admission.authority_len == sizeof(authority) - 1);
        assert(memcmp(admission.authority, authority, sizeof(authority) - 1) == 0);
        assert(admission.origin_len == sizeof(origin) - 1);
        assert(memcmp(admission.origin, origin, sizeof(origin) - 1) == 0);
        assert(trevrpc_rpc_admission_respond_v1(event, 201) == -EINVAL);
        assert(atomic_load_explicit(&fake->admission_response_calls, memory_order_acquire) == 0);
        assert(trevrpc_rpc_admission_respond_v1(event, index == 0 ? 200 : 403) == 0);
        assert(atomic_load_explicit(&fake->admission_response_calls, memory_order_acquire) == 1);
        assert(atomic_load_explicit(&fake->admission_last_status, memory_order_acquire) == (index == 0 ? 200u : 403u));
        assert(trevrpc_rpc_admission_respond_v1(event, 500) == -EALREADY);
        trevrpc_rpc_event_release(event);
        assert(atomic_load_explicit(&fake->admission_undecided_release_calls, memory_order_acquire) == 0);
        close_listener_runtime(fake, runtime, &wake, endpoint);
    }

    {
        fake_transport* fake = fake_create();
        trevrpc_rpc_runtime_config_v1 config;
        trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_event* event;
        endpoint_close_args close_args = {0};
        pthread_t close_thread;
        unsigned attempt;

        assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
        assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
        assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
        assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
        assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
                   runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
        consume_endpoint_ready(runtime, &wake, endpoint);

        pthread_mutex_lock(&fake->mutex);
        fake->admission_info_blocked = true;
        fake->listener_close_blocked = true;
        pthread_mutex_unlock(&fake->mutex);
        assert(fake_push_admission_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION,
                   path,
                   sizeof(path) - 1,
                   authority,
                   sizeof(authority) - 1,
                   NULL,
                   0) == 0);
        pthread_mutex_lock(&fake->mutex);
        while (!fake->admission_info_entered) {
            pthread_cond_wait(&fake->condition, &fake->mutex);
        }
        pthread_mutex_unlock(&fake->mutex);

        close_args.runtime = runtime;
        close_args.endpoint = endpoint;
        close_args.operation_id = 2;
        assert(pthread_create(&close_thread, NULL, endpoint_close_thread, &close_args) == 0);
        pthread_mutex_lock(&fake->mutex);
        while (!fake->listener_close_entered) {
            pthread_cond_wait(&fake->condition, &fake->mutex);
        }
        fake->admission_info_release = true;
        fake->listener_close_release = true;
        pthread_cond_broadcast(&fake->condition);
        pthread_mutex_unlock(&fake->mutex);
        assert(pthread_join(close_thread, NULL) == 0);
        assert(close_args.result == 0);

        for (attempt = 0; attempt < 1000; ++attempt) {
            if (atomic_load_explicit(&fake->admission_response_calls, memory_order_acquire) == 1) {
                break;
            }
            assert(poll(NULL, 0, 1) >= 0);
        }
        assert(attempt != 1000);
        assert(atomic_load_explicit(&fake->admission_undecided_release_calls, memory_order_acquire) == 1);
        assert(atomic_load_explicit(&fake->admission_last_status, memory_order_acquire) == 500);

        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
        assert(event_info(event).operation_id == 2);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
        assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_runtime_drain(runtime) == 0);
        assert(trevrpc_rpc_runtime_release(runtime) == 0);
    }

    {
        fake_transport* fake = fake_create();
        trevrpc_rpc_runtime_config_v1 config;
        trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_event* event;
        unsigned attempt;

        assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
        config.event_capacity = 1;
        assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
        assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
        assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
        assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
                   runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
        consume_endpoint_ready(runtime, &wake, endpoint);

        assert(fake_push_admission_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION,
                   path,
                   sizeof(path) - 1,
                   authority,
                   sizeof(authority) - 1,
                   origin,
                   sizeof(origin) - 1) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION);
        trevrpc_rpc_event_release(event);
        assert(atomic_load_explicit(&fake->admission_undecided_release_calls, memory_order_acquire) == 1);
        assert(atomic_load_explicit(&fake->admission_last_status, memory_order_acquire) == 500);

        assert(fake_push_admission_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION,
                   path,
                   sizeof(path) - 1,
                   authority,
                   sizeof(authority) - 1,
                   NULL,
                   0) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_HTTP3_ADMISSION);
        atomic_store_explicit(&fake->admission_respond_result, -EIO, memory_order_release);
        assert(trevrpc_rpc_admission_respond_v1(event, 200) == -EIO);
        assert(atomic_load_explicit(&fake->admission_response_calls, memory_order_acquire) == 2);
        assert(trevrpc_rpc_admission_respond_v1(event, 403) == -EALREADY);
        trevrpc_rpc_event_release(event);
        assert(atomic_load_explicit(&fake->admission_undecided_release_calls, memory_order_acquire) == 1);

        atomic_store_explicit(&fake->admission_get_info_result, -EIO, memory_order_release);
        assert(fake_push_admission_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION,
                   path,
                   sizeof(path) - 1,
                   authority,
                   sizeof(authority) - 1,
                   NULL,
                   0) == 0);
        for (attempt = 0; attempt < 1000; ++attempt) {
            if (atomic_load_explicit(&fake->admission_response_calls, memory_order_acquire) == 3) {
                break;
            }
            assert(poll(NULL, 0, 1) >= 0);
        }
        assert(attempt != 1000);
        assert(atomic_load_explicit(&fake->admission_undecided_release_calls, memory_order_acquire) == 2);
        event = NULL;
        assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);

        assert(fake_push_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
                   0,
                   (trevrpc_rpc_transport_handle){0},
                   (trevrpc_rpc_transport_handle){0}) == 0);
        assert(fake_push_admission_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION,
                   path,
                   sizeof(path) - 1,
                   authority,
                   sizeof(authority) - 1,
                   NULL,
                   0) == 0);
        for (attempt = 0; attempt < 1000; ++attempt) {
            if (atomic_load_explicit(&fake->admission_response_calls, memory_order_acquire) == 4) {
                break;
            }
            assert(poll(NULL, 0, 1) >= 0);
        }
        assert(attempt != 1000);
        assert(atomic_load_explicit(&fake->admission_undecided_release_calls, memory_order_acquire) == 3);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_DIAGNOSTIC);
        trevrpc_rpc_event_release(event);
        close_listener_runtime(fake, runtime, &wake, endpoint);
    }
}

static void run_success_before_deadline_is_not_cancelled(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_call_context_info_v1 context;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "CleanDeadline";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    call_config.timeout_nanos = UINT64_C(500000000);
    setup_ready_client_call_with_config(
        fake, &runtime_config, &call_config, &runtime, &wake, &endpoint, &call, &stream);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE) != 0);
    assert((context.flags & (TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED | TREVRPC_RPC_CALL_CONTEXT_CANCELLED)) == 0);
    close_ready_client_call(runtime, &wake, endpoint, call, stream);
}

static void run_context_cancellation_is_durable(void) {
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    trevrpc_rpc_call_context_info_v1 context;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    setup_client_call(fake, &runtime, &wake, &endpoint, &call, &stream, TREVRPC_RPC_KIND_UNARY, 2);
    assert(trevrpc_rpc_call_cancel(runtime, call, 3, 77) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CANCELLED);
    assert(info.operation_id == 3);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE) == 0);
    /* The fake provider records aborts but does not synthesize the eventual
     * peer terminal notification; drive the normal terminal path explicitly. */
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    if (info.kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        info = event_info(event);
    }
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == -ESTALE);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 4) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 5) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_rejected_abort_does_not_cancel(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_call_context_info_v1 context;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "RejectedAbort";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    setup_ready_client_call_with_config(
        fake, &runtime_config, &call_config, &runtime, &wake, &endpoint, &call, &stream);

    fake->stream_abort_result = -EIO;
    assert(trevrpc_rpc_call_cancel(runtime, call, 3, 77) == -EIO);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) == 0);
    assert(trevrpc_rpc_call_close(runtime, call, 4, TREVRPC_RPC_CLOSE_FLAG_ABORT, 78) == -EIO);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) == 0);

    fake->stream_abort_result = 0;
    assert(trevrpc_rpc_call_cancel(runtime, call, 5, 79) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CANCELLED);
    trevrpc_rpc_event_release(event);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_next_event(runtime, &wake);
    if (event_info(event).kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
    }
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 6) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 7) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_runtime_shutdown_is_durable_cancellation(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_call_context_info_v1 context;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "RuntimeShutdown";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    setup_ready_client_call_with_config(
        fake, &runtime_config, &call_config, &runtime, &wake, &endpoint, &call, &stream);

    atomic_store_explicit(&fake->next_event_result, -EAGAIN, memory_order_release);
    assert(trevrpc_rpc_runtime_close(runtime, 3) == 0);
    assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
    assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
    assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0);
    atomic_store_explicit(&fake->next_event_result, 0, memory_order_release);

    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    assert(event_info(event).status == 0);
    assert((event_info(event).flags & TREVRPC_RPC_EVENT_FLAG_FATAL) == 0);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void run_initial_request_timeout_is_silent(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_event* event = NULL;
    fake_transport* fake = fake_create();
    unsigned attempt;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0);
    config.initial_request_timeout_nanos = UINT64_C(20000000);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&config, &fake->base, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_SERVER, 1, &endpoint) == 0);
    consume_endpoint_ready(runtime, &wake, endpoint);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) != 0) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    assert(trevrpc_rpc_runtime_next_event(runtime, &event) == -EAGAIN);
    close_listener_runtime(fake, runtime, &wake, endpoint);
}

static void run_task240_config_limit_validation(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_call_config_v1 call_config;
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    runtime_config.max_stream_messages = -2;
    runtime_config.max_stream_body_size = INT64_MIN;
    assert(trevrpc_rpc_validate_runtime_config(&runtime_config) == 0);
    runtime_config.max_stream_messages = 0;
    runtime_config.max_stream_body_size = 0;
    assert(trevrpc_rpc_validate_runtime_config(&runtime_config) == 0);
    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    assert(call_config.max_response_body_size == -1);
    assert(call_config.max_response_messages == -1);
    assert(call_config.max_response_stream_body_size == -1);
}

static void run_server_idle_timers(void) {
    const uint32_t kinds[] = {TREVRPC_RPC_KIND_CLIENT_STREAMING, TREVRPC_RPC_KIND_SERVER_STREAMING};
    unsigned index;

    for (index = 0; index < sizeof(kinds) / sizeof(kinds[0]); ++index) {
        trevrpc_rpc_runtime_config_v1 runtime_config;
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_call_v1 call;
        trevrpc_rpc_stream_v1 stream;
        trevrpc_rpc_event* event;
        trevrpc_rpc_call_context_info_v1 context;
        fake_transport* fake = fake_create();
        unsigned attempt;

        assert(fake != NULL);
        assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
        runtime_config.stream_idle_timeout_nanos = UINT64_C(20000000);
        setup_accepted_peer_call(fake, &runtime_config, kinds[index], &runtime, &wake, &endpoint, &call, &stream);
        for (attempt = 0; attempt != 1000; ++attempt) {
            if (atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) != 0) {
                break;
            }
            assert(poll(NULL, 0, 1) >= 0);
        }
        assert(attempt != 1000);
        assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
        assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
        assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0);
        assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
        trevrpc_rpc_event_release(event);
        close_ready_client_call(runtime, &wake, endpoint, call, stream);
    }
}

static void run_idle_disabled_and_receive_finish_disarm(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_event* event;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_status_v1 status;
    uint64_t send_operation;
    fake_transport* fake = fake_create();

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    setup_accepted_peer_call(
        fake, &runtime_config, TREVRPC_RPC_KIND_SERVER_STREAMING, &runtime, &wake, &endpoint, &call, &stream);
    assert(poll(NULL, 0, 40) >= 0);
    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0);
    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);

    fake = fake_create();
    assert(fake != NULL);
    runtime = NULL;
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    runtime_config.stream_idle_timeout_nanos = UINT64_C(100000000);
    setup_accepted_peer_call(
        fake, &runtime_config, TREVRPC_RPC_KIND_CLIENT_STREAMING, &runtime, &wake, &endpoint, &call, &stream);
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    status.code = TREVRPC_RPC_STATUS_CANCELLED;
    assert(trevrpc_rpc_call_respond_copy_v1(runtime, call, 3, &status, NULL, 0) == 0);
    send_operation = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle,
               send_operation) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_SEND_COMPLETE);
    trevrpc_rpc_event_release(event);
    push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_RPC_STATUS_OK, NULL, 0);
    assert(fake_push_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle) == 0);
    event = wait_next_event(runtime, &wake);
    assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
    assert(poll(NULL, 0, 150) >= 0);
    assert(atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) == 0);
    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
}

static void run_client_stream_response_continuation_completion(void) {
    const uint8_t message[] = "response";
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t first_transport_operation;
    uint64_t second_transport_operation;
    fake_transport* fake = fake_create();
    unsigned attempt;

    assert(fake != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    setup_accepted_peer_call(
        fake, &runtime_config, TREVRPC_RPC_KIND_CLIENT_STREAMING, &runtime, &wake, &endpoint, &call, &stream);
    assert(call.owner != fake_stream_handle.owner || call.slot != fake_stream_handle.slot ||
           call.generation != fake_stream_handle.generation);
    assert(stream.owner != fake_stream_handle.owner || stream.slot != fake_stream_handle.slot ||
           stream.generation != fake_stream_handle.generation);
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    assert(trevrpc_rpc_call_respond_copy_v1(runtime, call, 3, &status, message, sizeof(message) - 1) == 0);
    first_transport_operation = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(first_transport_operation != 0);
    assert(atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 1);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle,
               first_transport_operation) == 0);
    for (attempt = 0; attempt != 1000; ++attempt) {
        if (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) == 2) {
            break;
        }
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    second_transport_operation = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
    assert(second_transport_operation != first_transport_operation);
    assert(fake_push_operation_event(fake,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
               fake_stream_handle,
               fake_listener_handle,
               second_transport_operation) == 0);
    event = wait_next_event(runtime, &wake);
    info = event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE);
    assert(info.operation_id == 3);
    assert(info.status == 0);
    trevrpc_rpc_event_release(event);
    assert(atomic_load_explicit(&fake->stream_finish_send_calls, memory_order_acquire) == 1);
    finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
}

static void run_server_receive_limits(void) {
    const uint8_t body[] = "next";
    unsigned body_limit;

    for (body_limit = 0; body_limit != 2; ++body_limit) {
        trevrpc_rpc_runtime_config_v1 runtime_config;
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_call_v1 call;
        trevrpc_rpc_stream_v1 stream;
        trevrpc_rpc_event* event;
        trevrpc_rpc_receive* receive = NULL;
        trevrpc_rpc_call_context_info_v1 context;
        fake_transport* fake = fake_create();

        assert(fake != NULL);
        assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
        runtime_config.max_stream_messages = body_limit == 0 ? 1 : -1;
        runtime_config.max_stream_body_size = body_limit == 0 ? -1 : 10;
        setup_accepted_peer_call(
            fake, &runtime_config, TREVRPC_RPC_KIND_CLIENT_STREAMING, &runtime, &wake, &endpoint, &call, &stream);
        push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, body, sizeof(body) - 1);
        assert(fake_push_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
                   fake_stream_handle,
                   fake_listener_handle) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EMSGSIZE);
        assert(receive == NULL);
        assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
        assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
        assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
        trevrpc_rpc_event_release(event);
        close_ready_client_call(runtime, &wake, endpoint, call, stream);
    }
}

static void run_server_send_limits(void) {
    const uint8_t message[] = "one";
    unsigned body_limit;

    for (body_limit = 0; body_limit != 2; ++body_limit) {
        trevrpc_rpc_runtime_config_v1 runtime_config;
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_call_v1 call;
        trevrpc_rpc_stream_v1 stream;
        trevrpc_rpc_event* event;
        fake_transport* fake = fake_create();
        uint64_t send_operation;

        assert(fake != NULL);
        assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
        runtime_config.max_stream_messages = body_limit == 0 ? 1 : -1;
        runtime_config.max_stream_body_size = body_limit == 0 ? -1 : (int64_t)(sizeof(message) - 1);
        setup_accepted_peer_call(
            fake, &runtime_config, TREVRPC_RPC_KIND_SERVER_STREAMING, &runtime, &wake, &endpoint, &call, &stream);
        assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 3, message, sizeof(message) - 1, 0) == 0);
        send_operation = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
        assert(fake_push_operation_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
                   fake_stream_handle,
                   fake_listener_handle,
                   send_operation) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_SEND_COMPLETE);
        trevrpc_rpc_event_release(event);
        assert(trevrpc_rpc_stream_send_copy_v1(runtime, stream, 4, message, sizeof(message) - 1, 0) == -EMSGSIZE);
        finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
    }
}

static void run_server_client_streaming_response_limits(void) {
    const uint8_t message[] = "123456789";
    unsigned body_limit;

    for (body_limit = 0; body_limit != 1; ++body_limit) {
        trevrpc_rpc_runtime_config_v1 runtime_config;
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_call_v1 call;
        trevrpc_rpc_stream_v1 stream;
        trevrpc_rpc_status_v1 status;
        fake_transport* fake = fake_create();

        assert(fake != NULL);
        assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
        runtime_config.max_stream_messages = -1;
        runtime_config.max_stream_body_size = 8;
        setup_accepted_peer_call(
            fake, &runtime_config, TREVRPC_RPC_KIND_CLIENT_STREAMING, &runtime, &wake, &endpoint, &call, &stream);
        assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
        status.code = TREVRPC_RPC_STATUS_OK;
        assert(trevrpc_rpc_call_respond_copy_v1(runtime, call, 3, &status, message, sizeof(message) - 1) == -EMSGSIZE);
        finish_call_and_runtime(fake, runtime, &wake, endpoint, call, stream, TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
    }
}

static void run_client_response_limits(void) {
    const uint8_t body[] = "response";
    const uint8_t second_body[] = "again";
    const uint32_t kinds[] = {TREVRPC_RPC_KIND_UNARY,
        TREVRPC_RPC_KIND_CLIENT_STREAMING,
        TREVRPC_RPC_KIND_SERVER_STREAMING,
        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING};
    unsigned index;

    for (index = 0; index < sizeof(kinds) / sizeof(kinds[0]); ++index) {
        trevrpc_rpc_runtime_config_v1 runtime_config;
        trevrpc_rpc_call_config_v1 call_config;
        trevrpc_rpc_runtime* runtime = NULL;
        trevrpc_rpc_wake_source_v1 wake;
        trevrpc_rpc_endpoint_v1 endpoint;
        trevrpc_rpc_call_v1 call;
        trevrpc_rpc_stream_v1 stream;
        trevrpc_rpc_event* event;
        trevrpc_rpc_receive* receive = NULL;
        trevrpc_rpc_call_context_info_v1 context;
        fake_transport* fake = fake_create();
        uint8_t* response_frame = NULL;
        size_t response_frame_len = 0;

        assert(fake != NULL);
        assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
        /* Exercise public ingestion: every negative value, not only -1, is unlimited. */
        runtime_config.max_stream_messages = -2;
        runtime_config.max_stream_body_size = INT64_MIN;
        assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
        call_config.max_response_body_size = -2;
        call_config.max_response_messages = INT64_MIN;
        call_config.max_response_stream_body_size = -2;
        call_config.kind = kinds[index];
        call_config.service = "fake.Service";
        call_config.service_len = (uint32_t)strlen(call_config.service);
        call_config.method = "ResponseLimit";
        call_config.method_len = (uint32_t)strlen(call_config.method);
        call_config.initial_message = (const uint8_t*)"request";
        call_config.initial_message_len = strlen("request");
        if (kinds[index] == TREVRPC_RPC_KIND_UNARY) {
            call_config.max_response_body_size = (int64_t)(sizeof(body) - 2);
        } else if (kinds[index] == TREVRPC_RPC_KIND_SERVER_STREAMING) {
            call_config.max_response_messages = 1;
        } else {
            call_config.max_response_stream_body_size = (int64_t)(sizeof(body) - 2);
        }
        setup_ready_client_call_with_config(
            fake, &runtime_config, &call_config, &runtime, &wake, &endpoint, &call, &stream);
        if (kinds[index] == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_wire_response_values response = {0};
            response.status = TREVRPC_RPC_STATUS_OK;
            response.body.data = (uint8_t*)body;
            response.body.len = sizeof(body) - 1;
            assert(trevrpc_wire_encode_response(&response, 1024u * 1024u, &response_frame, &response_frame_len) == 0);
            assert(fake_push_receive(fake, response_frame + 4, response_frame_len - 4) == 0);
            free(response_frame);
        } else {
            push_stream_frame(fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, body, sizeof(body) - 1);
        }
        assert(fake_push_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                   fake_stream_handle,
                   fake_connection_handle) == 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
        trevrpc_rpc_event_release(event);
        if (kinds[index] == TREVRPC_RPC_KIND_SERVER_STREAMING) {
            assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == 0);
            trevrpc_rpc_receive_release(receive);
            receive = NULL;
            push_stream_frame(
                fake, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, second_body, sizeof(second_body) - 1);
            assert(fake_push_event(fake,
                       TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
                       TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                       fake_stream_handle,
                       fake_connection_handle) == 0);
            event = wait_next_event(runtime, &wake);
            assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
            trevrpc_rpc_event_release(event);
        }
        assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EMSGSIZE);
        assert(receive == NULL);
        assert(trevrpc_rpc_call_context_info_v1_init(&context, sizeof(context)) == 0);
        assert(trevrpc_rpc_call_get_context_v1(runtime, call, &context) == 0);
        assert((context.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
        trevrpc_rpc_event_release(event);
        event = wait_next_event(runtime, &wake);
        assert(event_info(event).kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
        trevrpc_rpc_event_release(event);
        close_ready_client_call(runtime, &wake, endpoint, call, stream);
    }
}

static void run_atomic_incoming_enqueue(void) {
    static const uint8_t body[] = "incoming";
    fake_transport* fake = fake_create();
    size_t index;

    assert(fake != NULL);
    for (index = 0; index < FAKE_EVENT_CAPACITY - 1u; ++index) {
        assert(fake_push_event(fake,
                   TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC,
                   0,
                   (trevrpc_rpc_transport_handle){0},
                   (trevrpc_rpc_transport_handle){0}) == 0);
    }
    assert(fake_push_incoming_stream(fake, body, sizeof(body)) == -EAGAIN);
    assert(fake->event_count == FAKE_EVENT_CAPACITY - 1u);
    assert(fake->stream_receive_head == NULL);
    assert(fake->stream_receive_tail == NULL);
    fake_destroy(&fake->base);

    fake = fake_create();
    assert(fake != NULL);
    assert(fake_push_incoming_stream(fake, NULL, sizeof(body)) == -EINVAL);
    assert(fake->event_count == 0);
    assert(fake->stream_receive_head == NULL);
    assert(fake->stream_receive_tail == NULL);
    assert(fake_push_incoming_stream(fake, body, sizeof(body)) == 0);
    assert(fake->event_count == 2u);
    assert(fake->stream_receive_head != NULL);
    assert(fake->stream_receive_head == fake->stream_receive_tail);
    fake_destroy(&fake->base);
}

int main(void) {
    run_atomic_incoming_enqueue();
    run_task240_config_limit_validation();
    run_success_before_deadline_is_not_cancelled();
    run_context_cancellation_is_durable();
    run_rejected_abort_does_not_cancel();
    run_runtime_shutdown_is_durable_cancellation();
    run_initial_request_timeout_is_silent();
    run_server_idle_timers();
    run_idle_disabled_and_receive_finish_disarm();
    run_client_stream_response_continuation_completion();
    run_server_receive_limits();
    run_server_send_limits();
    run_server_client_streaming_response_limits();
    run_client_response_limits();
    run_admission_events();
    run_finish_send_status_then_fin();
    run_lifecycle_operation_id_scope();
    run_send_operation_id_collision_and_reuse();
    run_late_reused_operation_id();
    run_failed_request_send_complete();
    run_send_stopped_preserves_receive_direction();
    run_peer_stopped_local_finish_is_settled(false);
    run_peer_stopped_local_finish_is_settled(true);
    run_send_stopped_readies_initial_request();
    run_coalesced_streaming_response_before_terminal();
    run_clean_peer_terminal_settles_local_finish();
    run_clean_terminal_waits_for_late_receive_fin();
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_UNARY, RESPONSE_TERMINAL_LATE_FIN);
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_CLIENT_STREAMING, RESPONSE_TERMINAL_LATE_FIN);
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_SERVER_STREAMING, RESPONSE_TERMINAL_LATE_FIN);
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_UNARY, RESPONSE_TERMINAL_OPTIONAL_FIN);
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_CLIENT_STREAMING, RESPONSE_TERMINAL_OPTIONAL_FIN);
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_UNARY, RESPONSE_TERMINAL_GRACEFUL_CLOSE);
    run_terminal_before_drained_response(TREVRPC_RPC_KIND_CLIENT_STREAMING, RESPONSE_TERMINAL_ABORT_CLOSE);
    run_trailing_response_after_status();
    run_receive_allocation_failure_rearms_readable(false);
    run_receive_allocation_failure_rearms_readable(true);
    run_coalesced_peer_streaming_request_before_terminal();
    run_peer_request_status_rejection(false);
    run_peer_request_status_rejection(true);
    run_peer_terminal_before_initial_readable();
    run_receive_fin_before_ready();
    run_readable_backpressure_retry();
    run_unary_error_status_receive();
    run_outgoing_deadline_before_stream_ready();
    run_deadline_while_queue_full();
    run_endpoint_terminal_before_ready(TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    run_endpoint_terminal_before_ready(TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED);
    run_orphan_stream_release_retry();
    run_peer_capacity_rejection_does_not_stall();
    run_waiting_request_receive_failure(false);
    run_waiting_request_receive_failure(true);
    run_waiting_request_decode_rejections();
    run_outgoing_ready_send_abort_failure();
    run_closed_listener_rejects_late_peer();
    run_dropped_incoming_abort_failure();
    run_stopping_driver_oom();
    run_driver_fatal_paths();
    run_api_admission_barrier();
    run_incoming_survives_ordinary_saturation();
    run_receive_failure_aborts_stream(false);
    run_receive_failure_aborts_stream(true);
    run_stopped_with_retained_endpoint();
    return 0;
}
