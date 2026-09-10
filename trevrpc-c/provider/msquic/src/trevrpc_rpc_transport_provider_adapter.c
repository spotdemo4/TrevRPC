#include "trevrpc_rpc_transport_provider_adapter.h"

#include <string.h>

static int trevrpc_transport_rpc_get_wake_sources(
    void* context, trevrpc_transport_provider_wake_v1* wakes, size_t capacity, size_t* out_count) {
    return trevrpc_rpc_transport_get_wake_sources(context, wakes, capacity, out_count);
}

static int trevrpc_transport_rpc_poll_timeout_ms(void* context) {
    return trevrpc_rpc_transport_poll_timeout_ms(context);
}

static int trevrpc_transport_rpc_next_event(void* context, trevrpc_transport_event_v1** out_event) {
    return trevrpc_rpc_transport_next_event(context, (trevrpc_rpc_transport_event**)out_event);
}

static int trevrpc_transport_rpc_event_get_info(
    void* context, const trevrpc_transport_event_v1* event, trevrpc_transport_provider_event_info_v1* info) {
    return trevrpc_rpc_transport_event_get_info(context, (const trevrpc_rpc_transport_event*)event, info);
}

static int trevrpc_transport_rpc_event_get_admission_info(
    void* context, const trevrpc_transport_event_v1* event, trevrpc_transport_provider_admission_info_v1* info) {
    return trevrpc_rpc_transport_event_get_admission_info(context, (const trevrpc_rpc_transport_event*)event, info);
}

static int trevrpc_transport_rpc_event_get_protocol_info(
    void* context, const trevrpc_transport_event_v1* event, trevrpc_transport_provider_event_protocol_info_v1* info) {
    return trevrpc_rpc_transport_event_get_protocol_info(context, (const trevrpc_rpc_transport_event*)event, info);
}

static int trevrpc_transport_rpc_admission_respond(
    void* context, const trevrpc_transport_event_v1* event, uint16_t status) {
    return trevrpc_rpc_transport_admission_respond(context, (const trevrpc_rpc_transport_event*)event, status);
}

static void trevrpc_transport_rpc_event_release(void* context, trevrpc_transport_event_v1* event) {
    trevrpc_rpc_transport_event_release(context, (trevrpc_rpc_transport_event*)event);
}

static int trevrpc_transport_rpc_receive_get_info(
    void* context, const trevrpc_transport_receive_v1* receive, trevrpc_transport_provider_receive_info_v1* info) {
    return trevrpc_rpc_transport_receive_get_info(context, (const trevrpc_rpc_transport_receive*)receive, info);
}

static void trevrpc_transport_rpc_receive_release(void* context, trevrpc_transport_receive_v1* receive) {
    trevrpc_rpc_transport_receive_release(context, (trevrpc_rpc_transport_receive*)receive);
}

static int trevrpc_transport_rpc_get_diagnostics(
    void* context, trevrpc_transport_provider_diagnostics_v1* diagnostics) {
    return trevrpc_rpc_transport_get_diagnostics(context, diagnostics);
}

static int trevrpc_transport_rpc_listen(
    void* context, const trevrpc_transport_provider_endpoint_config_v1* config, trevrpc_transport_handle_v1* listener) {
    return trevrpc_rpc_transport_endpoint_listen(context, config, listener);
}

static int trevrpc_transport_rpc_listener_get_port(
    void* context, trevrpc_transport_handle_v1 listener, uint16_t* port) {
    return trevrpc_rpc_transport_endpoint_get_port(context, listener, port);
}

static int trevrpc_transport_rpc_dial(void* context,
    const trevrpc_transport_provider_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* connection) {
    return trevrpc_rpc_transport_endpoint_dial(context, config, operation_id, connection);
}

static int trevrpc_transport_rpc_dial_cancel(void* context, trevrpc_transport_handle_v1 connection) {
    return trevrpc_rpc_transport_dial_cancel(context, connection);
}

static int trevrpc_transport_rpc_stream_open(
    void* context, trevrpc_transport_handle_v1 connection, uint64_t operation_id, trevrpc_transport_handle_v1* stream) {
    return trevrpc_rpc_transport_stream_open(context, connection, operation_id, stream);
}

static int trevrpc_transport_rpc_stream_send(
    void* context, trevrpc_transport_handle_v1 stream, uint64_t operation_id, const uint8_t* body, size_t body_len) {
    return trevrpc_rpc_transport_stream_send(context, stream, operation_id, body, body_len);
}

static int trevrpc_transport_rpc_stream_receive(
    void* context, trevrpc_transport_handle_v1 stream, trevrpc_transport_receive_v1** receive) {
    return trevrpc_rpc_transport_stream_receive(context, stream, (trevrpc_rpc_transport_receive**)receive);
}

static int trevrpc_transport_rpc_stream_finish_send(void* context, trevrpc_transport_handle_v1 stream) {
    return trevrpc_rpc_transport_stream_finish_send(context, stream);
}

static int trevrpc_transport_rpc_stream_abort_receive(
    void* context, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    return trevrpc_rpc_transport_stream_abort_receive(context, stream, error_code);
}

static int trevrpc_transport_rpc_stream_abort_send(
    void* context, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    return trevrpc_rpc_transport_stream_abort_send(context, stream, error_code);
}

static int trevrpc_transport_rpc_stream_abort(void* context, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    return trevrpc_rpc_transport_stream_abort(context, stream, error_code);
}

static int trevrpc_transport_rpc_stream_close(void* context, trevrpc_transport_handle_v1 stream) {
    return trevrpc_rpc_transport_stream_close(context, stream);
}

static int trevrpc_transport_rpc_connection_close(
    void* context, trevrpc_transport_handle_v1 connection, uint64_t error_code) {
    return trevrpc_rpc_transport_connection_close(context, connection, error_code);
}

static int trevrpc_transport_rpc_listener_close(void* context, trevrpc_transport_handle_v1 listener) {
    return trevrpc_rpc_transport_listener_close(context, listener);
}

static int trevrpc_transport_rpc_release_handle(
    void* context, trevrpc_transport_handle_v1 handle, uint32_t object_kind) {
    return trevrpc_rpc_transport_release_handle(context, handle, object_kind);
}

static int trevrpc_transport_rpc_close(void* context) {
    return trevrpc_rpc_transport_close(context);
}

static int trevrpc_transport_rpc_drain(void* context) {
    return trevrpc_rpc_transport_drain(context);
}

static int trevrpc_transport_rpc_prepare_release(void* context) {
    return trevrpc_rpc_transport_prepare_release(context);
}

static void trevrpc_transport_rpc_destroy(void* context) {
    trevrpc_rpc_transport_destroy(context);
}

static const trevrpc_transport_provider_ops_v1 trevrpc_transport_rpc_provider_ops = {
    .struct_size = sizeof(trevrpc_transport_provider_ops_v1),
    .struct_version = TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1,
    .get_wake_sources = trevrpc_transport_rpc_get_wake_sources,
    .poll_timeout_ms = trevrpc_transport_rpc_poll_timeout_ms,
    .next_event = trevrpc_transport_rpc_next_event,
    .event_get_info = trevrpc_transport_rpc_event_get_info,
    .event_get_admission_info = trevrpc_transport_rpc_event_get_admission_info,
    .event_get_protocol_info = trevrpc_transport_rpc_event_get_protocol_info,
    .admission_respond = trevrpc_transport_rpc_admission_respond,
    .event_release = trevrpc_transport_rpc_event_release,
    .receive_get_info = trevrpc_transport_rpc_receive_get_info,
    .receive_release = trevrpc_transport_rpc_receive_release,
    .get_diagnostics = trevrpc_transport_rpc_get_diagnostics,
    .listen = trevrpc_transport_rpc_listen,
    .listener_get_port = trevrpc_transport_rpc_listener_get_port,
    .dial = trevrpc_transport_rpc_dial,
    .dial_cancel = trevrpc_transport_rpc_dial_cancel,
    .connection_open_bidi_stream = trevrpc_transport_rpc_stream_open,
    .stream_send = trevrpc_transport_rpc_stream_send,
    .stream_receive = trevrpc_transport_rpc_stream_receive,
    .stream_finish_send = trevrpc_transport_rpc_stream_finish_send,
    .stream_abort_receive = trevrpc_transport_rpc_stream_abort_receive,
    .stream_abort_send = trevrpc_transport_rpc_stream_abort_send,
    .stream_abort = trevrpc_transport_rpc_stream_abort,
    .stream_close = trevrpc_transport_rpc_stream_close,
    .connection_close = trevrpc_transport_rpc_connection_close,
    .listener_close = trevrpc_transport_rpc_listener_close,
    .release_handle = trevrpc_transport_rpc_release_handle,
    .close = trevrpc_transport_rpc_close,
    .drain = trevrpc_transport_rpc_drain,
    .prepare_release = trevrpc_transport_rpc_prepare_release,
    .destroy = trevrpc_transport_rpc_destroy,
};

int trevrpc_rpc_transport_provider_descriptor_create_v1(
    trevrpc_rpc_transport* provider, trevrpc_transport_provider_descriptor_v1* descriptor) {
    if (provider == NULL || descriptor == NULL) {
        return -EINVAL;
    }
    memset(descriptor, 0, sizeof(*descriptor));
    descriptor->struct_size = sizeof(*descriptor);
    descriptor->struct_version = TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1;
    descriptor->operations = &trevrpc_transport_rpc_provider_ops;
    descriptor->context = provider;
    return 0;
}
