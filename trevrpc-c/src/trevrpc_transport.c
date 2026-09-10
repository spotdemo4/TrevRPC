#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L

#include "trevrpc_transport.h"
#include "trevrpc_transport_provider.h"

#include "trevrpc_rpc_transport_internal.h"
#include "trevrpc_credential_internal.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

struct trevrpc_transport {
    trevrpc_transport_provider_ops_v1 provider_ops;
    void* provider_context;
};

static int trevrpc_transport_provider_get_wake_sources(
    trevrpc_transport* transport, trevrpc_transport_provider_wake_v1* wakes, size_t capacity, size_t* out_count) {
    return transport->provider_ops.get_wake_sources(transport->provider_context, wakes, capacity, out_count);
}

static int trevrpc_transport_provider_poll_timeout_ms(trevrpc_transport* transport) {
    return transport->provider_ops.poll_timeout_ms(transport->provider_context);
}

static int trevrpc_transport_provider_next_event(trevrpc_transport* transport, trevrpc_transport_event_v1** out_event) {
    return transport->provider_ops.next_event(transport->provider_context, out_event);
}

static int trevrpc_transport_provider_event_get_info(trevrpc_transport* transport,
    const trevrpc_transport_event_v1* event,
    trevrpc_transport_provider_event_info_v1* info) {
    return transport->provider_ops.event_get_info(transport->provider_context, event, info);
}

static int trevrpc_transport_provider_event_get_admission_info(trevrpc_transport* transport,
    const trevrpc_transport_event_v1* event,
    trevrpc_transport_provider_admission_info_v1* info) {
    return transport->provider_ops.event_get_admission_info(transport->provider_context, event, info);
}

static int trevrpc_transport_provider_event_get_protocol_info(trevrpc_transport* transport,
    const trevrpc_transport_event_v1* event,
    trevrpc_transport_provider_event_protocol_info_v1* info) {
    return transport->provider_ops.event_get_protocol_info(transport->provider_context, event, info);
}

static int trevrpc_transport_provider_admission_respond(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, uint16_t status) {
    return transport->provider_ops.admission_respond(transport->provider_context, event, status);
}

static void trevrpc_transport_provider_event_release(trevrpc_transport* transport, trevrpc_transport_event_v1* event) {
    transport->provider_ops.event_release(transport->provider_context, event);
}

static int trevrpc_transport_provider_receive_get_info(trevrpc_transport* transport,
    const trevrpc_transport_receive_v1* receive,
    trevrpc_transport_provider_receive_info_v1* info) {
    return transport->provider_ops.receive_get_info(transport->provider_context, receive, info);
}

static void trevrpc_transport_provider_receive_release(
    trevrpc_transport* transport, trevrpc_transport_receive_v1* receive) {
    transport->provider_ops.receive_release(transport->provider_context, receive);
}

static int trevrpc_transport_provider_get_diagnostics(
    trevrpc_transport* transport, trevrpc_transport_provider_diagnostics_v1* diagnostics) {
    return transport->provider_ops.get_diagnostics(transport->provider_context, diagnostics);
}

static int trevrpc_transport_provider_listen(trevrpc_transport* transport,
    const trevrpc_transport_provider_endpoint_config_v1* config,
    trevrpc_transport_handle_v1* listener) {
    return transport->provider_ops.listen(transport->provider_context, config, listener);
}

static int trevrpc_transport_provider_listener_get_port(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 listener, uint16_t* port) {
    return transport->provider_ops.listener_get_port(transport->provider_context, listener, port);
}

static int trevrpc_transport_provider_dial(trevrpc_transport* transport,
    const trevrpc_transport_provider_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* connection) {
    return transport->provider_ops.dial(transport->provider_context, config, operation_id, connection);
}

static int trevrpc_transport_provider_dial_cancel(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 connection) {
    return transport->provider_ops.dial_cancel(transport->provider_context, connection);
}

static int trevrpc_transport_provider_stream_open(trevrpc_transport* transport,
    trevrpc_transport_handle_v1 connection,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* stream) {
    return transport->provider_ops.connection_open_bidi_stream(
        transport->provider_context, connection, operation_id, stream);
}

static int trevrpc_transport_provider_stream_send(trevrpc_transport* transport,
    trevrpc_transport_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len) {
    return transport->provider_ops.stream_send(transport->provider_context, stream, operation_id, body, body_len);
}

static int trevrpc_transport_provider_stream_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, trevrpc_transport_receive_v1** receive) {
    return transport->provider_ops.stream_receive(transport->provider_context, stream, receive);
}

static int trevrpc_transport_provider_stream_finish_send(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream) {
    return transport->provider_ops.stream_finish_send(transport->provider_context, stream);
}

static int trevrpc_transport_provider_stream_abort_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    return transport->provider_ops.stream_abort_receive(transport->provider_context, stream, error_code);
}

static int trevrpc_transport_provider_stream_abort_send(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    return transport->provider_ops.stream_abort_send(transport->provider_context, stream, error_code);
}

static int trevrpc_transport_provider_stream_abort(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    return transport->provider_ops.stream_abort(transport->provider_context, stream, error_code);
}

static int trevrpc_transport_provider_stream_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream) {
    return transport->provider_ops.stream_close(transport->provider_context, stream);
}

static int trevrpc_transport_provider_connection_close(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 connection, uint64_t error_code) {
    return transport->provider_ops.connection_close(transport->provider_context, connection, error_code);
}

static int trevrpc_transport_provider_listener_close(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 listener) {
    return transport->provider_ops.listener_close(transport->provider_context, listener);
}

static int trevrpc_transport_provider_release_handle(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 handle, uint32_t object_kind) {
    return transport->provider_ops.release_handle(transport->provider_context, handle, object_kind);
}

static int trevrpc_transport_provider_close(trevrpc_transport* transport) {
    return transport->provider_ops.close(transport->provider_context);
}

static int trevrpc_transport_provider_drain(trevrpc_transport* transport) {
    return transport->provider_ops.drain(transport->provider_context);
}

static int trevrpc_transport_provider_prepare_release(trevrpc_transport* transport) {
    return transport->provider_ops.prepare_release(transport->provider_context);
}

static void trevrpc_transport_provider_destroy(trevrpc_transport* transport) {
    transport->provider_ops.destroy(transport->provider_context);
}

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

static int trevrpc_transport_initialize_structure(void* structure, size_t struct_size, size_t required_size) {
    uint32_t size_field;
    uint32_t version_field = TREVRPC_TRANSPORT_STRUCT_VERSION_1;
    uint8_t* bytes = structure;
    if (structure == NULL || struct_size < required_size) {
        return -EINVAL;
    }
    if (struct_size > UINT32_MAX) {
        return -EOVERFLOW;
    }
    size_field = (uint32_t)struct_size;
    memset(structure, 0, struct_size);
    memcpy(bytes, &size_field, sizeof(size_field));
    memcpy(bytes + sizeof(size_field), &version_field, sizeof(version_field));
    return 0;
}

static int trevrpc_transport_validate_structure(
    const void* structure, uint32_t struct_size, uint32_t struct_version, size_t required_size) {
    if (structure == NULL || struct_size < required_size) {
        return -EINVAL;
    }
    return struct_version == TREVRPC_TRANSPORT_STRUCT_VERSION_1 ? 0 : -ENOTSUP;
}

static int trevrpc_transport_reserved_is_zero(const uint64_t* reserved, size_t count) {
    size_t index;
    for (index = 0; index < count; ++index) {
        if (reserved[index] != 0) {
            return 0;
        }
    }
    return 1;
}

static int trevrpc_transport_normalize_result(int result) {
    return result > 0 ? -result : result;
}

static trevrpc_rpc_transport_handle trevrpc_transport_internal_handle(trevrpc_transport_handle_v1 handle) {
    trevrpc_rpc_transport_handle result = {handle.owner, handle.slot, handle.generation};
    return result;
}

static trevrpc_transport_handle_v1 trevrpc_transport_public_handle(trevrpc_rpc_transport_handle handle) {
    trevrpc_transport_handle_v1 result = {handle.owner, handle.slot, handle.generation};
    return result;
}

static int trevrpc_transport_validate_endpoint_config(const trevrpc_transport_endpoint_config_v1* config, int dialing) {
    int result;
    if (config == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_validate_structure(config, config->struct_size, config->struct_version, sizeof(*config));
    if (result != 0) {
        return result;
    }
    if (config->protocol > TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED ||
        (config->flags & ~(TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION |
                             TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION)) != 0 ||
        config->reserved0 != 0 || config->reserved1 != 0 || config->reserved2 != 0 || config->reserved3 != 0 ||
        config->reserved4 != 0 || (config->host == NULL && config->host_len != 0) ||
        (config->server_name == NULL && config->server_name_len != 0) ||
        (config->alpn == NULL && config->alpn_len != 0) || (config->cert_file == NULL && config->cert_file_len != 0) ||
        (config->key_file == NULL && config->key_file_len != 0) ||
        (config->ca_cert_file == NULL && config->ca_cert_file_len != 0) ||
        (config->cert_data == NULL && config->cert_data_len != 0) ||
        (config->key_data == NULL && config->key_data_len != 0) ||
        (config->ca_cert_data == NULL && config->ca_cert_data_len != 0) ||
        (config->path == NULL && config->path_len != 0) || (config->origin == NULL && config->origin_len != 0) ||
        (config->webtransport_profiles & ~TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED) != 0) {
        return -EINVAL;
    }
    if (!trevrpc_transport_reserved_is_zero(config->reserved, sizeof(config->reserved) / sizeof(config->reserved[0]))) {
        return -EINVAL;
    }
    result = trevrpc_credential_validate_pointers(config->cert_file,
        config->cert_file_len,
        config->key_file,
        config->key_file_len,
        config->ca_cert_file,
        config->ca_cert_file_len,
        config->cert_data,
        config->cert_data_len,
        config->key_data,
        config->key_data_len,
        config->ca_cert_data,
        config->ca_cert_data_len);
    if (result != 0)
        return result;
    if ((config->protocol == TREVRPC_TRANSPORT_PROTOCOL_AUTO ||
            config->protocol == TREVRPC_TRANSPORT_PROTOCOL_NATIVE) &&
        config->server_name_len != 0 &&
        (!dialing || config->server_name_len != config->host_len ||
            memcmp(config->server_name, config->host, config->host_len) != 0)) {
        return -ENOTSUP;
    }
    if ((config->flags & TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION) != 0 &&
        config->protocol != TREVRPC_TRANSPORT_PROTOCOL_HTTP3 &&
        config->protocol != TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT &&
        config->protocol != TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED)
        return -ENOTSUP;
    return 0;
}

static void trevrpc_transport_internal_endpoint_config(
    const trevrpc_transport_endpoint_config_v1* source, trevrpc_rpc_transport_endpoint_config* destination) {
    static const uint8_t native_alpn[] = "trevrpc/1";
    static const uint8_t h3_alpn[] = "h3";
    memset(destination, 0, sizeof(*destination));
    destination->protocol = source->protocol;
    destination->flags = source->flags;
    destination->host = source->host;
    destination->host_len = source->host_len;
    destination->port = source->port;
    destination->peer_bidi_stream_count = source->peer_bidi_stream_count;
    destination->server_name = source->server_name;
    destination->server_name_len = source->server_name_len;
    destination->alpn = source->alpn;
    destination->alpn_len = source->alpn_len;
    if (destination->alpn_len == 0) {
        if (source->protocol == TREVRPC_TRANSPORT_PROTOCOL_HTTP3 ||
            source->protocol == TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
            destination->alpn = h3_alpn;
            destination->alpn_len = (uint32_t)(sizeof(h3_alpn) - 1u);
        } else {
            destination->alpn = native_alpn;
            destination->alpn_len = (uint32_t)(sizeof(native_alpn) - 1u);
        }
    }
    destination->cert_file = source->cert_file;
    destination->cert_file_len = source->cert_file_len;
    destination->key_file = source->key_file;
    destination->key_file_len = source->key_file_len;
    destination->ca_cert_file = source->ca_cert_file;
    destination->ca_cert_file_len = source->ca_cert_file_len;
    destination->cert_data = source->cert_data;
    destination->cert_data_len = source->cert_data_len;
    destination->key_data = source->key_data;
    destination->key_data_len = source->key_data_len;
    destination->ca_cert_data = source->ca_cert_data;
    destination->ca_cert_data_len = source->ca_cert_data_len;
    destination->path = source->path;
    destination->path_len = source->path_len;
    destination->origin = source->origin;
    destination->origin_len = source->origin_len;
    destination->webtransport_profiles = source->webtransport_profiles;
    destination->max_sessions = source->max_sessions;
    destination->max_pending_send_count = source->max_pending_send_count;
    destination->max_pending_receive_count = source->max_pending_receive_count;
    destination->max_pending_send_bytes = source->max_pending_send_bytes;
    destination->max_pending_receive_bytes = source->max_pending_receive_bytes;
    destination->max_frame_size = source->max_frame_size;
    destination->max_field_section_size = source->max_field_section_size;
    destination->max_idle_timeout_ms = source->max_idle_timeout_ms;
    destination->keep_alive_ms = source->keep_alive_ms;
    destination->stream_recv_window = source->stream_recv_window;
    destination->conn_flow_control_window = source->conn_flow_control_window;
    destination->unresolved_stream_count = source->unresolved_stream_count;
    destination->unresolved_stream_bytes = source->unresolved_stream_bytes;
    destination->unresolved_stream_timeout_ms = source->unresolved_stream_timeout_ms;
}

static int trevrpc_transport_validate_provider_ops(const trevrpc_transport_provider_ops_v1* operations) {
    if (operations == NULL || operations->struct_size < sizeof(*operations)) {
        return -EINVAL;
    }
    if (operations->struct_version != TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (!trevrpc_transport_reserved_is_zero(
            operations->reserved, sizeof(operations->reserved) / sizeof(operations->reserved[0])) ||
        operations->get_wake_sources == NULL || operations->poll_timeout_ms == NULL || operations->next_event == NULL ||
        operations->event_get_info == NULL || operations->event_get_admission_info == NULL ||
        operations->event_get_protocol_info == NULL || operations->admission_respond == NULL ||
        operations->event_release == NULL || operations->receive_get_info == NULL ||
        operations->receive_release == NULL || operations->get_diagnostics == NULL || operations->listen == NULL ||
        operations->listener_get_port == NULL || operations->dial == NULL || operations->dial_cancel == NULL ||
        operations->connection_open_bidi_stream == NULL || operations->stream_send == NULL ||
        operations->stream_receive == NULL || operations->stream_finish_send == NULL ||
        operations->stream_abort_receive == NULL || operations->stream_abort_send == NULL ||
        operations->stream_abort == NULL || operations->stream_close == NULL || operations->connection_close == NULL ||
        operations->listener_close == NULL || operations->release_handle == NULL || operations->close == NULL ||
        operations->drain == NULL || operations->prepare_release == NULL || operations->destroy == NULL) {
        return -EINVAL;
    }
    return 0;
}

uint32_t trevrpc_transport_provider_abi_version(void) {
    return TREVRPC_TRANSPORT_PROVIDER_ABI_VERSION;
}

void trevrpc_transport_provider_abi_1_anchor(void) {
}

int trevrpc_transport_provider_descriptor_v1_init(
    trevrpc_transport_provider_descriptor_v1* descriptor, size_t struct_size) {
    uint32_t size_field;
    if (descriptor == NULL || struct_size < sizeof(*descriptor)) {
        return -EINVAL;
    }
    if (struct_size > UINT32_MAX) {
        return -EOVERFLOW;
    }
    size_field = (uint32_t)struct_size;
    memset(descriptor, 0, struct_size);
    descriptor->struct_size = size_field;
    descriptor->struct_version = TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1;
    return 0;
}

int trevrpc_transport_provider_adopt_v1(
    const trevrpc_transport_provider_descriptor_v1* descriptor, trevrpc_transport** out_transport) {
    trevrpc_transport* transport;
    int result;
    if (out_transport == NULL) {
        return -EINVAL;
    }
    *out_transport = NULL;
    if (descriptor == NULL || descriptor->struct_size < sizeof(*descriptor) ||
        !trevrpc_transport_reserved_is_zero(
            descriptor->reserved, sizeof(descriptor->reserved) / sizeof(descriptor->reserved[0]))) {
        return -EINVAL;
    }
    if (descriptor->struct_version != TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    result = trevrpc_transport_validate_provider_ops(descriptor->operations);
    if (result != 0) {
        return result;
    }
    transport = calloc(1, sizeof(*transport));
    if (transport == NULL) {
        descriptor->operations->destroy(descriptor->context);
        return -ENOMEM;
    }
    transport->provider_ops = *descriptor->operations;
    transport->provider_context = descriptor->context;
    *out_transport = transport;
    return 0;
}

int trevrpc_transport_adopt_rpc_private(trevrpc_rpc_transport* provider, trevrpc_transport** out_transport) {
    trevrpc_transport_provider_descriptor_v1 descriptor;
    int result;
    if (provider == NULL || out_transport == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_provider_descriptor_v1_init(&descriptor, sizeof(descriptor));
    if (result != 0) {
        return result;
    }
    descriptor.operations = &trevrpc_transport_rpc_provider_ops;
    descriptor.context = provider;
    return trevrpc_transport_provider_adopt_v1(&descriptor, out_transport);
}

uint32_t trevrpc_transport_abi_version(void) {
    return TREVRPC_TRANSPORT_ABI_VERSION;
}

void trevrpc_transport_abi_1_anchor(void) {
}

int trevrpc_transport_config_v1_init(trevrpc_transport_config_v1* config, size_t struct_size) {
    int result = trevrpc_transport_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->event_capacity = TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY;
        config->listener_capacity = TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY;
        config->connection_capacity = TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY;
        config->stream_capacity = TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY;
        config->max_receive_owned_count = TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT;
        config->max_receive_owned_bytes = TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES;
    }
    return result;
}

int trevrpc_transport_endpoint_config_v1_init(trevrpc_transport_endpoint_config_v1* config, size_t struct_size) {
    int result = trevrpc_transport_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->protocol = TREVRPC_TRANSPORT_PROTOCOL_AUTO;
        config->peer_bidi_stream_count = 100;
        config->webtransport_profiles = TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED;
        config->max_sessions = 1;
        config->max_frame_size = TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE;
        config->max_field_section_size = TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE;
        config->max_pending_send_bytes = TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_BYTES;
        config->max_pending_receive_bytes = TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_BYTES;
        config->max_pending_send_count = TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_COUNT;
        config->max_pending_receive_count = TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_COUNT;
        config->unresolved_stream_count = TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_COUNT;
        config->unresolved_stream_bytes = TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_BYTES;
        config->unresolved_stream_timeout_ms = TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_TIMEOUT_MS;
    }
    return result;
}

int trevrpc_transport_wake_source_v1_init(trevrpc_transport_wake_source_v1* wake_source, size_t struct_size) {
    return trevrpc_transport_initialize_structure(wake_source, struct_size, sizeof(*wake_source));
}

int trevrpc_transport_event_info_v1_init(trevrpc_transport_event_info_v1* info, size_t struct_size) {
    return trevrpc_transport_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_transport_receive_info_v1_init(trevrpc_transport_receive_info_v1* info, size_t struct_size) {
    return trevrpc_transport_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_transport_admission_info_v1_init(trevrpc_transport_admission_info_v1* info, size_t struct_size) {
    return trevrpc_transport_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_transport_event_protocol_info_v1_init(trevrpc_transport_event_protocol_info_v1* info, size_t struct_size) {
    return trevrpc_transport_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_transport_diagnostics_v1_init(trevrpc_transport_diagnostics_v1* diagnostics, size_t struct_size) {
    int result = trevrpc_transport_initialize_structure(diagnostics, struct_size, sizeof(*diagnostics));
    if (result == 0) {
        diagnostics->transport_abi_version = TREVRPC_TRANSPORT_ABI_VERSION;
    }
    return result;
}

int trevrpc_transport_get_wake_sources_v1(
    trevrpc_transport* transport, trevrpc_transport_wake_source_v1* wake_sources, size_t capacity, size_t* out_count) {
    trevrpc_rpc_transport_wake internal[TREVRPC_TRANSPORT_MAX_WAKE_SOURCES];
    size_t count;
    size_t index;
    int result;
    if (transport == NULL || wake_sources == NULL || out_count == NULL || capacity == 0 ||
        capacity > TREVRPC_TRANSPORT_MAX_WAKE_SOURCES) {
        return -EINVAL;
    }
    for (index = 0; index < capacity; ++index) {
        result = trevrpc_transport_validate_structure(&wake_sources[index],
            wake_sources[index].struct_size,
            wake_sources[index].struct_version,
            sizeof(wake_sources[index]));
        if (result != 0) {
            return result;
        }
        if (!trevrpc_transport_reserved_is_zero(wake_sources[index].reserved,
                sizeof(wake_sources[index].reserved) / sizeof(wake_sources[index].reserved[0]))) {
            return -EINVAL;
        }
    }
    result = trevrpc_transport_normalize_result(
        trevrpc_transport_provider_get_wake_sources(transport, internal, capacity, &count));
    if (result != 0) {
        return result;
    }
    for (index = 0; index < count; ++index) {
        wake_sources[index].kind = internal[index].kind;
        wake_sources[index].flags = internal[index].flags;
        wake_sources[index].native_handle = internal[index].native_handle;
    }
    *out_count = count;
    return 0;
}

int trevrpc_transport_poll_timeout_ms(trevrpc_transport* transport) {
    return transport == NULL ? -EINVAL : trevrpc_transport_provider_poll_timeout_ms(transport);
}

int trevrpc_transport_next_event(trevrpc_transport* transport, trevrpc_transport_event_v1** out_event) {
    trevrpc_rpc_transport_event_info info;
    int result;
    if (transport == NULL || out_event == NULL) {
        return -EINVAL;
    }
    for (;;) {
        result = trevrpc_transport_normalize_result(trevrpc_transport_provider_next_event(transport, out_event));
        if (result != 0 || *out_event == NULL) {
            return result;
        }
        result =
            trevrpc_transport_normalize_result(trevrpc_transport_provider_event_get_info(transport, *out_event, &info));
        if (result != 0) {
            trevrpc_transport_provider_event_release(transport, *out_event);
            *out_event = NULL;
            return result;
        }
        if (info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_ACCEPTED) {
            return 0;
        }
        /* This event coordinates the RPC runtime with HTTP/3 and is not part of
         * the canonical Transport ABI event vocabulary. */
        trevrpc_transport_provider_event_release(transport, *out_event);
        *out_event = NULL;
    }
}

int trevrpc_transport_event_get_info_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, trevrpc_transport_event_info_v1* info) {
    trevrpc_rpc_transport_event_info source;
    int result;
    if (transport == NULL || event == NULL || info == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_validate_structure(info, info->struct_size, info->struct_version, sizeof(*info));
    if (result != 0) {
        return result;
    }
    if (!trevrpc_transport_reserved_is_zero(info->reserved, sizeof(info->reserved) / sizeof(info->reserved[0]))) {
        return -EINVAL;
    }
    result = trevrpc_transport_normalize_result(trevrpc_transport_provider_event_get_info(transport, event, &source));
    if (result != 0) {
        return result;
    }
    info->kind = source.kind;
    info->flags = source.flags;
    info->status = trevrpc_transport_normalize_result(source.status);
    info->subject_kind = source.subject_kind;
    info->sequence = source.sequence;
    info->subject = trevrpc_transport_public_handle(source.subject);
    info->parent = trevrpc_transport_public_handle(source.parent);
    info->operation_id = source.operation_id;
    info->application_error_code = source.application_error_code;
    info->provider_error_code = source.provider_error_code;
    info->data = source.data;
    info->data_len = source.data_len;
    return 0;
}

int trevrpc_transport_event_get_admission_info_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, trevrpc_transport_admission_info_v1* info) {
    trevrpc_rpc_transport_admission_info source;
    int result;
    if (transport == NULL || event == NULL || info == NULL)
        return -EINVAL;
    result = trevrpc_transport_validate_structure(info, info->struct_size, info->struct_version, sizeof(*info));
    if (result != 0)
        return result;
    if (info->reserved0 != 0 ||
        !trevrpc_transport_reserved_is_zero(info->reserved, sizeof(info->reserved) / sizeof(info->reserved[0])))
        return -EINVAL;
    result = trevrpc_transport_normalize_result(
        trevrpc_transport_provider_event_get_admission_info(transport, event, &source));
    if (result != 0)
        return result;
    info->protocol = source.protocol;
    info->listener = trevrpc_transport_public_handle(source.listener);
    info->headers = source.headers;
    info->header_count = source.header_count;
    info->method = source.method;
    info->method_len = source.method_len;
    info->path = source.path;
    info->path_len = source.path_len;
    info->authority = source.authority;
    info->authority_len = source.authority_len;
    info->origin = source.origin;
    info->origin_len = source.origin_len;
    return 0;
}

int trevrpc_transport_event_get_protocol_info_v1(trevrpc_transport* transport,
    const trevrpc_transport_event_v1* event,
    trevrpc_transport_event_protocol_info_v1* info) {
    trevrpc_rpc_transport_event_protocol_info source;
    int result;
    if (transport == NULL || event == NULL || info == NULL)
        return -EINVAL;
    result = trevrpc_transport_validate_structure(info, info->struct_size, info->struct_version, sizeof(*info));
    if (result != 0)
        return result;
    if (info->reserved0 != 0 ||
        !trevrpc_transport_reserved_is_zero(info->reserved, sizeof(info->reserved) / sizeof(info->reserved[0])))
        return -EINVAL;
    result = trevrpc_transport_normalize_result(
        trevrpc_transport_provider_event_get_protocol_info(transport, event, &source));
    if (result == 0)
        info->protocol = source.protocol;
    return result;
}

int trevrpc_transport_admission_respond_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, uint16_t http_status) {
    if (transport == NULL || event == NULL || (http_status != 200 && (http_status < 400 || http_status > 599)))
        return -EINVAL;
    return trevrpc_transport_normalize_result(
        trevrpc_transport_provider_admission_respond(transport, event, http_status));
}

void trevrpc_transport_event_release(trevrpc_transport* transport, trevrpc_transport_event_v1* event) {
    if (transport != NULL && event != NULL) {
        trevrpc_transport_provider_event_release(transport, event);
    }
}

int trevrpc_transport_receive_get_info_v1(trevrpc_transport* transport,
    const trevrpc_transport_receive_v1* receive,
    trevrpc_transport_receive_info_v1* info) {
    trevrpc_rpc_transport_receive_info source;
    int result;
    if (transport == NULL || receive == NULL || info == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_validate_structure(info, info->struct_size, info->struct_version, sizeof(*info));
    if (result != 0) {
        return result;
    }
    if (info->reserved0 != 0 ||
        !trevrpc_transport_reserved_is_zero(info->reserved, sizeof(info->reserved) / sizeof(info->reserved[0]))) {
        return -EINVAL;
    }
    result =
        trevrpc_transport_normalize_result(trevrpc_transport_provider_receive_get_info(transport, receive, &source));
    if (result == 0) {
        info->flags = source.flags;
        info->data = source.data;
        info->data_len = source.data_len;
    }
    return result;
}

void trevrpc_transport_receive_release(trevrpc_transport* transport, trevrpc_transport_receive_v1* receive) {
    if (transport != NULL && receive != NULL) {
        trevrpc_transport_provider_receive_release(transport, receive);
    }
}

int trevrpc_transport_get_diagnostics_v1(trevrpc_transport* transport, trevrpc_transport_diagnostics_v1* diagnostics) {
    trevrpc_rpc_transport_diagnostics source;
    int result;
    if (transport == NULL || diagnostics == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_validate_structure(
        diagnostics, diagnostics->struct_size, diagnostics->struct_version, sizeof(*diagnostics));
    if (result != 0) {
        return result;
    }
    if (!trevrpc_transport_reserved_is_zero(
            diagnostics->reserved, sizeof(diagnostics->reserved) / sizeof(diagnostics->reserved[0]))) {
        return -EINVAL;
    }
    result = trevrpc_transport_normalize_result(trevrpc_transport_provider_get_diagnostics(transport, &source));
    if (result != 0) {
        return result;
    }
    diagnostics->transport_abi_version = TREVRPC_TRANSPORT_ABI_VERSION;
    diagnostics->state = source.state;
    diagnostics->terminal_status = trevrpc_transport_normalize_result(source.terminal_status);
    diagnostics->event_capacity = source.event_capacity;
    diagnostics->queue_depth = source.queue_depth;
    diagnostics->ordinary_queue_depth = source.ordinary_queue_depth;
    diagnostics->events_enqueued = source.events_enqueued;
    diagnostics->events_dequeued = source.events_dequeued;
    diagnostics->events_rejected = source.events_rejected;
    diagnostics->receive_owned_count = source.receive_owned_count;
    diagnostics->peak_receive_owned_count = source.peak_receive_owned_count;
    diagnostics->receive_owned_bytes = source.receive_owned_bytes;
    diagnostics->peak_receive_owned_bytes = source.peak_receive_owned_bytes;
    diagnostics->pending_send_bytes = source.pending_send_bytes;
    diagnostics->pending_send_count = source.pending_send_count;
    diagnostics->live_listeners = source.live_listeners;
    diagnostics->live_connections = source.live_connections;
    diagnostics->live_streams = source.live_streams;
    diagnostics->active_callbacks = source.active_callbacks;
    diagnostics->active_api_calls = source.active_api_calls;
    diagnostics->wake_signals = source.wake_signals;
    diagnostics->wake_write_eagain = source.wake_write_eagain;
    diagnostics->wake_failures = source.wake_failures;
    diagnostics->provider_error_code = source.provider_error_code;
    diagnostics->mandatory_reservations = source.mandatory_reservations;
    return 0;
}

int trevrpc_transport_listen_v1(trevrpc_transport* transport,
    const trevrpc_transport_endpoint_config_v1* config,
    trevrpc_transport_handle_v1* out_listener) {
    trevrpc_rpc_transport_endpoint_config source;
    trevrpc_rpc_transport_handle listener;
    int result;
    if (transport == NULL || out_listener == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_validate_endpoint_config(config, 0);
    if (result != 0) {
        return result;
    }
    if (config->protocol == TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED && config->alpn_len != 0)
        return -EINVAL;
    trevrpc_transport_internal_endpoint_config(config, &source);
    result = trevrpc_transport_normalize_result(trevrpc_transport_provider_listen(transport, &source, &listener));
    if (result == 0) {
        *out_listener = trevrpc_transport_public_handle(listener);
    }
    return result;
}

int trevrpc_transport_listener_get_port_v1(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 listener, uint16_t* out_port) {
    return transport == NULL || out_port == NULL
               ? -EINVAL
               : trevrpc_transport_normalize_result(trevrpc_transport_provider_listener_get_port(
                     transport, trevrpc_transport_internal_handle(listener), out_port));
}

int trevrpc_transport_dial_v1(trevrpc_transport* transport,
    const trevrpc_transport_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* out_connection) {
    trevrpc_rpc_transport_endpoint_config source;
    trevrpc_rpc_transport_handle connection;
    int result;
    if (transport == NULL || out_connection == NULL || operation_id == 0) {
        return -EINVAL;
    }
    result = trevrpc_transport_validate_endpoint_config(config, 1);
    if (result != 0) {
        return result;
    }
    if (config->protocol == TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED ||
        (config->flags & TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION) != 0)
        return -ENOTSUP;
    trevrpc_transport_internal_endpoint_config(config, &source);
    result = trevrpc_transport_normalize_result(
        trevrpc_transport_provider_dial(transport, &source, operation_id, &connection));
    if (result == 0) {
        *out_connection = trevrpc_transport_public_handle(connection);
    }
    return result;
}

int trevrpc_transport_dial_cancel(trevrpc_transport* transport, trevrpc_transport_handle_v1 connection) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_dial_cancel(
                                   transport, trevrpc_transport_internal_handle(connection)));
}

int trevrpc_transport_connection_open_bidi_stream_v1(trevrpc_transport* transport,
    trevrpc_transport_handle_v1 connection,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* out_stream) {
    trevrpc_rpc_transport_handle stream;
    int result;
    if (transport == NULL || out_stream == NULL || operation_id == 0) {
        return -EINVAL;
    }
    result = trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_open(
        transport, trevrpc_transport_internal_handle(connection), operation_id, &stream));
    if (result == 0) {
        *out_stream = trevrpc_transport_public_handle(stream);
    }
    return result;
}

int trevrpc_transport_stream_send_v1(trevrpc_transport* transport,
    trevrpc_transport_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* data,
    size_t data_len) {
    if (transport == NULL || operation_id == 0 || (data == NULL && data_len != 0)) {
        return -EINVAL;
    }
    return trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_send(
        transport, trevrpc_transport_internal_handle(stream), operation_id, data, data_len));
}

int trevrpc_transport_stream_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, trevrpc_transport_receive_v1** out_receive) {
    return transport == NULL || out_receive == NULL
               ? -EINVAL
               : trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_receive(
                     transport, trevrpc_transport_internal_handle(stream), out_receive));
}

int trevrpc_transport_stream_finish_send(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_finish_send(
                                   transport, trevrpc_transport_internal_handle(stream)));
}

int trevrpc_transport_stream_abort_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_abort_receive(
                                   transport, trevrpc_transport_internal_handle(stream), application_error_code));
}

int trevrpc_transport_stream_abort_send(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_abort_send(
                                   transport, trevrpc_transport_internal_handle(stream), application_error_code));
}

int trevrpc_transport_stream_abort(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_abort(
                                   transport, trevrpc_transport_internal_handle(stream), application_error_code));
}

int trevrpc_transport_stream_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_stream_close(
                                   transport, trevrpc_transport_internal_handle(stream)));
}

int trevrpc_transport_connection_close(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 connection, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_connection_close(
                                   transport, trevrpc_transport_internal_handle(connection), application_error_code));
}

int trevrpc_transport_listener_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 listener) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_listener_close(
                                   transport, trevrpc_transport_internal_handle(listener)));
}

int trevrpc_transport_release_handle(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 handle, uint32_t object_kind) {
    if (transport == NULL || object_kind < TREVRPC_TRANSPORT_OBJECT_LISTENER ||
        object_kind > TREVRPC_TRANSPORT_OBJECT_STREAM) {
        return -EINVAL;
    }
    return trevrpc_transport_normalize_result(
        trevrpc_transport_provider_release_handle(transport, trevrpc_transport_internal_handle(handle), object_kind));
}

int trevrpc_transport_close(trevrpc_transport* transport) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_close(transport));
}

int trevrpc_transport_drain(trevrpc_transport* transport) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_transport_provider_drain(transport));
}

int trevrpc_transport_release(trevrpc_transport* transport) {
    int result;
    if (transport == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_normalize_result(trevrpc_transport_provider_drain(transport));
    if (result != 0) {
        return result;
    }
    result = trevrpc_transport_normalize_result(trevrpc_transport_provider_prepare_release(transport));
    if (result != 0) {
        return result;
    }
    trevrpc_transport_provider_destroy(transport);
    free(transport);
    return 0;
}
