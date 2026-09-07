#define _POSIX_C_SOURCE 200809L

#include "trevrpc_transport.h"

#include "trevrpc_rpc_transport_internal.h"
#include "trevrpc_credential_internal.h"

#include <errno.h>
#include <limits.h>
#include <string.h>

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
        trevrpc_rpc_transport_get_wake_sources(transport, internal, capacity, &count));
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
    return transport == NULL ? -EINVAL : trevrpc_rpc_transport_poll_timeout_ms(transport);
}

int trevrpc_transport_next_event(trevrpc_transport* transport, trevrpc_transport_event_v1** out_event) {
    trevrpc_rpc_transport_event_info info;
    int result;
    if (transport == NULL || out_event == NULL) {
        return -EINVAL;
    }
    for (;;) {
        result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_next_event(transport, out_event));
        if (result != 0 || *out_event == NULL) {
            return result;
        }
        result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_event_get_info(transport, *out_event, &info));
        if (result != 0) {
            trevrpc_rpc_transport_event_release(transport, *out_event);
            *out_event = NULL;
            return result;
        }
        if (info.kind != TREVRPC_RPC_TRANSPORT_EVENT_STREAM_ACCEPTED) {
            return 0;
        }
        /* This event coordinates the RPC runtime with HTTP/3 and is not part of
         * the canonical Transport ABI event vocabulary. */
        trevrpc_rpc_transport_event_release(transport, *out_event);
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
    result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_event_get_info(transport, event, &source));
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
    result =
        trevrpc_transport_normalize_result(trevrpc_rpc_transport_event_get_admission_info(transport, event, &source));
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
    result =
        trevrpc_transport_normalize_result(trevrpc_rpc_transport_event_get_protocol_info(transport, event, &source));
    if (result == 0)
        info->protocol = source.protocol;
    return result;
}

int trevrpc_transport_admission_respond_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, uint16_t http_status) {
    if (transport == NULL || event == NULL || (http_status != 200 && (http_status < 400 || http_status > 599)))
        return -EINVAL;
    return trevrpc_transport_normalize_result(trevrpc_rpc_transport_admission_respond(transport, event, http_status));
}

void trevrpc_transport_event_release(trevrpc_transport* transport, trevrpc_transport_event_v1* event) {
    if (transport != NULL && event != NULL) {
        trevrpc_rpc_transport_event_release(transport, event);
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
    result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_receive_get_info(transport, receive, &source));
    if (result == 0) {
        info->flags = source.flags;
        info->data = source.data;
        info->data_len = source.data_len;
    }
    return result;
}

void trevrpc_transport_receive_release(trevrpc_transport* transport, trevrpc_transport_receive_v1* receive) {
    if (transport != NULL && receive != NULL) {
        trevrpc_rpc_transport_receive_release(transport, receive);
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
    result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_get_diagnostics(transport, &source));
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
    result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_endpoint_listen(transport, &source, &listener));
    if (result == 0) {
        *out_listener = trevrpc_transport_public_handle(listener);
    }
    return result;
}

int trevrpc_transport_listener_get_port_v1(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 listener, uint16_t* out_port) {
    return transport == NULL || out_port == NULL
               ? -EINVAL
               : trevrpc_transport_normalize_result(trevrpc_rpc_transport_endpoint_get_port(
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
        trevrpc_rpc_transport_endpoint_dial(transport, &source, operation_id, &connection));
    if (result == 0) {
        *out_connection = trevrpc_transport_public_handle(connection);
    }
    return result;
}

int trevrpc_transport_dial_cancel(trevrpc_transport* transport, trevrpc_transport_handle_v1 connection) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_dial_cancel(
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
    result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_open(
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
    return trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_send(
        transport, trevrpc_transport_internal_handle(stream), operation_id, data, data_len));
}

int trevrpc_transport_stream_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, trevrpc_transport_receive_v1** out_receive) {
    return transport == NULL || out_receive == NULL
               ? -EINVAL
               : trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_receive(
                     transport, trevrpc_transport_internal_handle(stream), out_receive));
}

int trevrpc_transport_stream_finish_send(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_finish_send(
                                   transport, trevrpc_transport_internal_handle(stream)));
}

int trevrpc_transport_stream_abort_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_abort_receive(
                                   transport, trevrpc_transport_internal_handle(stream), application_error_code));
}

int trevrpc_transport_stream_abort_send(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_abort_send(
                                   transport, trevrpc_transport_internal_handle(stream), application_error_code));
}

int trevrpc_transport_stream_abort(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_abort(
                                   transport, trevrpc_transport_internal_handle(stream), application_error_code));
}

int trevrpc_transport_stream_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_stream_close(
                                   transport, trevrpc_transport_internal_handle(stream)));
}

int trevrpc_transport_connection_close(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 connection, uint64_t application_error_code) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_connection_close(
                                   transport, trevrpc_transport_internal_handle(connection), application_error_code));
}

int trevrpc_transport_listener_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 listener) {
    return transport == NULL ? -EINVAL
                             : trevrpc_transport_normalize_result(trevrpc_rpc_transport_listener_close(
                                   transport, trevrpc_transport_internal_handle(listener)));
}

int trevrpc_transport_release_handle(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 handle, uint32_t object_kind) {
    if (transport == NULL || object_kind < TREVRPC_TRANSPORT_OBJECT_LISTENER ||
        object_kind > TREVRPC_TRANSPORT_OBJECT_STREAM) {
        return -EINVAL;
    }
    return trevrpc_transport_normalize_result(
        trevrpc_rpc_transport_release_handle(transport, trevrpc_transport_internal_handle(handle), object_kind));
}

int trevrpc_transport_close(trevrpc_transport* transport) {
    return transport == NULL ? -EINVAL : trevrpc_transport_normalize_result(trevrpc_rpc_transport_close(transport));
}

int trevrpc_transport_drain(trevrpc_transport* transport) {
    return transport == NULL ? -EINVAL : trevrpc_transport_normalize_result(trevrpc_rpc_transport_drain(transport));
}

int trevrpc_transport_release(trevrpc_transport* transport) {
    int result;
    if (transport == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_normalize_result(trevrpc_rpc_transport_drain(transport));
    if (result != 0) {
        return result;
    }
    trevrpc_rpc_transport_destroy(transport);
    return 0;
}
