#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L

#include "trevrpc_rpc_transport_engine_internal.h"
#include "trevrpc_engine_msquic_internal.h"
#include "trevrpc_credential_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef struct trevrpc_rpc_transport_engine {
    trevrpc_rpc_transport base;
    trevrpc_engine* engine;
    trevrpc_credential_cleanup_owner credential_cleanup;
} trevrpc_rpc_transport_engine;

static trevrpc_rpc_transport_engine* trevrpc_rpc_transport_engine_from_base(trevrpc_rpc_transport* transport) {
    return (trevrpc_rpc_transport_engine*)transport;
}

static trevrpc_engine_handle_v1 trevrpc_rpc_transport_engine_handle(trevrpc_rpc_transport_handle handle) {
    trevrpc_engine_handle_v1 result = {handle.owner, handle.slot, handle.generation};
    return result;
}

static trevrpc_rpc_transport_handle trevrpc_rpc_transport_handle_from_engine(trevrpc_engine_handle_v1 handle) {
    trevrpc_rpc_transport_handle result = {handle.owner, handle.slot, handle.generation};
    return result;
}

static int trevrpc_rpc_transport_engine_endpoint_config(const trevrpc_rpc_transport_endpoint_config* source,
    trevrpc_engine_endpoint_config_v1* destination,
    trevrpc_credential_files* credential_files,
    int server) {
    int result = trevrpc_credential_validate_endpoint(source->cert_file,
        source->cert_file_len,
        source->key_file,
        source->key_file_len,
        source->ca_cert_file,
        source->ca_cert_file_len,
        source->cert_data,
        source->cert_data_len,
        source->key_data,
        source->key_data_len,
        source->ca_cert_data,
        source->ca_cert_data_len,
        server);
    if (result != 0)
        return result;
    result = trevrpc_credential_files_prepare(credential_files,
        source->cert_data,
        source->cert_data_len,
        source->key_data,
        source->key_data_len,
        source->ca_cert_data,
        source->ca_cert_data_len);
    if (result != 0)
        return result;
    (void)trevrpc_engine_endpoint_config_v1_init(destination, sizeof(*destination));
    destination->host = source->host;
    destination->host_len = source->host_len;
    destination->port = source->port;
    destination->peer_bidi_stream_count = source->peer_bidi_stream_count;
    destination->alpn = source->alpn;
    destination->alpn_len = source->alpn_len;
    destination->flags = source->flags & TREVRPC_RPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION
                             ? TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION
                             : 0;
    destination->cert_file = credential_files->cert_created ? credential_files->cert_file
                                                            : (source->cert_file_len != 0 ? source->cert_file : NULL);
    destination->cert_file_len =
        credential_files->cert_created ? (uint32_t)strlen(credential_files->cert_file) : source->cert_file_len;
    destination->key_file = credential_files->key_created ? credential_files->key_file
                                                          : (source->key_file_len != 0 ? source->key_file : NULL);
    destination->key_file_len =
        credential_files->key_created ? (uint32_t)strlen(credential_files->key_file) : source->key_file_len;
    destination->ca_cert_file = credential_files->ca_cert_created
                                    ? credential_files->ca_cert_file
                                    : (source->ca_cert_file_len != 0 ? source->ca_cert_file : NULL);
    destination->ca_cert_file_len =
        credential_files->ca_cert_created ? (uint32_t)strlen(credential_files->ca_cert_file) : source->ca_cert_file_len;
    destination->max_pending_send_count = source->max_pending_send_count;
    destination->max_pending_send_bytes = source->max_pending_send_bytes;
    destination->max_frame_size = source->max_frame_size;
    destination->max_idle_timeout_ms = source->max_idle_timeout_ms;
    destination->keep_alive_ms = source->keep_alive_ms;
    destination->stream_recv_window = source->stream_recv_window;
    destination->conn_flow_control_window = source->conn_flow_control_window;
    return 0;
}

int trevrpc_rpc_transport_engine_adopt_accepted_connection(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_rpc_transport_handle* out_connection) {
    trevrpc_engine_endpoint_config_v1 source;
    trevrpc_engine_handle_v1 target;
    trevrpc_credential_files credential_files = {0};
    trevrpc_credential_cleanup_lease* cleanup_lease = NULL;
    trevrpc_rpc_transport_engine* adapter = trevrpc_rpc_transport_engine_from_base(transport);
    int result;
    if (transport == NULL || config == NULL || accepted == NULL || out_connection == NULL)
        return -EINVAL;
    result = trevrpc_credential_cleanup_owner_prepare_lease(&adapter->credential_cleanup,
        trevrpc_credential_cleanup_required(config->cert_data_len, config->key_data_len, config->ca_cert_data_len),
        &cleanup_lease);
    if (result != 0)
        return result;
    result = trevrpc_rpc_transport_engine_endpoint_config(config, &source, &credential_files, 0);
    if (result != 0) {
        trevrpc_credential_cleanup_owner_finish(&adapter->credential_cleanup, &credential_files, &cleanup_lease);
        return result;
    }
    result = trevrpc_engine_msquic_adopt_accepted_connection_v1(adapter->engine, &source, accepted, &target);
    trevrpc_credential_cleanup_owner_finish(&adapter->credential_cleanup, &credential_files, &cleanup_lease);
    if (result == 0)
        *out_connection = trevrpc_rpc_transport_handle_from_engine(target);
    return result;
}

static int trevrpc_rpc_transport_engine_get_wake_source(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wake) {
    trevrpc_engine_wake_source_v1 source;
    int result = trevrpc_engine_wake_source_v1_init(&source, sizeof(source));
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_get_wake_source_v1(trevrpc_rpc_transport_engine_from_base(transport)->engine, &source);
    if (result == 0) {
        wake->kind = source.kind;
        wake->flags = source.flags;
        wake->native_handle = source.native_handle;
    }
    return result;
}

static int trevrpc_rpc_transport_engine_next_event(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** out_event) {
    trevrpc_engine_event* event = NULL;
    int result = trevrpc_engine_next_event(trevrpc_rpc_transport_engine_from_base(transport)->engine, &event);
    if (result == 0) {
        *out_event = (trevrpc_rpc_transport_event*)event;
    }
    return result;
}

static int trevrpc_rpc_transport_engine_event_get_info(
    const trevrpc_rpc_transport_event* event, trevrpc_rpc_transport_event_info* info) {
    trevrpc_engine_event_info_v1 source;
    int result = trevrpc_engine_event_info_v1_init(&source, sizeof(source));
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_event_get_info_v1((const trevrpc_engine_event*)event, &source);
    if (result != 0) {
        return result;
    }
    info->kind = source.kind;
    info->flags = source.flags;
    info->status = source.status;
    info->subject_kind = source.subject_kind;
    info->sequence = source.sequence;
    info->subject = trevrpc_rpc_transport_handle_from_engine(source.subject);
    info->parent = trevrpc_rpc_transport_handle_from_engine(source.parent);
    info->operation_id = source.operation_id;
    info->application_error_code = source.application_error_code;
    info->provider_error_code = source.provider_error_code;
    info->data = source.data;
    info->data_len = source.data_len;
    return 0;
}

static int trevrpc_rpc_transport_engine_event_get_protocol_info(
    const trevrpc_rpc_transport_event* event, trevrpc_rpc_transport_event_protocol_info* info) {
    trevrpc_engine_event_info_v1 source;
    int result;
    if (event == NULL || info == NULL)
        return -EINVAL;
    result = trevrpc_engine_event_info_v1_init(&source, sizeof(source));
    if (result != 0)
        return result;
    result = trevrpc_engine_event_get_info_v1((const trevrpc_engine_event*)event, &source);
    if (result != 0)
        return result;
    if (source.subject_kind == TREVRPC_ENGINE_OBJECT_NONE)
        return -ENOTSUP;
    info->protocol = TREVRPC_RPC_TRANSPORT_PROTOCOL_NATIVE;
    return 0;
}

static void trevrpc_rpc_transport_engine_event_release(trevrpc_rpc_transport_event* event) {
    trevrpc_engine_event_release((trevrpc_engine_event*)event);
}

static int trevrpc_rpc_transport_engine_receive_get_info(
    const trevrpc_rpc_transport_receive* receive, trevrpc_rpc_transport_receive_info* info) {
    trevrpc_engine_receive_info_v1 source;
    int result = trevrpc_engine_receive_info_v1_init(&source, sizeof(source));
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_receive_get_info_v1((const trevrpc_engine_receive*)receive, &source);
    if (result == 0) {
        info->flags = source.flags;
        info->data = source.data;
        info->data_len = source.data_len;
    }
    return result;
}

static void trevrpc_rpc_transport_engine_receive_release(trevrpc_rpc_transport_receive* receive) {
    trevrpc_engine_receive_release((trevrpc_engine_receive*)receive);
}

static int trevrpc_rpc_transport_engine_get_diagnostics(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_diagnostics* diagnostics) {
    trevrpc_engine_diagnostics_v1 source;
    int result = trevrpc_engine_diagnostics_v1_init(&source, sizeof(source));
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_get_diagnostics_v1(trevrpc_rpc_transport_engine_from_base(transport)->engine, &source);
    if (result != 0) {
        return result;
    }
    diagnostics->state = source.state;
    diagnostics->terminal_status = source.terminal_status;
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

static int trevrpc_rpc_transport_engine_endpoint_listen(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_rpc_transport_handle* listener) {
    trevrpc_engine_endpoint_config_v1 source;
    trevrpc_engine_handle_v1 target;
    trevrpc_credential_files credential_files = {0};
    trevrpc_credential_cleanup_lease* cleanup_lease = NULL;
    trevrpc_rpc_transport_engine* adapter = trevrpc_rpc_transport_engine_from_base(transport);
    int result;
    if (config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_AUTO &&
        config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_NATIVE) {
        return -ENOTSUP;
    }
    result = trevrpc_credential_cleanup_owner_prepare_lease(&adapter->credential_cleanup,
        trevrpc_credential_cleanup_required(config->cert_data_len, config->key_data_len, config->ca_cert_data_len),
        &cleanup_lease);
    if (result != 0)
        return result;
    result = trevrpc_rpc_transport_engine_endpoint_config(config, &source, &credential_files, 1);
    if (result != 0) {
        trevrpc_credential_cleanup_owner_finish(&adapter->credential_cleanup, &credential_files, &cleanup_lease);
        return result;
    }
    result = trevrpc_engine_listen_v1(adapter->engine, &source, &target);
    trevrpc_credential_cleanup_owner_finish(&adapter->credential_cleanup, &credential_files, &cleanup_lease);
    if (result == 0) {
        *listener = trevrpc_rpc_transport_handle_from_engine(target);
    }
    return result;
}

static int trevrpc_rpc_transport_engine_endpoint_get_port(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener, uint16_t* port) {
    return trevrpc_engine_listener_get_port_v1(
        trevrpc_rpc_transport_engine_from_base(transport)->engine, trevrpc_rpc_transport_engine_handle(listener), port);
}

static int trevrpc_rpc_transport_engine_endpoint_dial(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* connection) {
    trevrpc_engine_endpoint_config_v1 source;
    trevrpc_engine_handle_v1 target;
    trevrpc_credential_files credential_files = {0};
    trevrpc_credential_cleanup_lease* cleanup_lease = NULL;
    trevrpc_rpc_transport_engine* adapter = trevrpc_rpc_transport_engine_from_base(transport);
    int result;
    if (config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_AUTO &&
        config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_NATIVE) {
        return -ENOTSUP;
    }
    result = trevrpc_credential_cleanup_owner_prepare_lease(&adapter->credential_cleanup,
        trevrpc_credential_cleanup_required(config->cert_data_len, config->key_data_len, config->ca_cert_data_len),
        &cleanup_lease);
    if (result != 0)
        return result;
    result = trevrpc_rpc_transport_engine_endpoint_config(config, &source, &credential_files, 0);
    if (result != 0) {
        trevrpc_credential_cleanup_owner_finish(&adapter->credential_cleanup, &credential_files, &cleanup_lease);
        return result;
    }
    result = trevrpc_engine_dial_v1(adapter->engine, &source, operation_id, &target);
    trevrpc_credential_cleanup_owner_finish(&adapter->credential_cleanup, &credential_files, &cleanup_lease);
    if (result == 0) {
        *connection = trevrpc_rpc_transport_handle_from_engine(target);
    }
    return result;
}

static int trevrpc_rpc_transport_engine_dial_cancel(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection) {
    return trevrpc_engine_dial_cancel(
        trevrpc_rpc_transport_engine_from_base(transport)->engine, trevrpc_rpc_transport_engine_handle(connection));
}

static int trevrpc_rpc_transport_engine_stream_open(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* stream) {
    trevrpc_engine_handle_v1 target;
    int result =
        trevrpc_engine_connection_open_bidi_stream_v1(trevrpc_rpc_transport_engine_from_base(transport)->engine,
            trevrpc_rpc_transport_engine_handle(connection),
            operation_id,
            &target);
    if (result == 0) {
        *stream = trevrpc_rpc_transport_handle_from_engine(target);
    }
    return result;
}

static int trevrpc_rpc_transport_engine_stream_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len) {
    return trevrpc_engine_stream_send_frame_v1(trevrpc_rpc_transport_engine_from_base(transport)->engine,
        trevrpc_rpc_transport_engine_handle(stream),
        operation_id,
        body,
        body_len);
}

static int trevrpc_rpc_transport_engine_stream_receive(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    trevrpc_rpc_transport_receive** out_receive) {
    trevrpc_engine_receive* receive = NULL;
    int result = trevrpc_engine_stream_receive_frame(trevrpc_rpc_transport_engine_from_base(transport)->engine,
        trevrpc_rpc_transport_engine_handle(stream),
        &receive);
    if (result == 0) {
        *out_receive = (trevrpc_rpc_transport_receive*)receive;
    }
    return result;
}

static int trevrpc_rpc_transport_engine_stream_finish_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    return trevrpc_engine_stream_finish_send(
        trevrpc_rpc_transport_engine_from_base(transport)->engine, trevrpc_rpc_transport_engine_handle(stream));
}

static int trevrpc_rpc_transport_engine_stream_abort_receive(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    return trevrpc_engine_stream_abort_receive(trevrpc_rpc_transport_engine_from_base(transport)->engine,
        trevrpc_rpc_transport_engine_handle(stream),
        error_code);
}

static int trevrpc_rpc_transport_engine_stream_abort_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    return trevrpc_engine_stream_abort_send(trevrpc_rpc_transport_engine_from_base(transport)->engine,
        trevrpc_rpc_transport_engine_handle(stream),
        error_code);
}

static int trevrpc_rpc_transport_engine_stream_abort(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    return trevrpc_engine_stream_abort(trevrpc_rpc_transport_engine_from_base(transport)->engine,
        trevrpc_rpc_transport_engine_handle(stream),
        error_code);
}

static int trevrpc_rpc_transport_engine_stream_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    return trevrpc_engine_stream_close(
        trevrpc_rpc_transport_engine_from_base(transport)->engine, trevrpc_rpc_transport_engine_handle(stream));
}

static int trevrpc_rpc_transport_engine_connection_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t error_code) {
    return trevrpc_engine_connection_close(trevrpc_rpc_transport_engine_from_base(transport)->engine,
        trevrpc_rpc_transport_engine_handle(connection),
        error_code);
}

static int trevrpc_rpc_transport_engine_listener_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener) {
    return trevrpc_engine_listener_close(
        trevrpc_rpc_transport_engine_from_base(transport)->engine, trevrpc_rpc_transport_engine_handle(listener));
}

static int trevrpc_rpc_transport_engine_close(trevrpc_rpc_transport* transport) {
    return trevrpc_engine_close(trevrpc_rpc_transport_engine_from_base(transport)->engine);
}

static int trevrpc_rpc_transport_engine_drain(trevrpc_rpc_transport* transport) {
    trevrpc_rpc_transport_engine* adapter = trevrpc_rpc_transport_engine_from_base(transport);
    int result = trevrpc_engine_drain(adapter->engine);
    trevrpc_credential_cleanup_owner_progress(&adapter->credential_cleanup);
    return result;
}

static int trevrpc_rpc_transport_engine_prepare_release(trevrpc_rpc_transport* transport) {
    return trevrpc_credential_cleanup_owner_prepare_release(
        &trevrpc_rpc_transport_engine_from_base(transport)->credential_cleanup);
}

static void trevrpc_rpc_transport_engine_destroy(trevrpc_rpc_transport* transport) {
    trevrpc_rpc_transport_engine* adapter = trevrpc_rpc_transport_engine_from_base(transport);
    (void)trevrpc_engine_release(adapter->engine);
    trevrpc_credential_cleanup_owner_destroy(&adapter->credential_cleanup);
    free(adapter);
}

static const trevrpc_rpc_transport_ops trevrpc_rpc_transport_engine_ops = {
    .get_wake_source = trevrpc_rpc_transport_engine_get_wake_source,
    .next_event = trevrpc_rpc_transport_engine_next_event,
    .event_get_info = trevrpc_rpc_transport_engine_event_get_info,
    .event_release = trevrpc_rpc_transport_engine_event_release,
    .receive_get_info = trevrpc_rpc_transport_engine_receive_get_info,
    .receive_release = trevrpc_rpc_transport_engine_receive_release,
    .get_diagnostics = trevrpc_rpc_transport_engine_get_diagnostics,
    .poll_timeout_ms = NULL,
    .endpoint_listen = trevrpc_rpc_transport_engine_endpoint_listen,
    .endpoint_get_port = trevrpc_rpc_transport_engine_endpoint_get_port,
    .endpoint_dial = trevrpc_rpc_transport_engine_endpoint_dial,
    .dial_cancel = trevrpc_rpc_transport_engine_dial_cancel,
    .stream_open = trevrpc_rpc_transport_engine_stream_open,
    .stream_send = trevrpc_rpc_transport_engine_stream_send,
    .stream_receive = trevrpc_rpc_transport_engine_stream_receive,
    .stream_finish_send = trevrpc_rpc_transport_engine_stream_finish_send,
    .stream_abort_receive = trevrpc_rpc_transport_engine_stream_abort_receive,
    .stream_abort_send = trevrpc_rpc_transport_engine_stream_abort_send,
    .stream_abort = trevrpc_rpc_transport_engine_stream_abort,
    .stream_close = trevrpc_rpc_transport_engine_stream_close,
    .connection_close = trevrpc_rpc_transport_engine_connection_close,
    .listener_close = trevrpc_rpc_transport_engine_listener_close,
    .close = trevrpc_rpc_transport_engine_close,
    .drain = trevrpc_rpc_transport_engine_drain,
    .prepare_release = trevrpc_rpc_transport_engine_prepare_release,
    .destroy = trevrpc_rpc_transport_engine_destroy,
    .get_wake_sources = NULL,
    .release_handle = NULL,
    .event_get_protocol_info = trevrpc_rpc_transport_engine_event_get_protocol_info,
    .adopt_accepted_connection = trevrpc_rpc_transport_engine_adopt_accepted_connection,
};

int trevrpc_rpc_transport_engine_adopt(trevrpc_engine* engine, trevrpc_rpc_transport** out_transport) {
    trevrpc_rpc_transport_engine* adapter;
    if (engine == NULL || out_transport == NULL) {
        return -EINVAL;
    }
    adapter = calloc(1, sizeof(*adapter));
    if (adapter == NULL) {
        return -ENOMEM;
    }
    adapter->base.ops = &trevrpc_rpc_transport_engine_ops;
    adapter->engine = engine;
    if (trevrpc_credential_cleanup_owner_init(&adapter->credential_cleanup) != 0) {
        free(adapter);
        return -ENOMEM;
    }
    *out_transport = &adapter->base;
    return 0;
}
