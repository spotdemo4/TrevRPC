#include "trevrpc.h"

#include "trevrpc_runtime_internal.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static trevrpc_versioned_test_connect_hook TrevrpcVersionedTestConnectHook;
static void* TrevrpcVersionedTestConnectHookContext;

void trevrpc_versioned_test_set_connect_hook(trevrpc_versioned_test_connect_hook hook, void* context) {
    TrevrpcVersionedTestConnectHook = hook;
    TrevrpcVersionedTestConnectHookContext = context;
}

static void trevrpc_versioned_test_connect_retained(void) {
    if (TrevrpcVersionedTestConnectHook != NULL) {
        TrevrpcVersionedTestConnectHook(TrevrpcVersionedTestConnectHookContext);
    }
}

static int trevrpc_versioned_init(void* value, size_t supplied_size, size_t required_size) {
    if (value == NULL || supplied_size < required_size) {
        return -EINVAL;
    }
    if (supplied_size > UINT32_MAX) {
        return -EOVERFLOW;
    }
    memset(value, 0, supplied_size);
    uint32_t* header = value;
    header[0] = (uint32_t)supplied_size;
    header[1] = TREVRPC_STRUCT_VERSION_1;
    return 0;
}

static int trevrpc_versioned_validate(const void* value, size_t required_size) {
    if (value == NULL) {
        return -EINVAL;
    }
    const uint32_t* header = value;
    if (header[0] < required_size) {
        return -EINVAL;
    }
    if (header[1] == 0 || header[1] != TREVRPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    return 0;
}

int trevrpc_client_config_v1_init(trevrpc_client_config_v1* config, size_t struct_size) {
    int err = trevrpc_versioned_init(config, struct_size, sizeof(*config));
    if (err != 0) {
        return err;
    }
    trevrpc_client_config_internal defaults = trevrpc_internal_default_config();
    config->cert_file = defaults.cert_file;
    config->key_file = defaults.key_file;
    config->ca_cert_file = defaults.ca_cert_file;
    config->skip_certificate_validation = defaults.skip_certificate_validation;
    config->max_idle_timeout_ms = defaults.max_idle_timeout_ms;
    config->keep_alive_ms = defaults.keep_alive_ms;
    config->peer_bidi_stream_count = defaults.peer_bidi_stream_count;
    config->max_stateless_operations = defaults.max_stateless_operations;
    config->max_binding_stateless_operations = defaults.max_binding_stateless_operations;
    config->max_pending_send_bytes = defaults.max_pending_send_bytes;
    config->max_pending_send_count = defaults.max_pending_send_count;
    config->max_frame_size = defaults.max_frame_size;
    config->stream_recv_window = defaults.stream_recv_window;
    config->conn_flow_control_window = defaults.conn_flow_control_window;
    config->msquic_execution_profile = defaults.msquic_execution_profile;
    config->msquic_send_buffering_enabled = defaults.msquic_send_buffering_enabled;
    return 0;
}

int trevrpc_server_config_v1_init(trevrpc_server_config_v1* config, size_t struct_size) {
    int err = trevrpc_versioned_init(config, struct_size, sizeof(*config));
    if (err != 0) {
        return err;
    }
    trevrpc_server_config_internal defaults = trevrpc_internal_default_server_config();
    config->host = defaults.host;
    config->port = defaults.port;
    config->cert_file = defaults.cert_file;
    config->key_file = defaults.key_file;
    config->webtransport_path = defaults.webtransport_path;
    config->webtransport_origin = defaults.webtransport_origin;
    config->webtransport_admission = defaults.webtransport_admission;
    config->webtransport_admission_user_data = defaults.webtransport_admission_user_data;
    config->enable_http3 = defaults.enable_http3;
    config->http3_path = defaults.http3_path;
    config->http3_admission = defaults.http3_admission;
    config->http3_admission_user_data = defaults.http3_admission_user_data;
    config->max_idle_timeout_ms = defaults.max_idle_timeout_ms;
    config->keep_alive_ms = defaults.keep_alive_ms;
    config->peer_bidi_stream_count = defaults.peer_bidi_stream_count;
    config->max_stateless_operations = defaults.max_stateless_operations;
    config->max_binding_stateless_operations = defaults.max_binding_stateless_operations;
    config->max_pending_send_bytes = defaults.max_pending_send_bytes;
    config->max_pending_send_count = defaults.max_pending_send_count;
    config->max_sessions_per_connection = defaults.max_sessions_per_connection;
    config->max_streams_per_session = defaults.max_streams_per_session;
    config->stream_recv_window = defaults.stream_recv_window;
    config->conn_flow_control_window = defaults.conn_flow_control_window;
    config->max_frame_size = defaults.max_frame_size;
    config->msquic_execution_profile = defaults.msquic_execution_profile;
    config->msquic_send_buffering_enabled = defaults.msquic_send_buffering_enabled;
    return 0;
}

int trevrpc_server_options_v1_init(trevrpc_server_options_v1* options, size_t struct_size) {
    int err = trevrpc_versioned_init(options, struct_size, sizeof(*options));
    if (err != 0) {
        return err;
    }
    trevrpc_server_options_internal defaults = trevrpc_internal_default_server_options();
    options->max_concurrent_connections = defaults.max_concurrent_connections;
    options->max_concurrent_streams_per_connection = defaults.max_concurrent_streams_per_connection;
    options->max_concurrent_requests = defaults.max_concurrent_requests;
    options->worker_count = defaults.worker_count;
    options->worker_queue_capacity = defaults.worker_queue_capacity;
    options->graceful_shutdown_timeout_nanos = defaults.graceful_shutdown_timeout_nanos;
    options->initial_request_timeout_nanos = defaults.initial_request_timeout_nanos;
    options->max_stream_messages = defaults.max_stream_messages;
    options->max_stream_body_size = defaults.max_stream_body_size;
    options->stream_idle_timeout_nanos = defaults.stream_idle_timeout_nanos;
    return 0;
}

int trevrpc_call_options_v1_init(trevrpc_call_options_v1* options, size_t struct_size) {
    int err = trevrpc_versioned_init(options, struct_size, sizeof(*options));
    if (err != 0) {
        return err;
    }
    trevrpc_call_options_internal defaults = trevrpc_internal_default_call_options();
    options->metadata = defaults.metadata;
    options->timeout_nanos = defaults.timeout_nanos;
    options->cancellation = defaults.cancellation;
    options->max_response_body_size = defaults.max_response_body_size;
    options->max_response_messages = defaults.max_response_messages;
    options->max_response_stream_body_size = defaults.max_response_stream_body_size;
    options->response_idle_timeout_nanos = defaults.response_idle_timeout_nanos;
    options->request_body_lifetime = TREVRPC_REQUEST_BODY_COPY;
    return 0;
}

int trevrpc_response_view_v1_init(trevrpc_response_view_v1* response, size_t struct_size) {
    return trevrpc_versioned_init(response, struct_size, sizeof(*response));
}

int trevrpc_status_view_v1_init(trevrpc_status_view_v1* status, size_t struct_size) {
    return trevrpc_versioned_init(status, struct_size, sizeof(*status));
}

static int trevrpc_client_config_v1_normalize(
    const trevrpc_client_config_v1* config, trevrpc_client_config_internal* normalized) {
    int err = trevrpc_versioned_validate(config, sizeof(*config));
    if (err != 0) {
        return err;
    }
    *normalized = (trevrpc_client_config_internal){
        .cert_file = config->cert_file,
        .key_file = config->key_file,
        .ca_cert_file = config->ca_cert_file,
        .skip_certificate_validation = config->skip_certificate_validation,
        .max_idle_timeout_ms = config->max_idle_timeout_ms,
        .keep_alive_ms = config->keep_alive_ms,
        .peer_bidi_stream_count = config->peer_bidi_stream_count,
        .max_stateless_operations = config->max_stateless_operations,
        .max_binding_stateless_operations = config->max_binding_stateless_operations,
        .max_pending_send_bytes = config->max_pending_send_bytes,
        .max_pending_send_count = config->max_pending_send_count,
        .max_frame_size = config->max_frame_size,
        .stream_recv_window = config->stream_recv_window,
        .conn_flow_control_window = config->conn_flow_control_window,
        .msquic_execution_profile = config->msquic_execution_profile,
        .msquic_send_buffering_enabled = config->msquic_send_buffering_enabled,
    };
    return 0;
}

static int trevrpc_server_config_v1_normalize(
    const trevrpc_server_config_v1* config, trevrpc_server_config_internal* normalized) {
    int err = trevrpc_versioned_validate(config, sizeof(*config));
    if (err != 0) {
        return err;
    }
    *normalized = (trevrpc_server_config_internal){
        .host = config->host,
        .port = config->port,
        .cert_file = config->cert_file,
        .key_file = config->key_file,
        .webtransport_path = config->webtransport_path,
        .webtransport_origin = config->webtransport_origin,
        .webtransport_admission = config->webtransport_admission,
        .webtransport_admission_user_data = config->webtransport_admission_user_data,
        .enable_http3 = config->enable_http3,
        .http3_path = config->http3_path,
        .http3_admission = config->http3_admission,
        .http3_admission_user_data = config->http3_admission_user_data,
        .max_idle_timeout_ms = config->max_idle_timeout_ms,
        .keep_alive_ms = config->keep_alive_ms,
        .peer_bidi_stream_count = config->peer_bidi_stream_count,
        .max_stateless_operations = config->max_stateless_operations,
        .max_binding_stateless_operations = config->max_binding_stateless_operations,
        .max_pending_send_bytes = config->max_pending_send_bytes,
        .max_pending_send_count = config->max_pending_send_count,
        .max_sessions_per_connection = config->max_sessions_per_connection,
        .max_streams_per_session = config->max_streams_per_session,
        .stream_recv_window = config->stream_recv_window,
        .conn_flow_control_window = config->conn_flow_control_window,
        .max_frame_size = config->max_frame_size,
        .msquic_execution_profile = config->msquic_execution_profile,
        .msquic_send_buffering_enabled = config->msquic_send_buffering_enabled,
    };
    return 0;
}

static int trevrpc_server_options_v1_normalize(
    const trevrpc_server_options_v1* options, trevrpc_server_options_internal* normalized) {
    int err = trevrpc_versioned_validate(options, sizeof(*options));
    if (err != 0) {
        return err;
    }
    *normalized = (trevrpc_server_options_internal){
        .max_concurrent_connections = options->max_concurrent_connections,
        .max_concurrent_streams_per_connection = options->max_concurrent_streams_per_connection,
        .max_concurrent_requests = options->max_concurrent_requests,
        .worker_count = options->worker_count,
        .worker_queue_capacity = options->worker_queue_capacity,
        .graceful_shutdown_timeout_nanos = options->graceful_shutdown_timeout_nanos,
        .initial_request_timeout_nanos = options->initial_request_timeout_nanos,
        .max_stream_messages = options->max_stream_messages,
        .max_stream_body_size = options->max_stream_body_size,
        .stream_idle_timeout_nanos = options->stream_idle_timeout_nanos,
    };
    return 0;
}

static int trevrpc_call_options_v1_normalize(
    const trevrpc_call_options_v1* options, trevrpc_call_options_internal* normalized, bool* borrow) {
    if (options == NULL) {
        *normalized = trevrpc_internal_default_call_options();
        *borrow = false;
        return 0;
    }
    int err = trevrpc_versioned_validate(options, sizeof(*options));
    if (err != 0) {
        return err;
    }
    if (options->request_body_lifetime != TREVRPC_REQUEST_BODY_COPY &&
        options->request_body_lifetime != TREVRPC_REQUEST_BODY_BORROW_UNTIL_RETURN) {
        return -EINVAL;
    }
    *normalized = (trevrpc_call_options_internal){
        .metadata = options->metadata,
        .timeout_nanos = options->timeout_nanos,
        .cancellation = options->cancellation,
        .max_response_body_size = options->max_response_body_size,
        .max_response_messages = options->max_response_messages,
        .max_response_stream_body_size = options->max_response_stream_body_size,
        .response_idle_timeout_nanos = options->response_idle_timeout_nanos,
    };
    *borrow = options->request_body_lifetime == TREVRPC_REQUEST_BODY_BORROW_UNTIL_RETURN;
    return 0;
}

int trevrpc_channel_connect_v1(const char* host,
    uint16_t port,
    const trevrpc_client_config_v1* config,
    const trevrpc_channel_options* options,
    uint64_t timeout_nanos,
    trevrpc_cancellation* cancellation,
    trevrpc_channel** channel) {
    trevrpc_client_config_internal normalized = {0};
    int err = trevrpc_client_config_v1_normalize(config, &normalized);
    if (err != 0 || cancellation == NULL) {
        return err == 0
                   ? trevrpc_internal_channel_connect(host, port, &normalized, options, timeout_nanos, NULL, channel)
                   : err;
    }
    err = trevrpc_cancellation_retain(cancellation);
    if (err != 0) {
        return err;
    }
    trevrpc_versioned_test_connect_retained();
    int connect_err =
        trevrpc_internal_channel_connect(host, port, &normalized, options, timeout_nanos, cancellation, channel);
    trevrpc_cancellation_release(cancellation);
    return connect_err;
}

int trevrpc_raw_client_connect_v1(const char* host,
    uint16_t port,
    const trevrpc_client_config_v1* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client** client) {
    trevrpc_client_config_internal normalized = {0};
    int err = trevrpc_client_config_v1_normalize(config, &normalized);
    if (err != 0 || cancellation == NULL) {
        return err == 0 ? trevrpc_internal_raw_client_connect_cancellable(host, port, &normalized, NULL, client) : err;
    }
    err = trevrpc_cancellation_retain(cancellation);
    if (err != 0) {
        return err;
    }
    trevrpc_versioned_test_connect_retained();
    int connect_err = trevrpc_internal_raw_client_connect_cancellable(host, port, &normalized, cancellation, client);
    trevrpc_cancellation_release(cancellation);
    return connect_err;
}

int trevrpc_raw_client_connect_webtransport_v1(
    const trevrpc_wt_config* wt_config, const trevrpc_client_config_v1* config, trevrpc_raw_client** client) {
    trevrpc_client_config_internal normalized = {0};
    int err = trevrpc_client_config_v1_normalize(config, &normalized);
    return err == 0 ? trevrpc_internal_raw_client_connect_webtransport(wt_config, &normalized, client) : err;
}

int trevrpc_raw_client_connect_v1_with_shutdown_callback(const char* host,
    uint16_t port,
    const trevrpc_client_config_v1* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client_shutdown_callback callback,
    void* user_data,
    trevrpc_raw_client** client) {
    trevrpc_client_config_internal normalized = {0};
    int err = trevrpc_client_config_v1_normalize(config, &normalized);
    if (err != 0) {
        return err;
    }
    if (cancellation != NULL) {
        err = trevrpc_cancellation_retain(cancellation);
        if (err != 0) {
            return err;
        }
        trevrpc_versioned_test_connect_retained();
    }
    int connect_err = trevrpc_internal_raw_client_connect_cancellable_with_shutdown_callback(
        host, port, &normalized, cancellation, callback, user_data, client);
    trevrpc_cancellation_release(cancellation);
    return connect_err;
}

int trevrpc_server_listen_v1(const trevrpc_server_config_v1* config, trevrpc_server** server) {
    trevrpc_server_config_internal normalized = {0};
    int err = trevrpc_server_config_v1_normalize(config, &normalized);
    if (err == 0) {
        err = trevrpc_internal_server_listen(&normalized, server);
    }
    if (err == 0) {
        trevrpc_server_enable_strict_lifecycle(*server);
    }
    return err;
}

int trevrpc_server_set_options_v1(trevrpc_server* server, const trevrpc_server_options_v1* options) {
    trevrpc_server_options_internal normalized = {0};
    int err = trevrpc_server_options_v1_normalize(options, &normalized);
    return err == 0 ? trevrpc_internal_server_set_options(server, &normalized) : err;
}

int trevrpc_server_get_options_v1(trevrpc_server* server, trevrpc_server_options_v1* options) {
    int err = trevrpc_versioned_validate(options, sizeof(*options));
    if (err != 0) {
        return err;
    }
    trevrpc_server_options_internal normalized = {0};
    err = trevrpc_internal_server_get_options(server, &normalized);
    if (err != 0) {
        return err;
    }
    options->max_concurrent_connections = normalized.max_concurrent_connections;
    options->max_concurrent_streams_per_connection = normalized.max_concurrent_streams_per_connection;
    options->max_concurrent_requests = normalized.max_concurrent_requests;
    options->worker_count = normalized.worker_count;
    options->worker_queue_capacity = normalized.worker_queue_capacity;
    options->graceful_shutdown_timeout_nanos = normalized.graceful_shutdown_timeout_nanos;
    options->initial_request_timeout_nanos = normalized.initial_request_timeout_nanos;
    options->max_stream_messages = normalized.max_stream_messages;
    options->max_stream_body_size = normalized.max_stream_body_size;
    options->stream_idle_timeout_nanos = normalized.stream_idle_timeout_nanos;
    return 0;
}

int trevrpc_raw_client_call_request_inbound_v1(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_v1* options,
    trevrpc_inbound_response** response) {
    trevrpc_call_options_internal normalized = {0};
    bool borrow = false;
    int err = trevrpc_call_options_v1_normalize(options, &normalized, &borrow);
    return err == 0 ? trevrpc_raw_client_call_request_versioned(client, request, &normalized, borrow, response) : err;
}

int trevrpc_channel_call_request_inbound_v1(trevrpc_channel* channel,
    const trevrpc_request* request,
    const trevrpc_call_options_v1* options,
    trevrpc_inbound_response** response) {
    trevrpc_call_options_internal normalized = {0};
    bool borrow = false;
    int err = trevrpc_call_options_v1_normalize(options, &normalized, &borrow);
    return err == 0 ? trevrpc_channel_call_request_versioned(channel, request, &normalized, borrow, response) : err;
}

int trevrpc_raw_client_start_stream_request_v1(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_v1* options,
    trevrpc_stream** stream) {
    trevrpc_call_options_internal normalized = {0};
    bool borrow = false;
    int err = trevrpc_call_options_v1_normalize(options, &normalized, &borrow);
    return err == 0 ? trevrpc_raw_client_start_stream_request_versioned(client, request, &normalized, borrow, stream)
                    : err;
}

int trevrpc_channel_start_stream_request_v1(trevrpc_channel* channel,
    const trevrpc_request* request,
    const trevrpc_call_options_v1* options,
    trevrpc_stream** stream) {
    trevrpc_call_options_internal normalized = {0};
    bool borrow = false;
    int err = trevrpc_call_options_v1_normalize(options, &normalized, &borrow);
    return err == 0 ? trevrpc_channel_start_stream_request_versioned(channel, request, &normalized, borrow, stream)
                    : err;
}

int trevrpc_stream_recv_inbound(trevrpc_stream* stream, trevrpc_inbound_stream_frame** frame) {
    return trevrpc_stream_recv_inbound_versioned(stream, frame);
}

int trevrpc_stream_recv_inbound_ready(trevrpc_stream* stream, trevrpc_inbound_stream_frame** frame, int* ready) {
    return trevrpc_stream_recv_inbound_ready_versioned(stream, frame, ready);
}

int trevrpc_stream_recv_inbound_ready_since(
    trevrpc_stream* stream, trevrpc_inbound_stream_frame** frame, int* ready, uint64_t wait_started_nanos) {
    return trevrpc_stream_recv_inbound_ready_since_versioned(stream, frame, ready, wait_started_nanos);
}

int trevrpc_stream_recv_inbound_batch(
    trevrpc_stream* stream, trevrpc_inbound_stream_frame** frames, size_t capacity, size_t* count, int* eof) {
    return trevrpc_stream_recv_inbound_batch_versioned(stream, frames, capacity, count, eof);
}

int trevrpc_stream_send_status_borrowed_v1(trevrpc_stream* stream, const trevrpc_status_view_v1* status) {
    int err = trevrpc_versioned_validate(status, sizeof(*status));
    if (err != 0) {
        return err;
    }
    if ((status->message == NULL && status->message_len > 0) ||
        (status->metadata != NULL && trevrpc_metadata_validate(status->metadata) != 0)) {
        return -EINVAL;
    }
    return trevrpc_stream_send_status_with_metadata(
        stream, status->status, status->message, status->message_len, status->metadata);
}

int trevrpc_call_finish_stream_borrowed_v1(trevrpc_call* call, const trevrpc_status_view_v1* status) {
    int err = trevrpc_versioned_validate(status, sizeof(*status));
    if (err != 0) {
        return err;
    }
    if ((status->message == NULL && status->message_len > 0) ||
        (status->metadata != NULL && trevrpc_metadata_validate(status->metadata) != 0)) {
        return -EINVAL;
    }
    return trevrpc_call_finish_stream_with_metadata(
        call, status->status, status->message, status->message_len, status->metadata);
}
