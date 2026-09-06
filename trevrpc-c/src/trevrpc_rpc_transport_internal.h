#ifndef TREVRPC_RPC_TRANSPORT_INTERNAL_H
#define TREVRPC_RPC_TRANSPORT_INTERNAL_H

#include "trevrpc_transport.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>

typedef struct trevrpc_msquic_accepted_connection trevrpc_msquic_accepted_connection;

/* Private RPC transport vocabulary.  This header is never installed. */
#define TREVRPC_RPC_TRANSPORT_STATE_RUNNING 0u
#define TREVRPC_RPC_TRANSPORT_STATE_STOPPING 1u
#define TREVRPC_RPC_TRANSPORT_STATE_STOPPED 2u

#define TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD 1u
#define TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED 0x1u
#define TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED 0x2u
#define TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES 8u

#define TREVRPC_RPC_TRANSPORT_OBJECT_NONE 0u
#define TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER 1u
#define TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION 2u
#define TREVRPC_RPC_TRANSPORT_OBJECT_STREAM 3u

#define TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC 1u
#define TREVRPC_RPC_TRANSPORT_EVENT_STOPPED 2u
#define TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED 3u
#define TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY 4u
#define TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED 5u
#define TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED 6u
#define TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY 7u
#define TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED 8u
#define TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE 9u
#define TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN 10u
#define TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE 11u
#define TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED 12u
#define TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION 13u
#define TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION 14u
#define TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED 15u
#define TREVRPC_RPC_TRANSPORT_EVENT_STREAM_ACCEPTED 16u

#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_FATAL 0x00000001u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL 0x00000002u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT 0x00000004u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER 0x00000008u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL 0x00000010u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER 0x00000020u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET 0x00000040u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR 0x00000080u
#define TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN 0x00000100u
#define TREVRPC_RPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION 0x00000001u
#define TREVRPC_RPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION 0x00000002u

#define TREVRPC_RPC_TRANSPORT_PROTOCOL_AUTO 0u
#define TREVRPC_RPC_TRANSPORT_PROTOCOL_NATIVE 1u
#define TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3 2u
#define TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT 3u
#define TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED 4u

typedef struct trevrpc_rpc_transport trevrpc_rpc_transport;
typedef struct trevrpc_rpc_transport_event trevrpc_rpc_transport_event;
typedef struct trevrpc_rpc_transport_receive trevrpc_rpc_transport_receive;

typedef struct trevrpc_rpc_transport_handle {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_rpc_transport_handle;

typedef struct trevrpc_rpc_transport_config {
    uint32_t event_capacity;
    uint32_t listener_capacity;
    uint32_t connection_capacity;
    uint32_t stream_capacity;
    uint32_t max_receive_owned_count;
    uint64_t max_receive_owned_bytes;
} trevrpc_rpc_transport_config;

typedef struct trevrpc_rpc_transport_endpoint_config {
    uint32_t protocol;
    uint32_t flags;
    const char* host;
    uint32_t host_len;
    uint16_t port;
    uint16_t peer_bidi_stream_count;
    const char* server_name;
    uint32_t server_name_len;
    const uint8_t* alpn;
    uint32_t alpn_len;
    const char* cert_file;
    uint32_t cert_file_len;
    const char* key_file;
    uint32_t key_file_len;
    const char* ca_cert_file;
    uint32_t ca_cert_file_len;
    const uint8_t* cert_data;
    uint64_t cert_data_len;
    const uint8_t* key_data;
    uint64_t key_data_len;
    const uint8_t* ca_cert_data;
    uint64_t ca_cert_data_len;
    const char* path;
    uint32_t path_len;
    const char* origin;
    uint32_t origin_len;
    uint32_t webtransport_profiles;
    uint32_t max_sessions;
    uint32_t max_pending_send_count;
    uint32_t max_pending_receive_count;
    uint64_t max_pending_send_bytes;
    uint64_t max_pending_receive_bytes;
    uint64_t max_frame_size;
    uint64_t max_field_section_size;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    uint32_t unresolved_stream_count;
    uint64_t unresolved_stream_bytes;
    uint64_t unresolved_stream_timeout_ms;
} trevrpc_rpc_transport_endpoint_config;

typedef struct trevrpc_rpc_transport_wake {
    uint32_t kind;
    uint32_t flags;
    intptr_t native_handle;
} trevrpc_rpc_transport_wake;

typedef struct trevrpc_rpc_transport_event_info {
    uint32_t kind;
    uint32_t flags;
    int32_t status;
    uint32_t subject_kind;
    uint64_t sequence;
    trevrpc_rpc_transport_handle subject;
    trevrpc_rpc_transport_handle parent;
    uint64_t operation_id;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    const uint8_t* data;
    uint64_t data_len;
} trevrpc_rpc_transport_event_info;

typedef struct trevrpc_rpc_transport_receive_info {
    uint32_t flags;
    const uint8_t* data;
    uint64_t data_len;
} trevrpc_rpc_transport_receive_info;

typedef trevrpc_transport_header_field_v1 trevrpc_rpc_transport_header_field;

typedef struct trevrpc_rpc_transport_admission_info {
    uint32_t protocol;
    trevrpc_rpc_transport_handle listener;
    const trevrpc_rpc_transport_header_field* headers;
    uint64_t header_count;
    const uint8_t* method;
    uint64_t method_len;
    const uint8_t* path;
    uint64_t path_len;
    const uint8_t* authority;
    uint64_t authority_len;
    const uint8_t* origin;
    uint64_t origin_len;
} trevrpc_rpc_transport_admission_info;

typedef struct trevrpc_rpc_transport_event_protocol_info {
    uint32_t protocol;
} trevrpc_rpc_transport_event_protocol_info;

typedef struct trevrpc_rpc_transport_diagnostics {
    uint32_t state;
    int32_t terminal_status;
    uint32_t event_capacity;
    uint32_t queue_depth;
    uint32_t ordinary_queue_depth;
    uint64_t events_enqueued;
    uint64_t events_dequeued;
    uint64_t events_rejected;
    uint64_t receive_owned_count;
    uint64_t peak_receive_owned_count;
    uint64_t receive_owned_bytes;
    uint64_t peak_receive_owned_bytes;
    uint64_t pending_send_bytes;
    uint64_t pending_send_count;
    uint64_t live_listeners;
    uint64_t live_connections;
    uint64_t live_streams;
    uint64_t active_callbacks;
    uint64_t active_api_calls;
    uint64_t wake_signals;
    uint64_t wake_write_eagain;
    uint64_t wake_failures;
    uint64_t provider_error_code;
    uint64_t mandatory_reservations;
} trevrpc_rpc_transport_diagnostics;

typedef struct trevrpc_rpc_transport_ops {
    int (*get_wake_source)(trevrpc_rpc_transport*, trevrpc_rpc_transport_wake*);
    int (*next_event)(trevrpc_rpc_transport*, trevrpc_rpc_transport_event**);
    int (*event_get_info)(const trevrpc_rpc_transport_event*, trevrpc_rpc_transport_event_info*);
    void (*event_release)(trevrpc_rpc_transport_event*);
    int (*receive_get_info)(const trevrpc_rpc_transport_receive*, trevrpc_rpc_transport_receive_info*);
    void (*receive_release)(trevrpc_rpc_transport_receive*);
    int (*get_diagnostics)(trevrpc_rpc_transport*, trevrpc_rpc_transport_diagnostics*);
    int (*poll_timeout_ms)(trevrpc_rpc_transport*);
    int (*endpoint_listen)(
        trevrpc_rpc_transport*, const trevrpc_rpc_transport_endpoint_config*, trevrpc_rpc_transport_handle*);
    int (*endpoint_get_port)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint16_t*);
    int (*endpoint_dial)(
        trevrpc_rpc_transport*, const trevrpc_rpc_transport_endpoint_config*, uint64_t, trevrpc_rpc_transport_handle*);
    int (*dial_cancel)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle);
    int (*stream_open)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint64_t, trevrpc_rpc_transport_handle*);
    int (*stream_send)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint64_t, const uint8_t*, size_t);
    int (*stream_receive)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, trevrpc_rpc_transport_receive**);
    int (*stream_finish_send)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle);
    int (*stream_abort_receive)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint64_t);
    int (*stream_abort_send)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint64_t);
    int (*stream_abort)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint64_t);
    int (*stream_close)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle);
    int (*connection_close)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint64_t);
    int (*listener_close)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle);
    int (*close)(trevrpc_rpc_transport*);
    int (*drain)(trevrpc_rpc_transport*);
    void (*destroy)(trevrpc_rpc_transport*);
    int (*get_wake_sources)(trevrpc_rpc_transport*, trevrpc_rpc_transport_wake*, size_t, size_t*);
    int (*release_handle)(trevrpc_rpc_transport*, trevrpc_rpc_transport_handle, uint32_t);
    int (*event_get_admission_info)(const trevrpc_rpc_transport_event*, trevrpc_rpc_transport_admission_info*);
    int (*event_get_protocol_info)(const trevrpc_rpc_transport_event*, trevrpc_rpc_transport_event_protocol_info*);
    int (*admission_respond)(const trevrpc_rpc_transport_event*, uint16_t);
    int (*adopt_accepted_connection)(trevrpc_rpc_transport*,
        const trevrpc_rpc_transport_endpoint_config*,
        trevrpc_msquic_accepted_connection*,
        trevrpc_rpc_transport_handle*);
} trevrpc_rpc_transport_ops;

struct trevrpc_rpc_transport {
    const trevrpc_rpc_transport_ops* ops;
};

static inline int trevrpc_rpc_transport_get_wake_source(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wake) {
    return transport->ops->get_wake_source(transport, wake);
}
static inline int trevrpc_rpc_transport_get_wake_sources(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wakes, size_t capacity, size_t* out_count) {
    int result;
    if (transport == NULL || wakes == NULL || capacity == 0 || out_count == NULL) {
        return -EINVAL;
    }
    if (transport->ops->get_wake_sources != NULL) {
        return transport->ops->get_wake_sources(transport, wakes, capacity, out_count);
    }
    result = transport->ops->get_wake_source(transport, &wakes[0]);
    if (result == 0) {
        *out_count = 1;
    }
    return result;
}
static inline int trevrpc_rpc_transport_next_event(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** event) {
    return transport->ops->next_event(transport, event);
}
static inline int trevrpc_rpc_transport_event_get_info(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_event* event,
    trevrpc_rpc_transport_event_info* info) {
    return transport->ops->event_get_info(event, info);
}
static inline int trevrpc_rpc_transport_event_get_admission_info(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_event* event,
    trevrpc_rpc_transport_admission_info* info) {
    return transport->ops->event_get_admission_info != NULL ? transport->ops->event_get_admission_info(event, info)
                                                            : -ENOTSUP;
}
static inline int trevrpc_rpc_transport_event_get_protocol_info(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_event* event,
    trevrpc_rpc_transport_event_protocol_info* info) {
    return transport->ops->event_get_protocol_info != NULL ? transport->ops->event_get_protocol_info(event, info)
                                                           : -ENOTSUP;
}
static inline int trevrpc_rpc_transport_admission_respond(
    trevrpc_rpc_transport* transport, const trevrpc_rpc_transport_event* event, uint16_t status) {
    return transport->ops->admission_respond != NULL ? transport->ops->admission_respond(event, status) : -ENOTSUP;
}
static inline void trevrpc_rpc_transport_event_release(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event* event) {
    transport->ops->event_release(event);
}
static inline int trevrpc_rpc_transport_receive_get_info(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_receive* receive,
    trevrpc_rpc_transport_receive_info* info) {
    return transport->ops->receive_get_info(receive, info);
}
static inline void trevrpc_rpc_transport_receive_release(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_receive* receive) {
    transport->ops->receive_release(receive);
}
static inline int trevrpc_rpc_transport_get_diagnostics(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_diagnostics* diagnostics) {
    return transport->ops->get_diagnostics(transport, diagnostics);
}
static inline int trevrpc_rpc_transport_poll_timeout_ms(trevrpc_rpc_transport* transport) {
    return transport->ops->poll_timeout_ms != NULL ? transport->ops->poll_timeout_ms(transport) : -1;
}
static inline int trevrpc_rpc_transport_endpoint_listen(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_rpc_transport_handle* listener) {
    return transport->ops->endpoint_listen(transport, config, listener);
}
static inline int trevrpc_rpc_transport_endpoint_get_port(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener, uint16_t* port) {
    return transport->ops->endpoint_get_port(transport, listener, port);
}
static inline int trevrpc_rpc_transport_endpoint_dial(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* connection) {
    return transport->ops->endpoint_dial(transport, config, operation_id, connection);
}
static inline int trevrpc_rpc_transport_dial_cancel(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection) {
    return transport->ops->dial_cancel(transport, connection);
}
static inline int trevrpc_rpc_transport_stream_open(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* stream) {
    return transport->ops->stream_open(transport, connection, operation_id, stream);
}
static inline int trevrpc_rpc_transport_stream_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len) {
    return transport->ops->stream_send(transport, stream, operation_id, body, body_len);
}
static inline int trevrpc_rpc_transport_stream_receive(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, trevrpc_rpc_transport_receive** receive) {
    return transport->ops->stream_receive(transport, stream, receive);
}
static inline int trevrpc_rpc_transport_stream_finish_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    return transport->ops->stream_finish_send(transport, stream);
}
static inline int trevrpc_rpc_transport_stream_abort_receive(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    return transport->ops->stream_abort_receive != NULL
               ? transport->ops->stream_abort_receive(transport, stream, error_code)
               : -ENOTSUP;
}
static inline int trevrpc_rpc_transport_stream_abort_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    return transport->ops->stream_abort_send != NULL ? transport->ops->stream_abort_send(transport, stream, error_code)
                                                     : -ENOTSUP;
}
static inline int trevrpc_rpc_transport_stream_abort(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    return transport->ops->stream_abort(transport, stream, error_code);
}
static inline int trevrpc_rpc_transport_stream_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    return transport->ops->stream_close(transport, stream);
}
static inline int trevrpc_rpc_transport_connection_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t error_code) {
    return transport->ops->connection_close(transport, connection, error_code);
}
static inline int trevrpc_rpc_transport_listener_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener) {
    return transport->ops->listener_close(transport, listener);
}
static inline int trevrpc_rpc_transport_close(trevrpc_rpc_transport* transport) {
    return transport->ops->close(transport);
}
static inline int trevrpc_rpc_transport_drain(trevrpc_rpc_transport* transport) {
    return transport->ops->drain(transport);
}
static inline int trevrpc_rpc_transport_release_handle(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    return transport->ops->release_handle != NULL ? transport->ops->release_handle(transport, handle, kind) : 0;
}
static inline void trevrpc_rpc_transport_destroy(trevrpc_rpc_transport* transport) {
    transport->ops->destroy(transport);
}

#endif
