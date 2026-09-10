#ifndef TREVRPC_TRANSPORT_H
#define TREVRPC_TRANSPORT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_TRANSPORT_ABI_VERSION 1u
#define TREVRPC_TRANSPORT_STRUCT_VERSION_1 1u

#define TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY 256u
#define TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY 16u
#define TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY 1024u
#define TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY 4096u
#define TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT 4096u
#define TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE (4ull * 1024ull * 1024ull)
#define TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE (64ull * 1024ull)
#define TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_COUNT 1024u
#define TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_COUNT 4096u
#define TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_COUNT 64u
#define TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_TIMEOUT_MS 5000u
#define TREVRPC_TRANSPORT_MAX_WAKE_SOURCES 8u

#define TREVRPC_TRANSPORT_STATE_RUNNING 0u
#define TREVRPC_TRANSPORT_STATE_STOPPING 1u
#define TREVRPC_TRANSPORT_STATE_STOPPED 2u

#define TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD 1u
#define TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED 0x1u
#define TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED 0x2u

#define TREVRPC_TRANSPORT_OBJECT_NONE 0u
#define TREVRPC_TRANSPORT_OBJECT_LISTENER 1u
#define TREVRPC_TRANSPORT_OBJECT_CONNECTION 2u
#define TREVRPC_TRANSPORT_OBJECT_STREAM 3u

#define TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC 1u
#define TREVRPC_TRANSPORT_EVENT_STOPPED 2u
#define TREVRPC_TRANSPORT_EVENT_LISTENER_STOPPED 3u
#define TREVRPC_TRANSPORT_EVENT_CONNECTION_READY 4u
#define TREVRPC_TRANSPORT_EVENT_CONNECTION_FAILED 5u
#define TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSED 6u
#define TREVRPC_TRANSPORT_EVENT_STREAM_READY 7u
#define TREVRPC_TRANSPORT_EVENT_STREAM_FAILED 8u
#define TREVRPC_TRANSPORT_EVENT_STREAM_READABLE 9u
#define TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN 10u
#define TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE 11u
#define TREVRPC_TRANSPORT_EVENT_STREAM_CLOSED 12u
#define TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION 13u
#define TREVRPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION 14u
#define TREVRPC_TRANSPORT_EVENT_SEND_STOPPED 15u

#define TREVRPC_TRANSPORT_EVENT_FLAG_FATAL 0x00000001u
#define TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL 0x00000002u
#define TREVRPC_TRANSPORT_EVENT_FLAG_CLIENT 0x00000004u
#define TREVRPC_TRANSPORT_EVENT_FLAG_SERVER 0x00000008u
#define TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL 0x00000010u
#define TREVRPC_TRANSPORT_EVENT_FLAG_PEER 0x00000020u
#define TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET 0x00000040u
#define TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR 0x00000080u
#define TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN 0x00000100u

#define TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION 0x00000001u
#define TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION 0x00000002u

#define TREVRPC_TRANSPORT_PROTOCOL_AUTO 0u
#define TREVRPC_TRANSPORT_PROTOCOL_NATIVE 1u
#define TREVRPC_TRANSPORT_PROTOCOL_HTTP3 2u
#define TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT 3u
#define TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED 4u

#define TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_02 0x00000001u
#define TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_07 0x00000002u
#define TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_14 0x00000004u
#define TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_15 0x00000008u
#define TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED 0x0000000fu

#define TREVRPC_TRANSPORT_RECEIVE_FLAG_NONE 0u

typedef struct trevrpc_transport trevrpc_transport;
typedef struct trevrpc_transport_event_v1 trevrpc_transport_event_v1;
typedef struct trevrpc_transport_receive_v1 trevrpc_transport_receive_v1;

typedef struct trevrpc_transport_handle_v1 {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_transport_handle_v1;

typedef struct trevrpc_transport_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t event_capacity;
    uint32_t listener_capacity;
    uint32_t connection_capacity;
    uint32_t stream_capacity;
    uint32_t max_receive_owned_count;
    uint32_t flags;
    uint64_t max_receive_owned_bytes;
    uint64_t reserved[5];
} trevrpc_transport_config_v1;

typedef struct trevrpc_transport_endpoint_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t protocol;
    uint32_t flags;

    const char* host;
    uint32_t host_len;
    uint16_t port;
    uint16_t peer_bidi_stream_count;

    /* Native/AUTO dialing currently uses host as SNI and accepts only an
     * identical server_name; native/AUTO listeners reject server_name. */
    const char* server_name;
    uint32_t server_name_len;
    uint32_t reserved0;

    const uint8_t* alpn;
    uint32_t alpn_len;
    uint32_t reserved1;

    const char* cert_file;
    uint32_t cert_file_len;
    uint32_t reserved2;
    const char* key_file;
    uint32_t key_file_len;
    uint32_t reserved3;
    const char* ca_cert_file;
    uint32_t ca_cert_file_len;
    uint32_t reserved4;

    /* Memory credential blobs are copied before endpoint creation returns.  For
     * the MsQuic provider they are PEM-encoded certificate/key/CA bytes.
     * Each blob is mutually exclusive with its corresponding file field. CA
     * credentials on listeners are currently unsupported. */
    const uint8_t* cert_data;
    uint64_t cert_data_len;
    const uint8_t* key_data;
    uint64_t key_data_len;
    const uint8_t* ca_cert_data;
    uint64_t ca_cert_data_len;

    const char* path;
    uint32_t path_len;
    uint32_t webtransport_profiles;
    const char* origin;
    uint32_t origin_len;
    uint32_t max_sessions;

    uint64_t max_frame_size;
    uint64_t max_field_section_size;
    uint64_t max_pending_send_bytes;
    uint64_t max_pending_receive_bytes;
    uint64_t max_idle_timeout_ms;

    uint32_t max_pending_send_count;
    uint32_t max_pending_receive_count;
    uint32_t keep_alive_ms;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    uint32_t unresolved_stream_count;
    uint64_t unresolved_stream_bytes;
    uint64_t unresolved_stream_timeout_ms;
    uint64_t reserved[5];
} trevrpc_transport_endpoint_config_v1;

typedef struct trevrpc_transport_wake_source_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    intptr_t native_handle;
    uint64_t reserved[3];
} trevrpc_transport_wake_source_v1;

typedef struct trevrpc_transport_event_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    int32_t status;
    uint32_t subject_kind;
    uint64_t sequence;
    trevrpc_transport_handle_v1 subject;
    trevrpc_transport_handle_v1 parent;
    uint64_t operation_id;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    const uint8_t* data;
    uint64_t data_len;
    uint64_t reserved[4];
} trevrpc_transport_event_info_v1;

typedef struct trevrpc_transport_receive_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t flags;
    uint32_t reserved0;
    const uint8_t* data;
    uint64_t data_len;
    uint64_t reserved[4];
} trevrpc_transport_receive_info_v1;

typedef struct trevrpc_transport_header_field_v1 {
    const uint8_t* name;
    uint64_t name_len;
    const uint8_t* value;
    uint64_t value_len;
} trevrpc_transport_header_field_v1;

/* All pointers returned in admission info remain valid until the originating
 * event is released. Header order and duplicates are preserved. */
typedef struct trevrpc_transport_admission_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t protocol;
    uint32_t reserved0;
    trevrpc_transport_handle_v1 listener;
    const trevrpc_transport_header_field_v1* headers;
    uint64_t header_count;
    const uint8_t* method;
    uint64_t method_len;
    const uint8_t* path;
    uint64_t path_len;
    const uint8_t* authority;
    uint64_t authority_len;
    const uint8_t* origin;
    uint64_t origin_len;
    uint64_t reserved[4];
} trevrpc_transport_admission_info_v1;

typedef struct trevrpc_transport_event_protocol_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t protocol;
    uint32_t reserved0;
    uint64_t reserved[4];
} trevrpc_transport_event_protocol_info_v1;

typedef struct trevrpc_transport_diagnostics_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t transport_abi_version;
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
    uint64_t reserved[3];
} trevrpc_transport_diagnostics_v1;

uint32_t trevrpc_transport_abi_version(void);
void trevrpc_transport_abi_1_anchor(void);

int trevrpc_transport_config_v1_init(trevrpc_transport_config_v1* config, size_t struct_size);
int trevrpc_transport_endpoint_config_v1_init(trevrpc_transport_endpoint_config_v1* config, size_t struct_size);
int trevrpc_transport_wake_source_v1_init(trevrpc_transport_wake_source_v1* wake_source, size_t struct_size);
int trevrpc_transport_event_info_v1_init(trevrpc_transport_event_info_v1* info, size_t struct_size);
int trevrpc_transport_receive_info_v1_init(trevrpc_transport_receive_info_v1* info, size_t struct_size);
int trevrpc_transport_admission_info_v1_init(trevrpc_transport_admission_info_v1* info, size_t struct_size);
int trevrpc_transport_event_protocol_info_v1_init(trevrpc_transport_event_protocol_info_v1* info, size_t struct_size);
int trevrpc_transport_diagnostics_v1_init(trevrpc_transport_diagnostics_v1* diagnostics, size_t struct_size);

int trevrpc_transport_get_wake_sources_v1(
    trevrpc_transport* transport, trevrpc_transport_wake_source_v1* wake_sources, size_t capacity, size_t* out_count);
int trevrpc_transport_poll_timeout_ms(trevrpc_transport* transport);
int trevrpc_transport_next_event(trevrpc_transport* transport, trevrpc_transport_event_v1** out_event);
int trevrpc_transport_event_get_info_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, trevrpc_transport_event_info_v1* info);
int trevrpc_transport_event_get_admission_info_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, trevrpc_transport_admission_info_v1* info);
int trevrpc_transport_event_get_protocol_info_v1(trevrpc_transport* transport,
    const trevrpc_transport_event_v1* event,
    trevrpc_transport_event_protocol_info_v1* info);
/* Admission events are one-shot response capabilities. HTTP status 200 accepts;
 * 400 through 599 reject. Releasing an undecided event fails closed with 500.
 * A valid response attempt consumes the capability even when the function returns
 * a nonzero status reporting response-delivery or transport failure; do not retry.
 *
 * The caller must serialize every operation on a returned event. In particular,
 * admission_respond and event_release must not execute concurrently for the same
 * event, and no accessor or response call may begin after event_release begins. */
int trevrpc_transport_admission_respond_v1(
    trevrpc_transport* transport, const trevrpc_transport_event_v1* event, uint16_t http_status);
void trevrpc_transport_event_release(trevrpc_transport* transport, trevrpc_transport_event_v1* event);
int trevrpc_transport_receive_get_info_v1(
    trevrpc_transport* transport, const trevrpc_transport_receive_v1* receive, trevrpc_transport_receive_info_v1* info);
void trevrpc_transport_receive_release(trevrpc_transport* transport, trevrpc_transport_receive_v1* receive);
int trevrpc_transport_get_diagnostics_v1(trevrpc_transport* transport, trevrpc_transport_diagnostics_v1* diagnostics);

int trevrpc_transport_listen_v1(trevrpc_transport* transport,
    const trevrpc_transport_endpoint_config_v1* config,
    trevrpc_transport_handle_v1* out_listener);
int trevrpc_transport_listener_get_port_v1(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 listener, uint16_t* out_port);
int trevrpc_transport_dial_v1(trevrpc_transport* transport,
    const trevrpc_transport_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* out_connection);
int trevrpc_transport_dial_cancel(trevrpc_transport* transport, trevrpc_transport_handle_v1 connection);
int trevrpc_transport_connection_open_bidi_stream_v1(trevrpc_transport* transport,
    trevrpc_transport_handle_v1 connection,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* out_stream);
/*
 * Stream sends and receives transfer one complete TrevRPC frame body per
 * operation. The transport owns the four-byte TrevRPC length prefix and any
 * protocol framing; callers must not include either prefix or split a body
 * across multiple sends.
 */
int trevrpc_transport_stream_send_v1(trevrpc_transport* transport,
    trevrpc_transport_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* data,
    size_t data_len);
int trevrpc_transport_stream_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, trevrpc_transport_receive_v1** out_receive);
int trevrpc_transport_stream_finish_send(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream);
int trevrpc_transport_stream_abort_receive(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code);
int trevrpc_transport_stream_abort_send(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code);
int trevrpc_transport_stream_abort(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 stream, uint64_t application_error_code);
int trevrpc_transport_stream_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 stream);
int trevrpc_transport_connection_close(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 connection, uint64_t application_error_code);
int trevrpc_transport_listener_close(trevrpc_transport* transport, trevrpc_transport_handle_v1 listener);
int trevrpc_transport_release_handle(
    trevrpc_transport* transport, trevrpc_transport_handle_v1 handle, uint32_t object_kind);
int trevrpc_transport_close(trevrpc_transport* transport);
int trevrpc_transport_drain(trevrpc_transport* transport);
/*
 * The caller must reach STOPPED, drain the event queue, release every returned
 * event and receive, and release every semantic handle before calling release.
 * These ownership preconditions are not all dynamically validated. A nonzero
 * result leaves the transport owned by the caller; zero destroys it.
 */
int trevrpc_transport_release(trevrpc_transport* transport);

#ifdef __cplusplus
}
#endif

#endif
