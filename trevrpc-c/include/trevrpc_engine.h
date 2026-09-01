#ifndef TREVRPC_ENGINE_H
#define TREVRPC_ENGINE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_ENGINE_ABI_VERSION 1u
#define TREVRPC_ENGINE_STRUCT_VERSION_1 1u

#define TREVRPC_ENGINE_DEFAULT_EVENT_CAPACITY 256u
#define TREVRPC_ENGINE_DEFAULT_LISTENER_CAPACITY 16u
#define TREVRPC_ENGINE_DEFAULT_CONNECTION_CAPACITY 1024u
#define TREVRPC_ENGINE_DEFAULT_STREAM_CAPACITY 4096u
#define TREVRPC_ENGINE_DEFAULT_MAX_RECEIVE_OWNED_COUNT 4096u
#define TREVRPC_ENGINE_DEFAULT_RECEIVE_OWNED_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_ENGINE_DEFAULT_MAX_FRAME_SIZE (4ull * 1024ull * 1024ull)
#define TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_COUNT 1024u
#define TREVRPC_ENGINE_MAX_EVENT_CAPACITY 65536u

#define TREVRPC_ENGINE_STATE_RUNNING 0u
#define TREVRPC_ENGINE_STATE_STOPPING 1u
#define TREVRPC_ENGINE_STATE_STOPPED 2u
#define TREVRPC_ENGINE_STATE_RELEASING 3u

#define TREVRPC_ENGINE_WAKE_SOURCE_POSIX_FD 1u
#define TREVRPC_ENGINE_WAKE_FLAG_BORROWED 0x1u
#define TREVRPC_ENGINE_WAKE_FLAG_LEVEL_TRIGGERED 0x2u

#define TREVRPC_ENGINE_OBJECT_NONE 0u
#define TREVRPC_ENGINE_OBJECT_LISTENER 1u
#define TREVRPC_ENGINE_OBJECT_CONNECTION 2u
#define TREVRPC_ENGINE_OBJECT_STREAM 3u

#define TREVRPC_ENGINE_EVENT_DIAGNOSTIC 1u
#define TREVRPC_ENGINE_EVENT_STOPPED 2u
#define TREVRPC_ENGINE_EVENT_LISTENER_STOPPED 3u
#define TREVRPC_ENGINE_EVENT_CONNECTION_READY 4u
#define TREVRPC_ENGINE_EVENT_CONNECTION_FAILED 5u
#define TREVRPC_ENGINE_EVENT_CONNECTION_CLOSED 6u
#define TREVRPC_ENGINE_EVENT_STREAM_READY 7u
#define TREVRPC_ENGINE_EVENT_STREAM_FAILED 8u
#define TREVRPC_ENGINE_EVENT_STREAM_READABLE 9u
#define TREVRPC_ENGINE_EVENT_RECEIVE_FIN 10u
#define TREVRPC_ENGINE_EVENT_SEND_COMPLETE 11u
#define TREVRPC_ENGINE_EVENT_STREAM_CLOSED 12u

#define TREVRPC_ENGINE_EVENT_FLAG_FATAL 0x00000001u
#define TREVRPC_ENGINE_EVENT_FLAG_TERMINAL 0x00000002u
#define TREVRPC_ENGINE_EVENT_FLAG_CLIENT 0x00000004u
#define TREVRPC_ENGINE_EVENT_FLAG_SERVER 0x00000008u
#define TREVRPC_ENGINE_EVENT_FLAG_LOCAL 0x00000010u
#define TREVRPC_ENGINE_EVENT_FLAG_PEER 0x00000020u
#define TREVRPC_ENGINE_EVENT_FLAG_PEER_RESET 0x00000040u
#define TREVRPC_ENGINE_EVENT_FLAG_TRANSPORT_ERROR 0x00000080u
#define TREVRPC_ENGINE_EVENT_FLAG_CLEAN_FIN 0x00000100u

#define TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION 0x00000001u
#define TREVRPC_ENGINE_ENDPOINT_DISABLE_SEND_BUFFERING 0x00000002u

#define TREVRPC_ENGINE_RECEIVE_FLAG_NONE 0u

typedef struct trevrpc_engine trevrpc_engine;
typedef struct trevrpc_engine_event trevrpc_engine_event;
typedef struct trevrpc_engine_receive trevrpc_engine_receive;

typedef struct trevrpc_engine_handle_v1 {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_engine_handle_v1;

typedef struct trevrpc_engine_config_v1 {
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
} trevrpc_engine_config_v1;

typedef struct trevrpc_engine_wake_source_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    intptr_t native_handle;
    uint64_t reserved[3];
} trevrpc_engine_wake_source_v1;

typedef struct trevrpc_engine_endpoint_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;

    const char* host;
    uint32_t host_len;
    uint16_t port;
    uint16_t peer_bidi_stream_count;

    const uint8_t* alpn;
    uint32_t alpn_len;
    uint32_t flags;

    const char* cert_file;
    uint32_t cert_file_len;
    uint32_t reserved0;

    const char* key_file;
    uint32_t key_file_len;
    uint32_t reserved1;

    const char* ca_cert_file;
    uint32_t ca_cert_file_len;
    uint32_t max_pending_send_count;

    uint64_t max_pending_send_bytes;
    uint64_t max_frame_size;
    uint64_t max_idle_timeout_ms;

    uint32_t keep_alive_ms;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    uint32_t reserved2;

    uint64_t reserved[4];
} trevrpc_engine_endpoint_config_v1;

typedef struct trevrpc_engine_event_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;

    int32_t status;
    uint32_t subject_kind;
    uint64_t sequence;

    trevrpc_engine_handle_v1 subject;
    trevrpc_engine_handle_v1 parent;

    uint64_t operation_id;
    uint64_t application_error_code;
    uint64_t provider_error_code;

    const uint8_t* data;
    uint64_t data_len;

    uint64_t reserved[4];
} trevrpc_engine_event_info_v1;

typedef struct trevrpc_engine_receive_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t flags;
    uint32_t reserved0;
    const uint8_t* data;
    uint64_t data_len;
    uint64_t reserved[4];
} trevrpc_engine_receive_info_v1;

typedef struct trevrpc_engine_diagnostics_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t engine_abi_version;
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
} trevrpc_engine_diagnostics_v1;

uint32_t trevrpc_engine_abi_version(void);
void trevrpc_engine_abi_1_anchor(void);

int trevrpc_engine_config_v1_init(trevrpc_engine_config_v1* config, size_t struct_size);
int trevrpc_engine_wake_source_v1_init(trevrpc_engine_wake_source_v1* wake_source, size_t struct_size);
int trevrpc_engine_endpoint_config_v1_init(trevrpc_engine_endpoint_config_v1* config, size_t struct_size);
int trevrpc_engine_event_info_v1_init(trevrpc_engine_event_info_v1* info, size_t struct_size);
int trevrpc_engine_receive_info_v1_init(trevrpc_engine_receive_info_v1* info, size_t struct_size);
int trevrpc_engine_diagnostics_v1_init(trevrpc_engine_diagnostics_v1* diagnostics, size_t struct_size);

int trevrpc_engine_get_wake_source_v1(trevrpc_engine* engine, trevrpc_engine_wake_source_v1* wake_source);
int trevrpc_engine_next_event(trevrpc_engine* engine, trevrpc_engine_event** out_event);
int trevrpc_engine_event_get_info_v1(const trevrpc_engine_event* event, trevrpc_engine_event_info_v1* info);
void trevrpc_engine_event_release(trevrpc_engine_event* event);
int trevrpc_engine_receive_get_info_v1(const trevrpc_engine_receive* receive, trevrpc_engine_receive_info_v1* info);
void trevrpc_engine_receive_release(trevrpc_engine_receive* receive);
int trevrpc_engine_get_diagnostics_v1(trevrpc_engine* engine, trevrpc_engine_diagnostics_v1* diagnostics);

int trevrpc_engine_listen_v1(
    trevrpc_engine* engine, const trevrpc_engine_endpoint_config_v1* config, trevrpc_engine_handle_v1* out_listener);
int trevrpc_engine_listener_get_port_v1(trevrpc_engine* engine, trevrpc_engine_handle_v1 listener, uint16_t* out_port);
int trevrpc_engine_dial_v1(trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_engine_handle_v1* out_connection);
int trevrpc_engine_dial_cancel(trevrpc_engine* engine, trevrpc_engine_handle_v1 connection);
int trevrpc_engine_connection_open_bidi_stream_v1(trevrpc_engine* engine,
    trevrpc_engine_handle_v1 connection,
    uint64_t operation_id,
    trevrpc_engine_handle_v1* out_stream);
int trevrpc_engine_stream_send_frame_v1(trevrpc_engine* engine,
    trevrpc_engine_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len);
int trevrpc_engine_stream_receive_frame(
    trevrpc_engine* engine, trevrpc_engine_handle_v1 stream, trevrpc_engine_receive** out_receive);
int trevrpc_engine_stream_finish_send(trevrpc_engine* engine, trevrpc_engine_handle_v1 stream);
int trevrpc_engine_stream_abort(
    trevrpc_engine* engine, trevrpc_engine_handle_v1 stream, uint64_t application_error_code);
int trevrpc_engine_stream_close(trevrpc_engine* engine, trevrpc_engine_handle_v1 stream);
int trevrpc_engine_connection_close(
    trevrpc_engine* engine, trevrpc_engine_handle_v1 connection, uint64_t application_error_code);
int trevrpc_engine_listener_close(trevrpc_engine* engine, trevrpc_engine_handle_v1 listener);
int trevrpc_engine_close(trevrpc_engine* engine);
int trevrpc_engine_drain(trevrpc_engine* engine);
/* The caller must prevent new raw-pointer API entry attempts before final release begins. */
int trevrpc_engine_release(trevrpc_engine* engine);

#ifdef __cplusplus
}
#endif

#endif
