#ifndef TREVRPC_TRANSPORT_PROVIDER_H
#define TREVRPC_TRANSPORT_PROVIDER_H

#include "trevrpc_transport.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_TRANSPORT_PROVIDER_ABI_VERSION 1u
#define TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1 1u

typedef struct trevrpc_transport_provider_endpoint_config_v1 {
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
} trevrpc_transport_provider_endpoint_config_v1;

typedef struct trevrpc_transport_provider_wake_v1 {
    uint32_t kind;
    uint32_t flags;
    intptr_t native_handle;
} trevrpc_transport_provider_wake_v1;

typedef struct trevrpc_transport_provider_event_info_v1 {
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
} trevrpc_transport_provider_event_info_v1;

typedef struct trevrpc_transport_provider_receive_info_v1 {
    uint32_t flags;
    const uint8_t* data;
    uint64_t data_len;
} trevrpc_transport_provider_receive_info_v1;

typedef struct trevrpc_transport_provider_admission_info_v1 {
    uint32_t protocol;
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
} trevrpc_transport_provider_admission_info_v1;

typedef struct trevrpc_transport_provider_event_protocol_info_v1 {
    uint32_t protocol;
} trevrpc_transport_provider_event_protocol_info_v1;

typedef struct trevrpc_transport_provider_diagnostics_v1 {
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
} trevrpc_transport_provider_diagnostics_v1;

typedef struct trevrpc_transport_provider_ops_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    int (*get_wake_sources)(void*, trevrpc_transport_provider_wake_v1*, size_t, size_t*);
    int (*poll_timeout_ms)(void*);
    int (*next_event)(void*, trevrpc_transport_event_v1**);
    int (*event_get_info)(void*, const trevrpc_transport_event_v1*, trevrpc_transport_provider_event_info_v1*);
    int (*event_get_admission_info)(
        void*, const trevrpc_transport_event_v1*, trevrpc_transport_provider_admission_info_v1*);
    int (*event_get_protocol_info)(
        void*, const trevrpc_transport_event_v1*, trevrpc_transport_provider_event_protocol_info_v1*);
    int (*admission_respond)(void*, const trevrpc_transport_event_v1*, uint16_t);
    void (*event_release)(void*, trevrpc_transport_event_v1*);
    int (*receive_get_info)(void*, const trevrpc_transport_receive_v1*, trevrpc_transport_provider_receive_info_v1*);
    void (*receive_release)(void*, trevrpc_transport_receive_v1*);
    int (*get_diagnostics)(void*, trevrpc_transport_provider_diagnostics_v1*);
    int (*listen)(void*, const trevrpc_transport_provider_endpoint_config_v1*, trevrpc_transport_handle_v1*);
    int (*listener_get_port)(void*, trevrpc_transport_handle_v1, uint16_t*);
    int (*dial)(void*, const trevrpc_transport_provider_endpoint_config_v1*, uint64_t, trevrpc_transport_handle_v1*);
    int (*dial_cancel)(void*, trevrpc_transport_handle_v1);
    int (*connection_open_bidi_stream)(void*, trevrpc_transport_handle_v1, uint64_t, trevrpc_transport_handle_v1*);
    int (*stream_send)(void*, trevrpc_transport_handle_v1, uint64_t, const uint8_t*, size_t);
    int (*stream_receive)(void*, trevrpc_transport_handle_v1, trevrpc_transport_receive_v1**);
    int (*stream_finish_send)(void*, trevrpc_transport_handle_v1);
    int (*stream_abort_receive)(void*, trevrpc_transport_handle_v1, uint64_t);
    int (*stream_abort_send)(void*, trevrpc_transport_handle_v1, uint64_t);
    int (*stream_abort)(void*, trevrpc_transport_handle_v1, uint64_t);
    int (*stream_close)(void*, trevrpc_transport_handle_v1);
    int (*connection_close)(void*, trevrpc_transport_handle_v1, uint64_t);
    int (*listener_close)(void*, trevrpc_transport_handle_v1);
    int (*release_handle)(void*, trevrpc_transport_handle_v1, uint32_t);
    int (*close)(void*);
    int (*drain)(void*);
    int (*prepare_release)(void*);
    void (*destroy)(void*);
    uint64_t reserved[8];
} trevrpc_transport_provider_ops_v1;

typedef struct trevrpc_transport_provider_descriptor_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    const trevrpc_transport_provider_ops_v1* operations;
    void* context;
    uint64_t reserved[6];
} trevrpc_transport_provider_descriptor_v1;

uint32_t trevrpc_transport_provider_abi_version(void);
void trevrpc_transport_provider_abi_1_anchor(void);
int trevrpc_transport_provider_descriptor_v1_init(
    trevrpc_transport_provider_descriptor_v1* descriptor, size_t struct_size);
/* Invalid descriptors are not consumed. Once descriptor validation succeeds,
 * ownership of descriptor->context transfers to the Transport even if parent
 * allocation fails. */
int trevrpc_transport_provider_adopt_v1(
    const trevrpc_transport_provider_descriptor_v1* descriptor, trevrpc_transport** out_transport);

#ifdef __cplusplus
}
#endif

#endif
