#ifndef TREVRPC_ENGINE_PROVIDER_H
#define TREVRPC_ENGINE_PROVIDER_H

#include "trevrpc_engine.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_ENGINE_PROVIDER_ABI_VERSION 1u
#define TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1 1u

typedef struct trevrpc_engine_reservation trevrpc_engine_provider_reservation_v1;

typedef void (*trevrpc_engine_provider_detach_hook_v1)(void* provider_context, void* hook_context);
typedef void (*trevrpc_engine_provider_owned_release_v1)(void* owner, void* release_context);

typedef struct trevrpc_engine_provider_event_spec_v1 {
    uint32_t kind;
    uint32_t flags;
    int32_t status;
    uint32_t subject_kind;
    trevrpc_engine_handle_v1 subject;
    trevrpc_engine_handle_v1 parent;
    uint64_t operation_id;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    const void* data;
    size_t data_len;
    trevrpc_engine_provider_detach_hook_v1 dequeue_hook;
    trevrpc_engine_provider_detach_hook_v1 drop_hook;
    void* hook_context;
    trevrpc_engine_provider_detach_hook_v1 mandatory_commit_hook;
    trevrpc_engine_provider_detach_hook_v1 mandatory_abort_hook;
    void* mandatory_hook_context;
} trevrpc_engine_provider_event_spec_v1;

typedef struct trevrpc_engine_provider_diagnostics_v1 {
    uint64_t receive_owned_count;
    uint64_t peak_receive_owned_count;
    uint64_t receive_owned_bytes;
    uint64_t peak_receive_owned_bytes;
    uint64_t pending_send_bytes;
    uint64_t pending_send_count;
    uint64_t live_listeners;
    uint64_t live_connections;
    uint64_t live_streams;
} trevrpc_engine_provider_diagnostics_v1;

typedef struct trevrpc_engine_provider_ops_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    int (*attach)(void* provider_context, trevrpc_engine* engine);
    int (*listen)(void* provider_context,
        const trevrpc_engine_endpoint_config_v1* config,
        trevrpc_engine_provider_reservation_v1* terminal_reservation,
        trevrpc_engine_handle_v1* out_listener);
    int (*listener_get_port)(void* provider_context, trevrpc_engine_handle_v1 listener, uint16_t* out_port);
    int (*dial)(void* provider_context,
        const trevrpc_engine_endpoint_config_v1* config,
        uint64_t operation_id,
        trevrpc_engine_provider_reservation_v1* completion_reservation,
        trevrpc_engine_provider_reservation_v1* terminal_reservation,
        trevrpc_engine_handle_v1* out_connection);
    int (*dial_cancel)(void* provider_context, trevrpc_engine_handle_v1 connection);
    int (*connection_open_bidi_stream)(void* provider_context,
        trevrpc_engine_handle_v1 connection,
        uint64_t operation_id,
        trevrpc_engine_provider_reservation_v1* completion_reservation,
        trevrpc_engine_provider_reservation_v1* terminal_reservation,
        trevrpc_engine_handle_v1* out_stream);
    int (*stream_send_frame)(void* provider_context,
        trevrpc_engine_handle_v1 stream,
        uint64_t operation_id,
        const uint8_t* body,
        size_t body_len,
        trevrpc_engine_provider_reservation_v1* completion_reservation);
    int (*stream_receive_frame)(
        void* provider_context, trevrpc_engine_handle_v1 stream, trevrpc_engine_receive** out_receive);
    int (*stream_finish_send)(void* provider_context, trevrpc_engine_handle_v1 stream);
    int (*stream_abort_receive)(
        void* provider_context, trevrpc_engine_handle_v1 stream, uint64_t application_error_code);
    int (*stream_abort_send)(void* provider_context, trevrpc_engine_handle_v1 stream, uint64_t application_error_code);
    int (*stream_abort)(void* provider_context, trevrpc_engine_handle_v1 stream, uint64_t application_error_code);
    int (*stream_close)(void* provider_context, trevrpc_engine_handle_v1 stream);
    int (*connection_close)(
        void* provider_context, trevrpc_engine_handle_v1 connection, uint64_t application_error_code);
    int (*listener_close)(void* provider_context, trevrpc_engine_handle_v1 listener);
    int (*close)(void* provider_context);
    void (*get_diagnostics)(void* provider_context, trevrpc_engine_provider_diagnostics_v1* diagnostics);
    void (*destroy)(void* provider_context);
    uint64_t reserved[8];
} trevrpc_engine_provider_ops_v1;

typedef struct trevrpc_engine_provider_descriptor_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    const trevrpc_engine_provider_ops_v1* operations;
    void* context;
    uint64_t owner_cookie;
    uint64_t reserved[5];
} trevrpc_engine_provider_descriptor_v1;

/* Parent-owned Engine operations exposed through the provider boundary. Providers
 * must use this table instead of linking directly to trevrpc_engine_* symbols. */
typedef struct trevrpc_engine_provider_runtime_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    int (*config_init)(trevrpc_engine_config_v1*, size_t);
    int (*wake_source_init)(trevrpc_engine_wake_source_v1*, size_t);
    int (*endpoint_config_init)(trevrpc_engine_endpoint_config_v1*, size_t);
    int (*event_info_init)(trevrpc_engine_event_info_v1*, size_t);
    int (*receive_info_init)(trevrpc_engine_receive_info_v1*, size_t);
    int (*diagnostics_init)(trevrpc_engine_diagnostics_v1*, size_t);
    int (*adopt)(const trevrpc_engine_config_v1*, const trevrpc_engine_provider_descriptor_v1*, trevrpc_engine**);
    int (*get_wake_source)(trevrpc_engine*, trevrpc_engine_wake_source_v1*);
    int (*next_event)(trevrpc_engine*, trevrpc_engine_event**);
    int (*event_get_info)(const trevrpc_engine_event*, trevrpc_engine_event_info_v1*);
    void (*event_release)(trevrpc_engine_event*);
    int (*receive_get_info)(const trevrpc_engine_receive*, trevrpc_engine_receive_info_v1*);
    void (*receive_release)(trevrpc_engine_receive*);
    int (*get_diagnostics)(trevrpc_engine*, trevrpc_engine_diagnostics_v1*);
    int (*listen)(trevrpc_engine*, const trevrpc_engine_endpoint_config_v1*, trevrpc_engine_handle_v1*);
    int (*listener_get_port)(trevrpc_engine*, trevrpc_engine_handle_v1, uint16_t*);
    int (*dial)(trevrpc_engine*, const trevrpc_engine_endpoint_config_v1*, uint64_t, trevrpc_engine_handle_v1*);
    int (*dial_cancel)(trevrpc_engine*, trevrpc_engine_handle_v1);
    int (*connection_open_bidi_stream)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t, trevrpc_engine_handle_v1*);
    int (*stream_send_frame)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t, const uint8_t*, size_t);
    int (*stream_receive_frame)(trevrpc_engine*, trevrpc_engine_handle_v1, trevrpc_engine_receive**);
    int (*stream_finish_send)(trevrpc_engine*, trevrpc_engine_handle_v1);
    int (*stream_abort_receive)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t);
    int (*stream_abort_send)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t);
    int (*stream_abort)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t);
    int (*stream_close)(trevrpc_engine*, trevrpc_engine_handle_v1);
    int (*connection_close)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t);
    int (*listener_close)(trevrpc_engine*, trevrpc_engine_handle_v1);
    int (*close)(trevrpc_engine*);
    int (*drain)(trevrpc_engine*);
    int (*release)(trevrpc_engine*);
    uint64_t reserved[8];
} trevrpc_engine_provider_runtime_v1;

typedef struct trevrpc_engine_provider_host_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    int (*callback_enter)(trevrpc_engine*);
    void (*callback_leave)(trevrpc_engine*);
    int (*operation_pin)(trevrpc_engine*);
    void (*operation_unpin)(trevrpc_engine*);
    int (*reserve_mandatory)(trevrpc_engine*, trevrpc_engine_provider_reservation_v1**);
    int (*publish_reserved)(
        trevrpc_engine*, trevrpc_engine_provider_reservation_v1*, const trevrpc_engine_provider_event_spec_v1*);
    void (*cancel_reservation)(trevrpc_engine*, trevrpc_engine_provider_reservation_v1*);
    int (*publish_event)(trevrpc_engine*, const trevrpc_engine_provider_event_spec_v1*);
    void (*fail)(trevrpc_engine*, int32_t, uint64_t);
    void (*stopped)(trevrpc_engine*, int32_t, uint64_t);
    int (*receive_create_copy)(const void*, size_t, uint32_t, trevrpc_engine_receive**);
    int (*receive_create_owned)(
        void*, size_t, uint32_t, void*, trevrpc_engine_provider_owned_release_v1, void*, trevrpc_engine_receive**);
    uint64_t (*owner_cookie)(const trevrpc_engine*);
    void* (*provider_context)(const trevrpc_engine*);
    const trevrpc_engine_provider_runtime_v1* runtime;
    uint64_t reserved[7];
} trevrpc_engine_provider_host_v1;

uint32_t trevrpc_engine_provider_abi_version(void);
void trevrpc_engine_provider_abi_1_anchor(void);
int trevrpc_engine_provider_descriptor_v1_init(trevrpc_engine_provider_descriptor_v1* descriptor, size_t struct_size);
const trevrpc_engine_provider_host_v1* trevrpc_engine_provider_host_v1_get(void);
/* Invalid descriptors are not consumed. Once descriptor validation succeeds,
 * ownership of descriptor->context transfers to the Engine even if creation fails. */
int trevrpc_engine_provider_adopt_v1(const trevrpc_engine_config_v1* config,
    const trevrpc_engine_provider_descriptor_v1* descriptor,
    trevrpc_engine** out_engine);

#ifdef __cplusplus
}
#endif

#endif
