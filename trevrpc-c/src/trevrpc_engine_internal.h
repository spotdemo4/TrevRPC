#ifndef TREVRPC_ENGINE_INTERNAL_H
#define TREVRPC_ENGINE_INTERNAL_H

#include "trevrpc_engine.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct trevrpc_engine_reservation trevrpc_engine_reservation;

typedef void (*trevrpc_engine_detach_hook)(void* provider_context, void* hook_context);
typedef void (*trevrpc_engine_owned_release)(void* owner, void* release_context);

typedef struct trevrpc_engine_event_spec {
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
    trevrpc_engine_detach_hook dequeue_hook;
    trevrpc_engine_detach_hook drop_hook;
    void* hook_context;
} trevrpc_engine_event_spec;

typedef struct trevrpc_engine_provider_diagnostics {
    uint64_t receive_owned_count;
    uint64_t peak_receive_owned_count;
    uint64_t receive_owned_bytes;
    uint64_t peak_receive_owned_bytes;
    uint64_t pending_send_bytes;
    uint64_t pending_send_count;
    uint64_t live_listeners;
    uint64_t live_connections;
    uint64_t live_streams;
} trevrpc_engine_provider_diagnostics;

typedef struct trevrpc_engine_provider_ops {
    int (*attach)(void* provider_context, trevrpc_engine* engine);
    int (*listen)(void* provider_context,
        const trevrpc_engine_endpoint_config_v1* config,
        trevrpc_engine_reservation* terminal_reservation,
        trevrpc_engine_handle_v1* out_listener);
    int (*listener_get_port)(void* provider_context, trevrpc_engine_handle_v1 listener, uint16_t* out_port);
    int (*dial)(void* provider_context,
        const trevrpc_engine_endpoint_config_v1* config,
        uint64_t operation_id,
        trevrpc_engine_reservation* completion_reservation,
        trevrpc_engine_reservation* terminal_reservation,
        trevrpc_engine_handle_v1* out_connection);
    int (*dial_cancel)(void* provider_context, trevrpc_engine_handle_v1 connection);
    int (*connection_open_bidi_stream)(void* provider_context,
        trevrpc_engine_handle_v1 connection,
        uint64_t operation_id,
        trevrpc_engine_reservation* completion_reservation,
        trevrpc_engine_reservation* terminal_reservation,
        trevrpc_engine_handle_v1* out_stream);
    int (*stream_send_frame)(void* provider_context,
        trevrpc_engine_handle_v1 stream,
        uint64_t operation_id,
        const uint8_t* body,
        size_t body_len,
        trevrpc_engine_reservation* completion_reservation);
    int (*stream_receive_frame)(
        void* provider_context, trevrpc_engine_handle_v1 stream, trevrpc_engine_receive** out_receive);
    int (*stream_finish_send)(void* provider_context, trevrpc_engine_handle_v1 stream);
    int (*stream_abort)(void* provider_context, trevrpc_engine_handle_v1 stream, uint64_t application_error_code);
    int (*stream_close)(void* provider_context, trevrpc_engine_handle_v1 stream);
    int (*connection_close)(
        void* provider_context, trevrpc_engine_handle_v1 connection, uint64_t application_error_code);
    int (*listener_close)(void* provider_context, trevrpc_engine_handle_v1 listener);
    /* close starts shutdown; even on error the provider must report quiescence with provider_stopped. */
    int (*close)(void* provider_context);
    void (*get_diagnostics)(void* provider_context, trevrpc_engine_provider_diagnostics* diagnostics);
    void (*destroy)(void* provider_context);
} trevrpc_engine_provider_ops;

int trevrpc_engine_provider_create_v1(const trevrpc_engine_config_v1* config,
    const trevrpc_engine_provider_ops* provider_ops,
    void* provider_context,
    uint64_t owner_cookie,
    trevrpc_engine** out_engine);

int trevrpc_engine_provider_callback_enter(trevrpc_engine* engine);
void trevrpc_engine_provider_callback_leave(trevrpc_engine* engine);
/* Operation pins are transferable and may be released by a different thread. */
int trevrpc_engine_provider_operation_pin(trevrpc_engine* engine);
void trevrpc_engine_provider_operation_unpin(trevrpc_engine* engine);

int trevrpc_engine_provider_reserve_mandatory(trevrpc_engine* engine, trevrpc_engine_reservation** out_reservation);
int trevrpc_engine_provider_publish_reserved(
    trevrpc_engine* engine, trevrpc_engine_reservation* reservation, const trevrpc_engine_event_spec* spec);
void trevrpc_engine_provider_cancel_reservation(trevrpc_engine* engine, trevrpc_engine_reservation* reservation);
int trevrpc_engine_provider_publish_event(trevrpc_engine* engine, const trevrpc_engine_event_spec* spec);
void trevrpc_engine_provider_fail(trevrpc_engine* engine, int32_t status, uint64_t provider_error_code);
void trevrpc_engine_provider_stopped(trevrpc_engine* engine, int32_t status, uint64_t provider_error_code);

int trevrpc_engine_receive_create_copy(
    const void* data, size_t data_len, uint32_t flags, trevrpc_engine_receive** out_receive);
int trevrpc_engine_receive_create_owned(void* data,
    size_t data_len,
    uint32_t flags,
    void* owner,
    trevrpc_engine_owned_release release,
    void* release_context,
    trevrpc_engine_receive** out_receive);

uint64_t trevrpc_engine_provider_owner_cookie(const trevrpc_engine* engine);
void* trevrpc_engine_provider_context(const trevrpc_engine* engine);

/* Private hooks used only by the test-support archive. */
int trevrpc_engine_internal_test_producer_acquire(trevrpc_engine* engine);
void trevrpc_engine_internal_test_producer_release(trevrpc_engine* engine);
void trevrpc_engine_internal_test_pause_last_event_drain(trevrpc_engine* engine);
void trevrpc_engine_internal_test_resume_last_event_drain(trevrpc_engine* engine);
int trevrpc_engine_internal_test_get_write_fd(trevrpc_engine* engine);
int trevrpc_engine_internal_test_force_wake_failure(trevrpc_engine* engine, uint32_t operation, uint64_t counter_seed);
void trevrpc_engine_internal_test_pause_api_enter(trevrpc_engine* engine);
void trevrpc_engine_internal_test_wait_for_api_enter(trevrpc_engine* engine);
void trevrpc_engine_internal_test_resume_api_enter(trevrpc_engine* engine);
void trevrpc_engine_internal_test_pause_api_leave(trevrpc_engine* engine);
void trevrpc_engine_internal_test_wait_for_api_leave(trevrpc_engine* engine);
void trevrpc_engine_internal_test_resume_api_leave(trevrpc_engine* engine);
int trevrpc_engine_internal_test_release_is_waiting(const trevrpc_engine* engine);

#endif
