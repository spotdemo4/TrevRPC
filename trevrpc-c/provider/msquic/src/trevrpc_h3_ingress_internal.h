#ifndef TREVRPC_H3_INGRESS_INTERNAL_H
#define TREVRPC_H3_INGRESS_INTERNAL_H

#include "trevrpc_h3_demux_internal.h"
#include "trevrpc_msquic_internal.h"

#include <stddef.h>
#include <stdint.h>
#ifdef TREVRPC_H3_INGRESS_TESTING
#include <time.h>
#endif

#define TREV_H3_INGRESS_APP_INTERNAL_ERROR 0x102
#define TREV_H3_INGRESS_APP_EXCESSIVE_LOAD 0x107

#define TREV_H3_INGRESS_SETTINGS_PENDING 0
#define TREV_H3_INGRESS_SETTINGS_READY 1
#define TREV_H3_INGRESS_SETTINGS_FAILED 2

typedef struct trevrpc_h3_ingress trevrpc_h3_ingress;

typedef enum trevrpc_h3_ingress_bidi_mode {
    TREV_H3_INGRESS_BIDI_CLASSIFY = 0,
    TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED = 1,
} trevrpc_h3_ingress_bidi_mode;

#ifdef TREVRPC_H3_INGRESS_TESTING
typedef int (*trevrpc_h3_ingress_clock_gettime)(void* context, clockid_t clock_id, struct timespec* out_time);
#endif

typedef struct trevrpc_h3_ingress_config {
    size_t max_live_entries;
    size_t max_queue_entries;
    trevrpc_h3_ingress_bidi_mode bidi_mode;
#ifdef TREVRPC_H3_INGRESS_TESTING
    trevrpc_h3_ingress_clock_gettime clock_gettime;
    void* clock_context;
#endif
} trevrpc_h3_ingress_config;

typedef void (*trevrpc_h3_ingress_observer)(void* context, uint32_t flags);

/* Private transport seam. Every accepted stream is released exactly once. */
typedef struct trevrpc_h3_ingress_transport_ops {
    int (*conn_accept_stream)(void* conn, void** out_stream);
    void (*conn_shutdown)(void* conn);
    void (*conn_shutdown_error)(void* conn, uint64_t application_error);
    int (*stream_id)(void* stream, uint64_t* out_stream_id);
    int (*stream_set_observer)(void* stream, trevrpc_h3_ingress_observer observer, void* observer_context);
    void (*stream_clear_observer)(void* stream);
    void (*stream_drain_observer)(void* stream);
    intptr_t (*stream_read_protocol_ready)(void* stream, uint8_t* data, size_t len);
    int (*stream_abort_with_error)(void* stream, uint64_t application_error);
    void (*stream_close)(void* stream);
} trevrpc_h3_ingress_transport_ops;

typedef struct trevrpc_h3_ingress_item {
    trevrpc_msquic_stream* stream;
    uint64_t stream_id;
    uint64_t accepted_at_nanos;
    trevrpc_h3_demux_direction direction;
    trevrpc_h3_demux_action action;
    uint64_t first_value;
    uint64_t session_id;
    trevrpc_h3_ingress_transport_ops _ops;
    void* _transport_stream;
} trevrpc_h3_ingress_item;

int trevrpc_h3_ingress_create_with_ops(void* conn,
    const trevrpc_h3_ingress_transport_ops* ops,
    const trevrpc_h3_ingress_config* config,
    trevrpc_h3_ingress** out_runtime);
int trevrpc_h3_ingress_start(trevrpc_h3_ingress* runtime);

/* timeout_nanos == 0 blocks; failure leaves out_item unchanged. */
int trevrpc_h3_ingress_pop(trevrpc_h3_ingress* runtime,
    trevrpc_h3_demux_action action,
    uint64_t timeout_nanos,
    trevrpc_h3_ingress_item* out_item);
/* Mode-specific bidi pops share the same timeout, ownership, and failure contract. */
int trevrpc_h3_ingress_pop_bidi(trevrpc_h3_ingress* runtime, uint64_t timeout_nanos, trevrpc_h3_ingress_item* out_item);
int trevrpc_h3_ingress_pop_unclassified_bidi(
    trevrpc_h3_ingress* runtime, uint64_t timeout_nanos, trevrpc_h3_ingress_item* out_item);

/* Close the popped item, or take it and become its sole owner. */
void trevrpc_h3_ingress_item_close(trevrpc_h3_ingress_item* item);
trevrpc_msquic_stream* trevrpc_h3_ingress_item_take_stream(trevrpc_h3_ingress_item* item);

/* The first fatal application error wins and shuts down the connection. */
int trevrpc_h3_ingress_fail(trevrpc_h3_ingress* runtime, uint64_t application_error);
int trevrpc_h3_ingress_fatal_error(trevrpc_h3_ingress* runtime, uint64_t* out_application_error);

/* SETTINGS parsing stays with the CONTROL consumer; publication is first-wins. */
int trevrpc_h3_ingress_publish_peer_settings(
    trevrpc_h3_ingress* runtime, int status, trevrpc_wt_profile_id selected_profile, uint64_t application_error);
int trevrpc_h3_ingress_wait_peer_settings(trevrpc_h3_ingress* runtime,
    int* out_status,
    trevrpc_wt_profile_id* out_selected_profile,
    uint64_t* out_application_error);

/* Shutdown is idempotent and joins both runtime threads before returning. */
void trevrpc_h3_ingress_shutdown(trevrpc_h3_ingress* runtime);
/*
 * Exact-once final release. The public identity remains a closed tombstone, so
 * a call delayed before admission returns -EPIPE rather than aliasing a later runtime.
 */
void trevrpc_h3_ingress_release(trevrpc_h3_ingress* runtime);

#ifdef TREVRPC_H3_INGRESS_TESTING
void trevrpc_h3_ingress_test_pause_before_reference(trevrpc_h3_ingress* runtime, int pause);
void trevrpc_h3_ingress_test_wait_before_reference_paused(trevrpc_h3_ingress* runtime);
void trevrpc_h3_ingress_test_pause_after_admission(trevrpc_h3_ingress* runtime, int pause);
void trevrpc_h3_ingress_test_wait_admission_paused(trevrpc_h3_ingress* runtime);
void trevrpc_h3_ingress_test_recycle_runtime_allocations(int recycle);
void* trevrpc_h3_ingress_test_runtime_address(trevrpc_h3_ingress* runtime);
void trevrpc_h3_ingress_test_wait_runtime_detached(trevrpc_h3_ingress* runtime);
void trevrpc_h3_ingress_test_wait_runtime_reaped(trevrpc_h3_ingress* runtime);
size_t trevrpc_h3_ingress_test_runtime_reap_count(trevrpc_h3_ingress* runtime);
void trevrpc_h3_ingress_test_force_next_timed_wait_timeout(void);
int trevrpc_h3_ingress_test_wait_timed_wait_entered(void);
size_t trevrpc_h3_ingress_test_registry_size(void);
void trevrpc_h3_ingress_test_released_count_reset(void);
size_t trevrpc_h3_ingress_test_released_count_get(void);
#endif

#endif
