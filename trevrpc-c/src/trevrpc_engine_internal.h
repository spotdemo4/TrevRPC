#ifndef TREVRPC_ENGINE_INTERNAL_H
#define TREVRPC_ENGINE_INTERNAL_H

#include "trevrpc_engine_provider.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef trevrpc_engine_provider_reservation_v1 trevrpc_engine_reservation;
typedef trevrpc_engine_provider_detach_hook_v1 trevrpc_engine_detach_hook;
typedef trevrpc_engine_provider_owned_release_v1 trevrpc_engine_owned_release;
typedef trevrpc_engine_provider_event_spec_v1 trevrpc_engine_event_spec;
typedef trevrpc_engine_provider_diagnostics_v1 trevrpc_engine_provider_diagnostics;
typedef trevrpc_engine_provider_ops_v1 trevrpc_engine_provider_ops;

int trevrpc_engine_provider_create_v1(const trevrpc_engine_config_v1* config,
    const trevrpc_engine_provider_ops_v1* provider_ops,
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
