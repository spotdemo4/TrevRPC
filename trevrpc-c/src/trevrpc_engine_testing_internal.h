#ifndef TREVRPC_ENGINE_TESTING_INTERNAL_H
#define TREVRPC_ENGINE_TESTING_INTERNAL_H

#include "trevrpc_engine.h"

#include <stddef.h>
#include <stdint.h>

#define TREVRPC_ENGINE_TEST_WAKE_FAILURE_READ 1u
#define TREVRPC_ENGINE_TEST_WAKE_FAILURE_WRITE 2u

int trevrpc_engine_testing_create(const trevrpc_engine_config_v1* config, trevrpc_engine** out_engine);
int trevrpc_engine_testing_enqueue_diagnostic_copy(trevrpc_engine* engine, const void* data, size_t data_len);
int trevrpc_engine_testing_producer_acquire(trevrpc_engine* engine);
int trevrpc_engine_testing_producer_release(trevrpc_engine* engine);
void trevrpc_engine_testing_pause_last_event_drain(trevrpc_engine* engine);
void trevrpc_engine_testing_resume_last_event_drain(trevrpc_engine* engine);
int trevrpc_engine_testing_get_write_fd(trevrpc_engine* engine);
int trevrpc_engine_testing_force_wake_failure(trevrpc_engine* engine, uint32_t operation, uint64_t counter_seed);
void trevrpc_engine_testing_pause_api_enter(trevrpc_engine* engine);
void trevrpc_engine_testing_wait_for_api_enter(trevrpc_engine* engine);
void trevrpc_engine_testing_resume_api_enter(trevrpc_engine* engine);
void trevrpc_engine_testing_pause_api_leave(trevrpc_engine* engine);
void trevrpc_engine_testing_wait_for_api_leave(trevrpc_engine* engine);
void trevrpc_engine_testing_resume_api_leave(trevrpc_engine* engine);
int trevrpc_engine_testing_release_is_waiting(const trevrpc_engine* engine);

#endif
